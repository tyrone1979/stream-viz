"""Real open-data source for StreamViz: the MIT-BIH Arrhythmia Database.

Provenance
----------
* Dataset : MIT-BIH Arrhythmia Database, version 1.0.0 (PhysioNet)
* DOI     : 10.13026/C2F305
* License : Open Data Commons Attribution License v1.0 (ODC-By 1.0)
* Source  : https://physionet.org/files/mitdb/1.0.0/
* Citation: Moody GB, Mark RG. The impact of the MIT-BIH Arrhythmia
            Database. IEEE Engineering in Medicine and Biology Magazine.
            2001;20(3):45-50.

Record 100 is a 30-minute, two-lead (MLII, V5) ambulatory ECG digitised at
360 Hz with 11-bit resolution.  The signal files are WFDB ``212`` format:
three bytes carry two interleaved 12-bit two's-complement samples.

Unlike :mod:`src.synthetic`, nothing here is generated from a formula.  Every
payload value is derived from the recorded samples:

* ``WAVEFORM`` -- 256 consecutive recorded samples of one lead;
* ``SPECTRUM`` -- magnitude of a Hann-windowed real FFT of a 512-sample
  window of the same lead, scaled by a fixed record-wide reference;
* ``AUDIO``    -- *audification*: the recorded samples are played back as a
  16 kHz int16 waveform (time compression, no synthesised tone).  This makes
  the low-frequency ECG audible; it is not a separate audio recording.

Normalisation (zero-centering and division by a robust amplitude reference)
is the only preprocessing.  It keeps the real signal inside the fixed display
scales and does not alter its shape.  The original ADC/mV values remain
available on :class:`RealRecord` for anyone who needs physical units.
"""

from __future__ import annotations

import hashlib
import socket
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional

import numpy as np

from .protocol import PacketType, encode, now_ms, packet_size, rewrite_header

_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_DIR = _ROOT / "data" / "mitdb"

DATASET: Dict[str, str] = {
    "name": "MIT-BIH Arrhythmia Database",
    "version": "1.0.0",
    "publisher": "PhysioNet",
    "license": "Open Data Commons Attribution License v1.0 (ODC-By 1.0)",
    "doi": "10.13026/C2F305",
    "url": "https://physionet.org/files/mitdb/1.0.0/",
    "citation": (
        "Moody GB, Mark RG. The impact of the MIT-BIH Arrhythmia Database. "
        "IEEE Engineering in Medicine and Biology Magazine. 2001;20(3):45-50."
    ),
}

# SHA-256 of the exact files used for the reported measurements.  Keeping the
# hashes here makes any published number traceable to a byte-identical input.
EXPECTED_SHA256: Dict[str, str] = {
    "100.hea": "60ebc904c7bf3e04d142638d3fd5c903e8dc9f10f1ea3264e07926aa089ee75e",
    "100.dat": "b2ea3c250e56e48f4b7b90697832b8ecd1afa1e0bb31f2dcfea4ed6e1075a639",
    "100.atr": "8d8a5349fb16638ebbf649f1779d12e96d91b736b2aafe59db43719ae583d471",
}

# One frame consumes this many recorded samples and emits four datagrams.
_FRAME_SAMPLES = 1024
_FRAME_PACKETS = (
    PacketType.SPECTRUM,
    PacketType.WAVEFORM,
    PacketType.AUDIO,
    PacketType.AUDIO,
)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def verify_files(data_dir: Path, record: str = "100") -> Dict[str, str]:
    """Return hashes of the signal/header files, refusing unknown inputs."""
    data_dir = Path(data_dir)
    found: Dict[str, str] = {}
    for suffix in ("hea", "dat"):
        name = f"{record}.{suffix}"
        path = data_dir / name
        if not path.is_file():
            raise FileNotFoundError(
                f"missing {path}; run `python tools/fetch_dataset.py` first"
            )
        digest = sha256_file(path)
        expected = EXPECTED_SHA256.get(name)
        if expected and digest != expected:
            raise ValueError(f"checksum mismatch for {name}: {digest}")
        found[name] = digest
    return found


def _decode_212(blob: bytes) -> np.ndarray:
    """Vectorised WFDB 212 decoder -> one flat array of int16 ADC units."""
    if len(blob) % 3:
        blob = blob[: len(blob) - len(blob) % 3]
    trio = np.frombuffer(blob, dtype=np.uint8).reshape(-1, 3)
    b0 = trio[:, 0].astype(np.uint16)
    b1 = trio[:, 1].astype(np.uint16)
    b2 = trio[:, 2].astype(np.uint16)
    first = (b0 | ((b1 & 0x0F) << 8)).astype(np.int32)
    second = ((b1 >> 4) | (b2 << 4)).astype(np.int32)
    first = np.where(first >= 2048, first - 4096, first)
    second = np.where(second >= 2048, second - 4096, second)
    out = np.empty(len(trio) * 2, dtype=np.int16)
    out[0::2] = first.astype(np.int16)
    out[1::2] = second.astype(np.int16)
    return out


@dataclass
class RealRecord:
    """A verified, decoded PhysioNet recording."""

    record: str
    fs: int
    n_samples: int
    sig_names: tuple
    gain: float
    adc_zero: float
    signals_mv: np.ndarray                 # (n_samples, n_channels), physical units
    signals: np.ndarray                    # same shape, robust-normalised to ~[-1, 1]
    spectrum_reference: float
    checksums: Dict[str, str] = field(default_factory=dict)

    @property
    def n_channels(self) -> int:
        return self.signals.shape[1]

    @property
    def provenance(self) -> Dict:
        return {
            **DATASET,
            "record": self.record,
            "sampling_rate_hz": self.fs,
            "samples_per_channel": self.n_samples,
            "channels": list(self.sig_names),
            "duration_s": round(self.n_samples / self.fs, 3),
            "sha256": self.checksums,
        }


def load_record(record: str = "100", data_dir: Optional[Path] = None) -> RealRecord:
    """Read ``<record>.hea`` / ``<record>.dat`` and decode the real ECG."""
    data_dir = Path(data_dir) if data_dir else DEFAULT_DATA_DIR
    checksums = verify_files(data_dir, record)

    header_lines = [
        line.strip()
        for line in (data_dir / f"{record}.hea").read_text(encoding="ascii").splitlines()
        if line.strip() and not line.startswith("#")
    ]
    parts = header_lines[0].split()
    n_channels, fs, n_samples = int(parts[1]), int(parts[2]), int(parts[3])
    sig_lines = header_lines[1 : 1 + n_channels]
    fmt = sig_lines[0].split()[1]
    if fmt != "212":
        raise ValueError(f"unsupported WFDB format {fmt!r}; expected 212")
    gain = float(sig_lines[0].split()[2])
    adc_zero = float(sig_lines[0].split()[4])
    sig_names = tuple(line.split()[-1] for line in sig_lines)

    blob = (data_dir / f"{record}.dat").read_bytes()
    flat = _decode_212(blob)
    needed = n_samples * n_channels
    if len(flat) < needed:
        raise ValueError(f"truncated signal file: {len(flat)} < {needed} samples")
    interleaved = flat[:needed].reshape(n_samples, n_channels)
    signals_mv = (interleaved.astype(np.float32) - adc_zero) / gain

    # Per-lead robust normalisation.  Lead amplitudes differ by an order of
    # magnitude in this recording (V5 carries much stronger baseline drift),
    # so a single global scale would flatten one channel and saturate the
    # other.  Each lead is zero-centred and scaled by its own 99th percentile
    # absolute amplitude; this is display normalisation only and is applied
    # identically to every sample of that lead.
    centred = signals_mv - np.median(signals_mv, axis=0, keepdims=True)
    scales = np.percentile(np.abs(centred), 99, axis=0, keepdims=True)
    scales = np.maximum(scales, 1e-6)
    signals = np.clip(centred / scales, -1.5, 1.5).astype(np.float32)

    # Fixed spectral reference from many real windows of BOTH leads, so
    # colours remain comparable across frames.  Each window is DC-removed
    # first: otherwise bin 0 (baseline drift) saturates the colour scale.
    window = np.hanning(512).astype(np.float32)
    offsets = np.linspace(0, n_samples - 512, 64).astype(int)
    peak_magnitudes = []
    for channel in range(n_channels):
        for o in offsets:
            segment = signals[o : o + 512, channel].copy()
            segment -= segment.mean()
            magnitude = np.abs(np.fft.rfft(segment * window)) / 512.0 * 2.0
            peak_magnitudes.append(float(magnitude[1:].max()))
    spectrum_reference = float(np.percentile(peak_magnitudes, 90))
    spectrum_reference = max(spectrum_reference, 1e-6)

    return RealRecord(
        record=record,
        fs=fs,
        n_samples=n_samples,
        sig_names=sig_names,
        gain=gain,
        adc_zero=adc_zero,
        signals_mv=signals_mv,
        signals=signals,
        spectrum_reference=spectrum_reference,
        checksums=checksums,
    )


@dataclass
class ReplaySpec:
    host: str = "127.0.0.1"
    port: int = 19000
    duration_s: float = 8.0
    rate_mbps: float = 4.0
    n_bins: int = 128
    waveform_len: int = 256
    audio_len: int = 512
    sample_rate: int = 16_000
    malformed_ratio: float = 0.0
    record: str = "100"
    data_dir: Optional[Path] = None
    loop: bool = True


def average_packet_bytes(spec: ReplaySpec) -> float:
    sizes = 0
    for ptype in _FRAME_PACKETS:
        n = spec.n_bins if ptype is PacketType.SPECTRUM else (
            spec.waveform_len if ptype is PacketType.WAVEFORM else spec.audio_len
        )
        sizes += packet_size(ptype, n)
    return sizes / len(_FRAME_PACKETS)


def _frame_payloads(
    rec: RealRecord, spec: ReplaySpec, frame_index: int
) -> list:
    """Build the four payloads of one frame entirely from recorded samples."""
    n_frames = rec.n_samples // _FRAME_SAMPLES
    start = (frame_index % n_frames) * _FRAME_SAMPLES
    channel = frame_index % rec.n_channels
    block = rec.signals[start : start + _FRAME_SAMPLES, channel]

    segment = block[:512].copy()
    segment -= segment.mean()  # remove DC / baseline drift before the FFT
    window = np.hanning(len(segment)).astype(np.float32)
    spectrum = np.abs(np.fft.rfft(segment * window)) / 512.0 * 2.0
    spectrum[0] = 0.0
    spectrum = spectrum[: spec.n_bins].astype(np.float32)
    if len(spectrum) < spec.n_bins:
        spectrum = np.pad(spectrum, (0, spec.n_bins - len(spectrum)))
    spectrum /= rec.spectrum_reference

    waveform = block[: spec.waveform_len].astype(np.float32)

    # Audification: the real samples themselves, only rescaled to audible
    # int16 range.  No oscillator, no noise, no synthesised tone.
    half = spec.audio_len
    audio_a = np.clip(block[:half] * 25_000, -32768, 32767).astype(np.int16)
    audio_b = np.clip(block[half : half * 2] * 25_000, -32768, 32767).astype(np.int16)

    return [
        (PacketType.SPECTRUM, spectrum),
        (PacketType.WAVEFORM, waveform),
        (PacketType.AUDIO, audio_a),
        (PacketType.AUDIO, audio_b),
    ]


def send_record(
    spec: ReplaySpec,
    stop_event: Optional[threading.Event] = None,
    start_delay: float = 0.0,
    record: Optional[RealRecord] = None,
) -> Dict:
    """Stream a real recording over UDP; interface matches ``send_stream``."""
    if start_delay > 0:
        time.sleep(start_delay)
    rec = record if record is not None else load_record(spec.record, spec.data_dir)
    avg_bytes = average_packet_bytes(spec)
    interval = 1.0 / max(1.0, spec.rate_mbps * 1e6 / (avg_bytes * 8))
    total_frames = rec.n_samples // _FRAME_SAMPLES

    # Pre-encode whole datagrams once.  FFT, array conversion and struct
    # packing must not sit in the pacing loop, otherwise the sender -- not
    # the pipeline under test -- becomes the throughput ceiling.  Every
    # payload byte still comes from the recording; only seq and the
    # wall-clock timestamp are rewritten at send time.
    frames = [
        [
            bytearray(encode(ptype, i % rec.n_channels, 0, 1, values))
            for ptype, values in _frame_payloads(rec, spec, i)
        ]
        for i in range(total_frames)
    ]

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    target = (spec.host, spec.port)
    rng = np.random.default_rng(0)  # only used for optional fault injection
    t0 = time.perf_counter()
    deadline = t0 + spec.duration_s
    next_slot = t0
    seq = 0
    sent = 0
    malformed = 0
    frames_used = 0
    try:
        while time.perf_counter() < deadline:
            if stop_event is not None and stop_event.is_set():
                break
            frame_index = frames_used
            if not spec.loop and frame_index >= total_frames:
                break
            frames_used += 1
            for buf in frames[frame_index % total_frames]:
                if time.perf_counter() >= deadline:
                    break
                rewrite_header(buf, seq, now_ms())
                if spec.malformed_ratio and rng.random() < spec.malformed_ratio:
                    packet = b"BAD!" + bytes(buf[4:])
                    malformed += 1
                else:
                    packet = buf
                sock.sendto(packet, target)
                sent += 1
                seq += 1
                next_slot += interval
                remaining = next_slot - time.perf_counter()
                if remaining > 0.02:
                    time.sleep(remaining - 0.01)
                    while time.perf_counter() < next_slot:
                        pass
                elif remaining > 0:
                    while time.perf_counter() < next_slot:
                        pass
                else:
                    next_slot = time.perf_counter()
    finally:
        sock.close()
    return {
        "sent_packets": sent,
        "sent_malformed": malformed,
        "frames_used": frames_used,
        "distinct_signal_seconds": round(
            min(frames_used, total_frames) * _FRAME_SAMPLES / rec.fs, 3
        ),
        "source": rec.provenance,
        "duration_s": round(time.perf_counter() - t0, 3),
        "average_packet_bytes": round(avg_bytes, 1),
    }
