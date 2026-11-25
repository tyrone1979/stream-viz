"""Replay of verified real open data over the StreamViz UDP transport.

Dataset-specific decoding lives in :mod:`src.datasets`; this module only
turns a :class:`~src.datasets.RealRecord` into the three generic payload
types and paces them onto the wire.  Nothing here knows whether the samples
are an ECG, a gravitational-wave strain, or a future third domain -- which
is the point of the adapter layer.

For every frame of 1024 recorded samples, four datagrams are emitted:

* ``WAVEFORM`` -- 256 consecutive recorded samples of one channel;
* ``SPECTRUM`` -- magnitude of a Hann-windowed real FFT of a 512-sample
  window of the same channel, scaled by a fixed record-wide reference;
* ``AUDIO`` x2 -- *audification*: the recorded samples themselves are played
  back as a 16 kHz int16 waveform (time compression, no synthesised tone).

The original physical-unit samples and the full provenance (license, DOI,
citation, SHA-256, preprocessing) remain on the record; see ``src/datasets.py``.
"""

from __future__ import annotations

import socket
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

import numpy as np

from .datasets import (
    DATASET,                    # noqa: F401 - backwards-compatible re-export
    DEFAULT_DATA_DIR,           # noqa: F401
    EXPECTED_SHA256,            # noqa: F401
    FRAME_SAMPLES,
    RealRecord,                 # noqa: F401
    _decode_212,                # noqa: F401
    load_dataset,
    load_record,                # noqa: F401
    sha256_file,                # noqa: F401
    verify_files,               # noqa: F401
)
from .protocol import PacketType, encode, now_ms, packet_size, rewrite_header

_FRAME_PACKETS = (
    PacketType.SPECTRUM,
    PacketType.WAVEFORM,
    PacketType.AUDIO,
    PacketType.AUDIO,
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
    dataset: str = "mitdb"          # any key in src.datasets.REGISTRY
    record: str = "100"             # legacy alias, ignored by the registry
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
    n_frames = rec.n_samples // FRAME_SAMPLES
    start = (frame_index % n_frames) * FRAME_SAMPLES
    channel = frame_index % rec.n_channels
    block = rec.signals[start : start + FRAME_SAMPLES, channel]

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
    """Stream any registered real recording over UDP (same API as send_stream)."""
    if start_delay > 0:
        time.sleep(start_delay)
    rec = record if record is not None else load_dataset(spec.dataset, spec.data_dir)
    avg_bytes = average_packet_bytes(spec)
    interval = 1.0 / max(1.0, spec.rate_mbps * 1e6 / (avg_bytes * 8))
    total_frames = rec.n_samples // FRAME_SAMPLES

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
                    # Behind schedule (OS scheduling under load).  Keep the
                    # accumulated debt so a short catch-up burst restores the
                    # long-run target rate instead of permanently lowering it;
                    # cap the debt so a suspended process cannot flood the
                    # receiver on wake-up.
                    behind = time.perf_counter() - next_slot
                    if behind > 0.1:
                        next_slot = time.perf_counter() - 0.1
    finally:
        sock.close()
    return {
        "sent_packets": sent,
        "sent_malformed": malformed,
        "frames_used": frames_used,
        "distinct_signal_seconds": round(
            min(frames_used, total_frames) * FRAME_SAMPLES / rec.fs, 3
        ),
        "source": rec.provenance,
        "duration_s": round(time.perf_counter() - t0, 3),
        "average_packet_bytes": round(avg_bytes, 1),
    }
