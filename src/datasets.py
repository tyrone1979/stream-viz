"""Pluggable adapters for real, openly licensed time-series datasets.

The StreamViz ingest/display pipeline is domain-independent: it only ever
sees the three payload types defined in :mod:`src.protocol`.  A dataset
adapter has exactly one job -- turn recorded samples into a
:class:`RealRecord`, i.e. a ``(n_samples, n_channels)`` float32 array plus
verifiable provenance.  Adding a third scientific domain requires one loader
function and one registry entry; the transport, buffers, views, GUI and
benchmark do not change.

Registered datasets
-------------------
``mitdb``  MIT-BIH Arrhythmia Database (PhysioNet), biomedical / ECG.
           ODC-By 1.0, DOI 10.13026/C2F305.
``gwosc``  GW150914 strain from the Gravitational Wave Open Science Center,
           gravitational-wave astronomy.  CC BY 4.0, DOI 10.7935/82H3-HH23.

No signal values are generated from a formula anywhere in this module.  The
only transformations are zero-centring, robust amplitude scaling, and for
the strain data a linear zero-phase band-pass -- all invertible-in-shape
preprocessing applied identically to every sample of a channel.  The original
physical-unit samples stay available on :attr:`RealRecord.signals_physical`.
"""

from __future__ import annotations

import gzip
import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np

_ROOT = Path(__file__).resolve().parents[1]

# One frame consumes this many recorded samples and emits four datagrams.
FRAME_SAMPLES = 1024


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with Path(path).open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


@dataclass
class RealRecord:
    """A verified, decoded recording from any registered scientific domain."""

    dataset_key: str
    dataset: Dict[str, Any]
    record: str
    fs: int
    n_samples: int
    sig_names: Tuple[str, ...]
    signals_physical: np.ndarray            # physical units, as published
    signals: np.ndarray                     # (n_samples, n_channels), display-scaled
    spectrum_reference: float
    physical_units: str
    preprocessing: str
    checksums: Dict[str, str] = field(default_factory=dict)
    gain: Optional[float] = None            # WFDB-specific, kept for compatibility
    adc_zero: Optional[float] = None

    @property
    def n_channels(self) -> int:
        return int(self.signals.shape[1])

    @property
    def signals_mv(self) -> np.ndarray:
        """Backwards-compatible alias for the physical-unit array."""
        return self.signals_physical

    @property
    def provenance(self) -> Dict[str, Any]:
        return {
            **self.dataset,
            "record": self.record,
            "sampling_rate_hz": self.fs,
            "samples_per_channel": self.n_samples,
            "channels": list(self.sig_names),
            "duration_s": round(self.n_samples / self.fs, 3),
            "physical_units": self.physical_units,
            "preprocessing": self.preprocessing,
            "sha256": self.checksums,
        }


@dataclass
class DatasetSpec:
    """Everything the framework needs to fetch, verify, describe and load."""

    key: str
    label: str
    domain: str
    name: str
    version: str
    publisher: str
    license: str
    doi: str
    url: str
    citation: str
    acknowledgement: str
    physical_units: str
    preprocessing: str
    download_base: str
    files: Dict[str, str]                    # filename -> expected SHA-256
    required_files: Tuple[str, ...]
    data_dir: Path
    loader: Callable[[Path], RealRecord]

    @property
    def metadata(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "label": self.label,
            "domain": self.domain,
            "name": self.name,
            "version": self.version,
            "publisher": self.publisher,
            "license": self.license,
            "doi": self.doi,
            "url": self.url,
            "citation": self.citation,
            "acknowledgement": self.acknowledgement,
            "physical_units": self.physical_units,
            "preprocessing": self.preprocessing,
        }

    @property
    def public(self) -> Dict[str, Any]:
        """Short description exposed through the GUI/CLI."""
        return {
            "key": self.key,
            "label": self.label,
            "domain": self.domain,
            "doi": self.doi,
            "license": self.license,
        }


REGISTRY: Dict[str, DatasetSpec] = {}


def register(spec: DatasetSpec) -> DatasetSpec:
    REGISTRY[spec.key] = spec
    return spec


def get_spec(key: str) -> DatasetSpec:
    try:
        return REGISTRY[key]
    except KeyError as exc:
        raise ValueError(
            f"unknown dataset {key!r}; available: {', '.join(sorted(REGISTRY))}"
        ) from exc


def available_datasets() -> List[Dict[str, Any]]:
    return [spec.public for spec in REGISTRY.values()]


def verify_files(data_dir: Path, key: str = "mitdb") -> Dict[str, str]:
    """Hash the required files, refusing missing or altered upstream inputs."""
    spec = get_spec(key)
    data_dir = Path(data_dir)
    found: Dict[str, str] = {}
    for name in spec.required_files:
        path = data_dir / name
        if not path.is_file():
            raise FileNotFoundError(
                f"missing {path}; run `python tools/fetch_dataset.py` first"
            )
        digest = sha256_file(path)
        expected = spec.files.get(name)
        if expected and digest != expected:
            raise ValueError(f"checksum mismatch for {name}: {digest}")
        found[name] = digest
    return found


def load_dataset(key: str, data_dir: Optional[Path] = None) -> RealRecord:
    spec = get_spec(key)
    return spec.loader(Path(data_dir) if data_dir else spec.data_dir)


# ---------------------------------------------------------------------------
# Shared preprocessing helpers
# ---------------------------------------------------------------------------

def _robust_scale(physical: np.ndarray) -> np.ndarray:
    """Per-channel zero-centring + 99th-percentile amplitude scaling.

    Sensor channels routinely differ by an order of magnitude (ECG leads,
    independent interferometers).  A single global scale would flatten one
    channel and saturate the other, so each channel gets its own reference.
    This is display normalisation only; it does not change the signal shape.
    """
    centred = physical - np.median(physical, axis=0, keepdims=True)
    scales = np.percentile(np.abs(centred), 99, axis=0, keepdims=True)
    # The floor must be relative to the recorded units, not an absolute
    # constant: ECG is ~1e0 mV while interferometer strain is ~1e-21.  An
    # absolute 1e-6 floor silently flattens the latter to zero.
    fallback = float(np.max(np.abs(centred)))
    scales = np.where(scales > 0, scales, fallback if fallback > 0 else 1.0)
    scales = np.maximum(scales, np.finfo(np.float32).tiny)
    return np.clip(centred / scales, -1.5, 1.5).astype(np.float32)


def _spectral_reference(signals: np.ndarray, n_windows: int = 64) -> float:
    """Fixed colour reference from many real windows of every channel."""
    window = np.hanning(512).astype(np.float32)
    offsets = np.linspace(0, signals.shape[0] - 512, n_windows).astype(int)
    peaks = []
    for channel in range(signals.shape[1]):
        for o in offsets:
            segment = signals[o : o + 512, channel].copy()
            segment -= segment.mean()  # remove DC before the FFT
            magnitude = np.abs(np.fft.rfft(segment * window)) / 512.0 * 2.0
            peaks.append(float(magnitude[1:].max()))
    return max(float(np.percentile(peaks, 90)), float(np.finfo(np.float32).tiny))


def _zero_phase_bandpass(series: np.ndarray, fs: int,
                         low_hz: float, high_hz: float) -> np.ndarray:
    """Linear, zero-phase band-pass via an FFT mask (numpy only, no scipy).

    This is the standard step used to reveal a transient in detector strain.
    It adds no signal content: every output sample is a linear combination of
    recorded samples only.
    """
    spectrum = np.fft.rfft(series)
    freqs = np.fft.rfftfreq(len(series), 1.0 / fs)
    spectrum[(freqs < low_hz) | (freqs > high_hz)] = 0.0
    return np.fft.irfft(spectrum, n=len(series))


# ---------------------------------------------------------------------------
# Adapter 1: biomedical ECG (PhysioNet MIT-BIH, WFDB 212 format)
# ---------------------------------------------------------------------------

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


def _load_mitdb(data_dir: Path) -> RealRecord:
    spec = get_spec("mitdb")
    record = "100"
    checksums = verify_files(data_dir, "mitdb")

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

    flat = _decode_212((data_dir / f"{record}.dat").read_bytes())
    needed = n_samples * n_channels
    if len(flat) < needed:
        raise ValueError(f"truncated signal file: {len(flat)} < {needed} samples")
    interleaved = flat[:needed].reshape(n_samples, n_channels)
    physical = (interleaved.astype(np.float32) - adc_zero) / gain  # mV
    signals = _robust_scale(physical)

    return RealRecord(
        dataset_key="mitdb",
        dataset=spec.metadata,
        record=record,
        fs=fs,
        n_samples=n_samples,
        sig_names=sig_names,
        signals_physical=physical,
        signals=signals,
        spectrum_reference=_spectral_reference(signals),
        physical_units="mV (ECG lead potential)",
        preprocessing="per-lead median removal and 99th-percentile amplitude scaling",
        checksums=checksums,
        gain=gain,
        adc_zero=adc_zero,
    )


# ---------------------------------------------------------------------------
# Adapter 2: gravitational-wave strain (LIGO GWOSC, gzipped ASCII)
# ---------------------------------------------------------------------------

_GWOSC_H1 = "H-H1_GWOSC_4KHZ_R1-1126259447-32.txt.gz"
_GWOSC_L1 = "L-L1_GWOSC_4KHZ_R1-1126259447-32.txt.gz"


def _read_gwosc_txt(path: Path) -> np.ndarray:
    """Read a GWOSC strain column, skipping the '#' metadata header."""
    with gzip.open(path, "rt", encoding="ascii") as fh:
        rows = [line for line in fh if line.strip() and not line.startswith("#")]
    values = np.asarray(rows, dtype=np.float64)
    if values.ndim != 1 or not np.all(np.isfinite(values)):
        raise ValueError(f"unexpected strain column in {path.name}")
    return values


def _load_gwosc(data_dir: Path) -> RealRecord:
    spec = get_spec("gwosc")
    checksums = verify_files(data_dir, "gwosc")
    h1 = _read_gwosc_txt(data_dir / _GWOSC_H1)
    l1 = _read_gwosc_txt(data_dir / _GWOSC_L1)
    if len(h1) != len(l1):
        raise ValueError("H1/L1 strain columns have different lengths")

    fs = 4096
    n_samples = len(h1)
    physical = np.column_stack((h1, l1)).astype(np.float32)  # dimensionless strain

    # Raw strain is dominated by low-frequency seismic noise and by very
    # strong narrow calibration/violin lines.  A 35-350 Hz zero-phase
    # band-pass is the standard linear preparation for transient searches;
    # it adds no synthetic samples.  Bins above the passband are therefore
    # exactly zero -- an honest domain signature, in deliberate contrast to
    # the ECG's continuous low-frequency spectrum.  A wider band lets one
    # calibration line dominate the whole view, which is why it is not used.
    # Scale each interferometer independently afterwards.
    filtered = np.column_stack([
        _zero_phase_bandpass(physical[:, ch].astype(np.float64), fs, 35.0, 350.0)
        for ch in range(physical.shape[1])
    ]).astype(np.float32)
    signals = _robust_scale(filtered)

    return RealRecord(
        dataset_key="gwosc",
        dataset=spec.metadata,
        record="GW150914_R1",
        fs=fs,
        n_samples=n_samples,
        sig_names=("H1", "L1"),
        signals_physical=physical,
        signals=signals,
        spectrum_reference=_spectral_reference(signals),
        physical_units="dimensionless strain h(t)",
        preprocessing=(
            "per-detector 35-350 Hz zero-phase FFT band-pass, then median "
            "removal and 99th-percentile amplitude scaling"
        ),
        checksums=checksums,
    )


# ---------------------------------------------------------------------------
# Registry entries
# ---------------------------------------------------------------------------

register(DatasetSpec(
    key="mitdb",
    label="MIT-BIH ECG — real open data",
    domain="Biomedical / electrophysiology",
    name="MIT-BIH Arrhythmia Database",
    version="1.0.0",
    publisher="PhysioNet",
    license="Open Data Commons Attribution License v1.0 (ODC-By 1.0)",
    doi="10.13026/C2F305",
    url="https://physionet.org/files/mitdb/1.0.0/",
    citation=(
        "Moody GB, Mark RG. The impact of the MIT-BIH Arrhythmia Database. "
        "IEEE Engineering in Medicine and Biology Magazine. 2001;20(3):45-50."
    ),
    acknowledgement=(
        "Cite Moody & Mark (2001) and DOI 10.13026/C2F305 in any figure "
        "caption using this record."
    ),
    physical_units="mV (ECG lead potential)",
    preprocessing="per-lead median removal and 99th-percentile amplitude scaling",
    download_base="https://physionet.org/files/mitdb/1.0.0/",
    files={
        "100.hea": "60ebc904c7bf3e04d142638d3fd5c903e8dc9f10f1ea3264e07926aa089ee75e",
        "100.dat": "b2ea3c250e56e48f4b7b90697832b8ecd1afa1e0bb31f2dcfea4ed6e1075a639",
        "100.atr": "8d8a5349fb16638ebbf649f1779d12e96d91b736b2aafe59db43719ae583d471",
    },
    required_files=("100.hea", "100.dat"),
    data_dir=_ROOT / "data" / "mitdb",
    loader=_load_mitdb,
))

register(DatasetSpec(
    key="gwosc",
    label="LIGO GW150914 strain — real open data",
    domain="Gravitational-wave astronomy / astrophysics",
    name="GWTC-1 GW150914 strain (4 kHz R1, 32 s)",
    version="v3",
    publisher="Gravitational Wave Open Science Center (LIGO/Virgo/KAGRA)",
    license="Creative Commons Attribution 4.0 International (CC BY 4.0)",
    doi="10.7935/82H3-HH23",
    url="https://gwosc.org/eventapi/json/GWTC-1-confident/GW150914/v3",
    citation=(
        "Abbott R, et al. (LIGO Scientific Collaboration and Virgo "
        "Collaboration). Open data from the first and second observing runs "
        "of Advanced LIGO and Advanced Virgo. SoftwareX. 2021;13:100658. "
        "doi:10.1016/j.softx.2021.100658. Dataset DOI 10.7935/82H3-HH23."
    ),
    acknowledgement=(
        "This research has made use of data obtained from the Gravitational "
        "Wave Open Science Center (gwosc.org), a service of the LIGO "
        "Scientific Collaboration, the Virgo Collaboration and KAGRA. "
        "Cite DOI 10.7935/82H3-HH23 and Abbott et al. (2021), SoftwareX 13:100658."
    ),
    physical_units="dimensionless strain h(t)",
    preprocessing="35-350 Hz zero-phase band-pass + per-detector robust scaling",
    download_base="https://gwosc.org/eventapi/json/GWTC-1-confident/GW150914/v3/",
    files={
        _GWOSC_H1: "fefe8717306109460b6c9cff74da6beb9e80ee624824b4b09bdd2c54ce9b4dfc",
        _GWOSC_L1: "43d30a710d6ed4a8f27d13f45182f3825cbe99f87287b518618c8ff0e25e0d7c",
    },
    required_files=(_GWOSC_H1, _GWOSC_L1),
    data_dir=_ROOT / "data" / "gwosc",
    loader=_load_gwosc,
))

# Backwards-compatible module-level names used by the original benchmark.
DEFAULT_DATA_DIR = REGISTRY["mitdb"].data_dir
DATASET = REGISTRY["mitdb"].metadata
EXPECTED_SHA256 = REGISTRY["mitdb"].files


def load_record(record: str = "100", data_dir: Optional[Path] = None,
                dataset: str = "mitdb") -> RealRecord:
    """Compatibility entry point; ``load_dataset`` is the general form."""
    if dataset == "mitdb" and record != "100":
        raise ValueError("only record 100 is registered for mitdb")
    return load_dataset(dataset, data_dir)
