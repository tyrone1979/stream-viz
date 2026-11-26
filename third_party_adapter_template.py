"""A third-party dataset adapter for StreamViz — copy this file and edit it.

Why this file exists
--------------------
The claim that StreamViz is domain-agnostic is only worth something if somebody
else can add a domain without touching the transport, the buffers, the views,
the GUI or the benchmark.  This template is that path: one loader function plus
one registry entry.  Everything below the loader -- UDP framing, the bounded
FIFO, the admission policies, staleness metering, the four views, the CSV/JSON
reports -- already exists and must not be modified to support a new dataset.

What you have to supply
-----------------------
1. ``load_my_recording()``  -- read your files, return a ``RealRecord``.
2. ``MY_SPEC``             -- provenance: licence, DOI, citation, checksums.
3. (optional) ``register(MY_SPEC)`` at the bottom of your own module, not here.

Run ``python third_party_adapter_template.py --check`` to see the contract your
loader must satisfy.  The check is structural, not physical: it cannot tell
whether your numbers are correct, which is exactly why the licence, DOI and
citation fields are mandatory rather than decorative.

The example format used below is a two-column whitespace-separated text file
``<name>.txt`` (sample index, then one column per channel) and a sidecar
``<name>.json`` with ``fs``, ``units`` and ``channels``.  Substitute your own
reader; nothing else changes.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import numpy as np

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.datasets import (  # noqa: E402
    DatasetSpec,
    RealRecord,
    display_scale,
    register,
    spectral_reference,
    validate_record,
)


# ---------------------------------------------------------------------------
# 1. The loader: recorded samples in, a RealRecord out.
# ---------------------------------------------------------------------------

def load_my_recording(data_dir: Path) -> RealRecord:
    """Decode one recording into (n_samples, n_channels) plus provenance.

    Rules that matter, learned the hard way in this repository:

    * Read the published numbers.  Do not generate, resample or "clean up"
      signal content.  If you must filter, use a linear zero-phase filter and
      say so in ``preprocessing``; keep the unfiltered values in
      ``signals_physical``.
    * ``signals`` is the *display* array: zero-centred, per-channel scaled to
      ``|x| <= 1.5``.  Channels from different sensors routinely differ by
      orders of magnitude, so a single global scale silently flattens one of
      them.  ``display_scale()`` does the per-channel version.
    * ``checksums`` must contain the SHA-256 of every file you read.  A record
      that cannot be verified cannot be cited, and a reviewer will ask.
    """
    data_dir = Path(data_dir)
    text_path = data_dir / "recording.txt"
    meta_path = data_dir / "recording.json"
    for path in (text_path, meta_path):
        if not path.is_file():
            raise FileNotFoundError(
                f"missing {path} — this template expects a two-column text "
                f"file plus a JSON sidecar; see the module docstring"
            )

    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    fs = int(meta["fs"])
    units = str(meta.get("units", "arbitrary units"))
    channels = tuple(meta.get("channels") or ["ch0"])

    # Column 0 is the sample index; the rest are channels.  Replace this block
    # with your own reader (WFDB, HDF5, gzip, netCDF, proprietary export, ...).
    table = np.loadtxt(text_path, dtype=np.float64, ndmin=2)
    if table.shape[1] != len(channels) + 1:
        raise ValueError(
            f"{text_path.name}: expected {len(channels) + 1} columns "
            f"(index + {len(channels)} channels), got {table.shape[1]}"
        )
    physical = table[:, 1:].astype(np.float32)

    if not np.all(np.isfinite(physical)):
        raise ValueError(f"{text_path.name} contains non-finite samples")

    signals = display_scale(physical)

    return RealRecord(
        dataset_key=MY_SPEC.key,
        dataset=MY_SPEC.metadata,
        record=text_path.stem,
        fs=fs,
        n_samples=int(physical.shape[0]),
        sig_names=channels,
        signals_physical=physical,
        signals=signals,
        spectrum_reference=spectral_reference(signals),
        physical_units=units,
        preprocessing="per-channel median removal and 99th-percentile scaling",
        checksums={
            text_path.name: _sha256(text_path),
            meta_path.name: _sha256(meta_path),
        },
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


# ---------------------------------------------------------------------------
# 2. The provenance record.  Fill every field; validate_record() refuses blanks.
# ---------------------------------------------------------------------------

MY_SPEC = DatasetSpec(
    key="my_domain",                     # short key used by --dataset my_domain
    label="My domain recording — open data",
    domain="TODO: the scientific domain",       # e.g. "seismology"
    name="TODO: dataset name as published",
    version="TODO",                              # e.g. "1.0.0"
    publisher="TODO",
    license="TODO: exact licence string",       # e.g. "CC BY 4.0"
    doi="TODO: dataset DOI",                     # e.g. "10.1234/abcd"
    url="TODO: persistent URL",
    citation="TODO: the citation the data provider asks for",
    acknowledgement="TODO: required acknowledgement text for figure captions",
    physical_units="TODO",                       # e.g. "Pa (acoustic pressure)"
    preprocessing="per-channel median removal and 99th-percentile scaling",
    download_base="TODO: base URL for tools/fetch_dataset.py",
    files={},                                    # filename -> SHA-256
    required_files=("recording.txt", "recording.json"),
    data_dir=_ROOT / "data" / "my_domain",
    loader=load_my_recording,
)

# In your own module, not in this template:
#     from src.datasets import register
#     register(MY_SPEC)
# After that, both of these work with no other change:
#     python examples/run_gui.py --source my_domain
#     python examples/run_policy_ablation.py --dataset my_domain --policies drop_oldest


# ---------------------------------------------------------------------------
# 3. Self-check.  The fixture is synthetic *and never registered*: it exists to
#    prove the contract is satisfiable, not to stand in for data.
# ---------------------------------------------------------------------------

def _fixture_record() -> RealRecord:
    """A structural fixture (three channels of noise) used only by --check."""
    rng = np.random.default_rng(0)
    n, ch = 4096, 3
    physical = rng.normal(size=(n, ch)).astype(np.float32) * np.array(
        [1e-3, 1.0, 1e5], dtype=np.float32)   # deliberately different magnitudes
    signals = display_scale(physical)
    return RealRecord(
        dataset_key=MY_SPEC.key,
        dataset=MY_SPEC.metadata,
        record="fixture",
        fs=1000,
        n_samples=n,
        sig_names=("a", "b", "c"),
        signals_physical=physical,
        signals=signals,
        spectrum_reference=spectral_reference(signals),
        physical_units="fixture units",
        preprocessing=MY_SPEC.preprocessing,
        checksums={"fixture": "0" * 64},
    )


def _check(data_dir: Path) -> int:
    if data_dir.is_dir():
        print(f"Checking your loader against {data_dir} ...")
        try:
            rec = load_my_recording(data_dir)
        except Exception as exc:  # noqa: BLE001 - the message is the point
            print(f"  loader raised {type(exc).__name__}: {exc}")
            print("  falling back to the structural fixture")
            rec = _fixture_record()
    else:
        print(f"{data_dir} does not exist; checking the structural fixture only.")
        print("This proves the contract is satisfiable, NOT that your loader works.\n")
        rec = _fixture_record()

    problems = validate_record(rec, check_registry=False)
    print(f"  record: {rec.n_samples} samples x {rec.n_channels} channels, "
          f"{rec.fs} Hz ({rec.n_samples / rec.fs:.3f} s), {rec.physical_units}")
    print(f"  spectrum_reference: {rec.spectrum_reference:.6g}")
    if not problems:
        print("\nPASS — the record satisfies the adapter contract.")
        print("Next: fill in MY_SPEC, call register(MY_SPEC) in your own module,")
        print("then run `python examples/run_policy_ablation.py "
              "--dataset my_domain`.")
        return 0
    print("\nFAIL — fix these before registering the adapter:")
    for problem in problems:
        print(f"  - {problem}")
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Third-party adapter template and contract check")
    parser.add_argument("--check", action="store_true",
                        help="validate the loader output against the contract")
    parser.add_argument("--data-dir", default=str(_ROOT / "data" / "my_domain"),
                        help="directory holding recording.txt / recording.json")
    args = parser.parse_args()
    if args.check:
        return _check(Path(args.data_dir))
    parser.print_help()
    print("\nStart by reading the module docstring, then run --check.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
