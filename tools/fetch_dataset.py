"""Download the open dataset used by the real-data benchmark.

    python tools/fetch_dataset.py

Fetches record 100 of the MIT-BIH Arrhythmia Database from PhysioNet and
verifies the SHA-256 of every file against ``src/open_dataset.py``.  The
signal files are deliberately NOT committed to this repository: ODC-By 1.0
allows redistribution, but requiring an explicit download keeps the upstream
provenance and attribution unambiguous.
"""

from __future__ import annotations

import sys
import urllib.request
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.open_dataset import DATASET, DEFAULT_DATA_DIR, EXPECTED_SHA256, sha256_file, verify_files  # noqa: E402

_BASE = "https://physionet.org/files/mitdb/1.0.0/"


def main() -> int:
    DEFAULT_DATA_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Dataset : {DATASET['name']} v{DATASET['version']}")
    print(f"License : {DATASET['license']}")
    print(f"DOI     : {DATASET['doi']}")
    print(f"Source  : {DATASET['url']}")
    print(f"Target  : {DEFAULT_DATA_DIR}\n")

    for name, expected in EXPECTED_SHA256.items():
        path = DEFAULT_DATA_DIR / name
        if path.is_file() and sha256_file(path) == expected:
            print(f"[ok]      {name} already present and verified")
            continue
        url = _BASE + name
        print(f"[fetch]   {url}")
        with urllib.request.urlopen(url, timeout=90) as response:
            payload = response.read()
        path.write_bytes(payload)
        digest = sha256_file(path)
        status = "ok" if digest == expected else "MISMATCH"
        print(f"[{status}] {name}: {len(payload)} bytes, sha256={digest}")
        if digest != expected:
            return 1

    print("\nVerified files:")
    for name, digest in verify_files(DEFAULT_DATA_DIR).items():
        print(f"  {name}  {digest}")
    print("\nCite this dataset in any publication using these measurements:")
    print(f"  {DATASET['citation']}")
    print(f"  https://doi.org/{DATASET['doi']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
