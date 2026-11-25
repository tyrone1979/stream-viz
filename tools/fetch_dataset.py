"""Download and verify every registered open dataset.

    python tools/fetch_dataset.py                 # all registered datasets
    python tools/fetch_dataset.py --dataset gwosc

Each dataset declares its own upstream URL, expected SHA-256 and citation in
``src/datasets.py``.  Signal files are deliberately NOT committed to this
repository: their licenses permit redistribution, but requiring an explicit
download keeps upstream provenance and attribution unambiguous.
"""

from __future__ import annotations

import argparse
import sys
import urllib.request
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.datasets import REGISTRY, sha256_file, verify_files  # noqa: E402


def fetch(key: str) -> int:
    spec = REGISTRY[key]
    spec.data_dir.mkdir(parents=True, exist_ok=True)
    print(f"Dataset : {spec.name} ({spec.domain})")
    print(f"License : {spec.license}")
    print(f"DOI     : {spec.doi}")
    print(f"Source  : {spec.url}")
    print(f"Target  : {spec.data_dir}\n")

    for name, expected in spec.files.items():
        path = spec.data_dir / name
        if path.is_file() and sha256_file(path) == expected:
            print(f"[ok]      {name} already present and verified")
            continue
        url = spec.download_base + name
        print(f"[fetch]   {url}")
        with urllib.request.urlopen(url, timeout=120) as response:
            payload = response.read()
        path.write_bytes(payload)
        digest = sha256_file(path)
        status = "ok" if digest == expected else "MISMATCH"
        print(f"[{status}] {name}: {len(payload)} bytes, sha256={digest}")
        if digest != expected:
            return 1

    print("\nVerified files:")
    for name, digest in verify_files(spec.data_dir, key).items():
        print(f"  {name}  {digest}")
    print("\nCite this dataset in any publication using these measurements:")
    print(f"  {spec.citation}")
    print(f"  https://doi.org/{spec.doi}")
    if spec.acknowledgement:
        print(f"  {spec.acknowledgement}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch verified open datasets")
    parser.add_argument("--dataset", default="all",
                        choices=("all", *sorted(REGISTRY)),
                        help="which registered dataset to fetch")
    args = parser.parse_args()
    keys = sorted(REGISTRY) if args.dataset == "all" else [args.dataset]
    for i, key in enumerate(keys):
        if i:
            print("\n" + "=" * 72 + "\n")
        rc = fetch(key)
        if rc:
            return rc
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
