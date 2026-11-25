"""Backward-compatible entry point for the synthetic UDP generator.

The implementation now lives in :mod:`src.synthetic` so the interactive GUI
can start a stream without depending on the ``examples`` package.  Existing
scripts and benchmarks keep importing from here.
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.synthetic import StreamSpec, average_packet_bytes, send_stream  # noqa: E402,F401

__all__ = ["StreamSpec", "average_packet_bytes", "send_stream"]
