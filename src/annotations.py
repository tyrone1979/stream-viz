"""Decoder for WFDB annotation files (MIT-BIH ``.atr``), numpy only.

The point of this module is to make one claim checkable: that a downstream
consumer of the display stream can flag the *abnormal* beats in a recording,
and that the delay before the flag appears is bounded by the frame staleness
the buffer policy produces.  Checking that claim needs ground truth, and the
ground truth for MIT-BIH is in ``100.atr`` -- a binary format that would
otherwise force a dependency on the ``wfdb`` package for one file.

Format (WFDB Programmer's Guide, annotation files)
--------------------------------------------------
An annotation is one 16-bit little-endian word:

* bits 0-9   ``A`` -- samples since the previous annotation;
* bits 10-15 ``I`` -- the annotation type code.

``A == 0 and I == 0`` terminates the file.  Four codes are structural rather
than annotations and consume trailing words:

* ``I == 59`` (SKIP)      -- the next four bytes are a 32-bit interval in
  samples, little-endian, packed two bytes per word;
* ``I == 60/61/62``       -- NUM / SUB / CHN, followed by one word;
* ``I == 63`` (AUX)       -- ``A`` is a byte count for a trailing string.

MIT-BIH record 100 needs no SKIP: its mean beat interval is ~285 samples at
360 Hz, well under the 1023-sample range of a single increment.  The decoder
still implements SKIP, because a third-party adapter may hand us a recording
with long annotated gaps.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, List, Optional, Tuple

import numpy as np

# WFDB annotation type codes -> MIT-BIH symbol.  Codes that describe a beat
# are listed in _BEAT_SYMBOLS; the rest annotate signal quality, rhythm
# changes or waveform onsets and are not beats.
SYMBOLS = {
    0: "NOTQRS", 1: "N", 2: "L", 3: "R", 4: "a", 5: "V", 6: "F", 7: "J",
    8: "A", 9: "S", 10: "E", 11: "j", 12: "/", 13: "Q", 14: "~",
    16: "|", 18: '"', 19: "+", 20: "~", 21: "s", 22: "T", 23: "*",
    24: "D", 25: "@", 26: "B", 27: "r", 28: "N", 29: "u", 30: "?",
    31: "!", 32: "]", 33: "[", 34: "e", 35: "n", 36: "(", 37: "x",
    38: "f", 39: "]", 40: "[", 41: "r",
}

# Beat annotations, per the MIT-BIH label set.
BEAT_SYMBOLS = frozenset("NLRaVFJASEj/Q")

# "Abnormal" in the strict, citable sense used here: every beat class the
# MIT-BIH documentation distinguishes from normal sinus rhythm.  "Q" is
# unclassifiable and is counted as abnormal -- excluding it would silently
# inflate specificity.  "N" and "NOTQRS" are the only non-abnormal codes.
NORMAL_SYMBOLS = frozenset({"N"})

_ANNOTATION_LABELS = {
    "N": "normal",
    "L": "left bundle branch block",
    "R": "right bundle branch block",
    "a": "aberrant atrial premature",
    "V": "premature ventricular contraction",
    "F": "fusion of ventricular and normal",
    "J": "nodal (junctional) premature",
    "A": "atrial premature",
    "S": "supraventricular premature",
    "E": "ventricular escape",
    "j": "nodal (junctional) escape",
    "/": "paced",
    "Q": "unclassifiable",
}


@dataclass(frozen=True)
class Annotation:
    """One annotation: sample index (from the start of the record) + symbol."""

    sample: int
    code: int
    symbol: str

    @property
    def is_beat(self) -> bool:
        return self.symbol in BEAT_SYMBOLS

    @property
    def is_abnormal(self) -> bool:
        return self.is_beat and self.symbol not in NORMAL_SYMBOLS

    @property
    def label(self) -> str:
        return _ANNOTATION_LABELS.get(self.symbol, self.symbol)


def decode(blob: bytes) -> List[Annotation]:
    """Decode an annotation file held in memory."""
    usable = len(blob) - (len(blob) % 2)
    words = np.frombuffer(blob[:usable], dtype="<u2")
    n = len(words)
    t = 0
    i = 0
    out: List[Annotation] = []
    while i < n:
        word = int(words[i])
        i += 1
        increment = word & 0x03FF
        code = (word >> 10) & 0x3F

        if code == 0:
            if increment == 0:
                break                              # end of file
            t += increment                         # time-only advance
            continue
        if code == 59:                             # SKIP
            if i + 2 > n:
                break
            lo, hi = int(words[i]), int(words[i + 1])
            i += 2
            interval = (lo & 0xFF) | (((lo >> 8) & 0xFF) << 8) \
                | ((hi & 0xFF) << 16) | (((hi >> 8) & 0xFF) << 24)
            t += interval
            continue
        if code in (60, 61, 62):                   # NUM / SUB / CHN
            i += 1
            continue
        if code == 63:                             # AUX
            i += (increment + 1) // 2
            continue

        t += increment
        symbol = SYMBOLS.get(code)
        if symbol is None:
            continue
        out.append(Annotation(sample=t, code=code, symbol=symbol))
    return out


def read(path: Path) -> List[Annotation]:
    return decode(Path(path).read_bytes())


def beats(annotations: List[Annotation]) -> List[Annotation]:
    return [a for a in annotations if a.is_beat]


def abnormal(annotations: List[Annotation]) -> List[Annotation]:
    return [a for a in annotations if a.is_abnormal]


def count_by_symbol(annotations: List[Annotation]) -> dict:
    counts: dict = {}
    for a in annotations:
        counts[a.symbol] = counts.get(a.symbol, 0) + 1
    return dict(sorted(counts.items(), key=lambda kv: -kv[1]))


def abnormal_positions(annotations: List[Annotation]) -> np.ndarray:
    """Sample indices of abnormal beats, sorted, as an int64 array."""
    return np.asarray([a.sample for a in annotations if a.is_abnormal], dtype=np.int64)
