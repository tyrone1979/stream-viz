"""Tests for the WFDB annotation decoder and the streaming outlier consumer.

These run without the downloaded recording: the annotation tests use a small
hand-encoded file, and the detector tests use a generated pulse train whose
beats are known exactly.
"""

import struct
import unittest

import numpy as np

from src.annotations import abnormal, beats, count_by_symbol, decode
from src.anomaly import (
    DEFAULT_THRESHOLD,
    PREMATURE_RATIO,
    StreamingBeatDetector,
    evaluate_windows,
    match_beats,
)


def encode_annotation_file(entries) -> bytes:
    """Hand-encode (sample, wfdb_code) pairs into the 2-byte-word format."""
    out = bytearray()
    previous = 0
    for sample, code in entries:
        increment = sample - previous
        assert 0 <= increment < 1024
        out += struct.pack("<H", increment | (code << 10))
        previous = sample
    out += struct.pack("<H", 0)          # end of file
    return bytes(out)


def make_pulse_train(n_beats=40, fs=360, interval_s=0.8, positions=None, length=None):
    """A clean ECG-like signal: sharp R spikes on a wandering baseline."""
    if positions is None:
        step = int(round(interval_s * fs))
        positions = [i * step for i in range(n_beats)]
    positions = np.asarray(positions, dtype=np.int64)
    last = length if length is not None else int(positions[-1] + 2 * fs)
    signal = np.zeros(last, dtype=np.float64)
    t = np.arange(last) / fs
    signal += 0.02 * np.sin(2 * np.pi * 1.2 * t)
    spike = np.array([0.2, 0.5, 1.1, 1.4, 1.0, 0.5, 0.2, 0.05, 0.0])
    for p in positions:
        lo = max(0, p - 4)
        hi = min(last, p + 5)
        signal[lo:hi] += spike[: hi - lo]
    signal += np.random.default_rng(0).normal(0, 0.005, last)
    return signal.astype(np.float32), positions


class AnnotationTests(unittest.TestCase):
    def test_round_trip(self):
        decoded = decode(encode_annotation_file([(100, 1), (280, 1), (450, 5), (820, 1)]))
        self.assertEqual([a.sample for a in decoded], [100, 280, 450, 820])
        self.assertEqual([a.symbol for a in decoded], ["N", "N", "V", "N"])

    def test_classification(self):
        decoded = decode(encode_annotation_file([(0, 1), (100, 5), (200, 8), (300, 14)]))
        self.assertEqual(len(beats(decoded)), 3)      # N, V, A -- noise is not a beat
        self.assertEqual(len(abnormal(decoded)), 2)   # V and A
        self.assertEqual(count_by_symbol(decoded)["~"], 1)

    def test_skip_advances_time_without_annotating(self):
        payload = bytearray()
        payload += struct.pack("<H", 200 | (1 << 10))       # beat at 200
        payload += struct.pack("<H", 0 | (59 << 10))        # SKIP
        payload += struct.pack("<HH", 5000 & 0xFFFF, 5000 >> 16)
        payload += struct.pack("<H", 7 | (1 << 10))         # beat at 5207
        payload += struct.pack("<H", 0)                     # end of file
        self.assertEqual([a.sample for a in decode(bytes(payload))], [200, 5207])

    def test_truncated_file_does_not_raise(self):
        payload = struct.pack("<H", 200 | (1 << 10)) + b"\x01\x02\x03"
        self.assertEqual(len(decode(payload)), 1)


class DetectorTests(unittest.TestCase):
    def test_finds_every_pulse(self):
        fs = 360
        signal, positions = make_pulse_train(n_beats=40, fs=fs, interval_s=0.8)
        detector = StreamingBeatDetector(fs=fs)
        found = []
        for start in range(0, len(signal) - 1024 + 1, 1024):
            verdict = detector.observe(signal[start:start + 1024])
            found.extend(start + b.offset for b in verdict.beats)
        result = match_beats(found, positions.tolist(), int(0.10 * fs))
        self.assertGreater(result["recall"], 0.95)
        self.assertGreater(result["precision"], 0.95)

    def test_premature_beat_is_flagged(self):
        """One beat pulled 40 % early must raise the flag, and only near it."""
        fs = 360
        normal = int(0.8 * fs)
        positions = []
        t = 0
        for i in range(60):
            positions.append(t)
            t += normal if i != 30 else int(0.48 * fs)
        signal, positions = make_pulse_train(positions=positions, fs=fs, length=t + fs)
        detector = StreamingBeatDetector(fs=fs)
        flagged = []
        for start in range(0, len(signal) - 1024 + 1, 1024):
            if detector.observe(signal[start:start + 1024]).flagged:
                flagged.append(start)
        event = int(positions[30])
        self.assertTrue(any(s <= event < s + 1024 for s in flagged))
        self.assertLessEqual(len(flagged), 30)      # not a blanket alarm

    def test_regular_tachycardia_is_not_flagged(self):
        """A fast but regular rhythm is not an anomaly: RR is compared to itself."""
        fs = 360
        signal, _ = make_pulse_train(n_beats=40, fs=fs, interval_s=0.40)
        detector = StreamingBeatDetector(fs=fs)
        verdicts = [detector.observe(signal[s:s + 1024])
                    for s in range(0, len(signal) - 1024 + 1, 1024)]
        self.assertLessEqual(sum(v.flagged for v in verdicts), 2)

    def test_stride_is_used_for_cross_window_rr(self):
        """A skipped-frame stream must count the unseen samples in the RR.

        Two windows of 200 samples, 400 samples apart on the source timebase;
        the beats sit 300 samples apart in real time.  Without the stride the
        detector would report a negative interval.
        """
        fs = 100
        window = np.zeros(200, dtype=np.float32)
        window[148:153] = [0.2, 1.4, 1.0, 0.4, 0.1]
        first = window.copy()
        second = np.zeros(200, dtype=np.float32)
        second[48:53] = [0.2, 1.4, 1.0, 0.4, 0.1]

        detector = StreamingBeatDetector(fs=fs)
        detector.observe(first, stride=400)
        detector.observe(second, stride=400)

        self.assertTrue(detector._rr, "cross-window RR was not measured")
        self.assertAlmostEqual(detector._rr[0], 3000.0, delta=150.0)

    def test_threshold_is_the_prematurity_rule(self):
        self.assertAlmostEqual(1.0 - PREMATURE_RATIO, DEFAULT_THRESHOLD, places=9)

    def test_confusion_sums_to_window_count(self):
        fs = 360
        signal, positions = make_pulse_train(n_beats=40, fs=fs, interval_s=0.8)
        result = evaluate_windows(
            signal, fs, window_samples=1024,
            abnormal_samples=np.asarray([positions[10]]),
        )
        c = result["confusion"]
        self.assertEqual(c["tp"] + c["fp"] + c["fn"] + c["tn"], len(result["windows"]))
        self.assertGreaterEqual(result["pr_auc"], 0.0)
        self.assertLessEqual(result["pr_auc"], 1.0)

    def test_short_window_is_ignored(self):
        verdict = StreamingBeatDetector(fs=360).observe(np.zeros(8))
        self.assertFalse(verdict.flagged)
        self.assertEqual(verdict.n_beats, 0)


if __name__ == "__main__":
    unittest.main()
