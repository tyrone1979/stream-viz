"""Tests for the third-party adapter contract (``validate_record``).

The contract is the thing a new domain has to satisfy, so it needs its own
tests: a validator that accepts everything would make the "one loader and one
registry entry" claim unfalsifiable.
"""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np

import third_party_adapter_template as template
from src.datasets import REGISTRY, RealRecord, validate_record


def _good_record(**overrides) -> RealRecord:
    n, ch = 512, 2
    physical = np.random.default_rng(0).normal(size=(n, ch)).astype(np.float32)
    physical[:, 1] *= 1e-6                      # different sensor magnitudes
    signals = (physical / np.abs(physical).max(axis=0, keepdims=True)).astype(
        np.float32)
    fields = dict(
        dataset_key="mitdb",
        dataset=REGISTRY["mitdb"].metadata,
        record="100",
        fs=360,
        n_samples=n,
        sig_names=("a", "b"),
        signals_physical=physical,
        signals=signals,
        spectrum_reference=0.5,
        physical_units="mV",
        preprocessing="median removal + scaling",
        checksums={"x": "0" * 64},
    )
    fields.update(overrides)
    return RealRecord(**fields)


class ValidateRecordTests(unittest.TestCase):
    def test_a_well_formed_record_passes(self):
        self.assertEqual(validate_record(_good_record()), [])

    def test_one_dimensional_signals_are_rejected(self):
        rec = _good_record()
        rec.signals = rec.signals[:, 0]
        problems = validate_record(rec)
        self.assertTrue(any("2-D" in p for p in problems), problems)

    def test_non_finite_samples_are_rejected(self):
        rec = _good_record()
        rec.signals = rec.signals.copy()
        rec.signals[3, 0] = np.nan
        problems = validate_record(rec)
        self.assertTrue(any("non-finite" in p for p in problems), problems)

    def test_unscaled_signals_are_rejected(self):
        rec = _good_record()
        rec.signals = rec.signals * 100.0
        problems = validate_record(rec)
        self.assertTrue(any("display-scaled" in p for p in problems), problems)

    def test_channel_name_mismatch_is_rejected(self):
        rec = _good_record(sig_names=("a",))
        problems = validate_record(rec)
        self.assertTrue(any("channel names" in p for p in problems), problems)

    def test_missing_provenance_is_rejected(self):
        meta = dict(REGISTRY["mitdb"].metadata)
        meta["doi"] = ""
        meta["license"] = "  "
        problems = validate_record(_good_record(dataset=meta))
        self.assertTrue(any("metadata missing" in p for p in problems), problems)
        joined = " ".join(problems)
        self.assertIn("doi", joined)
        self.assertIn("license", joined)

    def test_missing_checksums_are_rejected(self):
        problems = validate_record(_good_record(checksums={}))
        self.assertTrue(any("checksums" in p for p in problems), problems)

    def test_unregistered_key_is_rejected_only_in_registry_mode(self):
        rec = _good_record(dataset_key="not_a_dataset")
        self.assertTrue(any("not registered" in p
                            for p in validate_record(rec)))
        self.assertEqual(validate_record(rec, check_registry=False), [])

    def test_bad_sampling_rate_and_empty_record_are_rejected(self):
        problems = validate_record(_good_record(fs=0, n_samples=0))
        joined = " ".join(problems)
        self.assertIn("sampling rate", joined)
        self.assertIn("empty", joined)

    def test_nonpositive_spectrum_reference_is_rejected(self):
        problems = validate_record(_good_record(spectrum_reference=0.0))
        self.assertTrue(any("spectrum_reference" in p for p in problems),
                        problems)


class TemplateTests(unittest.TestCase):
    def test_fixture_satisfies_the_contract(self):
        problems = validate_record(template._fixture_record(),
                                   check_registry=False)
        self.assertEqual(problems, [])

    def test_fixture_rescales_channels_that_differ_by_orders_of_magnitude(self):
        rec = template._fixture_record()
        peaks = np.max(np.abs(rec.signals), axis=0)
        # Every channel must reach a comparable display amplitude even though
        # the physical scales span eight orders of magnitude.
        self.assertTrue(np.all(peaks > 0.9), peaks)
        self.assertLessEqual(float(peaks.max()), 1.5)

    def test_loader_reads_a_text_recording(self):
        rng = np.random.default_rng(2)
        n = 1024
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            cols = np.column_stack([np.arange(n),
                                    rng.normal(size=n) * 1e-3,
                                    rng.normal(size=n)])
            np.savetxt(data_dir / "recording.txt", cols, fmt="%.6g")
            (data_dir / "recording.json").write_text(
                json.dumps({"fs": 2000, "units": "Pa",
                            "channels": ["a", "b"]}), encoding="utf-8")
            rec = template.load_my_recording(data_dir)
        self.assertEqual(rec.n_samples, n)
        self.assertEqual(rec.fs, 2000)
        self.assertEqual(rec.sig_names, ("a", "b"))
        self.assertEqual(rec.physical_units, "Pa")
        self.assertEqual(validate_record(rec, check_registry=False), [])
        # Both files that were read must be hashed.
        self.assertEqual(set(rec.checksums), {"recording.txt", "recording.json"})

    def test_loader_rejects_a_column_count_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            np.savetxt(data_dir / "recording.txt", np.zeros((16, 2)))
            (data_dir / "recording.json").write_text(
                json.dumps({"fs": 100, "channels": ["a", "b", "c"]}),
                encoding="utf-8")
            with self.assertRaises(ValueError):
                template.load_my_recording(data_dir)

    def test_loader_reports_a_missing_file_instead_of_silently_returning_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(FileNotFoundError):
                template.load_my_recording(Path(tmp))


if __name__ == "__main__":
    unittest.main()
