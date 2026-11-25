"""Tests for the pluggable, cross-domain dataset adapter layer.

Dataset-dependent tests skip cleanly before ``tools/fetch_dataset.py`` has
been run, so CI stays green without third-party downloads.
"""

from __future__ import annotations

import socket
import tempfile
import threading
import unittest
from pathlib import Path

import numpy as np

from src.datasets import (
    REGISTRY,
    FRAME_SAMPLES,
    _robust_scale,
    _zero_phase_bandpass,
    available_datasets,
    get_spec,
    load_dataset,
    verify_files,
)
from src.open_dataset import ReplaySpec, _frame_payloads, send_record
from src.protocol import decode

_HAS_MITDB = (REGISTRY["mitdb"].data_dir / "100.dat").is_file()
_HAS_GWOSC = all(
    (REGISTRY["gwosc"].data_dir / name).is_file()
    for name in REGISTRY["gwosc"].required_files
)


class TestRegistry(unittest.TestCase):
    def test_two_distinct_scientific_domains(self) -> None:
        entries = {d["key"]: d for d in available_datasets()}
        self.assertIn("mitdb", entries)
        self.assertIn("gwosc", entries)
        self.assertNotEqual(entries["mitdb"]["domain"], entries["gwosc"]["domain"])
        self.assertNotEqual(entries["mitdb"]["doi"], entries["gwosc"]["doi"])
        self.assertIn("ODC-By", entries["mitdb"]["license"])
        self.assertIn("CC BY", entries["gwosc"]["license"])

    def test_metadata_is_complete_for_every_dataset(self) -> None:
        for key, spec in REGISTRY.items():
            with self.subTest(key=key):
                for field in ("name", "doi", "license", "citation", "url",
                              "acknowledgement", "preprocessing", "domain"):
                    self.assertTrue(spec.metadata.get(field), f"missing {field}")
                self.assertTrue(spec.required_files)
                self.assertTrue(spec.files)

    def test_unknown_dataset_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            get_spec("not-a-dataset")

    def test_robust_scale_is_unit_agnostic(self) -> None:
        """The 1e-21 strain regression: an absolute floor must not zero it out."""
        for magnitude in (1.0, 1e-3, 1e-21):
            rng = np.random.default_rng(3)
            raw = rng.normal(0, magnitude, (4096, 2)).astype(np.float32)
            scaled = _robust_scale(raw)
            with self.subTest(magnitude=magnitude):
                self.assertGreater(float(scaled.std()), 0.1)
                self.assertLess(float(np.abs(scaled).max()), 1.51)
                self.assertLess(
                    max(scaled.std(axis=0)) / min(scaled.std(axis=0)), 3.0
                )

    def test_bandpass_is_linear_and_removes_out_of_band(self) -> None:
        fs = 4096
        t = np.arange(fs) / fs
        signal = np.sin(2 * np.pi * 10 * t) + np.sin(2 * np.pi * 100 * t)
        out = _zero_phase_bandpass(signal, fs, 35.0, 350.0)
        np.testing.assert_allclose(
            out, np.sin(2 * np.pi * 100 * t), atol=1e-10
        )

    def test_verify_files_rejects_missing_download(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(FileNotFoundError):
                verify_files(Path(tmp), "gwosc")


@unittest.skipUnless(_HAS_GWOSC, "GWOSC strain files not downloaded")
class TestGravitationalWaveAdapter(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.rec = load_dataset("gwosc")

    def test_provenance_and_geometry(self) -> None:
        p = self.rec.provenance
        self.assertEqual(p["doi"], "10.7935/82H3-HH23")
        self.assertEqual(p["record"], "GW150914_R1")
        self.assertEqual(p["sampling_rate_hz"], 4096)
        self.assertEqual(p["samples_per_channel"], 131072)
        self.assertEqual(p["channels"], ["H1", "L1"])
        self.assertEqual(p["duration_s"], 32.0)
        self.assertIn("CC BY 4.0", p["license"])
        self.assertIn("band-pass", p["preprocessing"])
        self.assertEqual(len(p["sha256"]), 2)
        self.assertEqual(len(self.rec.checksums),
                         len(verify_files(REGISTRY["gwosc"].data_dir, "gwosc")))

    def test_physical_strain_is_tiny_and_display_is_scaled(self) -> None:
        # Gravitational-wave strain is O(1e-18 .. 1e-21), nine orders of
        # magnitude below ECG mV.  The adapter must scale it into view.
        self.assertLess(float(np.abs(self.rec.signals_physical).max()), 1e-17)
        self.assertGreater(float(self.rec.signals.std()), 0.1)
        self.assertGreater(float(self.rec.spectrum_reference), 1e-6)
        stds = self.rec.signals.std(axis=0)
        self.assertLess(max(stds) / min(stds), 3.0)

    def test_payloads_equal_recorded_samples(self) -> None:
        spec = ReplaySpec(dataset="gwosc")
        frame = 137
        packets = _frame_payloads(self.rec, spec, frame)
        self.assertEqual(len(packets), 4)
        n_frames = self.rec.n_samples // FRAME_SAMPLES
        start = (frame % n_frames) * FRAME_SAMPLES
        source = self.rec.signals[start:start + FRAME_SAMPLES,
                                  frame % self.rec.n_channels]
        waveform = packets[1][1]
        audio = packets[2][1]
        np.testing.assert_allclose(
            waveform, source[:256].astype(np.float32), atol=1e-6
        )
        np.testing.assert_allclose(
            audio.astype(np.float32),
            np.clip(source[:512] * 25_000, -32768, 32767).astype(np.float32),
            atol=1.0,
        )
        self.assertGreater(float(source.std()), 1e-4)

    def test_replay_over_udp_reports_correct_doi(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as receiver:
            receiver.bind(("127.0.0.1", 0))
            port = receiver.getsockname()[1]
            receiver.settimeout(0.25)
            spec = ReplaySpec(
                dataset="gwosc", port=port, duration_s=0.35,
                rate_mbps=2.0, loop=False, malformed_ratio=0.0,
            )
            stats: dict = {}
            sender = threading.Thread(
                target=lambda: stats.update(send_record(spec, record=self.rec)),
                daemon=True,
            )
            sender.start()
            seen = []
            while sender.is_alive() or len(seen) < 8:
                try:
                    data, _ = receiver.recvfrom(65535)
                except socket.timeout:
                    if not sender.is_alive():
                        break
                    continue
                pkt, _ = decode(data)
                if pkt is not None:
                    seen.append(pkt)
            sender.join(timeout=3)
        self.assertGreater(len(seen), 4)
        self.assertEqual(stats["source"]["doi"], "10.7935/82H3-HH23")


@unittest.skipUnless(_HAS_MITDB and _HAS_GWOSC, "both datasets not downloaded")
class TestCrossDomainUniformity(unittest.TestCase):
    def test_same_transport_for_both_domains(self) -> None:
        """The pipeline sees identical payload shapes regardless of domain."""
        shapes = {}
        for key in ("mitdb", "gwosc"):
            rec = load_dataset(key)
            packets = _frame_payloads(rec, ReplaySpec(dataset=key), 20)
            shapes[key] = [(p[0].name, p[1].shape, p[1].dtype.name)
                           for p in packets]
        self.assertEqual(shapes["mitdb"], shapes["gwosc"])

    def test_signals_are_not_identical_across_domains(self) -> None:
        a = load_dataset("mitdb").signals[:1024, 0]
        b = load_dataset("gwosc").signals[:1024, 0]
        self.assertAlmostEqual(float(np.corrcoef(a, b)[0, 1]), 0.0, delta=0.15)


if __name__ == "__main__":
    unittest.main()
