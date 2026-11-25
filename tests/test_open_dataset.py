"""Tests for the real open-data source (MIT-BIH / PhysioNet).

The dataset-dependent tests skip cleanly when the signal files have not been
downloaded yet, so CI stays green before ``tools/fetch_dataset.py`` is run.
"""

from __future__ import annotations

import socket
import tempfile
import threading
import unittest
from pathlib import Path

import numpy as np

from src.open_dataset import (
    DEFAULT_DATA_DIR,
    EXPECTED_SHA256,
    ReplaySpec,
    _decode_212,
    _frame_payloads,
    load_record,
    send_record,
    sha256_file,
    verify_files,
)
from src.protocol import PacketType, decode

_HAS_DATA = (DEFAULT_DATA_DIR / "100.dat").is_file()


class TestWFDBDecoder(unittest.TestCase):
    def test_known_212_bytes(self) -> None:
        # 100 and -100 as two interleaved 12-bit two's-complement samples.
        raw = bytes([0x64, 0xC0, 0xF9])
        np.testing.assert_array_equal(_decode_212(raw), np.array([100, -100], dtype=np.int16))

    def test_truncated_tail_is_dropped(self) -> None:
        self.assertEqual(len(_decode_212(b"\x01\x02\x03\x04\x05")), 2)

    def test_checksum_verification_rejects_unknown_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            (folder / "100.hea").write_text("100 2 360 10\n", encoding="ascii")
            (folder / "100.dat").write_bytes(b"wrong")
            with self.assertRaises(ValueError):
                verify_files(folder)


@unittest.skipUnless(_HAS_DATA, "MIT-BIH record 100 not downloaded")
class TestRealRecord(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.rec = load_record("100")

    def test_header_and_checksums(self) -> None:
        self.assertEqual(self.rec.fs, 360)
        self.assertEqual(self.rec.n_samples, 650000)
        self.assertEqual(self.rec.sig_names, ("MLII", "V5"))
        self.assertEqual(self.rec.n_channels, 2)
        self.assertEqual(
            sha256_file(DEFAULT_DATA_DIR / "100.dat"), EXPECTED_SHA256["100.dat"]
        )

    def test_payloads_come_from_recorded_samples(self) -> None:
        spec = ReplaySpec()
        packets = _frame_payloads(self.rec, spec, 137)
        self.assertEqual([p[0] for p in packets], [
            PacketType.SPECTRUM, PacketType.WAVEFORM,
            PacketType.AUDIO, PacketType.AUDIO,
        ])
        spectrum, waveform, audio_a, audio_b = (p[1] for p in packets)
        start = 137 * 1024
        source = self.rec.signals[start : start + 1024, 137 % self.rec.n_channels]

        np.testing.assert_allclose(waveform, source[:256].astype(np.float32), atol=1e-6)
        np.testing.assert_allclose(
            audio_a.astype(np.float32),
            np.clip(source[:512] * 25_000, -32768, 32767).astype(np.float32),
            atol=1.0,
        )
        segment = source[:512].copy()
        segment -= segment.mean()
        magnitude = (
            np.abs(np.fft.rfft(segment * np.hanning(512).astype(np.float32)))
            / 512.0 * 2.0
        )
        magnitude[0] = 0.0
        np.testing.assert_allclose(
            spectrum, magnitude[:128] / self.rec.spectrum_reference, atol=1e-5
        )
        self.assertGreater(float(source.std()), 1e-4)  # a real, non-constant signal

    def test_leads_are_comparable_after_normalisation(self) -> None:
        stds = self.rec.signals.std(axis=0)
        self.assertLess(max(stds) / min(stds), 3.0)

    def test_replay_over_udp(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as receiver:
            receiver.bind(("127.0.0.1", 0))
            port = receiver.getsockname()[1]
            receiver.settimeout(0.25)
            spec = ReplaySpec(
                port=port, duration_s=0.35, rate_mbps=2.0,
                loop=False, malformed_ratio=0.0,
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
                pkt, reason = decode(data)
                if pkt is not None:
                    seen.append(pkt)
            sender.join(timeout=3)
        self.assertGreater(len(seen), 4)
        self.assertTrue(all(b.seq > a.seq for a, b in zip(seen, seen[1:])))
        self.assertEqual(stats["source"]["doi"], "10.13026/C2F305")


if __name__ == "__main__":
    unittest.main()
