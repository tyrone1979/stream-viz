import struct
import unittest

import numpy as np

from src.protocol import MAGIC, PacketType, VERSION, _HEADER, decode, encode, now_ms, packet_size


class ProtocolTests(unittest.TestCase):
    def test_roundtrip_all_types(self):
        cases = [
            (PacketType.SPECTRUM, np.arange(128, dtype=np.float32) / 128),
            (PacketType.WAVEFORM, np.linspace(-1, 1, 256, dtype=np.float32)),
            (PacketType.AUDIO, np.arange(512, dtype=np.int16) % 1000),
        ]
        for ptype, values in cases:
            with self.subTest(ptype=ptype):
                raw = encode(ptype, 3, 42, now_ms(), values)
                packet, reason = decode(raw, received_ms=now_ms())
                self.assertEqual(reason, "ok")
                self.assertIs(packet.type, ptype)
                self.assertEqual(packet.channel, 3)
                self.assertEqual(packet.seq, 42)
                np.testing.assert_array_equal(packet.values, np.asarray(values))

    def test_packet_size_matches_wire_length(self):
        raw = encode(PacketType.SPECTRUM, 0, 0, 1, np.zeros(64, dtype=np.float32))
        self.assertEqual(len(raw), packet_size(PacketType.SPECTRUM, 64))
        self.assertEqual(_HEADER.size, 24)

    def test_cleaning_rejects_malformed(self):
        good = encode(PacketType.SPECTRUM, 0, 1, now_ms(), np.ones(64, dtype=np.float32))
        bad = {
            "short_header": good[:5],
            "bad_magic": b"XXXX" + good[4:],
            "bad_version": good[:4] + bytes([VERSION + 9]) + good[5:],
            "unknown_type": good[:5] + bytes([99]) + good[6:],
            "truncated_payload": good[:-8],
        }
        odd = bytearray(good[:_HEADER.size] + b"\x01\x02\x03")
        struct.pack_into("<I", odd, 20, 3)
        bad["misaligned_payload"] = bytes(odd)
        for reason, raw in bad.items():
            with self.subTest(reason=reason):
                packet, got = decode(raw)
                self.assertIsNone(packet)
                self.assertEqual(got, reason)

        zero_ts = bytearray(good)
        struct.pack_into("<q", zero_ts, 12, 0)
        packet, reason = decode(bytes(zero_ts))
        self.assertIsNone(packet)
        self.assertEqual(reason, "bad_timestamp")

    def test_rejects_oversized_payload(self):
        values = np.zeros(128, dtype=np.float32)
        raw = bytearray(encode(PacketType.SPECTRUM, 0, 1, now_ms(), values))
        struct.pack_into("<I", raw, 20, 100_000)
        packet, reason = decode(bytes(raw))
        self.assertIsNone(packet)
        self.assertIn(reason, ("bad_length", "truncated_payload"))


if __name__ == "__main__":
    unittest.main()
