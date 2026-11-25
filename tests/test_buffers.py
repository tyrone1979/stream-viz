import tempfile
import threading
import unittest
from pathlib import Path

import numpy as np

from src.buffers import BoundedFIFO, Signal
from src.player import AudioArchive, AudioPlayer, NullSink
from src.protocol import Packet, PacketType


class BufferTests(unittest.TestCase):
    def test_fifo_drops_oldest_on_overflow(self):
        fifo = BoundedFIFO(capacity=3)
        for i in range(5):
            fifo.push(i)
        self.assertEqual(list(fifo.drain()), [2, 3, 4])
        self.assertEqual(fifo.dropped, 2)
        self.assertEqual(len(fifo), 0)

    def test_push_returns_full_flag(self):
        fifo = BoundedFIFO(capacity=2)
        self.assertFalse(fifo.push("a"))
        self.assertTrue(fifo.push("b"))
        self.assertTrue(fifo.push("c"))

    def test_signal_auto_clears(self):
        sig = Signal()
        sig.set()
        self.assertTrue(sig.wait(0.01))
        self.assertFalse(sig.wait(0.01))

    def test_concurrent_producer_consumer(self):
        fifo = BoundedFIFO(capacity=1000)
        done = threading.Event()

        def produce():
            for i in range(2000):
                fifo.push(i)
            done.set()

        t = threading.Thread(target=produce)
        t.start()
        seen = []
        while not done.is_set() or len(fifo):
            seen.extend(fifo.drain())
        t.join()
        # Every pushed item is either consumed or dropped by overflow.
        self.assertEqual(len(seen) + fifo.dropped, 2000)
        self.assertEqual(seen, sorted(seen))
        self.assertTrue(all(0 <= v < 2000 for v in seen))


class ArchiveTests(unittest.TestCase):
    def _packet(self, seq: int, ts: int) -> Packet:
        values = np.full(512, 100, dtype=np.int16)
        return Packet(PacketType.AUDIO, 0, seq, ts, values, received_ms=ts)

    def test_flush_index_and_replay(self):
        with tempfile.TemporaryDirectory() as tmp:
            archive = AudioArchive(Path(tmp), sample_rate=16_000, block_samples=1024)
            for i in range(6):
                archive.append(self._packet(i, 1000 + i * 32))
            self.assertEqual(len(archive), 3)  # 6 x 512 samples / 1024 per block
            seg = archive.locate(1030)
            self.assertIsNotNone(seg)
            samples, rate = AudioArchive.read(seg)
            self.assertEqual(rate, 16_000)
            self.assertEqual(len(samples), 1024)
            tail, rate, found = AudioPlayer(archive, NullSink()).play_history(1030)
            self.assertIs(found, seg)
            self.assertGreater(len(tail), 0)
            self.assertLess(len(tail), 1024)

    def test_flush_partial_block(self):
        with tempfile.TemporaryDirectory() as tmp:
            archive = AudioArchive(Path(tmp), block_samples=4096)
            archive.append(self._packet(1, 5000))
            self.assertEqual(len(archive), 0)
            path = archive.flush()
            self.assertIsNotNone(path)
            self.assertEqual(len(archive), 1)
            self.assertIsNone(archive.flush())


if __name__ == "__main__":
    unittest.main()
