import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path

import numpy as np

from src.pipeline import PipelineConfig, StreamPipeline
from src.protocol import PacketType, encode, now_ms


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class EndToEndTests(unittest.TestCase):
    def test_udp_pipeline_and_replay(self):
        port = free_port()
        with tempfile.TemporaryDirectory() as tmp:
            cfg = PipelineConfig(
                port=port,
                frame_capacity=8,
                audio_block_samples=1024,
                enable_display=False,
                output_dir=Path(tmp),
            )
            pipeline = StreamPipeline(cfg).start()
            time.sleep(0.05)

            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            target = ("127.0.0.1", port)
            for seq in range(120):
                if seq % 4 == 3:
                    ptype = PacketType.AUDIO
                    values = np.full(512, 200, dtype=np.int16)
                elif seq % 4 == 2:
                    ptype = PacketType.WAVEFORM
                    values = np.zeros(256, dtype=np.float32)
                else:
                    ptype = PacketType.SPECTRUM
                    values = np.full(128, 0.5, dtype=np.float32)
                sock.sendto(encode(ptype, seq % 4, seq, now_ms(), values), target)
            # inject a corrupt datagram to exercise the cleaning stage
            sock.sendto(b"CORRUPTED-DATAGRAM", target)
            sock.close()

            # Wait for *both* the ingest and the cleaning stage to finish.
            # The corrupt datagram may arrive in a later receiver batch, so
            # stopping as soon as 100 packets are parsed makes this flaky.
            deadline = time.time() + 3.0
            while time.time() < deadline:
                counts = pipeline.counters.snapshot()
                cleaned = (counts.get("dropped_short_header", 0)
                           + counts.get("dropped_bad_magic", 0))
                if counts.get("parsed_packets", 0) >= 100 and cleaned >= 1:
                    break
                time.sleep(0.02)
            pipeline.stop()

            report = pipeline.report()
            counters = report["counters"]
            self.assertGreaterEqual(counters.get("parsed_packets", 0), 100)
            self.assertGreaterEqual(counters.get("cache_full_notifications", 0), 1)
            self.assertGreaterEqual(counters.get("dropped_short_header", 0)
                                    + counters.get("dropped_bad_magic", 0), 1)
            self.assertGreater(report["end_to_end_latency_ms"]["count"], 0)
            self.assertGreater(report["audio_segments"], 0)

            first = pipeline.audio_archive.segments[0]
            samples, rate, seg = pipeline.play_at(first.start_ms + 1)
            self.assertIsNotNone(seg)
            self.assertGreater(len(samples), 0)
            self.assertGreater(pipeline.play_realtime(), 0)


if __name__ == "__main__":
    unittest.main()
