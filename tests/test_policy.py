"""Overflow-policy behaviour, conservation and attribution."""

import tempfile
import threading
import time
import unittest
from pathlib import Path

from src.buffers import BoundedFIFO, OverflowPolicy
from src.pipeline import PipelineConfig, StreamPipeline


class PolicyTests(unittest.TestCase):
    def test_drop_oldest_is_still_the_default(self):
        fifo = BoundedFIFO(capacity=3)
        for i in range(5):
            fifo.push(i)
        self.assertEqual(list(fifo.drain()), [2, 3, 4])
        self.assertEqual(fifo.dropped, 2)
        self.assertIs(fifo.policy, OverflowPolicy.DROP_OLDEST)

    def test_drop_newest_keeps_the_oldest_items(self):
        fifo = BoundedFIFO(capacity=3, overflow_policy="drop_newest")
        for i in range(5):
            fifo.push(i)
        self.assertEqual(list(fifo.drain()), [0, 1, 2])
        self.assertEqual(fifo.dropped, 2)

    def test_block_waits_for_a_consumer_then_accepts(self):
        fifo = BoundedFIFO(capacity=1, overflow_policy="block", block_budget=1.0)
        fifo.push(1)
        done = threading.Event()

        def producer():
            fifo.push(2)
            done.set()

        t = threading.Thread(target=producer, daemon=True)
        t.start()
        time.sleep(0.05)
        self.assertTrue(t.is_alive(), "producer should be blocked on a full buffer")
        self.assertEqual(fifo.drain(), [1])
        t.join(timeout=1.0)
        self.assertTrue(done.is_set())
        self.assertEqual(fifo.drain(), [2])
        self.assertEqual(fifo.dropped, 0)

    def test_block_degrades_after_its_budget(self):
        fifo = BoundedFIFO(capacity=1, overflow_policy="block", block_budget=0.1)
        fifo.push(1)
        started = time.perf_counter()
        fifo.push(2)
        waited = time.perf_counter() - started
        self.assertGreaterEqual(waited, 0.09)
        self.assertLess(waited, 0.6)
        self.assertEqual(fifo.dropped, 1)
        self.assertEqual(fifo.dropped_block_timeout, 1)
        self.assertEqual(fifo.drain(), [1])

    def test_block_releases_immediately_when_stopping(self):
        stop = threading.Event()
        fifo = BoundedFIFO(capacity=1, overflow_policy="block",
                           block_budget=5.0, stop_event=stop)
        fifo.push(1)
        stop.set()
        started = time.perf_counter()
        fifo.push(2)
        self.assertLess(time.perf_counter() - started, 0.2)
        self.assertEqual(fifo.dropped, 1)

    def test_unbounded_grows_then_hits_the_hard_cap(self):
        fifo = BoundedFIFO(capacity=2, overflow_policy="unbounded", unbounded_growth=2)
        for i in range(6):
            fifo.push(i)
        # limit = 2 * 2 = 4; the last two pushes evict the two oldest items.
        self.assertEqual(list(fifo.drain()), [2, 3, 4, 5])
        self.assertEqual(fifo.dropped_hard_cap, 2)
        self.assertEqual(fifo.dropped, 2)

    def test_conservation_holds_under_every_policy(self):
        for policy in OverflowPolicy:
            fifo = BoundedFIFO(capacity=8, overflow_policy=policy, block_budget=0.01)
            seen = []
            for i in range(100):
                fifo.push(i)
                if i % 7 == 0:
                    seen.extend(fifo.drain())
            seen.extend(fifo.drain())
            self.assertEqual(len(seen) + fifo.dropped, 100,
                             f"pushed items unaccounted for under {policy.value}")
            self.assertEqual(seen, sorted(seen), f"order broken under {policy.value}")

    def test_discarded_items_report_their_age(self):
        fifo = BoundedFIFO(capacity=2, age_fn=lambda item: item * 10.0)
        for i in (1, 2, 3):
            fifo.push(i)
        stats = fifo.stats()
        self.assertEqual(stats["dropped"], 1)
        self.assertEqual(stats["dropped_age_ms"]["count"], 1)
        self.assertEqual(stats["dropped_age_ms"]["p50_ms"], 10.0)
        self.assertEqual(stats["policy"], "drop_oldest")
        self.assertEqual(stats["occupancy"], 2)


class PipelineReportTests(unittest.TestCase):
    def test_report_exposes_policy_conservation_and_staleness(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = PipelineConfig(
                port=19499, overflow_policy="drop_newest",
                enable_display=False, output_dir=Path(tmp),
            )
            pipeline = StreamPipeline(cfg)
            report = pipeline.report()
        self.assertEqual(report["overflow_policy"], "drop_newest")
        self.assertEqual(report["buffers"]["source"]["policy"], "drop_newest")
        self.assertEqual(report["buffers"]["frame"]["policy"], "drop_newest")
        self.assertIn("flow", report)
        self.assertIn("parse_yield", report["flow"])
        self.assertIn("dropped_at_parse", report["flow"])
        self.assertIn("staleness_ms", report)
        self.assertEqual(report["staleness_ms"]["count"], 0)

    def test_unknown_policy_is_rejected_at_construction(self):
        with self.assertRaises(ValueError):
            BoundedFIFO(capacity=2, overflow_policy="keep_everything")


if __name__ == "__main__":
    unittest.main()
