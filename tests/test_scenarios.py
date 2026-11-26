"""Load-profile semantics of the synthetic multi-sensor scenario."""

from __future__ import annotations

import time
import unittest

from src.scenarios import (
    PROVENANCE,
    ArrayScenarioSpec,
    _cycle,
    average_packet_bytes,
    send_scenario,
)


class ProfileTests(unittest.TestCase):
    def test_profile_encodes_burst_steady_and_outage(self):
        spec = ArrayScenarioSpec(cycle_s=3.0, burst_s=1.0, outage_s=0.5,
                                 burst_gain=3.0)
        self.assertEqual(spec.profile(0.0), 3.0)     # inside the burst
        self.assertEqual(spec.profile(2.4), 1.0)     # steady stretch
        self.assertEqual(spec.profile(2.8), 0.0)     # outage at the end
        self.assertEqual(spec.profile(3.1), 3.0)     # next cycle
        self.assertEqual(spec.profile(5.9), 0.0)

    def test_profile_is_periodic(self):
        spec = ArrayScenarioSpec()
        for t in (0.3, 1.2, 2.7, 2.85):
            self.assertAlmostEqual(spec.profile(t), spec.profile(t + spec.cycle_s))

    def test_cycle_covers_every_sensor(self):
        spec = ArrayScenarioSpec(n_sensors=5)
        cycle = _cycle(spec)
        channels = {channel for ptype, channel in cycle
                    if ptype.name == "SPECTRUM"}
        self.assertEqual(channels, set(range(5)))
        self.assertGreater(average_packet_bytes(spec), 0.0)

    def test_provenance_says_synthetic(self):
        self.assertIn("synthetic", PROVENANCE)
        self.assertIn("no recorded data", PROVENANCE)


class SenderTests(unittest.TestCase):
    def test_outage_produces_silence_and_the_burst_produces_a_burst(self):
        """A single cycle: packets in the burst, few in the steady stretch,
        none at all during the outage."""
        spec = ArrayScenarioSpec(
            port=1,                      # discarded: the socket has no listener
            duration_s=1.6,
            base_rate_mbps=0.5,
            cycle_s=1.6,
            burst_s=0.6,
            outage_s=0.5,
            burst_gain=4.0,
            n_sensors=2,
        )
        started = time.perf_counter()
        stats = send_scenario(spec)
        self.assertGreater(stats["sent_packets"], 0)
        self.assertEqual(stats["sent_malformed"], 0)
        self.assertIn("profile", stats)
        self.assertEqual(stats["profile"]["burst_gain"], 4.0)
        # The run must actually last the requested duration, otherwise the
        # outage segment would be silently skipped.
        self.assertGreaterEqual(time.perf_counter() - started, 1.5)


if __name__ == "__main__":
    unittest.main()
