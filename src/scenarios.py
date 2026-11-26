"""Synthetic multi-sensor acoustic scenario with bursty, extreme load.

Motivation
----------
Field deployments of a multi-sensor acoustic array do not produce a smooth
stream.  They produce long quiet stretches, short broadband bursts when a
transient crosses the array, and outages when a sensor or a link drops out.
The interesting behaviour of a display pipeline — what it discards, and how
stale the surviving frame is — only shows up in those regimes.

The recordings that would let one replay this faithfully are access-restricted
and cannot be redistributed, so this module generates a **load pattern and a
signal shape** that exercise the same regimes without carrying any recorded
data: multi-tone narrowband content per sensor, impulsive transients, a noise
floor, and a pacing profile with bursts and outages.  Every payload is
synthesised from a seeded generator; nothing here is derived from, or a
reconstruction of, a real recording.

What this is good for
---------------------
* reproducing the overload regime of a live deployment on a laptop,
* comparing overflow policies (``drop_oldest`` / ``drop_newest`` / ``block`` /
  ``unbounded``) under burst + outage rather than steady load,
* exercising the audio path (audification, WAV segmentation, timestamp-seek
  replay) with content that has transients worth listening for.

What this is **not**
--------------------
It is not a validated model of any particular sensor, array geometry, or
propagation environment, and it must not be reported as measured field data.
"""

from __future__ import annotations

import socket
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np

from .protocol import PacketType, encode, now_ms, packet_size

PROVENANCE = (
    "synthetic multi-sensor acoustic scenario (seeded generator); "
    "no recorded data, no field measurements"
)


@dataclass
class ArrayScenarioSpec:
    host: str = "127.0.0.1"
    port: int = 19000
    duration_s: float = 8.0
    base_rate_mbps: float = 3.0
    n_sensors: int = 6
    n_bins: int = 128
    waveform_len: int = 256
    audio_len: int = 512
    sample_rate: int = 16_000
    seed: int = 11
    # Load profile, repeated with period ``cycle_s``:
    #   [0, burst_s)                      -> burst_gain x base rate
    #   [burst_s, cycle_s - outage_s)     -> 1.0 x base rate
    #   [cycle_s - outage_s, cycle_s)     -> silence (link/sensor outage)
    cycle_s: float = 3.0
    burst_s: float = 0.9
    outage_s: float = 0.4
    burst_gain: float = 3.0
    transient_rate_hz: float = 0.7   # impulsive events per second per sensor
    malformed_ratio: float = 0.0

    def profile(self, t: float) -> float:
        """Instantaneous rate multiplier at scenario time ``t`` seconds."""
        phase = t % self.cycle_s
        if phase < self.burst_s:
            return self.burst_gain
        if phase >= max(self.burst_s, self.cycle_s - self.outage_s):
            return 0.0
        return 1.0


def _cycle(spec: ArrayScenarioSpec) -> List[Tuple[PacketType, int]]:
    """One round of packets: every sensor contributes a spectrum + waveform,
    and one audification block is interleaved.  Ordering matters because the
    parser labels the view by ``channel``."""
    cycle: List[Tuple[PacketType, int]] = []
    for sensor in range(spec.n_sensors):
        cycle.append((PacketType.SPECTRUM, sensor))
        cycle.append((PacketType.WAVEFORM, sensor))
    cycle.append((PacketType.AUDIO, 0))
    return cycle


def average_packet_bytes(spec: ArrayScenarioSpec) -> float:
    sizes = []
    for ptype, _ in _cycle(spec):
        n = (spec.n_bins if ptype is PacketType.SPECTRUM
             else spec.waveform_len if ptype is PacketType.WAVEFORM
             else spec.audio_len)
        sizes.append(packet_size(ptype, n))
    return float(np.mean(sizes))


def _sensor_spectrum(
    rng: np.random.Generator, spec: ArrayScenarioSpec, sensor: int, t: float
) -> np.ndarray:
    """Narrowband lines that drift slowly, plus a noise floor."""
    bins = np.arange(spec.n_bins, dtype=np.float32)
    floor = rng.random(spec.n_bins).astype(np.float32) * 0.05
    # Each sensor sits at its own place in the band, with two harmonics.
    base = (0.12 + 0.11 * sensor) * spec.n_bins
    drift = 2.0 * np.sin(0.35 * t + sensor)
    lines = np.zeros(spec.n_bins, dtype=np.float32)
    for k, weight in ((1.0, 1.0), (1.9, 0.45), (2.7, 0.2)):
        centre = base * k / 1.0 + drift
        lines += weight * np.exp(-0.5 * ((bins - centre) / 1.6) ** 2)
    return (floor + lines).astype(np.float32)


def _sensor_waveform(
    rng: np.random.Generator, spec: ArrayScenarioSpec, sensor: int, t: float
) -> np.ndarray:
    """Multi-tone content with a sparse impulsive transient."""
    x = np.arange(spec.waveform_len, dtype=np.float32) / spec.waveform_len
    f1 = 5.0 + sensor
    signal = 0.55 * np.sin(2 * np.pi * f1 * x + 1.7 * t)
    signal += 0.22 * np.sin(2 * np.pi * (2.4 * f1) * x + 0.6 * t)
    signal += 0.08 * rng.standard_normal(spec.waveform_len)
    if rng.random() < spec.transient_rate_hz / 50.0:
        # A short broadband click: what an operator actually listens for.
        width = max(2, spec.waveform_len // 40)
        start = int(rng.integers(0, spec.waveform_len - width))
        signal[start:start + width] += 1.6 * np.hanning(width)
    return signal.astype(np.float32)


def _audification(spec: ArrayScenarioSpec, seq: int, sensor: int) -> np.ndarray:
    """Time-compress the sensor content into a 16 kHz int16 audio block."""
    start = seq * spec.audio_len
    t = start / spec.sample_rate
    block = _sensor_waveform(np.random.default_rng(seq), spec, sensor, t)
    # Tile the short analysis frame up to the audio block length.
    reps = int(np.ceil(spec.audio_len / len(block)))
    tiled = np.tile(block, reps)[: spec.audio_len]
    return np.clip(tiled * 9000.0, -32767, 32767).astype(np.int16)


@dataclass
class ScenarioStats:
    sent_packets: int = 0
    sent_malformed: int = 0
    duration_s: float = 0.0
    peak_packets_per_s: float = 0.0
    profile: Dict[str, float] = field(default_factory=dict)
    provenance: str = PROVENANCE


def send_scenario(
    spec: ArrayScenarioSpec,
    stop_event: Optional[threading.Event] = None,
    start_delay: float = 0.0,
) -> Dict:
    """Blocking bursty sender.  Returns send-side statistics as a dict.

    Pacing is credit-based rather than slot-based: the credit accumulates at
    the *instantaneous* rate from :meth:`ArrayScenarioSpec.profile`, so a burst
    really is a burst and an outage really is silence.  A fixed inter-packet
    interval cannot express either.
    """
    if start_delay > 0:
        time.sleep(start_delay)
    rng = np.random.default_rng(spec.seed)
    cycle = _cycle(spec)
    avg_bytes = average_packet_bytes(spec)
    base_pps = max(1.0, spec.base_rate_mbps * 1e6 / (avg_bytes * 8))

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    target = (spec.host, spec.port)
    t0 = time.perf_counter()
    deadline = t0 + spec.duration_s
    seq = 0
    sent = malformed = 0
    credit = 0.0
    last = t0
    peak_pps = 0.0
    window_start = t0
    window_count = 0

    try:
        while time.perf_counter() < deadline:
            if stop_event is not None and stop_event.is_set():
                break
            now = time.perf_counter()
            dt = now - last
            last = now
            elapsed = now - t0
            credit += spec.profile(elapsed) * base_pps * dt

            # Cap the backlog so a long stall cannot turn into an unbounded
            # catch-up flood that is not part of the modelled profile.
            credit = min(credit, base_pps * spec.burst_gain * 0.25)
            to_send = int(credit)
            if to_send <= 0:
                time.sleep(0.0005)
                continue
            credit -= to_send

            for _ in range(to_send):
                ptype, sensor = cycle[seq % len(cycle)]
                if ptype is PacketType.SPECTRUM:
                    values = _sensor_spectrum(rng, spec, sensor, elapsed)
                elif ptype is PacketType.WAVEFORM:
                    values = _sensor_waveform(rng, spec, sensor, elapsed)
                else:
                    values = _audification(spec, seq, sensor)
                packet = encode(ptype, sensor % 256, seq, now_ms(), values)
                if spec.malformed_ratio and rng.random() < spec.malformed_ratio:
                    packet = b"BAD!" + packet[4:]
                    malformed += 1
                sock.sendto(packet, target)
                sent += 1
                seq += 1
                window_count += 1
            if now - window_start >= 0.25:
                peak_pps = max(peak_pps, window_count / (now - window_start))
                window_start, window_count = now, 0
    finally:
        sock.close()

    stats = ScenarioStats(
        sent_packets=sent,
        sent_malformed=malformed,
        duration_s=round(time.perf_counter() - t0, 3),
        peak_packets_per_s=round(peak_pps, 1),
        profile={
            "cycle_s": spec.cycle_s,
            "burst_s": spec.burst_s,
            "burst_gain": spec.burst_gain,
            "outage_s": spec.outage_s,
            "base_rate_mbps": spec.base_rate_mbps,
            "n_sensors": spec.n_sensors,
            "base_packets_per_s": round(base_pps, 1),
        },
    )
    return {
        "sent_packets": stats.sent_packets,
        "sent_malformed": stats.sent_malformed,
        "duration_s": stats.duration_s,
        "peak_packets_per_s": stats.peak_packets_per_s,
        "profile": stats.profile,
        "provenance": stats.provenance,
        "average_packet_bytes": round(avg_bytes, 1),
    }
