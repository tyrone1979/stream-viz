"""Synthetic UDP stream generator (no real-world or recorded data).

Produces three kinds of numeric vectors: spectra with slowly drifting peaks,
waveforms from a sum of sinusoids, and audio blocks from a pure tone.  The
sender paces itself to a target bit rate and may optionally inject malformed
datagrams to exercise the cleaning stage.

The generator lives in the package (rather than in ``examples``) because the
interactive GUI starts it as part of the application itself.
"""

from __future__ import annotations

import socket
import threading
import time
from dataclasses import dataclass
from typing import Dict, Optional

import numpy as np

from .protocol import PacketType, encode, now_ms, packet_size

# 5 spectrum + 1 waveform + 2 audio packets per cycle
_CYCLE = [
    PacketType.SPECTRUM,
    PacketType.SPECTRUM,
    PacketType.SPECTRUM,
    PacketType.SPECTRUM,
    PacketType.SPECTRUM,
    PacketType.WAVEFORM,
    PacketType.AUDIO,
    PacketType.AUDIO,
]


@dataclass
class StreamSpec:
    host: str = "127.0.0.1"
    port: int = 19000
    duration_s: float = 3.0
    rate_mbps: float = 4.0
    n_channels: int = 4
    n_bins: int = 128
    waveform_len: int = 256
    audio_len: int = 512
    sample_rate: int = 16_000
    seed: int = 7
    malformed_ratio: float = 0.0


def average_packet_bytes(spec: StreamSpec) -> float:
    sizes = 0
    for ptype in _CYCLE:
        n = spec.n_bins if ptype is PacketType.SPECTRUM else (
            spec.waveform_len if ptype is PacketType.WAVEFORM else spec.audio_len
        )
        sizes += packet_size(ptype, n)
    return sizes / len(_CYCLE)


def _spectrum(rng: np.random.Generator, spec: StreamSpec, channel: int, t: float) -> np.ndarray:
    bins = np.arange(spec.n_bins, dtype=np.float32)
    base = rng.random(spec.n_bins).astype(np.float32) * 0.08
    center = (0.25 + 0.12 * np.sin(t * 0.7 + channel)) * spec.n_bins
    peak = 1.1 * np.exp(-0.5 * ((bins - center) / 3.0) ** 2)
    second = 0.5 * np.exp(-0.5 * ((bins - (0.7 * spec.n_bins + 5 * np.cos(t))) / 5.0) ** 2)
    return (base + peak + second).astype(np.float32)


def _waveform(spec: StreamSpec, t: float) -> np.ndarray:
    x = np.arange(spec.waveform_len, dtype=np.float32) / spec.waveform_len
    return (0.7 * np.sin(2 * np.pi * 6 * x + t * 2.0)
            + 0.25 * np.sin(2 * np.pi * 17 * x + t)).astype(np.float32)


def _audio(spec: StreamSpec, seq: int) -> np.ndarray:
    start = seq * spec.audio_len
    phase = 2 * np.pi * 440.0 * (start + np.arange(spec.audio_len)) / spec.sample_rate
    tone = 3500 * np.sin(phase)
    return tone.astype(np.int16)


def send_stream(
    spec: StreamSpec,
    stop_event: Optional[threading.Event] = None,
    start_delay: float = 0.0,
) -> Dict:
    """Blocking sender.  Returns simple send-side statistics."""
    if start_delay > 0:
        time.sleep(start_delay)
    rng = np.random.default_rng(spec.seed)
    avg_bytes = average_packet_bytes(spec)
    packets_per_second = max(1.0, spec.rate_mbps * 1e6 / (avg_bytes * 8))
    interval = 1.0 / packets_per_second

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    target = (spec.host, spec.port)
    t0 = time.perf_counter()
    deadline = t0 + spec.duration_s
    next_slot = t0
    seq = 0
    sent = 0
    malformed = 0
    try:
        while time.perf_counter() < deadline:
            if stop_event is not None and stop_event.is_set():
                break
            elapsed = time.perf_counter() - t0
            ptype = _CYCLE[seq % len(_CYCLE)]
            channel = (seq // 2) % spec.n_channels
            ts = now_ms()
            if ptype is PacketType.SPECTRUM:
                values = _spectrum(rng, spec, channel, elapsed)
            elif ptype is PacketType.WAVEFORM:
                values = _waveform(spec, elapsed)
            else:
                values = _audio(spec, seq)
            packet = encode(ptype, channel, seq, ts, values)
            if spec.malformed_ratio and rng.random() < spec.malformed_ratio:
                packet = b"BAD!" + packet[4:]
                malformed += 1
            sock.sendto(packet, target)
            sent += 1
            seq += 1
            next_slot += interval
            remaining = next_slot - time.perf_counter()
            if remaining > 0.02:
                # Only sleep for long gaps; short OS sleeps overshoot badly and
                # silently cap the achievable packet rate.
                time.sleep(remaining - 0.01)
                while time.perf_counter() < next_slot:
                    pass
            elif remaining > 0:
                while time.perf_counter() < next_slot:
                    pass
            else:
                next_slot = time.perf_counter()
    finally:
        sock.close()
    actual = time.perf_counter() - t0
    return {
        "sent_packets": sent,
        "sent_malformed": malformed,
        "duration_s": round(actual, 3),
        "target_packets_per_second": round(packets_per_second, 1),
        "average_packet_bytes": round(avg_bytes, 1),
    }
