"""Parsing/cleaning thread: raw datagrams -> typed buffers + notification.

Pipeline stage responsibilities:
  * validate and clean every datagram (protocol.decode)
  * detect sequence gaps / duplicates (best-effort loss accounting)
  * route numeric vectors to the display FIFO or the audio archive
  * raise the ready signal when the display buffer fills
"""

from __future__ import annotations

import threading
from typing import Optional

from .buffers import BoundedFIFO, Datagram, Signal
from .metrics import Counters, LatencyMeter
from .player import AudioArchive
from .protocol import PacketType, decode


class ParserThread(threading.Thread):
    def __init__(
        self,
        source: BoundedFIFO[Datagram],
        frame_buffer: BoundedFIFO,
        audio_archive: AudioArchive,
        ready_signal: Signal,
        counters: Counters,
        latency: LatencyMeter,
        stop_event: threading.Event,
        idle_wait: float = 0.002,
        daemon: bool = True,
    ) -> None:
        super().__init__(name="parser", daemon=daemon)
        self.source = source
        self.frame_buffer = frame_buffer
        self.audio_archive = audio_archive
        self.ready_signal = ready_signal
        self.counters = counters
        self.latency = latency
        self.stop_event = stop_event
        self.idle_wait = idle_wait
        self._last_seq: Optional[int] = None

    def run(self) -> None:
        while not self.stop_event.is_set():
            batch = self.source.drain()
            if not batch:
                self.stop_event.wait(self.idle_wait)
                continue
            for item in batch:
                packet, reason = decode(item.data, item.received_ms)
                if packet is None:
                    self.counters.inc("dropped_" + reason)
                    continue

                seq = packet.seq
                if self._last_seq is not None:
                    if seq <= self._last_seq:
                        self.counters.inc("duplicate_or_reorder")
                        continue
                    gap = seq - self._last_seq - 1
                    if gap > 0:
                        self.counters.inc("sequence_gaps", gap)
                self._last_seq = seq

                self.counters.inc("parsed_packets")
                self.counters.inc("parsed_" + packet.type.name.lower())
                self.latency.add(item.received_ms - packet.timestamp_ms)

                if packet.type is PacketType.AUDIO:
                    self.audio_archive.append(packet)
                else:
                    full = self.frame_buffer.push(packet)
                    if full:
                        self.ready_signal.set()
                        self.counters.inc("cache_full_notifications")
