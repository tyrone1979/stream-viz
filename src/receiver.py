"""Ingestion thread: UDP socket -> raw source buffer."""

from __future__ import annotations

import socket
import threading
from typing import Optional

from .buffers import BoundedFIFO, Datagram
from .metrics import Counters
from .protocol import now_ms


class UdpReceiver(threading.Thread):
    """Binds a UDP port and pushes every datagram into the source FIFO."""

    def __init__(
        self,
        host: str,
        port: int,
        source: BoundedFIFO[Datagram],
        counters: Counters,
        stop_event: threading.Event,
        recv_timeout: float = 0.2,
        recv_buf_bytes: int = 0,
        daemon: bool = True,
    ) -> None:
        super().__init__(name="receiver", daemon=daemon)
        self.host = host
        self.port = port
        self.source = source
        self.counters = counters
        self.stop_event = stop_event
        self.recv_timeout = recv_timeout
        self.recv_buf_bytes = recv_buf_bytes
        self._sock: Optional[socket.socket] = None
        self.first_ms: Optional[int] = None
        self.last_ms: Optional[int] = None

    def run(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if self.recv_buf_bytes > 0:
            # An intentionally small receive buffer is how an over-subscribed
            # sender is made to lose datagrams at the kernel: a controlled
            # experimental condition, not a defect.
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self.recv_buf_bytes)
            except OSError:
                pass
        sock.settimeout(self.recv_timeout)
        sock.bind((self.host, self.port))
        self._sock = sock
        try:
            while not self.stop_event.is_set():
                try:
                    data, _addr = sock.recvfrom(65535)
                except socket.timeout:
                    continue
                except OSError:
                    break
                arrival = now_ms()
                if self.first_ms is None:
                    self.first_ms = arrival
                self.last_ms = arrival
                self.counters.inc("udp_datagrams")
                self.counters.inc("udp_bytes", len(data))
                self.source.push(Datagram(data=data, received_ms=arrival))
        finally:
            sock.close()
            self._sock = None

    def close(self) -> None:
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
