"""Thread-safe bounded buffers and the cache-full notification signal.

The design deliberately separates *ingestion* from *presentation*: a producer
never touches the display, it only fills a bounded FIFO and raises a signal
when the buffer is full.  A consumer waits on that signal with a timeout, so
it behaves both as an event-driven listener and as a periodic timer.
"""

from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass
from typing import Deque, Generic, List, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class Datagram:
    """Raw UDP payload plus the host-side arrival time."""

    data: bytes
    received_ms: int


class Signal:
    """Level-triggered, auto-clearing notification."""

    def __init__(self) -> None:
        self._event = threading.Event()

    def set(self) -> None:
        self._event.set()

    def wait(self, timeout: float) -> bool:
        """Wait up to ``timeout`` seconds; returns True if the signal fired."""
        fired = self._event.wait(timeout)
        if fired:
            self._event.clear()
        return fired

    def clear(self) -> None:
        self._event.clear()


class BoundedFIFO(Generic[T]):
    """Fixed-capacity FIFO.  Overflow drops the oldest item (real-time bias)."""

    def __init__(self, capacity: int) -> None:
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        self.capacity = capacity
        self._items: Deque[T] = deque()
        self._lock = threading.Lock()
        self.dropped = 0

    def push(self, item: T) -> bool:
        """Append one item; return True when the buffer has just become full."""
        with self._lock:
            self._items.append(item)
            while len(self._items) > self.capacity:
                self._items.popleft()
                self.dropped += 1
            return len(self._items) >= self.capacity

    def push_many(self, items: List[T]) -> bool:
        with self._lock:
            for item in items:
                self._items.append(item)
                while len(self._items) > self.capacity:
                    self._items.popleft()
                    self.dropped += 1
            return len(self._items) >= self.capacity

    def drain(self) -> List[T]:
        """Atomically remove and return everything currently buffered."""
        with self._lock:
            out = list(self._items)
            self._items.clear()
            return out

    def __len__(self) -> int:
        with self._lock:
            return len(self._items)
