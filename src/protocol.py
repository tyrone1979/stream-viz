"""Domain-neutral binary packet protocol for high-rate streaming telemetry.

The header contains only generic transport fields (magic, version, packet
type, channel id, sequence number, timestamp, payload length).  Validation
rules in :func:`decode` implement the "data cleaning" stage of the pipeline:
malformed, misaligned or oversized packets are dropped before they reach any
buffer.

No application-specific field names, units, or acquisition geometry appear
in this module.  All payloads are plain numeric vectors.
"""

from __future__ import annotations

import struct
import time
from dataclasses import dataclass
from enum import IntEnum
from typing import Optional, Sequence, Tuple

import numpy as np

MAGIC = b"STRM"
VERSION = 1
MAX_PAYLOAD_BYTES = 64 * 1024

# magic | version | type | channel | seq | timestamp_ms | payload_len
_HEADER = struct.Struct("<4sBBHIqI")


class PacketType(IntEnum):
    """Numeric vector carried by the packet."""

    SPECTRUM = 1  # float32 bins
    WAVEFORM = 2  # float32 samples
    AUDIO = 3  # int16 samples


_DTYPE = {
    PacketType.SPECTRUM: np.float32,
    PacketType.WAVEFORM: np.float32,
    PacketType.AUDIO: np.int16,
}
_ITEM_BYTES = {
    PacketType.SPECTRUM: 4,
    PacketType.WAVEFORM: 4,
    PacketType.AUDIO: 2,
}


@dataclass(frozen=True)
class Packet:
    type: PacketType
    channel: int
    seq: int
    timestamp_ms: int
    values: np.ndarray
    received_ms: int = 0


def now_ms() -> int:
    """Wall-clock milliseconds, used for both timestamps and latency metering."""
    return time.time_ns() // 1_000_000


def encode(
    ptype: PacketType,
    channel: int,
    seq: int,
    timestamp_ms: int,
    values: Sequence[float],
) -> bytes:
    """Serialise one numeric vector into a wire packet."""
    ptype = PacketType(ptype)
    arr = np.asarray(values, dtype=_DTYPE[ptype])
    payload = arr.tobytes()
    if len(payload) > MAX_PAYLOAD_BYTES:
        raise ValueError("payload exceeds MAX_PAYLOAD_BYTES")
    header = _HEADER.pack(
        MAGIC,
        VERSION,
        int(ptype),
        int(channel) & 0xFFFF,
        int(seq) & 0xFFFFFFFF,
        int(timestamp_ms),
        len(payload),
    )
    return header + payload


def decode(raw: bytes, received_ms: Optional[int] = None) -> Tuple[Optional[Packet], str]:
    """Validate and parse one datagram.

    Returns ``(packet, "ok")`` on success, otherwise ``(None, reason)`` where
    ``reason`` is a stable key suitable for drop counters.
    """
    if received_ms is None:
        received_ms = now_ms()
    if len(raw) < _HEADER.size:
        return None, "short_header"

    magic, version, type_id, channel, seq, timestamp_ms, payload_len = _HEADER.unpack_from(raw)
    if magic != MAGIC:
        return None, "bad_magic"
    if version != VERSION:
        return None, "bad_version"
    try:
        ptype = PacketType(type_id)
    except ValueError:
        return None, "unknown_type"
    if payload_len <= 0 or payload_len > MAX_PAYLOAD_BYTES:
        return None, "bad_length"
    if payload_len % _ITEM_BYTES[ptype] != 0:
        return None, "misaligned_payload"
    body = raw[_HEADER.size :]
    if len(body) != payload_len:
        return None, "truncated_payload"
    if timestamp_ms <= 0:
        return None, "bad_timestamp"

    values = np.frombuffer(body, dtype=_DTYPE[ptype])
    packet = Packet(
        type=ptype,
        channel=channel,
        seq=seq,
        timestamp_ms=timestamp_ms,
        values=values,
        received_ms=received_ms,
    )
    return packet, "ok"


def packet_size(ptype: PacketType, n_items: int) -> int:
    """Wire size in bytes, useful for pacing a synthetic sender."""
    return _HEADER.size + n_items * _ITEM_BYTES[PacketType(ptype)]
