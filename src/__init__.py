"""StreamViz: low-latency streaming visualisation and replay prototype."""

from .pipeline import PipelineConfig, StreamPipeline
from .protocol import PacketType, decode, encode, now_ms, packet_size
from .buffers import BoundedFIFO, Datagram, Signal
from .player import AudioArchive, AudioPlayer, NullSink
from .viewstate import ViewState

__version__ = "0.2.0"
__all__ = [
    "PipelineConfig",
    "StreamPipeline",
    "PacketType",
    "decode",
    "encode",
    "now_ms",
    "packet_size",
    "BoundedFIFO",
    "Datagram",
    "Signal",
    "AudioArchive",
    "AudioPlayer",
    "NullSink",
    "ViewState",
]
