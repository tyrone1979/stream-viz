"""StreamViz: low-latency streaming visualisation and replay prototype."""

from .pipeline import PipelineConfig, StreamPipeline
from .protocol import PacketType, decode, encode, now_ms, packet_size
from .buffers import BoundedFIFO, Datagram, Signal
from .player import AudioArchive, AudioPlayer, NullSink
from .viewstate import ViewState
from .open_dataset import DATASET, RealRecord, ReplaySpec, load_record, send_record, verify_files

__version__ = "0.3.0"
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
    "DATASET",
    "RealRecord",
    "ReplaySpec",
    "load_record",
    "send_record",
    "verify_files",
]
