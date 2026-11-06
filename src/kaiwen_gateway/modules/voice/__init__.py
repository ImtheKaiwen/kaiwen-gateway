"""Voice streaming module for Kaiwen Gateway.

Provides voice-local media domain contracts, provider-neutral media ports,
and a bounded bidirectional audio bridge.
"""

from kaiwen_gateway.modules.voice.adapters.fake import (
    FakeAIMediaAdapter,
    FakeTelephonyMediaAdapter,
)
from kaiwen_gateway.modules.voice.bridge import (
    VoiceBridgeSession,
    VoiceBridgeState,
)
from kaiwen_gateway.modules.voice.domain import (
    DEFAULT_MAX_PAYLOAD_SIZE,
    AudioFormat,
    AudioFrame,
    InvalidAudioFrameError,
    MediaBackpressureOverflowError,
    MediaPortClosedError,
    MediaPortError,
    OutOfOrderAudioFrameError,
    OversizedAudioFrameError,
    VoiceBridgeSessionClosedError,
    VoiceDomainError,
    create_synthetic_pcm16le_frame,
)
from kaiwen_gateway.modules.voice.ports import (
    AIRealtimeMediaPort,
    TelephonyMediaPort,
)

__all__ = [
    "DEFAULT_MAX_PAYLOAD_SIZE",
    "AudioFormat",
    "AudioFrame",
    "VoiceDomainError",
    "InvalidAudioFrameError",
    "OversizedAudioFrameError",
    "OutOfOrderAudioFrameError",
    "MediaBackpressureOverflowError",
    "MediaPortError",
    "MediaPortClosedError",
    "VoiceBridgeSessionClosedError",
    "create_synthetic_pcm16le_frame",
    "TelephonyMediaPort",
    "AIRealtimeMediaPort",
    "VoiceBridgeState",
    "VoiceBridgeSession",
    "FakeTelephonyMediaAdapter",
    "FakeAIMediaAdapter",
]
