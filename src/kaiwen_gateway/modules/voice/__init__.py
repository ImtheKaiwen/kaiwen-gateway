"""Voice streaming module for Kaiwen Gateway.

Provides voice-local media domain contracts, provider-neutral media ports,
and a bounded bidirectional audio bridge.
"""

from kaiwen_gateway.modules.voice.adapters.fake import (
    FakeAIMediaAdapter,
    FakeTelephonyMediaAdapter,
    InterruptibleFakeTelephonyAdapter,
)
from kaiwen_gateway.modules.voice.adapters.fake_ai import (
    BoundedFakeAIEchoPort,
    InterruptibleFakeAIPort,
)
from kaiwen_gateway.modules.voice.bridge import (
    VoiceBridgeSession,
    VoiceBridgeState,
)
from kaiwen_gateway.modules.voice.domain import (
    DEFAULT_MAX_PAYLOAD_SIZE,
    AudioFormat,
    AudioFrame,
    FutureTurnAudioFrameError,
    InvalidAudioFrameError,
    MediaBackpressureOverflowError,
    MediaPortClosedError,
    MediaPortError,
    OutOfOrderAudioFrameError,
    OversizedAudioFrameError,
    UnsupportedInterruptionCapabilityError,
    VoiceBridgeSessionClosedError,
    VoiceDomainError,
    create_synthetic_pcm16le_frame,
)
from kaiwen_gateway.modules.voice.ports import (
    AIRealtimeMediaPort,
    InterruptibleAIPort,
    InterruptibleTelephonyPort,
    InterruptionCapabilities,
    InterruptionOutcome,
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
    "UnsupportedInterruptionCapabilityError",
    "FutureTurnAudioFrameError",
    "create_synthetic_pcm16le_frame",
    "TelephonyMediaPort",
    "AIRealtimeMediaPort",
    "InterruptionCapabilities",
    "InterruptionOutcome",
    "InterruptibleAIPort",
    "InterruptibleTelephonyPort",
    "VoiceBridgeState",
    "VoiceBridgeSession",
    "FakeTelephonyMediaAdapter",
    "FakeAIMediaAdapter",
    "InterruptibleFakeTelephonyAdapter",
    "BoundedFakeAIEchoPort",
    "InterruptibleFakeAIPort",
]
