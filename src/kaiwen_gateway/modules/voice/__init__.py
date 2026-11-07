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
    BoundedInterruptibleFakeAIEchoPort,
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
from kaiwen_gateway.modules.voice.telemetry import (
    DEFAULT_LATENCY_WINDOW_SIZE,
    DEFAULT_MAX_RETAINED_SESSIONS,
    SMALL_SAMPLE_THRESHOLD,
    BoundedLatencyAggregator,
    InMemoryVoiceTelemetrySink,
    LatencyDistributionSummary,
    MetricAvailabilityReason,
    VoiceAggregateMetricsSnapshot,
    VoiceErrorCategory,
    VoiceSessionMetricsSnapshot,
    VoiceTelemetrySink,
    VoiceTerminalCleanupState,
    classify_voice_error,
)

__all__ = [
    "DEFAULT_MAX_PAYLOAD_SIZE",
    "DEFAULT_LATENCY_WINDOW_SIZE",
    "DEFAULT_MAX_RETAINED_SESSIONS",
    "SMALL_SAMPLE_THRESHOLD",
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
    "VoiceErrorCategory",
    "VoiceTerminalCleanupState",
    "MetricAvailabilityReason",
    "LatencyDistributionSummary",
    "BoundedLatencyAggregator",
    "VoiceSessionMetricsSnapshot",
    "VoiceAggregateMetricsSnapshot",
    "VoiceTelemetrySink",
    "InMemoryVoiceTelemetrySink",
    "classify_voice_error",
    "FakeTelephonyMediaAdapter",
    "FakeAIMediaAdapter",
    "InterruptibleFakeTelephonyAdapter",
    "BoundedFakeAIEchoPort",
    "BoundedInterruptibleFakeAIEchoPort",
    "InterruptibleFakeAIPort",
]
