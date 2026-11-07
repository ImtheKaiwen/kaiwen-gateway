"""Synthetic WebSocket transport package (M3.2A).

Provides a test-only, versioned, and authenticated WebSocket transport vertical slice:
- Endpoint: `/ws/synthetic/v1`
- Ingress mode: `CoreConfig(ingress_mode="synthetic")`
- Token verification: :class:`SyntheticTokenVerifier`
- Identity & authorization bridge: :class:`SyntheticAuthBridge`
"""

from __future__ import annotations

from kaiwen_gateway.transports.synthetic.adapter import (
    SUPPORTED_TELEPHONY_CAPABILITIES,
    UNSUPPORTED_TELEPHONY_CAPABILITIES,
    SyntheticTelephonyAdapter,
)
from kaiwen_gateway.transports.synthetic.auth import (
    SyntheticAuthBridge,
    SyntheticCallAuthorizer,
    SyntheticProviderAuthenticator,
)
from kaiwen_gateway.transports.synthetic.endpoint import synthetic_websocket_endpoint
from kaiwen_gateway.transports.synthetic.media_adapter import (
    WebSocketTelephonyMediaAdapter,
)
from kaiwen_gateway.transports.synthetic.media_endpoint import (
    synthetic_media_websocket_endpoint,
)
from kaiwen_gateway.transports.synthetic.media_protocol import (
    EVENT_CALL_INTERRUPT,
    EVENT_CALL_INTERRUPTED,
    PCM16LE_CHANNELS,
    PCM16LE_FRAME_BYTES,
    PCM16LE_FRAME_MS,
    PCM16LE_SAMPLE_RATE,
    SUPPORTED_MEDIA_CLIENT_EVENTS,
    SUPPORTED_MEDIA_SERVER_EVENTS,
    SYNTHETIC_MEDIA_PROTOCOL_VERSION,
    SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
    InvalidAudioFrameSizeError,
    MediaAfterTerminationError,
    MediaBeforeActivationError,
    SyntheticMediaError,
    parse_and_validate_media_control_frame,
    validate_pcm16le_binary_frame,
)
from kaiwen_gateway.transports.synthetic.protocol import (
    EVENT_CALL_END,
    EVENT_CALL_ENDED,
    EVENT_CALL_START,
    EVENT_CALL_STARTED,
    EVENT_SESSION_END,
    EVENT_SESSION_PING,
    EVENT_SESSION_PONG,
    EVENT_SESSION_READY,
    MAX_FRAME_SIZE_BYTES,
    SUPPORTED_CLIENT_EVENTS,
    SUPPORTED_SERVER_EVENTS,
    SYNTHETIC_PROTOCOL_VERSION,
    SYNTHETIC_WS_ENDPOINT_PATH,
    InvalidJsonError,
    InvalidMessageFormatError,
    OversizedMessageError,
    SyntheticProtocolError,
    UnsupportedEventError,
    parse_and_validate_client_frame,
)
from kaiwen_gateway.transports.synthetic.tokens import (
    SyntheticTokenClaims,
    SyntheticTokenVerifier,
)

__all__ = [
    "EVENT_CALL_END",
    "EVENT_CALL_ENDED",
    "EVENT_CALL_INTERRUPT",
    "EVENT_CALL_INTERRUPTED",
    "EVENT_CALL_START",
    "EVENT_CALL_STARTED",
    "EVENT_SESSION_END",
    "EVENT_SESSION_PING",
    "EVENT_SESSION_PONG",
    "EVENT_SESSION_READY",
    "InvalidAudioFrameSizeError",
    "InvalidJsonError",
    "InvalidMessageFormatError",
    "MAX_FRAME_SIZE_BYTES",
    "MediaAfterTerminationError",
    "MediaBeforeActivationError",
    "OversizedMessageError",
    "PCM16LE_CHANNELS",
    "PCM16LE_FRAME_BYTES",
    "PCM16LE_FRAME_MS",
    "PCM16LE_SAMPLE_RATE",
    "SUPPORTED_CLIENT_EVENTS",
    "SUPPORTED_MEDIA_CLIENT_EVENTS",
    "SUPPORTED_MEDIA_SERVER_EVENTS",
    "SUPPORTED_SERVER_EVENTS",
    "SUPPORTED_TELEPHONY_CAPABILITIES",
    "SYNTHETIC_MEDIA_PROTOCOL_VERSION",
    "SYNTHETIC_MEDIA_WS_ENDPOINT_PATH",
    "SYNTHETIC_PROTOCOL_VERSION",
    "SYNTHETIC_WS_ENDPOINT_PATH",
    "SyntheticAuthBridge",
    "SyntheticCallAuthorizer",
    "SyntheticMediaError",
    "SyntheticProtocolError",
    "SyntheticProviderAuthenticator",
    "SyntheticTelephonyAdapter",
    "SyntheticTokenClaims",
    "SyntheticTokenVerifier",
    "UNSUPPORTED_TELEPHONY_CAPABILITIES",
    "UnsupportedEventError",
    "WebSocketTelephonyMediaAdapter",
    "parse_and_validate_client_frame",
    "parse_and_validate_media_control_frame",
    "synthetic_media_websocket_endpoint",
    "synthetic_websocket_endpoint",
    "validate_pcm16le_binary_frame",
]
