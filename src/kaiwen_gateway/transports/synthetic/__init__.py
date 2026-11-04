"""Synthetic WebSocket transport package (M3.2A).

Provides a test-only, versioned, and authenticated WebSocket transport vertical slice:
- Endpoint: `/ws/synthetic/v1`
- Ingress mode: `CoreConfig(ingress_mode="synthetic")`
- Token verification: :class:`SyntheticTokenVerifier`
- Identity & authorization bridge: :class:`SyntheticAuthBridge`
"""

from __future__ import annotations

from kaiwen_gateway.transports.synthetic.auth import (
    SyntheticAuthBridge,
    SyntheticCallAuthorizer,
    SyntheticProviderAuthenticator,
)
from kaiwen_gateway.transports.synthetic.endpoint import synthetic_websocket_endpoint
from kaiwen_gateway.transports.synthetic.protocol import (
    EVENT_SESSION_END,
    EVENT_SESSION_PING,
    EVENT_SESSION_PONG,
    EVENT_SESSION_READY,
    MAX_FRAME_SIZE_BYTES,
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
    "EVENT_SESSION_END",
    "EVENT_SESSION_PING",
    "EVENT_SESSION_PONG",
    "EVENT_SESSION_READY",
    "InvalidJsonError",
    "InvalidMessageFormatError",
    "MAX_FRAME_SIZE_BYTES",
    "OversizedMessageError",
    "SYNTHETIC_PROTOCOL_VERSION",
    "SYNTHETIC_WS_ENDPOINT_PATH",
    "SyntheticAuthBridge",
    "SyntheticCallAuthorizer",
    "SyntheticProtocolError",
    "SyntheticProviderAuthenticator",
    "SyntheticTokenClaims",
    "SyntheticTokenVerifier",
    "UnsupportedEventError",
    "parse_and_validate_client_frame",
    "synthetic_websocket_endpoint",
]
