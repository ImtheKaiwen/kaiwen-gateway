"""Synthetic WebSocket binary media protocol framing and schema definitions (v1).

Invariants:
- Versioned: explicitly scoped to `v1`.
- Raw binary media: audio frames are sent directly as WebSocket binary messages (zero JSON/Base64).
- Fixed profile: PCM16LE mono at 16,000 Hz, 20 ms per frame (exactly 640 bytes).
- Strict validation: non-640-byte binary payloads fail closed immediately.
- Control messages: JSON text frames for session and call lifecycle management.
"""

from __future__ import annotations

import re
from typing import Any, Final

from kaiwen_gateway.transports.synthetic.protocol import (
    SUPPORTED_CLIENT_EVENTS,
    SUPPORTED_SERVER_EVENTS,
    InvalidMessageFormatError,
    SyntheticProtocolError,
    parse_and_validate_client_frame,
)

__all__ = [
    "SYNTHETIC_MEDIA_WS_ENDPOINT_PATH",
    "SYNTHETIC_MEDIA_PROTOCOL_VERSION",
    "PCM16LE_SAMPLE_RATE",
    "PCM16LE_CHANNELS",
    "PCM16LE_FRAME_MS",
    "PCM16LE_FRAME_BYTES",
    "EVENT_CALL_INTERRUPT",
    "EVENT_CALL_INTERRUPTED",
    "SUPPORTED_MEDIA_CLIENT_EVENTS",
    "SUPPORTED_MEDIA_SERVER_EVENTS",
    "SyntheticMediaError",
    "InvalidAudioFrameSizeError",
    "MediaBeforeActivationError",
    "MediaAfterTerminationError",
    "validate_pcm16le_binary_frame",
    "parse_and_validate_media_control_frame",
]

SYNTHETIC_MEDIA_WS_ENDPOINT_PATH: Final[str] = "/ws/synthetic-media/v1"
SYNTHETIC_MEDIA_PROTOCOL_VERSION: Final[str] = "v1"

EVENT_CALL_INTERRUPT: Final[str] = "call.interrupt"
EVENT_CALL_INTERRUPTED: Final[str] = "call.interrupted"

SUPPORTED_MEDIA_CLIENT_EVENTS: Final[frozenset[str]] = frozenset(
    SUPPORTED_CLIENT_EVENTS | {EVENT_CALL_INTERRUPT}
)
SUPPORTED_MEDIA_SERVER_EVENTS: Final[frozenset[str]] = frozenset(
    SUPPORTED_SERVER_EVENTS | {EVENT_CALL_INTERRUPTED}
)

ALLOWED_CALL_INTERRUPT_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "event",
        "call_id",
        "turn_id",
        "request_id",
    }
)
REQUEST_ID_REGEX: Final[re.Pattern[str]] = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")

# Audio format profile constants
PCM16LE_SAMPLE_RATE: Final[int] = 16_000
PCM16LE_CHANNELS: Final[int] = 1
PCM16LE_FRAME_MS: Final[int] = 20
PCM16LE_FRAME_BYTES: Final[int] = 640  # 16000 samples/sec * 2 bytes/sample * 0.020 sec


class SyntheticMediaError(SyntheticProtocolError):
    """Base exception for synthetic media protocol errors."""

    def __init__(self, message: str, *, close_code: int = 1008) -> None:
        super().__init__(message, close_code=close_code)


class InvalidAudioFrameSizeError(SyntheticMediaError):
    """Raised when an inbound binary audio frame does not match expected size."""

    def __init__(self, actual_size: int, expected_size: int = PCM16LE_FRAME_BYTES) -> None:
        super().__init__(
            f"Invalid audio frame size: expected {expected_size} bytes, got {actual_size}",
            close_code=1003,
        )
        self.actual_size = actual_size
        self.expected_size = expected_size


class MediaBeforeActivationError(SyntheticMediaError):
    """Raised when binary audio frames are received before call.start."""

    def __init__(self) -> None:
        super().__init__("Binary media received before call.start", close_code=1008)


class MediaAfterTerminationError(SyntheticMediaError):
    """Raised when binary audio frames are received after call.end."""

    def __init__(self) -> None:
        super().__init__("Binary media received after call.end", close_code=1008)


def validate_pcm16le_binary_frame(payload: bytes) -> None:
    """Validate that an inbound binary payload conforms to the synthetic PCM16LE profile.

    Raises:
        InvalidAudioFrameSizeError: If byte length is not exactly PCM16LE_FRAME_BYTES (640 bytes).
        TypeError: If payload is not bytes.
    """
    if not isinstance(payload, bytes):
        raise TypeError(f"Audio frame payload must be bytes, got {type(payload).__name__}")
    if len(payload) != PCM16LE_FRAME_BYTES:
        raise InvalidAudioFrameSizeError(len(payload), PCM16LE_FRAME_BYTES)


def parse_and_validate_media_control_frame(raw_text: str) -> dict[str, Any]:
    """Parse and validate an inbound client control frame for the media endpoint.

    Extends standard synthetic client frame validation with media-specific event
    support, notably `call.interrupt`.

    Raises:
        OversizedMessageError: If frame byte length exceeds 64 KiB.
        InvalidJsonError: If frame cannot be parsed as valid JSON.
        InvalidMessageFormatError: If frame schema violates protocol rules or contains unknown fields.
        UnsupportedEventError: If event name is not recognized in synthetic media protocol.
    """
    payload = parse_and_validate_client_frame(
        raw_text,
        allowed_events=SUPPORTED_MEDIA_CLIENT_EVENTS,
    )

    event = payload["event"]
    if event == EVENT_CALL_INTERRUPT:
        unknown_fields = set(payload.keys()) - ALLOWED_CALL_INTERRUPT_FIELDS
        if unknown_fields:
            unknown_name = sorted(unknown_fields)[0]
            raise InvalidMessageFormatError(
                f"Unknown field in {EVENT_CALL_INTERRUPT}: {unknown_name!r}"
            )

        if "call_id" not in payload:
            raise InvalidMessageFormatError(f"Missing required 'call_id' in {EVENT_CALL_INTERRUPT}")
        call_id = payload["call_id"]
        if not isinstance(call_id, str) or not call_id or len(call_id) > 128:
            raise InvalidMessageFormatError(
                "'call_id' must be a non-empty string <= 128 characters"
            )

        if "turn_id" not in payload:
            raise InvalidMessageFormatError(f"Missing required 'turn_id' in {EVENT_CALL_INTERRUPT}")
        turn_id = payload["turn_id"]
        if isinstance(turn_id, bool) or not isinstance(turn_id, int) or turn_id < 1:
            raise InvalidMessageFormatError("'turn_id' must be a positive integer >= 1")

        if "request_id" in payload:
            request_id = payload["request_id"]
            if (
                isinstance(request_id, bool)
                or not isinstance(request_id, str)
                or not request_id
                or len(request_id) > 64
                or not REQUEST_ID_REGEX.match(request_id)
            ):
                raise InvalidMessageFormatError(
                    "'request_id' must be a string <= 64 characters matching [a-zA-Z0-9_-]"
                )

    return payload
