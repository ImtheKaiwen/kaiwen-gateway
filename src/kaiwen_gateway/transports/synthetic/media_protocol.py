"""Synthetic WebSocket binary media protocol framing and schema definitions (v1).

Invariants:
- Versioned: explicitly scoped to `v1`.
- Raw binary media: audio frames are sent directly as WebSocket binary messages (zero JSON/Base64).
- Fixed profile: PCM16LE mono at 16,000 Hz, 20 ms per frame (exactly 640 bytes).
- Strict validation: non-640-byte binary payloads fail closed immediately.
- Control messages: JSON text frames for session and call lifecycle management.
"""

from __future__ import annotations

from typing import Final

from kaiwen_gateway.transports.synthetic.protocol import SyntheticProtocolError

__all__ = [
    "SYNTHETIC_MEDIA_WS_ENDPOINT_PATH",
    "SYNTHETIC_MEDIA_PROTOCOL_VERSION",
    "PCM16LE_SAMPLE_RATE",
    "PCM16LE_CHANNELS",
    "PCM16LE_FRAME_MS",
    "PCM16LE_FRAME_BYTES",
    "SyntheticMediaError",
    "InvalidAudioFrameSizeError",
    "MediaBeforeActivationError",
    "MediaAfterTerminationError",
    "validate_pcm16le_binary_frame",
]

SYNTHETIC_MEDIA_WS_ENDPOINT_PATH: Final[str] = "/ws/synthetic-media/v1"
SYNTHETIC_MEDIA_PROTOCOL_VERSION: Final[str] = "v1"

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
