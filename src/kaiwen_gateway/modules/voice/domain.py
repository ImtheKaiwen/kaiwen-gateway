"""Voice-local domain types, invariants, and audio frame contracts.

Invariants:
- Immutable media frames: AudioFrame is a frozen dataclass holding immutable bytes.
- Frame-size validation: non-empty payload and bounded maximum payload size.
- Strictly validated format, sample rate, channels, sequence, and timing metadata.
- Finite timing: timestamp_ms must be finite and non-negative.
- Zero codec logic: domain models represent raw frame envelopes, not transcoding logic.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import StrEnum
from typing import Final

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
    "create_synthetic_pcm16le_frame",
]

DEFAULT_MAX_PAYLOAD_SIZE: Final[int] = 65_536  # 64 KiB per audio frame


class AudioFormat(StrEnum):
    """Known audio format identifiers for media frames."""

    PCM16LE = "pcm16le"


class VoiceDomainError(Exception):
    """Base exception for voice domain errors."""


class InvalidAudioFrameError(VoiceDomainError):
    """Raised when an audio frame violates invariant specifications."""


class OversizedAudioFrameError(InvalidAudioFrameError):
    """Raised when an audio frame payload exceeds the maximum allowed size."""


class OutOfOrderAudioFrameError(VoiceDomainError):
    """Raised when an audio frame sequence number violates ordering requirements."""


class MediaBackpressureOverflowError(VoiceDomainError):
    """Raised when a bounded media buffer overflows or backpressure wait times out."""


class MediaPortError(VoiceDomainError):
    """Base exception for media port errors."""


class MediaPortClosedError(MediaPortError):
    """Raised when an I/O operation is attempted on a closed media port."""


class VoiceBridgeSessionClosedError(VoiceDomainError):
    """Raised when an operation is attempted on a closed voice bridge session."""


class UnsupportedInterruptionCapabilityError(VoiceDomainError):
    """Raised when an unsupported interruption or playback control capability is requested."""


@dataclass(frozen=True, slots=True)
class AudioFrame:
    """Immutable provider-neutral audio frame envelope.

    Attributes:
        payload: Immutable bytes representing the raw audio frame data.
        format: Audio format identifier (e.g. 'pcm16le').
        sample_rate: Sample rate in Hertz (e.g. 16000).
        channels: Channel count (1 for mono, 2 for stereo).
        sequence: Monotonically increasing sequence number (>= 0).
        timestamp_ms: Monotonic or relative timing offset in milliseconds (finite, >= 0.0).
        turn_id: Optional monotonically increasing response generation / turn identifier.
    """

    payload: bytes
    format: str | AudioFormat
    sample_rate: int
    channels: int
    sequence: int
    timestamp_ms: float
    turn_id: int | None = None

    def __post_init__(self) -> None:
        # 1. Payload validation
        if not isinstance(self.payload, bytes):
            raise InvalidAudioFrameError(
                f"Audio frame payload must be bytes, got {type(self.payload).__name__}"
            )
        if len(self.payload) == 0:
            raise InvalidAudioFrameError("Audio frame payload must not be empty")
        if len(self.payload) > DEFAULT_MAX_PAYLOAD_SIZE:
            raise OversizedAudioFrameError(
                f"Audio frame payload size {len(self.payload)} exceeds maximum limit "
                f"of {DEFAULT_MAX_PAYLOAD_SIZE} bytes"
            )

        # 2. Format validation
        if isinstance(self.format, AudioFormat):
            format_str = self.format.value
        elif isinstance(self.format, str):
            format_str = self.format.strip()
        else:
            raise InvalidAudioFrameError(
                f"Audio frame format must be str or AudioFormat, got {type(self.format).__name__}"
            )
        if not format_str:
            raise InvalidAudioFrameError("Audio frame format must not be empty")
        object.__setattr__(self, "format", format_str)

        # 3. Sample rate validation
        if not isinstance(self.sample_rate, int) or isinstance(self.sample_rate, bool):
            raise InvalidAudioFrameError(
                f"Sample rate must be int, got {type(self.sample_rate).__name__}"
            )
        if self.sample_rate < 8_000 or self.sample_rate > 192_000:
            raise InvalidAudioFrameError(
                f"Sample rate must be between 8000 and 192000 Hz, got {self.sample_rate}"
            )

        # 4. Channels validation
        if not isinstance(self.channels, int) or isinstance(self.channels, bool):
            raise InvalidAudioFrameError(
                f"Channels must be int, got {type(self.channels).__name__}"
            )
        if self.channels < 1 or self.channels > 8:
            raise InvalidAudioFrameError(
                f"Channel count must be between 1 and 8, got {self.channels}"
            )

        # 5. Sequence validation
        if not isinstance(self.sequence, int) or isinstance(self.sequence, bool):
            raise InvalidAudioFrameError(
                f"Sequence must be int, got {type(self.sequence).__name__}"
            )
        if self.sequence < 0:
            raise InvalidAudioFrameError(
                f"Sequence number must be non-negative, got {self.sequence}"
            )

        # 6. Timing validation
        if not isinstance(self.timestamp_ms, (int, float)) or isinstance(self.timestamp_ms, bool):
            raise InvalidAudioFrameError(
                f"timestamp_ms must be a float, got {type(self.timestamp_ms).__name__}"
            )
        ts_float = float(self.timestamp_ms)
        if not math.isfinite(ts_float):
            raise InvalidAudioFrameError(f"timestamp_ms must be finite, got {self.timestamp_ms}")
        if ts_float < 0.0:
            raise InvalidAudioFrameError(f"timestamp_ms must be non-negative, got {ts_float}")
        object.__setattr__(self, "timestamp_ms", ts_float)

        # 7. Turn identity validation (optional)
        if self.turn_id is not None:
            if not isinstance(self.turn_id, int) or isinstance(self.turn_id, bool):
                raise InvalidAudioFrameError(
                    f"turn_id must be int or None, got {type(self.turn_id).__name__}"
                )
            if self.turn_id < 0:
                raise InvalidAudioFrameError(f"turn_id must be non-negative, got {self.turn_id}")


def create_synthetic_pcm16le_frame(
    payload: bytes,
    *,
    sequence: int,
    timestamp_ms: float = 0.0,
    sample_rate: int = 16_000,
    channels: int = 1,
    turn_id: int | None = None,
) -> AudioFrame:
    """Create a verified synthetic PCM16LE audio frame for testing and bridge demonstration."""
    return AudioFrame(
        payload=payload,
        format=AudioFormat.PCM16LE,
        sample_rate=sample_rate,
        channels=channels,
        sequence=sequence,
        timestamp_ms=timestamp_ms,
        turn_id=turn_id,
    )
