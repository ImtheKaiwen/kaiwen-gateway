"""Unit tests for voice domain contracts, AudioFrame invariants, and error models."""

from __future__ import annotations

import math
from dataclasses import FrozenInstanceError

import pytest

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


class TestVoiceExceptions:
    def test_exception_hierarchy(self) -> None:
        assert issubclass(InvalidAudioFrameError, VoiceDomainError)
        assert issubclass(OversizedAudioFrameError, InvalidAudioFrameError)
        assert issubclass(OutOfOrderAudioFrameError, VoiceDomainError)
        assert issubclass(MediaBackpressureOverflowError, VoiceDomainError)
        assert issubclass(MediaPortError, VoiceDomainError)
        assert issubclass(MediaPortClosedError, MediaPortError)
        assert issubclass(VoiceBridgeSessionClosedError, VoiceDomainError)


class TestAudioFrame:
    def test_valid_audio_frame_pcm16le(self) -> None:
        payload = b"\x00\x01\x02\x03"
        frame = AudioFrame(
            payload=payload,
            format=AudioFormat.PCM16LE,
            sample_rate=16_000,
            channels=1,
            sequence=0,
            timestamp_ms=0.0,
        )
        assert frame.payload == payload
        assert frame.format == "pcm16le"
        assert frame.sample_rate == 16_000
        assert frame.channels == 1
        assert frame.sequence == 0
        assert frame.timestamp_ms == 0.0

    def test_valid_custom_format_string(self) -> None:
        frame = AudioFrame(
            payload=b"raw-data",
            format="custom-codec",
            sample_rate=24_000,
            channels=2,
            sequence=5,
            timestamp_ms=100.5,
        )
        assert frame.format == "custom-codec"
        assert frame.sample_rate == 24_000
        assert frame.channels == 2
        assert frame.sequence == 5
        assert frame.timestamp_ms == 100.5

    def test_frame_is_immutable(self) -> None:
        frame = create_synthetic_pcm16le_frame(b"\x00\x01", sequence=0)
        with pytest.raises(FrozenInstanceError):
            frame.payload = b"\x02\x03"  # type: ignore[misc]
        with pytest.raises(FrozenInstanceError):
            frame.sequence = 1  # type: ignore[misc]

    def test_payload_must_be_bytes(self) -> None:
        with pytest.raises(InvalidAudioFrameError, match="must be bytes"):
            AudioFrame(
                payload="not-bytes",  # type: ignore[arg-type]
                format=AudioFormat.PCM16LE,
                sample_rate=16_000,
                channels=1,
                sequence=0,
                timestamp_ms=0.0,
            )

    def test_payload_must_not_be_empty(self) -> None:
        with pytest.raises(InvalidAudioFrameError, match="must not be empty"):
            AudioFrame(
                payload=b"",
                format=AudioFormat.PCM16LE,
                sample_rate=16_000,
                channels=1,
                sequence=0,
                timestamp_ms=0.0,
            )

    def test_payload_oversized(self) -> None:
        oversized_payload = b"\x00" * (DEFAULT_MAX_PAYLOAD_SIZE + 1)
        with pytest.raises(OversizedAudioFrameError, match="exceeds maximum limit"):
            AudioFrame(
                payload=oversized_payload,
                format=AudioFormat.PCM16LE,
                sample_rate=16_000,
                channels=1,
                sequence=0,
                timestamp_ms=0.0,
            )

    def test_payload_exact_max_size_allowed(self) -> None:
        max_payload = b"\x00" * DEFAULT_MAX_PAYLOAD_SIZE
        frame = AudioFrame(
            payload=max_payload,
            format=AudioFormat.PCM16LE,
            sample_rate=16_000,
            channels=1,
            sequence=0,
            timestamp_ms=0.0,
        )
        assert len(frame.payload) == DEFAULT_MAX_PAYLOAD_SIZE

    @pytest.mark.parametrize("invalid_format", ["", "   ", None, 123, []])
    def test_invalid_format(self, invalid_format: object) -> None:
        with pytest.raises(InvalidAudioFrameError, match="format"):
            AudioFrame(
                payload=b"\x01",
                format=invalid_format,  # type: ignore[arg-type]
                sample_rate=16_000,
                channels=1,
                sequence=0,
                timestamp_ms=0.0,
            )

    @pytest.mark.parametrize(
        "invalid_rate",
        [0, -1, 100, 7999, 192001, True, False, "16000", 16000.0],
    )
    def test_invalid_sample_rate(self, invalid_rate: object) -> None:
        with pytest.raises(InvalidAudioFrameError, match="Sample rate"):
            AudioFrame(
                payload=b"\x01",
                format=AudioFormat.PCM16LE,
                sample_rate=invalid_rate,  # type: ignore[arg-type]
                channels=1,
                sequence=0,
                timestamp_ms=0.0,
            )

    @pytest.mark.parametrize("invalid_channels", [0, -1, 9, 10, True, False, "1", 1.0])
    def test_invalid_channels(self, invalid_channels: object) -> None:
        with pytest.raises(InvalidAudioFrameError, match="Channel"):
            AudioFrame(
                payload=b"\x01",
                format=AudioFormat.PCM16LE,
                sample_rate=16_000,
                channels=invalid_channels,  # type: ignore[arg-type]
                sequence=0,
                timestamp_ms=0.0,
            )

    @pytest.mark.parametrize("invalid_seq", [-1, -99, True, False, "0", 0.0])
    def test_invalid_sequence(self, invalid_seq: object) -> None:
        with pytest.raises(InvalidAudioFrameError, match="Sequence"):
            AudioFrame(
                payload=b"\x01",
                format=AudioFormat.PCM16LE,
                sample_rate=16_000,
                channels=1,
                sequence=invalid_seq,  # type: ignore[arg-type]
                timestamp_ms=0.0,
            )

    @pytest.mark.parametrize(
        "invalid_ts",
        [
            -0.001,
            -10.0,
            math.nan,
            math.inf,
            -math.inf,
            True,
            False,
            "0.0",
            None,
        ],
    )
    def test_invalid_timestamp(self, invalid_ts: object) -> None:
        with pytest.raises(InvalidAudioFrameError, match="timestamp_ms"):
            AudioFrame(
                payload=b"\x01",
                format=AudioFormat.PCM16LE,
                sample_rate=16_000,
                channels=1,
                sequence=0,
                timestamp_ms=invalid_ts,  # type: ignore[arg-type]
            )

    def test_create_synthetic_pcm16le_frame_helper(self) -> None:
        payload = b"\x00\x01\x02\x03"
        frame = create_synthetic_pcm16le_frame(
            payload,
            sequence=3,
            timestamp_ms=60.0,
            sample_rate=8000,
            channels=2,
        )
        assert frame.payload == payload
        assert frame.format == "pcm16le"
        assert frame.sequence == 3
        assert frame.timestamp_ms == 60.0
        assert frame.sample_rate == 8000
        assert frame.channels == 2
