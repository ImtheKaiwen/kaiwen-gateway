"""PCM16LE audio validation, resampling (16 kHz <-> 24 kHz), and frame packing.

Gateway canonical audio:
- Format: PCM16LE (signed 16-bit little-endian linear PCM)
- Sample rate: 16,000 Hz
- Channels: 1 (mono)
- Frame duration: 20 ms (320 samples = 640 bytes)

OpenAI Realtime ``audio/pcm`` format:
- Format: PCM16LE (signed 16-bit little-endian linear PCM)
- Sample rate: 24,000 Hz
- Channels: 1 (mono)
"""

from __future__ import annotations

import array
import sys
from typing import Final

from kaiwen_gateway.modules.voice.domain import (
    DEFAULT_MAX_PAYLOAD_SIZE,
    AudioFormat,
    AudioFrame,
    InvalidAudioFrameError,
)

GATEWAY_SAMPLE_RATE: Final[int] = 16_000
OPENAI_SAMPLE_RATE: Final[int] = 24_000
GATEWAY_CHANNELS: Final[int] = 1
GATEWAY_FRAME_DURATION_MS: Final[float] = 20.0
GATEWAY_FRAME_SAMPLES: Final[int] = 320
GATEWAY_FRAME_BYTES: Final[int] = 640


def _clamp_pcm16(value: float) -> int:
    iv = int(round(value))
    if iv > 32767:
        return 32767
    if iv < -32768:
        return -32768
    return iv


def _bytes_to_int16_array(pcm_bytes: bytes) -> array.array[int]:
    samples = array.array("h")
    samples.frombytes(pcm_bytes)
    if sys.byteorder != "little":
        samples.byteswap()
    return samples


def _int16_array_to_bytes(samples: array.array[int]) -> bytes:
    if sys.byteorder != "little":
        copied = array.array("h", samples)
        copied.byteswap()
        return copied.tobytes()
    return samples.tobytes()


def validate_inbound_frame(frame: AudioFrame) -> None:
    """Validate that an inbound Gateway frame conforms to canonical 16 kHz mono PCM16LE."""
    if not isinstance(frame, AudioFrame):
        raise InvalidAudioFrameError(f"Expected AudioFrame, got {type(frame).__name__}")
    if frame.format != AudioFormat.PCM16LE:
        raise InvalidAudioFrameError(
            f"Unsupported audio format {frame.format!r}; expected {AudioFormat.PCM16LE!r}"
        )
    if frame.sample_rate != GATEWAY_SAMPLE_RATE:
        raise InvalidAudioFrameError(
            f"Unsupported sample_rate {frame.sample_rate}; expected {GATEWAY_SAMPLE_RATE} Hz"
        )
    if frame.channels != GATEWAY_CHANNELS:
        raise InvalidAudioFrameError(
            f"Unsupported channels {frame.channels}; expected {GATEWAY_CHANNELS} (mono)"
        )
    if len(frame.payload) > DEFAULT_MAX_PAYLOAD_SIZE:
        raise InvalidAudioFrameError(
            f"Payload size {len(frame.payload)} exceeds maximum {DEFAULT_MAX_PAYLOAD_SIZE}"
        )


class PCM16Resampler16kTo24k:
    """Stateful 16 kHz -> 24 kHz (2:3) linear-interpolation PCM16LE resampler.

    Maintains the previous tail sample across calls so 20 ms frame boundaries
    (320 samples @ 16 kHz -> 480 samples @ 24 kHz) remain phase-continuous.
    """

    def __init__(self) -> None:
        self._last_sample: int | None = None
        self._carry_byte: bytes = b""

    def reset(self) -> None:
        """Reset boundary state."""
        self._last_sample = None
        self._carry_byte = b""

    def resample(self, pcm16_16k: bytes) -> bytes:
        """Resample 16 kHz little-endian PCM16 mono bytes to 24 kHz."""
        if not pcm16_16k:
            return b""
        data = self._carry_byte + pcm16_16k
        if len(data) % 2 != 0:
            self._carry_byte = data[-1:]
            data = data[:-1]
        else:
            self._carry_byte = b""
        if not data:
            return b""

        src = _bytes_to_int16_array(data)
        n = len(src)
        out = array.array("h")
        prev = src[0] if self._last_sample is None else self._last_sample

        # Output sample spacing is 2/3 of an input sample period.
        # Pair every 2 input samples (s0, s1) -> 3 output samples at offsets 0, 2/3, 4/3:
        #   y0 = (2/3)*prev + (1/3)*s0? Wait: using standard t = k * (2/3) over [prev, src...]:
        # Let index j = 0 .. (3*n // 2) - 1, pos = j * (2/3) relative to src[0]:
        # To map exact 2 samples -> 3 samples cleanly:
        # For odd n, handle trailing sample with prev -> src[-1] interpolation.
        out_len = (n * 3) // 2
        for j in range(out_len):
            pos = (j * 2) / 3.0
            idx = int(pos)
            frac = pos - idx
            s_left = (
                prev if idx == 0 and frac == 0.0 and self._last_sample is not None else src[idx]
            )
            if idx == 0 and self._last_sample is not None and j == 0:
                s_left = src[0]
            s_right = src[idx + 1] if (idx + 1) < n else src[idx]
            val = s_left + frac * (s_right - s_left)
            out.append(_clamp_pcm16(val))

        self._last_sample = src[-1]
        return _int16_array_to_bytes(out)


class PCM16Resampler24kTo16k:
    """Stateful 24 kHz -> 16 kHz (3:2) linear-interpolation PCM16LE resampler.

    Handles arbitrary upstream chunk byte lengths (including odd byte boundaries)
    and maintains fractional sample phase across chunks.
    """

    def __init__(self) -> None:
        self._carry_byte: bytes = b""
        self._pos: float = 0.0
        self._prev_sample: int | None = None

    def reset(self) -> None:
        """Reset carry byte and phase state."""
        self._carry_byte = b""
        self._pos = 0.0
        self._prev_sample = None

    def resample(self, pcm16_24k: bytes) -> bytes:
        """Resample 24 kHz little-endian PCM16 mono bytes to 16 kHz."""
        if not pcm16_24k:
            return b""

        data = self._carry_byte + pcm16_24k
        if len(data) % 2 != 0:
            self._carry_byte = data[-1:]
            data = data[:-1]
        else:
            self._carry_byte = b""

        if not data:
            return b""

        src = _bytes_to_int16_array(data)
        n = len(src)
        out = array.array("h")
        pos = self._pos

        while pos < n - 1:
            if pos < 0.0:
                # Interpolate between previous chunk's last sample and src[0]
                left = self._prev_sample if self._prev_sample is not None else src[0]
                frac = pos + 1.0
                val = left + frac * (src[0] - left)
            else:
                idx = int(pos)
                frac = pos - idx
                s0 = src[idx]
                s1 = src[idx + 1]
                val = s0 + frac * (s1 - s0)
            out.append(_clamp_pcm16(val))
            pos += 1.5

        self._prev_sample = src[-1]
        self._pos = pos - n
        return _int16_array_to_bytes(out)


class OutboundFramePacker:
    """Packs resampled 16 kHz PCM16LE bytes into canonical 640-byte ``AudioFrame``s."""

    def __init__(self) -> None:
        self._buffer = bytearray()
        self._sequence = 0

    def reset(self) -> None:
        """Discard any partial buffered bytes without resetting monotonic sequence."""
        self._buffer.clear()

    def push_bytes(self, pcm16_16k: bytes, *, turn_id: int | None) -> list[AudioFrame]:
        """Append 16 kHz PCM16LE bytes and emit complete 640-byte ``AudioFrame``s."""
        if not pcm16_16k:
            return []
        self._buffer.extend(pcm16_16k)
        frames: list[AudioFrame] = []
        offset = 0
        total = len(self._buffer)
        while total - offset >= GATEWAY_FRAME_BYTES:
            chunk = bytes(self._buffer[offset : offset + GATEWAY_FRAME_BYTES])
            seq = self._sequence
            self._sequence += 1
            frames.append(
                AudioFrame(
                    payload=chunk,
                    format=AudioFormat.PCM16LE,
                    sample_rate=GATEWAY_SAMPLE_RATE,
                    channels=GATEWAY_CHANNELS,
                    sequence=seq,
                    timestamp_ms=seq * GATEWAY_FRAME_DURATION_MS,
                    turn_id=turn_id,
                )
            )
            offset += GATEWAY_FRAME_BYTES
        if offset > 0:
            del self._buffer[:offset]
        return frames

    def flush_partial(self, *, turn_id: int | None) -> AudioFrame | None:
        """Zero-pad and emit any trailing partial frame (<640 bytes) at clean turn end."""
        if not self._buffer:
            return None
        remainder = bytes(self._buffer)
        self._buffer.clear()
        padded = remainder + b"\x00" * (GATEWAY_FRAME_BYTES - len(remainder))
        seq = self._sequence
        self._sequence += 1
        return AudioFrame(
            payload=padded,
            format=AudioFormat.PCM16LE,
            sample_rate=GATEWAY_SAMPLE_RATE,
            channels=GATEWAY_CHANNELS,
            sequence=seq,
            timestamp_ms=seq * GATEWAY_FRAME_DURATION_MS,
            turn_id=turn_id,
        )
