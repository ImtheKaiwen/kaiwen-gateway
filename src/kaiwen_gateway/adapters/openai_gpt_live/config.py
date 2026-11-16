"""Configuration and sanitized error types for the OpenAI GPT-Live adapter."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Final

from kaiwen_gateway.adapters.openai_realtime.config import (
    OPENAI_PCM_SAMPLE_RATE,
    _read_dotenv_var,
    sanitize_provider_message,
    validate_endpoint_url,
)
from kaiwen_gateway.modules.voice.domain import MediaPortError

DEFAULT_OPENAI_GPT_LIVE_URL: Final[str] = "wss://api.openai.com/v1/live/sessions"
DEFAULT_OPENAI_GPT_LIVE_MODEL: Final[str] = "gpt-live-1"
DEFAULT_OPENAI_GPT_LIVE_VOICE: Final[str] = "marin"
GPT_LIVE_PCM_SAMPLE_RATE: Final[int] = OPENAI_PCM_SAMPLE_RATE


class OpenAIGPTLiveError(MediaPortError):
    """Sanitized error raised for OpenAI GPT-Live adapter or protocol failures."""


class OpenAIGPTLiveDependencyError(OpenAIGPTLiveError):
    """Raised when the optional ``websockets`` dependency is not installed."""


def _default_delegation() -> dict[str, Any]:
    return {"type": "client"}


@dataclass(frozen=True, slots=True)
class OpenAIGPTLiveConfig:
    """Configuration for a per-call OpenAI GPT-Live WebSocket adapter.

    Targets the official ``wss://api.openai.com/v1/live/sessions`` protocol.
    Credentials are server-owned and never accepted from client WebSocket payloads.
    """

    api_key: str | None = field(default=None, repr=False)
    model: str = DEFAULT_OPENAI_GPT_LIVE_MODEL
    voice: str = DEFAULT_OPENAI_GPT_LIVE_VOICE
    instructions: str | None = None
    endpoint_url: str = DEFAULT_OPENAI_GPT_LIVE_URL
    connect_timeout_seconds: float = 10.0
    session_start_timeout_seconds: float = 8.0
    close_timeout_seconds: float = 2.0
    cancel_timeout_seconds: float = 2.0
    inbound_queue_capacity: int = 32
    outbound_queue_capacity: int = 32
    backpressure_timeout_seconds: float = 0.05
    delegation: dict[str, Any] | None = field(default_factory=_default_delegation)

    def __post_init__(self) -> None:
        if not self.model or not self.model.strip():
            raise ValueError("model must be a non-empty string")
        if not self.voice or not self.voice.strip():
            raise ValueError("voice must be a non-empty string")
        validate_endpoint_url(self.endpoint_url)
        if self.connect_timeout_seconds <= 0:
            raise ValueError("connect_timeout_seconds must be > 0")
        if self.session_start_timeout_seconds <= 0:
            raise ValueError("session_start_timeout_seconds must be > 0")
        if self.close_timeout_seconds <= 0:
            raise ValueError("close_timeout_seconds must be > 0")
        if self.cancel_timeout_seconds <= 0:
            raise ValueError("cancel_timeout_seconds must be > 0")
        if self.inbound_queue_capacity <= 0:
            raise ValueError("inbound_queue_capacity must be > 0")
        if self.outbound_queue_capacity <= 0:
            raise ValueError("outbound_queue_capacity must be > 0")
        if self.backpressure_timeout_seconds <= 0:
            raise ValueError("backpressure_timeout_seconds must be > 0")

    @classmethod
    def from_env(
        cls,
        *,
        env_var: str = "OPENAI_API_KEY",
        dotenv_path: str | os.PathLike[str] | None = ".env",
        model: str = DEFAULT_OPENAI_GPT_LIVE_MODEL,
        voice: str = DEFAULT_OPENAI_GPT_LIVE_VOICE,
        instructions: str | None = None,
        endpoint_url: str = DEFAULT_OPENAI_GPT_LIVE_URL,
        connect_timeout_seconds: float = 10.0,
        session_start_timeout_seconds: float = 8.0,
        close_timeout_seconds: float = 2.0,
        cancel_timeout_seconds: float = 2.0,
        inbound_queue_capacity: int = 32,
        outbound_queue_capacity: int = 32,
        backpressure_timeout_seconds: float = 0.05,
    ) -> OpenAIGPTLiveConfig:
        """Build configuration using a server-side environment variable or local ``.env`` file."""
        api_key = os.environ.get(env_var, "").strip()
        if not api_key and dotenv_path is not None:
            api_key = _read_dotenv_var(dotenv_path, env_var)
        if not api_key:
            raise OpenAIGPTLiveError(
                f"Missing required server environment variable {env_var!r} for OpenAI GPT-Live"
            )
        return cls(
            api_key=api_key,
            model=model,
            voice=voice,
            instructions=instructions,
            endpoint_url=endpoint_url,
            connect_timeout_seconds=connect_timeout_seconds,
            session_start_timeout_seconds=session_start_timeout_seconds,
            close_timeout_seconds=close_timeout_seconds,
            cancel_timeout_seconds=cancel_timeout_seconds,
            inbound_queue_capacity=inbound_queue_capacity,
            outbound_queue_capacity=outbound_queue_capacity,
            backpressure_timeout_seconds=backpressure_timeout_seconds,
        )

    def build_websocket_url(self) -> str:
        """Return the GPT-Live WebSocket endpoint URL (no model query parameter)."""
        return self.endpoint_url

    def build_session_start_event(self, *, event_id: str = "evt_start_001") -> dict[str, Any]:
        """Build the initial ``session.start`` client event for 24 kHz PCM16 audio."""
        session: dict[str, Any] = {
            "model": self.model,
            "audio": {
                "format": {
                    "type": "audio/pcm",
                    "rate": GPT_LIVE_PCM_SAMPLE_RATE,
                },
                "output": {
                    "voice": self.voice,
                },
            },
        }
        if self.instructions is not None:
            session["instructions"] = self.instructions
        if self.delegation is not None:
            session["delegation"] = dict(self.delegation)

        return {
            "type": "session.start",
            "event_id": event_id,
            "session": session,
        }


__all__ = [
    "DEFAULT_OPENAI_GPT_LIVE_MODEL",
    "DEFAULT_OPENAI_GPT_LIVE_URL",
    "DEFAULT_OPENAI_GPT_LIVE_VOICE",
    "GPT_LIVE_PCM_SAMPLE_RATE",
    "OpenAIGPTLiveConfig",
    "OpenAIGPTLiveDependencyError",
    "OpenAIGPTLiveError",
    "sanitize_provider_message",
    "validate_endpoint_url",
]
