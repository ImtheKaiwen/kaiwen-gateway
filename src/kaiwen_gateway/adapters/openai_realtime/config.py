"""Configuration and sanitized error types for the OpenAI Realtime adapter."""

from __future__ import annotations

import ipaddress
import os
import pathlib
from dataclasses import dataclass, field
from typing import Any, Final
from urllib.parse import quote, urlparse

from kaiwen_gateway.modules.voice.domain import MediaPortError

DEFAULT_OPENAI_REALTIME_URL: Final[str] = "wss://api.openai.com/v1/realtime"
DEFAULT_OPENAI_REALTIME_MODEL: Final[str] = "gpt-realtime"
DEFAULT_OPENAI_REALTIME_VOICE: Final[str] = "alloy"
OPENAI_PCM_SAMPLE_RATE: Final[int] = 24_000


class OpenAIRealtimeError(MediaPortError):
    """Sanitized error raised for OpenAI Realtime adapter or protocol failures."""


class OpenAIRealtimeDependencyError(OpenAIRealtimeError):
    """Raised when the optional ``websockets`` dependency is not installed."""


def sanitize_provider_message(message: str, api_key: str | None = None) -> str:
    """Scrub API keys or bearer tokens from provider error messages."""
    cleaned = message
    if api_key:
        cleaned = cleaned.replace(api_key, "[REDACTED]")
    return cleaned


def _read_dotenv_var(dotenv_path: str | os.PathLike[str], env_var: str) -> str:
    """Read ``env_var`` from a local ``.env`` file if present, without mutating ``os.environ``."""
    path = pathlib.Path(dotenv_path)
    if not path.is_file():
        return ""
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return ""
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, val = line.split("=", 1)
        if key.strip() != env_var:
            continue
        cleaned = val.strip()
        if len(cleaned) >= 2 and cleaned[0] == cleaned[-1] and cleaned[0] in {'"', "'"}:
            cleaned = cleaned[1:-1].strip()
        if cleaned:
            return cleaned
    return ""


def _is_loopback_host(hostname: str | None) -> bool:
    if not hostname:
        return False
    normalized = hostname.strip().lower()
    if normalized == "localhost":
        return True
    try:
        return ipaddress.ip_address(normalized).is_loopback
    except ValueError:
        return False


def validate_endpoint_url(endpoint_url: str) -> None:
    """Validate that ``endpoint_url`` uses ``wss://`` or loopback-only ``ws://``."""
    parsed = urlparse(endpoint_url)
    if parsed.scheme not in {"ws", "wss"} or not parsed.netloc or not parsed.hostname:
        raise ValueError("endpoint_url must be a valid ws:// or wss:// URL")
    if parsed.scheme == "ws" and not _is_loopback_host(parsed.hostname):
        raise ValueError(
            "Plaintext ws:// endpoint_url is only allowed for loopback hosts; "
            "use wss:// for remote endpoints"
        )


def _default_turn_detection() -> dict[str, Any]:
    return {
        "type": "server_vad",
        "threshold": 0.5,
        "prefix_padding_ms": 300,
        "silence_duration_ms": 500,
        "create_response": True,
        "interrupt_response": True,
    }


@dataclass(frozen=True, slots=True)
class OpenAIRealtimeConfig:
    """Configuration for a per-call OpenAI Realtime WebSocket adapter.

    Credentials are server-owned and never accepted from client WebSocket payloads.
    """

    api_key: str | None = field(default=None, repr=False)
    model: str = DEFAULT_OPENAI_REALTIME_MODEL
    voice: str = DEFAULT_OPENAI_REALTIME_VOICE
    instructions: str | None = None
    endpoint_url: str = DEFAULT_OPENAI_REALTIME_URL
    connect_timeout_seconds: float = 10.0
    close_timeout_seconds: float = 2.0
    cancel_timeout_seconds: float = 2.0
    inbound_queue_capacity: int = 32
    outbound_queue_capacity: int = 32
    backpressure_timeout_seconds: float = 0.05
    turn_detection: dict[str, Any] | None = field(default_factory=_default_turn_detection)

    def __post_init__(self) -> None:
        if not self.model or not self.model.strip():
            raise ValueError("model must be a non-empty string")
        if not self.voice or not self.voice.strip():
            raise ValueError("voice must be a non-empty string")
        validate_endpoint_url(self.endpoint_url)
        if self.connect_timeout_seconds <= 0:
            raise ValueError("connect_timeout_seconds must be > 0")
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
        model: str = DEFAULT_OPENAI_REALTIME_MODEL,
        voice: str = DEFAULT_OPENAI_REALTIME_VOICE,
        instructions: str | None = None,
        endpoint_url: str = DEFAULT_OPENAI_REALTIME_URL,
        connect_timeout_seconds: float = 10.0,
        close_timeout_seconds: float = 2.0,
        cancel_timeout_seconds: float = 2.0,
        inbound_queue_capacity: int = 32,
        outbound_queue_capacity: int = 32,
        backpressure_timeout_seconds: float = 0.05,
    ) -> OpenAIRealtimeConfig:
        """Build configuration using a server-side environment variable or local ``.env`` file."""
        api_key = os.environ.get(env_var, "").strip()
        if not api_key and dotenv_path is not None:
            api_key = _read_dotenv_var(dotenv_path, env_var)
        if not api_key:
            raise OpenAIRealtimeError(
                f"Missing required server environment variable {env_var!r} for OpenAI Realtime"
            )
        return cls(
            api_key=api_key,
            model=model,
            voice=voice,
            instructions=instructions,
            endpoint_url=endpoint_url,
            connect_timeout_seconds=connect_timeout_seconds,
            close_timeout_seconds=close_timeout_seconds,
            cancel_timeout_seconds=cancel_timeout_seconds,
            inbound_queue_capacity=inbound_queue_capacity,
            outbound_queue_capacity=outbound_queue_capacity,
            backpressure_timeout_seconds=backpressure_timeout_seconds,
        )

    def build_websocket_url(self) -> str:
        """Return the WebSocket URL with the configured ``model`` query parameter."""
        separator = "&" if "?" in self.endpoint_url else "?"
        return f"{self.endpoint_url}{separator}model={quote(self.model, safe='')}"

    def build_session_update_event(self) -> dict[str, Any]:
        """Build the GA ``session.update`` client event for 24 kHz PCM16 audio."""
        input_cfg: dict[str, Any] = {
            "format": {
                "type": "audio/pcm",
                "rate": OPENAI_PCM_SAMPLE_RATE,
            }
        }
        if self.turn_detection is not None:
            input_cfg["turn_detection"] = dict(self.turn_detection)

        session: dict[str, Any] = {
            "type": "realtime",
            "output_modalities": ["audio"],
            "audio": {
                "input": input_cfg,
                "output": {
                    "format": {
                        "type": "audio/pcm",
                        "rate": OPENAI_PCM_SAMPLE_RATE,
                    },
                    "voice": self.voice,
                },
            },
        }
        if self.instructions is not None:
            session["instructions"] = self.instructions

        return {
            "type": "session.update",
            "session": session,
        }
