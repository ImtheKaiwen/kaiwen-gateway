"""WebSocket transport abstraction and default ``websockets`` implementation."""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from typing import Any, Protocol, runtime_checkable

from kaiwen_gateway.adapters.openai_realtime.config import (
    OpenAIRealtimeConfig,
    OpenAIRealtimeDependencyError,
    OpenAIRealtimeError,
    sanitize_provider_message,
)


@runtime_checkable
class OpenAIRealtimeTransport(Protocol):
    """Minimal JSON WebSocket transport protocol for OpenAI Realtime."""

    async def send_json(self, payload: dict[str, Any]) -> None:
        """Serialize and send a JSON client event."""
        ...

    async def receive_json(self) -> dict[str, Any] | None:
        """Receive and parse the next JSON server event, or None on clean EOF."""
        ...

    async def close(self) -> None:
        """Idempotently close the underlying WebSocket connection."""
        ...


OpenAIRealtimeTransportConnector = Callable[
    [OpenAIRealtimeConfig], Awaitable[OpenAIRealtimeTransport]
]


class _WebsocketsRealtimeTransport:
    """``websockets``-backed implementation of ``OpenAIRealtimeTransport``."""

    def __init__(self, ws: Any, *, api_key: str | None) -> None:
        self._ws = ws
        self._api_key = api_key
        self._closed = False

    async def send_json(self, payload: dict[str, Any]) -> None:
        if self._closed:
            raise OpenAIRealtimeError("OpenAI Realtime WebSocket transport is closed")
        try:
            await self._ws.send(json.dumps(payload))
        except Exception as exc:
            raise OpenAIRealtimeError(
                sanitize_provider_message(
                    f"Failed sending OpenAI Realtime event: {exc}", self._api_key
                )
            ) from exc

    async def receive_json(self) -> dict[str, Any] | None:
        if self._closed:
            return None
        try:
            raw = await self._ws.recv()
        except Exception as exc:
            # websockets ConnectionClosedOK represents a clean close (1000/1001)
            exc_name = type(exc).__name__
            if exc_name == "ConnectionClosedOK":
                return None
            raise OpenAIRealtimeError(
                sanitize_provider_message(
                    f"OpenAI Realtime WebSocket receive failed: {exc}", self._api_key
                )
            ) from exc

        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        if not isinstance(raw, str):
            raise OpenAIRealtimeError("Received non-text WebSocket frame from OpenAI Realtime")
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise OpenAIRealtimeError("Received invalid JSON from OpenAI Realtime") from exc
        if not isinstance(parsed, dict):
            raise OpenAIRealtimeError("OpenAI Realtime event must be a JSON object")
        return parsed

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            await self._ws.close()
        except Exception:
            pass


async def connect_openai_realtime_websocket(
    config: OpenAIRealtimeConfig,
) -> OpenAIRealtimeTransport:
    """Connect to the OpenAI Realtime WebSocket API using ``websockets``.

    Imports ``websockets`` lazily so core-only installations remain unaffected.
    """
    if not config.api_key or not config.api_key.strip():
        raise OpenAIRealtimeError(
            "OpenAI Realtime API key is required for upstream WebSocket connections"
        )

    try:
        import websockets
    except ImportError as exc:
        raise OpenAIRealtimeDependencyError(
            "The 'websockets' package is required for OpenAI Realtime WebSocket connections. "
            "Install with: pip install kaiwen-gateway[openai]"
        ) from exc

    url = config.build_websocket_url()
    headers = {"Authorization": f"Bearer {config.api_key}"}
    try:
        ws = await websockets.connect(
            url,
            additional_headers=headers,
            open_timeout=config.connect_timeout_seconds,
            close_timeout=config.close_timeout_seconds,
            max_size=4 * 1024 * 1024,
        )
    except Exception as exc:
        raise OpenAIRealtimeError(
            sanitize_provider_message(
                f"Failed to connect to OpenAI Realtime: {exc}", config.api_key
            )
        ) from exc

    return _WebsocketsRealtimeTransport(ws, api_key=config.api_key)
