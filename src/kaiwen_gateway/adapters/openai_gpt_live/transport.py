"""WebSocket transport abstraction and default ``websockets`` implementation for GPT-Live."""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from typing import Any, Protocol, runtime_checkable

from kaiwen_gateway.adapters.openai_gpt_live.config import (
    OpenAIGPTLiveConfig,
    OpenAIGPTLiveDependencyError,
    OpenAIGPTLiveError,
    sanitize_provider_message,
    validate_endpoint_url,
)


@runtime_checkable
class OpenAIGPTLiveTransport(Protocol):
    """Minimal JSON WebSocket transport protocol for OpenAI GPT-Live."""

    async def send_json(self, payload: dict[str, Any]) -> None:
        """Serialize and send a JSON client event."""
        ...

    async def receive_json(self) -> dict[str, Any] | None:
        """Receive and parse the next JSON server event, or None on clean EOF."""
        ...

    async def close(self) -> None:
        """Idempotently close the underlying WebSocket connection."""
        ...


OpenAIGPTLiveTransportConnector = Callable[[OpenAIGPTLiveConfig], Awaitable[OpenAIGPTLiveTransport]]


class _WebsocketsGPTLiveTransport:
    """``websockets``-backed implementation of ``OpenAIGPTLiveTransport``."""

    def __init__(self, ws: Any, *, api_key: str | None) -> None:
        self._ws = ws
        self._api_key = api_key
        self._closed = False

    async def send_json(self, payload: dict[str, Any]) -> None:
        if self._closed:
            raise OpenAIGPTLiveError("OpenAI GPT-Live WebSocket transport is closed")
        try:
            await self._ws.send(json.dumps(payload))
        except Exception as exc:
            raise OpenAIGPTLiveError(
                sanitize_provider_message(
                    f"Failed sending OpenAI GPT-Live event: {exc}", self._api_key
                )
            ) from None

    async def receive_json(self) -> dict[str, Any] | None:
        if self._closed:
            return None
        try:
            raw = await self._ws.recv()
        except Exception as exc:
            exc_name = type(exc).__name__
            if exc_name == "ConnectionClosedOK":
                return None
            raise OpenAIGPTLiveError(
                sanitize_provider_message(
                    f"OpenAI GPT-Live WebSocket receive failed: {exc}", self._api_key
                )
            ) from None

        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        if not isinstance(raw, str):
            raise OpenAIGPTLiveError("Received non-text WebSocket frame from OpenAI GPT-Live")
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise OpenAIGPTLiveError("Received invalid JSON from OpenAI GPT-Live") from exc
        if not isinstance(parsed, dict):
            raise OpenAIGPTLiveError("OpenAI GPT-Live event must be a JSON object")
        return parsed

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            await self._ws.close()
        except Exception:
            pass


async def connect_openai_gpt_live_websocket(
    config: OpenAIGPTLiveConfig,
) -> OpenAIGPTLiveTransport:
    """Connect to the OpenAI GPT-Live WebSocket API using ``websockets``.

    Imports ``websockets`` lazily so core-only installations remain unaffected.
    """
    if not config.api_key or not config.api_key.strip():
        raise OpenAIGPTLiveError(
            "OpenAI GPT-Live API key is required for upstream WebSocket connections"
        )

    url = config.build_websocket_url()
    try:
        validate_endpoint_url(url)
    except ValueError as exc:
        raise OpenAIGPTLiveError(str(exc)) from None

    try:
        from websockets.asyncio.client import connect as ws_connect
    except ImportError as exc:
        raise OpenAIGPTLiveDependencyError(
            "The 'websockets' package is required for OpenAI GPT-Live WebSocket connections. "
            "Install with: pip install kaiwen-gateway[openai]"
        ) from exc

    headers = {"Authorization": f"Bearer {config.api_key}"}
    try:
        ws = await ws_connect(
            url,
            additional_headers=headers,
            open_timeout=config.connect_timeout_seconds,
            close_timeout=config.close_timeout_seconds,
            max_size=4 * 1024 * 1024,
        )
    except Exception as exc:
        raise OpenAIGPTLiveError(
            sanitize_provider_message(
                f"Failed to connect to OpenAI GPT-Live: {exc}", config.api_key
            )
        ) from None

    return _WebsocketsGPTLiveTransport(ws, api_key=config.api_key)
