"""Conformance test suite for OpenAIRealtimeMediaAdapter (all 8 dimensions, zero skips)."""

from __future__ import annotations

import asyncio
from typing import Any

from kaiwen_gateway.adapters.openai_realtime import (
    OpenAIRealtimeConfig,
    OpenAIRealtimeMediaAdapter,
    OpenAIRealtimeTransport,
)
from kaiwen_gateway.adapters.openai_realtime.adapter import _EOF
from kaiwen_gateway.modules.voice.domain import AudioFrame
from kaiwen_gateway.modules.voice.ports import AIRealtimeMediaPort
from kaiwen_gateway.testing.conformance import RealtimeAdapterConformanceSuite


class _MockConformanceTransport:
    """In-memory OpenAI Realtime JSON WebSocket transport for conformance verification."""

    def __init__(self) -> None:
        self.sent_events: list[dict[str, Any]] = []
        self._server_queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
        self.closed = False

    async def send_json(self, payload: dict[str, Any]) -> None:
        self.sent_events.append(payload)

    async def receive_json(self) -> dict[str, Any] | None:
        if self.closed and self._server_queue.empty():
            return None
        return await self._server_queue.get()

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self._server_queue.put_nowait(None)


class TestOpenAIRealtimeAdapterConformance(RealtimeAdapterConformanceSuite):
    """Runs all 8 RealtimeAdapterConformanceSuite dimensions against OpenAIRealtimeMediaAdapter."""

    _transports: dict[int, _MockConformanceTransport] = {}

    def create_adapter(
        self,
        *,
        call_id: str = "call-default",
        tenant_id: str = "tenant-default",
        queue_capacity: int = 8,
        **kwargs: Any,
    ) -> AIRealtimeMediaPort:
        backpressure_timeout = float(kwargs.get("backpressure_timeout", 0.05))
        config = OpenAIRealtimeConfig(
            api_key="sk-test-conformance-mock-only",
            endpoint_url="wss://mock.openai.local/v1/realtime",
            inbound_queue_capacity=queue_capacity,
            outbound_queue_capacity=queue_capacity,
            backpressure_timeout_seconds=backpressure_timeout,
        )
        transport = _MockConformanceTransport()

        async def _connector(_cfg: OpenAIRealtimeConfig) -> OpenAIRealtimeTransport:
            return transport

        adapter = OpenAIRealtimeMediaAdapter(
            call_id=call_id,
            tenant_id=tenant_id,
            config=config,
            queue_capacity=queue_capacity,
            backpressure_timeout=backpressure_timeout,
            transport_connector=_connector,
        )
        self._transports[id(adapter)] = transport
        return adapter

    def inject_upstream_frame(self, adapter: AIRealtimeMediaPort, frame: AudioFrame) -> bool:
        if isinstance(adapter, OpenAIRealtimeMediaAdapter):
            adapter._outbound_queue.put_nowait(frame)
            return True
        return False

    async def inject_upstream_eof(self, adapter: AIRealtimeMediaPort) -> bool:
        if isinstance(adapter, OpenAIRealtimeMediaAdapter):
            adapter._outbound_queue.put_nowait(_EOF)
            return True
        return False

    def inject_upstream_failure(self, adapter: AIRealtimeMediaPort, error: Exception) -> bool:
        if isinstance(adapter, OpenAIRealtimeMediaAdapter):
            adapter.trigger_upstream_failure(error)
            return True
        return False
