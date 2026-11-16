"""Conformance test suite for OpenAIGPTLiveMediaAdapter (all 8 dimensions, zero skips)."""

from __future__ import annotations

import asyncio
from typing import Any

from kaiwen_gateway.adapters.openai_gpt_live import (
    OpenAIGPTLiveConfig,
    OpenAIGPTLiveMediaAdapter,
    OpenAIGPTLiveTransport,
)
from kaiwen_gateway.adapters.openai_gpt_live.adapter import _EOF
from kaiwen_gateway.modules.voice.domain import AudioFrame
from kaiwen_gateway.modules.voice.ports import AIRealtimeMediaPort
from kaiwen_gateway.testing.conformance import RealtimeAdapterConformanceSuite


class _MockGPTLiveConformanceTransport:
    """In-memory OpenAI GPT-Live JSON WebSocket transport for conformance verification."""

    def __init__(self) -> None:
        self.sent_events: list[dict[str, Any]] = []
        self._server_queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
        self.closed = False

    async def send_json(self, payload: dict[str, Any]) -> None:
        self.sent_events.append(payload)
        if payload.get("type") == "session.start":
            self._server_queue.put_nowait(
                {
                    "type": "session.started",
                    "event_id": "evt_started_conf",
                    "session": {
                        "id": "live_conf_1",
                        "model": "gpt-live-1",
                        "status": "active",
                    },
                }
            )

    async def receive_json(self) -> dict[str, Any] | None:
        if self.closed and self._server_queue.empty():
            return None
        return await self._server_queue.get()

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self._server_queue.put_nowait(None)


class TestOpenAIGPTLiveAdapterConformance(RealtimeAdapterConformanceSuite):
    """Runs all 8 RealtimeAdapterConformanceSuite dimensions against OpenAIGPTLiveMediaAdapter."""

    _transports: dict[int, _MockGPTLiveConformanceTransport] = {}

    def create_adapter(
        self,
        *,
        call_id: str = "call-default",
        tenant_id: str = "tenant-default",
        queue_capacity: int = 8,
        **kwargs: Any,
    ) -> AIRealtimeMediaPort:
        backpressure_timeout = float(kwargs.get("backpressure_timeout", 0.05))
        config = OpenAIGPTLiveConfig(
            api_key="sk-test-gpt-live-conformance-mock-only",
            endpoint_url="wss://mock.openai.local/v1/live/sessions",
            inbound_queue_capacity=queue_capacity,
            outbound_queue_capacity=queue_capacity,
            backpressure_timeout_seconds=backpressure_timeout,
        )
        transport = _MockGPTLiveConformanceTransport()

        async def _connector(_cfg: OpenAIGPTLiveConfig) -> OpenAIGPTLiveTransport:
            return transport

        adapter = OpenAIGPTLiveMediaAdapter(
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
        if isinstance(adapter, OpenAIGPTLiveMediaAdapter):
            adapter._outbound_queue.put_nowait(frame)
            return True
        return False

    async def inject_upstream_eof(self, adapter: AIRealtimeMediaPort) -> bool:
        if isinstance(adapter, OpenAIGPTLiveMediaAdapter):
            adapter._outbound_queue.put_nowait(_EOF)
            return True
        return False

    def inject_upstream_failure(self, adapter: AIRealtimeMediaPort, error: Exception) -> bool:
        if isinstance(adapter, OpenAIGPTLiveMediaAdapter):
            adapter.trigger_upstream_failure(error)
            return True
        return False
