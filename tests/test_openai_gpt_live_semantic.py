"""Semantic side channel tests for OpenAIGPTLiveMediaAdapter (mock WS server, no keys).

Event names follow developers.openai.com/api/docs/guides/live-conversations
(session.input_transcript.delta / session.output_transcript.delta, no final marker)
and the primary WebSocket reference (terminal session.closed with a reason).
Tool calls are intentionally unsupported by this adapter.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest
import websockets

from kaiwen_gateway.adapters.openai_gpt_live import (
    OpenAIGPTLiveConfig,
    OpenAIGPTLiveMediaAdapter,
)
from kaiwen_gateway.modules.voice import (
    AudioFormat,
    AudioFrame,
    SemanticEvent,
    SessionLifecycleEvent,
    ToolResult,
    TranscriptEvent,
    UnsupportedSemanticCapabilityError,
)


@asynccontextmanager
async def _server(handler: Any) -> AsyncIterator[str]:
    server = await websockets.serve(handler, "127.0.0.1", 0)
    port = next(iter(server.sockets)).getsockname()[1]
    try:
        yield f"ws://127.0.0.1:{port}/v1/live/sessions"
    finally:
        server.close()
        await server.wait_closed()


def _frame() -> AudioFrame:
    return AudioFrame(
        payload=b"\x01\x00" * 320,
        format=AudioFormat.PCM16LE,
        sample_rate=16000,
        channels=1,
        sequence=0,
        timestamp_ms=0.0,
    )


def test_gpt_live_semantic_disabled_by_default() -> None:
    cfg = OpenAIGPTLiveConfig(api_key="sk-x", endpoint_url="ws://127.0.0.1:1/v1/live/sessions")
    adapter = OpenAIGPTLiveMediaAdapter(call_id="c", tenant_id="t", config=cfg)
    assert adapter.semantic_capabilities.output_transcript_delta is False

    async def run() -> None:
        with pytest.raises(UnsupportedSemanticCapabilityError):
            await adapter.receive_semantic_event()
        await adapter.close()

    asyncio.run(run())


def test_gpt_live_transcripts_lifecycle_and_no_tool_support() -> None:
    async def handler(ws: Any) -> None:
        async for raw in ws:
            evt = json.loads(raw)
            if evt["type"] == "session.start":
                await ws.send(
                    json.dumps({"type": "session.started", "session": {"id": "sess_live"}})
                )
            elif evt["type"] == "session.input_audio.append":
                await ws.send(
                    json.dumps(
                        {
                            "type": "session.input_transcript.delta",
                            "delta": "merhaba",
                            "start_ms": 0,
                        }
                    )
                )
                await ws.send(
                    json.dumps({"type": "session.output_transcript.delta", "delta": "Selam"})
                )
                await ws.send(json.dumps({"type": "session.closed", "reason": "close_requested"}))
                await ws.close()
                return

    async def run() -> None:
        async with _server(handler) as url:
            cfg = OpenAIGPTLiveConfig(api_key="sk-x", endpoint_url=url, enable_semantic_events=True)
            adapter = OpenAIGPTLiveMediaAdapter(call_id="c", tenant_id="t", config=cfg)
            caps = adapter.semantic_capabilities
            assert caps.input_transcript_delta and caps.output_transcript_delta
            assert not caps.tool_calls and not caps.input_transcript_final
            with pytest.raises(UnsupportedSemanticCapabilityError):
                await adapter.submit_tool_result(ToolResult(call_id="x", output="{}"))
            await adapter.send_frame(_frame())
            events: list[SemanticEvent] = []
            async with asyncio.timeout(3):
                while (ev := await adapter.receive_semantic_event()) is not None:
                    events.append(ev)
            assert isinstance(events[0], SessionLifecycleEvent) and events[0].phase == "ready"
            transcripts = [e for e in events if isinstance(e, TranscriptEvent)]
            assert [(t.role, t.text, t.is_final) for t in transcripts] == [
                ("user", "merhaba", False),
                ("assistant", "Selam", False),
            ]
            assert isinstance(events[-1], SessionLifecycleEvent)
            assert events[-1].phase == "closed" and events[-1].reason == "close_requested"
            await adapter.close()

    asyncio.run(run())
