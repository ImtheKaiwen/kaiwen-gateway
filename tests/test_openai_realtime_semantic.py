"""Semantic/tool side channel tests for OpenAIRealtimeMediaAdapter (mock WS server, no keys).

Wire event names and fields follow the OpenAI Realtime GA reference
(developers.openai.com/api/reference/resources/realtime): session.tools,
response.function_call_arguments.done, response.output_item.done (function_call),
conversation.item.create (function_call_output) + response.create, transcript events.
"""

from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

import pytest
import websockets

from kaiwen_gateway.adapters.openai_realtime import (
    OpenAIRealtimeConfig,
    OpenAIRealtimeError,
    OpenAIRealtimeMediaAdapter,
)
from kaiwen_gateway.modules.voice import (
    AudioFormat,
    AudioFrame,
    DuplicateToolResultError,
    ProviderErrorEvent,
    ResponseLifecycleEvent,
    SemanticEvent,
    SemanticRealtimePort,
    SessionLifecycleEvent,
    StaleToolCallError,
    ToolCallCancelled,
    ToolCallRequested,
    ToolDeclaration,
    ToolResult,
    TranscriptEvent,
    UnknownToolCallError,
    UnsupportedSemanticCapabilityError,
    parse_tool_arguments,
)

TOOLS = (
    ToolDeclaration(
        name="tasks_add",
        description="Add a task",
        parameters={
            "type": "object",
            "properties": {"title": {"type": "string"}},
            "required": ["title"],
        },
    ),
)

Handler = Callable[[Any, list[dict[str, Any]]], Awaitable[None]]


@asynccontextmanager
async def _server(handler: Handler) -> AsyncIterator[tuple[str, list[dict[str, Any]]]]:
    received: list[dict[str, Any]] = []

    async def _wrapped(ws: Any) -> None:
        await handler(ws, received)

    server = await websockets.serve(_wrapped, "127.0.0.1", 0)
    port = next(iter(server.sockets)).getsockname()[1]
    try:
        yield f"ws://127.0.0.1:{port}/v1/realtime", received
    finally:
        server.close()
        await server.wait_closed()


def _frame(seq: int = 0, turn_id: int | None = None) -> AudioFrame:
    return AudioFrame(
        payload=b"\x01\x00" * 320,
        format=AudioFormat.PCM16LE,
        sample_rate=16000,
        channels=1,
        sequence=seq,
        timestamp_ms=seq * 20.0,
        turn_id=turn_id,
    )


def _cfg(url: str, **kw: Any) -> OpenAIRealtimeConfig:
    base: dict[str, Any] = {
        "api_key": "sk-test-semantic-secret",
        "endpoint_url": url,
        "enable_semantic_events": True,
        "tools": TOOLS,
    }
    base.update(kw)
    return OpenAIRealtimeConfig(**base)


async def _recv_until(
    adapter: OpenAIRealtimeMediaAdapter, kind: type, timeout: float = 3.0
) -> list[SemanticEvent]:
    seen: list[SemanticEvent] = []
    async with asyncio.timeout(timeout):
        while True:
            ev = await adapter.receive_semantic_event()
            assert ev is not None, f"channel ended before {kind.__name__}: {seen}"
            seen.append(ev)
            if isinstance(ev, kind):
                return seen


async def _wait_for(received: list[dict[str, Any]], etype: str, timeout: float = 3.0) -> None:
    async with asyncio.timeout(timeout):
        while not any(e.get("type") == etype for e in received):
            await asyncio.sleep(0.01)


def _function_call_events(call_id: str, args: str, resp: str = "resp_1") -> list[dict[str, Any]]:
    item = {
        "id": "item_fc",
        "type": "function_call",
        "status": "completed",
        "call_id": call_id,
        "name": "tasks_add",
        "arguments": args,
    }
    return [
        {"type": "response.created", "response": {"id": resp, "status": "in_progress"}},
        {
            "type": "response.function_call_arguments.done",
            "response_id": resp,
            "item_id": "item_fc",
            "output_index": 0,
            "call_id": call_id,
            "name": "tasks_add",
            "arguments": args,
        },
        {"type": "response.output_item.done", "response_id": resp, "output_index": 0, "item": item},
        {
            "type": "response.done",
            "response": {"id": resp, "status": "completed", "output": [item]},
        },
    ]


def test_legacy_default_is_audio_only_and_session_update_unchanged() -> None:
    cfg = OpenAIRealtimeConfig(api_key="sk-x", endpoint_url="ws://127.0.0.1:1/v1/realtime")
    session = cfg.build_session_update_event()["session"]
    assert "tools" not in session and "tool_choice" not in session
    assert "transcription" not in session["audio"]["input"]
    adapter = OpenAIRealtimeMediaAdapter(call_id="c", tenant_id="t", config=cfg)
    assert adapter.semantic_capabilities.tool_calls is False
    assert adapter.semantic_capabilities.response_lifecycle is False

    async def run() -> None:
        with pytest.raises(UnsupportedSemanticCapabilityError):
            await adapter.receive_semantic_event()
        with pytest.raises(UnsupportedSemanticCapabilityError):
            await adapter.submit_tool_result(ToolResult(call_id="c1", output="{}"))
        await adapter.close()

    asyncio.run(run())


def test_config_requires_semantic_opt_in_for_tools_and_transcription() -> None:
    with pytest.raises(ValueError):
        OpenAIRealtimeConfig(api_key="k", tools=TOOLS)
    with pytest.raises(ValueError):
        OpenAIRealtimeConfig(api_key="k", input_transcription_model="gpt-4o-transcribe")
    with pytest.raises(ValueError):
        OpenAIRealtimeConfig(api_key="k", enable_semantic_events=True, tools=TOOLS * 2)
    with pytest.raises(ValueError):
        OpenAIRealtimeConfig(api_key="k", enable_semantic_events=True, tool_choice="maybe")


def test_session_update_declares_tools_and_transcription() -> None:
    cfg = _cfg("ws://127.0.0.1:1/v1/realtime", input_transcription_model="gpt-4o-transcribe")
    session = cfg.build_session_update_event()["session"]
    assert session["type"] == "realtime"
    assert session["tool_choice"] == "auto"
    assert session["tools"] == [
        {
            "type": "function",
            "name": "tasks_add",
            "description": "Add a task",
            "parameters": dict(TOOLS[0].parameters),
        }
    ]
    assert session["audio"]["input"]["transcription"] == {"model": "gpt-4o-transcribe"}
    adapter = OpenAIRealtimeMediaAdapter(call_id="c", tenant_id="t", config=cfg)
    assert isinstance(adapter, SemanticRealtimePort)
    caps = adapter.semantic_capabilities
    assert caps.tool_calls and caps.tool_result_continuation and caps.input_transcript_final


def test_tool_call_round_trip_same_session_with_dedupe_and_continuation() -> None:
    """function call -> single ToolCallRequested -> result -> function_call_output + response.create."""

    async def handler(ws: Any, received: list[dict[str, Any]]) -> None:
        async for raw in ws:
            evt = json.loads(raw)
            received.append(evt)
            if evt["type"] == "session.update":
                await ws.send(json.dumps({"type": "session.updated", "session": evt["session"]}))
            elif evt["type"] == "input_audio_buffer.append" and len(received) == 2:
                await ws.send(
                    json.dumps(
                        {
                            "type": "conversation.item.input_audio_transcription.completed",
                            "item_id": "item_user",
                            "content_index": 0,
                            "transcript": "add a task",
                        }
                    )
                )
                for e in _function_call_events("call_A", '{"title": "Sunum"}'):
                    await ws.send(json.dumps(e))
            elif evt["type"] == "response.create":
                await ws.send(
                    json.dumps({"type": "response.created", "response": {"id": "resp_2"}})
                )
                await ws.send(
                    json.dumps(
                        {
                            "type": "response.output_audio_transcript.delta",
                            "response_id": "resp_2",
                            "item_id": "item_a",
                            "delta": "Ekledim",
                        }
                    )
                )
                await ws.send(
                    json.dumps(
                        {
                            "type": "response.output_audio.delta",
                            "response_id": "resp_2",
                            "delta": base64.b64encode(b"\x02\x00" * 480).decode("ascii"),
                        }
                    )
                )
                await ws.send(
                    json.dumps(
                        {
                            "type": "response.output_audio_transcript.done",
                            "response_id": "resp_2",
                            "item_id": "item_a",
                            "transcript": "Ekledim.",
                        }
                    )
                )
                await ws.send(
                    json.dumps(
                        {
                            "type": "response.done",
                            "response": {"id": "resp_2", "status": "completed"},
                        }
                    )
                )

    async def run() -> None:
        async with _server(handler) as (url, received):
            adapter = OpenAIRealtimeMediaAdapter(
                call_id="call_1",
                tenant_id="tenant_a",
                config=_cfg(url, input_transcription_model="gpt-4o-transcribe"),
            )
            await adapter.send_frame(_frame(0))
            events = await _recv_until(adapter, ToolCallRequested)
            assert isinstance(events[0], SessionLifecycleEvent) and events[0].phase == "ready"
            user = [e for e in events if isinstance(e, TranscriptEvent)]
            assert user and user[0].role == "user" and user[0].is_final
            call = events[-1]
            assert isinstance(call, ToolCallRequested)
            assert call.call_id == "call_A" and call.name == "tasks_add"
            assert parse_tool_arguments(call) == {"title": "Sunum"}

            # The same call reported by output_item.done and response.done is not re-emitted.
            after = await _recv_until(adapter, ResponseLifecycleEvent)
            assert not any(isinstance(e, ToolCallRequested) for e in after)
            assert adapter.rejected_tool_call_count == 0

            await adapter.submit_tool_result(ToolResult(call_id="call_A", output='{"ok":true}'))
            await _wait_for(received, "response.create")
            item_events = [e for e in received if e["type"] == "conversation.item.create"]
            assert item_events == [
                {
                    "type": "conversation.item.create",
                    "item": {
                        "type": "function_call_output",
                        "call_id": "call_A",
                        "output": '{"ok":true}',
                    },
                }
            ]
            idx_item = received.index(item_events[0])
            assert received[idx_item + 1] == {"type": "response.create"}

            with pytest.raises(DuplicateToolResultError):
                await adapter.submit_tool_result(ToolResult(call_id="call_A", output="{}"))
            with pytest.raises(UnknownToolCallError):
                await adapter.submit_tool_result(ToolResult(call_id="nope", output="{}"))

            # Continuation in the same session: assistant transcript + audio arrive.
            frame = await asyncio.wait_for(adapter.receive_frame(), 3.0)
            assert frame is not None
            tail: list[SemanticEvent] = []
            while not (
                tail
                and isinstance(tail[-1], ResponseLifecycleEvent)
                and tail[-1].phase == "completed"
            ):
                tail.extend(await _recv_until(adapter, ResponseLifecycleEvent))
            started = [e for e in tail if isinstance(e, ResponseLifecycleEvent)][0]
            assert started.phase == "started" and started.response_ref == "resp_2"
            finals = [e for e in tail if isinstance(e, TranscriptEvent) and e.is_final]
            assert finals and finals[0].text == "Ekledim." and finals[0].role == "assistant"
            await adapter.close()
            last = await _recv_until(adapter, SessionLifecycleEvent)
            assert isinstance(last[-1], SessionLifecycleEvent) and last[-1].phase == "closed"
            assert await adapter.receive_semantic_event() is None

    asyncio.run(run())


def test_replayed_and_malformed_tool_calls_are_not_surfaced_twice() -> None:
    async def handler(ws: Any, received: list[dict[str, Any]]) -> None:
        async for raw in ws:
            evt = json.loads(raw)
            received.append(evt)
            if evt["type"] == "session.update":
                await ws.send(json.dumps({"type": "session.updated", "session": evt["session"]}))
                for e in _function_call_events("call_R", '{"title": "x"}'):
                    await ws.send(json.dumps(e))
                # replay of the same call id in a new response
                for e in _function_call_events("call_R", '{"title": "x"}', resp="resp_9"):
                    await ws.send(json.dumps(e))
                # malformed: missing call_id and non-string arguments
                await ws.send(
                    json.dumps(
                        {
                            "type": "response.function_call_arguments.done",
                            "name": "tasks_add",
                            "arguments": {"title": "obj"},
                        }
                    )
                )
                await ws.send(
                    json.dumps(
                        {
                            "type": "response.function_call_arguments.done",
                            "call_id": "call_big",
                            "name": "tasks_add",
                            "arguments": "x" * 40_000,
                        }
                    )
                )
                await ws.send(json.dumps({"type": "response.created", "response": {"id": "end"}}))

    async def run() -> None:
        async with _server(handler) as (url, _received):
            adapter = OpenAIRealtimeMediaAdapter(call_id="c", tenant_id="t", config=_cfg(url))
            await adapter.send_frame(_frame(0))
            calls: list[ToolCallRequested] = []
            async with asyncio.timeout(3):
                while True:
                    ev = await adapter.receive_semantic_event()
                    assert ev is not None
                    if isinstance(ev, ToolCallRequested):
                        calls.append(ev)
                    if isinstance(ev, ResponseLifecycleEvent) and ev.response_ref == "end":
                        break
            assert [c.call_id for c in calls] == ["call_R"]
            assert adapter.rejected_tool_call_count == 2  # pending duplicates are deduped silently
            await adapter.close()

    asyncio.run(run())


def test_interrupt_cancels_pending_call_and_rejects_stale_result() -> None:
    async def handler(ws: Any, received: list[dict[str, Any]]) -> None:
        async for raw in ws:
            evt = json.loads(raw)
            received.append(evt)
            if evt["type"] == "session.update":
                await ws.send(json.dumps({"type": "session.updated", "session": evt["session"]}))
                for e in _function_call_events("call_S", '{"title": "old"}'):
                    await ws.send(json.dumps(e))

    async def run() -> None:
        async with _server(handler) as (url, received):
            adapter = OpenAIRealtimeMediaAdapter(call_id="c", tenant_id="t", config=_cfg(url))
            await adapter.send_frame(_frame(0))
            call = (await _recv_until(adapter, ToolCallRequested))[-1]
            assert isinstance(call, ToolCallRequested)
            await adapter.cancel_turn(call.turn_id)
            cancelled = (await _recv_until(adapter, ToolCallCancelled))[-1]
            assert isinstance(cancelled, ToolCallCancelled)
            assert cancelled.call_id == "call_S" and cancelled.reason == "turn_interrupted"
            with pytest.raises(StaleToolCallError):
                await adapter.submit_tool_result(ToolResult(call_id="call_S", output="{}"))
            await asyncio.sleep(0.05)
            assert not any(e["type"] == "conversation.item.create" for e in received)
            await adapter.close()

    asyncio.run(run())


def test_connection_loss_cancels_pending_calls_and_ends_channel() -> None:
    async def handler(ws: Any, received: list[dict[str, Any]]) -> None:
        async for raw in ws:
            evt = json.loads(raw)
            received.append(evt)
            if evt["type"] == "session.update":
                await ws.send(json.dumps({"type": "session.updated", "session": evt["session"]}))
                for e in _function_call_events("call_L", "{}"):
                    await ws.send(json.dumps(e))
                await ws.send(
                    json.dumps(
                        {
                            "type": "error",
                            "error": {
                                "code": "server_error",
                                "message": "boom sk-test-semantic-secret",
                            },
                        }
                    )
                )
                return

    async def run() -> None:
        async with _server(handler) as (url, _received):
            adapter = OpenAIRealtimeMediaAdapter(call_id="c", tenant_id="t", config=_cfg(url))
            await adapter.send_frame(_frame(0))
            events: list[SemanticEvent] = []
            async with asyncio.timeout(3):
                while (ev := await adapter.receive_semantic_event()) is not None:
                    events.append(ev)
            kinds = [type(e).__name__ for e in events]
            assert "ToolCallRequested" in kinds
            cancel = next(e for e in events if isinstance(e, ToolCallCancelled))
            assert cancel.reason == "session_closed"
            err = next(e for e in events if isinstance(e, ProviderErrorEvent))
            assert err.fatal and err.code == "server_error"
            assert "sk-test-semantic-secret" not in err.message
            assert isinstance(events[-1], SessionLifecycleEvent) and events[-1].phase == "closed"
            with pytest.raises(OpenAIRealtimeError):
                await adapter.submit_tool_result(ToolResult(call_id="call_L", output="{}"))
            await adapter.close()

    asyncio.run(run())


def test_slow_semantic_consumer_never_stalls_audio() -> None:
    """With nobody reading semantic events, audio deltas still flow and transcripts drop."""

    async def handler(ws: Any, received: list[dict[str, Any]]) -> None:
        async for raw in ws:
            evt = json.loads(raw)
            if evt["type"] == "session.update":
                await ws.send(json.dumps({"type": "session.updated", "session": evt["session"]}))
                await ws.send(json.dumps({"type": "response.created", "response": {"id": "r"}}))
                for i in range(200):
                    await ws.send(
                        json.dumps(
                            {
                                "type": "response.output_audio_transcript.delta",
                                "response_id": "r",
                                "delta": f"w{i} ",
                            }
                        )
                    )
                for _ in range(5):
                    await ws.send(
                        json.dumps(
                            {
                                "type": "response.output_audio.delta",
                                "response_id": "r",
                                "delta": base64.b64encode(b"\x03\x00" * 480).decode("ascii"),
                            }
                        )
                    )

    async def run() -> None:
        async with _server(handler) as (url, _received):
            adapter = OpenAIRealtimeMediaAdapter(
                call_id="c", tenant_id="t", config=_cfg(url, semantic_queue_capacity=8)
            )
            await adapter.send_frame(_frame(0))
            frames = [await asyncio.wait_for(adapter.receive_frame(), 3.0) for _ in range(3)]
            assert all(f is not None for f in frames)
            assert adapter.semantic_dropped_count > 0
            assert not adapter.is_closed
            await adapter.close()

    asyncio.run(run())
