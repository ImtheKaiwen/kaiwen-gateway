"""GPT-Live Responses delegation (mock WS server, no keys, no paid traffic).

Wire shapes follow developers.openai.com/api/docs/guides/live-delegation and the Live
API reference (session.delegation.created, response.event, response.item.create,
response.create). A mock proves the adapter's protocol handling only; it does not prove
how the real GPT-Live or backend model behaves.
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
    ResponsesDelegationConfig,
)
from kaiwen_gateway.adapters.openai_gpt_live.delegation import DelegationLedger
from kaiwen_gateway.modules.voice import (
    AudioFormat,
    AudioFrame,
    DuplicateToolResultError,
    ProviderErrorEvent,
    ResponseLifecycleEvent,
    StaleToolCallError,
    ToolCallCancelled,
    ToolCallRequested,
    ToolDeclaration,
    ToolResult,
)

TOOL = ToolDeclaration(
    name="tasks_add",
    description="Add a task.",
    parameters={"type": "object", "properties": {"title": {"type": "string"}}},
)


class MockLive:
    def __init__(self) -> None:
        self.received: list[dict[str, Any]] = []
        self.ws: Any = None

    async def handler(self, ws: Any) -> None:
        self.ws = ws
        async for raw in ws:
            evt = json.loads(raw)
            if evt["type"] == "session.input_audio.append":
                continue
            self.received.append(evt)
            if evt["type"] == "session.start":
                await self.send({"type": "session.started", "session": {"id": "sess"}})

    async def send(self, event: dict[str, Any]) -> None:
        await self.ws.send(json.dumps(event))

    async def nested(self, delegation: str, event: dict[str, Any]) -> None:
        await self.send({"type": "response.event", "delegation_id": delegation, "event": event})

    async def call(self, delegation: str, call_id: str, title: str = "Su içmek") -> None:
        await self.nested(
            delegation,
            {
                "type": "response.output_item.done",
                "item": {
                    "type": "function_call",
                    "call_id": call_id,
                    "name": "tasks_add",
                    "arguments": json.dumps({"title": title}),
                },
            },
        )

    async def lifecycle(self, delegation: str, kind: str, response_id: str = "resp_1") -> None:
        await self.nested(delegation, {"type": kind, "response": {"id": response_id}})

    def sent(self, kind: str) -> list[dict[str, Any]]:
        return [e for e in self.received if e["type"] == kind]


@asynccontextmanager
async def _live() -> AsyncIterator[tuple[MockLive, OpenAIGPTLiveMediaAdapter]]:
    mock = MockLive()
    server = await websockets.serve(mock.handler, "127.0.0.1", 0)
    port = next(iter(server.sockets)).getsockname()[1]
    config = OpenAIGPTLiveConfig(
        api_key="sk-test",
        endpoint_url=f"ws://127.0.0.1:{port}/v1/live/sessions",
        enable_semantic_events=True,
        responses_delegation=ResponsesDelegationConfig(
            model="gpt-6-luna", tools=(TOOL,), instructions="backend", max_output_tokens=400
        ),
    )
    adapter = OpenAIGPTLiveMediaAdapter(call_id="c", tenant_id="t", config=config)
    try:
        await adapter.send_frame(
            AudioFrame(
                payload=b"\x01\x00" * 320,
                format=AudioFormat.PCM16LE,
                sample_rate=16000,
                channels=1,
                sequence=0,
                timestamp_ms=0.0,
            )
        )
        await _until(lambda: bool(mock.sent("session.start")))
        yield mock, adapter
    finally:
        await adapter.close()
        server.close()
        await server.wait_closed()


async def _until(predicate: Any) -> None:
    async with asyncio.timeout(5):
        while not predicate():
            await asyncio.sleep(0.005)


async def _next(adapter: OpenAIGPTLiveMediaAdapter, cls: type) -> Any:
    async with asyncio.timeout(5):
        while True:
            event = await adapter.receive_semantic_event()
            assert event is not None
            if isinstance(event, cls):
                return event


def _result(call_id: str) -> ToolResult:
    return ToolResult(call_id=call_id, output='{"ok": true}')


def test_session_start_declares_responses_delegation_and_tools() -> None:
    async def run() -> None:
        async with _live() as (mock, adapter):
            delegation = mock.sent("session.start")[0]["session"]["delegation"]
            assert delegation["type"] == "responses"
            backend = delegation["responses"]
            assert backend["model"] == "gpt-6-luna"
            assert backend["tools"][0] == {
                "type": "function",
                "name": "tasks_add",
                "description": "Add a task.",
                "parameters": dict(TOOL.parameters),
            }
            assert backend["tool_choice"] == "auto"
            assert backend["parallel_tool_calls"] is False
            assert backend["max_output_tokens"] == 400
            assert adapter.semantic_capabilities.tool_calls
            assert adapter.semantic_capabilities.tool_result_continuation

    asyncio.run(run())


def test_function_call_result_and_single_continuation_after_completion() -> None:
    async def run() -> None:
        async with _live() as (mock, adapter):
            await mock.send(
                {
                    "type": "session.delegation.created",
                    "delegation": {"id": "d1", "target": "responses", "response_id": "resp_1"},
                }
            )
            await mock.lifecycle("d1", "response.created")
            await mock.call("d1", "call_1")
            await mock.call("d1", "call_1")  # replayed event must not surface twice
            requested = await _next(adapter, ToolCallRequested)
            assert requested.call_id == "call_1" and requested.name == "tasks_add"
            assert json.loads(requested.arguments_json) == {"title": "Su içmek"}

            await adapter.submit_tool_result(_result("call_1"))
            await _until(lambda: bool(mock.sent("response.item.create")))
            assert mock.sent("response.item.create")[0]["item"] == {
                "type": "function_call_output",
                "call_id": "call_1",
                "output": '{"ok": true}',
            }
            assert mock.sent("response.create") == []  # not before the response completed
            await mock.lifecycle("d1", "response.completed")
            await _until(lambda: bool(mock.sent("response.create")))
            assert len(mock.sent("response.create")) == 1
            assert mock.sent("conversation.item.create") == []  # Realtime shape never used
            with pytest.raises(DuplicateToolResultError):
                await adapter.submit_tool_result(_result("call_1"))
            assert adapter.rejected_tool_call_count == 1

    asyncio.run(run())


def test_two_calls_continue_only_after_both_are_answered() -> None:
    async def run() -> None:
        async with _live() as (mock, adapter):
            await mock.lifecycle("d1", "response.created")
            await mock.call("d1", "call_1")
            await mock.call("d1", "call_2")
            await mock.lifecycle("d1", "response.completed")
            await _next(adapter, ToolCallRequested)
            await _next(adapter, ToolCallRequested)
            await adapter.submit_tool_result(_result("call_1"))
            await asyncio.sleep(0.05)
            assert mock.sent("response.create") == []
            await adapter.submit_tool_result(_result("call_2"))
            await _until(lambda: bool(mock.sent("response.create")))
            assert len(mock.sent("response.create")) == 1

    asyncio.run(run())


def test_interrupt_cancels_pending_call_and_late_result_is_stale() -> None:
    async def run() -> None:
        async with _live() as (mock, adapter):
            await mock.lifecycle("d1", "response.created")
            await mock.call("d1", "call_1")
            await mock.lifecycle("d1", "response.completed")
            requested = await _next(adapter, ToolCallRequested)
            await adapter.cancel_turn(requested.turn_id)
            cancelled = await _next(adapter, ToolCallCancelled)
            assert cancelled.reason == "turn_interrupted"
            await _until(lambda: bool(mock.sent("response.create")))
            output = mock.sent("response.item.create")[0]["item"]["output"]
            assert json.loads(output)["ok"] is False
            with pytest.raises(StaleToolCallError):
                await adapter.submit_tool_result(_result("call_1"))
            assert len(mock.sent("response.item.create")) == 1  # the late result is not sent

    asyncio.run(run())


def test_failed_backend_response_cancels_calls_and_is_reported_non_fatally() -> None:
    async def run() -> None:
        async with _live() as (mock, adapter):
            await mock.lifecycle("d1", "response.created")
            await mock.call("d1", "call_1")
            await _next(adapter, ToolCallRequested)
            await mock.lifecycle("d1", "response.failed")
            assert (await _next(adapter, ToolCallCancelled)).reason == "backend_failed"
            error = await _next(adapter, ProviderErrorEvent)
            assert error.code == "backend_response_failed" and not error.fatal
            with pytest.raises(StaleToolCallError):
                await adapter.submit_tool_result(_result("call_1"))
            assert mock.sent("response.create") == []
            assert not adapter.is_closed

    asyncio.run(run())


def test_responses_validation_failed_keeps_audio_open_and_withdraws_tool_capability() -> None:
    async def run() -> None:
        async with _live() as (mock, adapter):
            await mock.send(
                {
                    "type": "error",
                    "error": {
                        "type": "invalid_request_error",
                        "code": "responses_validation_failed",
                        "message": "model not accessible",
                    },
                }
            )
            error = await _next(adapter, ProviderErrorEvent)
            assert error.code == "responses_validation_failed" and not error.fatal
            assert not adapter.is_closed
            assert adapter.semantic_capabilities.tool_calls is False

    asyncio.run(run())


def test_rejected_delegation_command_is_non_fatal_but_other_errors_stay_fatal() -> None:
    async def run() -> None:
        async with _live() as (mock, adapter):
            await mock.send(
                {
                    "type": "error",
                    "error": {"code": "invalid_value", "client_event_id": "kg_dlg_7"},
                }
            )
            assert not (await _next(adapter, ProviderErrorEvent)).fatal
            assert not adapter.is_closed
            await mock.send({"type": "error", "error": {"code": "server_error"}})
            assert (await _next(adapter, ProviderErrorEvent)).fatal
            await _until(lambda: adapter.is_closed)

    asyncio.run(run())


def test_close_cancels_pending_calls() -> None:
    async def run() -> None:
        async with _live() as (mock, adapter):
            await mock.call("d1", "call_1")
            await _next(adapter, ToolCallRequested)
            await adapter.close()
            assert (await _next(adapter, ToolCallCancelled)).reason == "session_closed"

    asyncio.run(run())


def test_call_proposed_after_interrupt_by_old_work_is_not_surfaced() -> None:
    async def run() -> None:
        async with _live() as (mock, adapter):
            await mock.lifecycle("d1", "response.created")
            await _next(adapter, ResponseLifecycleEvent)
            await adapter.cancel_turn(1)
            await mock.call("d1", "late_call")
            await mock.lifecycle("d1", "response.completed")
            await _until(lambda: bool(mock.sent("response.create")))
            output = mock.sent("response.item.create")[0]["item"]
            assert output["call_id"] == "late_call" and json.loads(output["output"])["ok"] is False
            # A new delegation after the interrupt is surfaced normally.
            await mock.lifecycle("d2", "response.created", "resp_2")
            await mock.call("d2", "new_call")
            assert (await _next(adapter, ToolCallRequested)).call_id == "new_call"

    asyncio.run(run())


def test_continuation_never_overtakes_a_slow_tool_output() -> None:
    async def run() -> None:
        async with _live() as (mock, adapter):
            await mock.lifecycle("d1", "response.created")
            await mock.call("d1", "call_1")
            await _next(adapter, ToolCallRequested)
            transport = adapter._transport
            assert transport is not None
            original = transport.send_json
            released = asyncio.Event()

            async def slow(payload: dict[str, Any]) -> None:
                if payload["type"] == "response.item.create":
                    await released.wait()
                await original(payload)

            transport.send_json = slow  # type: ignore[method-assign]
            submit = asyncio.create_task(adapter.submit_tool_result(_result("call_1")))
            await asyncio.sleep(0.02)
            await mock.lifecycle("d1", "response.completed")
            await asyncio.sleep(0.05)
            assert mock.sent("response.create") == []
            released.set()
            await submit
            await _until(lambda: bool(mock.sent("response.create")))
            kinds = [e["type"] for e in mock.received if e["type"].startswith("response.")]
            assert kinds == ["response.item.create", "response.create"]

    asyncio.run(run())


def test_answered_calls_do_not_accumulate_across_backend_responses() -> None:
    ledger = DelegationLedger()
    for index in range(200):
        ledger.start("d1", f"resp_{index}")
        ledger.add_call("d1", f"call_{index}")
        ledger.answer(f"call_{index}")
    ledger.start("d1", "last")
    assert ledger.indexed_call_count == 0


def test_delegated_tools_require_semantic_events() -> None:
    with pytest.raises(ValueError, match="enable_semantic_events"):
        OpenAIGPTLiveConfig(
            api_key="sk-test",
            responses_delegation=ResponsesDelegationConfig(model="m", tools=(TOOL,)),
        )
