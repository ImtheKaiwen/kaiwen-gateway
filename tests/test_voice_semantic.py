"""Unit tests for the provider-neutral semantic side channel contracts."""

from __future__ import annotations

import asyncio

import pytest

from kaiwen_gateway.modules.voice import (
    BoundedSemanticEventChannel,
    DuplicateToolResultError,
    MalformedToolArgumentsError,
    ResponseLifecycleEvent,
    SemanticBackpressureError,
    SemanticCapabilities,
    SemanticChannelError,
    StaleToolCallError,
    ToolCallRequested,
    ToolCallTracker,
    ToolDeclaration,
    ToolResult,
    TranscriptEvent,
    UnknownToolCallError,
    parse_tool_arguments,
)


def _delta(seq: int) -> TranscriptEvent:
    return TranscriptEvent(sequence=seq, turn_id=1, role="assistant", text="x", is_final=False)


def _lifecycle(seq: int) -> ResponseLifecycleEvent:
    return ResponseLifecycleEvent(sequence=seq, turn_id=1, phase="started")


def test_capabilities_default_false_and_consistency() -> None:
    caps = SemanticCapabilities()
    assert not any((caps.tool_calls, caps.output_transcript_delta, caps.response_lifecycle))
    with pytest.raises(SemanticChannelError):
        SemanticCapabilities(tool_result_continuation=True)


@pytest.mark.parametrize("name", ["tasks.add", "", "a" * 65, "x y", "../etc"])
def test_tool_declaration_rejects_unsafe_names(name: str) -> None:
    with pytest.raises(ValueError):
        ToolDeclaration(name=name, description="d")


def test_tool_declaration_requires_object_schema() -> None:
    with pytest.raises(ValueError):
        ToolDeclaration(name="t", description="d", parameters={"type": "string"})
    ok = ToolDeclaration(name="tasks_add", description="Add a task")
    assert ok.parameters["type"] == "object"


def test_tool_result_limits() -> None:
    with pytest.raises(ValueError):
        ToolResult(call_id="", output="{}")
    with pytest.raises(ValueError):
        ToolResult(call_id="c\n1", output="{}")
    with pytest.raises(ValueError):
        ToolResult(call_id="c1", output="x" * 40_000)


def test_parse_tool_arguments() -> None:
    def ev(args: str) -> ToolCallRequested:
        return ToolCallRequested(sequence=0, turn_id=1, call_id="c", name="t", arguments_json=args)

    assert parse_tool_arguments(ev('{"a": 1}')) == {"a": 1}
    assert parse_tool_arguments(ev("")) == {}
    with pytest.raises(MalformedToolArgumentsError):
        parse_tool_arguments(ev("{not json"))
    with pytest.raises(MalformedToolArgumentsError):
        parse_tool_arguments(ev("[1, 2]"))


def test_channel_delivers_in_order_then_none_after_close() -> None:
    async def run() -> None:
        ch = BoundedSemanticEventChannel(4)
        assert ch.publish(_lifecycle(ch.next_sequence()))
        assert ch.publish(_delta(ch.next_sequence()))
        ch.close()
        first = await ch.receive()
        second = await ch.receive()
        assert isinstance(first, ResponseLifecycleEvent) and first.sequence == 0
        assert isinstance(second, TranscriptEvent) and second.sequence == 1
        assert await ch.receive() is None
        assert not ch.publish(_delta(9))

    asyncio.run(run())


def test_channel_drops_transcript_deltas_before_critical_events() -> None:
    async def run() -> None:
        ch = BoundedSemanticEventChannel(2)
        ch.publish(_delta(0))
        ch.publish(_delta(1))
        assert not ch.publish(_delta(2))  # incoming droppable discarded
        assert ch.publish(_lifecycle(3))  # evicts oldest droppable
        assert ch.dropped_count == 2
        items = [await ch.receive(), await ch.receive()]
        assert [type(i).__name__ for i in items] == ["TranscriptEvent", "ResponseLifecycleEvent"]

    asyncio.run(run())


def test_channel_overflow_of_critical_events_terminates_without_blocking() -> None:
    async def run() -> None:
        ch = BoundedSemanticEventChannel(1)
        assert ch.publish(_lifecycle(0))
        assert not ch.publish(_lifecycle(1))  # returns immediately, never awaits
        assert ch.is_closed
        assert isinstance(await ch.receive(), ResponseLifecycleEvent)
        with pytest.raises(SemanticBackpressureError):
            await ch.receive()

    asyncio.run(run())


def test_channel_receive_waits_for_publish() -> None:
    async def run() -> None:
        ch = BoundedSemanticEventChannel(2)
        waiter = asyncio.create_task(ch.receive())
        await asyncio.sleep(0)
        assert not waiter.done()
        ch.publish(_lifecycle(0))
        assert isinstance(await asyncio.wait_for(waiter, 1.0), ResponseLifecycleEvent)

    asyncio.run(run())


def test_tracker_single_use_and_stale_semantics() -> None:
    tracker = ToolCallTracker(max_pending=2)
    assert tracker.register("a", 1)
    assert not tracker.register("a", 1)  # duplicate
    assert tracker.claim_for_result("a") == 1
    with pytest.raises(DuplicateToolResultError):
        tracker.claim_for_result("a")
    assert not tracker.register("a", 2)  # replay of a settled call
    with pytest.raises(UnknownToolCallError):
        tracker.claim_for_result("zzz")

    assert tracker.register("b", 1)
    assert tracker.register("c", 2)
    assert not tracker.register("d", 2)  # pending limit
    assert tracker.cancel_through_turn(1) == [("b", 1)]
    with pytest.raises(StaleToolCallError):
        tracker.claim_for_result("b")
    assert tracker.cancel_all() == [("c", 2)]
    assert tracker.pending_count == 0
