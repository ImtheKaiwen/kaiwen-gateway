"""Unit and deterministic integration tests for Voice turn interruption and playback control.

Verifies the 15 required scenarios for M4.2B.1 and Gates A - F:
1. Normal bidirectional streaming without interruption.
2. Interruption before AI output begins.
3. Interruption when output queue is full (releases backpressure).
4. Discarding pending output of old turn.
5. Correct ordering of new turn frames.
6. Outbound send waiting during interruption.
7. Duplicate / idempotent interruption.
8. Simultaneous interruption and close.
9. Interruption after FAILED/CLOSED state raises VoiceBridgeSessionClosedError.
10. Two-tenant / two-call isolation (Call A interruption does not affect Call B).
11. Unsupported interruption capability raises UnsupportedInterruptionCapabilityError.
12. Provider cancellation failure handled safely.
13. Controlled cleanup without leaks.
14. No orphan background tasks (asyncio.all_tasks()).
15. Existing call.end lossless drain unchanged.
+ GATE A: In-flight send race and linearization barrier.
+ GATE B: Exact untagged frame policy across interruptions.
+ GATE C: Sequence continuity vs per-turn resets.
+ GATE D: Queue task accounting and EOF join balance.
+ GATE E: Capability validation and honest outcome reporting.
+ GATE F: Public AudioFrame dataclass compatibility and invariant preservation.
"""

from __future__ import annotations

import asyncio
import dataclasses
from typing import Any

import pytest

from kaiwen_gateway.modules.voice import (
    AudioFrame,
    FutureTurnAudioFrameError,
    InterruptibleAIPort,
    InterruptibleFakeAIPort,
    InterruptibleFakeTelephonyAdapter,
    InterruptibleTelephonyPort,
    InterruptionCapabilities,
    InterruptionOutcome,
    InvalidAudioFrameError,
    OutOfOrderAudioFrameError,
    UnsupportedInterruptionCapabilityError,
    VoiceBridgeSession,
    VoiceBridgeSessionClosedError,
    VoiceBridgeState,
    VoiceDomainError,
    create_synthetic_pcm16le_frame,
)
from kaiwen_gateway.modules.voice.adapters.fake import FakeAIMediaAdapter
from kaiwen_gateway.modules.voice.adapters.fake_ai import BoundedFakeAIEchoPort


@pytest.fixture
def frame_bytes() -> bytes:
    """160 samples of 16-bit mono PCM (320 bytes = 20ms at 16kHz)."""
    return b"\x00\x00" * 160


# -----------------------------------------------------------------------------
# Unit contract tests
# -----------------------------------------------------------------------------


def test_audio_frame_turn_id_validation(frame_bytes: bytes) -> None:
    """AudioFrame validates turn_id type and bounds."""
    # Valid turn_id
    f1 = create_synthetic_pcm16le_frame(frame_bytes, sequence=0, turn_id=0)
    assert f1.turn_id == 0

    f2 = create_synthetic_pcm16le_frame(frame_bytes, sequence=1, turn_id=42)
    assert f2.turn_id == 42

    f3 = create_synthetic_pcm16le_frame(frame_bytes, sequence=2, turn_id=None)
    assert f3.turn_id is None

    # Invalid turn_id: negative
    with pytest.raises(InvalidAudioFrameError, match="turn_id must be non-negative"):
        create_synthetic_pcm16le_frame(frame_bytes, sequence=3, turn_id=-1)

    # Invalid turn_id: boolean
    with pytest.raises(InvalidAudioFrameError, match="turn_id must be int or None"):
        create_synthetic_pcm16le_frame(frame_bytes, sequence=4, turn_id=True)

    # Invalid turn_id: string
    with pytest.raises(InvalidAudioFrameError, match="turn_id must be int or None"):
        create_synthetic_pcm16le_frame(frame_bytes, sequence=5, turn_id="turn-1")  # type: ignore[arg-type]


def test_interruption_capabilities_validation() -> None:
    """InterruptionCapabilities enforces playback honesty invariant."""
    # Valid configurations
    caps_default = InterruptionCapabilities()
    assert caps_default.cannot_confirm_playback is True
    assert caps_default.can_observe_playback_acks is False

    caps_acked = InterruptionCapabilities(
        can_observe_playback_acks=True,
        cannot_confirm_playback=False,
    )
    assert caps_acked.can_observe_playback_acks is True
    assert caps_acked.cannot_confirm_playback is False

    # Contradictory: cannot_confirm_playback=True while can_observe_playback_acks=True
    with pytest.raises(VoiceDomainError, match="cannot_confirm_playback cannot be True"):
        InterruptionCapabilities(
            can_observe_playback_acks=True,
            cannot_confirm_playback=True,
        )

    # Contradictory: cannot_confirm_playback=False while can_observe_playback_acks=False
    with pytest.raises(VoiceDomainError, match="cannot_confirm_playback must be True"):
        InterruptionCapabilities(
            can_observe_playback_acks=False,
            cannot_confirm_playback=False,
        )


def test_interruption_protocols_runtime_checkable() -> None:
    """InterruptibleAIPort and InterruptibleTelephonyPort protocols are runtime checkable."""
    fake_ai = InterruptibleFakeAIPort()
    assert isinstance(fake_ai, InterruptibleAIPort)

    fake_tel = InterruptibleFakeTelephonyAdapter()
    assert isinstance(fake_tel, InterruptibleTelephonyPort)


# -----------------------------------------------------------------------------
# 15 Required Scenarios
# -----------------------------------------------------------------------------


def test_scenario_01_normal_streaming_without_interruption(frame_bytes: bytes) -> None:
    """Scenario 1: Normal bidirectional streaming without interruption."""

    async def _test() -> None:
        tel_port = InterruptibleFakeTelephonyAdapter()
        ai_port = InterruptibleFakeAIPort()

        session = VoiceBridgeSession(
            call_id="call-01",
            tenant_id="tenant-test",
            telephony_port=tel_port,
            ai_port=ai_port,
        )

        async with session:
            # Inbound: 3 frames from telephony to AI
            for seq in range(3):
                tel_port.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=seq))
            tel_port.push_eof()

            # Outbound: 3 frames from AI to telephony
            for seq in range(3):
                ai_port.push_frame(
                    create_synthetic_pcm16le_frame(frame_bytes, sequence=seq, turn_id=1)
                )
            ai_port.push_eof()

            await session.wait_until_closed()

        assert session.state == VoiceBridgeState.CLOSED
        assert session.inbound_frames_transferred == 3
        assert session.outbound_frames_transferred == 3
        assert session.current_turn_id == 1
        assert session.interruption_count == 0
        assert session.stale_frames_suppressed == 0
        assert session.locally_discarded_frames == 0
        assert len(tel_port.sent_frames) == 3

    asyncio.run(_test())


def test_scenario_02_interruption_before_ai_output_begins(frame_bytes: bytes) -> None:
    """Scenario 2: Interruption before AI output begins."""

    async def _test() -> None:
        tel_port = InterruptibleFakeTelephonyAdapter()
        ai_port = InterruptibleFakeAIPort()

        session = VoiceBridgeSession(
            call_id="call-02",
            tenant_id="tenant-test",
            telephony_port=tel_port,
            ai_port=ai_port,
        )

        async with session:
            # Interrupt while session is running but no frames generated yet
            outcome = await session.interrupt()
            assert outcome.interrupted_turn_id == 1
            assert outcome.new_turn_id == 2
            assert outcome.locally_discarded_frames == 0
            assert outcome.future_generation_cancelled is True
            assert session.current_turn_id == 2
            assert session.interruption_count == 1

            # Now AI produces frames tagged with turn_id=2
            ai_port.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=0, turn_id=2))
            ai_port.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=1, turn_id=2))
            tel_port.push_eof()
            ai_port.push_eof()
            await session.wait_until_closed()

        assert session.outbound_frames_transferred == 2
        assert len(tel_port.sent_frames) == 2
        assert [f.turn_id for f in tel_port.sent_frames] == [2, 2]
        assert session.stale_frames_suppressed == 0

    asyncio.run(_test())


def test_scenario_03_interruption_when_output_queue_is_full_releases_backpressure(
    frame_bytes: bytes,
) -> None:
    """Scenario 3: Interruption when output queue is full releases backpressure."""

    async def _test() -> None:
        tel_port = InterruptibleFakeTelephonyAdapter()
        tel_port.set_send_delay(10.0)
        ai_port = InterruptibleFakeAIPort()

        # Small capacity 2 so queue fills immediately
        session = VoiceBridgeSession(
            call_id="call-03",
            tenant_id="tenant-test",
            telephony_port=tel_port,
            ai_port=ai_port,
            queue_capacity=2,
        )

        await session.start()
        try:
            # Push 3 frames for turn 1 into AI output
            ai_port.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=0, turn_id=1))
            ai_port.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=1, turn_id=1))
            ai_port.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=2, turn_id=1))

            # Wait until queue is full
            while session.outbound_queue_depth < 2:
                await asyncio.sleep(0.005)

            assert session.outbound_queue_depth == 2

            # Interrupt session! Should immediately purge queue and release backpressure
            outcome = await session.interrupt()
            assert outcome.interrupted_turn_id == 1
            assert outcome.new_turn_id == 2
            assert outcome.locally_discarded_frames == 2
            assert session.outbound_queue_depth == 0

            tel_port.set_send_delay(0.0)
        finally:
            await session.close()

    asyncio.run(_test())


def test_scenario_04_discarding_pending_output_of_old_turn(frame_bytes: bytes) -> None:
    """Scenario 4: Discarding pending output of old turn."""

    async def _test() -> None:
        tel_port = InterruptibleFakeTelephonyAdapter()
        ai_port = InterruptibleFakeAIPort()

        session = VoiceBridgeSession(
            call_id="call-04",
            tenant_id="tenant-test",
            telephony_port=tel_port,
            ai_port=ai_port,
            queue_capacity=8,
        )

        # Queue frames before starting session
        ai_port.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=0, turn_id=1))
        ai_port.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=1, turn_id=1))

        # Interrupt in IDLE state
        outcome = await session.interrupt()
        assert outcome.interrupted_turn_id == 1
        assert outcome.new_turn_id == 2

        async with session:
            # AI sends 1 frame of old turn (suppressed) and 2 of new turn
            ai_port.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=2, turn_id=1))
            ai_port.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=3, turn_id=2))
            ai_port.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=4, turn_id=2))
            tel_port.push_eof()
            ai_port.push_eof()
            await session.wait_until_closed()

        assert session.stale_frames_suppressed == 3
        assert session.outbound_frames_transferred == 2
        assert len(tel_port.sent_frames) == 2
        assert [f.turn_id for f in tel_port.sent_frames] == [2, 2]

    asyncio.run(_test())


def test_scenario_05_correct_ordering_of_new_turn_frames(frame_bytes: bytes) -> None:
    """Scenario 5: Correct ordering of new turn frames."""

    async def _test() -> None:
        tel_port = InterruptibleFakeTelephonyAdapter()
        ai_port = InterruptibleFakeAIPort()

        session = VoiceBridgeSession(
            call_id="call-05",
            tenant_id="tenant-test",
            telephony_port=tel_port,
            ai_port=ai_port,
        )

        async with session:
            # Turn 1: 1 frame delivered
            ai_port.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=0, turn_id=1))
            while session.outbound_frames_transferred < 1:
                await asyncio.sleep(0.005)

            # Interruption
            await session.interrupt()
            assert session.current_turn_id == 2

            # Turn 2: 3 frames in exact order
            for seq in range(1, 4):
                ai_port.push_frame(
                    create_synthetic_pcm16le_frame(frame_bytes, sequence=seq, turn_id=2)
                )

            tel_port.push_eof()
            ai_port.push_eof()
            await session.wait_until_closed()

        assert session.outbound_frames_transferred == 4
        delivered_seqs = [f.sequence for f in tel_port.sent_frames]
        delivered_turns = [f.turn_id for f in tel_port.sent_frames]
        assert delivered_seqs == [0, 1, 2, 3]
        assert delivered_turns == [1, 2, 2, 2]

    asyncio.run(_test())


def test_scenario_06_outbound_send_waiting_during_interruption(frame_bytes: bytes) -> None:
    """Scenario 6: Outbound send waiting during interruption."""

    async def _test() -> None:
        tel_port = InterruptibleFakeTelephonyAdapter()
        tel_port.set_send_delay(0.05)
        ai_port = InterruptibleFakeAIPort()

        session = VoiceBridgeSession(
            call_id="call-06",
            tenant_id="tenant-test",
            telephony_port=tel_port,
            ai_port=ai_port,
            queue_capacity=4,
        )

        async with session:
            # Enqueue turn 1 frames
            ai_port.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=0, turn_id=1))
            ai_port.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=1, turn_id=1))
            ai_port.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=2, turn_id=1))

            # While forwarder is sending frame 0, trigger interruption
            await asyncio.sleep(0.01)
            outcome = await session.interrupt()
            assert outcome.interrupted_turn_id == 1
            assert outcome.new_turn_id == 2
            assert outcome.locally_discarded_frames >= 1

            # Turn 2 frames
            ai_port.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=3, turn_id=2))
            tel_port.set_send_delay(0.0)
            tel_port.push_eof()
            ai_port.push_eof()
            await session.wait_until_closed()

        assert session.state == VoiceBridgeState.CLOSED
        assert len(tel_port.sent_frames) <= 2
        assert tel_port.sent_frames[-1].turn_id == 2

    asyncio.run(_test())


def test_scenario_07_duplicate_idempotent_interruption(frame_bytes: bytes) -> None:
    """Scenario 7: Duplicate / idempotent interruption."""

    async def _test() -> None:
        tel_port = InterruptibleFakeTelephonyAdapter()
        ai_port = InterruptibleFakeAIPort()

        session = VoiceBridgeSession(
            call_id="call-07",
            tenant_id="tenant-test",
            telephony_port=tel_port,
            ai_port=ai_port,
        )

        async with session:
            # First interruption for turn 1
            out1 = await session.interrupt(turn_id=1)
            assert out1.interrupted_turn_id == 1
            assert out1.new_turn_id == 2
            assert session.current_turn_id == 2

            # Second interruption passing superseded turn_id=1 is idempotent no-op
            out2 = await session.interrupt(turn_id=1)
            assert out2.interrupted_turn_id == 1
            assert out2.new_turn_id == 2
            assert out2.locally_discarded_frames == 0
            assert out2.future_generation_cancelled is False
            assert session.current_turn_id == 2
            assert session.interruption_count == 1  # Not incremented

            # Trying to interrupt an unreached future turn raises VoiceDomainError
            with pytest.raises(VoiceDomainError, match="Cannot interrupt future turn 5"):
                await session.interrupt(turn_id=5)

            tel_port.push_eof()
            ai_port.push_eof()
            await session.wait_until_closed()

        assert session.state == VoiceBridgeState.CLOSED

    asyncio.run(_test())


def test_scenario_08_simultaneous_interruption_and_close(frame_bytes: bytes) -> None:
    """Scenario 8: Simultaneous interruption and close."""

    async def _test() -> None:
        tel_port = InterruptibleFakeTelephonyAdapter()
        ai_port = InterruptibleFakeAIPort()

        session = VoiceBridgeSession(
            call_id="call-08",
            tenant_id="tenant-test",
            telephony_port=tel_port,
            ai_port=ai_port,
        )

        await session.start()

        async def do_interrupt() -> Any:
            try:
                return await session.interrupt()
            except VoiceBridgeSessionClosedError as e:
                return e

        async def do_close() -> None:
            await session.close()

        res_int, _ = await asyncio.gather(do_interrupt(), do_close())
        assert isinstance(res_int, (InterruptionOutcome, VoiceBridgeSessionClosedError))
        assert session.state == VoiceBridgeState.CLOSED
        assert tel_port.is_closed is True
        assert ai_port.is_closed is True

    asyncio.run(_test())


def test_scenario_09_interruption_after_closed_or_failed_raises(frame_bytes: bytes) -> None:
    """Scenario 9: Interruption after FAILED/CLOSED state raises VoiceBridgeSessionClosedError."""

    async def _test() -> None:
        tel_port = InterruptibleFakeTelephonyAdapter()
        ai_port = InterruptibleFakeAIPort()

        session = VoiceBridgeSession(
            call_id="call-09",
            tenant_id="tenant-test",
            telephony_port=tel_port,
            ai_port=ai_port,
        )

        await session.start()
        await session.close()
        assert session.state == VoiceBridgeState.CLOSED

        with pytest.raises(
            VoiceBridgeSessionClosedError,
            match="Cannot interrupt VoiceBridgeSession in state closed",
        ):
            await session.interrupt()

    asyncio.run(_test())


def test_scenario_10_two_tenant_two_call_isolation(frame_bytes: bytes) -> None:
    """Scenario 10: Two-tenant / two-call isolation (Call A interruption does not affect Call B)."""

    async def _test() -> None:
        tel_a = InterruptibleFakeTelephonyAdapter(name="tel-a")
        ai_a = InterruptibleFakeAIPort(name="ai-a")
        session_a = VoiceBridgeSession(
            call_id="call-10-a",
            tenant_id="tenant-alpha",
            telephony_port=tel_a,
            ai_port=ai_a,
        )

        tel_b = InterruptibleFakeTelephonyAdapter(name="tel-b")
        ai_b = InterruptibleFakeAIPort(name="ai-b")
        session_b = VoiceBridgeSession(
            call_id="call-10-b",
            tenant_id="tenant-beta",
            telephony_port=tel_b,
            ai_port=ai_b,
        )

        async with session_a, session_b:
            ai_a.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=0, turn_id=1))
            ai_a.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=1, turn_id=1))

            ai_b.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=0, turn_id=1))
            ai_b.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=1, turn_id=1))

            # Interrupt ONLY Call A
            outcome_a = await session_a.interrupt()
            assert outcome_a.call_id == "call-10-a"
            assert session_a.current_turn_id == 2
            assert session_b.current_turn_id == 1  # Unaffected!

            # Feed turn 2 to Call A
            ai_a.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=2, turn_id=2))

            tel_a.push_eof()
            ai_a.push_eof()
            tel_b.push_eof()
            ai_b.push_eof()
            await session_a.wait_until_closed()
            await session_b.wait_until_closed()

        assert session_a.current_turn_id == 2
        assert session_b.current_turn_id == 1
        assert session_b.interruption_count == 0
        assert len(tel_b.sent_frames) == 2
        assert all(f.turn_id == 1 for f in tel_b.sent_frames)

    asyncio.run(_test())


def test_scenario_11_unsupported_interruption_capability_raises() -> None:
    """Scenario 11: Unsupported interruption capability raises UnsupportedInterruptionCapabilityError."""

    async def _test() -> None:
        caps_no_cancel = InterruptionCapabilities(can_cancel_future_generation=False)
        ai_port = InterruptibleFakeAIPort(capabilities=caps_no_cancel)
        tel_port = InterruptibleFakeTelephonyAdapter()

        session = VoiceBridgeSession(
            call_id="call-11",
            tenant_id="tenant-test",
            telephony_port=tel_port,
            ai_port=ai_port,
        )

        with pytest.raises(
            UnsupportedInterruptionCapabilityError,
            match="AI port does not support future generation cancellation",
        ):
            await session.interrupt(require_future_generation_cancellation=True)

        assert session.current_turn_id == 1
        assert session.interruption_count == 0

        caps_no_clear = InterruptionCapabilities(can_clear_provider_playback=False)
        tel_port_no_clear = InterruptibleFakeTelephonyAdapter(capabilities=caps_no_clear)
        session_2 = VoiceBridgeSession(
            call_id="call-11-b",
            tenant_id="tenant-test",
            telephony_port=tel_port_no_clear,
            ai_port=InterruptibleFakeAIPort(),
        )

        with pytest.raises(
            UnsupportedInterruptionCapabilityError,
            match="Telephony port does not support clearing provider playback",
        ):
            await session_2.interrupt(require_playback_clear=True)

        assert session_2.current_turn_id == 1

    asyncio.run(_test())


def test_scenario_12_provider_cancellation_failure_handled_safely(frame_bytes: bytes) -> None:
    """Scenario 12: Provider cancellation failure handled safely."""

    async def _test() -> None:
        ai_port = InterruptibleFakeAIPort(
            simulate_cancellation_error=RuntimeError("Upstream AI cancellation timeout")
        )
        tel_port = InterruptibleFakeTelephonyAdapter()

        session = VoiceBridgeSession(
            call_id="call-12",
            tenant_id="tenant-test",
            telephony_port=tel_port,
            ai_port=ai_port,
        )

        async with session:
            outcome = await session.interrupt(require_future_generation_cancellation=False)
            assert outcome.interrupted_turn_id == 1
            assert outcome.new_turn_id == 2
            assert outcome.future_generation_cancelled is False
            assert session.current_turn_id == 2

            ai_port._simulate_cancellation_error = RuntimeError("Upstream AI cancellation fatal")
            with pytest.raises(RuntimeError, match="Upstream AI cancellation fatal"):
                await session.interrupt(require_future_generation_cancellation=True)

            tel_port.push_eof()
            ai_port.push_eof()
            await session.wait_until_closed()

        assert session.state == VoiceBridgeState.CLOSED

    asyncio.run(_test())


def test_scenario_13_controlled_cleanup_without_leaks(frame_bytes: bytes) -> None:
    """Scenario 13: Controlled cleanup without leaks."""

    async def _test() -> None:
        tel_port = InterruptibleFakeTelephonyAdapter()
        ai_port = InterruptibleFakeAIPort()

        session = VoiceBridgeSession(
            call_id="call-13",
            tenant_id="tenant-test",
            telephony_port=tel_port,
            ai_port=ai_port,
        )

        async with session:
            ai_port.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=0, turn_id=1))
            await session.interrupt()
            tel_port.push_eof()
            ai_port.push_eof()
            await session.wait_until_closed()

        assert session.state == VoiceBridgeState.CLOSED
        assert tel_port.is_closed is True
        assert ai_port.is_closed is True
        assert session.outbound_queue_depth == 0
        assert session.inbound_queue_depth == 0

    asyncio.run(_test())


def test_scenario_14_no_orphan_background_tasks(frame_bytes: bytes) -> None:
    """Scenario 14: No orphan background tasks (asyncio.all_tasks())."""

    async def _test() -> None:
        initial_tasks = {t for t in asyncio.all_tasks() if not t.done()}

        tel_port = InterruptibleFakeTelephonyAdapter()
        ai_port = InterruptibleFakeAIPort()

        session = VoiceBridgeSession(
            call_id="call-14",
            tenant_id="tenant-test",
            telephony_port=tel_port,
            ai_port=ai_port,
        )

        async with session:
            ai_port.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=0, turn_id=1))
            await session.interrupt()
            tel_port.push_eof()
            ai_port.push_eof()
            await session.wait_until_closed()

        current_tasks = {t for t in asyncio.all_tasks() if not t.done()}
        leaked_tasks = [
            t for t in (current_tasks - initial_tasks) if t is not asyncio.current_task()
        ]
        assert not leaked_tasks, f"Leaked background tasks: {leaked_tasks}"

    asyncio.run(_test())


def test_scenario_15_existing_call_end_lossless_drain_unchanged(frame_bytes: bytes) -> None:
    """Scenario 15: Existing call.end lossless drain unchanged."""

    async def _test() -> None:
        tel_port = InterruptibleFakeTelephonyAdapter()
        ai_port = InterruptibleFakeAIPort()

        session = VoiceBridgeSession(
            call_id="call-15",
            tenant_id="tenant-test",
            telephony_port=tel_port,
            ai_port=ai_port,
        )

        async with session:
            for seq in range(5):
                ai_port.push_frame(
                    create_synthetic_pcm16le_frame(frame_bytes, sequence=seq, turn_id=1)
                )
            tel_port.push_eof()
            ai_port.push_eof()
            await session.wait_until_closed()

        assert session.state == VoiceBridgeState.CLOSED
        assert session.outbound_frames_transferred == 5
        assert len(tel_port.sent_frames) == 5
        assert [f.sequence for f in tel_port.sent_frames] == [0, 1, 2, 3, 4]
        assert session.locally_discarded_frames == 0
        assert session.stale_frames_suppressed == 0

    asyncio.run(_test())


# -----------------------------------------------------------------------------
# Detailed Gates A - F Verification Tests
# -----------------------------------------------------------------------------


def test_gate_a_send_race_linearization(frame_bytes: bytes) -> None:
    """GATE A: Interruption linearization point and send race correctness.

    Verifies that:
    1. A frame already handed to telephony send_frame is in-flight beyond Gateway queue ownership.
    2. Local queue purge drops buffered frames (locally_discarded_frames).
    3. Gateway does not falsely claim transport_pending_cancelled when transport lacks capability.
    4. Release of the send barrier completes cleanly without deadlock or orphan tasks.
    """

    async def _test() -> None:
        send_entered = asyncio.Event()
        send_release = asyncio.Event()

        class BarrierTelephonyAdapter(InterruptibleFakeTelephonyAdapter):
            async def send_frame(self, frame: AudioFrame) -> None:
                if frame.sequence == 0:
                    send_entered.set()
                    await send_release.wait()
                await super().send_frame(frame)

        caps = InterruptionCapabilities(
            can_cancel_transport_pending=False,
            can_clear_provider_playback=False,
        )
        tel_port = BarrierTelephonyAdapter(capabilities=caps)
        ai_port = InterruptibleFakeAIPort()

        session = VoiceBridgeSession(
            call_id="call-gate-a",
            tenant_id="tenant-gate-a",
            telephony_port=tel_port,
            ai_port=ai_port,
            queue_capacity=4,
        )

        await session.start()
        try:
            ai_port.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=0, turn_id=1))
            ai_port.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=1, turn_id=1))

            await send_entered.wait()
            assert session.outbound_queue_depth == 1

            outcome = await session.interrupt()
            assert outcome.interrupted_turn_id == 1
            assert outcome.new_turn_id == 2
            assert outcome.locally_discarded_frames == 1  # Frame 1 dropped
            assert outcome.transport_pending_cancelled is False  # Honest reporting
            assert outcome.provider_playback_cleared is False
            assert outcome.playback_confirmed is False
            assert session.outbound_queue_depth == 0

            # Release in-flight Frame 0
            send_release.set()

            # Push Frame 2 (turn 2)
            ai_port.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=2, turn_id=2))
            tel_port.push_eof()
            ai_port.push_eof()
            await session.wait_until_closed()

            assert session.state == VoiceBridgeState.CLOSED
            assert len(tel_port.sent_frames) == 2
            assert [f.sequence for f in tel_port.sent_frames] == [0, 2]
            assert [f.turn_id for f in tel_port.sent_frames] == [1, 2]
            assert session.locally_discarded_frames == 1
        finally:
            send_release.set()
            await session.close()

    asyncio.run(_test())


def test_gate_b_untagged_frame_policy(frame_bytes: bytes) -> None:
    """GATE B: Exact untagged frame policy across interruptions.

    Verifies:
    1. Legacy port emitting turn_id=None before interruption flows cleanly (backward compatible).
    2. Once interruption occurs (turn > 1), continuing untagged frames are suppressed as stale.
    3. Untagged frames are never silently upgraded to the new generation.
    4. New turn frames tagged with turn_id=2 are delivered normally.
    5. A legacy port without InterruptibleAIPort reports future_generation_cancelled=False.
    """

    async def _test() -> None:
        tel_port = InterruptibleFakeTelephonyAdapter()
        legacy_ai = FakeAIMediaAdapter(name="legacy-ai")

        session = VoiceBridgeSession(
            call_id="call-gate-b",
            tenant_id="tenant-gate-b",
            telephony_port=tel_port,
            ai_port=legacy_ai,
        )

        async with session:
            # 1. Before interruption: untagged frame delivered
            legacy_ai.push_frame(
                create_synthetic_pcm16le_frame(frame_bytes, sequence=0, turn_id=None)
            )
            while session.outbound_frames_transferred < 1:
                await asyncio.sleep(0.005)

            assert len(tel_port.sent_frames) == 1
            assert tel_port.sent_frames[0].turn_id is None

            # 2. Interruption occurs
            outcome = await session.interrupt()
            assert outcome.interrupted_turn_id == 1
            assert outcome.new_turn_id == 2
            assert outcome.future_generation_cancelled is False

            # 3. Legacy AI continues producing untagged frames: MUST BE SUPPRESSED
            legacy_ai.push_frame(
                create_synthetic_pcm16le_frame(frame_bytes, sequence=1, turn_id=None)
            )
            legacy_ai.push_frame(
                create_synthetic_pcm16le_frame(frame_bytes, sequence=2, turn_id=None)
            )

            # 4. Turn 2 frames with turn_id=2: DELIVERED
            legacy_ai.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=3, turn_id=2))

            tel_port.push_eof()
            legacy_ai.push_eof()
            await session.wait_until_closed()

        assert session.state == VoiceBridgeState.CLOSED
        assert session.stale_frames_suppressed == 2
        assert len(tel_port.sent_frames) == 2
        assert tel_port.sent_frames[0].sequence == 0
        assert tel_port.sent_frames[0].turn_id is None
        assert tel_port.sent_frames[1].sequence == 3
        assert tel_port.sent_frames[1].turn_id == 2

    asyncio.run(_test())


def test_gate_c_sequence_continuity(frame_bytes: bytes) -> None:
    """GATE C: Sequence continuity vs logical turn identity contracts.

    Verifies:
    1. strict_sequence=True: monotonic sequence across turns succeeds.
    2. strict_sequence=True: per-turn sequence reset raises OutOfOrderAudioFrameError.
    3. strict_sequence=False (default): per-turn sequence reset succeeds cleanly.
    4. Stale frames continuing to arrive do not corrupt new turn delivery.
    """

    async def _test_strict_monotonic() -> None:
        tel = InterruptibleFakeTelephonyAdapter()
        ai = InterruptibleFakeAIPort()
        session = VoiceBridgeSession(
            call_id="call-c-1",
            tenant_id="tenant-c",
            telephony_port=tel,
            ai_port=ai,
            strict_sequence=True,
        )
        async with session:
            ai.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=0, turn_id=1))
            while session.outbound_frames_transferred < 1:
                await asyncio.sleep(0.005)
            await session.interrupt()
            ai.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=1, turn_id=2))
            ai.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=2, turn_id=2))
            tel.push_eof()
            ai.push_eof()
            await session.wait_until_closed()

        assert session.state == VoiceBridgeState.CLOSED
        assert len(tel.sent_frames) == 3

    async def _test_strict_reset_fails() -> None:
        tel = InterruptibleFakeTelephonyAdapter()
        ai = InterruptibleFakeAIPort()
        session = VoiceBridgeSession(
            call_id="call-c-2",
            tenant_id="tenant-c",
            telephony_port=tel,
            ai_port=ai,
            strict_sequence=True,
        )
        async with session:
            ai.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=0, turn_id=1))
            while session.outbound_frames_transferred < 1:
                await asyncio.sleep(0.005)
            await session.interrupt()
            # Sequence reset to 0 under strict_sequence=True MUST fail cleanly
            ai.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=0, turn_id=2))
            tel.push_eof()
            ai.push_eof()
            with pytest.raises(OutOfOrderAudioFrameError):
                await session.wait_until_closed()

        assert session.state == VoiceBridgeState.FAILED

    async def _test_default_sequence_reset_succeeds() -> None:
        tel = InterruptibleFakeTelephonyAdapter()
        ai = InterruptibleFakeAIPort()
        session = VoiceBridgeSession(
            call_id="call-c-3",
            tenant_id="tenant-c",
            telephony_port=tel,
            ai_port=ai,
            strict_sequence=False,
        )
        async with session:
            ai.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=0, turn_id=1))
            while session.outbound_frames_transferred < 1:
                await asyncio.sleep(0.005)
            await session.interrupt()
            ai.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=0, turn_id=2))
            ai.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=1, turn_id=2))
            tel.push_eof()
            ai.push_eof()
            await session.wait_until_closed()

        assert session.state == VoiceBridgeState.CLOSED
        assert len(tel.sent_frames) == 3
        assert [f.sequence for f in tel.sent_frames] == [0, 0, 1]
        assert [f.turn_id for f in tel.sent_frames] == [1, 2, 2]

    asyncio.run(_test_strict_monotonic())
    asyncio.run(_test_strict_reset_fails())
    asyncio.run(_test_default_sequence_reset_succeeds())


def test_gate_d_queue_accounting_and_eof(frame_bytes: bytes) -> None:
    """GATE D: Queue task accounting, unfinished_tasks balance, and EOF sentinel preservation.

    Verifies:
    1. get_nowait() and task_done() remain strictly balanced during queue purge.
    2. queue.join() does not hang or complete prematurely.
    3. EOF sentinel is preserved across interruptions without duplication.
    """

    async def _test() -> None:
        tel = InterruptibleFakeTelephonyAdapter()
        ai = InterruptibleFakeAIPort()
        session = VoiceBridgeSession(
            call_id="call-gate-d",
            tenant_id="tenant-gate-d",
            telephony_port=tel,
            ai_port=ai,
            queue_capacity=8,
        )

        f1 = create_synthetic_pcm16le_frame(frame_bytes, sequence=0, turn_id=1)
        f2 = create_synthetic_pcm16le_frame(frame_bytes, sequence=1, turn_id=1)
        session._outbound_queue.put_nowait(f1)
        session._outbound_queue.put_nowait(f2)
        session._outbound_queue.put_nowait(None)  # EOF sentinel

        assert session.outbound_queue_depth == 3

        # Interrupt 1: drops 2 frames, preserves EOF
        outcome = await session.interrupt()
        assert outcome.locally_discarded_frames == 2
        assert session.outbound_queue_depth == 1

        # Interrupt 2: preserves EOF, 0 discarded
        outcome_2 = await session.interrupt()
        assert outcome_2.locally_discarded_frames == 0
        assert session.outbound_queue_depth == 1

        # Drain the single preserved EOF sentinel
        item = session._outbound_queue.get_nowait()
        session._outbound_queue.task_done()
        assert item is None
        assert session._outbound_queue.empty()

        # queue.join() MUST complete immediately because unfinished_tasks == 0
        await asyncio.wait_for(session._outbound_queue.join(), timeout=0.1)

    asyncio.run(_test())


def test_gate_f_audio_frame_public_compatibility(frame_bytes: bytes) -> None:
    """GATE F: Public AudioFrame dataclass compatibility and invariant preservation."""
    # 1. Positional construction (6 args: backwards compatible)
    f6 = AudioFrame(frame_bytes, "pcm16le", 16000, 1, 0, 0.0)
    assert f6.turn_id is None

    # 2. Positional construction (7 args)
    f7 = AudioFrame(frame_bytes, "pcm16le", 16000, 1, 1, 20.0, 42)
    assert f7.turn_id == 42

    # 3. Slots behavior (no __dict__)
    assert not hasattr(f7, "__dict__")

    # 4. Frozen immutability
    with pytest.raises(dataclasses.FrozenInstanceError):
        f7.turn_id = 99  # type: ignore[misc]

    # 5. Equality semantics
    f7_same = AudioFrame(frame_bytes, "pcm16le", 16000, 1, 1, 20.0, 42)
    f7_diff = AudioFrame(frame_bytes, "pcm16le", 16000, 1, 1, 20.0, 43)
    assert f7 == f7_same
    assert f7 != f7_diff

    # 6. Dataclass asdict serialization
    d = dataclasses.asdict(f7)
    assert d["turn_id"] == 42
    assert d["sequence"] == 1

    # 7. BoundedFakeAIEchoPort preserves turn_id
    async def _test_echo_port() -> None:
        port = BoundedFakeAIEchoPort()
        await port.send_frame(f7)
        echoed = await port.receive_frame()
        assert echoed is not None
        assert echoed.turn_id == 42
        assert echoed.sequence == 1
        await port.close()

    asyncio.run(_test_echo_port())


# -----------------------------------------------------------------------------
# Remediation Gates A - D Tests
# -----------------------------------------------------------------------------


def test_remediation_gate_a_playback_confirmed_strictly_false(frame_bytes: bytes) -> None:
    """GATE A: Playback acknowledgment honesty.

    Verifies that InterruptionOutcome.playback_confirmed is strictly False, even
    if telephony adapter declares can_observe_playback_acks=True, because M4.2B.1
    has no remote wire ACK confirmation protocol.
    """

    async def _test() -> None:
        caps = InterruptionCapabilities(
            can_cancel_transport_pending=True,
            can_clear_provider_playback=True,
            can_observe_playback_acks=True,
            cannot_confirm_playback=False,
        )
        tel_port = InterruptibleFakeTelephonyAdapter(capabilities=caps)
        ai_port = InterruptibleFakeAIPort()

        session = VoiceBridgeSession(
            call_id="call-gate-a",
            tenant_id="tenant-gate-a",
            telephony_port=tel_port,
            ai_port=ai_port,
        )

        async with session:
            outcome = await session.interrupt()
            assert outcome.playback_confirmed is False
            assert outcome.provider_playback_cleared is True
            assert outcome.transport_pending_cancelled is True
            assert tel_port.clear_count == 1

            tel_port.push_eof()
            ai_port.push_eof()
            await session.wait_until_closed()

    asyncio.run(_test())


def test_remediation_gate_b_required_failure_and_retry(frame_bytes: bytes) -> None:
    """GATE B: Required cancellation/clear failure semantics and retryability.

    Verifies that:
    1. If require_future_generation_cancellation=True fails, exception propagates,
       turn does NOT advance, interruption_count does NOT increment, queue is not purged,
       and a subsequent retry with the same turn_id succeeds without being marked stale.
    2. If require_playback_clear=True fails, exception propagates, turn does not advance,
       and retry succeeds.
    3. If AI cancel succeeds but telephony clear fails with require_playback_clear=True,
       turn does not advance, and retry succeeds.
    """

    async def _test() -> None:
        tel_port = InterruptibleFakeTelephonyAdapter()
        ai_port = InterruptibleFakeAIPort()

        session = VoiceBridgeSession(
            call_id="call-gate-b",
            tenant_id="tenant-gate-b",
            telephony_port=tel_port,
            ai_port=ai_port,
        )

        async with session:
            # 1. AI cancellation failure with require_future_generation_cancellation=True
            ai_port._simulate_cancellation_error = RuntimeError("Upstream AI cancel failed")
            with pytest.raises(RuntimeError, match="Upstream AI cancel failed"):
                await session.interrupt(turn_id=1, require_future_generation_cancellation=True)

            assert session.current_turn_id == 1
            assert session.interruption_count == 0

            # Retry turn 1 after error is cleared -> MUST succeed and advance to turn 2
            outcome = await session.interrupt(
                turn_id=1, require_future_generation_cancellation=True
            )
            assert outcome.interrupted_turn_id == 1
            assert outcome.new_turn_id == 2
            assert outcome.future_generation_cancelled is True
            assert session.current_turn_id == 2
            assert session.interruption_count == 1

            # 2. Telephony clear failure with require_playback_clear=True
            tel_port._simulate_clear_error = RuntimeError("Telephony clear buffer failed")
            with pytest.raises(RuntimeError, match="Telephony clear buffer failed"):
                await session.interrupt(turn_id=2, require_playback_clear=True)

            assert session.current_turn_id == 2
            assert session.interruption_count == 1

            # Retry turn 2 after error cleared -> MUST succeed and advance to turn 3
            outcome_2 = await session.interrupt(turn_id=2, require_playback_clear=True)
            assert outcome_2.interrupted_turn_id == 2
            assert outcome_2.new_turn_id == 3
            assert outcome_2.provider_playback_cleared is True
            assert session.current_turn_id == 3
            assert session.interruption_count == 2

            tel_port.push_eof()
            ai_port.push_eof()
            await session.wait_until_closed()

    asyncio.run(_test())


def test_remediation_gate_c_future_turn_frame_rejected(frame_bytes: bytes) -> None:
    """GATE C: Future turn frame rejection in outbound pump.

    Verifies that:
    1. If upstream AI emits an AudioFrame tagged with a future turn
       (turn_id > current_turn_id), the outbound pump raises FutureTurnAudioFrameError,
       the session transitions to FAILED, and the frame is never forwarded to telephony.
    2. Once a turn is properly reached via session.interrupt(), frames matching
       that turn_id are accepted and transferred cleanly.
    """

    async def _test() -> None:
        tel_port = InterruptibleFakeTelephonyAdapter()
        ai_port = InterruptibleFakeAIPort()

        session = VoiceBridgeSession(
            call_id="call-gate-c",
            tenant_id="tenant-gate-c",
            telephony_port=tel_port,
            ai_port=ai_port,
        )

        await session.start()
        assert session.current_turn_id == 1

        # AI emits frame tagged with turn_id=2 before turn 2 has started locally
        bad_future_frame = create_synthetic_pcm16le_frame(
            frame_bytes,
            sequence=0,
            timestamp_ms=0.0,
            turn_id=2,
        )
        ai_port.push_frame(bad_future_frame)

        # Wait for supervisor to observe failure; wait_until_closed propagates session.error
        with pytest.raises(
            FutureTurnAudioFrameError, match="Received audio frame for future turn 2"
        ):
            await session.wait_until_closed()

        assert session.state == VoiceBridgeState.FAILED
        assert isinstance(session.error, FutureTurnAudioFrameError)
        assert "future turn 2" in str(session.error)
        assert tel_port.send_count == 0

        # Now verify normal turn progression: Turn 2 frame accepted after interrupt() advances to Turn 2
        tel_port_valid = InterruptibleFakeTelephonyAdapter()
        ai_port_valid = InterruptibleFakeAIPort()
        session_valid = VoiceBridgeSession(
            call_id="call-gate-c-valid",
            tenant_id="tenant-gate-c",
            telephony_port=tel_port_valid,
            ai_port=ai_port_valid,
        )

        async with session_valid:
            assert session_valid.current_turn_id == 1
            await session_valid.interrupt()
            assert session_valid.current_turn_id == 2

            # Now turn_id=2 is the current turn -> accepted!
            valid_turn2_frame = create_synthetic_pcm16le_frame(
                frame_bytes,
                sequence=0,
                timestamp_ms=0.0,
                turn_id=2,
            )
            ai_port_valid.push_frame(valid_turn2_frame)
            tel_port_valid.push_eof()
            ai_port_valid.push_eof()
            await session_valid.wait_until_closed()

        assert session_valid.state == VoiceBridgeState.CLOSED
        assert tel_port_valid.send_count == 1

    asyncio.run(_test())


def test_remediation_gate_d_provider_timeout_and_close_race(frame_bytes: bytes) -> None:
    """GATE D: Provider timeout enforcement and close race during interruption.

    Verifies that:
    1. A hanging provider cancel/clear operation aborts after timeout with TimeoutError.
    2. Invalid timeout parameters raise VoiceDomainError.
    3. If session is closed while awaiting provider operations, interrupt() aborts
       with VoiceBridgeSessionClosedError and does not advance turn_id.
    """

    async def _test() -> None:
        # 1. Timeout enforcement
        class HangingAIPort(InterruptibleFakeAIPort):
            def __init__(self) -> None:
                super().__init__()
                self.entered_cancel = asyncio.Event()

            async def cancel_turn(self, turn_id: int) -> None:
                self.entered_cancel.set()
                await asyncio.Event().wait()  # Never completes

        hanging_ai = HangingAIPort()
        tel_port = InterruptibleFakeTelephonyAdapter()

        session = VoiceBridgeSession(
            call_id="call-gate-d-1",
            tenant_id="tenant-gate-d-1",
            telephony_port=tel_port,
            ai_port=hanging_ai,
        )

        async with session:
            # Invalid timeout values
            with pytest.raises(VoiceDomainError, match="timeout must be a positive finite float"):
                await session.interrupt(timeout=-1.0)
            with pytest.raises(VoiceDomainError, match="timeout must be a positive finite float"):
                await session.interrupt(timeout=0.0)
            with pytest.raises(VoiceDomainError, match="timeout must be a positive finite float"):
                await session.interrupt(timeout=float("nan"))
            with pytest.raises(VoiceDomainError, match="timeout must be a positive finite float"):
                await session.interrupt(timeout=True)
            with pytest.raises(VoiceDomainError, match="timeout must be a positive finite float"):
                await session.interrupt(timeout="fast")  # type: ignore[arg-type]

            # Bounded timeout triggers TimeoutError
            with pytest.raises(TimeoutError):
                await session.interrupt(timeout=0.05, require_future_generation_cancellation=True)

            assert session.current_turn_id == 1
            assert session.interruption_count == 0

            tel_port.push_eof()
            hanging_ai.push_eof()
            await session.wait_until_closed()

        # 2. Close race during interruption
        class CloseRaceAIPort(InterruptibleFakeAIPort):
            def __init__(self) -> None:
                super().__init__()
                self.cancel_started = asyncio.Event()

            async def cancel_turn(self, turn_id: int) -> None:
                self.cancel_started.set()
                # Yield control to let close() run
                await asyncio.sleep(0.02)

        tel_port_2 = InterruptibleFakeTelephonyAdapter()
        race_ai = CloseRaceAIPort()

        session_2 = VoiceBridgeSession(
            call_id="call-gate-d-2",
            tenant_id="tenant-gate-d-2",
            telephony_port=tel_port_2,
            ai_port=race_ai,
        )

        await session_2.start()

        async def _do_close() -> None:
            await race_ai.cancel_started.wait()
            # Session closed while cancel_turn was in flight
            await session_2.close()

        race_task = asyncio.create_task(_do_close())
        with pytest.raises(VoiceBridgeSessionClosedError):
            await session_2.interrupt()

        await race_task
        assert session_2.current_turn_id == 1
        assert session_2.interruption_count == 0
        assert session_2.state == VoiceBridgeState.CLOSED

    asyncio.run(_test())


# -----------------------------------------------------------------------------
# Final Reliability Gates: Local Output Fence, Partial Retry & Boundaries
# -----------------------------------------------------------------------------


def test_gate_a_immediate_local_output_fence_deterministic_reproduction(
    frame_bytes: bytes,
) -> None:
    """GATE A: Immediate local output fence prevents forwarding buffered frames during slow AI cancellation.

    Verifies the 9-step deterministic barrier reproduction:
    1. Start Turn 1.
    2. Hold Frame 0 in telephony send_frame().
    3. Place additional Turn 1 frames (Frame 1, Frame 2) in outbound queue.
    4. Start interrupt(turn_id=1).
    5. Block AI cancel_turn() with an Event barrier.
    6. Release in-flight Frame 0 send.
    7. Observe that NO buffered Turn 1 frames (Frame 1, 2) are forwarded while cancel_turn() remains blocked.
    8. Release cancel_turn().
    9. Complete interruption; verify only the non-retractable in-flight Frame 0 was sent,
       while buffered frames were discarded and suppressed.
    """

    async def _test() -> None:
        send_entered = asyncio.Event()
        send_release = asyncio.Event()

        class BarrierTelephony(InterruptibleFakeTelephonyAdapter):
            async def send_frame(self, frame: AudioFrame) -> None:
                if frame.sequence == 0:
                    send_entered.set()
                    await send_release.wait()
                await super().send_frame(frame)

        ai_cancel_entered = asyncio.Event()
        ai_cancel_release = asyncio.Event()

        class BlockingAIPort(InterruptibleFakeAIPort):
            async def cancel_turn(self, turn_id: int) -> None:
                ai_cancel_entered.set()
                await ai_cancel_release.wait()
                await super().cancel_turn(turn_id)

        tel_port = BarrierTelephony()
        ai_port = BlockingAIPort()

        session = VoiceBridgeSession(
            call_id="call-gate-a-repro",
            tenant_id="tenant-test",
            telephony_port=tel_port,
            ai_port=ai_port,
            queue_capacity=8,
        )

        await session.start()
        try:
            # 1. Start Turn 1, push Frame 0 (will block in send_frame)
            f0 = create_synthetic_pcm16le_frame(frame_bytes, sequence=0, turn_id=1)
            ai_port.push_frame(f0)
            await send_entered.wait()
            # Frame 0 is in-flight in telephony transport

            # 2. Place additional Turn 1 frames in _outbound_queue
            f1 = create_synthetic_pcm16le_frame(frame_bytes, sequence=1, turn_id=1)
            f2 = create_synthetic_pcm16le_frame(frame_bytes, sequence=2, turn_id=1)
            ai_port.push_frame(f1)
            ai_port.push_frame(f2)

            # Wait briefly to ensure f1 and f2 are queued in outbound_queue
            while session.outbound_queue_depth < 2:
                await asyncio.sleep(0.005)

            assert session.outbound_queue_depth == 2

            # 4. Start interrupt(turn_id=1) as a concurrent task
            interrupt_task = asyncio.create_task(
                session.interrupt(turn_id=1, require_future_generation_cancellation=True)
            )

            # 5. Wait until AI cancel_turn() is entered and blocked
            await ai_cancel_entered.wait()

            # At this point, the immediate fence is active and outbound queue was purged!
            assert session.fenced_turn_id == 1
            assert session.outbound_queue_depth == 0

            # 6. Release in-flight send
            send_release.set()

            # 7. Yield event loop and verify NO buffered Turn 1 frames are sent while cancellation remains blocked
            await asyncio.sleep(0.02)
            assert tel_port.send_count == 1
            assert [f.sequence for f in tel_port.sent_frames] == [0]

            # Even if AI emits more Turn 1 frames while cancellation is blocked:
            f3 = create_synthetic_pcm16le_frame(frame_bytes, sequence=3, turn_id=1)
            ai_port.push_frame(f3)
            await asyncio.sleep(0.02)
            assert tel_port.send_count == 1  # Still only Frame 0!

            # 8. Release cancellation
            ai_cancel_release.set()

            # 9. Complete interruption
            outcome = await interrupt_task
            assert outcome.interrupted_turn_id == 1
            assert outcome.new_turn_id == 2
            assert outcome.future_generation_cancelled is True
            assert outcome.locally_discarded_frames >= 2  # f1 and f2 discarded from queue

            # Post-interruption: new turn (Turn 2) frame is delivered cleanly
            f4 = create_synthetic_pcm16le_frame(frame_bytes, sequence=4, turn_id=2)
            ai_port.push_frame(f4)

            tel_port.push_eof()
            ai_port.push_eof()
            await session.wait_until_closed()

            assert session.state == VoiceBridgeState.CLOSED
            # Only Frame 0 (in-flight non-retractable) and Frame 4 (Turn 2) reached telephony!
            assert [f.sequence for f in tel_port.sent_frames] == [0, 4]
            assert [f.turn_id for f in tel_port.sent_frames] == [1, 2]
        finally:
            send_release.set()
            ai_cancel_release.set()
            await session.close()

    asyncio.run(_test())


def test_gate_b_partial_provider_success_and_retry_with_non_idempotent_provider(
    frame_bytes: bytes,
) -> None:
    """GATE B: Partial provider success, non-duplicate cancellation on retry, and local fence retention."""

    async def _test() -> None:
        class NonIdempotentAIPort(InterruptibleFakeAIPort):
            def __init__(self) -> None:
                super().__init__()
                self.cancel_call_count = 0

            async def cancel_turn(self, turn_id: int) -> None:
                self.cancel_call_count += 1
                if self.cancel_call_count > 1:
                    raise RuntimeError(
                        f"Duplicate cancel_turn({turn_id}) rejected by non-idempotent AI provider"
                    )
                await super().cancel_turn(turn_id)

        tel_port = InterruptibleFakeTelephonyAdapter()
        ai_port = NonIdempotentAIPort()

        session = VoiceBridgeSession(
            call_id="call-gate-b-partial",
            tenant_id="tenant-test",
            telephony_port=tel_port,
            ai_port=ai_port,
            queue_capacity=8,
        )

        await session.start()
        try:
            # Queue Turn 1 frames
            ai_port.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=0, turn_id=1))
            ai_port.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=1, turn_id=1))

            # Simulate telephony clear failure on first attempt
            tel_port._simulate_clear_error = RuntimeError(
                "Telephony clear provider playback failed"
            )

            with pytest.raises(RuntimeError, match="Telephony clear provider playback failed"):
                await session.interrupt(
                    turn_id=1,
                    require_future_generation_cancellation=True,
                    require_playback_clear=True,
                )

            # Assert partial state:
            # 1. AI cancel was called once and recorded as partial success
            assert ai_port.cancel_call_count == 1
            # 2. Turn did NOT advance (no false-success)
            assert session.current_turn_id == 1
            assert session.interruption_count == 0
            # 3. Local fence remains active throughout partial failure
            assert session.fenced_turn_id == 1
            # 4. Outbound queue was purged: no stale audio sent to telephony
            assert session.outbound_queue_depth == 0
            assert tel_port.send_count == 0

            # 5. AI pushing more Turn 1 frames during partial failure state: suppressed by fence!
            ai_port.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=2, turn_id=1))
            await asyncio.sleep(0.02)
            assert tel_port.send_count == 0

            # 6. Caller retries interruption of the exact same turn (turn_id=1)
            # Telephony error was cleared automatically by _simulate_clear_error
            outcome = await session.interrupt(
                turn_id=1,
                require_future_generation_cancellation=True,
                require_playback_clear=True,
            )

            # 7. Verified: AI cancel was NOT repeated (cancel_call_count is still 1)
            assert ai_port.cancel_call_count == 1
            # 8. Interruption now succeeds and advances turn
            assert outcome.interrupted_turn_id == 1
            assert outcome.new_turn_id == 2
            assert outcome.future_generation_cancelled is True
            assert outcome.provider_playback_cleared is True
            assert session.current_turn_id == 2
            assert session.interruption_count == 1

            # 9. Deliver Turn 2 frame
            ai_port.push_frame(create_synthetic_pcm16le_frame(frame_bytes, sequence=3, turn_id=2))
            tel_port.push_eof()
            ai_port.push_eof()
            await session.wait_until_closed()

            assert session.state == VoiceBridgeState.CLOSED
            assert [f.turn_id for f in tel_port.sent_frames] == [2]
        finally:
            await session.close()

    asyncio.run(_test())


def test_gate_b_required_ai_cancel_failure_before_success(frame_bytes: bytes) -> None:
    """GATE B: Required AI cancellation failure before success allows retry once healed."""

    async def _test() -> None:
        tel_port = InterruptibleFakeTelephonyAdapter()
        ai_port = InterruptibleFakeAIPort()

        session = VoiceBridgeSession(
            call_id="call-gate-b-ai-fail",
            tenant_id="tenant-test",
            telephony_port=tel_port,
            ai_port=ai_port,
        )

        async with session:
            ai_port._simulate_cancellation_error = RuntimeError("AI backend unavailable")
            with pytest.raises(RuntimeError, match="AI backend unavailable"):
                await session.interrupt(turn_id=1, require_future_generation_cancellation=True)

            assert session.current_turn_id == 1
            assert session.interruption_count == 0
            assert session.fenced_turn_id == 1

            # Retry after AI recovers
            outcome = await session.interrupt(
                turn_id=1, require_future_generation_cancellation=True
            )
            assert outcome.interrupted_turn_id == 1
            assert outcome.new_turn_id == 2
            assert outcome.future_generation_cancelled is True
            assert session.current_turn_id == 2

            tel_port.push_eof()
            ai_port.push_eof()
            await session.wait_until_closed()

    asyncio.run(_test())


def test_gate_c_caller_cancellation_and_timeout_boundaries(frame_bytes: bytes) -> None:
    """GATE C: Caller cancellation, shared timeout after partial success, and concurrent close."""

    async def _test() -> None:
        # 1. Caller cancellation during AI cancel
        cancel_started = asyncio.Event()

        class CancellableAIPort(InterruptibleFakeAIPort):
            async def cancel_turn(self, turn_id: int) -> None:
                cancel_started.set()
                await asyncio.sleep(10.0)  # Long wait

        tel_1 = InterruptibleFakeTelephonyAdapter()
        ai_1 = CancellableAIPort()
        session_1 = VoiceBridgeSession(
            call_id="call-c-cancel-1",
            tenant_id="tenant-test",
            telephony_port=tel_1,
            ai_port=ai_1,
        )

        await session_1.start()
        try:
            task = asyncio.create_task(
                session_1.interrupt(turn_id=1, require_future_generation_cancellation=True)
            )
            await cancel_started.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

            assert session_1.current_turn_id == 1
            assert session_1.fenced_turn_id == 1
        finally:
            await session_1.close()

        # 2. Caller cancellation after AI cancel but before telephony clear
        ai_done = asyncio.Event()
        tel_hold = asyncio.Event()

        class SteppedTelPort(InterruptibleFakeTelephonyAdapter):
            async def clear_pending_output(self) -> None:
                ai_done.set()
                await tel_hold.wait()
                await super().clear_pending_output()

        class NonIdempotentAI2(InterruptibleFakeAIPort):
            def __init__(self) -> None:
                super().__init__()
                self.calls = 0

            async def cancel_turn(self, turn_id: int) -> None:
                self.calls += 1
                if self.calls > 1:
                    raise RuntimeError("Duplicate cancel_turn")
                await super().cancel_turn(turn_id)

        tel_2 = SteppedTelPort()
        ai_2 = NonIdempotentAI2()
        session_2 = VoiceBridgeSession(
            call_id="call-c-cancel-2",
            tenant_id="tenant-test",
            telephony_port=tel_2,
            ai_port=ai_2,
        )

        await session_2.start()
        try:
            task2 = asyncio.create_task(
                session_2.interrupt(
                    turn_id=1,
                    require_future_generation_cancellation=True,
                    require_playback_clear=True,
                )
            )
            await ai_done.wait()
            # AI cancel completed, now task is waiting inside clear_pending_output
            task2.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task2

            assert ai_2.calls == 1
            assert session_2.current_turn_id == 1
            assert session_2.fenced_turn_id == 1

            # Retrying the interruption: AI cancel is not repeated!
            tel_hold.set()
            outcome = await session_2.interrupt(
                turn_id=1,
                require_future_generation_cancellation=True,
                require_playback_clear=True,
            )
            assert ai_2.calls == 1  # No duplicate cancel!
            assert outcome.interrupted_turn_id == 1
            assert outcome.new_turn_id == 2
        finally:
            tel_hold.set()
            await session_2.close()

        # 3. Provider timeout under shared deadline after partial success
        class HangingTelPort(InterruptibleFakeTelephonyAdapter):
            async def clear_pending_output(self) -> None:
                await asyncio.sleep(10.0)

        ai_3 = NonIdempotentAI2()
        tel_3 = HangingTelPort()
        session_3 = VoiceBridgeSession(
            call_id="call-c-timeout-shared",
            tenant_id="tenant-test",
            telephony_port=tel_3,
            ai_port=ai_3,
        )

        await session_3.start()
        try:
            # AI cancel takes ~0s, telephony clear hangs -> overall timeout 0.05s triggers TimeoutError
            with pytest.raises(TimeoutError):
                await session_3.interrupt(turn_id=1, require_playback_clear=True, timeout=0.05)

            assert ai_3.calls == 1  # AI cancel completed before timeout
            assert session_3.current_turn_id == 1
            assert session_3.fenced_turn_id == 1
        finally:
            await session_3.close()

        # 4. Concurrent close during partial failure leaves zero orphan tasks
        tel_4 = InterruptibleFakeTelephonyAdapter()
        ai_4 = InterruptibleFakeAIPort()
        session_4 = VoiceBridgeSession(
            call_id="call-c-close-leak",
            tenant_id="tenant-test",
            telephony_port=tel_4,
            ai_port=ai_4,
        )
        await session_4.start()
        # Cause partial failure
        tel_4._simulate_clear_error = RuntimeError("Clear failed")
        with pytest.raises(RuntimeError):
            await session_4.interrupt(require_playback_clear=True)
        # Close session
        await session_4.close()
        assert session_4.state == VoiceBridgeState.CLOSED

        # Check for orphan background tasks
        all_tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        voice_tasks = [t for t in all_tasks if "voice-" in t.get_name()]
        assert len(voice_tasks) == 0

    asyncio.run(_test())


def test_gate_c_duplicate_interrupt_concurrency_and_eof(frame_bytes: bytes) -> None:
    """GATE C: Duplicate concurrent interrupt requests and EOF during interruption."""

    async def _test() -> None:
        ai_cancel_barrier = asyncio.Event()

        class SlowAIPort(InterruptibleFakeAIPort):
            async def cancel_turn(self, turn_id: int) -> None:
                await ai_cancel_barrier.wait()
                await super().cancel_turn(turn_id)

        tel = InterruptibleFakeTelephonyAdapter()
        ai = SlowAIPort()
        session = VoiceBridgeSession(
            call_id="call-c-dup-eof",
            tenant_id="tenant-test",
            telephony_port=tel,
            ai_port=ai,
        )

        await session.start()
        try:
            # Task 1 starts interruption
            task1 = asyncio.create_task(session.interrupt(turn_id=1))
            # Yield so task1 acquires _interruption_lock and blocks on cancel_turn
            await asyncio.sleep(0.01)

            # AI pushes EOF sentinel during interruption
            ai.push_eof()

            # Task 2 calls interrupt for the same turn concurrently (will await lock)
            task2 = asyncio.create_task(session.interrupt(turn_id=1))

            # Unblock AI cancellation
            ai_cancel_barrier.set()

            res1 = await task1
            res2 = await task2

            # Task 1 executed the interruption and advanced turn to 2
            assert res1.interrupted_turn_id == 1
            assert res1.new_turn_id == 2
            # Task 2 saw turn 1 was already completed, returned idempotent result
            assert res2.interrupted_turn_id == 1
            assert res2.new_turn_id == 2
            assert res2.locally_discarded_frames == 0

            # EOF is preserved and session finishes cleanly
            tel.push_eof()
            await session.wait_until_closed()
            assert session.state == VoiceBridgeState.CLOSED
        finally:
            ai_cancel_barrier.set()
            await session.close()

    asyncio.run(_test())
