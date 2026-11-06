"""Comprehensive unit and negative tests for VoiceBridgeSession."""

from __future__ import annotations

import asyncio

import pytest

from kaiwen_gateway.modules.voice.adapters.fake import (
    FakeAIMediaAdapter,
    FakeTelephonyMediaAdapter,
)
from kaiwen_gateway.modules.voice.bridge import (
    VoiceBridgeSession,
    VoiceBridgeState,
)
from kaiwen_gateway.modules.voice.domain import (
    InvalidAudioFrameError,
    MediaBackpressureOverflowError,
    OutOfOrderAudioFrameError,
    VoiceBridgeSessionClosedError,
    VoiceDomainError,
    create_synthetic_pcm16le_frame,
)


class TestVoiceBridgeSessionInitialization:
    def test_valid_init(self) -> None:
        tel = FakeTelephonyMediaAdapter()
        ai = FakeAIMediaAdapter()
        session = VoiceBridgeSession(
            call_id="call-1",
            tenant_id="tenant-1",
            telephony_port=tel,
            ai_port=ai,
            queue_capacity=10,
            backpressure_timeout=2.0,
            strict_sequence=True,
        )
        assert session.call_id == "call-1"
        assert session.tenant_id == "tenant-1"
        assert session.state == VoiceBridgeState.IDLE
        assert session.inbound_queue_depth == 0
        assert session.outbound_queue_depth == 0
        assert session.inbound_frames_transferred == 0
        assert session.outbound_frames_transferred == 0
        assert not session.inbound_completed
        assert not session.outbound_completed
        assert session.error is None

    @pytest.mark.parametrize("invalid_call_id", ["", "   ", None, 123])
    def test_invalid_call_id(self, invalid_call_id: object) -> None:
        tel = FakeTelephonyMediaAdapter()
        ai = FakeAIMediaAdapter()
        with pytest.raises(VoiceDomainError, match="call_id"):
            VoiceBridgeSession(
                call_id=invalid_call_id,  # type: ignore[arg-type]
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
            )

    @pytest.mark.parametrize("invalid_tenant_id", ["", "   ", None, 123])
    def test_invalid_tenant_id(self, invalid_tenant_id: object) -> None:
        tel = FakeTelephonyMediaAdapter()
        ai = FakeAIMediaAdapter()
        with pytest.raises(VoiceDomainError, match="tenant_id"):
            VoiceBridgeSession(
                call_id="call-1",
                tenant_id=invalid_tenant_id,  # type: ignore[arg-type]
                telephony_port=tel,
                ai_port=ai,
            )

    @pytest.mark.parametrize("invalid_capacity", [0, -1, "8", None])
    def test_invalid_queue_capacity(self, invalid_capacity: object) -> None:
        tel = FakeTelephonyMediaAdapter()
        ai = FakeAIMediaAdapter()
        with pytest.raises(VoiceDomainError, match="queue_capacity"):
            VoiceBridgeSession(
                call_id="call-1",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
                queue_capacity=invalid_capacity,  # type: ignore[arg-type]
            )

    @pytest.mark.parametrize("invalid_timeout", [0.0, -1.0, -0.001, "timeout"])
    def test_invalid_backpressure_timeout(self, invalid_timeout: object) -> None:
        tel = FakeTelephonyMediaAdapter()
        ai = FakeAIMediaAdapter()
        with pytest.raises(VoiceDomainError, match="backpressure_timeout"):
            VoiceBridgeSession(
                call_id="call-1",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
                backpressure_timeout=invalid_timeout,  # type: ignore[arg-type]
            )


class TestVoiceBridgeLifecycleAndStreaming:
    def test_bidirectional_streaming_success(self) -> None:
        async def _test() -> None:
            tel = FakeTelephonyMediaAdapter()
            ai = FakeAIMediaAdapter()
            session = VoiceBridgeSession(
                call_id="call-bio-1",
                tenant_id="tenant-a",
                telephony_port=tel,
                ai_port=ai,
                queue_capacity=8,
            )

            inbound_frames = [
                create_synthetic_pcm16le_frame(b"IN_0", sequence=0, timestamp_ms=0.0),
                create_synthetic_pcm16le_frame(b"IN_1", sequence=1, timestamp_ms=20.0),
            ]
            outbound_frames = [
                create_synthetic_pcm16le_frame(b"OUT_0", sequence=0, timestamp_ms=0.0),
                create_synthetic_pcm16le_frame(b"OUT_1", sequence=1, timestamp_ms=20.0),
            ]

            for f in inbound_frames:
                tel.push_frame(f)
            tel.push_eof()

            for f in outbound_frames:
                ai.push_frame(f)
            ai.push_eof()

            await session.run()

            assert session.state == VoiceBridgeState.CLOSED
            assert session.inbound_completed
            assert session.outbound_completed
            assert session.inbound_frames_transferred == 2
            assert session.outbound_frames_transferred == 2
            assert ai.sent_frames == inbound_frames
            assert tel.sent_frames == outbound_frames
            assert tel.is_closed
            assert ai.is_closed

        asyncio.run(_test())

    def test_async_context_manager(self) -> None:
        async def _test() -> None:
            tel = FakeTelephonyMediaAdapter()
            ai = FakeAIMediaAdapter()
            f = create_synthetic_pcm16le_frame(b"PCM", sequence=0)
            tel.push_frame(f)
            tel.push_eof()
            ai.push_eof()

            async with VoiceBridgeSession(
                call_id="call-cm",
                tenant_id="tenant-cm",
                telephony_port=tel,
                ai_port=ai,
            ) as session:
                assert session.state.value == "running"
                await session.wait_until_closed()

            assert session.state.value == "closed"
            assert tel.is_closed
            assert ai.is_closed

        asyncio.run(_test())

    def test_start_already_started_fails(self) -> None:
        async def _test() -> None:
            tel = FakeTelephonyMediaAdapter()
            ai = FakeAIMediaAdapter()
            session = VoiceBridgeSession(
                call_id="call-double-start",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
            )
            await session.start()
            try:
                await session.start()
                raise AssertionError("Expected VoiceBridgeSessionClosedError")
            except VoiceBridgeSessionClosedError as err:
                assert "Cannot start" in str(err)
            await session.close()

        asyncio.run(_test())

    def test_start_after_close_fails(self) -> None:
        async def _test() -> None:
            tel = FakeTelephonyMediaAdapter()
            ai = FakeAIMediaAdapter()
            session = VoiceBridgeSession(
                call_id="call-start-after-close",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
            )
            await session.close()
            assert session.state == VoiceBridgeState.CLOSED
            try:
                await session.start()
                raise AssertionError("Expected VoiceBridgeSessionClosedError")
            except VoiceBridgeSessionClosedError as err:
                assert "Cannot start" in str(err)

        asyncio.run(_test())

    def test_idempotent_close(self) -> None:
        async def _test() -> None:
            tel = FakeTelephonyMediaAdapter()
            ai = FakeAIMediaAdapter()
            session = VoiceBridgeSession(
                call_id="call-idem",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
            )
            await session.start()
            await session.close()
            await session.close()
            await session.close()
            assert session.state == VoiceBridgeState.CLOSED
            assert tel.close_count == 1
            assert ai.close_count == 1

        asyncio.run(_test())

    def test_parallel_close_calls(self) -> None:
        async def _test() -> None:
            tel = FakeTelephonyMediaAdapter()
            ai = FakeAIMediaAdapter()
            session = VoiceBridgeSession(
                call_id="call-par-close",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
            )
            await session.start()
            await asyncio.gather(
                session.close(),
                session.close(),
                session.close(),
                session.close(),
            )
            assert session.state == VoiceBridgeState.CLOSED

        asyncio.run(_test())

    def test_wait_until_closed_without_propagation(self) -> None:
        async def _test() -> None:
            tel = FakeTelephonyMediaAdapter()
            ai = FakeAIMediaAdapter()
            session = VoiceBridgeSession(
                call_id="call-no-propagate",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
            )
            tel.inject_receive_error(RuntimeError("no-propagate-err"))
            await session.start()
            # Should not raise when propagate_error=False
            await session.wait_until_closed(propagate_error=False)
            assert session.state == VoiceBridgeState.FAILED
            assert isinstance(session.error, RuntimeError)

        asyncio.run(_test())

    def test_supervisor_cancelled_path(self) -> None:
        async def _test() -> None:
            tel = FakeTelephonyMediaAdapter()
            ai = FakeAIMediaAdapter()
            session = VoiceBridgeSession(
                call_id="call-sup-cancel",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
            )
            await session.start()
            assert session._supervisor_task is not None
            session._supervisor_task.cancel()
            try:
                await session._supervisor_task
            except asyncio.CancelledError:
                pass
            await session.close()
            assert session.state == VoiceBridgeState.CLOSED

        asyncio.run(_test())


class TestVoiceBridgeNegativeAndFaultScenarios:
    def test_inbound_out_of_order_sequence_strict(self) -> None:
        async def _test() -> None:
            tel = FakeTelephonyMediaAdapter()
            ai = FakeAIMediaAdapter()
            session = VoiceBridgeSession(
                call_id="call-seq-err",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
                strict_sequence=True,
            )
            tel.push_frame(create_synthetic_pcm16le_frame(b"FRAME_0", sequence=0))
            tel.push_frame(create_synthetic_pcm16le_frame(b"FRAME_2", sequence=2))

            await session.start()
            try:
                await session.wait_until_closed()
                raise AssertionError("Expected OutOfOrderAudioFrameError")
            except OutOfOrderAudioFrameError as err:
                assert "sequence mismatch: expected 1, got 2" in str(err)

            assert session.state == VoiceBridgeState.FAILED
            assert isinstance(session.error, OutOfOrderAudioFrameError)
            assert tel.is_closed
            assert ai.is_closed

        asyncio.run(_test())

    def test_outbound_out_of_order_sequence_strict(self) -> None:
        async def _test() -> None:
            tel = FakeTelephonyMediaAdapter()
            ai = FakeAIMediaAdapter()
            session = VoiceBridgeSession(
                call_id="call-out-seq-err",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
                strict_sequence=True,
            )
            ai.push_frame(create_synthetic_pcm16le_frame(b"AI_0", sequence=0))
            ai.push_frame(create_synthetic_pcm16le_frame(b"AI_5", sequence=5))

            await session.start()
            try:
                await session.wait_until_closed()
                raise AssertionError("Expected OutOfOrderAudioFrameError")
            except OutOfOrderAudioFrameError as err:
                assert "Outbound frame sequence mismatch" in str(err)

            assert session.state == VoiceBridgeState.FAILED
            assert tel.is_closed
            assert ai.is_closed

        asyncio.run(_test())

    def test_non_audio_frame_in_inbound_port_fails(self) -> None:
        async def _test() -> None:
            tel = FakeTelephonyMediaAdapter()
            ai = FakeAIMediaAdapter()
            session = VoiceBridgeSession(
                call_id="call-invalid-type",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
            )
            tel._inbound_queue.put_nowait("not-an-audio-frame")

            await session.start()
            try:
                await session.wait_until_closed()
                raise AssertionError("Expected InvalidAudioFrameError")
            except InvalidAudioFrameError as err:
                assert "Expected AudioFrame" in str(err)

            assert session.state == VoiceBridgeState.FAILED

        asyncio.run(_test())

    def test_non_audio_frame_in_outbound_port_fails(self) -> None:
        async def _test() -> None:
            tel = FakeTelephonyMediaAdapter()
            ai = FakeAIMediaAdapter()
            session = VoiceBridgeSession(
                call_id="call-invalid-out-type",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
            )
            ai._inbound_queue.put_nowait({"not": "frame"})

            await session.start()
            try:
                await session.wait_until_closed()
                raise AssertionError("Expected InvalidAudioFrameError")
            except InvalidAudioFrameError as err:
                assert "Expected AudioFrame" in str(err)

            assert session.state == VoiceBridgeState.FAILED

        asyncio.run(_test())

    def test_inbound_receive_error_cancels_other_pump(self) -> None:
        async def _test() -> None:
            tel = FakeTelephonyMediaAdapter()
            ai = FakeAIMediaAdapter()
            session = VoiceBridgeSession(
                call_id="call-in-recv-err",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
            )
            tel.inject_receive_error(ConnectionResetError("telephony-dropped"))

            await session.start()
            try:
                await session.wait_until_closed()
                raise AssertionError("Expected ConnectionResetError")
            except ConnectionResetError as err:
                assert "telephony-dropped" in str(err)

            assert session.state == VoiceBridgeState.FAILED
            assert tel.is_closed
            assert ai.is_closed

        asyncio.run(_test())

    def test_outbound_receive_error_cancels_other_pump(self) -> None:
        async def _test() -> None:
            tel = FakeTelephonyMediaAdapter()
            ai = FakeAIMediaAdapter()
            session = VoiceBridgeSession(
                call_id="call-out-recv-err",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
            )
            ai.inject_receive_error(TimeoutError("ai-stream-timed-out"))

            await session.start()
            try:
                await session.wait_until_closed()
                raise AssertionError("Expected TimeoutError")
            except TimeoutError as err:
                assert "ai-stream-timed-out" in str(err)

            assert session.state == VoiceBridgeState.FAILED
            assert tel.is_closed
            assert ai.is_closed

        asyncio.run(_test())

    def test_inbound_send_error_cancels_other_pump(self) -> None:
        async def _test() -> None:
            tel = FakeTelephonyMediaAdapter()
            ai = FakeAIMediaAdapter()
            session = VoiceBridgeSession(
                call_id="call-in-send-err",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
            )
            tel.push_frame(create_synthetic_pcm16le_frame(b"TEST", sequence=0))
            ai.inject_send_error(RuntimeError("ai-backend-rejected"))

            await session.start()
            try:
                await session.wait_until_closed()
                raise AssertionError("Expected RuntimeError")
            except RuntimeError as err:
                assert "ai-backend-rejected" in str(err)

            assert session.state == VoiceBridgeState.FAILED
            assert tel.is_closed
            assert ai.is_closed

        asyncio.run(_test())

    def test_outbound_send_error_cancels_other_pump(self) -> None:
        async def _test() -> None:
            tel = FakeTelephonyMediaAdapter()
            ai = FakeAIMediaAdapter()
            session = VoiceBridgeSession(
                call_id="call-out-send-err",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
            )
            ai.push_frame(create_synthetic_pcm16le_frame(b"TEST", sequence=0))
            tel.inject_send_error(BrokenPipeError("telephony-send-broken"))

            await session.start()
            try:
                await session.wait_until_closed()
                raise AssertionError("Expected BrokenPipeError")
            except BrokenPipeError as err:
                assert "telephony-send-broken" in str(err)

            assert session.state == VoiceBridgeState.FAILED
            assert tel.is_closed
            assert ai.is_closed

        asyncio.run(_test())

    def test_slow_consumer_backpressure_and_queue_depth(self) -> None:
        async def _test() -> None:
            tel = FakeTelephonyMediaAdapter()
            ai = FakeAIMediaAdapter()
            ai.set_send_delay(0.05)

            session = VoiceBridgeSession(
                call_id="call-slow-consumer",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
                queue_capacity=3,
            )

            for i in range(5):
                tel.push_frame(create_synthetic_pcm16le_frame(f"F_{i}".encode(), sequence=i))
            tel.push_eof()
            ai.push_eof()

            await session.start()
            await asyncio.sleep(0.02)
            depth = session.inbound_queue_depth
            assert depth > 0
            assert depth <= 3

            await session.wait_until_closed()
            assert session.state == VoiceBridgeState.CLOSED
            assert session.inbound_frames_transferred == 5

        asyncio.run(_test())

    def test_backpressure_timeout_fail_close(self) -> None:
        async def _test() -> None:
            tel = FakeTelephonyMediaAdapter()
            ai = FakeAIMediaAdapter()
            ai.set_send_delay(0.5)

            session = VoiceBridgeSession(
                call_id="call-bp-timeout",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
                queue_capacity=2,
                backpressure_timeout=0.03,
            )

            for i in range(10):
                tel.push_frame(create_synthetic_pcm16le_frame(f"DATA_{i}".encode(), sequence=i))

            await session.start()
            try:
                await session.wait_until_closed()
                raise AssertionError("Expected MediaBackpressureOverflowError")
            except MediaBackpressureOverflowError as err:
                assert "backpressure timed out" in str(err)

            assert session.state == VoiceBridgeState.FAILED
            assert tel.is_closed
            assert ai.is_closed

        asyncio.run(_test())

    def test_outbound_backpressure_timeout_fail_close(self) -> None:
        async def _test() -> None:
            tel = FakeTelephonyMediaAdapter()
            ai = FakeAIMediaAdapter()
            tel.set_send_delay(0.5)

            session = VoiceBridgeSession(
                call_id="call-out-bp-timeout",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
                queue_capacity=2,
                backpressure_timeout=0.03,
            )

            for i in range(10):
                ai.push_frame(create_synthetic_pcm16le_frame(f"AI_DATA_{i}".encode(), sequence=i))

            await session.start()
            try:
                await session.wait_until_closed()
                raise AssertionError("Expected MediaBackpressureOverflowError")
            except MediaBackpressureOverflowError as err:
                assert "Outbound queue capacity" in str(err)

            assert session.state == VoiceBridgeState.FAILED
            assert tel.is_closed
            assert ai.is_closed

        asyncio.run(_test())

    def test_ports_raising_on_close_handled_safely(self) -> None:
        async def _test() -> None:
            tel = FakeTelephonyMediaAdapter()
            ai = FakeAIMediaAdapter()

            async def bad_close() -> None:
                raise RuntimeError("port-close-error")

            tel.close = bad_close  # type: ignore[method-assign]
            ai.close = bad_close  # type: ignore[method-assign]

            session = VoiceBridgeSession(
                call_id="call-close-err",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
            )
            await session.start()
            await session.close()
            assert session.state == VoiceBridgeState.CLOSED

        asyncio.run(_test())

    def test_cancellation_during_session(self) -> None:
        async def _test() -> None:
            tel = FakeTelephonyMediaAdapter()
            ai = FakeAIMediaAdapter()
            session = VoiceBridgeSession(
                call_id="call-cancel",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
            )
            await session.start()
            await asyncio.sleep(0.01)
            await session.close()
            assert session.state == VoiceBridgeState.CLOSED

        asyncio.run(_test())

    def test_no_orphan_tasks_after_forced_close(self) -> None:
        async def _test() -> None:
            tel = FakeTelephonyMediaAdapter()
            ai = FakeAIMediaAdapter()
            session = VoiceBridgeSession(
                call_id="call-no-orphans",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
            )
            await session.start()
            inbound = session._inbound_task
            outbound = session._outbound_task
            supervisor = session._supervisor_task

            assert inbound is not None and not inbound.done()
            assert outbound is not None and not outbound.done()
            assert supervisor is not None and not supervisor.done()

            await session.close()

            assert inbound.done()
            assert outbound.done()
            assert supervisor.done()
            assert session.inbound_queue_depth == 0
            assert session.outbound_queue_depth == 0

        asyncio.run(_test())

    def test_cancellation_while_bounded_queue_is_full(self) -> None:
        async def _test() -> None:
            tel = FakeTelephonyMediaAdapter()
            ai = FakeAIMediaAdapter()
            ai.set_send_delay(1.0)  # Consumer blocked for 1s

            session = VoiceBridgeSession(
                call_id="call-cancel-full-q",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
                queue_capacity=2,
            )
            for i in range(5):
                tel.push_frame(create_synthetic_pcm16le_frame(f"FRAME_{i}".encode(), sequence=i))

            await session.start()
            await asyncio.sleep(0.02)  # Allow queue to fill to capacity 2
            assert session.inbound_queue_depth == 2

            # Close while receiver is blocked waiting on put
            await session.close()
            assert session.state == VoiceBridgeState.CLOSED
            assert session._inbound_task is not None and session._inbound_task.done()
            assert session._outbound_task is not None and session._outbound_task.done()

        asyncio.run(_test())

    def test_cancellation_while_waiting_for_input(self) -> None:
        async def _test() -> None:
            tel = FakeTelephonyMediaAdapter()
            ai = FakeAIMediaAdapter()

            session = VoiceBridgeSession(
                call_id="call-cancel-empty-in",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
            )
            await session.start()
            await asyncio.sleep(0.02)  # Receivers waiting for input frames
            await session.close()
            assert session.state == VoiceBridgeState.CLOSED
            assert session._inbound_task is not None and session._inbound_task.done()
            assert session._outbound_task is not None and session._outbound_task.done()

        asyncio.run(_test())

    def test_one_sided_eof_with_other_direction_still_active(self) -> None:
        async def _test() -> None:
            tel = FakeTelephonyMediaAdapter()
            ai = FakeAIMediaAdapter()

            session = VoiceBridgeSession(
                call_id="call-one-sided-eof",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
            )

            # Inbound: pushes 2 frames and EOF
            f0 = create_synthetic_pcm16le_frame(b"IN_0", sequence=0)
            f1 = create_synthetic_pcm16le_frame(b"IN_1", sequence=1)
            tel.push_frame(f0)
            tel.push_frame(f1)
            tel.push_eof()

            await session.start()
            # Allow inbound to complete EOF
            await asyncio.sleep(0.02)
            inbound_mid = session.inbound_completed
            outbound_mid = session.outbound_completed
            state_mid = session.state.value
            assert inbound_mid is True
            assert outbound_mid is False
            assert state_mid == "running"

            # Outbound is still completely active and can transfer frames
            out0 = create_synthetic_pcm16le_frame(b"OUT_0", sequence=0)
            out1 = create_synthetic_pcm16le_frame(b"OUT_1", sequence=1)
            ai.push_frame(out0)
            ai.push_frame(out1)
            await asyncio.sleep(0.02)

            assert len(tel.sent_frames) == 2
            assert tel.sent_frames[0].payload == b"OUT_0"
            assert tel.sent_frames[1].payload == b"OUT_1"

            # Finally AI pushes EOF
            ai.push_eof()
            await session.wait_until_closed()

            inbound_final = session.inbound_completed
            outbound_final = session.outbound_completed
            state_final = session.state.value
            assert state_final == "closed"
            assert inbound_final is True
            assert outbound_final is True
            assert session.inbound_frames_transferred == 2
            assert session.outbound_frames_transferred == 2

        asyncio.run(_test())


class TestMultiCallAndTenantIsolation:
    def test_two_concurrent_calls_isolation(self) -> None:
        async def _test() -> None:
            tel_a = FakeTelephonyMediaAdapter(name="tel-a")
            ai_a = FakeAIMediaAdapter(name="ai-a")
            session_a = VoiceBridgeSession(
                call_id="call-alpha",
                tenant_id="tenant-alpha",
                telephony_port=tel_a,
                ai_port=ai_a,
            )

            tel_b = FakeTelephonyMediaAdapter(name="tel-b")
            ai_b = FakeAIMediaAdapter(name="ai-b")
            session_b = VoiceBridgeSession(
                call_id="call-beta",
                tenant_id="tenant-beta",
                telephony_port=tel_b,
                ai_port=ai_b,
            )

            frames_a = [
                create_synthetic_pcm16le_frame(b"CALL_A_FRAME_0", sequence=0),
                create_synthetic_pcm16le_frame(b"CALL_A_FRAME_1", sequence=1),
            ]
            frames_b = [
                create_synthetic_pcm16le_frame(b"CALL_B_FRAME_0", sequence=0),
                create_synthetic_pcm16le_frame(b"CALL_B_FRAME_1", sequence=1),
            ]

            for f in frames_a:
                tel_a.push_frame(f)
            tel_a.push_eof()
            ai_a.push_eof()

            for f in frames_b:
                tel_b.push_frame(f)
            tel_b.push_eof()
            ai_b.push_eof()

            await asyncio.gather(session_a.run(), session_b.run())

            assert len(ai_a.sent_frames) == 2
            for f in ai_a.sent_frames:
                assert b"CALL_A" in f.payload
                assert b"CALL_B" not in f.payload

            assert len(ai_b.sent_frames) == 2
            for f in ai_b.sent_frames:
                assert b"CALL_B" in f.payload
                assert b"CALL_A" not in f.payload

            assert session_a.state == VoiceBridgeState.CLOSED
            assert session_b.state == VoiceBridgeState.CLOSED

        asyncio.run(_test())


class _BarrierTelephonyMediaAdapter(FakeTelephonyMediaAdapter):
    """Fake adapter that signals when close starts and waits for test release."""

    def __init__(
        self,
        *,
        enter_close_event: asyncio.Event,
        release_close_event: asyncio.Event,
        name: str = "barrier-telephony",
    ) -> None:
        super().__init__(name=name)
        self.enter_close_event = enter_close_event
        self.release_close_event = release_close_event

    async def close(self) -> None:
        self.enter_close_event.set()
        await self.release_close_event.wait()
        await super().close()


class TestVoiceBridgeStrictConfigValidation:
    """Gate C: Strict boolean, non-finite, and range validation on configuration."""

    @pytest.mark.parametrize("invalid_capacity", [True, False])
    def test_queue_capacity_boolean_rejected(self, invalid_capacity: object) -> None:
        tel = FakeTelephonyMediaAdapter()
        ai = FakeAIMediaAdapter()
        with pytest.raises(VoiceDomainError, match="queue_capacity must be an integer >= 1"):
            VoiceBridgeSession(
                call_id="call-c1",
                tenant_id="tenant-c1",
                telephony_port=tel,
                ai_port=ai,
                queue_capacity=invalid_capacity,  # type: ignore[arg-type]
            )

    @pytest.mark.parametrize("invalid_timeout", [True, False])
    def test_backpressure_timeout_boolean_rejected(self, invalid_timeout: object) -> None:
        tel = FakeTelephonyMediaAdapter()
        ai = FakeAIMediaAdapter()
        with pytest.raises(
            VoiceDomainError, match="backpressure_timeout must be a positive finite float or None"
        ):
            VoiceBridgeSession(
                call_id="call-c2",
                tenant_id="tenant-c2",
                telephony_port=tel,
                ai_port=ai,
                backpressure_timeout=invalid_timeout,  # type: ignore[arg-type]
            )

    @pytest.mark.parametrize("invalid_float", [float("nan"), float("inf"), float("-inf")])
    def test_backpressure_timeout_non_finite_rejected(self, invalid_float: float) -> None:
        tel = FakeTelephonyMediaAdapter()
        ai = FakeAIMediaAdapter()
        with pytest.raises(
            VoiceDomainError, match="backpressure_timeout must be a positive finite float or None"
        ):
            VoiceBridgeSession(
                call_id="call-c3",
                tenant_id="tenant-c3",
                telephony_port=tel,
                ai_port=ai,
                backpressure_timeout=invalid_float,
            )

    @pytest.mark.parametrize("invalid_non_positive", [0, 0.0, -1, -1.0])
    def test_backpressure_timeout_non_positive_rejected(self, invalid_non_positive: float) -> None:
        tel = FakeTelephonyMediaAdapter()
        ai = FakeAIMediaAdapter()
        with pytest.raises(
            VoiceDomainError, match="backpressure_timeout must be a positive finite float or None"
        ):
            VoiceBridgeSession(
                call_id="call-c4",
                tenant_id="tenant-c4",
                telephony_port=tel,
                ai_port=ai,
                backpressure_timeout=invalid_non_positive,
            )


class TestVoiceBridgeParentRunCancellation:
    """Gate A: Parent run() task cancellation guarantees clean teardown and propagation."""

    def test_parent_run_cancellation_during_active_streaming(self) -> None:
        async def _test() -> None:
            tel = FakeTelephonyMediaAdapter()
            ai = FakeAIMediaAdapter()
            session = VoiceBridgeSession(
                call_id="call-cancel-1",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
            )

            # Push initial frame so streaming starts
            frame = create_synthetic_pcm16le_frame(b"AUDIO_DATA_0", sequence=0)
            tel.push_frame(frame)

            run_task = asyncio.create_task(session.run())
            # Allow run_task to start pumps and transfer first frame
            while session.inbound_frames_transferred < 1:
                await asyncio.sleep(0.001)

            # Cancel parent run task
            run_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await run_task

            # Verify session cleanup invariants
            state_val = session.state.value
            assert state_val == "closed"
            assert tel.is_closed is True
            assert ai.is_closed is True
            assert session._closed_event.is_set() is True

            # Verify zero orphan tasks
            assert session._supervisor_task is not None
            assert session._supervisor_task.done() is True
            assert session._inbound_task is not None
            assert session._inbound_task.done() is True
            assert session._outbound_task is not None
            assert session._outbound_task.done() is True

        asyncio.run(_test())

    def test_parent_run_cancellation_during_backpressure_wait(self) -> None:
        async def _test() -> None:
            tel = FakeTelephonyMediaAdapter()
            ai = FakeAIMediaAdapter()
            ai.set_send_delay(1.0)  # Slow down AI forwarder to fill inbound queue
            session = VoiceBridgeSession(
                call_id="call-cancel-bp",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
                queue_capacity=1,
                backpressure_timeout=10.0,
            )

            # Push frames to fill the queue and block on backpressure
            tel.push_frame(create_synthetic_pcm16le_frame(b"FRAME_1", sequence=0))
            tel.push_frame(create_synthetic_pcm16le_frame(b"FRAME_2", sequence=1))
            tel.push_frame(create_synthetic_pcm16le_frame(b"FRAME_3", sequence=2))

            run_task = asyncio.create_task(session.run())
            await asyncio.sleep(0.01)

            run_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await run_task

            state_val = session.state.value
            assert state_val == "closed"
            assert tel.is_closed is True
            assert ai.is_closed is True
            assert session._closed_event.is_set() is True

        asyncio.run(_test())

    def test_parent_run_cancellation_during_receive_wait(self) -> None:
        async def _test() -> None:
            tel = FakeTelephonyMediaAdapter()
            ai = FakeAIMediaAdapter()
            session = VoiceBridgeSession(
                call_id="call-cancel-recv",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
            )

            # No frames pushed: receiver will be blocked waiting for input
            run_task = asyncio.create_task(session.run())
            await asyncio.sleep(0.01)

            run_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await run_task

            state_val = session.state.value
            assert state_val == "closed"
            assert tel.is_closed is True
            assert ai.is_closed is True
            assert session._closed_event.is_set() is True

        asyncio.run(_test())

    def test_parent_run_cancellation_unblocks_concurrent_waiters(self) -> None:
        async def _test() -> None:
            tel = FakeTelephonyMediaAdapter()
            ai = FakeAIMediaAdapter()
            session = VoiceBridgeSession(
                call_id="call-cancel-waiters",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
            )

            run_task = asyncio.create_task(session.run())
            waiter_task = asyncio.create_task(session.wait_until_closed(propagate_error=False))
            await asyncio.sleep(0.01)

            run_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await run_task

            # The waiter must unblock without error because _closed_event is set
            await asyncio.wait_for(waiter_task, timeout=1.0)
            assert waiter_task.done() is True
            state_val = session.state.value
            assert state_val == "closed"

        asyncio.run(_test())


class TestVoiceBridgeSingleOwnerCleanupAndRaces:
    """Gate B: Single-owner cleanup, concurrent close synchronization, and state preservation."""

    def test_delayed_port_close_blocks_concurrent_session_close(self) -> None:
        async def _test() -> None:
            enter_close_event = asyncio.Event()
            release_close_event = asyncio.Event()
            tel = _BarrierTelephonyMediaAdapter(
                enter_close_event=enter_close_event,
                release_close_event=release_close_event,
            )
            ai = FakeAIMediaAdapter()
            session = VoiceBridgeSession(
                call_id="call-race-1",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
            )

            await session.start()

            # First close task enters cleanup and pauses at telephony_port.close()
            close_task_1 = asyncio.create_task(session.close())
            await enter_close_event.wait()

            # Launch concurrent close calls while port cleanup is in progress
            close_task_2 = asyncio.create_task(session.close())
            close_task_3 = asyncio.create_task(session.close())

            # Give event loop cycles to attempt completion
            await asyncio.sleep(0.01)

            # Invariant: None of the close tasks have returned, _closed_event not set
            assert close_task_1.done() is False
            assert close_task_2.done() is False
            assert close_task_3.done() is False
            assert session._closed_event.is_set() is False

            # Release the barrier
            release_close_event.set()

            # All close tasks must now complete
            await asyncio.wait_for(
                asyncio.gather(close_task_1, close_task_2, close_task_3),
                timeout=1.0,
            )

            state_val = session.state.value
            assert state_val == "closed"
            assert session._closed_event.is_set() is True
            assert tel.is_closed is True
            assert ai.is_closed is True

        asyncio.run(_test())

    def test_supervisor_failure_cleanup_preserves_failed_state_against_concurrent_close(
        self,
    ) -> None:
        async def _test() -> None:
            enter_close_event = asyncio.Event()
            release_close_event = asyncio.Event()
            tel = _BarrierTelephonyMediaAdapter(
                enter_close_event=enter_close_event,
                release_close_event=release_close_event,
            )
            ai = FakeAIMediaAdapter()
            session = VoiceBridgeSession(
                call_id="call-race-fail",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
            )

            # Inject error in telephony receive
            tel.inject_receive_error(InvalidAudioFrameError("injected frame failure"))
            await session.start()

            # Wait for supervisor to detect failure, set FAILED, and pause at port close
            await enter_close_event.wait()

            state_val = session.state.value
            assert state_val == "failed"

            # Concurrently call close() from outside while supervisor is cleaning up
            close_task_1 = asyncio.create_task(session.close())
            close_task_2 = asyncio.create_task(session.close())
            await asyncio.sleep(0.01)

            # Invariant: close tasks have not returned, FAILED state is NOT overwritten
            assert close_task_1.done() is False
            assert close_task_2.done() is False
            state_val_mid = session.state.value
            assert state_val_mid == "failed"
            assert session._closed_event.is_set() is False

            # Release port close barrier
            release_close_event.set()

            await asyncio.wait_for(
                asyncio.gather(close_task_1, close_task_2),
                timeout=1.0,
            )

            # Invariant: terminal state is strictly FAILED, error preserved
            state_val_end = session.state.value
            assert state_val_end == "failed"
            assert isinstance(session.error, InvalidAudioFrameError)
            assert session._closed_event.is_set() is True
            assert tel.is_closed is True
            assert ai.is_closed is True

        asyncio.run(_test())

    def test_clean_eof_with_delayed_port_close_and_concurrent_close(self) -> None:
        async def _test() -> None:
            enter_close_event = asyncio.Event()
            release_close_event = asyncio.Event()
            tel = _BarrierTelephonyMediaAdapter(
                enter_close_event=enter_close_event,
                release_close_event=release_close_event,
            )
            ai = FakeAIMediaAdapter()
            session = VoiceBridgeSession(
                call_id="call-race-eof",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
            )

            # Push valid frames then EOF
            frame = create_synthetic_pcm16le_frame(b"AUDIO_DATA", sequence=0)
            tel.push_frame(frame)
            tel.push_eof()
            ai.push_eof()

            run_task = asyncio.create_task(session.run())

            # Wait until streams finish and supervisor enters cleanup at port close
            await enter_close_event.wait()

            # Concurrently invoke close()
            close_task = asyncio.create_task(session.close())
            await asyncio.sleep(0.01)

            assert close_task.done() is False
            assert run_task.done() is False
            assert session._closed_event.is_set() is False

            # Release barrier
            release_close_event.set()

            await asyncio.wait_for(asyncio.gather(run_task, close_task), timeout=1.0)

            state_val = session.state.value
            assert state_val == "closed"
            assert session.error is None
            assert session.inbound_completed is True
            assert session._closed_event.is_set() is True
            assert tel.is_closed is True
            assert ai.is_closed is True

        asyncio.run(_test())
