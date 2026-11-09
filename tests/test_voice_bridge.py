"""Comprehensive unit and negative tests for VoiceBridgeSession."""

from __future__ import annotations

import asyncio

import pytest

from kaiwen_gateway.core.identity import VerifiedConnectionContext
from kaiwen_gateway.modules.voice.adapters.fake import (
    FakeAIMediaAdapter,
    FakeTelephonyMediaAdapter,
    InterruptibleFakeTelephonyAdapter,
)
from kaiwen_gateway.modules.voice.bridge import (
    VoiceBridgeSession,
    VoiceBridgeState,
)
from kaiwen_gateway.modules.voice.domain import (
    AudioFrame,
    InvalidAudioFrameError,
    MediaBackpressureOverflowError,
    MediaPortClosedError,
    MediaPortError,
    OutOfOrderAudioFrameError,
    VoiceBridgeSessionClosedError,
    VoiceDomainError,
    create_synthetic_pcm16le_frame,
)
from kaiwen_gateway.modules.voice.telemetry import (
    InMemoryVoiceTelemetrySink,
    VoiceErrorCategory,
)
from kaiwen_gateway.transports.synthetic.media_adapter import WebSocketTelephonyMediaAdapter


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
            state_val = session.state.value
            assert state_val == "failed"
            assert isinstance(session.error, ExceptionGroup)

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
            for _ in range(50):
                if session.inbound_completed:
                    break
                await asyncio.sleep(0.01)
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
            for _ in range(50):
                if len(tel.sent_frames) == 2:
                    break
                await asyncio.sleep(0.01)

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


class TestVoiceBridgeCleanupCancellationResilience:
    """Gate A: Cleanup ownership, caller cancellation propagation, and background completion."""

    def test_cancellation_during_cleanup_unstarted_session(self) -> None:
        async def _test() -> None:
            enter_close_event = asyncio.Event()
            release_close_event = asyncio.Event()
            tel = _BarrierTelephonyMediaAdapter(
                enter_close_event=enter_close_event,
                release_close_event=release_close_event,
            )
            ai = FakeAIMediaAdapter()
            session = VoiceBridgeSession(
                call_id="call-cancel-clean-unstarted",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
            )

            # 2. Session cleanup starts via close()
            close_caller_task = asyncio.create_task(session.close())

            # 3. Verify port close started
            await asyncio.wait_for(enter_close_event.wait(), timeout=1.0)
            assert enter_close_event.is_set() is True
            assert session._closed_event.is_set() is False

            # 4. Cancel cleanup caller task
            close_caller_task.cancel()

            # 5. Cancellation propagated immediately to caller task
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(close_caller_task, timeout=1.0)

            # Invariant: barrier still blocked, cleanup task running in background
            assert session._closed_event.is_set() is False
            tel_early_closed = bool(tel.is_closed)
            assert tel_early_closed is False

            # 6. Release port close barrier
            release_close_event.set()

            # 7. Verify background cleanup completes
            await asyncio.wait_for(
                session.wait_until_closed(propagate_error=False),
                timeout=1.0,
            )

            # 8. _closed_event is set
            assert session._closed_event.is_set() is True

            # 9. Both ports closed properly
            tel_final_closed = bool(tel.is_closed)
            ai_final_closed = bool(ai.is_closed)
            assert tel_final_closed is True
            assert ai_final_closed is True

            # 10. No orphan tasks or lockups
            state_val = session.state.value
            assert state_val == "closed"
            assert session.error is None
            assert session._cleanup_task is not None
            assert session._cleanup_task.done() is True

        asyncio.run(_test())

    def test_cancellation_during_cleanup_active_streaming_session(self) -> None:
        async def _test() -> None:
            enter_close_event = asyncio.Event()
            release_close_event = asyncio.Event()
            tel = _BarrierTelephonyMediaAdapter(
                enter_close_event=enter_close_event,
                release_close_event=release_close_event,
            )
            ai = FakeAIMediaAdapter()
            session = VoiceBridgeSession(
                call_id="call-cancel-clean-active",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
            )

            # Push audio frame for active streaming
            frame = create_synthetic_pcm16le_frame(b"AUDIO_DATA", sequence=0)
            tel.push_frame(frame)

            await session.start()

            while session.inbound_frames_transferred < 1:
                await asyncio.sleep(0.001)

            # 2. Session cleanup starts
            close_caller_task = asyncio.create_task(session.close())

            # 3. Verify port close started
            await asyncio.wait_for(enter_close_event.wait(), timeout=1.0)
            assert enter_close_event.is_set() is True
            assert session._closed_event.is_set() is False

            # 4. Cancel cleanup caller task
            close_caller_task.cancel()

            # 5. Cancellation propagated to caller task
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(close_caller_task, timeout=1.0)

            # Invariant: barrier still blocked, cleanup task running in background
            assert session._closed_event.is_set() is False
            tel_early_closed = bool(tel.is_closed)
            assert tel_early_closed is False

            # Verify concurrent second close() caller awaits in-progress cleanup
            second_caller_task = asyncio.create_task(session.close())
            await asyncio.sleep(0.01)
            assert second_caller_task.done() is False

            # 6. Release port close barrier
            release_close_event.set()

            # Second caller finishes cleanly once barrier is released
            await asyncio.wait_for(second_caller_task, timeout=1.0)

            # 7. Verify cleanup completes
            await asyncio.wait_for(
                session.wait_until_closed(propagate_error=False),
                timeout=1.0,
            )

            # 8. _closed_event is set
            assert session._closed_event.is_set() is True

            # 9. Both ports closed properly
            tel_final_closed = bool(tel.is_closed)
            ai_final_closed = bool(ai.is_closed)
            assert tel_final_closed is True
            assert ai_final_closed is True

            # 10. No orphan tasks or lockups
            state_val = session.state.value
            assert state_val == "closed"
            assert session.error is None
            assert session._cleanup_task is not None
            assert session._cleanup_task.done() is True
            assert session._supervisor_task is not None
            assert session._supervisor_task.done() is True
            assert session._inbound_task is not None
            assert session._inbound_task.done() is True
            assert session._outbound_task is not None
            assert session._outbound_task.done() is True

        asyncio.run(_test())


class TestVoiceBridgePortCloseFailureSemantics:
    """Gate B: Port cleanup failures cannot report success and preserve error fidelity."""

    def test_telephony_port_close_failure_marks_failed(self) -> None:
        async def _test() -> None:
            tel = FakeTelephonyMediaAdapter()
            ai = FakeAIMediaAdapter()

            async def failing_tel_close() -> None:
                raise RuntimeError("telephony close hardware failure")

            tel.close = failing_tel_close  # type: ignore[method-assign]

            session = VoiceBridgeSession(
                call_id="call-fail-tel-close",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
            )

            await session.start()
            await session.close()

            # Terminal state must be FAILED, not CLOSED
            state_val = session.state.value
            assert state_val == "failed"
            assert isinstance(session.error, RuntimeError)
            assert str(session.error) == "telephony close hardware failure"

            # AI port close must still have been attempted and succeeded
            assert ai.is_closed is True

            # wait_until_closed with propagate_error=True raises
            with pytest.raises(RuntimeError, match="telephony close hardware failure"):
                await session.wait_until_closed(propagate_error=True)

            # wait_until_closed with propagate_error=False returns without error
            await session.wait_until_closed(propagate_error=False)

        asyncio.run(_test())

    def test_ai_port_close_failure_marks_failed(self) -> None:
        async def _test() -> None:
            tel = FakeTelephonyMediaAdapter()
            ai = FakeAIMediaAdapter()

            async def failing_ai_close() -> None:
                raise RuntimeError("ai realtime socket reset")

            ai.close = failing_ai_close  # type: ignore[method-assign]

            session = VoiceBridgeSession(
                call_id="call-fail-ai-close",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
            )

            await session.start()
            await session.close()

            state_val = session.state.value
            assert state_val == "failed"
            assert isinstance(session.error, RuntimeError)
            assert str(session.error) == "ai realtime socket reset"

            # Telephony port close must have succeeded
            assert tel.is_closed is True

            with pytest.raises(RuntimeError, match="ai realtime socket reset"):
                await session.wait_until_closed(propagate_error=True)

        asyncio.run(_test())

    def test_both_ports_close_failure_records_all_failures(self) -> None:
        async def _test() -> None:
            tel = FakeTelephonyMediaAdapter()
            ai = FakeAIMediaAdapter()

            async def bad_tel_close() -> None:
                raise RuntimeError("tel-hardware-err")

            async def bad_ai_close() -> None:
                raise RuntimeError("ai-socket-err")

            tel.close = bad_tel_close  # type: ignore[method-assign]
            ai.close = bad_ai_close  # type: ignore[method-assign]

            session = VoiceBridgeSession(
                call_id="call-fail-both-close",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
            )

            await session.start()
            await session.close()

            state_val = session.state.value
            assert state_val == "failed"
            assert isinstance(session.error, ExceptionGroup)
            err_group = session.error
            assert len(err_group.exceptions) == 2
            messages = [str(e) for e in err_group.exceptions]
            assert "tel-hardware-err" in messages
            assert "ai-socket-err" in messages

            with pytest.raises(ExceptionGroup):
                await session.wait_until_closed(propagate_error=True)

        asyncio.run(_test())

    def test_pump_exception_preserved_when_port_close_fails(self) -> None:
        async def _test() -> None:
            tel = FakeTelephonyMediaAdapter()
            ai = FakeAIMediaAdapter()

            async def failing_close() -> None:
                raise RuntimeError("secondary port close error")

            tel.close = failing_close  # type: ignore[method-assign]
            tel.inject_receive_error(InvalidAudioFrameError("primary pump frame error"))

            session = VoiceBridgeSession(
                call_id="call-fail-pump-and-close",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
            )

            await session.start()

            # Original pump exception must be preserved, not overwritten by port close error
            with pytest.raises(InvalidAudioFrameError, match="primary pump frame error"):
                await session.wait_until_closed(propagate_error=True)

            state_val = session.state.value
            assert state_val == "failed"
            assert isinstance(session.error, InvalidAudioFrameError)
            assert str(session.error) == "primary pump frame error"
            assert ai.is_closed is True

        asyncio.run(_test())

    def test_concurrent_close_while_port_cleanup_fails(self) -> None:
        async def _test() -> None:
            enter_close_event = asyncio.Event()
            release_close_event = asyncio.Event()

            class _FailingBarrierTelephony(FakeTelephonyMediaAdapter):
                async def close(self) -> None:
                    enter_close_event.set()
                    await release_close_event.wait()
                    raise RuntimeError("delayed telephony failure")

            tel = _FailingBarrierTelephony()
            ai = FakeAIMediaAdapter()

            session = VoiceBridgeSession(
                call_id="call-concurrent-fail",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
            )

            await session.start()

            close_task_1 = asyncio.create_task(session.close())
            await enter_close_event.wait()

            close_task_2 = asyncio.create_task(session.close())
            close_task_3 = asyncio.create_task(session.close())
            await asyncio.sleep(0.01)

            assert close_task_1.done() is False
            assert close_task_2.done() is False
            assert close_task_3.done() is False

            release_close_event.set()

            await asyncio.wait_for(
                asyncio.gather(close_task_1, close_task_2, close_task_3),
                timeout=1.0,
            )

            state_val = session.state.value
            assert state_val == "failed"
            assert isinstance(session.error, RuntimeError)
            assert str(session.error) == "delayed telephony failure"
            assert ai.is_closed is True

        asyncio.run(_test())

    def test_failure_after_cancellation(self) -> None:
        async def _test() -> None:
            enter_close_event = asyncio.Event()
            release_close_event = asyncio.Event()

            class _FailingBarrierTelephony(FakeTelephonyMediaAdapter):
                async def close(self) -> None:
                    enter_close_event.set()
                    await release_close_event.wait()
                    raise RuntimeError("failure after cancellation")

            tel = _FailingBarrierTelephony()
            ai = FakeAIMediaAdapter()

            session = VoiceBridgeSession(
                call_id="call-fail-after-cancel",
                tenant_id="tenant-1",
                telephony_port=tel,
                ai_port=ai,
            )

            caller_task = asyncio.create_task(session.close())
            await enter_close_event.wait()

            caller_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await caller_task

            release_close_event.set()

            await asyncio.wait_for(
                session.wait_until_closed(propagate_error=False),
                timeout=1.0,
            )

            state_val = session.state.value
            assert state_val == "failed"
            assert isinstance(session.error, RuntimeError)
            assert str(session.error) == "failure after cancellation"
            assert session._closed_event.is_set() is True
            assert ai.is_closed is True

        asyncio.run(_test())


class TestVoiceBridgeConcurrentFailureAndLifecycleHardening:
    """Targeted regression and adversarial tests for concurrent failure handling,

    task exception retrieval, and deterministic lifecycle state transitions.
    """

    def test_simultaneous_dual_pump_failures_retrieves_all_exceptions(self) -> None:
        """Verify that when both inbound and outbound pumps fail in the same scheduling cycle,

        every task's exception is retrieved, zero unretrieved exceptions are logged,
        and the inbound error is chosen deterministically as the primary session error.
        """

        async def _test() -> None:
            loop = asyncio.get_running_loop()
            unretrieved: list[dict[str, object]] = []

            def _handler(_loop: asyncio.AbstractEventLoop, ctx: dict[str, object]) -> None:
                unretrieved.append(ctx)

            old_handler = loop.get_exception_handler()
            loop.set_exception_handler(_handler)

            try:
                barrier = asyncio.Barrier(2)

                class _SimultaneousFailingPort(FakeTelephonyMediaAdapter):
                    def __init__(self, exc: Exception) -> None:
                        super().__init__()
                        self._exc = exc

                    async def receive_frame(self) -> None:
                        await barrier.wait()
                        raise self._exc

                inbound_exc = RuntimeError("simultaneous inbound pump failure")
                outbound_exc = ValueError("simultaneous outbound pump failure")

                tel = _SimultaneousFailingPort(inbound_exc)

                class _SimultaneousFailingAIPort(FakeAIMediaAdapter):
                    async def receive_frame(self) -> None:
                        await barrier.wait()
                        raise outbound_exc

                ai_failing = _SimultaneousFailingAIPort()

                session = VoiceBridgeSession(
                    call_id="call-dual-fail",
                    tenant_id="tenant-dual-fail",
                    telephony_port=tel,
                    ai_port=ai_failing,
                )

                await session.start()

                with pytest.raises(RuntimeError) as exc_info:
                    await asyncio.wait_for(
                        session.wait_until_closed(propagate_error=True), timeout=2.0
                    )

                assert str(exc_info.value) == "simultaneous inbound pump failure"
                assert session.state == VoiceBridgeState.FAILED
                assert session.error is inbound_exc

                # Verify both tasks had their exceptions observed (_log_traceback is False)
                t_in = session._inbound_task
                t_out = session._outbound_task
                assert t_in is not None and t_in.done() is True
                assert t_out is not None and t_out.done() is True
                assert getattr(t_in, "_log_traceback", None) is False
                assert getattr(t_out, "_log_traceback", None) is False

                # Force garbage collection to trigger __del__ on tasks
                del session, t_in, t_out
                import gc

                gc.collect()
                await asyncio.sleep(0.01)

                assert len(unretrieved) == 0, f"Unretrieved exceptions detected: {unretrieved}"
            finally:
                loop.set_exception_handler(old_handler)

        asyncio.run(asyncio.wait_for(_test(), timeout=5.0))

    def test_simultaneous_real_error_takes_precedence_over_cancellation(self) -> None:
        """Verify that when one pump fails with a real error while the other is cancelled,

        the real error is deterministically preserved as the primary session error.
        """

        async def _test() -> None:
            loop = asyncio.get_running_loop()
            unretrieved: list[dict[str, object]] = []
            loop.set_exception_handler(lambda _l, ctx: unretrieved.append(ctx))

            try:
                barrier = asyncio.Barrier(2)
                real_exc = OutOfOrderAudioFrameError("inbound sequence violation")

                class _InboundFailingPort(FakeTelephonyMediaAdapter):
                    async def receive_frame(self) -> None:
                        await barrier.wait()
                        raise real_exc

                class _OutboundCancelledPort(FakeAIMediaAdapter):
                    async def receive_frame(self) -> None:
                        await barrier.wait()
                        raise asyncio.CancelledError()

                session = VoiceBridgeSession(
                    call_id="call-prec-in",
                    tenant_id="tenant-prec-in",
                    telephony_port=_InboundFailingPort(),
                    ai_port=_OutboundCancelledPort(),
                )

                await session.start()

                with pytest.raises(OutOfOrderAudioFrameError):
                    await asyncio.wait_for(
                        session.wait_until_closed(propagate_error=True), timeout=2.0
                    )

                assert session.state == VoiceBridgeState.FAILED
                assert session.error is real_exc

                # Clean task outcome observation
                t_in = session._inbound_task
                t_out = session._outbound_task
                assert t_in is not None and getattr(t_in, "_log_traceback", None) is False
                assert t_out is not None and getattr(t_out, "_log_traceback", None) is False
            finally:
                loop.set_exception_handler(None)

        asyncio.run(asyncio.wait_for(_test(), timeout=5.0))

    def test_outbound_real_error_takes_precedence_over_inbound_cancellation(self) -> None:
        """Verify that an outbound real error takes precedence over inbound cancellation."""

        async def _test() -> None:
            loop = asyncio.get_running_loop()
            unretrieved: list[dict[str, object]] = []
            loop.set_exception_handler(lambda _l, ctx: unretrieved.append(ctx))

            try:
                barrier = asyncio.Barrier(2)
                real_exc = MediaBackpressureOverflowError("outbound queue overflow")

                class _InboundCancelledPort(FakeTelephonyMediaAdapter):
                    async def receive_frame(self) -> None:
                        await barrier.wait()
                        raise asyncio.CancelledError()

                class _OutboundFailingPort(FakeAIMediaAdapter):
                    async def receive_frame(self) -> None:
                        await barrier.wait()
                        raise real_exc

                session = VoiceBridgeSession(
                    call_id="call-prec-out",
                    tenant_id="tenant-prec-out",
                    telephony_port=_InboundCancelledPort(),
                    ai_port=_OutboundFailingPort(),
                )

                await session.start()

                with pytest.raises(MediaBackpressureOverflowError):
                    await asyncio.wait_for(
                        session.wait_until_closed(propagate_error=True), timeout=2.0
                    )

                assert session.state == VoiceBridgeState.FAILED
                assert session.error is real_exc

                t_in = session._inbound_task
                t_out = session._outbound_task
                assert t_in is not None and getattr(t_in, "_log_traceback", None) is False
                assert t_out is not None and getattr(t_out, "_log_traceback", None) is False
            finally:
                loop.set_exception_handler(None)

        asyncio.run(asyncio.wait_for(_test(), timeout=5.0))

    def test_inner_pump_receiver_and_forwarder_concurrent_failures(self) -> None:
        """Verify that when both receiver and forwarder in an inner pump fail concurrently,

        all inner exceptions are retrieved, receiver error is chosen deterministically,
        and no duplicate error counts are recorded.
        """

        async def _test() -> None:
            loop = asyncio.get_running_loop()
            unretrieved: list[dict[str, object]] = []
            loop.set_exception_handler(lambda _l, ctx: unretrieved.append(ctx))

            try:
                barrier = asyncio.Barrier(2)
                rec_exc = InvalidAudioFrameError("receiver invalid frame")
                fwd_exc = RuntimeError("forwarder send failure")

                class _DualFailingInboundPort(FakeTelephonyMediaAdapter):
                    def __init__(self) -> None:
                        super().__init__()
                        self._called = False

                    async def receive_frame(self) -> AudioFrame | None:
                        if not self._called:
                            self._called = True
                            return create_synthetic_pcm16le_frame(b"PAYLOAD", sequence=0)
                        await barrier.wait()
                        raise rec_exc

                class _DualFailingAIPort(FakeAIMediaAdapter):
                    async def send_frame(self, frame: object) -> None:
                        await barrier.wait()
                        raise fwd_exc

                tel = _DualFailingInboundPort()
                ai = _DualFailingAIPort()

                session = VoiceBridgeSession(
                    call_id="call-inner-dual",
                    tenant_id="tenant-inner-dual",
                    telephony_port=tel,
                    ai_port=ai,
                )

                await session.start()

                with pytest.raises(InvalidAudioFrameError):
                    await asyncio.wait_for(
                        session.wait_until_closed(propagate_error=True), timeout=2.0
                    )

                assert session.state == VoiceBridgeState.FAILED
                assert session.error is rec_exc

                # Verify telemetry error counts do not double-count
                snapshot = session.metrics_snapshot()
                assert sum(snapshot.error_counts.values()) == 1

                import gc

                del session
                gc.collect()
                await asyncio.sleep(0.01)
                assert len(unretrieved) == 0
            finally:
                loop.set_exception_handler(None)

        asyncio.run(asyncio.wait_for(_test(), timeout=5.0))

    def test_supervisor_cancellation_during_active_pump_failure(self) -> None:
        """Verify that cancelling supervisor during pump failure executes clean termination,

        observes the pump error, and leaves zero unretrieved exceptions.
        """

        async def _test() -> None:
            loop = asyncio.get_running_loop()
            unretrieved: list[dict[str, object]] = []
            loop.set_exception_handler(lambda _l, ctx: unretrieved.append(ctx))

            try:
                hold_event = asyncio.Event()
                real_exc = RuntimeError("pump failed before supervisor cancelled")

                class _HoldingPort(FakeTelephonyMediaAdapter):
                    async def receive_frame(self) -> None:
                        await hold_event.wait()
                        raise real_exc

                session = VoiceBridgeSession(
                    call_id="call-sup-cancel",
                    tenant_id="tenant-sup-cancel",
                    telephony_port=_HoldingPort(),
                    ai_port=FakeAIMediaAdapter(),
                )

                await session.start()
                hold_event.set()
                assert session._inbound_task is not None
                while not session._inbound_task.done():
                    await asyncio.sleep(0.001)

                # Cancel supervisor task directly
                assert session._supervisor_task is not None
                session._supervisor_task.cancel()

                await asyncio.wait_for(
                    session.wait_until_closed(propagate_error=False), timeout=2.0
                )

                assert session.state == VoiceBridgeState.FAILED
                assert session.error is not None

                t_in = session._inbound_task
                t_out = session._outbound_task
                assert t_in is not None and getattr(t_in, "_log_traceback", None) is False
                assert t_out is not None and getattr(t_out, "_log_traceback", None) is False

                import gc

                del session, t_in, t_out
                gc.collect()
                await asyncio.sleep(0.01)
                assert len(unretrieved) == 0
            finally:
                loop.set_exception_handler(None)

        asyncio.run(asyncio.wait_for(_test(), timeout=5.0))

    def test_idempotent_repeated_wait_and_close_after_terminal_failure(self) -> None:
        """Verify that sequential and concurrent wait_until_closed and close calls after failure

        are idempotent, propagate the same error, and maintain FAILED state.
        """

        async def _test() -> None:
            fail_exc = RuntimeError("terminal fatal error")

            class _FailingPort(FakeTelephonyMediaAdapter):
                async def receive_frame(self) -> None:
                    raise fail_exc

            tel = _FailingPort()
            ai = FakeAIMediaAdapter()

            session = VoiceBridgeSession(
                call_id="call-idempotent-fail",
                tenant_id="tenant-idempotent-fail",
                telephony_port=tel,
                ai_port=ai,
            )

            await session.start()

            # First wait propagates failure
            with pytest.raises(RuntimeError) as exc1:
                await asyncio.wait_for(session.wait_until_closed(propagate_error=True), timeout=2.0)
            assert exc1.value is fail_exc

            # Repeated wait propagates same failure
            with pytest.raises(RuntimeError) as exc2:
                await asyncio.wait_for(session.wait_until_closed(propagate_error=True), timeout=2.0)
            assert exc2.value is fail_exc

            # wait without propagation returns cleanly
            await asyncio.wait_for(session.wait_until_closed(propagate_error=False), timeout=2.0)

            # Concurrent close calls after failure succeed cleanly
            await asyncio.wait_for(
                asyncio.gather(session.close(), session.close(), session.close()),
                timeout=2.0,
            )

            assert session.state == VoiceBridgeState.FAILED
            assert session.error is fail_exc
            assert tel.is_closed is True
            assert ai.is_closed is True

        asyncio.run(asyncio.wait_for(_test(), timeout=5.0))

    def test_zero_leaked_tasks_after_dual_failure(self) -> None:
        """Verify that after dual failure and session termination, zero voice bridge tasks leak."""

        async def _test() -> None:
            pre_tasks = {t for t in asyncio.all_tasks() if not t.done()}

            class _InstantFailPort(FakeTelephonyMediaAdapter):
                async def receive_frame(self) -> None:
                    raise RuntimeError("instant fail")

            session = VoiceBridgeSession(
                call_id="call-task-leak-check",
                tenant_id="tenant-task-leak-check",
                telephony_port=_InstantFailPort(),
                ai_port=_InstantFailPort(),
            )

            await session.start()
            await asyncio.wait_for(session.wait_until_closed(propagate_error=False), timeout=2.0)
            await asyncio.wait_for(session.close(), timeout=2.0)

            post_tasks = {t for t in asyncio.all_tasks() if not t.done()}
            new_tasks = post_tasks - pre_tasks - {asyncio.current_task()}
            voice_tasks = [t for t in new_tasks if t.get_name().startswith("voice-")]

            assert len(voice_tasks) == 0, f"Leaked voice tasks detected: {voice_tasks}"

        asyncio.run(asyncio.wait_for(_test(), timeout=5.0))


class TestVoiceBridgeOutboundBackpressureAndProducerLifecycle:
    """M5.1C-B regression suite: Outbound backpressure, producer lifecycle, and EOF guarantees."""

    def test_outbound_queue_saturation_times_out_and_fails_closed(self) -> None:
        """Verify that when downstream stalls beyond backpressure_timeout, session fails closed."""

        async def _test() -> None:
            block_event = asyncio.Event()

            class _StalledTelephony(FakeTelephonyMediaAdapter):
                async def send_frame(self, frame: AudioFrame) -> None:
                    await block_event.wait()

            tel = _StalledTelephony()
            ai = FakeAIMediaAdapter()
            sink = InMemoryVoiceTelemetrySink()
            session = VoiceBridgeSession(
                call_id="call-outbound-bp-timeout",
                tenant_id="tenant-bp",
                telephony_port=tel,
                ai_port=ai,
                queue_capacity=2,
                backpressure_timeout=0.05,
                telemetry_sink=sink,
            )

            for i in range(5):
                ai.push_frame(create_synthetic_pcm16le_frame(f"AI_FRAME_{i}".encode(), sequence=i))

            await session.start()
            try:
                with pytest.raises(MediaBackpressureOverflowError) as exc_info:
                    await asyncio.wait_for(
                        session.wait_until_closed(propagate_error=True), timeout=1.0
                    )
                assert "Outbound queue capacity 2 exceeded" in str(exc_info.value)
                assert "backpressure timed out after 0.05s" in str(exc_info.value)
            finally:
                block_event.set()
                await session.close()

            assert session.state == VoiceBridgeState.FAILED
            assert isinstance(session.error, MediaBackpressureOverflowError)
            snap = sink.snapshot()
            assert snap.error_counts.get(VoiceErrorCategory.BACKPRESSURE_OVERFLOW, 0) == 1
            assert session.outbound_queue_depth == 0
            assert session.inbound_queue_depth == 0
            sess_snap = session.metrics_snapshot()
            # Total unforwarded frames must balance received frames
            assert session.outbound_frames_received == sess_snap.unforwarded_outbound_frames

        asyncio.run(asyncio.wait_for(_test(), timeout=5.0))

    def test_outbound_queue_saturation_recovers_before_timeout(self) -> None:
        """Verify that short downstream delays before backpressure_timeout drain losslessly."""

        async def _test() -> None:
            resume_event = asyncio.Event()

            class _PausingTelephony(FakeTelephonyMediaAdapter):
                def __init__(self) -> None:
                    super().__init__()
                    self._paused = True

                async def send_frame(self, frame: AudioFrame) -> None:
                    if self._paused:
                        self._paused = False
                        await resume_event.wait()
                    await super().send_frame(frame)

            tel = _PausingTelephony()
            ai = FakeAIMediaAdapter()
            session = VoiceBridgeSession(
                call_id="call-outbound-bp-recover",
                tenant_id="tenant-bp",
                telephony_port=tel,
                ai_port=ai,
                queue_capacity=2,
                backpressure_timeout=0.2,
            )

            for i in range(4):
                ai.push_frame(create_synthetic_pcm16le_frame(f"AI_DATA_{i}".encode(), sequence=i))
            ai.push_eof()
            tel.push_eof()

            await session.start()
            # Downstream pauses briefly (40ms), then unblocks well before 200ms timeout
            await asyncio.sleep(0.04)
            resume_event.set()

            await asyncio.wait_for(session.wait_until_closed(propagate_error=True), timeout=2.0)
            assert session.state == VoiceBridgeState.CLOSED
            assert session.outbound_frames_transferred == 4
            snap = session.metrics_snapshot()
            assert snap.unforwarded_outbound_frames == 0
            assert session.outbound_queue_depth == 0

        asyncio.run(asyncio.wait_for(_test(), timeout=5.0))

    def test_outbound_eof_sentinel_under_saturation_times_out(self) -> None:
        """Verify that EOF sentinel insertion put(None) does not deadlock when queue is full."""

        async def _test() -> None:
            block_event = asyncio.Event()

            class _StalledTelephony(FakeTelephonyMediaAdapter):
                async def send_frame(self, frame: AudioFrame) -> None:
                    await block_event.wait()

            tel = _StalledTelephony()
            ai = FakeAIMediaAdapter()
            session = VoiceBridgeSession(
                call_id="call-outbound-eof-saturation",
                tenant_id="tenant-bp",
                telephony_port=tel,
                ai_port=ai,
                queue_capacity=2,
                backpressure_timeout=0.05,
            )

            # Capacity 2: 1 taken by forwarder, 2 queued = queue saturated
            ai.push_frame(create_synthetic_pcm16le_frame(b"FRAME_0", sequence=0))
            ai.push_frame(create_synthetic_pcm16le_frame(b"FRAME_1", sequence=1))
            ai.push_frame(create_synthetic_pcm16le_frame(b"FRAME_2", sequence=2))
            # AI reaches EOF while queue is full
            ai.push_eof()

            await session.start()
            try:
                # Must not hang! Must fail with MediaBackpressureOverflowError on EOF insertion
                with pytest.raises(MediaBackpressureOverflowError) as exc_info:
                    await asyncio.wait_for(
                        session.wait_until_closed(propagate_error=True), timeout=1.0
                    )
                assert "Outbound queue capacity 2 exceeded" in str(exc_info.value)
                assert "backpressure timed out after 0.05s" in str(exc_info.value)
            finally:
                block_event.set()
                await session.close()

            assert session.state == VoiceBridgeState.FAILED
            assert isinstance(session.error, MediaBackpressureOverflowError)
            assert session.outbound_queue_depth == 0

        asyncio.run(asyncio.wait_for(_test(), timeout=5.0))

    def test_inbound_eof_sentinel_under_saturation_times_out(self) -> None:
        """Verify that inbound EOF sentinel insertion put(None) does not deadlock when inbound queue is full."""

        async def _test() -> None:
            block_event = asyncio.Event()

            class _StalledAI(FakeAIMediaAdapter):
                async def send_frame(self, frame: AudioFrame) -> None:
                    await block_event.wait()

            tel = FakeTelephonyMediaAdapter()
            ai = _StalledAI()
            session = VoiceBridgeSession(
                call_id="call-inbound-eof-saturation",
                tenant_id="tenant-bp",
                telephony_port=tel,
                ai_port=ai,
                queue_capacity=2,
                backpressure_timeout=0.05,
            )

            # Saturate inbound queue: 1 in flight, 2 in queue
            tel.push_frame(create_synthetic_pcm16le_frame(b"IN_0", sequence=0))
            tel.push_frame(create_synthetic_pcm16le_frame(b"IN_1", sequence=1))
            tel.push_frame(create_synthetic_pcm16le_frame(b"IN_2", sequence=2))
            tel.push_eof()

            await session.start()
            try:
                with pytest.raises(MediaBackpressureOverflowError) as exc_info:
                    await asyncio.wait_for(
                        session.wait_until_closed(propagate_error=True), timeout=1.0
                    )
                assert "Inbound queue capacity 2 exceeded" in str(exc_info.value)
                assert "backpressure timed out after 0.05s" in str(exc_info.value)
            finally:
                block_event.set()
                await session.close()

            assert session.state == VoiceBridgeState.FAILED
            assert isinstance(session.error, MediaBackpressureOverflowError)
            assert session.inbound_queue_depth == 0

        asyncio.run(asyncio.wait_for(_test(), timeout=5.0))

    def test_explicit_close_while_outbound_producer_blocked(self) -> None:
        """Verify that calling close() immediately unblocks a producer waiting on backpressure."""

        async def _test() -> None:
            block_event = asyncio.Event()

            class _StalledTelephony(FakeTelephonyMediaAdapter):
                async def send_frame(self, frame: AudioFrame) -> None:
                    await block_event.wait()

            tel = _StalledTelephony()
            ai = FakeAIMediaAdapter()
            session = VoiceBridgeSession(
                call_id="call-outbound-close-race",
                tenant_id="tenant-bp",
                telephony_port=tel,
                ai_port=ai,
                queue_capacity=2,
                backpressure_timeout=10.0,  # Long timeout
            )

            for i in range(5):
                ai.push_frame(create_synthetic_pcm16le_frame(f"AI_DATA_{i}".encode(), sequence=i))

            await session.start()
            await asyncio.sleep(0.02)  # Wait for queue to saturate

            t_close_start = asyncio.get_running_loop().time()
            await session.close()
            t_close_elapsed = asyncio.get_running_loop().time() - t_close_start

            block_event.set()
            assert t_close_elapsed < 1.0, f"close() took too long: {t_close_elapsed}s"
            assert session.state in (VoiceBridgeState.CLOSED, VoiceBridgeState.FAILED)
            assert session.outbound_queue_depth == 0
            assert tel.is_closed
            assert ai.is_closed

        asyncio.run(asyncio.wait_for(_test(), timeout=5.0))

    def test_interruption_while_outbound_frame_waiting_under_backpressure(self) -> None:
        """Verify that an interruption purges queued frames even when downstream send is blocked."""

        async def _test() -> None:
            block_event = asyncio.Event()

            class _StalledTelephony(InterruptibleFakeTelephonyAdapter):
                async def send_frame(self, frame: AudioFrame) -> None:
                    await block_event.wait()

            tel = _StalledTelephony()
            ai = FakeAIMediaAdapter()
            session = VoiceBridgeSession(
                call_id="call-outbound-bp-interrupt",
                tenant_id="tenant-bp",
                telephony_port=tel,
                ai_port=ai,
                queue_capacity=2,
                backpressure_timeout=0.5,
            )

            for i in range(4):
                ai.push_frame(
                    create_synthetic_pcm16le_frame(
                        f"AI_T1_{i}".encode(),
                        sequence=i,
                        turn_id=1,
                    )
                )

            await session.start()
            await asyncio.sleep(0.02)

            outcome = await session.interrupt(turn_id=1, timeout=0.2)
            assert outcome.interrupted_turn_id == 1
            assert outcome.locally_discarded_frames > 0

            block_event.set()
            await session.close()
            assert session.outbound_queue_depth == 0

        asyncio.run(asyncio.wait_for(_test(), timeout=5.0))

    def test_concurrent_sessions_stalled_and_healthy_isolation(self) -> None:
        """Verify multi-call isolation: a stalled session failing does not affect a healthy concurrent session."""

        async def _test() -> None:
            block_event = asyncio.Event()

            class _StalledTelephony(FakeTelephonyMediaAdapter):
                async def send_frame(self, frame: AudioFrame) -> None:
                    await block_event.wait()

            # Stalled session
            tel_stall = _StalledTelephony()
            ai_stall = FakeAIMediaAdapter()
            sess_stall = VoiceBridgeSession(
                call_id="call-iso-stall",
                tenant_id="tenant-iso-1",
                telephony_port=tel_stall,
                ai_port=ai_stall,
                queue_capacity=2,
                backpressure_timeout=0.05,
            )
            for i in range(5):
                ai_stall.push_frame(create_synthetic_pcm16le_frame(b"STALL_FRAME", sequence=i))

            # Healthy session
            tel_healthy = FakeTelephonyMediaAdapter()
            ai_healthy = FakeAIMediaAdapter()
            sess_healthy = VoiceBridgeSession(
                call_id="call-iso-healthy",
                tenant_id="tenant-iso-2",
                telephony_port=tel_healthy,
                ai_port=ai_healthy,
                queue_capacity=4,
                backpressure_timeout=0.5,
            )
            for i in range(3):
                ai_healthy.push_frame(create_synthetic_pcm16le_frame(b"HEALTHY_FRAME", sequence=i))
            ai_healthy.push_eof()
            tel_healthy.push_eof()

            await sess_stall.start()
            await sess_healthy.start()

            # Healthy session should complete normally without error
            await asyncio.wait_for(
                sess_healthy.wait_until_closed(propagate_error=True), timeout=2.0
            )
            assert sess_healthy.state == VoiceBridgeState.CLOSED
            assert sess_healthy.outbound_frames_transferred == 3

            # Stalled session should fail with backpressure overflow
            with pytest.raises(MediaBackpressureOverflowError):
                await asyncio.wait_for(
                    sess_stall.wait_until_closed(propagate_error=True), timeout=2.0
                )
            assert sess_stall.state == VoiceBridgeState.FAILED
            assert isinstance(sess_stall.error, MediaBackpressureOverflowError)

            block_event.set()
            await sess_stall.close()
            await sess_healthy.close()

        asyncio.run(asyncio.wait_for(_test(), timeout=5.0))

    def test_zero_leaked_tasks_and_zero_retained_frames_after_backpressure_overflow(self) -> None:
        """Verify that backpressure overflow completely drains queues and leaves zero leaked tasks."""

        async def _test() -> None:
            pre_tasks = {t for t in asyncio.all_tasks() if not t.done()}
            block_event = asyncio.Event()

            class _StalledTelephony(FakeTelephonyMediaAdapter):
                async def send_frame(self, frame: AudioFrame) -> None:
                    await block_event.wait()

            tel = _StalledTelephony()
            ai = FakeAIMediaAdapter()
            session = VoiceBridgeSession(
                call_id="call-bp-task-leak",
                tenant_id="tenant-bp-leak",
                telephony_port=tel,
                ai_port=ai,
                queue_capacity=2,
                backpressure_timeout=0.04,
            )

            for i in range(4):
                ai.push_frame(create_synthetic_pcm16le_frame(b"BYTES_LEAK_CHECK", sequence=i))

            await session.start()
            try:
                with pytest.raises(MediaBackpressureOverflowError):
                    await asyncio.wait_for(
                        session.wait_until_closed(propagate_error=True), timeout=2.0
                    )
            finally:
                block_event.set()
                await session.close()

            assert session.inbound_queue_depth == 0
            assert session.outbound_queue_depth == 0

            post_tasks = {t for t in asyncio.all_tasks() if not t.done()}
            new_tasks = post_tasks - pre_tasks - {asyncio.current_task()}
            voice_tasks = [t for t in new_tasks if t.get_name().startswith("voice-")]
            assert len(voice_tasks) == 0, f"Leaked voice tasks detected: {voice_tasks}"

        asyncio.run(asyncio.wait_for(_test(), timeout=5.0))

    def test_single_outbound_frame_send_timeout_with_zero_queue_depth(self) -> None:
        """P1 Remediation: A single outbound frame blocked in downstream send_bytes times out with zero queue depth."""

        async def _test() -> None:
            pre_tasks = {t for t in asyncio.all_tasks() if not t.done()}
            stall_event = asyncio.Event()

            class _StalledWebSocket:
                async def send_bytes(self, data: bytes) -> None:
                    await stall_event.wait()

                async def send_text(self, text: str) -> None:
                    pass

            ctx = VerifiedConnectionContext(
                connection_id="conn-p1",
                tenant_id="tenant-p1",
                provider_id="provider-p1",
                call_id="call-p1-single-stall",
                allowed_scopes=frozenset({"voice.stream"}),
            )
            mock_ws = _StalledWebSocket()
            adapter = WebSocketTelephonyMediaAdapter(
                websocket=mock_ws,
                context=ctx,
                backpressure_timeout=10.0,
                send_timeout=0.05,
            )
            ai = FakeAIMediaAdapter()
            sink = InMemoryVoiceTelemetrySink()
            session = VoiceBridgeSession(
                call_id="call-p1-single-stall",
                tenant_id="tenant-p1",
                telephony_port=adapter,
                ai_port=ai,
                queue_capacity=8,
                backpressure_timeout=10.0,
                telemetry_sink=sink,
            )

            # Produce exactly ONE frame from AI.
            # This frame is dequeued by the forwarder, so outbound queue depth remains 0.
            # No further frames are produced.
            ai.push_frame(create_synthetic_pcm16le_frame(b"SINGLE_STALL_FRAME", sequence=0))

            await session.start()
            try:
                # With send_timeout=0.05s, the blocked send_bytes must timeout and raise MediaPortError
                with pytest.raises(MediaPortError) as exc_info:
                    await asyncio.wait_for(
                        session.wait_until_closed(propagate_error=True), timeout=2.0
                    )
                assert "Downstream WebSocket frame delivery timed out after 0.05s" in str(
                    exc_info.value
                )
            finally:
                stall_event.set()
                await session.close()

            assert session.state == VoiceBridgeState.FAILED
            assert isinstance(session.error, MediaPortError)
            assert adapter.is_closed is True

            # Telemetry verification: exactly one PORT_IO_ERROR recorded
            snap = sink.snapshot()
            assert snap.error_counts.get(VoiceErrorCategory.PORT_IO_ERROR, 0) == 1
            sess_snap = session.metrics_snapshot()
            assert sess_snap.outbound_frames_received == 1
            assert sess_snap.outbound_frames_sent == 0
            assert sess_snap.unforwarded_outbound_frames == 1
            assert sess_snap.outbound_queue_depth == 0

            # Zero task leaks
            post_tasks = {t for t in asyncio.all_tasks() if not t.done()}
            new_tasks = post_tasks - pre_tasks - {asyncio.current_task()}
            voice_tasks = [t for t in new_tasks if t.get_name().startswith("voice-")]
            assert len(voice_tasks) == 0, f"Leaked voice tasks detected: {voice_tasks}"

        asyncio.run(asyncio.wait_for(_test(), timeout=5.0))

    def test_send_deadline_recovery_before_timeout(self) -> None:
        """P1 Remediation: Downstream pauses during send_bytes that unblock before send_timeout deliver losslessly."""

        async def _test() -> None:
            resume_event = asyncio.Event()

            class _PausingWebSocket:
                def __init__(self) -> None:
                    self.sent: list[bytes] = []
                    self._paused = True

                async def send_bytes(self, data: bytes) -> None:
                    if self._paused:
                        self._paused = False
                        await resume_event.wait()
                    self.sent.append(data)

                async def send_text(self, text: str) -> None:
                    pass

            ctx = VerifiedConnectionContext(
                connection_id="conn-p1-rec",
                tenant_id="tenant-p1",
                provider_id="provider-p1",
                call_id="call-p1-recover",
                allowed_scopes=frozenset({"voice.stream"}),
            )
            mock_ws = _PausingWebSocket()
            adapter = WebSocketTelephonyMediaAdapter(
                websocket=mock_ws,
                context=ctx,
                send_timeout=0.2,
            )
            ai = FakeAIMediaAdapter()
            session = VoiceBridgeSession(
                call_id="call-p1-recover",
                tenant_id="tenant-p1",
                telephony_port=adapter,
                ai_port=ai,
                queue_capacity=8,
            )

            ai.push_frame(create_synthetic_pcm16le_frame(b"PAUSED_RECOVER_FRAME", sequence=0))
            ai.push_eof()
            adapter.push_inbound_eof_nowait()

            await session.start()
            # Downstream pauses 30ms then unblocks well before 200ms send_timeout
            await asyncio.sleep(0.03)
            resume_event.set()

            await asyncio.wait_for(session.wait_until_closed(propagate_error=True), timeout=2.0)
            assert session.state == VoiceBridgeState.CLOSED
            assert session.outbound_frames_transferred == 1
            assert len(mock_ws.sent) == 1
            sess_snap = session.metrics_snapshot()
            assert sess_snap.unforwarded_outbound_frames == 0

        asyncio.run(asyncio.wait_for(_test(), timeout=5.0))

    def test_explicit_close_during_downstream_send_stall(self) -> None:
        """P1 Remediation: Explicit session.close() promptly unblocks a forwarder hung in downstream send_bytes."""

        async def _test() -> None:
            stall_event = asyncio.Event()

            class _StalledWebSocket:
                async def send_bytes(self, data: bytes) -> None:
                    await stall_event.wait()

                async def send_text(self, text: str) -> None:
                    pass

            ctx = VerifiedConnectionContext(
                connection_id="conn-p1-close",
                tenant_id="tenant-p1",
                provider_id="provider-p1",
                call_id="call-p1-close-during-send",
                allowed_scopes=frozenset({"voice.stream"}),
            )
            mock_ws = _StalledWebSocket()
            adapter = WebSocketTelephonyMediaAdapter(
                websocket=mock_ws,
                context=ctx,
                send_timeout=10.0,  # Long timeout
            )
            ai = FakeAIMediaAdapter()
            session = VoiceBridgeSession(
                call_id="call-p1-close-during-send",
                tenant_id="tenant-p1",
                telephony_port=adapter,
                ai_port=ai,
            )

            ai.push_frame(create_synthetic_pcm16le_frame(b"STALL_THEN_CLOSE", sequence=0))
            await session.start()
            await asyncio.sleep(0.02)  # Wait for forwarder to enter send_bytes

            t0 = asyncio.get_running_loop().time()
            await session.close()
            t_elapsed = asyncio.get_running_loop().time() - t0

            stall_event.set()
            assert t_elapsed < 1.0, f"session.close() took too long: {t_elapsed}s"
            assert session.state in (VoiceBridgeState.CLOSED, VoiceBridgeState.FAILED)
            assert adapter.is_closed is True

        asyncio.run(asyncio.wait_for(_test(), timeout=5.0))

    def test_adapter_close_behavior_following_send_timeout(self) -> None:
        """P1 Remediation: Following send timeout, adapter is terminal, turn waiters unblock, subsequent I/O raises."""

        async def _test() -> None:
            stall_event = asyncio.Event()

            class _StalledWebSocket:
                async def send_bytes(self, data: bytes) -> None:
                    await stall_event.wait()

                async def send_text(self, text: str) -> None:
                    pass

            ctx = VerifiedConnectionContext(
                connection_id="conn-p1-adapter-test",
                tenant_id="tenant-p1",
                provider_id="provider-p1",
                call_id="call-p1-adapter-test",
                allowed_scopes=frozenset({"voice.stream"}),
            )
            mock_ws = _StalledWebSocket()
            adapter = WebSocketTelephonyMediaAdapter(
                websocket=mock_ws,
                context=ctx,
                send_timeout=0.04,
            )

            frame = create_synthetic_pcm16le_frame(b"ADAPTER_TIMEOUT_FRAME", sequence=0)
            try:
                with pytest.raises(MediaPortError) as exc_info:
                    await adapter.send_frame(frame)
                assert "Downstream WebSocket frame delivery timed out after 0.04s" in str(
                    exc_info.value
                )
            finally:
                stall_event.set()

            # Verify adapter lifecycle state
            assert adapter.is_closed is True
            assert adapter.frames_sent == 0
            assert adapter.bytes_sent == 0

            # Subsequent operations must fail with MediaPortClosedError
            with pytest.raises(MediaPortClosedError):
                await adapter.send_frame(frame)
            with pytest.raises(MediaPortClosedError):
                await adapter.send_control_text("test")
            with pytest.raises(MediaPortClosedError):
                await adapter.receive_frame()

            # Idempotent close
            await adapter.close()
            assert adapter.is_closed is True

        asyncio.run(asyncio.wait_for(_test(), timeout=5.0))

    def test_concurrent_sessions_single_frame_send_stall_isolation(self) -> None:
        """P1 Remediation: A single-frame downstream send stall on one call does not impact another healthy call."""

        async def _test() -> None:
            stall_event = asyncio.Event()

            class _StalledWS:
                async def send_bytes(self, data: bytes) -> None:
                    await stall_event.wait()

                async def send_text(self, text: str) -> None:
                    pass

            class _HealthyWS:
                def __init__(self) -> None:
                    self.sent: list[bytes] = []

                async def send_bytes(self, data: bytes) -> None:
                    self.sent.append(data)

                async def send_text(self, text: str) -> None:
                    pass

            ctx_stall = VerifiedConnectionContext(
                connection_id="conn-iso-s",
                tenant_id="tenant-iso-1",
                provider_id="provider-p1",
                call_id="call-iso-stall",
                allowed_scopes=frozenset({"voice.stream"}),
            )
            ctx_healthy = VerifiedConnectionContext(
                connection_id="conn-iso-h",
                tenant_id="tenant-iso-2",
                provider_id="provider-p1",
                call_id="call-iso-healthy",
                allowed_scopes=frozenset({"voice.stream"}),
            )

            adapter_stall = WebSocketTelephonyMediaAdapter(
                websocket=_StalledWS(),
                context=ctx_stall,
                send_timeout=0.05,
            )
            adapter_healthy = WebSocketTelephonyMediaAdapter(
                websocket=_HealthyWS(),
                context=ctx_healthy,
                send_timeout=0.5,
            )

            ai_stall = FakeAIMediaAdapter()
            ai_healthy = FakeAIMediaAdapter()

            sess_stall = VoiceBridgeSession(
                call_id="call-iso-stall",
                tenant_id="tenant-iso-1",
                telephony_port=adapter_stall,
                ai_port=ai_stall,
            )
            sess_healthy = VoiceBridgeSession(
                call_id="call-iso-healthy",
                tenant_id="tenant-iso-2",
                telephony_port=adapter_healthy,
                ai_port=ai_healthy,
            )

            ai_stall.push_frame(create_synthetic_pcm16le_frame(b"STALL_SINGLE", sequence=0))
            ai_healthy.push_frame(create_synthetic_pcm16le_frame(b"HEALTHY_SINGLE", sequence=0))
            ai_healthy.push_eof()
            adapter_healthy.push_inbound_eof_nowait()

            await sess_stall.start()
            await sess_healthy.start()

            # Healthy session must finish normally
            await asyncio.wait_for(
                sess_healthy.wait_until_closed(propagate_error=True), timeout=2.0
            )
            assert sess_healthy.state == VoiceBridgeState.CLOSED
            assert sess_healthy.outbound_frames_transferred == 1

            # Stalled session must fail with MediaPortError after 0.05s
            with pytest.raises(MediaPortError):
                await asyncio.wait_for(
                    sess_stall.wait_until_closed(propagate_error=True), timeout=2.0
                )
            assert sess_stall.state == VoiceBridgeState.FAILED
            assert isinstance(sess_stall.error, MediaPortError)

            stall_event.set()
            await sess_stall.close()
            await sess_healthy.close()

        asyncio.run(asyncio.wait_for(_test(), timeout=5.0))
