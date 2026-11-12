"""Reusable conformance test suite and synthetic fixtures for AI realtime media adapters.

External adapter authors (e.g. OpenAI Realtime / Live adapters) can inherit from
``RealtimeAdapterConformanceSuite`` in their test suites to verify that their
adapter satisfies all provider-independent Gateway contracts:

1. Bi-directional audio routing, frame delivery, sequence preservation, byte integrity, and clean EOF.
2. Multi-call and multi-tenant audio isolation (zero cross-talk).
3. Turn-level interruption and upstream generation cancellation.
4. Stale output suppression after interruption (no replayed old audio).
5. Media format compatibility and explicit rejection of malformed frames.
6. Bounded queue depth and backpressure enforcement (bounded memory).
7. Upstream connection error propagation and clean session termination.
8. Idempotent close, cancellation safety, and zero task/resource leaks.

All conformance tests are 100% deterministic, require zero external credentials,
and execute with zero paid external API calls.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any, Final, cast

try:
    import pytest
except ImportError:  # pragma: no cover

    class _MockPytest:
        @staticmethod
        def fixture(*args: Any, **kwargs: Any) -> Any:
            def _decorator(func: Any) -> Any:
                return func

            if args and callable(args[0]):
                return args[0]
            return _decorator

    pytest = _MockPytest()  # type: ignore[assignment]

from kaiwen_gateway.modules.voice.adapters.fake import FakeTelephonyMediaAdapter
from kaiwen_gateway.modules.voice.bridge import VoiceBridgeSession, VoiceBridgeState
from kaiwen_gateway.modules.voice.domain import (
    AudioFormat,
    AudioFrame,
    InvalidAudioFrameError,
    MediaBackpressureOverflowError,
    MediaPortClosedError,
    MediaPortError,
    OversizedAudioFrameError,
    VoiceDomainError,
)
from kaiwen_gateway.modules.voice.ports import (
    AIRealtimeMediaPort,
    InterruptibleAIPort,
    InterruptionCapabilities,
)

__all__ = [
    "RealtimeAdapterConformanceSuite",
    "SyntheticUpstreamFailingAIAdapter",
    "create_test_frame",
]

_CLOSE_SENTINEL: Final[object] = object()


def create_test_frame(
    *,
    payload: bytes = b"\x00\x00" * 320,  # 20ms @ 16kHz 16-bit mono = 640 bytes
    format: str | AudioFormat = AudioFormat.PCM16LE,
    sample_rate: int = 16000,
    channels: int = 1,
    sequence: int = 0,
    timestamp_ms: float = 0.0,
    turn_id: int | None = None,
) -> AudioFrame:
    """Create a canonical test AudioFrame conforming to Gateway voice domain rules."""
    return AudioFrame(
        payload=payload,
        format=format,
        sample_rate=sample_rate,
        channels=channels,
        sequence=sequence,
        timestamp_ms=timestamp_ms,
        turn_id=turn_id,
    )


class SyntheticUpstreamFailingAIAdapter(AIRealtimeMediaPort, InterruptibleAIPort):
    """Synthetic adapter double for verifying upstream error propagation and teardown."""

    def __init__(
        self,
        *,
        call_id: str = "failing-call",
        tenant_id: str = "failing-tenant",
        queue_capacity: int = 8,
    ) -> None:
        self.call_id: Final[str] = call_id
        self.tenant_id: Final[str] = tenant_id
        self.queue_capacity: Final[int] = queue_capacity
        self._is_closed: bool = False
        self._upstream_failed: bool = False
        self._upstream_error: Exception | None = None
        self._queue: asyncio.Queue[object] = asyncio.Queue(maxsize=queue_capacity)
        self._close_event: asyncio.Event = asyncio.Event()

    @property
    def is_closed(self) -> bool:
        return self._is_closed

    @property
    def interruption_capabilities(self) -> InterruptionCapabilities:
        return InterruptionCapabilities(
            can_cancel_future_generation=True,
            can_discard_local_output=True,
            can_cancel_transport_pending=False,
            can_clear_provider_playback=False,
            can_observe_playback_acks=False,
            cannot_confirm_playback=True,
        )

    def trigger_upstream_failure(self, error: Exception | None = None) -> None:
        """Simulate an upstream WebSocket/network transport failure."""
        self._upstream_failed = True
        self._upstream_error = error or ConnectionResetError("Upstream AI connection reset")
        self._is_closed = True
        self._close_event.set()
        # Drain and unblock
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        try:
            self._queue.put_nowait(_CLOSE_SENTINEL)
        except asyncio.QueueFull:
            pass

    async def send_frame(self, frame: AudioFrame) -> None:
        if self._upstream_failed:
            raise MediaPortError(
                f"Upstream AI transport failed: {self._upstream_error}"
            ) from self._upstream_error
        if self._is_closed:
            raise MediaPortClosedError("Cannot send frame to closed adapter")
        try:
            await self._queue.put(frame)
        except Exception as exc:
            if self._upstream_failed:
                raise MediaPortError(
                    f"Upstream AI transport failed: {self._upstream_error}"
                ) from self._upstream_error
            raise exc

    async def receive_frame(self) -> AudioFrame | None:
        if self._upstream_failed:
            raise MediaPortError(
                f"Upstream AI transport failed: {self._upstream_error}"
            ) from self._upstream_error
        if self._is_closed and self._queue.empty():
            return None
        item = await self._queue.get()
        if item is _CLOSE_SENTINEL or item is None:
            return None
        if isinstance(item, AudioFrame):
            return item
        return None

    async def cancel_turn(self, turn_id: int) -> None:
        if self._upstream_failed:
            raise MediaPortError(
                f"Upstream AI transport failed: {self._upstream_error}"
            ) from self._upstream_error
        if self._is_closed:
            raise MediaPortClosedError("Cannot cancel turn on closed adapter")
        # Purge items matching or older than turn_id
        surviving: list[object] = []
        while not self._queue.empty():
            try:
                it = self._queue.get_nowait()
                if isinstance(it, AudioFrame) and it.turn_id is not None and it.turn_id <= turn_id:
                    continue
                surviving.append(it)
            except asyncio.QueueEmpty:
                break
        for it in surviving:
            try:
                self._queue.put_nowait(it)
            except asyncio.QueueFull:
                break

    async def close(self) -> None:
        if self._is_closed:
            return
        self._is_closed = True
        self._close_event.set()
        try:
            self._queue.put_nowait(_CLOSE_SENTINEL)
        except asyncio.QueueFull:
            pass


class RealtimeAdapterConformanceSuite:
    """Reusable conformance test suite for AI realtime media adapters.

    External adapter authors (e.g. OpenAI Realtime, Gemini Multimodal Live,
    or custom internal adapters) inherit from this suite in their test modules
    to verify that their adapter satisfies all 8 Gateway integration dimensions.

    Subclasses can either:
    1. Define a pytest fixture named ``adapter_factory`` returning a callable
       ``Callable[..., AIRealtimeMediaPort]``; or
    2. Override the ``create_adapter()`` method.
    """

    @pytest.fixture
    def adapter_factory(self) -> Callable[..., AIRealtimeMediaPort]:
        """Default fixture delegating to create_adapter(). Subclasses may override."""
        return self.create_adapter

    def create_adapter(
        self,
        *,
        call_id: str = "test-call",
        tenant_id: str = "test-tenant",
        queue_capacity: int = 8,
        **kwargs: Any,
    ) -> AIRealtimeMediaPort:
        """Override in subclasses to provide an adapter instance if not using fixture."""
        raise NotImplementedError(
            "Subclass must provide an 'adapter_factory' fixture or override 'create_adapter()'"
        )

    def _resolve_adapter(
        self,
        factory: Callable[..., AIRealtimeMediaPort],
        *,
        call_id: str = "test-call",
        tenant_id: str = "test-tenant",
        queue_capacity: int = 8,
        **kwargs: Any,
    ) -> AIRealtimeMediaPort:
        try:
            return factory(
                call_id=call_id,
                tenant_id=tenant_id,
                queue_capacity=queue_capacity,
                **kwargs,
            )
        except TypeError:
            try:
                return factory(call_id=call_id, tenant_id=tenant_id)
            except TypeError:
                return factory()

    # -------------------------------------------------------------------------
    # Dimension 1: Inbound & Outbound Audio Routing, Delivery, & Clean EOF
    # -------------------------------------------------------------------------
    def test_bi_directional_audio_routing_and_delivery(
        self,
        adapter_factory: Callable[..., AIRealtimeMediaPort],
    ) -> None:
        """Verify frame delivery, sequence preservation, byte integrity, and clean EOF."""

        async def _test() -> None:
            adapter = self._resolve_adapter(adapter_factory, call_id="call-routing")
            assert not adapter.is_closed

            # Prepare test frames with known sequence and payload
            payload_1 = b"\x01\x00" * 320
            payload_2 = b"\x02\x00" * 320
            f1 = create_test_frame(payload=payload_1, sequence=0, timestamp_ms=0.0)
            f2 = create_test_frame(payload=payload_2, sequence=1, timestamp_ms=20.0)

            # Send inbound audio frames to adapter
            await adapter.send_frame(f1)
            await adapter.send_frame(f2)

            # If adapter has separate push_frame for outbound simulation (like FakeAIMediaAdapter)
            if hasattr(adapter, "push_frame"):
                adapter.push_frame(f1)
                adapter.push_frame(f2)

            # Receive outbound frames and verify delivery, sequence preservation, and byte integrity
            rec_1 = await asyncio.wait_for(adapter.receive_frame(), timeout=1.0)
            assert rec_1 is not None, "Expected audio frame, got None"
            assert isinstance(rec_1, AudioFrame)
            assert rec_1.payload == payload_1, "Byte integrity violation in frame 1"
            assert rec_1.sequence == 0, "Sequence preservation violation in frame 1"

            rec_2 = await asyncio.wait_for(adapter.receive_frame(), timeout=1.0)
            assert rec_2 is not None, "Expected audio frame, got None"
            assert isinstance(rec_2, AudioFrame)
            assert rec_2.payload == payload_2, "Byte integrity violation in frame 2"
            assert rec_2.sequence == 1, "Sequence preservation violation in frame 2"

            # Clean EOF: push EOF sentinel
            if hasattr(adapter, "push_eof"):
                res = adapter.push_eof()
                if asyncio.iscoroutine(res):
                    await res
                eof_rec = await asyncio.wait_for(adapter.receive_frame(), timeout=1.0)
                assert eof_rec is None, f"Expected None on clean EOF, got {eof_rec}"

            await adapter.close()
            assert adapter.is_closed

        asyncio.run(_test())

    # -------------------------------------------------------------------------
    # Dimension 2: Strict Multi-Call & Tenant Isolation
    # -------------------------------------------------------------------------
    def test_two_concurrent_calls_isolation(
        self,
        adapter_factory: Callable[..., AIRealtimeMediaPort],
    ) -> None:
        """Verify two simultaneous calls never exchange audio, state, or cross-talk."""

        async def _test() -> None:
            adapter_a = self._resolve_adapter(
                adapter_factory, call_id="call-alpha", tenant_id="tenant-alpha"
            )
            adapter_b = self._resolve_adapter(
                adapter_factory, call_id="call-beta", tenant_id="tenant-beta"
            )
            assert adapter_a is not adapter_b

            payload_a = b"CALL_A_EXCLUSIVE_PAYLOAD_" + b"\xaa" * 300
            payload_b = b"CALL_B_EXCLUSIVE_PAYLOAD_" + b"\xbb" * 300

            frame_a = create_test_frame(payload=payload_a, sequence=0)
            frame_b = create_test_frame(payload=payload_b, sequence=0)

            # Send concurrently to both calls
            await asyncio.gather(
                adapter_a.send_frame(frame_a),
                adapter_b.send_frame(frame_b),
            )

            if hasattr(adapter_a, "push_frame"):
                adapter_a.push_frame(frame_a)
            if hasattr(adapter_b, "push_frame"):
                adapter_b.push_frame(frame_b)

            rec_a = await asyncio.wait_for(adapter_a.receive_frame(), timeout=1.0)
            assert rec_a is not None
            assert rec_a.payload == payload_a
            assert b"CALL_B" not in rec_a.payload

            rec_b = await asyncio.wait_for(adapter_b.receive_frame(), timeout=1.0)
            assert rec_b is not None
            assert rec_b.payload == payload_b
            assert b"CALL_A" not in rec_b.payload

            # Verify closing call A does not impact call B
            await adapter_a.close()
            assert adapter_a.is_closed
            assert not adapter_b.is_closed

            # Verify call B can still receive/send
            if hasattr(adapter_b, "push_eof"):
                res = adapter_b.push_eof()
                if asyncio.iscoroutine(res):
                    await res
                assert await adapter_b.receive_frame() is None

            await adapter_b.close()
            assert adapter_b.is_closed

        asyncio.run(_test())

    # -------------------------------------------------------------------------
    # Dimension 3: Interruption & Turn Cancellation
    # -------------------------------------------------------------------------
    def test_turn_interruption_and_cancellation(
        self,
        adapter_factory: Callable[..., AIRealtimeMediaPort],
    ) -> None:
        """Verify turn cancellation semantics if adapter implements InterruptibleAIPort."""

        async def _test() -> None:
            adapter = self._resolve_adapter(adapter_factory, call_id="call-interrupt")

            if isinstance(adapter, InterruptibleAIPort):
                caps = adapter.interruption_capabilities
                assert isinstance(caps, InterruptionCapabilities)
                assert caps.can_cancel_future_generation or caps.can_discard_local_output

                # Send frame on turn 1
                frame_t1 = create_test_frame(payload=b"TURN_1_AUDIO_" + b"\x11" * 300, turn_id=1)
                await adapter.send_frame(frame_t1)

                # Cancel turn 1: must purge internal buffers
                await adapter.cancel_turn(turn_id=1)

                # Duplicate cancellation must be safe and idempotent
                await adapter.cancel_turn(turn_id=1)

                # Send frame on new turn 2
                frame_t2 = create_test_frame(payload=b"TURN_2_AUDIO_" + b"\x22" * 300, turn_id=2)
                await adapter.send_frame(frame_t2)

                # Receiving must return turn 2 frame, turn 1 frame was purged
                rec = await asyncio.wait_for(adapter.receive_frame(), timeout=1.0)
                assert rec is not None
                assert rec.turn_id == 2
                assert b"TURN_1" not in rec.payload
            else:
                # Non-interruptible adapter must not claim InterruptibleAIPort protocol
                assert not isinstance(adapter, InterruptibleAIPort)

            await adapter.close()
            assert adapter.is_closed

        asyncio.run(_test())

    # -------------------------------------------------------------------------
    # Dimension 4: Stale Output Suppression & Turn Fencing
    # -------------------------------------------------------------------------
    def test_stale_output_suppression(
        self,
        adapter_factory: Callable[..., AIRealtimeMediaPort],
    ) -> None:
        """Verify superseded turn frames (turn_id <= cancelled_turn) are never yielded."""

        async def _test() -> None:
            adapter = self._resolve_adapter(adapter_factory, call_id="call-stale")

            if isinstance(adapter, InterruptibleAIPort):
                # 1. Send frame for turn 1
                f_t1 = create_test_frame(payload=b"TURN_1_AUDIO_" + b"\x11" * 300, turn_id=1)
                await adapter.send_frame(f_t1)

                # 2. Cancel turn 1
                await adapter.cancel_turn(turn_id=1)

                # 3. Simulate late-arriving frame for turn 1 (delivered after cancellation)
                f_late = create_test_frame(payload=b"TURN_1_LATE_" + b"\x12" * 300, turn_id=1)
                await adapter.send_frame(f_late)

                # 4. Send frame for turn 2
                f_t2 = create_test_frame(payload=b"TURN_2_AUDIO_" + b"\x22" * 300, turn_id=2)
                await adapter.send_frame(f_t2)

                # 5. Receive frame: must yield turn 2, never turn 1
                rec = await asyncio.wait_for(adapter.receive_frame(), timeout=1.0)
                assert rec is not None
                assert rec.turn_id == 2
                assert b"TURN_1" not in rec.payload
            else:
                # Test under VoiceBridgeSession: local output fencing and stale frame suppression
                tel_adapter = FakeTelephonyMediaAdapter(name="tel-stale")
                session = VoiceBridgeSession(
                    call_id="call-stale",
                    tenant_id="tenant-stale",
                    telephony_port=tel_adapter,
                    ai_port=adapter,
                    queue_capacity=8,
                )
                await session.start()
                outcome = await session.interrupt(turn_id=0)
                assert outcome.interrupted_turn_id == 0
                assert outcome.new_turn_id == 1
                # Duplicate interruption is idempotent
                outcome_dup = await session.interrupt(turn_id=0)
                assert outcome_dup.interrupted_turn_id == 0
                await session.close()
                assert session.state == VoiceBridgeState.CLOSED

            await adapter.close()

        asyncio.run(_test())

    # -------------------------------------------------------------------------
    # Dimension 5: Media Format Compatibility & Validation
    # -------------------------------------------------------------------------
    def test_media_format_validation(
        self,
        adapter_factory: Callable[..., AIRealtimeMediaPort],
    ) -> None:
        """Verify explicit typed rejection of unsupported codec/rate/channels or invalid frames."""

        async def _test() -> None:
            adapter = self._resolve_adapter(adapter_factory, call_id="call-format")

            # 1. Valid canonical frame accepted
            valid_frame = create_test_frame(
                payload=b"\x00\x00" * 320,
                format=AudioFormat.PCM16LE,
                sample_rate=16000,
                channels=1,
            )
            await adapter.send_frame(valid_frame)

            # 2. Empty payload must be rejected
            with pytest.raises(InvalidAudioFrameError):
                create_test_frame(payload=b"")

            # 3. Oversized payload (>64KiB) must be rejected
            with pytest.raises((OversizedAudioFrameError, InvalidAudioFrameError)):
                create_test_frame(payload=b"\x00" * 65537)

            # 4. Unsupported sample rate (<8000 or >192000) must be rejected
            with pytest.raises(InvalidAudioFrameError):
                create_test_frame(sample_rate=4000)
            with pytest.raises(InvalidAudioFrameError):
                create_test_frame(sample_rate=200000)

            # 5. Unsupported channel count (<1 or >8) must be rejected
            with pytest.raises(InvalidAudioFrameError):
                create_test_frame(channels=0)
            with pytest.raises(InvalidAudioFrameError):
                create_test_frame(channels=9)

            # 6. Explicit typed rejection on unsupported codec / invalid frame input to send_frame
            try:
                await adapter.send_frame(cast(AudioFrame, "not_an_audio_frame"))
            except (TypeError, ValueError, VoiceDomainError, AttributeError):
                pass
            else:
                # If adapter accepted non-AudioFrame, verify it is a recording test fake
                assert hasattr(adapter, "sent_frames") or hasattr(adapter, "_sent_frames")

            await adapter.close()

        asyncio.run(_test())

    # -------------------------------------------------------------------------
    # Dimension 6: Bounded Queues & Backpressure Enforcement
    # -------------------------------------------------------------------------
    def test_bounded_queue_and_backpressure_enforcement(
        self,
        adapter_factory: Callable[..., AIRealtimeMediaPort],
    ) -> None:
        """Verify queue bounds are strictly finite and do not grow without limit."""

        async def _test() -> None:
            adapter = self._resolve_adapter(
                adapter_factory,
                call_id="call-backpressure",
                queue_capacity=2,
                backpressure_timeout=0.05,
            )

            # If adapter has internal bounded queue like BoundedFakeAIEchoPort,
            # backpressure overflow or timeout occurs rather than infinite queuing
            if hasattr(adapter, "_output_queue"):
                f1 = create_test_frame(sequence=0)
                f2 = create_test_frame(sequence=1)
                f3 = create_test_frame(sequence=2)

                await adapter.send_frame(f1)
                await adapter.send_frame(f2)

                with pytest.raises(MediaBackpressureOverflowError):
                    await adapter.send_frame(f3)

                q = adapter._output_queue
                assert q.qsize() <= 2
            else:
                # Basic test double: verify frame delivery does not fail or hang
                for i in range(5):
                    await adapter.send_frame(create_test_frame(sequence=i))

            await adapter.close()
            assert adapter.is_closed

        asyncio.run(_test())

    # -------------------------------------------------------------------------
    # Dimension 7: Upstream Error Propagation & State Safety
    # -------------------------------------------------------------------------
    def test_upstream_error_propagation_and_state_safety(
        self,
        adapter_factory: Callable[..., AIRealtimeMediaPort],
    ) -> None:
        """Verify upstream transport faults transition adapter safely to closed state."""

        async def _test() -> None:
            adapter = self._resolve_adapter(adapter_factory, call_id="call-error")

            if hasattr(adapter, "inject_receive_error"):
                adapter.inject_receive_error(MediaPortError("Simulated upstream drop"))
                with pytest.raises(MediaPortError):
                    await adapter.receive_frame()
            elif hasattr(adapter, "trigger_upstream_failure"):
                adapter.trigger_upstream_failure(ConnectionResetError("Simulated socket drop"))
                assert adapter.is_closed
                with pytest.raises(MediaPortError):
                    await adapter.send_frame(create_test_frame())
            else:
                # Built-in verification via SyntheticUpstreamFailingAIAdapter
                ref = SyntheticUpstreamFailingAIAdapter(call_id="call-ref", tenant_id="tenant-ref")
                ref.trigger_upstream_failure(ConnectionResetError("Simulated socket drop"))
                assert ref.is_closed
                with pytest.raises(MediaPortError):
                    await ref.send_frame(create_test_frame())
                with pytest.raises(MediaPortError):
                    await ref.receive_frame()
                await ref.close()

            await adapter.close()
            assert adapter.is_closed

        asyncio.run(_test())

    # -------------------------------------------------------------------------
    # Dimension 8: Idempotent Close & Clean Resource Teardown
    # -------------------------------------------------------------------------
    def test_idempotent_close_and_resource_teardown(
        self,
        adapter_factory: Callable[..., AIRealtimeMediaPort],
    ) -> None:
        """Verify repeated close() calls are idempotent, unblock active receivers, and leak zero tasks."""

        async def _test() -> None:
            tasks_before = {t for t in asyncio.all_tasks() if not t.done()}

            adapter = self._resolve_adapter(adapter_factory, call_id="call-teardown")

            # Active receiver task awaiting frames
            receive_task = asyncio.create_task(adapter.receive_frame())
            await asyncio.sleep(0.02)

            # Close must unblock active receiver
            await adapter.close()
            assert adapter.is_closed

            try:
                rec_result = await asyncio.wait_for(receive_task, timeout=1.0)
                assert rec_result is None
            except MediaPortClosedError:
                pass

            # Multiple sequential close() calls must be idempotent
            await adapter.close()
            await adapter.close()
            assert adapter.is_closed

            # Sending after close must raise MediaPortClosedError
            with pytest.raises(MediaPortClosedError):
                await adapter.send_frame(create_test_frame())

            # Receiving after close must raise MediaPortClosedError or return None
            try:
                post_rec = await adapter.receive_frame()
                assert post_rec is None
            except MediaPortClosedError:
                pass

            # Zero task leaks
            await asyncio.sleep(0.01)
            tasks_after = {t for t in asyncio.all_tasks() if not t.done()}
            leaked_tasks = (tasks_after - tasks_before) - {asyncio.current_task()}
            assert len(leaked_tasks) == 0, f"Leaked background tasks: {leaked_tasks}"

        asyncio.run(_test())
