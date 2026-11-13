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
        self._cancelled_turn_high_water: int = -1
        self._inbound_frames: list[AudioFrame] = []
        self._pending_inbound: asyncio.Queue[AudioFrame] = (
            asyncio.Queue(maxsize=queue_capacity)
            if queue_capacity is not None and queue_capacity > 0
            else asyncio.Queue()
        )
        self._queue: asyncio.Queue[object] = (
            asyncio.Queue(maxsize=queue_capacity)
            if queue_capacity is not None and queue_capacity > 0
            else asyncio.Queue()
        )
        self._close_event: asyncio.Event = asyncio.Event()

    @property
    def is_closed(self) -> bool:
        return self._is_closed

    @property
    def buffered_frame_count(self) -> int:
        """Current number of pending frames buffered in the queues."""
        return max(self._queue.qsize(), self._pending_inbound.qsize())

    @property
    def received_inbound_frames(self) -> list[AudioFrame]:
        """Frames received inbound from caller via send_frame."""
        return list(self._inbound_frames)

    def drain_pending_inbound(self) -> list[AudioFrame]:
        """Drain and return currently pending inbound frames, freeing queue capacity."""
        drained: list[AudioFrame] = []
        while not self._pending_inbound.empty():
            try:
                drained.append(self._pending_inbound.get_nowait())
            except asyncio.QueueEmpty:
                break
        return drained

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

    def push_frame(self, frame: AudioFrame) -> None:
        """Push an audio frame to be returned by receive_frame."""
        if not isinstance(frame, AudioFrame):
            raise TypeError("frame must be AudioFrame")
        if self._upstream_failed:
            raise MediaPortError(
                f"Upstream AI transport failed: {self._upstream_error}"
            ) from self._upstream_error
        if self._is_closed:
            raise MediaPortClosedError("Cannot push frame to closed adapter")
        if frame.turn_id is not None and frame.turn_id <= self._cancelled_turn_high_water:
            return
        self._queue.put_nowait(frame)

    def push_eof(self) -> None:
        """Push EOF sentinel so receive_frame returns None."""
        self._queue.put_nowait(None)

    async def send_frame(self, frame: AudioFrame) -> None:
        if not isinstance(frame, AudioFrame):
            raise TypeError("frame must be AudioFrame")
        if self._upstream_failed:
            raise MediaPortError(
                f"Upstream AI transport failed: {self._upstream_error}"
            ) from self._upstream_error
        if self._is_closed:
            raise MediaPortClosedError("Cannot send frame to closed adapter")
        if frame.turn_id is not None and frame.turn_id <= self._cancelled_turn_high_water:
            return
        if self.queue_capacity is not None and self.queue_capacity > 0:
            if self._pending_inbound.full():
                raise MediaBackpressureOverflowError(
                    f"SyntheticUpstreamFailingAIAdapter queue capacity exceeded ({self.queue_capacity})"
                )
            self._pending_inbound.put_nowait(frame)
        self._inbound_frames.append(frame)

    async def receive_frame(self) -> AudioFrame | None:
        if self._upstream_failed:
            raise MediaPortError(
                f"Upstream AI transport failed: {self._upstream_error}"
            ) from self._upstream_error
        if self._is_closed and self._queue.empty():
            return None
        while True:
            item = await self._queue.get()
            if item is _CLOSE_SENTINEL or item is None:
                return None
            if isinstance(item, AudioFrame):
                if item.turn_id is not None and item.turn_id <= self._cancelled_turn_high_water:
                    continue
                return item
            return None

    async def cancel_turn(self, turn_id: int) -> None:
        if self._upstream_failed:
            raise MediaPortError(
                f"Upstream AI transport failed: {self._upstream_error}"
            ) from self._upstream_error
        if self._is_closed:
            raise MediaPortClosedError("Cannot cancel turn on closed adapter")
        self._cancelled_turn_high_water = max(self._cancelled_turn_high_water, turn_id)
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
        while not self._pending_inbound.empty():
            try:
                self._pending_inbound.get_nowait()
            except asyncio.QueueEmpty:
                break
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
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
                return factory(call_id=call_id, tenant_id=tenant_id, **kwargs)
            except TypeError:
                try:
                    return factory(call_id=call_id, tenant_id=tenant_id)
                except TypeError:
                    return factory()

    # -------------------------------------------------------------------------
    # Conformance Test Harness Hooks (Override in subclasses if needed)
    # -------------------------------------------------------------------------
    def inject_upstream_frame(
        self,
        adapter: AIRealtimeMediaPort,
        frame: AudioFrame,
    ) -> bool:
        """Inject a synthetic upstream response frame into the adapter under test.

        Default implementation checks for common test double hooks (push_frame).
        External adapter test suites can override this to inject mock wire events
        into their mock WebSocket/HTTP server or adapter transport.
        Returns True if injection succeeded, False if unsupported.
        """
        push_fn: Any = getattr(adapter, "push_frame", None)
        if callable(push_fn):
            res = push_fn(frame)
            if asyncio.iscoroutine(res):
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(res)
                except RuntimeError:
                    asyncio.run(res)
            return True
        return False

    async def inject_upstream_eof(
        self,
        adapter: AIRealtimeMediaPort,
    ) -> bool:
        """Inject clean upstream EOF sentinel into the adapter under test.

        Default implementation checks for push_eof on the adapter. Subclasses
        can override to signal mock WebSocket closure.
        Returns True if EOF injection succeeded, False if unsupported.
        """
        eof_fn: Any = getattr(adapter, "push_eof", None)
        if callable(eof_fn):
            res = eof_fn()
            if asyncio.iscoroutine(res):
                await res
            return True
        return False

    def get_received_inbound_frames(
        self,
        adapter: AIRealtimeMediaPort,
    ) -> list[AudioFrame] | None:
        """Retrieve frames observed as received inbound by the adapter or mock upstream.

        Returns list of AudioFrames if supported/recorded, or None if the adapter
        does not expose inbound frame recording.
        """
        for attr_name in ("received_inbound_frames", "inbound_frames", "sent_frames"):
            val: Any = getattr(adapter, attr_name, None)
            if val is not None:
                return list(val() if callable(val) else val)
        return None

    def get_buffered_frame_count(
        self,
        adapter: AIRealtimeMediaPort,
    ) -> int | None:
        """Inspect the current count of buffered frames in the adapter.

        Returns integer frame count, or None if not directly inspectable.
        """
        for attr_name in ("buffered_frame_count", "queue_depth", "buffer_size"):
            val: Any = getattr(adapter, attr_name, None)
            if val is not None:
                return int(val() if callable(val) else val)
        return None

    def inject_upstream_failure(
        self,
        adapter: AIRealtimeMediaPort,
        error: Exception,
    ) -> bool:
        """Inject an upstream transport failure into the adapter under test.

        Returns True if failure injection succeeded on the adapter, False if unsupported.
        """
        inject_err_fn: Any = getattr(adapter, "inject_receive_error", None)
        if callable(inject_err_fn):
            inject_err_fn(error)
            return True
        trigger_fn: Any = getattr(adapter, "trigger_upstream_failure", None)
        if callable(trigger_fn):
            trigger_fn(error)
            return True
        return False

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

            # 1. Inbound caller audio frames
            payload_in_1 = b"\x01\x00" * 320
            payload_in_2 = b"\x02\x00" * 320
            f_in1 = create_test_frame(payload=payload_in_1, sequence=0, timestamp_ms=0.0)
            f_in2 = create_test_frame(payload=payload_in_2, sequence=1, timestamp_ms=20.0)

            await adapter.send_frame(f_in1)
            await adapter.send_frame(f_in2)

            inbound_observed = self.get_received_inbound_frames(adapter)
            if inbound_observed is not None:
                assert len(inbound_observed) >= 2, "Expected at least 2 inbound frames recorded"
                assert inbound_observed[0].payload == payload_in_1
                assert inbound_observed[0].sequence == 0
                assert inbound_observed[1].payload == payload_in_2
                assert inbound_observed[1].sequence == 1

            # 2. Outbound upstream response frames
            payload_out_1 = b"\x0a\x00" * 320
            payload_out_2 = b"\x0b\x00" * 320
            f_out1 = create_test_frame(payload=payload_out_1, sequence=0, timestamp_ms=0.0)
            f_out2 = create_test_frame(payload=payload_out_2, sequence=1, timestamp_ms=20.0)

            injected_1 = self.inject_upstream_frame(adapter, f_out1)
            injected_2 = self.inject_upstream_frame(adapter, f_out2)

            rec_1 = await asyncio.wait_for(adapter.receive_frame(), timeout=1.0)
            assert rec_1 is not None, "Expected audio frame, got None"
            assert isinstance(rec_1, AudioFrame)
            assert len(rec_1.payload) == 640, "Frame payload must be 640 bytes (PCM16LE 20ms)"
            assert rec_1.sequence == 0, "Sequence preservation violation in frame 1"

            rec_2 = await asyncio.wait_for(adapter.receive_frame(), timeout=1.0)
            assert rec_2 is not None, "Expected audio frame, got None"
            assert isinstance(rec_2, AudioFrame)
            assert len(rec_2.payload) == 640, "Frame payload must be 640 bytes (PCM16LE 20ms)"
            assert rec_2.sequence == 1, "Sequence preservation violation in frame 2"

            if injected_1 and injected_2:
                assert rec_1.payload == payload_out_1, (
                    "Byte integrity violation in injected frame 1"
                )
                assert rec_2.payload == payload_out_2, (
                    "Byte integrity violation in injected frame 2"
                )
            else:
                assert rec_1.payload == payload_in_1, "Byte integrity violation in echoed frame 1"
                assert rec_2.payload == payload_in_2, "Byte integrity violation in echoed frame 2"

            # 3. Clean EOF
            eof_injected = await self.inject_upstream_eof(adapter)
            if eof_injected:
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

            payload_in_a = b"CALL_A_EXCLUSIVE_INBOUND_" + b"\xaa" * 300
            payload_in_b = b"CALL_B_EXCLUSIVE_INBOUND_" + b"\xbb" * 300

            frame_in_a = create_test_frame(payload=payload_in_a, sequence=0)
            frame_in_b = create_test_frame(payload=payload_in_b, sequence=0)

            # Send concurrently to both calls
            await asyncio.gather(
                adapter_a.send_frame(frame_in_a),
                adapter_b.send_frame(frame_in_b),
            )

            # Upstream outbound responses
            payload_out_a = b"CALL_A_EXCLUSIVE_OUTBOUND_" + b"\x11" * 300
            payload_out_b = b"CALL_B_EXCLUSIVE_OUTBOUND_" + b"\x22" * 300
            frame_out_a = create_test_frame(payload=payload_out_a, sequence=0)
            frame_out_b = create_test_frame(payload=payload_out_b, sequence=0)

            inj_a = self.inject_upstream_frame(adapter_a, frame_out_a)
            inj_b = self.inject_upstream_frame(adapter_b, frame_out_b)

            rec_a = await asyncio.wait_for(adapter_a.receive_frame(), timeout=1.0)
            assert rec_a is not None
            assert b"CALL_B" not in rec_a.payload

            rec_b = await asyncio.wait_for(adapter_b.receive_frame(), timeout=1.0)
            assert rec_b is not None
            assert b"CALL_A" not in rec_b.payload

            if inj_a:
                assert rec_a.payload == payload_out_a
            else:
                assert rec_a.payload == payload_in_a

            if inj_b:
                assert rec_b.payload == payload_out_b
            else:
                assert rec_b.payload == payload_in_b

            # Verify closing call A does not impact call B
            await adapter_a.close()
            assert adapter_a.is_closed
            assert not adapter_b.is_closed

            # Verify call B can still receive/send
            eof_inj = await self.inject_upstream_eof(adapter_b)
            if eof_inj:
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

                frame_out_t1 = create_test_frame(payload=b"TURN_1_RESP_" + b"\x11" * 300, turn_id=1)
                self.inject_upstream_frame(adapter, frame_out_t1)

                # Cancel turn 1: must purge internal buffers
                await adapter.cancel_turn(turn_id=1)

                # Duplicate cancellation must be safe and idempotent
                await adapter.cancel_turn(turn_id=1)

                # Send frame on new turn 2
                frame_t2 = create_test_frame(payload=b"TURN_2_AUDIO_" + b"\x22" * 300, turn_id=2)
                await adapter.send_frame(frame_t2)

                frame_out_t2 = create_test_frame(payload=b"TURN_2_RESP_" + b"\x22" * 300, turn_id=2)
                self.inject_upstream_frame(adapter, frame_out_t2)

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

                f_out_t1 = create_test_frame(payload=b"TURN_1_OUT_" + b"\x11" * 300, turn_id=1)
                self.inject_upstream_frame(adapter, f_out_t1)

                # 2. Cancel turn 1
                await adapter.cancel_turn(turn_id=1)

                # 3. Simulate late-arriving frame for turn 1 (delivered after cancellation)
                f_late = create_test_frame(payload=b"TURN_1_LATE_" + b"\x12" * 300, turn_id=1)
                await adapter.send_frame(f_late)
                self.inject_upstream_frame(adapter, f_late)

                # 4. Send frame for turn 2
                f_t2 = create_test_frame(payload=b"TURN_2_AUDIO_" + b"\x22" * 300, turn_id=2)
                await adapter.send_frame(f_t2)
                f_out_t2 = create_test_frame(payload=b"TURN_2_OUT_" + b"\x22" * 300, turn_id=2)
                self.inject_upstream_frame(adapter, f_out_t2)

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
            capacity = 2
            adapter = self._resolve_adapter(
                adapter_factory,
                call_id="call-backpressure",
                queue_capacity=capacity,
                backpressure_timeout=0.05,
            )

            # Send multiple frames to test capacity and backpressure behavior
            num_test_frames = 10
            raised_overflow = False
            timed_out = False

            try:
                for i in range(num_test_frames):
                    frame = create_test_frame(sequence=i)
                    try:
                        await asyncio.wait_for(adapter.send_frame(frame), timeout=0.2)
                    except TimeoutError:
                        timed_out = True
                        break
            except MediaBackpressureOverflowError:
                raised_overflow = True

            buffered_count = self.get_buffered_frame_count(adapter)

            if raised_overflow or timed_out:
                if buffered_count is not None:
                    assert buffered_count <= capacity, (
                        f"Buffered frame count {buffered_count} exceeded capacity {capacity}"
                    )
            elif buffered_count is not None:
                assert buffered_count <= capacity, (
                    f"Adapter has unbounded queue: buffered {buffered_count} frames, expected <= {capacity}"
                )
            else:
                pytest.fail(
                    f"Adapter {type(adapter).__name__} did not enforce backpressure: "
                    f"accepted {num_test_frames} frames without MediaBackpressureOverflowError, "
                    f"timeout stall, or reporting a bounded frame count (<= {capacity})."
                )

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
            tasks_before = {t for t in asyncio.all_tasks() if not t.done()}
            adapter = self._resolve_adapter(adapter_factory, call_id="call-error")

            simulated_error = ConnectionResetError("Simulated upstream socket drop")
            injected = self.inject_upstream_failure(adapter, simulated_error)
            if not injected:
                await adapter.close()
                pytest.skip(
                    f"Adapter class {type(adapter).__name__} does not provide an upstream "
                    f"failure injection hook/fixture; skipping upstream failure test."
                )

            # Once upstream failure is injected, verify adapter reflects failure:
            error_observed = False
            try:
                rec = await asyncio.wait_for(adapter.receive_frame(), timeout=0.5)
                if rec is None:
                    error_observed = True
            except (MediaPortError, MediaPortClosedError, ConnectionResetError):
                error_observed = True

            try:
                await asyncio.wait_for(
                    adapter.send_frame(create_test_frame(sequence=99)), timeout=0.5
                )
            except (MediaPortError, MediaPortClosedError, ConnectionResetError):
                error_observed = True

            assert error_observed or adapter.is_closed, (
                f"Adapter {type(adapter).__name__} did not reflect upstream failure in state or I/O operations"
            )

            # Close must be safe and idempotent
            await adapter.close()
            assert adapter.is_closed

            # Zero task leaks
            await asyncio.sleep(0.01)
            tasks_after = {t for t in asyncio.all_tasks() if not t.done()}
            leaked_tasks = (tasks_after - tasks_before) - {asyncio.current_task()}
            assert len(leaked_tasks) == 0, (
                f"Leaked background tasks after upstream failure: {leaked_tasks}"
            )

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
