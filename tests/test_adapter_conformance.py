"""Tests for the reusable AI realtime adapter conformance test suite and context factory.

Verifies that:
1. Built-in synthetic adapters pass the full 8-dimension RealtimeAdapterConformanceSuite:
   - BoundedInterruptibleFakeAIEchoPort
   - BoundedFakeAIEchoPort
   - FakeAIMediaAdapter
2. Context-aware ai_media_port_factory resolution in media_endpoint passes call_id, tenant_id,
   and context while maintaining 100% backwards compatibility with 0-argument factories.
3. SyntheticUpstreamFailingAIAdapter correctly simulates upstream transport failure under
   VoiceBridgeSession without hanging or leaking asyncio tasks.
4. AIRealtimePort protocol alias is exported and identical to AIRealtimeMediaPort.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from kaiwen_gateway.modules.voice.adapters.fake import FakeAIMediaAdapter, FakeTelephonyMediaAdapter
from kaiwen_gateway.modules.voice.adapters.fake_ai import (
    BoundedFakeAIEchoPort,
    BoundedInterruptibleFakeAIEchoPort,
)
from kaiwen_gateway.modules.voice.bridge import VoiceBridgeSession, VoiceBridgeState
from kaiwen_gateway.modules.voice.domain import (
    AudioFrame,
    MediaBackpressureOverflowError,
    MediaPortClosedError,
    VoiceDomainError,
)
from kaiwen_gateway.modules.voice.ports import (
    AIRealtimeMediaPort,
    AIRealtimePort,
)
from kaiwen_gateway.testing.conformance import (
    RealtimeAdapterConformanceSuite,
    SyntheticUpstreamFailingAIAdapter,
    create_test_frame,
)
from kaiwen_gateway.transports.synthetic.media_endpoint import _instantiate_ai_port


class TestBoundedInterruptibleFakeAIEchoPortConformance(RealtimeAdapterConformanceSuite):
    """Run conformance suite against BoundedInterruptibleFakeAIEchoPort."""

    @pytest.fixture
    def adapter_factory(self) -> Any:
        def _factory(
            *,
            call_id: str,
            tenant_id: str,
            queue_capacity: int = 8,
            backpressure_timeout: float = 0.05,
            **kwargs: Any,
        ) -> AIRealtimeMediaPort:
            return BoundedInterruptibleFakeAIEchoPort(
                queue_capacity=queue_capacity,
                backpressure_timeout=backpressure_timeout,
            )

        return _factory


class TestBoundedFakeAIEchoPortConformance(RealtimeAdapterConformanceSuite):
    """Run conformance suite against BoundedFakeAIEchoPort."""

    @pytest.fixture
    def adapter_factory(self) -> Any:
        def _factory(
            *,
            call_id: str,
            tenant_id: str,
            queue_capacity: int = 8,
            backpressure_timeout: float = 0.05,
            **kwargs: Any,
        ) -> AIRealtimeMediaPort:
            return BoundedFakeAIEchoPort(
                queue_capacity=queue_capacity,
                backpressure_timeout=backpressure_timeout,
            )

        return _factory


class TestFakeAIMediaAdapterConformance(RealtimeAdapterConformanceSuite):
    """Run conformance suite against FakeAIMediaAdapter."""

    @pytest.fixture
    def adapter_factory(self) -> Any:
        def _factory(
            *,
            call_id: str,
            tenant_id: str,
            queue_capacity: int = 8,
            **kwargs: Any,
        ) -> AIRealtimeMediaPort:
            return FakeAIMediaAdapter(
                name=f"fake-ai-{call_id}",
                queue_capacity=queue_capacity,
            )

        return _factory


# -----------------------------------------------------------------------------
# Additional Unit Tests for Context-Aware Resolution & Edge Cases
# -----------------------------------------------------------------------------


def test_ai_realtime_port_alias_identity() -> None:
    """Verify AIRealtimePort is exported and identical to AIRealtimeMediaPort."""
    assert AIRealtimePort is AIRealtimeMediaPort


def test_instantiate_ai_port_backwards_compatible_zero_arg() -> None:
    """Verify 0-argument factories work without error for backwards compatibility."""

    def zero_arg_factory() -> AIRealtimeMediaPort:
        return BoundedFakeAIEchoPort()

    port = _instantiate_ai_port(zero_arg_factory, call_id="call-123", tenant_id="tenant-456")
    assert isinstance(port, AIRealtimeMediaPort)
    assert not port.is_closed


def test_instantiate_ai_port_context_aware_named_args() -> None:
    """Verify context-aware factories receive call_id and tenant_id."""
    received_context: dict[str, str] = {}

    def context_factory(*, call_id: str, tenant_id: str) -> AIRealtimeMediaPort:
        received_context["call_id"] = call_id
        received_context["tenant_id"] = tenant_id
        return BoundedFakeAIEchoPort()

    port = _instantiate_ai_port(context_factory, call_id="call-xyz", tenant_id="tenant-abc")
    assert isinstance(port, AIRealtimeMediaPort)
    assert received_context == {"call_id": "call-xyz", "tenant_id": "tenant-abc"}


def test_instantiate_ai_port_kwargs_factory() -> None:
    """Verify factories accepting **kwargs receive call_id and tenant_id."""
    received_kwargs: dict[str, Any] = {}

    def kwargs_factory(**kwargs: Any) -> AIRealtimeMediaPort:
        received_kwargs.update(kwargs)
        return BoundedFakeAIEchoPort()

    port = _instantiate_ai_port(kwargs_factory, call_id="c-999", tenant_id="t-999")
    assert isinstance(port, AIRealtimeMediaPort)
    assert received_kwargs.get("call_id") == "c-999"
    assert received_kwargs.get("tenant_id") == "t-999"


def test_instantiate_ai_port_positional_context() -> None:
    """Verify factories accepting a single positional context argument receive it."""
    received: list[Any] = []

    class DummyContext:
        call_id = "call-pos"
        tenant_id = "tenant-pos"

    ctx = DummyContext()

    def positional_context_factory(context: Any) -> AIRealtimeMediaPort:
        received.append(context)
        return BoundedFakeAIEchoPort()

    port = _instantiate_ai_port(
        positional_context_factory,
        call_id="call-pos",
        tenant_id="tenant-pos",
        context=ctx,
    )
    assert isinstance(port, AIRealtimeMediaPort)
    assert len(received) == 1
    assert received[0] is ctx


def test_upstream_failing_adapter_under_voice_bridge() -> None:
    """Verify SyntheticUpstreamFailingAIAdapter triggers clean bridge termination."""

    async def _test() -> None:
        ai_adapter = SyntheticUpstreamFailingAIAdapter(call_id="call-test", tenant_id="tenant-test")
        tel_adapter = FakeTelephonyMediaAdapter(name="tel-test")

        session = VoiceBridgeSession(
            call_id="call-test",
            tenant_id="tenant-test",
            telephony_port=tel_adapter,
            ai_port=ai_adapter,
        )

        await session.start()

        # Stream one frame
        tel_adapter.push_frame(create_test_frame(sequence=0))

        # Inject upstream network drop
        ai_adapter.trigger_upstream_failure(ConnectionResetError("Simulated socket drop"))

        # Wait for bridge to detect failure and close cleanly
        try:
            await asyncio.wait_for(session.wait_until_closed(propagate_error=False), timeout=2.0)
        except TimeoutError:
            pass

        assert session.state in (VoiceBridgeState.CLOSED, VoiceBridgeState.FAILED)

        # Cleanup
        await session.close()
        assert session.state in (VoiceBridgeState.CLOSED, VoiceBridgeState.FAILED)

    asyncio.run(_test())


class TestSyntheticUpstreamFailingAIAdapterConformance(RealtimeAdapterConformanceSuite):
    """Run full 8-dimension conformance suite against SyntheticUpstreamFailingAIAdapter."""

    @pytest.fixture
    def adapter_factory(self) -> Any:
        def _factory(
            *,
            call_id: str,
            tenant_id: str = "tenant-test",
            queue_capacity: int = 8,
            **kwargs: Any,
        ) -> AIRealtimeMediaPort:
            return SyntheticUpstreamFailingAIAdapter(
                call_id=call_id,
                tenant_id=tenant_id,
                queue_capacity=queue_capacity,
            )

        return _factory


def test_adversarial_factory_raises_type_error_invoked_exactly_once() -> None:
    """Verify factory raising internal TypeError is invoked exactly once and never retried (Finding A)."""
    call_count = 0
    received_identity: dict[str, str] = {}

    def failing_factory(*, call_id: str, tenant_id: str) -> AIRealtimeMediaPort:
        nonlocal call_count
        call_count += 1
        received_identity["call_id"] = call_id
        received_identity["tenant_id"] = tenant_id
        raise TypeError("Simulated internal TypeError inside factory initialization")

    with pytest.raises(TypeError, match="Simulated internal TypeError"):
        _instantiate_ai_port(
            failing_factory,
            call_id="call-secure-1",
            tenant_id="tenant-secure-1",
        )

    assert call_count == 1, f"Expected exactly 1 invocation, got {call_count}"
    assert received_identity == {"call_id": "call-secure-1", "tenant_id": "tenant-secure-1"}


def test_adversarial_factory_optional_identity_receives_verified_context() -> None:
    """Verify factory with default identity arguments receives verified context, not defaults (Finding A)."""
    call_count = 0
    received_identity: dict[str, str] = {}

    def optional_identity_factory(
        call_id: str = "default_call",
        tenant_id: str = "default_tenant",
    ) -> AIRealtimeMediaPort:
        nonlocal call_count
        call_count += 1
        received_identity["call_id"] = call_id
        received_identity["tenant_id"] = tenant_id
        return BoundedFakeAIEchoPort()

    port = _instantiate_ai_port(
        optional_identity_factory,
        call_id="call-verified-99",
        tenant_id="tenant-verified-99",
    )
    assert isinstance(port, AIRealtimeMediaPort)
    assert call_count == 1
    assert received_identity == {
        "call_id": "call-verified-99",
        "tenant_id": "tenant-verified-99",
    }


def test_adversarial_factory_unsupported_required_signature_fails_closed() -> None:
    """Verify factory with unsupported required signature fails closed with VoiceDomainError (Finding A)."""
    call_count = 0

    def unsupported_factory(unsupported_req_arg: int) -> AIRealtimeMediaPort:
        nonlocal call_count
        call_count += 1
        return BoundedFakeAIEchoPort()

    with pytest.raises(VoiceDomainError, match="Unsupported required parameter"):
        _instantiate_ai_port(
            unsupported_factory,
            call_id="call-x",
            tenant_id="tenant-x",
        )

    assert call_count == 0, "Factory with unsupported signature must not be invoked"


def test_adversarial_unbounded_adapter_fails_backpressure_conformance() -> None:
    """Verify an intentionally defective unbounded adapter fails Dimension 6 (Finding C)."""

    class DefectiveUnboundedAIAdapter(AIRealtimeMediaPort):
        def __init__(self, **kwargs: Any) -> None:
            self._frames: list[AudioFrame] = []
            self._closed = False

        @property
        def is_closed(self) -> bool:
            return self._closed

        @property
        def buffered_frame_count(self) -> int:
            return len(self._frames)

        async def send_frame(self, frame: AudioFrame) -> None:
            # Defect: appends infinitely without enforcing queue capacity or timeout
            self._frames.append(frame)

        async def receive_frame(self) -> AudioFrame | None:
            return None

        async def close(self) -> None:
            self._closed = True

    class DefectiveSuite(RealtimeAdapterConformanceSuite):
        def create_adapter(self, **kwargs: Any) -> AIRealtimeMediaPort:
            return DefectiveUnboundedAIAdapter(**kwargs)

    suite = DefectiveSuite()
    with pytest.raises(AssertionError, match="Adapter has unbounded queue"):
        suite.test_bounded_queue_and_backpressure_enforcement(suite.create_adapter)


def test_adversarial_adapter_ignoring_failure_fails_error_conformance() -> None:
    """Verify an intentionally defective adapter ignoring upstream failure fails Dimension 7 (Finding D)."""

    class DefectiveImmuneFailureAIAdapter(AIRealtimeMediaPort):
        def __init__(self, **kwargs: Any) -> None:
            self._closed = False

        @property
        def is_closed(self) -> bool:
            return self._closed

        def trigger_upstream_failure(self, error: Exception) -> None:
            # Defect: accepts hook but ignores failure completely
            pass

        async def send_frame(self, frame: AudioFrame) -> None:
            pass

        async def receive_frame(self) -> AudioFrame | None:
            return create_test_frame()

        async def close(self) -> None:
            self._closed = True

    class DefectiveSuite(RealtimeAdapterConformanceSuite):
        def create_adapter(self, **kwargs: Any) -> AIRealtimeMediaPort:
            return DefectiveImmuneFailureAIAdapter(**kwargs)

    suite = DefectiveSuite()
    with pytest.raises(AssertionError, match="did not reflect upstream failure"):
        suite.test_upstream_error_propagation_and_state_safety(suite.create_adapter)


def test_non_echo_independent_upstream_audio_adapter_passes_conformance() -> None:
    """Verify an adapter generating independent upstream response audio passes conformance (Finding B)."""

    class NonEchoRealtimeAIAdapter(AIRealtimeMediaPort):
        """Adapter that simulates real AI behavior: generates independent speech, never echoes caller."""

        def __init__(self, *, queue_capacity: int = 8, **kwargs: Any) -> None:
            self.inbound_frames_received: list[AudioFrame] = []
            self._outbound_queue: asyncio.Queue[object] = asyncio.Queue(maxsize=queue_capacity)
            self._closed = False
            self.queue_capacity = queue_capacity

        @property
        def is_closed(self) -> bool:
            return self._closed

        @property
        def buffered_frame_count(self) -> int:
            return self._outbound_queue.qsize()

        def push_frame(self, frame: AudioFrame) -> None:
            self._outbound_queue.put_nowait(frame)

        def push_eof(self) -> None:
            self._outbound_queue.put_nowait(None)

        async def send_frame(self, frame: AudioFrame) -> None:
            if self._closed:
                raise MediaPortClosedError("Port is closed")
            self.inbound_frames_received.append(frame)

        async def receive_frame(self) -> AudioFrame | None:
            if self._closed:
                raise MediaPortClosedError("Port is closed")
            item = await self._outbound_queue.get()
            if item is None:
                return None
            return item  # type: ignore[return-value]

        async def close(self) -> None:
            self._closed = True

    class NonEchoSuite(RealtimeAdapterConformanceSuite):
        def create_adapter(self, **kwargs: Any) -> AIRealtimeMediaPort:
            return NonEchoRealtimeAIAdapter(**kwargs)

    suite = NonEchoSuite()
    # Must pass Dimension 1 (Routing & delivery with independent audio)
    suite.test_bi_directional_audio_routing_and_delivery(suite.create_adapter)
    # Must pass Dimension 2 (Multi-call isolation with independent audio)
    suite.test_two_concurrent_calls_isolation(suite.create_adapter)


def test_fake_ai_media_adapter_close_when_full_queue_is_safe() -> None:
    """Verify closing a FakeAIMediaAdapter with a full queue succeeds without QueueFull."""

    async def _test() -> None:
        adapter = FakeAIMediaAdapter(queue_capacity=1)
        adapter.push_frame(create_test_frame())
        assert adapter.buffered_frame_count == 1

        # Closing must not raise QueueFull
        await adapter.close()
        assert adapter.is_closed

    asyncio.run(_test())


def test_synthetic_upstream_failing_adapter_drains_pending_inbound_on_failure_and_close() -> None:
    """Verify pending inbound frames are drained when upstream failure is triggered."""

    async def _test() -> None:
        adapter = SyntheticUpstreamFailingAIAdapter(queue_capacity=8)

        await adapter.send_frame(create_test_frame(sequence=0))
        await adapter.send_frame(create_test_frame(sequence=1))
        assert adapter.buffered_frame_count == 2

        # Trigger failure: must drain pending queues
        adapter.trigger_upstream_failure(ConnectionResetError("Drop"))
        assert adapter.is_closed
        assert adapter.buffered_frame_count == 0

        # Close must remain idempotent and queues stay empty
        await adapter.close()
        assert adapter.is_closed
        assert adapter.buffered_frame_count == 0

    asyncio.run(_test())


def test_synthetic_upstream_failing_adapter_pending_backpressure_vs_lifetime_history() -> None:
    """Verify backpressure applies to pending unconsumed frames, not total call lifetime frames."""

    async def _test() -> None:
        adapter = SyntheticUpstreamFailingAIAdapter(queue_capacity=4)

        # Stream 20 frames across iterations where pending frames are drained
        for batch in range(5):
            for i in range(4):
                seq = batch * 4 + i
                await adapter.send_frame(create_test_frame(sequence=seq))
            assert adapter.buffered_frame_count == 4
            # Drain pending frames to simulate successful upstream delivery
            drained = adapter.drain_pending_inbound()
            assert len(drained) == 4
            assert adapter.buffered_frame_count == 0

        # Lifetime history contains all 20 frames
        assert len(adapter.received_inbound_frames) == 20

        # Now fill up to capacity 4 and verify the 5th raises overflow
        for i in range(4):
            await adapter.send_frame(create_test_frame(sequence=100 + i))

        with pytest.raises(MediaBackpressureOverflowError):
            await adapter.send_frame(create_test_frame(sequence=105))

        await adapter.close()
        assert adapter.is_closed

    asyncio.run(_test())
