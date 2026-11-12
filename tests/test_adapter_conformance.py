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
            return FakeAIMediaAdapter(name=f"fake-ai-{call_id}")

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
