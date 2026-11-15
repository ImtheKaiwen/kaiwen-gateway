"""ASGI-level module lifecycle orchestration tests (S1-A)."""

from __future__ import annotations

import asyncio

import httpx
import pytest
from starlette.testclient import TestClient

from kaiwen_gateway.core import (
    CORE_CONTRACT_VERSION,
    CoreConfig,
    DrainPolicy,
    InstanceDrainCoordinator,
    InstanceDrainState,
    ModuleDescriptor,
    ModuleLifecyclePolicy,
    bind_verified_connection_context,
    compose_gateway_core,
)
from kaiwen_gateway.core.identity import ProviderPrincipal, TenantCallBinding
from kaiwen_gateway.server import ServerLifecycleError, create_gateway_app, gateway_lifespan

SECRET = "sk-live-ASGI-SECRET-999"


class AsgiRecordingModule:
    """Fake GatewayModule for ASGI lifespan verification."""

    def __init__(
        self,
        module_id: str,
        events: list[str],
        *,
        fail_startup: Exception | None = None,
        fail_shutdown: Exception | None = None,
        startup_gate: asyncio.Event | None = None,
        hang_startup: bool = False,
        hang_shutdown: bool = False,
    ) -> None:
        self.descriptor = ModuleDescriptor(
            module_id=module_id, contract_version=CORE_CONTRACT_VERSION
        )
        self._events = events
        self._fail_startup = fail_startup
        self._fail_shutdown = fail_shutdown
        self._startup_gate = startup_gate
        self._hang_startup = hang_startup
        self._hang_shutdown = hang_shutdown
        self.startup_calls = 0
        self.shutdown_calls = 0

    async def startup(self) -> None:
        self.startup_calls += 1
        if self._startup_gate is not None:
            await self._startup_gate.wait()
        if self._hang_startup:
            await asyncio.sleep(3600)
        if self._fail_startup is not None:
            raise self._fail_startup
        self._events.append(f"start:{self.descriptor.module_id}")

    async def shutdown(self) -> None:
        self.shutdown_calls += 1
        if self._hang_shutdown:
            await asyncio.sleep(3600)
        if self._fail_shutdown is not None:
            raise self._fail_shutdown
        self._events.append(f"stop:{self.descriptor.module_id}")


def test_asgi_lifespan_starts_and_stops_modules_in_order() -> None:
    events: list[str] = []
    alpha = AsgiRecordingModule("alpha", events)
    beta = AsgiRecordingModule("beta", events)
    core = compose_gateway_core(modules=[alpha, beta])
    app = create_gateway_app(core=core)

    with TestClient(app):
        assert events == ["start:alpha", "start:beta"]
        assert app.state.started is True

    assert events == ["start:alpha", "start:beta", "stop:beta", "stop:alpha"]
    assert app.state.stopped is True


def test_asgi_readiness_unavailable_until_all_modules_start() -> None:
    async def _run() -> None:
        events: list[str] = []
        gate = asyncio.Event()
        alpha = AsgiRecordingModule("alpha", events)
        gated = AsgiRecordingModule("gated", events, startup_gate=gate)
        core = compose_gateway_core(modules=[alpha, gated])
        app = create_gateway_app(core=core)

        async def _enter_lifespan() -> None:
            async with gateway_lifespan(app):
                await asyncio.sleep(0.05)

        lifespan_task = asyncio.create_task(_enter_lifespan())
        await asyncio.sleep(0.02)

        # Alpha started, gated is still waiting on gate: app must NOT be started/ready
        assert events == ["start:alpha"]
        assert app.state.started is False

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            resp = await client.get("/ready")
            assert resp.status_code == 503
            assert resp.json()["reason"] == "not_started"

        gate.set()
        await lifespan_task
        assert events == ["start:alpha", "start:gated", "stop:gated", "stop:alpha"]

    asyncio.run(_run())


def test_asgi_partial_startup_failure_rolls_back_and_fails_closed() -> None:
    async def _run() -> None:
        events: list[str] = []
        alpha = AsgiRecordingModule("alpha", events)
        bad = AsgiRecordingModule("bad", events, fail_startup=RuntimeError(f"leak={SECRET}"))
        never = AsgiRecordingModule("never", events)
        core = compose_gateway_core(modules=[alpha, bad, never])
        app = create_gateway_app(core=core)

        with pytest.raises(ServerLifecycleError, match="Gateway module startup failed") as exc_info:
            async with gateway_lifespan(app):
                pass  # pragma: no cover

        assert SECRET not in str(exc_info.value)
        assert SECRET not in str(exc_info.value.__cause__)
        assert events == ["start:alpha", "stop:alpha"]
        assert never.startup_calls == 0

        # Instance must remain permanently non-serving (fail closed)
        assert app.state.started is False
        assert app.state.startup_failed is True
        assert app.state.stopped is True
        assert app.state.drain_coordinator.state == InstanceDrainState.STOPPED

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            resp = await client.get("/ready")
            assert resp.status_code == 503
            assert resp.json()["reason"] == "startup_failed"

        # Restarting a failed instance must be rejected
        with pytest.raises(ServerLifecycleError):
            async with gateway_lifespan(app):
                pass  # pragma: no cover

    asyncio.run(_run())


def test_asgi_shutdown_drains_active_connections_before_stopping_modules() -> None:
    async def _run() -> None:
        events: list[str] = []
        mod = AsgiRecordingModule("voice_mod", events)
        core = compose_gateway_core(modules=[mod])
        app = create_gateway_app(
            core=core,
            drain_policy=DrainPolicy(grace_period=0.5, force_close_timeout=0.2),
        )
        coord: InstanceDrainCoordinator = app.state.drain_coordinator
        ctx = bind_verified_connection_context(
            principal=ProviderPrincipal("synthetic_provider"),
            binding=TenantCallBinding(
                authorized_provider_id="synthetic_provider",
                tenant_id="tenant_a",
                call_id="call_drain_order",
                allowed_scopes=["voice.stream"],
            ),
            connection_id="conn_drain_order",
        )

        async with gateway_lifespan(app):
            supervisor = await coord.admit_connection(ctx)

            async def worker() -> None:
                await asyncio.sleep(0.05)
                events.append("connection:finished")

            await supervisor.start(worker)

        assert events == ["start:voice_mod", "connection:finished", "stop:voice_mod"]
        assert app.state.stopped is True

    asyncio.run(_run())


def test_asgi_shutdown_continues_cleanup_when_module_fails_and_raises_safely() -> None:
    async def _run() -> None:
        events: list[str] = []
        alpha = AsgiRecordingModule("alpha", events)
        bad = AsgiRecordingModule("bad", events, fail_shutdown=RuntimeError(f"secret={SECRET}"))
        gamma = AsgiRecordingModule("gamma", events)
        core = compose_gateway_core(modules=[alpha, bad, gamma])
        app = create_gateway_app(core=core)

        with pytest.raises(
            ServerLifecycleError, match="Gateway module shutdown failed"
        ) as exc_info:
            async with gateway_lifespan(app):
                pass

        assert SECRET not in str(exc_info.value)
        assert SECRET not in str(exc_info.value.__cause__)
        assert events == ["start:alpha", "start:bad", "start:gamma", "stop:gamma", "stop:alpha"]
        assert app.state.shutdown_failed is True
        assert app.state.stopped is False

    asyncio.run(_run())


def test_asgi_module_lifecycle_respects_bounded_timeouts() -> None:
    async def _run() -> None:
        events: list[str] = []
        alpha = AsgiRecordingModule("alpha", events)
        hang = AsgiRecordingModule("hang", events, hang_startup=True)
        core = compose_gateway_core(modules=[alpha, hang])
        app = create_gateway_app(
            core=core,
            module_lifecycle_policy=ModuleLifecyclePolicy(
                startup_timeout=0.05, shutdown_timeout=0.05
            ),
        )

        with pytest.raises(ServerLifecycleError, match="Gateway module startup failed"):
            async with gateway_lifespan(app):
                pass  # pragma: no cover

        assert events == ["start:alpha", "stop:alpha"]
        assert app.state.startup_failed is True

    asyncio.run(_run())


def test_asgi_multiple_instances_remain_independent() -> None:
    events_a: list[str] = []
    events_b: list[str] = []
    mod_a = AsgiRecordingModule("mod_a", events_a)
    mod_b = AsgiRecordingModule("mod_b", events_b, fail_startup=RuntimeError("boom"))

    app_a = create_gateway_app(
        core=compose_gateway_core(CoreConfig(instance_name="inst_a"), modules=[mod_a])
    )
    app_b = create_gateway_app(
        core=compose_gateway_core(CoreConfig(instance_name="inst_b"), modules=[mod_b])
    )

    with TestClient(app_a):
        assert events_a == ["start:mod_a"]
        with pytest.raises(ServerLifecycleError):
            with TestClient(app_b):
                pass  # pragma: no cover
        assert app_a.state.started is True
        assert app_b.state.startup_failed is True

    assert events_a == ["start:mod_a", "stop:mod_a"]
    assert events_b == []


def test_asgi_shutdown_cancelled_during_drain_waits_for_active_connection_before_module_stop() -> (
    None
):
    async def _run() -> None:
        events: list[str] = []
        mod = AsgiRecordingModule("voice_mod", events)
        core = compose_gateway_core(modules=[mod])
        app = create_gateway_app(
            core=core,
            drain_policy=DrainPolicy(grace_period=0.5, force_close_timeout=0.2),
        )
        coord: InstanceDrainCoordinator = app.state.drain_coordinator
        ctx = bind_verified_connection_context(
            principal=ProviderPrincipal("synthetic_provider"),
            binding=TenantCallBinding(
                authorized_provider_id="synthetic_provider",
                tenant_id="tenant_a",
                call_id="call_cancel_drain",
                allowed_scopes=["voice.stream"],
            ),
            connection_id="conn_cancel_drain",
        )
        ready_to_exit = asyncio.Event()
        draining_started = asyncio.Event()

        async def _run_lifespan() -> None:
            async with gateway_lifespan(app):
                supervisor = await coord.admit_connection(ctx)

                async def worker() -> None:
                    await asyncio.sleep(0.08)
                    events.append("connection:finished")

                await supervisor.start(worker)
                ready_to_exit.set()
                await asyncio.sleep(0.01)
            # Triggered on exit: drain() will run

        task = asyncio.create_task(_run_lifespan())
        await ready_to_exit.wait()
        while coord.state == InstanceDrainState.ACCEPTING:
            await asyncio.sleep(0.005)
        draining_started.set()
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        assert coord.state == InstanceDrainState.STOPPED
        assert events == ["start:voice_mod", "connection:finished", "stop:voice_mod"]
        assert mod.shutdown_calls == 1

    asyncio.run(_run())
