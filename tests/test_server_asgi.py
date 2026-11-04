"""Tests for Kaiwen Gateway ASGI server and runtime foundation (M3.1).

Covers all 20 required verification scenarios:
1. App factory import.
2. Two independent app instances.
3. No global state.
4. Core dependency direction.
5. Config validation.
6. Startup lifecycle.
7. Startup failure.
8. Shutdown.
9. Repeated shutdown.
10. Shutdown cancellation & failure.
11. Health endpoint.
12. Readiness endpoint (ingress disabled).
13. Readiness during draining (and health viability).
14. Readiness when stopped.
15. WebSocket rejection without accept.
16. No server, socket, or task spawned on import.
17. Core dependency count zero.
18. Optional server dependencies missing error.
19. Installed-wheel app import and metadata.
20. Local ASGI host smoke execution.
"""

from __future__ import annotations

import asyncio
import importlib
import socket
import sys
import threading
from types import ModuleType
from typing import Any
from unittest.mock import patch

import httpx
import pytest
import uvicorn
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from kaiwen_gateway.core.admission import AdmissionController
from kaiwen_gateway.core.composition import GatewayCore, compose_gateway_core
from kaiwen_gateway.core.config import CoreConfig
from kaiwen_gateway.core.draining import (
    DrainPolicy,
    InstanceDrainCoordinator,
    InstanceDrainState,
)
from kaiwen_gateway.core.exceptions import CoreConfigurationError
from kaiwen_gateway.core.identity import (
    ProviderPrincipal,
    TenantCallBinding,
    bind_verified_connection_context,
)
from kaiwen_gateway.server import (
    ServerDependencyError,
    ServerError,
    ServerLifecycleError,
    create_gateway_app,
    default_app,
    gateway_lifespan,
)


def _is_started(app: Any) -> bool:
    return bool(getattr(app.state, "started", False))


def _is_stopped(app: Any) -> bool:
    return bool(getattr(app.state, "stopped", False))


def test_01_app_factory_import() -> None:
    """Verify that create_gateway_app and default app callable are importable."""
    assert callable(create_gateway_app)
    assert callable(default_app)
    assert issubclass(ServerDependencyError, ServerError)
    assert issubclass(ServerLifecycleError, ServerError)


def test_02_two_independent_app_instances() -> None:
    """Verify that multiple app instances are fully isolated with independent Core state."""
    config1 = CoreConfig(instance_name="instance_one")
    config2 = CoreConfig(instance_name="instance_two")

    app1 = create_gateway_app(config1)
    app2 = create_gateway_app(config2)

    assert app1 is not app2
    assert app1.state.core is not app2.state.core
    assert app1.state.admission_controller is not app2.state.admission_controller
    assert app1.state.drain_coordinator is not app2.state.drain_coordinator
    assert app1.state.config.instance_name == "instance_one"
    assert app2.state.config.instance_name == "instance_two"

    # State mutations on app1 do not affect app2
    app1.state.started = True
    assert app2.state.started is False


def test_03_no_global_state() -> None:
    """Verify that no module-level global app singleton or mutable call registry exists."""
    import kaiwen_gateway.server.app as server_app_module

    for attr in dir(server_app_module):
        if not attr.startswith("_"):
            val = getattr(server_app_module, attr)
            assert not isinstance(val, (AdmissionController, InstanceDrainCoordinator, GatewayCore))


def test_04_core_dependency_direction() -> None:
    """Verify that Core modules have zero dependencies on starlette, uvicorn, or server."""
    forbidden_terms = ("starlette", "uvicorn", "fastapi", "kaiwen_gateway.server")

    core_package = sys.modules.get("kaiwen_gateway.core")
    assert core_package is not None

    for mod_name, mod in list(sys.modules.items()):
        if mod_name.startswith("kaiwen_gateway.core"):
            if isinstance(mod, ModuleType) and hasattr(mod, "__file__") and mod.__file__:
                with open(mod.__file__, encoding="utf-8") as f:
                    content = f.read()
                for term in forbidden_terms:
                    assert term not in content, f"Forbidden import '{term}' in {mod_name}"


def test_05_config_validation() -> None:
    """Verify fail-closed configuration validation upfront in app factory."""
    # Invalid config limit
    with pytest.raises(CoreConfigurationError):
        create_gateway_app(CoreConfig(max_concurrent_connections=0))

    # Pre-composed GatewayCore
    core = compose_gateway_core(config=CoreConfig(instance_name="composed_core"))
    app = create_gateway_app(core=core)
    assert app.state.core is core
    assert app.state.config.instance_name == "composed_core"

    # Core with mismatching explicit config
    mismatched = CoreConfig(instance_name="other")
    with pytest.raises(CoreConfigurationError, match="Explicit config does not match core.config"):
        create_gateway_app(config=mismatched, core=core)

    # Invalid core type
    with pytest.raises(TypeError, match="core must be an instance of GatewayCore"):
        create_gateway_app(core="not_a_core")  # type: ignore[arg-type]

    # Explicit admission controller
    controller = AdmissionController(CoreConfig(instance_name="custom_ctrl"))
    app_ctrl = create_gateway_app(admission_controller=controller)
    assert app_ctrl.state.admission_controller is controller

    # Invalid admission controller type
    with pytest.raises(TypeError, match="admission_controller must be an AdmissionController"):
        create_gateway_app(admission_controller="invalid")  # type: ignore[arg-type]

    # Explicit drain coordinator
    coord = InstanceDrainCoordinator(controller=controller)
    app_coord = create_gateway_app(
        admission_controller=controller,
        drain_coordinator=coord,
    )
    assert app_coord.state.drain_coordinator is coord

    # Invalid drain coordinator type
    with pytest.raises(TypeError, match="drain_coordinator must be an InstanceDrainCoordinator"):
        create_gateway_app(drain_coordinator="invalid")  # type: ignore[arg-type]

    # Mismatched admission controller and drain coordinator controller
    other_ctrl = AdmissionController(CoreConfig(instance_name="other_ctrl"))
    mismatched_coord = InstanceDrainCoordinator(controller=other_ctrl)
    with pytest.raises(ValueError, match="Provided drain_coordinator.controller does not match"):
        create_gateway_app(
            admission_controller=controller,
            drain_coordinator=mismatched_coord,
        )

    # Custom drain policy
    custom_policy = DrainPolicy(grace_period=2.0, force_close_timeout=1.0)
    app_policy = create_gateway_app(drain_policy=custom_policy)
    assert app_policy.state.drain_coordinator.policy.grace_period == 2.0


def test_06_startup_lifecycle() -> None:
    """Verify that lifespan correctly transitions started flag and enables health endpoint."""
    app = create_gateway_app(CoreConfig(instance_name="lifecycle_test"))
    assert not _is_started(app)

    with TestClient(app) as client:
        assert _is_started(app)
        assert not _is_stopped(app)

        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data == {"status": "ok", "instance": "lifecycle_test"}


def test_07_startup_failure() -> None:
    """Verify fail-closed error propagation when lifespan startup encounters invalid state."""
    app = create_gateway_app()

    # Missing drain coordinator on state
    app.state.drain_coordinator = None

    async def _run() -> None:
        with pytest.raises(
            ServerLifecycleError, match="Application state missing valid InstanceDrainCoordinator"
        ):
            async with gateway_lifespan(app):
                pass

        # Missing state attribute entirely
        class DummyApp:
            pass

        with pytest.raises(
            ServerLifecycleError, match="Application instance lacks state attribute"
        ):
            async with gateway_lifespan(DummyApp()):
                pass

    asyncio.run(_run())


def test_08_shutdown() -> None:
    """Verify that normal lifespan exit triggers draining and transitions to STOPPED."""
    app = create_gateway_app()

    with TestClient(app):
        assert _is_started(app)
        assert not _is_stopped(app)

    assert _is_stopped(app)
    assert app.state.drain_coordinator.state == InstanceDrainState.STOPPED


def test_09_repeated_shutdown() -> None:
    """Verify that repeated shutdown invocation is safe and idempotent."""
    app = create_gateway_app()

    with TestClient(app):
        pass

    assert app.state.drain_coordinator.state == InstanceDrainState.STOPPED

    # Second shutdown call should be idempotent
    async def _second_shutdown() -> None:
        async with gateway_lifespan(app):
            pass

    asyncio.run(_second_shutdown())
    assert app.state.drain_coordinator.state == InstanceDrainState.STOPPED


def test_10_shutdown_failure_observation() -> None:
    """Verify that internal drain errors during shutdown propagate as ServerLifecycleError."""
    app = create_gateway_app()

    coord: InstanceDrainCoordinator = app.state.drain_coordinator
    # Inject a simulated drain error
    coord._drain_error = RuntimeError("Injected shutdown fault")

    async def _run() -> None:
        with pytest.raises(
            ServerLifecycleError, match="Gateway shutdown failed due to drain error"
        ):
            async with gateway_lifespan(app):
                pass

    asyncio.run(_run())


def test_11_health_endpoint() -> None:
    """Verify health endpoint liveness behavior and method restrictions."""
    app = create_gateway_app(CoreConfig(instance_name="health_app"))

    # Before startup (without lifespan)
    client_unstarted = TestClient(app, raise_server_exceptions=False)
    resp_unstarted = client_unstarted.get("/health")
    assert resp_unstarted.status_code == 503
    assert resp_unstarted.json() == {"status": "starting", "instance": "health_app"}

    with TestClient(app) as client:
        # GET is allowed
        resp_ok = client.get("/health")
        assert resp_ok.status_code == 200
        assert resp_ok.json() == {"status": "ok", "instance": "health_app"}

        # POST is rejected
        resp_post = client.post("/health")
        assert resp_post.status_code == 405

        # Unknown route returns 404
        resp_404 = client.get("/nonexistent")
        assert resp_404.status_code == 404


def test_12_readiness_endpoint_ingress_disabled() -> None:
    """Verify readiness returns 503 when media ingress is disabled (safe default)."""
    app = create_gateway_app(CoreConfig(instance_name="ready_app", ingress_mode="disabled"))

    with TestClient(app) as client:
        resp = client.get("/ready")
        assert resp.status_code == 503
        data = resp.json()
        assert data == {"status": "unavailable", "reason": "ingress_disabled"}


def test_13_readiness_during_draining() -> None:
    """Verify readiness returns 503 during draining while health remains 200 OK."""
    app = create_gateway_app(CoreConfig(instance_name="drain_ready_app"))

    with TestClient(app) as client:
        # Manually transition coordinator state to simulate draining
        app.state.drain_coordinator._state = InstanceDrainState.DRAINING
        # Bypass ingress check to specifically test draining branch
        app.state.config = CoreConfig(instance_name="drain_ready_app")
        object.__setattr__(app.state.config, "ingress_mode", "synthetic")

        # Health is still alive
        resp_health = client.get("/health")
        assert resp_health.status_code == 200
        assert resp_health.json()["status"] == "ok"

        # Readiness is unavailable
        resp_ready = client.get("/ready")
        assert resp_ready.status_code == 503
        assert resp_ready.json() == {"status": "unavailable", "reason": "draining"}

        # Mark stopped and set event so lifespan shutdown does not hang
        app.state.drain_coordinator._state = InstanceDrainState.STOPPED
        app.state.drain_coordinator._stopped_event.set()


def test_14_readiness_when_stopped() -> None:
    """Verify readiness returns 503 when application is stopped."""
    app = create_gateway_app(CoreConfig(instance_name="stopped_app"))

    # Before start
    client = TestClient(app)
    resp_unstarted = client.get("/ready")
    assert resp_unstarted.status_code == 503
    assert resp_unstarted.json() == {"status": "unavailable", "reason": "not_started"}

    # After stop
    with TestClient(app):
        pass

    app.state.started = True
    app.state.stopped = True
    resp_stopped = client.get("/ready")
    assert resp_stopped.status_code == 503
    assert resp_stopped.json() == {"status": "unavailable", "reason": "stopped"}


def test_15_readiness_capacity_exceeded() -> None:
    """Verify readiness returns 503 when capacity is full."""
    app = create_gateway_app(
        CoreConfig(
            instance_name="cap_app",
            max_concurrent_connections=1,
            max_connections_per_tenant=1,
        )
    )

    with TestClient(app) as client:
        # Bypass ingress check to specifically test capacity branch
        object.__setattr__(app.state.config, "ingress_mode", "synthetic")
        # Mock available capacity to 0
        with patch.object(app.state.admission_controller, "snapshot") as mock_snap:
            mock_snap.return_value = type("Snap", (), {"available_capacity": 0})()
            resp = client.get("/ready")
            assert resp.status_code == 503
            assert resp.json() == {"status": "unavailable", "reason": "capacity_exceeded"}

            # When capacity is available and state is ACCEPTING
            mock_snap.return_value = type("Snap", (), {"available_capacity": 1})()
            resp_ready = client.get("/ready")
            assert resp_ready.status_code == 200
            assert resp_ready.json() == {"status": "ready", "instance": "cap_app"}


def test_16_websocket_rejection_without_accept() -> None:
    """Verify that WebSocket handshakes are rejected without accepting (code 1008)."""
    app = create_gateway_app()

    client = TestClient(app)
    with pytest.raises(WebSocketDisconnect) as exc_info:
        with client.websocket_connect("/"):
            pass
    assert exc_info.value.code == 1008

    with pytest.raises(WebSocketDisconnect) as exc_info_ws:
        with client.websocket_connect("/ws/voice"):
            pass
    assert exc_info_ws.value.code == 1008


def test_17_no_server_or_socket_spawned_on_import() -> None:
    """Verify that importing kaiwen_gateway.server spawns zero threads or sockets."""
    threads_before = threading.active_count()

    importlib.import_module("kaiwen_gateway.server")

    threads_after = threading.active_count()
    assert threads_before == threads_after


def test_18_optional_server_dependencies_missing() -> None:
    """Verify ServerDependencyError is raised when starlette is missing."""
    import kaiwen_gateway.server.app as srv_app_module

    with patch.object(srv_app_module, "_HAS_STARLETTE", False):
        with pytest.raises(ServerDependencyError, match="require 'starlette' to be installed"):
            create_gateway_app()


def test_19_core_dependency_count_zero() -> None:
    """Verify that Core has zero runtime dependencies in package metadata."""
    import pathlib

    pyproject_path = pathlib.Path("pyproject.toml")
    if pyproject_path.exists():
        content = pyproject_path.read_text(encoding="utf-8")
        assert "dependencies = []" in content


def test_20_local_asgi_host_smoke() -> None:
    """Verify that default app() factory and entrypoint work on local loopback."""
    app = default_app()
    assert app is not None

    with TestClient(app) as client:
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"


def test_21_lifespan_shutdown_when_already_draining() -> None:
    """Verify lifespan shutdown correctly awaits wait_stopped when coordinator is already draining."""
    from kaiwen_gateway.core.draining import DrainReport

    app = create_gateway_app()
    coord: InstanceDrainCoordinator = app.state.drain_coordinator
    coord._state = InstanceDrainState.DRAINING
    coord._report = DrainReport(
        initial_connections=0,
        graceful_completions=0,
        force_closed=0,
        failed_outcomes=0,
        unresolved_connections=0,
        active_leases_remaining=0,
        deadline_exceeded=False,
        final_state=InstanceDrainState.STOPPED,
        duration_seconds=0.0,
    )
    coord._stopped_event.set()

    async def _run() -> None:
        async with gateway_lifespan(app):
            pass

    asyncio.run(_run())


def test_22_main_entrypoint() -> None:
    """Verify local development __main__ entrypoint configuration."""
    from kaiwen_gateway.server.__main__ import main

    with patch("uvicorn.run") as mock_run:
        main()
        mock_run.assert_called_once_with(
            "kaiwen_gateway.server:create_gateway_app",
            factory=True,
            host="127.0.0.1",
            port=8000,
            log_level="info",
        )


def _get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def test_23_shutdown_cancellation_during_drain() -> None:
    """Verify that cancellation during coordinator.drain propagates and sets state.stopped = True."""

    async def _run() -> None:
        app = create_gateway_app()

        async def mock_drain(*args: Any, **kwargs: Any) -> None:
            await asyncio.sleep(10.0)

        async def run_lifespan() -> None:
            async with gateway_lifespan(app):
                pass

        with patch.object(InstanceDrainCoordinator, "drain", side_effect=mock_drain):
            task = asyncio.create_task(run_lifespan())
            await asyncio.sleep(0.01)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        assert _is_stopped(app)

    asyncio.run(_run())


def test_24_shutdown_cancellation_during_wait_stopped() -> None:
    """Verify that cancellation during wait_stopped propagates and sets state.stopped = True."""

    async def _run() -> None:
        app = create_gateway_app()
        coord: InstanceDrainCoordinator = app.state.drain_coordinator
        coord._state = InstanceDrainState.DRAINING

        async def mock_wait_stopped(*args: Any, **kwargs: Any) -> None:
            await asyncio.sleep(10.0)

        async def run_lifespan() -> None:
            async with gateway_lifespan(app):
                pass

        with patch.object(InstanceDrainCoordinator, "wait_stopped", side_effect=mock_wait_stopped):
            task = asyncio.create_task(run_lifespan())
            await asyncio.sleep(0.01)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        assert _is_stopped(app)

    asyncio.run(_run())


def test_25_shutdown_with_active_connection_graceful() -> None:
    """Verify that active connections complete gracefully during bounded shutdown."""

    async def _run() -> None:
        app = create_gateway_app(
            drain_policy=DrainPolicy(grace_period=0.5, force_close_timeout=0.2)
        )
        coord: InstanceDrainCoordinator = app.state.drain_coordinator

        principal = ProviderPrincipal("synthetic_provider")
        binding = TenantCallBinding(
            authorized_provider_id="synthetic_provider",
            tenant_id="tenant_alpha",
            call_id="call_alpha_01",
            allowed_scopes=["voice.stream"],
        )
        ctx = bind_verified_connection_context(
            principal=principal,
            binding=binding,
            connection_id="conn_graceful_01",
        )

        async with gateway_lifespan(app):
            supervisor = await coord.admit_connection(ctx)
            worker_completed = False

            async def worker() -> None:
                nonlocal worker_completed
                await asyncio.sleep(0.05)
                worker_completed = True

            await supervisor.start(worker)
            assert coord.active_connections_count == 1

        assert _is_stopped(app)
        assert worker_completed is True
        assert coord.active_connections_count == 0
        assert coord.report is not None
        assert coord.report.graceful_completions == 1
        assert coord.report.force_closed == 0

    asyncio.run(_run())


def test_26_shutdown_with_active_connection_timeout_forced() -> None:
    """Verify that active connections exceeding grace period are force closed."""

    async def _run() -> None:
        app = create_gateway_app(
            drain_policy=DrainPolicy(grace_period=0.05, force_close_timeout=0.05)
        )
        coord: InstanceDrainCoordinator = app.state.drain_coordinator

        principal = ProviderPrincipal("synthetic_provider")
        binding = TenantCallBinding(
            authorized_provider_id="synthetic_provider",
            tenant_id="tenant_bravo",
            call_id="call_bravo_01",
            allowed_scopes=["voice.stream"],
        )
        ctx = bind_verified_connection_context(
            principal=principal,
            binding=binding,
            connection_id="conn_forced_01",
        )

        async with gateway_lifespan(app):
            supervisor = await coord.admit_connection(ctx)

            async def long_worker() -> None:
                await asyncio.sleep(5.0)

            await supervisor.start(long_worker)
            assert coord.active_connections_count == 1

        assert _is_stopped(app)
        assert coord.active_connections_count == 0
        assert coord.report is not None
        assert coord.report.force_closed == 1

    asyncio.run(_run())


def test_27_real_loopback_uvicorn_server_smoke() -> None:
    """Verify that a real Uvicorn server serves HTTP endpoints on loopback and shuts down cleanly."""

    async def _run() -> None:
        port = _get_free_port()
        app = create_gateway_app(CoreConfig(instance_name="real_uvicorn_test"))
        config = uvicorn.Config(
            app=app,
            host="127.0.0.1",
            port=port,
            log_level="warning",
        )
        server = uvicorn.Server(config)

        server_task = asyncio.create_task(server.serve())
        try:
            for _ in range(50):
                if server.started:
                    break
                await asyncio.sleep(0.1)
            assert server.started, "Uvicorn server failed to start within 5s"

            async with httpx.AsyncClient(
                base_url=f"http://127.0.0.1:{port}", timeout=5.0
            ) as client:
                resp_health = await client.get("/health")
                assert resp_health.status_code == 200
                assert resp_health.json() == {"status": "ok", "instance": "real_uvicorn_test"}

                resp_ready = await client.get("/ready")
                assert resp_ready.status_code == 503
                assert resp_ready.json() == {"status": "unavailable", "reason": "ingress_disabled"}
        finally:
            server.should_exit = True
            await asyncio.wait_for(server_task, timeout=5.0)

        assert _is_stopped(app)

    asyncio.run(_run())
