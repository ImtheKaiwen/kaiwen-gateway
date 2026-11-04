"""ASGI application factory for Kaiwen Gateway."""

from __future__ import annotations

from typing import Any

from kaiwen_gateway.core.admission import AdmissionController
from kaiwen_gateway.core.composition import GatewayCore, compose_gateway_core
from kaiwen_gateway.core.config import CoreConfig, validate_core_config
from kaiwen_gateway.core.draining import (
    DrainPolicy,
    InstanceDrainCoordinator,
    InstanceDrainState,
)
from kaiwen_gateway.core.exceptions import CoreConfigurationError
from kaiwen_gateway.server.exceptions import ServerDependencyError
from kaiwen_gateway.server.lifespan import gateway_lifespan

try:
    from starlette.applications import Starlette
    from starlette.requests import Request
    from starlette.responses import JSONResponse
    from starlette.routing import Route, WebSocketRoute
    from starlette.websockets import WebSocket

    _HAS_STARLETTE = True
except ImportError:  # pragma: no cover
    _HAS_STARLETTE = False
    Request = Any  # type: ignore[assignment,misc]
    WebSocket = Any  # type: ignore[assignment,misc]
    Starlette = Any  # type: ignore[assignment,misc]


async def health_endpoint(request: Request) -> Any:
    """HTTP liveness endpoint indicating ASGI host process viability."""
    app_state = request.app.state
    if not getattr(app_state, "started", False):
        return JSONResponse(
            {"status": "starting", "instance": app_state.config.instance_name},
            status_code=503,
        )

    return JSONResponse(
        {"status": "ok", "instance": app_state.config.instance_name},
        status_code=200,
    )


async def readiness_endpoint(request: Request) -> Any:
    """HTTP readiness endpoint indicating whether the instance accepts connections."""
    app_state = request.app.state
    if not getattr(app_state, "started", False):
        return JSONResponse(
            {"status": "unavailable", "reason": "not_started"},
            status_code=503,
        )

    if getattr(app_state, "stopped", False):
        return JSONResponse(
            {"status": "unavailable", "reason": "stopped"},
            status_code=503,
        )

    coordinator: InstanceDrainCoordinator = app_state.drain_coordinator
    if coordinator.state != InstanceDrainState.ACCEPTING:
        reason = "draining" if coordinator.state == InstanceDrainState.DRAINING else "stopped"
        return JSONResponse(
            {"status": "unavailable", "reason": reason},
            status_code=503,
        )

    config: CoreConfig = app_state.config
    if config.ingress_mode == "disabled":
        return JSONResponse(
            {"status": "unavailable", "reason": "ingress_disabled"},
            status_code=503,
        )

    controller: AdmissionController = app_state.admission_controller
    if controller.snapshot().available_capacity <= 0:
        return JSONResponse(
            {"status": "unavailable", "reason": "capacity_exceeded"},
            status_code=503,
        )

    return JSONResponse(
        {"status": "ready", "instance": config.instance_name},
        status_code=200,
    )


async def reject_websocket_endpoint(websocket: WebSocket) -> None:
    """Reject WebSocket connection attempts without accepting."""
    await websocket.close(code=1008, reason="WebSocket ingress not enabled")


def create_gateway_app(
    config: CoreConfig | None = None,
    *,
    core: GatewayCore | None = None,
    admission_controller: AdmissionController | None = None,
    drain_coordinator: InstanceDrainCoordinator | None = None,
    drain_policy: DrainPolicy | None = None,
) -> Any:
    """Construct a fresh, instance-scoped ASGI application for Kaiwen Gateway.

    Args:
        config: Core configuration. Validated upfront if provided.
        core: Optional pre-composed GatewayCore instance.
        admission_controller: Optional admission controller.
        drain_coordinator: Optional instance drain coordinator.
        drain_policy: Optional drain policy used if drain_coordinator is not supplied.

    Returns:
        A configured Starlette ASGI application instance.

    Raises:
        ServerDependencyError: If starlette is not installed.
        CoreConfigurationError: If configuration is invalid.
    """
    if not _HAS_STARLETTE:
        raise ServerDependencyError("ASGI server features require 'starlette' to be installed.")

    # Resolve effective config and GatewayCore instance
    if core is not None:
        if not isinstance(core, GatewayCore):
            raise TypeError(f"core must be an instance of GatewayCore, got {type(core).__name__}")
        if config is not None and config != core.config:
            raise CoreConfigurationError("Explicit config does not match core.config")
        effective_config = core.config
    else:
        effective_config = config if config is not None else CoreConfig()
        validate_core_config(effective_config)
        core = compose_gateway_core(config=effective_config)

    # Resolve AdmissionController
    if admission_controller is not None:
        if not isinstance(admission_controller, AdmissionController):
            raise TypeError(
                f"admission_controller must be an AdmissionController, got {type(admission_controller).__name__}"
            )
        effective_controller = admission_controller
    else:
        effective_controller = AdmissionController(effective_config)

    # Resolve InstanceDrainCoordinator
    if drain_coordinator is not None:
        if not isinstance(drain_coordinator, InstanceDrainCoordinator):
            raise TypeError(
                f"drain_coordinator must be an InstanceDrainCoordinator, got {type(drain_coordinator).__name__}"
            )
        if (
            admission_controller is not None
            and drain_coordinator.controller is not effective_controller
        ):
            raise ValueError(
                "Provided drain_coordinator.controller does not match provided admission_controller"
            )
        effective_coordinator = drain_coordinator
    else:
        effective_policy = drain_policy if drain_policy is not None else DrainPolicy()
        effective_coordinator = InstanceDrainCoordinator(
            controller=effective_controller,
            policy=effective_policy,
        )

    routes = [
        Route("/health", endpoint=health_endpoint, methods=["GET"]),
        Route("/ready", endpoint=readiness_endpoint, methods=["GET"]),
        WebSocketRoute("/", endpoint=reject_websocket_endpoint),
        WebSocketRoute("/{path:path}", endpoint=reject_websocket_endpoint),
    ]

    app = Starlette(
        debug=False,
        routes=routes,
        lifespan=gateway_lifespan,
    )

    app.state.core = core
    app.state.config = effective_config
    app.state.admission_controller = effective_controller
    app.state.drain_coordinator = effective_coordinator
    app.state.started = False
    app.state.stopped = False

    return app


def default_app() -> Any:
    """Construct a default instance-scoped ASGI application for ASGI runners."""
    return create_gateway_app()
