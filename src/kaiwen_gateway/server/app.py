"""ASGI application factory for Kaiwen Gateway."""

from __future__ import annotations

from typing import Any

from kaiwen_gateway.core.admission import AdmissionController
from kaiwen_gateway.core.composition import GatewayCore, compose_gateway_core
from kaiwen_gateway.core.config import INGRESS_MODE_SYNTHETIC, CoreConfig, validate_core_config
from kaiwen_gateway.core.draining import (
    DrainPolicy,
    InstanceDrainCoordinator,
    InstanceDrainState,
)
from kaiwen_gateway.core.exceptions import CoreConfigurationError
from kaiwen_gateway.core.identity import CallAuthorizationPort, ProviderAuthenticatorPort
from kaiwen_gateway.server.exceptions import ServerDependencyError
from kaiwen_gateway.server.lifespan import gateway_lifespan
from kaiwen_gateway.transports.synthetic import (
    SYNTHETIC_WS_ENDPOINT_PATH,
    SyntheticAuthBridge,
    SyntheticCallAuthorizer,
    SyntheticProviderAuthenticator,
    synthetic_websocket_endpoint,
)

try:
    from starlette.applications import Starlette
    from starlette.middleware import Middleware
    from starlette.requests import Request
    from starlette.responses import JSONResponse, Response
    from starlette.routing import Route, WebSocketRoute
    from starlette.websockets import WebSocket

    _HAS_STARLETTE = True
except ImportError:  # pragma: no cover
    _HAS_STARLETTE = False
    Request = Any  # type: ignore[assignment,misc]
    WebSocket = Any  # type: ignore[assignment,misc]
    Starlette = Any  # type: ignore[assignment,misc]
    JSONResponse = Any  # type: ignore[assignment,misc]
    Response = Any  # type: ignore[assignment,misc]
    Middleware = Any  # type: ignore[assignment,misc]


class UpgradeRejectionMiddleware:
    """HTTP pre-upgrade safety middleware refusing WebSocket upgrade attempts with HTTP 403.

    Note on ASGI Handshake vs HTTP Scope:
        In ASGI specifications, an HTTP request carrying `Upgrade: websocket` headers
        remains an `http` scope unless an installed protocol engine (such as `wsproto`
        or `websockets`) translates it to a `websocket` scope. This middleware ensures
        that any HTTP upgrade-like request is rejected early with HTTP 403 Forbidden,
        preventing fallback to standard 404 routing. Actual WebSocket connections
        (`websocket` scope) are handled separately by :func:`reject_websocket_endpoint`
        via `WebSocketRoute`.

        Configuring a concrete WebSocket protocol backend (`websockets` or `wsproto`)
        is a prerequisite for the M3.2 authenticated transport milestone.
    """

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope.get("type") == "http":
            headers = dict(scope.get("headers", []))
            upgrade = headers.get(b"upgrade", b"").lower()
            if upgrade == b"websocket":
                response = Response(
                    content="WebSocket ingress not enabled",
                    status_code=403,
                    media_type="text/plain",
                )
                await response(scope, receive, send)
                return
        await self.app(scope, receive, send)


async def health_endpoint(request: Request) -> JSONResponse:
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


async def readiness_endpoint(request: Request) -> JSONResponse:
    """HTTP readiness endpoint indicating whether the instance accepts connections."""
    app_state = request.app.state
    if not getattr(app_state, "started", False):
        return JSONResponse(
            {"status": "unavailable", "reason": "not_started"},
            status_code=503,
        )

    if getattr(app_state, "stopped", False) or getattr(app_state, "shutdown_failed", False):
        return JSONResponse(
            {"status": "unavailable", "reason": "stopped"},
            status_code=503,
        )

    coordinator: InstanceDrainCoordinator = app_state.drain_coordinator
    if (
        getattr(app_state, "shutting_down", False)
        or coordinator.state != InstanceDrainState.ACCEPTING
    ):
        reason = "draining" if coordinator.state == InstanceDrainState.DRAINING else "stopped"
        return JSONResponse(
            {"status": "unavailable", "reason": reason},
            status_code=503,
        )

    controller: AdmissionController = app_state.admission_controller
    if controller.snapshot().available_capacity <= 0:
        return JSONResponse(
            {"status": "unavailable", "reason": "capacity_exceeded"},
            status_code=503,
        )

    if app_state.config.ingress_mode == INGRESS_MODE_SYNTHETIC:
        bridge = getattr(app_state, "synthetic_auth_bridge", None)
        if (
            bridge is None
            or not bridge.enforce_call_exclusivity
            or not bridge.verifier.enable_replay_protection
        ):
            return JSONResponse(
                {"status": "unavailable", "reason": "synthetic_ingress_misconfigured"},
                status_code=503,
            )
        return JSONResponse(
            {
                "status": "ready",
                "ingress_mode": "synthetic",
                "instance": app_state.config.instance_name,
            },
            status_code=200,
        )

    # Ingress remains fail-closed until an authenticated transport is configured
    return JSONResponse(
        {"status": "unavailable", "reason": "ingress_disabled"},
        status_code=503,
    )


async def reject_websocket_endpoint(websocket: WebSocket) -> None:
    """Reject WebSocket connection attempts prior to upgrade acceptance.

    In the ASGI / Starlette protocol contract, closing an unaccepted WebSocket
    connection (sending `websocket.close` without a preceding `websocket.accept`)
    signals the ASGI server (such as Uvicorn) to refuse the WebSocket handshake.
    In-process test clients observe the internal ASGI close code 1008.

    No WebSocket frame is exchanged, no connection is accepted, and no upstream
    AI session or voice pipeline resource is allocated. Real network wire testing
    with an ASGI server requires a WebSocket protocol engine (wsproto or websockets),
    which is an M3.2 milestone prerequisite.
    """
    await websocket.close(code=1008, reason="WebSocket ingress not enabled")


def create_gateway_app(
    config: CoreConfig | None = None,
    *,
    core: GatewayCore | None = None,
    admission_controller: AdmissionController | None = None,
    drain_coordinator: InstanceDrainCoordinator | None = None,
    drain_policy: DrainPolicy | None = None,
    authenticator: ProviderAuthenticatorPort | None = None,
    authorizer: CallAuthorizationPort | None = None,
    synthetic_auth_bridge: SyntheticAuthBridge | None = None,
) -> Starlette:
    """Construct a fresh, instance-scoped ASGI application for Kaiwen Gateway.

    Ownership & Lifecycle Notes:
        - Each application instance strictly owns its Core composition, admission
          controller, and drain coordinator. Sharing mutable controllers or coordinators
          across multiple app instances creates process-wide state and is not supported.
        - When an external `drain_coordinator` is provided without an `admission_controller`,
          the application binds `drain_coordinator.controller` as its admission controller,
          ensuring a single consistent controller for capacity accounting and draining.
        - If both `drain_coordinator` and `admission_controller` are provided, they must
          reference the exact same `AdmissionController` instance.
        - If an external controller or coordinator has capacity ceilings differing from
          the resolved `CoreConfig`, the factory fails closed with `CoreConfigurationError`.
        - The ASGI lifespan context manager (`gateway_lifespan`) manages bounded shutdown
          for `drain_coordinator`.

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
        CoreConfigurationError: If configuration or component limits conflict.
        TypeError: If provided components are not instances of expected types.
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

    # Validate provided component types upfront
    if admission_controller is not None and not isinstance(
        admission_controller, AdmissionController
    ):
        raise TypeError(
            f"admission_controller must be an AdmissionController, got {type(admission_controller).__name__}"
        )
    if drain_coordinator is not None and not isinstance(
        drain_coordinator, InstanceDrainCoordinator
    ):
        raise TypeError(
            f"drain_coordinator must be an InstanceDrainCoordinator, got {type(drain_coordinator).__name__}"
        )

    # Resolve AdmissionController and InstanceDrainCoordinator with single ownership
    if drain_coordinator is not None:
        if drain_policy is not None and drain_coordinator.policy != drain_policy:
            raise CoreConfigurationError(
                "Explicit drain_policy conflicts with provided drain_coordinator.policy"
            )
        if (
            admission_controller is not None
            and drain_coordinator.controller is not admission_controller
        ):
            raise CoreConfigurationError(
                "Provided drain_coordinator.controller does not match provided admission_controller"
            )
        effective_controller = (
            admission_controller
            if admission_controller is not None
            else drain_coordinator.controller
        )
        effective_coordinator = drain_coordinator
    else:
        effective_controller = (
            admission_controller
            if admission_controller is not None
            else AdmissionController(effective_config)
        )
        effective_policy = drain_policy if drain_policy is not None else DrainPolicy()
        effective_coordinator = InstanceDrainCoordinator(
            controller=effective_controller,
            policy=effective_policy,
        )

    # Enforce capacity limit consistency between effective_controller and effective_config
    if (
        effective_controller.max_concurrent_connections
        != effective_config.max_concurrent_connections
        or effective_controller.max_connections_per_tenant
        != effective_config.max_connections_per_tenant
    ):
        raise CoreConfigurationError(
            "AdmissionController capacity limits conflict with CoreConfig limits: "
            f"controller=({effective_controller.max_concurrent_connections}, {effective_controller.max_connections_per_tenant}) "
            f"vs config=({effective_config.max_concurrent_connections}, {effective_config.max_connections_per_tenant})"
        )

    # Resolve and validate authenticator / authorizer / synthetic auth bridge
    effective_auth_bridge = synthetic_auth_bridge
    effective_authenticator = authenticator
    effective_authorizer = authorizer

    if effective_config.ingress_mode == INGRESS_MODE_SYNTHETIC:
        if effective_auth_bridge is not None:
            if not isinstance(effective_auth_bridge, SyntheticAuthBridge):
                raise TypeError(
                    f"synthetic_auth_bridge must be a SyntheticAuthBridge, got {type(effective_auth_bridge).__name__}"
                )
            if (
                authenticator is not None
                and authenticator is not effective_auth_bridge.authenticator
            ):
                raise CoreConfigurationError(
                    "Explicit authenticator conflicts with synthetic_auth_bridge.authenticator"
                )
            if authorizer is not None and authorizer is not effective_auth_bridge.authorizer:
                raise CoreConfigurationError(
                    "Explicit authorizer conflicts with synthetic_auth_bridge.authorizer"
                )
            effective_authenticator = effective_auth_bridge.authenticator
            effective_authorizer = effective_auth_bridge.authorizer
        else:
            if effective_authenticator is None or effective_authorizer is None:
                raise CoreConfigurationError(
                    "Synthetic ingress mode requires explicit authenticator and authorizer (or synthetic_auth_bridge)"
                )
            if not isinstance(effective_authenticator, ProviderAuthenticatorPort):
                raise TypeError("authenticator must satisfy ProviderAuthenticatorPort")
            if not isinstance(effective_authorizer, CallAuthorizationPort):
                raise TypeError("authorizer must satisfy CallAuthorizationPort")
            if (
                isinstance(effective_authenticator, SyntheticProviderAuthenticator)
                and isinstance(effective_authorizer, SyntheticCallAuthorizer)
                and effective_authenticator._bridge is effective_authorizer._bridge
            ):
                effective_auth_bridge = effective_authenticator._bridge
            else:
                raise CoreConfigurationError(
                    "Synthetic ingress requires authenticator and authorizer belonging to the same SyntheticAuthBridge"
                )

        assert effective_auth_bridge is not None
        if not effective_auth_bridge.enforce_call_exclusivity:
            raise CoreConfigurationError(
                "Synthetic ingress requires active call exclusivity enforcement to be enabled"
            )
        if not effective_auth_bridge.verifier.enable_replay_protection:
            raise CoreConfigurationError(
                "Synthetic ingress requires token replay protection to be enabled"
            )
    else:
        # Disabled ingress mode
        if effective_authenticator is not None and not isinstance(
            effective_authenticator, ProviderAuthenticatorPort
        ):
            raise TypeError("authenticator must satisfy ProviderAuthenticatorPort")
        if effective_authorizer is not None and not isinstance(
            effective_authorizer, CallAuthorizationPort
        ):
            raise TypeError("authorizer must satisfy CallAuthorizationPort")

    if effective_config.ingress_mode == INGRESS_MODE_SYNTHETIC:
        routes = [
            Route("/health", endpoint=health_endpoint, methods=["GET"]),
            Route("/ready", endpoint=readiness_endpoint, methods=["GET"]),
            WebSocketRoute(SYNTHETIC_WS_ENDPOINT_PATH, endpoint=synthetic_websocket_endpoint),
            WebSocketRoute("/", endpoint=reject_websocket_endpoint),
            WebSocketRoute("/{path:path}", endpoint=reject_websocket_endpoint),
        ]
    else:
        routes = [
            Route("/health", endpoint=health_endpoint, methods=["GET"]),
            Route("/ready", endpoint=readiness_endpoint, methods=["GET"]),
            WebSocketRoute("/", endpoint=reject_websocket_endpoint),
            WebSocketRoute("/{path:path}", endpoint=reject_websocket_endpoint),
        ]

    middleware = [Middleware(UpgradeRejectionMiddleware)]

    app = Starlette(
        debug=False,
        routes=routes,
        middleware=middleware,
        lifespan=gateway_lifespan,
    )

    app.state.core = core
    app.state.config = effective_config
    app.state.admission_controller = effective_controller
    app.state.drain_coordinator = effective_coordinator
    app.state.authenticator = effective_authenticator
    app.state.authorizer = effective_authorizer
    app.state.synthetic_auth_bridge = effective_auth_bridge
    app.state.started = False
    app.state.shutting_down = False
    app.state.stopped = False
    app.state.shutdown_failed = False

    return app


def default_app() -> Starlette:
    """Construct a default instance-scoped ASGI application for ASGI runners."""
    return create_gateway_app()
