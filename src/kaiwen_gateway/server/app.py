"""ASGI application factory for Kaiwen Gateway."""

from __future__ import annotations

import math
from collections.abc import Callable, Collection, Sequence
from typing import Any

from kaiwen_gateway.core.admission import AdmissionController
from kaiwen_gateway.core.composition import GatewayCore, compose_gateway_core
from kaiwen_gateway.core.config import (
    EVENTS_INGRESS_MODE_AUTHENTICATED,
    EVENTS_INGRESS_MODE_DISABLED,
    INGRESS_MODE_SYNTHETIC,
    CoreConfig,
    validate_core_config,
)
from kaiwen_gateway.core.draining import (
    DrainPolicy,
    InstanceDrainCoordinator,
    InstanceDrainState,
)
from kaiwen_gateway.core.exceptions import CoreConfigurationError
from kaiwen_gateway.core.identity import CallAuthorizationPort, ProviderAuthenticatorPort
from kaiwen_gateway.core.module_lifecycle import ModuleLifecycleManager, ModuleLifecyclePolicy
from kaiwen_gateway.modules.events import (
    BoundedTicketReplayCache,
    EventBrokerConfig,
    EventsAuthenticatorPort,
    EventsAuthorizerPort,
    EventsModule,
    EventsTicketVerifierPort,
    EventTopicAuthorizerPort,
    TicketReplayCachePort,
)
from kaiwen_gateway.modules.notifications import (
    NotificationPublisherPort,
    NotificationsDispatcher,
    NotificationsModule,
)
from kaiwen_gateway.modules.voice.ports import AIRealtimeMediaPort
from kaiwen_gateway.server.exceptions import ServerDependencyError
from kaiwen_gateway.server.lifespan import gateway_lifespan
from kaiwen_gateway.transports.events import EVENTS_WS_ENDPOINT_PATH, events_websocket_endpoint
from kaiwen_gateway.transports.synthetic import (
    SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
    SYNTHETIC_WS_ENDPOINT_PATH,
    SyntheticAuthBridge,
    SyntheticCallAuthorizer,
    SyntheticProviderAuthenticator,
    synthetic_media_websocket_endpoint,
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
    if getattr(app_state, "startup_failed", False):
        return JSONResponse(
            {"status": "unavailable", "reason": "startup_failed"},
            status_code=503,
        )
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

    voice_enabled = app_state.config.ingress_mode == INGRESS_MODE_SYNTHETIC
    events_enabled = (
        app_state.config.events_ingress_mode == EVENTS_INGRESS_MODE_AUTHENTICATED
        and getattr(app_state, "enable_events", False)
    )

    if voice_enabled:
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

    if events_enabled:
        events_mod: EventsModule | None = getattr(app_state, "events_module", None)
        if events_mod is None or not events_mod.is_started or events_mod.broker.is_closed:
            return JSONResponse(
                {"status": "unavailable", "reason": "events_not_started"},
                status_code=503,
            )
        if not getattr(events_mod.broker, "is_healthy", True):
            return JSONResponse(
                {"status": "unavailable", "reason": "events_backend_unavailable"},
                status_code=503,
            )

    if getattr(app_state, "enable_notifications", False):
        notif_mod: NotificationsModule | None = getattr(app_state, "notifications_module", None)
        if notif_mod is None or not notif_mod.is_started or notif_mod.is_closed:
            return JSONResponse(
                {"status": "unavailable", "reason": "notifications_not_started"},
                status_code=503,
            )
        if getattr(app_state, "events_topic_authorizer", None) is None:
            return JSONResponse(
                {"status": "unavailable", "reason": "notifications_misconfigured"},
                status_code=503,
            )

    if voice_enabled and not events_enabled:
        return JSONResponse(
            {
                "status": "ready",
                "ingress_mode": "synthetic",
                "instance": app_state.config.instance_name,
            },
            status_code=200,
        )

    if events_enabled and not voice_enabled:
        return JSONResponse(
            {
                "status": "ready",
                "ingress_mode": app_state.config.ingress_mode,
                "events_ingress_mode": "authenticated",
                "instance": app_state.config.instance_name,
            },
            status_code=200,
        )

    if voice_enabled and events_enabled:
        return JSONResponse(
            {
                "status": "ready",
                "ingress_mode": "synthetic",
                "events_ingress_mode": "authenticated",
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
    enable_synthetic_media: bool = False,
    ai_media_port_factory: Callable[..., AIRealtimeMediaPort] | None = None,
    module_lifecycle_policy: ModuleLifecyclePolicy | None = None,
    enable_events: bool = False,
    events_authenticator: EventsAuthenticatorPort | None = None,
    events_authorizer: EventsAuthorizerPort | None = None,
    events_ticket_verifier: EventsTicketVerifierPort | None = None,
    events_allowed_origins: Sequence[str] = (),
    events_broker_config: EventBrokerConfig | None = None,
    events_module: EventsModule | None = None,
    events_replay_cache: TicketReplayCachePort | None = None,
    events_send_timeout_seconds: float = 5.0,
    events_topic_authorizer: EventTopicAuthorizerPort | None = None,
    enable_notifications: bool = False,
    notifications_module: NotificationsModule | None = None,
    notifications_dispatcher: NotificationPublisherPort | None = None,
    notifications_authoritative_promotional_categories: Collection[str] = (),
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
          for `drain_coordinator` and owns the startup/shutdown orchestration of the
          modules registered on `core` (registration order on startup, reverse order after
          connections drained on shutdown). Readiness stays unavailable until every module
          started; a module startup failure rolls back and leaves the instance stopped.
          Applications without registered modules are unaffected.

    Args:
        config: Core configuration. Validated upfront if provided.
        core: Optional pre-composed GatewayCore instance.
        admission_controller: Optional admission controller.
        drain_coordinator: Optional instance drain coordinator.
        drain_policy: Optional drain policy used if drain_coordinator is not supplied.
        module_lifecycle_policy: Optional per-hook startup/shutdown time bounds for
            registered modules (defaults to 30 seconds per hook).

    Returns:
        A configured Starlette ASGI application instance.

    Raises:
        ServerDependencyError: If starlette is not installed.
        CoreConfigurationError: If configuration or component limits conflict.
        TypeError: If provided components are not instances of expected types.
    """
    if not _HAS_STARLETTE:
        raise ServerDependencyError("ASGI server features require 'starlette' to be installed.")

    if not isinstance(enable_synthetic_media, bool):
        raise TypeError(
            f"enable_synthetic_media must be a bool, got {type(enable_synthetic_media).__name__}"
        )
    if not isinstance(enable_events, bool):
        raise TypeError(f"enable_events must be a bool, got {type(enable_events).__name__}")
    if not isinstance(enable_notifications, bool):
        raise TypeError(
            f"enable_notifications must be a bool, got {type(enable_notifications).__name__}"
        )
    if events_topic_authorizer is not None and not isinstance(
        events_topic_authorizer, EventTopicAuthorizerPort
    ):
        raise TypeError("events_topic_authorizer must satisfy EventTopicAuthorizerPort")
    if (
        isinstance(events_send_timeout_seconds, bool)
        or not isinstance(events_send_timeout_seconds, (int, float))
        or not math.isfinite(events_send_timeout_seconds)
        or events_send_timeout_seconds <= 0.0
    ):
        raise CoreConfigurationError(
            "events_send_timeout_seconds must be a finite positive number",
            field_name="events_send_timeout_seconds",
        )
    if ai_media_port_factory is not None and not callable(ai_media_port_factory):
        raise TypeError("ai_media_port_factory must be callable")

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

    # Resolve and validate Events ingress configuration (fail-closed)
    effective_events_module: EventsModule | None = events_module
    normalized_origins = tuple(events_allowed_origins)
    for origin in normalized_origins:
        if not isinstance(origin, str) or not origin:
            raise CoreConfigurationError(
                "events_allowed_origins entries must be non-empty strings",
                field_name="events_allowed_origins",
            )

    if enable_events and effective_config.events_ingress_mode == EVENTS_INGRESS_MODE_DISABLED:
        raise CoreConfigurationError(
            "enable_events=True requires CoreConfig.events_ingress_mode='authenticated'",
            field_name="events_ingress_mode",
        )
    if (
        effective_config.events_ingress_mode == EVENTS_INGRESS_MODE_AUTHENTICATED
        and not enable_events
    ):
        raise CoreConfigurationError(
            "events_ingress_mode='authenticated' requires enable_events=True",
            field_name="events_ingress_mode",
        )

    if enable_events:
        if (events_authenticator is None) != (events_authorizer is None):
            raise CoreConfigurationError(
                "events_authenticator and events_authorizer must both be provided together"
            )
        if events_authenticator is not None and not isinstance(
            events_authenticator, EventsAuthenticatorPort
        ):
            raise TypeError("events_authenticator must satisfy EventsAuthenticatorPort")
        if events_authorizer is not None and not isinstance(
            events_authorizer, EventsAuthorizerPort
        ):
            raise TypeError("events_authorizer must satisfy EventsAuthorizerPort")
        if events_ticket_verifier is not None:
            if not isinstance(events_ticket_verifier, EventsTicketVerifierPort):
                raise TypeError("events_ticket_verifier must satisfy EventsTicketVerifierPort")
            if not normalized_origins:
                raise CoreConfigurationError(
                    "events_ticket_verifier requires non-empty events_allowed_origins",
                    field_name="events_allowed_origins",
                )
        if events_authenticator is None and events_ticket_verifier is None:
            raise CoreConfigurationError(
                "enable_events=True requires events_authenticator+events_authorizer "
                "and/or events_ticket_verifier"
            )

        existing_events = core.find_module("events")
        if existing_events is not None:
            if not isinstance(existing_events, EventsModule):
                raise CoreConfigurationError(
                    "Registered 'events' module is not an EventsModule instance"
                )
            if (
                effective_events_module is not None
                and effective_events_module is not existing_events
            ):
                raise CoreConfigurationError(
                    "Explicit events_module conflicts with module already registered on core"
                )
            effective_events_module = existing_events
        else:
            if effective_events_module is None:
                effective_events_module = EventsModule(
                    broker_config=events_broker_config,
                    topic_authorizer=events_topic_authorizer,
                )
            elif not isinstance(effective_events_module, EventsModule):
                raise TypeError("events_module must be an EventsModule instance")
            core = compose_gateway_core(
                config=effective_config,
                modules=[*core.modules(), effective_events_module],
            )

    effective_notifications_module: NotificationsModule | None = notifications_module
    effective_notifications_dispatcher: NotificationPublisherPort | None = notifications_dispatcher
    if enable_notifications:
        if not enable_events:
            raise CoreConfigurationError(
                "enable_notifications=True requires enable_events=True",
                field_name="enable_notifications",
            )
        if events_topic_authorizer is None:
            raise CoreConfigurationError(
                "enable_notifications=True requires events_topic_authorizer to be configured fail-closed",
                field_name="events_topic_authorizer",
            )
        existing_notifications = core.find_module("notifications")
        if existing_notifications is not None:
            if not isinstance(existing_notifications, NotificationsModule):
                raise CoreConfigurationError(
                    "Registered 'notifications' module is not a NotificationsModule instance"
                )
            if (
                effective_notifications_module is not None
                and effective_notifications_module is not existing_notifications
            ):
                raise CoreConfigurationError(
                    "Explicit notifications_module conflicts with module already registered on core"
                )
            effective_notifications_module = existing_notifications
        else:
            if effective_notifications_module is None:
                effective_notifications_module = NotificationsModule()
            elif not isinstance(effective_notifications_module, NotificationsModule):
                raise TypeError("notifications_module must be a NotificationsModule instance")
            core = compose_gateway_core(
                config=effective_config,
                modules=[*core.modules(), effective_notifications_module],
            )

        assert effective_events_module is not None
        broker_topic_auth = getattr(effective_events_module.broker, "topic_authorizer", None)
        if broker_topic_auth is not events_topic_authorizer:
            raise CoreConfigurationError(
                "enable_notifications=True requires events_module.broker.topic_authorizer "
                "to match events_topic_authorizer",
                field_name="events_topic_authorizer",
            )
        if effective_notifications_dispatcher is None:
            effective_notifications_dispatcher = NotificationsDispatcher(
                broker=effective_events_module.broker,
                preference_port=effective_notifications_module.preference_port,
                producer_authorizer=effective_notifications_module.producer_authorizer,
                telemetry=effective_notifications_module.telemetry,
                authoritative_promotional_categories=(
                    notifications_authoritative_promotional_categories
                ),
            )
        elif not isinstance(effective_notifications_dispatcher, NotificationPublisherPort):
            raise TypeError("notifications_dispatcher must implement NotificationPublisherPort")

    if events_replay_cache is not None and not isinstance(
        events_replay_cache, TicketReplayCachePort
    ):
        raise TypeError("events_replay_cache must implement TicketReplayCachePort")
    effective_replay_cache = (
        events_replay_cache if events_replay_cache is not None else BoundedTicketReplayCache()
    )

    routes: list[Route | WebSocketRoute] = [
        Route("/health", endpoint=health_endpoint, methods=["GET"]),
        Route("/ready", endpoint=readiness_endpoint, methods=["GET"]),
    ]
    if effective_config.ingress_mode == INGRESS_MODE_SYNTHETIC:
        routes.append(
            WebSocketRoute(SYNTHETIC_WS_ENDPOINT_PATH, endpoint=synthetic_websocket_endpoint)
        )
        if enable_synthetic_media:
            routes.append(
                WebSocketRoute(
                    SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
                    endpoint=synthetic_media_websocket_endpoint,
                )
            )
    if enable_events:
        routes.append(WebSocketRoute(EVENTS_WS_ENDPOINT_PATH, endpoint=events_websocket_endpoint))
    routes.extend(
        [
            WebSocketRoute("/", endpoint=reject_websocket_endpoint),
            WebSocketRoute("/{path:path}", endpoint=reject_websocket_endpoint),
        ]
    )

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
    app.state.enable_synthetic_media = enable_synthetic_media
    app.state.ai_media_port_factory = ai_media_port_factory
    app.state.enable_events = enable_events
    app.state.events_authenticator = events_authenticator
    app.state.events_authorizer = events_authorizer
    app.state.events_ticket_verifier = events_ticket_verifier
    app.state.events_allowed_origins = normalized_origins
    app.state.events_module = effective_events_module
    app.state.events_replay_cache = effective_replay_cache
    app.state.events_send_timeout_seconds = float(events_send_timeout_seconds)
    app.state.events_topic_authorizer = events_topic_authorizer
    app.state.enable_notifications = enable_notifications
    app.state.notifications_module = effective_notifications_module
    app.state.notifications_dispatcher = effective_notifications_dispatcher
    app.state.module_lifecycle = ModuleLifecycleManager(core, policy=module_lifecycle_policy)
    app.state.started = False
    app.state.shutting_down = False
    app.state.stopped = False
    app.state.shutdown_failed = False
    app.state.startup_failed = False

    return app


def default_app() -> Starlette:
    """Construct a default instance-scoped ASGI application for ASGI runners."""
    return create_gateway_app()
