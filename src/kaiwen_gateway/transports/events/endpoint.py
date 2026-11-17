"""Authenticated `/ws/events/v1` WebSocket endpoint for Kaiwen Gateway."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any, Final

from kaiwen_gateway.core.config import EVENTS_INGRESS_MODE_AUTHENTICATED
from kaiwen_gateway.core.draining import InstanceDrainCoordinator, InstanceDrainState
from kaiwen_gateway.core.exceptions import (
    AdmissionError,
    IdentityError,
    InstanceDrainingError,
)
from kaiwen_gateway.core.identity import VerifiedPrincipalContext, bind_verified_principal_context
from kaiwen_gateway.core.lifecycle import ConnectionState, TerminalReason
from kaiwen_gateway.modules.events.auth import (
    EventsAuthenticatorPort,
    EventsAuthorizerPort,
    EventsTicketVerifierPort,
    TicketReplayCachePort,
    verify_and_consume_ticket,
)
from kaiwen_gateway.modules.events.domain import (
    EventAuthorizationError,
    EventBackpressureError,
    EventRateLimitError,
    EventsBackendUnavailableError,
    EventsModuleClosedError,
    InvalidEventError,
)
from kaiwen_gateway.modules.events.module import EventsModule
from kaiwen_gateway.modules.events.ratelimit import RateLimitConfig, TokenBucketRateLimiter
from kaiwen_gateway.transports.events.protocol import (
    CMD_EVENTS_SUBSCRIBE,
    CMD_EVENTS_UNSUBSCRIBE,
    CMD_SESSION_PING,
    EVENTS_MAX_FRAME_BYTES,
    EventsProtocolError,
    OversizedEventsFrameError,
    build_delivery_frame,
    build_error_frame,
    build_session_pong_frame,
    build_session_ready_frame,
    build_subscribed_frame,
    build_unsubscribed_frame,
    parse_and_validate_events_client_frame,
)

try:
    from starlette.websockets import WebSocket, WebSocketDisconnect, WebSocketState

    _HAS_STARLETTE = True
except ImportError:  # pragma: no cover
    _HAS_STARLETTE = False
    WebSocket = Any  # type: ignore[assignment,misc]
    WebSocketDisconnect = Any  # type: ignore[assignment,misc]
    WebSocketState = Any  # type: ignore[assignment,misc]

DEFAULT_EVENTS_IDLE_TIMEOUT_SECONDS: Final[float] = 60.0
DEFAULT_EVENTS_SEND_TIMEOUT_SECONDS: Final[float] = 5.0
DEFAULT_CONTROL_RATE_PER_SECOND: Final[float] = 20.0
DEFAULT_CONTROL_BURST: Final[float] = 40.0
MAX_REPEATED_RATE_LIMIT_VIOLATIONS: Final[int] = 3
_FORBIDDEN_QUERY_TOKEN_KEYS: Final[frozenset[str]] = frozenset(
    {"token", "access_token", "authorization"}
)


async def _safe_close_ws(websocket: WebSocket, *, code: int, reason: str) -> None:
    safe_reason = reason.encode("utf-8")[:120].decode("utf-8", errors="ignore")
    try:
        if getattr(websocket, "client_state", None) != WebSocketState.DISCONNECTED:
            async with asyncio.timeout(1.0):
                await websocket.close(code=code, reason=safe_reason)
    except Exception:
        pass


async def _authenticate_events_handshake(
    websocket: WebSocket,
    *,
    connection_id: str,
    app_state: Any,
) -> VerifiedPrincipalContext:
    query_params = websocket.query_params
    lower_query_keys = {k.lower() for k in query_params.keys()}
    if lower_query_keys & _FORBIDDEN_QUERY_TOKEN_KEYS:
        raise EventAuthorizationError("Token query parameters are forbidden")

    auth_header = websocket.headers.get("authorization", "")
    ticket = query_params.get("ticket")

    if auth_header:
        if not auth_header.startswith("Bearer "):
            raise EventAuthorizationError("Invalid Authorization header scheme")
        token = auth_header[7:].strip()
        if not token:
            raise EventAuthorizationError("Empty Bearer token")

        authenticator: EventsAuthenticatorPort | None = getattr(
            app_state, "events_authenticator", None
        )
        authorizer: EventsAuthorizerPort | None = getattr(app_state, "events_authorizer", None)
        if authenticator is None or authorizer is None:
            raise EventAuthorizationError("Bearer authentication is not configured")

        principal = await authenticator.authenticate_bearer(
            token=token,
            connection_id=connection_id,
        )
        binding = await authorizer.authorize_principal(
            principal=principal,
            connection_id=connection_id,
        )
        return bind_verified_principal_context(
            principal=principal,
            binding=binding,
            connection_id=connection_id,
        )

    if ticket is not None:
        if not ticket:
            raise EventAuthorizationError("Empty WebSocket ticket")
        ticket_verifier: EventsTicketVerifierPort | None = getattr(
            app_state, "events_ticket_verifier", None
        )
        replay_cache: TicketReplayCachePort | None = getattr(app_state, "events_replay_cache", None)
        allowed_origins: tuple[str, ...] = getattr(app_state, "events_allowed_origins", ())
        if ticket_verifier is None or replay_cache is None or not allowed_origins:
            raise EventAuthorizationError("Browser ticket authentication is not configured")

        origin = websocket.headers.get("origin", "")
        return await verify_and_consume_ticket(
            ticket=ticket,
            request_origin=origin,
            allowed_origins=allowed_origins,
            connection_id=connection_id,
            verifier=ticket_verifier,
            replay_cache=replay_cache,
        )

    raise EventAuthorizationError("Missing authentication credentials")


async def events_websocket_endpoint(websocket: WebSocket) -> None:
    """Handle authenticated WebSocket connections on `/ws/events/v1`."""
    app_state = websocket.app.state
    config = app_state.config
    coordinator: InstanceDrainCoordinator = app_state.drain_coordinator
    events_module: EventsModule | None = getattr(app_state, "events_module", None)

    if (
        config.events_ingress_mode != EVENTS_INGRESS_MODE_AUTHENTICATED
        or not getattr(app_state, "enable_events", False)
        or events_module is None
        or not events_module.is_started
        or events_module.broker.is_closed
    ):
        await _safe_close_ws(websocket, code=1008, reason="Events ingress not enabled")
        return

    if (
        coordinator.state != InstanceDrainState.ACCEPTING
        or coordinator.is_stopped
        or getattr(app_state, "shutting_down", False)
        or getattr(app_state, "stopped", False)
    ):
        await _safe_close_ws(websocket, code=1008, reason="Server draining or unavailable")
        return

    connection_id = f"conn_evt_{uuid.uuid4().hex[:16]}"
    try:
        verified_context = await _authenticate_events_handshake(
            websocket,
            connection_id=connection_id,
            app_state=app_state,
        )
    except EventsBackendUnavailableError:
        await _safe_close_ws(websocket, code=1011, reason="Backend unavailable")
        return
    except (EventAuthorizationError, IdentityError, InvalidEventError):
        await _safe_close_ws(
            websocket,
            code=1008,
            reason="Authentication or authorization failed",
        )
        return
    except Exception:
        await _safe_close_ws(websocket, code=1008, reason="Authentication failed")
        return

    try:
        supervisor = await coordinator.admit_principal_connection(verified_context)
    except (AdmissionError, InstanceDrainingError):
        await _safe_close_ws(websocket, code=1008, reason="Connection capacity exceeded")
        return
    except Exception:
        await _safe_close_ws(websocket, code=1011, reason="Admission error")
        return

    if supervisor.state != ConnectionState.ADMITTED:
        await _safe_close_ws(websocket, code=1008, reason="Connection capacity exceeded")
        return

    broker = events_module.broker
    owned_subs: dict[str, str] = {}
    subscribed_signal = asyncio.Event()
    stop_event = asyncio.Event()
    terminal_reason = TerminalReason.NORMAL
    send_lock = asyncio.Lock()
    idle_timeout: float = float(
        getattr(app_state, "events_idle_timeout_seconds", DEFAULT_EVENTS_IDLE_TIMEOUT_SECONDS)
    )
    send_timeout: float = float(
        getattr(app_state, "events_send_timeout_seconds", DEFAULT_EVENTS_SEND_TIMEOUT_SECONDS)
    )
    control_limiter = TokenBucketRateLimiter(
        RateLimitConfig(
            rate_per_second=float(
                getattr(
                    app_state,
                    "events_control_rate_per_second",
                    DEFAULT_CONTROL_RATE_PER_SECOND,
                )
            ),
            burst=float(getattr(app_state, "events_control_burst", DEFAULT_CONTROL_BURST)),
        )
    )

    async def _send_json_safe(payload: dict[str, Any]) -> None:
        async with send_lock:
            async with asyncio.timeout(send_timeout):
                await websocket.send_json(payload)

    async def _cleanup_connection() -> None:
        subscribed_signal.set()
        stop_event.set()
        await broker.close_connection(connection_id)

    try:
        await websocket.accept()
        supervisor.register_resource(_cleanup_connection)
        await _send_json_safe(
            build_session_ready_frame(
                connection_id=connection_id,
                tenant_id=verified_context.tenant_id,
                principal_id=verified_context.principal_id,
                allowed_scopes=sorted(verified_context.allowed_scopes),
            )
        )
    except TimeoutError:
        await asyncio.shield(supervisor.close(reason=TerminalReason.TIMEOUT))
        await _safe_close_ws(websocket, code=1008, reason="Send timeout")
        return
    except asyncio.CancelledError:
        await asyncio.shield(supervisor.close(reason=TerminalReason.CANCELLED))
        raise
    except Exception:
        await asyncio.shield(supervisor.close(reason=TerminalReason.STARTUP_FAILED))
        await _safe_close_ws(websocket, code=1011, reason="Session initialization failed")
        return

    async def reader_worker() -> None:
        nonlocal terminal_reason
        rate_violations = 0
        try:
            while not stop_event.is_set():
                try:
                    async with asyncio.timeout(idle_timeout):
                        message = await websocket.receive()
                except TimeoutError:
                    terminal_reason = TerminalReason.TIMEOUT
                    await _safe_close_ws(websocket, code=1000, reason="Idle timeout")
                    return

                msg_type = message.get("type")
                if msg_type == "websocket.disconnect":
                    return

                raw_data: str | bytes | None = message.get("text")
                if raw_data is None:
                    raw_data = message.get("bytes")
                if raw_data is None:
                    continue

                if len(raw_data.encode("utf-8") if isinstance(raw_data, str) else raw_data) > (
                    EVENTS_MAX_FRAME_BYTES
                ):
                    await _safe_close_ws(websocket, code=1009, reason="Frame too large")
                    return

                decision = await control_limiter.check(connection_id)
                if not decision.allowed:
                    rate_violations += 1
                    await _send_json_safe(
                        build_error_frame(
                            code="RATE_LIMITED",
                            message="Control message rate limit exceeded",
                        )
                    )
                    if rate_violations >= MAX_REPEATED_RATE_LIMIT_VIOLATIONS:
                        await _safe_close_ws(
                            websocket, code=1008, reason="Control message rate limit abuse"
                        )
                        return
                    continue
                rate_violations = 0

                try:
                    frame = parse_and_validate_events_client_frame(raw_data)
                except EventsProtocolError as exc:
                    await _safe_close_ws(websocket, code=exc.close_code, reason=exc.error_code)
                    return

                if frame.command == CMD_SESSION_PING:
                    await _send_json_safe(
                        build_session_pong_frame(request_id=frame.request_id, seq=frame.seq)
                    )
                    continue

                if frame.command == CMD_EVENTS_SUBSCRIBE:
                    assert frame.topic_pattern is not None
                    try:
                        sub = await broker.subscribe(verified_context, frame.topic_pattern)
                    except EventAuthorizationError:
                        await _send_json_safe(
                            build_error_frame(
                                code="FORBIDDEN_SUBSCRIPTION",
                                message="Not authorized for requested topic pattern",
                                request_id=frame.request_id,
                            )
                        )
                        continue
                    except EventRateLimitError:
                        await _send_json_safe(
                            build_error_frame(
                                code="RATE_LIMITED",
                                message="Subscription rate limit exceeded",
                                request_id=frame.request_id,
                            )
                        )
                        continue
                    except EventBackpressureError:
                        await _send_json_safe(
                            build_error_frame(
                                code="SUBSCRIPTION_LIMIT_EXCEEDED",
                                message="Maximum subscriptions per connection exceeded",
                                request_id=frame.request_id,
                            )
                        )
                        continue
                    except InvalidEventError:
                        await _send_json_safe(
                            build_error_frame(
                                code="INVALID_TOPIC",
                                message="Invalid topic_pattern",
                                request_id=frame.request_id,
                            )
                        )
                        continue
                    except EventsBackendUnavailableError:
                        await _send_json_safe(
                            build_error_frame(
                                code="BACKEND_UNAVAILABLE",
                                message="Events backend unavailable",
                                request_id=frame.request_id,
                            )
                        )
                        await _safe_close_ws(websocket, code=1011, reason="Backend unavailable")
                        return
                    except EventsModuleClosedError:
                        await _safe_close_ws(websocket, code=1008, reason="Events module closed")
                        return

                    owned_subs[sub.subscription_id] = sub.topic_pattern
                    subscribed_signal.set()
                    await _send_json_safe(
                        build_subscribed_frame(
                            subscription_id=sub.subscription_id,
                            topic_pattern=sub.topic_pattern,
                            request_id=frame.request_id,
                        )
                    )
                    continue

                if frame.command == CMD_EVENTS_UNSUBSCRIBE:
                    if frame.subscription_id is not None:
                        if frame.subscription_id not in owned_subs:
                            await _send_json_safe(
                                build_unsubscribed_frame(
                                    subscription_id=frame.subscription_id,
                                    removed=False,
                                    request_id=frame.request_id,
                                )
                            )
                            continue
                        removed = await broker.unsubscribe(frame.subscription_id)
                        owned_subs.pop(frame.subscription_id, None)
                        await _send_json_safe(
                            build_unsubscribed_frame(
                                subscription_id=frame.subscription_id,
                                removed=removed,
                                request_id=frame.request_id,
                            )
                        )
                        continue

                    assert frame.topic_pattern is not None
                    matching_ids = [
                        sid for sid, pat in list(owned_subs.items()) if pat == frame.topic_pattern
                    ]
                    any_removed = False
                    for sid in matching_ids:
                        if await broker.unsubscribe(sid):
                            any_removed = True
                        owned_subs.pop(sid, None)
                    await _send_json_safe(
                        build_unsubscribed_frame(
                            topic=frame.topic_pattern,
                            removed=any_removed,
                            request_id=frame.request_id,
                        )
                    )
        except TimeoutError:
            terminal_reason = TerminalReason.TIMEOUT
            await _safe_close_ws(websocket, code=1008, reason="Send timeout")
        except WebSocketDisconnect:
            return
        except asyncio.CancelledError:
            raise
        except Exception:
            terminal_reason = TerminalReason.WORKER_FAILED
            await _safe_close_ws(websocket, code=1011, reason="Internal error")
        finally:
            subscribed_signal.set()
            stop_event.set()

    async def writer_worker() -> None:
        nonlocal terminal_reason
        try:
            await subscribed_signal.wait()
            if stop_event.is_set() or supervisor.is_closed or broker.is_closed:
                return
            try:
                consumer = broker.consumer(connection_id)
            except EventsModuleClosedError:
                return

            async for envelope in consumer:
                if stop_event.is_set():
                    return
                try:
                    delivery_frame = build_delivery_frame(envelope)
                except OversizedEventsFrameError:
                    await _send_json_safe(
                        build_error_frame(
                            code="FRAME_TOO_LARGE",
                            message="Outbound delivery frame exceeds maximum allowed size",
                        )
                    )
                    continue
                await _send_json_safe(delivery_frame)

            # Queue closed while session was still running (e.g. slow-consumer overflow or shutdown)
            if not stop_event.is_set() and not supervisor.is_closed:
                await _safe_close_ws(
                    websocket,
                    code=1008,
                    reason="Slow consumer overflow or broker closed",
                )
        except TimeoutError:
            terminal_reason = TerminalReason.TIMEOUT
            await _safe_close_ws(websocket, code=1008, reason="Send timeout")
        except WebSocketDisconnect:
            return
        except asyncio.CancelledError:
            raise
        except Exception:
            terminal_reason = TerminalReason.WORKER_FAILED
            await _safe_close_ws(websocket, code=1011, reason="Internal delivery error")
        finally:
            stop_event.set()

    try:
        await supervisor.start(reader_worker, writer_worker)
        await stop_event.wait()
    except asyncio.CancelledError:
        terminal_reason = TerminalReason.CANCELLED
        raise
    finally:
        await asyncio.shield(supervisor.close(reason=terminal_reason))
        await _safe_close_ws(websocket, code=1000, reason="Session closed")


__all__ = [
    "DEFAULT_CONTROL_BURST",
    "DEFAULT_CONTROL_RATE_PER_SECOND",
    "DEFAULT_EVENTS_IDLE_TIMEOUT_SECONDS",
    "MAX_REPEATED_RATE_LIMIT_VIOLATIONS",
    "events_websocket_endpoint",
]
