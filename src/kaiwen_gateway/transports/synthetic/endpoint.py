"""WebSocket endpoint handling inbound synthetic connections for Kaiwen Gateway."""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any

from kaiwen_gateway.core.config import INGRESS_MODE_SYNTHETIC
from kaiwen_gateway.core.draining import InstanceDrainCoordinator, InstanceDrainState
from kaiwen_gateway.core.exceptions import (
    AdmissionError,
    IdentityAuthorizationError,
    InstanceDrainingError,
)
from kaiwen_gateway.core.lifecycle import ConnectionState, ConnectionSupervisor, TerminalReason
from kaiwen_gateway.transports.synthetic.auth import SyntheticAuthBridge
from kaiwen_gateway.transports.synthetic.protocol import (
    EVENT_SESSION_PING,
    EVENT_SESSION_PONG,
    EVENT_SESSION_READY,
    SYNTHETIC_PROTOCOL_VERSION,
    SyntheticProtocolError,
    parse_and_validate_client_frame,
)

try:
    from starlette.websockets import WebSocket, WebSocketDisconnect

    _HAS_STARLETTE = True
except ImportError:  # pragma: no cover
    _HAS_STARLETTE = False
    WebSocket = Any  # type: ignore[assignment,misc]
    WebSocketDisconnect = Any  # type: ignore[assignment,misc]


async def synthetic_websocket_endpoint(websocket: WebSocket) -> None:
    """Handle incoming WebSocket connection attempts on `/ws/synthetic/v1`."""
    app_state = websocket.app.state
    config = app_state.config
    coordinator: InstanceDrainCoordinator = app_state.drain_coordinator
    auth_bridge: SyntheticAuthBridge | None = getattr(app_state, "synthetic_auth_bridge", None)

    # 1. Ingress enablement verification
    if config.ingress_mode != INGRESS_MODE_SYNTHETIC or auth_bridge is None:
        await websocket.close(code=1008, reason="Synthetic ingress not enabled")
        return

    # 2. Coordinator viability verification (no admission during draining or after shutdown)
    if (
        coordinator.state != InstanceDrainState.ACCEPTING
        or coordinator.is_stopped
        or getattr(app_state, "shutting_down", False)
        or getattr(app_state, "stopped", False)
    ):
        await websocket.close(code=1008, reason="Server draining or unavailable")
        return

    # 3. Credential extraction from Authorization header
    auth_header = websocket.headers.get("authorization", "")
    if not auth_header.startswith("Bearer "):
        await websocket.close(code=1008, reason="Missing or invalid Authorization header")
        return

    token = auth_header[7:].strip()
    if not token:
        await websocket.close(code=1008, reason="Empty bearer token")
        return

    # 4. Host-assigned connection identifier (never chosen by client payload)
    connection_id = f"conn_syn_{uuid.uuid4().hex[:16]}"

    # 5. Resolve verified connection context via Core ports
    try:
        verified_context = await auth_bridge.resolve_context(
            connection_id=connection_id,
            raw_token=token,
        )
    except (IdentityAuthorizationError, Exception):
        # Never leak raw token or exception details
        await websocket.close(code=1008, reason="Authentication or authorization failed")
        return

    # 6. Managed admission check via InstanceDrainCoordinator
    supervisor: ConnectionSupervisor | None = None
    try:
        supervisor = await coordinator.admit_connection(verified_context)
    except (AdmissionError, InstanceDrainingError, Exception):
        await websocket.close(
            code=1008, reason="Admission rejected (capacity exceeded or draining)"
        )
        return

    if supervisor.state != ConnectionState.ADMITTED:
        await websocket.close(code=1008, reason="Admission rejected (capacity exceeded)")
        return

    # Register active call session
    await auth_bridge.register_active_call(
        connection_id, verified_context.tenant_id, verified_context.call_id
    )

    # 7. Accept the WebSocket connection (completing upgrade handshake)
    try:
        await websocket.accept()
    except Exception:
        # Handshake failure or client abort before accept completion: release capacity lease immediately
        await auth_bridge.unregister_active_call(
            connection_id, verified_context.tenant_id, verified_context.call_id
        )
        await supervisor.close(reason=TerminalReason.STARTUP_FAILED)
        return

    # Register resource cleanup callback so supervisor can close WebSocket on shutdown or cancellation
    async def _cleanup_socket() -> None:
        await auth_bridge.unregister_active_call(
            connection_id, verified_context.tenant_id, verified_context.call_id
        )
        try:
            await websocket.close(code=1000, reason="Server shutting down")
        except Exception:
            pass

    supervisor.register_resource(_cleanup_socket)

    # 8. Supervised session worker coroutine
    async def _session_worker() -> None:
        try:
            # Send session.ready event upon established connection
            ready_payload = {
                "event": EVENT_SESSION_READY,
                "protocol_version": SYNTHETIC_PROTOCOL_VERSION,
                "connection_id": verified_context.connection_id,
                "tenant_id": verified_context.tenant_id,
                "call_id": verified_context.call_id,
                "allowed_scopes": sorted(verified_context.allowed_scopes),
            }
            await websocket.send_text(json.dumps(ready_payload, separators=(",", ":")))

            # Control message processing loop
            while True:
                message = await websocket.receive()
                msg_type = message.get("type")

                if msg_type == "websocket.disconnect":
                    break

                # Reject binary frames (voice audio streaming is not accepted in M3.2A)
                if "bytes" in message and message["bytes"] is not None:
                    await websocket.close(code=1003, reason="Binary frames not supported")
                    break

                text_data = message.get("text")
                if text_data is None:
                    continue

                # Validate frame size, JSON syntax, and event schema
                try:
                    payload = parse_and_validate_client_frame(text_data)
                except SyntheticProtocolError as proto_err:
                    await websocket.close(code=proto_err.close_code, reason=str(proto_err))
                    break

                event_name = payload["event"]
                if event_name == EVENT_SESSION_PING:
                    pong: dict[str, Any] = {"event": EVENT_SESSION_PONG}
                    if "seq" in payload:
                        pong["seq"] = payload["seq"]
                    await websocket.send_text(json.dumps(pong, separators=(",", ":")))
                else:
                    # Client requested graceful termination (EVENT_SESSION_END)
                    await websocket.close(code=1000, reason="Session ended by client")
                    break

        except asyncio.CancelledError:
            raise
        except WebSocketDisconnect:
            pass
        except Exception:
            try:
                await websocket.close(code=1011, reason="Internal server error")
            except Exception:
                pass
            raise
        finally:
            await auth_bridge.unregister_active_call(
                connection_id, verified_context.tenant_id, verified_context.call_id
            )
            if not supervisor.state.is_terminal:
                await supervisor.close(reason=TerminalReason.NORMAL)

    # 9. Start supervised worker and await its termination
    try:
        await supervisor.start(_session_worker)
        await supervisor.wait_closed()
    finally:
        # 10. Ensure supervisor is closed and lease is released
        await auth_bridge.unregister_active_call(
            connection_id, verified_context.tenant_id, verified_context.call_id
        )
        if not supervisor.state.is_terminal:
            await supervisor.close(reason=TerminalReason.NORMAL)
