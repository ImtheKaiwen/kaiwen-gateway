"""WebSocket endpoint handling inbound synthetic binary media connections for Kaiwen Gateway."""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Callable
from typing import Any, Final

from kaiwen_gateway.core.config import INGRESS_MODE_SYNTHETIC
from kaiwen_gateway.core.draining import InstanceDrainCoordinator, InstanceDrainState
from kaiwen_gateway.core.exceptions import (
    AdmissionError,
    IdentityAuthorizationError,
    InstanceDrainingError,
)
from kaiwen_gateway.core.lifecycle import ConnectionState, ConnectionSupervisor, TerminalReason
from kaiwen_gateway.modules.voice.adapters.fake_ai import BoundedFakeAIEchoPort
from kaiwen_gateway.modules.voice.bridge import VoiceBridgeSession
from kaiwen_gateway.modules.voice.domain import AudioFormat, AudioFrame
from kaiwen_gateway.modules.voice.ports import AIRealtimeMediaPort
from kaiwen_gateway.transports.synthetic.auth import SyntheticAuthBridge
from kaiwen_gateway.transports.synthetic.media_adapter import WebSocketTelephonyMediaAdapter
from kaiwen_gateway.transports.synthetic.media_protocol import (
    PCM16LE_CHANNELS,
    PCM16LE_FRAME_BYTES,
    PCM16LE_FRAME_MS,
    PCM16LE_SAMPLE_RATE,
    SYNTHETIC_MEDIA_PROTOCOL_VERSION,
)
from kaiwen_gateway.transports.synthetic.protocol import (
    EVENT_CALL_END,
    EVENT_CALL_ENDED,
    EVENT_CALL_START,
    EVENT_CALL_STARTED,
    EVENT_SESSION_END,
    EVENT_SESSION_PING,
    EVENT_SESSION_PONG,
    EVENT_SESSION_READY,
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

DEFAULT_IDLE_TIMEOUT_SECONDS: Final[float] = 60.0
REQUIRED_SYNTHETIC_SCOPE: Final[str] = "voice.stream"


async def synthetic_media_websocket_endpoint(websocket: WebSocket) -> None:
    """Handle incoming WebSocket connection attempts on `/ws/synthetic-media/v1`."""
    app_state = websocket.app.state
    config = app_state.config
    coordinator: InstanceDrainCoordinator = app_state.drain_coordinator
    auth_bridge: SyntheticAuthBridge | None = getattr(app_state, "synthetic_auth_bridge", None)
    enable_synthetic_media: bool = bool(getattr(app_state, "enable_synthetic_media", False))

    # 1. Ingress enablement verification (explicit opt-in required)
    if (
        config.ingress_mode != INGRESS_MODE_SYNTHETIC
        or not enable_synthetic_media
        or auth_bridge is None
    ):
        await websocket.close(code=1008, reason="Synthetic media ingress not enabled")
        return

    if (
        not auth_bridge.enforce_call_exclusivity
        or not auth_bridge.verifier.enable_replay_protection
    ):
        await websocket.close(code=1008, reason="Synthetic ingress security policy misconfigured")
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
    connection_id = f"conn_syn_media_{uuid.uuid4().hex[:16]}"

    # 5. Resolve verified connection context via Core ports
    try:
        verified_context = await auth_bridge.resolve_context(
            connection_id=connection_id,
            raw_token=token,
        )
    except (IdentityAuthorizationError, Exception):
        await websocket.close(code=1008, reason="Authentication or authorization failed")
        return

    # 5b. Verify required permission scope for synthetic media transport
    try:
        verified_context.require_scope(REQUIRED_SYNTHETIC_SCOPE)
    except IdentityAuthorizationError:
        await websocket.close(
            code=1008, reason=f"Missing required scope: {REQUIRED_SYNTHETIC_SCOPE}"
        )
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

    # -------------------------------------------------------------------------
    # Handshake & Session Supervision with unified lifecycle ownership
    # -------------------------------------------------------------------------
    active_call_registered = False
    supervisor_closed = False
    socket_closed = False
    socket_disconnected = False
    wire_accepted = False

    def _is_wire_accepted() -> bool:
        if wire_accepted:
            return True
        state = getattr(websocket, "client_state", None)
        return state is not None and str(state).endswith("CONNECTED")

    async def _close_socket(code: int, reason: str) -> None:
        nonlocal socket_closed
        if socket_closed or socket_disconnected:
            return
        safe_reason = reason.encode("utf-8")[:123].decode("utf-8", "ignore")
        await websocket.close(code=code, reason=safe_reason)
        socket_closed = True

    # Telephony adapter, AI port, and VoiceBridgeSession composition
    telephony_adapter = WebSocketTelephonyMediaAdapter(
        websocket=websocket,
        context=verified_context,
    )
    ai_port_factory: Callable[[], AIRealtimeMediaPort] | None = getattr(
        app_state, "ai_media_port_factory", None
    )
    ai_port: AIRealtimeMediaPort = (
        ai_port_factory() if ai_port_factory is not None else BoundedFakeAIEchoPort()
    )
    voice_bridge = VoiceBridgeSession(
        call_id=verified_context.call_id,
        tenant_id=verified_context.tenant_id,
        telephony_port=telephony_adapter,
        ai_port=ai_port,
    )

    # Register resource cleanup callback so supervisor owns all leg resources
    async def _cleanup_media_session() -> None:
        try:
            if active_call_registered:
                await auth_bridge.unregister_active_call(
                    connection_id, verified_context.tenant_id, verified_context.call_id
                )
        finally:
            try:
                await voice_bridge.close()
            except Exception:
                pass
            try:
                await telephony_adapter.close()
            except Exception:
                pass
            try:
                await ai_port.close()
            except Exception:
                pass
            accepted_flag = _is_wire_accepted()
            close_code = 1000 if accepted_flag else 1008
            close_reason = "Server shutting down" if accepted_flag else "Handshake aborted"
            await _close_socket(code=close_code, reason=close_reason)

    try:
        supervisor.register_resource(_cleanup_media_session)
    except Exception:
        try:
            await _close_socket(code=1011, reason="Lifecycle initialization failed")
        except Exception:
            pass
        await asyncio.shield(supervisor.close(reason=TerminalReason.STARTUP_FAILED))
        supervisor_closed = True
        return

    try:
        # 7. Register active call exclusivity
        try:
            await auth_bridge.register_active_call(
                connection_id, verified_context.tenant_id, verified_context.call_id
            )
            active_call_registered = True
        except IdentityAuthorizationError:
            try:
                await _close_socket(code=1008, reason="Call session already active")
            finally:
                await asyncio.shield(supervisor.close(reason=TerminalReason.STARTUP_FAILED))
                supervisor_closed = True
            return

        # 8. Accept the WebSocket connection (completing upgrade handshake)
        try:
            await websocket.accept()
            wire_accepted = True
        except Exception:
            try:
                await _close_socket(code=1011, reason="Handshake accept failed")
            finally:
                await asyncio.shield(supervisor.close(reason=TerminalReason.STARTUP_FAILED))
                supervisor_closed = True
            return

        # 9. Supervised session worker with single ingress read-loop
        idle_timeout = float(
            getattr(app_state, "synthetic_idle_timeout", DEFAULT_IDLE_TIMEOUT_SECONDS)
        )

        async def _media_session_worker() -> None:
            nonlocal socket_disconnected
            call_active = False
            call_started_once = False
            inbound_seq = 0

            try:
                # Send session.ready greeting
                ready_payload = {
                    "event": EVENT_SESSION_READY,
                    "protocol_version": SYNTHETIC_MEDIA_PROTOCOL_VERSION,
                    "connection_id": verified_context.connection_id,
                    "tenant_id": verified_context.tenant_id,
                    "call_id": verified_context.call_id,
                    "allowed_scopes": sorted(verified_context.allowed_scopes),
                }
                await telephony_adapter.send_control_text(
                    json.dumps(ready_payload, separators=(",", ":"))
                )

                # Single ingress read-loop demuxing control frames and binary audio
                while True:
                    try:
                        message = await asyncio.wait_for(websocket.receive(), timeout=idle_timeout)
                    except TimeoutError:
                        await _close_socket(code=1000, reason="Session idle timeout")
                        break

                    msg_type = message.get("type")
                    if msg_type == "websocket.disconnect":
                        socket_disconnected = True
                        break

                    # Case A: Binary media frame
                    if "bytes" in message and message["bytes"] is not None:
                        payload = message["bytes"]

                        # Verify call has been started before accepting audio
                        if not call_active:
                            telephony_adapter.record_reject()
                            reject_reason = (
                                "Binary media received after call.end"
                                if call_started_once
                                else "Binary media received before call.start"
                            )
                            await _close_socket(code=1008, reason=reject_reason)
                            break

                        # Validate frame size (exactly 640 bytes for PCM16LE 20ms)
                        if len(payload) != PCM16LE_FRAME_BYTES:
                            telephony_adapter.record_reject()
                            await _close_socket(
                                code=1008,
                                reason=(
                                    f"Invalid audio frame size: expected {PCM16LE_FRAME_BYTES} bytes, "
                                    f"got {len(payload)}"
                                ),
                            )
                            break

                        # Wrap in validated AudioFrame domain envelope
                        frame = AudioFrame(
                            payload=payload,
                            format=AudioFormat.PCM16LE,
                            sample_rate=PCM16LE_SAMPLE_RATE,
                            channels=PCM16LE_CHANNELS,
                            sequence=inbound_seq,
                            timestamp_ms=float(inbound_seq * PCM16LE_FRAME_MS),
                        )
                        inbound_seq += 1

                        # Push to telephony adapter's bounded inbound queue
                        await telephony_adapter.push_inbound_frame(frame)
                        continue

                    # Case B: Text control frame
                    text_data = message.get("text")
                    if text_data is None:
                        continue

                    try:
                        ctrl_payload = parse_and_validate_client_frame(text_data)
                    except SyntheticProtocolError as proto_err:
                        telephony_adapter.record_reject()
                        await _close_socket(code=proto_err.close_code, reason=str(proto_err))
                        break

                    event_name = ctrl_payload["event"]

                    if event_name == EVENT_SESSION_PING:
                        pong: dict[str, Any] = {"event": EVENT_SESSION_PONG}
                        if "seq" in ctrl_payload:
                            pong["seq"] = ctrl_payload["seq"]
                        await telephony_adapter.send_control_text(
                            json.dumps(pong, separators=(",", ":"))
                        )

                    elif event_name == EVENT_SESSION_END:
                        call_active = False
                        telephony_adapter.push_inbound_eof()
                        await _close_socket(code=1000, reason="Session ended by client")
                        break

                    elif event_name == EVENT_CALL_START:
                        if call_active:
                            telephony_adapter.record_reject()
                            await _close_socket(code=1008, reason="Duplicate call.start")
                            break

                        payload_call_id = ctrl_payload.get("call_id")
                        if (
                            payload_call_id is not None
                            and payload_call_id != verified_context.call_id
                        ):
                            telephony_adapter.record_reject()
                            await _close_socket(
                                code=1008,
                                reason="Call ID in payload does not match connection context",
                            )
                            break

                        call_active = True
                        call_started_once = True
                        # Start VoiceBridgeSession pumps
                        await voice_bridge.start()

                        started_response = {
                            "event": EVENT_CALL_STARTED,
                            "call_id": verified_context.call_id,
                        }
                        await telephony_adapter.send_control_text(
                            json.dumps(started_response, separators=(",", ":"))
                        )

                    elif event_name == EVENT_CALL_END:
                        if not call_active:
                            telephony_adapter.record_reject()
                            await _close_socket(
                                code=1008, reason="call.end received but call is not active"
                            )
                            break

                        payload_call_id = ctrl_payload.get("call_id")
                        if (
                            payload_call_id is not None
                            and payload_call_id != verified_context.call_id
                        ):
                            telephony_adapter.record_reject()
                            await _close_socket(
                                code=1008,
                                reason="Call ID in payload does not match connection context",
                            )
                            break

                        call_active = False
                        ended_response = {
                            "event": EVENT_CALL_ENDED,
                            "call_id": verified_context.call_id,
                        }
                        await telephony_adapter.send_control_text(
                            json.dumps(ended_response, separators=(",", ":"))
                        )
                        # Signal clean EOF to the inbound pump
                        telephony_adapter.push_inbound_eof()
                        if hasattr(ai_port, "push_eof"):
                            ai_port.push_eof()

                        # Await clean closure of the bridge with small timeout
                        try:
                            async with asyncio.timeout(2.0):
                                await voice_bridge.wait_until_closed(propagate_error=False)
                        except (TimeoutError, Exception):
                            pass

            except asyncio.CancelledError:
                raise
            except WebSocketDisconnect:
                socket_disconnected = True
            except Exception:
                try:
                    await _close_socket(code=1011, reason="Internal server error")
                except Exception:
                    pass
                raise
            finally:
                telephony_adapter.push_inbound_eof()
                if hasattr(ai_port, "push_eof"):
                    ai_port.push_eof()

        # 10. Start supervised worker and await its termination
        try:
            await supervisor.start(_media_session_worker)
        except asyncio.CancelledError:
            raise
        except Exception:
            try:
                await _close_socket(code=1011, reason="Worker startup failed")
            except Exception:
                pass
            await asyncio.shield(supervisor.close(reason=TerminalReason.STARTUP_FAILED))
            supervisor_closed = True
            raise

        await supervisor.wait_closed()

    except asyncio.CancelledError:
        if supervisor is not None and not supervisor_closed and not supervisor.state.is_terminal:
            await asyncio.shield(supervisor.close(reason=TerminalReason.CANCELLED))
            supervisor_closed = True
        raise
    finally:
        if supervisor is not None and not supervisor_closed:
            await asyncio.shield(supervisor.close(reason=TerminalReason.NORMAL))
