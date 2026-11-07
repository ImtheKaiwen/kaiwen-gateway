"""WebSocket endpoint handling inbound synthetic binary media connections for Kaiwen Gateway."""

from __future__ import annotations

import asyncio
import json
import math
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
from kaiwen_gateway.modules.voice.adapters.fake_ai import BoundedInterruptibleFakeAIEchoPort
from kaiwen_gateway.modules.voice.bridge import VoiceBridgeSession, VoiceBridgeState
from kaiwen_gateway.modules.voice.domain import (
    AudioFormat,
    AudioFrame,
    UnsupportedInterruptionCapabilityError,
    VoiceDomainError,
)
from kaiwen_gateway.modules.voice.ports import AIRealtimeMediaPort
from kaiwen_gateway.transports.synthetic.auth import SyntheticAuthBridge
from kaiwen_gateway.transports.synthetic.media_adapter import WebSocketTelephonyMediaAdapter
from kaiwen_gateway.transports.synthetic.media_protocol import (
    EVENT_CALL_INTERRUPT,
    EVENT_CALL_INTERRUPTED,
    PCM16LE_CHANNELS,
    PCM16LE_FRAME_BYTES,
    PCM16LE_FRAME_MS,
    PCM16LE_SAMPLE_RATE,
    SYNTHETIC_MEDIA_PROTOCOL_VERSION,
    parse_and_validate_media_control_frame,
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
)

try:
    from starlette.websockets import WebSocket, WebSocketDisconnect

    _HAS_STARLETTE = True
except ImportError:  # pragma: no cover
    _HAS_STARLETTE = False
    WebSocket = Any  # type: ignore[assignment,misc]
    WebSocketDisconnect = Any  # type: ignore[assignment,misc]

DEFAULT_IDLE_TIMEOUT_SECONDS: Final[float] = 60.0
DEFAULT_DRAIN_TIMEOUT_SECONDS: Final[float] = 2.0
REQUIRED_SYNTHETIC_SCOPE: Final[str] = "voice.stream"


def _resolve_positive_float(value: Any, default: float) -> float:
    """Resolve a timeout setting to a positive finite float, falling back to default."""
    if value is None or isinstance(value, bool):
        return default
    try:
        val = float(value)
        if not math.isfinite(val) or val <= 0.0:
            return default
        return val
    except (TypeError, ValueError):
        return default


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
        try:
            await websocket.close(code=code, reason=safe_reason)
        except Exception:
            pass
        finally:
            socket_closed = True

    telephony_adapter: WebSocketTelephonyMediaAdapter | None = None
    ai_port: AIRealtimeMediaPort | None = None
    voice_bridge: VoiceBridgeSession | None = None

    # Register resource cleanup callback so supervisor owns all leg resources
    async def _cleanup_media_session() -> None:
        nonlocal active_call_registered
        cleanup_errors: list[Exception] = []
        if active_call_registered:
            try:
                await auth_bridge.unregister_active_call(
                    connection_id, verified_context.tenant_id, verified_context.call_id
                )
            except Exception as exc:
                cleanup_errors.append(exc)
            active_call_registered = False

        if voice_bridge is not None:
            if voice_bridge.state == VoiceBridgeState.FAILED:
                err = voice_bridge.error
                if isinstance(err, Exception):
                    cleanup_errors.append(err)
                else:
                    cleanup_errors.append(
                        VoiceDomainError(f"Voice bridge ended in FAILED state: {err}")
                    )
            try:
                await voice_bridge.close()
            except Exception as exc:
                cleanup_errors.append(exc)

            if voice_bridge.state == VoiceBridgeState.FAILED:
                err = voice_bridge.error
                if isinstance(err, Exception) and err not in cleanup_errors:
                    cleanup_errors.append(err)
                elif err is None and not cleanup_errors:
                    cleanup_errors.append(VoiceDomainError("Voice bridge ended in FAILED state"))

        if telephony_adapter is not None:
            try:
                await telephony_adapter.close()
            except Exception as exc:
                cleanup_errors.append(exc)

        if ai_port is not None:
            try:
                await ai_port.close()
            except Exception as exc:
                cleanup_errors.append(exc)

        try:
            accepted_flag = _is_wire_accepted()
            is_failed = bool(
                (voice_bridge is not None and voice_bridge.state == VoiceBridgeState.FAILED)
                or bool(cleanup_errors)
            )
            close_code = 1011 if (not accepted_flag or is_failed) else 1000
            close_reason = (
                "Internal server error"
                if is_failed
                else ("Server shutting down" if accepted_flag else "Initialization failed")
            )
            await _close_socket(code=close_code, reason=close_reason)
        except Exception as exc:
            cleanup_errors.append(exc)

        if cleanup_errors:
            if len(cleanup_errors) == 1:
                raise cleanup_errors[0]
            raise ExceptionGroup("Media session cleanup failed", cleanup_errors)

    try:
        supervisor.register_resource(_cleanup_media_session)

        # Telephony adapter, AI port, and VoiceBridgeSession composition
        telephony_adapter = WebSocketTelephonyMediaAdapter(
            websocket=websocket,
            context=verified_context,
        )
        ai_port_factory: Callable[[], AIRealtimeMediaPort] | None = getattr(
            app_state, "ai_media_port_factory", None
        )
        ai_port = (
            ai_port_factory()
            if ai_port_factory is not None
            else BoundedInterruptibleFakeAIEchoPort()
        )
        telemetry_sink = getattr(app_state, "voice_telemetry_sink", None)
        voice_bridge = VoiceBridgeSession(
            call_id=verified_context.call_id,
            tenant_id=verified_context.tenant_id,
            telephony_port=telephony_adapter,
            ai_port=ai_port,
            telemetry_sink=telemetry_sink,
        )
    except Exception:
        try:
            await _cleanup_media_session()
        except Exception:
            pass
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
        idle_timeout = _resolve_positive_float(
            getattr(app_state, "synthetic_idle_timeout", None),
            DEFAULT_IDLE_TIMEOUT_SECONDS,
        )
        drain_timeout = _resolve_positive_float(
            getattr(app_state, "synthetic_drain_timeout", None),
            DEFAULT_DRAIN_TIMEOUT_SECONDS,
        )

        async def _media_session_worker() -> None:
            nonlocal socket_disconnected
            assert telephony_adapter is not None
            assert ai_port is not None
            assert voice_bridge is not None

            call_active = False
            call_started_once = False
            inbound_seq = 0
            ingress_turn_id = 1
            receive_task: asyncio.Task[Any] | None = None
            bridge_waiter_task: asyncio.Task[None] | None = None

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
                    if receive_task is None or receive_task.done():
                        receive_task = asyncio.create_task(websocket.receive())

                    tasks_to_wait: list[asyncio.Task[Any]] = [receive_task]
                    if bridge_waiter_task is not None and not bridge_waiter_task.done():
                        tasks_to_wait.append(bridge_waiter_task)

                    done, _ = await asyncio.wait(
                        tasks_to_wait,
                        timeout=idle_timeout,
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    if not done:
                        await _close_socket(code=1000, reason="Session idle timeout")
                        break

                    # 1. Proactive observation of upstream bridge failure
                    if bridge_waiter_task is not None and bridge_waiter_task in done:
                        if voice_bridge.state == VoiceBridgeState.FAILED:
                            await _close_socket(
                                code=1011,
                                reason="Upstream media bridge failed",
                            )
                            break
                        if call_active and voice_bridge.state == VoiceBridgeState.CLOSED:
                            await _close_socket(
                                code=1011,
                                reason="Upstream media bridge closed prematurely",
                            )
                            break

                    # 2. Check if receive_task completed
                    if receive_task not in done:
                        continue

                    try:
                        message = receive_task.result()
                    except WebSocketDisconnect:
                        socket_disconnected = True
                        break
                    except Exception:
                        await _close_socket(code=1011, reason="Transport receive error")
                        break
                    finally:
                        receive_task = None

                    # If the bridge has failed, reject further traffic immediately
                    if call_started_once and voice_bridge.state == VoiceBridgeState.FAILED:
                        await _close_socket(
                            code=1011,
                            reason="Upstream media bridge failed",
                        )
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
                                code=1003,
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
                            turn_id=ingress_turn_id,
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
                        ctrl_payload = parse_and_validate_media_control_frame(text_data)
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
                        if bridge_waiter_task is not None and not bridge_waiter_task.done():
                            bridge_waiter_task.cancel()
                            try:
                                await bridge_waiter_task
                            except (asyncio.CancelledError, Exception):
                                pass
                            bridge_waiter_task = None
                        telephony_adapter.push_inbound_eof_nowait()
                        if hasattr(ai_port, "push_eof_nowait"):
                            ai_port.push_eof_nowait()
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
                        bridge_waiter_task = asyncio.create_task(
                            voice_bridge.wait_until_closed(propagate_error=False),
                            name=f"voice-bridge-waiter-{verified_context.call_id}",
                        )

                        started_response = {
                            "event": EVENT_CALL_STARTED,
                            "call_id": verified_context.call_id,
                        }
                        await telephony_adapter.send_control_text(
                            json.dumps(started_response, separators=(",", ":"))
                        )

                    elif event_name == EVENT_CALL_INTERRUPT:
                        if not call_active:
                            telephony_adapter.record_reject()
                            await _close_socket(
                                code=1008,
                                reason="call.interrupt received but call is not active",
                            )
                            break

                        payload_call_id = ctrl_payload.get("call_id")
                        if payload_call_id != verified_context.call_id:
                            telephony_adapter.record_reject()
                            await _close_socket(
                                code=1008,
                                reason="Call ID in payload does not match connection context",
                            )
                            break

                        target_turn_id: int = ctrl_payload["turn_id"]

                        # Future turn rejection
                        if target_turn_id > voice_bridge.current_turn_id:
                            telephony_adapter.record_reject()
                            await _close_socket(
                                code=1008,
                                reason=(
                                    f"Cannot interrupt future turn {target_turn_id} "
                                    f"(current turn is {voice_bridge.current_turn_id})"
                                ),
                            )
                            break

                        # Stale / superseded turn: return idempotent response without advancing turn
                        if target_turn_id < voice_bridge.current_turn_id:
                            stale_response: dict[str, Any] = {
                                "event": EVENT_CALL_INTERRUPTED,
                                "call_id": verified_context.call_id,
                                "interrupted_turn_id": target_turn_id,
                                "new_turn_id": voice_bridge.current_turn_id,
                                "locally_discarded_frames": 0,
                                "stale_frames_suppressed": voice_bridge.stale_frames_suppressed,
                                "future_generation_cancelled": False,
                                "transport_pending_cancelled": False,
                                "provider_playback_cleared": False,
                                "playback_confirmed": False,
                            }
                            if "request_id" in ctrl_payload:
                                stale_response["request_id"] = ctrl_payload["request_id"]
                            await telephony_adapter.send_control_text(
                                json.dumps(stale_response, separators=(",", ":"))
                            )
                            continue

                        # Current active turn interruption: engage outbound ordering barrier
                        telephony_adapter.hold_outbound_barrier()
                        outcome = None
                        try:
                            outcome = await voice_bridge.interrupt(
                                turn_id=target_turn_id,
                                require_future_generation_cancellation=True,
                                timeout=2.0,
                            )
                        except UnsupportedInterruptionCapabilityError:
                            telephony_adapter.record_reject()
                            await _close_socket(
                                code=1011,
                                reason="Upstream media interruption failed",
                            )
                            break
                        except Exception:
                            telephony_adapter.record_reject()
                            await _close_socket(
                                code=1011,
                                reason="Upstream media interruption failed",
                            )
                            break
                        finally:
                            if outcome is None:
                                telephony_adapter.release_outbound_barrier(
                                    voice_bridge.current_turn_id
                                )

                        # Update active ingress turn tracking
                        ingress_turn_id = outcome.new_turn_id

                        # Emit call.interrupted response BEFORE lifting outbound barrier
                        interrupted_response: dict[str, Any] = {
                            "event": EVENT_CALL_INTERRUPTED,
                            "call_id": verified_context.call_id,
                            "interrupted_turn_id": outcome.interrupted_turn_id,
                            "new_turn_id": outcome.new_turn_id,
                            "locally_discarded_frames": outcome.locally_discarded_frames,
                            "stale_frames_suppressed": outcome.stale_frames_suppressed,
                            "future_generation_cancelled": outcome.future_generation_cancelled,
                            "transport_pending_cancelled": outcome.transport_pending_cancelled,
                            "provider_playback_cleared": outcome.provider_playback_cleared,
                            "playback_confirmed": False,
                        }
                        if "request_id" in ctrl_payload:
                            interrupted_response["request_id"] = ctrl_payload["request_id"]

                        await telephony_adapter.send_control_text(
                            json.dumps(interrupted_response, separators=(",", ":"))
                        )

                        # Release barrier so newly generated frames may be transmitted
                        telephony_adapter.release_outbound_barrier(outcome.new_turn_id)
                        continue

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

                        # 1. Stop new inbound audio acceptance
                        call_active = False

                        # Stop bridge waiter task so drain sequence owns waiting for bridge close
                        if bridge_waiter_task is not None and not bridge_waiter_task.done():
                            bridge_waiter_task.cancel()
                            try:
                                await bridge_waiter_task
                            except (asyncio.CancelledError, Exception):
                                pass
                            bridge_waiter_task = None

                        # 2-6. Lossless bounded drain sequence
                        drain_success = False
                        try:
                            # 2. Push clean EOF to telephony inbound queue (FIFO, waits if full)
                            await telephony_adapter.push_inbound_eof(timeout=drain_timeout)

                            # 3. Wait until the inbound bridge leg has completed (telephony -> AI)
                            async with asyncio.timeout(drain_timeout):
                                while not voice_bridge.inbound_completed:
                                    if voice_bridge.state in (
                                        VoiceBridgeState.CLOSED,
                                        VoiceBridgeState.FAILED,
                                    ):
                                        break
                                    await asyncio.sleep(0.005)

                            if voice_bridge.state == VoiceBridgeState.FAILED:
                                raise VoiceDomainError(
                                    f"Voice bridge failed during inbound drain: {voice_bridge.error}"
                                )

                            # 4. Signal EOF to AI port now that all inbound frames have reached AI
                            if hasattr(ai_port, "push_eof"):
                                eof_fn = ai_port.push_eof
                                if asyncio.iscoroutinefunction(eof_fn):
                                    await eof_fn(timeout=drain_timeout)
                                else:
                                    res = eof_fn()
                                    if asyncio.iscoroutine(res):
                                        await res

                            # 5-6. Wait for outbound pump to finish emitting all echoed frames
                            async with asyncio.timeout(drain_timeout):
                                await voice_bridge.wait_until_closed(propagate_error=True)

                            if voice_bridge.state == VoiceBridgeState.CLOSED:
                                drain_success = True
                            else:
                                raise VoiceDomainError(
                                    f"Voice bridge ended in state {voice_bridge.state}"
                                )

                        except (TimeoutError, Exception):
                            drain_success = False

                        if drain_success:
                            # 7. Send successful terminal acknowledgment
                            ended_response = {
                                "event": EVENT_CALL_ENDED,
                                "call_id": verified_context.call_id,
                            }
                            await telephony_adapter.send_control_text(
                                json.dumps(ended_response, separators=(",", ":")),
                                allow_when_closed=True,
                            )
                            # 8. Server normally closes connection and releases resources
                            await _close_socket(code=1000, reason="Call ended normally")
                        else:
                            # Failed drain: do NOT send call.ended; close with error
                            await _close_socket(code=1011, reason="Call drain failed or timed out")
                        break

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
                if receive_task is not None and not receive_task.done():
                    receive_task.cancel()
                    try:
                        await receive_task
                    except (asyncio.CancelledError, WebSocketDisconnect, Exception):
                        pass
                if bridge_waiter_task is not None and not bridge_waiter_task.done():
                    bridge_waiter_task.cancel()
                    try:
                        await bridge_waiter_task
                    except (asyncio.CancelledError, Exception):
                        pass
                if telephony_adapter is not None:
                    telephony_adapter.push_inbound_eof_nowait()
                if ai_port is not None and hasattr(ai_port, "push_eof_nowait"):
                    ai_port.push_eof_nowait()

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
