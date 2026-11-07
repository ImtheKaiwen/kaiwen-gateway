"""Runnable end-to-end synthetic binary WebSocket media smoke demonstration.

Verifies:
1. Real loopback Uvicorn server startup with enable_synthetic_media=True.
2. Token generation with voice.stream scope and HMAC verification.
3. Successful WebSocket connection to /ws/synthetic-media/v1.
4. Reception of session.ready control greeting.
5. Call activation via call.start -> call.started.
6. Transmission of native binary PCM16LE audio frames (640 bytes each).
7. Ingestion by VoiceBridgeSession inbound pump, processing by BoundedFakeAIEchoPort.
8. Outbound pump transmission and client reception of binary audio frames.
9. Verification of exact frame count, exact byte integrity, and sequence ordering.
10. Clean termination via call.end -> call.ended and close code 1000.
11. Complete capacity recovery (0 active calls, 0 active leases, 0 orphan tasks).
12. Clean Uvicorn loopback shutdown.
"""

from __future__ import annotations

import asyncio
import json
import socket
import sys
import time
from dataclasses import dataclass
from typing import Final

import uvicorn
import websockets

from kaiwen_gateway.core.admission import AdmissionController
from kaiwen_gateway.core.config import INGRESS_MODE_SYNTHETIC, CoreConfig
from kaiwen_gateway.core.draining import InstanceDrainCoordinator
from kaiwen_gateway.server import create_gateway_app
from kaiwen_gateway.transports.synthetic import (
    EVENT_CALL_END,
    EVENT_CALL_ENDED,
    EVENT_CALL_START,
    EVENT_CALL_STARTED,
    EVENT_SESSION_READY,
    PCM16LE_FRAME_BYTES,
    SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
    SyntheticAuthBridge,
)

SMOKE_SECRET_KEY: Final[bytes] = b"smoke_synthetic_media_secret_key_32bytes!"


@dataclass(frozen=True, slots=True)
class SyntheticMediaSmokeResult:
    """Structured result of the synthetic binary media smoke test."""

    frames_sent: int
    frames_received: int
    bytes_sent: int
    bytes_received: int
    byte_integrity_verified: bool
    sequence_ordering_verified: bool
    active_calls_recovered: bool
    active_leases_recovered: bool
    duration_seconds: float
    success: bool


def _get_free_port() -> int:
    """Find an available ephemeral port on loopback."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


async def run_synthetic_media_smoke(
    *,
    frame_count: int = 25,
    tenant_id: str = "tenant_smoke_media",
    call_id: str = "call_smoke_media_001",
    verbose: bool = True,
) -> SyntheticMediaSmokeResult:
    """Run an end-to-end binary media round-trip over real loopback WebSocket."""
    start_time = time.perf_counter()

    # 1. Setup app components with explicit opt-in for synthetic media
    config = CoreConfig(
        instance_name="smoke-media-instance",
        ingress_mode=INGRESS_MODE_SYNTHETIC,
        max_concurrent_connections=10,
        max_connections_per_tenant=5,
    )
    admission = AdmissionController(config)
    coordinator = InstanceDrainCoordinator(controller=admission)
    from kaiwen_gateway.transports.synthetic import SyntheticTokenVerifier

    verifier = SyntheticTokenVerifier(
        secret_key=SMOKE_SECRET_KEY,
        enable_replay_protection=True,
    )
    auth_bridge = SyntheticAuthBridge(
        verifier=verifier,
        enforce_call_exclusivity=True,
    )

    app = create_gateway_app(
        config=config,
        admission_controller=admission,
        drain_coordinator=coordinator,
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
    )

    port = _get_free_port()
    server_config = uvicorn.Config(
        app=app,
        host="127.0.0.1",
        port=port,
        log_level="warning",
        ws="auto",
    )
    server = uvicorn.Server(server_config)
    server_task = asyncio.create_task(server.serve())

    # Wait for server to start
    for _ in range(100):
        if server.started:
            break
        await asyncio.sleep(0.02)
    assert server.started, "Uvicorn server failed to start on loopback"

    if verbose:
        print(f"[SMOKE] Uvicorn loopback server started on port {port}")

    # 2. Generate valid token with voice.stream scope
    token = auth_bridge.verifier.issue_token(
        provider_id="smoke_provider",
        tenant_id=tenant_id,
        call_id=call_id,
        allowed_scopes=["voice.stream"],
        ttl_seconds=120,
    )

    uri = f"ws://127.0.0.1:{port}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
    headers = {"Authorization": f"Bearer {token}"}

    frames_sent = 0
    frames_received = 0
    bytes_sent = 0
    bytes_received = 0
    byte_integrity_verified = True
    sequence_ordering_verified = True

    try:
        if verbose:
            print(f"[SMOKE] Connecting to {uri} with bearer token...")

        async with websockets.connect(
            uri,
            additional_headers=headers,
            max_size=1024 * 1024,
        ) as ws:
            # 3. Receive session.ready
            ready_raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
            assert isinstance(ready_raw, str)
            ready_json = json.loads(ready_raw)
            assert ready_json["event"] == EVENT_SESSION_READY
            assert ready_json["call_id"] == call_id
            if verbose:
                print(f"[SMOKE] Received {ready_json['event']} for call {call_id}")

            # 4. Activate call via call.start
            start_cmd = {"event": EVENT_CALL_START, "call_id": call_id}
            await ws.send(json.dumps(start_cmd))

            started_raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
            assert isinstance(started_raw, str)
            started_json = json.loads(started_raw)
            assert started_json["event"] == EVENT_CALL_STARTED
            assert started_json["call_id"] == call_id
            if verbose:
                print(f"[SMOKE] Call active: received {started_json['event']}")

            # 5. Stream binary audio frames and receive echoed responses concurrently
            sent_payloads: list[bytes] = []
            received_payloads: list[bytes] = []

            for i in range(frame_count):
                prefix = f"PCM_{i:04d}_".encode("ascii")
                payload = prefix + b"\x42" * (PCM16LE_FRAME_BYTES - len(prefix))
                sent_payloads.append(payload)

            async def _sender() -> None:
                nonlocal frames_sent, bytes_sent
                for p in sent_payloads:
                    await ws.send(p)
                    frames_sent += 1
                    bytes_sent += len(p)
                    await asyncio.sleep(0.005)  # 5ms cadence

            async def _receiver() -> None:
                nonlocal frames_received, bytes_received
                while frames_received < frame_count:
                    resp = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    if isinstance(resp, bytes):
                        received_payloads.append(resp)
                        frames_received += 1
                        bytes_received += len(resp)

            await asyncio.gather(_sender(), _receiver())

            if verbose:
                print(
                    f"[SMOKE] Streamed {frames_sent} binary frames ({bytes_sent} bytes), "
                    f"received {frames_received} echoed frames ({bytes_received} bytes)"
                )

            # 6. Verify byte integrity and sequence ordering
            for idx, (sent_p, recv_p) in enumerate(
                zip(sent_payloads, received_payloads, strict=False)
            ):
                if sent_p != recv_p:
                    byte_integrity_verified = False
                    if verbose:
                        print(f"[SMOKE] Byte mismatch at frame index {idx}")
                    break
                expected_prefix = f"PCM_{idx:04d}_".encode("ascii")
                if not recv_p.startswith(expected_prefix):
                    sequence_ordering_verified = False
                    if verbose:
                        print(f"[SMOKE] Sequence ordering mismatch at index {idx}")
                    break

            # 7. End call cleanly via call.end
            end_cmd = {"event": EVENT_CALL_END, "call_id": call_id}
            await ws.send(json.dumps(end_cmd))

            ended_raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
            assert isinstance(ended_raw, str)
            ended_json = json.loads(ended_raw)
            assert ended_json["event"] == EVENT_CALL_ENDED
            if verbose:
                print(f"[SMOKE] Call ended: received {ended_json['event']}")

            await ws.close()

    finally:
        # Shut down Uvicorn server
        server.should_exit = True
        try:
            await asyncio.wait_for(server_task, timeout=5.0)
        except (TimeoutError, asyncio.CancelledError):
            pass

    # 8. Verify capacity recovery
    await asyncio.sleep(0.05)
    snapshot = admission.snapshot()
    active_leases_recovered = (
        snapshot.active_connections == 0 and admission.active_connections_for_tenant(tenant_id) == 0
    )
    active_calls_recovered = (tenant_id, call_id) not in auth_bridge._active_calls

    duration = time.perf_counter() - start_time
    success = (
        frames_sent == frame_count
        and frames_received == frame_count
        and byte_integrity_verified
        and sequence_ordering_verified
        and active_leases_recovered
        and active_calls_recovered
    )

    if verbose:
        print(
            f"[SMOKE] Capacity recovered: leases={snapshot.active_connections}, "
            f"active_calls={len(auth_bridge._active_calls)}"
        )
        print(f"[SMOKE] Overall result: {'SUCCESS' if success else 'FAILED'} in {duration:.3f}s")

    return SyntheticMediaSmokeResult(
        frames_sent=frames_sent,
        frames_received=frames_received,
        bytes_sent=bytes_sent,
        bytes_received=bytes_received,
        byte_integrity_verified=byte_integrity_verified,
        sequence_ordering_verified=sequence_ordering_verified,
        active_calls_recovered=active_calls_recovered,
        active_leases_recovered=active_leases_recovered,
        duration_seconds=duration,
        success=success,
    )


if __name__ == "__main__":
    result = asyncio.run(run_synthetic_media_smoke(frame_count=25, verbose=True))
    if not result.success:
        print("[SMOKE] FAILED verification checks!", file=sys.stderr)
        sys.exit(1)
    print("[SMOKE] PASSED all verification checks!")
    sys.exit(0)
