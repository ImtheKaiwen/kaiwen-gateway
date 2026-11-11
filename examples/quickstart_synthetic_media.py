"""Quickstart example: End-to-end synthetic binary audio media streaming.

Demonstrates:
1. Configuring and instantiating Kaiwen Gateway with synthetic media enabled.
2. Generating an HMAC-signed bearer token with `voice.stream` scope.
3. Connecting to the `/ws/synthetic-media/v1` binary WebSocket endpoint.
4. Sending and receiving standard 20ms PCM16LE audio frames (640 bytes).
5. Exercising turn-based barge-in interruption.
6. Clean call termination and resource recovery.

Requires:
    pip install "kaiwen-gateway[server]"

Usage:
    python examples/quickstart_synthetic_media.py
"""

from __future__ import annotations

import asyncio
import json
import secrets
import socket
import sys
import time

import uvicorn
import websockets

from kaiwen_gateway.core.admission import AdmissionController
from kaiwen_gateway.core.config import INGRESS_MODE_SYNTHETIC, CoreConfig
from kaiwen_gateway.core.draining import InstanceDrainCoordinator
from kaiwen_gateway.server.app import create_gateway_app
from kaiwen_gateway.transports.synthetic import (
    EVENT_CALL_END,
    EVENT_CALL_ENDED,
    EVENT_CALL_INTERRUPT,
    EVENT_CALL_INTERRUPTED,
    EVENT_CALL_START,
    EVENT_CALL_STARTED,
    EVENT_SESSION_READY,
    PCM16LE_FRAME_BYTES,
    SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
    SyntheticAuthBridge,
    SyntheticTokenVerifier,
)


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


async def main() -> None:
    print("--- Kaiwen Gateway v0.1.0a1 Synthetic Media Quickstart ---")

    # 1. Ephemeral credentials and gateway configuration
    secret_key = secrets.token_bytes(32)
    verifier = SyntheticTokenVerifier(secret_key=secret_key, enable_replay_protection=True)
    auth_bridge = SyntheticAuthBridge(verifier=verifier, enforce_call_exclusivity=True)

    config = CoreConfig(
        instance_name="quickstart-gateway",
        ingress_mode=INGRESS_MODE_SYNTHETIC,
        max_concurrent_connections=10,
        max_connections_per_tenant=5,
    )
    admission = AdmissionController(config)
    coordinator = InstanceDrainCoordinator(controller=admission)

    # 2. Build the ASGI application with synthetic media enabled
    app = create_gateway_app(
        config=config,
        admission_controller=admission,
        drain_coordinator=coordinator,
        synthetic_auth_bridge=auth_bridge,
        enable_synthetic_media=True,
    )

    port = _find_free_port()
    server_cfg = uvicorn.Config(
        app=app,
        host="127.0.0.1",
        port=port,
        log_level="error",
        ws="auto",
    )
    server = uvicorn.Server(server_cfg)
    server_task = asyncio.create_task(server.serve())

    # Wait for loopback server to start
    for _ in range(50):
        if server.started:
            break
        await asyncio.sleep(0.02)
    if not server.started:
        print("Error: Server failed to start on loopback.")
        sys.exit(1)

    print(f"[1] Gateway listening on http://127.0.0.1:{port}")

    # 3. Mint an authenticated token for tenant and call
    tenant_id = "quickstart-tenant"
    call_id = "quickstart-call-001"
    token = verifier.issue_token(
        provider_id="quickstart_provider",
        tenant_id=tenant_id,
        call_id=call_id,
        allowed_scopes=["voice.stream"],
        ttl_seconds=120,
    )
    print(f"[2] Minted bearer token for tenant={tenant_id!r}, call={call_id!r}")

    uri = f"ws://127.0.0.1:{port}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        # 4. Connect to the binary WebSocket endpoint
        print(f"[3] Connecting to {uri} ...")
        async with websockets.connect(uri, additional_headers=headers) as ws:
            # 5. Receive session.ready greeting
            ready_msg = json.loads(await ws.recv())
            print(
                f"[4] Received greeting: event={ready_msg.get('event')!r}, call_id={ready_msg.get('call_id')!r}"
            )
            assert ready_msg.get("event") == EVENT_SESSION_READY

            # 6. Start the call
            await ws.send(json.dumps({"event": EVENT_CALL_START, "call_id": call_id}))
            started_msg = json.loads(await ws.recv())
            print(f"[5] Call started: event={started_msg.get('event')!r}")
            assert started_msg.get("event") == EVENT_CALL_STARTED

            # 7. Stream 10 binary PCM16LE audio frames and verify echo
            print("[6] Streaming 10 audio frames (200ms total, 640 bytes/frame) ...")
            sent_frames: list[bytes] = []
            received_frames: list[bytes] = []

            for seq in range(10):
                prefix = f"AUDIO_{seq:04d}_".encode("ascii")
                payload = prefix + b"\x00" * (PCM16LE_FRAME_BYTES - len(prefix))
                sent_frames.append(payload)

            async def send_all() -> None:
                for f in sent_frames:
                    await ws.send(f)
                    await asyncio.sleep(0.010)

            async def recv_all() -> None:
                while len(received_frames) < 10:
                    resp = await ws.recv()
                    if isinstance(resp, bytes):
                        received_frames.append(resp)

            t0 = time.perf_counter()
            await asyncio.gather(send_all(), recv_all())
            elapsed = time.perf_counter() - t0
            print(f"    Streamed and received 10 frames in {elapsed * 1000:.1f} ms.")

            for i, (sf, rf) in enumerate(zip(sent_frames, received_frames, strict=False)):
                assert sf == rf, f"Frame {i} content mismatch"
            print("    Byte integrity and sequence ordering verified: OK.")

            # 8. Simulate barge-in interruption (Turn 1 -> Turn 2)
            print("[7] Simulating barge-in interruption ...")
            interrupt_payload = {
                "event": EVENT_CALL_INTERRUPT,
                "call_id": call_id,
                "turn_id": 1,
                "request_id": "quickstart-interrupt-01",
            }
            await ws.send(json.dumps(interrupt_payload))
            interrupted_msg = json.loads(await ws.recv())
            print(
                f"    Interruption acknowledged: event={interrupted_msg.get('event')!r}, "
                f"new_turn_id={interrupted_msg.get('new_turn_id')}"
            )
            assert interrupted_msg.get("event") == EVENT_CALL_INTERRUPTED

            # Send Turn 2 frame
            t2_prefix = b"AUDIO_T2_0001_"
            t2_frame = t2_prefix + b"\x55" * (PCM16LE_FRAME_BYTES - len(t2_prefix))
            await ws.send(t2_frame)
            t2_echo = await ws.recv()
            assert t2_echo == t2_frame
            print("    Turn 2 frame streamed and echoed cleanly: OK.")

            # 9. Cleanly end call
            print("[8] Ending call ...")
            await ws.send(json.dumps({"event": EVENT_CALL_END, "call_id": call_id}))
            ended_msg = json.loads(await ws.recv())
            print(f"    Call ended: event={ended_msg.get('event')!r}")
            assert ended_msg.get("event") == EVENT_CALL_ENDED

    finally:
        server.should_exit = True
        try:
            await asyncio.wait_for(server_task, timeout=3.0)
        except (TimeoutError, asyncio.CancelledError):
            pass

    snap = admission.snapshot()
    print(f"[9] Capacity recovered: active_connections={snap.active_connections}")
    print("\n--- Synthetic Media Quickstart Completed Successfully (PASS) ---")


if __name__ == "__main__":
    asyncio.run(main())
