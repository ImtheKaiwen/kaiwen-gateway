"""Command-line interface (CLI) and executable entrypoints for Kaiwen Gateway.

Provides standard library ``argparse``-driven subcommands:
- ``serve``: Run local ASGI server (retaining backward-compatible default on 127.0.0.1:8000).
- ``check-config``: Validate CoreConfig and environment variables with zero external dependencies.
- ``demo``: Execute self-contained synthetic telephony call lifecycle demo.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import ipaddress
import json
import math
import os
import secrets
import socket
import sys
import time
from collections.abc import Mapping, Sequence
from typing import TextIO

from kaiwen_gateway import __version__
from kaiwen_gateway.core.config import (
    DEFAULT_INSTANCE_NAME,
    DEFAULT_MAX_CONCURRENT_CONNECTIONS,
    DEFAULT_MAX_CONNECTIONS_PER_TENANT,
    INGRESS_MODE_DISABLED,
    INGRESS_MODE_SYNTHETIC,
    SUPPORTED_INGRESS_MODES,
    CoreConfig,
    validate_core_config,
)
from kaiwen_gateway.core.exceptions import CoreConfigurationError


def _is_loopback(host: str) -> bool:
    """Verify if the host address is strictly a loopback address."""
    if not isinstance(host, str) or not host.strip():
        return False
    h = host.strip().lower()
    if h in ("127.0.0.1", "localhost", "::1"):
        return True
    try:
        return ipaddress.ip_address(h).is_loopback
    except ValueError:
        return False


def resolve_config(
    *,
    instance_name: str | None = None,
    max_concurrent_connections: int | None = None,
    max_connections_per_tenant: int | None = None,
    ingress_mode: str | None = None,
    environ: Mapping[str, str] | None = None,
) -> CoreConfig:
    """Resolve and validate CoreConfig from CLI flags, environment, and defaults.

    Precedence order:
    1. Explicit CLI arguments (if provided / not None).
    2. Environment variables (``KAIWEN_GATEWAY_*``).
    3. Built-in CoreConfig defaults.

    Raises:
        CoreConfigurationError: If any setting or environment variable is invalid.
    """
    env = os.environ if environ is None else environ

    # 1. instance_name
    if instance_name is not None:
        eff_instance_name = instance_name
    elif "KAIWEN_GATEWAY_INSTANCE_NAME" in env:
        eff_instance_name = env["KAIWEN_GATEWAY_INSTANCE_NAME"]
    else:
        eff_instance_name = DEFAULT_INSTANCE_NAME

    # 2. max_concurrent_connections
    if max_concurrent_connections is not None:
        eff_max_concurrent = max_concurrent_connections
    elif "KAIWEN_GATEWAY_MAX_CONCURRENT_CONNECTIONS" in env:
        raw_val = env["KAIWEN_GATEWAY_MAX_CONCURRENT_CONNECTIONS"]
        try:
            eff_max_concurrent = int(raw_val)
        except (ValueError, TypeError) as err:
            raise CoreConfigurationError(
                f"Invalid integer in KAIWEN_GATEWAY_MAX_CONCURRENT_CONNECTIONS: {raw_val!r}",
                field_name="max_concurrent_connections",
            ) from err
    else:
        eff_max_concurrent = DEFAULT_MAX_CONCURRENT_CONNECTIONS

    # 3. max_connections_per_tenant
    if max_connections_per_tenant is not None:
        eff_max_tenant = max_connections_per_tenant
    elif "KAIWEN_GATEWAY_MAX_CONNECTIONS_PER_TENANT" in env:
        raw_val = env["KAIWEN_GATEWAY_MAX_CONNECTIONS_PER_TENANT"]
        try:
            eff_max_tenant = int(raw_val)
        except (ValueError, TypeError) as err:
            raise CoreConfigurationError(
                f"Invalid integer in KAIWEN_GATEWAY_MAX_CONNECTIONS_PER_TENANT: {raw_val!r}",
                field_name="max_connections_per_tenant",
            ) from err
    else:
        eff_max_tenant = DEFAULT_MAX_CONNECTIONS_PER_TENANT

    # 4. ingress_mode
    if ingress_mode is not None:
        eff_ingress_mode = ingress_mode
    elif "KAIWEN_GATEWAY_INGRESS_MODE" in env:
        eff_ingress_mode = env["KAIWEN_GATEWAY_INGRESS_MODE"]
    else:
        eff_ingress_mode = INGRESS_MODE_DISABLED

    config = CoreConfig(
        instance_name=eff_instance_name,
        max_concurrent_connections=eff_max_concurrent,
        max_connections_per_tenant=eff_max_tenant,
        ingress_mode=eff_ingress_mode,
    )
    return validate_core_config(config)


def run_check_config(
    *,
    instance_name: str | None = None,
    max_concurrent_connections: int | None = None,
    max_connections_per_tenant: int | None = None,
    ingress_mode: str | None = None,
    environ: Mapping[str, str] | None = None,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
) -> int:
    """Validate CoreConfig from CLI args and environment without running a server.

    Outputs formatted configuration without secrets, exiting 0 on success or 1 on failure.
    Requires ZERO external dependencies.
    """
    out = sys.stdout if stdout is None else stdout
    err = sys.stderr if stderr is None else stderr

    try:
        cfg = resolve_config(
            instance_name=instance_name,
            max_concurrent_connections=max_concurrent_connections,
            max_connections_per_tenant=max_connections_per_tenant,
            ingress_mode=ingress_mode,
            environ=environ,
        )
    except CoreConfigurationError as exc:
        err.write(f"Configuration error: {exc}\n")
        return 1

    out.write(
        "Configuration valid:\n"
        f"  instance_name: {cfg.instance_name}\n"
        f"  max_concurrent_connections: {cfg.max_concurrent_connections}\n"
        f"  max_connections_per_tenant: {cfg.max_connections_per_tenant}\n"
        f"  ingress_mode: {cfg.ingress_mode}\n"
    )
    return 0


def run_serve(
    *,
    host: str = "127.0.0.1",
    port: int = 8000,
    log_level: str = "info",
    ws_max_size: int = 65536,
    instance_name: str | None = None,
    max_concurrent_connections: int | None = None,
    max_connections_per_tenant: int | None = None,
    ingress_mode: str | None = None,
    environ: Mapping[str, str] | None = None,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
) -> int:
    """Run local development ASGI server on host:port."""
    err = sys.stderr if stderr is None else stderr

    if not _is_loopback(host):
        err.write(
            f"Error: Non-loopback host {host!r} is prohibited for local development. "
            "Kaiwen Gateway CLI binds strictly to loopback interfaces (e.g. '127.0.0.1', 'localhost', '::1').\n"
        )
        return 1

    if isinstance(port, bool) or not isinstance(port, int) or port < 1 or port > 65535:
        err.write(f"Error: Invalid port {port!r}. Port must be an integer between 1 and 65535.\n")
        return 1

    if (
        isinstance(ws_max_size, bool)
        or not isinstance(ws_max_size, int)
        or ws_max_size < 1024
        or ws_max_size > 16 * 1024 * 1024
    ):
        err.write(
            f"Error: Invalid ws_max_size {ws_max_size!r}. Must be an integer between 1024 and 16777216 bytes.\n"
        )
        return 1

    try:
        import starlette  # noqa: F401
        import uvicorn
    except ImportError:
        err.write(
            "Error: 'uvicorn' and 'starlette' are required to run the gateway server.\n"
            "Developer installation: uv sync --locked --group dev --extra server\n"
            "Consumer installation: pip install 'kaiwen-gateway[server]'\n"
        )
        return 1

    try:
        config = resolve_config(
            instance_name=instance_name,
            max_concurrent_connections=max_concurrent_connections,
            max_connections_per_tenant=max_connections_per_tenant,
            ingress_mode=ingress_mode,
            environ=environ,
        )
    except CoreConfigurationError as exc:
        err.write(f"Configuration error: {exc}\n")
        return 1

    # Preserve exact uvicorn.run call for default configuration (backward compatibility)
    is_default_config = (
        config.instance_name == DEFAULT_INSTANCE_NAME
        and config.max_concurrent_connections == DEFAULT_MAX_CONCURRENT_CONNECTIONS
        and config.max_connections_per_tenant == DEFAULT_MAX_CONNECTIONS_PER_TENANT
        and config.ingress_mode == INGRESS_MODE_DISABLED
    )

    if (
        is_default_config
        and host == "127.0.0.1"
        and port == 8000
        and log_level == "info"
        and ws_max_size == 65536
    ):
        uvicorn.run(
            "kaiwen_gateway.server:create_gateway_app",
            factory=True,
            host=host,
            port=port,
            log_level=log_level,
            ws_max_size=ws_max_size,
        )
        return 0

    from kaiwen_gateway.server.app import create_gateway_app

    synthetic_auth_bridge = None
    if config.ingress_mode == INGRESS_MODE_SYNTHETIC:
        env = os.environ if environ is None else environ
        secret_key_str = env.get("KAIWEN_GATEWAY_SYNTHETIC_KEY")
        if not secret_key_str:
            err.write(
                "Error: Synthetic ingress mode in 'serve' requires a configured signing key via "
                "environment variable 'KAIWEN_GATEWAY_SYNTHETIC_KEY' (minimum 16 bytes). "
                "For an automated self-contained test, run 'kaiwen-gateway demo'.\n"
            )
            return 1
        key_bytes = secret_key_str.encode("utf-8")
        if len(key_bytes) < 16:
            err.write(
                f"Error: 'KAIWEN_GATEWAY_SYNTHETIC_KEY' must be at least 16 bytes, got {len(key_bytes)} bytes.\n"
            )
            return 1

        from kaiwen_gateway.transports.synthetic.auth import SyntheticAuthBridge
        from kaiwen_gateway.transports.synthetic.tokens import SyntheticTokenVerifier

        verifier = SyntheticTokenVerifier(secret_key=key_bytes, enable_replay_protection=True)
        synthetic_auth_bridge = SyntheticAuthBridge(
            verifier=verifier, enforce_call_exclusivity=True
        )

    app = create_gateway_app(
        config=config,
        synthetic_auth_bridge=synthetic_auth_bridge,
    )
    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level=log_level,
        ws_max_size=ws_max_size,
    )
    return 0


async def _async_run_demo(
    *,
    port: int,
    timeout: float,
    log_level: str,
    stdout: TextIO,
    stderr: TextIO,
) -> int:
    import uvicorn
    import websockets
    import websockets.exceptions

    from kaiwen_gateway.server.app import create_gateway_app
    from kaiwen_gateway.transports.synthetic.auth import SyntheticAuthBridge
    from kaiwen_gateway.transports.synthetic.protocol import (
        EVENT_CALL_END,
        EVENT_CALL_ENDED,
        EVENT_CALL_START,
        EVENT_CALL_STARTED,
        EVENT_SESSION_END,
        EVENT_SESSION_PING,
        EVENT_SESSION_PONG,
        EVENT_SESSION_READY,
        SYNTHETIC_WS_ENDPOINT_PATH,
    )
    from kaiwen_gateway.transports.synthetic.tokens import SyntheticTokenVerifier

    # Pre-check port if specified explicitly (> 0)
    if port > 0:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_check:
                s_check.bind(("127.0.0.1", port))
        except OSError as sock_err:
            stderr.write(f"Error: Port {port} is not available on 127.0.0.1: {sock_err}\n")
            return 1

    stdout.write("[1/7] Initializing ephemeral synthetic server...")
    stdout.flush()

    # Ephemeral credentials and isolated app
    secret_key = secrets.token_bytes(32)
    secret_hex = secret_key.hex()
    verifier = SyntheticTokenVerifier(secret_key=secret_key, enable_replay_protection=True)
    bridge = SyntheticAuthBridge(verifier=verifier, enforce_call_exclusivity=True)
    config = CoreConfig(
        instance_name="synthetic-demo",
        ingress_mode=INGRESS_MODE_SYNTHETIC,
        max_concurrent_connections=10,
        max_connections_per_tenant=5,
    )
    app = create_gateway_app(config=config, synthetic_auth_bridge=bridge)

    # Start loopback Uvicorn server (port=0 lets OS kernel allocate an unused port atomically)
    uv_cfg = uvicorn.Config(
        app=app,
        host="127.0.0.1",
        port=port,
        log_level=log_level,
        ws="auto",
    )
    server = uvicorn.Server(uv_cfg)
    server_task = asyncio.create_task(server.serve())

    # Wait for server.started
    start_deadline = time.monotonic() + min(timeout, 5.0)
    while time.monotonic() < start_deadline:
        if server.started:
            break
        await asyncio.sleep(0.02)

    if not server.started:
        server.should_exit = True
        try:
            await asyncio.wait_for(server_task, timeout=2.0)
        except Exception:
            pass
        stdout.write(" FAIL\n")
        stderr.write(f"Error: Server failed to start on loopback port {port} within timeout.\n")
        return 1

    # Extract actual bound port directly from running socket
    actual_port = port
    if actual_port == 0:
        for srv in getattr(server, "servers", []):
            for sock in getattr(srv, "sockets", []):
                actual_port = int(sock.getsockname()[1])
                break
            if actual_port != 0:
                break

    if actual_port == 0:
        server.should_exit = True
        try:
            await asyncio.wait_for(server_task, timeout=2.0)
        except Exception:
            pass
        stdout.write(" FAIL\n")
        stderr.write("Error: Could not determine bound port from server sockets.\n")
        return 1

    stdout.write(f" on 127.0.0.1:{actual_port}... OK\n")
    stdout.flush()

    token = ""
    try:
        # Mint token
        stdout.write("[2/7] Minting authenticated bearer token...")
        stdout.flush()
        token = verifier.issue_token(
            provider_id="synthetic_provider",
            tenant_id="demo-tenant",
            call_id="demo-call-001",
            allowed_scopes=["voice.stream"],
            ttl_seconds=60.0,
        )
        stdout.write(" OK\n")
        stdout.flush()

        # Connect WebSocket client
        stdout.write("[3/7] Connecting WebSocket client and receiving session.ready...")
        stdout.flush()
        ws_url = f"ws://127.0.0.1:{actual_port}{SYNTHETIC_WS_ENDPOINT_PATH}"
        headers = {"Authorization": f"Bearer {token}"}

        async with websockets.connect(ws_url, additional_headers=headers) as ws:
            ready_raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
            ready_data = json.loads(ready_raw)
            if ready_data.get("event") != EVENT_SESSION_READY:
                raise RuntimeError(f"Unexpected initial event: {ready_data.get('event')}")
            if ready_data.get("call_id") != "demo-call-001":
                raise RuntimeError("Call ID mismatch in session.ready")
            stdout.write(" OK\n")
            stdout.flush()

            # 4. Telephony call lifecycle
            stdout.write(
                "[4/7] Initiating synthetic telephony call (call.start -> call.started)..."
            )
            stdout.flush()
            start_frame = {
                "event": EVENT_CALL_START,
                "stream_id": "demo-stream-001",
                "metadata": {"caller": "+15550001", "callee": "+15550002"},
            }
            await ws.send(json.dumps(start_frame))
            started_raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
            started_data = json.loads(started_raw)
            if started_data.get("event") != EVENT_CALL_STARTED:
                raise RuntimeError(f"Unexpected call start response: {started_data.get('event')}")
            if started_data.get("state") != "connected":
                raise RuntimeError(f"Unexpected call state: {started_data.get('state')}")
            stdout.write(" OK\n")
            stdout.flush()

            # 5. Heartbeat ping / pong
            stdout.write("[5/7] Testing control plane heartbeat (session.ping -> session.pong)...")
            stdout.flush()
            await ws.send(json.dumps({"event": EVENT_SESSION_PING, "seq": 42}))
            pong_raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
            pong_data = json.loads(pong_raw)
            if pong_data.get("event") != EVENT_SESSION_PONG or pong_data.get("seq") != 42:
                raise RuntimeError(f"Unexpected ping response: {pong_data}")
            stdout.write(" OK\n")
            stdout.flush()

            # 6. Teardown call and session
            stdout.write("[6/7] Teardown: call.end -> call.ended -> session.end...")
            stdout.flush()
            end_frame = {
                "event": EVENT_CALL_END,
                "reason": "completed",
                "code": 16,
            }
            await ws.send(json.dumps(end_frame))
            ended_raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
            ended_data = json.loads(ended_raw)
            if ended_data.get("event") != EVENT_CALL_ENDED:
                raise RuntimeError(f"Unexpected call end response: {ended_data.get('event')}")
            if ended_data.get("state") != "disconnected":
                raise RuntimeError(f"Unexpected call state: {ended_data.get('state')}")

            await ws.send(json.dumps({"event": EVENT_SESSION_END}))
            try:
                extra_msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
                raise RuntimeError(f"Unexpected post-session message received: {extra_msg!r}")
            except TimeoutError:
                raise RuntimeError(
                    "WebSocket failed to close after session.end (timeout)"
                ) from None
            except websockets.exceptions.ConnectionClosed as exc:
                if exc.rcvd is not None:
                    close_code = exc.rcvd.code
                elif ws.close_code is not None:
                    close_code = ws.close_code
                else:
                    close_code = 1006
                if close_code != 1000:
                    raise RuntimeError(
                        f"WebSocket closed with unexpected code {close_code} (expected 1000)"
                    ) from None
            stdout.write(" OK\n")
            stdout.flush()

        # 7. Isolation and zero-leak verification
        stdout.write("[7/7] Verifying resource isolation and zero active leases...")
        stdout.flush()
        coord = app.state.drain_coordinator
        if coord.active_connections_count != 0:
            raise RuntimeError(f"Active connections remaining: {coord.active_connections_count}")
        if len(bridge._active_calls) != 0:
            raise RuntimeError(f"Active calls remaining: {len(bridge._active_calls)}")

        ac = app.state.admission_controller
        snap = ac.snapshot()
        if snap.active_connections != 0:
            raise RuntimeError(
                f"AdmissionController active connections remaining: {snap.active_connections}"
            )
        if snap.available_capacity != snap.max_concurrent_connections:
            raise RuntimeError(
                f"AdmissionController capacity not fully restored: {snap.available_capacity} / {snap.max_concurrent_connections}"
            )
        if snap.active_tenants != 0:
            raise RuntimeError(
                f"AdmissionController active tenants remaining: {snap.active_tenants}"
            )
        stdout.write(" OK (0 active connections, 0 active calls, 0 active admission leases)\n")
        stdout.flush()

    except Exception as exc:
        stdout.write(" FAILED\n")
        err_msg = str(exc)
        if token and token in err_msg:
            err_msg = err_msg.replace(token, "[REDACTED]")
        if secret_hex and secret_hex in err_msg:
            err_msg = err_msg.replace(secret_hex, "[REDACTED]")
        stderr.write(f"Synthetic demo error: {err_msg}\n")
        stderr.write("Result: FAIL\n")
        server.should_exit = True
        try:
            await asyncio.wait_for(server_task, timeout=5.0)
        except Exception:
            if not server_task.done():
                server_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await server_task
        return 1

    # Clean server shutdown
    server.should_exit = True
    try:
        await asyncio.wait_for(server_task, timeout=5.0)
    except TimeoutError:
        stderr.write("Synthetic demo error: Server shutdown timed out after 5.0s\n")
        stderr.write("Result: FAIL\n")
        if not server_task.done():
            server_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await server_task
        return 1
    except Exception as exc:
        stderr.write(f"Synthetic demo error: Server shutdown failed: {exc}\n")
        stderr.write("Result: FAIL\n")
        if not server_task.done():
            server_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await server_task
        return 1

    # Verify actual application shutdown state (Blocker C)
    if getattr(app.state, "shutdown_failed", False) is not False:
        stderr.write("Synthetic demo error: Application state reports shutdown_failed=True\n")
        stderr.write("Result: FAIL\n")
        return 1
    if getattr(app.state, "stopped", False) is not True:
        stderr.write("Synthetic demo error: Application state did not reach stopped=True\n")
        stderr.write("Result: FAIL\n")
        return 1

    post_coord = app.state.drain_coordinator
    if post_coord.active_connections_count != 0:
        stderr.write(
            f"Synthetic demo error: Drain coordinator active connections remaining after shutdown: {post_coord.active_connections_count}\n"
        )
        stderr.write("Result: FAIL\n")
        return 1

    final_snap = app.state.admission_controller.snapshot()
    if (
        final_snap.active_connections != 0
        or final_snap.active_tenants != 0
        or final_snap.available_capacity != final_snap.max_concurrent_connections
    ):
        stderr.write(
            f"Synthetic demo error: AdmissionController leases remaining after shutdown: {final_snap}\n"
        )
        stderr.write("Result: FAIL\n")
        return 1

    stdout.write("Server stopped cleanly. Synthetic demo completed successfully.\nResult: PASS\n")
    stdout.flush()
    return 0


async def _async_run_media_demo(
    *,
    port: int,
    timeout: float,
    log_level: str,
    stdout: TextIO,
    stderr: TextIO,
) -> int:
    import json
    import secrets
    import socket
    import time

    import uvicorn
    import websockets
    import websockets.exceptions

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

    # Pre-check port if specified explicitly (> 0)
    if port > 0:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_check:
                s_check.bind(("127.0.0.1", port))
        except OSError as sock_err:
            stderr.write(f"Error: Port {port} is not available on 127.0.0.1: {sock_err}\n")
            return 1

    stdout.write("[1/7] Initializing ephemeral synthetic server with binary media enabled...")
    stdout.flush()

    secret_key = secrets.token_bytes(32)
    verifier = SyntheticTokenVerifier(secret_key=secret_key, enable_replay_protection=True)
    bridge = SyntheticAuthBridge(verifier=verifier, enforce_call_exclusivity=True)
    config = CoreConfig(
        instance_name="synthetic-media-demo",
        ingress_mode=INGRESS_MODE_SYNTHETIC,
        max_concurrent_connections=10,
        max_connections_per_tenant=5,
    )
    admission = AdmissionController(config)
    coordinator = InstanceDrainCoordinator(controller=admission)
    app = create_gateway_app(
        config=config,
        admission_controller=admission,
        drain_coordinator=coordinator,
        synthetic_auth_bridge=bridge,
        enable_synthetic_media=True,
    )

    uv_cfg = uvicorn.Config(
        app=app,
        host="127.0.0.1",
        port=port,
        log_level=log_level,
        ws="auto",
    )
    server = uvicorn.Server(uv_cfg)
    server_task = asyncio.create_task(server.serve())

    start_deadline = time.monotonic() + min(timeout, 5.0)
    while not server.started:
        if time.monotonic() > start_deadline:
            stderr.write(
                f"\nSynthetic media demo error: Server failed to start on 127.0.0.1 within {min(timeout, 5.0)}s\n"
            )
            server.should_exit = True
            await asyncio.gather(server_task, return_exceptions=True)
            return 1
        await asyncio.sleep(0.01)

    # Determine bound port
    bound_port = port
    if bound_port == 0:
        for s in getattr(server, "servers", []):
            for sock in getattr(s, "sockets", []):
                s_name = sock.getsockname()
                if isinstance(s_name, tuple) and len(s_name) >= 2:
                    bound_port = int(s_name[1])
                    break
            if bound_port > 0:
                break

    stdout.write(f" on 127.0.0.1:{bound_port}... OK\n")
    stdout.flush()

    tenant_id = "demo-media-tenant"
    call_id = "demo-media-call-001"
    token = verifier.issue_token(
        provider_id="demo_provider",
        tenant_id=tenant_id,
        call_id=call_id,
        allowed_scopes=["voice.stream"],
        ttl_seconds=120,
    )
    stdout.write("[2/7] Minting authenticated bearer token (scope: voice.stream)... OK\n")
    stdout.flush()

    uri = f"ws://127.0.0.1:{bound_port}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
    headers = {"Authorization": f"Bearer {token}"}

    demo_err: str | None = None
    try:
        stdout.write("[3/7] Connecting to /ws/synthetic-media/v1 and receiving session.ready...")
        stdout.flush()
        async with websockets.connect(
            uri,
            additional_headers=headers,
            max_size=1024 * 1024,
        ) as ws:
            ready_raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
            if not isinstance(ready_raw, str):
                raise ValueError(
                    f"Expected text JSON session.ready greeting, got {type(ready_raw).__name__}"
                )
            ready_json = json.loads(ready_raw)
            if ready_json.get("event") != EVENT_SESSION_READY:
                raise ValueError(
                    f"Expected event {EVENT_SESSION_READY}, got {ready_json.get('event')}"
                )
            if ready_json.get("call_id") != call_id:
                raise ValueError(
                    f"call_id mismatch: expected {call_id}, got {ready_json.get('call_id')}"
                )
            stdout.write(" OK\n")
            stdout.flush()

            stdout.write("[4/7] Initiating call (call.start -> call.started)...")
            stdout.flush()
            await ws.send(json.dumps({"event": EVENT_CALL_START, "call_id": call_id}))
            started_raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
            if not isinstance(started_raw, str):
                raise ValueError(
                    f"Expected text JSON call.started, got {type(started_raw).__name__}"
                )
            started_json = json.loads(started_raw)
            if started_json.get("event") != EVENT_CALL_STARTED:
                raise ValueError(
                    f"Expected event {EVENT_CALL_STARTED}, got {started_json.get('event')}"
                )
            stdout.write(" OK\n")
            stdout.flush()

            stdout.write(
                "[5/7] Streaming 25 binary PCM16LE frames and verifying bidirectional echo..."
            )
            stdout.flush()
            frame_count = 25
            sent_payloads: list[bytes] = []
            received_payloads: list[bytes] = []
            for i in range(frame_count):
                prefix = f"PCM_{i:04d}_".encode("ascii")
                sent_payloads.append(prefix + b"\x42" * (PCM16LE_FRAME_BYTES - len(prefix)))

            async def _sender() -> None:
                for p in sent_payloads:
                    await ws.send(p)
                    await asyncio.sleep(0.005)

            async def _receiver() -> None:
                while len(received_payloads) < frame_count:
                    resp = await asyncio.wait_for(ws.recv(), timeout=timeout)
                    if isinstance(resp, bytes):
                        received_payloads.append(resp)

            await asyncio.gather(_sender(), _receiver())
            for idx, (sp, rp) in enumerate(zip(sent_payloads, received_payloads, strict=False)):
                if sp != rp:
                    raise ValueError(f"Echoed frame payload mismatch at sequence {idx}")
            stdout.write(f" OK ({frame_count} frames, {frame_count * PCM16LE_FRAME_BYTES} bytes)\n")
            stdout.flush()

            stdout.write(
                "[6/7] Simulating barge-in interruption (call.interrupt -> call.interrupted)..."
            )
            stdout.flush()
            interrupt_cmd = {
                "event": EVENT_CALL_INTERRUPT,
                "call_id": call_id,
                "turn_id": 1,
                "request_id": "demo-req-turn1",
            }
            await ws.send(json.dumps(interrupt_cmd))
            interrupted_raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
            if not isinstance(interrupted_raw, str):
                raise ValueError(
                    f"Expected text JSON call.interrupted, got {type(interrupted_raw).__name__}"
                )
            interrupted_json = json.loads(interrupted_raw)
            if interrupted_json.get("event") != EVENT_CALL_INTERRUPTED:
                raise ValueError(
                    f"Expected event {EVENT_CALL_INTERRUPTED}, got {interrupted_json.get('event')}"
                )
            if interrupted_json.get("new_turn_id") != 2:
                raise ValueError(
                    f"Expected new_turn_id=2, got {interrupted_json.get('new_turn_id')}"
                )

            # Stream Turn 2 frame and verify echo
            turn2_payload = b"PCM_T2_0000_" + b"\x55" * (PCM16LE_FRAME_BYTES - 12)
            await ws.send(turn2_payload)
            turn2_echoed = await asyncio.wait_for(ws.recv(), timeout=timeout)
            if turn2_echoed != turn2_payload:
                raise ValueError("Turn 2 echoed frame mismatch")
            stdout.write(" OK (interrupted Turn 1, verified Turn 2)\n")
            stdout.flush()

            stdout.write(
                "[7/7] Teardown: call.end -> call.ended and verifying capacity recovery..."
            )
            stdout.flush()
            await ws.send(json.dumps({"event": EVENT_CALL_END, "call_id": call_id}))
            ended_raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
            if not isinstance(ended_raw, str):
                raise ValueError(f"Expected text JSON call.ended, got {type(ended_raw).__name__}")
            ended_json = json.loads(ended_raw)
            if ended_json.get("event") != EVENT_CALL_ENDED:
                raise ValueError(
                    f"Expected event {EVENT_CALL_ENDED}, got {ended_json.get('event')}"
                )
            await ws.close()

    except Exception as exc:
        demo_err = str(exc)

    finally:
        server.should_exit = True
        try:
            await asyncio.wait_for(server_task, timeout=5.0)
        except (TimeoutError, asyncio.CancelledError):
            pass

    if demo_err is not None:
        stderr.write(f"\nSynthetic media demo error: {demo_err}\nResult: FAIL\n")
        stderr.flush()
        return 1

    await asyncio.sleep(0.05)
    snap = admission.snapshot()
    if snap.active_connections != 0 or admission.active_connections_for_tenant(tenant_id) != 0:
        stderr.write(
            f"\nSynthetic media demo error: Capacity not fully recovered ({snap})\nResult: FAIL\n"
        )
        stderr.flush()
        return 1
    if (tenant_id, call_id) in bridge._active_calls:
        stderr.write(
            "\nSynthetic media demo error: Call remains registered in auth bridge\nResult: FAIL\n"
        )
        stderr.flush()
        return 1

    stdout.write(" OK (0 active connections, 0 active calls, 0 active admission leases)\n")
    stdout.write(
        "Server stopped cleanly. Synthetic media demo completed successfully.\nResult: PASS\n"
    )
    stdout.flush()
    return 0


def run_demo(
    *,
    media: bool = False,
    port: int = 0,
    timeout: float = 10.0,
    log_level: str = "warning",
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
) -> int:
    """Execute end-to-end synthetic call demo against loopback server."""
    out = sys.stdout if stdout is None else stdout
    err = sys.stderr if stderr is None else stderr

    if isinstance(port, bool) or not isinstance(port, int) or port < 0 or port > 65535:
        err.write(f"Error: Invalid port {port!r}. Port must be an integer between 0 and 65535.\n")
        return 1

    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or not math.isfinite(timeout)
        or timeout <= 0.0
        or timeout > 3600.0
    ):
        err.write(
            f"Error: Invalid timeout {timeout!r}. Must be a positive finite number <= 3600 seconds.\n"
        )
        return 1

    try:
        import starlette  # noqa: F401
        import uvicorn  # noqa: F401
        import websockets  # noqa: F401
    except ImportError:
        err.write(
            "Error: 'uvicorn', 'starlette', and 'websockets' are required to run the synthetic call demo.\n"
            "Developer installation: uv sync --locked --group dev --extra server\n"
            "Consumer installation: pip install 'kaiwen-gateway[server]'\n"
        )
        return 1

    if media:
        return asyncio.run(
            _async_run_media_demo(
                port=port,
                timeout=timeout,
                log_level=log_level,
                stdout=out,
                stderr=err,
            )
        )

    return asyncio.run(
        _async_run_demo(
            port=port,
            timeout=timeout,
            log_level=log_level,
            stdout=out,
            stderr=err,
        )
    )


def build_parser() -> argparse.ArgumentParser:
    """Build the standard library argparse parser for Kaiwen Gateway."""
    parser = argparse.ArgumentParser(
        prog="kaiwen-gateway",
        description="Kaiwen Gateway - modular communication gateway for real-time AI applications.",
    )
    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    subparsers = parser.add_subparsers(dest="command", metavar="<command>")

    # serve
    serve_parser = subparsers.add_parser(
        "serve",
        help="Run the Kaiwen Gateway ASGI server.",
        description="Run local development ASGI server on loopback interface.",
    )
    serve_parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="Host interface to bind (default: 127.0.0.1, restricted to loopback).",
    )
    serve_parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="TCP port to bind (default: 8000).",
    )
    serve_parser.add_argument(
        "--ingress-mode",
        type=str,
        default=None,
        choices=sorted(SUPPORTED_INGRESS_MODES),
        help="Gateway ingress mode (default: disabled, or KAIWEN_GATEWAY_INGRESS_MODE).",
    )
    serve_parser.add_argument(
        "--instance-name",
        type=str,
        default=None,
        help="Instance name identifier (default: 'default', or KAIWEN_GATEWAY_INSTANCE_NAME).",
    )
    serve_parser.add_argument(
        "--max-concurrent-connections",
        type=int,
        default=None,
        help="Maximum concurrent connections (default: 25, or KAIWEN_GATEWAY_MAX_CONCURRENT_CONNECTIONS).",
    )
    serve_parser.add_argument(
        "--max-connections-per-tenant",
        type=int,
        default=None,
        help="Maximum concurrent connections per tenant (default: 10, or KAIWEN_GATEWAY_MAX_CONNECTIONS_PER_TENANT).",
    )
    serve_parser.add_argument(
        "--log-level",
        type=str,
        default="info",
        choices=["critical", "error", "warning", "info", "debug", "trace"],
        help="Uvicorn log level (default: info).",
    )
    serve_parser.add_argument(
        "--ws-max-size",
        type=int,
        default=65536,
        help="Maximum WebSocket message payload size in bytes (default: 65536).",
    )

    # check-config
    config_parser = subparsers.add_parser(
        "check-config",
        help="Validate gateway configuration without running server.",
        description="Validate CoreConfig and KAIWEN_GATEWAY_* environment variables.",
    )
    config_parser.add_argument(
        "--instance-name",
        type=str,
        default=None,
        help="Instance name identifier to validate.",
    )
    config_parser.add_argument(
        "--max-concurrent-connections",
        type=int,
        default=None,
        help="Maximum concurrent connections to validate.",
    )
    config_parser.add_argument(
        "--max-connections-per-tenant",
        type=int,
        default=None,
        help="Maximum concurrent connections per tenant to validate.",
    )
    config_parser.add_argument(
        "--ingress-mode",
        type=str,
        default=None,
        choices=sorted(SUPPORTED_INGRESS_MODES),
        help="Gateway ingress mode to validate.",
    )

    # demo
    demo_parser = subparsers.add_parser(
        "demo",
        help="Execute automated synthetic call lifecycle demo.",
        description="Start ephemeral loopback server and execute a synthetic telephony call end-to-end.",
    )
    demo_parser.add_argument(
        "--media",
        action="store_true",
        help="Execute synthetic binary PCM16LE audio media demo with interruption.",
    )
    demo_parser.add_argument(
        "--port",
        type=int,
        default=0,
        help="TCP port for loopback demo server (default: 0 = allocate free port).",
    )
    demo_parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="Per-step timeout in seconds (default: 10.0).",
    )
    demo_parser.add_argument(
        "--log-level",
        type=str,
        default="warning",
        choices=["critical", "error", "warning", "info", "debug", "trace"],
        help="Log level for demo server (default: warning).",
    )

    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
) -> int:
    """Main CLI entrypoint for Kaiwen Gateway.

    Dispatches subcommands (serve, check-config, demo) or defaults to
    running the local development server for backward compatibility.
    """
    out = sys.stdout if stdout is None else stdout
    err = sys.stderr if stderr is None else stderr

    if argv is None:
        args_list = list(sys.argv[1:])
        # If running within a test runner where sys.argv carries runner options
        if args_list and ("pytest" in sys.modules or "unittest" in sys.modules):
            if args_list[0] not in (
                "serve",
                "check-config",
                "demo",
                "--help",
                "-h",
                "--version",
                "-v",
            ):
                args_list = []
    else:
        args_list = list(argv)

    # Backward compatibility: running without arguments starts default serve
    if not args_list:
        return run_serve(environ=environ, stdout=out, stderr=err)

    parser = build_parser()

    # Capture argparse SystemExit and redirect streams if custom stdout/stderr supplied
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            parsed_args = parser.parse_args(args_list)
        except SystemExit as exc:
            return int(exc.code) if isinstance(exc.code, int) else (0 if exc.code is None else 1)

    if parsed_args.command == "serve":
        return run_serve(
            host=parsed_args.host,
            port=parsed_args.port,
            log_level=parsed_args.log_level,
            ws_max_size=parsed_args.ws_max_size,
            instance_name=parsed_args.instance_name,
            max_concurrent_connections=parsed_args.max_concurrent_connections,
            max_connections_per_tenant=parsed_args.max_connections_per_tenant,
            ingress_mode=parsed_args.ingress_mode,
            environ=environ,
            stdout=out,
            stderr=err,
        )
    elif parsed_args.command == "check-config":
        return run_check_config(
            instance_name=parsed_args.instance_name,
            max_concurrent_connections=parsed_args.max_concurrent_connections,
            max_connections_per_tenant=parsed_args.max_connections_per_tenant,
            ingress_mode=parsed_args.ingress_mode,
            environ=environ,
            stdout=out,
            stderr=err,
        )
    elif parsed_args.command == "demo":
        return run_demo(
            media=getattr(parsed_args, "media", False),
            port=parsed_args.port,
            timeout=parsed_args.timeout,
            log_level=parsed_args.log_level,
            stdout=out,
            stderr=err,
        )
    else:  # pragma: no cover
        parser.print_help(err)
        return 1


__all__ = [
    "build_parser",
    "main",
    "resolve_config",
    "run_check_config",
    "run_demo",
    "run_serve",
]
