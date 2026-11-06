"""Comprehensive tests for Kaiwen Gateway CLI and executable synthetic demo (M3.3A).

Verifies:
1. Version flag (--version / -v).
2. Help flag (--help / -h).
3. Invalid CLI commands.
4. Backward compatibility (no args defaults to serve on 127.0.0.1:8000).
5. check-config with default configuration.
6. check-config flag precedence over environment variables.
7. check-config with valid environment variables.
8. check-config with invalid environment variables (non-int).
9. check-config with invalid capacity limits (tenant > concurrent).
10. check-config with invalid instance name.
11. check-config zero external dependencies (no starlette/uvicorn needed).
12. serve missing server extra reporting.
13. serve invalid config reporting.
14. serve custom flags execution.
15. serve synthetic ingress configuration.
16. demo missing server extra reporting.
17. demo occupied port error handling.
18. demo end-to-end executable synthetic call lifecycle.
19. demo repeated execution (zero capacity or lease leak).
20. demo failure and clean teardown.
"""

from __future__ import annotations

import io
import socket
import sys
from typing import Any
from unittest.mock import patch

from kaiwen_gateway import __version__
from kaiwen_gateway.server.cli import (
    main,
    run_check_config,
    run_demo,
    run_serve,
)


def test_01_cli_version() -> None:
    """Verify --version and -v return 0 and output current version."""
    for flag in ["--version", "-v"]:
        out = io.StringIO()
        err = io.StringIO()
        code = main([flag], stdout=out, stderr=err)
        assert code == 0
        combined = out.getvalue() + err.getvalue()
        assert f"kaiwen-gateway {__version__}" in combined


def test_02_cli_help() -> None:
    """Verify --help and -h return 0 and describe subcommands."""
    for flag in ["--help", "-h"]:
        out = io.StringIO()
        err = io.StringIO()
        code = main([flag], stdout=out, stderr=err)
        assert code == 0
        combined = out.getvalue() + err.getvalue()
        assert "serve" in combined
        assert "check-config" in combined
        assert "demo" in combined


def test_03_cli_invalid_command() -> None:
    """Verify unrecognized subcommand returns non-zero code (2 from argparse)."""
    err = io.StringIO()
    code = main(["unknown-subcommand"], stderr=err)
    assert code == 2


def test_04_cli_backward_compatible_no_args() -> None:
    """Verify running without arguments defaults to serve on 127.0.0.1:8000."""
    with patch("uvicorn.run") as mock_run:
        code = main([])
        assert code == 0
        mock_run.assert_called_once_with(
            "kaiwen_gateway.server:create_gateway_app",
            factory=True,
            host="127.0.0.1",
            port=8000,
            log_level="info",
            ws_max_size=65536,
        )


def test_05_check_config_valid_defaults() -> None:
    """Verify check-config validates default configuration and outputs clean summary."""
    out = io.StringIO()
    err = io.StringIO()
    code = main(["check-config"], environ={}, stdout=out, stderr=err)
    assert code == 0
    output = out.getvalue()
    assert "Configuration valid:" in output
    assert "instance_name: default" in output
    assert "max_concurrent_connections: 25" in output
    assert "max_connections_per_tenant: 10" in output
    assert "ingress_mode: disabled" in output
    assert err.getvalue() == ""


def test_06_check_config_flag_precedence_over_env() -> None:
    """Verify explicit CLI arguments take precedence over environment variables."""
    env = {
        "KAIWEN_GATEWAY_INSTANCE_NAME": "from_env",
        "KAIWEN_GATEWAY_MAX_CONCURRENT_CONNECTIONS": "50",
        "KAIWEN_GATEWAY_MAX_CONNECTIONS_PER_TENANT": "20",
        "KAIWEN_GATEWAY_INGRESS_MODE": "disabled",
    }
    out = io.StringIO()
    err = io.StringIO()
    code = run_check_config(
        instance_name="from_cli",
        max_concurrent_connections=100,
        max_connections_per_tenant=30,
        ingress_mode="synthetic",
        environ=env,
        stdout=out,
        stderr=err,
    )
    assert code == 0
    output = out.getvalue()
    assert "instance_name: from_cli" in output
    assert "max_concurrent_connections: 100" in output
    assert "max_connections_per_tenant: 30" in output
    assert "ingress_mode: synthetic" in output


def test_07_check_config_with_valid_env_vars() -> None:
    """Verify check-config reads and validates KAIWEN_GATEWAY_* environment variables."""
    env = {
        "KAIWEN_GATEWAY_INSTANCE_NAME": "custom_prod_01",
        "KAIWEN_GATEWAY_MAX_CONCURRENT_CONNECTIONS": "40",
        "KAIWEN_GATEWAY_MAX_CONNECTIONS_PER_TENANT": "15",
        "KAIWEN_GATEWAY_INGRESS_MODE": "synthetic",
    }
    out = io.StringIO()
    err = io.StringIO()
    code = run_check_config(environ=env, stdout=out, stderr=err)
    assert code == 0
    output = out.getvalue()
    assert "instance_name: custom_prod_01" in output
    assert "max_concurrent_connections: 40" in output
    assert "max_connections_per_tenant: 15" in output
    assert "ingress_mode: synthetic" in output


def test_08_check_config_with_invalid_env_non_int() -> None:
    """Verify non-integer environment variables fail closed with clear error message."""
    env1 = {"KAIWEN_GATEWAY_MAX_CONCURRENT_CONNECTIONS": "not-an-int"}
    err1 = io.StringIO()
    code1 = run_check_config(environ=env1, stderr=err1)
    assert code1 == 1
    assert "Invalid integer in KAIWEN_GATEWAY_MAX_CONCURRENT_CONNECTIONS" in err1.getvalue()

    env2 = {"KAIWEN_GATEWAY_MAX_CONNECTIONS_PER_TENANT": "bad-int"}
    err2 = io.StringIO()
    code2 = run_check_config(environ=env2, stderr=err2)
    assert code2 == 1
    assert "Invalid integer in KAIWEN_GATEWAY_MAX_CONNECTIONS_PER_TENANT" in err2.getvalue()


def test_09_check_config_invalid_capacity_limits() -> None:
    """Verify max_connections_per_tenant exceeding max_concurrent_connections fails."""
    err = io.StringIO()
    code = run_check_config(
        max_concurrent_connections=10,
        max_connections_per_tenant=20,
        stderr=err,
    )
    assert code == 1
    assert "must not exceed" in err.getvalue()


def test_10_check_config_invalid_instance_name() -> None:
    """Verify malformed instance name fails closed."""
    err = io.StringIO()
    code = run_check_config(
        instance_name="invalid instance name with spaces!",
        stderr=err,
    )
    assert code == 1
    assert "instance_name" in err.getvalue()


def test_11_check_config_zero_external_dependencies() -> None:
    """Verify check-config runs with zero optional server dependencies (no uvicorn/starlette)."""
    with patch.dict("sys.modules", {"uvicorn": None, "starlette": None, "websockets": None}):
        out = io.StringIO()
        err = io.StringIO()
        code = run_check_config(stdout=out, stderr=err)
        assert code == 0
        assert "Configuration valid:" in out.getvalue()


def test_12_serve_missing_server_extra() -> None:
    """Verify serve without uvicorn prints helpful install instructions and exits 1."""
    with patch.dict("sys.modules", {"uvicorn": None}):
        err = io.StringIO()
        code = run_serve(stderr=err)
        assert code == 1
        output = err.getvalue()
        assert "Error: 'uvicorn' and 'starlette' are required" in output
        assert "pip install 'kaiwen-gateway[server]'" in output


def test_13_serve_invalid_config() -> None:
    """Verify serve with invalid configuration exits 1 with error message."""
    err = io.StringIO()
    code = run_serve(
        max_concurrent_connections=10,
        max_connections_per_tenant=50,
        stderr=err,
    )
    assert code == 1
    assert "Configuration error:" in err.getvalue()


def test_14_serve_custom_flags() -> None:
    """Verify serve with custom flags starts uvicorn with configured parameters."""
    with patch("uvicorn.run") as mock_run:
        out = io.StringIO()
        err = io.StringIO()
        code = run_serve(
            host="127.0.0.2",
            port=9090,
            log_level="warning",
            instance_name="custom-node",
            max_concurrent_connections=50,
            max_connections_per_tenant=20,
            stdout=out,
            stderr=err,
        )
        assert code == 0
        mock_run.assert_called_once()
        _, kwargs = mock_run.call_args
        assert kwargs["host"] == "127.0.0.2"
        assert kwargs["port"] == 9090
        assert kwargs["log_level"] == "warning"


def test_15_serve_synthetic_ingress() -> None:
    """Verify serve with synthetic ingress requires configured key and creates bridge."""
    # 1. Missing key fails closed
    err_missing = io.StringIO()
    code_missing = run_serve(
        ingress_mode="synthetic",
        environ={},
        stderr=err_missing,
    )
    assert code_missing == 1
    assert "requires a configured signing key via environment variable" in err_missing.getvalue()

    # 2. Key too short (< 16 bytes) fails closed
    err_short = io.StringIO()
    code_short = run_serve(
        ingress_mode="synthetic",
        environ={"KAIWEN_GATEWAY_SYNTHETIC_KEY": "too-short"},
        stderr=err_short,
    )
    assert code_short == 1
    assert "must be at least 16 bytes" in err_short.getvalue()

    # 3. Valid key (>= 16 bytes) initializes app with SyntheticAuthBridge
    with patch("uvicorn.run") as mock_run:
        out = io.StringIO()
        err = io.StringIO()
        code = run_serve(
            ingress_mode="synthetic",
            environ={"KAIWEN_GATEWAY_SYNTHETIC_KEY": "a" * 32},
            stdout=out,
            stderr=err,
        )
        assert code == 0
        mock_run.assert_called_once()
        app_arg = mock_run.call_args[0][0]
        assert hasattr(app_arg, "state")
        assert app_arg.state.config.ingress_mode == "synthetic"
        assert app_arg.state.synthetic_auth_bridge is not None


def test_16_demo_missing_server_extra() -> None:
    """Verify demo without websockets prints install instructions and exits 1."""
    with patch.dict("sys.modules", {"websockets": None}):
        err = io.StringIO()
        code = run_demo(stderr=err)
        assert code == 1
        output = err.getvalue()
        assert "Error: 'uvicorn', 'starlette', and 'websockets' are required" in output
        assert "pip install 'kaiwen-gateway[server]'" in output


def test_17_demo_occupied_port() -> None:
    """Verify demo fails closed if the requested TCP port is already in use."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        occupied_port = int(s.getsockname()[1])

        err = io.StringIO()
        code = run_demo(port=occupied_port, stderr=err)
        assert code == 1
        assert f"Port {occupied_port} is not available" in err.getvalue()


def test_18_demo_end_to_end_success() -> None:
    """Verify demo executes full synthetic call lifecycle cleanly over loopback WebSocket."""
    out = io.StringIO()
    err = io.StringIO()
    code = run_demo(port=0, timeout=10.0, stdout=out, stderr=err)
    assert code == 0
    output = out.getvalue()

    # Step-by-step verification
    assert "[1/7] Initializing ephemeral synthetic server" in output
    assert "[2/7] Minting authenticated bearer token" in output
    assert "[3/7] Connecting WebSocket client and receiving session.ready" in output
    assert "[4/7] Initiating synthetic telephony call (call.start -> call.started)" in output
    assert "[5/7] Testing control plane heartbeat (session.ping -> session.pong)" in output
    assert "[6/7] Teardown: call.end -> call.ended -> session.end" in output
    assert (
        "[7/7] Verifying resource isolation and zero active leases... OK (0 active connections, 0 active calls)"
        in output
    )
    assert "Result: PASS" in output
    assert err.getvalue() == ""


def test_19_demo_repeated_execution_zero_leaks() -> None:
    """Verify repeated demo execution leaves no leaked ports, sockets, or capacity leases."""
    for _ in range(2):
        out = io.StringIO()
        err = io.StringIO()
        code = run_demo(port=0, timeout=10.0, stdout=out, stderr=err)
        assert code == 0
        assert "Result: PASS" in out.getvalue()


def test_20_demo_failure_clean_teardown() -> None:
    """Verify demo failure prints FAIL, stops server, and exits 1 without orphan tasks."""
    out = io.StringIO()
    err = io.StringIO()
    # Trigger error by providing invalid verifier issue token
    with patch(
        "kaiwen_gateway.transports.synthetic.tokens.SyntheticTokenVerifier.issue_token",
        side_effect=RuntimeError("Simulated token generation failure"),
    ):
        code = run_demo(port=0, timeout=5.0, stdout=out, stderr=err)
        assert code == 1
        assert "FAILED" in out.getvalue()
        assert "Result: FAIL" in err.getvalue()
        assert "Simulated token generation failure" in err.getvalue()


def test_21_cli_main_subcommand_dispatch() -> None:
    """Verify main() properly dispatches check-config, serve, and demo."""
    with patch("kaiwen_gateway.server.cli.run_check_config", return_value=0) as mock_cc:
        code = main(["check-config", "--instance-name", "foo"])
        assert code == 0
        mock_cc.assert_called_once()

    with patch("kaiwen_gateway.server.cli.run_serve", return_value=0) as mock_serve:
        code = main(["serve", "--port", "9999"])
        assert code == 0
        mock_serve.assert_called_once()

    with patch("kaiwen_gateway.server.cli.run_demo", return_value=0) as mock_demo:
        code = main(["demo", "--timeout", "5.0"])
        assert code == 0
        mock_demo.assert_called_once()


def test_22_demo_server_start_timeout() -> None:
    """Verify demo server startup timeout fails closed with clear error message."""
    out = io.StringIO()
    err = io.StringIO()

    async def fake_serve(*args: Any, **kwargs: Any) -> None:
        pass

    with patch("uvicorn.Server.serve", side_effect=fake_serve):
        code = run_demo(port=0, timeout=0.1, stdout=out, stderr=err)
        assert code == 1
        assert "Server failed to start on loopback port" in err.getvalue()


def test_23_main_argv_none_dispatch() -> None:
    """Verify main() with argv=None inspects sys.argv correctly."""
    with patch.object(sys, "argv", ["kaiwen-gateway", "check-config"]):
        out = io.StringIO()
        code = main(stdout=out)
        assert code == 0
        assert "Configuration valid:" in out.getvalue()

    with patch.object(sys, "argv", ["kaiwen-gateway"]):
        with patch("uvicorn.run") as mock_run:
            code = main()
            assert code == 0
            mock_run.assert_called_once()


def test_24_demo_protocol_validation_mismatch() -> None:
    """Verify demo protocol response mismatches trigger clean error exit."""
    import json

    async def fake_recv(*args: Any, **kwargs: Any) -> str:
        return json.dumps({"event": "wrong.event", "call_id": "demo-call-001"})

    with patch("websockets.asyncio.client.ClientConnection.recv", new=fake_recv):
        out = io.StringIO()
        err = io.StringIO()
        code = run_demo(port=0, timeout=1.0, stdout=out, stderr=err)
        assert code == 1
        assert "Result: FAIL" in err.getvalue()
        assert "Unexpected initial event" in err.getvalue()


def test_25_demo_active_leak_detected() -> None:
    """Verify demo reports failure if active connection leases remain."""
    from kaiwen_gateway.core.draining import InstanceDrainCoordinator

    with patch.object(InstanceDrainCoordinator, "active_connections_count", 1):
        out = io.StringIO()
        err = io.StringIO()
        code = run_demo(port=0, timeout=5.0, stdout=out, stderr=err)
        assert code == 1
        assert "Active connections remaining: 1" in err.getvalue()


def test_26_serve_non_loopback_prohibited() -> None:
    """Verify serve strictly prohibits binding to non-loopback addresses."""
    for bad_host in ["0.0.0.0", "192.168.1.1", "10.0.0.1", "example.com"]:
        err = io.StringIO()
        code = run_serve(host=bad_host, stderr=err)
        assert code == 1
        assert "Non-loopback host" in err.getvalue()
        assert "prohibited for local development" in err.getvalue()


def test_27_serve_invalid_port_and_ws_max_size() -> None:
    """Verify serve rejects out-of-range port and ws_max_size values."""
    err_port = io.StringIO()
    assert run_serve(port=0, stderr=err_port) == 1
    assert "Invalid port" in err_port.getvalue()

    err_port2 = io.StringIO()
    assert run_serve(port=70000, stderr=err_port2) == 1
    assert "Invalid port" in err_port2.getvalue()

    err_ws = io.StringIO()
    assert run_serve(ws_max_size=500, stderr=err_ws) == 1
    assert "Invalid ws_max_size" in err_ws.getvalue()


def test_28_demo_invalid_port_and_timeout() -> None:
    """Verify demo rejects out-of-range ports and non-finite/negative timeouts."""
    for bad_port in [-1, 65536, 99999]:
        err = io.StringIO()
        assert run_demo(port=bad_port, stderr=err) == 1
        assert "Invalid port" in err.getvalue()

    for bad_timeout in [0.0, -5.0, 5000.0, float("nan"), float("inf")]:
        err = io.StringIO()
        assert run_demo(timeout=bad_timeout, stderr=err) == 1
        assert "Invalid timeout" in err.getvalue()


def test_29_demo_auth_rejection_fails_closed() -> None:
    """Verify demo fails closed when token authentication fails."""
    # Issue a token with an unauthorized/mismatched signature
    with patch(
        "kaiwen_gateway.transports.synthetic.tokens.SyntheticTokenVerifier.issue_token",
        return_value="forged.invalid.token",
    ):
        out = io.StringIO()
        err = io.StringIO()
        code = run_demo(port=0, timeout=2.0, stdout=out, stderr=err)
        assert code == 1
        assert "Result: FAIL" in err.getvalue()


def test_30_demo_call_started_state_mismatch() -> None:
    """Verify demo fails closed when call.started event reports non-connected state."""
    import json

    from websockets.asyncio.client import ClientConnection

    orig_recv = ClientConnection.recv

    call_count = 0

    async def mock_recv(self: Any, *args: Any, **kwargs: Any) -> str:
        nonlocal call_count
        call_count += 1
        res = await orig_recv(self, *args, **kwargs)
        if call_count == 2:
            # Modify call.started response to indicate failed state
            data = json.loads(res)
            data["state"] = "failed"
            return json.dumps(data)
        return str(res)

    with patch.object(ClientConnection, "recv", mock_recv):
        out = io.StringIO()
        err = io.StringIO()
        code = run_demo(port=0, timeout=3.0, stdout=out, stderr=err)
        assert code == 1
        assert "Result: FAIL" in err.getvalue()
        assert "Unexpected call state: failed" in err.getvalue()


def test_31_demo_ping_pong_seq_mismatch() -> None:
    """Verify demo fails closed when session.pong seq number mismatches."""
    import json

    from websockets.asyncio.client import ClientConnection

    orig_recv = ClientConnection.recv

    call_count = 0

    async def mock_recv(self: Any, *args: Any, **kwargs: Any) -> str:
        nonlocal call_count
        call_count += 1
        res = await orig_recv(self, *args, **kwargs)
        if call_count == 3:
            # Modify session.pong response to have wrong seq
            data = json.loads(res)
            data["seq"] = 999
            return json.dumps(data)
        return str(res)

    with patch.object(ClientConnection, "recv", mock_recv):
        out = io.StringIO()
        err = io.StringIO()
        code = run_demo(port=0, timeout=3.0, stdout=out, stderr=err)
        assert code == 1
        assert "Result: FAIL" in err.getvalue()
        assert "Unexpected ping response" in err.getvalue()


def test_32_demo_call_ended_state_mismatch() -> None:
    """Verify demo fails closed when call.ended reports unexpected state."""
    import json

    from websockets.asyncio.client import ClientConnection

    orig_recv = ClientConnection.recv

    call_count = 0

    async def mock_recv(self: Any, *args: Any, **kwargs: Any) -> str:
        nonlocal call_count
        call_count += 1
        res = await orig_recv(self, *args, **kwargs)
        if call_count == 4:
            # Modify call.ended response to have wrong state
            data = json.loads(res)
            data["state"] = "still_connected"
            return json.dumps(data)
        return str(res)

    with patch.object(ClientConnection, "recv", mock_recv):
        out = io.StringIO()
        err = io.StringIO()
        code = run_demo(port=0, timeout=3.0, stdout=out, stderr=err)
        assert code == 1
        assert "Result: FAIL" in err.getvalue()
        assert "Unexpected call state: still_connected" in err.getvalue()


def test_33_demo_active_call_leak_detected() -> None:
    """Verify demo detects and reports active call leaks in the auth bridge."""
    from kaiwen_gateway.transports.synthetic.auth import SyntheticAuthBridge

    # Mock _active_calls on bridge to simulate lingering call
    orig_init = SyntheticAuthBridge.__init__

    def mock_init(self: Any, *args: Any, **kwargs: Any) -> None:
        orig_init(self, *args, **kwargs)
        # Add a dummy active call that is not removed
        self._active_calls[("tenant-demo", "lingering-call-99")] = "conn-99"

    with patch.object(SyntheticAuthBridge, "__init__", mock_init):
        out = io.StringIO()
        err = io.StringIO()
        code = run_demo(port=0, timeout=5.0, stdout=out, stderr=err)
        assert code == 1
        assert "Active calls remaining: 1" in err.getvalue()


def test_34_is_loopback_helper() -> None:
    """Verify _is_loopback helper handles invalid and non-string inputs safely."""
    from kaiwen_gateway.server.cli import _is_loopback

    assert _is_loopback("127.0.0.1") is True
    assert _is_loopback("localhost") is True
    assert _is_loopback("::1") is True
    assert _is_loopback("127.0.0.99") is True
    assert _is_loopback("0.0.0.0") is False
    assert _is_loopback("192.168.1.1") is False
    assert _is_loopback("") is False
    assert _is_loopback("   ") is False
    assert _is_loopback(None) is False  # type: ignore[arg-type]


def test_35_demo_ready_call_id_mismatch() -> None:
    """Verify demo fails closed when session.ready call_id mismatches."""
    import json

    from websockets.asyncio.client import ClientConnection

    orig_recv = ClientConnection.recv
    call_count = 0

    async def mock_recv(self: Any, *args: Any, **kwargs: Any) -> str:
        nonlocal call_count
        call_count += 1
        res = await orig_recv(self, *args, **kwargs)
        if call_count == 1:
            data = json.loads(res)
            data["call_id"] = "mismatched-call-id"
            return json.dumps(data)
        return str(res)

    with patch.object(ClientConnection, "recv", mock_recv):
        out = io.StringIO()
        err = io.StringIO()
        code = run_demo(port=0, timeout=3.0, stdout=out, stderr=err)
        assert code == 1
        assert "Call ID mismatch in session.ready" in err.getvalue()


def test_36_demo_call_started_event_mismatch() -> None:
    """Verify demo fails closed when call.started event type is unexpected."""
    import json

    from websockets.asyncio.client import ClientConnection

    orig_recv = ClientConnection.recv
    call_count = 0

    async def mock_recv(self: Any, *args: Any, **kwargs: Any) -> str:
        nonlocal call_count
        call_count += 1
        res = await orig_recv(self, *args, **kwargs)
        if call_count == 2:
            data = json.loads(res)
            data["event"] = "call.rejected"
            return json.dumps(data)
        return str(res)

    with patch.object(ClientConnection, "recv", mock_recv):
        out = io.StringIO()
        err = io.StringIO()
        code = run_demo(port=0, timeout=3.0, stdout=out, stderr=err)
        assert code == 1
        assert "Unexpected call start response: call.rejected" in err.getvalue()


def test_37_demo_call_ended_event_mismatch() -> None:
    """Verify demo fails closed when call.ended event type is unexpected."""
    import json

    from websockets.asyncio.client import ClientConnection

    orig_recv = ClientConnection.recv
    call_count = 0

    async def mock_recv(self: Any, *args: Any, **kwargs: Any) -> str:
        nonlocal call_count
        call_count += 1
        res = await orig_recv(self, *args, **kwargs)
        if call_count == 4:
            data = json.loads(res)
            data["event"] = "unexpected.event"
            return json.dumps(data)
        return str(res)

    with patch.object(ClientConnection, "recv", mock_recv):
        out = io.StringIO()
        err = io.StringIO()
        code = run_demo(port=0, timeout=3.0, stdout=out, stderr=err)
        assert code == 1
        assert "Unexpected call end response: unexpected.event" in err.getvalue()


def test_38_demo_actual_port_undetermined_fallback() -> None:
    """Verify demo fails closed if actual bound port cannot be determined."""
    import asyncio

    import uvicorn

    async def mock_serve(self: Any, *args: Any, **kwargs: Any) -> None:
        self.started = True
        self.servers = []
        while not self.should_exit:
            await asyncio.sleep(0.01)

    with patch.object(uvicorn.Server, "serve", mock_serve):
        out = io.StringIO()
        err = io.StringIO()
        code = run_demo(port=0, timeout=1.0, stdout=out, stderr=err)
        assert code == 1
        assert "Could not determine bound port from server sockets" in err.getvalue()
