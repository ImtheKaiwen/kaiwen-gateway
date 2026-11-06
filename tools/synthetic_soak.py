"""Synthetic Telephony Load, Soak & Fault Simulation Runner (M3.3B).

Exclusively exercises Kaiwen Gateway's synthetic telephony control plane under
staged concurrency, capacity saturation, protocol fault injection, and sustained
or repeated soak testing over loopback WebSockets.

Scope & Invariants:
1. Control Plane Only: Tests connection handshakes, tokens, admission, lifecycle
   (call.start, call.started, call.end, call.ended, ping/pong). Media streaming
   is strictly deferred to M4.
2. Zero Core Dependencies: Standalone developer tool requiring optional 'server'
   and 'dev' extras. Base package library remains strictly zero-dependency.
3. Measurement Distinction: Local loopback control-event RTTs measure Python/ASGI
   coordination, NOT audio transport latency or NFR-PERF-004's 50ms voice target.
4. Memory Metrics: Tracemalloc measures Python heap allocation, not OS process RSS.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import dataclasses
import json
import math
import platform
import secrets
import sys
import time
import tracemalloc
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, TextIO

import uvicorn
import websockets
import websockets.exceptions

from kaiwen_gateway import __version__ as gateway_version
from kaiwen_gateway.core.admission import AdmissionController
from kaiwen_gateway.core.config import (
    DEFAULT_MAX_CONCURRENT_CONNECTIONS,
    DEFAULT_MAX_CONNECTIONS_PER_TENANT,
    INGRESS_MODE_SYNTHETIC,
    CoreConfig,
)
from kaiwen_gateway.core.draining import (
    DrainPolicy,
    InstanceDrainCoordinator,
)
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

RUNNER_VERSION = "0.1.0"
DEFAULT_TEST_SECRET = secrets.token_bytes(32)


# ==============================================================================
# Data Models & Metrics
# ==============================================================================


@dataclass
class CallLatencyRecord:
    """Individual call lifecycle timing records in milliseconds."""

    handshake_ms: float = 0.0
    call_start_rtt_ms: float = 0.0
    ping_rtt_ms: float = 0.0
    call_end_rtt_ms: float = 0.0
    cleanup_ms: float = 0.0
    total_duration_ms: float = 0.0
    observed_events: int = 0


@dataclass
class MetricSummary:
    """Statistical summary of a latency metric."""

    count: int
    min_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float
    mean_ms: float
    is_small_sample: bool = False

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


def compute_metric_summary(values: Sequence[float]) -> MetricSummary:
    """Calculate min, p50, p95, p99, max, and mean for a sequence of millisecond measurements."""
    if not values:
        return MetricSummary(
            count=0,
            min_ms=0.0,
            p50_ms=0.0,
            p95_ms=0.0,
            p99_ms=0.0,
            max_ms=0.0,
            mean_ms=0.0,
            is_small_sample=False,
        )

    s = sorted(values)
    n = len(s)

    def _percentile(p: float) -> float:
        k = (n - 1) * p
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return s[int(k)]
        return s[f] * (c - k) + s[c] * (k - f)

    return MetricSummary(
        count=n,
        min_ms=round(s[0], 2),
        p50_ms=round(_percentile(0.50), 2),
        p95_ms=round(_percentile(0.95), 2),
        p99_ms=round(_percentile(0.99), 2),
        max_ms=round(s[-1], 2),
        mean_ms=round(sum(s) / n, 2),
        is_small_sample=n < 20,
    )


@dataclass
class StageResult:
    """Execution results for a specific scenario stage."""

    scenario: str
    status: str  # "PASS" | "FAIL"
    connection_attempts: int
    successful_admissions: int
    expected_rejections: int
    unexpected_failures: int
    completed_calls: int
    completed_iterations: int
    requested_concurrency: int
    achieved_concurrency: int
    peak_active_connections: int
    final_active_connections: int
    active_leases_after_cleanup: int
    duration_seconds: float
    configured_duration_seconds: float
    events_per_second: float
    latencies: dict[str, MetricSummary]
    capacity_before: dict[str, Any]
    capacity_after: dict[str, Any]
    observed_events: int = 0
    min_active_connections: int = 0
    max_active_connections: int = 0
    sustained_holding_seconds: float = 0.0
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d = dataclasses.asdict(self)
        d["latencies"] = {k: v.to_dict() for k, v in self.latencies.items()}
        return d


def resolve_sustained_tenant_distribution(
    *,
    concurrency: int,
    tenant_distribution: dict[str, int] | None = None,
    max_connections_per_tenant: int = DEFAULT_MAX_CONNECTIONS_PER_TENANT,
    max_concurrent_connections: int = DEFAULT_MAX_CONCURRENT_CONNECTIONS,
) -> dict[str, int]:
    """Resolve and validate tenant distribution for sustained soak stage.

    Invariants:
    1. Distribution total must strictly match concurrency.
    2. No tenant may exceed max_connections_per_tenant.
    3. Concurrency must not exceed max_concurrent_connections.
    4. Validation must occur BEFORE starting the WebSocket server.
    """
    if concurrency <= 0:
        raise ValueError(f"Concurrency must be positive (got {concurrency})")
    if concurrency > max_concurrent_connections:
        raise ValueError(
            f"Requested concurrency {concurrency} exceeds max capacity {max_concurrent_connections}"
        )

    if tenant_distribution is None:
        if concurrency == 1:
            return {"tenant_a": 1}
        if concurrency == 5:
            return {"tenant_a": 5}
        if concurrency == 25:
            return {"tenant_a": 10, "tenant_b": 10, "tenant_c": 5}
        if concurrency <= max_connections_per_tenant:
            return {"tenant_a": concurrency}

        # Generic even distribution respecting max_connections_per_tenant
        dist: dict[str, int] = {}
        remaining = concurrency
        idx = 0
        while remaining > 0:
            chunk = min(remaining, max_connections_per_tenant)
            dist[f"tenant_{chr(ord('a') + idx)}"] = chunk
            remaining -= chunk
            idx += 1
        return dist

    total = sum(tenant_distribution.values())
    if total != concurrency:
        raise ValueError(
            f"Mismatched tenant distribution: sum of connections ({total}) does not equal requested concurrency ({concurrency})"
        )

    for tid, count in tenant_distribution.items():
        if count <= 0:
            raise ValueError(f"Tenant count for '{tid}' must be positive (got {count})")
        if count > max_connections_per_tenant:
            raise ValueError(
                f"Tenant count {count} for '{tid}' exceeds per-tenant limit {max_connections_per_tenant}"
            )

    return dict(tenant_distribution)


@dataclass
class SoakReport:
    """Full execution report containing environment, stages, and resource stats."""

    runner_version: str
    gateway_version: str
    git_commit: str
    python_version: str
    os_platform: str
    cpu_info: str
    workload: dict[str, Any]
    stages: list[StageResult] = field(default_factory=list)
    overall_status: str = "NOT_RUN"
    total_duration_seconds: float = 0.0
    cpu_time_seconds: float = 0.0
    heap_memory_peak_kib: float = 0.0
    limitations: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "runner_version": self.runner_version,
            "gateway_version": self.gateway_version,
            "git_commit": self.git_commit,
            "python_version": self.python_version,
            "os_platform": self.os_platform,
            "cpu_info": self.cpu_info,
            "workload": self.workload,
            "stages": [s.to_dict() for s in self.stages],
            "overall_status": self.overall_status,
            "total_duration_seconds": round(self.total_duration_seconds, 3),
            "cpu_time_seconds": round(self.cpu_time_seconds, 3),
            "heap_memory_peak_kib": round(self.heap_memory_peak_kib, 2),
            "limitations": self.limitations,
        }


def get_git_commit() -> str:
    """Attempt to resolve the current Git commit SHA."""
    try:
        import subprocess

        res = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=2.0,
        )
        return res.stdout.strip()
    except Exception:
        return "unknown"


# ==============================================================================
# In-Process Server Lifecycle Context
# ==============================================================================


@dataclass
class RunningServerContext:
    """Active loopback test server instance and bound internal controllers."""

    app: Any
    server: uvicorn.Server
    server_task: asyncio.Task[None]
    port: int
    ws_base_url: str
    verifier: SyntheticTokenVerifier
    bridge: SyntheticAuthBridge
    controller: AdmissionController
    coordinator: InstanceDrainCoordinator


@contextlib.asynccontextmanager
async def start_ephemeral_soak_server(
    *,
    port: int = 0,
    max_concurrent_connections: int = DEFAULT_MAX_CONCURRENT_CONNECTIONS,
    max_connections_per_tenant: int = DEFAULT_MAX_CONNECTIONS_PER_TENANT,
    log_level: str = "warning",
    secret_key: bytes | None = None,
    enable_replay_protection: bool = True,
) -> Any:
    """Start an isolated loopback ASGI server configured for synthetic transport."""
    key = secret_key if secret_key is not None else secrets.token_bytes(32)
    verifier = SyntheticTokenVerifier(
        secret_key=key,
        enable_replay_protection=enable_replay_protection,
    )
    bridge = SyntheticAuthBridge(verifier=verifier, enforce_call_exclusivity=True)
    config = CoreConfig(
        instance_name="synthetic-soak-server",
        ingress_mode=INGRESS_MODE_SYNTHETIC,
        max_concurrent_connections=max_concurrent_connections,
        max_connections_per_tenant=max_connections_per_tenant,
    )
    app = create_gateway_app(config=config, synthetic_auth_bridge=bridge)

    uv_cfg = uvicorn.Config(
        app=app,
        host="127.0.0.1",
        port=port,
        log_level=log_level,
        ws="auto",
    )
    server = uvicorn.Server(uv_cfg)
    server_task = asyncio.create_task(server.serve())

    start_deadline = time.monotonic() + 5.0
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
        raise RuntimeError(f"Soak server failed to start on 127.0.0.1:{port} within timeout")

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
        raise RuntimeError("Could not determine bound loopback port from server sockets")

    ctx = RunningServerContext(
        app=app,
        server=server,
        server_task=server_task,
        port=actual_port,
        ws_base_url=f"ws://127.0.0.1:{actual_port}",
        verifier=verifier,
        bridge=bridge,
        controller=app.state.admission_controller,
        coordinator=app.state.drain_coordinator,
    )

    try:
        yield ctx
    finally:
        server.should_exit = True
        try:
            await asyncio.wait_for(server_task, timeout=5.0)
        except TimeoutError:
            if not server_task.done():
                server_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await server_task
        except Exception:
            if not server_task.done():
                server_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await server_task


# ==============================================================================
# Call Client Protocol Simulation
# ==============================================================================


async def run_single_synthetic_call(
    *,
    ws_endpoint_url: str,
    token: str,
    call_id: str,
    start_barrier: asyncio.Barrier | None = None,
    end_barrier: asyncio.Barrier | None = None,
    timeout: float = 10.0,
    seq: int = 42,
) -> CallLatencyRecord:
    """Execute full single-call synthetic lifecycle and return measured latencies."""
    record = CallLatencyRecord()
    t_global_start = time.perf_counter_ns()

    headers = {"Authorization": f"Bearer {token}"}

    # 1. Handshake & session.ready
    t0 = time.perf_counter_ns()
    async with websockets.connect(ws_endpoint_url, additional_headers=headers) as ws:
        ready_raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
        ready_data = json.loads(ready_raw)
        if ready_data.get("event") != EVENT_SESSION_READY:
            raise RuntimeError(f"Unexpected initial event: {ready_data.get('event')}")
        if ready_data.get("call_id") != call_id:
            raise RuntimeError(
                f"Call ID mismatch in session.ready: expected {call_id}, got {ready_data.get('call_id')}"
            )
        record.observed_events += 1
        record.handshake_ms = (time.perf_counter_ns() - t0) / 1e6

        # 2. call.start -> call.started
        t1 = time.perf_counter_ns()
        start_frame = {
            "event": EVENT_CALL_START,
            "stream_id": f"stream_{call_id}",
            "metadata": {"caller": "+15550001", "callee": "+15550002"},
        }
        await ws.send(json.dumps(start_frame))
        record.observed_events += 1
        started_raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
        started_data = json.loads(started_raw)
        if (
            started_data.get("event") != EVENT_CALL_STARTED
            or started_data.get("state") != "connected"
        ):
            raise RuntimeError(f"Unexpected call start response: {started_data}")
        record.observed_events += 1
        record.call_start_rtt_ms = (time.perf_counter_ns() - t1) / 1e6

        # Synchronization barrier: align active concurrency across all client tasks
        if start_barrier is not None:
            await start_barrier.wait()

        # 3. Heartbeat ping -> pong
        t2 = time.perf_counter_ns()
        await ws.send(json.dumps({"event": EVENT_SESSION_PING, "seq": seq}))
        record.observed_events += 1
        pong_raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
        pong_data = json.loads(pong_raw)
        if pong_data.get("event") != EVENT_SESSION_PONG or pong_data.get("seq") != seq:
            raise RuntimeError(f"Unexpected ping response: {pong_data}")
        record.observed_events += 1
        record.ping_rtt_ms = (time.perf_counter_ns() - t2) / 1e6

        # Optional synchronization barrier before teardown
        if end_barrier is not None:
            await end_barrier.wait()

        # 4. call.end -> call.ended
        t3 = time.perf_counter_ns()
        end_frame = {"event": EVENT_CALL_END, "reason": "completed", "code": 16}
        await ws.send(json.dumps(end_frame))
        record.observed_events += 1
        ended_raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
        ended_data = json.loads(ended_raw)
        if ended_data.get("event") != EVENT_CALL_ENDED or ended_data.get("state") != "disconnected":
            raise RuntimeError(f"Unexpected call end response: {ended_data}")
        record.observed_events += 1
        record.call_end_rtt_ms = (time.perf_counter_ns() - t3) / 1e6

        # 5. session.end -> clean 1000 close
        t4 = time.perf_counter_ns()
        await ws.send(json.dumps({"event": EVENT_SESSION_END}))
        record.observed_events += 1
        try:
            extra_msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
            raise RuntimeError(f"Unexpected post-session message received: {extra_msg!r}")
        except TimeoutError:
            raise RuntimeError("WebSocket failed to close after session.end (timeout)") from None
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
        record.cleanup_ms = (time.perf_counter_ns() - t4) / 1e6

    record.total_duration_ms = (time.perf_counter_ns() - t_global_start) / 1e6
    return record


# ==============================================================================
# Concurrency Scenario Runners
# ==============================================================================


async def run_concurrency_stage(
    server_ctx: RunningServerContext,
    *,
    concurrency: int,
    tenant_distribution: dict[str, int] | None = None,
    timeout: float = 10.0,
) -> StageResult:
    """Execute coordinated concurrent synthetic calls and record latency distribution."""
    snap_before = server_ctx.controller.snapshot()
    cap_before = {
        "active_connections": snap_before.active_connections,
        "available_capacity": snap_before.available_capacity,
        "active_tenants": snap_before.active_tenants,
    }

    ws_url = f"{server_ctx.ws_base_url}{SYNTHETIC_WS_ENDPOINT_PATH}"
    barrier = asyncio.Barrier(concurrency + 1)  # N clients + 1 inspector coordinator

    # Establish distribution across tenants
    if tenant_distribution is None:
        tenant_distribution = {"tenant_default": concurrency}

    call_specs: list[tuple[str, str, str]] = []  # (tenant_id, call_id, token)
    for t_id, count in tenant_distribution.items():
        for i in range(count):
            c_id = f"call_{t_id}_{i}_{secrets.token_hex(4)}"
            tok = server_ctx.verifier.issue_token(
                provider_id="soak_provider",
                tenant_id=t_id,
                call_id=c_id,
                allowed_scopes=["voice.stream"],
                ttl_seconds=60.0,
            )
            call_specs.append((t_id, c_id, tok))

    records: list[CallLatencyRecord] = []
    failed_sessions = 0
    t_start = time.perf_counter_ns()

    async def _worker(t_id: str, c_id: str, tok: str, idx: int) -> None:
        nonlocal failed_sessions
        try:
            rec = await run_single_synthetic_call(
                ws_endpoint_url=ws_url,
                token=tok,
                call_id=c_id,
                start_barrier=barrier,
                timeout=timeout,
                seq=100 + idx,
            )
            records.append(rec)
        except Exception as err:
            failed_sessions += 1
            await barrier.abort()
            raise err

    # Spawn concurrent client tasks
    tasks = [
        asyncio.create_task(_worker(t_id, c_id, tok, idx))
        for idx, (t_id, c_id, tok) in enumerate(call_specs)
    ]

    # Inspector coordinator: awaits all calls reaching active state
    achieved_active = 0
    stage_error: str | None = None
    try:
        await asyncio.wait_for(barrier.wait(), timeout=timeout + 5.0)
        # While all calls are held at barrier, verify concurrent active capacity
        snap_mid = server_ctx.controller.snapshot()
        achieved_active = snap_mid.active_connections
        if achieved_active != concurrency:
            stage_error = f"Achieved active concurrency mismatch: expected {concurrency}, got {achieved_active}"
        if len(server_ctx.bridge._active_calls) != concurrency:
            stage_error = f"Active calls registry mismatch: expected {concurrency}, got {len(server_ctx.bridge._active_calls)}"
        for t_id, exp_count in tenant_distribution.items():
            actual_t = server_ctx.controller.active_connections_for_tenant(t_id)
            if actual_t != exp_count:
                stage_error = f"Tenant {t_id} active connections mismatch: expected {exp_count}, got {actual_t}"
    except (TimeoutError, asyncio.BrokenBarrierError) as err:
        stage_error = f"Barrier alignment failed: {err}"
        await barrier.abort()
        for t in tasks:
            if not t.done():
                t.cancel()

    # Wait for all client tasks to complete teardown
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for res in results:
        if isinstance(res, Exception) and stage_error is None:
            stage_error = str(res)

    duration_sec = (time.perf_counter_ns() - t_start) / 1e9

    # Verify complete post-stage cleanup
    snap_after = server_ctx.controller.snapshot()
    cap_after = {
        "active_connections": snap_after.active_connections,
        "available_capacity": snap_after.available_capacity,
        "active_tenants": snap_after.active_tenants,
    }

    if (
        snap_after.active_connections != 0
        or snap_after.active_tenants != 0
        or snap_after.available_capacity != snap_after.max_concurrent_connections
        or len(server_ctx.bridge._active_calls) != 0
        or server_ctx.coordinator.active_connections_count != 0
    ):
        cleanup_leak = (
            f"Resource leak detected after stage: active_connections={snap_after.active_connections}, "
            f"active_calls={len(server_ctx.bridge._active_calls)}, "
            f"available_capacity={snap_after.available_capacity}"
        )
        stage_error = cleanup_leak if stage_error is None else f"{stage_error}; {cleanup_leak}"

    # Compile latency summaries
    latencies = {
        "handshake": compute_metric_summary([r.handshake_ms for r in records]),
        "call_start_rtt": compute_metric_summary([r.call_start_rtt_ms for r in records]),
        "ping_rtt": compute_metric_summary([r.ping_rtt_ms for r in records]),
        "call_end_rtt": compute_metric_summary([r.call_end_rtt_ms for r in records]),
        "cleanup": compute_metric_summary([r.cleanup_ms for r in records]),
        "total_duration": compute_metric_summary([r.total_duration_ms for r in records]),
    }

    observed_events = sum(r.observed_events for r in records)
    eps = round(observed_events / duration_sec, 1) if duration_sec > 0 else 0.0

    return StageResult(
        scenario=f"concurrency_{concurrency}",
        status="PASS" if stage_error is None and failed_sessions == 0 else "FAIL",
        connection_attempts=concurrency,
        successful_admissions=len(records),
        expected_rejections=0,
        unexpected_failures=failed_sessions,
        completed_calls=len(records),
        completed_iterations=1,
        requested_concurrency=concurrency,
        achieved_concurrency=achieved_active,
        peak_active_connections=achieved_active,
        final_active_connections=snap_after.active_connections,
        active_leases_after_cleanup=server_ctx.coordinator.active_connections_count,
        duration_seconds=round(duration_sec, 3),
        configured_duration_seconds=timeout,
        events_per_second=eps,
        latencies=latencies,
        capacity_before=cap_before,
        capacity_after=cap_after,
        observed_events=observed_events,
        min_active_connections=achieved_active,
        max_active_connections=achieved_active,
        sustained_holding_seconds=0.0,
        error=stage_error,
    )


# ==============================================================================
# Capacity Saturation Runner
# ==============================================================================


async def run_capacity_saturation_stage(
    server_ctx: RunningServerContext,
    *,
    timeout: float = 15.0,
    _fail_holder_after_admission: bool = False,
) -> StageResult:
    """Validate per-tenant and global connection quotas fail-closed during HTTP upgrade handshake."""
    snap_before = server_ctx.controller.snapshot()
    cap_before = {
        "active_connections": snap_before.active_connections,
        "available_capacity": snap_before.available_capacity,
        "active_tenants": snap_before.active_tenants,
    }

    ws_url = f"{server_ctx.ws_base_url}{SYNTHETIC_WS_ENDPOINT_PATH}"
    t_start = time.perf_counter_ns()
    stage_error: str | None = None
    admitted_count = 0
    expected_rejections = 0
    unexpected_failures = 0
    completed_calls = 0
    total_attempts = 0
    total_observed_events = 0
    tenant_peak_active = 0
    global_peak_active = 0

    # 1. Per-tenant saturation test: fill tenant_alpha to limit (10 connections)
    barrier_tenant = asyncio.Barrier(11)  # 10 clients + 1 inspector
    end_tenant = asyncio.Event()

    async def _tenant_holder(c_id: str, tok: str, idx: int) -> None:
        nonlocal admitted_count, completed_calls, total_observed_events
        try:
            async with websockets.connect(
                ws_url, additional_headers={"Authorization": f"Bearer {tok}"}
            ) as ws:
                ready_raw = await ws.recv()  # session.ready
                ready_data = json.loads(ready_raw)
                if ready_data.get("event") != EVENT_SESSION_READY:
                    raise RuntimeError(f"Unexpected initial event: {ready_data.get('event')}")
                admitted_count += 1
                total_observed_events += 1

                if _fail_holder_after_admission and idx == 0:
                    raise RuntimeError("Simulated tenant holder failure after admission")

                await ws.send(
                    json.dumps(
                        {
                            "event": EVENT_CALL_START,
                            "stream_id": f"str_{c_id}",
                            "metadata": {"caller": "+1001"},
                        }
                    )
                )
                total_observed_events += 1
                started_raw = await ws.recv()  # call.started
                started_data = json.loads(started_raw)
                if started_data.get("event") != EVENT_CALL_STARTED:
                    raise RuntimeError(f"Unexpected start event: {started_data}")
                total_observed_events += 1

                await asyncio.wait_for(barrier_tenant.wait(), timeout=timeout)
                await end_tenant.wait()

                await ws.send(
                    json.dumps({"event": EVENT_CALL_END, "reason": "completed", "code": 16})
                )
                total_observed_events += 1
                ended_raw = await ws.recv()  # call.ended
                ended_data = json.loads(ended_raw)
                if ended_data.get("event") == EVENT_CALL_ENDED:
                    completed_calls += 1
                total_observed_events += 1

                await ws.send(json.dumps({"event": EVENT_SESSION_END}))
                total_observed_events += 1
                with contextlib.suppress(Exception):
                    await ws.recv()
        except Exception:
            with contextlib.suppress(Exception):
                await barrier_tenant.abort()
            raise

    tenant_tokens = [
        (
            f"call_t1_{i}",
            server_ctx.verifier.issue_token(
                provider_id="soak_provider",
                tenant_id="tenant_alpha",
                call_id=f"call_t1_{i}",
                allowed_scopes=["voice.stream"],
            ),
        )
        for i in range(10)
    ]
    t_tasks = [
        asyncio.create_task(_tenant_holder(cid, tok, i))
        for i, (cid, tok) in enumerate(tenant_tokens)
    ]
    total_attempts += 10

    try:
        await asyncio.wait_for(barrier_tenant.wait(), timeout=timeout)
        snap_t_mid = server_ctx.controller.snapshot()
        tenant_peak_active = snap_t_mid.active_connections
        if tenant_peak_active != 10:
            stage_error = (
                f"Expected 10 active connections for tenant saturation, got {tenant_peak_active}"
            )

        # Tenant alpha is at limit 10/10; attempt 11th connection for tenant_alpha
        total_attempts += 1
        tok_11 = server_ctx.verifier.issue_token(
            provider_id="soak_provider",
            tenant_id="tenant_alpha",
            call_id="call_t1_overflow",
            allowed_scopes=["voice.stream"],
        )
        try:
            async with websockets.connect(
                ws_url, additional_headers={"Authorization": f"Bearer {tok_11}"}
            ):
                stage_error = (
                    "Expected 11th connection for tenant_alpha to be rejected with HTTP 403"
                )
                unexpected_failures += 1
        except websockets.exceptions.InvalidStatus as err:
            if err.response.status_code == 403:
                expected_rejections += 1
            else:
                stage_error = (
                    f"Expected HTTP 403 for tenant overflow, got {err.response.status_code}"
                )
                unexpected_failures += 1

        # Connect 1st call for tenant_beta (should succeed because global capacity 10/25 has room)
        total_attempts += 1
        tok_beta = server_ctx.verifier.issue_token(
            provider_id="soak_provider",
            tenant_id="tenant_beta",
            call_id="call_beta_01",
            allowed_scopes=["voice.stream"],
        )
        async with websockets.connect(
            ws_url, additional_headers={"Authorization": f"Bearer {tok_beta}"}
        ) as ws_b:
            ready_b = await ws_b.recv()  # session.ready
            if json.loads(ready_b).get("event") == EVENT_SESSION_READY:
                admitted_count += 1
                total_observed_events += 1
            # Note: tok_beta only connects and ends session; it does NOT start a call, so completed_calls is NOT incremented!
            await ws_b.send(json.dumps({"event": EVENT_SESSION_END}))
            total_observed_events += 1
            with contextlib.suppress(Exception):
                await ws_b.recv()
    except (TimeoutError, asyncio.BrokenBarrierError) as b_err:
        stage_error = f"Tenant saturation barrier failed: {b_err}"
        with contextlib.suppress(Exception):
            await barrier_tenant.abort()
        unexpected_failures += 1
    finally:
        end_tenant.set()
        with contextlib.suppress(Exception):
            await barrier_tenant.abort()
        t_results = await asyncio.gather(*t_tasks, return_exceptions=True)
        for res in t_results:
            if isinstance(res, Exception):
                unexpected_failures += 1
                if stage_error is None or "barrier failed" in stage_error:
                    stage_error = f"Tenant holder failed: {res}"

    # 2. Global saturation test: fill all 25 connections across tenants (10 on tA, 10 on tB, 5 on tC)
    barrier_global = asyncio.Barrier(26)  # 25 clients + 1 inspector
    end_global = asyncio.Event()

    async def _global_holder(t_id: str, c_id: str, tok: str) -> None:
        nonlocal admitted_count, completed_calls, total_observed_events
        try:
            async with websockets.connect(
                ws_url, additional_headers={"Authorization": f"Bearer {tok}"}
            ) as ws:
                ready_raw = await ws.recv()
                ready_data = json.loads(ready_raw)
                if ready_data.get("event") != EVENT_SESSION_READY:
                    raise RuntimeError(f"Unexpected initial event: {ready_data.get('event')}")
                admitted_count += 1
                total_observed_events += 1

                await ws.send(
                    json.dumps(
                        {
                            "event": EVENT_CALL_START,
                            "stream_id": f"str_{c_id}",
                            "metadata": {"caller": "+1001"},
                        }
                    )
                )
                total_observed_events += 1
                started_raw = await ws.recv()
                started_data = json.loads(started_raw)
                if started_data.get("event") != EVENT_CALL_STARTED:
                    raise RuntimeError(f"Unexpected start event: {started_data}")
                total_observed_events += 1

                await asyncio.wait_for(barrier_global.wait(), timeout=timeout + 5.0)
                await end_global.wait()

                await ws.send(
                    json.dumps({"event": EVENT_CALL_END, "reason": "completed", "code": 16})
                )
                total_observed_events += 1
                ended_raw = await ws.recv()
                ended_data = json.loads(ended_raw)
                if ended_data.get("event") == EVENT_CALL_ENDED:
                    completed_calls += 1
                total_observed_events += 1

                await ws.send(json.dumps({"event": EVENT_SESSION_END}))
                total_observed_events += 1
                with contextlib.suppress(Exception):
                    await ws.recv()
        except Exception:
            with contextlib.suppress(Exception):
                await barrier_global.abort()
            raise

    g_specs: list[tuple[str, str, str]] = []
    for i in range(10):
        g_specs.append(
            (
                "tenant_gA",
                f"call_gA_{i}",
                server_ctx.verifier.issue_token(
                    provider_id="soak_provider",
                    tenant_id="tenant_gA",
                    call_id=f"call_gA_{i}",
                    allowed_scopes=["voice.stream"],
                ),
            )
        )
    for i in range(10):
        g_specs.append(
            (
                "tenant_gB",
                f"call_gB_{i}",
                server_ctx.verifier.issue_token(
                    provider_id="soak_provider",
                    tenant_id="tenant_gB",
                    call_id=f"call_gB_{i}",
                    allowed_scopes=["voice.stream"],
                ),
            )
        )
    for i in range(5):
        g_specs.append(
            (
                "tenant_gC",
                f"call_gC_{i}",
                server_ctx.verifier.issue_token(
                    provider_id="soak_provider",
                    tenant_id="tenant_gC",
                    call_id=f"call_gC_{i}",
                    allowed_scopes=["voice.stream"],
                ),
            )
        )

    g_tasks = [asyncio.create_task(_global_holder(tid, cid, tok)) for tid, cid, tok in g_specs]
    total_attempts += 25

    try:
        await asyncio.wait_for(barrier_global.wait(), timeout=timeout + 5.0)
        snap_g_mid = server_ctx.controller.snapshot()
        global_peak_active = snap_g_mid.active_connections
        if global_peak_active != 25:
            stage_error = (
                f"Expected 25 active connections for global saturation, got {global_peak_active}"
            )

        # Global capacity 25/25 is saturated. Attempt 26th call for tenant_gC (which only has 5/10 tenant usage)
        total_attempts += 1
        tok_26 = server_ctx.verifier.issue_token(
            provider_id="soak_provider",
            tenant_id="tenant_gC",
            call_id="call_gC_overflow_26",
            allowed_scopes=["voice.stream"],
        )
        try:
            async with websockets.connect(
                ws_url, additional_headers={"Authorization": f"Bearer {tok_26}"}
            ):
                stage_error = (
                    "Expected 26th connection to be rejected with HTTP 403 on global saturation"
                )
                unexpected_failures += 1
        except websockets.exceptions.InvalidStatus as err:
            if err.response.status_code == 403:
                expected_rejections += 1
            else:
                stage_error = (
                    f"Expected HTTP 403 for global overflow, got {err.response.status_code}"
                )
                unexpected_failures += 1
    except (TimeoutError, asyncio.BrokenBarrierError) as b_err:
        stage_error = f"Global saturation barrier failed: {b_err}"
        with contextlib.suppress(Exception):
            await barrier_global.abort()
        unexpected_failures += 1
    finally:
        end_global.set()
        with contextlib.suppress(Exception):
            await barrier_global.abort()
        g_results = await asyncio.gather(*g_tasks, return_exceptions=True)
        for res in g_results:
            if isinstance(res, Exception):
                unexpected_failures += 1
                if stage_error is None or "barrier failed" in stage_error:
                    stage_error = f"Global holder failed: {res}"

    duration_sec = (time.perf_counter_ns() - t_start) / 1e9

    # Verify zero capacity or lease leaks
    snap_after = server_ctx.controller.snapshot()
    cap_after = {
        "active_connections": snap_after.active_connections,
        "available_capacity": snap_after.available_capacity,
        "active_tenants": snap_after.active_tenants,
    }

    if (
        snap_after.active_connections != 0
        or snap_after.available_capacity != snap_after.max_concurrent_connections
        or len(server_ctx.bridge._active_calls) != 0
        or server_ctx.coordinator.active_connections_count != 0
    ):
        leak_msg = f"Resource leak after saturation stage: {cap_after}"
        stage_error = leak_msg if stage_error is None else f"{stage_error}; {leak_msg}"

    eps = round(total_observed_events / duration_sec, 1) if duration_sec > 0 else 0.0
    peak_active = max(tenant_peak_active, global_peak_active)

    return StageResult(
        scenario="capacity_saturation",
        status="PASS" if stage_error is None and unexpected_failures == 0 else "FAIL",
        connection_attempts=total_attempts,
        successful_admissions=admitted_count,
        expected_rejections=expected_rejections,
        unexpected_failures=unexpected_failures,
        completed_calls=completed_calls,
        completed_iterations=1,
        requested_concurrency=25,
        achieved_concurrency=peak_active,
        peak_active_connections=peak_active,
        final_active_connections=snap_after.active_connections,
        active_leases_after_cleanup=server_ctx.coordinator.active_connections_count,
        duration_seconds=round(duration_sec, 3),
        configured_duration_seconds=timeout,
        events_per_second=eps,
        latencies={},
        capacity_before=cap_before,
        capacity_after=cap_after,
        observed_events=total_observed_events,
        min_active_connections=0,
        max_active_connections=peak_active,
        sustained_holding_seconds=0.0,
        error=stage_error,
    )


# ==============================================================================
# Protocol & Fault Injection Runner
# ==============================================================================


async def run_fault_injection_stage(
    server_ctx: RunningServerContext,
    *,
    timeout: float = 15.0,
) -> StageResult:
    """Execute a comprehensive matrix of synthetic protocol, authentication, and lifecycle faults."""
    snap_before = server_ctx.controller.snapshot()
    cap_before = {
        "active_connections": snap_before.active_connections,
        "available_capacity": snap_before.available_capacity,
        "active_tenants": snap_before.active_tenants,
    }

    ws_url = f"{server_ctx.ws_base_url}{SYNTHETIC_WS_ENDPOINT_PATH}"
    t_start = time.perf_counter_ns()
    stage_error: str | None = None
    total_attempts = 0
    successful_admissions = 0
    expected_rejections = 0
    unexpected_failures = 0
    completed_calls = 0
    total_observed_events = 0

    # 1. Invalid forged token signature
    total_attempts += 1
    valid_tok = server_ctx.verifier.issue_token(
        provider_id="soak_provider",
        tenant_id="fault_tenant",
        call_id="call_f1",
        allowed_scopes=["voice.stream"],
    )
    forged_tok = valid_tok[:-5] + ("aaaaa" if valid_tok[-5:] != "aaaaa" else "bbbbb")
    try:
        async with websockets.connect(
            ws_url, additional_headers={"Authorization": f"Bearer {forged_tok}"}
        ):
            stage_error = "Expected forged token to be rejected with 403"
            unexpected_failures += 1
    except websockets.exceptions.InvalidStatus as err:
        if err.response.status_code == 403:
            expected_rejections += 1
        else:
            stage_error = f"Expected 403 for forged token, got {err.response.status_code}"
            unexpected_failures += 1

    # 2. Replay token rejection
    total_attempts += 2
    replay_tok = server_ctx.verifier.issue_token(
        provider_id="soak_provider",
        tenant_id="fault_tenant",
        call_id="call_f2",
        allowed_scopes=["voice.stream"],
    )
    # First use -> success
    async with websockets.connect(
        ws_url, additional_headers={"Authorization": f"Bearer {replay_tok}"}
    ) as ws:
        await ws.recv()  # session.ready
        successful_admissions += 1
        await ws.send(json.dumps({"event": EVENT_SESSION_END}))
        with contextlib.suppress(Exception):
            await ws.recv()
        completed_calls += 1

    # Second use -> replay rejection (HTTP 403)
    try:
        async with websockets.connect(
            ws_url, additional_headers={"Authorization": f"Bearer {replay_tok}"}
        ):
            stage_error = "Expected replayed token to be rejected with 403"
            unexpected_failures += 1
    except websockets.exceptions.InvalidStatus as err:
        if err.response.status_code == 403:
            expected_rejections += 1
        else:
            stage_error = f"Expected 403 for replayed token, got {err.response.status_code}"
            unexpected_failures += 1

    # 3. Missing required scope (e.g. only 'voice.read' instead of 'voice.stream')
    total_attempts += 1
    unscoped_tok = server_ctx.verifier.issue_token(
        provider_id="soak_provider",
        tenant_id="fault_tenant",
        call_id="call_f3",
        allowed_scopes=["voice.read"],
    )
    try:
        async with websockets.connect(
            ws_url, additional_headers={"Authorization": f"Bearer {unscoped_tok}"}
        ):
            stage_error = "Expected unscoped token to be rejected with 403"
            unexpected_failures += 1
    except websockets.exceptions.InvalidStatus as err:
        if err.response.status_code == 403:
            expected_rejections += 1
        else:
            stage_error = f"Expected 403 for unscoped token, got {err.response.status_code}"
            unexpected_failures += 1

    # 4. Invalid event / malformed JSON frame during session
    total_attempts += 1
    tok_inv = server_ctx.verifier.issue_token(
        provider_id="soak_provider",
        tenant_id="fault_tenant",
        call_id="call_f4",
        allowed_scopes=["voice.stream"],
    )
    async with websockets.connect(
        ws_url, additional_headers={"Authorization": f"Bearer {tok_inv}"}
    ) as ws:
        await ws.recv()  # session.ready
        successful_admissions += 1
        total_observed_events += 1
        # Send illegal malformed JSON
        await ws.send("not-valid-json-{{{")
        total_observed_events += 1
        try:
            await ws.recv()
            stage_error = "Expected malformed JSON frame to trigger connection closure"
            unexpected_failures += 1
        except websockets.exceptions.ConnectionClosed as close_err:
            close_code = close_err.rcvd.code if close_err.rcvd is not None else 1006
            if close_code == 1007:
                pass  # Correct RFC 6455 1007 Invalid Frame Payload
            else:
                stage_error = f"Expected close code 1007 for malformed frame, got {close_code}"
                unexpected_failures += 1

    # 5. Client-initiated clean WebSocket close frame (code 1001 Going Away)
    total_attempts += 1
    tok_close = server_ctx.verifier.issue_token(
        provider_id="soak_provider",
        tenant_id="fault_tenant",
        call_id="call_f5_close",
        allowed_scopes=["voice.stream"],
    )
    ws_close = await websockets.connect(
        ws_url, additional_headers={"Authorization": f"Bearer {tok_close}"}
    )
    await ws_close.recv()  # session.ready
    successful_admissions += 1
    total_observed_events += 1
    await ws_close.send(
        json.dumps(
            {
                "event": EVENT_CALL_START,
                "stream_id": "str_close",
                "metadata": {"caller": "+1001"},
            }
        )
    )
    total_observed_events += 1
    await ws_close.recv()  # call.started
    total_observed_events += 1
    assert server_ctx.controller.snapshot().active_connections >= 1
    await ws_close.close(code=1001, reason="Going away")
    await asyncio.sleep(0.1)  # Allow supervisor task to detect close frame and release lease
    assert server_ctx.controller.snapshot().active_connections == 0

    # 6. Genuine underlying transport abort without clean close handshake (TCP drop)
    # Simultaneously maintain a concurrent active call on another tenant to verify isolation!
    total_attempts += 2
    tok_abort = server_ctx.verifier.issue_token(
        provider_id="soak_provider",
        tenant_id="fault_tenant_abort",
        call_id="call_f6_abort",
        allowed_scopes=["voice.stream"],
    )
    tok_ctrl = server_ctx.verifier.issue_token(
        provider_id="soak_provider",
        tenant_id="fault_tenant_ctrl",
        call_id="call_f6_ctrl",
        allowed_scopes=["voice.stream"],
    )
    ws_abort = await websockets.connect(
        ws_url, additional_headers={"Authorization": f"Bearer {tok_abort}"}
    )
    ws_ctrl = await websockets.connect(
        ws_url, additional_headers={"Authorization": f"Bearer {tok_ctrl}"}
    )
    await ws_abort.recv()  # ready
    await ws_ctrl.recv()  # ready
    successful_admissions += 2
    total_observed_events += 2

    await ws_abort.send(
        json.dumps(
            {
                "event": EVENT_CALL_START,
                "stream_id": "str_abort",
                "metadata": {},
            }
        )
    )
    await ws_ctrl.send(
        json.dumps(
            {
                "event": EVENT_CALL_START,
                "stream_id": "str_ctrl",
                "metadata": {},
            }
        )
    )
    total_observed_events += 2
    await ws_abort.recv()  # started
    await ws_ctrl.recv()  # started
    total_observed_events += 2
    assert server_ctx.controller.snapshot().active_connections == 2

    # Abruptly abort raw transport socket without sending WebSocket close frame
    if hasattr(ws_abort, "transport") and ws_abort.transport is not None:
        ws_abort.transport.abort()
    else:
        await ws_abort.close(code=1006)
    await asyncio.sleep(0.15)  # Allow server-side supervisor to process EOF/disconnect

    # Verify aborted connection lease released, but control client on other tenant is UNAFFECTED
    assert server_ctx.controller.snapshot().active_connections == 1
    assert "call_f6_abort" not in [c for t, c in server_ctx.bridge._active_calls.keys()]
    # Control client continues healthy exchange
    await ws_ctrl.send(json.dumps({"event": EVENT_SESSION_PING, "seq": 999}))
    pong_ctrl = await ws_ctrl.recv()
    assert json.loads(pong_ctrl).get("event") == EVENT_SESSION_PONG
    total_observed_events += 2
    await ws_ctrl.send(json.dumps({"event": EVENT_CALL_END, "reason": "completed", "code": 16}))
    await ws_ctrl.recv()
    total_observed_events += 2
    await ws_ctrl.send(json.dumps({"event": EVENT_SESSION_END}))
    total_observed_events += 1
    with contextlib.suppress(Exception):
        await ws_ctrl.recv()
    completed_calls += 1

    # 7. Duplicate call.start on already active call
    total_attempts += 1
    tok_dup = server_ctx.verifier.issue_token(
        provider_id="soak_provider",
        tenant_id="fault_tenant",
        call_id="call_f7_dup",
        allowed_scopes=["voice.stream"],
    )
    async with websockets.connect(
        ws_url, additional_headers={"Authorization": f"Bearer {tok_dup}"}
    ) as ws:
        await ws.recv()  # session.ready
        successful_admissions += 1
        total_observed_events += 1
        await ws.send(
            json.dumps(
                {
                    "event": EVENT_CALL_START,
                    "stream_id": "str_dup",
                    "metadata": {"caller": "+1001"},
                }
            )
        )
        total_observed_events += 1
        await ws.recv()  # call.started
        total_observed_events += 1
        # Send duplicate call.start
        await ws.send(
            json.dumps(
                {
                    "event": EVENT_CALL_START,
                    "stream_id": "str_dup_2",
                    "metadata": {"caller": "+1001"},
                }
            )
        )
        total_observed_events += 1
        try:
            await ws.recv()
            stage_error = "Expected duplicate call.start to close connection with 1008"
            unexpected_failures += 1
        except websockets.exceptions.ConnectionClosed as close_err:
            close_code = close_err.rcvd.code if close_err.rcvd is not None else 1006
            if close_code == 1008:
                pass  # Correct 1008 Policy Violation
            else:
                stage_error = f"Expected close code 1008 for duplicate start, got {close_code}"
                unexpected_failures += 1

    # 8. Server draining with active call
    total_attempts += 2
    tok_drain = server_ctx.verifier.issue_token(
        provider_id="soak_provider",
        tenant_id="fault_tenant",
        call_id="call_f8_active",
        allowed_scopes=["voice.stream"],
    )
    ws_drain = await websockets.connect(
        ws_url, additional_headers={"Authorization": f"Bearer {tok_drain}"}
    )
    await ws_drain.recv()  # session.ready
    successful_admissions += 1
    total_observed_events += 1
    # Initiate coordinator drain with short grace period
    drain_task = asyncio.create_task(
        server_ctx.coordinator.drain(policy=DrainPolicy(grace_period=0.2, force_close_timeout=0.2))
    )
    # Verify new admission during draining is rejected
    tok_drained = server_ctx.verifier.issue_token(
        provider_id="soak_provider",
        tenant_id="fault_tenant",
        call_id="call_f8_rejected",
        allowed_scopes=["voice.stream"],
    )
    try:
        async with websockets.connect(
            ws_url, additional_headers={"Authorization": f"Bearer {tok_drained}"}
        ):
            stage_error = "Expected connection to be rejected during draining state"
            unexpected_failures += 1
    except (websockets.exceptions.InvalidStatus, websockets.exceptions.ConnectionClosed):
        expected_rejections += 1

    # Complete active call and await drain completion
    await ws_drain.send(json.dumps({"event": EVENT_SESSION_END}))
    total_observed_events += 1
    with contextlib.suppress(Exception):
        await ws_drain.recv()
    await drain_task
    completed_calls += 1

    duration_sec = (time.perf_counter_ns() - t_start) / 1e9

    # Verify zero leaks
    snap_after = server_ctx.controller.snapshot()
    cap_after = {
        "active_connections": snap_after.active_connections,
        "available_capacity": snap_after.available_capacity,
        "active_tenants": snap_after.active_tenants,
    }

    if (
        snap_after.active_connections != 0
        or snap_after.available_capacity != snap_after.max_concurrent_connections
        or len(server_ctx.bridge._active_calls) != 0
        or server_ctx.coordinator.active_connections_count != 0
    ):
        leak_msg = f"Resource leak after fault injection stage: {cap_after}"
        stage_error = leak_msg if stage_error is None else f"{stage_error}; {leak_msg}"

    eps = round(total_observed_events / duration_sec, 1) if duration_sec > 0 else 0.0

    return StageResult(
        scenario="fault_injection",
        status="PASS" if stage_error is None and unexpected_failures == 0 else "FAIL",
        connection_attempts=total_attempts,
        successful_admissions=successful_admissions,
        expected_rejections=expected_rejections,
        unexpected_failures=unexpected_failures,
        completed_calls=completed_calls,
        completed_iterations=1,
        requested_concurrency=1,
        achieved_concurrency=1,
        peak_active_connections=2,
        final_active_connections=snap_after.active_connections,
        active_leases_after_cleanup=server_ctx.coordinator.active_connections_count,
        duration_seconds=round(duration_sec, 3),
        configured_duration_seconds=timeout,
        events_per_second=eps,
        latencies={},
        capacity_before=cap_before,
        capacity_after=cap_after,
        observed_events=total_observed_events,
        min_active_connections=0,
        max_active_connections=2,
        sustained_holding_seconds=0.0,
        error=stage_error,
    )


# ==============================================================================
# Repeated Soak Test Runner
# ==============================================================================


async def run_repeated_soak_stage(
    server_ctx: RunningServerContext,
    *,
    concurrency: int = 5,
    max_duration: float = 30.0,
    iterations: int = 5,
) -> StageResult:
    """Execute bounded repeated connect/call/disconnect cycles verifying capacity recovery at each cycle."""
    snap_before = server_ctx.controller.snapshot()
    cap_before = {
        "active_connections": snap_before.active_connections,
        "available_capacity": snap_before.available_capacity,
        "active_tenants": snap_before.active_tenants,
    }

    total_attempts = 0
    admitted = 0
    completed_calls = 0
    unexpected_failures = 0
    total_observed_events = 0
    stage_error: str | None = None
    completed_iterations = 0
    t_start = time.perf_counter_ns()
    deadline = t_start + int(max_duration * 1e9)

    for cycle in range(iterations):
        if time.perf_counter_ns() >= deadline:
            break

        res = await run_concurrency_stage(
            server_ctx,
            concurrency=concurrency,
            timeout=10.0,
        )
        total_attempts += res.connection_attempts
        admitted += res.successful_admissions
        completed_calls += res.completed_calls
        unexpected_failures += res.unexpected_failures
        total_observed_events += res.observed_events
        completed_iterations += 1

        if res.status != "PASS":
            stage_error = f"Cycle {cycle + 1} failed: {res.error}"
            break

    duration_sec = (time.perf_counter_ns() - t_start) / 1e9

    snap_after = server_ctx.controller.snapshot()
    cap_after = {
        "active_connections": snap_after.active_connections,
        "available_capacity": snap_after.available_capacity,
        "active_tenants": snap_after.active_tenants,
    }

    eps = round(total_observed_events / duration_sec, 1) if duration_sec > 0 else 0.0

    return StageResult(
        scenario=f"repeated_soak_{concurrency}",
        status="PASS" if stage_error is None and unexpected_failures == 0 else "FAIL",
        connection_attempts=total_attempts,
        successful_admissions=admitted,
        expected_rejections=0,
        unexpected_failures=unexpected_failures,
        completed_calls=completed_calls,
        completed_iterations=completed_iterations,
        requested_concurrency=concurrency,
        achieved_concurrency=concurrency,
        peak_active_connections=concurrency,
        final_active_connections=snap_after.active_connections,
        active_leases_after_cleanup=server_ctx.coordinator.active_connections_count,
        duration_seconds=round(duration_sec, 3),
        configured_duration_seconds=max_duration,
        events_per_second=eps,
        latencies={},
        capacity_before=cap_before,
        capacity_after=cap_after,
        observed_events=total_observed_events,
        min_active_connections=0,
        max_active_connections=concurrency,
        sustained_holding_seconds=0.0,
        error=stage_error,
    )


# ==============================================================================
# Sustained Soak Runner (Long-Running Opt-In Concurrency)
# ==============================================================================


async def run_sustained_soak_stage(
    server_ctx: RunningServerContext,
    *,
    concurrency: int = 25,
    duration: float = 60.0,
    tenant_distribution: dict[str, int] | None = None,
    ping_interval: float = 5.0,
    sample_interval: float = 1.0,
    timeout: float = 75.0,
    _fail_worker_before_barrier: bool = False,
) -> StageResult:
    """Execute sustained active concurrent calls held over target duration with keepalives and periodic sampling."""
    snap_before = server_ctx.controller.snapshot()
    cap_before = {
        "active_connections": snap_before.active_connections,
        "available_capacity": snap_before.available_capacity,
        "active_tenants": snap_before.active_tenants,
    }

    # Pre-validate and resolve tenant distribution against server limits
    resolved_dist = resolve_sustained_tenant_distribution(
        concurrency=concurrency,
        tenant_distribution=tenant_distribution,
        max_connections_per_tenant=server_ctx.controller.max_connections_per_tenant,
        max_concurrent_connections=server_ctx.controller.max_concurrent_connections,
    )

    ws_url = f"{server_ctx.ws_base_url}{SYNTHETIC_WS_ENDPOINT_PATH}"
    t_start = time.perf_counter_ns()

    call_specs: list[tuple[str, str, str]] = []
    for t_id, count in resolved_dist.items():
        for i in range(count):
            c_id = f"call_sustained_{t_id}_{i}_{secrets.token_hex(4)}"
            tok = server_ctx.verifier.issue_token(
                provider_id="soak_provider",
                tenant_id=t_id,
                call_id=c_id,
                allowed_scopes=["voice.stream"],
                ttl_seconds=max(duration + 120.0, 300.0),
            )
            call_specs.append((t_id, c_id, tok))

    start_barrier = asyncio.Barrier(concurrency + 1)
    hold_event = asyncio.Event()

    records: list[CallLatencyRecord] = []
    failed_sessions = 0
    completed_calls = 0
    admitted_count = 0
    stage_error: str | None = None

    async def _sustained_client(t_id: str, c_id: str, tok: str, idx: int) -> None:
        nonlocal failed_sessions, completed_calls, admitted_count
        rec = CallLatencyRecord()
        t_global_start = time.perf_counter_ns()
        headers = {"Authorization": f"Bearer {tok}"}

        try:
            t0 = time.perf_counter_ns()
            async with websockets.connect(ws_url, additional_headers=headers) as ws:
                ready_raw = await asyncio.wait_for(ws.recv(), timeout=10.0)
                ready_data = json.loads(ready_raw)
                if ready_data.get("event") != EVENT_SESSION_READY:
                    raise RuntimeError(f"Unexpected initial event: {ready_data.get('event')}")
                rec.handshake_ms = (time.perf_counter_ns() - t0) / 1e6
                rec.observed_events += 1
                admitted_count += 1

                if _fail_worker_before_barrier and idx == 0:
                    raise RuntimeError("Simulated sustained client failure before barrier")

                # Start call
                t1 = time.perf_counter_ns()
                start_frame = {
                    "event": EVENT_CALL_START,
                    "stream_id": f"stream_{c_id}",
                    "metadata": {"caller": f"+1555{idx:04d}"},
                }
                await ws.send(json.dumps(start_frame))
                rec.observed_events += 1
                started_raw = await asyncio.wait_for(ws.recv(), timeout=10.0)
                started_data = json.loads(started_raw)
                if (
                    started_data.get("event") != EVENT_CALL_STARTED
                    or started_data.get("state") != "connected"
                ):
                    raise RuntimeError(f"Unexpected call start response: {started_data}")
                rec.call_start_rtt_ms = (time.perf_counter_ns() - t1) / 1e6
                rec.observed_events += 1

                # Synchronize active concurrency: all clients wait here with bounded timeout
                barrier_timeout = max(10.0, timeout / 2.0)
                await asyncio.wait_for(start_barrier.wait(), timeout=barrier_timeout)

                # Sustained holding loop with periodic keepalives
                seq = 1000 + idx
                ping_rtts: list[float] = []
                while not hold_event.is_set():
                    # Send keepalive ping
                    t_ping = time.perf_counter_ns()
                    seq += 1
                    await ws.send(json.dumps({"event": EVENT_SESSION_PING, "seq": seq}))
                    rec.observed_events += 1
                    pong_raw = await asyncio.wait_for(ws.recv(), timeout=10.0)
                    pong_data = json.loads(pong_raw)
                    if pong_data.get("event") != EVENT_SESSION_PONG or pong_data.get("seq") != seq:
                        raise RuntimeError(f"Ping mismatch: {pong_data}")
                    rec.observed_events += 1
                    ping_rtts.append((time.perf_counter_ns() - t_ping) / 1e6)

                    # Sleep until next ping or until hold_event is set
                    try:
                        await asyncio.wait_for(hold_event.wait(), timeout=ping_interval)
                    except TimeoutError:
                        pass

                if ping_rtts:
                    rec.ping_rtt_ms = sum(ping_rtts) / len(ping_rtts)

                # Clean teardown
                t3 = time.perf_counter_ns()
                await ws.send(
                    json.dumps({"event": EVENT_CALL_END, "reason": "completed", "code": 16})
                )
                rec.observed_events += 1
                ended_raw = await asyncio.wait_for(ws.recv(), timeout=10.0)
                ended_data = json.loads(ended_raw)
                if ended_data.get("event") != EVENT_CALL_ENDED:
                    raise RuntimeError(f"Unexpected call end response: {ended_data}")
                rec.observed_events += 1
                rec.call_end_rtt_ms = (time.perf_counter_ns() - t3) / 1e6

                t4 = time.perf_counter_ns()
                await ws.send(json.dumps({"event": EVENT_SESSION_END}))
                rec.observed_events += 1
                try:
                    await asyncio.wait_for(ws.recv(), timeout=5.0)
                except websockets.exceptions.ConnectionClosed as exc:
                    close_code = exc.rcvd.code if exc.rcvd is not None else (ws.close_code or 1006)
                    if close_code != 1000:
                        raise RuntimeError(f"Unexpected close code {close_code}") from None
                rec.cleanup_ms = (time.perf_counter_ns() - t4) / 1e6
                rec.total_duration_ms = (time.perf_counter_ns() - t_global_start) / 1e6

                completed_calls += 1
                records.append(rec)
        except Exception as err:
            failed_sessions += 1
            with contextlib.suppress(Exception):
                await start_barrier.abort()
            hold_event.set()
            raise err

    tasks = [
        asyncio.create_task(_sustained_client(tid, cid, tok, i))
        for i, (tid, cid, tok) in enumerate(call_specs)
    ]

    sampled_active: list[int] = []

    async def _active_sampler() -> None:
        while not hold_event.is_set():
            snap = server_ctx.controller.snapshot()
            sampled_active.append(snap.active_connections)
            try:
                await asyncio.wait_for(hold_event.wait(), timeout=sample_interval)
            except TimeoutError:
                pass

    sampler_task: asyncio.Task[None] | None = None
    sustained_holding_sec = 0.0

    try:
        # 1. Await all clients reaching active connected state at the start barrier
        barrier_timeout = max(15.0, timeout / 2.0)
        await asyncio.wait_for(start_barrier.wait(), timeout=barrier_timeout)

        # 2. Verify all active connections initially
        snap_initial = server_ctx.controller.snapshot()
        achieved_active = snap_initial.active_connections
        if achieved_active != concurrency:
            stage_error = f"Expected {concurrency} active connections, got {achieved_active}"
        if len(server_ctx.bridge._active_calls) != concurrency:
            stage_error = f"Expected {concurrency} registered active calls, got {len(server_ctx.bridge._active_calls)}"

        # 3. Start sampler during the verified sustained holding window
        sampler_task = asyncio.create_task(_active_sampler())

        # 4. Hold active concurrent calls for the specified duration
        t_hold_start = time.perf_counter_ns()
        await asyncio.sleep(duration)
        sustained_holding_sec = (time.perf_counter_ns() - t_hold_start) / 1e9

    except (TimeoutError, asyncio.BrokenBarrierError) as b_err:
        stage_error = f"Start barrier alignment failed: {b_err}"
        with contextlib.suppress(Exception):
            await start_barrier.abort()
    finally:
        hold_event.set()
        with contextlib.suppress(Exception):
            await start_barrier.abort()
        if sampler_task is not None:
            await sampler_task
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, Exception) and stage_error is None:
                stage_error = str(r)

    duration_sec = (time.perf_counter_ns() - t_start) / 1e9

    snap_after = server_ctx.controller.snapshot()
    cap_after = {
        "active_connections": snap_after.active_connections,
        "available_capacity": snap_after.available_capacity,
        "active_tenants": snap_after.active_tenants,
    }

    if (
        snap_after.active_connections != 0
        or snap_after.available_capacity != snap_after.max_concurrent_connections
        or len(server_ctx.bridge._active_calls) != 0
        or server_ctx.coordinator.active_connections_count != 0
    ):
        cleanup_leak = (
            f"Resource leak after sustained stage: active_connections={snap_after.active_connections}, "
            f"active_calls={len(server_ctx.bridge._active_calls)}, "
            f"available_capacity={snap_after.available_capacity}"
        )
        stage_error = cleanup_leak if stage_error is None else f"{stage_error}; {cleanup_leak}"

    min_sampled = min(sampled_active) if sampled_active else 0
    max_sampled = max(sampled_active) if sampled_active else 0

    latencies = {
        "handshake": compute_metric_summary([r.handshake_ms for r in records]),
        "call_start_rtt": compute_metric_summary([r.call_start_rtt_ms for r in records]),
        "ping_rtt": compute_metric_summary([r.ping_rtt_ms for r in records]),
        "call_end_rtt": compute_metric_summary([r.call_end_rtt_ms for r in records]),
        "cleanup": compute_metric_summary([r.cleanup_ms for r in records]),
        "total_duration": compute_metric_summary([r.total_duration_ms for r in records]),
    }

    observed_events = sum(r.observed_events for r in records)
    eps = round(observed_events / duration_sec, 1) if duration_sec > 0 else 0.0

    return StageResult(
        scenario=f"sustained_soak_{concurrency}",
        status="PASS" if stage_error is None and failed_sessions == 0 else "FAIL",
        connection_attempts=concurrency,
        successful_admissions=admitted_count,
        expected_rejections=0,
        unexpected_failures=failed_sessions,
        completed_calls=completed_calls,
        completed_iterations=1,
        requested_concurrency=concurrency,
        achieved_concurrency=concurrency,
        peak_active_connections=max_sampled,
        final_active_connections=snap_after.active_connections,
        active_leases_after_cleanup=server_ctx.coordinator.active_connections_count,
        duration_seconds=round(duration_sec, 3),
        configured_duration_seconds=duration,
        events_per_second=eps,
        latencies=latencies,
        capacity_before=cap_before,
        capacity_after=cap_after,
        observed_events=observed_events,
        min_active_connections=min_sampled,
        max_active_connections=max_sampled,
        sustained_holding_seconds=round(sustained_holding_sec, 3),
        error=stage_error,
    )


# ==============================================================================
# Suite Execution Coordinator & CLI Dispatcher
# ==============================================================================


async def execute_soak_suite(
    *,
    scenario: str = "concurrency",
    concurrency: int = 1,
    duration: float = 10.0,
    iterations: int = 1,
    port: int = 0,
    log_level: str = "warning",
) -> SoakReport:
    """Execute the configured soak scenarios and compile a complete SoakReport."""
    tracemalloc.start()
    t_cpu_start = time.process_time()
    t_wall_start = time.perf_counter_ns()

    report = SoakReport(
        runner_version=RUNNER_VERSION,
        gateway_version=gateway_version,
        git_commit=get_git_commit(),
        python_version=platform.python_version(),
        os_platform=platform.platform(),
        cpu_info=platform.processor() or "unknown",
        workload={
            "scenario": scenario,
            "concurrency": concurrency,
            "duration_seconds": duration,
            "iterations": iterations,
            "port": port,
        },
        limitations=[
            "Local control-event RTT measured over loopback; does NOT reflect audio transport latency or NFR-PERF-004 50ms voice guarantee.",
            "Memory metrics reflect Python heap allocation via tracemalloc, not total OS process RSS.",
            "CPU time reflects runner process user+system CPU seconds.",
            "Replay protection is process-local and in-memory.",
        ],
    )

    all_passed = True

    try:
        if scenario in ("concurrency", "all"):
            # Stage A: 1 call
            async with start_ephemeral_soak_server(port=port, log_level=log_level) as s_ctx:
                r1 = await run_concurrency_stage(s_ctx, concurrency=1, timeout=duration)
                report.stages.append(r1)
                if r1.status != "PASS":
                    all_passed = False

            # Stage B: 5 concurrent calls
            if concurrency >= 5 or scenario == "all":
                async with start_ephemeral_soak_server(port=port, log_level=log_level) as s_ctx:
                    r5 = await run_concurrency_stage(s_ctx, concurrency=5, timeout=duration)
                    report.stages.append(r5)
                    if r5.status != "PASS":
                        all_passed = False

            # Stage C: 25 concurrent calls (10 tA, 10 tB, 5 tC)
            if concurrency >= 25 or scenario == "all":
                async with start_ephemeral_soak_server(port=port, log_level=log_level) as s_ctx:
                    t_dist = {"tenant_a": 10, "tenant_b": 10, "tenant_c": 5}
                    r25 = await run_concurrency_stage(
                        s_ctx, concurrency=25, tenant_distribution=t_dist, timeout=duration
                    )
                    report.stages.append(r25)
                    if r25.status != "PASS":
                        all_passed = False

        if scenario in ("saturation", "all"):
            async with start_ephemeral_soak_server(port=port, log_level=log_level) as s_ctx:
                r_sat = await run_capacity_saturation_stage(s_ctx, timeout=duration)
                report.stages.append(r_sat)
                if r_sat.status != "PASS":
                    all_passed = False

        if scenario in ("fault", "all"):
            async with start_ephemeral_soak_server(port=port, log_level=log_level) as s_ctx:
                r_fault = await run_fault_injection_stage(s_ctx, timeout=duration)
                report.stages.append(r_fault)
                if r_fault.status != "PASS":
                    all_passed = False

        if scenario in ("soak", "all"):
            async with start_ephemeral_soak_server(port=port, log_level=log_level) as s_ctx:
                r_soak = await run_repeated_soak_stage(
                    s_ctx,
                    concurrency=min(concurrency, 5),
                    max_duration=duration,
                    iterations=max(iterations, 3),
                )
                report.stages.append(r_soak)
                if r_soak.status != "PASS":
                    all_passed = False

        if scenario == "sustained":
            # GATE A: Pre-flight validation of distribution BEFORE ephemeral server start
            resolved_dist = resolve_sustained_tenant_distribution(
                concurrency=concurrency,
                tenant_distribution=None,
            )
            async with start_ephemeral_soak_server(port=port, log_level=log_level) as s_ctx:
                r_sustained = await run_sustained_soak_stage(
                    s_ctx,
                    concurrency=concurrency,
                    duration=duration,
                    tenant_distribution=resolved_dist,
                )
                report.stages.append(r_sustained)
                if r_sustained.status != "PASS":
                    all_passed = False

    finally:
        report.total_duration_seconds = (time.perf_counter_ns() - t_wall_start) / 1e9
        report.cpu_time_seconds = time.process_time() - t_cpu_start
        _cur_mem, peak_mem = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        report.heap_memory_peak_kib = peak_mem / 1024.0

    report.overall_status = "PASS" if all_passed and report.stages else "FAIL"
    return report


def render_console_report(report: SoakReport, out: TextIO) -> None:
    """Format and print an informative, clean human-readable console report."""
    sep = "=" * 80
    subsep = "-" * 80

    out.write(f"\n{sep}\n")
    out.write("  KAIWEN GATEWAY -- SYNTHETIC LOAD & SOAK TEST REPORT (M3.3B)\n")
    out.write(f"{sep}\n")
    out.write(f"Runner Version    : {report.runner_version}\n")
    out.write(f"Gateway Version   : {report.gateway_version} (commit: {report.git_commit[:10]})\n")
    out.write(f"Environment       : Python {report.python_version} on {report.os_platform}\n")
    out.write(f"Processor / CPU   : {report.cpu_info}\n")
    out.write(f"Workload Config   : {report.workload}\n")
    out.write(f"Wall Duration     : {report.total_duration_seconds:.3f} s\n")
    out.write(f"CPU Process Time  : {report.cpu_time_seconds:.3f} s\n")
    out.write(f"Heap Peak Memory  : {report.heap_memory_peak_kib:.1f} KiB (tracemalloc)\n")
    out.write(f"Overall Result    : {report.overall_status}\n")
    out.write(f"{sep}\n\n")

    out.write("STAGES SUMMARY:\n")
    out.write(f"{subsep}\n")
    out.write(
        f"{'Scenario':<22} {'Req':<4} {'Peak':<5} {'Attempts':<9} {'Admitted':<9} {'Rej':<5} {'Calls':<6} {'Status':<7} {'Duration':<8}\n"
    )
    out.write(f"{subsep}\n")
    for s in report.stages:
        out.write(
            f"{s.scenario:<22} {s.requested_concurrency:<4} {s.peak_active_connections:<5} {s.connection_attempts:<9} "
            f"{s.successful_admissions:<9} {s.expected_rejections:<5} {s.completed_calls:<6} {s.status:<7} {s.duration_seconds:<8.3f}s\n"
        )
        if s.sustained_holding_seconds > 0:
            out.write(
                f"   [Sustained hold: {s.sustained_holding_seconds:.2f}s | Min sampled: {s.min_active_connections} | Max sampled: {s.max_active_connections}]\n"
            )
        if s.error:
            out.write(f"   [Error: {s.error}]\n")
    out.write(f"{subsep}\n\n")

    # Latencies
    stages_with_lat = [s for s in report.stages if s.latencies]
    if stages_with_lat:
        out.write("LATENCY PERCENTILES (milliseconds):\n")
        out.write(f"{subsep}\n")
        out.write(
            f"{'Stage':<18} {'Metric':<15} {'Count':<6} {'p50':<8} {'p95':<8} {'p99':<8} {'Max':<8} {'Mean':<8}\n"
        )
        out.write(f"{subsep}\n")
        for s in stages_with_lat:
            for m_name, m_sum in s.latencies.items():
                if m_sum.count > 0:
                    note = "*" if m_sum.is_small_sample else ""
                    out.write(
                        f"{s.scenario:<18} {m_name:<15} {m_sum.count:<6} {m_sum.p50_ms:<8.2f} "
                        f"{m_sum.p95_ms:<8.2f}{note} {m_sum.p99_ms:<8.2f}{note} {m_sum.max_ms:<8.2f} {m_sum.mean_ms:<8.2f}\n"
                    )
        out.write(f"{subsep}\n")
        out.write("  (*) Note: Sample size < 20; p95/p99 values are indicative only.\n\n")

    out.write("RESOURCE RECOVERY:\n")
    out.write(f"{subsep}\n")
    for s in report.stages:
        out.write(
            f"[{s.scenario}] Active Connections: {s.capacity_after.get('active_connections')} | "
            f"Available Capacity: {s.capacity_after.get('available_capacity')} / 25 | "
            f"Active Tenants: {s.capacity_after.get('active_tenants')}\n"
        )
    out.write(f"{subsep}\n")
    out.flush()


def write_json_report(report: SoakReport, file_path: str) -> None:
    """Write machine-readable JSON representation of SoakReport to file."""
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(report.to_dict(), f, indent=2)


def build_arg_parser() -> argparse.ArgumentParser:
    """Construct command line argument parser for synthetic_soak runner."""
    parser = argparse.ArgumentParser(
        prog="synthetic_soak",
        description="Kaiwen Gateway — Synthetic Load & Soak Test Runner (M3.3B).",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        choices=[1, 5, 25],
        default=1,
        help="Target concurrent call volume to exercise (1, 5, or 25).",
    )
    parser.add_argument(
        "--scenario",
        choices=["concurrency", "saturation", "fault", "soak", "sustained", "all"],
        default="concurrency",
        help="Test scenario to execute.",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=10.0,
        help="Maximum duration for the run or individual stage in seconds (default: 10.0).",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=1,
        help="Number of repeated cycles for soak mode (default: 1).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=0,
        help="Loopback port to bind ephemeral server (default: 0 for atomic OS assignment).",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Optional path to output machine-readable JSON report.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional integer seed for test timing / jitter determinism.",
    )
    parser.add_argument(
        "--log-level",
        choices=["critical", "error", "warning", "info", "debug"],
        default="warning",
        help="Uvicorn server log level (default: warning).",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Main CLI entrypoint for synthetic_soak."""
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.duration <= 0 or not math.isfinite(args.duration) or args.duration > 3600:
        sys.stderr.write(
            f"Error: Invalid --duration {args.duration}. Must be finite and <= 3600s.\n"
        )
        return 1

    if args.iterations < 1 or args.iterations > 10000:
        sys.stderr.write(
            f"Error: Invalid --iterations {args.iterations}. Must be between 1 and 10000.\n"
        )
        return 1

    if args.port < 0 or args.port > 65535:
        sys.stderr.write(f"Error: Invalid --port {args.port}. Must be between 0 and 65535.\n")
        return 1

    try:
        report = asyncio.run(
            execute_soak_suite(
                scenario=args.scenario,
                concurrency=args.concurrency,
                duration=args.duration,
                iterations=args.iterations,
                port=args.port,
                log_level=args.log_level,
            )
        )
    except ValueError as val_err:
        sys.stderr.write(f"Configuration error: {val_err}\n")
        return 1

    render_console_report(report, sys.stdout)

    if args.output:
        try:
            write_json_report(report, args.output)
            sys.stdout.write(f"Saved machine-readable JSON report to: {args.output}\n")
        except Exception as err:
            sys.stderr.write(f"Failed to write JSON report to {args.output}: {err}\n")
            return 1

    return 0 if report.overall_status == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
