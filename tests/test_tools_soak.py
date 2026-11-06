"""Tests for Synthetic Telephony Load & Soak Test Runner (M3.3B).

Verifies:
1. CLI argument parsing and default options.
2. CLI argument validation (negative duration, out-of-range iterations, invalid port).
3. Percentile computation logic and small-sample indicative flags (<20 samples).
4. SoakReport data model, dictionary serialization, and JSON file export.
5. Human-readable console report rendering.
6. In-process ephemeral loopback server context with atomic port allocation.
7. Single synthetic call execution and latency capture.
8. Stage A (1 concurrency) execution and zero-leak resource verification.
9. Capacity saturation stage (tenant and global HTTP 403 rejection).
10. Fault injection stage (8 fault classes including genuine transport abort and client close 1001).
11. Sustained soak stage (fast opt-in holding test with active connection sampling).
12. Repeated soak stage duration and iteration precedence.
13. Barrier abort on client failure (deadlock prevention).
14. End-to-end CLI execution with JSON output.
"""

from __future__ import annotations

import asyncio
import io
import json
import math
from pathlib import Path

import pytest
from tools.synthetic_soak import (
    MetricSummary,
    RunningServerContext,
    SoakReport,
    StageResult,
    build_arg_parser,
    compute_metric_summary,
    main,
    render_console_report,
    resolve_sustained_tenant_distribution,
    run_capacity_saturation_stage,
    run_concurrency_stage,
    run_fault_injection_stage,
    run_repeated_soak_stage,
    run_single_synthetic_call,
    run_sustained_soak_stage,
    start_ephemeral_soak_server,
    write_json_report,
)

from kaiwen_gateway.transports.synthetic.protocol import SYNTHETIC_WS_ENDPOINT_PATH


def test_cli_argument_parser_defaults() -> None:
    """Verify default CLI arguments."""
    parser = build_arg_parser()
    args = parser.parse_args([])
    assert args.concurrency == 1
    assert args.scenario == "concurrency"
    assert args.duration == 10.0
    assert args.iterations == 1
    assert args.port == 0
    assert args.output is None
    assert args.log_level == "warning"


def test_cli_argument_parser_custom() -> None:
    """Verify custom CLI argument parsing."""
    parser = build_arg_parser()
    args = parser.parse_args(
        [
            "--concurrency",
            "25",
            "--scenario",
            "sustained",
            "--duration",
            "60.0",
            "--iterations",
            "3",
            "--port",
            "8080",
            "--output",
            "report.json",
            "--log-level",
            "error",
        ]
    )
    assert args.concurrency == 25
    assert args.scenario == "sustained"
    assert args.duration == 60.0
    assert args.iterations == 3
    assert args.port == 8080
    assert args.output == "report.json"
    assert args.log_level == "error"


def test_cli_validation_invalid_duration(capsys: pytest.CaptureFixture[str]) -> None:
    """Verify invalid --duration fails with exit code 1."""
    ret_neg = main(["--duration", "-5.0"])
    assert ret_neg == 1
    out_neg = capsys.readouterr()
    assert "Invalid --duration" in out_neg.err

    ret_inf = main(["--duration", "inf"])
    assert ret_inf == 1
    out_inf = capsys.readouterr()
    assert "Invalid --duration" in out_inf.err


def test_cli_validation_invalid_iterations(capsys: pytest.CaptureFixture[str]) -> None:
    """Verify invalid --iterations fails with exit code 1."""
    ret_zero = main(["--iterations", "0"])
    assert ret_zero == 1
    out_zero = capsys.readouterr()
    assert "Invalid --iterations" in out_zero.err

    ret_huge = main(["--iterations", "999999"])
    assert ret_huge == 1
    out_huge = capsys.readouterr()
    assert "Invalid --iterations" in out_huge.err


def test_cli_validation_invalid_port(capsys: pytest.CaptureFixture[str]) -> None:
    """Verify invalid --port fails with exit code 1."""
    ret_neg = main(["--port", "-1"])
    assert ret_neg == 1
    out_neg = capsys.readouterr()
    assert "Invalid --port" in out_neg.err

    ret_high = main(["--port", "70000"])
    assert ret_high == 1
    out_high = capsys.readouterr()
    assert "Invalid --port" in out_high.err


def test_compute_metric_summary_empty() -> None:
    """Verify metric summary calculation on empty sequence."""
    summary = compute_metric_summary([])
    assert summary.count == 0
    assert summary.p50_ms == 0.0
    assert summary.p95_ms == 0.0
    assert summary.p99_ms == 0.0
    assert summary.mean_ms == 0.0
    assert summary.min_ms == 0.0
    assert summary.max_ms == 0.0
    assert summary.is_small_sample is False


def test_compute_metric_summary_single() -> None:
    """Verify metric summary calculation on single sample."""
    summary = compute_metric_summary([42.5])
    assert summary.count == 1
    assert summary.p50_ms == 42.5
    assert summary.p95_ms == 42.5
    assert summary.p99_ms == 42.5
    assert summary.mean_ms == 42.5
    assert summary.min_ms == 42.5
    assert summary.max_ms == 42.5
    assert summary.is_small_sample is True


def test_compute_metric_summary_small_sample() -> None:
    """Verify metric summary on small sample (< 20 items) flags is_small_sample=True."""
    vals = [float(i) for i in range(1, 11)]  # 1.0 .. 10.0
    summary = compute_metric_summary(vals)
    assert summary.count == 10
    assert summary.is_small_sample is True
    assert math.isclose(summary.min_ms, 1.0)
    assert math.isclose(summary.max_ms, 10.0)
    assert math.isclose(summary.mean_ms, 5.5)
    assert math.isclose(summary.p50_ms, 5.5)


def test_compute_metric_summary_adequate_sample() -> None:
    """Verify metric summary on adequate sample (>= 20 items) flags is_small_sample=False."""
    vals = [float(i) for i in range(1, 101)]  # 1.0 .. 100.0
    summary = compute_metric_summary(vals)
    assert summary.count == 100
    assert summary.is_small_sample is False
    assert math.isclose(summary.min_ms, 1.0)
    assert math.isclose(summary.max_ms, 100.0)
    assert math.isclose(summary.mean_ms, 50.5)
    assert math.isclose(summary.p50_ms, 50.5)
    assert math.isclose(summary.p95_ms, 95.05)
    assert math.isclose(summary.p99_ms, 99.01)


def test_soak_report_json_serialization(tmp_path: Path) -> None:
    """Verify SoakReport serialization and JSON file write."""
    report = SoakReport(
        runner_version="0.1.0",
        gateway_version="0.1.0a1",
        git_commit="d347eaed36",
        python_version="3.12.0",
        os_platform="Windows",
        cpu_info="Intel",
        workload={"scenario": "concurrency", "concurrency": 1},
        overall_status="PASS",
        total_duration_seconds=1.234,
        cpu_time_seconds=0.456,
        heap_memory_peak_kib=1024.5,
        stages=[
            StageResult(
                scenario="concurrency_1",
                status="PASS",
                connection_attempts=1,
                successful_admissions=1,
                expected_rejections=0,
                unexpected_failures=0,
                completed_calls=1,
                completed_iterations=1,
                requested_concurrency=1,
                achieved_concurrency=1,
                peak_active_connections=1,
                final_active_connections=0,
                active_leases_after_cleanup=0,
                duration_seconds=0.05,
                configured_duration_seconds=10.0,
                events_per_second=120.0,
                latencies={
                    "handshake": MetricSummary(
                        count=1,
                        min_ms=10.0,
                        p50_ms=10.0,
                        p95_ms=10.0,
                        p99_ms=10.0,
                        max_ms=10.0,
                        mean_ms=10.0,
                        is_small_sample=True,
                    )
                },
                capacity_before={"active_connections": 0},
                capacity_after={"active_connections": 0},
            )
        ],
        limitations=["Local test disclaimer."],
    )

    data = report.to_dict()
    assert data["runner_version"] == "0.1.0"
    assert data["overall_status"] == "PASS"
    assert len(data["stages"]) == 1
    assert data["stages"][0]["scenario"] == "concurrency_1"
    assert data["stages"][0]["connection_attempts"] == 1
    assert data["stages"][0]["successful_admissions"] == 1

    out_file = tmp_path / "report.json"
    write_json_report(report, str(out_file))
    assert out_file.exists()

    with open(out_file, encoding="utf-8") as f:
        loaded = json.load(f)
    assert loaded["runner_version"] == "0.1.0"
    assert loaded["overall_status"] == "PASS"


def test_console_report_rendering() -> None:
    """Verify render_console_report produces clean human-readable output."""
    report = SoakReport(
        runner_version="0.1.0",
        gateway_version="0.1.0a1",
        git_commit="d347eaed36",
        python_version="3.12.0",
        os_platform="Windows",
        cpu_info="Intel",
        workload={"scenario": "concurrency", "concurrency": 1},
        overall_status="PASS",
        total_duration_seconds=0.5,
        cpu_time_seconds=0.1,
        heap_memory_peak_kib=2048.0,
        stages=[
            StageResult(
                scenario="concurrency_1",
                status="PASS",
                connection_attempts=1,
                successful_admissions=1,
                expected_rejections=0,
                unexpected_failures=0,
                completed_calls=1,
                completed_iterations=1,
                requested_concurrency=1,
                achieved_concurrency=1,
                peak_active_connections=1,
                final_active_connections=0,
                active_leases_after_cleanup=0,
                duration_seconds=0.03,
                configured_duration_seconds=10.0,
                events_per_second=200.0,
                latencies={
                    "handshake": MetricSummary(
                        count=1,
                        min_ms=12.5,
                        p50_ms=12.5,
                        p95_ms=12.5,
                        p99_ms=12.5,
                        max_ms=12.5,
                        mean_ms=12.5,
                        is_small_sample=True,
                    )
                },
                capacity_before={
                    "active_connections": 0,
                    "available_capacity": 25,
                    "active_tenants": 0,
                },
                capacity_after={
                    "active_connections": 0,
                    "available_capacity": 25,
                    "active_tenants": 0,
                },
            )
        ],
    )

    buf = io.StringIO()
    render_console_report(report, buf)
    output = buf.getvalue()
    assert "KAIWEN GATEWAY -- SYNTHETIC LOAD & SOAK TEST REPORT" in output
    assert "concurrency_1" in output
    assert "RESOURCE RECOVERY:" in output


def test_loopback_single_call_execution() -> None:
    """Verify single synthetic call execution and latency record collection."""

    async def _run() -> None:
        async with start_ephemeral_soak_server() as s_ctx:
            assert isinstance(s_ctx, RunningServerContext)
            token = s_ctx.verifier.issue_token(
                provider_id="soak_pvd",
                tenant_id="soak_tnt",
                call_id="call_unit_01",
                allowed_scopes=["voice.stream"],
            )
            ws_url = f"{s_ctx.ws_base_url}{SYNTHETIC_WS_ENDPOINT_PATH}"
            rec = await run_single_synthetic_call(
                ws_endpoint_url=ws_url,
                token=token,
                call_id="call_unit_01",
                timeout=5.0,
            )
            assert rec.handshake_ms > 0.0
            assert rec.call_start_rtt_ms > 0.0
            assert rec.ping_rtt_ms > 0.0
            assert rec.call_end_rtt_ms > 0.0
            assert rec.cleanup_ms > 0.0
            assert rec.total_duration_ms > 0.0

            # Verify capacity zero-leak
            snap = s_ctx.controller.snapshot()
            assert snap.active_connections == 0
            assert snap.available_capacity == 25
            assert len(s_ctx.bridge._active_calls) == 0

    asyncio.run(_run())


def test_loopback_concurrency_stage_1() -> None:
    """Verify Stage A (1 call concurrency) with overlap barrier and zero leaks."""

    async def _run() -> None:
        async with start_ephemeral_soak_server() as s_ctx:
            res = await run_concurrency_stage(s_ctx, concurrency=1, timeout=5.0)
            assert res.status == "PASS"
            assert res.requested_concurrency == 1
            assert res.achieved_concurrency == 1
            assert res.successful_admissions == 1
            assert res.expected_rejections == 0
            assert res.unexpected_failures == 0
            assert res.completed_calls == 1
            assert "handshake" in res.latencies
            assert "ping_rtt" in res.latencies

            # Verify resource recovery
            snap = s_ctx.controller.snapshot()
            assert snap.active_connections == 0
            assert snap.available_capacity == 25
            assert len(s_ctx.bridge._active_calls) == 0

    asyncio.run(_run())


def test_loopback_capacity_saturation_stage() -> None:
    """Verify capacity saturation runner rejects 11th tenant connection and 26th global connection."""

    async def _run() -> None:
        async with start_ephemeral_soak_server() as s_ctx:
            res = await run_capacity_saturation_stage(s_ctx, timeout=10.0)
            assert res.status == "PASS"
            assert res.achieved_concurrency == 25
            assert res.connection_attempts == 38
            assert res.successful_admissions == 36
            assert res.expected_rejections == 2  # 1 tenant overflow + 1 global overflow
            assert (
                res.completed_calls == 35
            )  # 10 tenant + 25 global (beta is admission only, not call)
            assert res.completed_calls == 35  # 10 tenant + 25 global (beta is admission only, not call)
            assert res.unexpected_failures == 0
            assert res.error is None

            # Verify zero leaks
            snap = s_ctx.controller.snapshot()
            assert snap.active_connections == 0
            assert snap.available_capacity == 25
            assert len(s_ctx.bridge._active_calls) == 0

    asyncio.run(_run())


def test_saturation_holder_failure_yields_stage_fail() -> None:
    """Verify that a failure in a saturation holder task marks stage as FAIL and does not swallow error."""

    async def _run() -> None:
        async with start_ephemeral_soak_server() as s_ctx:
            res = await run_capacity_saturation_stage(
                s_ctx,
                timeout=5.0,
                _fail_holder_after_admission=True,
            )
            assert res.status == "FAIL"
            assert res.unexpected_failures >= 1
            assert res.error is not None
            assert "Simulated tenant holder failure after admission" in res.error

            # Verify complete resource recovery even after failure
            snap = s_ctx.controller.snapshot()
            assert snap.active_connections == 0
            assert snap.available_capacity == 25
            assert len(s_ctx.bridge._active_calls) == 0

    asyncio.run(_run())


def test_sustained_tenant_distribution_resolution_and_validation() -> None:
    """Verify tenant distribution resolution rules and validation invariants."""
    # 1 -> 1 tenant
    d1 = resolve_sustained_tenant_distribution(concurrency=1)
    assert d1 == {"tenant_a": 1}

    # 5 -> 5 on single tenant
    d5 = resolve_sustained_tenant_distribution(concurrency=5)
    assert d5 == {"tenant_a": 5}

    # 25 -> 10 + 10 + 5 across three tenants
    d25 = resolve_sustained_tenant_distribution(concurrency=25)
    assert d25 == {"tenant_a": 10, "tenant_b": 10, "tenant_c": 5}
    assert sum(d25.values()) == 25

    # Valid custom distribution
    custom = resolve_sustained_tenant_distribution(
        concurrency=5,
        tenant_distribution={"t1": 3, "t2": 2},
    )
    assert custom == {"t1": 3, "t2": 2}

    # Mismatched total vs concurrency
    with pytest.raises(ValueError, match="does not equal requested concurrency"):
        resolve_sustained_tenant_distribution(
            concurrency=5,
            tenant_distribution={"t1": 2, "t2": 2},
        )

    # Exceeding per-tenant limit (10)
    with pytest.raises(ValueError, match="exceeds per-tenant limit"):
        resolve_sustained_tenant_distribution(
            concurrency=15,
            tenant_distribution={"t1": 15},
        )

    # Exceeding max concurrent capacity (25)
    with pytest.raises(ValueError, match="exceeds max capacity"):
        resolve_sustained_tenant_distribution(concurrency=30)

    # Invalid non-positive concurrency
    with pytest.raises(ValueError, match="Concurrency must be positive"):
        resolve_sustained_tenant_distribution(concurrency=0)


def test_loopback_sustained_soak_concurrency_1() -> None:
    """Verify sustained soak runner executes cleanly with concurrency 1."""

    async def _run() -> None:
        async with start_ephemeral_soak_server() as s_ctx:
            res = await run_sustained_soak_stage(
                s_ctx,
                concurrency=1,
                duration=0.15,
                ping_interval=0.05,
                sample_interval=0.05,
            )
            assert res.status == "PASS"
            assert res.achieved_concurrency == 1
            assert res.completed_calls == 1
            assert res.unexpected_failures == 0
            assert res.peak_active_connections == 1
            assert res.min_active_connections == 1
            assert res.final_active_connections == 0
            assert res.active_leases_after_cleanup == 0

            # Zero leaks
            snap = s_ctx.controller.snapshot()
            assert snap.active_connections == 0
            assert len(s_ctx.bridge._active_calls) == 0

    asyncio.run(_run())


def test_loopback_sustained_soak_concurrency_5() -> None:
    """Verify sustained soak runner executes cleanly with concurrency 5."""

    async def _run() -> None:
        async with start_ephemeral_soak_server() as s_ctx:
            res = await run_sustained_soak_stage(
                s_ctx,
                concurrency=5,
                duration=0.15,
                ping_interval=0.05,
                sample_interval=0.05,
            )
            assert res.status == "PASS"
            assert res.achieved_concurrency == 5
            assert res.completed_calls == 5
            assert res.unexpected_failures == 0
            assert res.peak_active_connections == 5
            assert res.min_active_connections == 5
            assert res.final_active_connections == 0
            assert res.active_leases_after_cleanup == 0

            # Zero leaks
            snap = s_ctx.controller.snapshot()
            assert snap.active_connections == 0
            assert len(s_ctx.bridge._active_calls) == 0

    asyncio.run(_run())


def test_loopback_sustained_soak_concurrency_25() -> None:
    """Verify sustained soak runner executes cleanly with concurrency 25 across 3 tenants."""

    async def _run() -> None:
        async with start_ephemeral_soak_server() as s_ctx:
            res = await run_sustained_soak_stage(
                s_ctx,
                concurrency=25,
                duration=0.15,
                ping_interval=0.05,
                sample_interval=0.05,
            )
            assert res.status == "PASS"
            assert res.achieved_concurrency == 25
            assert res.completed_calls == 25
            assert res.unexpected_failures == 0
            assert res.peak_active_connections == 25
            assert res.min_active_connections == 25
            assert res.final_active_connections == 0
            assert res.active_leases_after_cleanup == 0

            # Zero leaks
            snap = s_ctx.controller.snapshot()
            assert snap.active_connections == 0
            assert len(s_ctx.bridge._active_calls) == 0

    asyncio.run(_run())


def test_sustained_worker_failure_before_barrier() -> None:
    """Verify worker failure before barrier aborts barrier, cancels tasks, and leaves zero leases."""

    async def _run() -> None:
        async with start_ephemeral_soak_server() as s_ctx:
            res = await run_sustained_soak_stage(
                s_ctx,
                concurrency=5,
                duration=1.0,
                _fail_worker_before_barrier=True,
            )
            assert res.status == "FAIL"
            assert res.unexpected_failures >= 1
            assert res.error is not None
            assert res.final_active_connections == 0
            assert res.active_leases_after_cleanup == 0

            # Zero leaks
            snap = s_ctx.controller.snapshot()
            assert snap.active_connections == 0
            assert len(s_ctx.bridge._active_calls) == 0

    asyncio.run(_run())


def test_sustained_bounded_cancellation() -> None:
    """Verify that canceling a sustained soak stage cancels child tasks boundedly with zero leaks."""

    async def _run() -> None:
        async with start_ephemeral_soak_server() as s_ctx:
            task = asyncio.create_task(
                run_sustained_soak_stage(
                    s_ctx,
                    concurrency=5,
                    duration=10.0,
                    ping_interval=0.1,
                    sample_interval=0.05,
                )
            )
            # Allow clients to reach connected / holding state
            await asyncio.sleep(0.2)
            assert s_ctx.controller.snapshot().active_connections == 5

            # Cancel the parent task
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

            # Allow brief moment for client tasks to finish teardown
            await asyncio.sleep(0.1)

            # Verify resource cleanup
            snap = s_ctx.controller.snapshot()
            assert snap.active_connections == 0
            assert len(s_ctx.bridge._active_calls) == 0

    asyncio.run(_run())


def test_loopback_fault_injection_stage() -> None:
    """Verify fault injection stage exercises all 8 fault classes with zero leaks."""

    async def _run() -> None:
        async with start_ephemeral_soak_server() as s_ctx:
            res = await run_fault_injection_stage(s_ctx, timeout=10.0)
            assert res.status == "PASS"
            assert res.connection_attempts == 11
            assert res.successful_admissions == 7
            assert res.expected_rejections == 4  # forged, replayed, unscoped, drain
            assert res.unexpected_failures == 0
            assert res.completed_calls == 3
            assert res.error is None

            # Verify zero leaks
            snap = s_ctx.controller.snapshot()
            assert snap.active_connections == 0
            assert snap.available_capacity == 25
            assert len(s_ctx.bridge._active_calls) == 0

    asyncio.run(_run())


def test_loopback_sustained_soak_stage_fast() -> None:
    """Verify sustained soak runner logic on fast duration with active sampling and zero leaks."""

    async def _run() -> None:
        async with start_ephemeral_soak_server() as s_ctx:
            res = await run_sustained_soak_stage(
                s_ctx,
                concurrency=2,
                duration=0.3,
                tenant_distribution={"t_sustained_a": 1, "t_sustained_b": 1},
                ping_interval=0.1,
                sample_interval=0.05,
            )
            assert res.status == "PASS"
            assert res.achieved_concurrency == 2
            assert res.completed_calls == 2
            assert res.unexpected_failures == 0
            assert res.peak_active_connections == 2
            assert res.min_active_connections == 2
            assert res.sustained_holding_seconds >= 0.25
            assert res.final_active_connections == 0
            assert res.active_leases_after_cleanup == 0

            # Verify resource recovery
            snap = s_ctx.controller.snapshot()
            assert snap.active_connections == 0
            assert snap.available_capacity == 25
            assert len(s_ctx.bridge._active_calls) == 0

    asyncio.run(_run())


def test_repeated_soak_duration_precedence() -> None:
    """Verify repeated soak stops when max_duration is reached even if iterations remains."""

    async def _run() -> None:
        async with start_ephemeral_soak_server() as s_ctx:
            res = await run_repeated_soak_stage(
                s_ctx,
                concurrency=1,
                max_duration=0.1,
                iterations=100,  # Huge iterations budget, should stop by duration
            )
            assert res.status == "PASS"
            assert res.completed_iterations < 100
            assert res.final_active_connections == 0
            assert res.active_leases_after_cleanup == 0

    asyncio.run(_run())


def test_barrier_abort_on_worker_failure() -> None:
    """Verify that if a worker fails before barrier, start_barrier.abort() breaks without hang."""

    async def _run() -> None:
        async with start_ephemeral_soak_server() as s_ctx:
            # Run concurrency stage where one token is deliberately bad
            bad_token = "invalid.token.signature"
            ws_url = f"{s_ctx.ws_base_url}{SYNTHETIC_WS_ENDPOINT_PATH}"
            barrier = asyncio.Barrier(2)

            async def _bad_worker() -> None:
                try:
                    await run_single_synthetic_call(
                        ws_endpoint_url=ws_url,
                        token=bad_token,
                        call_id="call_bad",
                        start_barrier=barrier,
                        timeout=2.0,
                    )
                except Exception:
                    await barrier.abort()

            task = asyncio.create_task(_bad_worker())
            with pytest.raises(asyncio.BrokenBarrierError):
                await asyncio.wait_for(barrier.wait(), timeout=2.0)
            await task

            # Zero leaks
            assert s_ctx.controller.snapshot().active_connections == 0

    asyncio.run(_run())


def test_cli_main_execution_concurrency_1() -> None:
    """Verify CLI main() runs concurrency 1 scenario cleanly."""
    ret = main(["--concurrency", "1", "--duration", "2.0"])
    assert ret == 0


def test_cli_main_execution_output_json(tmp_path: Path) -> None:
    """Verify CLI main() outputs a valid JSON report to specified path."""
    json_path = tmp_path / "soak_out.json"
    ret = main(["--concurrency", "1", "--duration", "2.0", "--output", str(json_path)])
    assert ret == 0
    assert json_path.exists()

    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)
    assert data["runner_version"] == "0.1.0"
    assert data["overall_status"] == "PASS"
    assert len(data["stages"]) >= 1
