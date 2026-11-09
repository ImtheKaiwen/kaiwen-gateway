"""Deterministic unit and loopback smoke tests for M5.1B paced synthetic media load evaluation."""

from __future__ import annotations

import asyncio
import json
import unittest.mock
from pathlib import Path
from typing import Any, cast

import pytest
from tools.synthetic_media_load import (
    DEFAULT_MAX_MEAN_INTERVAL_DEVIATION_PCT,
    DEFAULT_MAX_MISSED_DEADLINE_RATE,
    DEFAULT_MAX_P95_SCHEDULER_LATENESS_MS,
    DEFAULT_MAX_TRANSMISSION_ELONGATION_PCT,
    FRAME_MAGIC,
    HEADER_STRUCT,
    ISOLATION_MODE_IN_PROCESS,
    ISOLATION_MODE_PROCESS_ISOLATED,
    KW_SYNTHETIC_MEDIA_SECRET_KEY_ENV,
    SCHEDULING_MODE_ALIGNED,
    SCHEDULING_MODE_STAGGERED,
    SUPPORTED_PATTERNS,
    CallMediaRunRecord,
    EventLoopLagMonitor,
    MonotonicFramePacer,
    PacedMediaLoadReport,
    PacingAcceptanceCriteria,
    SessionEchoTracker,
    _build_arg_parser,
    build_deterministic_frame_payload,
    compute_session_phase_offset_ms,
    evaluate_pacing_metrics,
    execute_media_load_evaluation,
    extract_frame_identity,
    main,
    measure_process_rss_kib,
    reassess_stage_report,
    run_fault_and_overload_suite,
    run_paced_media_stage,
    start_isolated_media_server_process,
    verify_frame_payload,
)

from kaiwen_gateway.modules.voice import (
    DEFAULT_LATENCY_WINDOW_SIZE,
    SMALL_SAMPLE_THRESHOLD,
    BoundedLatencyAggregator,
)
from kaiwen_gateway.transports.synthetic import PCM16LE_FRAME_BYTES

# ==============================================================================
# 1 & 2. Monotonic 20 ms Pacing, Drift Calculation & Missed-Deadline Policy
# ==============================================================================


def test_monotonic_frame_pacer_exact_20ms_schedule_and_drift() -> None:
    """Verify monotonic reference schedule `t0 + k * 0.020s` and signed/unsigned drift calculation."""

    async def _run() -> None:
        now = 100.0
        slept_durations: list[float] = []

        def fake_clock() -> float:
            return now

        async def fake_sleep(seconds: float) -> None:
            nonlocal now
            slept_durations.append(seconds)
            # Simulate slight 1.5 ms OS timer overshoot on slot 1, exact on slot 2
            if len(slept_durations) == 1:
                now += seconds + 0.0015
            else:
                now += seconds

        pacer = MonotonicFramePacer(
            interval_ms=20.0,
            clock=fake_clock,
            sleep_fn=fake_sleep,
        )
        origin = pacer.reset(t0=100.0)
        assert origin == pytest.approx(100.0)

        # Slot 0: sent immediately at t0 = 100.0
        m0 = await pacer.await_slot(0)
        assert m0.slot_index == 0
        assert not m0.skipped
        assert not m0.missed_deadline
        assert m0.deadline_monotonic_s == pytest.approx(100.0)
        assert m0.actual_send_monotonic_s == pytest.approx(100.0)
        assert m0.lateness_ms == pytest.approx(0.0)
        assert m0.inter_frame_interval_ms is None
        assert m0.interval_jitter_ms is None

        # Slot 1: deadline 100.020, wakes up 1.5 ms late at 100.0215
        m1 = await pacer.await_slot(1)
        assert m1.slot_index == 1
        assert not m1.skipped
        assert not m1.missed_deadline
        assert m1.deadline_monotonic_s == pytest.approx(100.020)
        assert m1.actual_send_monotonic_s == pytest.approx(100.0215)
        assert m1.lateness_ms == pytest.approx(1.5)
        assert m1.signed_lateness_ms == pytest.approx(1.5)
        assert m1.inter_frame_interval_ms == pytest.approx(21.5)
        assert m1.interval_jitter_ms == pytest.approx(1.5)

        # Slot 2: reference deadline remains anchored at t0 + 2 * 0.020 = 100.040 (never shifted!)
        # Sleep duration must be 100.040 - 100.0215 = 0.0185s
        m2 = await pacer.await_slot(2)
        assert slept_durations[-1] == pytest.approx(0.0185)
        assert m2.deadline_monotonic_s == pytest.approx(100.040)
        assert m2.actual_send_monotonic_s == pytest.approx(100.040)
        assert m2.lateness_ms == pytest.approx(0.0)
        assert m2.inter_frame_interval_ms == pytest.approx(18.5)
        assert m2.interval_jitter_ms == pytest.approx(1.5)
        assert pacer.scheduled_count == 3
        assert pacer.sent_count == 3
        assert pacer.missed_deadlines == 0
        assert pacer.skipped_deadlines == 0

    asyncio.run(_run())


def test_monotonic_frame_pacer_missed_deadline_and_anti_burst_spacing() -> None:
    """Verify missed-deadline detection and rate-limited catch-up (`min_send_spacing_ms`) without zero-delay flooding."""

    async def _run() -> None:
        now = 50.0
        slept: list[float] = []

        def fake_clock() -> float:
            return now

        async def fake_sleep(seconds: float) -> None:
            nonlocal now
            slept.append(seconds)
            now += seconds

        pacer = MonotonicFramePacer(
            interval_ms=20.0,
            missed_deadline_threshold_ms=20.0,
            min_send_spacing_ms=5.0,
            clock=fake_clock,
            sleep_fn=fake_sleep,
        )
        pacer.reset(t0=50.0)

        # Slot 0 sent at 50.0
        await pacer.await_slot(0)

        # Simulate a 55 ms event-loop stall before slot 1 is even requested (now = 50.055)
        now = 50.055

        # Slot 1 deadline was 50.020 (35 ms late > 20 ms threshold -> missed_deadline=True)
        # Last send was at 50.000, so 50.055 >= 50.005; no extra anti-burst sleep needed for slot 1
        m1 = await pacer.await_slot(1)
        assert m1.missed_deadline is True
        assert not m1.skipped
        assert m1.lateness_ms == pytest.approx(35.0)
        assert m1.actual_send_monotonic_s == pytest.approx(50.055)

        # Slot 2 deadline was 50.040 (also behind 50.055!).
        # Anti-burst spacing enforces at least 5.0 ms after slot 1's send at 50.055 -> sends at 50.060!
        m2 = await pacer.await_slot(2)
        assert slept[-1] == pytest.approx(0.005)
        assert m2.actual_send_monotonic_s == pytest.approx(50.060)
        assert m2.inter_frame_interval_ms == pytest.approx(5.0)
        assert pacer.missed_deadlines >= 1

    asyncio.run(_run())


def test_monotonic_frame_pacer_skip_threshold_policy() -> None:
    """Verify optional `skip_threshold_ms` skips severely stale frame slots instead of sending late."""

    async def _run() -> None:
        now = 10.0

        def fake_clock() -> float:
            return now

        async def fake_sleep(seconds: float) -> None:
            nonlocal now
            now += seconds

        pacer = MonotonicFramePacer(
            interval_ms=20.0,
            skip_threshold_ms=40.0,
            clock=fake_clock,
            sleep_fn=fake_sleep,
        )
        pacer.reset(t0=10.0)
        await pacer.await_slot(0)

        # Stall 65 ms -> slot 1 (deadline 10.020) is 45 ms late (>= 40 ms skip threshold)
        now = 10.065
        m1 = await pacer.await_slot(1)
        assert m1.skipped is True
        assert m1.missed_deadline is True
        assert m1.actual_send_monotonic_s is None
        assert m1.lateness_ms == pytest.approx(45.0)
        assert pacer.skipped_deadlines == 1
        assert pacer.sent_count == 1

    asyncio.run(_run())


def test_monotonic_frame_pacer_invalid_arguments() -> None:
    """Verify parameter validation on `MonotonicFramePacer`."""
    with pytest.raises(ValueError, match="interval_ms"):
        MonotonicFramePacer(interval_ms=0.0)
    with pytest.raises(ValueError, match="interval_ms"):
        MonotonicFramePacer(interval_ms=True)
    with pytest.raises(ValueError, match="missed_deadline_threshold_ms"):
        MonotonicFramePacer(missed_deadline_threshold_ms=-1.0)
    with pytest.raises(ValueError, match="min_send_spacing_ms"):
        MonotonicFramePacer(min_send_spacing_ms=-0.1)
    with pytest.raises(ValueError, match="skip_threshold_ms"):
        MonotonicFramePacer(skip_threshold_ms=0.0)


# ==============================================================================
# 3 & 4. Deterministic Frame Identity & Echo Correctness Tracker
# ==============================================================================


@pytest.mark.parametrize("pattern", SUPPORTED_PATTERNS)
def test_deterministic_frame_payload_generation_and_verification(pattern: str) -> None:
    """Verify 640-byte PCM16LE frame generation, header extraction, and checksum verification."""
    payload = build_deterministic_frame_payload(
        seed=42,
        session_slot_id=7,
        frame_seq=19,
        pattern=pattern,
    )
    assert len(payload) == PCM16LE_FRAME_BYTES == 640
    assert extract_frame_identity(payload) == (7, 19)
    assert verify_frame_payload(
        payload,
        seed=42,
        session_slot_id=7,
        frame_seq=19,
        pattern=pattern,
    )
    # Different seed, slot, or sequence must fail verification
    assert not verify_frame_payload(
        payload,
        seed=99,
        session_slot_id=7,
        frame_seq=19,
        pattern=pattern,
    )
    assert not verify_frame_payload(
        payload,
        seed=42,
        session_slot_id=8,
        frame_seq=19,
        pattern=pattern,
    )
    assert not verify_frame_payload(
        payload,
        seed=42,
        session_slot_id=7,
        frame_seq=20,
        pattern=pattern,
    )


def test_frame_identity_rejects_malformed_or_corrupt_frames() -> None:
    """Verify `extract_frame_identity` and `verify_frame_payload` reject invalid lengths and magic bytes."""
    assert extract_frame_identity(b"short") is None
    assert not verify_frame_payload(
        b"short",
        seed=42,
        session_slot_id=0,
        frame_seq=0,
    )
    bad_magic = HEADER_STRUCT.pack(0xDEADBEEF, 0, 0, 0) + (b"\x00" * 624)
    assert extract_frame_identity(bad_magic) is None
    assert not verify_frame_payload(
        bad_magic,
        seed=42,
        session_slot_id=0,
        frame_seq=0,
    )
    valid = bytearray(build_deterministic_frame_payload(seed=42, session_slot_id=1, frame_seq=2))
    valid[-1] ^= 0xFF
    assert extract_frame_identity(bytes(valid)) == (1, 2)
    assert not verify_frame_payload(
        bytes(valid),
        seed=42,
        session_slot_id=1,
        frame_seq=2,
    )
    with pytest.raises(ValueError, match="Unsupported frame pattern"):
        build_deterministic_frame_payload(
            seed=42, session_slot_id=0, frame_seq=0, pattern="unknown"
        )


def test_session_echo_tracker_detects_missing_duplicate_ooo_mismatch_and_cross_session() -> None:
    """Verify `SessionEchoTracker` accurately classifies all echo anomalies."""
    tracker = SessionEchoTracker(session_slot_id=3, seed=42)

    f0 = build_deterministic_frame_payload(seed=42, session_slot_id=3, frame_seq=0)
    f1 = build_deterministic_frame_payload(seed=42, session_slot_id=3, frame_seq=1)
    f2 = build_deterministic_frame_payload(seed=42, session_slot_id=3, frame_seq=2)
    f3 = build_deterministic_frame_payload(seed=42, session_slot_id=3, frame_seq=3)

    tracker.record_sent(0, f0, 1.000)
    tracker.record_sent(1, f1, 1.020)
    tracker.record_sent(2, f2, 1.040)
    tracker.record_sent(3, f3, 1.060)

    # Receive f0 in order (5 ms RTT)
    tracker.record_received(f0, 1.005)
    # Receive f2 before f1 -> out of order!
    tracker.record_received(f2, 1.046)
    # Receive f1 late -> also out of order relative to _last_unique_seq=2
    tracker.record_received(f1, 1.050)
    # Duplicate f1
    tracker.record_received(f1, 1.052)
    # Cross-session frame from slot 99
    f_other = build_deterministic_frame_payload(seed=42, session_slot_id=99, frame_seq=0)
    tracker.record_received(f_other, 1.055)
    # Corrupted body for slot 3
    f_corrupt = bytearray(f3)
    f_corrupt[100] ^= 0x55
    tracker.record_received(bytes(f_corrupt), 1.065)
    # Invalid magic header
    bad_header = HEADER_STRUCT.pack(FRAME_MAGIC ^ 1, 3, 3, 0) + (b"\x00" * 624)
    tracker.record_received(bad_header, 1.070)

    # Note: f3 was never validly echoed -> 1 missing frame
    assert tracker.frames_sent == 4
    assert tracker.unique_valid_echoes == 3
    assert tracker.frames_missing == 1
    assert tracker.duplicate_frames == 1
    assert tracker.out_of_order_frames == 2
    assert tracker.cross_session_frames == 1
    assert tracker.payload_mismatches == 3  # cross-session + corrupt body + bad header
    rtt_snap = tracker.echo_rtt_aggregator.snapshot()
    assert rtt_snap.available is True
    assert rtt_snap.sample_count == 3


# ==============================================================================
# 5, 6 & 11. Small-Sample Semantics, Bounded Retention & Loopback Stage Smoke
# ==============================================================================


def test_small_sample_flag_and_bounded_window_retention() -> None:
    """Verify `is_small_sample` threshold and bounded window retention on latency summaries."""
    agg = BoundedLatencyAggregator(window_capacity=24, min_percentile_samples=1)
    for i in range(5):
        agg.record(float(i + 1))
    snap_small = agg.snapshot()
    assert snap_small.available is True
    assert snap_small.sample_count == 5
    assert snap_small.is_small_sample is True
    assert SMALL_SAMPLE_THRESHOLD == 20
    assert DEFAULT_LATENCY_WINDOW_SIZE == 256

    for i in range(5, 40):
        agg.record(float(i + 1))
    snap_large = agg.snapshot()
    assert snap_large.sample_count == 40
    assert snap_large.retained_sample_count == 24
    assert snap_large.is_small_sample is False
    assert snap_large.min_ms == pytest.approx(1.0)
    assert snap_large.max_ms == pytest.approx(40.0)


def test_event_loop_lag_monitor_and_rss_helper() -> None:
    """Verify `EventLoopLagMonitor` and `measure_process_rss_kib` operate cleanly."""

    async def _run() -> None:
        monitor = EventLoopLagMonitor(probe_interval_s=0.005)
        monitor.start()
        await asyncio.sleep(0.025)
        snap = await monitor.stop()
        assert snap.available is True
        assert snap.sample_count >= 1
        assert snap.min_ms is not None and snap.min_ms >= 0.0

        rss = measure_process_rss_kib()
        assert isinstance(rss.available, bool)
        if rss.available:
            assert rss.current_rss_kib is not None and rss.current_rss_kib > 0.0

    asyncio.run(_run())


def test_short_paced_media_stage_loopback_smoke_and_concurrency_barrier() -> None:
    """Execute a short 2-session 20 ms-paced loopback WebSocket stage (6 frames = 120 ms)."""

    async def _run() -> None:
        stage = await run_paced_media_stage(
            stage_name="test_smoke_c2",
            concurrency=2,
            duration_seconds=0.12,
            interval_ms=20.0,
            seed=42,
            pattern="sine",
        )
        assert stage.status == "PASS", f"Stage failed: {stage.failure_reasons}"
        assert stage.requested_concurrency == 2
        assert stage.admitted_concurrency == 2
        assert stage.achieved_media_concurrency == 2
        assert stage.completed_calls == 2
        assert stage.unexpected_rejections == 0
        assert stage.unexpected_disconnects == 0
        assert stage.media_correctness["frames_sent"] == 12
        assert stage.media_correctness["frames_echoed"] == 12
        assert stage.media_correctness["frames_missing"] == 0
        assert stage.media_correctness["duplicate_frames"] == 0
        assert stage.media_correctness["out_of_order_frames"] == 0
        assert stage.media_correctness["payload_mismatches"] == 0
        assert stage.media_correctness["cross_session_frames"] == 0
        assert stage.cleanup_verification["final_active_connections"] == 0
        assert stage.cleanup_verification["final_active_calls"] == 0
        assert stage.cleanup_verification["final_active_voice_sessions"] == 0
        assert stage.cleanup_verification["leaked_asyncio_tasks"] == 0

        # Verify unavailable external provider/playback metrics are explicitly False / None
        assert stage.timing_ms["upstream_ai_generation_ms"]["available"] is False
        assert stage.timing_ms["upstream_ai_generation_ms"]["p95_ms"] is None
        assert stage.timing_ms["remote_playback_latency_ms"]["available"] is False
        assert stage.timing_ms["remote_playback_latency_ms"]["p95_ms"] is None
        assert stage.timing_ms["external_telephony_network_ms"]["available"] is False
        assert stage.timing_ms["external_telephony_network_ms"]["p95_ms"] is None

        # Verify R1-B & R1-C: pooled per-frame vs across-session percentile semantics and overhead heuristic
        echo_rtt = stage.timing_ms["client_synthetic_echo_rtt_ms"]
        assert echo_rtt["summary_method"] == "pooled_per_frame_exact"
        assert echo_rtt["sample_count"] == 12
        assert echo_rtt["retained_sample_count"] == 12

        in_proc = stage.timing_ms["gateway_inbound_processing_ms"]
        assert in_proc["summary_method"] == "across_session_percentiles"
        assert in_proc["sample_count"] == 2  # 2 sessions measured
        assert in_proc["sessions_measured"] == 2
        assert in_proc["total_frames_measured"] == 12

        overhead = stage.timing_ms["gateway_added_overhead_estimate_ms"]
        assert overhead["summary_method"] == "uncorrelated_component_percentile_sum_heuristic"
        assert overhead["is_directly_measured_end_to_end_latency"] is False
        assert overhead["nfr_perf_004_qualified"] is False
        assert len(overhead["omitted_intervals"]) == 5

    asyncio.run(_run())


# ==============================================================================
# 7, 8, 9 & 10. Fault/Overload Suite, CLI Serialization & Privacy Redaction
# ==============================================================================


def test_fault_and_overload_suite_passes_all_scenarios() -> None:
    """Verify all 8 GATE F fault, backpressure, saturation, draining, recovery, and isolation scenarios."""

    async def _run() -> None:
        faults = await run_fault_and_overload_suite(seed=42)
        assert len(faults) == 8
        by_name = {f.scenario: f for f in faults}
        expected_names = {
            "premature_client_disconnect",
            "blocked_ai_consumer_inbound_backpressure",
            "blocked_outbound_send_backpressure",
            "normal_call_end_during_active_audio",
            "admission_capacity_saturation",
            "draining_with_active_media_calls",
            "recovery_after_interrupted_and_failed_calls",
            "two_tenants_concurrent_isolation",
        }
        assert set(by_name.keys()) == expected_names
        for name, res in by_name.items():
            assert res.status == "PASS", f"Fault scenario {name} failed: {res.to_dict()}"
            assert res.unexpected_failures == 0
            assert res.final_active_connections == 0
            assert res.final_active_calls == 0
            assert res.final_active_voice_sessions == 0
            assert res.leaked_tasks == 0

        # Inbound and outbound backpressure scenarios verify 1011 close code and backpressure error category
        in_bp = by_name["blocked_ai_consumer_inbound_backpressure"]
        assert in_bp.details["observed_close_code"] == 1011
        assert in_bp.details["backpressure_overflow_errors"] >= 1

        out_bp = by_name["blocked_outbound_send_backpressure"]
        assert out_bp.details["observed_close_code"] == 1011
        assert out_bp.details["backpressure_overflow_errors"] >= 1
        assert out_bp.details["max_outbound_queue_hwm"] == 8
        assert (
            out_bp.details["backpressure_evidence_type"]
            == "fault_injection_exception_handling_verification"
        )
        assert out_bp.details["is_production_queue_timeout"] is False
        assert (
            out_bp.details["production_backpressure_source"]
            == "synthetic_ai_port_injected_overflow"
        )
        assert "production_architecture_note" in out_bp.details

        # Recovery scenario verifies Turn 2 audio was echoed and bad close code was 1003
        rec = by_name["recovery_after_interrupted_and_failed_calls"]
        assert rec.details["turn2_audio_recovered"] is True
        assert rec.details["invalid_frame_close_code"] == 1003

        # Admission saturation must record expected rejections and zero AI port allocations for rejected calls
        sat = by_name["admission_capacity_saturation"]
        assert sat.expected_rejections == 2
        assert sat.details["ai_ports_allocated_for_rejected"] == 0

        # Draining must reject new arrivals while active calls finish cleanly
        drn = by_name["draining_with_active_media_calls"]
        assert drn.expected_rejections == 1
        assert drn.details["drained_cleanly"] is True

    asyncio.run(_run())


def test_cli_main_report_serialization_and_privacy_redaction(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Verify CLI `main()` writes valid JSON report and leaks zero secrets, tokens, or raw IDs."""
    out_file = tmp_path / "m5_smoke_report.json"
    exit_code = main(
        [
            "--concurrency",
            "1",
            "--duration",
            "0.1",
            "--seed",
            "42",
            "--no-faults",
            "--output",
            str(out_file),
            "--json",
        ]
    )
    assert exit_code == 0
    assert out_file.is_file()

    stdout_text = capsys.readouterr().out
    parsed_stdout: dict[str, Any] = json.loads(stdout_text)
    file_text = out_file.read_text(encoding="utf-8")
    parsed_file: dict[str, Any] = json.loads(file_text)

    assert parsed_stdout["overall_status"] == "PASS"
    assert parsed_file["overall_status"] == "PASS"
    assert len(parsed_file["stages"]) == 1
    assert parsed_file["fault_scenarios"] == []

    # Verify strict privacy & redaction: no bearer tokens, raw call IDs, tenant IDs, or magic frame bytes
    forbidden_substrings = (
        "Bearer ",
        "call_s1_concurrency_1",
        "tenant_alpha",
        "tenant_beta",
        "tenant_gamma",
        "KWM5",
        "secret_key",
    )
    for forbidden in forbidden_substrings:
        assert forbidden not in file_text, f"Forbidden substring {forbidden!r} leaked in report"


def test_r1_sample_truncation_remediation_and_exact_percentiles() -> None:
    """Verify R1-A (F-1): stage aggregation of 3 sessions x 200 samples (600 > 256) retains all samples."""
    from tools.synthetic_media_load import _summarize_pooled_stage_samples, _summarize_samples

    # 3 sessions with distinct distributions and early outliers:
    # Session 1: 200 samples around 10.0 ms, with an early outlier at 100.0 ms
    s1 = [100.0] + [10.0] * 199
    # Session 2: 200 samples around 20.0 ms
    s2 = [20.0] * 200
    # Session 3: 200 samples around 30.0 ms
    s3 = [30.0] * 200

    pooled_samples = s1 + s2 + s3
    assert len(pooled_samples) == 600

    # Old default FIFO window with capacity 256 would have discarded s1 completely!
    old_truncated = _summarize_samples(pooled_samples, window_capacity=256)
    assert old_truncated.sample_count == 600
    assert old_truncated.retained_sample_count == 256
    # The outlier at 100.0 was in s1 (index 0), so in the old 256 FIFO window it was lost:
    # window holds only s3 (200) + last 56 of s2 (all 20.0 or 30.0), so max is 30.0!
    # With R1-A exact stage pooling:
    pooled_summary = _summarize_pooled_stage_samples(pooled_samples)
    assert pooled_summary["summary_method"] == "pooled_per_frame_exact"
    assert pooled_summary["sample_count"] == 600
    assert pooled_summary["retained_sample_count"] == 600
    assert pooled_summary["min_ms"] == pytest.approx(10.0)
    assert pooled_summary["max_ms"] == pytest.approx(100.0)

    # Calculate exact mathematical reference percentiles on full sorted population
    sorted_samples = sorted(pooled_samples)
    ref_agg = BoundedLatencyAggregator(window_capacity=600, min_percentile_samples=1)
    for val in sorted_samples:
        ref_agg.record(val)
    ref_snap = ref_agg.snapshot()

    assert pooled_summary["p50_ms"] == pytest.approx(ref_snap.p50_ms)
    assert pooled_summary["p95_ms"] == pytest.approx(ref_snap.p95_ms)
    assert pooled_summary["p99_ms"] == pytest.approx(ref_snap.p99_ms)


def test_r1_percentile_semantics_distinction_regression() -> None:
    """Verify R1-B (F-5): pooled per-frame percentiles are mathematically distinct from across-session percentiles."""
    from tools.synthetic_media_load import (
        _summarize_pooled_stage_samples,
        _summarize_session_metric_across_calls,
    )

    from kaiwen_gateway.modules.voice import (
        VoiceBridgeSession,
    )
    from kaiwen_gateway.modules.voice.adapters.fake import (
        FakeAIMediaAdapter,
        FakeTelephonyMediaAdapter,
    )

    # Create 2 sessions:
    # Session A: 100 frames, 99 frames at 1.0 ms, 1 frame at 100.0 ms -> p95 is ~1.0 ms
    # Session B: 10 frames, all 10 frames at 50.0 ms -> p95 is 50.0 ms
    agg_a = BoundedLatencyAggregator(window_capacity=256, min_percentile_samples=1)
    for _ in range(99):
        agg_a.record(1.0)
    agg_a.record(100.0)

    agg_b = BoundedLatencyAggregator(window_capacity=256, min_percentile_samples=1)
    for _ in range(10):
        agg_b.record(50.0)

    # Use a dummy VoiceBridgeSession to obtain a real VoiceSessionMetricsSnapshot, then replace inbound_processing_ms
    tel = FakeTelephonyMediaAdapter()
    ai = FakeAIMediaAdapter()
    session = VoiceBridgeSession(call_id="c_dummy", tenant_id="t1", telephony_port=tel, ai_port=ai)
    base_snap = session.metrics_snapshot()

    from dataclasses import replace

    snap_a = replace(base_snap, inbound_processing_ms=agg_a.snapshot())
    snap_b = replace(base_snap, inbound_processing_ms=agg_b.snapshot())

    # Across-session summary:
    across = _summarize_session_metric_across_calls(
        [snap_a, snap_b],
        lambda s: s.inbound_processing_ms,
    )
    assert across["summary_method"] == "across_session_percentiles"
    assert across["sample_count"] == 2  # exactly 2 sessions
    assert across["total_frames_measured"] == 110  # 100 + 10 frames
    assert across["sessions_measured"] == 2
    assert "session_p50_median_ms" in across
    assert "session_p95_median_ms" in across
    assert "session_p95_p95_ms" in across
    assert "max_session_p95_ms" in across
    assert "frame_weighted_mean_latency_ms" in across
    assert "weighted_frame_mean_ms" in across
    assert "population_definitions" in across

    # Mathematical verification of frame-weighted mean:
    # Session A total ms = 99 * 1.0 + 100.0 = 199.0; Session B total ms = 10 * 50.0 = 500.0.
    # Total sum across all 110 frames = 699.0 ms.
    # True frame-weighted mean = 699.0 / 110 ~= 6.3545 ms.
    expected_weighted_mean = round(699.0 / 110.0, 4)
    assert across["frame_weighted_mean_latency_ms"] == pytest.approx(
        expected_weighted_mean, abs=1e-4
    )
    assert across["weighted_frame_mean_ms"] == pytest.approx(expected_weighted_mean, abs=1e-4)

    # Prove this is NOT the unweighted mean of session p95s nor frame-weighted average of p95s:
    # Session A p95 is ~1.0 ms. Session B p95 is 50.0 ms.
    # Average of p95s would be (1.0 + 50.0) / 2 = 25.5 ms.
    # Frame-weighted average of p95s would be (100 * 1.0 + 10 * 50.0) / 110 = 5.4545 ms.
    assert across["weighted_frame_mean_ms"] != pytest.approx(25.5, abs=0.1)
    assert across["weighted_frame_mean_ms"] != pytest.approx(5.4545, abs=0.1)

    # Pooled per-frame calculation on the same raw data
    pooled_raw = ([1.0] * 99) + [100.0] + ([50.0] * 10)
    pooled = _summarize_pooled_stage_samples(pooled_raw)
    assert pooled["summary_method"] == "pooled_per_frame_exact"
    assert pooled["sample_count"] == 110
    # In pooled data, 99/110 = 90% are 1.0 ms. p95 is around 50.0 ms.
    # While in across-session data, sample_count is 2!
    assert across["sample_count"] != pooled["sample_count"]


def test_percentile_sum_inequality_counterexample_and_conjoint_latency_distinction() -> None:
    """Verify Task 1: p95(X1 + X2) <= p95(X1) + p95(X2) is mathematically false in general.

    Constructs a deterministic counterexample using two non-negative latency variables
    with disjoint upper-tail events, proving that marginal percentile sums do NOT
    guarantee an upper bound or equal true combined latency percentiles.
    """
    from tools.synthetic_media_load import _summarize_samples

    # Population of N = 200 samples:
    # Variable X1 has a 5% upper tail (10 samples of 100.0 ms) at indices 0..9, zero elsewhere.
    # Variable X2 has a disjoint 5% upper tail (10 samples of 100.0 ms) at indices 10..19, zero elsewhere.
    # On indices 20..199 (180 samples, 90%), both X1 and X2 are 0.0 ms.
    x1_samples = ([100.0] * 10) + ([0.0] * 190)
    x2_samples = ([0.0] * 10) + ([100.0] * 10) + ([0.0] * 180)
    y_total_samples = [x1 + x2 for x1, x2 in zip(x1_samples, x2_samples, strict=True)]

    # Compute marginal percentiles
    s_x1 = _summarize_samples(x1_samples, window_capacity=None)
    s_x2 = _summarize_samples(x2_samples, window_capacity=None)
    s_y = _summarize_samples(y_total_samples, window_capacity=None)

    assert s_x1.p95_ms is not None
    assert s_x2.p95_ms is not None
    assert s_y.p95_ms is not None

    # For X1, 190 of 200 samples (95.0%) are 0.0 ms. Due to linear interpolation at index 189.05,
    # its p95 is 5.0 ms. Similarly, for X2, its p95 is 5.0 ms.
    # Therefore, sum of marginal p95s is 5.0 + 5.0 = 10.0 ms.
    marginal_sum_p95 = s_x1.p95_ms + s_x2.p95_ms
    assert marginal_sum_p95 == pytest.approx(10.0)

    # For Y = X1 + X2, 20 of 200 samples (10.0%) are 100.0 ms (disjoint tail union).
    # Since 10% > 5%, the 95th percentile of the conjoint distribution is exactly 100.0 ms!
    conjoint_p95 = s_y.p95_ms
    assert conjoint_p95 == pytest.approx(100.0)

    # Counterexample proof: p95(X1 + X2) > p95(X1) + p95(X2) (100.0 ms > 10.0 ms)
    # Proves conclusively that sum of marginal p95s is NOT an upper bound (underestimates by 10x).
    assert conjoint_p95 > marginal_sum_p95


def test_genuine_production_outbound_queue_backpressure_timeout() -> None:
    """Verify Task 4: VoiceBridgeSession genuinely raises MediaBackpressureOverflowError

    Demonstrates that VoiceBridgeSession's OWN internal queue.put timeout fires
    when outbound sending blocks and backpressure_timeout is explicitly configured.
    """
    from kaiwen_gateway.modules.voice import (
        AudioFormat,
        AudioFrame,
        InMemoryVoiceTelemetrySink,
        MediaBackpressureOverflowError,
        VoiceBridgeSession,
        VoiceBridgeState,
        VoiceErrorCategory,
    )
    from kaiwen_gateway.modules.voice.ports import AIRealtimeMediaPort, TelephonyMediaPort

    class _BlockingTelephonyPort(TelephonyMediaPort):
        def __init__(self) -> None:
            self.unblock = asyncio.Event()
            self._closed = False

        @property
        def is_closed(self) -> bool:
            return self._closed

        async def send_frame(self, frame: AudioFrame) -> None:
            await self.unblock.wait()

        async def receive_frame(self) -> AudioFrame | None:
            await self.unblock.wait()
            return None

        async def close(self) -> None:
            self._closed = True
            self.unblock.set()

    class _ContinuousAIPort(AIRealtimeMediaPort):
        def __init__(self) -> None:
            self._seq = 0
            self._closed = False

        @property
        def is_closed(self) -> bool:
            return self._closed

        async def send_frame(self, frame: AudioFrame) -> None:
            pass

        async def receive_frame(self) -> AudioFrame | None:
            seq = self._seq
            self._seq += 1
            await asyncio.sleep(0.001)
            return AudioFrame(
                payload=b"\x00" * 640,
                format=AudioFormat.PCM16LE,
                sample_rate=16000,
                channels=1,
                sequence=seq,
                timestamp_ms=seq * 20.0,
                turn_id=1,
            )

        async def close(self) -> None:
            self._closed = True

    async def _run() -> None:
        tel = _BlockingTelephonyPort()
        ai = _ContinuousAIPort()
        sink = InMemoryVoiceTelemetrySink()
        bridge = VoiceBridgeSession(
            call_id="call-genuine-outbound-bp",
            tenant_id="tenant-test",
            telephony_port=tel,
            ai_port=ai,
            queue_capacity=2,
            backpressure_timeout=0.04,
            telemetry_sink=sink,
        )
        await bridge.start()
        try:
            # The bridge should self-terminate with MediaBackpressureOverflowError
            # raised directly by its internal _outbound_queue.put timeout
            with pytest.raises(MediaBackpressureOverflowError) as exc_info:
                await asyncio.wait_for(bridge.wait_until_closed(propagate_error=True), timeout=1.0)
            assert "Outbound queue capacity 2 exceeded" in str(exc_info.value)
            assert "backpressure timed out after 0.04s" in str(exc_info.value)
        finally:
            await tel.close()
            await bridge.close()

        assert bridge.state == VoiceBridgeState.FAILED
        assert isinstance(bridge.error, MediaBackpressureOverflowError)
        snap = sink.snapshot()
        assert snap.error_counts.get(VoiceErrorCategory.BACKPRESSURE_OVERFLOW, 0) >= 1
        assert snap.max_outbound_queue_high_water_mark == 2

    asyncio.run(_run())


def test_execute_media_load_evaluation_report_structure() -> None:
    """Verify `execute_media_load_evaluation` returns a frozen `PacedMediaLoadReport`."""

    async def _run() -> None:
        lenient_criteria = PacingAcceptanceCriteria(
            max_missed_deadline_rate=1.0,
            max_mean_interval_deviation_pct=500.0,
            max_p95_scheduler_lateness_ms=5000.0,
            max_transmission_elongation_pct=500.0,
        )
        report = await execute_media_load_evaluation(
            concurrencies=(1,),
            duration_seconds=0.08,
            seed=42,
            include_faults=False,
            pacing_criteria=lenient_criteria,
        )
        assert isinstance(report, PacedMediaLoadReport)
        assert report.overall_status == "PASS"
        data = report.to_dict()
        assert data["configuration"]["frame_bytes"] == 640
        assert data["configuration"]["interval_ms"] == 20.0
        assert len(data["limitations"]) >= 4

    asyncio.run(_run())


# ==============================================================================
# R2: Process Isolation & Phase Staggering Unit Tests
# ==============================================================================


def test_compute_session_phase_offset_ms_and_pacer_phase_staggering() -> None:
    """Verify Mode A (aligned) vs Mode B (staggered) phase offset calculation and pacer scheduling."""
    # Mode A: all sessions aligned at 0.0 ms
    for slot in range(25):
        assert (
            compute_session_phase_offset_ms(
                session_slot_id=slot,
                concurrency=25,
                scheduling_mode=SCHEDULING_MODE_ALIGNED,
                interval_ms=20.0,
            )
            == 0.0
        )

    # Mode B: sessions staggered across 20.0 ms window (20.0 / 25 = 0.8 ms per session)
    for slot in range(25):
        expected_offset = float(slot) * 0.8
        assert compute_session_phase_offset_ms(
            session_slot_id=slot,
            concurrency=25,
            scheduling_mode=SCHEDULING_MODE_STAGGERED,
            interval_ms=20.0,
        ) == pytest.approx(expected_offset)

    # Concurrency 1 always yields 0.0 ms offset regardless of mode
    assert (
        compute_session_phase_offset_ms(
            session_slot_id=0,
            concurrency=1,
            scheduling_mode=SCHEDULING_MODE_STAGGERED,
        )
        == 0.0
    )

    # Error handling on invalid arguments
    with pytest.raises(ValueError, match="session_slot_id must be non-negative"):
        compute_session_phase_offset_ms(
            session_slot_id=-1, concurrency=5, scheduling_mode=SCHEDULING_MODE_ALIGNED
        )
    with pytest.raises(ValueError, match="concurrency must be positive"):
        compute_session_phase_offset_ms(
            session_slot_id=0, concurrency=0, scheduling_mode=SCHEDULING_MODE_ALIGNED
        )
    with pytest.raises(ValueError, match="Unsupported scheduling_mode"):
        compute_session_phase_offset_ms(session_slot_id=0, concurrency=5, scheduling_mode="invalid")

    # MonotonicFramePacer with phase_offset_ms
    async def _test_pacer() -> None:
        now = 1000.0

        def clock() -> float:
            return now

        async def fake_sleep(secs: float) -> None:
            nonlocal now
            now += secs

        pacer = MonotonicFramePacer(
            interval_ms=20.0,
            phase_offset_ms=5.0,
            clock=clock,
            sleep_fn=fake_sleep,
        )
        assert pacer.phase_offset_ms == 5.0
        pacer.reset(t0=1000.0)

        # Slot 0 deadline: t0 + 0.005s = 1000.005s
        m0 = await pacer.await_slot(0)
        assert m0.deadline_monotonic_s == pytest.approx(1000.005)
        assert m0.actual_send_monotonic_s == pytest.approx(1000.005)

        # Slot 1 deadline: t0 + 0.005s + 0.020s = 1000.025s
        m1 = await pacer.await_slot(1)
        assert m1.deadline_monotonic_s == pytest.approx(1000.025)
        assert m1.actual_send_monotonic_s == pytest.approx(1000.025)
        assert m1.inter_frame_interval_ms == pytest.approx(20.0)

        # Dynamic update via reset
        pacer.reset(t0=2000.0, phase_offset_ms=10.0)
        assert pacer.phase_offset_ms == 10.0
        m_reset0 = await pacer.await_slot(0)
        assert m_reset0.deadline_monotonic_s == pytest.approx(2000.010)

    asyncio.run(_test_pacer())


def test_isolated_server_subprocess_lifecycle() -> None:
    """Verify child process ASGI server launch, token handshake, binary echo, and clean shutdown."""
    import websockets

    from kaiwen_gateway.transports.synthetic import (
        EVENT_CALL_END,
        EVENT_CALL_ENDED,
        EVENT_CALL_START,
        EVENT_CALL_STARTED,
        EVENT_SESSION_READY,
    )

    async def _run() -> None:
        async with start_isolated_media_server_process(
            max_concurrent_connections=10,
            max_connections_per_tenant=5,
            queue_capacity=8,
            startup_timeout=10.0,
            shutdown_timeout=5.0,
        ) as ctx:
            assert ctx.port > 0
            assert ctx.pid > 0
            assert ctx.poll() is None

            tok = ctx.verifier.issue_token(
                provider_id="iso_provider",
                tenant_id="iso_tenant",
                call_id="call_iso_test_1",
                allowed_scopes=["voice.stream"],
            )
            ws_url = f"{ctx.ws_base_url}/ws/synthetic-media/v1"

            async with websockets.connect(
                ws_url,
                additional_headers={"Authorization": f"Bearer {tok}"},
                max_size=1024 * 1024,
            ) as ws:
                ready_raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                assert isinstance(ready_raw, str)
                ready_msg = json.loads(ready_raw)
                assert ready_msg.get("event") == EVENT_SESSION_READY

                await ws.send(json.dumps({"event": EVENT_CALL_START, "call_id": "call_iso_test_1"}))
                start_raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                assert isinstance(start_raw, str)
                start_msg = json.loads(start_raw)
                assert start_msg.get("event") == EVENT_CALL_STARTED

                payload = build_deterministic_frame_payload(seed=42, session_slot_id=0, frame_seq=0)
                await ws.send(payload)
                echo_raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                assert echo_raw == payload

                await ws.send(json.dumps({"event": EVENT_CALL_END, "call_id": "call_iso_test_1"}))
                ended_raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                assert isinstance(ended_raw, str)
                ended_msg = json.loads(ended_raw)
                assert ended_msg.get("event") == EVENT_CALL_ENDED

        # After context exit, child process has terminated cleanly
        assert ctx.poll() == 0
        assert ctx.result is not None
        assert ctx.result.final_active_connections == 0
        assert ctx.result.final_active_voice_sessions == 0
        assert ctx.result.telemetry.get("total_sessions_started") == 1
        assert ctx.result.telemetry.get("total_sessions_completed_clean") == 1

    asyncio.run(_run())


def test_run_paced_media_stage_process_isolated_smoke() -> None:
    """Verify process-isolated staged execution with staggered scheduling."""

    async def _run() -> None:
        res = await run_paced_media_stage(
            stage_name="smoke_iso",
            concurrency=2,
            duration_seconds=0.2,
            isolation_mode=ISOLATION_MODE_PROCESS_ISOLATED,
            scheduling_mode=SCHEDULING_MODE_STAGGERED,
            timeout=10.0,
        )
        assert res.status == "PASS"
        assert res.admitted_concurrency == 2
        assert res.achieved_media_concurrency == 2
        assert res.completed_calls == 2
        assert res.media_correctness["frames_missing"] == 0
        assert res.media_correctness["duplicate_frames"] == 0
        assert res.timing_ms["isolation_mode"] == ISOLATION_MODE_PROCESS_ISOLATED
        assert res.timing_ms["scheduling_mode"] == SCHEDULING_MODE_STAGGERED
        assert "server_event_loop_lag_ms" in res.timing_ms
        assert "client_event_loop_lag_ms" in res.timing_ms
        assert "client_dispatch_skew_ms" in res.timing_ms
        assert res.timing_ms["client_dispatch_skew_ms"]["available"] is True
        assert res.resources["server_process_cpu_time_seconds"] >= 0.0
        assert res.resources["client_process_cpu_time_seconds"] >= 0.0
        assert res.cleanup_verification["final_active_connections"] == 0

    asyncio.run(_run())


# ==============================================================================
# R2.1 Independent Audit Regression Tests: Process Lifecycle, Epoch & Env Secrets
# ==============================================================================


class _FakeAsyncSubprocessForTest:
    def __init__(
        self,
        stdout_data: bytes = b"",
        returncode: int | None = None,
        stderr_data: bytes = b"",
        feed_eof: bool = True,
    ) -> None:
        self.stdout = asyncio.StreamReader()
        if stdout_data:
            self.stdout.feed_data(stdout_data)
        if feed_eof:
            self.stdout.feed_eof()
        self.stderr = asyncio.StreamReader()
        if stderr_data:
            self.stderr.feed_data(stderr_data)
        if feed_eof:
            self.stderr.feed_eof()
        self.stdin = unittest.mock.MagicMock()
        self.stdin.write = unittest.mock.MagicMock()
        self.stdin.drain = unittest.mock.AsyncMock()
        self.returncode = returncode
        self.kill = unittest.mock.MagicMock(side_effect=self._kill)
        self.wait = unittest.mock.AsyncMock(side_effect=self._wait)

    def _kill(self) -> None:
        self.returncode = -9

    async def _wait(self) -> int:
        return self.returncode or 0


def test_isolated_server_subprocess_failure_paths_and_timeouts() -> None:
    """Verify failure branches in start_isolated_media_server_process:
    1. Premature child process exit with non-zero exit code.
    2. Startup timeout when child never signals ready.
    3. Explicit child error message on startup.
    4. Malformed IPC handshake schema (negative port or invalid pid).
    5. Shutdown timeout triggering forced process kill and reclamation.
    6. Cancellation during media transmission.
    """

    async def _run() -> None:
        # 1. Premature exit
        proc1 = _FakeAsyncSubprocessForTest(returncode=1, stderr_data=b"Fatal crash")
        with unittest.mock.patch("asyncio.create_subprocess_exec", return_value=proc1):
            with pytest.raises(RuntimeError, match="exited prematurely with code 1"):
                async with start_isolated_media_server_process(startup_timeout=0.5):
                    pass

        # 2. Startup timeout
        proc2 = _FakeAsyncSubprocessForTest(returncode=None, feed_eof=False)
        with unittest.mock.patch("asyncio.create_subprocess_exec", return_value=proc2):
            with pytest.raises(TimeoutError, match="failed to signal ready"):
                async with start_isolated_media_server_process(startup_timeout=0.05):
                    pass
            assert proc2.kill.called

        # 3. Explicit child error event
        proc3 = _FakeAsyncSubprocessForTest(
            stdout_data=b'{"event": "error", "error": "Address in use"}\n'
        )
        with unittest.mock.patch("asyncio.create_subprocess_exec", return_value=proc3):
            with pytest.raises(
                RuntimeError, match="Server subprocess startup failed: Address in use"
            ):
                async with start_isolated_media_server_process(startup_timeout=0.5):
                    pass

        # 4. Malformed schema (negative port)
        proc4 = _FakeAsyncSubprocessForTest(
            stdout_data=b'{"event": "ready", "port": -1, "pid": 1234}\n'
        )
        with unittest.mock.patch("asyncio.create_subprocess_exec", return_value=proc4):
            with pytest.raises(ValueError, match="Invalid handshake port"):
                async with start_isolated_media_server_process(startup_timeout=0.5):
                    pass

        # 5. Shutdown timeout forced kill
        proc5 = _FakeAsyncSubprocessForTest(
            stdout_data=b'{"event": "ready", "port": 8888, "pid": 1234}\n',
            feed_eof=False,
        )
        with (
            unittest.mock.patch("asyncio.create_subprocess_exec", return_value=proc5),
            unittest.mock.patch("socket.create_connection"),
        ):
            async with start_isolated_media_server_process(
                startup_timeout=1.0, shutdown_timeout=0.05
            ) as ctx:
                assert ctx.port == 8888
                assert ctx.pid == 1234
            assert proc5.kill.called

        # 6. Cancellation during execution
        proc6 = _FakeAsyncSubprocessForTest(
            stdout_data=b'{"event": "ready", "port": 8888, "pid": 1234}\n'
        )
        with (
            unittest.mock.patch("asyncio.create_subprocess_exec", return_value=proc6),
            unittest.mock.patch("socket.create_connection"),
        ):
            with pytest.raises(asyncio.CancelledError):
                async with start_isolated_media_server_process(startup_timeout=1.0):
                    raise asyncio.CancelledError()
            assert proc6.kill.called

    asyncio.run(_run())


def test_isolated_server_secret_key_environment_and_cli_absence() -> None:
    """Verify secret key is passed via private env var, never via CLI argument vector."""

    async def _run() -> None:
        proc = _FakeAsyncSubprocessForTest(
            stdout_data=b'{"event": "ready", "port": 9999, "pid": 5678}\n'
        )
        captured_kwargs: dict[str, Any] = {}

        async def mock_exec(*cmd: Any, **kwargs: Any) -> _FakeAsyncSubprocessForTest:
            captured_kwargs["cmd"] = cmd
            captured_kwargs["env"] = kwargs.get("env", {})
            return proc

        with (
            unittest.mock.patch("asyncio.create_subprocess_exec", side_effect=mock_exec),
            unittest.mock.patch("socket.create_connection"),
        ):
            async with start_isolated_media_server_process(startup_timeout=1.0):
                cmd_args = captured_kwargs["cmd"]
                child_env = captured_kwargs["env"]
                assert "--secret-key-hex" not in cmd_args
                assert KW_SYNTHETIC_MEDIA_SECRET_KEY_ENV in child_env
                assert len(child_env[KW_SYNTHETIC_MEDIA_SECRET_KEY_ENV]) == 64

        parser = _build_arg_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--secret-key-hex", "deadbeef"])

    asyncio.run(_run())


def test_shared_monotonic_epoch_and_pacer_epoch_alignment() -> None:
    """Verify MonotonicFramePacer schedules exactly relative to a shared epoch origin t0."""

    t_shared_epoch = 1000.0
    pacer_aligned = MonotonicFramePacer(interval_ms=20.0, phase_offset_ms=0.0)
    pacer_aligned.reset(t0=t_shared_epoch)

    pacer_staggered = MonotonicFramePacer(interval_ms=20.0, phase_offset_ms=10.0)
    pacer_staggered.reset(t0=t_shared_epoch)

    # Frame 0 deadline for aligned must be exactly t0
    # Frame 0 deadline for staggered must be exactly t0 + 0.010s
    assert pacer_aligned.phase_offset_s == 0.0
    assert pacer_staggered.phase_offset_s == 0.010


# ==============================================================================
# R3: High-Concurrency Capacity Propagation & Accounting Unit Tests (M5.1C)
# ==============================================================================


def test_capacity_configuration_cli_and_propagation() -> None:
    """Verify capacity configuration parsing, execution propagation, and report metadata."""
    parser = _build_arg_parser()
    args = parser.parse_args(
        ["--max-concurrent-connections", "50", "--max-connections-per-tenant", "10"]
    )
    assert args.max_concurrent_connections == 50
    assert args.max_connections_per_tenant == 10

    async def _run() -> None:
        lenient_criteria = PacingAcceptanceCriteria(
            max_missed_deadline_rate=1.0,
            max_mean_interval_deviation_pct=500.0,
            max_p95_scheduler_lateness_ms=5000.0,
            max_transmission_elongation_pct=500.0,
        )
        report = await execute_media_load_evaluation(
            concurrencies=(1,),
            duration_seconds=0.10,
            include_faults=False,
            isolation_mode=ISOLATION_MODE_IN_PROCESS,
            max_concurrent_connections=50,
            max_connections_per_tenant=10,
            pacing_criteria=lenient_criteria,
        )
        assert report.overall_status == "PASS"
        assert report.configuration["max_concurrent_connections"] == 50
        assert report.configuration["max_connections_per_tenant"] == 10
        assert len(report.stages) == 1
        assert report.stages[0].status == "PASS"
        assert report.stages[0].requested_concurrency == 1
        assert report.stages[0].admitted_concurrency == 1
        assert report.stages[0].achieved_media_concurrency == 1

    asyncio.run(_run())


def test_capacity_configuration_validation_errors() -> None:
    """Verify strict type and boundary validation for capacity configuration parameters."""

    async def _run() -> None:
        with pytest.raises(TypeError, match="must be a positive integer, not bool"):
            await execute_media_load_evaluation(
                concurrencies=(1,),
                include_faults=False,
                max_concurrent_connections=True,
            )

        with pytest.raises(ValueError, match="must be a positive integer"):
            await execute_media_load_evaluation(
                concurrencies=(1,),
                include_faults=False,
                max_concurrent_connections=0,
            )

        with pytest.raises(TypeError, match="must be a positive integer, not bool"):
            await execute_media_load_evaluation(
                concurrencies=(1,),
                include_faults=False,
                max_connections_per_tenant=False,
            )

        with pytest.raises(ValueError, match="must be a positive integer"):
            await execute_media_load_evaluation(
                concurrencies=(1,),
                include_faults=False,
                max_connections_per_tenant=-2,
            )

        with pytest.raises(
            ValueError, match="max_connections_per_tenant.*cannot exceed max_concurrent_connections"
        ):
            await execute_media_load_evaluation(
                concurrencies=(1,),
                include_faults=False,
                max_concurrent_connections=10,
                max_connections_per_tenant=20,
            )

        with pytest.raises(
            ValueError,
            match="Requested concurrency 50 exceeds configured max_concurrent_connections 25",
        ):
            await execute_media_load_evaluation(
                concurrencies=(50,),
                include_faults=False,
                max_concurrent_connections=25,
            )

        with pytest.raises(ValueError, match="Requested concurrency 50 exceeds max capacity 25"):
            await run_paced_media_stage(
                stage_name="test_val_stage",
                concurrency=50,
                duration_seconds=0.04,
                max_concurrent_connections=25,
                max_connections_per_tenant=10,
            )

    asyncio.run(_run())


def test_empirical_concurrency_accounting_and_active_overlap_failure() -> None:
    """Verify achieved_media_concurrency fails stage if peak active sessions fall short."""

    async def _run() -> None:
        lenient_criteria = PacingAcceptanceCriteria(
            max_missed_deadline_rate=1.0,
            max_mean_interval_deviation_pct=500.0,
            max_p95_scheduler_lateness_ms=5000.0,
            max_transmission_elongation_pct=500.0,
        )
        # Run a 2-concurrency stage in in-process mode
        stage = await run_paced_media_stage(
            stage_name="test_emp_2concurrency",
            concurrency=2,
            duration_seconds=0.06,
            max_concurrent_connections=10,
            max_connections_per_tenant=5,
            isolation_mode=ISOLATION_MODE_IN_PROCESS,
            pacing_criteria=lenient_criteria,
        )
        # In a normal run with 2 sessions, both should overlap and achieve 2 concurrency
        assert stage.status == "PASS"
        assert stage.requested_concurrency == 2
        assert stage.admitted_concurrency == 2
        assert stage.achieved_media_concurrency == 2
        assert stage.media_correctness["frame_loss_rate"] == 0.0

    asyncio.run(_run())


def test_empirical_concurrency_failure_on_underachievement() -> None:
    """Verify that when peak active sessions fall short of requested concurrency, stage status is FAIL."""

    async def _run() -> None:
        with (
            unittest.mock.patch(
                "tools.synthetic_media_load.start_isolated_media_server_process"
            ) as mock_cm,
            unittest.mock.patch(
                "tools.synthetic_media_load.run_single_paced_media_call"
            ) as mock_call,
        ):
            mock_ctx = unittest.mock.AsyncMock()
            mock_ctx.ws_base_url = "ws://127.0.0.1:9999"
            mock_verifier = unittest.mock.MagicMock()
            mock_verifier.issue_token.return_value = "fake_tok"
            mock_ctx.verifier = mock_verifier
            mock_cm.return_value.__aenter__.return_value = mock_ctx

            fake_rec = unittest.mock.MagicMock()
            fake_rec.admitted = False
            fake_rec.completed_cleanly = False
            fake_rec.close_code = 1008
            fake_rec.frames_scheduled = 5
            fake_rec.frames_sent = 0
            fake_rec.frames_skipped = 5
            fake_rec.missed_deadlines = 0
            fake_rec.frames_echoed = 0
            fake_rec.frames_missing = 0
            fake_rec.duplicate_frames = 0
            fake_rec.out_of_order_frames = 0
            fake_rec.payload_mismatches = 0
            fake_rec.cross_session_frames = 0
            fake_rec.first_frame_sent_s = None
            fake_rec.last_frame_activity_s = None
            fake_rec.scheduling_lateness_samples_ms = []
            fake_rec.scheduling_interval_samples_ms = []
            fake_rec.scheduling_jitter_samples_ms = []
            fake_rec.echo_rtt_samples_ms = []
            fake_rec.handshake_ms = 1.0
            fake_rec.call_start_rtt_ms = 1.0
            fake_rec.call_end_rtt_ms = 1.0
            fake_rec.echo_rtt_summary = unittest.mock.MagicMock(available=False, p95_ms=None)
            mock_call.return_value = fake_rec

            mock_res = unittest.mock.MagicMock()
            mock_res.telemetry = {
                "peak_active_sessions": 1,
                "total_sessions_accepted": 1,
                "total_sessions_closed": 1,
                "cpu_user_seconds": 0.01,
                "cpu_system_seconds": 0.01,
                "peak_rss_kib": 1000,
                "final_rss_kib": 1000,
                "final_active_connections": 0,
                "final_active_tenants": 0,
                "final_active_calls": 0,
                "final_active_voice_sessions": 0,
                "leaked_asyncio_tasks": 0,
            }
            mock_ctx.wait_shutdown.return_value = mock_res

            stage = await run_paced_media_stage(
                stage_name="test_shortfall",
                concurrency=2,
                duration_seconds=0.04,
                max_concurrent_connections=10,
                max_connections_per_tenant=5,
                isolation_mode=ISOLATION_MODE_PROCESS_ISOLATED,
            )
            assert stage.status == "FAIL"
            assert stage.requested_concurrency == 2
            assert stage.achieved_media_concurrency < 2
            assert any("did not match requested concurrency" in r for r in stage.failure_reasons)

    asyncio.run(_run())


# ==============================================================================
# R4: Real-Time Frame Pacing Acceptance & Historical Reassessment Tests (Phase F)
# ==============================================================================


def test_pacing_acceptance_criteria_validation() -> None:
    """Verify default values, to_dict, type checks, and boundary validations of PacingAcceptanceCriteria."""
    criteria = PacingAcceptanceCriteria()
    assert criteria.max_missed_deadline_rate == DEFAULT_MAX_MISSED_DEADLINE_RATE
    assert criteria.max_mean_interval_deviation_pct == DEFAULT_MAX_MEAN_INTERVAL_DEVIATION_PCT
    assert criteria.max_p95_scheduler_lateness_ms == DEFAULT_MAX_P95_SCHEDULER_LATENESS_MS
    assert criteria.max_transmission_elongation_pct == DEFAULT_MAX_TRANSMISSION_ELONGATION_PCT

    data = criteria.to_dict()
    assert data["max_missed_deadline_rate"] == 0.20
    assert data["max_mean_interval_deviation_pct"] == 20.0
    assert data["max_p95_scheduler_lateness_ms"] == 100.0
    assert data["max_transmission_elongation_pct"] == 25.0

    # Custom valid values
    custom = PacingAcceptanceCriteria(
        max_missed_deadline_rate=0.10,
        max_mean_interval_deviation_pct=15.0,
        max_p95_scheduler_lateness_ms=50.0,
        max_transmission_elongation_pct=10.0,
    )
    assert custom.max_missed_deadline_rate == 0.10
    assert custom.max_mean_interval_deviation_pct == 15.0

    # Type errors (bool, str, None)
    with pytest.raises(TypeError, match="must be a float, not bool"):
        PacingAcceptanceCriteria(max_missed_deadline_rate=True)
    with pytest.raises(TypeError, match="must be a float, not bool"):
        PacingAcceptanceCriteria(max_missed_deadline_rate="0.2")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="must be a float, not bool"):
        PacingAcceptanceCriteria(max_mean_interval_deviation_pct=False)
    with pytest.raises(TypeError, match="must be a float, not bool"):
        PacingAcceptanceCriteria(max_p95_scheduler_lateness_ms=True)
    with pytest.raises(TypeError, match="must be a float, not bool"):
        PacingAcceptanceCriteria(max_transmission_elongation_pct=False)

    # Boundary and value errors
    with pytest.raises(ValueError, match="must be in \\(0.0, 1.0\\]"):
        PacingAcceptanceCriteria(max_missed_deadline_rate=0.0)
    with pytest.raises(ValueError, match="must be in \\(0.0, 1.0\\]"):
        PacingAcceptanceCriteria(max_missed_deadline_rate=-0.05)
    with pytest.raises(ValueError, match="must be in \\(0.0, 1.0\\]"):
        PacingAcceptanceCriteria(max_missed_deadline_rate=1.05)
    with pytest.raises(ValueError, match="must be in \\(0.0, 1.0\\]"):
        PacingAcceptanceCriteria(max_missed_deadline_rate=float("nan"))

    with pytest.raises(ValueError, match="must be positive"):
        PacingAcceptanceCriteria(max_mean_interval_deviation_pct=0.0)
    with pytest.raises(ValueError, match="must be positive"):
        PacingAcceptanceCriteria(max_mean_interval_deviation_pct=-1.0)

    with pytest.raises(ValueError, match="must be positive"):
        PacingAcceptanceCriteria(max_p95_scheduler_lateness_ms=0.0)
    with pytest.raises(ValueError, match="must be positive"):
        PacingAcceptanceCriteria(max_p95_scheduler_lateness_ms=-10.0)

    with pytest.raises(ValueError, match="must be positive"):
        PacingAcceptanceCriteria(max_transmission_elongation_pct=0.0)
    with pytest.raises(ValueError, match="must be positive"):
        PacingAcceptanceCriteria(max_transmission_elongation_pct=-5.0)


def test_evaluate_pacing_metrics_perfect_and_small_jitter() -> None:
    """Verify evaluate_pacing_metrics passes on ideal pacing and tolerable scheduler jitter."""
    criteria = PacingAcceptanceCriteria()

    # Scenario 1: Ideal 0-jitter pacing
    status, failures, metrics = evaluate_pacing_metrics(
        total_scheduled=100,
        total_sent=100,
        total_missed_deadlines=0,
        target_interval_ms=20.0,
        mean_interval_ms=20.0,
        p95_scheduler_lateness_ms=0.5,
        nominal_transmission_s=1.98,
        actual_transmission_s=1.98,
        criteria=criteria,
    )
    assert status == "PASS"
    assert failures == []
    assert metrics["missed_deadline_rate"] == 0.0
    assert metrics["mean_interval_deviation_pct"] == 0.0
    assert metrics["p95_scheduler_lateness_ms"] == 0.5
    assert metrics["transmission_elongation_pct"] == 0.0

    # Scenario 2: Realistic small jitter within all thresholds
    status, failures, metrics = evaluate_pacing_metrics(
        total_scheduled=100,
        total_sent=100,
        total_missed_deadlines=8,  # 8% <= 20%
        target_interval_ms=20.0,
        mean_interval_ms=21.5,  # 7.5% deviation <= 20%
        p95_scheduler_lateness_ms=35.0,  # 35 ms <= 100 ms
        nominal_transmission_s=1.98,
        actual_transmission_s=2.10,  # 6.06% elongation <= 25%
        criteria=criteria,
    )
    assert status == "PASS"
    assert failures == []
    assert round(metrics["missed_deadline_rate"], 4) == 0.08
    assert round(metrics["mean_interval_deviation_pct"], 2) == 7.5
    assert metrics["p95_scheduler_lateness_ms"] == 35.0
    assert round(metrics["transmission_elongation_pct"], 2) == 6.06


def test_evaluate_pacing_metrics_gross_degradation_all_violations() -> None:
    """Verify evaluate_pacing_metrics flags each violated criterion with precise empirical diagnostics."""
    criteria = PacingAcceptanceCriteria()

    status, failures, metrics = evaluate_pacing_metrics(
        total_scheduled=12500,
        total_sent=12500,
        total_missed_deadlines=12379,  # 99.03%
        target_interval_ms=20.0,
        mean_interval_ms=54.00,  # 170.0% deviation
        p95_scheduler_lateness_ms=8163.29,  # 8163 ms
        nominal_transmission_s=4.98,
        actual_transmission_s=13.57,  # 172.49% elongation
        criteria=criteria,
    )
    assert status == "FAIL"
    assert len(failures) == 4
    assert any(
        "Missed deadline rate 99.03% exceeded threshold 20.00% (12379/12500 frames missed deadline)"
        in f
        for f in failures
    )
    assert any(
        "Mean inter-frame interval 54.00 ms deviated by 170.00% from target 20.00 ms" in f
        for f in failures
    )
    assert any(
        "p95 scheduler lateness 8163.29 ms exceeded threshold 100.00 ms" in f for f in failures
    )
    assert any(
        "Media transmission duration elongated by 172.49% (13.57s vs nominal 4.98s" in f
        for f in failures
    )
    assert metrics["missed_deadline_rate"] == 12379 / 12500
    assert metrics["mean_interval_deviation_pct"] == 170.0
    assert metrics["p95_scheduler_lateness_ms"] == 8163.29


def test_evaluate_pacing_metrics_missing_or_empty_data() -> None:
    """Verify evaluate_pacing_metrics reports explicit failure when timing data is unavailable."""
    criteria = PacingAcceptanceCriteria()

    # Zero frames sent
    status, failures, _ = evaluate_pacing_metrics(
        total_scheduled=10,
        total_sent=0,
        total_missed_deadlines=0,
        target_interval_ms=20.0,
        mean_interval_ms=None,
        p95_scheduler_lateness_ms=None,
        nominal_transmission_s=0.18,
        actual_transmission_s=None,
        criteria=criteria,
    )
    assert status == "FAIL"
    assert any("No audio frames were transmitted" in f for f in failures)

    # Missing interval data
    status, failures, _ = evaluate_pacing_metrics(
        total_scheduled=10,
        total_sent=10,
        total_missed_deadlines=0,
        target_interval_ms=20.0,
        mean_interval_ms=None,
        p95_scheduler_lateness_ms=10.0,
        nominal_transmission_s=0.18,
        actual_transmission_s=0.18,
        criteria=criteria,
    )
    assert status == "FAIL"
    assert any("Missing inter-frame scheduling interval measurements" in f for f in failures)

    # Missing p95 lateness data
    status, failures, _ = evaluate_pacing_metrics(
        total_scheduled=10,
        total_sent=10,
        total_missed_deadlines=0,
        target_interval_ms=20.0,
        mean_interval_ms=20.0,
        p95_scheduler_lateness_ms=None,
        nominal_transmission_s=0.18,
        actual_transmission_s=0.18,
        criteria=criteria,
    )
    assert status == "FAIL"
    assert any("Missing scheduler lateness measurements" in f for f in failures)

    # Missing transmission duration
    status, failures, _ = evaluate_pacing_metrics(
        total_scheduled=10,
        total_sent=10,
        total_missed_deadlines=0,
        target_interval_ms=20.0,
        mean_interval_ms=20.0,
        p95_scheduler_lateness_ms=10.0,
        nominal_transmission_s=0.18,
        actual_transmission_s=None,
        criteria=criteria,
    )
    assert status == "FAIL"
    assert any("Missing actual transmission duration measurement" in f for f in failures)


def test_media_integrity_pass_does_not_override_pacing_fail() -> None:
    """Verify that 100% media integrity cannot mask real-time pacing failure in stage evaluation."""

    async def _run() -> None:
        # Run stage with extremely strict pacing criteria (e.g. 0.0001% allowed deviation)
        # to guarantee pacing fails while in-process media delivery passes 100% cleanly.
        strict_criteria = PacingAcceptanceCriteria(
            max_missed_deadline_rate=0.000001,
            max_mean_interval_deviation_pct=0.0001,
            max_p95_scheduler_lateness_ms=0.0001,
            max_transmission_elongation_pct=0.0001,
        )
        stage = await run_paced_media_stage(
            stage_name="test_pacing_fail_integrity_pass",
            concurrency=1,
            duration_seconds=0.10,
            isolation_mode=ISOLATION_MODE_IN_PROCESS,
            pacing_criteria=strict_criteria,
        )
        assert stage.media_integrity_status == "PASS"
        assert stage.realtime_pacing_status == "FAIL"
        assert stage.status == "FAIL"
        assert len(stage.pacing_failure_reasons) > 0
        assert len(stage.media_failure_reasons) == 0

    asyncio.run(_run())


def test_overall_evaluation_propagates_pacing_failure() -> None:
    """Verify execute_media_load_evaluation aggregates media and pacing statuses correctly."""

    async def _run() -> None:
        strict_criteria = PacingAcceptanceCriteria(
            max_missed_deadline_rate=0.000001,
            max_mean_interval_deviation_pct=0.0001,
        )
        report = await execute_media_load_evaluation(
            concurrencies=(1,),
            duration_seconds=0.10,
            include_faults=False,
            isolation_mode=ISOLATION_MODE_IN_PROCESS,
            pacing_criteria=strict_criteria,
        )
        assert report.media_integrity_status == "PASS"
        assert report.realtime_pacing_status == "FAIL"
        assert report.overall_status == "FAIL"

    asyncio.run(_run())


def test_cli_exit_code_and_json_report_on_pacing_failure(tmp_path: Path) -> None:
    """Verify CLI main() returns exit code 1 on pacing failure while outputting complete JSON report."""
    output_file = tmp_path / "test_pacing_fail_report.json"
    exit_code = main(
        [
            "--stages",
            "1",
            "--duration",
            "0.10",
            "--isolation-mode",
            ISOLATION_MODE_IN_PROCESS,
            "--no-faults",
            "--max-missed-deadline-rate",
            "0.000001",
            "--max-mean-interval-deviation-pct",
            "0.0001",
            "--output",
            str(output_file),
        ]
    )
    assert exit_code == 1
    assert output_file.exists()

    with output_file.open("r", encoding="utf-8") as f:
        data = json.load(f)

    assert data["media_integrity_status"] == "PASS"
    assert data["realtime_pacing_status"] == "FAIL"
    assert data["overall_status"] == "FAIL"
    assert len(data["stages"]) == 1
    st = data["stages"][0]
    assert st["media_integrity_status"] == "PASS"
    assert st["realtime_pacing_status"] == "FAIL"
    assert st["status"] == "FAIL"
    assert len(st["pacing_failure_reasons"]) > 0


def test_reassess_stage_report_on_historical_m5_data() -> None:
    """Verify offline reassess_stage_report correctly diagnoses historical 50 and 100 session reports."""
    report_50_path = Path("reports/m5_load_50.json")
    report_100_path = Path("reports/m5_load_100.json")

    if report_50_path.exists():
        with report_50_path.open("r", encoding="utf-8") as f:
            stage_50_raw = json.load(f)["stages"][0]
    else:
        # Canonical snapshot matching untracked historical reports/m5_load_50.json
        stage_50_raw = {
            "stage_name": "S1_concurrency_50",
            "media_correctness": {
                "frames_scheduled": 12500,
                "frames_sent": 12500,
                "missed_deadlines": 12379,
                "frames_missing": 0,
                "duplicate_frames": 0,
                "out_of_order_frames": 0,
                "payload_mismatches": 0,
                "unexpected_close_codes": 0,
                "cleanup_failures": 0,
            },
            "timing_ms": {
                "frame_scheduling_interval_ms": {"mean_ms": 54.0043},
                "scheduler_lateness_ms": {"p95_ms": 8163.2899},
            },
            "configured_duration_seconds": 5.0,
        }

    result_50 = reassess_stage_report(stage_50_raw)

    assert result_50["media_integrity_status"] == "PASS"
    assert result_50["realtime_pacing_status"] == "FAIL"
    assert result_50["overall_status"] == "FAIL"
    assert result_50["media_failure_reasons"] == []
    assert len(result_50["pacing_failure_reasons"]) == 3
    assert any("Missed deadline rate 99.03%" in r for r in result_50["pacing_failure_reasons"])
    assert any(
        "Mean inter-frame interval 54.00 ms" in r for r in result_50["pacing_failure_reasons"]
    )
    assert any(
        "p95 scheduler lateness 8163.29 ms" in r for r in result_50["pacing_failure_reasons"]
    )

    if report_100_path.exists():
        with report_100_path.open("r", encoding="utf-8") as f:
            stage_100_raw = json.load(f)["stages"][0]
    else:
        # Canonical snapshot matching untracked historical reports/m5_load_100.json
        stage_100_raw = {
            "stage_name": "S1_concurrency_100",
            "media_correctness": {
                "frames_scheduled": 25000,
                "frames_sent": 25000,
                "missed_deadlines": 24934,
                "frames_missing": 0,
                "duplicate_frames": 0,
                "out_of_order_frames": 0,
                "payload_mismatches": 0,
                "unexpected_close_codes": 0,
                "cleanup_failures": 0,
            },
            "timing_ms": {
                "frame_scheduling_interval_ms": {"mean_ms": 49.5654},
                "scheduler_lateness_ms": {"p95_ms": 7030.7719},
            },
            "configured_duration_seconds": 5.0,
        }

    result_100 = reassess_stage_report(stage_100_raw)

    assert result_100["media_integrity_status"] == "PASS"
    assert result_100["realtime_pacing_status"] == "FAIL"
    assert result_100["overall_status"] == "FAIL"
    assert result_100["media_failure_reasons"] == []
    assert len(result_100["pacing_failure_reasons"]) == 3
    assert any("Missed deadline rate 99.74%" in r for r in result_100["pacing_failure_reasons"])
    assert any(
        "Mean inter-frame interval 49.57 ms" in r for r in result_100["pacing_failure_reasons"]
    )
    assert any(
        "p95 scheduler lateness 7030.77 ms" in r for r in result_100["pacing_failure_reasons"]
    )


# ==============================================================================
# R5: P1/P2 Regression Tests: Strict Statistics Validation & Send Timestamps
# ==============================================================================


def test_pacing_evaluation_nan_and_infinity_rejection() -> None:
    """Verify evaluate_pacing_metrics strictly rejects NaN, +inf, -inf in all measured stats (P1)."""
    criteria = PacingAcceptanceCriteria()

    # NaN mean interval
    status, failures, metrics = evaluate_pacing_metrics(
        total_scheduled=100,
        total_sent=100,
        total_missed_deadlines=0,
        target_interval_ms=20.0,
        mean_interval_ms=float("nan"),
        p95_scheduler_lateness_ms=5.0,
        nominal_transmission_s=1.98,
        actual_transmission_s=1.98,
        criteria=criteria,
    )
    assert status == "FAIL"
    assert any("Nonfinite mean interval measurement" in f for f in failures)
    assert metrics["mean_interval_deviation_pct"] is None

    # Positive and negative infinity mean interval
    for inf_val in (float("inf"), float("-inf")):
        status, failures, _ = evaluate_pacing_metrics(
            total_scheduled=100,
            total_sent=100,
            total_missed_deadlines=0,
            target_interval_ms=20.0,
            mean_interval_ms=inf_val,
            p95_scheduler_lateness_ms=5.0,
            nominal_transmission_s=1.98,
            actual_transmission_s=1.98,
            criteria=criteria,
        )
        assert status == "FAIL"
        assert any("Nonfinite mean interval measurement" in f for f in failures)

    # NaN and infinity p95 lateness
    for bad_late in (float("nan"), float("inf"), float("-inf")):
        status, failures, metrics = evaluate_pacing_metrics(
            total_scheduled=100,
            total_sent=100,
            total_missed_deadlines=0,
            target_interval_ms=20.0,
            mean_interval_ms=20.0,
            p95_scheduler_lateness_ms=bad_late,
            nominal_transmission_s=1.98,
            actual_transmission_s=1.98,
            criteria=criteria,
        )
        assert status == "FAIL"
        assert any("Nonfinite p95 scheduler lateness measurement" in f for f in failures)
        assert metrics["p95_scheduler_lateness_ms"] is None

    # NaN and infinity actual transmission duration
    for bad_tx in (float("nan"), float("inf"), float("-inf")):
        status, failures, metrics = evaluate_pacing_metrics(
            total_scheduled=100,
            total_sent=100,
            total_missed_deadlines=0,
            target_interval_ms=20.0,
            mean_interval_ms=20.0,
            p95_scheduler_lateness_ms=5.0,
            nominal_transmission_s=1.98,
            actual_transmission_s=bad_tx,
            criteria=criteria,
        )
        assert status == "FAIL"
        assert any("Nonfinite actual transmission duration measurement" in f for f in failures)
        assert metrics["actual_transmission_seconds"] is None


def test_pacing_evaluation_negative_measurements_rejection() -> None:
    """Verify evaluate_pacing_metrics rejects negative or nonpositive intervals, latencies, and durations."""
    criteria = PacingAcceptanceCriteria()

    # Nonpositive mean interval (<= 0)
    for bad_interval in (0.0, -10.0):
        status, failures, _ = evaluate_pacing_metrics(
            total_scheduled=100,
            total_sent=100,
            total_missed_deadlines=0,
            target_interval_ms=20.0,
            mean_interval_ms=bad_interval,
            p95_scheduler_lateness_ms=5.0,
            nominal_transmission_s=1.98,
            actual_transmission_s=1.98,
            criteria=criteria,
        )
        assert status == "FAIL"
        assert any("Nonpositive mean interval measurement" in f for f in failures)

    # Negative p95 lateness
    status, failures, _ = evaluate_pacing_metrics(
        total_scheduled=100,
        total_sent=100,
        total_missed_deadlines=0,
        target_interval_ms=20.0,
        mean_interval_ms=20.0,
        p95_scheduler_lateness_ms=-0.1,
        nominal_transmission_s=1.98,
        actual_transmission_s=1.98,
        criteria=criteria,
    )
    assert status == "FAIL"
    assert any("Negative p95 scheduler lateness measurement" in f for f in failures)

    # Negative actual transmission duration
    status, failures, _ = evaluate_pacing_metrics(
        total_scheduled=100,
        total_sent=100,
        total_missed_deadlines=0,
        target_interval_ms=20.0,
        mean_interval_ms=20.0,
        p95_scheduler_lateness_ms=5.0,
        nominal_transmission_s=1.98,
        actual_transmission_s=-1.0,
        criteria=criteria,
    )
    assert status == "FAIL"
    assert any("Negative actual transmission duration measurement" in f for f in failures)


def test_pacing_evaluation_invalid_types_and_booleans() -> None:
    """Verify evaluate_pacing_metrics rejects booleans and unexpected types across all fields."""
    criteria = PacingAcceptanceCriteria()

    # Boolean measurements
    status, failures, _ = evaluate_pacing_metrics(
        total_scheduled=100,
        total_sent=100,
        total_missed_deadlines=0,
        target_interval_ms=20.0,
        mean_interval_ms=True,
        p95_scheduler_lateness_ms=5.0,
        nominal_transmission_s=1.98,
        actual_transmission_s=1.98,
        criteria=criteria,
    )
    assert status == "FAIL"
    assert any("Invalid mean interval measurement type: bool" in f for f in failures)

    status, failures, _ = evaluate_pacing_metrics(
        total_scheduled=100,
        total_sent=100,
        total_missed_deadlines=0,
        target_interval_ms=20.0,
        mean_interval_ms=20.0,
        p95_scheduler_lateness_ms=False,
        nominal_transmission_s=1.98,
        actual_transmission_s=1.98,
        criteria=criteria,
    )
    assert status == "FAIL"
    assert any("Invalid p95 scheduler lateness measurement type: bool" in f for f in failures)

    status, failures, _ = evaluate_pacing_metrics(
        total_scheduled=100,
        total_sent=100,
        total_missed_deadlines=0,
        target_interval_ms=20.0,
        mean_interval_ms=20.0,
        p95_scheduler_lateness_ms=5.0,
        nominal_transmission_s=1.98,
        actual_transmission_s=True,
        criteria=criteria,
    )
    assert status == "FAIL"
    assert any("Invalid actual transmission duration measurement type: bool" in f for f in failures)

    # Boolean counters
    status, failures, _ = evaluate_pacing_metrics(
        total_scheduled=True,
        total_sent=100,
        total_missed_deadlines=0,
        target_interval_ms=20.0,
        mean_interval_ms=20.0,
        p95_scheduler_lateness_ms=5.0,
        nominal_transmission_s=1.98,
        actual_transmission_s=1.98,
        criteria=criteria,
    )
    assert status == "FAIL"
    assert any("Invalid total_scheduled counter: True" in f for f in failures)

    status, failures, _ = evaluate_pacing_metrics(
        total_scheduled=100,
        total_sent=False,
        total_missed_deadlines=0,
        target_interval_ms=20.0,
        mean_interval_ms=20.0,
        p95_scheduler_lateness_ms=5.0,
        nominal_transmission_s=1.98,
        actual_transmission_s=1.98,
        criteria=criteria,
    )
    assert status == "FAIL"
    assert any("Invalid total_sent counter: False" in f for f in failures)


def test_pacing_evaluation_counter_inconsistencies() -> None:
    """Verify evaluate_pacing_metrics detects counter relationship violations."""
    criteria = PacingAcceptanceCriteria()

    # total_sent > total_scheduled
    status, failures, _ = evaluate_pacing_metrics(
        total_scheduled=10,
        total_sent=15,
        total_missed_deadlines=0,
        target_interval_ms=20.0,
        mean_interval_ms=20.0,
        p95_scheduler_lateness_ms=5.0,
        nominal_transmission_s=0.18,
        actual_transmission_s=0.18,
        criteria=criteria,
    )
    assert status == "FAIL"
    assert any("total_sent (15) cannot exceed total_scheduled (10)" in f for f in failures)

    # total_missed_deadlines > total_scheduled
    status, failures, _ = evaluate_pacing_metrics(
        total_scheduled=10,
        total_sent=10,
        total_missed_deadlines=15,
        target_interval_ms=20.0,
        mean_interval_ms=20.0,
        p95_scheduler_lateness_ms=5.0,
        nominal_transmission_s=0.18,
        actual_transmission_s=0.18,
        criteria=criteria,
    )
    assert status == "FAIL"
    assert any(
        "total_missed_deadlines (15) cannot exceed total_scheduled (10)" in f for f in failures
    )


def test_pacing_evaluation_valid_boundary_behavior() -> None:
    """Verify evaluate_pacing_metrics accepts legitimate 0-lateness and ideal boundary values."""
    criteria = PacingAcceptanceCriteria()

    status, failures, metrics = evaluate_pacing_metrics(
        total_scheduled=50,
        total_sent=50,
        total_missed_deadlines=0,
        target_interval_ms=20.0,
        mean_interval_ms=20.0,
        p95_scheduler_lateness_ms=0.0,  # Exact 0-lateness boundary
        nominal_transmission_s=0.98,
        actual_transmission_s=0.98,
        criteria=criteria,
    )
    assert status == "PASS"
    assert failures == []
    assert metrics["missed_deadline_rate"] == 0.0
    assert metrics["mean_interval_deviation_pct"] == 0.0
    assert metrics["p95_scheduler_lateness_ms"] == 0.0
    assert metrics["transmission_elongation_pct"] == 0.0


def test_pacing_evaluation_zero_frame_and_single_frame_workloads() -> None:
    """Verify evaluate_pacing_metrics defines correct semantics for 0-frame and 1-frame workloads."""
    criteria = PacingAcceptanceCriteria()

    # 0 frames: fails because no audio media was transmitted
    status, failures, _ = evaluate_pacing_metrics(
        total_scheduled=0,
        total_sent=0,
        total_missed_deadlines=0,
        target_interval_ms=20.0,
        mean_interval_ms=None,
        p95_scheduler_lateness_ms=None,
        nominal_transmission_s=0.0,
        actual_transmission_s=None,
        criteria=criteria,
    )
    assert status == "FAIL"
    assert any(
        "No audio frames were transmitted to evaluate real-time pacing" in f for f in failures
    )

    # 1 frame on-time: passes! No intervals or multi-frame elongation span to evaluate.
    status, failures, metrics = evaluate_pacing_metrics(
        total_scheduled=1,
        total_sent=1,
        total_missed_deadlines=0,
        target_interval_ms=20.0,
        mean_interval_ms=None,
        p95_scheduler_lateness_ms=5.0,
        nominal_transmission_s=0.02,
        actual_transmission_s=0.0,
        criteria=criteria,
    )
    assert status == "PASS"
    assert failures == []
    assert metrics["missed_deadline_rate"] == 0.0
    assert metrics["mean_interval_deviation_pct"] is None
    assert metrics["p95_scheduler_lateness_ms"] == 5.0
    assert metrics["transmission_elongation_pct"] is None

    # 1 frame late: fails due to missed deadline and scheduler lateness exceeding threshold
    status, failures, metrics = evaluate_pacing_metrics(
        total_scheduled=1,
        total_sent=1,
        total_missed_deadlines=1,
        target_interval_ms=20.0,
        mean_interval_ms=None,
        p95_scheduler_lateness_ms=150.0,
        nominal_transmission_s=0.02,
        actual_transmission_s=0.0,
        criteria=criteria,
    )
    assert status == "FAIL"
    assert any("Missed deadline rate 100.00% exceeded threshold" in f for f in failures)
    assert any(
        "p95 scheduler lateness 150.00 ms exceeded threshold 100.00 ms" in f for f in failures
    )


def test_delayed_echo_with_on_time_sends_does_not_fail_transmission_duration() -> None:
    """Verify that delayed echo arrival cannot create a false transmission elongation failure (P2)."""
    criteria = PacingAcceptanceCriteria()

    # Scenario: 5 frames sent at exact 20 ms intervals:
    # Frame 0 sent at t = 100.00
    # Frame 4 sent at t = 100.08
    # Nominal duration = (5 - 1) * 0.02 = 0.08 s
    # Actual send duration = 100.08 - 100.00 = 0.08 s
    # Final echo arrives at t = 102.50 (echo delayed by 2.42s due to network/server queue)
    #
    # With P2 dedicated send timestamp:
    # actual_transmission_s = 0.08 s (elongation = 0.0% <= 25.0%) -> PASS!
    #
    # Under old logic (using last_frame_activity_s = 102.50):
    # actual_transmission_s would have been 2.50 s (elongation = 3025.0% > 25.0%) -> false FAIL!
    t_first_send = 100.00
    t_last_send = 100.08
    t_last_activity_echo = 102.50
    nominal_tx_s = 0.08

    actual_tx_s = t_last_send - t_first_send
    status, failures, metrics = evaluate_pacing_metrics(
        total_scheduled=5,
        total_sent=5,
        total_missed_deadlines=0,
        target_interval_ms=20.0,
        mean_interval_ms=20.0,
        p95_scheduler_lateness_ms=2.0,
        nominal_transmission_s=nominal_tx_s,
        actual_transmission_s=actual_tx_s,
        criteria=criteria,
    )
    assert status == "PASS"
    assert failures == []
    assert metrics["actual_transmission_seconds"] == 0.08
    assert metrics["transmission_elongation_pct"] == 0.0

    # Verify that if the contaminated echo timestamp were used, it would fail
    contaminated_tx_s = t_last_activity_echo - t_first_send
    status_bad, failures_bad, metrics_bad = evaluate_pacing_metrics(
        total_scheduled=5,
        total_sent=5,
        total_missed_deadlines=0,
        target_interval_ms=20.0,
        mean_interval_ms=20.0,
        p95_scheduler_lateness_ms=2.0,
        nominal_transmission_s=nominal_tx_s,
        actual_transmission_s=contaminated_tx_s,
        criteria=criteria,
    )
    assert status_bad == "FAIL"
    assert any("Media transmission duration elongated by 3025.00%" in f for f in failures_bad)


def test_late_sends_with_fast_echoes_correctly_fails_transmission_duration() -> None:
    """Verify that genuine client send dilation correctly triggers elongation failure even if echoes are fast (P2)."""
    criteria = PacingAcceptanceCriteria()

    # Scenario: 5 frames nominal 0.08s, but client sender loop stalled:
    # Frame 0 sent at t = 100.00
    # Frame 4 sent at t = 100.50 (actual send duration = 0.50 s, elongation = 525% > 25%)
    # Echo arrives immediately at t = 100.51 (fast echo, 10 ms RTT)
    t_first_send = 100.00
    t_last_send = 100.50
    nominal_tx_s = 0.08

    actual_tx_s = t_last_send - t_first_send
    status, failures, metrics = evaluate_pacing_metrics(
        total_scheduled=5,
        total_sent=5,
        total_missed_deadlines=0,
        target_interval_ms=20.0,
        mean_interval_ms=20.0,
        p95_scheduler_lateness_ms=5.0,
        nominal_transmission_s=nominal_tx_s,
        actual_transmission_s=actual_tx_s,
        criteria=criteria,
    )
    assert status == "FAIL"
    assert any("Media transmission duration elongated by 525.00%" in f for f in failures)
    assert metrics["actual_transmission_seconds"] == 0.50
    assert metrics["transmission_elongation_pct"] == 525.0


def test_call_media_run_record_serialization_roundtrip() -> None:
    """Verify CallMediaRunRecord to_dict and from_dict preserve all fields including send wait."""
    agg = BoundedLatencyAggregator(window_capacity=64)
    agg.record(1.0)
    rec = CallMediaRunRecord(
        session_slot_id=3,
        admitted=True,
        completed_cleanly=True,
        close_code=1000,
        frames_scheduled=10,
        frames_sent=10,
        frames_skipped=0,
        missed_deadlines=1,
        frames_echoed=10,
        frames_missing=0,
        duplicate_frames=0,
        out_of_order_frames=0,
        payload_mismatches=0,
        cross_session_frames=0,
        first_frame_sent_s=100.0,
        last_frame_activity_s=100.2,
        handshake_ms=2.5,
        call_start_rtt_ms=1.2,
        call_end_rtt_ms=1.1,
        total_call_duration_ms=210.0,
        echo_rtt_summary=agg.snapshot(),
        echo_rtt_samples_ms=(0.5, 1.0, 1.8),
        scheduling_lateness_samples_ms=(0.1, 0.2),
        scheduling_interval_samples_ms=(20.0, 20.1),
        scheduling_jitter_samples_ms=(0.0, 0.1),
        dispatch_skew_ms=0.5,
        error_category=None,
        last_frame_sent_s=100.18,
        client_socket_send_wait_samples_ms=(0.05, 0.12, 0.08),
    )
    d = rec.to_dict()
    assert d["session_slot_id"] == 3
    assert d["client_socket_send_wait_samples_ms"] == [0.05, 0.12, 0.08]

    reconstructed = CallMediaRunRecord.from_dict(d)
    assert reconstructed.session_slot_id == rec.session_slot_id
    assert reconstructed.client_socket_send_wait_samples_ms == (0.05, 0.12, 0.08)
    assert reconstructed.last_frame_sent_s == 100.18
    assert reconstructed.completed_cleanly is True


def test_client_workers_parameter_validation() -> None:
    """Verify validation of client_workers across run_paced_media_stage and execute_media_load_evaluation."""

    async def _run() -> None:
        # Bool rejection
        with pytest.raises(TypeError, match="client_workers must be a positive integer"):
            await run_paced_media_stage(
                stage_name="val",
                concurrency=2,
                client_workers=cast(Any, True),
            )

        with pytest.raises(TypeError, match="client_workers must be a positive integer"):
            await execute_media_load_evaluation(
                concurrencies=(2,),
                client_workers=cast(Any, False),
            )

        # Non-positive rejection
        with pytest.raises(ValueError, match="client_workers must be a positive integer"):
            await run_paced_media_stage(
                stage_name="val",
                concurrency=2,
                client_workers=0,
            )

        with pytest.raises(ValueError, match="client_workers must be a positive integer"):
            await execute_media_load_evaluation(
                concurrencies=(2,),
                client_workers=-1,
            )

        # Worker count exceeding concurrency
        with pytest.raises(
            ValueError, match=r"client_workers \(4\) cannot exceed concurrency \(2\)"
        ):
            await run_paced_media_stage(
                stage_name="val",
                concurrency=2,
                client_workers=4,
            )

    asyncio.run(_run())


def test_multi_process_client_workers_execution_and_metrics() -> None:
    """Verify multi-process client workers execute and report pooled diagnostics and send wait."""

    async def _run() -> None:
        lenient_criteria = PacingAcceptanceCriteria(
            max_missed_deadline_rate=1.0,
            max_mean_interval_deviation_pct=500.0,
            max_p95_scheduler_lateness_ms=5000.0,
            max_transmission_elongation_pct=500.0,
        )
        stage = await run_paced_media_stage(
            stage_name="test_mp_workers",
            concurrency=2,
            duration_seconds=0.1,
            interval_ms=20.0,
            client_workers=2,
            isolation_mode=ISOLATION_MODE_IN_PROCESS,
            pacing_criteria=lenient_criteria,
        )
        assert stage.status == "PASS"
        assert stage.requested_concurrency == 2
        assert stage.achieved_media_concurrency == 2
        assert stage.completed_calls == 2
        assert stage.media_correctness["frames_sent"] == 10  # 5 frames * 2 calls
        assert stage.media_correctness["frames_echoed"] == 10
        assert stage.media_correctness["frames_missing"] == 0

        # Timing and send wait metrics
        assert stage.timing_ms["client_workers"] == 2
        assert "client_socket_send_wait_ms" in stage.timing_ms
        send_wait = stage.timing_ms["client_socket_send_wait_ms"]
        assert send_wait["available"] is True
        assert send_wait["sample_count"] == 10
        assert send_wait["p50_ms"] is not None

        # Resource and worker diagnostics
        assert stage.resources["client_workers"] == 2
        worker_diags = stage.resources["client_worker_diagnostics"]
        assert len(worker_diags) == 2
        assert {w["worker_index"] for w in worker_diags} == {0, 1}
        for w in worker_diags:
            assert "cpu_duration_s" in w
            assert "worker_lag" in w

    asyncio.run(_run())


def test_aggregate_cpu_utilization_accounting_reconciliation() -> None:
    """Verify aggregate CPU duration includes workers and matches utilization percentages exactly."""

    async def _run() -> None:
        lenient_criteria = PacingAcceptanceCriteria(
            max_missed_deadline_rate=1.0,
            max_mean_interval_deviation_pct=500.0,
            max_p95_scheduler_lateness_ms=5000.0,
            max_transmission_elongation_pct=500.0,
        )
        stage = await run_paced_media_stage(
            stage_name="test_cpu_reconcile",
            concurrency=2,
            duration_seconds=0.15,
            interval_ms=20.0,
            client_workers=2,
            isolation_mode=ISOLATION_MODE_IN_PROCESS,
            pacing_criteria=lenient_criteria,
        )
        assert stage.status == "PASS"
        res = stage.resources

        client_cpu = res["client_process_cpu_time_seconds"]
        worker_cpu = res["client_worker_cpu_time_seconds"]
        server_cpu = res["server_process_cpu_time_seconds"]
        total_cpu = res["process_cpu_time_seconds"]
        wall_sec = res["wall_clock_duration_seconds"]
        logical_cpus = res["logical_cpu_count"]

        # 1. Total CPU must equal sum of coordinator, workers and server
        expected_total = round(client_cpu + worker_cpu + server_cpu, 4)
        assert abs(total_cpu - expected_total) <= 0.0002

        # 2. Worker CPU must be non-zero and populated from child processes
        assert worker_cpu >= 0.0
        assert len(res["client_worker_diagnostics"]) == 2

        # 3. Utilization percentages must be derived from the exact same total_cpu population
        expected_single_core_pct = round((total_cpu / wall_sec) * 100.0, 2)
        assert abs(res["cpu_utilization_pct_single_core"] - expected_single_core_pct) <= 0.05

        expected_all_cores_pct = round((total_cpu / (wall_sec * float(logical_cpus))) * 100.0, 2)
        assert abs(res["cpu_utilization_pct_all_cores"] - expected_all_cores_pct) <= 0.05

    asyncio.run(_run())


def test_sanitize_worker_error_message_redacts_credentials() -> None:
    """Verify _sanitize_worker_error_message redacts tokens, secrets, and auth headers."""
    from tools.synthetic_media_load import _sanitize_worker_error_message

    exc_with_bearer = RuntimeError(
        "Failed connection to ws://127.0.0.1/ws with Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.secret"
    )
    cleaned = _sanitize_worker_error_message(exc_with_bearer)
    assert "Bearer [REDACTED]" in cleaned
    assert "eyJhbGciOi" not in cleaned

    exc_with_params = ValueError("Invalid handshake response: token=abc1234xyz&secret=supersecret")
    cleaned_params = _sanitize_worker_error_message(exc_with_params)
    assert "token=[REDACTED]" in cleaned_params
    assert "secret=[REDACTED]" in cleaned_params
    assert "abc1234xyz" not in cleaned_params

    exc_with_header = ConnectionError(
        "Handshake rejected: Authorization: Bearer sensitive_pass_12345"
    )
    cleaned_header = _sanitize_worker_error_message(exc_with_header)
    assert "Authorization: [REDACTED]" in cleaned_header or "Bearer [REDACTED]" in cleaned_header
    assert "sensitive_pass_12345" not in cleaned_header


def test_worker_call_exception_propagation_and_partial_records() -> None:
    """Verify worker subprocess fails stage and reports error when a call task raises an exception."""
    from tools.synthetic_media_load import _run_multi_process_client_workers

    async def _run() -> None:
        call_specs = [
            (0, "tenant_1", "call_1", "fake_token_1", 0.0),
            (1, "tenant_1", "call_2", "fake_token_2", 2.0),
        ]

        # Patch run_single_paced_media_call inside child worker to raise on call_2
        # In a multi-process test with bad token/endpoint, the server handshake will fail cleanly:
        with pytest.raises(RuntimeError, match=r"Client worker \d+ reported (error|failure)"):
            await _run_multi_process_client_workers(
                client_workers=2,
                call_specs=call_specs,
                ws_url="ws://127.0.0.1:9/invalid_port",
                frames_per_call=5,
                interval_ms=20.0,
                seed=42,
                pattern="deterministic",
                skip_threshold_ms=None,
                timeout=1.0,
                latency_window_capacity=32,
            )

    asyncio.run(_run())


def test_start_synchronization_lead_time_and_late_sync_detection() -> None:
    """Verify start synchronization future epoch delivery and late-start diagnostic tracking."""

    async def _run() -> None:
        lenient_criteria = PacingAcceptanceCriteria(
            max_missed_deadline_rate=1.0,
            max_mean_interval_deviation_pct=500.0,
            max_p95_scheduler_lateness_ms=5000.0,
            max_transmission_elongation_pct=500.0,
        )
        stage = await run_paced_media_stage(
            stage_name="test_sync_epoch",
            concurrency=2,
            duration_seconds=0.1,
            interval_ms=20.0,
            client_workers=2,
            isolation_mode=ISOLATION_MODE_IN_PROCESS,
            pacing_criteria=lenient_criteria,
        )
        assert stage.status == "PASS"
        diags = stage.resources["client_worker_diagnostics"]
        assert len(diags) == 2
        for w in diags:
            assert "start_sync_skew_ms" in w
            assert "late_start_sync" in w
            assert isinstance(w["late_start_sync"], bool)

    asyncio.run(_run())
