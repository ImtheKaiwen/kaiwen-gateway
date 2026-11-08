"""Deterministic unit and loopback smoke tests for M5.1B paced synthetic media load evaluation."""

from __future__ import annotations

import asyncio
import json
import unittest.mock
from pathlib import Path
from typing import Any

import pytest
from tools.synthetic_media_load import (
    FRAME_MAGIC,
    HEADER_STRUCT,
    ISOLATION_MODE_PROCESS_ISOLATED,
    KW_SYNTHETIC_MEDIA_SECRET_KEY_ENV,
    SCHEDULING_MODE_ALIGNED,
    SCHEDULING_MODE_STAGGERED,
    SUPPORTED_PATTERNS,
    EventLoopLagMonitor,
    MonotonicFramePacer,
    PacedMediaLoadReport,
    SessionEchoTracker,
    _build_arg_parser,
    build_deterministic_frame_payload,
    compute_session_phase_offset_ms,
    execute_media_load_evaluation,
    extract_frame_identity,
    main,
    measure_process_rss_kib,
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
        report = await execute_media_load_evaluation(
            concurrencies=(1,),
            duration_seconds=0.08,
            seed=42,
            include_faults=False,
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
