"""Unit tests for offline multi-trial benchmark aggregator (tools.aggregate_benchmark_trials)."""

import json
import shlex
from pathlib import Path
from typing import Any

import pytest
from tools.aggregate_benchmark_trials import (
    CLASSIFICATION_INVALID_EXPERIMENT,
    CLASSIFICATION_REPEATABLY_PASSING,
    aggregate_configuration_trials,
    build_qualification_manifest,
    extract_stage_snapshot,
    load_and_validate_report,
    process_report_files,
    scan_for_sensitive_data,
)
from tools.synthetic_media_load import _build_arg_parser


def _make_dummy_report(
    concurrency: int = 25,
    workers: int = 2,
    status: str = "PASS",
    missed_rate: float = 0.005,
    lateness: float = 12.0,
    commit: str = "425a88350803aa42414b81e6cb50f45f62c83f11",
    cleanup_failures: int = 0,
    leaked_tasks: int = 0,
    missing_cleanup: bool = False,
    missing_lateness: bool = False,
    server_rss: float | None = 50000.0,
    server_cpu_time: float | None = 0.65,
    wall_duration: float = 5.0,
    frames_skipped: int = 0,
    frames_missing: int = 0,
) -> dict[str, Any]:
    """Create a schema-faithful synthetic media load report fixture."""
    total_scheduled = concurrency * 250
    total_sent = total_scheduled - frames_skipped
    total_echoed = total_sent - frames_missing
    missed_count = int(total_sent * missed_rate)

    timing_dict: dict[str, Any] = {
        "client_workers": workers,
        "isolation_mode": "process-isolated",
        "scheduling_mode": "aligned",
        "client_synthetic_echo_rtt_ms": {
            "available": True,
            "p95_ms": 25.0,
        },
        "client_socket_send_wait_ms": {
            "available": True,
            "p95_ms": 0.35,
        },
        "event_loop_lag_ms": {
            "available": True,
            "p95_ms": 8.0,
        },
        "gateway_inbound_processing_ms": {
            "available": True,
            "p95_ms": 0.04,
        },
        "gateway_outbound_processing_ms": {
            "available": True,
            "p95_ms": 0.015,
        },
        "gateway_inbound_queue_wait_ms": {
            "available": True,
            "p95_ms": 3.5,
        },
        "gateway_outbound_queue_wait_ms": {
            "available": True,
            "p95_ms": 4.0,
        },
        "pacing_evaluation": {
            "status": status,
            "criteria": {
                "max_missed_deadline_rate": 0.2,
                "max_mean_interval_deviation_pct": 20.0,
                "max_p95_scheduler_lateness_ms": 100.0,
                "max_transmission_elongation_pct": 25.0,
            },
        },
    }

    if not missing_lateness:
        timing_dict["scheduler_lateness_ms"] = {
            "available": True,
            "p95_ms": lateness,
        }
    else:
        timing_dict["scheduler_lateness_ms"] = {
            "available": False,
            "p95_ms": None,
        }

    cleanup_dict: dict[str, Any] | None = None
    if not missing_cleanup:
        cleanup_dict = {
            "final_active_connections": 0,
            "final_active_tenants": 0,
            "final_active_calls": 0,
            "final_active_voice_sessions": 0,
            "leaked_asyncio_tasks": leaked_tasks,
        }

    res_dict: dict[str, Any] = {
        "isolation_mode": "process-isolated",
        "scheduling_mode": "aligned",
        "client_workers": workers,
        "wall_clock_duration_seconds": wall_duration,
    }
    if server_rss is not None:
        res_dict["server_rss_peak_kib"] = server_rss
    if server_cpu_time is not None:
        res_dict["server_process_cpu_time_seconds"] = server_cpu_time

    return {
        "runner_version": "0.1.0",
        "gateway_version": "0.1.0a1",
        "git_commit": commit,
        "python_version": "3.12.13",
        "os_platform": "Windows-11-10.0.26200-SP0",
        "configuration": {
            "concurrencies": [concurrency],
            "client_workers": workers,
            "duration_seconds": wall_duration,
            "interval_ms": 20.0,
            "isolation_mode": "process-isolated",
            "scheduling_mode": "aligned",
            "pacing_acceptance_criteria": {
                "max_missed_deadline_rate": 0.2,
                "max_mean_interval_deviation_pct": 20.0,
                "max_p95_scheduler_lateness_ms": 100.0,
                "max_transmission_elongation_pct": 25.0,
            },
        },
        "overall_status": status,
        "stages": [
            {
                "stage_name": f"S1_concurrency_{concurrency}",
                "status": status,
                "media_integrity_status": "PASS",
                "realtime_pacing_status": status,
                "requested_concurrency": concurrency,
                "achieved_media_concurrency": concurrency,
                "configured_duration_seconds": wall_duration,
                "media_correctness": {
                    "frames_scheduled": total_scheduled,
                    "frames_sent": total_sent,
                    "frames_skipped": frames_skipped,
                    "frames_echoed": total_echoed,
                    "frames_missing": frames_missing,
                    "missed_deadlines": missed_count,
                    "missed_deadline_rate": missed_rate,
                    "cleanup_failures": cleanup_failures,
                    "unexpected_rejections": 0,
                    "unexpected_disconnects": 0,
                    "unexpected_close_codes": 0,
                    "cross_session_frames": 0,
                    "payload_mismatches": 0,
                },
                "timing_ms": timing_dict,
                "resources": res_dict,
                "cleanup_verification": cleanup_dict,
            }
        ],
    }


def test_load_and_validate_report_valid(tmp_path: Path) -> None:
    rep = _make_dummy_report()
    p = tmp_path / "valid_report.json"
    p.write_text(json.dumps(rep), encoding="utf-8")

    data = load_and_validate_report(p)
    assert data["git_commit"] == "425a88350803aa42414b81e6cb50f45f62c83f11"
    assert len(data["stages"]) == 1


def test_load_and_validate_report_missing_fields(tmp_path: Path) -> None:
    p = tmp_path / "invalid_report.json"
    p.write_text(json.dumps({"stages": []}), encoding="utf-8")

    with pytest.raises(ValueError, match="missing mandatory top-level field"):
        load_and_validate_report(p)


def test_extract_stage_snapshot_success() -> None:
    rep = _make_dummy_report(
        concurrency=50,
        workers=2,
        status="PASS",
        missed_rate=0.01,
        lateness=14.5,
        server_rss=45000.0,
        server_cpu_time=1.0,
        wall_duration=5.0,
    )
    snap = extract_stage_snapshot("dummy.json", rep, stage_index=0, file_sha256="abc123")

    assert snap.filename == "dummy.json"
    assert snap.file_sha256 == "abc123"
    assert snap.requested_concurrency == 50
    assert snap.client_workers == 2
    assert snap.overall_status == "PASS"
    assert snap.media_integrity_status == "PASS"
    assert snap.realtime_pacing_status == "PASS"
    assert snap.cleanup_success is True
    assert snap.is_valid_experiment is True
    assert snap.validation_errors == ()
    assert snap.missed_deadline_rate_pct == 1.0
    assert snap.p95_scheduler_lateness_ms == 14.5
    assert snap.p95_client_send_wait_ms == 0.35
    assert snap.p95_client_loop_lag_ms == 8.0
    assert snap.p95_gateway_inbound_queue_wait_ms == 3.5
    assert snap.p95_gateway_outbound_queue_wait_ms == 4.0
    assert snap.server_rss_peak_kib == 45000.0
    assert snap.server_cpu_percent == 20.0  # (1.0 / 5.0) * 100.0


def test_cleanup_verification_failures() -> None:
    # 1. Missing cleanup dict
    rep1 = _make_dummy_report(missing_cleanup=True, status="FAIL")
    snap1 = extract_stage_snapshot("r1.json", rep1, stage_index=0)
    assert snap1.cleanup_success is False
    assert snap1.is_valid_experiment is False
    assert any("cleanup_verification" in err for err in snap1.validation_errors)

    # 2. Leaked task
    rep2 = _make_dummy_report(leaked_tasks=1, status="FAIL")
    snap2 = extract_stage_snapshot("r2.json", rep2, stage_index=0)
    assert snap2.cleanup_success is False
    assert snap2.is_valid_experiment is False
    assert any("leaked_asyncio_tasks=1" in err for err in snap2.validation_errors)

    # 3. cleanup_failures in media_correctness > 0
    rep3 = _make_dummy_report(cleanup_failures=2, status="FAIL")
    snap3 = extract_stage_snapshot("r3.json", rep3, stage_index=0)
    assert snap3.cleanup_success is False
    assert snap3.is_valid_experiment is False
    assert any("cleanup_failures=2" in err for err in snap3.validation_errors)


def test_required_latencies_and_no_zero_substitution() -> None:
    rep = _make_dummy_report(missing_lateness=True, status="FAIL")
    snap = extract_stage_snapshot("missing_late.json", rep, stage_index=0)

    assert snap.p95_scheduler_lateness_ms is None
    assert snap.is_valid_experiment is False
    assert any("scheduler_lateness_ms is missing" in err for err in snap.validation_errors)


def test_frame_accounting_violations() -> None:
    rep = _make_dummy_report(frames_skipped=10)
    rep["stages"][0]["media_correctness"]["frames_scheduled"] = 100
    rep["stages"][0]["media_correctness"]["frames_sent"] = 50
    rep["stages"][0]["media_correctness"]["frames_skipped"] = 0
    rep["stages"][0]["status"] = "FAIL"

    snap = extract_stage_snapshot("frame_err.json", rep, stage_index=0)
    assert snap.is_valid_experiment is False
    assert any("Frame accounting invariant violated" in err for err in snap.validation_errors)


def test_status_contradiction_detection() -> None:
    rep = _make_dummy_report(status="PASS", leaked_tasks=2)
    snap = extract_stage_snapshot("contradiction.json", rep, stage_index=0)
    assert snap.is_valid_experiment is False
    assert any("Status contradiction" in err for err in snap.validation_errors)


def test_aggregate_configuration_trials_repeatably_passing() -> None:
    snaps = [
        extract_stage_snapshot(
            f"r{i}.json",
            _make_dummy_report(concurrency=25, workers=2, status="PASS"),
            file_sha256=f"hash_{i}",
        )
        for i in range(1, 4)
    ]
    summary = aggregate_configuration_trials(snaps)

    assert summary.concurrency == 25
    assert summary.client_workers == 2
    assert summary.trial_count == 3
    assert summary.pass_count == 3
    assert summary.fail_count == 0
    assert summary.invalid_count == 0
    assert summary.classification == CLASSIFICATION_REPEATABLY_PASSING
    assert summary.media_integrity_success_rate_pct == 100.0
    assert summary.pacing_success_rate_pct == 100.0
    assert summary.cleanup_success_rate_pct == 100.0


def test_aggregate_configuration_trials_with_invalid_trial() -> None:
    s1 = extract_stage_snapshot("r1.json", _make_dummy_report(status="PASS"), file_sha256="h1")
    s2 = extract_stage_snapshot("r2.json", _make_dummy_report(status="PASS"), file_sha256="h2")
    s3 = extract_stage_snapshot(
        "r3.json", _make_dummy_report(missing_lateness=True, status="FAIL"), file_sha256="h3"
    )

    summary = aggregate_configuration_trials([s1, s2, s3])
    assert summary.trial_count == 3
    assert summary.invalid_count == 1
    assert summary.classification == CLASSIFICATION_INVALID_EXPERIMENT


def test_aggregate_configuration_trials_mismatched_commit() -> None:
    s1 = extract_stage_snapshot(
        "r1.json", _make_dummy_report(commit="commit_aaa"), file_sha256="h1"
    )
    s2 = extract_stage_snapshot(
        "r2.json", _make_dummy_report(commit="commit_bbb"), file_sha256="h2"
    )

    with pytest.raises(ValueError, match="Mismatched git_commit in group"):
        aggregate_configuration_trials([s1, s2])


def test_process_report_files_duplicate_path(tmp_path: Path) -> None:
    p = tmp_path / "trial_dup.json"
    p.write_text(json.dumps(_make_dummy_report()), encoding="utf-8")

    with pytest.raises(ValueError, match="Duplicate trial file provided"):
        process_report_files([p, p])


def test_process_report_files_duplicate_content(tmp_path: Path) -> None:
    rep = _make_dummy_report()
    p1 = tmp_path / "trial_1.json"
    p2 = tmp_path / "trial_2.json"
    p1.write_text(json.dumps(rep), encoding="utf-8")
    p2.write_text(json.dumps(rep), encoding="utf-8")

    with pytest.raises(ValueError, match="Duplicate trial file content detected"):
        process_report_files([p1, p2])


def test_manifest_building_and_cli_reconstruction(tmp_path: Path) -> None:
    paths = []
    for i in range(3):
        rep = _make_dummy_report(concurrency=10, workers=2, lateness=10.0 + i)
        p = tmp_path / f"trial_manifest_{i}.json"
        p.write_text(json.dumps(rep), encoding="utf-8")
        paths.append(p)

    summaries, snapshots, raw_reports = process_report_files(paths)
    manifest = build_qualification_manifest(snapshots, summaries, raw_reports=raw_reports)

    assert manifest["manifest_version"] == "1.0.0"
    assert manifest["integrity_verification"]["all_sha256_unique"] is True
    assert manifest["integrity_verification"]["all_cleanups_clean"] is True
    assert manifest["integrity_verification"]["all_experiments_valid"] is True
    assert len(manifest["trials"]) == 3
    assert manifest["provenance"]["git_commit"] == "425a88350803aa42414b81e6cb50f45f62c83f11"

    # Regression test: validate reconstructed CLI command against synthetic_media_load parser
    parser = _build_arg_parser()
    for trial_entry in manifest["trials"]:
        cli_cmd = trial_entry["reconstructed_cli"]
        prefix = "uv run python -m tools.synthetic_media_load "
        assert cli_cmd.startswith(prefix)
        args_str = cli_cmd[len(prefix) :]
        parsed_args = parser.parse_args(shlex.split(args_str))

        assert parsed_args.concurrency == trial_entry["concurrency"]
        assert parsed_args.client_workers == trial_entry["client_workers"]
        assert parsed_args.duration == trial_entry["duration_seconds"]
        assert parsed_args.interval_ms == trial_entry["interval_ms"]
        assert parsed_args.no_faults is True
        assert parsed_args.output == f"reports/{trial_entry['filename']}"


def test_scan_for_sensitive_data() -> None:
    # 1. Clean data
    clean_data = {
        "status": "PASS",
        "concurrency": 100,
        "notes": "Regular benchmark output without credentials",
    }
    c1, f1 = scan_for_sensitive_data(clean_data)
    assert c1 > 0
    assert len(f1) == 0

    # 2. Suspicious key
    bad_key_data = {
        "api_key": "mysecret123",
    }
    c2, f2 = scan_for_sensitive_data(bad_key_data)
    assert any("api_key" in f for f in f2)

    # 3. Suspicious token pattern in value
    bad_token_data = {
        "header": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.token123",
    }
    c3, f3 = scan_for_sensitive_data(bad_token_data)
    assert any("bearer" in f.lower() for f in f3)
