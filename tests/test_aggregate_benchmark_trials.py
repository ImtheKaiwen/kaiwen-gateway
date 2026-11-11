"""Unit tests for offline multi-trial benchmark aggregator (tools.aggregate_benchmark_trials)."""

import json
from pathlib import Path
from typing import Any

import pytest
from tools.aggregate_benchmark_trials import (
    CLASSIFICATION_MIXED_RESULTS,
    CLASSIFICATION_REPEATABLY_FAILING,
    CLASSIFICATION_REPEATABLY_PASSING,
    aggregate_configuration_trials,
    extract_stage_snapshot,
    format_markdown_summary_table,
    load_and_validate_report,
    process_report_files,
)


def _make_dummy_report(
    concurrency: int = 25,
    workers: int = 2,
    status: str = "PASS",
    missed_rate: float = 0.005,
    lateness: float = 12.0,
) -> dict[str, Any]:
    return {
        "git_commit": "425a88350803aa42414b81e6cb50f45f62c83f11",
        "configuration": {
            "concurrencies": [concurrency],
            "client_workers": workers,
            "duration_seconds": 5.0,
            "interval_ms": 20.0,
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
                "configured_duration_seconds": 5.0,
                "media_correctness": {
                    "frames_scheduled": concurrency * 250,
                    "frames_sent": concurrency * 250,
                    "frames_echoed": concurrency * 250,
                    "missed_deadlines": int(concurrency * 250 * missed_rate),
                    "missed_deadline_rate": missed_rate,
                    "cleanup_failures": 0,
                },
                "timing_ms": {
                    "client_workers": workers,
                    "scheduler_lateness_ms": {
                        "available": True,
                        "p95_ms": lateness,
                    },
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
                },
                "resources": {
                    "server_peak_rss_kib": 50000.0,
                    "server_cpu_percent_single_core": 65.0,
                },
                "cleanup_verification": {
                    "all_sessions_cleaned_up": True,
                },
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


def test_extract_stage_snapshot() -> None:
    rep = _make_dummy_report(
        concurrency=50, workers=2, status="PASS", missed_rate=0.01, lateness=14.5
    )
    snap = extract_stage_snapshot("dummy.json", rep, stage_index=0)

    assert snap.filename == "dummy.json"
    assert snap.requested_concurrency == 50
    assert snap.client_workers == 2
    assert snap.overall_status == "PASS"
    assert snap.media_integrity_status == "PASS"
    assert snap.realtime_pacing_status == "PASS"
    assert snap.cleanup_success is True
    assert snap.missed_deadline_rate_pct == 1.0
    assert snap.p95_scheduler_lateness_ms == 14.5
    assert snap.p95_client_send_wait_ms == 0.35
    assert snap.p95_client_loop_lag_ms == 8.0
    assert snap.p95_gateway_inbound_queue_wait_ms == 3.5
    assert snap.p95_gateway_outbound_queue_wait_ms == 4.0


def test_aggregate_configuration_trials_repeatably_passing() -> None:
    snaps = [
        extract_stage_snapshot(
            f"r{i}.json", _make_dummy_report(concurrency=25, workers=2, status="PASS")
        )
        for i in range(1, 4)
    ]
    summary = aggregate_configuration_trials(snaps)

    assert summary.concurrency == 25
    assert summary.client_workers == 2
    assert summary.trial_count == 3
    assert summary.pass_count == 3
    assert summary.fail_count == 0
    assert summary.classification == CLASSIFICATION_REPEATABLY_PASSING
    assert summary.media_integrity_success_rate_pct == 100.0
    assert summary.pacing_success_rate_pct == 100.0
    assert summary.cleanup_success_rate_pct == 100.0


def test_aggregate_configuration_trials_mixed_results() -> None:
    s1 = extract_stage_snapshot("r1.json", _make_dummy_report(status="PASS"))
    s2 = extract_stage_snapshot("r2.json", _make_dummy_report(status="FAIL"))
    s3 = extract_stage_snapshot("r3.json", _make_dummy_report(status="FAIL"))

    summary = aggregate_configuration_trials([s1, s2, s3])
    assert summary.trial_count == 3
    assert summary.pass_count == 1
    assert summary.fail_count == 2
    assert summary.classification == CLASSIFICATION_MIXED_RESULTS


def test_aggregate_configuration_trials_repeatably_failing() -> None:
    snaps = [
        extract_stage_snapshot(f"r{i}.json", _make_dummy_report(status="FAIL")) for i in range(1, 4)
    ]
    summary = aggregate_configuration_trials(snaps)
    assert summary.trial_count == 3
    assert summary.pass_count == 0
    assert summary.fail_count == 3
    assert summary.classification == CLASSIFICATION_REPEATABLY_FAILING


def test_format_markdown_summary_table() -> None:
    snaps = [
        extract_stage_snapshot(
            f"r{i}.json", _make_dummy_report(concurrency=10, workers=1, status="PASS")
        )
        for i in range(1, 4)
    ]
    summary = aggregate_configuration_trials(snaps)
    md = format_markdown_summary_table([summary])

    assert "| Concurrency | Workers | Trials |" in md
    assert "| 10 | 1 | 3 | 3/0 | `REPEATABLY_PASSING` |" in md


def test_process_report_files(tmp_path: Path) -> None:
    paths = []
    for i in range(3):
        p = tmp_path / f"trial_{i}.json"
        p.write_text(json.dumps(_make_dummy_report(concurrency=5, workers=1)), encoding="utf-8")
        paths.append(p)

    summaries = process_report_files(paths)
    assert len(summaries) == 1
    assert summaries[0].concurrency == 5
    assert summaries[0].trial_count == 3
    assert summaries[0].classification == CLASSIFICATION_REPEATABLY_PASSING
