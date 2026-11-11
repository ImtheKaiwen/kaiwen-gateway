"""Offline Multi-Trial Benchmark Report Aggregator and Qualification Tool.

Aggregates, validates, and analyzes multiple independent trial JSON reports produced
by tools.synthetic_media_load for repeatable synthetic performance qualification.
Strictly preserves empirical tail measurements and classifies repeatability.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import statistics
import sys
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

CLASSIFICATION_REPEATABLY_PASSING = "REPEATABLY_PASSING"
CLASSIFICATION_MIXED_RESULTS = "MIXED_RESULTS"
CLASSIFICATION_REPEATABLY_FAILING = "REPEATABLY_FAILING"
CLASSIFICATION_NOT_TESTED = "NOT_TESTED"
CLASSIFICATION_INVALID_EXPERIMENT = "INVALID_EXPERIMENT"

SUPPORTED_CLASSIFICATIONS = (
    CLASSIFICATION_REPEATABLY_PASSING,
    CLASSIFICATION_MIXED_RESULTS,
    CLASSIFICATION_REPEATABLY_FAILING,
    CLASSIFICATION_NOT_TESTED,
    CLASSIFICATION_INVALID_EXPERIMENT,
)

REQUIRED_CLEANUP_COUNTERS = (
    "final_active_connections",
    "final_active_tenants",
    "final_active_calls",
    "final_active_voice_sessions",
    "leaked_asyncio_tasks",
)

SENSITIVE_KEY_SUBSTRINGS = (
    "password",
    "secret",
    "private_key",
    "api_key",
    "auth_token",
    "access_token",
    "refresh_token",
)

SENSITIVE_VALUE_PATTERNS = (
    re.compile(r"(?i)bearer\s+[a-z0-9_\-\.]{10,}"),
    re.compile(r"(?i)basic\s+[a-z0-9+/=]{10,}"),
    re.compile(r"ghp_[A-Za-z0-9_]{20,}"),
    re.compile(r"sk-[A-Za-z0-9]{20,}"),
    re.compile(r"-----BEGIN [A-Z ]+ PRIVATE KEY-----"),
)


@dataclass(frozen=True)
class TrialMetricSnapshot:
    """Metrics extracted from a single stage trial report."""

    filename: str
    file_sha256: str
    git_commit: str
    os_platform: str
    stage_name: str
    requested_concurrency: int
    achieved_media_concurrency: int
    client_workers: int
    duration_s: float
    interval_ms: float
    isolation_mode: str
    scheduling_mode: str
    overall_status: str
    media_integrity_status: str
    realtime_pacing_status: str
    cleanup_success: bool
    frames_scheduled: int
    frames_sent: int
    frames_echoed: int
    missed_deadlines: int
    missed_deadline_rate_pct: float
    p95_scheduler_lateness_ms: float | None
    p95_echo_rtt_ms: float | None
    p95_client_send_wait_ms: float | None
    p95_client_loop_lag_ms: float | None
    p95_gateway_inbound_processing_ms: float | None
    p95_gateway_outbound_processing_ms: float | None
    p95_gateway_inbound_queue_wait_ms: float | None
    p95_gateway_outbound_queue_wait_ms: float | None
    p95_gateway_loop_lag_ms: float | None
    server_rss_peak_kib: float | None
    server_cpu_percent: float | None
    pacing_acceptance_criteria: dict[str, Any]
    is_valid_experiment: bool
    validation_errors: tuple[str, ...]


@dataclass(frozen=True)
class ConfigurationSummary:
    """Statistical summary across multiple independent trials of a specific configuration."""

    concurrency: int
    client_workers: int
    trial_count: int
    pass_count: int
    fail_count: int
    invalid_count: int
    classification: str
    media_integrity_success_rate_pct: float
    pacing_success_rate_pct: float
    cleanup_success_rate_pct: float
    median_missed_deadline_rate_pct: float
    worst_missed_deadline_rate_pct: float
    median_p95_scheduler_lateness_ms: float | None
    worst_p95_scheduler_lateness_ms: float | None
    median_p95_echo_rtt_ms: float | None
    worst_p95_echo_rtt_ms: float | None
    median_p95_client_send_wait_ms: float | None
    worst_p95_client_send_wait_ms: float | None
    median_p95_client_loop_lag_ms: float | None
    worst_p95_client_loop_lag_ms: float | None
    median_p95_gateway_inbound_processing_ms: float | None
    worst_p95_gateway_inbound_processing_ms: float | None
    median_p95_gateway_outbound_processing_ms: float | None
    worst_p95_gateway_outbound_processing_ms: float | None
    median_p95_gateway_inbound_queue_wait_ms: float | None
    worst_p95_gateway_inbound_queue_wait_ms: float | None
    median_p95_gateway_outbound_queue_wait_ms: float | None
    worst_p95_gateway_outbound_queue_wait_ms: float | None
    median_p95_gateway_loop_lag_ms: float | None
    worst_p95_gateway_loop_lag_ms: float | None
    min_server_cpu_percent: float | None
    max_server_cpu_percent: float | None
    min_server_rss_peak_kib: float | None
    max_server_rss_peak_kib: float | None
    trial_files: tuple[str, ...]


def scan_for_sensitive_data(obj: Any, path: str = "") -> tuple[int, list[str]]:
    """Recursively scan an object for sensitive keys and credential patterns.

    Returns:
        (scanned_field_count, list_of_findings)
    """
    scanned_count = 0
    findings: list[str] = []

    if isinstance(obj, Mapping):
        for key, val in obj.items():
            scanned_count += 1
            cur_path = f"{path}.{key}" if path else str(key)
            lower_key = str(key).lower()
            for forbidden_key in SENSITIVE_KEY_SUBSTRINGS:
                if forbidden_key in lower_key:
                    findings.append(f"Suspicious key {cur_path!r} contains {forbidden_key!r}")
            sub_count, sub_findings = scan_for_sensitive_data(val, cur_path)
            scanned_count += sub_count
            findings.extend(sub_findings)
    elif isinstance(obj, (list, tuple)):
        for idx, item in enumerate(obj):
            sub_count, sub_findings = scan_for_sensitive_data(item, f"{path}[{idx}]")
            scanned_count += sub_count
            findings.extend(sub_findings)
    elif isinstance(obj, str):
        scanned_count += 1
        for pattern in SENSITIVE_VALUE_PATTERNS:
            if pattern.search(obj):
                findings.append(f"Sensitive pattern match {pattern.pattern!r} found at {path}")

    return scanned_count, findings


def compute_file_sha256(path: Path | str) -> str:
    """Compute hex SHA-256 digest of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as fp:
        while chunk := fp.read(65536):
            h.update(chunk)
    return h.hexdigest()


def load_and_validate_report(path: Path | str) -> dict[str, Any]:
    """Load and validate schema of a synthetic media load report file."""
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(f"Report file not found: {p}")

    content = p.read_text(encoding="utf-8")
    data = json.loads(content)

    if not isinstance(data, dict):
        raise ValueError(f"Report {p.name} must be a JSON object")

    for field in ("git_commit", "configuration", "stages", "overall_status"):
        if field not in data:
            raise ValueError(f"Report {p.name} missing mandatory top-level field {field!r}")

    if not isinstance(data["stages"], list) or not data["stages"]:
        raise ValueError(f"Report {p.name} must contain at least one stage in 'stages'")

    return data


def extract_stage_snapshot(
    filename: str,
    report: Mapping[str, Any],
    stage_index: int = 0,
    file_sha256: str = "",
) -> TrialMetricSnapshot:
    """Extract structured metric snapshot from a stage inside a validated report."""
    git_commit = str(report.get("git_commit", ""))
    os_platform = str(report.get("os_platform", ""))
    top_config = report.get("configuration", {})
    stages = report.get("stages", [])
    if stage_index >= len(stages):
        raise IndexError(
            f"Stage index {stage_index} out of bounds for report with {len(stages)} stages"
        )

    st = stages[stage_index]
    mc = st.get("media_correctness", {})
    tm = st.get("timing_ms", {})
    res = st.get("resources", {})
    cl = st.get("cleanup_verification")

    validation_errors: list[str] = []

    # --- Strict cleanup verification ---
    cleanup_success = True
    if not isinstance(cl, dict):
        cleanup_success = False
        validation_errors.append("cleanup_verification object is missing or invalid")
    else:
        for counter in REQUIRED_CLEANUP_COUNTERS:
            if counter not in cl:
                cleanup_success = False
                validation_errors.append(f"cleanup_verification missing counter {counter!r}")
            elif not isinstance(cl[counter], int) or cl[counter] != 0:
                cleanup_success = False
                validation_errors.append(
                    f"cleanup_verification non-zero or invalid {counter}={cl[counter]!r}"
                )

    if "cleanup_failures" not in mc:
        cleanup_success = False
        validation_errors.append("media_correctness missing 'cleanup_failures'")
    elif not isinstance(mc["cleanup_failures"], int) or mc["cleanup_failures"] != 0:
        cleanup_success = False
        validation_errors.append(
            f"media_correctness cleanup_failures={mc['cleanup_failures']!r} (expected 0)"
        )

    # --- CPU / RSS extraction ---
    server_rss: float | None = None
    server_cpu: float | None = None
    if isinstance(res, dict):
        if "server_rss_peak_kib" in res and res["server_rss_peak_kib"] is not None:
            val_rss = res["server_rss_peak_kib"]
            if isinstance(val_rss, (int, float)):
                server_rss = float(val_rss)

        srv_time = res.get("server_process_cpu_time_seconds")
        wall_time = res.get("wall_clock_duration_seconds")
        if (
            srv_time is not None
            and wall_time is not None
            and isinstance(srv_time, (int, float))
            and isinstance(wall_time, (int, float))
            and wall_time > 0
        ):
            server_cpu = (float(srv_time) / float(wall_time)) * 100.0

    # --- Required latencies extraction ---
    def _extract_metric_p95(obj: Any) -> float | None:
        if isinstance(obj, dict) and obj.get("available") is True:
            val = obj.get("p95_ms")
            if val is not None and isinstance(val, (int, float)):
                return float(val)
        return None

    scheduler_lateness = _extract_metric_p95(tm.get("scheduler_lateness_ms"))
    echo_rtt = _extract_metric_p95(tm.get("client_synthetic_echo_rtt_ms"))
    client_send_wait = _extract_metric_p95(tm.get("client_socket_send_wait_ms"))

    client_loop_lag = _extract_metric_p95(tm.get("event_loop_lag_ms"))
    if client_loop_lag is None:
        client_loop_lag = _extract_metric_p95(tm.get("client_event_loop_lag_ms"))

    if scheduler_lateness is None:
        validation_errors.append("Required metric scheduler_lateness_ms is missing or unavailable")
    if echo_rtt is None:
        validation_errors.append(
            "Required metric client_synthetic_echo_rtt_ms is missing or unavailable"
        )
    if client_loop_lag is None:
        validation_errors.append("Required metric event_loop_lag_ms is missing or unavailable")

    gw_in = _extract_metric_p95(tm.get("gateway_inbound_processing_ms"))
    gw_out = _extract_metric_p95(tm.get("gateway_outbound_processing_ms"))
    gw_in_queue = _extract_metric_p95(tm.get("gateway_inbound_queue_wait_ms"))
    gw_out_queue = _extract_metric_p95(tm.get("gateway_outbound_queue_wait_ms"))
    gw_loop_lag = _extract_metric_p95(tm.get("gateway_event_loop_lag_ms"))
    if gw_loop_lag is None:
        gw_loop_lag = _extract_metric_p95(tm.get("server_event_loop_lag_ms"))

    # --- Frame accounting and status consistency ---
    frames_scheduled = int(mc.get("frames_scheduled", 0))
    frames_sent = int(mc.get("frames_sent", 0))
    frames_skipped = int(mc.get("frames_skipped", 0))
    frames_echoed = int(mc.get("frames_echoed", 0))
    frames_missing = int(mc.get("frames_missing", 0))
    missed_count = int(mc.get("missed_deadlines", 0))

    if frames_scheduled != (frames_sent + frames_skipped):
        validation_errors.append(
            f"Frame accounting invariant violated: frames_scheduled ({frames_scheduled}) "
            f"!= frames_sent ({frames_sent}) + frames_skipped ({frames_skipped})"
        )

    if frames_sent != (frames_echoed + frames_missing):
        validation_errors.append(
            f"Frame accounting invariant violated: frames_sent ({frames_sent}) "
            f"!= frames_echoed ({frames_echoed}) + frames_missing ({frames_missing})"
        )

    if missed_count > frames_sent:
        validation_errors.append(
            f"Frame accounting invariant violated: missed_deadlines ({missed_count}) > frames_sent ({frames_sent})"
        )

    for unexpected_field in (
        "unexpected_rejections",
        "unexpected_disconnects",
        "unexpected_close_codes",
        "cross_session_frames",
        "payload_mismatches",
    ):
        if mc.get(unexpected_field, 0) != 0:
            validation_errors.append(
                f"Protocol correctness violation: {unexpected_field}={mc.get(unexpected_field)}"
            )

    rate_val = mc.get("missed_deadline_rate")
    if rate_val is not None and isinstance(rate_val, (int, float)):
        missed_rate_pct = float(rate_val) * 100.0
    elif frames_sent > 0:
        missed_rate_pct = (missed_count / frames_sent) * 100.0
    else:
        missed_rate_pct = 0.0

    overall_st = str(st.get("status", ""))
    media_st = str(st.get("media_integrity_status", ""))
    pacing_st = str(st.get("realtime_pacing_status", ""))

    if overall_st == "PASS":
        if media_st != "PASS" or pacing_st != "PASS":
            validation_errors.append(
                f"Status contradiction: overall_status is PASS but media_integrity={media_st!r}, pacing={pacing_st!r}"
            )
        if not cleanup_success:
            validation_errors.append(
                "Status contradiction: overall_status is PASS but cleanup_verification failed"
            )

    pacing_eval = tm.get("pacing_evaluation")
    if isinstance(pacing_eval, dict):
        eval_status = pacing_eval.get("status")
        if eval_status and eval_status != pacing_st:
            validation_errors.append(
                f"Pacing status contradiction: pacing_evaluation.status={eval_status!r} != stage.realtime_pacing_status={pacing_st!r}"
            )

    pacing_criteria = (
        st.get("pacing_acceptance_criteria")
        or top_config.get("pacing_acceptance_criteria")
        or (pacing_eval.get("criteria") if isinstance(pacing_eval, dict) else {})
        or {}
    )

    is_valid = len(validation_errors) == 0

    return TrialMetricSnapshot(
        filename=filename,
        file_sha256=file_sha256,
        git_commit=git_commit,
        os_platform=os_platform,
        stage_name=str(st.get("stage_name", "")),
        requested_concurrency=int(st.get("requested_concurrency", 0)),
        achieved_media_concurrency=int(st.get("achieved_media_concurrency", 0)),
        client_workers=int(tm.get("client_workers", top_config.get("client_workers", 1))),
        duration_s=float(
            st.get("configured_duration_seconds", top_config.get("duration_seconds", 5.0))
        ),
        interval_ms=float(top_config.get("interval_ms", 20.0)),
        isolation_mode=str(
            tm.get("isolation_mode", top_config.get("isolation_mode", "process-isolated"))
        ),
        scheduling_mode=str(
            tm.get("scheduling_mode", top_config.get("scheduling_mode", "aligned"))
        ),
        overall_status=overall_st,
        media_integrity_status=media_st,
        realtime_pacing_status=pacing_st,
        cleanup_success=cleanup_success,
        frames_scheduled=frames_scheduled,
        frames_sent=frames_sent,
        frames_echoed=frames_echoed,
        missed_deadlines=missed_count,
        missed_deadline_rate_pct=round(missed_rate_pct, 4),
        p95_scheduler_lateness_ms=round(scheduler_lateness, 4)
        if scheduler_lateness is not None
        else None,
        p95_echo_rtt_ms=round(echo_rtt, 4) if echo_rtt is not None else None,
        p95_client_send_wait_ms=round(client_send_wait, 4)
        if client_send_wait is not None
        else None,
        p95_client_loop_lag_ms=round(client_loop_lag, 4) if client_loop_lag is not None else None,
        p95_gateway_inbound_processing_ms=round(gw_in, 4) if gw_in is not None else None,
        p95_gateway_outbound_processing_ms=round(gw_out, 4) if gw_out is not None else None,
        p95_gateway_inbound_queue_wait_ms=round(gw_in_queue, 4)
        if gw_in_queue is not None
        else None,
        p95_gateway_outbound_queue_wait_ms=round(gw_out_queue, 4)
        if gw_out_queue is not None
        else None,
        p95_gateway_loop_lag_ms=round(gw_loop_lag, 4) if gw_loop_lag is not None else None,
        server_rss_peak_kib=round(server_rss, 2) if server_rss is not None else None,
        server_cpu_percent=round(server_cpu, 2) if server_cpu is not None else None,
        pacing_acceptance_criteria=pacing_criteria,
        is_valid_experiment=is_valid,
        validation_errors=tuple(validation_errors),
    )


def aggregate_configuration_trials(
    snapshots: Sequence[TrialMetricSnapshot],
) -> ConfigurationSummary:
    """Aggregate multiple trials for a single (concurrency, client_workers) configuration."""
    if not snapshots:
        raise ValueError("Cannot aggregate empty trial sequence")

    concurrency = snapshots[0].requested_concurrency
    workers = snapshots[0].client_workers
    baseline_commit = snapshots[0].git_commit
    baseline_duration = snapshots[0].duration_s
    baseline_interval = snapshots[0].interval_ms
    baseline_isolation = snapshots[0].isolation_mode
    baseline_scheduling = snapshots[0].scheduling_mode
    baseline_criteria = snapshots[0].pacing_acceptance_criteria

    for s in snapshots:
        if s.requested_concurrency != concurrency or s.client_workers != workers:
            raise ValueError(
                f"Heterogeneous snapshots in group: expected concurrency={concurrency}, "
                f"workers={workers}; found concurrency={s.requested_concurrency}, workers={s.client_workers}"
            )
        if s.git_commit != baseline_commit:
            raise ValueError(
                f"Mismatched git_commit in group: expected {baseline_commit!r}, found {s.git_commit!r} in {s.filename}"
            )
        if s.duration_s != baseline_duration or s.interval_ms != baseline_interval:
            raise ValueError(
                f"Mismatched benchmark timing parameters in group: duration={s.duration_s} (exp {baseline_duration}), "
                f"interval={s.interval_ms} (exp {baseline_interval}) in {s.filename}"
            )
        if s.isolation_mode != baseline_isolation or s.scheduling_mode != baseline_scheduling:
            raise ValueError(
                f"Mismatched execution modes in group: isolation={s.isolation_mode}, scheduling={s.scheduling_mode} in {s.filename}"
            )
        if s.pacing_acceptance_criteria != baseline_criteria:
            raise ValueError(
                f"Mismatched pacing_acceptance_criteria in group for file {s.filename}"
            )

    trial_count = len(snapshots)
    invalid_count = sum(1 for s in snapshots if not s.is_valid_experiment)
    pass_count = sum(1 for s in snapshots if s.overall_status == "PASS" and s.is_valid_experiment)
    fail_count = trial_count - pass_count

    media_pass_count = sum(1 for s in snapshots if s.media_integrity_status == "PASS")
    pacing_pass_count = sum(1 for s in snapshots if s.realtime_pacing_status == "PASS")
    cleanup_pass_count = sum(1 for s in snapshots if s.cleanup_success)

    if invalid_count > 0:
        classification = CLASSIFICATION_INVALID_EXPERIMENT
    elif trial_count >= 3 and pass_count == trial_count and cleanup_pass_count == trial_count:
        classification = CLASSIFICATION_REPEATABLY_PASSING
    elif pass_count == 0:
        classification = CLASSIFICATION_REPEATABLY_FAILING
    elif 0 < pass_count < trial_count:
        classification = CLASSIFICATION_MIXED_RESULTS
    elif trial_count < 3 and pass_count == trial_count:
        classification = CLASSIFICATION_MIXED_RESULTS
    else:
        classification = CLASSIFICATION_INVALID_EXPERIMENT

    def _median_and_worst(
        vals: Sequence[float | None], find_max: bool = True
    ) -> tuple[float | None, float | None]:
        clean = [v for v in vals if v is not None]
        if not clean:
            return None, None
        med = float(statistics.median(clean))
        worst = max(clean) if find_max else min(clean)
        return round(med, 4), round(worst, 4)

    med_miss, worst_miss = _median_and_worst([s.missed_deadline_rate_pct for s in snapshots])
    med_late, worst_late = _median_and_worst([s.p95_scheduler_lateness_ms for s in snapshots])
    med_rtt, worst_rtt = _median_and_worst([s.p95_echo_rtt_ms for s in snapshots])
    med_send_wait, worst_send_wait = _median_and_worst(
        [s.p95_client_send_wait_ms for s in snapshots]
    )
    med_loop_lag, worst_loop_lag = _median_and_worst([s.p95_client_loop_lag_ms for s in snapshots])

    med_gw_in, worst_gw_in = _median_and_worst(
        [s.p95_gateway_inbound_processing_ms for s in snapshots]
    )
    med_gw_out, worst_gw_out = _median_and_worst(
        [s.p95_gateway_outbound_processing_ms for s in snapshots]
    )
    med_gw_in_q, worst_gw_in_q = _median_and_worst(
        [s.p95_gateway_inbound_queue_wait_ms for s in snapshots]
    )
    med_gw_out_q, worst_gw_out_q = _median_and_worst(
        [s.p95_gateway_outbound_queue_wait_ms for s in snapshots]
    )
    med_gw_lag, worst_gw_lag = _median_and_worst([s.p95_gateway_loop_lag_ms for s in snapshots])

    cpu_vals = [s.server_cpu_percent for s in snapshots if s.server_cpu_percent is not None]
    rss_vals = [s.server_rss_peak_kib for s in snapshots if s.server_rss_peak_kib is not None]

    return ConfigurationSummary(
        concurrency=concurrency,
        client_workers=workers,
        trial_count=trial_count,
        pass_count=pass_count,
        fail_count=fail_count,
        invalid_count=invalid_count,
        classification=classification,
        media_integrity_success_rate_pct=round((media_pass_count / trial_count) * 100.0, 2),
        pacing_success_rate_pct=round((pacing_pass_count / trial_count) * 100.0, 2),
        cleanup_success_rate_pct=round((cleanup_pass_count / trial_count) * 100.0, 2),
        median_missed_deadline_rate_pct=med_miss if med_miss is not None else 0.0,
        worst_missed_deadline_rate_pct=worst_miss if worst_miss is not None else 0.0,
        median_p95_scheduler_lateness_ms=med_late,
        worst_p95_scheduler_lateness_ms=worst_late,
        median_p95_echo_rtt_ms=med_rtt,
        worst_p95_echo_rtt_ms=worst_rtt,
        median_p95_client_send_wait_ms=med_send_wait,
        worst_p95_client_send_wait_ms=worst_send_wait,
        median_p95_client_loop_lag_ms=med_loop_lag,
        worst_p95_client_loop_lag_ms=worst_loop_lag,
        median_p95_gateway_inbound_processing_ms=med_gw_in,
        worst_p95_gateway_inbound_processing_ms=worst_gw_in,
        median_p95_gateway_outbound_processing_ms=med_gw_out,
        worst_p95_gateway_outbound_processing_ms=worst_gw_out,
        median_p95_gateway_inbound_queue_wait_ms=med_gw_in_q,
        worst_p95_gateway_inbound_queue_wait_ms=worst_gw_in_q,
        median_p95_gateway_outbound_queue_wait_ms=med_gw_out_q,
        worst_p95_gateway_outbound_queue_wait_ms=worst_gw_out_q,
        median_p95_gateway_loop_lag_ms=med_gw_lag,
        worst_p95_gateway_loop_lag_ms=worst_gw_lag,
        min_server_cpu_percent=round(min(cpu_vals), 2) if cpu_vals else None,
        max_server_cpu_percent=round(max(cpu_vals), 2) if cpu_vals else None,
        min_server_rss_peak_kib=round(min(rss_vals), 2) if rss_vals else None,
        max_server_rss_peak_kib=round(max(rss_vals), 2) if rss_vals else None,
        trial_files=tuple(s.filename for s in snapshots),
    )


def format_markdown_summary_table(summaries: Sequence[ConfigurationSummary]) -> str:
    """Format configuration summaries into a clear, comprehensive Markdown table."""
    headers = [
        "Concurrency",
        "Workers",
        "Trials",
        "Pass/Fail",
        "Classification",
        "Missed DL % (Med/Worst)",
        "Sched Late p95 ms (Med/Worst)",
        "Echo RTT p95 ms (Med/Worst)",
        "GW In Q Wait p95 ms",
        "GW Out Q Wait p95 ms",
        "GW In Proc p95 ms",
        "Send Wait p95 ms",
        "Client Lag p95 ms",
    ]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]

    for s in sorted(summaries, key=lambda x: (x.concurrency, x.client_workers)):
        in_q = (
            f"{s.median_p95_gateway_inbound_queue_wait_ms:.2f} / {s.worst_p95_gateway_inbound_queue_wait_ms:.2f}"
            if s.median_p95_gateway_inbound_queue_wait_ms is not None
            else "N/A"
        )
        out_q = (
            f"{s.median_p95_gateway_outbound_queue_wait_ms:.2f} / {s.worst_p95_gateway_outbound_queue_wait_ms:.2f}"
            if s.median_p95_gateway_outbound_queue_wait_ms is not None
            else "N/A"
        )
        in_proc = (
            f"{s.median_p95_gateway_inbound_processing_ms:.3f}"
            if s.median_p95_gateway_inbound_processing_ms is not None
            else "N/A"
        )
        send_wait = (
            f"{s.median_p95_client_send_wait_ms:.2f}"
            if s.median_p95_client_send_wait_ms is not None
            else "N/A"
        )
        sched_late = (
            f"{s.median_p95_scheduler_lateness_ms:.1f} / {s.worst_p95_scheduler_lateness_ms:.1f}"
            if s.median_p95_scheduler_lateness_ms is not None
            else "N/A"
        )
        echo_rtt = (
            f"{s.median_p95_echo_rtt_ms:.1f} / {s.worst_p95_echo_rtt_ms:.1f}"
            if s.median_p95_echo_rtt_ms is not None
            else "N/A"
        )
        client_lag = (
            f"{s.median_p95_client_loop_lag_ms:.1f}"
            if s.median_p95_client_loop_lag_ms is not None
            else "N/A"
        )
        row = [
            str(s.concurrency),
            str(s.client_workers),
            str(s.trial_count),
            f"{s.pass_count}/{s.fail_count}",
            f"`{s.classification}`",
            f"{s.median_missed_deadline_rate_pct:.2f}% / {s.worst_missed_deadline_rate_pct:.2f}%",
            sched_late,
            echo_rtt,
            in_q,
            out_q,
            in_proc,
            send_wait,
            client_lag,
        ]
        lines.append("| " + " | ".join(row) + " |")

    return "\n".join(lines)


def process_report_files(
    paths: Sequence[Path | str],
) -> tuple[list[ConfigurationSummary], list[TrialMetricSnapshot], dict[str, dict[str, Any]]]:
    """Load, validate, and aggregate a collection of report file paths.

    Validates that report file paths and contents (SHA-256) are non-duplicate.
    """
    seen_paths: set[Path] = set()
    seen_hashes: dict[str, str] = {}
    snapshots_by_config: dict[tuple[int, int], list[TrialMetricSnapshot]] = {}
    all_snapshots: list[TrialMetricSnapshot] = []
    raw_reports: dict[str, dict[str, Any]] = {}

    for p in paths:
        path_obj = Path(p).resolve()
        if path_obj in seen_paths:
            raise ValueError(f"Duplicate trial file provided: {path_obj.name}")
        seen_paths.add(path_obj)

        sha = compute_file_sha256(path_obj)
        if sha in seen_hashes:
            raise ValueError(
                f"Duplicate trial file content detected: {path_obj.name} has identical SHA-256 to {seen_hashes[sha]}"
            )
        seen_hashes[sha] = path_obj.name

        report = load_and_validate_report(path_obj)
        raw_reports[path_obj.name] = report
        for stage_idx in range(len(report.get("stages", []))):
            snap = extract_stage_snapshot(
                path_obj.name, report, stage_index=stage_idx, file_sha256=sha
            )
            key = (snap.requested_concurrency, snap.client_workers)
            snapshots_by_config.setdefault(key, []).append(snap)
            all_snapshots.append(snap)

    summaries: list[ConfigurationSummary] = []
    for key in sorted(snapshots_by_config.keys()):
        summary = aggregate_configuration_trials(snapshots_by_config[key])
        summaries.append(summary)

    return summaries, all_snapshots, raw_reports


def build_qualification_manifest(
    snapshots: Sequence[TrialMetricSnapshot],
    summaries: Sequence[ConfigurationSummary],
    raw_reports: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a reviewable qualification manifest mapping trial files to metadata and metrics."""
    if not snapshots:
        raise ValueError("Cannot build manifest from empty snapshots")

    common_commit = snapshots[0].git_commit
    common_platform = snapshots[0].os_platform

    trials_manifest: list[dict[str, Any]] = []
    for s in snapshots:
        max_conn = max(25, s.requested_concurrency)
        reconstructed_cli = (
            f"uv run python -m tools.synthetic_media_load "
            f"--concurrency {s.requested_concurrency} "
            f"--client-workers {s.client_workers} "
            f"--duration {s.duration_s} "
            f"--interval-ms {s.interval_ms} "
            f"--max-concurrent-connections {max_conn} "
            f"--max-connections-per-tenant 10 "
            f"--no-faults "
            f"--output reports/{s.filename}"
        )
        trials_manifest.append(
            {
                "filename": s.filename,
                "sha256": s.file_sha256,
                "git_commit": s.git_commit,
                "os_platform": s.os_platform,
                "concurrency": s.requested_concurrency,
                "client_workers": s.client_workers,
                "duration_seconds": s.duration_s,
                "interval_ms": s.interval_ms,
                "overall_status": s.overall_status,
                "media_integrity_status": s.media_integrity_status,
                "realtime_pacing_status": s.realtime_pacing_status,
                "cleanup_success": s.cleanup_success,
                "is_valid_experiment": s.is_valid_experiment,
                "validation_errors": list(s.validation_errors),
                "reconstructed_cli": reconstructed_cli,
                "metrics": {
                    "frames_scheduled": s.frames_scheduled,
                    "frames_sent": s.frames_sent,
                    "frames_echoed": s.frames_echoed,
                    "missed_deadlines": s.missed_deadlines,
                    "missed_deadline_rate_pct": s.missed_deadline_rate_pct,
                    "p95_scheduler_lateness_ms": s.p95_scheduler_lateness_ms,
                    "p95_echo_rtt_ms": s.p95_echo_rtt_ms,
                    "p95_client_send_wait_ms": s.p95_client_send_wait_ms,
                    "p95_client_loop_lag_ms": s.p95_client_loop_lag_ms,
                    "p95_gateway_inbound_processing_ms": s.p95_gateway_inbound_processing_ms,
                    "p95_gateway_outbound_processing_ms": s.p95_gateway_outbound_processing_ms,
                    "p95_gateway_inbound_queue_wait_ms": s.p95_gateway_inbound_queue_wait_ms,
                    "p95_gateway_outbound_queue_wait_ms": s.p95_gateway_outbound_queue_wait_ms,
                    "p95_gateway_loop_lag_ms": s.p95_gateway_loop_lag_ms,
                    "server_rss_peak_kib": s.server_rss_peak_kib,
                    "server_cpu_percent": s.server_cpu_percent,
                },
            }
        )

    all_unique_hashes = len({s.file_sha256 for s in snapshots}) == len(snapshots)
    all_cleanups_clean = all(s.cleanup_success for s in snapshots)
    all_experiments_valid = all(s.is_valid_experiment for s in snapshots)

    total_scanned_fields = 0
    all_sensitive_findings: list[str] = []
    if raw_reports:
        for fname, r_dict in raw_reports.items():
            scanned, findings = scan_for_sensitive_data(r_dict)
            total_scanned_fields += scanned
            for f in findings:
                all_sensitive_findings.append(f"[{fname}] {f}")

    sensitive_validation = {
        "status": "VALIDATED_CLEAN" if not all_sensitive_findings else "FINDINGS_DETECTED",
        "scanned_fields_count": total_scanned_fields,
        "findings_count": len(all_sensitive_findings),
        "findings": all_sensitive_findings,
        "scan_limitations": (
            "Rule-based heuristic pattern matching across all JSON keys and string values "
            "for known credential, secret, API token, and authorization header formats. "
            "Does not perform semantic NLP or offline entity extraction."
        ),
    }

    return {
        "manifest_version": "1.0.0",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "provenance": {
            "git_commit": common_commit,
            "os_platform": common_platform,
            "total_trials": len(snapshots),
            "configuration_groups": len(summaries),
        },
        "integrity_verification": {
            "all_sha256_unique": all_unique_hashes,
            "all_cleanups_clean": all_cleanups_clean,
            "all_experiments_valid": all_experiments_valid,
        },
        "sensitive_data_validation": sensitive_validation,
        "trials": trials_manifest,
    }


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint for offline benchmark trial aggregation."""
    parser = argparse.ArgumentParser(
        description="Aggregate and analyze multiple independent synthetic media benchmark trials.",
    )
    parser.add_argument(
        "files",
        nargs="+",
        help="One or more trial JSON report files to aggregate.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print summary results in JSON format.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Optional path to write aggregated summary JSON.",
    )
    parser.add_argument(
        "--manifest-output",
        type=str,
        default=None,
        help="Optional path to write full qualification manifest JSON.",
    )

    args = parser.parse_args(argv)

    resolved_paths: list[Path] = []
    for f in args.files:
        p = Path(f)
        if any(char in f for char in "*?[]"):
            parent = p.parent if str(p.parent) != "." else Path.cwd()
            pattern = p.name
            matched = sorted(parent.glob(pattern))
            if not matched:
                sys.stderr.write(f"Warning: pattern {f!r} matched zero files\n")
            resolved_paths.extend(matched)
        elif p.is_file():
            resolved_paths.append(p)
        else:
            sys.stderr.write(f"Error: file not found {f!r}\n")
            return 1

    if not resolved_paths:
        sys.stderr.write("Error: no valid report files provided.\n")
        return 1

    try:
        summaries, snapshots, raw_reports = process_report_files(resolved_paths)
    except Exception as exc:
        sys.stderr.write(f"Aggregation failed: {exc}\n")
        return 1

    highest_repeatably_passing = max(
        [s.concurrency for s in summaries if s.classification == CLASSIFICATION_REPEATABLY_PASSING],
        default=0,
    )

    payload = {
        "configurations": [asdict(s) for s in summaries],
        "highest_repeatably_passing_concurrency": highest_repeatably_passing,
    }

    if args.output:
        out_p = Path(args.output)
        out_p.parent.mkdir(parents=True, exist_ok=True)
        out_p.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    if args.manifest_output:
        manifest_payload = build_qualification_manifest(
            snapshots, summaries, raw_reports=raw_reports
        )
        manifest_p = Path(args.manifest_output)
        manifest_p.parent.mkdir(parents=True, exist_ok=True)
        manifest_p.write_text(json.dumps(manifest_payload, indent=2) + "\n", encoding="utf-8")

    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print("\n=== KAIWEN GATEWAY MULTI-TRIAL BENCHMARK QUALIFICATION SUMMARY ===")
        print(format_markdown_summary_table(summaries))
        print(f"\nHighest Repeatably Passing Concurrency: {highest_repeatably_passing}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
