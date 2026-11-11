"""Offline Multi-Trial Benchmark Report Aggregator and Qualification Tool.

Aggregates, validates, and analyzes multiple independent trial JSON reports produced
by tools.synthetic_media_load for repeatable synthetic performance qualification.
Strictly preserves empirical tail measurements and classifies repeatability.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
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


@dataclass(frozen=True)
class TrialMetricSnapshot:
    """Metrics extracted from a single stage trial report."""

    filename: str
    git_commit: str
    stage_name: str
    requested_concurrency: int
    achieved_media_concurrency: int
    client_workers: int
    duration_s: float
    overall_status: str
    media_integrity_status: str
    realtime_pacing_status: str
    cleanup_success: bool
    frames_scheduled: int
    frames_sent: int
    frames_echoed: int
    missed_deadlines: int
    missed_deadline_rate_pct: float
    p95_scheduler_lateness_ms: float
    p95_echo_rtt_ms: float
    p95_client_send_wait_ms: float | None
    p95_client_loop_lag_ms: float
    p95_gateway_inbound_processing_ms: float | None
    p95_gateway_outbound_processing_ms: float | None
    p95_gateway_inbound_queue_wait_ms: float | None
    p95_gateway_outbound_queue_wait_ms: float | None
    p95_gateway_loop_lag_ms: float | None
    server_rss_peak_kib: float | None
    server_cpu_percent: float | None


@dataclass(frozen=True)
class ConfigurationSummary:
    """Statistical summary across multiple independent trials of a specific configuration."""

    concurrency: int
    client_workers: int
    trial_count: int
    pass_count: int
    fail_count: int
    classification: str
    media_integrity_success_rate_pct: float
    pacing_success_rate_pct: float
    cleanup_success_rate_pct: float
    median_missed_deadline_rate_pct: float
    worst_missed_deadline_rate_pct: float
    median_p95_scheduler_lateness_ms: float
    worst_p95_scheduler_lateness_ms: float
    median_p95_client_send_wait_ms: float | None
    worst_p95_client_send_wait_ms: float | None
    median_p95_client_loop_lag_ms: float
    worst_p95_client_loop_lag_ms: float
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
    filename: str, report: Mapping[str, Any], stage_index: int = 0
) -> TrialMetricSnapshot:
    """Extract structured metric snapshot from a stage inside a validated report."""
    git_commit = str(report.get("git_commit", ""))
    stages = report.get("stages", [])
    if stage_index >= len(stages):
        raise IndexError(
            f"Stage index {stage_index} out of bounds for report with {len(stages)} stages"
        )

    st = stages[stage_index]
    mc = st.get("media_correctness", {})
    tm = st.get("timing_ms", {})
    res = st.get("resources", {})
    cl = st.get("cleanup_verification", {})

    cleanup_success = bool(cl.get("all_sessions_cleaned_up", True)) and not bool(
        mc.get("cleanup_failures", 0)
    )

    def _extract_metric_p95(obj: Any) -> float | None:
        if isinstance(obj, dict) and obj.get("available") is not False:
            val = obj.get("p95_ms")
            if val is not None and isinstance(val, (int, float)):
                return float(val)
        return None

    frames_sent = int(mc.get("frames_sent", 0))
    missed_count = int(mc.get("missed_deadlines", 0))
    rate_val = mc.get("missed_deadline_rate")
    if rate_val is not None and isinstance(rate_val, (int, float)):
        missed_rate_pct = float(rate_val) * 100.0
    elif frames_sent > 0:
        missed_rate_pct = (missed_count / frames_sent) * 100.0
    else:
        missed_rate_pct = 0.0

    scheduler_lateness = _extract_metric_p95(tm.get("scheduler_lateness_ms")) or 0.0
    echo_rtt = _extract_metric_p95(tm.get("client_synthetic_echo_rtt_ms")) or 0.0
    client_send_wait = _extract_metric_p95(tm.get("client_socket_send_wait_ms"))
    client_loop_lag = _extract_metric_p95(tm.get("event_loop_lag_ms")) or 0.0

    gw_in = _extract_metric_p95(tm.get("gateway_inbound_processing_ms"))
    gw_out = _extract_metric_p95(tm.get("gateway_outbound_processing_ms"))
    gw_in_queue = _extract_metric_p95(tm.get("gateway_inbound_queue_wait_ms"))
    gw_out_queue = _extract_metric_p95(tm.get("gateway_outbound_queue_wait_ms"))
    gw_loop_lag = _extract_metric_p95(tm.get("gateway_event_loop_lag_ms"))

    server_rss = None
    server_cpu = None
    if isinstance(res, dict):
        if "server_peak_rss_kib" in res and isinstance(res["server_peak_rss_kib"], (int, float)):
            server_rss = float(res["server_peak_rss_kib"])
        if "server_cpu_percent_single_core" in res and isinstance(
            res["server_cpu_percent_single_core"], (int, float)
        ):
            server_cpu = float(res["server_cpu_percent_single_core"])

    return TrialMetricSnapshot(
        filename=filename,
        git_commit=git_commit,
        stage_name=str(st.get("stage_name", "")),
        requested_concurrency=int(st.get("requested_concurrency", 0)),
        achieved_media_concurrency=int(st.get("achieved_media_concurrency", 0)),
        client_workers=int(tm.get("client_workers", 1)),
        duration_s=float(st.get("configured_duration_seconds", 5.0)),
        overall_status=str(st.get("status", "")),
        media_integrity_status=str(st.get("media_integrity_status", "")),
        realtime_pacing_status=str(st.get("realtime_pacing_status", "")),
        cleanup_success=cleanup_success,
        frames_scheduled=int(mc.get("frames_scheduled", 0)),
        frames_sent=frames_sent,
        frames_echoed=int(mc.get("frames_echoed", 0)),
        missed_deadlines=missed_count,
        missed_deadline_rate_pct=round(missed_rate_pct, 4),
        p95_scheduler_lateness_ms=round(scheduler_lateness, 4),
        p95_echo_rtt_ms=round(echo_rtt, 4),
        p95_client_send_wait_ms=round(client_send_wait, 4)
        if client_send_wait is not None
        else None,
        p95_client_loop_lag_ms=round(client_loop_lag, 4),
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
    )


def aggregate_configuration_trials(
    snapshots: Sequence[TrialMetricSnapshot],
) -> ConfigurationSummary:
    """Aggregate multiple trials for a single (concurrency, client_workers) configuration."""
    if not snapshots:
        raise ValueError("Cannot aggregate empty trial sequence")

    concurrency = snapshots[0].requested_concurrency
    workers = snapshots[0].client_workers
    for s in snapshots:
        if s.requested_concurrency != concurrency or s.client_workers != workers:
            raise ValueError(
                f"Heterogeneous snapshots in group: expected concurrency={concurrency}, "
                f"workers={workers}; found concurrency={s.requested_concurrency}, workers={s.client_workers}"
            )

    trial_count = len(snapshots)
    pass_count = sum(1 for s in snapshots if s.overall_status == "PASS")
    fail_count = trial_count - pass_count

    media_pass_count = sum(1 for s in snapshots if s.media_integrity_status == "PASS")
    pacing_pass_count = sum(1 for s in snapshots if s.realtime_pacing_status == "PASS")
    cleanup_pass_count = sum(1 for s in snapshots if s.cleanup_success)

    # Classification rules:
    # REPEATABLY_PASSING requires >= 3 valid independent trials, ALL passing media integrity and pacing, with successful cleanup
    if trial_count >= 3 and pass_count == trial_count and cleanup_pass_count == trial_count:
        classification = CLASSIFICATION_REPEATABLY_PASSING
    elif pass_count == 0:
        classification = CLASSIFICATION_REPEATABLY_FAILING
    elif 0 < pass_count < trial_count:
        classification = CLASSIFICATION_MIXED_RESULTS
    elif trial_count < 3 and pass_count == trial_count:
        # Less than 3 trials cannot be certified as REPEATABLY_PASSING
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
        classification=classification,
        media_integrity_success_rate_pct=round((media_pass_count / trial_count) * 100.0, 2),
        pacing_success_rate_pct=round((pacing_pass_count / trial_count) * 100.0, 2),
        cleanup_success_rate_pct=round((cleanup_pass_count / trial_count) * 100.0, 2),
        median_missed_deadline_rate_pct=med_miss if med_miss is not None else 0.0,
        worst_missed_deadline_rate_pct=worst_miss if worst_miss is not None else 0.0,
        median_p95_scheduler_lateness_ms=med_late if med_late is not None else 0.0,
        worst_p95_scheduler_lateness_ms=worst_late if worst_late is not None else 0.0,
        median_p95_client_send_wait_ms=med_send_wait,
        worst_p95_client_send_wait_ms=worst_send_wait,
        median_p95_client_loop_lag_ms=med_loop_lag if med_loop_lag is not None else 0.0,
        worst_p95_client_loop_lag_ms=worst_loop_lag if worst_loop_lag is not None else 0.0,
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
        row = [
            str(s.concurrency),
            str(s.client_workers),
            str(s.trial_count),
            f"{s.pass_count}/{s.fail_count}",
            f"`{s.classification}`",
            f"{s.median_missed_deadline_rate_pct:.2f}% / {s.worst_missed_deadline_rate_pct:.2f}%",
            f"{s.median_p95_scheduler_lateness_ms:.1f} / {s.worst_p95_scheduler_lateness_ms:.1f}",
            in_q,
            out_q,
            in_proc,
            send_wait,
            f"{s.median_p95_client_loop_lag_ms:.1f}",
        ]
        lines.append("| " + " | ".join(row) + " |")

    return "\n".join(lines)


def process_report_files(paths: Sequence[Path | str]) -> list[ConfigurationSummary]:
    """Load, validate, and aggregate a collection of report file paths."""
    snapshots_by_config: dict[tuple[int, int], list[TrialMetricSnapshot]] = {}

    for p in paths:
        path_obj = Path(p)
        report = load_and_validate_report(path_obj)
        for stage_idx in range(len(report.get("stages", []))):
            snap = extract_stage_snapshot(path_obj.name, report, stage_index=stage_idx)
            key = (snap.requested_concurrency, snap.client_workers)
            snapshots_by_config.setdefault(key, []).append(snap)

    summaries: list[ConfigurationSummary] = []
    for key in sorted(snapshots_by_config.keys()):
        summary = aggregate_configuration_trials(snapshots_by_config[key])
        summaries.append(summary)

    return summaries


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
        help="Optional path to write aggregated output JSON.",
    )

    args = parser.parse_args(argv)

    resolved_paths: list[Path] = []
    for f in args.files:
        p = Path(f)
        if any(char in f for char in "*?[]"):
            parent = p.parent if str(p.parent) != "." else Path.cwd()
            pattern = p.name
            matched = list(parent.glob(pattern))
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
        summaries = process_report_files(resolved_paths)
    except Exception as exc:
        sys.stderr.write(f"Aggregation failed: {exc}\n")
        return 1

    payload = {
        "configurations": [asdict(s) for s in summaries],
        "highest_repeatably_passing_concurrency": max(
            [
                s.concurrency
                for s in summaries
                if s.classification == CLASSIFICATION_REPEATABLY_PASSING
            ],
            default=0,
        ),
    }

    if args.output:
        out_p = Path(args.output)
        out_p.parent.mkdir(parents=True, exist_ok=True)
        out_p.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print("\n=== KAIWEN GATEWAY MULTI-TRIAL BENCHMARK QUALIFICATION SUMMARY ===")
        print(format_markdown_summary_table(summaries))
        print(
            f"\nHighest Repeatably Passing Concurrency: {payload['highest_repeatably_passing_concurrency']}"
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
