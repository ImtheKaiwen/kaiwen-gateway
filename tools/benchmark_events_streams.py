"""Reproducible E4 Redis Streams paced load & recovery qualification harness."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import sys
import time
import tracemalloc
import uuid
from typing import Any

from kaiwen_gateway.modules.events import EVENTS_PROTOCOL_VERSION, EventEnvelope
from kaiwen_gateway.modules.events.redis import (
    DEFAULT_CLAIM_MIN_IDLE_MS,
    DEFAULT_MAX_DELIVERY_ATTEMPTS,
    DEFAULT_STREAM_MAXLEN,
    RedisClientConfig,
    RedisStreamsConfig,
    RedisStreamsDurableStore,
    create_async_redis_client,
)


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((pct / 100.0) * (len(ordered) - 1)))))
    return round(ordered[idx], 3)


def _summary(values: list[float]) -> dict[str, float]:
    return {
        "count": float(len(values)),
        "p50": _percentile(values, 50.0),
        "p95": _percentile(values, 95.0),
        "p99": _percentile(values, 99.0),
        "max": round(max(values), 3) if values else 0.0,
    }


async def run_benchmark(
    *,
    redis_url: str,
    event_bytes: int,
    rate_per_sec: float,
    producers: int,
    consumers: int,
    duration_seconds: float,
    maxlen: int,
    inject_failure: bool = False,
) -> dict[str, Any]:
    """Execute a paced multi-producer/multi-consumer Redis Streams qualification run."""
    prefix = f"kwbench{uuid.uuid4().hex[:8]}"
    tenant_id = "tenant:bench"
    topic = "bench.events"
    group = "bench-workers"

    cfg = RedisStreamsConfig(
        redis=RedisClientConfig(url=redis_url, key_prefix=prefix),
        maxlen=maxlen,
        claim_min_idle_ms=50,
        max_delivery_attempts=DEFAULT_MAX_DELIVERY_ATTEMPTS,
    )
    info_client = create_async_redis_client(cfg.redis)
    producer_stores = [RedisStreamsDurableStore(cfg) for _ in range(producers)]
    consumer_stores = [RedisStreamsDurableStore(cfg) for _ in range(consumers)]
    for s in (*producer_stores, *consumer_stores):
        await s.startup()

    tracemalloc.start()
    append_lat_ms: list[float] = []
    read_lat_ms: list[float] = []
    ack_lat_ms: list[float] = []
    e2e_lat_ms: list[float] = []
    recovery_lat_ms: list[float] = []
    published_ids: set[str] = set()
    acked_ids: set[str] = set()
    publish_timestamps_ns: dict[str, int] = {}
    redelivery_count = 0
    reclaimed_total = 0

    padding = "x" * max(1, event_bytes - 220)
    total_events_target = max(1, int(round(rate_per_sec * duration_seconds)))
    per_producer = max(1, total_events_target // producers)
    interval = (1.0 / (rate_per_sec / producers)) if rate_per_sec > 0 else 0.0
    producers_done = asyncio.Event()

    async def producer_task(idx: int, store: RedisStreamsDurableStore) -> None:
        start_ns = time.perf_counter_ns()
        for seq in range(per_producer):
            target_ns = start_ns + int(seq * interval * 1_000_000_000)
            now_ns = time.perf_counter_ns()
            if target_ns > now_ns:
                await asyncio.sleep((target_ns - now_ns) / 1_000_000_000.0)
            eid = f"b-{idx}-{seq}"
            env = EventEnvelope(
                protocol_version=EVENTS_PROTOCOL_VERSION,
                event_id=eid,
                event_type="bench.tick",
                topic=topic,
                timestamp="2026-11-18T14:00:00Z",
                tenant_id=tenant_id,
                principal_id=f"prod-{idx}",
                payload={"pad": padding},
            )
            t0_ns = time.perf_counter_ns()
            publish_timestamps_ns[eid] = t0_ns
            await store.append(env)
            append_lat_ms.append((time.perf_counter_ns() - t0_ns) / 1_000_000.0)
            published_ids.add(eid)

    async def consumer_task(idx: int, store: RedisStreamsDurableStore) -> None:
        nonlocal redelivery_count
        cname = f"cons-{idx}"
        while not (producers_done.is_set() and len(acked_ids) >= len(published_ids)):
            t0_ns = time.perf_counter_ns()
            recs = await store.read_group(
                tenant_id=tenant_id,
                topic=topic,
                group=group,
                consumer=cname,
                count=25,
                block_ms=50,
            )
            if recs:
                read_lat_ms.append((time.perf_counter_ns() - t0_ns) / 1_000_000.0)
                to_ack: list[str] = []
                for rec in recs:
                    eid = rec.envelope.event_id
                    if eid in acked_ids:
                        redelivery_count += 1
                    t_pub_ns = publish_timestamps_ns.get(eid)
                    if t_pub_ns is not None:
                        e2e_lat_ms.append((time.perf_counter_ns() - t_pub_ns) / 1_000_000.0)
                    if rec.sequence_id is not None:
                        to_ack.append(rec.sequence_id)
                t_ack0_ns = time.perf_counter_ns()
                await store.ack(
                    tenant_id=tenant_id,
                    topic=topic,
                    group=group,
                    record_ids=to_ack,
                )
                ack_lat_ms.append((time.perf_counter_ns() - t_ack0_ns) / 1_000_000.0)
                for rec in recs:
                    acked_ids.add(rec.envelope.event_id)
            elif producers_done.is_set():
                break

    async def fault_and_recovery_task() -> None:
        nonlocal reclaimed_total, redelivery_count
        # Mid-run simulated consumer crash: reads a batch without ACKing while producers run
        await asyncio.sleep(min(0.5, duration_seconds * 0.35))
        crashed_store = RedisStreamsDurableStore(cfg)
        await crashed_store.startup()
        try:
            crashed_recs = await crashed_store.read_group(
                tenant_id=tenant_id,
                topic=topic,
                group=group,
                consumer="cons-crashed-midrun",
                count=10,
                block_ms=50,
            )
        finally:
            await crashed_store.shutdown()

        if crashed_recs:
            await asyncio.sleep(0.08)
            t_rec0_ns = time.perf_counter_ns()
            rec_res = await consumer_stores[-1].reclaim_pending(
                tenant_id=tenant_id,
                topic=topic,
                group=group,
                consumer="cons-recover-midrun",
                min_idle_ms=50,
                count=25,
            )
            if rec_res.reclaimed:
                to_ack = [r.sequence_id for r in rec_res.reclaimed if r.sequence_id is not None]
                await consumer_stores[-1].ack(
                    tenant_id=tenant_id,
                    topic=topic,
                    group=group,
                    record_ids=to_ack,
                )
                recovery_lat_ms.append((time.perf_counter_ns() - t_rec0_ns) / 1_000_000.0)
                reclaimed_total += len(rec_res.reclaimed)
                for rec in rec_res.reclaimed:
                    eid = rec.envelope.event_id
                    if eid in acked_ids:
                        redelivery_count += 1
                    t_pub_ns = publish_timestamps_ns.get(eid)
                    if t_pub_ns is not None:
                        e2e_lat_ms.append((time.perf_counter_ns() - t_pub_ns) / 1_000_000.0)
                    acked_ids.add(eid)

    bench_start_ns = time.perf_counter_ns()
    try:
        prod_coros = [producer_task(i, producer_stores[i]) for i in range(producers)]
        cons_coros = [consumer_task(i, consumer_stores[i]) for i in range(consumers)]
        cons_tasks = [asyncio.create_task(c) for c in cons_coros]
        extra_tasks: list[asyncio.Task[None]] = []
        if inject_failure:
            extra_tasks.append(asyncio.create_task(fault_and_recovery_task()))
        await asyncio.gather(*prod_coros, *extra_tasks)
        producers_done.set()
        await asyncio.wait_for(asyncio.gather(*cons_tasks), timeout=20.0)
        elapsed = max(0.001, (time.perf_counter_ns() - bench_start_ns) / 1_000_000_000.0)

        # Controlled end-of-run crash-recovery probe if no mid-run failure was injected
        if not inject_failure:
            crash_env = EventEnvelope(
                protocol_version=EVENTS_PROTOCOL_VERSION,
                event_id="b-crash-probe",
                event_type="bench.tick",
                topic=topic,
                timestamp="2026-11-18T14:00:00Z",
                tenant_id=tenant_id,
                principal_id="prod-0",
                payload={"pad": padding},
            )
            await producer_stores[0].append(crash_env)
            await consumer_stores[0].read_group(
                tenant_id=tenant_id,
                topic=topic,
                group=group,
                consumer="cons-crashed",
                count=1,
                block_ms=0,
            )
            await asyncio.sleep(0.06)
            t_rec0_ns = time.perf_counter_ns()
            rec_res = await consumer_stores[-1].reclaim_pending(
                tenant_id=tenant_id,
                topic=topic,
                group=group,
                consumer="cons-recover",
                min_idle_ms=50,
            )
            if rec_res.reclaimed:
                await consumer_stores[-1].ack(
                    tenant_id=tenant_id,
                    topic=topic,
                    group=group,
                    record_ids=[
                        r.sequence_id for r in rec_res.reclaimed if r.sequence_id is not None
                    ],
                )
                recovery_lat_ms.append((time.perf_counter_ns() - t_rec0_ns) / 1_000_000.0)
                reclaimed_total += len(rec_res.reclaimed)

        stats = await producer_stores[0].stream_stats(tenant_id=tenant_id, topic=topic, group=group)
        mem_info = await info_client.info("memory")
        srv_info = await info_client.info("server")
        gw_curr, gw_peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
        await info_client.aclose()
        for s in (*producer_stores, *consumer_stores):
            await s.shutdown()

    return {
        "config": {
            "event_bytes": event_bytes,
            "target_rate_per_sec": rate_per_sec,
            "producers": producers,
            "consumers": consumers,
            "duration_seconds": duration_seconds,
            "maxlen": maxlen,
            "inject_failure": inject_failure,
            "clock": "time.perf_counter_ns",
            "provisional_defaults": {
                "maxlen": DEFAULT_STREAM_MAXLEN,
                "claim_min_idle_ms": DEFAULT_CLAIM_MIN_IDLE_MS,
                "max_delivery_attempts": DEFAULT_MAX_DELIVERY_ATTEMPTS,
            },
        },
        "environment": {
            "redis_version": srv_info.get("redis_version", "unknown"),
            "platform": platform.platform(),
            "python_version": sys.version.split()[0],
            "cpu_count": os.cpu_count(),
        },
        "results": {
            "elapsed_seconds": round(elapsed, 3),
            "published_total": len(published_ids),
            "acked_total": len(acked_ids),
            "event_loss_count": len(published_ids - acked_ids),
            "reclaimed_total": reclaimed_total,
            "redelivery_count": redelivery_count,
            "throughput_events_per_sec": round(len(acked_ids) / elapsed, 2),
            "append_latency_ms": _summary(append_lat_ms),
            "read_latency_ms": _summary(read_lat_ms),
            "ack_latency_ms": _summary(ack_lat_ms),
            "recovery_latency_ms": _summary(recovery_lat_ms),
            "end_to_end_latency_ms": _summary(e2e_lat_ms),
            "stream_length": stats.length,
            "pending_entries_list_size": stats.group_pending,
            "consumer_lag": stats.group_lag,
            "redis_used_memory_bytes": int(mem_info.get("used_memory", 0)),
            "redis_used_memory_peak_bytes": int(mem_info.get("used_memory_peak", 0)),
            "gateway_tracemalloc_current_bytes": gw_curr,
            "gateway_tracemalloc_peak_bytes": gw_peak,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="E4 Redis Streams load qualification harness")
    parser.add_argument(
        "--redis-url",
        default=os.environ.get("KAIWEN_TEST_REDIS_URL", "redis://localhost:6379/0"),
    )
    parser.add_argument("--event-bytes", type=int, default=512)
    parser.add_argument("--rate-per-sec", type=float, default=120.0)
    parser.add_argument("--producers", type=int, default=2)
    parser.add_argument("--consumers", type=int, default=2)
    parser.add_argument("--duration-seconds", type=float, default=2.0)
    parser.add_argument("--maxlen", type=int, default=DEFAULT_STREAM_MAXLEN)
    parser.add_argument("--inject-failure", action="store_true")
    args = parser.parse_args()

    report = asyncio.run(
        run_benchmark(
            redis_url=args.redis_url,
            event_bytes=args.event_bytes,
            rate_per_sec=args.rate_per_sec,
            producers=args.producers,
            consumers=args.consumers,
            duration_seconds=args.duration_seconds,
            maxlen=args.maxlen,
            inject_failure=args.inject_failure,
        )
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
