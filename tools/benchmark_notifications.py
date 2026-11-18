"""Reproducible N4 logical multi-instance Notifications load & recovery qualification harness."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import math
import os
import platform
import sys
import time
import tracemalloc
import uuid
from dataclasses import dataclass, field
from typing import Any

from kaiwen_gateway.core.identity import VerifiedPrincipalContext
from kaiwen_gateway.modules.events import EventBrokerConfig, InMemoryEventBroker
from kaiwen_gateway.modules.events.redis import (
    DEFAULT_MAX_DELIVERY_ATTEMPTS,
    DEFAULT_STREAM_MAXLEN,
    RedisClientConfig,
    RedisPubSubBrokerConfig,
    RedisPubSubEventBroker,
    RedisStreamsConfig,
    RedisStreamsDurableStore,
    create_async_redis_client,
)
from kaiwen_gateway.modules.notifications import (
    NOTIFICATIONS_SCHEMA_VERSION,
    InMemoryNotificationPreferenceStore,
    InMemoryNotificationsDurableTelemetrySink,
    InMemoryNotificationsTelemetrySink,
    NotificationEnvelope,
    NotificationPriority,
    NotificationsDurableConfig,
    NotificationsDurablePublisher,
    NotificationsDurableWorker,
    NotificationsTopicAuthorizer,
    derive_recipient_topic,
    generate_notification_id,
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


def distribute_workload(total_target: int, concurrency: int) -> list[int]:
    """Split `total_target` evenly across `concurrency` workers so sum == total_target."""
    if total_target < 0:
        raise ValueError("total_target must be >= 0")
    if concurrency <= 0:
        raise ValueError("concurrency must be >= 1")
    base, rem = divmod(total_target, concurrency)
    return [base + (1 if i < rem else 0) for i in range(concurrency)]


def validate_benchmark_params(
    *,
    payload_bytes: int,
    rate_per_sec: float,
    concurrency: int,
    workers: int,
    warmup: int,
    duration_seconds: float,
    maxlen: int,
) -> None:
    """Validate N4 benchmark parameters fail-closed."""
    if isinstance(payload_bytes, bool) or payload_bytes <= 0:
        raise ValueError("payload_bytes must be a positive int")
    if isinstance(rate_per_sec, bool) or not math.isfinite(rate_per_sec) or rate_per_sec <= 0.0:
        raise ValueError("rate_per_sec must be a finite positive number")
    if isinstance(concurrency, bool) or concurrency <= 0:
        raise ValueError("concurrency must be a positive int")
    if isinstance(workers, bool) or workers <= 0:
        raise ValueError("workers must be a positive int")
    if isinstance(warmup, bool) or warmup < 0:
        raise ValueError("warmup must be a non-negative int")
    if (
        isinstance(duration_seconds, bool)
        or not math.isfinite(duration_seconds)
        or duration_seconds <= 0.0
    ):
        raise ValueError("duration_seconds must be a finite positive number")
    if isinstance(maxlen, bool) or maxlen <= 0:
        raise ValueError("maxlen must be a positive int")


@dataclass(slots=True)
class DeliveryMetricsTracker:
    """Separates first-delivery latency, redelivery count/latency, and acceptance failures."""

    measured_ids: set[str] = field(default_factory=set)
    publish_ns: dict[str, int] = field(default_factory=dict)
    delivered_ids: set[str] = field(default_factory=set)
    total_delivery_attempts_observed: int = 0
    redelivery_count: int = 0
    publisher_acceptance_failures: int = 0
    first_delivery_lat_ms: list[float] = field(default_factory=list)
    redelivery_lat_ms: list[float] = field(default_factory=list)

    def record_publish_start(self, nid: str, ts_ns: int) -> None:
        self.measured_ids.add(nid)
        self.publish_ns[nid] = ts_ns

    def record_publish_failure(self) -> None:
        self.publisher_acceptance_failures += 1

    def record_delivery(self, nid: str, recv_ns: int) -> None:
        if nid not in self.measured_ids:
            return
        self.total_delivery_attempts_observed += 1
        t_start = self.publish_ns.get(nid)
        lat_ms = ((recv_ns - t_start) / 1_000_000.0) if t_start is not None else 0.0
        if nid not in self.delivered_ids:
            self.delivered_ids.add(nid)
            self.first_delivery_lat_ms.append(lat_ms)
        else:
            self.redelivery_count += 1
            self.redelivery_lat_ms.append(lat_ms)


class _BenchProducerAuthorizer:
    async def authorize_publish(
        self,
        *,
        producer: VerifiedPrincipalContext | None,
        notification: NotificationEnvelope,
    ) -> None:
        return None


async def run_benchmark(
    *,
    redis_url: str,
    payload_bytes: int,
    rate_per_sec: float,
    concurrency: int,
    workers: int,
    warmup: int,
    duration_seconds: float,
    maxlen: int,
    inject_failure: bool = False,
) -> dict[str, Any]:
    """Run a paced logical multi-instance Notifications N4 benchmark over Redis Streams + Pub/Sub."""
    validate_benchmark_params(
        payload_bytes=payload_bytes,
        rate_per_sec=rate_per_sec,
        concurrency=concurrency,
        workers=workers,
        warmup=warmup,
        duration_seconds=duration_seconds,
        maxlen=maxlen,
    )

    prefix = f"kwnbench{uuid.uuid4().hex[:8]}"
    tenant_id = "tenant:bench"
    recipient_id = "user-bench-1"
    recipient_topic = derive_recipient_topic(tenant_id, recipient_id)

    redis_cfg = RedisClientConfig(url=redis_url, key_prefix=prefix)
    streams_cfg = RedisStreamsConfig(
        redis=redis_cfg,
        maxlen=maxlen,
        claim_min_idle_ms=50,
        max_delivery_attempts=DEFAULT_MAX_DELIVERY_ATTEMPTS,
    )
    pubsub_cfg = RedisPubSubBrokerConfig(
        redis=redis_cfg,
        broker=EventBrokerConfig(enable_rate_limiting=False),
    )

    info_client = create_async_redis_client(redis_cfg)
    store = RedisStreamsDurableStore(streams_cfg)
    # Logical multi-instance topology inside one Python process sharing Redis
    broker_pub = RedisPubSubEventBroker(pubsub_cfg, topic_authorizer=NotificationsTopicAuthorizer())
    broker_sub = RedisPubSubEventBroker(pubsub_cfg, topic_authorizer=NotificationsTopicAuthorizer())
    await store.startup()
    await broker_pub.startup()
    await broker_sub.startup()

    sub_ctx = VerifiedPrincipalContext(
        provider_id="bench-idp",
        tenant_id=tenant_id,
        connection_id="conn_bench_sub",
        principal_id=recipient_id,
        allowed_scopes=frozenset({"events:subscribe"}),
    )
    await broker_sub.subscribe(sub_ctx, recipient_topic)
    consumer = broker_sub.consumer("conn_bench_sub")

    prefs = InMemoryNotificationPreferenceStore()
    telemetry = InMemoryNotificationsTelemetrySink()
    durable_tel = InMemoryNotificationsDurableTelemetrySink()

    publisher = NotificationsDurablePublisher(
        durable_store=store,
        producer_authorizer=_BenchProducerAuthorizer(),
        telemetry=telemetry,
        durable_telemetry=durable_tel,
    )
    worker_instances = [
        NotificationsDurableWorker(
            config=NotificationsDurableConfig(
                tenant_id=tenant_id,
                consumer=f"bench-w-{i}",
                read_batch_size=32,
                block_ms=100,
                poll_interval_seconds=0.0,
                recovery_interval_seconds=0.2,
                claim_min_idle_ms=50,
            ),
            durable_store=store,
            broker=broker_pub,
            preference_port=prefs,
            telemetry=telemetry,
            durable_telemetry=durable_tel,
        )
        for i in range(workers)
    ]

    padding = "x" * max(1, payload_bytes)
    tracker = DeliveryMetricsTracker()
    loop_lag_ms: list[float] = []
    injected_ids: set[str] = set()
    injected_delivered: set[str] = set()

    for w in worker_instances:
        w.start()

    stop_lag = asyncio.Event()

    async def lag_probe() -> None:
        loop = asyncio.get_running_loop()
        while not stop_lag.is_set():
            t0 = loop.time()
            await asyncio.sleep(0.01)
            lag = max(0.0, ((loop.time() - t0) - 0.01) * 1000.0)
            loop_lag_ms.append(lag)

    async def recv_loop() -> None:
        while True:
            env = await consumer.receive()
            if env is None:
                break
            recv_ns = time.perf_counter_ns()
            if env.event_id in tracker.measured_ids:
                tracker.record_delivery(env.event_id, recv_ns)
            elif env.event_id in injected_ids:
                injected_delivered.add(env.event_id)

    lag_task = asyncio.create_task(lag_probe())
    recv_task = asyncio.create_task(recv_loop())

    try:
        # Warm-up phase (excluded from measured metrics)
        for _ in range(warmup):
            await publisher.publish(
                NotificationEnvelope(
                    notification_id=generate_notification_id(),
                    schema_version=NOTIFICATIONS_SCHEMA_VERSION,
                    tenant_id=tenant_id,
                    recipient_id=recipient_id,
                    category="bench",
                    notification_type="bench.warmup",
                    priority=NotificationPriority.NORMAL,
                    payload={"pad": "w"},
                    created_at="2026-11-18T12:00:00Z",
                )
            )
        await asyncio.sleep(0.15)

        tracemalloc.start()
        total_target = max(1, int(round(rate_per_sec * duration_seconds)))
        counts_per_prod = distribute_workload(total_target, concurrency)
        interval = (1.0 / (rate_per_sec / concurrency)) if rate_per_sec > 0 else 0.0

        async def prod_task(idx: int, count: int) -> None:
            t0 = time.perf_counter_ns()
            for seq in range(count):
                target_ns = t0 + int(seq * interval * 1_000_000_000)
                now_ns = time.perf_counter_ns()
                if target_ns > now_ns:
                    await asyncio.sleep((target_ns - now_ns) / 1_000_000_000.0)
                nid = f"nbench_{idx}_{seq}_{uuid.uuid4().hex[:6]}"
                notif = NotificationEnvelope(
                    notification_id=nid,
                    schema_version=NOTIFICATIONS_SCHEMA_VERSION,
                    tenant_id=tenant_id,
                    recipient_id=recipient_id,
                    category="bench",
                    notification_type="bench.msg",
                    priority=NotificationPriority.NORMAL,
                    payload={"pad": padding},
                    created_at="2026-11-18T12:00:00Z",
                )
                tracker.record_publish_start(nid, time.perf_counter_ns())
                try:
                    await publisher.publish(notif)
                except Exception:
                    tracker.record_publish_failure()

        bench_start = time.perf_counter()
        await asyncio.gather(
            *(prod_task(i, cnt) for i, cnt in enumerate(counts_per_prod) if cnt > 0)
        )

        # Wait up to 3s for all measured notifications to arrive at broker consumer
        # and finish their XACK before stopping background workers.
        deadline = time.perf_counter() + 3.0
        while (
            len(tracker.delivered_ids) < len(tracker.measured_ids)
            or durable_tel.snapshot().worker_acked < durable_tel.snapshot().durable_accepted
        ) and time.perf_counter() < deadline:
            await asyncio.sleep(0.02)
        bench_elapsed = max(0.001, time.perf_counter() - bench_start)

        failure_injection_report: dict[str, Any] = {"enabled": inject_failure}
        if inject_failure:
            # Stop background workers first so raw unacked XREADGROUP + recover_once() is deterministic
            for w in worker_instances:
                await w.stop()

            crash_nid = generate_notification_id()
            injected_ids.add(crash_nid)
            await publisher.publish(
                NotificationEnvelope(
                    notification_id=crash_nid,
                    schema_version=NOTIFICATIONS_SCHEMA_VERSION,
                    tenant_id=tenant_id,
                    recipient_id=recipient_id,
                    category="bench",
                    notification_type="bench.crash_test",
                    priority=NotificationPriority.NORMAL,
                    payload={"pad": "crash"},
                    created_at="2026-11-18T12:00:00Z",
                )
            )
            unacked = await store.read_group(
                tenant_id=tenant_id,
                topic="notifications.ingest",
                group="notifications-workers",
                consumer="bench-crashed-consumer",
                count=1,
                block_ms=50,
            )
            unacked_event_id = unacked[0].envelope.event_id if unacked else None
            await asyncio.sleep(0.08)
            recovered_acked = await worker_instances[0].recover_once(min_idle_ms=50)
            rec_deadline = time.perf_counter() + 2.0
            while crash_nid not in injected_delivered and time.perf_counter() < rec_deadline:
                await asyncio.sleep(0.01)
            failure_injection_report = {
                "enabled": True,
                "expected_crash_event_id": crash_nid,
                "unacked_event_id": unacked_event_id,
                "matches_expected_crash_id": unacked_event_id == crash_nid,
                "unacked_read_count": len(unacked),
                "recovered_and_acked": recovered_acked,
                "recovered_delivered_to_broker_consumer": crash_nid in injected_delivered,
            }

        current_mem, peak_mem = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        server_info = await info_client.info("server")
        redis_version = str(server_info.get("redis_version", "unknown"))
        stream_stats = await store.stream_stats(
            tenant_id=tenant_id,
            topic="notifications.ingest",
            group="notifications-workers",
        )
        broker_stats = broker_sub.stats()
        dt_snap = durable_tel.snapshot()
        t_snap = telemetry.snapshot()

        return {
            "environment": {
                "python_version": sys.version.split()[0],
                "platform": platform.platform(),
                "redis_version": redis_version,
                "topology": "logical_multi_instance_in_process",
                "latency_boundary": "publisher_to_broker_consumer_receive_not_websocket_wire",
            },
            "configuration": {
                "concurrency": concurrency,
                "workers": workers,
                "rate_per_sec": rate_per_sec,
                "warmup": warmup,
                "duration_seconds": duration_seconds,
                "requested_measured_notifications": total_target,
                "payload_bytes": payload_bytes,
                "maxlen": maxlen,
                "inject_failure": inject_failure,
            },
            "results": {
                "unique_notifications_published": len(tracker.measured_ids),
                "unique_notifications_delivered": len(tracker.delivered_ids),
                "total_delivery_attempts_observed": tracker.total_delivery_attempts_observed,
                "redelivery_count": tracker.redelivery_count,
                "publisher_acceptance_failures": tracker.publisher_acceptance_failures,
                "delivery_success_rate": round(
                    len(tracker.delivered_ids) / max(1, len(tracker.measured_ids)), 4
                ),
                "throughput_unique_delivered_per_sec": round(
                    len(tracker.delivered_ids) / bench_elapsed, 2
                ),
                "first_delivery_latency_ms": _summary(tracker.first_delivery_lat_ms),
                "redelivery_latency_ms": _summary(tracker.redelivery_lat_ms),
                "event_loop_lag_ms": _summary(loop_lag_ms),
                "traced_python_allocations_kib": {
                    "current": round(current_mem / 1024.0, 2),
                    "peak": round(peak_mem / 1024.0, 2),
                },
                "failure_injection": failure_injection_report,
                "durable_telemetry": {
                    "durable_accepted": dt_snap.durable_accepted,
                    "worker_processed": dt_snap.worker_processed,
                    "worker_acked": dt_snap.worker_acked,
                    "worker_reclaimed": dt_snap.worker_reclaimed,
                    "worker_dead_lettered": dt_snap.worker_dead_lettered,
                    "worker_retryable_failures": dt_snap.worker_retryable_failures,
                    "redis_failures": dt_snap.redis_failures,
                },
                "notifications_telemetry": {
                    "published": t_snap.notifications_published,
                    "dispatched": t_snap.notifications_dispatched,
                },
                "stream_stats": {
                    "length": stream_stats.length,
                    "group_pending": stream_stats.group_pending,
                    "group_lag": stream_stats.group_lag,
                },
                "broker_stats": {
                    "active_connections": broker_stats.active_connections,
                    "queued_events": broker_stats.queued_events,
                },
            },
        }
    finally:
        stop_lag.set()
        for w in worker_instances:
            await w.stop()
        lag_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await lag_task
        await broker_sub.shutdown()
        await broker_pub.shutdown()
        recv_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await recv_task
        await store.shutdown()
        await info_client.aclose()


def main() -> None:
    parser = argparse.ArgumentParser(description="Notifications N4 benchmark harness")
    parser.add_argument(
        "--redis-url",
        default=os.environ.get("KAIWEN_TEST_REDIS_URL", "redis://127.0.0.1:6379/0"),
    )
    parser.add_argument("--payload-bytes", type=int, default=512)
    parser.add_argument("--rate", type=float, default=200.0)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--warmup", type=int, default=50)
    parser.add_argument("--duration", type=float, default=3.0)
    parser.add_argument("--maxlen", type=int, default=DEFAULT_STREAM_MAXLEN)
    parser.add_argument("--inject-failure", action="store_true")
    args = parser.parse_args()

    _ = InMemoryEventBroker
    report = asyncio.run(
        run_benchmark(
            redis_url=args.redis_url,
            payload_bytes=args.payload_bytes,
            rate_per_sec=args.rate,
            concurrency=args.concurrency,
            workers=args.workers,
            warmup=args.warmup,
            duration_seconds=args.duration,
            maxlen=args.maxlen,
            inject_failure=args.inject_failure,
        )
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
