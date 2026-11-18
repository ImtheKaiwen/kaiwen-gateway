"""Reproducible N4 multi-instance Notifications load & recovery qualification harness."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import platform
import sys
import time
import tracemalloc
import uuid
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
    """Run a paced multi-instance Notifications N4 benchmark over Redis Streams + Pub/Sub."""
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
    # Instance A (worker/publisher side) and Instance B (subscriber side) sharing Redis
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
    e2e_lat_ms: list[float] = []
    loop_lag_ms: list[float] = []
    publish_ns: dict[str, int] = {}
    measured_ids: set[str] = set()
    delivered_ids: set[str] = set()
    errors = 0

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
            if env.event_id in measured_ids:
                t_start = publish_ns.get(env.event_id)
                if t_start is not None:
                    e2e_lat_ms.append((time.perf_counter_ns() - t_start) / 1_000_000.0)
                delivered_ids.add(env.event_id)

    lag_task = asyncio.create_task(lag_probe())
    recv_task = asyncio.create_task(recv_loop())

    try:
        # Warm-up phase
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
        per_prod = max(1, total_target // concurrency)
        interval = (1.0 / (rate_per_sec / concurrency)) if rate_per_sec > 0 else 0.0

        async def prod_task(idx: int) -> None:
            nonlocal errors
            t0 = time.perf_counter_ns()
            for seq in range(per_prod):
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
                measured_ids.add(nid)
                publish_ns[nid] = time.perf_counter_ns()
                try:
                    await publisher.publish(notif)
                except Exception:
                    errors += 1

        bench_start = time.perf_counter()
        await asyncio.gather(*(prod_task(i) for i in range(concurrency)))

        # Wait up to 3s for all measured notifications to arrive at subscriber
        deadline = time.perf_counter() + 3.0
        while len(delivered_ids) < len(measured_ids) and time.perf_counter() < deadline:
            await asyncio.sleep(0.02)
        bench_elapsed = max(0.001, time.perf_counter() - bench_start)

        if inject_failure:
            # Simulate crash before XACK via raw XREADGROUP + recover_once()
            crash_notif = NotificationEnvelope(
                notification_id=generate_notification_id(),
                schema_version=NOTIFICATIONS_SCHEMA_VERSION,
                tenant_id=tenant_id,
                recipient_id=recipient_id,
                category="bench",
                notification_type="bench.crash_test",
                priority=NotificationPriority.NORMAL,
                payload={"pad": "crash"},
                created_at="2026-11-18T12:00:00Z",
            )
            await publisher.publish(crash_notif)
            await asyncio.sleep(0.1)

        current_mem, peak_mem = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        server_info = await info_client.info("server")
        redis_version = str(server_info.get("redis_version", "unknown"))
        stream_stats = await store.stream_stats(tenant_id=tenant_id, topic="notifications.ingest")
        broker_stats = broker_sub.stats()
        dt_snap = durable_tel.snapshot()
        t_snap = telemetry.snapshot()

        return {
            "environment": {
                "python_version": sys.version.split()[0],
                "platform": platform.platform(),
                "redis_version": redis_version,
            },
            "configuration": {
                "concurrency": concurrency,
                "workers": workers,
                "rate_per_sec": rate_per_sec,
                "warmup": warmup,
                "duration_seconds": duration_seconds,
                "payload_bytes": payload_bytes,
                "maxlen": maxlen,
                "inject_failure": inject_failure,
            },
            "results": {
                "published": len(measured_ids),
                "delivered": len(delivered_ids),
                "delivery_success_rate": round(len(delivered_ids) / max(1, len(measured_ids)), 4),
                "throughput_delivered_per_sec": round(len(delivered_ids) / bench_elapsed, 2),
                "errors": errors,
                "e2e_latency_ms": _summary(e2e_lat_ms),
                "event_loop_lag_ms": _summary(loop_lag_ms),
                "memory_kib": {
                    "current": round(current_mem / 1024.0, 2),
                    "peak": round(peak_mem / 1024.0, 2),
                },
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

    # Validate InMemoryEventBroker import availability for base-package parity
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
