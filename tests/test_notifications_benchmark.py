"""Deterministic regression tests for tools/benchmark_notifications.py (N4-R1)."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

import pytest
from tools.benchmark_notifications import (
    DeliveryMetricsTracker,
    distribute_workload,
    run_benchmark,
    validate_benchmark_params,
)

from kaiwen_gateway.modules.events import (
    DurableEventRecord,
    EventEnvelope,
    InMemoryEventBroker,
)
from kaiwen_gateway.modules.events.redis.streams import (
    StreamRecoveryResult,
    StreamStats,
)
from kaiwen_gateway.modules.notifications import NotificationsTopicAuthorizer


def test_validate_benchmark_params_rejects_invalid_inputs() -> None:
    valid: dict[str, Any] = {
        "payload_bytes": 128,
        "rate_per_sec": 50.0,
        "concurrency": 2,
        "workers": 1,
        "warmup": 0,
        "duration_seconds": 1.0,
        "maxlen": 100,
    }
    validate_benchmark_params(**valid)

    bad_overrides: list[dict[str, Any]] = [
        {"payload_bytes": 0},
        {"payload_bytes": True},
        {"rate_per_sec": 0.0},
        {"rate_per_sec": float("nan")},
        {"concurrency": 0},
        {"workers": 0},
        {"warmup": -1},
        {"duration_seconds": 0.0},
        {"maxlen": 0},
    ]
    for override in bad_overrides:
        with pytest.raises(ValueError):
            validate_benchmark_params(**{**valid, **override})


def test_distribute_workload_non_divisible_concurrency() -> None:
    counts = distribute_workload(10, 3)
    assert counts == [4, 3, 3]
    assert sum(counts) == 10
    assert distribute_workload(2, 5) == [1, 1, 0, 0, 0]


def test_delivery_metrics_tracker_separates_first_and_redelivery() -> None:
    tracker = DeliveryMetricsTracker()
    tracker.record_publish_start("n1", 1_000_000)
    tracker.record_publish_start("n2", 2_000_000)
    tracker.record_publish_failure()

    # First delivery for n1 (5 ms) and n2 (8 ms), plus redelivery for n1 (25 ms)
    tracker.record_delivery("n1", 6_000_000)
    tracker.record_delivery("n2", 10_000_000)
    tracker.record_delivery("n1", 26_000_000)
    # Warmup / untracked IDs are ignored
    tracker.record_delivery("warmup-1", 30_000_000)

    assert len(tracker.measured_ids) == 2
    assert len(tracker.delivered_ids) == 2
    assert tracker.total_delivery_attempts_observed == 3
    assert tracker.redelivery_count == 1
    assert tracker.publisher_acceptance_failures == 1
    assert tracker.first_delivery_lat_ms == [5.0, 8.0]
    assert tracker.redelivery_lat_ms == [25.0]


def test_run_benchmark_non_divisible_and_failure_injection_deterministic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import tools.benchmark_notifications as bench_mod

    class _FakeInfoClient:
        async def info(self, section: str) -> dict[str, Any]:
            return {"redis_version": "7.4.12-mock"}

        async def aclose(self) -> None:
            return None

    class _FakeStreamStore:
        def __init__(self, cfg: Any) -> None:
            self._entries: list[tuple[str, EventEnvelope]] = []
            self._pending: dict[str, EventEnvelope] = {}
            self._seq = 0

        async def startup(self) -> None:
            return None

        async def shutdown(self) -> None:
            return None

        async def append(self, event: EventEnvelope) -> str:
            self._seq += 1
            eid = f"{self._seq}-0"
            self._entries.append((eid, event))
            return eid

        async def read_group(
            self,
            *,
            tenant_id: str,
            topic: str,
            group: str,
            consumer: str,
            count: int,
            block_ms: int,
        ) -> list[DurableEventRecord]:
            out: list[DurableEventRecord] = []
            rest: list[tuple[str, EventEnvelope]] = []
            for eid, env in self._entries:
                if len(out) < count and env.tenant_id == tenant_id and env.topic == topic:
                    self._pending[eid] = env
                    out.append(DurableEventRecord(envelope=env, sequence_id=eid))
                else:
                    rest.append((eid, env))
            self._entries = rest
            if not out and block_ms > 0:
                await asyncio.sleep(min(0.01, block_ms / 1000.0))
            return out

        async def ack(
            self,
            *,
            tenant_id: str,
            topic: str,
            group: str,
            record_ids: Sequence[str],
        ) -> int:
            acked = 0
            for rid in record_ids:
                if rid in self._pending:
                    self._pending.pop(rid)
                    acked += 1
            return acked

        async def replay(
            self,
            *,
            tenant_id: str,
            topic: str,
            from_id: str = "0-0",
            limit: int = 100,
        ) -> list[DurableEventRecord]:
            return []

        async def reclaim_pending(
            self,
            *,
            tenant_id: str,
            topic: str,
            group: str,
            consumer: str,
            min_idle_ms: int | None = None,
            count: int | None = None,
            start_id: str = "0-0",
        ) -> StreamRecoveryResult:
            reclaimed = [
                DurableEventRecord(envelope=env, sequence_id=eid, delivery_attempts=2)
                for eid, env in list(self._pending.items())
            ]
            return StreamRecoveryResult(
                reclaimed=tuple(reclaimed),
                dead_lettered=(),
                next_start_id="0-0",
            )

        async def stream_stats(
            self, *, tenant_id: str, topic: str, group: str | None = None
        ) -> StreamStats:
            return StreamStats(
                length=self._seq,
                first_entry_id="1-0" if self._seq else None,
                last_entry_id=f"{self._seq}-0" if self._seq else None,
                group_pending=len(self._pending),
                group_lag=len(self._entries),
            )

    shared_broker = InMemoryEventBroker(topic_authorizer=NotificationsTopicAuthorizer())

    monkeypatch.setattr(bench_mod, "create_async_redis_client", lambda cfg: _FakeInfoClient())
    monkeypatch.setattr(bench_mod, "RedisStreamsDurableStore", _FakeStreamStore)
    monkeypatch.setattr(bench_mod, "RedisPubSubEventBroker", lambda *args, **kwargs: shared_broker)

    report = asyncio.run(
        run_benchmark(
            redis_url="redis://127.0.0.1:6379/0",
            payload_bytes=64,
            rate_per_sec=10.0,
            concurrency=3,  # 10 does not evenly divide by 3
            workers=1,
            warmup=2,
            duration_seconds=1.0,
            maxlen=100,
            inject_failure=True,
        )
    )
    assert report["environment"]["topology"] == "logical_multi_instance_in_process"
    assert (
        report["environment"]["latency_boundary"]
        == "publisher_to_broker_consumer_receive_not_websocket_wire"
    )
    res = report["results"]
    assert res["unique_notifications_published"] == 10
    assert res["unique_notifications_delivered"] == 10
    assert res["total_delivery_attempts_observed"] == 10
    assert res["redelivery_count"] == 0
    assert res["publisher_acceptance_failures"] == 0
    assert res["failure_injection"]["enabled"] is True
    assert res["failure_injection"]["matches_expected_crash_id"] is True
    assert (
        res["failure_injection"]["unacked_event_id"]
        == res["failure_injection"]["expected_crash_event_id"]
    )
    assert res["failure_injection"]["unacked_read_count"] == 1
    assert res["failure_injection"]["recovered_and_acked"] == 1
    assert res["failure_injection"]["recovered_delivered_to_broker_consumer"] is True
    assert res["stream_stats"]["group_pending"] == 0
    assert res["stream_stats"]["group_lag"] == 0
    assert res["durable_telemetry"]["durable_accepted"] == 13  # 2 warmup + 10 measured + 1 crash
    assert res["durable_telemetry"]["worker_acked"] == 13
    assert res["durable_telemetry"]["worker_reclaimed"] == 1
    assert "traced_python_allocations_kib" in res
