"""Deterministic offline unit tests for E4 RedisStreamsDurableStore & StreamRecoveryWorker."""

from __future__ import annotations

import asyncio
import copy
from typing import Any

import pytest

from kaiwen_gateway.modules.events import (
    EVENTS_PROTOCOL_VERSION,
    DurableEventStorePort,
    EventEnvelope,
    EventsBackendUnavailableError,
    EventsModuleClosedError,
    InMemoryDurableStreamTelemetrySink,
    InvalidEventError,
)
from kaiwen_gateway.modules.events.redis import (
    RedisClientConfig,
    RedisStreamsConfig,
    RedisStreamsDurableStore,
    StreamRecoveryWorker,
)


class _FakeStreamBackend:
    """In-memory deterministic simulator for Redis Streams commands."""

    def __init__(self) -> None:
        self.streams: dict[str, list[tuple[str, dict[str, str]]]] = {}
        self.groups: dict[tuple[str, str], dict[str, Any]] = {}
        self.seq: int = 0
        self.now_ms: int = 1_700_000_000_000
        self.fail_ping: bool = False
        self.timeout_xadd: bool = False
        self.fail_xreadgroup: bool = False
        self.closed: bool = False

    def _next_id(self) -> str:
        self.seq += 1
        return f"{self.now_ms}-{self.seq}"

    async def ping(self) -> bool:
        if self.fail_ping:
            raise ConnectionError("Redis ping failed")
        return True

    async def aclose(self) -> None:
        self.closed = True

    async def xadd(
        self,
        name: str,
        fields: dict[str, str],
        *,
        maxlen: int | None = None,
        approximate: bool = True,
    ) -> str:
        if self.timeout_xadd:
            raise TimeoutError("Redis xadd timed out")
        entry_id = self._next_id()
        stream = self.streams.setdefault(name, [])
        stream.append((entry_id, dict(fields)))
        if maxlen is not None and len(stream) > maxlen:
            del stream[: len(stream) - maxlen]
        return entry_id

    async def xgroup_create(
        self, name: str, groupname: str, *, id: str = "0", mkstream: bool = False
    ) -> bool:
        if mkstream:
            self.streams.setdefault(name, [])
        key = (name, groupname)
        if key in self.groups:
            raise RuntimeError("BUSYGROUP Consumer Group name already exists")
        self.groups[key] = {
            "last_index": 0,
            "pel": {},  # entry_id -> {"consumer": str, "delivered_ms": int, "times": int}
        }
        return True

    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        *,
        count: int = 10,
        block: int | None = None,
    ) -> list[list[Any]]:
        if self.fail_xreadgroup:
            raise ConnectionError("Redis xreadgroup failed")
        out: list[list[Any]] = []
        for stream_name, cursor in streams.items():
            grp = self.groups.get((stream_name, groupname))
            entries = self.streams.get(stream_name, [])
            if grp is None or cursor != ">":
                continue
            start_idx = grp["last_index"]
            batch = entries[start_idx : start_idx + count]
            if not batch:
                continue
            grp["last_index"] = start_idx + len(batch)
            for eid, _fields in batch:
                grp["pel"][eid] = {
                    "consumer": consumername,
                    "delivered_ms": self.now_ms,
                    "times": 1,
                }
            out.append([stream_name, [(eid, copy.deepcopy(f)) for eid, f in batch]])
        return out

    async def xack(self, name: str, groupname: str, *ids: str) -> int:
        grp = self.groups.get((name, groupname))
        if grp is None:
            return 0
        acked = 0
        for eid in ids:
            if eid in grp["pel"]:
                grp["pel"].pop(eid)
                acked += 1
        return acked

    async def xrange(
        self, name: str, *, min: str = "-", max: str = "+", count: int = 100
    ) -> list[tuple[str, dict[str, str]]]:
        entries = self.streams.get(name, [])
        out: list[tuple[str, dict[str, str]]] = []
        for eid, fields in entries:
            if min not in ("-", "0-0") and _id_tuple(eid) < _id_tuple(min):
                continue
            out.append((eid, copy.deepcopy(fields)))
            if len(out) >= count:
                break
        return out

    async def xpending(self, name: str, groupname: str) -> dict[str, Any]:
        grp = self.groups.get((name, groupname))
        if grp is None or not grp["pel"]:
            return {"pending": 0, "min": None, "max": None, "consumers": []}
        ids = sorted(grp["pel"].keys(), key=_id_tuple)
        return {"pending": len(ids), "min": ids[0], "max": ids[-1], "consumers": []}

    async def xpending_range(
        self, name: str, groupname: str, *, min: str = "-", max: str = "+", count: int = 50
    ) -> list[dict[str, Any]]:
        grp = self.groups.get((name, groupname))
        if grp is None:
            return []
        rows: list[dict[str, Any]] = []
        for eid in sorted(grp["pel"].keys(), key=_id_tuple):
            if min not in ("-", "0-0") and _id_tuple(eid) < _id_tuple(min):
                continue
            if max != "+" and _id_tuple(eid) > _id_tuple(max):
                continue
            meta = grp["pel"][eid]
            rows.append(
                {
                    "message_id": eid,
                    "consumer": meta["consumer"],
                    "time_since_delivered": self.now_ms - meta["delivered_ms"],
                    "times_delivered": meta["times"],
                }
            )
            if len(rows) >= count:
                break
        return rows

    async def xautoclaim(
        self,
        name: str,
        groupname: str,
        consumername: str,
        *,
        min_idle_time: int,
        start_id: str = "0-0",
        count: int = 50,
    ) -> tuple[str, list[tuple[str, dict[str, str]]], list[str]]:
        grp = self.groups.get((name, groupname))
        if grp is None:
            return ("0-0", [], [])
        entry_map = {eid: f for eid, f in self.streams.get(name, [])}
        claimed: list[tuple[str, dict[str, str]]] = []
        for eid in sorted(grp["pel"].keys(), key=_id_tuple):
            meta = grp["pel"][eid]
            if (self.now_ms - meta["delivered_ms"]) >= min_idle_time:
                meta["consumer"] = consumername
                meta["delivered_ms"] = self.now_ms
                meta["times"] += 1
                fields = entry_map.get(eid)
                if fields is not None:
                    claimed.append((eid, copy.deepcopy(fields)))
                if len(claimed) >= count:
                    break
        return ("0-0", claimed, [])

    async def xinfo_stream(self, name: str) -> dict[str, Any]:
        entries = self.streams.get(name, [])
        if not entries:
            raise RuntimeError("ERR no such key")
        return {
            "length": len(entries),
            "first-entry": entries[0],
            "last-entry": entries[-1],
        }

    async def xinfo_groups(self, name: str) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        total = len(self.streams.get(name, []))
        for (s_name, g_name), grp in self.groups.items():
            if s_name == name:
                out.append(
                    {
                        "name": g_name,
                        "pending": len(grp["pel"]),
                        "lag": max(0, total - grp["last_index"]),
                    }
                )
        return out


def _id_tuple(eid: str) -> tuple[int, int]:
    a, b = eid.split("-", 1)
    return int(a), int(b)


def _env(
    *, event_id: str = "evt-1", tenant_id: str = "tenant:1", topic: str = "orders.created"
) -> EventEnvelope:
    return EventEnvelope(
        protocol_version=EVENTS_PROTOCOL_VERSION,
        event_id=event_id,
        event_type="order.created",
        topic=topic,
        timestamp="2026-11-18T12:00:00Z",
        tenant_id=tenant_id,
        principal_id="svc-orders",
        payload={"amount": 42},
        correlation_id="corr-1",
    )


def test_streams_config_validation_and_protocol_conformance() -> None:
    backend = _FakeStreamBackend()
    store = RedisStreamsDurableStore(redis_client=backend)
    assert isinstance(store, DurableEventStorePort)

    for bad_kwargs in (
        {"maxlen": 0},
        {"maxlen": True},
        {"claim_min_idle_ms": -1},
        {"max_delivery_attempts": 0},
        {"recovery_interval_seconds": float("nan")},
        {"approximate_trim": "yes"},
    ):
        with pytest.raises(InvalidEventError):
            RedisStreamsConfig(**bad_kwargs)


def test_streams_append_read_no_auto_ack_recovery_and_poison_dlq() -> None:
    async def scenario() -> None:
        backend = _FakeStreamBackend()
        telemetry = InMemoryDurableStreamTelemetrySink()
        cfg = RedisStreamsConfig(
            redis=RedisClientConfig(key_prefix="kwe4"),
            maxlen=5,
            claim_min_idle_ms=100,
            max_delivery_attempts=3,
            recovery_interval_seconds=0.02,
        )
        store = RedisStreamsDurableStore(cfg, redis_client=backend, telemetry=telemetry)
        await store.startup()
        assert store.is_healthy is True

        # Append and read_group without auto-ack
        eid = await store.append(_env(event_id="evt-a1"))
        recs = await store.read_group(
            tenant_id="tenant:1",
            topic="orders.created",
            group="workers",
            consumer="c-1",
            count=10,
            block_ms=0,
        )
        assert len(recs) == 1
        assert recs[0].sequence_id == eid
        assert recs[0].envelope.event_id == "evt-a1"

        # Still pending because read_group never auto-acks
        pend = await store.pending_summary(
            tenant_id="tenant:1", topic="orders.created", group="workers"
        )
        assert pend.pending_count == 1

        # Simulate consumer c-1 crash -> c-2 reclaims up to max_delivery_attempts=3, then DLQ
        backend.now_ms += 200
        r1 = await store.reclaim_pending(
            tenant_id="tenant:1", topic="orders.created", group="workers", consumer="c-2"
        )
        assert len(r1.reclaimed) == 1
        assert len(r1.dead_lettered) == 0

        backend.now_ms += 200
        r2 = await store.reclaim_pending(
            tenant_id="tenant:1", topic="orders.created", group="workers", consumer="c-2"
        )
        assert len(r2.reclaimed) == 1
        assert len(r2.dead_lettered) == 0

        # 4th delivery exceeds max_delivery_attempts=3 -> quarantined to DLQ and ACKed from PEL
        backend.now_ms += 200
        r3 = await store.reclaim_pending(
            tenant_id="tenant:1", topic="orders.created", group="workers", consumer="c-2"
        )
        assert len(r3.reclaimed) == 0
        assert len(r3.dead_lettered) == 1
        assert r3.dead_lettered[0].event_id == "evt-a1"
        assert r3.dead_lettered[0].reason == "max_delivery_attempts_exceeded"

        pend_after = await store.pending_summary(
            tenant_id="tenant:1", topic="orders.created", group="workers"
        )
        assert pend_after.pending_count == 0
        dlq = await store.dead_letter_entries(tenant_id="tenant:1", topic="orders.created")
        assert len(dlq) == 1

        snap = telemetry.snapshot()
        assert snap.append_succeeded == 1
        assert snap.retry_exhausted == 1
        assert snap.dead_lettered == 1

        await store.shutdown()
        assert store.is_closed is True
        with pytest.raises(EventsModuleClosedError):
            await store.append(_env(event_id="evt-closed"))

    asyncio.run(scenario())


def test_streams_malformed_quarantine_ambiguous_write_retention_and_worker() -> None:
    async def scenario() -> None:
        backend = _FakeStreamBackend()
        telemetry = InMemoryDurableStreamTelemetrySink()
        cfg = RedisStreamsConfig(
            maxlen=3,
            claim_min_idle_ms=10,
            max_delivery_attempts=5,
            recovery_interval_seconds=0.02,
        )
        store = RedisStreamsDurableStore(cfg, redis_client=backend, telemetry=telemetry)
        await store.startup()

        # Ambiguous write on timeout does not retry or return fake ID
        backend.timeout_xadd = True
        with pytest.raises(EventsBackendUnavailableError, match="ambiguous"):
            await store.append(_env(event_id="evt-timeout"))
        assert telemetry.snapshot().append_ambiguous == 1
        backend.timeout_xadd = False

        # Malformed entry in stream is quarantined without crashing consumer
        skey = store.stream_key("tenant:1", "orders.created")
        backend.streams[skey] = [
            ("1700000000000-1", {"envelope": "{bad-json", "event_id": "evt-bad"})
        ]
        recs = await store.read_group(
            tenant_id="tenant:1",
            topic="orders.created",
            group="g1",
            consumer="c1",
            count=10,
            block_ms=0,
        )
        assert recs == []
        assert telemetry.snapshot().malformed_records == 1
        assert (
            len(await store.dead_letter_entries(tenant_id="tenant:1", topic="orders.created")) == 1
        )

        # Retention loss detection in replay()
        for idx in range(5):
            await store.append(_env(event_id=f"evt-r-{idx}"))
        # Requesting an older cursor below first-entry records retention_trimmed_losses
        replayed = await store.replay(
            tenant_id="tenant:1",
            topic="orders.created",
            from_id="1-0",
            limit=10,
        )
        assert len(replayed) == 3  # maxlen=3
        assert telemetry.snapshot().retention_trimmed_losses >= 1

        # StreamRecoveryWorker processes reclaimed entry and ACKs only after handler succeeds
        await store.read_group(
            tenant_id="tenant:1",
            topic="orders.created",
            group="g-worker",
            consumer="c-dead",
            count=1,
            block_ms=0,
        )
        backend.now_ms += 50
        processed: list[str] = []

        async def _handle(rec: Any) -> None:
            processed.append(rec.envelope.event_id)

        worker = StreamRecoveryWorker(
            store=store,
            tenant_id="tenant:1",
            topic="orders.created",
            group="g-worker",
            consumer="c-recover",
            handler=_handle,
        )
        worker.start()
        for _ in range(30):
            if processed:
                break
            await asyncio.sleep(0.01)
        await worker.stop()
        assert len(processed) == 1
        assert (
            await store.pending_summary(
                tenant_id="tenant:1", topic="orders.created", group="g-worker"
            )
        ).pending_count == 0
        await store.shutdown()

    asyncio.run(scenario())


def test_streams_p1_active_pel_ahead_of_poison_partial_batch_and_validation_edges() -> None:
    async def scenario() -> None:
        backend = _FakeStreamBackend()
        cfg = RedisStreamsConfig(
            maxlen=50,
            claim_min_idle_ms=100,
            max_delivery_attempts=2,
            max_read_batch_size=5,
            max_replay_limit=5,
            recovery_batch_size=1,
        )
        store = RedisStreamsDurableStore(cfg, redis_client=backend)
        await store.startup()

        # Empty stream stats
        empty_stats = await store.stream_stats(
            tenant_id="tenant:empty", topic="orders.created", group="g1"
        )
        assert empty_stats.length == 0
        assert empty_stats.group_pending == 0

        # Batch / replay bounds
        with pytest.raises(InvalidEventError):
            await store.read_group(
                tenant_id="tenant:1",
                topic="orders.created",
                group="g1",
                consumer="c1",
                count=6,
                block_ms=0,
            )
        with pytest.raises(InvalidEventError):
            await store.replay(tenant_id="tenant:1", topic="orders.created", limit=6)

        # Tenant mismatch in stream payload is quarantined
        skey = store.stream_key("tenant:1", "orders.created")
        backend.streams[skey] = [
            (
                "1700000000000-1",
                {"envelope": _env(event_id="evt-forged", tenant_id="tenant:2").to_json()},
            )
        ]
        assert (
            await store.read_group(
                tenant_id="tenant:1",
                topic="orders.created",
                group="g-tm",
                consumer="c1",
                count=1,
                block_ms=0,
            )
            == []
        )

        # P1 #1: Active non-idle entry ahead of idle poison entry in PEL (recovery_batch_size=1)
        await store.append(_env(event_id="evt-active", tenant_id="tenant:p1"))
        await store.append(_env(event_id="evt-poison-behind", tenant_id="tenant:p1"))
        await store.read_group(
            tenant_id="tenant:p1",
            topic="orders.created",
            group="gp1",
            consumer="c-1",
            count=1,
            block_ms=0,
        )
        # Advance clock so evt-active is NOT idle yet when evt-poison-behind becomes idle?
        # Read poison first, then active later so active is newer and non-idle? Wait:
        # Both are in PEL; if active is read AFTER poison, poison is first. To put active AHEAD of poison:
        # read active at t=0, poison at t=0, re-read/ack active? No, re-deliver active at t=150 so delivered_ms=150!
        backend.now_ms += 150
        # Reclaim only 1 (evt-active) at t=150 so its delivered_ms becomes 150, while evt-poison-behind stays at t=0?
        # Wait: let's read evt-active first, advance 150ms, read evt-poison-behind? Then active is idle too!
        # Instead: read evt-active, then read evt-poison-behind, advance 110ms -> reclaim evt-active at t=110 (now active delivered_ms=110, idle=0),
        # while evt-poison-behind was read at t=0 (idle=110)! Now active is ahead in PEL (lower ID) and NON-idle!
        await store.read_group(
            tenant_id="tenant:p1",
            topic="orders.created",
            group="gp1",
            consumer="c-2",
            count=1,
            block_ms=0,
        )
        # evt-active was delivered at t=0; reclaim it at t=110 -> its delivered_ms resets to 110 (idle=0)
        backend.now_ms += 110
        await store.reclaim_pending(
            tenant_id="tenant:p1",
            topic="orders.created",
            group="gp1",
            consumer="c-active",
            min_idle_ms=100,
            count=1,
        )
        # Now evt-active (ahead in PEL) has idle=0, while evt-poison-behind (2nd in PEL) has idle=260!
        # Reclaim with count=1: XAUTOCLAIM skips evt-active and claims evt-poison-behind (attempts=2 -> reclaimed)
        r_ok = await store.reclaim_pending(
            tenant_id="tenant:p1",
            topic="orders.created",
            group="gp1",
            consumer="c-rec",
            min_idle_ms=100,
            count=1,
        )
        assert len(r_ok.reclaimed) == 1
        assert r_ok.reclaimed[0].envelope.event_id == "evt-poison-behind"
        # Advance 110ms again (still keep evt-active non-idle by not letting it exceed, or reclaim at +110ms):
        # Next reclaim at +110ms: evt-poison-behind reaches attempts=3 > max_delivery_attempts=2 -> DLQ!
        backend.now_ms += 110
        # Reclaim evt-active once so it's fresh, or use count=2 -> both idle, let's test poison quarantined!
        r_dlq = await store.reclaim_pending(
            tenant_id="tenant:p1",
            topic="orders.created",
            group="gp1",
            consumer="c-rec",
            min_idle_ms=100,
            count=2,
        )
        assert any(d.event_id == "evt-poison-behind" for d in r_dlq.dead_lettered)

        # P1 #2: StreamRecoveryWorker.run_once() partial batch failure ACKs succeeded record
        await store.append(_env(event_id="evt-ok", tenant_id="tenant:batch"))
        await store.append(_env(event_id="evt-fail", tenant_id="tenant:batch"))
        await store.read_group(
            tenant_id="tenant:batch",
            topic="orders.created",
            group="gb",
            consumer="c-dead",
            count=2,
            block_ms=0,
        )
        backend.now_ms += 200

        async def _partial_handler(rec: Any) -> None:
            if rec.envelope.event_id == "evt-fail":
                raise RuntimeError("boom")

        worker = StreamRecoveryWorker(
            store=RedisStreamsDurableStore(
                RedisStreamsConfig(claim_min_idle_ms=100, recovery_batch_size=5),
                redis_client=backend,
            ),
            tenant_id="tenant:batch",
            topic="orders.created",
            group="gb",
            consumer="c-worker",
            handler=_partial_handler,
        )
        with pytest.raises(RuntimeError, match="boom"):
            await worker.run_once()
        # evt-ok MUST have been ACKed; only evt-fail remains pending
        pend = await store.pending_summary(
            tenant_id="tenant:batch", topic="orders.created", group="gb"
        )
        assert pend.pending_count == 1
        await store.shutdown()

    asyncio.run(scenario())
