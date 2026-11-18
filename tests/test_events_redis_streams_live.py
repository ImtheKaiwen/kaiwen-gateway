"""Live Redis integration tests for E4 RedisStreamsDurableStore (TESTS A–L).

All tests are skipped unless ``KAIWEN_TEST_REDIS_URL`` is set to a reachable Redis server.
"""

from __future__ import annotations

import asyncio
import logging
import os
import uuid
from urllib.parse import urlparse

import pytest

from kaiwen_gateway.modules.events import (
    EVENTS_PROTOCOL_VERSION,
    DurableEventRecord,
    EventEnvelope,
    EventsBackendUnavailableError,
    InMemoryDurableStreamTelemetrySink,
)
from kaiwen_gateway.modules.events.redis import (
    RedisClientConfig,
    RedisStreamsConfig,
    RedisStreamsDurableStore,
    StreamRecoveryWorker,
    create_async_redis_client,
)

pytestmark = pytest.mark.skipif(
    not os.environ.get("KAIWEN_TEST_REDIS_URL"),
    reason="Opt-in live Redis E4 integration tests require KAIWEN_TEST_REDIS_URL",
)


def _prefix() -> str:
    return f"kwe4{uuid.uuid4().hex[:10]}"


def _redis_url() -> str:
    return os.environ["KAIWEN_TEST_REDIS_URL"]


def _env(
    *,
    event_id: str,
    tenant_id: str = "tenant:e4:a",
    topic: str = "orders.created",
    payload: dict[str, object] | None = None,
) -> EventEnvelope:
    return EventEnvelope(
        protocol_version=EVENTS_PROTOCOL_VERSION,
        event_id=event_id,
        event_type="order.created",
        topic=topic,
        timestamp="2026-11-18T14:00:00Z",
        tenant_id=tenant_id,
        principal_id="svc-orders",
        payload=payload if payload is not None else {"seq": event_id},
        correlation_id=f"corr-{event_id}",
    )


class _TcpChaosProxy:
    """Localhost-only async TCP proxy to inject real Redis disconnects and recovery."""

    def __init__(self, target_host: str, target_port: int) -> None:
        self._target_host = target_host
        self._target_port = target_port
        self._server: asyncio.Server | None = None
        self._accepting = True
        self._active_writers: set[asyncio.StreamWriter] = set()
        self._tasks: set[asyncio.Task[None]] = set()
        self.port: int = 0

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle_client, "127.0.0.1", 0)
        self.port = int(self._server.sockets[0].getsockname()[1])

    async def _handle_client(
        self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter
    ) -> None:
        if not self._accepting:
            client_writer.close()
            return
        try:
            upstream_reader, upstream_writer = await asyncio.open_connection(
                self._target_host, self._target_port
            )
        except Exception:
            client_writer.close()
            return
        self._active_writers.add(client_writer)
        self._active_writers.add(upstream_writer)

        async def pipe(src: asyncio.StreamReader, dst: asyncio.StreamWriter) -> None:
            try:
                while True:
                    chunk = await src.read(4096)
                    if not chunk:
                        break
                    dst.write(chunk)
                    await dst.drain()
            except Exception:
                pass
            finally:
                dst.close()

        t1 = asyncio.create_task(pipe(client_reader, upstream_writer))
        t2 = asyncio.create_task(pipe(upstream_reader, client_writer))
        self._tasks.update((t1, t2))
        await asyncio.gather(t1, t2, return_exceptions=True)
        self._active_writers.discard(client_writer)
        self._active_writers.discard(upstream_writer)
        self._tasks.discard(t1)
        self._tasks.discard(t2)

    async def drop_and_reject(self) -> None:
        self._accepting = False
        for writer in list(self._active_writers):
            writer.close()
        await asyncio.sleep(0.05)

    def resume(self) -> None:
        self._accepting = True

    async def stop(self) -> None:
        self._accepting = False
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        for writer in list(self._active_writers):
            writer.close()
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)


def test_live_streams_a_b_c_d_e_f_append_group_ack_recovery_competition_isolation_idempotency() -> (
    None
):
    """TESTS A, B, C, D, E, F against real Redis."""

    async def scenario() -> None:
        prefix = _prefix()
        cfg = RedisStreamsConfig(
            redis=RedisClientConfig(url=_redis_url(), key_prefix=prefix),
            claim_min_idle_ms=40,
            max_delivery_attempts=5,
        )
        store_a = RedisStreamsDurableStore(cfg)
        store_b = RedisStreamsDurableStore(cfg)
        await asyncio.wait_for(store_a.startup(), timeout=5.0)
        await asyncio.wait_for(store_b.startup(), timeout=5.0)
        try:
            # TEST A & B: Append -> read_group (no auto-ack) -> explicit ack
            eid_1 = await store_a.append(_env(event_id="evt-a1", tenant_id="tenant:e4:a"))
            recs = await store_a.read_group(
                tenant_id="tenant:e4:a",
                topic="orders.created",
                group="g1",
                consumer="c-1",
                count=10,
                block_ms=0,
            )
            assert len(recs) == 1
            assert recs[0].sequence_id == eid_1
            assert recs[0].envelope.event_id == "evt-a1"
            pend_before_ack = await store_a.pending_summary(
                tenant_id="tenant:e4:a", topic="orders.created", group="g1"
            )
            assert pend_before_ack.pending_count == 1
            assert (
                await store_a.ack(
                    tenant_id="tenant:e4:a",
                    topic="orders.created",
                    group="g1",
                    record_ids=[eid_1],
                )
                == 1
            )
            pend_after_ack = await store_a.pending_summary(
                tenant_id="tenant:e4:a", topic="orders.created", group="g1"
            )
            assert pend_after_ack.pending_count == 0

            # TEST C & F: Crash before ACK -> c-2 reclaims via XPENDING/XAUTOCLAIM; idempotent event_id
            eid_crash = await store_a.append(_env(event_id="evt-crash", tenant_id="tenant:e4:a"))
            crashed_read = await store_a.read_group(
                tenant_id="tenant:e4:a",
                topic="orders.created",
                group="g1",
                consumer="c-dead",
                count=10,
                block_ms=0,
            )
            assert len(crashed_read) == 1
            await asyncio.sleep(0.08)
            rec_res = await store_b.reclaim_pending(
                tenant_id="tenant:e4:a",
                topic="orders.created",
                group="g1",
                consumer="c-alive",
                min_idle_ms=40,
            )
            assert len(rec_res.reclaimed) == 1
            assert rec_res.reclaimed[0].sequence_id == eid_crash
            assert rec_res.reclaimed[0].envelope.event_id == "evt-crash"
            # Idempotent consumer tracks processed event_ids
            seen_event_ids: set[str] = {rec_res.reclaimed[0].envelope.event_id}
            # Duplicate event_id appended later is detected by consumer idempotency
            eid_dup = await store_a.append(_env(event_id="evt-crash", tenant_id="tenant:e4:a"))
            dup_read = await store_b.read_group(
                tenant_id="tenant:e4:a",
                topic="orders.created",
                group="g1",
                consumer="c-alive",
                count=10,
                block_ms=0,
            )
            assert len(dup_read) == 1
            assert dup_read[0].envelope.event_id in seen_event_ids
            await store_b.ack(
                tenant_id="tenant:e4:a",
                topic="orders.created",
                group="g1",
                record_ids=[eid_crash, eid_dup],
            )

            # TEST D: Two consumers in the same group compete for events
            for idx in range(10):
                await store_a.append(_env(event_id=f"evt-comp-{idx}", tenant_id="tenant:e4:comp"))
            r_c1, r_c2 = await asyncio.gather(
                store_a.read_group(
                    tenant_id="tenant:e4:comp",
                    topic="orders.created",
                    group="g-comp",
                    consumer="worker-1",
                    count=5,
                    block_ms=0,
                ),
                store_b.read_group(
                    tenant_id="tenant:e4:comp",
                    topic="orders.created",
                    group="g-comp",
                    consumer="worker-2",
                    count=5,
                    block_ms=0,
                ),
            )
            ids_1 = {r.envelope.event_id for r in r_c1}
            ids_2 = {r.envelope.event_id for r in r_c2}
            assert ids_1.isdisjoint(ids_2)
            assert len(ids_1 | ids_2) == 10
            await store_a.ack(
                tenant_id="tenant:e4:comp",
                topic="orders.created",
                group="g-comp",
                record_ids=[r.sequence_id for r in r_c1 if r.sequence_id is not None],
            )
            await store_b.ack(
                tenant_id="tenant:e4:comp",
                topic="orders.created",
                group="g-comp",
                record_ids=[r.sequence_id for r in r_c2 if r.sequence_id is not None],
            )

            # TEST E: Tenant isolation (Tenant B never sees Tenant A events)
            await store_a.append(_env(event_id="evt-secret-a", tenant_id="tenant:alpha:1"))
            b_recs = await store_b.read_group(
                tenant_id="tenant:beta:1",
                topic="orders.created",
                group="g-iso",
                consumer="c-beta",
                count=10,
                block_ms=0,
            )
            assert b_recs == []
        finally:
            await asyncio.wait_for(store_a.shutdown(), timeout=5.0)
            await asyncio.wait_for(store_b.shutdown(), timeout=5.0)

    asyncio.run(scenario())


def test_live_streams_g_h_k_l_poison_retention_replay_and_malformed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """TESTS G, H, K, L against real Redis."""

    async def scenario() -> None:
        prefix = _prefix()
        telemetry = InMemoryDurableStreamTelemetrySink()
        cfg = RedisStreamsConfig(
            redis=RedisClientConfig(url=_redis_url(), key_prefix=prefix),
            maxlen=5,
            approximate_trim=False,
            claim_min_idle_ms=20,
            max_delivery_attempts=2,
            max_replay_limit=4,
        )
        store = RedisStreamsDurableStore(cfg, telemetry=telemetry)
        raw_client = create_async_redis_client(cfg.redis)
        await asyncio.wait_for(store.startup(), timeout=5.0)
        try:
            # TEST G: Poison event retry limit -> DLQ + removed from PEL
            await store.append(_env(event_id="evt-poison", tenant_id="tenant:poison"))
            first = await store.read_group(
                tenant_id="tenant:poison",
                topic="orders.created",
                group="g-p",
                consumer="c-1",
                count=1,
                block_ms=0,
            )
            assert len(first) == 1
            await asyncio.sleep(0.04)
            # 2nd delivery attempt (within max_delivery_attempts=2)
            rec1 = await store.reclaim_pending(
                tenant_id="tenant:poison",
                topic="orders.created",
                group="g-p",
                consumer="c-2",
                min_idle_ms=20,
            )
            assert len(rec1.reclaimed) == 1
            assert len(rec1.dead_lettered) == 0
            await asyncio.sleep(0.04)
            # 3rd delivery attempt (> max_delivery_attempts=2) -> quarantined to DLQ
            rec2 = await store.reclaim_pending(
                tenant_id="tenant:poison",
                topic="orders.created",
                group="g-p",
                consumer="c-3",
                min_idle_ms=20,
            )
            assert len(rec2.reclaimed) == 0
            assert len(rec2.dead_lettered) == 1
            assert rec2.dead_lettered[0].event_id == "evt-poison"
            assert (
                await store.pending_summary(
                    tenant_id="tenant:poison", topic="orders.created", group="g-p"
                )
            ).pending_count == 0
            assert (
                len(
                    await store.dead_letter_entries(
                        tenant_id="tenant:poison", topic="orders.created"
                    )
                )
                == 1
            )

            # TEST H & K: Retention trimming (maxlen=5 exact) & bounded replay
            ids: list[str] = []
            for idx in range(10):
                ids.append(
                    await store.append(_env(event_id=f"evt-trim-{idx}", tenant_id="tenant:trim"))
                )
            stats = await store.stream_stats(tenant_id="tenant:trim", topic="orders.created")
            assert stats.length == 5
            replayed = await store.replay(
                tenant_id="tenant:trim",
                topic="orders.created",
                from_id=ids[0],  # Trimmed cursor!
                limit=4,
            )
            assert len(replayed) == 4
            assert telemetry.snapshot().retention_trimmed_losses >= 1

            # TEST L: Malformed serialized event quarantined without exposing payload in logs
            skey = store.stream_key("tenant:bad", "orders.created")
            await raw_client.xadd(
                skey,
                {"event_id": "evt-malformed", "envelope": '{"secret_leak":"DO_NOT_LOG", bad'},
            )
            bad_recs = await store.read_group(
                tenant_id="tenant:bad",
                topic="orders.created",
                group="g-bad",
                consumer="c-bad",
                count=5,
                block_ms=0,
            )
            assert bad_recs == []
            assert telemetry.snapshot().malformed_records >= 1
            assert (
                await store.pending_summary(
                    tenant_id="tenant:bad", topic="orders.created", group="g-bad"
                )
            ).pending_count == 0
            dlq_bad = await store.dead_letter_entries(
                tenant_id="tenant:bad", topic="orders.created"
            )
            assert len(dlq_bad) == 1
            assert dlq_bad[0].reason == "malformed_or_tenant_mismatch"
        finally:
            await raw_client.aclose()
            await asyncio.wait_for(store.shutdown(), timeout=5.0)

    with caplog.at_level(logging.DEBUG):
        asyncio.run(scenario())
    assert "DO_NOT_LOG" not in caplog.text


def test_live_streams_i_j_disconnect_recovery_and_shutdown_during_processing() -> None:
    """TESTS I & J against real Redis + _TcpChaosProxy."""

    async def scenario() -> None:
        parsed = urlparse(_redis_url())
        proxy = _TcpChaosProxy(parsed.hostname or "127.0.0.1", parsed.port or 6379)
        await proxy.start()
        prefix = _prefix()
        cfg = RedisStreamsConfig(
            redis=RedisClientConfig(
                url=f"redis://127.0.0.1:{proxy.port}/0",
                key_prefix=prefix,
                connect_timeout_seconds=0.4,
                socket_timeout_seconds=0.4,
            ),
            claim_min_idle_ms=20,
            recovery_interval_seconds=0.05,
        )
        store = RedisStreamsDurableStore(cfg)
        await asyncio.wait_for(store.startup(), timeout=5.0)

        processed: list[str] = []

        async def _handler(rec: DurableEventRecord) -> None:
            processed.append(rec.envelope.event_id)

        worker = StreamRecoveryWorker(
            store=store,
            tenant_id="tenant:net",
            topic="orders.created",
            group="g-net",
            consumer="c-rec",
            handler=_handler,
        )
        try:
            # Append and leave pending on crashed consumer
            await store.append(_env(event_id="evt-net-1", tenant_id="tenant:net"))
            await store.read_group(
                tenant_id="tenant:net",
                topic="orders.created",
                group="g-net",
                consumer="c-dead",
                count=1,
                block_ms=0,
            )

            # TEST I: Disconnect fails closed, then reconnect recovers
            await proxy.drop_and_reject()
            with pytest.raises(EventsBackendUnavailableError):
                await store.append(_env(event_id="evt-fail", tenant_id="tenant:net"))
            healthy_down = store.is_healthy
            assert healthy_down is False

            proxy.resume()
            await asyncio.sleep(0.05)
            worker.start()
            for _ in range(40):
                if "evt-net-1" in processed:
                    break
                await asyncio.sleep(0.05)
            assert "evt-net-1" in processed
            assert store.is_healthy is True

            # TEST J: Clean shutdown during active recovery worker
            await asyncio.wait_for(worker.stop(), timeout=5.0)
            running_after = worker.is_running
            assert running_after is False
        finally:
            await asyncio.wait_for(worker.stop(), timeout=5.0)
            await asyncio.wait_for(store.shutdown(), timeout=5.0)
            await proxy.stop()

    asyncio.run(scenario())
