"""Opt-in live-Redis real-process combined beta acceptance tests (Phases 3, 4, 5).

Skipped unless ``KAIWEN_TEST_REDIS_URL`` is set to a reachable Redis server.
Exercises independent Gateway OS processes (Uvicorn), real ``websockets`` clients,
concurrent Voice + Events + Notifications workloads, TCP-proxy Redis outage isolation,
and Redis Streams crash/redelivery recovery.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from urllib.parse import urlparse

import pytest
import websockets

from kaiwen_gateway.modules.events.redis import (
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
from kaiwen_gateway.transports.synthetic import (
    EVENT_CALL_END,
    EVENT_CALL_ENDED,
    EVENT_CALL_INTERRUPT,
    EVENT_CALL_INTERRUPTED,
    EVENT_CALL_START,
    EVENT_CALL_STARTED,
    EVENT_SESSION_READY,
    PCM16LE_FRAME_BYTES,
    SYNTHETIC_MEDIA_WS_ENDPOINT_PATH,
    SyntheticTokenVerifier,
)
from tests.combined_beta_process_app import VOICE_SECRET

pytestmark = pytest.mark.skipif(
    not os.environ.get("KAIWEN_TEST_REDIS_URL"),
    reason="Opt-in live Redis combined beta tests require KAIWEN_TEST_REDIS_URL",
)


def _get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _http_get_json(url: str, timeout: float = 2.0) -> tuple[int, dict[str, Any]]:
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def _http_post_json(url: str, payload: dict[str, Any], timeout: float = 5.0) -> dict[str, Any]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return dict(json.loads(resp.read().decode("utf-8")))


def _start_gateway_process(
    *,
    port: int,
    instance_id: str,
    redis_url: str,
    key_prefix: str,
    enable_worker: bool = True,
) -> subprocess.Popen[str]:
    env = os.environ.copy()
    env["KAIWEN_TEST_REDIS_URL"] = redis_url
    env["KAIWEN_BETA_INSTANCE_ID"] = instance_id
    env["KAIWEN_BETA_KEY_PREFIX"] = key_prefix
    env["KAIWEN_BETA_ENABLE_WORKER"] = "1" if enable_worker else "0"
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "tests.combined_beta_process_app:build_process_app",
            "--factory",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    deadline = time.monotonic() + 20.0
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            out, err = proc.communicate(timeout=2.0)
            raise RuntimeError(
                f"Gateway process {instance_id} exited early ({proc.returncode}): {out}\n{err}"
            )
        try:
            status, body = _http_get_json(f"http://127.0.0.1:{port}/ready", timeout=1.0)
            if status == 200 and body.get("status") == "ready":
                return proc
        except Exception:
            time.sleep(0.15)
    proc.terminate()
    try:
        out, err = proc.communicate(timeout=5.0)
    except subprocess.TimeoutExpired:
        proc.kill()
        out, err = proc.communicate(timeout=5.0)
    raise TimeoutError(
        f"Gateway process {instance_id} failed to become ready on port {port}: {out}\n{err}"
    )


def _stop_gateway_process(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=8.0)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5.0)


def _issue_voice_token(tenant_id: str, call_id: str) -> str:
    verifier = SyntheticTokenVerifier(secret_key=VOICE_SECRET, enable_replay_protection=True)
    return verifier.issue_token(
        provider_id="synthetic",
        tenant_id=tenant_id,
        call_id=call_id,
        allowed_scopes=("voice.stream",),
        ttl_seconds=120.0,
    )


def _percentile(sorted_vals: list[float], pct: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, max(0, int(round((pct / 100.0) * (len(sorted_vals) - 1)))))
    return sorted_vals[idx]


class _RestartableTcpProxy:
    """In-test TCP proxy allowing clean Redis outage & recovery simulation."""

    def __init__(self, listen_port: int, target_host: str, target_port: int) -> None:
        self._listen_port = listen_port
        self._target_host = target_host
        self._target_port = target_port
        self._server: asyncio.Server | None = None
        self._active_writers: set[asyncio.StreamWriter] = set()
        self._tasks: set[asyncio.Task[None]] = set()

    async def _pipe(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                data = await reader.read(65_536)
                if not data:
                    break
                writer.write(data)
                await writer.drain()
        except Exception:
            pass
        finally:
            try:
                writer.close()
            except Exception:
                pass

    async def _handle_client(
        self, client_r: asyncio.StreamReader, client_w: asyncio.StreamWriter
    ) -> None:
        self._active_writers.add(client_w)
        try:
            upstream_r, upstream_w = await asyncio.open_connection(
                self._target_host, self._target_port
            )
        except Exception:
            client_w.close()
            self._active_writers.discard(client_w)
            return
        self._active_writers.add(upstream_w)
        t1 = asyncio.create_task(self._pipe(client_r, upstream_w))
        t2 = asyncio.create_task(self._pipe(upstream_r, client_w))
        self._tasks.update((t1, t2))
        try:
            await asyncio.gather(t1, t2, return_exceptions=True)
        finally:
            self._tasks.discard(t1)
            self._tasks.discard(t2)
            self._active_writers.discard(client_w)
            self._active_writers.discard(upstream_w)

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_client, "127.0.0.1", self._listen_port
        )

    async def drop_all(self) -> None:
        server = self._server
        self._server = None
        if server is not None:
            server.close()
        for writer in list(self._active_writers):
            try:
                writer.close()
            except Exception:
                pass
        self._active_writers.clear()
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        if server is not None:
            try:
                await asyncio.wait_for(server.wait_closed(), timeout=2.0)
            except TimeoutError:
                pass


@asynccontextmanager
async def _redis_cleanup(redis_url: str, key_prefix: str) -> AsyncIterator[None]:
    try:
        yield
    finally:
        client = create_async_redis_client(RedisClientConfig(url=redis_url))
        try:
            keys = [k async for k in client.scan_iter(match=f"{key_prefix}*")]
            if keys:
                await client.delete(*keys)
        finally:
            await client.aclose()


# ---------------------------------------------------------------------------
# Phase 3 & Phase 4: Real Multi-Process Cross-Instance + Concurrent Workloads
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_phase3_and_phase4_real_multiprocess_concurrent_workloads() -> None:
    redis_url = os.environ["KAIWEN_TEST_REDIS_URL"]
    run_id = uuid.uuid4().hex[:8]
    key_prefix = f"kwbeta{run_id}"
    port1 = _get_free_port()
    port2 = _get_free_port()

    async with _redis_cleanup(redis_url, key_prefix):
        proc1 = _start_gateway_process(
            port=port1,
            instance_id=f"gw-1-{run_id}",
            redis_url=redis_url,
            key_prefix=key_prefix,
        )
        proc2 = _start_gateway_process(
            port=port2,
            instance_id=f"gw-2-{run_id}",
            redis_url=redis_url,
            key_prefix=key_prefix,
        )
        proc3: subprocess.Popen[str] | None = None
        try:
            u1_topic = derive_recipient_topic("tenant-alpha", "user-1")
            u2_topic = derive_recipient_topic("tenant-alpha", "user-2")

            # Connect real WebSocket clients to independent OS processes
            async with (
                websockets.connect(
                    f"ws://127.0.0.1:{port2}/ws/events/v1",
                    additional_headers={"Authorization": "Bearer tok-t1-u1"},
                ) as ws_gw2_u1,
                websockets.connect(
                    f"ws://127.0.0.1:{port1}/ws/events/v1",
                    additional_headers={"Authorization": "Bearer tok-t2-u1"},
                ) as ws_gw1_beta,
            ):
                assert json.loads(await ws_gw2_u1.recv())["type"] == "session.ready"
                assert json.loads(await ws_gw1_beta.recv())["type"] == "session.ready"

                # Mandatory recipient isolation: user-1 denied subscribing to user-2 and wildcard
                await ws_gw2_u1.send(
                    json.dumps(
                        {
                            "type": "events.subscribe",
                            "protocol_version": "v1",
                            "topic": u2_topic,
                            "request_id": "deny-cross",
                        }
                    )
                )
                deny_frame = json.loads(await ws_gw2_u1.recv())
                assert deny_frame["type"] == "events.error"
                assert deny_frame["code"] == "FORBIDDEN_SUBSCRIPTION"

                await ws_gw2_u1.send(
                    json.dumps(
                        {
                            "type": "events.subscribe",
                            "protocol_version": "v1",
                            "topic_pattern": "notifications.r.*",
                            "request_id": "deny-wild",
                        }
                    )
                )
                deny_wild = json.loads(await ws_gw2_u1.recv())
                assert deny_wild["type"] == "events.error"
                assert deny_wild["code"] == "FORBIDDEN_SUBSCRIPTION"

                # Authorized subscriptions
                await ws_gw2_u1.send(
                    json.dumps(
                        {
                            "type": "events.subscribe",
                            "protocol_version": "v1",
                            "topic": u1_topic,
                            "request_id": "sub-u1",
                        }
                    )
                )
                assert json.loads(await ws_gw2_u1.recv())["type"] == "events.subscribed"

                await ws_gw2_u1.send(
                    json.dumps(
                        {
                            "type": "events.subscribe",
                            "protocol_version": "v1",
                            "topic": "calls.status",
                            "request_id": "sub-evt",
                        }
                    )
                )
                assert json.loads(await ws_gw2_u1.recv())["type"] == "events.subscribed"

                await ws_gw1_beta.send(
                    json.dumps(
                        {
                            "type": "events.subscribe",
                            "protocol_version": "v1",
                            "topic": "calls.status",
                            "request_id": "sub-beta",
                        }
                    )
                )
                assert json.loads(await ws_gw1_beta.recv())["type"] == "events.subscribed"

                # 1) Cross-instance Events delivery (gw-1 publishes -> gw-2 client receives; tenant-beta isolated)
                pub_evt = await asyncio.to_thread(
                    _http_post_json,
                    f"http://127.0.0.1:{port1}/_test/publish-event",
                    {
                        "event_id": f"evt-{uuid.uuid4().hex[:8]}",
                        "event_type": "call.progress",
                        "tenant_id": "tenant-alpha",
                        "topic": "calls.status",
                        "payload": {"step": "cross-instance"},
                    },
                )
                assert pub_evt["accepted"] is True
                delivered_evt = json.loads(await asyncio.wait_for(ws_gw2_u1.recv(), timeout=5.0))
                assert delivered_evt["type"] == "events.delivery"
                assert delivered_evt["event"]["payload"]["step"] == "cross-instance"

                # Verify tenant-beta did NOT receive tenant-alpha's event
                with pytest.raises(asyncio.TimeoutError):
                    await asyncio.wait_for(ws_gw1_beta.recv(), timeout=0.25)

                # 2) Concurrent Voice + Events + Durable Notifications across gw-1 and gw-2
                voice_tok_1 = _issue_voice_token("tenant-alpha", "call-mp-1")
                voice_tok_2 = _issue_voice_token("tenant-alpha", "call-mp-2")

                loop_lags_ms: list[float] = []
                lag_stop = asyncio.Event()

                async def _probe_loop_lag() -> None:
                    while not lag_stop.is_set():
                        t0 = time.perf_counter()
                        await asyncio.sleep(0.01)
                        lag_ms = max(0.0, (time.perf_counter() - t0 - 0.01) * 1000.0)
                        loop_lags_ms.append(lag_ms)

                lag_task = asyncio.create_task(_probe_loop_lag())

                voice_latencies_ms: list[float] = []
                notif_latencies_ms: list[float] = []
                received_notif_ids: list[str] = []
                sent_times: dict[str, float] = {}

                async def _run_voice_session(port: int, call_id: str, token: str) -> None:
                    uri = f"ws://127.0.0.1:{port}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
                    async with websockets.connect(
                        uri, additional_headers={"Authorization": f"Bearer {token}"}
                    ) as vws:
                        assert json.loads(await vws.recv())["event"] == EVENT_SESSION_READY
                        await vws.send(json.dumps({"event": EVENT_CALL_START, "call_id": call_id}))
                        assert json.loads(await vws.recv())["event"] == EVENT_CALL_STARTED

                        for i in range(5):
                            frame = bytes([0x10 + i]) * PCM16LE_FRAME_BYTES
                            t0 = time.perf_counter()
                            await vws.send(frame)
                            echo = await vws.recv()
                            voice_latencies_ms.append((time.perf_counter() - t0) * 1000.0)
                            assert echo == frame

                        # Barge-in interruption mid-call
                        await vws.send(
                            json.dumps(
                                {
                                    "event": EVENT_CALL_INTERRUPT,
                                    "call_id": call_id,
                                    "turn_id": 1,
                                    "request_id": f"req-{call_id}",
                                }
                            )
                        )
                        interrupted = json.loads(await vws.recv())
                        assert interrupted["event"] == EVENT_CALL_INTERRUPTED
                        assert interrupted["new_turn_id"] == 2

                        # Turn 2 frame
                        frame2 = b"\x42" * PCM16LE_FRAME_BYTES
                        await vws.send(frame2)
                        assert await vws.recv() == frame2

                        await vws.send(json.dumps({"event": EVENT_CALL_END, "call_id": call_id}))
                        assert json.loads(await vws.recv())["event"] == EVENT_CALL_ENDED

                async def _publish_notifications(count: int) -> None:
                    for idx in range(count):
                        nid = generate_notification_id()
                        sent_times[nid] = time.perf_counter()
                        res = await asyncio.to_thread(
                            _http_post_json,
                            f"http://127.0.0.1:{port1}/_test/publish-notification",
                            {
                                "notification_id": nid,
                                "tenant_id": "tenant-alpha",
                                "recipient_id": "user-1",
                                "payload": {"seq": idx},
                            },
                        )
                        assert res["status"] == "durable_accepted"

                async def _consume_notifications(expected: int) -> None:
                    while len(received_notif_ids) < expected:
                        raw = await asyncio.wait_for(ws_gw2_u1.recv(), timeout=15.0)
                        msg = json.loads(raw)
                        if msg.get("type") == "events.delivery":
                            nid = msg["event"]["payload"]["notification_id"]
                            received_notif_ids.append(nid)
                            if nid in sent_times:
                                notif_latencies_ms.append(
                                    (time.perf_counter() - sent_times[nid]) * 1000.0
                                )

                batch_start = time.perf_counter()
                await asyncio.gather(
                    _run_voice_session(port1, "call-mp-1", voice_tok_1),
                    _run_voice_session(port2, "call-mp-2", voice_tok_2),
                    _publish_notifications(30),
                    _consume_notifications(30),
                )
                batch_elapsed = time.perf_counter() - batch_start
                lag_stop.set()
                await lag_task

                assert len(received_notif_ids) == 30
                assert len(set(received_notif_ids)) == 30

                # Collect process stats & Redis Stream pending/lag
                stats1 = _http_get_json(f"http://127.0.0.1:{port1}/_test/stats")[1]
                stats2 = _http_get_json(f"http://127.0.0.1:{port2}/_test/stats")[1]
                assert stats1["worker_acked"] + stats2["worker_acked"] == 30
                assert stats1["worker_processed"] + stats2["worker_processed"] == 30

                sorted_n = sorted(notif_latencies_ms)
                sorted_v = sorted(voice_latencies_ms)
                sorted_l = sorted(loop_lags_ms)
                print(
                    f"\n[COMBINED_BETA_METRICS] batch_notifs=30 elapsed_s={batch_elapsed:.3f} "
                    f"throughput_notif_s={30 / batch_elapsed:.2f} "
                    f"notif_p50_ms={_percentile(sorted_n, 50):.2f} "
                    f"notif_p95_ms={_percentile(sorted_n, 95):.2f} "
                    f"notif_p99_ms={_percentile(sorted_n, 99):.2f} "
                    f"voice_echo_p50_ms={_percentile(sorted_v, 50):.2f} "
                    f"voice_echo_p95_ms={_percentile(sorted_v, 95):.2f} "
                    f"loop_lag_p95_ms={_percentile(sorted_l, 95):.2f} "
                    f"gw1_rss_mb={stats1['rss_bytes'] / (1024 * 1024):.1f} "
                    f"gw2_rss_mb={stats2['rss_bytes'] / (1024 * 1024):.1f}"
                )

            # 3) Lifecycle during process termination & replacement (gw-2 -> gw-3)
            _stop_gateway_process(proc2)
            port3 = _get_free_port()
            proc3 = _start_gateway_process(
                port=port3,
                instance_id=f"gw-3-{run_id}",
                redis_url=redis_url,
                key_prefix=key_prefix,
            )
            async with websockets.connect(
                f"ws://127.0.0.1:{port3}/ws/events/v1",
                additional_headers={"Authorization": "Bearer tok-t1-u1"},
            ) as ws_gw3:
                assert json.loads(await ws_gw3.recv())["type"] == "session.ready"
                await ws_gw3.send(
                    json.dumps(
                        {
                            "type": "events.subscribe",
                            "protocol_version": "v1",
                            "topic": u1_topic,
                            "request_id": "sub-gw3",
                        }
                    )
                )
                assert json.loads(await ws_gw3.recv())["type"] == "events.subscribed"
                nid_after = generate_notification_id()
                _http_post_json(
                    f"http://127.0.0.1:{port1}/_test/publish-notification",
                    {
                        "notification_id": nid_after,
                        "tenant_id": "tenant-alpha",
                        "recipient_id": "user-1",
                        "payload": {"after_restart": True},
                    },
                )
                msg_after = json.loads(await asyncio.wait_for(ws_gw3.recv(), timeout=10.0))
                assert msg_after["event"]["payload"]["notification_id"] == nid_after
        finally:
            _stop_gateway_process(proc1)
            _stop_gateway_process(proc2)
            if proc3 is not None:
                _stop_gateway_process(proc3)


# ---------------------------------------------------------------------------
# Phase 5: Live Redis Disconnection During Active Voice + Recovery + Worker Crash
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_phase5_redis_disconnection_isolates_active_voice_and_recovers() -> None:
    real_redis_url = os.environ["KAIWEN_TEST_REDIS_URL"]
    parsed = urlparse(real_redis_url)
    target_host = parsed.hostname or "127.0.0.1"
    target_port = parsed.port or 6379
    proxy_port = _get_free_port()
    proxy_url = f"redis://127.0.0.1:{proxy_port}/0"

    run_id = uuid.uuid4().hex[:8]
    key_prefix = f"kwbetaout{run_id}"
    gw_port = _get_free_port()

    proxy = _RestartableTcpProxy(proxy_port, target_host, target_port)
    await proxy.start()
    proc: subprocess.Popen[str] | None = None

    async with _redis_cleanup(real_redis_url, key_prefix):
        try:
            proc = await asyncio.to_thread(
                _start_gateway_process,
                port=gw_port,
                instance_id=f"gw-outage-{run_id}",
                redis_url=proxy_url,
                key_prefix=key_prefix,
                enable_worker=False,
            )
            voice_tok = _issue_voice_token("tenant-alpha", "call-outage-1")
            uri = f"ws://127.0.0.1:{gw_port}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"

            async with (
                websockets.connect(
                    uri, additional_headers={"Authorization": f"Bearer {voice_tok}"}
                ) as vws,
                websockets.connect(
                    f"ws://127.0.0.1:{gw_port}/ws/events/v1",
                    additional_headers={"Authorization": "Bearer tok-t1-u1"},
                ) as ews,
            ):
                assert json.loads(await vws.recv())["event"] == EVENT_SESSION_READY
                assert json.loads(await ews.recv())["type"] == "session.ready"
                await ews.send(
                    json.dumps(
                        {
                            "type": "events.subscribe",
                            "protocol_version": "v1",
                            "topic": "calls.status",
                            "request_id": "sub-outage",
                        }
                    )
                )
                assert json.loads(await ews.recv())["type"] == "events.subscribed"

                await vws.send(json.dumps({"event": EVENT_CALL_START, "call_id": "call-outage-1"}))
                assert json.loads(await vws.recv())["event"] == EVENT_CALL_STARTED

                # Drop Redis TCP connections while Voice call and Events subscriber are active
                await proxy.drop_all()

                # Wait for Events broker health to transition to unhealthy (/ready -> 503)
                deadline = time.monotonic() + 10.0
                ready_status = 200
                ready_body: dict[str, Any] = {}
                while time.monotonic() < deadline:
                    ready_status, ready_body = await asyncio.to_thread(
                        _http_get_json, f"http://127.0.0.1:{gw_port}/ready"
                    )
                    if ready_status == 503:
                        break
                    await asyncio.sleep(0.1)

                assert ready_status == 503
                assert ready_body["reason"] == "events_backend_unavailable"

                # Active Voice session MUST remain functional during Redis outage
                for i in range(4):
                    frame = bytes([0x30 + i]) * PCM16LE_FRAME_BYTES
                    await vws.send(frame)
                    echo = await vws.recv()
                    assert echo == frame

                await vws.send(
                    json.dumps(
                        {
                            "event": EVENT_CALL_INTERRUPT,
                            "call_id": "call-outage-1",
                            "turn_id": 1,
                            "request_id": "req-outage-int",
                        }
                    )
                )
                interrupted = json.loads(await vws.recv())
                assert interrupted["event"] == EVENT_CALL_INTERRUPTED
                assert interrupted["new_turn_id"] == 2

                # Restore Redis TCP proxy and verify /ready recovers to 200 and Events resumes
                await proxy.start()
                rec_deadline = time.monotonic() + 12.0
                while time.monotonic() < rec_deadline:
                    ready_status, _ = await asyncio.to_thread(
                        _http_get_json, f"http://127.0.0.1:{gw_port}/ready"
                    )
                    if ready_status == 200:
                        break
                    await asyncio.sleep(0.15)
                assert ready_status == 200

                pub_res = await asyncio.to_thread(
                    _http_post_json,
                    f"http://127.0.0.1:{gw_port}/_test/publish-event",
                    {
                        "event_id": f"evt-rec-{uuid.uuid4().hex[:8]}",
                        "event_type": "call.recovered",
                        "tenant_id": "tenant-alpha",
                        "topic": "calls.status",
                        "payload": {"recovered": True},
                    },
                )
                assert pub_res["accepted"] is True
                rec_evt = json.loads(await asyncio.wait_for(ews.recv(), timeout=5.0))
                assert rec_evt["type"] == "events.delivery"
                assert rec_evt["event"]["payload"]["recovered"] is True

                await vws.send(json.dumps({"event": EVENT_CALL_END, "call_id": "call-outage-1"}))
                assert json.loads(await vws.recv())["event"] == EVENT_CALL_ENDED
        finally:
            if proc is not None:
                _stop_gateway_process(proc)
            await proxy.drop_all()


@pytest.mark.anyio
async def test_phase5_worker_crash_before_and_after_dispatch_before_xack() -> None:
    redis_url = os.environ["KAIWEN_TEST_REDIS_URL"]
    run_id = uuid.uuid4().hex[:8]
    key_prefix = f"kwbetacrash{run_id}"
    group_name = f"{key_prefix}-group"
    worker_cfg = NotificationsDurableConfig(
        tenant_id="tenant-alpha",
        group=group_name,
        consumer="recovery-worker",
        poll_interval_seconds=0.02,
        recovery_interval_seconds=0.05,
        claim_min_idle_ms=50,
    )

    redis_cfg = RedisClientConfig(url=redis_url, key_prefix=key_prefix)
    topic_auth = NotificationsTopicAuthorizer()
    broker = RedisPubSubEventBroker(
        RedisPubSubBrokerConfig(redis=redis_cfg),
        topic_authorizer=topic_auth,
    )
    pref_store = InMemoryNotificationPreferenceStore()
    notif_telemetry = InMemoryNotificationsTelemetrySink()
    durable_telemetry = InMemoryNotificationsDurableTelemetrySink()

    class _AllowAuth:
        async def authorize_publish(
            self, *, producer: Any, notification: NotificationEnvelope
        ) -> None:
            return None

    store = RedisStreamsDurableStore(
        RedisStreamsConfig(
            redis=redis_cfg,
            claim_min_idle_ms=50,
            max_delivery_attempts=5,
        )
    )
    publisher = NotificationsDurablePublisher(
        durable_store=store,
        producer_authorizer=_AllowAuth(),
        telemetry=notif_telemetry,
        durable_telemetry=durable_telemetry,
    )

    async with _redis_cleanup(redis_url, key_prefix):
        await broker.startup()
        await store.startup()
        try:
            # Enqueue 2 notifications:
            # 1) Consumed via XREADGROUP and crashed BEFORE broker dispatch and BEFORE XACK
            # 2) Consumed via XREADGROUP, dispatched to broker, and crashed BEFORE XACK
            n1 = NotificationEnvelope(
                notification_id=generate_notification_id(),
                schema_version=NOTIFICATIONS_SCHEMA_VERSION,
                tenant_id="tenant-alpha",
                recipient_id="user-1",
                category="system.alert",
                notification_type="alert.crash1",
                priority=NotificationPriority.HIGH,
                payload={"case": "crash-before-dispatch"},
                created_at="2026-11-18T20:00:00Z",
            )
            n2 = NotificationEnvelope(
                notification_id=generate_notification_id(),
                schema_version=NOTIFICATIONS_SCHEMA_VERSION,
                tenant_id="tenant-alpha",
                recipient_id="user-1",
                category="system.alert",
                notification_type="alert.crash2",
                priority=NotificationPriority.HIGH,
                payload={"case": "crash-after-dispatch-before-xack"},
                created_at="2026-11-18T20:00:00Z",
            )
            r1 = await publisher.publish(n1)
            r2 = await publisher.publish(n2)
            assert r1.sequence_id is not None
            assert r2.sequence_id is not None

            # Simulate crashed worker consuming both without XACK
            crashed_store = RedisStreamsDurableStore(
                RedisStreamsConfig(
                    redis=redis_cfg,
                    claim_min_idle_ms=50,
                )
            )
            await crashed_store.startup()
            try:
                records = await crashed_store.read_group(
                    tenant_id="tenant-alpha",
                    topic=worker_cfg.topic,
                    group=group_name,
                    consumer="crashed-worker",
                    count=10,
                    block_ms=500,
                )
                assert len(records) == 2
                # Dispatch record 2 to broker to simulate crash after dispatch but before XACK
                await broker.publish(records[1].envelope)
            finally:
                await crashed_store.shutdown()

            # Wait past claim_min_idle_ms=50 and recover with NotificationsDurableWorker
            await asyncio.sleep(0.15)
            worker = NotificationsDurableWorker(
                config=worker_cfg,
                durable_store=store,
                broker=broker,
                preference_port=pref_store,
                telemetry=notif_telemetry,
                durable_telemetry=durable_telemetry,
            )
            worker.start()
            try:
                deadline = time.monotonic() + 5.0
                while durable_telemetry.snapshot().worker_acked < 2 and time.monotonic() < deadline:
                    await asyncio.sleep(0.03)
                assert durable_telemetry.snapshot().worker_reclaimed == 2
                assert durable_telemetry.snapshot().worker_acked == 2
                summary = await store.pending_summary(
                    tenant_id="tenant-alpha",
                    topic=worker_cfg.topic,
                    group=group_name,
                )
                assert summary.pending_count == 0
            finally:
                await worker.stop()
        finally:
            await store.shutdown()
            await broker.shutdown()


# ---------------------------------------------------------------------------
# R1 Evidence Closure: Competing Workers, 20ms Paced Voice + In-Flight Interrupt,
# Server-Side Loop Lag, and Idle Redis Readiness Characterization
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_r1_phase2_competing_workers_individual_processing_and_bounded_loops() -> None:
    """Deterministically prove both independent workers process from one consumer group."""
    redis_url = os.environ["KAIWEN_TEST_REDIS_URL"]
    run_id = uuid.uuid4().hex[:8]
    key_prefix = f"kwr1w{run_id}"
    group_name = f"{key_prefix}-group"
    id1 = f"gw-1-{run_id}"
    id2 = f"gw-2-{run_id}"
    port1 = _get_free_port()
    port2 = _get_free_port()

    proc1: subprocess.Popen[str] | None = None
    proc2: subprocess.Popen[str] | None = None
    proc1_restarted: subprocess.Popen[str] | None = None

    async with _redis_cleanup(redis_url, key_prefix):
        try:
            # Phase A: Only gw-1 runs -> deterministically processes 5 notifications
            proc1 = _start_gateway_process(
                port=port1,
                instance_id=id1,
                redis_url=redis_url,
                key_prefix=key_prefix,
            )
            for idx in range(5):
                res = await asyncio.to_thread(
                    _http_post_json,
                    f"http://127.0.0.1:{port1}/_test/publish-notification",
                    {
                        "notification_id": generate_notification_id(),
                        "tenant_id": "tenant-alpha",
                        "recipient_id": "user-1",
                        "payload": {"phase": "A", "idx": idx},
                    },
                )
                assert res["status"] == "durable_accepted"

            deadline = time.monotonic() + 8.0
            stats1: dict[str, Any] = {}
            while time.monotonic() < deadline:
                stats1 = _http_get_json(f"http://127.0.0.1:{port1}/_test/stats")[1]
                if stats1["worker_acked"] == 5:
                    break
                await asyncio.sleep(0.05)
            assert stats1["worker_running"] is True
            assert stats1["worker_consumer"] == id1
            assert stats1["worker_acked"] == 5
            assert stats1["worker_task_count"] == 2

            # Phase B: Start gw-2, stop gw-1 -> gw-2 deterministically processes 5 notifications
            proc2 = _start_gateway_process(
                port=port2,
                instance_id=id2,
                redis_url=redis_url,
                key_prefix=key_prefix,
            )
            _stop_gateway_process(proc1)
            proc1 = None

            for idx in range(5):
                res = await asyncio.to_thread(
                    _http_post_json,
                    f"http://127.0.0.1:{port2}/_test/publish-notification",
                    {
                        "notification_id": generate_notification_id(),
                        "tenant_id": "tenant-alpha",
                        "recipient_id": "user-1",
                        "payload": {"phase": "B", "idx": idx},
                    },
                )
                assert res["status"] == "durable_accepted"

            deadline = time.monotonic() + 8.0
            stats2: dict[str, Any] = {}
            while time.monotonic() < deadline:
                stats2 = _http_get_json(f"http://127.0.0.1:{port2}/_test/stats")[1]
                if stats2["worker_acked"] == 5:
                    break
                await asyncio.sleep(0.05)
            assert stats2["worker_running"] is True
            assert stats2["worker_consumer"] == id2
            assert stats2["worker_acked"] == 5
            assert stats2["worker_task_count"] == 2

            # Phase C: Both gw-1 (restarted) and gw-2 run concurrently on the same consumer group
            port1b = _get_free_port()
            proc1_restarted = _start_gateway_process(
                port=port1b,
                instance_id=id1,
                redis_url=redis_url,
                key_prefix=key_prefix,
            )
            for idx in range(20):
                res = await asyncio.to_thread(
                    _http_post_json,
                    f"http://127.0.0.1:{port1b}/_test/publish-notification",
                    {
                        "notification_id": generate_notification_id(),
                        "tenant_id": "tenant-alpha",
                        "recipient_id": "user-1",
                        "payload": {"phase": "C", "idx": idx},
                    },
                )
                assert res["status"] == "durable_accepted"

            deadline = time.monotonic() + 10.0
            s1b: dict[str, Any] = {}
            s2b: dict[str, Any] = {}
            while time.monotonic() < deadline:
                s1b = _http_get_json(f"http://127.0.0.1:{port1b}/_test/stats")[1]
                s2b = _http_get_json(f"http://127.0.0.1:{port2}/_test/stats")[1]
                if s1b["worker_acked"] + (s2b["worker_acked"] - 5) == 20:
                    break
                await asyncio.sleep(0.05)

            assert s1b["worker_acked"] + (s2b["worker_acked"] - 5) == 20
            assert s1b["worker_task_count"] == 2
            assert s2b["worker_task_count"] == 2

            # Verify Redis XINFO CONSUMERS contains both independent workers and pending == 0
            store = RedisStreamsDurableStore(
                RedisStreamsConfig(redis=RedisClientConfig(url=redis_url, key_prefix=key_prefix))
            )
            await store.startup()
            try:
                topic = NotificationsDurableConfig(tenant_id="tenant-alpha").topic
                summary = await store.pending_summary(
                    tenant_id="tenant-alpha", topic=topic, group=group_name
                )
                assert summary.pending_count == 0

                raw_client = create_async_redis_client(RedisClientConfig(url=redis_url))
                try:
                    stream_key = store.stream_key("tenant-alpha", topic)
                    consumers = await raw_client.xinfo_consumers(stream_key, group_name)
                    consumer_names = {
                        c["name"].decode("utf-8")
                        if isinstance(c["name"], bytes)
                        else str(c["name"])
                        for c in consumers
                    }
                    assert id1 in consumer_names
                    assert id2 in consumer_names
                finally:
                    await raw_client.aclose()
            finally:
                await store.shutdown()
        finally:
            if proc1 is not None:
                _stop_gateway_process(proc1)
            if proc2 is not None:
                _stop_gateway_process(proc2)
            if proc1_restarted is not None:
                _stop_gateway_process(proc1_restarted)


@pytest.mark.anyio
async def test_r1_phase3_voice_20ms_pacing_and_inflight_interruption_with_events() -> None:
    """Verify 20ms paced PCM16LE frames, in-flight turn-fencing interruption, and server loop lag."""
    redis_url = os.environ["KAIWEN_TEST_REDIS_URL"]
    run_id = uuid.uuid4().hex[:8]
    key_prefix = f"kwr1v{run_id}"
    port1 = _get_free_port()
    port2 = _get_free_port()

    async with _redis_cleanup(redis_url, key_prefix):
        proc1 = _start_gateway_process(
            port=port1,
            instance_id=f"gw-1-{run_id}",
            redis_url=redis_url,
            key_prefix=key_prefix,
        )
        proc2 = _start_gateway_process(
            port=port2,
            instance_id=f"gw-2-{run_id}",
            redis_url=redis_url,
            key_prefix=key_prefix,
        )
        try:
            u1_topic = derive_recipient_topic("tenant-alpha", "user-1")
            async with websockets.connect(
                f"ws://127.0.0.1:{port2}/ws/events/v1",
                additional_headers={"Authorization": "Bearer tok-t1-u1"},
            ) as ws_evt:
                assert json.loads(await ws_evt.recv())["type"] == "session.ready"
                await ws_evt.send(
                    json.dumps(
                        {
                            "type": "events.subscribe",
                            "protocol_version": "v1",
                            "topic": u1_topic,
                            "request_id": "sub-r1-p3",
                        }
                    )
                )
                assert json.loads(await ws_evt.recv())["type"] == "events.subscribed"

                send_intervals_ms: list[float] = []
                echo_rtts_ms: list[float] = []
                notif_latencies_ms: list[float] = []
                sent_notifs: dict[str, float] = {}
                received_notifs: list[str] = []

                async def _paced_voice_session(port: int, call_id: str, tag: int) -> None:
                    tok = _issue_voice_token("tenant-alpha", call_id)
                    uri = f"ws://127.0.0.1:{port}{SYNTHETIC_MEDIA_WS_ENDPOINT_PATH}"
                    async with websockets.connect(
                        uri, additional_headers={"Authorization": f"Bearer {tok}"}
                    ) as vws:
                        assert json.loads(await vws.recv())["event"] == EVENT_SESSION_READY
                        await vws.send(json.dumps({"event": EVENT_CALL_START, "call_id": call_id}))
                        assert json.loads(await vws.recv())["event"] == EVENT_CALL_STARTED

                        # 1) 10 identifiable PCM16LE frames paced at 20 ms intervals
                        prev_send: float | None = None
                        for seq in range(10):
                            t_now = time.perf_counter()
                            if prev_send is not None:
                                target = prev_send + 0.020
                                if t_now < target:
                                    await asyncio.sleep(target - t_now)
                                t_send = time.perf_counter()
                                send_intervals_ms.append((t_send - prev_send) * 1000.0)
                            else:
                                t_send = t_now
                            prev_send = t_send
                            frame = bytes([tag, seq]) * (PCM16LE_FRAME_BYTES // 2)
                            await vws.send(frame)
                            echo = await vws.recv()
                            echo_rtts_ms.append((time.perf_counter() - t_send) * 1000.0)
                            assert isinstance(echo, bytes)
                            assert echo == frame

                        # 2) Controlled in-flight burst (6 Turn-1 frames queued) + immediate interrupt
                        burst_frames = [
                            bytes([0xA0 + idx, tag]) * (PCM16LE_FRAME_BYTES // 2)
                            for idx in range(6)
                        ]
                        for bf in burst_frames:
                            await vws.send(bf)
                        await vws.send(
                            json.dumps(
                                {
                                    "event": EVENT_CALL_INTERRUPT,
                                    "call_id": call_id,
                                    "turn_id": 1,
                                    "request_id": f"req-inflight-{call_id}",
                                }
                            )
                        )

                        # Drain any pre-interrupt Turn-1 frames until call.interrupted arrives
                        interrupted_msg: dict[str, Any] | None = None
                        for _ in range(16):
                            raw = await asyncio.wait_for(vws.recv(), timeout=5.0)
                            if isinstance(raw, str):
                                parsed = json.loads(raw)
                                if parsed.get("event") == EVENT_CALL_INTERRUPTED:
                                    interrupted_msg = parsed
                                    break
                        assert interrupted_msg is not None
                        assert interrupted_msg["interrupted_turn_id"] == 1
                        assert interrupted_msg["new_turn_id"] == 2

                        # 3) Turn-2 frames: zero Turn-1 frames (0xA0..0xA5) may leak after interrupt
                        for seq in range(5):
                            t2_frame = bytes([0xB0 + seq, tag]) * (PCM16LE_FRAME_BYTES // 2)
                            await vws.send(t2_frame)
                            echo2 = await asyncio.wait_for(vws.recv(), timeout=5.0)
                            assert isinstance(echo2, bytes)
                            assert echo2 == t2_frame

                        await vws.send(json.dumps({"event": EVENT_CALL_END, "call_id": call_id}))
                        assert json.loads(await vws.recv())["event"] == EVENT_CALL_ENDED

                async def _publish_concurrent_notifs(count: int) -> None:
                    for idx in range(count):
                        nid = generate_notification_id()
                        sent_notifs[nid] = time.perf_counter()
                        res = await asyncio.to_thread(
                            _http_post_json,
                            f"http://127.0.0.1:{port1}/_test/publish-notification",
                            {
                                "notification_id": nid,
                                "tenant_id": "tenant-alpha",
                                "recipient_id": "user-1",
                                "payload": {"r1_p3_seq": idx},
                            },
                        )
                        assert res["status"] == "durable_accepted"

                async def _receive_notifs(expected: int) -> None:
                    while len(received_notifs) < expected:
                        msg = json.loads(await asyncio.wait_for(ws_evt.recv(), timeout=15.0))
                        if msg.get("type") == "events.delivery":
                            nid = msg["event"]["payload"]["notification_id"]
                            received_notifs.append(nid)
                            if nid in sent_notifs:
                                notif_latencies_ms.append(
                                    (time.perf_counter() - sent_notifs[nid]) * 1000.0
                                )

                t_batch = time.perf_counter()
                await asyncio.gather(
                    _paced_voice_session(port1, "call-r1-v1", 0x11),
                    _paced_voice_session(port2, "call-r1-v2", 0x22),
                    _publish_concurrent_notifs(20),
                    _receive_notifs(20),
                )
                elapsed_s = time.perf_counter() - t_batch
                assert len(received_notifs) == 20
                assert len(set(received_notifs)) == 20

                s1 = _http_get_json(f"http://127.0.0.1:{port1}/_test/stats")[1]
                s2 = _http_get_json(f"http://127.0.0.1:{port2}/_test/stats")[1]
                assert s1["server_loop_lag_samples"] > 0
                assert s2["server_loop_lag_samples"] > 0

                s_int = sorted(send_intervals_ms)
                s_rtt = sorted(echo_rtts_ms)
                s_n = sorted(notif_latencies_ms)
                print(
                    f"\n[R1_PHASE3_METRICS] notifs=20 elapsed_s={elapsed_s:.3f} "
                    f"notif_p50_ms={_percentile(s_n, 50):.2f} notif_p95_ms={_percentile(s_n, 95):.2f} "
                    f"voice_send_interval_p50_ms={_percentile(s_int, 50):.2f} "
                    f"voice_send_interval_p95_ms={_percentile(s_int, 95):.2f} "
                    f"voice_rtt_p50_ms={_percentile(s_rtt, 50):.2f} "
                    f"voice_rtt_p95_ms={_percentile(s_rtt, 95):.2f} "
                    f"gw1_srv_lag_p95_ms={s1['server_loop_lag_p95_ms']:.2f} "
                    f"gw2_srv_lag_p95_ms={s2['server_loop_lag_p95_ms']:.2f} "
                    f"gw1_ws_mb={s1['rss_bytes'] / (1024 * 1024):.1f} "
                    f"gw2_ws_mb={s2['rss_bytes'] / (1024 * 1024):.1f}"
                )
        finally:
            _stop_gateway_process(proc1)
            _stop_gateway_process(proc2)


@pytest.mark.anyio
async def test_r1_phase4_idle_redis_readiness_characterization() -> None:
    """Characterize Phase 4 P2 observation: idle RedisPubSubEventBroker readiness on outage."""
    real_redis_url = os.environ["KAIWEN_TEST_REDIS_URL"]
    parsed = urlparse(real_redis_url)
    target_host = parsed.hostname or "127.0.0.1"
    target_port = parsed.port or 6379
    proxy_port = _get_free_port()
    proxy_url = f"redis://127.0.0.1:{proxy_port}/0"

    run_id = uuid.uuid4().hex[:8]
    key_prefix = f"kwr1idle{run_id}"
    gw_port = _get_free_port()

    proxy = _RestartableTcpProxy(proxy_port, target_host, target_port)
    await proxy.start()
    proc: subprocess.Popen[str] | None = None

    async with _redis_cleanup(real_redis_url, key_prefix):
        try:
            proc = await asyncio.to_thread(
                _start_gateway_process,
                port=gw_port,
                instance_id=f"gw-idle-{run_id}",
                redis_url=proxy_url,
                key_prefix=key_prefix,
                enable_worker=False,
            )
            # Drop Redis while ZERO Pub/Sub subscriptions exist
            await proxy.drop_all()
            await asyncio.sleep(0.3)

            # Documented P2 behavior: /ready stays 200 while idle because listener has no subs
            status_idle, body_idle = await asyncio.to_thread(
                _http_get_json, f"http://127.0.0.1:{gw_port}/ready"
            )
            assert status_idle == 200
            assert body_idle["status"] == "ready"

            # First Redis operation fails closed and flips broker.is_healthy -> /ready 503
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                await asyncio.to_thread(
                    _http_post_json,
                    f"http://127.0.0.1:{gw_port}/_test/publish-event",
                    {
                        "event_id": f"evt-idle-{run_id}",
                        "event_type": "call.probe",
                        "tenant_id": "tenant-alpha",
                        "topic": "calls.status",
                        "payload": {"probe": True},
                    },
                )
            assert exc_info.value.code == 503
            exc_info.value.close()

            status_after, body_after = await asyncio.to_thread(
                _http_get_json, f"http://127.0.0.1:{gw_port}/ready", 5.0
            )
            assert status_after == 503
            assert body_after["reason"] == "events_backend_unavailable"

            # Restoring Redis allows automatic recovery to 200
            await proxy.start()
            deadline = time.monotonic() + 10.0
            rec_status = 503
            while time.monotonic() < deadline:
                rec_status, _ = await asyncio.to_thread(
                    _http_get_json, f"http://127.0.0.1:{gw_port}/ready"
                )
                if rec_status == 200:
                    break
                await asyncio.sleep(0.15)
            assert rec_status == 200
        finally:
            if proc is not None:
                _stop_gateway_process(proc)
            await proxy.drop_all()
