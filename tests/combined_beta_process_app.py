"""Test-only ASGI application factory for real-process combined beta acceptance tests.

Launches an independent Gateway OS process via `uvicorn --factory` configured with
Core + Voice + Events + Notifications, Redis Pub/Sub broker, and optional competing
NotificationsDurableWorker.

Note: The ``/_test/*`` HTTP routes added by this module exist strictly for test-harness
control and telemetry collection and are NOT part of the documented public Gateway API
or built wheel distributions.
"""

from __future__ import annotations

import asyncio
import collections
import os
import sys
import time
from typing import Any

from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from kaiwen_gateway.core import (
    CORE_CONTRACT_VERSION,
    CoreConfig,
    GatewayCore,
    ModuleDescriptor,
    ProviderPrincipal,
    TenantPrincipalBinding,
    VerifiedPrincipalContext,
    compose_gateway_core,
)
from kaiwen_gateway.modules.events import (
    EVENTS_PROTOCOL_VERSION,
    EventAuthorizationError,
    EventEnvelope,
    EventsBackendUnavailableError,
    EventsModule,
)
from kaiwen_gateway.modules.events.redis import (
    RedisClientConfig,
    RedisPubSubBrokerConfig,
    RedisPubSubEventBroker,
    RedisStreamsConfig,
    RedisStreamsDurableStore,
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
    NotificationsModule,
    NotificationsTopicAuthorizer,
    generate_notification_id,
)
from kaiwen_gateway.modules.voice import BoundedInterruptibleFakeAIEchoPort
from kaiwen_gateway.server import create_gateway_app
from kaiwen_gateway.transports.synthetic import SyntheticAuthBridge, SyntheticTokenVerifier

VOICE_SECRET = b"combined_beta_process_secret_32!"


def _get_process_rss_bytes() -> int:
    """Return current process resident set size (RSS) in bytes without external deps."""
    if sys.platform == "win32":
        import ctypes
        from ctypes import wintypes

        class _PROCESS_MEMORY_COUNTERS(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        counters = _PROCESS_MEMORY_COUNTERS()
        counters.cb = ctypes.sizeof(_PROCESS_MEMORY_COUNTERS)
        kernel32 = ctypes.windll.kernel32
        psapi = ctypes.windll.psapi
        kernel32.GetCurrentProcess.restype = wintypes.HANDLE
        psapi.GetProcessMemoryInfo.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(_PROCESS_MEMORY_COUNTERS),
            wintypes.DWORD,
        ]
        psapi.GetProcessMemoryInfo.restype = wintypes.BOOL
        handle = kernel32.GetCurrentProcess()
        if psapi.GetProcessMemoryInfo(handle, ctypes.byref(counters), counters.cb):
            return int(counters.WorkingSetSize)
        return 0
    else:
        try:
            with open("/proc/self/statm", encoding="utf-8") as fh:
                parts = fh.read().split()
                if len(parts) >= 2:
                    return int(parts[1]) * os.sysconf("SC_PAGE_SIZE")
        except OSError:
            pass
        return 0


def _make_principal(
    tenant_id: str,
    principal_id: str,
    scopes: frozenset[str] = frozenset({"events:subscribe"}),
) -> VerifiedPrincipalContext:
    return VerifiedPrincipalContext(
        connection_id=f"conn-{tenant_id}-{principal_id}",
        provider_id="synthetic-events",
        tenant_id=tenant_id,
        principal_id=principal_id,
        allowed_scopes=scopes,
    )


class _TokenMapEventsAuth:
    def __init__(self, principals: dict[str, VerifiedPrincipalContext]) -> None:
        self._principals = principals

    async def authenticate_bearer(self, *, token: str, connection_id: str) -> ProviderPrincipal:
        if token not in self._principals:
            raise EventAuthorizationError("Invalid bearer token")
        return ProviderPrincipal(provider_id=f"idp:{token}")


class _TokenMapEventsAuthorizer:
    def __init__(self, principals: dict[str, VerifiedPrincipalContext]) -> None:
        self._by_provider = {f"idp:{tok}": ctx for tok, ctx in principals.items()}

    async def authorize_principal(
        self, *, principal: ProviderPrincipal, connection_id: str
    ) -> TenantPrincipalBinding:
        ctx = self._by_provider.get(principal.provider_id)
        if ctx is None:
            raise EventAuthorizationError("Unknown principal")
        return TenantPrincipalBinding(
            authorized_provider_id=principal.provider_id,
            tenant_id=ctx.tenant_id,
            principal_id=ctx.principal_id,
            allowed_scopes=ctx.allowed_scopes,
        )


class _AllowTenantProducerAuth:
    async def authorize_publish(
        self,
        *,
        producer: VerifiedPrincipalContext | None,
        notification: NotificationEnvelope,
    ) -> None:
        if producer is not None and producer.tenant_id != notification.tenant_id:
            raise EventAuthorizationError("Cross-tenant producer publish denied")


class _DurableWorkerModule:
    """Lifecycle-managed NotificationsDurableWorker + server loop-lag probe for tests."""

    def __init__(
        self,
        store: RedisStreamsDurableStore,
        worker: NotificationsDurableWorker | None,
    ) -> None:
        self._descriptor = ModuleDescriptor(
            module_id="notifications_durable_worker",
            contract_version=CORE_CONTRACT_VERSION,
            description="Test-only durable worker module",
        )
        self._store = store
        self._worker = worker
        self._lag_samples_ms: collections.deque[float] = collections.deque(maxlen=2000)
        self._lag_task: asyncio.Task[None] | None = None

    @property
    def descriptor(self) -> ModuleDescriptor:
        return self._descriptor

    @property
    def lag_samples_ms(self) -> list[float]:
        return list(self._lag_samples_ms)

    async def _probe_server_loop_lag(self) -> None:
        interval = 0.02
        while True:
            t0 = time.perf_counter()
            await asyncio.sleep(interval)
            lag_ms = max(0.0, (time.perf_counter() - t0 - interval) * 1000.0)
            self._lag_samples_ms.append(lag_ms)

    async def startup(self) -> None:
        await self._store.startup()
        if self._worker is not None:
            self._worker.start()
        if self._lag_task is None:
            self._lag_task = asyncio.create_task(
                self._probe_server_loop_lag(),
                name="test-server-loop-lag-probe",
            )

    async def shutdown(self) -> None:
        if self._lag_task is not None:
            self._lag_task.cancel()
            try:
                await self._lag_task
            except asyncio.CancelledError:
                pass
            self._lag_task = None
        if self._worker is not None:
            await self._worker.stop()
        await self._store.shutdown()


def build_process_app() -> Any:
    """Factory invoked by `uvicorn tests.combined_beta_process_app:build_process_app --factory`."""
    redis_url = os.environ["KAIWEN_TEST_REDIS_URL"]
    instance_id = os.environ.get("KAIWEN_BETA_INSTANCE_ID", "gw-1")
    key_prefix = os.environ.get("KAIWEN_BETA_KEY_PREFIX", "kwbeta")
    enable_worker = os.environ.get("KAIWEN_BETA_ENABLE_WORKER", "1") == "1"

    redis_cfg = RedisClientConfig(url=redis_url, key_prefix=key_prefix)
    topic_auth = NotificationsTopicAuthorizer()
    broker = RedisPubSubEventBroker(
        RedisPubSubBrokerConfig(redis=redis_cfg),
        topic_authorizer=topic_auth,
    )
    events_mod = EventsModule(broker=broker, topic_authorizer=topic_auth)
    pref_store = InMemoryNotificationPreferenceStore()
    notif_telemetry = InMemoryNotificationsTelemetrySink()
    notif_mod = NotificationsModule(
        preference_port=pref_store,
        producer_authorizer=_AllowTenantProducerAuth(),
        telemetry=notif_telemetry,
    )

    durable_store = RedisStreamsDurableStore(
        RedisStreamsConfig(
            redis=redis_cfg,
            claim_min_idle_ms=150,
            max_delivery_attempts=5,
        )
    )
    durable_telemetry = InMemoryNotificationsDurableTelemetrySink()
    durable_publisher = NotificationsDurablePublisher(
        durable_store=durable_store,
        producer_authorizer=_AllowTenantProducerAuth(),
        telemetry=notif_telemetry,
        durable_telemetry=durable_telemetry,
    )

    worker: NotificationsDurableWorker | None = None
    if enable_worker:
        worker = NotificationsDurableWorker(
            config=NotificationsDurableConfig(
                tenant_id="tenant-alpha",
                group=f"{key_prefix}-group",
                consumer=instance_id,
                poll_interval_seconds=0.02,
                recovery_interval_seconds=0.1,
                claim_min_idle_ms=150,
            ),
            durable_store=durable_store,
            broker=broker,
            preference_port=pref_store,
            telemetry=notif_telemetry,
            durable_telemetry=durable_telemetry,
        )
    worker_mod = _DurableWorkerModule(durable_store, worker)
    modules: list[Any] = [events_mod, notif_mod, worker_mod]

    cfg = CoreConfig(
        instance_name=instance_id,
        ingress_mode="synthetic",
        events_ingress_mode="authenticated",
        max_concurrent_connections=64,
        max_connections_per_tenant=32,
    )
    core: GatewayCore = compose_gateway_core(config=cfg, modules=modules)

    principals = {
        "tok-t1-u1": _make_principal("tenant-alpha", "user-1"),
        "tok-t1-u2": _make_principal("tenant-alpha", "user-2"),
        "tok-t2-u1": _make_principal("tenant-beta", "user-1"),
    }
    bridge = SyntheticAuthBridge(
        verifier=SyntheticTokenVerifier(
            secret_key=VOICE_SECRET,
            enable_replay_protection=True,
        ),
        enforce_call_exclusivity=True,
    )

    app = create_gateway_app(
        cfg,
        core=core,
        synthetic_auth_bridge=bridge,
        enable_synthetic_media=True,
        ai_media_port_factory=lambda: BoundedInterruptibleFakeAIEchoPort(),
        enable_events=True,
        events_authenticator=_TokenMapEventsAuth(principals),
        events_authorizer=_TokenMapEventsAuthorizer(principals),
        events_module=events_mod,
        events_topic_authorizer=topic_auth,
        enable_notifications=True,
        notifications_module=notif_mod,
    )

    async def _publish_event(request: Request) -> JSONResponse:
        body = await request.json()
        env = EventEnvelope(
            event_id=body["event_id"],
            protocol_version=EVENTS_PROTOCOL_VERSION,
            event_type=body["event_type"],
            tenant_id=body["tenant_id"],
            principal_id=body.get("principal_id", "svc-events"),
            topic=body["topic"],
            timestamp="2026-11-18T20:00:00Z",
            payload=body["payload"],
        )
        try:
            res = await broker.publish(env)
        except EventsBackendUnavailableError:
            return JSONResponse({"error": "backend_unavailable"}, status_code=503)
        return JSONResponse(
            {"accepted": res.accepted, "delivered_subscribers": res.delivered_subscribers}
        )

    async def _publish_notification(request: Request) -> JSONResponse:
        body = await request.json()
        tenant_id = body["tenant_id"]
        producer = _make_principal(
            tenant_id, "svc-notifier", scopes=frozenset({"notifications:publish"})
        )
        notif = NotificationEnvelope(
            notification_id=body.get("notification_id") or generate_notification_id(),
            schema_version=NOTIFICATIONS_SCHEMA_VERSION,
            tenant_id=tenant_id,
            recipient_id=body["recipient_id"],
            category=body.get("category", "system.alert"),
            notification_type=body.get("notification_type", "alert.live"),
            priority=NotificationPriority.HIGH,
            payload=body["payload"],
            created_at="2026-11-18T20:00:00Z",
        )
        res = await durable_publisher.publish(notif, producer=producer)
        return JSONResponse(
            {
                "status": res.status.value,
                "notification_id": notif.notification_id,
                "sequence_id": res.sequence_id,
            }
        )

    def _pct(sorted_vals: list[float], pct: float) -> float:
        if not sorted_vals:
            return 0.0
        idx = min(
            len(sorted_vals) - 1,
            max(0, int(round((pct / 100.0) * (len(sorted_vals) - 1)))),
        )
        return sorted_vals[idx]

    async def _test_stats(request: Request) -> JSONResponse:
        snap = durable_telemetry.snapshot()
        lag_sorted = sorted(worker_mod.lag_samples_ms)
        worker_tasks = [
            t
            for t in asyncio.all_tasks()
            if not t.done() and "gw-notif-durable" in t.get_name().lower()
        ]
        return JSONResponse(
            {
                "instance_id": instance_id,
                "broker_healthy": broker.is_healthy,
                "broker_active_connections": broker.stats().active_connections,
                "worker_running": bool(worker is not None and worker.is_running),
                "worker_consumer": worker.config.consumer if worker is not None else None,
                "worker_task_count": len(worker_tasks),
                "worker_processed": snap.worker_processed,
                "worker_acked": snap.worker_acked,
                "worker_reclaimed": snap.worker_reclaimed,
                "worker_deduplicated": snap.worker_deduplicated,
                "rss_bytes": _get_process_rss_bytes(),
                "server_loop_lag_samples": len(lag_sorted),
                "server_loop_lag_p50_ms": round(_pct(lag_sorted, 50.0), 3),
                "server_loop_lag_p95_ms": round(_pct(lag_sorted, 95.0), 3),
            }
        )

    app.routes.insert(0, Route("/_test/publish-event", _publish_event, methods=["POST"]))
    app.routes.insert(
        0, Route("/_test/publish-notification", _publish_notification, methods=["POST"])
    )
    app.routes.insert(0, Route("/_test/stats", _test_stats, methods=["GET"]))
    return app
