# Kaiwen Gateway — Notifications N4 Acceptance & Qualification Report

> **Milestone:** `N4 — Notifications Integration, Performance & Acceptance`
> **Status:** `Ready for review`
> **Architecture Baseline:** [`ADR-0007`](decisions/0007-notifications-module-architecture.md) (`Accepted`) · [`docs/NOTIFICATIONS_MODULE_DESIGN.md`](NOTIFICATIONS_MODULE_DESIGN.md)

---

## 1. Deployment Composition & Worker Lifecycle

- **Base Package (`kaiwen-gateway`, `dependencies = []`):** Provides `NotificationEnvelope`, `NotificationsDispatcher`, `NotificationsTopicAuthorizer`, `ChainedNotificationsTopicAuthorizer`, `NotificationsDurableConfig`, `NotificationsDurablePublisher`, `NotificationsDurableWorker`, and `NotificationsModule` with zero Redis runtime dependencies.
- **Multi-Instance Redis Deployment (`kaiwen-gateway[redis]`):**
  - Each Gateway ASGI instance runs `create_gateway_app(..., enable_events=True, enable_notifications=True, events_topic_authorizer=NotificationsTopicAuthorizer())` backed by `RedisPubSubEventBroker` over `/ws/events/v1`.
  - Durable ingestion writes to `{prefix}:str:v1:{tenant_id}:notifications.ingest` via `NotificationsDurablePublisher` (`XADD MAXLEN ~`).
  - Bounded `NotificationsDurableWorker` instances share one consumer group (`notifications-workers`) per `(tenant_id, topic)` without Redis key scans (`SCAN`) or per-recipient worker proliferation.
  - `gateway_lifespan` drains active WebSocket connections before shutting down `NotificationsModule` and `EventsModule` in reverse registration order; `NotificationsDurableWorker.stop()` cancels and awaits its bounded consumer and recovery tasks with zero leaked tasks.

## 2. Multi-Instance Topology & Integration Results

Executed via [`tests/test_notifications_n4_multi_instance_live.py`](../tests/test_notifications_n4_multi_instance_live.py) against live Redis `7.4.12` (`3 passed in 0.83s`):

- **Two-Instance Cross-Node Delivery & Isolation:** Producer persists notification to Redis Streams; `NotificationsDurableWorker` on Instance 1 (`XREADGROUP` -> `RedisPubSubEventBroker.publish()` -> `XACK`) delivers `notifications.v1` over `/ws/events/v1` to Recipient A connected to Instance 2. Cross-recipient (`user-b` in `tenant-alpha`) and cross-tenant (`user-a` in `tenant-beta`) isolation verified.
- **Competing Consumers & Crash-After-Dispatch Window:** Two competing workers (`w-1`, `w-2`) in one consumer group process 6 notifications with zero concurrent duplicate dispatches during normal operation. Simulating a crash after `broker.publish()` and before `XACK` causes `w-2` to reclaim the pending entry via `XPENDING`/`XAUTOCLAIM` (`recover_once()`), resulting in an explicit **at-least-once duplicate-delivery window** across crash/recovery boundaries.
- **Slow Consumer & Poison DLQ Across Instances:** A slow WebSocket subscriber (`max_queued_events_per_connection=2`) disconnects with `1008` without affecting healthy subscribers on the peer instance; outer-envelope mismatched poison records are quarantined to the DLQ stream before `XACK`.

## 3. Reproducible Performance Benchmark

Harness: [`tools/benchmark_notifications.py`](../tools/benchmark_notifications.py)

### Reproducibility Command

```bash
docker run -d --rm -p 16379:6379 redis:7-alpine
uv run --all-extras python tools/benchmark_notifications.py \
  --redis-url redis://127.0.0.1:16379/0 \
  --rate 200 --concurrency 4 --workers 2 \
  --warmup 50 --duration 3.0 --payload-bytes 512 --inject-failure
```

### Measured Environment & Workload

- **Python:** `3.12.13`
- **Platform:** `Windows-11-10.0.26200-SP0`
- **Redis Server:** `7.4.12` (`redis:7-alpine` over localhost TCP)
- **Configuration:** `concurrency=4`, `workers=2`, `rate_per_sec=200.0`, `warmup=50`, `duration_seconds=3.0`, `payload_bytes=512`, `maxlen=10000`, `inject_failure=true`

### Empirical Measurements

| Metric | Measured Value |
| :--- | :--- |
| **Published / Delivered (measured)** | `600` / `600` (`delivery_success_rate = 1.0`, `errors = 0`) |
| **Delivered Throughput** | `185.66` notifications/s |
| **End-to-End Latency (p50 / p95 / p99 / max)** | `24.932 ms` / `279.741 ms` / `298.377 ms` / `329.599 ms` (`count=622`, includes `29` reclaimed under `claim_min_idle_ms=50`) |
| **Event-Loop Lag (p50 / p95 / p99 / max)** | `0.0 ms` / `6.0 ms` / `21.0 ms` / `22.0 ms` (`count=1560`) |
| **Python Memory (`tracemalloc` current / peak)** | `895.81 KiB` / `1187.72 KiB` |
| **Durable Telemetry** | `durable_accepted=651`, `worker_processed=680`, `worker_acked=651`, `worker_reclaimed=29`, `worker_dead_lettered=0`, `worker_retryable_failures=0`, `redis_failures=0` |
| **Broker End State** | `active_connections=1`, `queued_events=0` |

> **Note:** These figures reflect a single-host loopback qualification run and are **not** a production SLA guarantee.

## 4. Six-Stage Delivery Guarantees & Remaining Limitations

- **Stage 1 (`DURABLE_ACCEPTED`):** Confirmed only after Redis `XADD` returns a stream entry ID; timeouts fail closed as `NotificationsBackendUnavailableError`.
- **Stage 3 (`DISPATCHED_TO_BROKER`):** Confirms `EventBrokerPort.publish()` acceptance only (`0` active connections when offline is a valid terminal outcome). It does **not** prove WebSocket socket write completion or client receipt (Stage 4/5 remain outside Gateway MVP).
- **Stage 6 (`XACK`):** Issued strictly after terminal Stage 2+3 processing or confirmed DLQ `XADD`.
- **Remaining Limitations:** Bounded in-memory `(tenant_id, recipient_id, notification_id)` deduplication is non-crash-persistent; Redis `MAXLEN` trimming can evict entries if consumer groups lag beyond retention; per-user offline inbox persistence and Email/SMS/Push/Webhook providers remain outside N0–N4 MVP.
