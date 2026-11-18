# Kaiwen Gateway — Notifications Module Design (`N0` Architecture & Contracts)

> **Status:** `Proposed` (`N0` Architecture & Documentation Baseline — **not yet implemented**)
> **Date:** 2026-11-18
> **Related ADR:** [`ADR-0007 — Notifications Module Architecture`](decisions/0007-notifications-module-architecture.md) (`Proposed`)
> **Related Baseline:** [`ADR-0006`](decisions/0006-events-module-architecture.md) (`Accepted`) · [`docs/EVENTS_MODULE_DESIGN.md`](EVENTS_MODULE_DESIGN.md) (`D0`–`E4 Complete` on `main` `9e9af79`)
> **Scope:** Modular `src/kaiwen_gateway/modules/notifications/` design reusing Core (`GatewayModule`, `VerifiedPrincipalContext`) and Events (`EventBrokerPort`, `DurableEventStorePort`, `/ws/events/v1`, `[redis]`) without altering Voice or adding mandatory runtime dependencies.

---

## 1. Architecture Audit & Reuse Inventory

Auditing `main` (`9e9af79b953b1b10ed4b977ebb3a35cf27a334c0`, `D0`–`E4` complete) confirms that Kaiwen Gateway already provides the identity, lifecycle, transport, rate-limiting, and Redis durability primitives required for Notifications. Notifications **must not** duplicate these subsystems.

### 1.1 Directly Reused Interfaces & Components

| Layer | Existing Interface / Component | Reuse Role in Notifications |
|---|---|---|
| **Core Identity** | `VerifiedPrincipalContext`, `TenantPrincipalBinding`, `PrincipalId`, `TenantId`, `validate_principal_id`, `validate_tenant_id` (`core/identity.py`) | Server-verified `(provider_id, tenant_id, principal_id, connection_id, allowed_scopes)` for recipient authorization and tenant isolation |
| **Core Lifecycle & Admission** | `GatewayModule`, `ModuleDescriptor`, `AdmissionController.acquire_principal`, `InstanceDrainCoordinator` (`core/contracts.py`, `core/admission.py`, `core/draining.py`) | Bounded startup/shutdown lifecycle (`NotificationsModule`) and connection capacity accounting |
| **Events Live Fan-Out** | `EventEnvelope`, `EventPublisherPort`, `EventSubscriptionPort`, `EventBrokerPort`, `InMemoryEventBroker`, `RedisPubSubEventBroker` (`modules/events/`) | Real-time tenant- and recipient-scoped notification delivery (`notifications.*` topics) with `1 MiB`/conn & `32 MiB` global bounded queues |
| **Events Durability & Recovery** | `DurableEventStorePort`, `DurableEventRecord`, `RedisStreamsDurableStore`, `StreamRecoveryWorker` (`modules/events/ports.py`, `modules/events/redis/streams.py`) | Optional durable notification queue (`XADD MAXLEN ~`), consumer-group processing (`XREADGROUP`), explicit `XACK`, `XPENDING`/`XAUTOCLAIM` crash recovery, and DLQ quarantine |
| **Events Auth & Rate Limiting** | `EventsAuthenticatorPort`, `EventsAuthorizerPort`, `EventsTicketVerifierPort`, `TicketReplayCachePort`, `RateLimiterPort`, `RedisDistributedRateLimiter` | Bearer & single-use `?ticket=` WebSocket auth, cross-instance replay protection, and distributed publish/subscribe rate limits |
| **WebSocket Transport** | `/ws/events/v1` (`transports/events/endpoint.py`, `protocol.py`) | Real-time server-to-client notification delivery over `events.delivery` frames (`<= 64 KiB`, `1008`/`1009`/`1011` close semantics) |
| **Server & Readiness** | `create_gateway_app()`, `/health`, `/ready` (`server/app.py`) | Composition root wiring and `503 events_backend_unavailable` readiness degradation |

### 1.2 Explicit Non-Duplication Boundaries

- **No second Redis stack:** Notifications uses `EventBrokerPort`, `DurableEventStorePort`, and `RateLimiterPort` rather than importing `redis.asyncio` or creating separate Pub/Sub/Streams clients.
- **No second WebSocket server:** Real-time in-app notifications ride over `/ws/events/v1` (`events.delivery`).
- **No Voice / Core coupling:** `kaiwen_gateway.core` and `kaiwen_gateway.modules.voice` remain unaware of Notifications.
- **No `kaiwen-agent` or Speech dependency:** Future Agent and Speech integrations may publish into `NotificationPublisherPort` or `EventPublisherPort`, but Notifications has zero compile/runtime dependency on them.
- **Zero mandatory base dependencies:** Base `kaiwen-gateway` preserves `dependencies = []`; Redis durability remains optional via `kaiwen-gateway[redis]`.

---

## 2. Notification Domain Contracts (`modules/notifications/domain.py`)

```text
NotificationEnvelope (notifications.v1)
├── notification_id: str          # Canonical synthetic ID (e.g. notif_<uuid4_hex>)
├── schema_version: str           # "notifications.v1"
├── tenant_id: str                # Server-verified TenantId (never client-forged)
├── recipient_id: str             # Server-verified PrincipalId of target recipient
├── category: str                 # Bounded category slug (e.g. "system", "alert", "workflow", "agent")
├── notification_type: str        # Domain event name (e.g. "task.completed", "quota.warning")
├── priority: NotificationPriority # "low" | "normal" | "high" | "urgent"
├── payload: Mapping[str, Any]    # Immutable JSON-safe dict (depth <= 16, NotificationEnvelope serialized <= 48 KiB so outer EventEnvelope fits <= 64 KiB)
├── created_at: str               # ISO-8601 UTC timestamp
├── expires_at: str | None        # Optional ISO-8601 UTC expiration timestamp
├── idempotency_key: str | None   # Optional producer deduplication key (scoped to tenant + recipient)
└── correlation_id: str | None    # Optional safe trace/correlation ID
```

### 2.1 Lifecycle & Delivery Status Model

```text
CREATED ──▶ ACCEPTED_DURABLE (optional E4 XADD) / PUBLISHED_LIVE (E1/E3 publish)
                 │
                 ├──▶ EXPIRED (dropped before delivery if now >= expires_at)
                 ├──▶ FILTERED_BY_PREFERENCE (muted category / priority threshold)
                 ├──▶ DEAD_LETTERED (poison/malformed after max_delivery_attempts)
                 └──▶ DELIVERED_TO_TRANSPORT (sent on /ws/events/v1 events.delivery)
                           └──▶ CLIENT_ACKNOWLEDGED (optional app-level read/ack receipt)
```

- **Deduplication & Idempotency:** Producers may supply `idempotency_key`; durable workers and in-app consumers deduplicate using `(tenant_id, recipient_id, notification_id)` (and optional bounded `idempotency_key` cache in N3).
- **Schema Versioning:** `schema_version = "notifications.v1"` is validated fail-closed on construction and deserialization (`UnsupportedNotificationVersionError` for unknown versions).

---

## 3. Delivery Architecture & 5-Stage Acknowledgment Separation

Notifications **must never** conflate transport queuing or event publication with user receipt. The architecture enforces five distinct, independently observable stages:

| Stage | Owner | Meaning | Guarantees / Failure Semantics |
|---|---|---|---|
| **1. Event Publication** | Producer / App | `EventPublisherPort.publish()` or `DurableEventStorePort.append()` called | Live publish is **at-most-once**; durable append (`XADD`) is confirmed only when Redis returns entry ID (timeouts fail closed as ambiguous) |
| **2. Notification Processing** | `NotificationsDispatcher` / N3 Worker | Validates schema, checks `expires_at`, evaluates `NotificationPreferencePort`, constructs `EventEnvelope` | Drops expired/muted notifications cleanly; records telemetry without logging raw payloads |
| **3. Transport Delivery** | `/ws/events/v1` + `EventBrokerPort` | Enqueues onto recipient's bounded WebSocket queue and writes `events.delivery` frame | Bounded (`1 MiB`/conn, `32 MiB` global); slow consumers disconnect with `1008` |
| **4. Client Acknowledgment** | Consuming Application / Client | Explicit app-level "read/dismissed" signal (outside `XACK`) | Optional N2/N3 application concern; **not** implied by WebSocket frame write |
| **5. Durable Processing Acknowledgment** | Durable Consumer Worker (`N3`) | `DurableEventStorePort.ack()` (`XACK`) | Issued **only** after Stage 2 + Stage 3 dispatch (or external channel send) completes; **never** issued on `XREADGROUP` or WebSocket queue enqueue |

### 3.1 Recipient Routing, Offline Recipients & Reconnect Behavior

- **Recipient-Scoped Topics:** Live delivery publishes to `notifications.r.<token>` where `<token>` is a deterministic 24-hex-character SHA-256 prefix of `(tenant_id, recipient_id)` (compatible with `TOPIC_PATTERN`, which forbids `:`, and bounded well within `MAX_SCOPE_LENGTH = 64`).
- **Authorization (Additive `N2` Hook):** Today, `transports/events/endpoint.py` delegates `events.subscribe` directly to `broker.subscribe(verified_context, frame.topic_pattern)`, and `_is_subscription_authorized()` grants any exact topic to `"events:subscribe"`. Therefore, `N2` requires an **additive, backward-compatible** per-subscription topic-authorization hook (e.g., an optional `topic_authorizer` in `/ws/events/v1` / `create_gateway_app()`) that denies `"events:subscribe"` on `notifications.r.*` unless the derived token matches `verified_context.principal_id` or an explicit admin scope (e.g., `notifications:subscribe:tenant`) is present. Wildcard subscriptions across other recipients' topics are denied fail-closed.
- **Offline Recipients (Honest Scope):**
  - In **N2 (Real-Time MVP)**, `/ws/events/v1` + `EventBrokerPort` delivers **live** notifications to currently connected sessions (`at-most-once`). Offline recipients do **not** receive missed live notifications over Pub/Sub alone.
  - In **N3 (Durable Delivery & Recovery)**, `DurableEventStorePort` (`RedisStreamsDurableStore`) persists notifications within `MAXLEN` retention so workers recover from crashes (`XPENDING`/`XAUTOCLAIM`), and bounded `replay()` / application unread state can surface missed notifications on reconnect within the retention window.

### 3.2 Expiration, Retention, Backpressure & Shutdown

- **Expiration:** Every worker/dispatcher checks `expires_at` before transport/channel dispatch; expired notifications are discarded with `notifications_expired` telemetry (never delivered stale).
- **Retention & Backpressure:** Reuses `EventBrokerConfig` (`128` events / `1 MiB` per connection, `32 MiB` global budget, `disconnect_slow_consumer` `1008`) and `RedisStreamsConfig` (`maxlen=10_000`, `dead_letter_maxlen=1_000` provisional).
- **Cancellation & Shutdown:** `NotificationsModule` implements `GatewayModule` (`startup()` / `shutdown()`), cancelling background recovery/worker tasks cleanly with bounded timeouts on `InstanceDrainCoordinator` shutdown.

---

## 4. User Preferences & Future Delivery Channels

### 4.1 User Preferences (`NotificationPreferencePort`)

Near-term MVP requires only lightweight, fail-safe filtering—**not** a full templating/localization engine:

- `NotificationPreference`: `tenant_id`, `recipient_id`, `muted_categories: frozenset[str]`, `min_priority: NotificationPriority = LOW`, `disabled_channels: frozenset[str]`.
- `NotificationPreferencePort`: `async def get_preferences(self, *, tenant_id: str, recipient_id: str) -> NotificationPreference | None`.
- **Deferral:** Rich localization, Mustache/Jinja templates, and digest batching remain **application responsibilities** outside Gateway MVP (`N1`–`N4`).

### 4.2 Future External Delivery Channels (`NotificationChannelPort`)

- Reserve an interface-only `NotificationChannelPort` (`channel_id: str`, `async def send(self, notification: NotificationEnvelope) -> ChannelDeliveryResult`) for future **Email**, **SMS**, **Push**, and **Webhook** adapters.
- **Explicitly deferred:** Zero external channel providers (SMTP, Twilio, FCM/APNs, outbound HTTP webhooks) are implemented in `N0`–`N4` MVP; only in-app real-time + durable Streams plumbing is in scope for `N1`–`N4`.

---

## 5. Transport Evaluation: `/ws/events/v1` vs. Separate REST Endpoints

| Option | Pros | Cons | Decision |
|---|---|---|---|
| **A. Reuse `/ws/events/v1` only (MVP)** | Zero new HTTP surface; reuses Bearer + single-use `?ticket=` + `Origin` + `RateLimiterPort` + `AdmissionController`; preserves Gateway library/transport role | Consuming app sends notifications via Python `NotificationPublisher` / server-side API rather than Gateway REST | **Selected for N1–N2** |
| **B. Add Gateway REST API (`POST /notifications`, `GET /notifications`, `POST .../ack`)** | Direct HTTP ingress without Python SDK | Duplicates auth middleware, pagination, DB/unread-state ownership, and turns Gateway into a product backend | **Deferred** (evaluate post-N4 only if a dedicated HTTP ingest boundary is authorized) |

---

## 6. Security & Reliability Threat Model

| Threat | Impact | Mitigation (Fail-Closed) |
|---|---|---|
| **Cross-tenant notification leakage** | Tenant A reads Tenant B's notifications | `tenant_id` always bound from `VerifiedPrincipalContext`; Events/Streams enforce tenant-isolated keys (`kw:evt:v1:{tenant_id}:...`, `kw:str:v1:{tenant_id}:...`) and payload-vs-channel `tenant_id` checks |
| **Unauthorized recipient targeting / subscription** | User A subscribes to User B's notifications | Recipient topic derived server-side from verified `principal_id`; `NotificationsAuthorizerPort` rejects subscriptions to other recipients' topics and denies unauthorized wildcard scopes |
| **Subscription & ACL bypass** | Unauthenticated or forged claims | Pre-accept `Origin` allowlist + single-use `?ticket=` (`SET NX PX` in E3) + `EventsAuthorizerPort` scope checks; fail closed (`1008` / `1011`) |
| **Replay & duplicate delivery** | Duplicate side effects on redelivery | Ticket `jti` single-use replay cache (`E2`/`E3`); `notification_id` + `idempotency_key` consumer deduplication; explicit `at-least-once` documentation |
| **Notification flooding** | Resource exhaustion / noisy neighbor | `RedisDistributedRateLimiter` per-tenant/producer publish limits + `EventBrokerPort` per-connection `1 MiB` / global `32 MiB` budgets + `1008` slow-consumer disconnect |
| **Sensitive data exposure** | PII/secrets in logs, metrics, or errors | `NotificationsTelemetrySink` uses bounded-cardinality counters only (`category`, `priority`, `outcome`, `error_category`); never logs raw payloads, tokens, tickets, or `notification_id`s as metric labels |
| **Unbounded queues** | OOM under slow consumers or outage | Dual count + byte queue caps in `EventBrokerPort`; `MAXLEN ~` in `RedisStreamsDurableStore` + bounded `dead_letter_maxlen` DLQ |
| **Redis disconnections** | Split-brain limits or silent drops | Fail closed with `EventsBackendUnavailableError` (`503` on `/ready`, `1011` on WS); no silent downgrade to process-local state when Redis is configured |
| **Malformed notification payloads** | Worker crash or PEL wedge | Strict `NotificationEnvelope` validation (`<= 64 KiB`, depth `<= 16`, finite numbers); malformed stream entries quarantined to DLQ metadata and `XACK`ed (`N3`) |

---

## 7. Compatibility Assessment

- **Core (`kaiwen_gateway.core`):** Zero changes required; reuses `VerifiedPrincipalContext`, `GatewayModule`, `AdmissionController`, and `InstanceDrainCoordinator`.
- **Voice (`kaiwen_gateway.modules.voice`):** Completely isolated; audio hot path (`VoiceBridgeSession`) untouched.
- **Events (`E1`–`E4`):** Consumed via public ports (`EventBrokerPort`, `DurableEventStorePort`, `RateLimiterPort`) and `/ws/events/v1` with zero breaking changes. `N2` adds an optional, backward-compatible per-subscription topic-authorization hook in `/ws/events/v1` / `create_gateway_app()` to enforce recipient-scoped `notifications.r.*` access; `N3` handles application-level poison `NotificationEnvelope` quarantine via an explicit `NotificationsDeadLetterSink` or `RedisStreamsDurableStore` DLQ hook (since `DurableEventStorePort` itself exposes `append`, `read_group`, `ack`, `replay`).
- **Packaging:** `kaiwen_gateway.modules.notifications` ships inside `kaiwen-gateway` with `dependencies = []` unchanged; Redis features remain optional under `kaiwen-gateway[redis]`.

---

## 8. Bounded Implementation Roadmap (`N0`–`N4`)

| Milestone | Scope | Key Deliverables | Acceptance Criteria & Required Tests | Dependencies | Risks & Deferrals |
|---|---|---|---|---|---|
| **N0 — Architecture & Contracts** | Design, ADR-0007, threat model, compatibility & roadmap | `NOTIFICATIONS_MODULE_DESIGN.md`, `ADR-0007`, `ROADMAP.md` update | Docs consistent, zero code/dep changes, maintainer ADR review | `E1`–`E4` Complete (`9e9af79`) | No code; REST API & external channels deferred |
| **N1 — Notification Core** | Domain types, validation, ports, in-memory preference & telemetry sinks | `modules/notifications/` (`domain.py`, `ports.py`, `telemetry.py`, `module.py`) | Unit tests for validation, expiration, topic derivation, zero-Redis import, `mypy --strict`, `ruff` | `N0` Accepted | Defer localization/templates & external providers |
| **N2 — Real-Time Delivery** | `NotificationsDispatcher` publishing to `EventBrokerPort` + recipient-scoped `/ws/events/v1` auth helper | `dispatcher.py`, recipient authorization guard, loopback WS tests | Online recipient receives `notifications.v1` over `/ws/events/v1`; cross-tenant/cross-recipient denied; slow-consumer `1008` | `N1` Complete | Offline catch-up deferred to `N3`/app store |
| **N3 — Durable Delivery & Recovery** | Durable notification worker backed by `DurableEventStorePort` (`RedisStreamsDurableStore`), `StreamRecoveryWorker`, idempotent dedup & DLQ | `durable_worker.py`, live-Redis integration tests (crash/reclaim, poison DLQ, `expires_at`, outage) | Crash before `XACK` recovers via `XAUTOCLAIM`; `XACK` only after dispatch; poison entries quarantined; `0` silent loss within `MAXLEN` | `N2` Complete + `[redis]` | Defer external Email/SMS/Push/Webhook adapters |
| **N4 — Integration, Performance & Acceptance** | Multi-instance load qualification, failure-injection suite, docs & compatibility closeout | `tools/benchmark_notifications.py`, acceptance report, `PUBLIC_API_AND_COMPATIBILITY.md` update | Reproducible p50/p95/p99 & memory bounds; Python 3.11–3.13 CI green; `0` P0/P1 findings | `N3` Complete | Gate D operational review required before production SLA |

---

## 9. Open Decisions Requiring Maintainer Approval

1. **ADR-0007 Acceptance:** Approve `src/kaiwen_gateway/modules/notifications/` boundaries, `/ws/events/v1` transport reuse (deferring Gateway REST endpoints), and `N1`–`N4` roadmap.
2. **Recipient Topic Derivation Scheme:** Confirm deterministic server-derived recipient topic naming (`notifications.r.<hash_or_slug>`) so `PrincipalId`s containing `:` remain compatible with `TOPIC_PATTERN` without weakening tenant isolation.
3. **Preference Miss Policy (`N1`/`N2`):** Confirm whether `NotificationPreferencePort` returning `None` defaults to **allow** (opt-out model) or **deny** (opt-in model) per category.
4. **External Channel Scope:** Confirm deferring Email/SMS/Push/Webhook concrete adapters until after `N4` core/real-time/durable acceptance.
