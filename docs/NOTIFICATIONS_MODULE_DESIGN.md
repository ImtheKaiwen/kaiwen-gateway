# Kaiwen Gateway — Notifications Module Design (`N0` Architecture & Contracts)

> **Status:** `Proposed` (`N0-R1` Architecture & Documentation Baseline — **not yet implemented**)
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
| **Core Identity** | `VerifiedPrincipalContext`, `TenantPrincipalBinding`, `PrincipalId`, `TenantId`, `validate_principal_id`, `validate_tenant_id` (`core/identity.py`) | Server-verified `(provider_id, tenant_id, principal_id, connection_id, allowed_scopes)` for producer/recipient authorization and tenant isolation |
| **Core Lifecycle & Admission** | `GatewayModule`, `ModuleDescriptor`, `AdmissionController.acquire_principal`, `InstanceDrainCoordinator` (`core/contracts.py`, `core/admission.py`, `core/draining.py`) | Bounded startup/shutdown lifecycle (`NotificationsModule`) and connection capacity accounting |
| **Events Live Fan-Out** | `EventEnvelope`, `EventPublisherPort`, `EventSubscriptionPort`, `EventBrokerPort`, `InMemoryEventBroker`, `RedisPubSubEventBroker` (`modules/events/`) | Real-time tenant- and recipient-scoped notification delivery (`notifications.r.*`) with `1 MiB`/conn & `32 MiB` global bounded queues |
| **Events Durability & Recovery** | `DurableEventStorePort`, `DurableEventRecord`, `RedisStreamsDurableStore`, `StreamRecoveryWorker` (`modules/events/ports.py`, `modules/events/redis/streams.py`) | Optional durable notification processing (`XADD MAXLEN ~`), consumer-group processing (`XREADGROUP`), explicit `XACK`, `XPENDING`/`XAUTOCLAIM` crash recovery, and DLQ quarantine |
| **Events Auth & Rate Limiting** | `EventsAuthenticatorPort`, `EventsAuthorizerPort`, `EventsTicketVerifierPort`, `TicketReplayCachePort`, `RateLimiterPort`, `RedisDistributedRateLimiter` | Bearer & single-use `?ticket=` WebSocket auth, cross-instance replay protection, and distributed publish/subscribe rate limits |
| **WebSocket Transport** | `/ws/events/v1` (`transports/events/endpoint.py`, `protocol.py`) | Real-time server-to-client notification delivery over `events.delivery` frames (`<= EVENTS_MAX_FRAME_BYTES` = `65_536` bytes, `1008`/`1009`/`1011` close semantics) |
| **Server & Readiness** | `create_gateway_app()`, `/health`, `/ready` (`server/app.py`) | Composition root wiring and `503 events_backend_unavailable` readiness degradation |

### 1.2 Explicit Non-Duplication Boundaries

- **No second Redis stack:** Notifications uses `EventBrokerPort`, `DurableEventStorePort`, and `RateLimiterPort` rather than importing `redis.asyncio` or creating separate Pub/Sub/Streams clients.
- **No second WebSocket server:** Real-time in-app notifications ride over `/ws/events/v1` (`events.delivery`).
- **No Voice / Core coupling:** `kaiwen_gateway.core` and `kaiwen_gateway.modules.voice` remain unaware of Notifications.
- **No `kaiwen-agent` or Speech dependency:** Future Agent and Speech integrations may publish through `NotificationPublisherPort`, but Notifications has zero compile/runtime dependency on them.
- **Zero mandatory base dependencies:** Base `kaiwen-gateway` preserves `dependencies = []`; Redis durability remains optional via `kaiwen-gateway[redis]`.

---

## 2. Notification Domain Contracts (`modules/notifications/domain.py`)

```text
NotificationEnvelope (notifications.v1)
├── notification_id: str           # Canonical synthetic ID (e.g. notif_<uuid4_hex>)
├── schema_version: str            # "notifications.v1"
├── tenant_id: str                 # Server-verified TenantId (never client-forged)
├── recipient_id: str              # Target PrincipalId (authorized by host app policy)
├── category: str                  # Bounded category slug (e.g. "system", "alert", "workflow", "agent")
├── is_promotional: bool           # Distinguishes operational/in-app vs promotional (default False)
├── notification_type: str         # Domain event name (e.g. "task.completed", "quota.warning")
├── priority: NotificationPriority # "low" | "normal" | "high" | "urgent"
├── payload: Mapping[str, Any]     # Immutable JSON-safe dict (depth <= 16)
├── created_at: str                # ISO-8601 UTC timestamp
├── expires_at: str | None         # Optional ISO-8601 UTC expiration timestamp
├── idempotency_key: str | None    # Optional producer deduplication key (scoped to tenant + recipient)
└── correlation_id: str | None     # Optional safe trace/correlation ID
```

### 2.1 Frame Size Safety (`R4`)

Relying solely on an inner `48 KiB` `NotificationEnvelope` limit is **insufficient**, because wrapping `NotificationEnvelope` inside `EventEnvelope` and `build_events_delivery_frame()` adds outer envelope fields, `subscription_id`, and JSON escaping.

- **Inner early check:** `NotificationEnvelope` enforces `serialized_bytes <= 49_152` (`48 KiB`) and `max_json_depth <= 16` for early rejection.
- **Authoritative final-frame check:** Before accepting or dispatching a notification, `NotificationsDispatcher` / publisher **must** validate:
  1. The constructed outer `EventEnvelope.serialized_bytes <= DEFAULT_MAX_EVENT_BYTES` (`65_536`), and
  2. The final serialized `events.delivery` WebSocket frame (via `build_delivery_frame(..., subscription_id=...)` with worst-case `subscription_id` length) is `<= EVENTS_MAX_FRAME_BYTES` (`65_536` bytes). Oversized notifications fail closed (`OversizedNotificationError`) at publish/dispatch time rather than failing mid-stream on WebSocket send.

### 2.2 Lifecycle, Deduplication & Schema Versioning

- **Deduplication & Idempotency:** Producers may supply `idempotency_key`; durable workers and in-app consumers deduplicate using `(tenant_id, recipient_id, notification_id)` (and optional bounded `idempotency_key` cache in `N3`).
- **Schema Versioning:** `schema_version = "notifications.v1"` is validated fail-closed on construction and deserialization (`UnsupportedNotificationVersionError` for unknown versions).

---

## 3. Delivery Architecture & 6-Stage Acknowledgment Model (`R2`)

Notifications **must never** promise guaranteed client delivery merely because Redis Streams provides recoverable worker processing. The architecture separates six distinct stages:

| Stage | Owner | Meaning | Guarantees / Failure Semantics |
|---|---|---|---|
| **1. Durable Acceptance** | `N3` Producer / Store | `DurableEventStorePort.append()` (`XADD`) confirmed by Redis | Confirmed only when Redis returns an entry ID; timeouts fail closed as ambiguous (`EventsBackendUnavailableError`) |
| **2. Notification Processing** | `NotificationsDispatcher` / `N3` Worker | Validates `NotificationEnvelope`, checks `expires_at`, evaluates `NotificationPreferencePort`, verifies final frame size | Drops expired/muted notifications cleanly; preference-store errors fail closed (never silently treated as missing) |
| **3. Transport Dispatch** | `/ws/events/v1` + `EventBrokerPort` | Enqueues onto active WebSocket connection's bounded queue and writes `events.delivery` | Best-effort `at-most-once` for currently connected clients; slow consumers disconnect with `1008` |
| **4. Actual Client Receipt** | Recipient Client | Client socket receives and parses `events.delivery` | **Unobserved by Gateway** without Stage 5 |
| **5. Application Acknowledgment** | Consuming Application / Client | App-level "read/dismissed" signal | Owned by consuming application (outside Gateway MVP); **never** implied by Stage 3 |
| **6. Redis Streams Acknowledgment** | `N3` Consumer Worker | `DurableEventStorePort.ack()` (`XACK`) | Issued **only** after Stage 2 + Stage 3 dispatch completes; **never** issued on `XREADGROUP` or WebSocket queue enqueue |

### 3.1 Failure & Edge-Case Behavior (`R2`)

| Scenario | Behavior |
|---|---|
| **1. Recipient is offline** | `EventBrokerPort.publish()` delivers to `0` active WebSocket connections. In `N2` (live Pub/Sub), the notification is dropped (`at-most-once`). In `N3`, the worker completes Stage 2+3 dispatch (`0` live WS connections) and issues `XACK`—durable processing does **not** hold entries in the PEL until a user comes online. |
| **2. WebSocket delivery fails** | Connection closes or hits slow-consumer `1008` overflow; the frame is not delivered to the client. |
| **3. Connection closes after server dispatch** | TCP/WS drops after `send_json` / queue flush before client reads -> client misses notification (`at-most-once` transport). |
| **4. Worker crashes before `XACK`** | Entry remains in Redis Streams PEL; another worker reclaims it via `XPENDING`/`XAUTOCLAIM` after `claim_min_idle_ms` -> potential duplicate delivery (requires idempotent consumer keyed by `notification_id`). |
| **5. Notification expires while pending** | Worker checks `expires_at` during Stage 2; expired notifications are discarded (telemetry incremented) and `XACK`ed so they do not wedge the PEL. |
| **6. Stream entry evicted by `MAXLEN`** | Approximate `MAXLEN ~` trims entries when a consumer group lags beyond retention -> permanent loss for that group (`retention_trimmed_losses` telemetry). |

### 3.2 `N3` Scope Clarification: Durable Processing vs. Offline Inbox

- **`N3` provides durable worker processing and crash recovery** (`XADD`, `XREADGROUP`, `XPENDING`/`XAUTOCLAIM`, DLQ quarantine) plus bounded stream `replay()` within `MAXLEN`.
- **`N3` does NOT provide application-owned per-user offline inbox persistence** (unread counts, pagination, read/dismissed state). Application-owned inbox persistence remains explicitly outside Gateway MVP; consuming applications needing guaranteed offline inbox history must persist notifications in their own application store or read-model.

---

## 4. Recipient & Producer Authorization (`R1` & `R3`)

### 4.1 Recipient Topic Naming & Mandatory Authorization (`R1`)

- **Naming Compatibility, Not Authentication:** Because `PrincipalId` permits `:` while `TOPIC_PATTERN` forbids `:`, Notifications maps `(tenant_id, recipient_id)` canonically using length-prefixed SHA-256 (`len(tenant_id):tenant_id:len(recipient_id):recipient_id`) to `notifications.r.<24-hex-chars>` (fitting well within `MAX_SCOPE_LENGTH = 64`). **This deterministic hash provides naming compatibility only—it is never a secret or capability token.**
- **Mandatory Fail-Closed Namespace Guard:** Whenever Notifications is enabled (`enable_notifications=True`):
  1. **No implicit `events:subscribe` inheritance:** General `events:subscribe` and `events:subscribe:wildcard` **must never** grant access to `notifications.r.*`.
  2. **Exact-topic subscriptions (`notifications.r.<token>`):** Allowed **only** if `<token>` equals the canonical token derived from `(verified_context.tenant_id, verified_context.principal_id)` (self-subscription) OR the principal holds an explicit administrative scope (`notifications:subscribe:tenant` / `events:admin`).
  3. **Wildcard subscriptions (`notifications.*`, `notifications.r.*`):** Denied fail-closed unless an explicit administrative scope (`notifications:subscribe:tenant` / `events:admin`) is held.
  4. **Cross-recipient & Cross-tenant access:** Cross-recipient subscriptions are denied without admin scope; cross-tenant access is always denied (`VerifiedPrincipalContext.tenant_id` + tenant-isolated Redis keys).
  5. **Missing authorization hooks:** `create_gateway_app()` / `NotificationsModule` **must fail closed at startup** (`CoreConfigurationError`) if `enable_notifications=True` is configured without the recipient topic-authorization hook wired into `/ws/events/v1` / `EventBrokerPort`.

### 4.2 Producer Authorization Trust Boundary (`R3`)

A verified producer (`VerifiedPrincipalContext` or trusted host caller) **must not** automatically gain permission to target arbitrary recipients, even within the same tenant.

```text
[Verified Producer] ──▶ [NotificationProducerAuthorizerPort] ──▶ [NotificationsDispatcher]
(tenant_id, principal_id,   (host-app policy checks whether         (validates NotificationEnvelope,
 allowed_scopes)             producer may target recipient_id        checks prefs/expiry/frame size,
                             within tenant_id; fail-closed)          publishes to notifications.r.<token>)
```

- **`NotificationProducerAuthorizerPort`:** `async def authorize_publish(self, *, producer: VerifiedPrincipalContext | None, notification: NotificationEnvelope) -> None` (raises `NotificationAuthorizationError` on denial).
- **Zero `kaiwen-agent` Dependency:** The host application supplies its own `NotificationProducerAuthorizerPort` implementation at the composition root (`create_gateway_app()` / `NotificationsModule`), enforcing tenant-matching (`producer.tenant_id == notification.tenant_id` when `producer is not None`) and recipient-targeting rules without importing `kaiwen-agent` or external policy engines. Missing `NotificationProducerAuthorizerPort` fails closed when untrusted/external producers are enabled.

---

## 5. User Preferences, Future Channels & Transport Evaluation

### 5.1 User Preferences (`NotificationPreferencePort`)

- `NotificationPreference`: `tenant_id`, `recipient_id`, `muted_categories: frozenset[str]`, `min_priority: NotificationPriority = LOW`, `disabled_channels: frozenset[str]`.
- `NotificationPreferencePort`: `async def get_preferences(self, *, tenant_id: str, recipient_id: str) -> NotificationPreference | None`.
- **Missing vs. Failed Preferences (`R5`):**
  - Returning `None` (no preference record exists) defaults to **allow only for non-promotional (`is_promotional=False`) in-app notifications**; promotional notifications (`is_promotional=True`) fail closed (denied) unless explicitly opted in.
  - Preference-store I/O or backend **failures** raise `NotificationsPreferenceError` / `EventsBackendUnavailableError` (fail closed—**never** silently coerced to `None`).
- **Deferral:** Localization, templates, and digest batching remain application responsibilities outside `N1`–`N4`.

### 5.2 Future External Channels (`NotificationChannelPort`)

- Reserve interface-only `NotificationChannelPort` (`channel_id: str`, `async def send(self, notification: NotificationEnvelope) -> ChannelDeliveryResult`) for future Email, SMS, Push, and Webhooks; zero external providers are implemented in `N0`–`N4`.

### 5.3 Transport Evaluation: `/ws/events/v1` vs. Separate REST Endpoints

- **Decision:** Reuse `/ws/events/v1` (`events.delivery`) for `N1`–`N2` real-time delivery; defer separate Gateway REST endpoints (`POST /notifications`, `GET /notifications`, `POST .../ack`) so Gateway remains a modular library/transport host rather than owning product database/inbox state.

---

## 6. Security & Reliability Threat Model

| Threat | Impact | Mitigation (Fail-Closed) |
|---|---|---|
| **Cross-tenant notification leakage** | Tenant A reads Tenant B's notifications | `tenant_id` bound from `VerifiedPrincipalContext`; tenant-isolated Redis keys (`kw:evt:v1:{tenant_id}:...`, `kw:str:v1:{tenant_id}:...`) |
| **Unauthorized recipient targeting** | Compromised producer spams arbitrary users | `NotificationProducerAuthorizerPort` enforces host-app recipient-targeting policy fail-closed |
| **Unauthorized recipient subscription** | User A subscribes to User B's `notifications.r.*` | Mandatory `N2` topic-authorization hook denies `notifications.r.*` for general `events:subscribe`; self-subscription verified against `principal_id`; startup fails closed if hook missing |
| **Replay & duplicate delivery** | Duplicate side effects on redelivery | Single-use `?ticket=` `jti` cache (`E2`/`E3`); `notification_id` + `idempotency_key` consumer deduplication; explicit `at-least-once` documentation |
| **Notification flooding** | Resource exhaustion / noisy neighbor | `RedisDistributedRateLimiter` per-tenant/producer limits + `EventBrokerPort` `1 MiB`/conn & `32 MiB` global caps + `1008` disconnect |
| **Sensitive data exposure** | PII/secrets in logs, metrics, or errors | `NotificationsTelemetrySink` uses bounded-cardinality counters only (`category`, `priority`, `outcome`, `error_category`); never logs payloads, tokens, tickets, or `notification_id` metric labels |
| **Unbounded queues / oversized frames** | OOM or WS `1009` failures | Dual count+byte queue caps; `MAXLEN ~`; `<= 48 KiB` inner + `<= 65_536 B` final `events.delivery` frame check |
| **Redis disconnections** | Split-brain limits or silent drops | Fail closed (`EventsBackendUnavailableError`, `503` on `/ready`, `1011` on WS); no silent local downgrade |
| **Malformed notification payloads** | Worker crash or PEL wedge | Strict validation (`<= 48 KiB`, depth `<= 16`, finite numbers); malformed stream entries quarantined to DLQ and `XACK`ed (`N3`) |

---

## 7. Compatibility Assessment

- **Core (`kaiwen_gateway.core`):** Zero breaking changes; reuses `VerifiedPrincipalContext`, `GatewayModule`, `AdmissionController`, and `InstanceDrainCoordinator`.
- **Voice (`kaiwen_gateway.modules.voice`):** Completely isolated; audio hot path (`VoiceBridgeSession`) untouched.
- **Events (`E1`–`E4`):** Consumed via public ports (`EventBrokerPort`, `DurableEventStorePort`, `RateLimiterPort`) and `/ws/events/v1` with zero breaking changes. `N2` adds an optional, backward-compatible per-subscription topic-authorization hook in `/ws/events/v1` / `create_gateway_app()` (mandatory when `enable_notifications=True`); `N3` handles application-level poison `NotificationEnvelope` quarantine via an explicit `NotificationsDeadLetterSink` or `RedisStreamsDurableStore` DLQ hook.
- **Packaging:** `kaiwen_gateway.modules.notifications` ships inside `kaiwen-gateway` with `dependencies = []` unchanged; Redis features remain optional under `kaiwen-gateway[redis]`.

---

## 8. Bounded Implementation Roadmap (`N0`–`N4`)

| Milestone | Scope | Key Deliverables | Acceptance Criteria & Required Tests | Dependencies | Risks & Deferrals |
|---|---|---|---|---|---|
| **N0 — Architecture & Contracts** | Design, ADR-0007, threat model, compatibility & roadmap | `NOTIFICATIONS_MODULE_DESIGN.md`, `ADR-0007`, `ROADMAP.md` | Docs consistent, zero code/dep changes, maintainer ADR review | `E1`–`E4` Complete (`9e9af79`) | No code; REST API & external channels deferred |
| **N1 — Notification Core** | Domain types, `<= 48 KiB` + final-frame check helper, ports (`NotificationProducerAuthorizerPort`, `NotificationPreferencePort`, `NotificationChannelPort`), telemetry, `NotificationsModule` | `modules/notifications/` (`domain.py`, `ports.py`, `telemetry.py`, `module.py`) | Unit tests for validation, TTL, canonical topic hash, preference miss vs failure, zero-Redis import, `mypy --strict`, `ruff` | `N0` Accepted | Defer localization/templates & external providers |
| **N2 — Real-Time Delivery** | `NotificationsDispatcher` + mandatory recipient-scoped `/ws/events/v1` topic-authorization hook (`N2`) + producer auth wiring | `dispatcher.py`, `/ws/events/v1` topic-auth hook, loopback WS tests | Online recipient receives `notifications.v1`; cross-tenant/cross-recipient/wildcard denied; missing hook fails closed at startup; slow-consumer `1008` | `N1` Complete | Offline inbox deferred outside Gateway MVP |
| **N3 — Durable Delivery & Recovery** | Durable worker backed by `DurableEventStorePort` (`RedisStreamsDurableStore`, `StreamRecoveryWorker`), idempotent dedup & DLQ | `durable_worker.py`, live-Redis integration tests (crash/reclaim, poison DLQ, `expires_at`, outage) | Crash before `XACK` recovers via `XAUTOCLAIM`; `XACK` only after Stage 2+3 dispatch; poison entries quarantined; `0` silent loss within `MAXLEN` | `N2` Complete + `[redis]` | No offline per-user inbox; no external Email/SMS/Push/Webhook adapters |
| **N4 — Integration, Performance & Acceptance** | Multi-instance load qualification, failure-injection suite, docs & compatibility closeout | `tools/benchmark_notifications.py`, acceptance report, `PUBLIC_API_AND_COMPATIBILITY.md` | Reproducible p50/p95/p99 & memory bounds; Python 3.11–3.13 CI green; `0` P0/P1 findings | `N3` Complete | Gate D operational review required before production SLA |

---

## 9. Open Decisions Requiring Maintainer Approval

1. **ADR-0007 Acceptance:** Approve `src/kaiwen_gateway/modules/notifications/` boundaries, `/ws/events/v1` reuse (deferring REST endpoints), 6-stage ACK model, and `N1`–`N4` roadmap.
2. **Additive `N2` `/ws/events/v1` Topic-Authorization Hook:** Approve adding an optional per-subscription topic-authorization hook in `/ws/events/v1` / `create_gateway_app()` (mandatory/fail-closed when `enable_notifications=True`).
3. **Preference Miss Policy:** Confirm defaulting missing preferences (`None`) to allow only non-promotional in-app notifications (`is_promotional=False`), while failing closed on promotional notifications and preference-store errors.
4. **Offline Inbox & External Channel Deferrals:** Confirm keeping application-owned offline inbox persistence outside Gateway MVP and deferring Email/SMS/Push/Webhook adapters until post-`N4`.
