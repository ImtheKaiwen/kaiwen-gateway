# Kaiwen Gateway — Events Module Design Specification (`D0` Proposed Baseline)

> **Status:** `Proposed` (`D0` Architecture & Documentation Baseline — **NOT YET IMPLEMENTED**)
> **Baseline Reference:** `kaiwen-gateway v0.1.0b1` (`main` `91ee612fdfc179b94945ec9cde332748de57ea56`; **Voice** is the only implemented communication module)
> **Related ADR:** [ADR-0006 — Events Module Architecture (`Proposed`)](decisions/0006-events-module-architecture.md)
> **Related Docs:** [Architecture](ARCHITECTURE.md) · [Requirements](REQUIREMENTS.md) · [Roadmap](../ROADMAP.md) · [Public API Policy](PUBLIC_API_AND_COMPATIBILITY.md)

---

## 1. Goals and Non-Goals

### 1.1 Goals
1. Provide a reusable, provider-neutral **Events** module (`src/kaiwen_gateway/modules/events/`) inside the existing `kaiwen-gateway` modular monolith (`ADR-0001`).
2. Support real-time application event publishing, multi-tenant Pub/Sub fan-out, and authenticated WebSocket delivery (`/ws/events/v1`).
3. Enforce strict multi-tenant isolation, server-derived principal identity, channel access controls, configurable token-bucket rate limiting, and bounded per-subscriber backpressure.
4. Support zero-dependency in-process operation (`InMemoryEventBroker`) by default, plus optional distributed live fan-out (`RedisPubSubEventBroker`), optional durable stream processing (`RedisStreamsDurableStore`), and optional distributed rate limiting behind a future `kaiwen-gateway[redis]` extra.
5. Expose clean, stable contracts that future **Notifications** and **Agent Streaming** modules can consume without duplicating broker or transport logic.

### 1.2 Non-Goals (and D0 Boundaries)
- **No implementation during D0:** No Python source files, dependencies, Dockerfiles, or CI changes are introduced in D0.
- **No Voice audio hot-path routing:** Canonical 16 kHz PCM16LE `AudioFrame`s (`VoiceBridgeSession`) must **never** be routed through Events.
- **No universal Event Bus in `kaiwen_gateway.core`:** Core owns lifecycle, identity, admission, and shared contracts—not topic routing or pub/sub brokers.
- **No global ordering or exactly-once claims:** Live Pub/Sub is `at-most-once` with per-publisher/per-channel best-effort ordering; durable Redis Streams is `at-least-once` with idempotent consumer design.
- **No Notifications or Agent Streaming implementation:** Only consumption boundaries are defined.

---

## 2. Architecture, Packaging & Component Ownership

### 2.1 Packaging & Module Layout
Events lives inside the single `kaiwen-gateway` PyPI distribution:

```text
src/kaiwen_gateway/
├── core/                         # Zero-dep Core (registration, lifecycle, identity, admission)
├── modules/
│   ├── voice/                    # Implemented in v0.1.0b1 (isolated PCM16LE media hot path)
│   └── events/                   # Proposed (E1–E4):
│       ├── domain/               # EventEnvelope, Topic, EventError, limits, validation
│       ├── ports/                # EventPublisherPort, EventSubscriptionPort, DurableEventStorePort, RateLimiterPort, EventsTelemetrySink
│       ├── application/          # EventsModule (GatewayModule), EventsDispatcher, SubscriptionManager, TokenBucketRateLimiter
│       └── adapters/             # InMemoryEventBroker, RedisPubSubEventBroker (optional), RedisStreamsDurableStore (optional)
├── transports/
│   ├── synthetic/                # Implemented synthetic telephony/media transports
│   └── events/                   # Proposed (E2): /ws/events/v1 ASGI/WSS endpoint & frame codec
└── server/                       # Composition root (app.py, lifespan.py)
```

Optional dependencies (`redis>=5,<6` or similar) will be gated behind `kaiwen-gateway[redis]` in a future `E3` milestone. Importing `kaiwen_gateway`, `kaiwen_gateway.core`, or `kaiwen_gateway.modules.voice` must never require or import `redis`.

### 2.2 Dependency Diagram

```mermaid
flowchart TD
    WS["/ws/events/v1 Transport (transports/events)"] --> APP["Events Application & Subscriptions (modules/events/application)"]
    NOTIF["Future Notifications Module"] -.->|"EventPublisherPort / DurableEventStorePort"| PORTS["Events Ports (modules/events/ports)"]
    AGENT["Future Agent Streaming Adapter"] -.->|"EventPublisherPort / EventSubscriptionPort"| PORTS
    APP --> PORTS
    APP --> DOM["Events Domain (modules/events/domain)"]
    PORTS --> DOM
    INMEM["InMemoryEventBroker"] --> PORTS
    RPUB["RedisPubSubEventBroker (optional [redis])"] --> PORTS
    RSTR["RedisStreamsDurableStore (optional [redis])"] --> PORTS
    COMP["Composition Root (server/app.py)"] --> APP
    COMP --> INMEM
    COMP --> RPUB
    COMP --> RSTR
    APP --> CORE["Gateway Core (core/)"]
    WS --> CORE
    VOICE["Voice Module (modules/voice/)"] ---|"Isolated audio hot path (NO raw AudioFrame via Events)"| CORE
```

### 2.3 Ownership Matrix

| Layer | Owns | Must Not Own |
| --- | --- | --- |
| **Gateway Core (`core/`)** | `GatewayModule` registration (`ModuleRegistry`), `ModuleLifecycleManager`, `AdmissionController`, `ConnectionSupervisor`, `InstanceDrainCoordinator`, trusted identity primitives | Topic routing, JSON event envelopes, Pub/Sub brokers, Redis clients, WebSocket frame parsing |
| **Events Module (`modules/events/`)** | `EventEnvelope`, topic validation, `EventPublisherPort` / `EventSubscriptionPort` / `DurableEventStorePort`, subscriber registry, per-subscriber bounded queues, token-bucket rate limiting, `EventsTelemetrySink` | ASGI/Starlette WebSocket objects, raw HTTP headers, Voice PCM audio frames, notification templates, agent reasoning |
| **Events Transport (`transports/events/`)** | `/ws/events/v1` handshake, pre-upgrade/post-connect auth invocation, JSON frame size checks, command dispatch (`subscribe`, `unsubscribe`, `ping`), close codes | Direct Redis access, cross-tenant routing, trusted identity fabrication from client JSON |
| **Composition Root (`server/`)** | Concrete broker/store instantiation, Redis client lifecycle, `core.register_module(events_module)`, `gateway_lifespan` wiring | Business authorization rules or hidden global broker singletons |
| **Future Notifications / Agent Streaming** | Domain templates, delivery channels (email/push/webhook), LLM token/tool streaming logic | Re-implementing Pub/Sub fan-out, WebSocket subscription registries, or Redis Streams consumer plumbing |

---

## 3. Broker & Durable Store Abstractions (`modules/events/ports/`)

To avoid redundant interfaces while keeping live fan-out distinct from durable stream storage, the proposed port contracts are:

1. **`EventPublisherPort` (Live Fan-Out Write Side):**
   - `async def publish(self, event: EventEnvelope) -> PublishResult`
   - Publishes an immutable, already-authorized `EventEnvelope` to live subscribers matching `(event.tenant_id, event.topic)`.
   - Returns `PublishResult(accepted: bool, delivered_local_count: int, dropped_slow_subscribers: int)` without blocking on slow consumers.
2. **`EventSubscriptionPort` (Live Fan-Out Read Side):**
   - `async def subscribe(self, *, tenant_id: str, subscriber_id: str, topic_pattern: str, queue_capacity: int) -> EventSubscription`
   - `async def unsubscribe(self, *, tenant_id: str, subscriber_id: str, topic_pattern: str | None = None) -> None`
   - Returns an `EventSubscription` handle exposing `async def receive(self) -> EventEnvelope | None` (bounded `asyncio.Queue` backed) and `async def close() -> None`.
3. **`EventBrokerPort` (Unified Live Broker Protocol):**
   - Structural `Protocol` combining `EventPublisherPort` and `EventSubscriptionPort` plus `async def startup() -> None` and `async def shutdown() -> None` (satisfied by `InMemoryEventBroker` and `RedisPubSubEventBroker`).
4. **`DurableEventStorePort` (Optional Append-Only Log & Replay):**
   - `async def append(self, event: EventEnvelope) -> str` (returns stream entry ID).
   - `async def read_group(self, *, tenant_id: str, topic: str, group: str, consumer: str, count: int, block_ms: int) -> list[DurableEventRecord]`
   - `async def ack(self, *, tenant_id: str, topic: str, group: str, record_ids: Sequence[str]) -> int`
   - `async def replay(self, *, tenant_id: str, topic: str, from_id: str = "0-0", limit: int = 100) -> list[DurableEventRecord]`

### 3.1 Delivery & Ordering Semantics Comparison

| Backend / Port | Purpose | Delivery Guarantee | Ordering Guarantee | Offline Replay |
| --- | --- | --- | --- | --- |
| `InMemoryEventBroker` | Single-process live fan-out | **At-most-once** | Per-publisher FIFO within a single topic | None |
| `RedisPubSubEventBroker` | Multi-instance live fan-out | **At-most-once** | Per-publisher FIFO on a single Redis channel; no cross-instance global order | None |
| `RedisStreamsDurableStore` | Durable event log & worker groups | **At-least-once** (with explicit `ack`) | Total order within a single Redis Stream key `(tenant_id, topic)` | Bounded by `MAXLEN` retention |

---

## 4. Event Envelope & `/ws/events/v1` Protocol

### 4.1 Canonical `EventEnvelope` (JSON Schema)

```json
{
  "protocol_version": "v1",
  "event_id": "evt_01JCH9W8K2M3N4P5Q6R7S8T9VW",
  "event_type": "agent.turn.completed",
  "topic": "agent.session.sess_42",
  "timestamp": "2026-11-17T20:00:00.123456Z",
  "tenant_id": "tenant_alpha",
  "principal_id": "svc_kaiwen_agent",
  "correlation_id": "corr_9f8e7d6c",
  "payload": {
    "session_id": "sess_42",
    "status": "completed"
  }
}
```

- **Canonical `event_type` naming:** Lowercase dot-separated ASCII slug `^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*){1,4}$` (max 96 chars, e.g. `voice.call.ended`, `notifications.alert.created`).
- **Topic naming & validation:** `^[a-z0-9][a-z0-9._-]{0,127}$` (wildcard subscriptions, if explicitly authorized by scope, use trailing `.*` only; bare `*` is forbidden for public clients). Internal routing keys are **always** prefixed by server-verified `tenant_id` (e.g., `kw:evt:v1:{tenant_id}:{topic}`).
- **Serialization & Size Limits:** UTF-8 JSON only (`allow_nan=False`), max serialized frame size `65,536` bytes (`64 KiB`, provisional), max JSON nesting depth `16`.
- **Schema Compatibility:** Additive optional fields allowed within `"v1"`; breaking envelope changes require `"v2"` (`/ws/events/v2`).
- **Unknown Version / Malformed / Oversized Handling:**
  - Unknown `protocol_version` -> error frame `{"type": "error", "code": "UNSUPPORTED_VERSION", ...}` and close `1008` (or reject command).
  - Malformed JSON / invalid schema -> error frame `{"type": "error", "code": "INVALID_FRAME", ...}` and close `1007`.
  - Oversized frame (`> 65,536` bytes) -> close `1009` (`Message Too Big`) before full JSON parsing.
- **Duplicate Event Handling:** `event_id` is globally unique per published event (UUIDv7/ULID). At-least-once durable consumers must deduplicate by `event_id` within their domain handler.

### 4.2 `/ws/events/v1` Wire Messages (Client Commands vs. Server Events)

Client-to-server frames (`command` / `type` namespace) are strictly separated from server-to-client event deliveries:

| Direction | `type` | Required Fields | Behavior |
| --- | --- | --- | --- |
| **Server -> Client** | `session.ready` | `protocol_version`, `connection_id`, `tenant_id`, `principal_id`, `allowed_scopes` | Sent immediately after authenticated upgrade. |
| **Client -> Server** | `events.subscribe` | `request_id`, `topic` | Checks scope & subscription quota; binds to verified `tenant_id`; replies `events.subscribed` or `error`. |
| **Client -> Server** | `events.unsubscribe` | `request_id`, `topic` | Removes subscription and cleans queue state; replies `events.unsubscribed`. |
| **Client -> Server** | `session.ping` | `seq` (optional) | Replies `session.pong` (`seq` echoed). |
| **Server -> Client** | `events.delivery` | `request_id` (optional), `event` (`EventEnvelope`) | Delivers matching tenant-scoped event from subscriber queue. |
| **Server -> Client** | `events.error` | `request_id` (optional), `code`, `message` | Sanitized diagnostic error (no secrets, stack traces, or cross-tenant details). |
| **Client -> Server** | `events.publish` | *(Disabled for public clients by default)* | Rejected with `FORBIDDEN_PUBLISH` (`1008`) unless explicitly enabled for a trusted scope (`events:publish:<topic>`). |

---

## 5. Security Model & Core Identity Gap Resolution

### 5.1 Fail-Closed Trust Boundaries
1. **Authentication Before Privileged Operations:** `/ws/events/v1` authenticates bearer credentials (e.g., `Authorization: Bearer <token>`) before WebSocket `accept()` or before admitting any subscription.
2. **Server-Derived Tenant & Principal:** `tenant_id` and `principal_id` come **exclusively** from the verified server token/authenticator. Any `tenant_id` or `principal_id` inside a client JSON payload is ignored or rejected on mismatch.
3. **Channel & Wildcard Authorization:** Subscriptions require explicit scopes (e.g., `events.subscribe.<prefix>` or `events.subscribe`). Wildcard patterns (`topic.*`) require an explicit wildcard-capable scope and are denied by default.
4. **Trusted Server-Side Publishing Preference:** Public WebSocket clients cannot publish arbitrary events merely by holding an active connection; publishing is performed by trusted server-side modules (Voice lifecycle hooks, Notifications, Agent Streaming) via `EventPublisherPort`.
5. **Redaction & Resource Exhaustion Resistance:** Payloads are size-capped, depth-capped, and never logged verbatim by default (`SEC-006`). Error responses use fixed sanitized codes.

### 5.2 Audit of Existing Core Identity (`src/kaiwen_gateway/core/identity.py`)
- **Constraint Discovered:** Current `TenantCallBinding` and `VerifiedConnectionContext` require a non-empty `call_id` (`validate_call_id`). `AdmissionController` and `ConnectionSupervisor` accept `VerifiedConnectionContext`.
- **D0 Resolution Proposal (for `E1`/`E2`, no Core changes in D0):**
  - **Do NOT** invent synthetic `call_id`s (such as `"events-none"`) and **do NOT** make `call_id` optional on `TenantCallBinding` (which would weaken Voice's compile-time/runtime invariants).
  - Instead, introduce an **additive** `TenantPrincipalBinding` and `VerifiedPrincipalContext` (with `connection_id`, `authorized_provider_id`, `tenant_id`, `principal_id`, `allowed_scopes`) in `kaiwen_gateway.core.identity`, or a shared `AdmissibleContext` protocol satisfied by both `VerifiedConnectionContext` (Voice) and `VerifiedPrincipalContext` (Events).

---

## 6. Rate Limiting, Capacity & Backpressure

### 6.1 Provisional Limits & Distinction from `AdmissionController`
`AdmissionController` enforces **concurrent connection leases** (`max_concurrent_connections`, `max_connections_per_tenant`). It does **not** enforce per-second message or publish rates. Events adds explicit rate and capacity controls (**all numerical defaults below are provisional until `E4` benchmark qualification**):

| Control Dimension | Scope | Provisional Default | Rejection / Overflow Behavior |
| --- | --- | --- | --- |
| Concurrent Connections | Global / Tenant (`AdmissionController`) | Inherited from `CoreConfig` (`25` / `10`) | Reject handshake (`403` / `1008`) |
| Client Control Message Rate | Per-connection token bucket | `20 msg/s`, burst `40` | `events.error` (`RATE_LIMITED`); close `1008` on repeated abuse |
| Publish Rate | Per-tenant / Per-principal token bucket | `200 evt/s`, burst `400` | Reject `publish()` with `EventRateLimitError` (increment rejection metric) |
| Subscription Operations | Per-connection token bucket | `10 ops/s`, burst `20` | `events.error` (`RATE_LIMITED`) |
| Max Subscriptions per Connection | Per-connection | `32` | `events.error` (`SUBSCRIPTION_LIMIT_EXCEEDED`) |
| Max Payload / Frame Size | Per-frame | `65,536` bytes (`64 KiB`) | Close `1009` (`Message Too Big`) |
| Subscriber Queue Capacity | Per-subscriber `asyncio.Queue` | `128` events | Overflow policy: `disconnect_slow_consumer` (close `1008`) or `drop_oldest` (explicitly configured per profile); **never unbounded** |
| Idle Timeout | Per-connection | `60.0 s` without ping/activity | Server closes `1000` (`Idle timeout`) |

### 6.2 Process-Local vs. Distributed Rate Limiting
- **Process-local (`E1`/`E2`):** In-memory token bucket (`TokenBucketRateLimiter`) with zero external dependencies. Applies per worker process.
- **Distributed (`E3`, optional `[redis]`):** Lua/atomic token-bucket or sliding-window counters in Redis keyed by `kw:rl:v1:{tenant_id}:{dimension}`. Redis does **not** automatically enforce limits without explicit adapter wiring; if Redis is unavailable, tenant-facing privileged operations fail closed (or fall back to stricter local caps only when explicitly configured).

### 6.3 Backpressure & Lifecycle Integration
- **Slow-Consumer Isolation:** Broker `publish()` enqueues into each matching subscriber's bounded `asyncio.Queue` via `put_nowait()`. One slow subscriber never blocks `publish()` or stalls other subscribers/tenants.
- **Module Lifecycle:** `EventsModule` implements `GatewayModule` (`module_id="events"`, `contract_version="1.0"`). `ModuleLifecycleManager` starts the broker on ASGI startup and drains/closes active subscriptions and Redis pools on shutdown before `gateway_lifespan` exits.

---

## 7. Optional Redis Architecture (`kaiwen-gateway[redis]`)

1. **Redis Pub/Sub (`RedisPubSubEventBroker`):**
   - Cross-instance live fan-out on `kw:evt:v1:{tenant_id}:{topic}`.
   - **At-most-once**, no offline replay. Dedicated background listener task with bounded exponential-backoff reconnect (`100 ms` -> `5 s` cap) and health status reporting.
2. **Redis Streams (`RedisStreamsDurableStore`):**
   - Append-only durable log (`XADD` with `MAXLEN ~ 10000` provisional retention cap) + consumer groups (`XREADGROUP` / `XACK`).
   - **At-least-once** delivery; does **not** provide exactly-once processing (consumers must handle redelivery idempotently) and does **not** replace per-WebSocket bounded memory queues.
3. **Unavailability & Degraded Mode:**
   - Core and in-memory Events work with zero Redis dependency.
   - When Redis is configured and becomes unreachable: `publish()` / `append()` raise `EventsBackendUnavailableError` (fail closed for durable writes; live WebSocket clients receive sanitized `BACKEND_UNAVAILABLE` and clean close if unrecoverable).

---

## 8. Observability (`EventsTelemetrySink`)

Auditing `src/kaiwen_gateway/` confirms that only `modules/voice/telemetry.py` exists today (`VoiceTelemetrySink`, `InMemoryVoiceTelemetrySink`); Gateway Core does **not** yet provide a general-purpose structured logger or shared metric registry.

- **D0/E1 Design:** Mirror Voice's clean sink pattern with `EventsTelemetrySink` and `InMemoryEventsTelemetrySink` inside `modules/events/ports/` & `telemetry.py`:
  - **Metrics:** `active_connections`, `active_subscriptions`, `events_published_total`, `events_delivered_total`, `events_dropped_total` (overflow), `rate_limit_rejections_total`, `redis_errors_total`, `subscriber_queue_depth` / high-water mark, `delivery_latency_ms` (`BoundedLatencyAggregator`), and `shutdown_cleanup_failures_total`.
  - **Cardinality & Privacy Rules:** Metric labels must **never** include unbounded `event_id`, `topic`, `connection_id`, or `payload` values; aggregate by bounded `outcome` / `error_category`. Structured diagnostic logs carry only safe correlation IDs (`tenant_id`, `connection_id`, `event_id`, `event_type`), never raw event payloads or tokens.
  - **Shared Core Prerequisite (Separate Scope):** Any cross-module Core structured logger or OpenTelemetry exporter belongs in a separate reviewed Core observability package, not implemented in D0.

---

## 9. Testing Strategy, Deployment Limitations & Open Questions

### 9.1 Testing Strategy (for `E1`–`E4`)
- **E1 (Unit & Broker Conformance):** Deterministic `InMemoryEventBroker` tests, tenant isolation, slow-consumer overflow, token-bucket rate limiter, `ModuleLifecycleManager` startup/shutdown, and zero-Redis import isolation test.
- **E2 (WebSocket & Security):** Loopback `/ws/events/v1` tests covering pre-auth rejection (`403`/`1008`), forged `tenant_id` rejection, unauthorized wildcard rejection, `1009` oversized rejection, subscription cap, and drain cleanup.
- **E3 (Optional Redis):** Mocked + opt-in local Redis integration tests for Pub/Sub fan-out, Streams `XREADGROUP`/`XACK`/replay, reconnect recovery, and fail-closed outage behavior.
- **E4 (Qualification):** Multi-process paced load harness measuring p50/p95/p99 delivery latency, overflow behavior, and memory bounds to replace provisional defaults.

### 9.2 Deployment Limitations & Operational Risks
- Without Redis (`[redis]` extra), Events state (subscriptions, in-memory broker, token buckets) is **process-local** to a single ASGI worker.
- Redis Streams retention (`MAXLEN`) must be tuned per deployment to prevent Redis memory exhaustion under consumer outages.

### 9.3 Open Questions Requiring Maintainer Approval
1. **Identity Generalization (`E1`/`E2`):** Approve adding `TenantPrincipalBinding` / `VerifiedPrincipalContext` in `kaiwen_gateway.core.identity` alongside `TenantCallBinding`.
2. **Default Overflow Policy:** Confirm `disconnect_slow_consumer` (`1008`) as the default WebSocket slow-consumer policy vs. `drop_oldest`.
3. **Provisional Rate-Limit Defaults:** Approve the provisional numbers in §6.1 as starting baselines subject to `E4` benchmarking.
4. **Core Ingress Config (`E2`):** Approve how `CoreConfig` or `create_gateway_app(..., enable_events=True)` enables `/ws/events/v1` without loosening Voice's fail-closed `ingress_mode="disabled"` default.
