# ADR-0006: Adopt a Bounded, Port-Driven Events Module Architecture (`Proposed`)

- **Status:** Proposed
- **Date:** 2026-11-17
- **Decision owners:** Kaiwen Gateway maintainers
- **Scope:** Post-`v0.1.0b1` Events communication module (`src/kaiwen_gateway/modules/events/`), optional Redis infrastructure (`kaiwen-gateway[redis]`), and `/ws/events/v1` WebSocket transport
- **Related decisions:** [ADR-0001 — Modular Monolith](0001-modular-monolith.md) · [ADR-0002 — Hexagonal Architecture](0002-hexagonal-architecture.md) · [ADR-0003 — Library and Server](0003-library-and-server.md) · [ADR-0004 — Synthetic Binary WebSocket Media](0004-synthetic-binary-websocket-media.md)
- **Related documents:** [Events Module Design](../EVENTS_MODULE_DESIGN.md) · [Architecture](../ARCHITECTURE.md) · [Requirements](../REQUIREMENTS.md) · [Roadmap](../../ROADMAP.md)

> **Decision status note:** This ADR is **`Proposed`** for architectural review during milestone **D0**. It does **not** authorize implementation of Events runtime code, Redis dependencies, or new package extras until explicitly accepted by the maintainers.

---

## 1. Context

Kaiwen Gateway `v0.1.0b1` establishes the Modular Monolith + Hexagonal Architecture baseline with **Voice** (`kaiwen_gateway.modules.voice`) as the only implemented communication module. As the platform expands beyond bidirectional PCM audio calls toward general real-time application events, future **Notifications**, and future **Agent Streaming**, the repository needs an explicit architectural boundary for a general-purpose **Events** module.

Auditing the current `v0.1.0b1` codebase reveals several concrete constraints that shape this decision:

1. **Voice hot-path isolation (`ADR-0001` §2.8, `ADR-0002` §1):** Voice streams canonical `16,000 Hz` mono `pcm16le` `AudioFrame`s (`640` bytes / 20 ms) through dedicated bounded in-memory queues (`VoiceBridgeSession`). Routing raw audio frames through a JSON event bus would violate latency, memory, and CPU constraints.
2. **Core zero-dependency invariant (`pyproject.toml`, `ADR-0003`):** The base `kaiwen-gateway` distribution has `dependencies = []`. Optional infrastructure such as `redis` must remain an optional extra (proposed `kaiwen-gateway[redis]`) and must never be required to import or run `kaiwen_gateway.core` or `kaiwen_gateway.modules.voice`.
3. **Call-oriented Core identity (`src/kaiwen_gateway/core/identity.py`):** Existing `TenantCallBinding` and `VerifiedConnectionContext` require a non-empty `call_id`. General Events WebSocket connections and server-side publishers are tenant- and principal-scoped, not telephone calls, and must not fabricate synthetic `call_id`s or weaken Voice's mandatory `call_id` invariant.
4. **Connection admission vs. message rate limiting (`src/kaiwen_gateway/core/admission.py`):** `AdmissionController` enforces process-local concurrent connection ceilings (`max_concurrent_connections`, `max_connections_per_tenant`). It does **not** perform per-second message, publish, or subscribe rate limiting.
5. **Module-local observability (`src/kaiwen_gateway/modules/voice/telemetry.py`):** The codebase currently provides Voice-specific telemetry (`VoiceTelemetrySink`, `InMemoryVoiceTelemetrySink`) rather than a general-purpose Core structured logger or shared metric registry.

---

## 2. Decision Proposal

Adopt an independently bounded **Events** communication module under `src/kaiwen_gateway/modules/events/` inside the existing `kaiwen-gateway` modular-monolith distribution, following Hexagonal Architecture (`ADR-0002`):

1. **Single Distribution, Bounded Module:**
   - Package Events inside `kaiwen-gateway` under `src/kaiwen_gateway/modules/events/` (no second PyPI package).
   - Keep `redis` strictly optional behind a future `kaiwen-gateway[redis]` extra (never added in D0; never imported by `core` or `modules.voice`).
2. **No Universal Event Bus in Core:**
   - `kaiwen_gateway.core` remains unaware of Events topics, JSON envelopes, pub/sub brokers, and Redis.
   - Events registers with `GatewayCore` as a `GatewayModule` (`module_id="events"`, `contract_version="1.0"`) so `ModuleLifecycleManager` and `gateway_lifespan` manage bounded startup/shutdown and drain safety uniformly.
3. **Voice Hot-Path Preservation:**
   - Raw audio frames (`AudioFrame`) remain strictly on `VoiceBridgeSession` and are **never** routed through Events. Voice may optionally emit low-frequency lifecycle notifications only if explicitly configured at the composition root.
4. **Focused Broker & Durable Store Ports:**
   - Separate live ephemeral fan-out (`EventPublisherPort`, `EventSubscriptionPort`, unified `EventBrokerPort` capability) from optional durable stream storage (`DurableEventStorePort` with explicit consumer-group read, `ack`, and bounded replay).
   - Provide an in-process `InMemoryEventBroker` default (zero external dependencies) alongside optional `RedisPubSubEventBroker` (live cross-instance fan-out, at-most-once) and `RedisStreamsDurableStore` (append-only durable log, at-least-once with explicit `XACK`).
   - Do **not** claim global cross-topic ordering or exactly-once delivery.
5. **Additive Generalized Principal Identity Integrated with Admission & Lifecycle (for `E1`/`E2`):**
   - Preserve `TenantCallBinding` and `VerifiedConnectionContext` unchanged for Voice (`call_id` remains mandatory for telephony/voice).
   - Introduce `TenantPrincipalBinding` and `VerifiedPrincipalContext` in `kaiwen_gateway.core.identity` alongside a closed two-type union / `VerifiedConnectionIdentity` structural contract accepted by `AdmissionController`, `AdmissionLease`, and `ConnectionSupervisor`. This ensures Events WebSocket connections share global/per-tenant capacity leases and `InstanceDrainCoordinator` lifecycle supervision without fabricating `call_id`s.
6. **Fail-Closed Ingress, Security & Trusted Server-Side Publishing:**
   - `/ws/events/v1` requires explicit authenticated ingress configuration (`enable_events=True` alone must fail closed if `ingress_mode="disabled"` or no authenticator/verifier is configured).
   - Public WebSocket clients are authenticated pre-accept, bound strictly to server-verified `tenant_id` and `principal_id`, and denied arbitrary client publishing or unauthorized wildcard subscriptions by default.
   - Future **Notifications** and **Agent Streaming** consume Events through typed port contracts (`EventPublisherPort`, `EventSubscriptionPort`, `DurableEventStorePort`) injected at the composition root without duplicating broker logic.
7. **Provisional Rate Limiting, Byte-Bounded Queues & Explicit Overload Semantics:**
   - Use **one** bounded outbound queue per subscriber connection (multiplexed across up to `32` subscriptions) enforcing **both** event count (`128`) and byte limits (`1 MiB` per connection, `32 MiB` global queued-byte budget) with `disconnect_slow_consumer` (`1008`) as the default slow-consumer policy.
   - Separate process-local token-bucket rate limiting from optional Redis distributed counters; treat all initial numerical defaults as **provisional** until `E4` benchmark evidence is collected.
   - Separate **E3** (Redis Pub/Sub cross-instance live fan-out, distributed rate limiting, Redis connection lifecycle) from **E4** (Redis Streams durable store, PEL `XPENDING`/`XAUTOCLAIM` recovery, retention/trimming, durable delivery qualification, load/failure testing).

---

## 3. Alternatives Considered

| Option | Benefits | Costs and Risks | Outcome |
| --- | --- | --- | --- |
| **A. Bounded `modules/events/` inside `kaiwen-gateway` with optional `[redis]` extra (Proposed)** | Preserves `ADR-0001` modular monolith, zero mandatory core deps, clean port isolation, shared `GatewayCore` lifecycle/admission, direct reuse by future Notifications & Agent Streaming | Requires disciplined import boundaries and additive principal identity in Core (`E1`/`E2`) | **Proposed** |
| **B. Separate PyPI package (`kaiwen-events`)** | Independent release cadence | Duplicates Core identity, admission, draining, ASGI lifespan, and synthetic test infrastructure; violates `ADR-0001` single-distribution policy | **Rejected** |
| **C. Universal Event Bus inside `kaiwen_gateway.core`** | Any module can publish/subscribe without wiring | Bloats `core` with topic routing, pub/sub, and serialization concerns; risks coupling Voice media hot path to generic event machinery | **Rejected** |
| **D. Mandatory Redis / Redis Streams for all Events & WebSocket queues** | Single distributed backend | Breaks zero-dependency local/embedded library usage; adds network hop and operational dependency even for single-process deployments; Redis Streams is not a drop-in replacement for per-WebSocket bounded memory queues | **Rejected** |

---

## 4. Rationale & Tradeoffs

- **Hexagonal Separation:** Defining `EventPublisherPort`, `EventSubscriptionPort`, and `DurableEventStorePort` inside `modules/events/ports/` ensures the Events domain and `/ws/events/v1` transport never import `redis.asyncio` directly. Unit and conformance tests can run deterministically in-memory with zero external services.
- **Honest Delivery Semantics:** Splitting live fan-out (`InMemoryEventBroker` / `RedisPubSubEventBroker` -> **at-most-once**, no offline replay) from durable storage (`RedisStreamsDurableStore` -> **at-least-once** with consumer groups and explicit `XACK`, consumers must be idempotent) prevents false reliability claims.
- **Tradeoff — Process-Local vs. Distributed Limits:** Process-local token buckets and `AdmissionController` enforce limits with zero network latency, whereas Redis-backed distributed rate limiting adds a network RTT and failure-mode policy (fail-closed for privileged operations or bounded degraded mode). Making Redis optional lets deployments choose the appropriate tradeoff.

---

## 5. Security, Performance, Migration & Operational Implications

- **Security:** Enforces pre-operation authentication, server-derived `tenant_id` / `principal_id`, per-channel/topic scope checks, explicit denial of unauthorized wildcards, strict JSON payload/size bounds, and secret/payload redaction in logs and error responses.
- **Performance:** Voice media pumps remain isolated from Events. Per-subscriber bounded queues isolate slow WebSocket consumers. Redis Pub/Sub and Redis Streams I/O occur off the Voice event path.
- **Migration & Compatibility:** No breaking changes to existing `v0.1.0b1` Core, Voice, Server, or OpenAI Adapter APIs. Core identity generalization in `E1`/`E2` must be strictly **additive** so existing `TenantCallBinding` callers are unaffected.
- **Operational Risks:** Redis unavailability, split-brain rate limits across workers without Redis, and unbounded Redis Stream growth without explicit `MAXLEN` trimming policies (addressed in [Events Module Design §11–§12](../EVENTS_MODULE_DESIGN.md)).

---

## 6. Conditions for Acceptance

Before transitioning this ADR from `Proposed` to `Accepted` and authorizing `E1`:

1. Maintainers approve the `src/kaiwen_gateway/modules/events/` package boundary and optional `kaiwen-gateway[redis]` extra strategy.
2. Maintainers approve the additive Core principal-identity approach (`TenantPrincipalBinding` / `VerifiedPrincipalContext` without weakening `TenantCallBinding.call_id` for Voice).
3. Maintainers approve the separation between live fan-out (`at-most-once`) and durable Redis Streams (`at-least-once`), plus the default prohibition on untrusted client-side WebSocket publishing.
4. Maintainers review and accept the provisional rate-limit/backpressure defaults and the `D0`–`E4` milestone plan in [`ROADMAP.md`](../../ROADMAP.md).
