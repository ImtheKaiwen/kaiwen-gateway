# ADR-0007: Adopt a Bounded, Events-Backed Notifications Module Architecture

- **Status:** Proposed
- **Date:** 2026-11-18
- **Decision owners:** Kaiwen Gateway maintainers
- **Scope:** Post-`E4` Notifications module (`src/kaiwen_gateway/modules/notifications/`), reusing Core identity/lifecycle and Events (`EventBrokerPort`, `DurableEventStorePort`, `/ws/events/v1`, `[redis]`)
- **Related decisions:** [ADR-0001 — Modular Monolith](0001-modular-monolith.md) · [ADR-0002 — Hexagonal Architecture](0002-hexagonal-architecture.md) · [ADR-0003 — Library and Server](0003-library-and-server.md) · [ADR-0006 — Events Module Architecture](0006-events-module-architecture.md)
- **Related documents:** [Notifications Module Design](../NOTIFICATIONS_MODULE_DESIGN.md) · [Events Module Design](../EVENTS_MODULE_DESIGN.md) · [Architecture](../ARCHITECTURE.md) · [Roadmap](../../ROADMAP.md)

> **Decision status note:** This ADR is **`Proposed`** (`N0` Architecture & Documentation Baseline). Acceptance applies to the architectural design only; implementation milestones **N1–N4 remain `Not started`** until separately authorized.

---

## 1. Context

With Events milestones `D0`–`E4` completed on `main` (`9e9af79b953b1b10ed4b977ebb3a35cf27a334c0`), Kaiwen Gateway now provides:
- Additive principal/tenant identity (`VerifiedPrincipalContext`, `TenantPrincipalBinding`, `PrincipalAdmissionLease`).
- Bounded live fan-out (`EventBrokerPort`, `InMemoryEventBroker`, `RedisPubSubEventBroker`).
- Authenticated `/ws/events/v1` WebSocket ingress (`Bearer` + single-use `Origin`-checked `?ticket=`).
- Optional Redis Streams durability (`DurableEventStorePort`, `RedisStreamsDurableStore`, `StreamRecoveryWorker`).

Building **Notifications** directly on top of these verified primitives avoids duplicating Redis clients, WebSocket servers, rate limiters, or connection admission logic, while preserving Voice's isolated audio hot path and the base package's `dependencies = []` invariant.

---

## 2. Decision Proposal

Adopt a bounded **Notifications** module under `src/kaiwen_gateway/modules/notifications/` following Hexagonal Architecture (`ADR-0002`):

1. **Reuse Events & Core Ports (Zero Infrastructure Duplication):**
   - `NotificationsModule` implements Core's `GatewayModule` and consumes `EventBrokerPort`, `DurableEventStorePort`, `RateLimiterPort`, and `VerifiedPrincipalContext` without importing `redis.asyncio` or creating a second WebSocket server.
2. **Reuse `/ws/events/v1` for Real-Time In-App Delivery (`N2`); Defer REST Endpoints:**
   - Deliver real-time `notifications.v1` envelopes over `/ws/events/v1` (`events.delivery`) using server-derived, recipient-scoped topics (`notifications.r.<token>`, 24-hex SHA-256 prefix) and an additive, backward-compatible per-subscription topic-authorization hook in `/ws/events/v1` (`N2`) so `"events:subscribe"` cannot subscribe to another recipient's `notifications.r.*` topic.
   - Bound `NotificationEnvelope` serialized size to `<= 48 KiB` so the outer `EventEnvelope` + `events.delivery` frame fits within `64 KiB` (`65_536` bytes).
   - Defer dedicated Gateway REST endpoints (`POST /notifications`, `GET /notifications`, `POST .../ack`) so Gateway remains a modular library/transport host rather than owning product database/pagination state.
3. **Strict 5-Stage Acknowledgment Separation:**
   - Explicitly distinguish: (1) Event publication, (2) Notification processing, (3) Transport delivery, (4) Client acknowledgment, and (5) Durable processing acknowledgment (`XACK`).
   - `XACK` (`DurableEventStorePort.ack()`) is issued **only** after downstream notification processing/dispatch completes—never on `XREADGROUP` or WebSocket queue enqueue.
4. **Honest Delivery Semantics:**
   - Live in-app delivery over `EventBrokerPort` (`N2`) is **at-most-once** for online recipients.
   - Durable processing over `RedisStreamsDurableStore` (`N3`) is **at-least-once** within `MAXLEN` retention and Redis persistence guarantees, requiring idempotent consumers keyed by `notification_id` / `idempotency_key`.
5. **Minimal Preference & Channel Contracts:**
   - Include a lightweight `NotificationPreferencePort` (muted categories, minimum priority, channel opt-outs) and reserve an interface-only `NotificationChannelPort` for future Email/SMS/Push/Webhook adapters without implementing external providers in `N1`–`N4`.
6. **Fail-Closed Security & Bounded Observability:**
   - Derive `tenant_id` and `recipient_id` authorization strictly from `VerifiedPrincipalContext`; deny cross-tenant and unauthorized cross-recipient subscriptions; enforce `expires_at` before delivery; use bounded-cardinality telemetry (`NotificationsTelemetrySink`) without logging raw payloads, tokens, or tickets.

---

## 3. Alternatives Considered

| Alternative | Pros | Cons | Verdict |
|---|---|---|---|
| **A. Bounded `modules/notifications/` reusing Events & `/ws/events/v1` (Proposed)** | Zero Redis/WS duplication; clean `GatewayModule` lifecycle; preserves `dependencies = []` and Voice isolation | Requires deterministic mapping from `PrincipalId` (which allows `:`) to `TOPIC_PATTERN` | **Proposed** |
| **B. Separate `/ws/notifications/v1` WebSocket server** | Custom notification-only wire frames | Duplicates auth, `?ticket=` replay cache, `Origin` checks, `1008`/`1009` framing, and drain logic | **Rejected** |
| **C. Mandatory Gateway REST + SQL notification inbox** | Built-in REST inbox for apps | Forces database/ORM dependencies into Gateway Core and blurs app vs. gateway ownership | **Rejected for MVP** |

---

## 4. Consequences & Conditions for Acceptance

- **Positive:** Fast, low-risk `N1`–`N4` delivery with zero new runtime dependencies and full reuse of `E1`–`E4` hardening.
- **Tradeoffs:** Offline catch-up relies on `N3` durable replay within `MAXLEN` or consuming-application state; external Email/SMS/Push/Webhook adapters remain future work beyond `N4`.
- **Acceptance Conditions:** Maintainer approval of `docs/NOTIFICATIONS_MODULE_DESIGN.md`, recipient-topic derivation rules, preference-miss default (`allow` vs. `deny`), and the `N1`–`N4` milestone plan in `ROADMAP.md`.
