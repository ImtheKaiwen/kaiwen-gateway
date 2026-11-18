# ADR-0007: Adopt a Bounded, Events-Backed Notifications Module Architecture

- **Status:** Accepted
- **Date:** 2026-11-18 (accepted 2026-11-18)
- **Decision owners:** Kaiwen Gateway maintainers
- **Scope:** Post-`E4` Notifications module (`src/kaiwen_gateway/modules/notifications/`), reusing Core identity/lifecycle and Events (`EventBrokerPort`, `DurableEventStorePort`, `/ws/events/v1`, `[redis]`)
- **Related decisions:** [ADR-0001 — Modular Monolith](0001-modular-monolith.md) · [ADR-0002 — Hexagonal Architecture](0002-hexagonal-architecture.md) · [ADR-0003 — Library and Server](0003-library-and-server.md) · [ADR-0006 — Events Module Architecture](0006-events-module-architecture.md)
- **Related documents:** [Notifications Module Design](../NOTIFICATIONS_MODULE_DESIGN.md) · [Events Module Design](../EVENTS_MODULE_DESIGN.md) · [Architecture](../ARCHITECTURE.md) · [Roadmap](../../ROADMAP.md)

> **Decision status note:** This ADR was **accepted** by the maintainer on 2026-11-18 as the architectural baseline for the post-`E4` Notifications track (**`N0` Complete**). Acceptance applies to the architectural design only; implementation milestones **N1–N4 remain `Not started`** until separately authorized.

---

## 1. Context

With Events milestones `D0`–`E4` completed on `main` (`9e9af79b953b1b10ed4b977ebb3a35cf27a334c0`), Kaiwen Gateway provides:
- Additive principal/tenant identity (`VerifiedPrincipalContext`, `TenantPrincipalBinding`, `PrincipalAdmissionLease`).
- Bounded live fan-out (`EventBrokerPort`, `InMemoryEventBroker`, `RedisPubSubEventBroker`).
- Authenticated `/ws/events/v1` WebSocket ingress (`Bearer` + single-use `Origin`-checked `?ticket=`).
- Optional Redis Streams durability (`DurableEventStorePort`, `RedisStreamsDurableStore`, `StreamRecoveryWorker`).

Building **Notifications** on top of these verified primitives avoids duplicating Redis clients, WebSocket servers, rate limiters, or connection admission logic, while preserving Voice's isolated audio hot path and the base package's `dependencies = []` invariant.

---

## 2. Decision Proposal

Adopt a bounded **Notifications** module under `src/kaiwen_gateway/modules/notifications/` following Hexagonal Architecture (`ADR-0002`):

1. **Reuse Events & Core Ports (Zero Infrastructure Duplication):**
   - `NotificationsModule` implements Core's `GatewayModule` and consumes `EventBrokerPort`, `DurableEventStorePort`, `RateLimiterPort`, and `VerifiedPrincipalContext` without importing `redis.asyncio` or creating a second WebSocket server.
2. **Reuse `/ws/events/v1` for Real-Time In-App Delivery (`N2`); Defer REST Endpoints:**
   - Deliver real-time `notifications.v1` envelopes over `/ws/events/v1` (`events.delivery`).
   - Defer dedicated Gateway REST endpoints (`POST /notifications`, `GET /notifications`, `POST .../ack`) so Gateway remains a modular library/transport host rather than owning product database/inbox state.
3. **Deterministic Recipient Topics (Naming Only) & Mandatory Recipient Authorization:**
   - Map `(tenant_id, recipient_id)` canonically (`len(tenant_id):tenant_id:len(recipient_id):recipient_id`) to `notifications.r.<24-hex-sha256>` for `TOPIC_PATTERN` compatibility. **Never treat these hashes as secrets or authentication.**
   - Require an additive, fail-closed per-subscription topic-authorization hook in `/ws/events/v1` / `create_gateway_app()` (`N2`) whenever Notifications is enabled: general `events:subscribe` and `events:subscribe:wildcard` **must never** grant access to any exact or wildcard pattern capable of matching the reserved `notifications.r.*` namespace (including `notifications.r.*`, `notifications.*`, and broad wildcards such as `*`); cross-recipient and unauthorized wildcard subscriptions are denied, and missing authorization hooks fail closed at startup (`CoreConfigurationError`).
4. **Explicit Producer Authorization Trust Boundary:**
   - Verified producers do **not** automatically gain permission to target arbitrary recipients; host applications authorize recipient targeting via `NotificationProducerAuthorizerPort` without introducing `kaiwen-agent` dependencies.
5. **Final-Frame Byte Safety:**
   - Enforce `NotificationEnvelope` inner bound (`<= 48 KiB`) **and** validate the final serialized `events.delivery` WebSocket frame (`<= EVENTS_MAX_FRAME_BYTES` = `65_536` bytes) at publish/dispatch time.
6. **6-Stage Acknowledgment Separation & Honest Offline Semantics:**
   - Distinguish (1) Durable acceptance (`XADD`), (2) Notification processing, (3) Transport dispatch (separating broker dispatch acceptance, zero-active-recipient outcome, and actual WebSocket frame transmission), (4) Actual client receipt (unobserved), (5) Application acknowledgment (app-owned), and (6) Redis Streams acknowledgment (`XACK`).
   - `XACK` is issued **only** after Stage 2 processing and the documented terminal Stage 3 broker dispatch outcome (including zero active recipients)—never on `XREADGROUP` or WebSocket queue enqueue. `EventBrokerPort.publish()` success never claims confirmed socket transmission or client receipt.
   - Do **not** promise offline per-user inbox persistence without an explicit application owner outside Gateway MVP (`N3` provides durable worker processing & crash recovery within `MAXLEN`, not a per-user unread inbox).
7. **Preference Policy & Deferred External Channels:**
   - Missing preferences (`None`) default to **allow only for non-promotional (`is_promotional=False`) in-app notifications**; promotional notifications require explicit opt-in, and preference-store errors fail closed (never coerced to `None`).
   - Reserve interface-only `NotificationChannelPort` and defer Email/SMS/Push/Webhook implementations until after `N4`.

---

## 3. Alternatives Considered

| Alternative | Pros | Cons | Verdict |
|---|---|---|---|
| **A. Bounded `modules/notifications/` reusing Events & `/ws/events/v1` (Proposed)** | Zero Redis/WS duplication; clean `GatewayModule` lifecycle; preserves `dependencies = []` and Voice isolation | Requires an additive `N2` per-subscription topic-authorization hook in `/ws/events/v1` | **Proposed** |
| **B. Separate `/ws/notifications/v1` WebSocket server** | Custom notification-only wire frames | Duplicates auth, `?ticket=` replay cache, `Origin` checks, `1008`/`1009` framing, and drain logic | **Rejected** |
| **C. Mandatory Gateway REST + SQL notification inbox** | Built-in REST inbox for apps | Forces database/ORM dependencies into Gateway Core and blurs app vs. gateway ownership | **Rejected for MVP** |

---

## 4. Consequences & Conditions for Acceptance

- **Positive:** Fast, low-risk `N1`–`N4` delivery with zero new runtime dependencies, fail-closed producer/recipient authorization, and full reuse of `E1`–`E4` hardening.
- **Tradeoffs:** Offline per-user inbox history remains outside Gateway MVP; external Email/SMS/Push/Webhook adapters remain post-`N4` work.
- **Compatibility Note:** `N2` requires an additive, backward-compatible per-subscription topic-authorization hook in `/ws/events/v1` / `create_gateway_app()` (existing Events-only callers without Notifications remain unaffected).
- **Acceptance Conditions:** Maintainer approval of `docs/NOTIFICATIONS_MODULE_DESIGN.md`, the `N2` topic-authorization hook, preference defaults, and the `N1`–`N4` milestone plan in `ROADMAP.md`.
