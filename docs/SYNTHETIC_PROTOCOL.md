# Synthetic Telephony Protocol Specification (v1)

> **Status:** Reference specification for local simulation and synthetic ingress (M3.2A/M3.2B)<br>
> **Package:** `kaiwen-gateway` / `kaiwen_gateway.transports.synthetic`<br>
> **Endpoint:** `/ws/synthetic/v1`<br>
> **Protocol Version:** `v1`<br>
> **Important:** This is a **fictional, project-owned synthetic test protocol**. It is **not** real Verimor, has **no** relationship to any proprietary carrier or telephony API, and must **never** be represented as live telephony interoperability.

---

## 1. Scope and Architectural Boundary

The Synthetic Telephony Protocol establishes a controlled, deterministic WebSocket ingress path for testing Kaiwen Gateway's core lifecycle, connection supervision, admission control, and call state management without requiring real telephony infrastructure or live carrier credentials.

In accordance with [`docs/ARCHITECTURE.md`](ARCHITECTURE.md) and [`AGENTS.md`](../AGENTS.md):
- **Control plane only:** This protocol currently manages connection handshakes, session readiness, ping/pong liveness, and call lifecycle (`call.start`, `call.end`). Audio media streaming is deferred to Milestone 4.
- **Provider-neutral port binding:** Inbound synthetic events are normalized into domain-level `TelephonyControlEvent` instances handled by `SyntheticTelephonyAdapter`, implementing `TelephonyControlPort`.
- **Zero Core runtime dependencies:** All protocol parsing, schema validation, and token cryptography remain strictly inside `kaiwen_gateway.transports.synthetic` and the optional `server` extra.

---

## 2. Transport & Framing

- **Underlying transport:** WebSocket over TLS/TCP (`wss://` in production, `ws://` in local loopback tests).
- **Endpoint path:** `/ws/synthetic/v1`
- **Frame type:** **Text frames only** (`Opcode 1`), containing UTF-8 encoded JSON.
- **Binary frames:** Strictly forbidden; receiving a binary frame terminates the connection immediately with WebSocket close code `1003` (`Unsupported Data`).
- **Maximum frame size:** 64 KiB (`65,536 bytes`). Frames exceeding this limit are rejected immediately with WebSocket close code `1009` (`Message Too Big`).
- **Strict JSON conformance (RFC 8259):** Non-standard JSON constants (`NaN`, `Infinity`, `-Infinity`) are strictly forbidden and rejected with close code `1007` (`Invalid Frame Payload`).

---

## 3. Authentication & Handshake

Ingress authentication occurs during the HTTP WebSocket upgrade handshake via standard HTTP authorization headers:

```text
GET /ws/synthetic/v1 HTTP/1.1
Host: localhost:8000
Upgrade: websocket
Connection: Upgrade
Authorization: Bearer <hmac_token>
```

> [!CAUTION]
> **Token Transport Security:** Bearer tokens must **never** be transmitted via query-string URL parameters, as URLs are routinely logged by intermediate proxies, access logs, and browser histories. The gateway strictly requires the `Authorization: Bearer <token>` header.

### 3.1 Token Verification
The token is verified by `SyntheticTokenVerifier` using HMAC-SHA256 (`kaiwen-synthetic-v1` format):
- Signature integrity verified in constant time.
- Expiration check (`exp` > current monotonic / wall time).
- Not-before check (`nbf` <= current time).
- Required claims: `provider_id`, `tenant_id`, `call_id`, `scopes`.
- Required scope: `voice.stream` or equivalent admitted capability.

### 3.2 Pre-Admission Handshake Policy
1. If the `Authorization` header is missing, malformed, or carries an expired/forged token or lacks required scopes, the handshake is rejected with WebSocket close code `1008` (`Policy Violation`).
2. If the token is valid, the Gateway checks `AdmissionController`:
   - If capacity is exhausted or the instance is draining, the server rejects the connection with close code `1008` (`Admission rejected`) **before** the WebSocket handshake is accepted.
3. If admitted, the server registers `(tenant_id, call_id)` exclusivity:
   - A concurrent inbound connection with an active duplicate `(tenant_id, call_id)` is rejected with close code `1008` (`Call session already active`).

---

## 4. Message Schemas & Event Types

Every frame is a JSON object with a required `"event"` field (string, 1 to 64 ASCII alphanumeric/dot/underscore characters).

### 4.1 Server-to-Client Events

#### `session.ready`
Emitted by the server immediately upon accepting the connection:
```json
{
  "event": "session.ready",
  "protocol_version": "v1",
  "connection_id": "conn_syn_a1b2c3d4e5f67890",
  "tenant_id": "tenant_prod",
  "provider_id": "synthetic",
  "call_id": "call_12345"
}
```

#### `session.pong`
Emitted by the server in response to `session.ping`:
```json
{
  "event": "session.pong",
  "seq": 42
}
```
*(The `seq` field echoes the client's `seq` if provided).*

#### `call.started`
Emitted by the server in response to a valid `call.start` request:
```json
{
  "event": "call.started",
  "call_id": "call_12345",
  "state": "connected",
  "stream_id": "str_abc"
}
```

#### `call.ended`
Emitted by the server in response to a `call.end` request:
```json
{
  "event": "call.ended",
  "call_id": "call_12345",
  "state": "disconnected",
  "reason": "caller_hangup",
  "code": 16
}
```

---

### 4.2 Client-to-Server Events

#### `session.ping`
Keep-alive probe sent by the client.
```json
{
  "event": "session.ping",
  "seq": 42
}
```
- `seq` (optional): Non-negative integer (`0` to `2,147,483,647`). Boolean values are rejected.

#### `session.end`
Graceful session teardown requested by client.
```json
{
  "event": "session.end"
}
```
If a call is active when `session.end` is received, the adapter deterministically transitions the call state to `DISCONNECTED` with reason `"session_ended"`. The server acknowledges by closing the WebSocket connection with code `1000` (`Normal Closure`).

#### `call.start`
Initiates the telephony call leg.
```json
{
  "event": "call.start",
  "stream_id": "str_abc",
  "metadata": {
    "carrier_zone": "us-east-1"
  }
}
```
- `stream_id` (optional): String (1 to 128 characters).
- `metadata` (optional): JSON object (maximum 32 keys, key names <= 64 characters, values primitive string/int/float/bool, strings <= 256 chars, no NaN/Infinity).
- **Identity spoofing protection:** Any attempt by the client to supply `tenant_id`, `provider_id`, `call_id`, or `allowed_scopes` conflicting with the verified token context causes immediate termination with close code `1008`.

#### `call.end`
Terminates the telephony call leg.
```json
{
  "event": "call.end",
  "reason": "caller_hangup",
  "code": 16
}
```
- `reason` (optional): String (1 to 128 characters). Default: `"caller_hangup"`.
- `code` (optional): Integer (`0` to `65535`). Boolean values are rejected.
- **Idempotency:** A duplicate `call.end` when the call is already in `DISCONNECTED` state is handled idempotently and returns `call.ended` with `"idempotent": true` without error.

---

## 5. Call State Machine & Transport Teardown

The synthetic telephony call leg follows a strict, unidirectional state machine:

```text
                  ┌──────────────┐
                  │     IDLE     │
                  └──────┬───────┘
                         │
             call.start  │
                         ▼
                  ┌──────────────┐
       ┌─────────►│  CONNECTED   │
       │          └──────┬───────┘
       │                 │
ping / │     call.end    │ transport_close /
pong   │                 │ session.end
       │                 ▼
       │          ┌──────────────┐
       └──────────┤ DISCONNECTED │ (Terminal; idempotent call.end)
                  └──────────────┘
```

### Transition & Teardown Invariants
1. `IDLE -> CONNECTED`: Valid only on `call.start`.
2. `CONNECTED -> DISCONNECTED`: Valid on `call.end` (wire event), client `session.end`, socket disconnect (`WebSocketDisconnect`), timeout, or server cancellation.
3. `call.start` while `CONNECTED` or `DISCONNECTED`: Illegal state transition; closes connection with code `1008`.
4. `call.end` while `IDLE`: Illegal state transition; closes connection with code `1008`.
5. `call.end` while `DISCONNECTED`: Idempotent; responds with `call.ended` acknowledging existing disconnection.
6. **Separation of Semantics:** A WebSocket transport close does not imply telephony call completion, but an active telephony call leg bound to a closed transport is deterministically cleaned up and marked `DISCONNECTED` with reason `"transport_closed"` or `"session_ended"`.

---

## 6. Capability Negotiation & Unsupported Operations

The synthetic adapter declares its capabilities deterministically:

| Capability | Status | Behavior when requested |
|---|---|---|
| `call_start` | **Supported** | Transitions `IDLE -> CONNECTED`, acknowledges with `call.started` |
| `call_end` | **Supported** | Transitions `CONNECTED -> DISCONNECTED`, acknowledges with `call.ended` |
| `ping_pong` | **Supported** | Responds with `session.pong` |
| `call_transfer` | **Unsupported** | Fail-closed: raises `UnsupportedOperationError`, closes socket with `1008` |
| `dtmf` | **Unsupported** | Fail-closed: raises `UnsupportedOperationError`, closes socket with `1008` |
| `media_streaming` | **Unsupported** *(M3.2)* | Out of scope until Milestone 4 |

---

## 7. WebSocket Close Codes & Error Mapping

| Close Code | Name | Condition |
|---|---|---|
| `1000` | Normal Closure | Graceful client exit via `session.end` or server-initiated graceful drain. |
| `1003` | Unsupported Data | Client transmitted a binary WebSocket frame. |
| `1007` | Invalid Frame Payload | Malformed JSON, non-UTF-8 payload, non-standard constants (`NaN`/`Infinity`), or schema validation failure. |
| `1008` | Policy Violation | Auth failure, expired token, identity spoofing, duplicate active call, illegal state transition, or unsupported capability. |
| `1009` | Message Too Big | Received frame exceeding 64 KiB (65,536 bytes). |
| `1011` | Internal Error | Unexpected server exception or connection worker startup failure. |
