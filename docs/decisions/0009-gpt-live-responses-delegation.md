# ADR-0009: GPT-Live Responses Delegation for Function Tools

- **Status:** Proposed (implemented on `feat/gpt-live-responses-delegation`; maintainer acceptance pending)
- **Date:** 2026-11-26
- **Extends:** [ADR-0008](0008-realtime-semantic-tool-side-channel.md)
- **Sources:** developers.openai.com/api/docs/guides/live-delegation and the Live API reference, read 2026-11-26

## Context

GPT-Live does not choose function tools itself. It delegates to a backend: either a
provider-run Responses model (Responses delegation) or the application (client
delegation, whose event carries no task text).

## Decision

Implement **Responses delegation** behind an explicit `ResponsesDelegationConfig`.
The application then needs no second model client, credential or conversation-context
builder: the provider supplies context to the backend model and the same socket carries
function calls and results. Client delegation stays the default.

- Calls are read from nested `response.output_item.done`; deduplicated and bounded by
  `ToolCallTracker`.
- Results use `response.item.create`; `response.create` is sent once, after the nested
  response completed and every call of it has an output.
- Live documents no cancellation of delegated work. On `cancel_turn()` pending calls are
  cancelled (late results raise `StaleToolCallError`) and a fixed failure output is sent
  so the backend response is not left blocked.
- `responses_validation_failed` keeps the session open and withdraws tool capabilities.
- Gateway still carries proposals only: no validation, approval or execution.

## Consequences

- Backend model tokens are billed in addition to Live session time.
- Application control over backend context and routing is limited to the managed
  configuration; client delegation would be a separate decision.
- Verification is mock-only. Live behaviour (whether the model delegates, which calls the
  backend makes, what it says) is **NOT_VERIFIED**.
