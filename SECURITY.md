# Security Policy

> **Project:** [Kaiwen Gateway](https://github.com/ImtheKaiwen/kaiwen-gateway)
> **Status:** Design / pre-implementation; this policy is a proposed documentation baseline.
> **Target first alpha:** `0.1.0a1` (**not published**).
> **Last reviewed:** 2026-10-31

Kaiwen Gateway is intended to handle authenticated, real-time communication between external providers and AI systems. Its initial use case is a bidirectional telephone-audio bridge over WebSockets. A failure at this boundary could expose sensitive conversations, mix different tenants' sessions, authorize unintended actions, or incur upstream service costs.

**Security is a release requirement, not a feature that can be deferred until production.** This policy covers vulnerability reporting, responsible disclosure, security ownership, and the minimum safeguards expected from implementations and deployments. It does **not** claim that the Gateway has been implemented, audited, tested against a live telephony provider, or approved for production use.

The authoritative implementation requirements are in [docs/REQUIREMENTS.md](docs/REQUIREMENTS.md) (`SEC-*`, acceptance gates); technical boundaries are in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md); coding-agent obligations are in [AGENTS.md](AGENTS.md). If this policy conflicts with a security-critical requirement, resolve the conflict before changing code or relaxing a control.

## 1. Reporting a vulnerability

**Do not disclose exploitable security details in public GitHub Issues, Discussions, pull requests, or social media.** Do not publish credentials, personal information, call recordings, transcripts, or exploit traffic.

### Preferred private reporting channel

The intended channel is **GitHub Private Vulnerability Reporting**, using this repository's **Security / Advisories / Report a vulnerability** flow **once the repository owner enables that feature**.

- Repository: https://github.com/ImtheKaiwen/kaiwen-gateway
- Security and advisories page: https://github.com/ImtheKaiwen/kaiwen-gateway/security
- GitHub instructions for reporting privately: https://docs.github.com/en/code-security/how-tos/report-and-fix-vulnerabilities/report-privately

**Important: The private-reporting setting has not been verified as enabled.** Publishing `SECURITY.md` does **not** enable private vulnerability reporting. The maintainer must enable the setting in the repository's security settings, verify that the **Report a vulnerability** action is available to an external reporter, and enable appropriate notifications before treating this channel as operational.

If that action is unavailable, **do not submit the vulnerability details publicly**. A generic issue asking the maintainer to establish a confidential reporting route is acceptable **only if it includes no vulnerability details or hints that would enable exploitation**. This is a temporary fallback, **not** a substitute for a configured private reporting channel. No alternative security email address is currently designated; do not guess or invent one.

### Include in a private report, when safe

1. A concise summary of the suspected weakness and its potential impact.
2. Affected package version, commit, module, adapter, configuration, and deployment type (as applicable).
3. Reproduction steps or a **minimal synthetic** proof of concept; identify prerequisites and permissions.
4. Expected behavior versus observed behavior.
5. Whether authentication, tenant isolation, call state, AI tool permissions, customer data, or secrets may be affected.
6. Any observed indicators of exploitation and safe mitigations, without supplying unrelated sensitive records.
7. A private way to follow up through the GitHub advisory conversation.

Use redacted logs and synthetic fixtures. If a secret or personal dataset is already exposed, describe its type and approximate scope **without copying it into the report**. Coordinate secure evidence transfer privately if actually needed.

### What is security-relevant?

Examples include:

- Authentication bypass, forged telephony-provider identity, token replay, or unauthorized AI-session creation.
- Cross-tenant or cross-call audio, conversation, context, tool-scope, or metadata leakage.
- Authorization bypass, prompt-injection paths that lead to **unauthorized tool execution**, or unsafe business-side effects.
- Server-side request forgery (SSRF), arbitrary upstream URL selection, leaked provider tokens or API credentials.
- Unsafe WebSocket framing or deserialization; input-triggered crash, memory exhaustion, or unbounded paid-provider usage.
- Recording, telemetry, artifacts, or test fixtures leaking call audio, transcripts, personal data, or secrets.
- Vulnerable dependencies, compromised build/release workflows, malicious package artifacts, or tampered provenance.

A model producing a poor answer **without a security boundary being crossed** is usually a product-quality issue; a prompt injection that defeats access controls or causes unauthorized data disclosure **is** a security issue. Operational failures that affect confidentiality, integrity, or availability may also qualify.

## 2. Supported versions and maintenance expectations

| Version / branch | Status | Security support |
| --- | --- | --- |
| `main` | Public design / pre-implementation baseline | Best-effort review; **no production support or service-level commitment**. |
| `0.1.0a1` | Planned first alpha; **not released** | No published artifact or supported release yet. |
| Future releases | Not yet defined | Policy will be updated when versions are actually published and maintained. |

There are currently **no published, security-supported Gateway releases**. Do not infer support from a version string in planning documents. Once a release is published, maintainers should explicitly document supported version ranges, upgrade/patch expectations, and end-of-support policy in this table.

**Neither an alpha release nor this policy constitutes a security certification, penetration-test result, availability guarantee, or permission to process real customer calls.**

## 3. Coordinated disclosure and maintainer process

The project aims to follow a coordinated, private disclosure process:

1. **Receive privately.** Restrict report details to people needed to triage and fix the issue.
2. **Acknowledge and triage.** Check reproducibility, the affected trust boundary, severity, likely impact, and whether external providers or customers are involved.
3. **Contain if necessary.** Prepare mitigations such as disabling a vulnerable adapter, revoking credentials, lowering limits, or blocking unsafe traffic. Never expose sensitive incident evidence in public logs.
4. **Fix and verify.** Add an appropriate regression test (including negative or concurrency cases when relevant); review the fix for unintended compatibility effects.
5. **Publish deliberately.** Obtain maintainer authorization for package releases and advisories; communicate mitigations, affected versions, and upgrade steps when disclosure is safe.
6. **Document learning.** Summarize security-relevant user-visible changes in [CHANGELOG.md](CHANGELOG.md), without reproducing exploit details or private incident material.

Severity is assessed using **actual impact and exploitability**, not labels supplied by a reporter. High-impact categories include unauthorized tenant access, credential exposure, unauthenticated paid AI-session creation, unauthorized agent tool actions, and easily exploitable remote denial of service.

**No guaranteed response or remediation SLA is currently offered.** Before live customer operations, the deploying organization must establish incident ownership, alert routing, escalation contacts, response objectives, and disclosure/legal obligations. Public GitHub contributors are not an implicit on-call team.

Security advisories or CVE/GHSA identifiers must not be invented or announced before the relevant process has produced them.

## 4. Trust boundaries and mandatory implementation principles

The following are **required design and acceptance criteria**, not statements that the current repository already implements them.

### 4.1 Inbound connections and identities

- Use **TLS/WSS** for externally accessible real-time media. Do not expose unencrypted telephone-media endpoints publicly.
- Authenticate and authorize an external provider **before allocating an upstream AI session or expensive resources**.
- Select a provider-supported verification method (e.g., mTLS or validated short-lived credentials). Do not assume Verimor supports any particular method until its authoritative contract is available.
- Derive `provider_id`, `tenant_id`, `call_id`, and permitted scopes from **trusted server-controlled policy**. Treat phone numbers, voice commands, unauthenticated headers, and inbound frame fields as untrusted metadata.
- Reject forged, expired, revoked, replayed, or out-of-scope credentials; use atomic replay protection where required by the actual event/handshake protocol.
- Treat browser `Origin` validation as an **additional browser-client control**, not as provider authentication and not as a reliable identity signal for non-browser telephony clients.

### 4.2 Isolation, authorization, and lifecycle

- Each live call owns a separately scoped connection, audio buffers, cancellation tasks, AI session, and authorized agent context.
- Enforce tenant and resource ownership at **every sensitive lookup and action**, not only during the initial WebSocket handshake.
- Prevent client-controlled changes to trusted tenant, user, call, and tool permissions mid-session.
- Ensure disconnect, failure, timeout, and cancellation release all associated resources, even on partially initialized calls.
- Delegate business-tool permissions, approvals, and idempotency to trusted consuming-application/Kaiwen Agent policy components. **An AI-generated tool call is a request, not authorization.**
- Use deny-by-default policy when identity or authorization context is missing, ambiguous, or invalid.

### 4.3 Streaming safety and resource control

- Validate handshake inputs, control-event schemas, binary frame lengths and formats, and permitted codecs.
- Apply bounded queues, connection/admission limits, per-tenant resource budgets, rate limits, timeouts, and controlled backpressure. Limits must account for legitimate high-frequency audio traffic rather than copy generic REST limits.
- Protect the event loop from blocking work and uncontrolled task creation; use explicit task ownership and cancellation semantics.
- Avoid unbounded buffering and costly JSON/base64 transcoding on the audio hot path unless actually required by a verified provider protocol.
- Disable WebSocket compression by default unless its need, security implications, and resource costs have been reviewed.
- Bound outbound connections and upstream spending; implement a safe policy for provider failure, quota exhaustion, and interrupted playback.

### 4.4 Outbound AI/providers and tool safety

- Restrict external connections to configured and approved upstream endpoints; prevent user-controlled targets and SSRF.
- Scope provider API credentials server-side by environment and the smallest practical permissions.
- Do not share AI provider sessions, caller context, or data caches between unrelated calls or tenants.
- Treat caller speech, transcriptions, upstream content, tool responses, and model output as **untrusted content**. They cannot alter trusted policy, provider configuration, tool registration, or approval requirements.
- Keep proprietary telephony control messages and real provider authentication behavior within provider adapters, not the reusable core.

### 4.5 Secrets, logs, and telemetry

- Never commit `.env` files, credentials, certificates, private keys, access tokens, customer data, or confidential provider agreements.
- Use server-side secret providers or explicitly controlled configuration; support rotation and revocation plans appropriate to deployments.
- Redact authentication headers, tokens, secrets, telephone numbers, media bytes, and transcript content from routine logs, traces, metrics, CI artifacts, error reports, and exceptions.
- Use structured, safely correlated lifecycle and failure metadata; **do not log every audio frame**.
- Avoid high-cardinality or identifying metric labels. Restrict telemetry access and define retention periods.
- Protect report attachments and test fixtures against accidental publication in GitHub Actions artifacts.

### 4.6 Data protection and privacy

- The Gateway's **target default behavior** is transient media processing: **no durable raw-audio or full-transcript recording by default**.
- Any recording, retention, retrieval, deletion, and access policy must be explicitly authorized and controlled by the consuming application/operator.
- WSS/TLS protects transport; it does **not** mean the Gateway or upstream AI processor cannot access the audio being processed.
- Evaluate provider processing/storage terms, legal basis, notices/consent where applicable, access control, retention, and cross-border transfers **before real customer data is routed**.
- Do not imply that open-source code or a successful synthetic test establishes compliance with KVKK, GDPR, or other applicable requirements. Deployment-specific legal review remains necessary.
- Use synthetic test audio and fictitious identities in the public repository, documentation, CI, examples, and performance results.

## 5. Secure development and software supply chain

The required engineering direction is:

- Keep `main` protected by reviewed pull requests and required status checks when repository controls are configured.
- Use Ruff, strict type checking, unit/integration/contract tests, negative-auth tests, cross-tenant tests, and malformed-frame/fault tests as appropriate to each change.
- Scan dependencies, source, secrets, container artifacts, and GitHub Actions configurations with tools supported by the actual repository and plan. Review findings rather than treating a scan's exit status as a complete security assessment.
- Pin and review build/workflow dependencies; give each workflow the **least GitHub token permissions** needed.
- Keep release credentials and deployment authority isolated from ordinary CI/PR jobs. Prefer **OIDC / trusted publishing** to persistent publishing tokens when supported.
- Build reproducible-enough, traceable artifacts and record the source commit and image/package identifiers. Do not claim signatures, attestations, or provenance exist before they are generated and verified.
- Review third-party contribution changes to security boundaries and dependencies before merge; untrusted PRs must not receive production secrets.
- Update `SECURITY.md`, related requirements, and regression tests when new public attack surfaces or supported release branches are introduced.

A passing workflow is evidence of the checks it actually executed; it is **not** proof that the project has no vulnerabilities.

## 6. Responsible security research

Security research should be performed **only on the researcher's own local installation, synthetic fixtures, or an environment for which explicit authorization was granted**.

Please do not:

- Access, record, or alter real phone calls, customer accounts, data, or provider infrastructure without authorization.
- Perform denial-of-service testing, high-volume scanning, or expensive AI-session creation against public or third-party systems.
- Attempt persistence, lateral movement, social engineering, or exfiltration.
- Disclose live secrets or publish working exploitation details before coordinated remediation.
- Submit public pull requests containing exploit proofs, production credentials, private datasets, or sensitive reports.

If testing unexpectedly exposes another person's data or a live credential, **stop immediately**, preserve only the minimum non-sensitive evidence, and use the private reporting process. Do not continue probing unrelated records.

## 7. Incident response and production responsibilities

This repository provides reusable software; it does not operate every deployment. A production operator must establish, test, and document:

- An incident owner, escalation/notification channels, and emergency contacts.
- Credential rotation, tenant isolation/disablement, call termination, and AI-provider cut-off procedures.
- Deploy/rollback procedures, auditable release identifiers, and a safe behavior for calls already in progress.
- Alerts and runbooks for auth spikes, cross-tenant policy failures, provider outages, unexpected resource consumption, and abnormal spending.
- Retention/access controls for application logs and operational audit data; applicable incident notification requirements.
- A post-incident review process focused on causes, controls, and regression tests rather than publicizing sensitive data.

**Live Verimor or company/customer calls remain blocked** until the provider's authoritative WSS contract is obtained, the integration is verified in an authorized environment, and the consuming organization approves the relevant security/privacy/data-flow assessment. A passing simulator test is not evidence of live interoperability.

## 8. Security verification and release gates

| Gate | Required evidence |
| --- | --- |
| **M0 — Documentation** | Review of this policy, requirements, architecture, and ADRs; maintainer acceptance; private vulnerability reporting **enabled and verified before accepting reports as operational**. |
| **M2/M3 — Core and ingress** | Auth-before-allocation, immutable scoped identity, replay/forgery rejection, frame limits, connection lifecycle, and cross-tenant negative tests. |
| **M4/M5 — Audio bridge and resilience** | Isolated upstream sessions, controlled backpressure, safe interruption/cleanup, provider-failure tests, no sensitive log leakage, synthetic fault/load evidence. |
| **M6 — Alpha release review** | Relevant CI/security scans, clean package install, reproducible test evidence, accurate version/support statement, approved release authority. |
| **M7 — Conditional real-provider pilot** | Official Verimor protocol, authorized sandbox proof, data-flow/privacy review, operator incident plan, explicit deployment approval. |

No milestone is complete merely because the table lists a desired check. Status and evidence are tracked in [ROADMAP.md](ROADMAP.md); implementation tests and release acceptance criteria are in [docs/REQUIREMENTS.md](docs/REQUIREMENTS.md).

## 9. References

- [GitHub: Privately reporting a security vulnerability](https://docs.github.com/en/code-security/how-tos/report-and-fix-vulnerabilities/report-privately)
- [GitHub: Configuring private vulnerability reporting](https://docs.github.com/en/code-security/how-tos/report-and-fix-vulnerabilities/configure-vulnerability-reporting/configure-for-a-repository)
- [OWASP: WebSocket Security Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/WebSocket_Security_Cheat_Sheet.html)
- [Project requirements and acceptance gates](docs/REQUIREMENTS.md)
- [Architecture and trust boundaries](docs/ARCHITECTURE.md)
- [Coding-agent security requirements](AGENTS.md)

---

**Maintainer action required before public security intake:** Enable and verify GitHub Private Vulnerability Reporting, configure notifications, and review this proposed policy. Do not publish an unverified email address or claim a response SLA that has not been staffed.
