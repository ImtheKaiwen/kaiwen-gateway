# Changelog

All notable **user-facing changes** to Kaiwen Gateway are recorded in this file.

This project follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and intends to use [Semantic Versioning](https://semver.org/spec/v2.0.0.html) for its Python distribution. The **Python package version**, any **external Gateway wire/API protocol version**, and **provider adapter compatibility** are separate concerns; a package release does not automatically change a wire protocol version.

> **Project status (2026-10-31): Design / pre-implementation.** The intended first alpha is `0.1.0a1`, but **no alpha package, GitHub release, public protocol implementation, or production deployment has been published or verified**. Items under **Unreleased** are not shipped features. Proposed architecture documents are not proof of working code or accepted ADRs.

## [Unreleased]

### Added

- Prepared the initial, **proposed** V1 documentation set: project overview (`README.md`), requirements (`docs/REQUIREMENTS.md`), architecture (`docs/ARCHITECTURE.md`), development roadmap (`ROADMAP.md`), coding-agent rules (`AGENTS.md`), contribution guide (`CONTRIBUTING.md`), security policy (`SECURITY.md`), and the first three Architecture Decision Records (`docs/decisions/0001`–`0003`).
- Documented the intended initial scope: a provider-neutral Python library with an optional ASGI server, focusing exclusively on the Voice module for V1. **This is design documentation, not an implemented or tested feature.**

## Maintenance policy

This section describes **how to keep the changelog accurate**; it is not a record of released functionality.

1. **Record verified changes only.** Add an entry under `Unreleased` when a meaningful change is completed and supported by the relevant code, documentation, tests, or other reviewable evidence. Do not add promises, speculative features, failed experiments, or unverified provider compatibility.
2. **Write for library consumers and operators.** Describe observable behavior and impact, not every commit, internal refactor, agent prompt, or development step. Prefer concise, specific entries. Link an issue, pull request, or accepted ADR when it materially helps explain a change.
3. **Use the appropriate categories.** The standard headings are `Added`, `Changed`, `Deprecated`, `Removed`, `Fixed`, and `Security`. Include only headings that have actual entries; do not pre-populate empty sections.
4. **Keep proposed and implemented work distinct.** Documentation-only changes may be recorded as such. An adapter passing synthetic tests is **not** a verified live Verimor integration. A successful alpha is **not** production readiness.
5. **Identify compatibility impact.** Clearly flag breaking changes to the public Python API, CLI, configuration, wire events, authentication, or provider adapter contracts. Explain the migration path in the related release notes when applicable.
6. **Protect sensitive information.** Summarize security fixes without disclosing exploitable details, customer data, credentials, private provider contracts, or incident evidence. Coordinate sensitive disclosures under [`SECURITY.md`](SECURITY.md).
7. **Cut releases deliberately.** Only after authorization and passing the release gate, move the relevant `Unreleased` entries beneath a new `## [version] - YYYY-MM-DD` heading. The date is the actual publication date (ISO 8601), not a planned date. Leave a fresh `Unreleased` section above it. Add Git tag/compare links **only after the corresponding tags exist**.
8. **Do not use changelog entries as acceptance evidence.** Requirement status and acceptance criteria live in [`docs/REQUIREMENTS.md`](docs/REQUIREMENTS.md); milestones and blockers live in [`ROADMAP.md`](ROADMAP.md); architectural rationale belongs in [`docs/decisions/`](docs/decisions/); coding-agent reporting obligations live in [`AGENTS.md`](AGENTS.md).

### Release format (example only; not an actual release)

The following fenced example illustrates formatting. **It does not declare that a release exists.**

```markdown
## [0.1.0a1] - YYYY-MM-DD

### Added
- <Verified, user-visible capability and its scope.>

### Fixed
- <Verified user-visible defect that was resolved.>
```

Keep this file concise as the project grows; detailed implementation evidence belongs in the associated pull requests, test reports, and release notes.
