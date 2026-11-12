# ADR-0005: v0.1.0a1 PyPI Publishing Recovery & Action Pin Remediation

- **Status:** Accepted
- **Date:** 2026-11-12
- **Context:** Emergency recovery of failed v0.1.0a1 PyPI publication caused by an unresolvable commit SHA pin for `pypa/gh-action-pypi-publish`.
- **Deciders:** Maintainer, Kaiwen Gateway Team

---

## 1. Context and Problem Statement

Following the authorization and creation of GitHub Release `v0.1.0a1` (commit `7d7cf9754cae512233229d9da77ec7a61d6ca35a`), the automated publishing workflow ([run `43128221867`](https://github.com/ImtheKaiwen/kaiwen-gateway/actions/runs/43128221867)) failed during job setup in `publish-pypi` with:

```text
Unable to resolve action pypa/gh-action-pypi-publish@76f52e660529645856eb5948ea221fb86e5c2848
```

The commit hash `76f52e660529645856eb5948ea221fb86e5c2848` was invalid/unresolvable upstream in `pypa/gh-action-pypi-publish`.

Meanwhile:
1. The GitHub Release `v0.1.0a1` and git tag `v0.1.0a1` are already published and immutable.
2. The package has not been published to PyPI (HTTP 404).
3. The maintainer configured the GitHub environment `pypi` with required reviewer protection (`ImtheKaiwen`).
4. The repository variable `PYPI_PUBLISH_AUTHORIZED` is set to `true`.
5. Rerunning the existing release workflow re-executes the broken workflow YAML attached to tag `v0.1.0a1`.
6. Policy prohibits deleting, moving, or recreating existing release tags or publishing an unauthorized new version.

---

## 2. Decision & Remediation

We resolve this failure through a three-part recovery strategy:

### A. Upstream Action Pin Remediation
We update `pypa/gh-action-pypi-publish` to PGP-verified upstream release `v1.14.3` at commit:
```text
7245906fb61fa33a13554f9d5fb856fe61758086 # v1.14.3
```
This version includes critical security fixes (GHSA-5xjx-3w67-w9x9), supports PEP 740 attestations, maintains full OIDC Trusted Publishing compatibility, and is signed by maintainer Sviatoslav Sydorenko.

### B. Controlled Emergency Release Recovery Path
We add an explicitly restricted, temporary emergency recovery option to `.github/workflows/release.yml` on `workflow_dispatch`:
- Normal `workflow_dispatch` remains strictly verification/dry-run only.
- Publishing is unlocked ONLY when all five independent conditions are met simultaneously:
  1. `inputs.dry_run == false`
  2. `inputs.recovery_release_tag == 'v0.1.0a1'` (hard-coded; no other tag or package is permitted)
  3. `vars.PYPI_PUBLISH_AUTHORIZED == 'true'`
  4. Git tag `v0.1.0a1` commit matches expected commit `7d7cf9754cae512233229d9da77ec7a61d6ca35a` exactly
  5. The protected `pypi` GitHub environment required reviewer approves the deployment.
- The build job checks out immutable tag `v0.1.0a1` to ensure distribution artifacts originate strictly from the released tag.
- Pre-upload validation verifies distribution filenames and SHA256 checksums before invoking PyPI publishing.

### C. Post-Publication Cleanup
Once `v0.1.0a1` is verified published on PyPI:
- `PYPI_PUBLISH_AUTHORIZED` will be reset to `false` to maintain fail-closed security.
- The temporary recovery parameter on `workflow_dispatch` will be deprecated/removed in a follow-up commit to restore strict release-event-only publication.

---

## 3. Consequences

- **Security:** Preserves supply-chain integrity, action pinning, least-privilege OIDC tokens, required reviewer gate, and immutable release tags.
- **Traceability:** Complete audit trail connecting GitHub Release `v0.1.0a1` to the published PyPI distributions.
- **Zero Drift:** Distribution code is guaranteed identical to git tag `v0.1.0a1`.
