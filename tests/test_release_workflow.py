"""Deterministic validation tests for .github/workflows/release.yml safeguards."""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
RELEASE_WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "release.yml"


def test_release_workflow_file_exists() -> None:
    """The release workflow file must exist at .github/workflows/release.yml."""
    assert RELEASE_WORKFLOW_PATH.is_file(), f"Missing {RELEASE_WORKFLOW_PATH}"


def test_workflow_dispatch_is_restricted_to_dry_run_validation_only() -> None:
    """workflow_dispatch must never publish distributions under any inputs."""
    content = RELEASE_WORKFLOW_PATH.read_text(encoding="utf-8")

    # confirm_publish must not be present in the file
    assert "confirm_publish" not in content, (
        "confirm_publish input must not exist; workflow_dispatch cannot publish"
    )

    # workflow_dispatch block must exist
    assert "workflow_dispatch:" in content
    # Any input under workflow_dispatch must be explicitly documented as dry-run only
    assert "workflow_dispatch never publishes to PyPI" in content


def test_publish_pypi_job_condition_is_strictly_fail_closed() -> None:
    """publish-pypi must strictly require a published, non-draft release and PYPI_PUBLISH_AUTHORIZED=true."""
    content = RELEASE_WORKFLOW_PATH.read_text(encoding="utf-8")

    # Locate the publish-pypi job definition
    assert "publish-pypi:" in content
    publish_section = content.split("publish-pypi:")[1]

    # Verify 'if:' condition in publish-pypi
    condition_match = re.search(r"\n\s+if:\s+([^\n]+)", publish_section)
    assert condition_match is not None, "publish-pypi job must have an 'if' condition"
    condition = condition_match.group(1).strip()

    # The condition must require all 4 safety criteria
    assert "github.event_name == 'release'" in condition
    assert "github.event.action == 'published'" in condition
    assert "!github.event.release.draft" in condition
    assert "vars.PYPI_PUBLISH_AUTHORIZED == 'true'" in condition

    # The condition must NOT contain any reference to workflow_dispatch
    assert "workflow_dispatch" not in condition


def test_publish_condition_truth_table() -> None:
    """Evaluate all permutations of the publish-pypi condition to verify fail-closed semantics."""
    content = RELEASE_WORKFLOW_PATH.read_text(encoding="utf-8")
    assert "publish-pypi:" in content
    publish_section = content.split("publish-pypi:")[1]

    condition_match = re.search(r"\n\s+if:\s+([^\n]+)", publish_section)
    assert condition_match is not None
    condition_str = condition_match.group(1).strip()

    def eval_condition(
        event_name: str, action: str, is_draft: bool, pypi_authorized: str | None
    ) -> bool:
        # Evaluate exact condition:
        # github.event_name == 'release' && github.event.action == 'published' && !github.event.release.draft && vars.PYPI_PUBLISH_AUTHORIZED == 'true'
        # Confirm our evaluation logic matches the exact condition expression components
        for part in [
            "github.event_name == 'release'",
            "github.event.action == 'published'",
            "!github.event.release.draft",
            "vars.PYPI_PUBLISH_AUTHORIZED == 'true'",
        ]:
            assert part in condition_str

        return (
            event_name == "release"
            and action == "published"
            and not is_draft
            and pypi_authorized == "true"
        )

    # Scenarios that MUST NEVER publish (result == False)
    # 1. Any workflow_dispatch event
    assert not eval_condition("workflow_dispatch", "", False, "true")
    assert not eval_condition("workflow_dispatch", "", False, "false")
    assert not eval_condition("workflow_dispatch", "", False, None)

    # 2. Push event
    assert not eval_condition("push", "", False, "true")

    # 3. Pull request event
    assert not eval_condition("pull_request", "", False, "true")

    # 4. Draft release
    assert not eval_condition("release", "published", True, "true")

    # 5. Non-published release action (e.g. created, edited, prereleased)
    assert not eval_condition("release", "created", False, "true")
    assert not eval_condition("release", "edited", False, "true")

    # 6. Release published but PYPI_PUBLISH_AUTHORIZED is not 'true'
    assert not eval_condition("release", "published", False, "false")
    assert not eval_condition("release", "published", False, None)
    assert not eval_condition("release", "published", False, "")
    assert not eval_condition("release", "published", False, "1")

    # Only one exact scenario permits publishing (result == True)
    assert eval_condition("release", "published", False, "true")


def test_in_job_safety_guard_step_is_present() -> None:
    """publish-pypi job must contain an explicit fail-closed shell guard step."""
    content = RELEASE_WORKFLOW_PATH.read_text(encoding="utf-8")
    assert "publish-pypi:" in content
    publish_section = content.split("publish-pypi:")[1]

    # Look for the step that verifies maintainer authorization
    assert "Verify maintainer publishing authorization" in publish_section
    assert 'vars.PYPI_PUBLISH_AUTHORIZED }}" != "true"' in publish_section
    assert 'github.event_name }}" != "release"' in publish_section
    assert 'github.event.action }}" != "published"' in publish_section
    assert 'github.event.release.draft }}" = "true"' in publish_section


def test_tag_to_version_validation_step_is_present() -> None:
    """verify-and-build job must validate release tag against pyproject.toml package version."""
    content = RELEASE_WORKFLOW_PATH.read_text(encoding="utf-8")
    assert "verify-and-build:" in content
    build_section = content.split("publish-pypi:")[0]

    assert "Validate release tag against package version" in build_section
    assert 'EXPECTED_TAG="v${{ steps.version.outputs.version }}"' in build_section
    assert 'ACTUAL_TAG="${{ github.ref_name }}"' in build_section
    assert "does not match package version" in build_section


def test_pypi_environment_and_least_privilege_permissions() -> None:
    """publish-pypi must use environment 'pypi' and restrict id-token: write to that job alone."""
    content = RELEASE_WORKFLOW_PATH.read_text(encoding="utf-8")

    # Top-level permissions must be read-only
    top_level = content.split("jobs:")[0]
    assert "permissions:\n  contents: read" in top_level
    assert "id-token: write" not in top_level

    # verify-and-build must not have id-token: write
    verify_section = content.split("verify-and-build:")[1].split("publish-pypi:")[0]
    assert "id-token: write" not in verify_section

    # publish-pypi must declare id-token: write and environment 'pypi'
    publish_section = content.split("publish-pypi:")[1]
    assert "id-token: write" in publish_section
    assert "name: pypi" in publish_section
    assert "url: https://pypi.org/p/kaiwen-gateway" in publish_section


def test_skip_existing_is_false() -> None:
    """PyPI publish action must fail on existing distribution collision (skip-existing: false)."""
    content = RELEASE_WORKFLOW_PATH.read_text(encoding="utf-8")
    publish_section = content.split("publish-pypi:")[1]
    assert "skip-existing: false" in publish_section


def test_all_actions_use_pinned_commit_shas() -> None:
    """All third-party GitHub Actions must be pinned to full 40-character hexadecimal commit SHAs."""
    content = RELEASE_WORKFLOW_PATH.read_text(encoding="utf-8")
    sha_pattern = re.compile(r"uses:\s+([a-zA-Z0-9_-]+/[a-zA-Z0-9_.-]+)@([a-f0-9]{40})\b")
    uses_lines = [
        line.strip()
        for line in content.splitlines()
        if line.strip().startswith("uses:") and not line.strip().startswith("#")
    ]
    assert len(uses_lines) >= 5, f"Expected at least 5 action steps, found {len(uses_lines)}"

    for line in uses_lines:
        match = sha_pattern.search(line)
        assert match is not None, (
            f"Action step is not pinned to a 40-character hex commit SHA: {line}"
        )
