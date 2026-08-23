"""Wrap `towncrier check` with exemptions for release-cutting and Dependabot commits.

`towncrier check` can already tell that a commit doesn't need its own changelog.d
fragment when that commit's diff includes `CHANGELOG.md` itself -- but a "Bump version to
X.Y.Z" commit (`scripts/cut_release.py`'s first commit) never touches `CHANGELOG.md`, and
on a feature/PR branch checked before the changelog-build commit exists, towncrier's own
skip never engages. Skip the check outright for commits matching either of
`cut_release.py`'s two release-commit message patterns; everything else defers to
towncrier normally.

Dependabot's dependency-bump commits hit the same gap from a different angle: towncrier
flags any branch with no new fragment regardless of which files changed, so a
workflow-file or lockfile-only bump fails a check meant to gate `src`/`tests` changes.
Dependabot commits are authored by `dependabot[bot]` and never carry a fragment, so skip
on author identity rather than trying to enumerate its commit-message formats.

At the git `pre-commit` stage (this hook has no `stages:` override, so it also runs there,
not just `pre-push`), `HEAD` still points at the commit *before* the one being made -- the
exemption can miss on a local `git commit` for that reason. It reliably matches wherever
this hook runs against a fully-formed commit, which is the case this exists to fix:
`ci.yml`'s `check` job (`prek run --all-files`, run against the real pushed commit).

References:
    https://towncrier.readthedocs.io/en/latest/cli.html#towncrier-check
"""

from __future__ import annotations

import logging
import re
import subprocess

LOGGER = logging.getLogger(__name__)

RELEASE_COMMIT_PATTERNS = (
    r"^Bump version to \d+\.\d+\.\d+\Z",
    r"^Update CHANGELOG\.md for v\d+\.\d+\.\d+\Z",
)


def head_commit_subject_and_author() -> tuple[str, str]:
    """Return the subject line and author name of the current `HEAD` commit.

    Returns:
        tuple of (subject, author name).

    Raises:
        subprocess.CalledProcessError: `git log` failed.
    """
    subject, author = subprocess.run(
        ["git", "log", "-1", "--pretty=%s%n%an"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.splitlines()
    return subject, author


def main() -> int:
    """Skip the changelog-fragment check for release/Dependabot commits, else defer.

    Returns:
        int process exit status, zero when the check is skipped or passes.
    """
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    subject, author = head_commit_subject_and_author()
    if author == "dependabot[bot]":
        LOGGER.info(f"Checks SKIPPED: Dependabot commit ({subject!r}).")
        return 0
    for pattern in RELEASE_COMMIT_PATTERNS:
        if re.match(pattern, subject):
            LOGGER.info(f"Checks SKIPPED: release commit ({subject!r}).")
            return 0
    return subprocess.run(
        ["uv", "run", "towncrier", "check", "--compare-with", "origin/main"]
    ).returncode


if __name__ == "__main__":
    raise SystemExit(main())
