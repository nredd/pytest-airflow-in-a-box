"""Wrap `towncrier check` with an exemption for release-cutting commits.

`towncrier check` can already tell that a commit doesn't need its own changelog.d
fragment when that commit's diff includes `CHANGELOG.md` itself -- but a "Bump version to
X.Y.Z" commit (`scripts/cut_release.py`'s first commit) never touches `CHANGELOG.md`, and
on a feature/PR branch checked before the changelog-build commit exists, towncrier's own
skip never engages. Skip the check outright for commits matching either of
`cut_release.py`'s two release-commit message patterns; everything else defers to
towncrier normally.

References:
    https://towncrier.readthedocs.io/en/latest/cli.html#towncrier-check
"""

from __future__ import annotations

import logging
import re
import subprocess

LOGGER = logging.getLogger(__name__)

RELEASE_COMMIT_PATTERNS = (
    r"^Bump version to \d",
    r"^Update CHANGELOG\.md for v\d",
)


def head_commit_subject() -> str:
    """Return the subject line of the current `HEAD` commit.

    Returns:
        str containing the commit subject.
    """
    return subprocess.run(
        ["git", "log", "-1", "--pretty=%s"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def main() -> int:
    """Skip the changelog-fragment check for release commits, else delegate to towncrier.

    Returns:
        int process exit status, zero when the check is skipped or passes.
    """
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    subject = head_commit_subject()
    for pattern in RELEASE_COMMIT_PATTERNS:
        if re.match(pattern, subject):
            LOGGER.info(f"Checks SKIPPED: release commit ({subject!r}).")
            return 0
    return subprocess.run(
        ["uv", "run", "towncrier", "check", "--compare-with", "origin/main"]
    ).returncode


if __name__ == "__main__":
    raise SystemExit(main())
