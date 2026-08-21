"""Test the release-commit exemption in `scripts/check_changelog_fragment.py`."""

from __future__ import annotations

import os
import shlex
import stat
import subprocess
import sys
import textwrap
from pathlib import Path

REPO_ROOT = Path(__file__).parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "check_changelog_fragment.py"


def _write_stub(path: Path, body: str) -> None:
    """Write an executable POSIX shell stub.

    Parameters:
        path: pathlib.Path containing the stub to create.
        body: str containing the shell body placed after the shebang.
    """
    path.write_text(f"#!/bin/sh\n{textwrap.dedent(body).strip()}\n", encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def _run_check(
    tmp_path: Path, subject: str, *, towncrier_exit_code: int = 0
) -> tuple[subprocess.CompletedProcess[str], Path]:
    """Run the wrapper against a real one-commit git repo with a stub `uv`.

    Parameters:
        tmp_path: pathlib.Path containing the sandbox root.
        subject: str used as the sole commit's message.
        towncrier_exit_code: int the stub `towncrier check` invocation exits with.

    Returns:
        tuple[subprocess.CompletedProcess[str], pathlib.Path] carrying the wrapper's
        result and the marker file the stub `uv` writes only when it is invoked.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=repo, check=True)
    (repo / "placeholder.txt").write_text("x\n", encoding="utf-8")
    subprocess.run(["git", "add", "placeholder.txt"], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-q", "-m", subject], cwd=repo, check=True)

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    invoked_marker = tmp_path / "towncrier-invoked"
    _write_stub(
        bin_dir / "uv",
        f"""
        if [ "$1" = "run" ] && [ "$2" = "towncrier" ]; then
            touch {shlex.quote(str(invoked_marker))}
            exit {towncrier_exit_code}
        fi
        printf 'unexpected uv: %s\\n' "$*" >&2
        exit 64
        """,
    )

    env = os.environ | {"PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}"}
    result = subprocess.run(
        [sys.executable, str(SCRIPT_PATH)],
        cwd=repo,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    return result, invoked_marker


def test_exempts_a_version_bump_commit(tmp_path: Path) -> None:
    """Skip the check outright for a `Bump version to X.Y.Z` commit."""
    result, invoked_marker = _run_check(tmp_path, "Bump version to 1.2.3")

    assert result.returncode == 0
    assert "SKIPPED" in result.stderr
    assert not invoked_marker.exists()


def test_exempts_a_changelog_build_commit(tmp_path: Path) -> None:
    """Skip the check outright for an `Update CHANGELOG.md for vX.Y.Z` commit."""
    result, invoked_marker = _run_check(tmp_path, "Update CHANGELOG.md for v1.2.3")

    assert result.returncode == 0
    assert "SKIPPED" in result.stderr
    assert not invoked_marker.exists()


def test_does_not_exempt_a_look_alike_subject(tmp_path: Path) -> None:
    """Do not exempt a commit that merely starts with a release-shaped prefix."""
    result, invoked_marker = _run_check(
        tmp_path, "Bump version to 3 for dependency compat", towncrier_exit_code=1
    )

    assert result.returncode == 1
    assert invoked_marker.exists()


def test_delegates_an_ordinary_commit_to_towncrier(tmp_path: Path) -> None:
    """Delegate to `towncrier check` and propagate its exit code for a normal commit."""
    result, invoked_marker = _run_check(tmp_path, "Add a new fixture", towncrier_exit_code=1)

    assert result.returncode == 1
    assert invoked_marker.exists()
