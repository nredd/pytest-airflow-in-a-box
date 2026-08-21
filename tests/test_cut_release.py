"""Test the guard paths of `scripts/cut_release.py`.

Drives the real script with stubbed `uv` and `git` executables, mirroring
`tests/test_release.py`'s approach for the `release` make target -- nothing here touches a
real repository, remote, or PyPI.

References:
    https://docs.astral.sh/uv/reference/cli/#uv-version
"""

from __future__ import annotations

import os
import shlex
import shutil
import stat
import subprocess
import sys
import textwrap
from dataclasses import dataclass
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "cut_release.py"
INIT_RELATIVE_PATH = Path("src") / "pytest_airflow_in_a_box" / "__init__.py"
CURRENT_VERSION = "1.0.0"
TARGET_VERSION = "1.1.0"


@dataclass(frozen=True)
class ReleaseSandbox:
    """Hold the staged project and its stub environment.

    Attributes:
        project: pathlib.Path containing the working directory the script runs in.
        bin_dir: pathlib.Path containing the stub `uv` and `git` executables.
        version_state_path: pathlib.Path holding the stub `uv`'s current version.
        commit_log_path: pathlib.Path recording each stub `git commit` message.
        push_log_path: pathlib.Path recording each stub `git push` invocation.
    """

    project: Path
    bin_dir: Path
    version_state_path: Path
    commit_log_path: Path
    push_log_path: Path


def _write_stub(path: Path, body: str) -> None:
    """Write an executable POSIX shell stub.

    Parameters:
        path: pathlib.Path containing the stub to create.
        body: str containing the shell body placed after the shebang.
    """
    path.write_text(f"#!/bin/sh\n{textwrap.dedent(body).strip()}\n", encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def _make_sandbox(
    tmp_path: Path,
    *,
    branch: str = "main",
    git_status: str = "",
    tag_exit_code: int = 1,
    current_version: str = CURRENT_VERSION,
    init_version: str = CURRENT_VERSION,
) -> ReleaseSandbox:
    """Stage the real `cut_release.py` against stubbed `uv` and `git` executables.

    Parameters:
        tmp_path: pathlib.Path containing the sandbox root.
        branch: str the stub `git branch --show-current` reports.
        git_status: str the stub `git status --porcelain` reports.
        tag_exit_code: int exit status of `git rev-parse --verify refs/tags/...`
            (0 means the tag already exists).
        current_version: str the stub `uv version --short` initially reports.
        init_version: str written into the sandbox's `__init__.py`.

    Returns:
        ReleaseSandbox describing the staged project and its stub directory.
    """
    project = tmp_path / "project"
    init_path = project / INIT_RELATIVE_PATH
    init_path.parent.mkdir(parents=True)
    init_path.write_text(f'__version__ = "{init_version}"\n', encoding="utf-8")

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    version_state_path = tmp_path / "uv-version-state"
    version_state_path.write_text(current_version, encoding="utf-8")
    commit_log_path = tmp_path / "commit-log"
    commit_log_path.write_text("", encoding="utf-8")
    push_log_path = tmp_path / "push-log"
    push_log_path.write_text("", encoding="utf-8")

    _write_stub(
        bin_dir / "uv",
        f"""
        case "$1" in
        version)
            if [ "$2" = "--short" ]; then
                cat {shlex.quote(str(version_state_path))}
            else
                printf '%s' "$2" > {shlex.quote(str(version_state_path))}
            fi
            ;;
        run)
            [ "$2" = "towncrier" ] || {{ printf 'unexpected uv run: %s\\n' "$*" >&2; exit 64; }}
            exit 0
            ;;
        *) printf 'unexpected uv: %s\\n' "$*" >&2; exit 64 ;;
        esac
        """,
    )
    _write_stub(
        bin_dir / "git",
        f"""
        case "$1" in
        branch) printf '%s\\n' {shlex.quote(branch)} ;;
        status) printf '%s' {shlex.quote(git_status)} ;;
        rev-parse) exit {tag_exit_code} ;;
        add) exit 0 ;;
        commit)
            shift
            printf '%s\\n' "$*" >> {shlex.quote(str(commit_log_path))}
            exit 0
            ;;
        tag) exit 0 ;;
        push)
            shift
            printf '%s\\n' "$*" >> {shlex.quote(str(push_log_path))}
            exit 0
            ;;
        *) printf 'unexpected git: %s\\n' "$*" >&2; exit 64 ;;
        esac
        """,
    )
    return ReleaseSandbox(
        project=project,
        bin_dir=bin_dir,
        version_state_path=version_state_path,
        commit_log_path=commit_log_path,
        push_log_path=push_log_path,
    )


def _run_cut_release(
    sandbox: ReleaseSandbox, version: str = TARGET_VERSION
) -> subprocess.CompletedProcess[str]:
    """Run the real `cut_release.py` inside the sandbox with the stubs ahead of real tools.

    Parameters:
        sandbox: ReleaseSandbox describing the staged project and stub directory.
        version: str passed as the script's sole argument.

    Returns:
        subprocess.CompletedProcess[str] carrying the script's status and output.
    """
    env = os.environ | {"PATH": f"{sandbox.bin_dir}{os.pathsep}{os.environ['PATH']}"}
    return subprocess.run(
        [sys.executable, str(SCRIPT_PATH), version],
        cwd=sandbox.project,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )


pytestmark = pytest.mark.skipif(shutil.which("sh") is None, reason="requires a POSIX shell")


def test_rejects_a_malformed_version_before_touching_anything(tmp_path: Path) -> None:
    """Reject a non-semver argument without invoking git or uv at all."""
    sandbox = _make_sandbox(tmp_path)

    result = _run_cut_release(sandbox, version="not-a-version")

    assert result.returncode != 0
    assert "usage:" in result.stderr
    assert sandbox.push_log_path.read_text(encoding="utf-8") == ""


def test_rejects_a_trailing_newline_in_the_version(tmp_path: Path) -> None:
    """Reject a version argument with a trailing newline instead of matching it loosely."""
    sandbox = _make_sandbox(tmp_path)

    result = _run_cut_release(sandbox, version=f"{TARGET_VERSION}\n")

    assert result.returncode != 0
    assert "usage:" in result.stderr


def test_non_main_branch_aborts_before_any_mutation(tmp_path: Path) -> None:
    """Refuse to run from a branch other than `main`."""
    sandbox = _make_sandbox(tmp_path, branch="feature")

    result = _run_cut_release(sandbox)

    assert result.returncode != 0
    assert "must run from 'main'" in result.stderr
    assert sandbox.push_log_path.read_text(encoding="utf-8") == ""
    assert sandbox.commit_log_path.read_text(encoding="utf-8") == ""


def test_dirty_tree_aborts(tmp_path: Path) -> None:
    """Refuse to run against an unclean working tree."""
    sandbox = _make_sandbox(tmp_path, git_status=" M some-file")

    result = _run_cut_release(sandbox)

    assert result.returncode != 0
    assert "working tree is not clean" in result.stderr
    assert sandbox.push_log_path.read_text(encoding="utf-8") == ""


def test_existing_tag_aborts(tmp_path: Path) -> None:
    """Refuse to re-cut a version whose tag already exists."""
    sandbox = _make_sandbox(tmp_path, tag_exit_code=0)

    result = _run_cut_release(sandbox)

    assert result.returncode != 0
    assert "already exists" in result.stderr
    assert sandbox.push_log_path.read_text(encoding="utf-8") == ""


def test_non_newer_version_aborts(tmp_path: Path) -> None:
    """Refuse to cut a version that is not newer than the current one."""
    sandbox = _make_sandbox(tmp_path, current_version="2.0.0", init_version="2.0.0")

    result = _run_cut_release(sandbox, version="1.5.0")

    assert result.returncode != 0
    assert "is not newer than" in result.stderr
    assert sandbox.push_log_path.read_text(encoding="utf-8") == ""


def test_inconsistent_partial_state_raises(tmp_path: Path) -> None:
    """Fail loudly when the two version files disagree instead of guessing."""
    sandbox = _make_sandbox(tmp_path, current_version=TARGET_VERSION, init_version=CURRENT_VERSION)

    result = _run_cut_release(sandbox)

    assert result.returncode != 0
    assert "inconsistent version state" in result.stderr
    assert sandbox.push_log_path.read_text(encoding="utf-8") == ""


def test_idempotent_retry_skips_the_bump_but_still_tags_and_pushes(tmp_path: Path) -> None:
    """Treat both files already reading the target version as a resumable retry."""
    sandbox = _make_sandbox(tmp_path, current_version=TARGET_VERSION, init_version=TARGET_VERSION)

    result = _run_cut_release(sandbox)

    assert result.returncode == 0, result.stderr
    assert sandbox.commit_log_path.read_text(encoding="utf-8") == ""
    pushes = sandbox.push_log_path.read_text(encoding="utf-8").splitlines()
    assert "origin main" in pushes
    assert f"origin v{TARGET_VERSION}" in pushes


def test_successful_run_bumps_commits_tags_and_pushes(tmp_path: Path) -> None:
    """Bump the version, commit once, tag, and push both the branch and the tag."""
    sandbox = _make_sandbox(tmp_path)

    result = _run_cut_release(sandbox)

    assert result.returncode == 0, result.stderr
    assert sandbox.version_state_path.read_text(encoding="utf-8") == TARGET_VERSION
    commits = sandbox.commit_log_path.read_text(encoding="utf-8").splitlines()
    assert commits == [f"-m Bump version to {TARGET_VERSION}"]
    pushes = sandbox.push_log_path.read_text(encoding="utf-8").splitlines()
    assert "origin main" in pushes
    assert f"origin v{TARGET_VERSION}" in pushes
