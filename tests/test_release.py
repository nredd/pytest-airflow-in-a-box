"""Test the failure handling of the `release` make target.

The recipe validates the version and the working tree, then tags and pushes. Those
mutating steps used to be separated by `;` in a recipe without `set -e`, so a failed
`git tag` still ran `git push` and still printed the success banner. These tests drive
the real `Makefile` with stubbed `uv` and `git` executables, so nothing touches a real
repository or remote.

References:
    - https://github.com/nredd/pytest-airflow-in-a-box/issues/152
    - https://www.gnu.org/software/make/manual/html_node/Errors.html
"""

from __future__ import annotations

import os
import shlex
import shutil
import stat
import subprocess
import textwrap
from dataclasses import dataclass
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parents[1]
MAKEFILE_PATH = REPO_ROOT / "Makefile"
INIT_RELATIVE_PATH = Path("src") / "pytest_airflow_in_a_box" / "__init__.py"
STUB_VERSION = "9.9.9"
STUB_TAG = f"v{STUB_VERSION}"

pytestmark = pytest.mark.skipif(
    shutil.which("make") is None, reason="`make` is not available on this platform"
)


@dataclass(frozen=True)
class ReleaseSandbox:
    """Hold the staged copy of the `release` target and its stub environment.

    Attributes:
        project: pathlib.Path containing the working directory `make` runs in.
        bin_dir: pathlib.Path containing the stub `uv` and `git` executables.
        push_marker: pathlib.Path written by the stub `git` only when `push` runs.
    """

    project: Path
    bin_dir: Path
    push_marker: Path


def _write_stub(path: Path, body: str) -> None:
    """Write an executable POSIX shell stub.

    Parameters:
        path: pathlib.Path containing the stub to create.
        body: str containing the shell body placed after the shebang.

    Returns:
        None.
    """

    path.write_text(f"#!/bin/sh\n{textwrap.dedent(body).strip()}\n", encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def _make_release_sandbox(
    tmp_path: Path,
    *,
    uv_version: str = STUB_VERSION,
    init_version: str = STUB_VERSION,
    git_status: str = "",
    tag_exit_code: int = 0,
) -> ReleaseSandbox:
    """Stage the real `release` recipe against stubbed `uv` and `git` executables.

    Parameters:
        tmp_path: pathlib.Path containing the sandbox root.
        uv_version: str containing the version the stub `uv version` reports.
        init_version: str containing the version written to `__init__.py`.
        git_status: str containing the stub `git status --porcelain` output.
        tag_exit_code: int containing the exit status of the stub `git tag`.

    Returns:
        ReleaseSandbox describing the staged project and its stub directory.
    """

    project = tmp_path / "project"
    init_path = project / INIT_RELATIVE_PATH
    init_path.parent.mkdir(parents=True)
    init_path.write_text(f'__version__ = "{init_version}"\n', encoding="utf-8")
    shutil.copy(MAKEFILE_PATH, project / "Makefile")

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    push_marker = tmp_path / "push-arguments"
    _write_stub(
        bin_dir / "uv",
        f"""
        [ "$1" = "version" ] || {{ printf 'unexpected uv: %s\\n' "$*" >&2; exit 64; }}
        printf '%s\\n' {shlex.quote(uv_version)}
        """,
    )
    _write_stub(
        bin_dir / "git",
        f"""
        case "$1" in
        status) printf '%s' {shlex.quote(git_status)} ;;
        tag)
            [ {tag_exit_code} -eq 0 ] || printf 'fatal: tag already exists\\n' >&2
            exit {tag_exit_code}
            ;;
        push) printf '%s\\n' "$*" > {shlex.quote(str(push_marker))} ;;
        *) printf 'unexpected git: %s\\n' "$*" >&2; exit 64 ;;
        esac
        """,
    )
    return ReleaseSandbox(project=project, bin_dir=bin_dir, push_marker=push_marker)


def _run_release(sandbox: ReleaseSandbox) -> subprocess.CompletedProcess[str]:
    """Run `make release` inside the sandbox with the stubs ahead of the real tools.

    Parameters:
        sandbox: ReleaseSandbox describing the staged project and stub directory.

    Returns:
        subprocess.CompletedProcess[str] carrying the recipe's status and output.
    """

    env = os.environ | {"PATH": f"{sandbox.bin_dir}{os.pathsep}{os.environ['PATH']}"}
    return subprocess.run(
        ["make", "release"],
        cwd=sandbox.project,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )


def test_failed_tag_stops_before_push(tmp_path: Path) -> None:
    """Fail the recipe on a failed `git tag` without pushing or reporting success."""

    sandbox = _make_release_sandbox(tmp_path, tag_exit_code=128)

    result = _run_release(sandbox)

    assert result.returncode != 0
    assert not sandbox.push_marker.exists()
    assert "pushed" not in result.stdout
    assert "gh release create" not in result.stdout


def test_successful_tag_pushes_and_reports(tmp_path: Path) -> None:
    """Keep the success path tagging, pushing, and printing the publish command."""

    sandbox = _make_release_sandbox(tmp_path)

    result = _run_release(sandbox)

    assert result.returncode == 0
    assert sandbox.push_marker.read_text(encoding="utf-8") == f"push origin {STUB_TAG}\n"
    assert f"Tag {STUB_TAG} pushed." in result.stdout
    assert f"gh release create {STUB_TAG} --title {STUB_TAG}" in result.stdout


@pytest.mark.parametrize(
    ("init_version", "git_status", "expected_message"),
    [
        pytest.param("0.0.1", "", "version mismatch", id="version-mismatch"),
        pytest.param(STUB_VERSION, " M Makefile", "not clean", id="dirty-tree"),
    ],
)
def test_guards_still_reject_before_tagging(
    tmp_path: Path, init_version: str, git_status: str, expected_message: str
) -> None:
    """Keep the version and clean-tree guards failing ahead of any mutation.

    Parameters:
        tmp_path: pathlib.Path containing the sandbox root.
        init_version: str containing the version written to `__init__.py`.
        git_status: str containing the stub `git status --porcelain` output.
        expected_message: str containing the fragment the guard writes to stderr.
    """

    sandbox = _make_release_sandbox(tmp_path, init_version=init_version, git_status=git_status)

    result = _run_release(sandbox)

    assert result.returncode != 0
    assert expected_message in result.stderr
    assert not sandbox.push_marker.exists()
    assert result.stdout == ""
