"""Cut a release: bump the version, build the changelog, tag, and push.

Used exclusively by `.github/workflows/cut-release.yml`'s restricted `workflow_dispatch`
job, running from a clean checkout of `main` authenticated with a PAT (not `GITHUB_TOKEN`)
so the resulting push/tag trigger `ci.yml`/`release.yml` normally. `make release` remains
the separate local/manual fallback for tagging an already-bumped `main`.

`bump_version` is idempotent -- if `pyproject.toml` and `__init__.py` already read the
target version (a retry after a prior run pushed the version bump but died before the tag
push), it skips straight to `build_changelog`/tagging instead of failing on an empty commit.

References:
    https://docs.astral.sh/uv/reference/cli/#uv-version
    https://towncrier.readthedocs.io/en/latest/cli.html#towncrier-build
"""

from __future__ import annotations

import datetime
import logging
import re
import subprocess
import sys
from pathlib import Path

from packaging.version import Version

LOGGER = logging.getLogger(__name__)

VERSION_PATTERN = re.compile(r"^\d+\.\d+\.\d+\Z")
INIT_FILE = Path("src/pytest_airflow_in_a_box/__init__.py")
INIT_VERSION_PATTERN = re.compile(r'^__version__ = "(?P<version>.*)"$', re.MULTILINE)
CHANGELOG_FRAGMENT_GLOB = "*.md"
CHANGELOG_FRAGMENT_EXCLUDE = "README.md"


def run(*args: str) -> None:
    """Run a command, streaming output, raising on a non-zero exit.

    Parameters:
        args: str command and arguments to execute.

    Raises:
        subprocess.CalledProcessError: The command exited non-zero.
    """
    LOGGER.info(f"+ {' '.join(args)}")
    subprocess.run(args, check=True)


def capture(*args: str) -> str:
    """Run a command and return its stripped stdout, raising on a non-zero exit.

    Parameters:
        args: str command and arguments to execute.

    Returns:
        str containing the command's stripped stdout.

    Raises:
        subprocess.CalledProcessError: The command exited non-zero.
    """
    return subprocess.run(args, capture_output=True, text=True, check=True).stdout.strip()


def current_branch() -> str:
    """Return the name of the currently checked-out branch.

    Returns:
        str containing the branch name, empty when `HEAD` is detached.

    Raises:
        subprocess.CalledProcessError: `git branch --show-current` failed.
    """
    return capture("git", "branch", "--show-current")


def pyproject_version() -> str:
    """Return the version currently recorded in `pyproject.toml`.

    Returns:
        str containing the version.

    Raises:
        subprocess.CalledProcessError: `uv version --short` failed.
    """
    return capture("uv", "version", "--short", "--color", "never")


def init_version() -> str:
    """Return the version currently recorded in `__init__.py`.

    Returns:
        str containing the version.

    Raises:
        RuntimeError: `INIT_FILE` has no `__version__` assignment.
    """
    match = INIT_VERSION_PATTERN.search(INIT_FILE.read_text(encoding="utf-8"))
    if match is None:
        raise RuntimeError(f"could not find __version__ assignment in '{INIT_FILE}'")
    return match.group("version")


def working_tree_is_clean() -> bool:
    """Report whether the working tree has no pending changes.

    Returns:
        bool True when `git status --porcelain` is empty.

    Raises:
        subprocess.CalledProcessError: `git status` failed.
    """
    return not capture("git", "status", "--porcelain")


def tag_exists(tag: str) -> bool:
    """Report whether `tag` already exists in this repository.

    Parameters:
        tag: str containing the tag name, e.g. "v0.10.0".

    Returns:
        bool True when the tag already exists.
    """
    result = subprocess.run(
        ["git", "rev-parse", "--quiet", "--verify", f"refs/tags/{tag}"], capture_output=True
    )
    return result.returncode == 0


def pending_changelog_fragments() -> list[Path]:
    """List changelog fragments awaiting release, excluding the README.

    Returns:
        list[pathlib.Path] containing pending fragment files, possibly empty.
    """
    return sorted(
        path
        for path in Path("changelog.d").glob(CHANGELOG_FRAGMENT_GLOB)
        if path.name != CHANGELOG_FRAGMENT_EXCLUDE
    )


def bump_version(version: str) -> None:
    """Write `version` into `pyproject.toml`, `uv.lock`, and `__init__.py`, committing it.

    A no-op when both files already read `version` -- see the module docstring.

    Parameters:
        version: str target version, e.g. "0.10.0".

    Raises:
        RuntimeError: The two version files disagree, either before or after the bump.
    """
    before_pyproject, before_init = pyproject_version(), init_version()
    if before_pyproject == version and before_init == version:
        LOGGER.info(f"pyproject.toml and __init__.py already read {version}; skipping bump.")
        return
    if before_pyproject == version or before_init == version:
        raise RuntimeError(
            f"inconsistent version state: pyproject.toml={before_pyproject} "
            f"__init__.py={before_init}, target={version}"
        )

    run("uv", "version", version)
    init_text = INIT_FILE.read_text(encoding="utf-8")
    new_init_text = INIT_VERSION_PATTERN.sub(f'__version__ = "{version}"', init_text, count=1)
    INIT_FILE.write_text(new_init_text, encoding="utf-8")

    after_pyproject, after_init = pyproject_version(), init_version()
    if after_pyproject != version or after_init != version:
        raise RuntimeError(
            f"version bump did not converge: pyproject.toml={after_pyproject} "
            f"__init__.py={after_init}, target={version}"
        )

    run("git", "add", "pyproject.toml", "uv.lock", str(INIT_FILE))
    run("git", "commit", "-m", f"Bump version to {version}")


def build_changelog(version: str) -> None:
    """Consume pending changelog fragments into `CHANGELOG.md`, committing the result.

    A no-op when no fragments are pending.

    Parameters:
        version: str target version, e.g. "0.10.0".
    """
    if not pending_changelog_fragments():
        LOGGER.info("No pending changelog.d fragments; skipping CHANGELOG.md update.")
        return
    today = datetime.datetime.now(tz=datetime.timezone.utc).date().isoformat()
    run("uv", "run", "towncrier", "build", "--version", version, "--date", today, "--yes")
    run("git", "add", "CHANGELOG.md", "changelog.d")
    run("git", "commit", "-m", f"Update CHANGELOG.md for v{version}")


def main(argv: list[str]) -> int:
    """Bump the version, build the changelog, and push a release commit + tag to `main`.

    Parameters:
        argv: list[str] containing the target version as its sole element, e.g. "0.10.0".

    Returns:
        int process exit status, zero on success.
    """
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if len(argv) != 1 or not VERSION_PATTERN.match(argv[0]):
        LOGGER.error("usage: cut_release.py X.Y.Z")
        return 1
    version = argv[0]
    tag = f"v{version}"

    branch = current_branch()
    if branch != "main":
        LOGGER.error(f"must run from 'main', got '{branch or '(detached HEAD)'}'")
        return 1
    if not working_tree_is_clean():
        LOGGER.error("working tree is not clean")
        return 1
    if tag_exists(tag):
        LOGGER.error(f"tag '{tag}' already exists")
        return 1
    current = pyproject_version()
    if Version(version) < Version(current):
        LOGGER.error(f"version {version} is not newer than the current version {current}")
        return 1

    bump_version(version)
    build_changelog(version)
    run("git", "push", "origin", "main")
    run("git", "tag", "-a", tag, "-m", tag)
    run("git", "push", "origin", tag)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
