"""Cut a release: bump the version, build the changelog, tag, and push.

Used exclusively by `.github/workflows/cut-release.yml`'s restricted `workflow_dispatch`
job, running from a clean checkout of `main` authenticated with a PAT (not `GITHUB_TOKEN`)
so the resulting push/tag trigger `ci.yml`/`release.yml` normally. `make release` remains
the separate local/manual fallback for tagging an already-bumped `main`.

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

LOGGER = logging.getLogger(__name__)

VERSION_PATTERN = re.compile(r"^\d+\.\d+\.\d+$")
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


def working_tree_is_clean() -> bool:
    """Report whether the working tree has no pending changes.

    Returns:
        bool True when `git status --porcelain` is empty.
    """
    status = subprocess.run(
        ["git", "status", "--porcelain"], capture_output=True, text=True, check=True
    )
    return not status.stdout.strip()


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
    """Write `version` into `pyproject.toml` and `__init__.py`, relocking the lockfile.

    Parameters:
        version: str target version, e.g. "0.10.0".
    """
    run("uv", "version", version)
    init_text = INIT_FILE.read_text(encoding="utf-8")
    new_init_text = INIT_VERSION_PATTERN.sub(f'__version__ = "{version}"', init_text, count=1)
    if new_init_text == init_text:
        raise RuntimeError(f"could not find __version__ assignment in '{INIT_FILE}'")
    INIT_FILE.write_text(new_init_text, encoding="utf-8")
    run("git", "add", "pyproject.toml", "uv.lock", str(INIT_FILE))
    run("git", "commit", "-m", f"Bump version to {version}")


def build_changelog(version: str) -> None:
    """Consume pending changelog fragments into `CHANGELOG.md`, committing the result.

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

    if not working_tree_is_clean():
        LOGGER.error("working tree is not clean")
        return 1
    if tag_exists(tag):
        LOGGER.error(f"tag '{tag}' already exists")
        return 1

    bump_version(version)
    build_changelog(version)
    run("git", "push", "origin", "main")
    run("git", "tag", "-a", tag, "-m", tag)
    run("git", "push", "origin", tag)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
