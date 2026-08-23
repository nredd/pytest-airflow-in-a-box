"""Surface the isolated ``AIRFLOW_HOME`` and decide whether it survives the run.

``bootstrap._owner_state`` creates a throwaway ``AIRFLOW_HOME`` before consumer
conftests are imported and, historically, removed it unconditionally from a
``config.add_cleanup`` closure. Nothing ever printed the path, so a failing run left
no artifacts behind and no way to find where they would have been -- most acutely on
Linux, where the storage ladder frequently lands the run on ``/dev/shm``.

Two pieces live here. ``report_header`` renders one ``pytest_report_header`` line
naming the run root, the storage-ladder rung that chose it, and the metadata database
backend. ``retain_airflow_home`` resolves a retention policy -- ``all`` / ``failed`` /
``none``, defaulting to ``failed`` -- that decides whether ``bootstrap``'s
``shutil.rmtree`` runs at all, and ``terminal_summary`` reports a surviving directory
next to the run's failures rather than only in the startup header. The vocabulary and
the default deliberately mirror pytest's own ``tmp_path_retention_policy``, which
``defaults.INI_DEFAULTS`` already re-registers a ``failed`` default for.

The cleanup closure receives no arguments and never sees the session outcome, so it is
threaded through the config stash in two steps. ``mark_session_started`` writes a
pessimistic "failed" from ``pytest_sessionstart``, and ``record_session_outcome``
overwrites it with the real answer from ``pytest_sessionfinish`` -- pytest dispatches
every non-wrapper ``pytest_sessionfinish`` hookimpl before the terminal reporter's
wrapper calls ``pytest_terminal_summary``, and both run before any registered cleanup.
A run that started and never finished therefore keeps its artifacts, which is the whole
point: the run that died is exactly the one worth looking at.

The two steps are needed because "the session crashed" and "no session ever ran" are
otherwise indistinguishable from an empty stash, and bootstrap happens far earlier than
either. ``pytest --help``, ``pytest --markers``, an argparse usage error, and a
configure-time abort all provision a run root and none of them is a failed test run;
with a single record they would all retain, forever, with nothing to inspect inside.
An absent mark now means no session started, and the directory goes.

Only the ``rmtree`` is conditional. The provisioner still stops on every policy,
``none`` included, so a retained run never leaks the testcontainers Postgres container
behind ``--airflow-db-backend=postgres``.

Both terminal channels can be absent on exactly the runs most likely to retain -- a
session that dies during configure prints no header, and an ``INTERNAL_ERROR`` exit
never reaches ``pytest_terminal_summary`` -- so ``announce_retained_root`` writes the
path to ``stderr`` from cleanup whenever neither channel got there first. A kept
directory is never kept silently.

Retained roots are also bounded. Whenever cleanup decides to keep a directory it calls
``mark_root_retained`` -- writing the ``RETAINED_MARKER_NAME`` sentinel -- and then
``prune_retained_roots``, which removes everything past the ``keep`` most recently
retained siblings under the same storage base, mirroring pytest's own
``tmp_path_retention_count``. Pruning only ever considers a marked sibling, so an
in-flight run (this process's own about-to-be-created root, or another concurrent
invocation sharing the same base) is never a candidate -- unlike pytest's numbered dirs,
``tempfile.mkdtemp`` gives these roots no sequence number or lock file to tell "finished
and kept" apart from "still running" any other way. The invariant holds immediately after
every retaining run, not just eventually: the directory that was just retained is always
the newest marked entry, so pruning never removes it.

References:
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.hookspec.pytest_report_header
    https://docs.pytest.org/en/stable/reference/reference.html#confval-tmp_path_retention_policy
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.Config.add_cleanup
"""

from __future__ import annotations

import logging
import os
import shutil
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from pytest_airflow_in_a_box.storage.locate import (
    RUN_DIRECTORY_PREFIX,
    SHARED_MEMORY_PATH,
    StorageReason,
    StrEnum,
)

if TYPE_CHECKING:
    # Type-only to keep the runtime dependency one-directional: `bootstrap` imports
    # `retain_airflow_home` from this module at module scope.
    from pytest_airflow_in_a_box.bootstrap import BootstrapState

LOGGER = logging.getLogger(__name__)

RETENTION_INI = "airflow_home_retention_policy"
RETENTION_OPTION = "airflow_home_retention"
RETENTION_COUNT_INI = "airflow_home_retention_count"
RETENTION_COUNT_OPTION = "airflow_home_retention_count"
HEADER_PREFIX = "pytest-airflow-in-a-box"
SUMMARY_TITLE = "airflow-in-a-box"
RETAINED_MARKER_NAME = ".retained"


class RetentionPolicy(StrEnum):
    """Which runs keep their isolated ``AIRFLOW_HOME`` instead of removing it."""

    ALL = "all"
    FAILED = "failed"
    NONE = "none"


DEFAULT_RETENTION_POLICY = RetentionPolicy.FAILED
DEFAULT_RETENTION_COUNT = 3

RETENTION_POLICY_KEY = pytest.StashKey[RetentionPolicy]()
RETENTION_COUNT_KEY = pytest.StashKey[int]()
SESSION_FAILED_KEY = pytest.StashKey[bool]()
RETENTION_ANNOUNCED_KEY = pytest.StashKey[bool]()

# Every other exit status -- tests failed, interrupted, internal error, usage error, a
# plugin's own code -- counts as a failure worth keeping artifacts for.
# `NO_TESTS_COLLECTED` is the one non-zero status that does not: nothing ever ran, so
# nothing touched the isolated run directory and there is nothing in it to inspect.
CLEAN_EXIT_STATUSES: frozenset[int] = frozenset(
    {int(pytest.ExitCode.OK), int(pytest.ExitCode.NO_TESTS_COLLECTED)}
)


def register_options(parser: pytest.Parser) -> None:
    """Register the retention command-line option and its ini counterpart.

    Parameters:
        parser: pytest.Parser receiving the command-line and ini registrations.
    """

    group = parser.getgroup("airflow-in-a-box")
    group.addoption(
        "--airflow-home-retention",
        action="store",
        default=None,
        dest=RETENTION_OPTION,
        choices=tuple(str(policy) for policy in RetentionPolicy),
        metavar="POLICY",
        help=(
            "Keep the isolated Airflow run directory after `all` runs, after a "
            "`failed` run only, or after `none`."
        ),
    )
    parser.addini(
        RETENTION_INI,
        "Which runs keep the isolated Airflow run directory: `all`, `failed`, or `none`.",
        default=str(DEFAULT_RETENTION_POLICY),
    )
    group.addoption(
        "--airflow-home-retention-count",
        action="store",
        default=None,
        dest=RETENTION_COUNT_OPTION,
        metavar="N",
        help=(
            "Keep at most N retained Airflow run directories per storage base, "
            f"garbage-collecting older ones whenever a run is retained (default: "
            f"{DEFAULT_RETENTION_COUNT})."
        ),
    )
    parser.addini(
        RETENTION_COUNT_INI,
        "Maximum number of retained Airflow run directories to keep per storage base.",
        default=str(DEFAULT_RETENTION_COUNT),
    )


def resolve_retention_policy(config: pytest.Config) -> RetentionPolicy:
    """Resolve and cache the effective ``AIRFLOW_HOME`` retention policy.

    The command-line option wins when supplied; otherwise the ini value decides,
    defaulting to ``failed``. Mirrors ``migration_strict.migration_strict_enabled``'s
    resolution and caching pattern. ``plugin.pytest_configure`` calls this eagerly so a
    malformed ini value aborts the session with an actionable usage error rather than
    raising from inside a cleanup callback at unconfigure time.

    Parameters:
        config: pytest.Config containing plugin options and ini values.

    Returns:
        RetentionPolicy deciding which runs keep the isolated run directory.

    Raises:
        pytest.UsageError: The resolved value is not a supported policy.
    """

    if RETENTION_POLICY_KEY in config.stash:
        return config.stash[RETENTION_POLICY_KEY]

    option_value: object = config.getoption(RETENTION_OPTION)
    raw: object = (
        option_value
        if isinstance(option_value, str) and option_value
        else config.getini(RETENTION_INI)
    )
    message = f"Ini/option `{RETENTION_INI}` must be `all`, `failed`, or `none`: '{raw}'"
    if not isinstance(raw, str) or not raw:
        raise pytest.UsageError(message)
    try:
        policy = RetentionPolicy(raw)
    except ValueError as error:
        raise pytest.UsageError(message) from error
    config.stash[RETENTION_POLICY_KEY] = policy
    return policy


def resolve_retention_count(config: pytest.Config) -> int:
    """Resolve and cache the maximum number of retained run directories to keep.

    Same option-wins-else-ini resolution as ``resolve_retention_policy``, called
    eagerly for the same reason: a malformed value aborts the session with an
    actionable usage error instead of raising from inside the cleanup closure that
    reads it back via ``retention_count`` at unconfigure time.

    Parameters:
        config: pytest.Config containing plugin options and ini values.

    Returns:
        int containing the maximum number of retained run directories to keep.

    Raises:
        pytest.UsageError: The resolved value is not a positive integer.
    """

    if RETENTION_COUNT_KEY in config.stash:
        return config.stash[RETENTION_COUNT_KEY]

    option_value: object = config.getoption(RETENTION_COUNT_OPTION)
    raw: object = (
        option_value
        if isinstance(option_value, str) and option_value
        else config.getini(RETENTION_COUNT_INI)
    )
    message = f"Ini/option `{RETENTION_COUNT_INI}` must be a positive integer: '{raw}'"
    if not isinstance(raw, str) or not raw:
        raise pytest.UsageError(message)
    try:
        count = int(raw)
    except ValueError as error:
        raise pytest.UsageError(message) from error
    if count < 1:
        raise pytest.UsageError(message)
    config.stash[RETENTION_COUNT_KEY] = count
    return count


def mark_session_started(config: pytest.Config) -> None:
    """Mark a started session as failed until ``pytest_sessionfinish`` says otherwise.

    Dispatched ``tryfirst`` from ``plugin.pytest_sessionstart`` so it lands before any
    other plugin's hookimpl can crash the startup. Everything between this mark and
    ``record_session_outcome`` -- a crashing conftest, an internal error, a killed
    controller -- therefore retains under the ``failed`` policy.

    Parameters:
        config: pytest.Config receiving the pessimistic session outcome.
    """

    config.stash[SESSION_FAILED_KEY] = True


def record_session_outcome(config: pytest.Config, exitstatus: int) -> None:
    """Record whether the finished session failed, for the cleanup closure to read.

    Every exit status outside ``CLEAN_EXIT_STATUSES`` counts as a failure, including
    ``USAGE_ERROR`` and any code a third-party plugin invents: a run that ended any way
    other than cleanly is one whose isolated state is worth inspecting.

    One status escapes by construction. pytest flips ``session.exitstatus`` to
    ``MAX_WARNINGS_ERROR`` inside the terminal reporter's ``pytest_sessionfinish``
    wrapper, after every hookimpl this one included has already run, so a
    ``--max-warnings`` breach is recorded as the clean status pytest reported at the
    time. There is no later hook carrying the corrected value.

    Parameters:
        config: pytest.Config receiving the stashed session outcome.
        exitstatus: int containing pytest's raw session exit status.
    """

    failed = exitstatus not in CLEAN_EXIT_STATUSES
    config.stash[SESSION_FAILED_KEY] = failed
    LOGGER.debug(f"Recorded session outcome for AIRFLOW_HOME retention: failed={failed}")


def session_failed(config: pytest.Config) -> bool:
    """Report whether a session started and did not finish cleanly.

    An absent record means no session ever started -- ``pytest --help``,
    ``pytest --markers``, an argparse usage error, an abort during
    ``pytest_configure`` -- which is not a failed run and must not retain anything.
    ``mark_session_started`` is what distinguishes that from a session that started and
    then died.

    Parameters:
        config: pytest.Config possibly carrying a recorded session outcome.

    Returns:
        bool reporting whether a session started and did not end cleanly.
    """

    return config.stash.get(SESSION_FAILED_KEY, False)


def retain_airflow_home(config: pytest.Config) -> bool:
    """Decide whether this run's isolated ``AIRFLOW_HOME`` survives cleanup.

    Reads the policy from the stash only. ``plugin.pytest_configure`` resolves it
    eagerly, so a malformed value has already failed the session loudly by the time any
    cleanup runs; an empty stash means the session died before ``pytest_configure``,
    and the documented default applies.

    Parameters:
        config: pytest.Config carrying the resolved policy and the session outcome.

    Returns:
        bool reporting whether the run directory must be kept.
    """

    policy = config.stash.get(RETENTION_POLICY_KEY, DEFAULT_RETENTION_POLICY)
    if policy is RetentionPolicy.NONE:
        return False
    if policy is RetentionPolicy.ALL:
        return True
    return session_failed(config)


def retention_count(config: pytest.Config) -> int:
    """Read the resolved retained-root count, or the documented default.

    Reads the count from the stash only, mirroring ``retain_airflow_home``'s pattern:
    ``plugin.pytest_configure`` resolves and caches it eagerly, so a malformed value has
    already failed the session loudly by the time cleanup calls this; an empty stash
    means the session died before ``pytest_configure``, and the documented default
    applies. Deliberately does not call ``resolve_retention_count`` -- re-parsing a still
    malformed value from a ``config.add_cleanup`` callback would raise a second time with
    nowhere useful for the error to go.

    Parameters:
        config: pytest.Config carrying the resolved retention count.

    Returns:
        int containing the maximum number of retained run directories to keep.
    """

    return config.stash.get(RETENTION_COUNT_KEY, DEFAULT_RETENTION_COUNT)


def _owns_run_root(state: BootstrapState) -> bool:
    """Report whether this process created the run directory it is about to describe.

    Local xdist workers inherit the controller's ``BootstrapState`` and use its
    ``AIRFLOW_HOME``, but own neither: the controller registered the cleanup, and only
    the controller's header and terminal summary reach a terminal.

    Parameters:
        state: BootstrapState containing the owning process identity.

    Returns:
        bool reporting whether this process owns the run directory.
    """

    return state.owner_pid == os.getpid()


def report_header(state: BootstrapState) -> list[str]:
    """Render the session header line naming this run's isolated ``AIRFLOW_HOME``.

    The storage rung is included because the ladder's choice is the surprising part:
    ``shared-memory`` means the run lives in RAM under ``/dev/shm``, which is where
    "so where did my logs go?" usually starts.

    Parameters:
        state: BootstrapState containing the resolved run root and its provenance.

    Returns:
        list[str] containing one header line, or no lines on an xdist worker.
    """

    if not _owns_run_root(state):
        return []
    return [
        f"{HEADER_PREFIX}: AIRFLOW_HOME={state.root} "
        f"(storage: {state.storage_reason}, db: {state.db_backend})"
    ]


def mark_root_retained(root: Path) -> None:
    """Mark a run directory as finished and retained, for ``prune_retained_roots`` to see.

    Written by ``bootstrap``'s cleanup closure at the moment it decides to keep a
    directory instead of removing it. Its absence is what keeps ``prune_retained_roots``
    from ever touching an in-flight run: only a directory whose owning process reached
    this call is a GC candidate.

    Parameters:
        root: pathlib.Path containing the run directory to mark.
    """

    (root / RETAINED_MARKER_NAME).touch()


def _retained_siblings(base: Path) -> list[Path]:
    """List marked-retained run directories under a storage base, newest first.

    Parameters:
        base: pathlib.Path containing the storage base to scan.

    Returns:
        list[pathlib.Path] containing retained run directories, sorted by the marker's
        modification time, most recent first.
    """

    dated: list[tuple[float, Path]] = []
    for entry in base.glob(f"{RUN_DIRECTORY_PREFIX}*"):
        try:
            mtime = (entry / RETAINED_MARKER_NAME).stat().st_mtime
        except OSError:
            continue
        dated.append((mtime, entry))
    dated.sort(key=lambda pair: pair[0], reverse=True)
    return [entry for _, entry in dated]


def prune_retained_roots(base: Path, *, keep: int) -> None:
    """Remove retained run directories beyond the ``keep`` most recent under a base.

    Only directories carrying the ``RETAINED_MARKER_NAME`` sentinel are ever a GC
    candidate, so an active run -- this process's own about-to-be-created root, or
    another concurrent invocation sharing the same base -- is never touched.

    Parameters:
        base: pathlib.Path containing the storage base to prune.
        keep: int containing the maximum number of retained directories to keep.
    """

    for stale in _retained_siblings(base)[keep:]:
        shutil.rmtree(stale, ignore_errors=True)


def _retained_lines(
    config: pytest.Config, root: Path, storage_reason: str, base: Path
) -> list[str]:
    """Render the lines describing one surviving run directory.

    Shared by both announcement channels so the wording cannot drift between them.

    Parameters:
        config: pytest.Config carrying the resolved retention policy.
        root: pathlib.Path containing the surviving run directory.
        storage_reason: str naming the storage-ladder rung that chose the base.
        base: pathlib.Path containing the storage base ``root`` was created under.

    Returns:
        list[str] containing the retention line plus, on ``/dev/shm``, a RAM warning.
    """

    policy = config.stash.get(RETENTION_POLICY_KEY, DEFAULT_RETENTION_POLICY)
    existing = len(_retained_siblings(base))
    plural = "" if existing == 1 else "s"
    lines = [
        f"Retained AIRFLOW_HOME (retention policy: {policy}; {existing} other retained "
        f"root{plural} kept): {root}"
    ]
    if storage_reason == StorageReason.SHARED_MEMORY:
        lines.append(
            f"WARNING: '{SHARED_MEMORY_PATH}' is RAM-backed, so this directory holds "
            f"memory until it is removed or the machine reboots. Pass "
            f"`--airflow-home=PATH` to put the run on durable storage instead."
        )
    return lines


def terminal_summary(
    terminalreporter: pytest.TerminalReporter,
    config: pytest.Config,
    state: BootstrapState,
) -> None:
    """Report a surviving ``AIRFLOW_HOME`` next to the run's failures.

    Dispatched from ``plugin.py``'s ``pytest_terminal_summary`` hookimpl, which pytest
    invokes after every ``pytest_sessionfinish`` hookimpl has stashed the outcome and
    before any ``config.add_cleanup`` callback runs, so the decision reported here is
    the one cleanup will act on and the directory still exists. A no-op on an xdist
    worker and whenever the directory is about to be removed.

    Records that the path reached a terminal, which suppresses the ``stderr`` fallback
    ``announce_retained_root`` would otherwise emit from cleanup.

    Parameters:
        terminalreporter: pytest.TerminalReporter receiving the rendered summary.
        config: pytest.Config carrying the resolved policy and the session outcome.
        state: BootstrapState containing the run root and its storage provenance.
    """

    if not _owns_run_root(state) or not retain_airflow_home(config):
        return
    terminalreporter.write_sep("=", SUMMARY_TITLE)
    for line in _retained_lines(config, state.root, state.storage_reason, state.root.parent):
        terminalreporter.write_line(line)
    config.stash[RETENTION_ANNOUNCED_KEY] = True
    LOGGER.debug(f"Retained isolated Airflow run directory: '{state.root}'")


def announce_retained_root(config: pytest.Config, root: Path, storage_reason: str) -> None:
    """Name a surviving run directory on ``stderr`` when nothing else did.

    Both terminal channels can be skipped entirely while retention still fires. The
    header is written by the terminal reporter's ``trylast`` ``pytest_sessionstart``, so
    a session that dies during configure or startup never prints it; the summary is
    reached only through the reporter's ``pytest_sessionfinish`` wrapper, and only for
    the exit statuses in ``_pytest.terminal.summary_exit_codes``, which excludes
    ``INTERNAL_ERROR``. ``config.add_cleanup`` runs on every one of those paths, so this
    is the backstop that keeps a kept directory from being kept silently.

    Parameters:
        config: pytest.Config carrying the resolved policy and the announcement record.
        root: pathlib.Path containing the surviving run directory.
        storage_reason: str naming the storage-ladder rung that chose the base.
    """

    if config.stash.get(RETENTION_ANNOUNCED_KEY, False):
        return
    for line in _retained_lines(config, root, storage_reason, root.parent):
        sys.stderr.write(f"{HEADER_PREFIX}: {line}\n")
    config.stash[RETENTION_ANNOUNCED_KEY] = True
    LOGGER.debug(f"Announced retained Airflow run directory on stderr: '{root}'")


__all__ = (
    "CLEAN_EXIT_STATUSES",
    "DEFAULT_RETENTION_COUNT",
    "DEFAULT_RETENTION_POLICY",
    "RETAINED_MARKER_NAME",
    "RETENTION_COUNT_INI",
    "RETENTION_COUNT_OPTION",
    "RETENTION_INI",
    "RETENTION_OPTION",
    "RetentionPolicy",
    "announce_retained_root",
    "mark_root_retained",
    "mark_session_started",
    "prune_retained_roots",
    "record_session_outcome",
    "register_options",
    "report_header",
    "resolve_retention_count",
    "resolve_retention_policy",
    "retain_airflow_home",
    "retention_count",
    "session_failed",
    "terminal_summary",
)
