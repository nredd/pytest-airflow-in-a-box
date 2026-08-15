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

The cleanup closure receives no arguments and never sees the session outcome, so
``record_session_outcome`` stashes it from ``pytest_sessionfinish`` first -- pytest
dispatches every non-wrapper ``pytest_sessionfinish`` hookimpl before the terminal
reporter's wrapper calls ``pytest_terminal_summary``, and both run before any
registered cleanup. An absent record means the hook never ran (an internal error, a
crashed controller, a session that died during startup) and is deliberately read as a
failure: the run that died is exactly the one whose artifacts are worth keeping.

Only the ``rmtree`` is conditional. The provisioner still stops on every policy,
``none`` included, so a retained run never leaks the testcontainers Postgres container
behind ``--airflow-db-backend=postgres``.

References:
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.hookspec.pytest_report_header
    https://docs.pytest.org/en/stable/reference/reference.html#confval-tmp_path_retention_policy
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.Config.add_cleanup
"""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

import pytest

from pytest_airflow_in_a_box.storage.locate import (
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
HEADER_PREFIX = "pytest-airflow-in-a-box"
SUMMARY_TITLE = "airflow-in-a-box"


class RetentionPolicy(StrEnum):
    """Which runs keep their isolated ``AIRFLOW_HOME`` instead of removing it."""

    ALL = "all"
    FAILED = "failed"
    NONE = "none"


DEFAULT_RETENTION_POLICY = RetentionPolicy.FAILED

RETENTION_POLICY_KEY = pytest.StashKey[RetentionPolicy]()
SESSION_FAILED_KEY = pytest.StashKey[bool]()

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


def record_session_outcome(config: pytest.Config, exitstatus: int) -> None:
    """Record whether the finished session failed, for the cleanup closure to read.

    Every exit status outside ``CLEAN_EXIT_STATUSES`` counts as a failure, including
    ``USAGE_ERROR`` and any code a third-party plugin invents: a run that ended any way
    other than cleanly is one whose isolated state is worth inspecting.

    Also called from ``plugin.pytest_cmdline_main`` for ``--airflow-doctor``, which
    short-circuits the session before ``pytest_sessionfinish`` can run: without an
    explicit record, the diagnostic run's own bootstrap directory would read as a crash
    and survive.

    Parameters:
        config: pytest.Config receiving the stashed session outcome.
        exitstatus: int containing pytest's raw session exit status.
    """

    failed = exitstatus not in CLEAN_EXIT_STATUSES
    config.stash[SESSION_FAILED_KEY] = failed
    LOGGER.debug(f"Recorded session outcome for AIRFLOW_HOME retention: failed={failed}")


def session_failed(config: pytest.Config) -> bool:
    """Report whether the session failed, treating an unrecorded outcome as a failure.

    Parameters:
        config: pytest.Config possibly carrying a recorded session outcome.

    Returns:
        bool reporting whether the session failed or never reached
        ``pytest_sessionfinish`` at all.
    """

    return config.stash.get(SESSION_FAILED_KEY, True)


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

    Parameters:
        terminalreporter: pytest.TerminalReporter receiving the rendered summary.
        config: pytest.Config carrying the resolved policy and the session outcome.
        state: BootstrapState containing the run root and its storage provenance.
    """

    if not _owns_run_root(state) or not retain_airflow_home(config):
        return
    policy = config.stash.get(RETENTION_POLICY_KEY, DEFAULT_RETENTION_POLICY)
    terminalreporter.write_sep("=", SUMMARY_TITLE)
    terminalreporter.write_line(f"Retained AIRFLOW_HOME ({RETENTION_INI}={policy}): {state.root}")
    if state.storage_reason == StorageReason.SHARED_MEMORY:
        terminalreporter.write_line(
            f"WARNING: '{SHARED_MEMORY_PATH}' is RAM-backed, so this directory holds "
            f"memory until it is removed or the machine reboots. Pass "
            f"`--airflow-home=PATH` to put the run on durable storage instead."
        )
    LOGGER.debug(f"Retained isolated Airflow run directory: '{state.root}'")


__all__ = (
    "CLEAN_EXIT_STATUSES",
    "DEFAULT_RETENTION_POLICY",
    "RETENTION_INI",
    "RETENTION_OPTION",
    "RetentionPolicy",
    "record_session_outcome",
    "register_options",
    "report_header",
    "resolve_retention_policy",
    "retain_airflow_home",
    "session_failed",
    "terminal_summary",
)
