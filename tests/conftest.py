"""Configure pytest's own plugin-testing fixture."""

from __future__ import annotations

import logging

import pytest

from pytest_airflow_in_a_box import record
from pytest_airflow_in_a_box.bootstrap import (
    ISOLATED_WORKER_ENVIRONMENT_VARIABLE,
    STATE_ENVIRONMENT_VARIABLE,
)

pytest_plugins = ["pytester"]

# Root of this plugin's logger namespace. Every `LOGGER` in `src/` is a
# `logging.getLogger(__name__)` beneath it.
PLUGIN_LOGGER_ROOT = "pytest_airflow_in_a_box"

# Dev-only marker for the migration orchestrator's real-uv/network e2e test
# (tests/migration/test_e2e.py). Deliberately not part of the consumer-facing marker
# surface in markers.py/defaults.py -- it gates this repo's own opt-in `make
# test-migration-e2e` target, never something a plugin consumer would mark their tests
# with.
MIGRATION_E2E_MARKER = (
    "migration_e2e: real uv/network migration-orchestrator end-to-end test "
    "(opt in with `make test-migration-e2e`, excluded from `make test`/`make all`)"
)


def pytest_configure(config: pytest.Config) -> None:
    """Register the dev-only `migration_e2e` marker.

    Parameters:
        config: pytest.Config for the active test session.
    """

    config.addinivalue_line("markers", MIGRATION_E2E_MARKER)


@pytest.fixture(autouse=True)
def _isolate_nested_pytest_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Prevent an outer xdist worker identity from leaking into pytester subprocesses.

    Every ``runpytest_subprocess`` child inherits this process's environment. When
    the outer suite itself runs under xdist, an inherited worker identity plus
    bootstrap state would make nested sessions reuse the outer session's Airflow
    root instead of bootstrapping their own, so every test starts clean and the
    tests that assert inherited-state behavior set these variables explicitly.

    Parameters:
        monkeypatch: pytest.MonkeyPatch restoring the environment after each test.
    """

    monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)
    monkeypatch.delenv(STATE_ENVIRONMENT_VARIABLE, raising=False)
    monkeypatch.delenv(ISOLATED_WORKER_ENVIRONMENT_VARIABLE, raising=False)


@pytest.fixture(autouse=True)
def _reenable_plugin_loggers() -> None:
    """Undo `disable_existing_loggers` fallout before every test.

    A `logging.config.dictConfig` call that raises leaves every logger that already
    existed with `disabled = True`, and stdlib never reverts it -- so one failing
    `dictConfig` anywhere in the session silently mutes this plugin's loggers for the
    rest of the process. `caplog.at_level` does not undo it: that guards against
    `logging.disable()` and per-logger levels, not the per-logger `disabled` attribute.
    A test asserting on plugin log output therefore passes or fails depending on what
    ran before it in the same process -- serial ordering happened to hide that, and
    xdist worker assignment does not.

    Autouse and global for the same reason as `_isolate_record_active_config_stack`:
    a new test asserting on log output must not be able to reintroduce the order
    dependency by omission.

    Returns:
        None.
    """

    for name in list(logging.Logger.manager.loggerDict):
        if name.startswith(PLUGIN_LOGGER_ROOT):
            logging.getLogger(name).disabled = False


@pytest.fixture(autouse=True)
def _isolate_record_active_config_stack(monkeypatch: pytest.MonkeyPatch) -> None:
    """Snapshot and restore ``record``'s module-global active config stack per test.

    ``record._ACTIVE_CONFIG_STACK`` is process-global, and the real plugin session
    running this test suite has already pushed onto it via its own ``pytest_configure``
    hook. Any test that calls ``record.configure``/``record.unconfigure`` with a fake
    config, or that otherwise reads the stack, must not leak that fake into the rest of
    the suite -- a leak would silently misroute every later real test's phase reports
    away from the actual session's accumulator, which no other check in this suite
    would catch. Autouse and global, not local to ``test_record.py``/
    ``test_baseline.py``, so a new test file touching this global cannot reintroduce
    the leak by omission.

    The replacement must be a *copy* of the current stack, not the same list object:
    ``configure``/``unconfigure`` mutate the stack in place (``append``/``pop``), and
    ``monkeypatch.setattr`` only restores which object the attribute name points at, so
    restoring the identical list would not undo an in-place mutation made during the
    test.

    Parameters:
        monkeypatch: pytest.MonkeyPatch restoring the global after each test.
    """

    monkeypatch.setattr(record, "_ACTIVE_CONFIG_STACK", list(record._ACTIVE_CONFIG_STACK))
