"""Configure pytest's own plugin-testing fixture."""

from __future__ import annotations

import pytest

from pytest_airflow_in_a_box import record
from pytest_airflow_in_a_box.bootstrap import STATE_ENVIRONMENT_VARIABLE

pytest_plugins = ["pytester"]


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
