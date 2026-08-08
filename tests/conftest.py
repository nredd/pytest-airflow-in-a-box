"""Configure pytest's own plugin-testing fixture."""

from __future__ import annotations

import pytest

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
