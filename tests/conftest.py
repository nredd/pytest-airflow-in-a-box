"""Configure pytest's own plugin-testing fixture."""

from __future__ import annotations

import pytest

from pytest_airflow_in_a_box.bootstrap import STATE_ENVIRONMENT_VARIABLE

pytest_plugins = ["pytester"]

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
