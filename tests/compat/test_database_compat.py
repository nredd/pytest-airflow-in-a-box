"""Test guarded Apache Airflow metadata database initialization."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from pytest_airflow_in_a_box import _compat
from pytest_airflow_in_a_box._compat import database as database_module
from pytest_airflow_in_a_box._compat.capabilities import (
    AirflowCapabilities,
    DagBagLocation,
    TaskInstanceRunner,
)
from pytest_airflow_in_a_box._compat.database import DatabaseInitializationError


@pytest.fixture(autouse=True)
def _reset_database_initialization() -> Iterator[None]:
    """Isolate process-local database initialization state."""

    database_module._reset_database_for_testing()
    yield
    database_module._reset_database_for_testing()


def _capabilities() -> AirflowCapabilities:
    """Return representative validated compatibility metadata.

    Returns:
        AirflowCapabilities containing a complete certified contract.
    """

    return AirflowCapabilities(
        release=(3, 3, 0),
        dag_bag_location=DagBagLocation.DAG_PROCESSING,
        dag_bag_supports_include_examples=False,
        task_instance_runner=TaskInstanceRunner.SDK_RUN_TASK,
        refresh_from_task_supports_dag_run=True,
        startup_details_supports_sentry=True,
        runtime_task_instance_supports_queue=True,
    )


def test_database_api_is_exported() -> None:
    """Expose initialization and its domain failure through the compatibility boundary."""

    assert _compat.initialize_database is database_module.initialize_database
    assert _compat.DatabaseInitializationError is DatabaseInitializationError


def test_successful_initialization_is_ordered_and_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Validate capabilities first while calling Airflow initialization only once."""

    operations: list[str] = []

    def resolve() -> AirflowCapabilities:
        """Record compatibility validation."""

        operations.append("resolve")
        return _capabilities()

    def initialize() -> None:
        """Record the deferred Airflow operation."""

        operations.append("initdb")

    monkeypatch.setattr(database_module, "resolve_capabilities", resolve)
    monkeypatch.setattr(database_module, "_initialize_airflow_database", initialize)

    database_module.initialize_database()
    database_module.initialize_database()

    assert operations == ["resolve", "initdb", "resolve"]


def test_initialization_failure_is_actionable_and_retains_cause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Wrap an Airflow database failure without marking initialization complete."""

    operations: list[str] = []
    failure = OSError("database directory is read-only")

    def resolve() -> AirflowCapabilities:
        """Record compatibility validation."""

        operations.append("resolve")
        return _capabilities()

    def initialize() -> None:
        """Raise a representative Airflow database failure."""

        operations.append("initdb")
        raise failure

    monkeypatch.setattr(database_module, "resolve_capabilities", resolve)
    monkeypatch.setattr(database_module, "_initialize_airflow_database", initialize)

    with pytest.raises(
        DatabaseInitializationError, match="verify the configured database URL"
    ) as caught:
        database_module.initialize_database()

    assert operations == ["resolve", "initdb"]
    assert caught.value.__cause__ is failure
