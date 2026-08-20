"""Test guarded Apache Airflow metadata database initialization."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from pytest_airflow_in_a_box import _compat
from pytest_airflow_in_a_box._compat import database as database_module
from pytest_airflow_in_a_box._compat.capabilities import (
    AirflowCapabilities,
    AirflowFamily,
    ApiSurface,
    AssetUniqueKeyLocation,
    DagBagLocation,
    DagRunInterface,
    ExecutorContract,
    ParamsLocation,
    PluginsManagerShape,
    SecretsResolution,
    SharedModuleLoading,
    TaskInstanceRunner,
    TimezoneLocation,
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
        family=AirflowFamily.V3,
        dag_bag_location=DagBagLocation.DAG_PROCESSING,
        dag_bag_supports_include_examples=False,
        task_instance_runner=TaskInstanceRunner.SDK_RUN_TASK,
        refresh_from_task_supports_dag_run=True,
        startup_details_supports_sentry=True,
        runtime_task_instance_supports_queue=True,
        has_task_sdk=True,
        uses_structlog=True,
        has_dag_versioning=True,
        dagrun_interface=DagRunInterface.LOGICAL_DATE,
        api_surface=ApiSurface.API_SERVER,
        params_location=ParamsLocation.SDK,
        timezone_location=TimezoneLocation.SDK,
        secrets_resolution=SecretsResolution.SUPERVISOR_COMMS,
        max_python=None,
        dag_requires_start_date=False,
        asset_unique_key_location=AssetUniqueKeyLocation.SERIALIZATION,
        executor_contract=ExecutorContract.V3_3,
        sdk_listener_manager_available=True,
        task_instance_mutation_hook_supports_dag_run=True,
        plugins_manager=PluginsManagerShape.CACHED_FUNCTIONS,
        shared_module_loading=SharedModuleLoading.DUPLICATED,
    )


def test_database_api_is_exported() -> None:
    """Expose initialization and its domain failure through the compatibility boundary."""

    assert _compat.initialize_database is database_module.initialize_database
    assert _compat.ensure_database is database_module.ensure_database
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


def _record_initialization(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Replace the Airflow seams with recorders for `ensure_database` tests.

    Parameters:
        monkeypatch: pytest.MonkeyPatch scoped to the calling test.

    Returns:
        list[str] recording every compatibility and initialization operation.
    """

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
    return operations


def test_ensure_database_initializes_and_writes_sentinel(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Initialize once, then leave the ready sentinel and lock file behind."""

    operations = _record_initialization(monkeypatch)

    database_module.ensure_database(tmp_path)

    assert operations == ["resolve", "initdb"]
    assert (tmp_path / database_module.DATABASE_READY_SENTINEL_NAME).is_file()
    assert (tmp_path / database_module.DATABASE_INIT_LOCK_NAME).is_file()


def test_ensure_database_second_call_is_a_fast_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Skip all filesystem and Airflow work once the process is marked ensured."""

    operations = _record_initialization(monkeypatch)

    database_module.ensure_database(tmp_path)
    database_module.ensure_database(tmp_path)

    assert operations == ["resolve", "initdb"]


def test_ensure_database_observes_foreign_sentinel(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Trust another process's ready sentinel without importing Airflow."""

    operations = _record_initialization(monkeypatch)
    (tmp_path / database_module.DATABASE_READY_SENTINEL_NAME).touch()

    database_module.ensure_database(tmp_path)

    assert operations == []


def test_ensure_database_wraps_coordination_failures(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Wrap lock-file failures in the domain error with the original cause."""

    operations = _record_initialization(monkeypatch)
    missing_root = tmp_path / "missing"

    with pytest.raises(DatabaseInitializationError, match="coordinate cross-process") as caught:
        database_module.ensure_database(missing_root)

    assert operations == []
    assert isinstance(caught.value.__cause__, OSError)


def test_ensure_database_failure_leaves_state_retryable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Propagate an initialization failure unwrapped and allow a retry."""

    operations: list[str] = []
    failure = RuntimeError("migration interrupted")

    def resolve() -> AirflowCapabilities:
        """Record compatibility validation."""

        operations.append("resolve")
        return _capabilities()

    def initialize() -> None:
        """Fail on the first call and succeed on the second."""

        operations.append("initdb")
        if operations.count("initdb") == 1:
            raise failure

    monkeypatch.setattr(database_module, "resolve_capabilities", resolve)
    monkeypatch.setattr(database_module, "_initialize_airflow_database", initialize)

    with pytest.raises(
        DatabaseInitializationError, match="verify the configured database URL"
    ) as caught:
        database_module.ensure_database(tmp_path)

    assert caught.value.__cause__ is failure
    assert not (tmp_path / database_module.DATABASE_READY_SENTINEL_NAME).exists()

    database_module.ensure_database(tmp_path)

    assert operations == ["resolve", "initdb", "resolve", "initdb"]
    assert (tmp_path / database_module.DATABASE_READY_SENTINEL_NAME).is_file()
