"""Test the public Dag construction and persistence fixture.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-serialization.html
    https://docs.pytest.org/en/stable/how-to/fixtures.html
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import pytest
from airflow.models.dag import DagModel
from airflow.models.dag_version import DagVersion
from airflow.models.dagbundle import DagBundleModel
from airflow.models.serialized_dag import SerializedDagModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.session import create_session
from sqlalchemy import func, select

from pytest_airflow_in_a_box._compat import dag as dag_compat
from pytest_airflow_in_a_box._compat.dag import DagCleanupError, DagPersistenceError
from pytest_airflow_in_a_box.fixtures import dag as dag_fixture
from pytest_airflow_in_a_box.fixtures.dag import (
    DAG_ID_MAX_LENGTH,
    _bundle_name,
    _DagFactory,
    _DagRunner,
    _default_dag_id,
)
from pytest_airflow_in_a_box.types import DagMaker, RunDag

pytestmark = pytest.mark.db_test


def _require_count(value: int | None) -> int:
    """Require one aggregate metadata query result.

    Parameters:
        value: int | None returned by SQLAlchemy.

    Returns:
        int containing the aggregate count.

    Raises:
        RuntimeError: SQLAlchemy returned no aggregate value.
    """

    if value is None:
        raise RuntimeError("Metadata count query did not return a value")
    return value


def _row_counts(dag_id: str) -> tuple[int, int, int, int]:
    """Count the four required metadata models for one Dag.

    Parameters:
        dag_id: str identifying the Dag rows.

    Returns:
        tuple[int, int, int, int] containing bundle, DagModel, DagVersion, and serialized counts.
    """

    bundle_name = _bundle_name(dag_id)
    with create_session() as session:
        return (
            _require_count(
                session.scalar(
                    select(func.count())
                    .select_from(DagBundleModel)
                    .where(DagBundleModel.name == bundle_name)
                )
            ),
            _require_count(
                session.scalar(
                    select(func.count()).select_from(DagModel).where(DagModel.dag_id == dag_id)
                )
            ),
            _require_count(
                session.scalar(
                    select(func.count()).select_from(DagVersion).where(DagVersion.dag_id == dag_id)
                )
            ),
            _require_count(
                session.scalar(
                    select(func.count())
                    .select_from(SerializedDagModel)
                    .where(SerializedDagModel.dag_id == dag_id)
                )
            ),
        )


def test_default_id_context_and_required_metadata(
    dag_maker: DagMaker,
    request: pytest.FixtureRequest,
) -> None:
    """Derive a bounded worker-specific ID and persist the completed task graph."""

    expected_id = _default_dag_id(
        request.node.nodeid,
        os.environ.get("PYTEST_XDIST_WORKER", "master"),
        1,
    )

    with dag_maker() as dag:
        empty = EmptyOperator(task_id="empty")
        assert empty.dag is dag
        assert dag.dag_id == expected_id
        assert dag_maker.dag is dag
        assert dag_maker.session.is_active
        assert dag_maker.serialized_dag is None

    assert len(dag.dag_id) <= DAG_ID_MAX_LENGTH
    assert _row_counts(dag.dag_id) == (1, 1, 1, 1)
    assert dag_maker.session.get(DagModel, dag.dag_id) is not None
    assert dag_maker.serialized_dag is None


def test_default_id_is_bounded_and_worker_specific() -> None:
    """Bound long node IDs while retaining deterministic worker and invocation identity."""

    first = _default_dag_id("tests/" + ("long name/" * 100), "gw0", 1)

    assert len(first) == DAG_ID_MAX_LENGTH
    assert first == _default_dag_id("tests/" + ("long name/" * 100), "gw0", 1)
    assert first != _default_dag_id("tests/" + ("long name/" * 100), "gw1", 1)
    assert first != _default_dag_id("tests/" + ("long name/" * 100), "gw0", 2)


def test_explicit_id_and_sdk_arguments(dag_maker: DagMaker) -> None:
    """Preserve a valid explicit identifier and forward SDK constructor arguments."""

    with dag_maker(
        dag_id="explicit.dag-1",
        description="fixture Dag",
        tags=["fixture"],
    ) as dag:
        assert dag.dag_id == "explicit.dag-1"
        assert dag.description == "fixture Dag"
        assert dag.tags == {"fixture"}

    assert _row_counts("explicit.dag-1") == (1, 1, 1, 1)


@pytest.mark.parametrize("dag_id", ["", "contains/slash", "x" * 251])
def test_rejects_invalid_explicit_ids(dag_maker: DagMaker, dag_id: str) -> None:
    """Reject empty, unsupported, and unbounded identifiers before opening a session."""

    with pytest.raises(ValueError, match="`dag_id`"):
        dag_maker(dag_id=dag_id)


def test_rejects_invalid_argument_types(dag_maker: DagMaker) -> None:
    """Reject runtime values outside the typed public call contract."""

    invalid_id: Any = 42
    invalid_serialized: Any = "yes"

    with pytest.raises(TypeError, match="`dag_id` must be a string"):
        dag_maker(dag_id=invalid_id)
    with pytest.raises(TypeError, match="`serialized` must be a boolean"):
        dag_maker(serialized=invalid_serialized)


def test_properties_require_factory_progress(dag_maker: DagMaker) -> None:
    """Report property misuse before a Dag or metadata session exists."""

    with pytest.raises(RuntimeError, match="has not created a Dag"):
        _ = dag_maker.dag
    with pytest.raises(RuntimeError, match="has not entered a Dag context"):
        _ = dag_maker.session
    assert dag_maker.serialized_dag is None
    with pytest.raises(RuntimeError, match="has not persisted a Dag"):
        dag_maker.create_dagrun()


def test_context_cannot_exit_before_entry(dag_maker: DagMaker) -> None:
    """Reject direct misuse of a context manager that has not opened a session."""

    context = dag_maker(dag_id="never_entered")

    with pytest.raises(RuntimeError, match="before it was entered"):
        context.__exit__(None, None, None)


def test_context_cannot_be_entered_twice(dag_maker: DagMaker) -> None:
    """Reject re-entry without leaking the original context's metadata session."""

    context = dag_maker(dag_id="single_entry")
    with context, pytest.raises(RuntimeError, match="cannot be entered more than once"):
        context.__enter__()

    assert _row_counts("single_entry") == (1, 1, 1, 1)


def test_existing_dag_id_is_not_overwritten(dag_maker: DagMaker) -> None:
    """Close a rejected context session while preserving existing owned metadata."""

    with dag_maker(dag_id="duplicate_id"):
        EmptyOperator(task_id="original")

    with pytest.raises(ValueError, match="already exists"), dag_maker(dag_id="duplicate_id"):
        EmptyOperator(task_id="replacement")

    assert _row_counts("duplicate_id") == (1, 1, 1, 1)


@pytest.mark.need_serialized_dag
def test_marker_exposes_persisted_scheduler_dag(dag_maker: DagMaker) -> None:
    """Expose the real persisted scheduler representation after successful exit."""

    with dag_maker(dag_id="serialized_from_marker") as dag:
        EmptyOperator(task_id="marked")
        assert dag_maker.serialized_dag is None

    serialized_dag = dag_maker.serialized_dag
    assert serialized_dag is not None
    assert serialized_dag is not dag
    assert serialized_dag.dag_id == dag.dag_id
    assert serialized_dag.task_ids == ["marked"]


@pytest.mark.need_serialized_dag
def test_explicit_false_overrides_serialized_marker(dag_maker: DagMaker) -> None:
    """Give an explicit false factory argument precedence over a true marker."""

    with dag_maker(dag_id="serialized_override_false", serialized=False):
        EmptyOperator(task_id="not_exposed")

    assert dag_maker.serialized_dag is None
    assert _row_counts("serialized_override_false") == (1, 1, 1, 1)


@pytest.mark.need_serialized_dag(False)
def test_explicit_true_overrides_false_serialized_marker(dag_maker: DagMaker) -> None:
    """Give an explicit true factory argument precedence over a false marker."""

    with dag_maker(dag_id="serialized_override_true", serialized=True):
        EmptyOperator(task_id="exposed")

    serialized_dag = dag_maker.serialized_dag
    assert serialized_dag is not None
    assert serialized_dag.task_ids == ["exposed"]


def test_failed_context_body_does_not_persist_metadata(dag_maker: DagMaker) -> None:
    """Close the metadata session without writing rows when Dag construction fails."""

    with pytest.raises(RuntimeError, match="body failed"), dag_maker(dag_id="failed_context"):
        raise RuntimeError("body failed")

    assert _row_counts("failed_context") == (0, 0, 0, 0)


def test_persistence_failure_is_wrapped_and_cleans_committed_rows(
    dag_maker: DagMaker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retain the Airflow cause and clean metadata after a post-commit load failure."""

    failure = OSError("serialized read failed")

    def fail_load(record: dag_compat.DagPersistenceRecord) -> None:
        """Fail after the persistence transaction has committed."""

        del record
        raise failure

    monkeypatch.setattr(dag_compat, "_load_serialized_dag", fail_load)

    with (
        pytest.raises(
            DagPersistenceError,
            match="loading persisted serialized Dag metadata",
        ) as caught,
        dag_maker(dag_id="failed_persistence"),
    ):
        EmptyOperator(task_id="empty")

    assert caught.value.__cause__ is failure
    assert _row_counts("failed_persistence") == (0, 0, 0, 0)


def test_repeated_factory_calls_are_independent(dag_maker: DagMaker) -> None:
    """Create independent default IDs, sessions, and metadata from one function fixture."""

    with dag_maker() as first:
        EmptyOperator(task_id="first")
    first_session = dag_maker.session

    with dag_maker() as second:
        EmptyOperator(task_id="second")

    assert first.dag_id != second.dag_id
    assert first_session is not dag_maker.session
    assert _row_counts(first.dag_id) == (1, 1, 1, 1)
    assert _row_counts(second.dag_id) == (1, 1, 1, 1)


def test_low_level_cleanup_preserves_referenced_shared_bundle() -> None:
    """Delete each owned Dag while retaining a bundle until its final reference is gone."""

    bundle_name = "shared_compat_bundle"
    first_session = dag_compat.open_dag_session("shared_first")
    second_session = dag_compat.open_dag_session("shared_second")
    first_record = dag_compat.DagPersistenceRecord(
        dag_id="shared_first",
        bundle_name=bundle_name,
        session=first_session,
    )
    second_record = dag_compat.DagPersistenceRecord(
        dag_id="shared_second",
        bundle_name=bundle_name,
        session=second_session,
    )
    first = dag_compat.build_dag("shared_first", __file__, {})
    second = dag_compat.build_dag("shared_second", __file__, {})
    dag_compat.persist_dag(first, first_record)
    dag_compat.persist_dag(second, second_record)

    assert first_record.bundle_created
    assert not second_record.bundle_created
    dag_compat.cleanup_dag(first_record)
    with create_session() as session:
        assert session.get(DagBundleModel, bundle_name) is not None

    second_record.bundle_created = True
    dag_compat.cleanup_dag(second_record)
    with create_session() as session:
        assert session.get(DagBundleModel, bundle_name) is None


def test_cleanup_accepts_missing_owned_rows() -> None:
    """Make cleanup idempotent when a failed operation did not create metadata rows."""

    session = dag_compat.open_dag_session("missing_cleanup")
    record = dag_compat.DagPersistenceRecord(
        dag_id="missing_cleanup",
        bundle_name="missing_cleanup_bundle",
        session=session,
    )
    record.dag_run_ids.add(-1)

    dag_compat.cleanup_dag(record)


def test_factory_cleanup_continues_after_one_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Attempt every owned cleanup before reporting aggregated finalizer failures."""

    session: Any = object()
    first = dag_compat.DagPersistenceRecord("first", "first_bundle", session)
    second = dag_compat.DagPersistenceRecord("second", "second_bundle", session)
    factory = _DagFactory("node", __file__, "master", serialized=False)
    scheduler_dag: Any = object()
    factory._finish(first, scheduler_dag, None)
    factory._finish(second, scheduler_dag, None)
    attempted: list[str] = []

    def cleanup(record: dag_compat.DagPersistenceRecord) -> None:
        """Record every cleanup and fail the first reverse-order attempt."""

        attempted.append(record.dag_id)
        if record.dag_id == "second":
            raise OSError("second failed")

    monkeypatch.setattr(dag_fixture, "cleanup_dag", cleanup)

    with pytest.raises(
        DagCleanupError, match="Could not clean 1 fixture-owned Airflow Dags"
    ) as caught:
        factory.close()

    assert attempted == ["second", "first"]
    assert isinstance(caught.value.__cause__, OSError)


def test_function_scope_cleanup_and_shared_bundle_preservation(
    pytester: pytest.Pytester,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Remove owned rows after each test while retaining a pre-existing bundle."""

    pytester.makepyfile(
        """
        import pytest
        from airflow.models.dag import DagModel
        from airflow.models.dag_version import DagVersion
        from airflow.models.dagbundle import DagBundleModel
        from airflow.models.serialized_dag import SerializedDagModel
        from airflow.utils.session import create_session
        from sqlalchemy import func, select

        from pytest_airflow_in_a_box.fixtures.dag import _bundle_name

        pytestmark = pytest.mark.db_test
        DAG_ID = "pytester_cleanup"
        BUNDLE_NAME = _bundle_name(DAG_ID)


        def test_create(dag_maker):
            with create_session() as session:
                session.add(DagBundleModel(name=BUNDLE_NAME))
            with dag_maker(dag_id=DAG_ID) as dag:
                assert dag.dag_id == DAG_ID


        def test_cleaned():
            with create_session() as session:
                assert session.get(DagBundleModel, BUNDLE_NAME) is not None
                assert session.get(DagModel, DAG_ID) is None
                assert session.scalar(
                    select(func.count()).select_from(DagVersion).where(
                        DagVersion.dag_id == DAG_ID
                    )
                ) == 0
                assert session.scalar(
                    select(func.count()).select_from(SerializedDagModel).where(
                        SerializedDagModel.dag_id == DAG_ID
                    )
                ) == 0
                session.delete(session.get(DagBundleModel, BUNDLE_NAME))
        """
    )

    monkeypatch.setenv("PYTEST_DISABLE_PLUGIN_AUTOLOAD", "1")
    result = pytester.runpytest_subprocess("-p", "pytest_airflow_in_a_box.plugin", "-q")

    result.assert_outcomes(passed=2)


def test_run_dag_persists_executes_and_cleans_an_external_dag(run_dag: RunDag) -> None:
    """Adopt an externally-authored Dag and return its executed `DagRunResult`."""

    dag = dag_compat.build_dag("run_dag_basic", __file__, {})
    with dag:
        first = EmptyOperator(task_id="first")
        second = EmptyOperator(task_id="second")
        first >> second

    result = run_dag(dag)

    assert result.success
    assert result.dag_id == "run_dag_basic"
    assert result.order == ["first", "second"]
    assert _row_counts("run_dag_basic") == (1, 1, 1, 1)


def test_run_dag_rejects_a_colliding_dag_id(dag_maker: DagMaker, run_dag: RunDag) -> None:
    """Refuse to adopt a Dag whose id already has persisted metadata."""

    with dag_maker(dag_id="run_dag_duplicate"):
        EmptyOperator(task_id="owned_by_dag_maker")

    colliding = dag_compat.build_dag("run_dag_duplicate", __file__, {})
    with colliding:
        EmptyOperator(task_id="owned_by_run_dag")

    with pytest.raises(ValueError, match="already exists"):
        run_dag(colliding)

    assert _row_counts("run_dag_duplicate") == (1, 1, 1, 1)


def test_run_dag_closes_the_session_when_persist_dag_fails(
    run_dag: RunDag,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Close the metadata session even when persistence fails after commit."""

    failure = OSError("serialized read failed")

    def fail_load(record: dag_compat.DagPersistenceRecord) -> None:
        """Fail after the persistence transaction has committed."""

        del record
        raise failure

    monkeypatch.setattr(dag_compat, "_load_serialized_dag", fail_load)

    real_open_dag_session = dag_fixture.open_dag_session
    closed = {"value": False}

    def spying_open_dag_session(dag_id: str) -> Any:
        """Wrap the opened session to record whether it was closed."""

        session = real_open_dag_session(dag_id)
        original_close = session.close

        def spy_close() -> None:
            closed["value"] = True
            original_close()

        monkeypatch.setattr(session, "close", spy_close)
        return session

    monkeypatch.setattr(dag_fixture, "open_dag_session", spying_open_dag_session)

    dag = dag_compat.build_dag("run_dag_persist_failure", __file__, {})
    with dag:
        EmptyOperator(task_id="empty")

    with pytest.raises(
        DagPersistenceError, match="loading persisted serialized Dag metadata"
    ) as caught:
        run_dag(dag)

    assert caught.value.__cause__ is failure
    assert closed["value"]
    assert _row_counts("run_dag_persist_failure") == (0, 0, 0, 0)


def test_run_dag_supports_multiple_independent_dags_in_one_test(run_dag: RunDag) -> None:
    """Persist, execute, and independently own metadata across repeated calls."""

    first_dag = dag_compat.build_dag("run_dag_first", __file__, {})
    with first_dag:
        EmptyOperator(task_id="only")
    second_dag = dag_compat.build_dag("run_dag_second", __file__, {})
    with second_dag:
        EmptyOperator(task_id="only")

    first_result = run_dag(first_dag)
    second_result = run_dag(second_dag)

    assert first_result.dag_id != second_result.dag_id
    assert first_result.success
    assert second_result.success
    assert _row_counts("run_dag_first") == (1, 1, 1, 1)
    assert _row_counts("run_dag_second") == (1, 1, 1, 1)


def test_run_dag_passes_through_run_id_logical_date_and_dag_run_kwargs(
    run_dag: RunDag,
) -> None:
    """Forward explicit run identity and scheduler-Dag creation kwargs."""

    dag = dag_compat.build_dag("run_dag_passthrough", __file__, {})
    with dag:
        EmptyOperator(task_id="only")
    pinned_date = datetime(2024, 1, 1, tzinfo=timezone.utc)

    result = run_dag(
        dag,
        run_id="pinned-run-dag",
        logical_date=pinned_date,
        dag_run_kwargs={"conf": {"probe": 1}},
    )

    assert result.run_id == "pinned-run-dag"
    assert result.dag_run.logical_date == pinned_date
    assert result.dag_run.conf == {"probe": 1}


def test_run_dag_cleanup_continues_after_one_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Attempt every owned cleanup before reporting aggregated finalizer failures."""

    session: Any = object()
    first = dag_compat.DagPersistenceRecord("first", "first_bundle", session)
    second = dag_compat.DagPersistenceRecord("second", "second_bundle", session)
    runner = _DagRunner()
    runner._records.extend([first, second])
    attempted: list[str] = []

    def cleanup(record: dag_compat.DagPersistenceRecord) -> None:
        """Record every cleanup and fail the first reverse-order attempt."""

        attempted.append(record.dag_id)
        if record.dag_id == "second":
            raise OSError("second failed")

    monkeypatch.setattr(dag_fixture, "cleanup_dag", cleanup)

    with pytest.raises(
        DagCleanupError, match="Could not clean 1 fixture-owned Airflow Dags"
    ) as caught:
        runner.close()

    assert attempted == ["second", "first"]
    assert isinstance(caught.value.__cause__, OSError)
