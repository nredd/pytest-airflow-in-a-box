"""Test the public Dag construction and persistence fixture.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-serialization.html
    https://docs.pytest.org/en/stable/how-to/fixtures.html
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from importlib import import_module
from pathlib import Path
from typing import Any, cast

import pytest
from airflow.models.dag import DagModel
from airflow.models.dag_version import DagVersion
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagrun import DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.timetables.base import Timetable
from airflow.utils.session import create_session
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from pytest_airflow_in_a_box._compat import dag as dag_compat
from pytest_airflow_in_a_box._compat.dag import DagCleanupError, DagPersistenceError
from pytest_airflow_in_a_box.components import ComponentContractError
from pytest_airflow_in_a_box.fixtures import dag as dag_fixture
from pytest_airflow_in_a_box.fixtures.dag import (
    DAG_ID_MAX_LENGTH,
    _bundle_name,
    _DagFactory,
    _DagRunner,
    _default_dag_id,
)
from pytest_airflow_in_a_box.types import ComponentRegistry, DagMaker, RunDag

pytestmark = pytest.mark.db_test

# `provider_package` (in this corpus) supplies `ExampleTimetable`, an already-conformant,
# module-level `Timetable` -- registration resolves it by dotted import path later, so a
# corpus class is the correct fit; see `tests/fixtures/test_component_sandbox.py`'s
# identical `monkeypatch.syspath_prepend` idiom.
CORPUS = Path(__file__).parents[1] / "dags"


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
    serialized_dag = dag_maker.serialized_dag
    assert serialized_dag is not None
    assert serialized_dag.dag_id == dag.dag_id


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
    with pytest.raises(RuntimeError, match="has not persisted a Dag"):
        _ = dag_maker.dag_model
    with pytest.raises(RuntimeError, match="has not persisted a Dag"):
        dag_maker.sync_dagbag_to_db()


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
def test_explicit_false_serialized_is_an_exposure_noop(dag_maker: DagMaker) -> None:
    """Expose the scheduler Dag even when `serialized=False` is passed (docs/adr/0002)."""

    with dag_maker(dag_id="serialized_override_false", serialized=False):
        EmptyOperator(task_id="still_exposed")

    serialized_dag = dag_maker.serialized_dag
    assert serialized_dag is not None
    assert serialized_dag.task_ids == ["still_exposed"]
    assert _row_counts("serialized_override_false") == (1, 1, 1, 1)


@pytest.mark.need_serialized_dag(False)
def test_false_serialized_marker_is_an_exposure_noop(dag_maker: DagMaker) -> None:
    """Expose the scheduler Dag even under a false marker (docs/adr/0002)."""

    with dag_maker(dag_id="serialized_override_true", serialized=True):
        EmptyOperator(task_id="exposed")

    serialized_dag = dag_maker.serialized_dag
    assert serialized_dag is not None
    assert serialized_dag.task_ids == ["exposed"]


def test_failed_context_body_does_not_persist_metadata(dag_maker: DagMaker) -> None:
    """Close the metadata session without writing rows when Dag construction fails.

    The body raises a sentinel exception class instead of `RuntimeError` so that an entry-path
    `DagPersistenceError` (a `RuntimeError` subclass) propagates as itself rather than surfacing
    as a confusing `pytest.raises` regex mismatch -- the masking identified in issue #153.
    """

    class BodyFailedError(Exception):
        """Sentinel raised by the context body; never raised by the plugin itself."""

    with pytest.raises(BodyFailedError, match="body failed"), dag_maker(dag_id="failed_context"):
        raise BodyFailedError("body failed")

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


def test_harness_kwargs_are_not_forwarded_to_dag_init(
    dag_maker: DagMaker,
    session: Session,
) -> None:
    """Route the upstream harness keywords to persistence, not ``DAG.__init__``.

    Upstream ``tests_common``'s ``dag_maker`` swallows ``session``, ``bundle_name``,
    and ``bundle_version`` before building the ``DAG``; forwarding them raised
    ``TypeError`` here and was the single biggest divergence class in issue #238.
    """

    with dag_maker(
        dag_id="harness_kwargs",
        session=session,
        bundle_name="issue-238-bundle",
        bundle_version="v238",
        description="still a Dag kwarg",
    ) as dag:
        EmptyOperator(task_id="empty")
        assert dag.description == "still a Dag kwarg"

    dag_model = session.get(DagModel, "harness_kwargs")
    assert dag_model is not None
    assert dag_model.bundle_name == "issue-238-bundle"
    assert dag_model.bundle_version == "v238"


def test_borrowed_session_routes_persistence_and_is_exposed(
    dag_maker: DagMaker,
    session: Session,
) -> None:
    """Use a caller-supplied session for every metadata write and expose it."""

    with dag_maker(dag_id="borrowed_session", session=session):
        EmptyOperator(task_id="empty")

    assert dag_maker.session is session
    assert session.get(DagModel, "borrowed_session") is not None
    assert _row_counts("borrowed_session") == (1, 1, 1, 1)
    dag_run = dag_maker.create_dagrun()
    assert dag_run.run_id.startswith("manual__pytest-airflow-in-a-box")


def test_dag_model_returns_the_live_metadata_row(dag_maker: DagMaker) -> None:
    """Expose the persisted `DagModel` row on the factory's metadata session."""

    with dag_maker(dag_id="dag_model_handle"):
        EmptyOperator(task_id="empty")

    dag_model = dag_maker.dag_model
    assert dag_model.dag_id == "dag_model_handle"
    assert dag_model.is_paused is False
    assert dag_model is dag_maker.session.get(DagModel, "dag_model_handle")


def test_sync_dagbag_to_db_republishes_a_mutated_dag(dag_maker: DagMaker) -> None:
    """Re-persist the mutated authoring Dag and refresh the scheduler handles."""

    with dag_maker(dag_id="resync_mutation") as dag:
        EmptyOperator(task_id="original")

    before = dag_maker.serialized_dag
    assert before is not None
    assert before.task_ids == ["original"]

    EmptyOperator(task_id="added", dag=dag)
    reloaded = dag_maker.sync_dagbag_to_db()

    assert reloaded is dag_maker.serialized_dag
    assert sorted(reloaded.task_ids) == ["added", "original"]
    # DagRun creation revalidates against the latest DagVersion, which the resync
    # just advanced -- the round trip proves the handles stay coherent.
    dag_run = dag_maker.create_dagrun()
    assert sorted(ti.task_id for ti in dag_run.task_instances) == ["added", "original"]


def test_sync_dagbag_to_db_on_a_borrowed_session_commits_staged_state(
    dag_maker: DagMaker,
    session: Session,
) -> None:
    """Commit caller-staged metadata alongside the resync, as persistence does."""

    with dag_maker(dag_id="resync_borrowed", session=session):
        EmptyOperator(task_id="empty")

    dag_maker.dag_model.is_paused = True
    dag_maker.sync_dagbag_to_db()

    with create_session() as verification_session:
        persisted = verification_session.get(DagModel, "resync_borrowed")
        assert persisted is not None
        assert persisted.is_paused is True


def test_sync_dagbag_to_db_failure_leaves_rows_for_teardown(
    dag_maker: DagMaker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Wrap a resync failure without deleting the already-persisted metadata rows."""

    with dag_maker(dag_id="resync_failure"):
        EmptyOperator(task_id="empty")
    failure = OSError("serialized read failed")

    def fail_load(record: dag_compat.DagPersistenceRecord) -> None:
        """Fail after the resync transaction has committed."""

        del record
        raise failure

    monkeypatch.setattr(dag_compat, "_load_serialized_dag", fail_load)

    with pytest.raises(
        DagPersistenceError,
        match="Could not resync Airflow Dag 'resync_failure'",
    ) as caught:
        dag_maker.sync_dagbag_to_db()

    assert caught.value.__cause__ is failure
    assert _row_counts("resync_failure") == (1, 1, 1, 1)


def test_borrowed_session_survives_context_body_error(
    dag_maker: DagMaker,
    session: Session,
) -> None:
    """Leave a caller-supplied session open and usable after a failed body."""

    class BodyFailedError(Exception):
        """Sentinel raised by the context body; never raised by the plugin itself."""

    with (
        pytest.raises(BodyFailedError, match="body failed"),
        dag_maker(dag_id="borrowed_failed", session=session),
    ):
        raise BodyFailedError("body failed")

    assert session.get(DagModel, "borrowed_failed") is None
    assert _row_counts("borrowed_failed") == (0, 0, 0, 0)


def test_borrowed_session_not_closed_when_dag_id_exists(
    dag_maker: DagMaker,
    session: Session,
) -> None:
    """Refuse an owned identifier without closing the caller-supplied session."""

    with dag_maker(dag_id="borrowed_duplicate"):
        EmptyOperator(task_id="original")

    with (
        pytest.raises(ValueError, match="already exists"),
        dag_maker(dag_id="borrowed_duplicate", session=session),
    ):
        EmptyOperator(task_id="replacement")

    assert session.get(DagModel, "borrowed_duplicate") is not None
    assert _row_counts("borrowed_duplicate") == (1, 1, 1, 1)


def test_bundle_name_override_routes_to_all_metadata(dag_maker: DagMaker) -> None:
    """Record a caller-supplied bundle name instead of the derived one."""

    with dag_maker(dag_id="bundle_override", bundle_name="issue-238-named-bundle"):
        EmptyOperator(task_id="empty")

    with create_session() as check:
        assert check.get(DagBundleModel, "issue-238-named-bundle") is not None
        assert check.get(DagBundleModel, _bundle_name("bundle_override")) is None
        dag_model = check.get(DagModel, "bundle_override")
        assert dag_model is not None
        assert dag_model.bundle_name == "issue-238-named-bundle"
        version = check.scalar(select(DagVersion).where(DagVersion.dag_id == "bundle_override"))
        assert version is not None
        assert version.bundle_name == "issue-238-named-bundle"


def test_rejects_invalid_harness_argument_types(dag_maker: DagMaker) -> None:
    """Reject harness keyword values outside the typed public call contract."""

    invalid_session: Any = 42
    invalid_name: Any = 42
    invalid_version: Any = 42

    with pytest.raises(TypeError, match="`session` must be a SQLAlchemy session"):
        dag_maker(session=invalid_session)
    with pytest.raises(TypeError, match="`bundle_name` must be a string"):
        dag_maker(bundle_name=invalid_name)
    with pytest.raises(ValueError, match="`bundle_name` must be non-empty"):
        dag_maker(bundle_name="")
    with pytest.raises(TypeError, match="`bundle_version` must be a string"):
        dag_maker(bundle_version=invalid_version)


class _TouchRecordingSession:
    """Count the rollback and close calls cleanup ownership handling makes."""

    def __init__(self) -> None:
        """Initialize the rollback and close counters."""

        self.rollbacks = 0
        self.closes = 0

    def rollback(self) -> None:
        """Record one rollback."""

        self.rollbacks += 1

    def close(self) -> None:
        """Record one close."""

        self.closes += 1


def test_cleanup_dag_swaps_a_borrowed_session(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace a borrowed session with a fresh owned one before teardown cleanup."""

    borrowed: Any = _TouchRecordingSession()
    fresh: Any = _TouchRecordingSession()
    cleaned: list[dag_compat.DagPersistenceRecord] = []

    def open_fresh(dag_id: str) -> Any:
        """Return the fresh replacement session."""

        del dag_id
        return fresh

    monkeypatch.setattr(dag_compat, "open_dag_session", open_fresh)
    monkeypatch.setattr(dag_compat, "_cleanup_dag", cleaned.append)
    record = dag_compat.DagPersistenceRecord(
        dag_id="borrowed_teardown_swap",
        bundle_name="borrowed_teardown_bundle",
        session=borrowed,
        session_owned=False,
    )

    dag_compat.cleanup_dag(record)

    assert cleaned == [record]
    assert record.session is fresh
    assert record.session_owned is True
    assert fresh.closes == 1
    assert borrowed.rollbacks == 0
    assert borrowed.closes == 0


def test_cleanup_dag_open_failure_leaves_borrowed_session_untouched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Never roll back or close a borrowed session presumed dead at teardown."""

    borrowed: Any = _TouchRecordingSession()

    def fail_open(dag_id: str) -> Any:
        """Refuse the fresh cleanup session."""

        raise DagPersistenceError(f"no session for '{dag_id}'")

    monkeypatch.setattr(dag_compat, "open_dag_session", fail_open)
    record = dag_compat.DagPersistenceRecord(
        dag_id="borrowed_teardown_dead",
        bundle_name="borrowed_teardown_bundle",
        session=borrowed,
        session_owned=False,
    )

    with pytest.raises(DagCleanupError, match="Could not clean Airflow Dag metadata"):
        dag_compat.cleanup_dag(record)

    assert borrowed.rollbacks == 0
    assert borrowed.closes == 0


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


def test_cleanup_dag_clears_a_referencing_backfill_dag_run() -> None:
    """Delete both the `BackfillDagRun` referencing an owned DagRun and its `Backfill` parent.

    Regression test for issue #240: a leaked `BackfillDagRun` row referencing a
    fixture-owned DagRun used to make cleanup raise `sqlite3.IntegrityError: FOREIGN
    KEY constraint failed` instead of `DagCleanupError` wrapping a real failure.
    `DagRun.backfill_id` is set too -- the FK with no `ondelete` action that requires
    the DagRun gone before its `Backfill` parent can be deleted.
    """

    from airflow.models.backfill import Backfill, BackfillDagRun
    from airflow.sdk.timezone import utcnow
    from sqlalchemy import update

    session = dag_compat.open_dag_session("backfill_cleanup")
    record = dag_compat.DagPersistenceRecord(
        dag_id="backfill_cleanup",
        bundle_name=_bundle_name("backfill_cleanup"),
        session=session,
    )
    dag = dag_compat.build_dag("backfill_cleanup", __file__, {})
    scheduler_dag = dag_compat.persist_dag(dag, record)
    dag_run = dag_compat.create_dag_run(
        scheduler_dag,
        dag,
        record,
        run_id="backfill_cleanup_run",
        logical_date=None,
        run_after=None,
        start_date=None,
        dag_run_kwargs={},
    )

    with create_session() as setup_session:
        backfill = Backfill(
            dag_id="backfill_cleanup",
            from_date=utcnow(),
            to_date=utcnow(),
            max_active_runs=1,
        )
        setup_session.add(backfill)
        setup_session.flush()
        setup_session.add(
            BackfillDagRun(
                backfill_id=backfill.id,
                dag_run_id=dag_run.id,
                sort_ordinal=1,
                logical_date=utcnow(),
            )
        )
        setup_session.execute(
            update(DagRun).where(DagRun.id == dag_run.id).values(backfill_id=backfill.id)
        )

    dag_compat.cleanup_dag(record)

    with create_session() as verify_session:
        assert verify_session.get(DagRun, dag_run.id) is None
        assert verify_session.scalar(select(func.count()).select_from(BackfillDagRun)) == 0
        assert verify_session.scalar(select(func.count()).select_from(Backfill)) == 0


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
    factory = _DagFactory("node", __file__, "master")
    scheduler_dag: Any = object()
    factory._finish(first, scheduler_dag)
    factory._finish(second, scheduler_dag)
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


def test_shared_caller_bundle_name_cleans_up_after_last_reference(
    pytester: pytest.Pytester,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Delete a caller-supplied shared bundle row only after its last Dag is gone."""

    pytester.makepyfile(
        """
        import pytest
        from airflow.models.dag import DagModel
        from airflow.models.dagbundle import DagBundleModel
        from airflow.providers.standard.operators.empty import EmptyOperator
        from airflow.utils.session import create_session

        pytestmark = pytest.mark.db_test
        BUNDLE_NAME = "issue-238-shared-bundle"


        def test_create(dag_maker):
            with dag_maker(dag_id="shared_first", bundle_name=BUNDLE_NAME):
                EmptyOperator(task_id="empty")
            with dag_maker(dag_id="shared_second", bundle_name=BUNDLE_NAME):
                EmptyOperator(task_id="empty")
            with create_session() as session:
                assert session.get(DagBundleModel, BUNDLE_NAME) is not None


        def test_cleaned():
            with create_session() as session:
                assert session.get(DagBundleModel, BUNDLE_NAME) is None
                assert session.get(DagModel, "shared_first") is None
                assert session.get(DagModel, "shared_second") is None
        """
    )

    monkeypatch.setenv("PYTEST_DISABLE_PLUGIN_AUTOLOAD", "1")
    result = pytester.runpytest_subprocess("-p", "pytest_airflow_in_a_box.plugin", "-q")

    result.assert_outcomes(passed=2)


def test_borrowed_session_cleanup_survives_session_finalization(
    pytester: pytest.Pytester,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Clean owned rows through a fresh session after the borrowed one is dead.

    The test body closes the caller's session before ``dag_maker``'s finalizer runs --
    the same state pytest's reverse-order fixture finalization produces whenever the
    ``session`` fixture finalizes first.
    """

    pytester.makepyfile(
        """
        import pytest
        from airflow.models.dag import DagModel
        from airflow.providers.standard.operators.empty import EmptyOperator
        from airflow.utils.session import create_session

        pytestmark = pytest.mark.db_test


        def test_create(dag_maker, session):
            with dag_maker(dag_id="borrowed_teardown", session=session):
                EmptyOperator(task_id="empty")
            session.close()


        def test_cleaned():
            with create_session() as session:
                assert session.get(DagModel, "borrowed_teardown") is None
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


def test_run_dag_cleanup_continues_after_one_failure(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Attempt every owned cleanup before reporting aggregated finalizer failures.

    Parameters:
        request: pytest.FixtureRequest the runner holds for executor-driven runs; this
            one never takes that path, so nothing is looked up through it.
        monkeypatch: pytest.MonkeyPatch replacing the cleanup entry point.
    """

    session: Any = object()
    first = dag_compat.DagPersistenceRecord("first", "first_bundle", session)
    second = dag_compat.DagPersistenceRecord("second", "second_bundle", session)
    runner = _DagRunner(request)
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


# ---------------------------------------------------------------------------
# Transparent custom-timetable registration (#114)
# ---------------------------------------------------------------------------


@pytest.mark.need_serialized_dag
def test_dag_maker_registers_a_custom_timetable_transparently(
    dag_maker: DagMaker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Serialize a Dag scheduled by a custom timetable with zero plugin wiring.

    The user's whole surface here is `schedule=`: `dag_maker` pulls the component
    sandbox lazily, registers the timetable's class, and Airflow's own persist/load
    round trip reconstructs it -- the exact flow that raises `TimetableNotRegistered`
    without the registration.
    """

    monkeypatch.syspath_prepend(str(CORPUS))
    # `import_module` on purpose, not a static `from provider_package import ...`:
    # `provider_package` resolves only after `monkeypatch.syspath_prepend` above, a
    # runtime-only fact `ty` cannot see.
    ExampleTimetable = import_module("provider_package").ExampleTimetable

    with dag_maker(schedule=ExampleTimetable(hours=2)) as dag:
        EmptyOperator(task_id="scheduled")

    serialized_dag = dag_maker.serialized_dag
    assert serialized_dag is not None
    assert serialized_dag.dag_id == dag.dag_id
    timetable = cast("Any", serialized_dag).timetable
    assert type(timetable) is ExampleTimetable
    assert timetable.hours == 2


@pytest.mark.need_serialized_dag
def test_dag_maker_leaves_builtin_timetables_alone(dag_maker: DagMaker) -> None:
    """Serialize a built-in-timetable Dag without ever constructing the sandbox."""

    from airflow.timetables.trigger import CronTriggerTimetable

    with dag_maker(schedule=CronTriggerTimetable("@daily", timezone="UTC")):
        EmptyOperator(task_id="builtin")

    from pytest_airflow_in_a_box._compat import components as sandbox_compat

    core_module, _sdk_module = sandbox_compat._plugins_manager_modules()
    plugin_names = {
        type(candidate).__name__ for candidate in sandbox_compat._live_plugin_list(core_module)
    }
    assert "ComponentRegistryPlugin" not in plugin_names


def test_dag_maker_registers_even_for_an_unserialized_dag(
    dag_maker: DagMaker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Register at Dag construction regardless of serialization semantics.

    Registration happens at `__call__` because Airflow 3.1's `encode_timetable`
    already needs it when `persist_dag` runs; with `serialized=False` the persist
    still encodes, so the registration is load-bearing there too.
    """

    monkeypatch.syspath_prepend(str(CORPUS))
    # `import_module` on purpose; see the transparent-registration test above.
    ExampleTimetable = import_module("provider_package").ExampleTimetable

    with dag_maker(schedule=ExampleTimetable(hours=5), serialized=False) as dag:
        EmptyOperator(task_id="unserialized")

    assert dag_maker.serialized_dag is not None
    assert type(dag.timetable) is ExampleTimetable


def test_dag_factory_hook_fires_only_for_custom_timetable_instances(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Invoke the registration hook exactly once, for exactly the custom instance."""

    monkeypatch.syspath_prepend(str(CORPUS))
    # `import_module` on purpose; see the transparent-registration test above.
    ExampleTimetable = import_module("provider_package").ExampleTimetable
    registered: list[Any] = []
    factory = _DagFactory("node", __file__, "master", register_timetable=registered.append)

    factory("hook_none_schedule")
    factory("hook_string_schedule", schedule="@daily")
    timetable = ExampleTimetable(hours=6)
    factory("hook_custom_schedule", schedule=timetable)

    assert registered == [timetable]


def test_dag_factory_without_the_hook_skips_registration() -> None:
    """Build a custom-timetable Dag with no hook wired, for direct factory users."""

    factory = _DagFactory("node", __file__, "master")

    from airflow.timetables.base import Timetable

    class _InlineTimetable(Timetable):
        """Reach `build_dag` untouched; the None hook must short-circuit first."""

    factory("no_hook_schedule", schedule=_InlineTimetable())

    assert type(factory.dag.timetable) is _InlineTimetable


@pytest.mark.need_serialized_dag
def test_dag_maker_registers_a_timetable_nested_in_asset_or_time_schedule(
    dag_maker: DagMaker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Serialize an `AssetOrTimeSchedule` wrapping a custom timetable transparently.

    The wrapper lives under `airflow.timetables.assets`, but its `serialize()`
    encodes the INNER custom timetable -- the exact shape that raised
    `TimetableNotRegistered` before the collector descended one level.
    """

    from airflow.sdk import Asset
    from airflow.timetables.assets import AssetOrTimeSchedule

    monkeypatch.syspath_prepend(str(CORPUS))
    # `import_module` on purpose; see the transparent-registration test above.
    ExampleTimetable = import_module("provider_package").ExampleTimetable
    # `cast` because `AssetOrTimeSchedule` annotates `assets` with the serialized
    # asset type; the authoring-time SDK `Asset` is converted at runtime.
    schedule = AssetOrTimeSchedule(
        timetable=ExampleTimetable(hours=3),
        assets=cast("Any", [Asset("dag_maker_wrapped_asset")]),
    )

    with dag_maker(schedule=schedule) as dag:
        EmptyOperator(task_id="wrapped")

    serialized_dag = dag_maker.serialized_dag
    assert serialized_dag is not None
    assert serialized_dag.dag_id == dag.dag_id
    inner = cast("Any", serialized_dag).timetable.timetable
    assert type(inner) is ExampleTimetable
    assert inner.hours == 3


def test_dag_maker_rejects_a_custom_timetable_class_as_schedule(
    dag_maker: DagMaker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Name the class-instead-of-instance mistake before Airflow's late `KeyError`."""

    monkeypatch.syspath_prepend(str(CORPUS))
    # `import_module` on purpose; see the transparent-registration test above.
    ExampleTimetable = import_module("provider_package").ExampleTimetable

    with pytest.raises(TypeError, match=r"pass `ExampleTimetable\(\.\.\.\)` instead of the class"):
        dag_maker(schedule=ExampleTimetable)


class _SerializeOnlyTimetable(Timetable):
    """Override only `serialize`, the shape upstream accepts but the full gate flags.

    Upstream's base `deserialize` defaults to `return cls()`, so a stateless
    serialize-only timetable is fully functional -- `dag_maker`'s transparent
    registration must warn, not hard-fail, on `timetable-serialize-pair-incomplete`
    (and on the missing protocol methods, equally irrelevant to serialization).
    """

    def serialize(self) -> dict[str, Any]:
        """Emit an empty payload.

        Returns:
            dict[str, Any] containing nothing.
        """

        return {}


@pytest.mark.need_serialized_dag
def test_dag_maker_warns_but_registers_a_gate_flagged_timetable(
    dag_maker: DagMaker,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Serialize a serialize-only timetable that the full conformance gate would refuse.

    The registration-scoped gate downgrades every non-futile problem to a warning:
    a Dag that persisted fine before the transparent hook existed must keep
    persisting, with the conformance signal surviving in the log.
    """

    with (
        caplog.at_level(logging.WARNING, logger="pytest_airflow_in_a_box.fixtures.components"),
        dag_maker(schedule=_SerializeOnlyTimetable()),
    ):
        EmptyOperator(task_id="lenient")

    serialized_dag = dag_maker.serialized_dag
    assert serialized_dag is not None
    assert type(cast("Any", serialized_dag).timetable) is _SerializeOnlyTimetable
    assert "timetable-serialize-pair-incomplete" in caplog.text


def test_dag_maker_rejects_a_local_scope_timetable(dag_maker: DagMaker) -> None:
    """Keep the one futile-registration problem a hard failure on the transparent path."""

    from airflow.timetables.base import Timetable

    class _LocalTimetable(Timetable):
        """Fail `timetable-local-qualname` by construction."""

    with pytest.raises(ComponentContractError, match="timetable-local-qualname"):
        dag_maker(schedule=_LocalTimetable())


@pytest.mark.need_serialized_dag
def test_dag_maker_skips_registration_for_an_already_registered_timetable(
    dag_maker: DagMaker,
    airflow_components: ComponentRegistry,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Leave a class the registered lookup already resolves entirely alone.

    Simulates the deployed-the-supported-way setup (plugins folder / entry point) with
    an explicit prior registration: the transparent hook's lookup probe hits, so the
    lenient gate never runs and no second registration happens.
    """

    monkeypatch.syspath_prepend(str(CORPUS))
    # `import_module` on purpose; see the transparent-registration test above.
    ExampleTimetable = import_module("provider_package").ExampleTimetable
    airflow_components.timetable(ExampleTimetable)

    with dag_maker(schedule=ExampleTimetable(hours=8)):
        EmptyOperator(task_id="pre_registered")

    serialized_dag = dag_maker.serialized_dag
    assert serialized_dag is not None
    timetable = cast("Any", serialized_dag).timetable
    assert type(timetable) is ExampleTimetable
    assert timetable.hours == 8
