"""Test the public Dag construction and persistence fixture.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-serialization.html
    https://docs.pytest.org/en/stable/how-to/fixtures.html
"""

from __future__ import annotations

import gc
import logging
import os
from datetime import datetime, timedelta, timezone
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
from pytest_airflow_in_a_box._compat import registry
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


FOREIGN_BUNDLE = "issue-262-foreign-bundle"


def _seed_foreign_dag_model(dag_id: str) -> None:
    """Insert one bare ``DagModel`` row this process never persisted.

    Parameters:
        dag_id: str identifying the foreign row; ``DagModel.bundle_name`` is NOT NULL
            and FK-constrained, so a shared foreign bundle row backs it.
    """

    with create_session() as session:
        if session.get(DagBundleModel, FOREIGN_BUNDLE) is None:
            session.add(DagBundleModel(name=FOREIGN_BUNDLE))
            session.flush()
        session.add(DagModel(dag_id=dag_id, bundle_name=FOREIGN_BUNDLE))


def _delete_foreign_dag_model(dag_id: str) -> None:
    """Remove one seeded foreign ``DagModel`` row, asserting it survived the test.

    Parameters:
        dag_id: str identifying the foreign row.

    Raises:
        AssertionError: The refused registration deleted the foreign row anyway.
    """

    with create_session() as session:
        model = session.get(DagModel, dag_id)
        assert model is not None
        session.delete(model)


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
        _ = dag_maker.timetable
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


def test_recreating_owned_dag_id_replaces_metadata(
    dag_maker: DagMaker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Replace an identifier this process itself persisted, upstream-style.

    Upstream suites re-create short ids (`dag`, `test1`) within one test -- create,
    mutate, re-create -- and `tests_common` re-syncs silently. The second context must
    purge the first registration's rows and persist its own (issue #262). Replacement
    is serial-only, so the worker marker is cleared to keep this test deterministic
    under an xdist run of this suite.
    """

    monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)
    with dag_maker(dag_id="issue_262_recreated"):
        EmptyOperator(task_id="original")
    # A run (and its task instance) from the first registration exercises the purge's
    # run-cascade path when the second registration replaces it.
    dag_maker.create_dagrun()

    with dag_maker(dag_id="issue_262_recreated"):
        EmptyOperator(task_id="replacement")

    assert _row_counts("issue_262_recreated") == (1, 1, 1, 1)
    with create_session() as check:
        run_count = check.scalar(
            select(func.count()).select_from(DagRun).where(DagRun.dag_id == "issue_262_recreated")
        )
    assert run_count == 0
    serialized = dag_maker.serialized_dag
    assert serialized is not None
    assert serialized.task_ids == ["replacement"]


def test_foreign_dag_id_is_not_overwritten(dag_maker: DagMaker) -> None:
    """Refuse an identifier whose metadata this process never persisted."""

    _seed_foreign_dag_model("issue_262_foreign_maker")

    with (
        pytest.raises(ValueError, match="already exists"),
        dag_maker(dag_id="issue_262_foreign_maker"),
    ):
        EmptyOperator(task_id="rejected")

    _delete_foreign_dag_model("issue_262_foreign_maker")


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
    # Ownership history is recorded at the commit, not at full success: had the
    # follow-up `_cleanup_dag` also failed here, the committed row would be this
    # process's leftover and a later re-registration must replace it, not raise.
    assert registry.was_dag_id_persisted("failed_persistence")


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


def test_created_run_task_refresh_survives_garbage_collection(dag_maker: DagMaker) -> None:
    """Keep refreshed task instances reachable through the returned DagRun.

    Regression test for issue #259: `create_dag_run` used to refresh the transient
    `get_task_instances` query result, which the session's weak identity map dropped at
    the next garbage collection -- any later lookup rehydrated fresh rows with
    `ti.task = None` and upstream-shaped consumers crashed on `ti.task.queue`.

    Parameters:
        dag_maker: DagMaker building the fixture-owned Dag.
    """

    with dag_maker(dag_id="refresh_survives_gc"):
        EmptyOperator(task_id="first")
        EmptyOperator(task_id="second")

    dag_run = dag_maker.create_dagrun()
    gc.collect()

    fetched = dag_run.get_task_instances(session=dag_maker.session)
    assert sorted(ti.task_id for ti in fetched) == ["first", "second"]
    assert all(ti.task is not None for ti in fetched)


def test_created_run_relationship_exposes_refreshed_task_instances(
    dag_maker: DagMaker,
) -> None:
    """Expose usable instances through `dag_run.task_instances`, upstream's shape.

    The upstream `tests_common` consumer pattern (`test_cleartasks.py`): sort the
    relationship collection, then use each instance's refreshed authoring task.

    Parameters:
        dag_maker: DagMaker building the fixture-owned Dag.
    """

    with dag_maker(dag_id="relationship_task_instances") as dag:
        EmptyOperator(task_id="first")
        EmptyOperator(task_id="second", retries=2)

    dag_run = dag_maker.create_dagrun()
    gc.collect()

    # Genexp on purpose: ty resolves the relationship attribute as
    # `InstrumentedAttribute[Any]`, which fails `list(...)`'s `Iterable` protocol
    # check while plain iteration is accepted (same shape as `sync_dagbag_to_db`'s
    # round-trip assertion above).
    ti0, ti1 = sorted((ti for ti in dag_run.task_instances), key=lambda ti: str(ti.task_id))
    assert (ti0.task_id, ti1.task_id) == ("first", "second")
    for ti in (ti0, ti1):
        assert ti.task is not None
        assert ti.task is dag.get_task(ti.task_id)
        assert ti.queue == ti.task.queue


def test_dag_model_returns_the_live_metadata_row(dag_maker: DagMaker) -> None:
    """Expose the persisted `DagModel` row on the factory's metadata session."""

    with dag_maker(dag_id="dag_model_handle"):
        EmptyOperator(task_id="empty")

    dag_model = dag_maker.dag_model
    assert dag_model.dag_id == "dag_model_handle"
    assert dag_model.is_paused is False
    assert dag_model is dag_maker.session.get(DagModel, "dag_model_handle")


def test_timetable_returns_the_scheduler_timetable(dag_maker: DagMaker) -> None:
    """Expose the persisted scheduler timetable with its interval-inference method.

    The migration target for upstream's `dag.timetable.infer_manual_data_interval`
    pattern, which Airflow 3.2+ removed from the yielded authoring Dag's timetable
    (issue #261).
    """

    with dag_maker(
        dag_id="timetable_handle",
        schedule=timedelta(days=1),
        start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    ):
        EmptyOperator(task_id="empty")

    run_after = datetime(2024, 3, 4, tzinfo=timezone.utc)
    interval = dag_maker.timetable.infer_manual_data_interval(run_after=run_after)

    assert interval.end == run_after
    assert interval.start == run_after - timedelta(days=1)
    serialized_dag = dag_maker.serialized_dag
    assert serialized_dag is not None
    assert dag_maker.timetable is serialized_dag.timetable


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


def test_borrowed_session_replaces_owned_dag_id(
    dag_maker: DagMaker,
    session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Replace an owned identifier on a caller-supplied session without closing it."""

    monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)
    with dag_maker(dag_id="issue_262_borrowed_replaced"):
        EmptyOperator(task_id="original")

    with dag_maker(dag_id="issue_262_borrowed_replaced", session=session):
        EmptyOperator(task_id="replacement")

    assert session.get(DagModel, "issue_262_borrowed_replaced") is not None
    assert _row_counts("issue_262_borrowed_replaced") == (1, 1, 1, 1)


def test_borrowed_session_not_closed_when_foreign_dag_id_exists(
    dag_maker: DagMaker,
    session: Session,
) -> None:
    """Refuse a foreign identifier without closing the caller-supplied session."""

    _seed_foreign_dag_model("issue_262_borrowed_foreign")

    with (
        pytest.raises(ValueError, match="already exists"),
        dag_maker(dag_id="issue_262_borrowed_foreign", session=session),
    ):
        EmptyOperator(task_id="rejected")

    assert session.get(DagModel, "issue_262_borrowed_foreign") is not None
    _delete_foreign_dag_model("issue_262_borrowed_foreign")


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


def test_cleanup_dag_rolls_back_and_swaps_a_borrowed_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Roll back a borrowed session, then replace it with a fresh owned one.

    The rollback must land BEFORE the fresh session opens (issue #263): an uncommitted
    flush on the borrowed session's SQLite connection holds the database's single write
    lock, so a fresh session opened first would block on its own cleanup deletes.
    """

    borrowed: Any = _TouchRecordingSession()
    fresh: Any = _TouchRecordingSession()
    cleaned: list[dag_compat.DagPersistenceRecord] = []

    def open_fresh(dag_id: str) -> Any:
        """Return the fresh replacement session after the borrowed rollback."""

        del dag_id
        assert borrowed.rollbacks == 1
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
    assert borrowed.rollbacks == 1
    assert borrowed.closes == 0


def test_cleanup_dag_tolerates_a_dead_borrowed_handle(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Log and skip a borrowed handle whose rollback raises, then clean up anyway."""

    class _DeadSession(_TouchRecordingSession):
        """Refuse the pre-cleanup rollback like a finalized broken handle."""

        def rollback(self) -> None:
            """Raise a representative dead-handle failure."""

            super().rollback()
            raise RuntimeError("handle is dead")

    borrowed: Any = _DeadSession()
    fresh: Any = _TouchRecordingSession()
    cleaned: list[dag_compat.DagPersistenceRecord] = []

    def open_fresh(dag_id: str) -> Any:
        """Return the fresh replacement session."""

        del dag_id
        return fresh

    monkeypatch.setattr(dag_compat, "open_dag_session", open_fresh)
    monkeypatch.setattr(dag_compat, "_cleanup_dag", cleaned.append)
    record = dag_compat.DagPersistenceRecord(
        dag_id="borrowed_teardown_dead_handle",
        bundle_name="borrowed_teardown_bundle",
        session=borrowed,
        session_owned=False,
    )

    with caplog.at_level(logging.INFO, logger="pytest_airflow_in_a_box._compat.dag"):
        dag_compat.cleanup_dag(record)

    assert cleaned == [record]
    assert record.session is fresh
    assert fresh.closes == 1
    assert borrowed.closes == 0
    assert "Could not roll back the borrowed session" in caplog.text


def test_cleanup_dag_open_failure_never_closes_a_borrowed_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Roll back but never close a borrowed session when no fresh session opens."""

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

    assert borrowed.rollbacks == 1
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


def _attach_backfill(dag_id: str, dag_run_id: Any) -> int:
    """Attach a test-created `Backfill` (and its join row) to one existing DagRun.

    The shared scaffold of both backfill-cleanup regression tests: a `Backfill` for
    `dag_id`, a `BackfillDagRun` join row pointing at `dag_run_id`, and the DagRun's
    own `backfill_id` set -- the FK with no `ondelete` action that blocks the
    `Backfill` delete until the run is gone. `BackfillDagRun.logical_date` is set
    explicitly because Airflow 3.1.x declares the column NOT NULL.

    Parameters:
        dag_id: str identifying the Dag the `Backfill` targets.
        dag_run_id: Any containing the DagRun primary key to reference and mark as
            backfilled -- typed dynamically because Airflow's ORM attribute access
            is untyped.

    Returns:
        int containing the created `Backfill` primary key.
    """

    from airflow.models.backfill import Backfill, BackfillDagRun
    from airflow.sdk.timezone import utcnow
    from sqlalchemy import update

    with create_session() as setup_session:
        # The dynamic annotation keeps `backfill.id` statically valid: Airflow's ORM
        # models expose `InstrumentedAttribute` to the checker, not `int`.
        backfill: Any = Backfill(
            dag_id=dag_id,
            from_date=utcnow(),
            to_date=utcnow(),
            max_active_runs=1,
        )
        setup_session.add(backfill)
        setup_session.flush()
        setup_session.add(
            BackfillDagRun(
                backfill_id=backfill.id,
                dag_run_id=dag_run_id,
                sort_ordinal=1,
                logical_date=utcnow(),
            )
        )
        setup_session.execute(
            update(DagRun).where(DagRun.id == dag_run_id).values(backfill_id=backfill.id)
        )
        return backfill.id


def test_cleanup_dag_clears_a_referencing_backfill_dag_run() -> None:
    """Delete both the `BackfillDagRun` referencing an owned DagRun and its `Backfill` parent.

    Regression test for issue #240: a leaked `BackfillDagRun` row referencing a
    fixture-owned DagRun used to make cleanup raise `sqlite3.IntegrityError: FOREIGN
    KEY constraint failed` instead of `DagCleanupError` wrapping a real failure.
    `DagRun.backfill_id` is set too -- the FK with no `ondelete` action that requires
    the DagRun gone before its `Backfill` parent can be deleted.
    """

    from airflow.models.backfill import Backfill, BackfillDagRun

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

    _attach_backfill("backfill_cleanup", dag_run.id)

    dag_compat.cleanup_dag(record)

    with create_session() as verify_session:
        assert verify_session.get(DagRun, dag_run.id) is None
        assert verify_session.scalar(select(func.count()).select_from(BackfillDagRun)) == 0
        assert verify_session.scalar(select(func.count()).select_from(Backfill)) == 0


def test_cleanup_dag_clears_test_created_backfill_rows() -> None:
    """Delete backfill rows the test created outside fixture ownership.

    Regression test for issue #258: a test driving Airflow's Backfill machinery
    directly creates a `Backfill` for the fixture's Dag, plus DagRuns (with
    `backfill_id` set) and `BackfillDagRun` join rows that never pass through
    `create_dag_run`, so none of them are in `record.dag_run_ids`. Cleanup scoped to
    fixture-owned rows only used to leave them referencing the Dag's backfills and
    fail the dag_id-scoped `Backfill` delete with `sqlite3.IntegrityError: FOREIGN
    KEY constraint failed`.
    """

    from airflow.models.backfill import Backfill, BackfillDagRun

    session = dag_compat.open_dag_session("backfill_foreign_cleanup")
    record = dag_compat.DagPersistenceRecord(
        dag_id="backfill_foreign_cleanup",
        bundle_name=_bundle_name("backfill_foreign_cleanup"),
        session=session,
    )
    dag = dag_compat.build_dag("backfill_foreign_cleanup", __file__, {})
    scheduler_dag = dag_compat.persist_dag(dag, record)
    dag_run = dag_compat.create_dag_run(
        scheduler_dag,
        dag,
        record,
        run_id="backfill_foreign_cleanup_run",
        logical_date=None,
        run_after=None,
        start_date=None,
        dag_run_kwargs={},
    )
    # Disown the run: the Backfill machinery's runs never pass through
    # `create_dag_run`, so cleanup cannot find this one through `dag_run_ids`.
    record.dag_run_ids.discard(dag_run.id)
    record.task_instance_keys.clear()

    backfill_id = _attach_backfill("backfill_foreign_cleanup", dag_run.id)

    dag_compat.cleanup_dag(record)

    # Scoped to this test's own rows (not whole-table counts) so a concurrent xdist
    # worker's backfill rows cannot leak into the assertions.
    with create_session() as verify_session:
        assert verify_session.get(DagRun, dag_run.id) is None
        assert (
            verify_session.scalar(
                select(func.count())
                .select_from(BackfillDagRun)
                .where(BackfillDagRun.backfill_id == backfill_id)
            )
            == 0
        )
        assert verify_session.get(Backfill, backfill_id) is None
    assert _row_counts("backfill_foreign_cleanup") == (0, 0, 0, 0)


def test_cleanup_dag_clears_unowned_dag_run_task_instances() -> None:
    """Delete DagRuns Airflow created for the Dag outside the fixture before its versions.

    Regression test for issue #266: ``dag.test()`` creates a manual DagRun through
    the scheduler Dag directly, so neither the run nor its task instances ever enter
    ``record.dag_run_ids``. ``task_instance.dag_version_id`` FK-references
    ``dag_version.id`` with ``ondelete="RESTRICT"`` -- the only restrictive foreign
    key into ``dag_version`` -- so cleanup scoped to fixture-owned runs used to fail
    the ``dag_version`` delete with ``sqlite3.IntegrityError: FOREIGN KEY constraint
    failed`` and strand every row behind it.
    """

    from airflow.models.taskinstance import TaskInstance

    session = dag_compat.open_dag_session("unowned_run_cleanup")
    record = dag_compat.DagPersistenceRecord(
        dag_id="unowned_run_cleanup",
        bundle_name=_bundle_name("unowned_run_cleanup"),
        session=session,
    )
    dag = dag_compat.build_dag("unowned_run_cleanup", __file__, {})
    EmptyOperator(task_id="only_task", dag=dag)
    scheduler_dag = dag_compat.persist_dag(dag, record)
    dag_run = dag_compat.create_dag_run(
        scheduler_dag,
        dag,
        record,
        run_id="unowned_run_cleanup_run",
        logical_date=None,
        run_after=None,
        start_date=None,
        dag_run_kwargs={},
    )
    # Disown the run: `dag.test()` calls the scheduler Dag's `create_dagrun` itself,
    # so cleanup cannot find its run -- or the task instances referencing the Dag's
    # `DagVersion` rows -- through `dag_run_ids`.
    record.dag_run_ids.discard(dag_run.id)
    record.task_instance_keys.clear()

    with create_session() as verify_session:
        version_references = verify_session.scalar(
            select(func.count())
            .select_from(TaskInstance)
            .where(
                TaskInstance.run_id == "unowned_run_cleanup_run",
                TaskInstance.dag_version_id.is_not(None),
            )
        )
    assert version_references == 1

    dag_compat.cleanup_dag(record)

    with create_session() as verify_session:
        assert verify_session.get(DagRun, dag_run.id) is None
        assert (
            verify_session.scalar(
                select(func.count())
                .select_from(TaskInstance)
                .where(TaskInstance.dag_id == "unowned_run_cleanup")
            )
            == 0
        )
    assert _row_counts("unowned_run_cleanup") == (0, 0, 0, 0)


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


def test_borrowed_session_cleanup_survives_an_uncommitted_flush(
    pytester: pytest.Pytester,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Clean owned rows when the borrowed session still holds an uncommitted flush.

    Regression test for issue #263: the test body flushes on the borrowed session
    without committing, so its SQLite connection idles in a write transaction when
    ``dag_maker``'s finalizer runs -- ``session`` is requested FIRST, so pytest
    finalizes it LAST. Cleanup's fresh session used to block on that never-released
    write lock for the full ``busy_timeout`` and then raise ``DagCleanupError``
    (``database is locked``); rolling the borrowed session back first releases the
    lock and discards the flushed row.
    """

    pytester.makepyfile(
        """
        import pytest
        from airflow.models.dag import DagModel
        from airflow.models.variable import Variable
        from airflow.providers.standard.operators.empty import EmptyOperator
        from airflow.utils.session import create_session
        from sqlalchemy import select

        pytestmark = pytest.mark.db_test


        def test_create(session, dag_maker):
            with dag_maker(dag_id="borrowed_uncommitted_flush", session=session):
                EmptyOperator(task_id="empty")
            session.add(Variable(key="issue_263_leak", val="uncommitted"))
            session.flush()


        def test_cleaned():
            with create_session() as session:
                assert session.get(DagModel, "borrowed_uncommitted_flush") is None
                leaked = session.scalar(
                    select(Variable).where(Variable.key == "issue_263_leak")
                )
                assert leaked is None
        """
    )

    monkeypatch.setenv("PYTEST_DISABLE_PLUGIN_AUTOLOAD", "1")
    result = pytester.runpytest_subprocess("-p", "pytest_airflow_in_a_box.plugin", "-q")

    result.assert_outcomes(passed=2)


def test_leaked_dag_id_from_a_previous_test_is_replaced(
    pytester: pytest.Pytester,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Replace a previous test's leaked row instead of failing the next test.

    The upstream `test_asset.py` shape behind issue #262: the test body re-syncs the
    Dag under another bundle (`tests_common`'s `sync_dags_to_db` rewrites
    `DagModel.bundle_name` to `testing`), so `_cleanup_dag`'s bundle-guarded delete
    silently skips the `DagModel` row and the next test reusing the short `dag_id`
    used to die in `ensure_dag_registrable`. The per-process ownership history must
    survive the drift and let the second test replace the leftover. The subprocess
    inherits this process's environment, so the worker marker is cleared first --
    the inner run is serial and must take the serial replace path even when this
    suite itself runs under xdist.
    """

    pytester.makepyfile(
        """
        import pytest
        from airflow.models.dag import DagModel
        from airflow.models.dagbundle import DagBundleModel
        from airflow.providers.standard.operators.empty import EmptyOperator
        from airflow.utils.session import create_session
        from sqlalchemy import update

        pytestmark = pytest.mark.db_test
        DAG_ID = "test1"


        def test_drifting_sync_leaks_the_dag_model_row(dag_maker):
            with dag_maker(dag_id=DAG_ID):
                EmptyOperator(task_id="t1")
            with create_session() as session:
                # Upstream's `testing_dag_bundle` fixture supplies the bundle row the
                # drifted `DagModel.bundle_name` FK-references; flushed ahead of the
                # core UPDATE, which does not autoflush the pending insert first.
                session.add(DagBundleModel(name="testing"))
                session.flush()
                session.execute(
                    update(DagModel)
                    .where(DagModel.dag_id == DAG_ID)
                    .values(bundle_name="testing")
                )


        def test_reuse_replaces_the_leftover(dag_maker):
            with create_session() as session:
                leftover = session.get(DagModel, DAG_ID)
                assert leftover is not None
                assert leftover.bundle_name == "testing"
            with dag_maker(dag_id=DAG_ID):
                EmptyOperator(task_id="t1")
            with create_session() as session:
                # The purge never deletes bundle rows -- they are shared identities.
                assert session.get(DagBundleModel, "testing") is not None


        def test_replacement_was_cleaned():
            with create_session() as session:
                assert session.get(DagModel, DAG_ID) is None
        """
    )

    monkeypatch.setenv("PYTEST_DISABLE_PLUGIN_AUTOLOAD", "1")
    monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)
    result = pytester.runpytest_subprocess("-p", "pytest_airflow_in_a_box.plugin", "-q")

    result.assert_outcomes(passed=3)


def test_owned_dag_id_collision_still_raises_on_an_xdist_worker(
    dag_maker: DagMaker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Never replace on a worker, where a leftover may be another worker's live row.

    Ownership history is per-process and metadata rows carry no writer identity, so
    on a shared metadata database an id from this process's own history may belong to
    another worker's in-flight test -- the guard must stay loud there (issue #262
    review finding).
    """

    monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)
    with dag_maker(dag_id="issue_262_worker_guard"):
        EmptyOperator(task_id="original")

    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw7")
    with (
        pytest.raises(ValueError, match="never replaced") as caught,
        dag_maker(dag_id="issue_262_worker_guard"),
    ):
        EmptyOperator(task_id="rejected")

    assert "xdist_group" in str(caught.value)
    assert _row_counts("issue_262_worker_guard") == (1, 1, 1, 1)


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


def test_run_dag_replaces_an_owned_dag_id(
    dag_maker: DagMaker,
    run_dag: RunDag,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Adopt a Dag whose id this process itself persisted, replacing the leftover."""

    monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)
    with dag_maker(dag_id="issue_262_run_dag_replaced"):
        EmptyOperator(task_id="owned_by_dag_maker")

    colliding = dag_compat.build_dag("issue_262_run_dag_replaced", __file__, {})
    with colliding:
        EmptyOperator(task_id="owned_by_run_dag")

    result = run_dag(colliding)

    assert result.success
    assert result.order == ["owned_by_run_dag"]
    assert _row_counts("issue_262_run_dag_replaced") == (1, 1, 1, 1)


def test_run_dag_rejects_a_foreign_dag_id(run_dag: RunDag) -> None:
    """Refuse to adopt a Dag whose id has metadata this process never persisted."""

    _seed_foreign_dag_model("issue_262_run_dag_foreign")

    colliding = dag_compat.build_dag("issue_262_run_dag_foreign", __file__, {})
    with colliding:
        EmptyOperator(task_id="rejected")

    with pytest.raises(ValueError, match="already exists"):
        run_dag(colliding)

    _delete_foreign_dag_model("issue_262_run_dag_foreign")


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
