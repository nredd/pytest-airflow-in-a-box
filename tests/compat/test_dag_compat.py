"""Test guarded Dag compatibility operations and failure reporting."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from pytest_airflow_in_a_box._compat import dag as dag_compat
from pytest_airflow_in_a_box._compat.dag import (
    DagCleanupError,
    DagPersistenceError,
    DagPersistenceRecord,
)


class _Session:
    """Record the session lifecycle needed by compatibility failure tests."""

    def __init__(self) -> None:
        """Initialize lifecycle counters."""

        self.rollbacks = 0
        self.closes = 0

    def rollback(self) -> None:
        """Record one rollback."""

        self.rollbacks += 1

    def close(self) -> None:
        """Record one close."""

        self.closes += 1


def _record(session: Any) -> DagPersistenceRecord:
    """Build a representative persistence ownership record.

    Parameters:
        session: Any containing a fake metadata session.

    Returns:
        DagPersistenceRecord containing deterministic test identities.
    """

    return DagPersistenceRecord(
        dag_id="compat_dag",
        bundle_name="compat_bundle",
        session=session,
    )


def test_open_session_wraps_capability_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Name metadata session creation and retain the compatibility failure."""

    failure = RuntimeError("unsupported runtime")

    def fail() -> None:
        """Raise a representative capability failure."""

        raise failure

    monkeypatch.setattr(dag_compat, "resolve_capabilities", fail)

    with pytest.raises(DagPersistenceError, match="open an Airflow metadata session") as caught:
        dag_compat.open_dag_session("broken")

    assert caught.value.__cause__ is failure


def test_open_session_rejects_uninitialized_factory(monkeypatch: pytest.MonkeyPatch) -> None:
    """Wrap Airflow's absent process-local session factory with operation context."""

    from airflow import settings

    monkeypatch.setattr(dag_compat, "resolve_capabilities", lambda: None)
    monkeypatch.setattr(settings, "Session", None)

    with pytest.raises(DagPersistenceError, match="not initialized") as caught:
        dag_compat.open_dag_session("uninitialized")

    assert isinstance(caught.value.__cause__, RuntimeError)


def test_existing_dag_check_wraps_query_failure() -> None:
    """Name the ownership check and retain its SQLAlchemy failure."""

    failure = OSError("query failed")

    class FailingSession:
        """Raise one representative metadata query failure."""

        def get(self, model: object, dag_id: str) -> None:
            """Raise after accepting the model and identifier."""

            del model, dag_id
            raise failure

    session: Any = FailingSession()

    with pytest.raises(DagPersistenceError, match="check existing Airflow metadata") as caught:
        dag_compat.ensure_dag_absent("query_failure", session)

    assert caught.value.__cause__ is failure


@pytest.mark.parametrize(
    ("release", "expected_module"),
    [
        ((3, 1, 0), "airflow.serialization.serialized_objects"),
        ((3, 2, 0), "airflow.serialization.definitions.dag"),
    ],
)
def test_serialized_dag_location_branching(
    monkeypatch: pytest.MonkeyPatch,
    release: tuple[int, int, int],
    expected_module: str,
) -> None:
    """Resolve only the certified serialization module for each release family."""

    sentinel = object()
    imported: list[str] = []

    def import_module(module_name: str) -> SimpleNamespace:
        """Record and return one fake serialization module."""

        imported.append(module_name)
        return SimpleNamespace(SerializedDAG=sentinel)

    monkeypatch.setattr(
        dag_compat,
        "resolve_capabilities",
        lambda: SimpleNamespace(release=release),
    )
    monkeypatch.setattr(dag_compat, "import_module", import_module)

    assert dag_compat._get_serialized_dag_class() is sentinel
    assert imported == [expected_module]


def test_persistence_reports_cleanup_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Retain the persistence cause while reporting a secondary cleanup failure."""

    session = _Session()
    persistence_failure = OSError("bundle write failed")

    def fail_bundle(record: DagPersistenceRecord) -> None:
        """Raise the primary persistence failure."""

        del record
        raise persistence_failure

    def fail_cleanup(record: DagPersistenceRecord) -> None:
        """Raise a secondary cleanup failure."""

        del record
        raise RuntimeError("cleanup failed")

    monkeypatch.setattr(dag_compat, "_ensure_bundle", fail_bundle)
    monkeypatch.setattr(dag_compat, "_cleanup_dag", fail_cleanup)
    dag: Any = object()

    with pytest.raises(DagPersistenceError, match="cleanup also failed") as caught:
        dag_compat.persist_dag(dag, _record(session))

    assert caught.value.__cause__ is persistence_failure
    assert session.rollbacks == 1


def test_loading_requires_serialized_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reject a successful write that cannot be read back from Airflow metadata."""

    from airflow.models.serialized_dag import SerializedDagModel

    def return_missing(dag_id: str, session: object) -> None:
        """Return no serialized metadata for the requested Dag."""

        del dag_id, session

    monkeypatch.setattr(SerializedDagModel, "get_dag", return_missing)

    with pytest.raises(RuntimeError, match="did not return"):
        dag_compat._load_serialized_dag(_record(object()))


def test_cleanup_wraps_failure_and_always_closes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Roll back, retain, and close after an Airflow cleanup failure."""

    session = _Session()
    failure = OSError("delete failed")

    def fail_cleanup(record: DagPersistenceRecord) -> None:
        """Raise a representative Airflow deletion failure."""

        del record
        raise failure

    monkeypatch.setattr(dag_compat, "_cleanup_dag", fail_cleanup)

    with pytest.raises(DagCleanupError, match="compat_dag") as caught:
        dag_compat.cleanup_dag(_record(session))

    assert caught.value.__cause__ is failure
    assert session.rollbacks == 1
    assert session.closes == 1
