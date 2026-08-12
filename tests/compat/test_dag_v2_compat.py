"""Probe-double coverage for the Airflow 2.x branches of `_compat.dag`.

The 2.x branches are unreachable from a 3.x install, so these tests hold the
100% gate with fake capability contracts and fake `airflow` modules -- the real
behavior is certified end to end by the 2.x compat CI legs.
"""

from __future__ import annotations

import sys
from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import pytest

from pytest_airflow_in_a_box._compat import dag as dag_module
from pytest_airflow_in_a_box._compat.capabilities import (
    _CERTIFIED_CAPABILITIES,
    AirflowFamily,
    TimezoneLocation,
)


@pytest.fixture
def v2_capabilities(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Pin `_compat.dag` to the certified 2.11.2 contract.

    Parameters:
        monkeypatch: pytest.MonkeyPatch replacing the resolver.

    Returns:
        AirflowCapabilities containing the certified 2.11.2 contract.
    """

    capabilities = _CERTIFIED_CAPABILITIES[(2, 11, 2)]
    monkeypatch.setattr(dag_module, "resolve_capabilities", lambda: capabilities)
    return capabilities


def _record(session: Any) -> dag_module.DagPersistenceRecord:
    """Build a persistence record around one fake session.

    Parameters:
        session: Any standing in for a SQLAlchemy session.

    Returns:
        DagPersistenceRecord bound to the fake session.
    """

    return dag_module.DagPersistenceRecord(
        dag_id="fake_dag", bundle_name="fake-bundle", session=session
    )


@pytest.mark.usefixtures("v2_capabilities")
def test_build_dag_uses_the_v2_authoring_class(monkeypatch: pytest.MonkeyPatch) -> None:
    """Construct through `airflow.models.dag.DAG` without touching `relative_fileloc`."""

    constructed: dict[str, Any] = {}

    class FakeDag:
        """Record 2.x authoring construction."""

        def __init__(self, dag_id: str, schedule: Any = None, **kwargs: Any) -> None:
            constructed.update({"dag_id": dag_id, "schedule": schedule, **kwargs})

    monkeypatch.setitem(sys.modules, "airflow.models.dag", SimpleNamespace(DAG=FakeDag))

    dag = dag_module.build_dag("fake_dag", "/suite/test_module.py", {"tags": ["compat"]})

    assert constructed == {"dag_id": "fake_dag", "schedule": None, "tags": ["compat"]}
    assert dag.fileloc == "/suite/test_module.py"
    assert not hasattr(dag, "relative_fileloc")


@pytest.mark.usefixtures("v2_capabilities")
def test_ensure_bundle_is_vacuous_on_v2() -> None:
    """Skip bundle creation entirely on the 2.x family."""

    record = _record(session=None)

    dag_module._ensure_bundle(record)

    assert record.bundle_created is False


@pytest.mark.usefixtures("v2_capabilities")
def test_sync_dag_model_uses_the_authoring_writer_on_v2() -> None:
    """Write through the authoring class's bundle-free `bulk_write_to_db`."""

    calls: list[tuple[Any, ...]] = []

    class FakeAuthoringDag:
        """Expose the 2.x class-level writer."""

        @classmethod
        def bulk_write_to_db(cls, dags: list[Any], session: Any = None) -> None:
            calls.append((tuple(dags), session))

    dag: Any = FakeAuthoringDag()
    record = _record(session="session-token")

    dag_module._sync_dag_model(dag, record)

    assert calls == [((dag,), "session-token")]


@pytest.mark.usefixtures("v2_capabilities")
def test_write_serialized_dag_uses_the_v2_signature(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Serialize the authoring Dag directly with the 2.x keyword set."""

    calls: list[dict[str, Any]] = []

    class FakeSerializedDagModel:
        """Record 2.x `write_dag` calls."""

        @staticmethod
        def write_dag(dag: Any, min_update_interval: int, session: Any) -> None:
            calls.append(
                {"dag": dag, "min_update_interval": min_update_interval, "session": session}
            )

    monkeypatch.setitem(
        sys.modules,
        "airflow.models.serialized_dag",
        SimpleNamespace(SerializedDagModel=FakeSerializedDagModel),
    )
    record = _record(session="session-token")

    authoring_dag: Any = "authoring-dag"
    dag_module._write_serialized_dag(authoring_dag, record)

    assert calls == [
        {"dag": "authoring-dag", "min_update_interval": 0, "session": "session-token"}
    ]


def test_create_dag_run_uses_the_execution_date_interface(
    v2_capabilities: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Create the run with `execution_date`, no `triggered_by`, and explicit integrity."""

    assert v2_capabilities.timezone_location is TimezoneLocation.UTILS
    fixed_now = object()
    monkeypatch.setitem(
        sys.modules,
        "airflow.utils.timezone",
        SimpleNamespace(
            utcnow=lambda: fixed_now,
            coerce_datetime=lambda value: value,
            convert_to_utc=lambda value: value,
        ),
    )

    created: dict[str, Any] = {}

    class FakeDagRun:
        """Record integrity verification and expose owned identities."""

        id = 77

        def verify_integrity(self, session: Any = None) -> None:
            created["verified_session"] = session

        def get_task_instances(self, session: Any = None) -> list[Any]:
            del session
            return []

    class FakeSchedulerDag:
        """Record the 2.x `create_dagrun` call shape."""

        timetable = SimpleNamespace(
            infer_manual_data_interval=lambda run_after: ("interval", run_after)
        )

        def create_dagrun(self, **kwargs: Any) -> FakeDagRun:
            created["kwargs"] = kwargs
            return FakeDagRun()

    class FakeSession:
        """Record transaction outcomes."""

        committed = False

        def commit(self) -> None:
            self.committed = True

        def rollback(self) -> None:
            raise AssertionError("rollback must not run on success")

    session = FakeSession()
    record = _record(session=session)

    scheduler_dag: Any = FakeSchedulerDag()
    authoring_stub: Any = SimpleNamespace(get_task_instance=None)
    dag_run = dag_module.create_dag_run(
        scheduler_dag,
        authoring_stub,
        record,
        run_id="run-1",
        logical_date=None,
        run_after=None,
        start_date=None,
        dag_run_kwargs={},
    )

    kwargs = created["kwargs"]
    assert kwargs["execution_date"] is fixed_now
    assert "logical_date" not in kwargs
    assert "run_after" not in kwargs
    assert "triggered_by" not in kwargs
    assert kwargs["data_interval"] == ("interval", fixed_now)
    assert created["verified_session"] is session
    assert session.committed is True
    assert record.dag_run_ids == {77}
    assert dag_run.id == 77


@pytest.mark.usefixtures("v2_capabilities")
def test_expand_mapped_task_uses_the_operator_method_on_v2() -> None:
    """Expand through the 2.x mapped operator's own method."""

    calls: list[tuple[str, Any]] = []

    class FakeSession:
        """Record the commit closing the expansion."""

        committed = False

        def commit(self) -> None:
            self.committed = True

    class FakeMappedTask:
        """Expose the 2.x expansion entry point."""

        is_mapped = True

        def expand_mapped_task(self, run_id: str, session: Any = None) -> None:
            calls.append((run_id, session))

    session = FakeSession()

    mapped_task: Any = FakeMappedTask()
    fake_session: Any = session
    dag_module.expand_mapped_task_instances(mapped_task, "run-1", fake_session)

    assert calls == [("run-1", session)]
    assert session.committed is True


@pytest.mark.usefixtures("v2_capabilities")
def test_cleanup_dag_deletes_the_v2_row_set() -> None:
    """Delete runs, serialized rows, and the Dag model without version tables."""

    class FakeSession:
        """Record ORM and core delete traffic."""

        def __init__(self) -> None:
            self.deleted: list[Any] = []
            self.executed: list[str] = []
            self.committed = False

        def rollback(self) -> None:
            return None

        def get(self, model: Any, key: Any) -> Any:
            return f"{getattr(model, '__name__', model)}:{key}"

        def delete(self, row: Any) -> None:
            self.deleted.append(row)

        def flush(self) -> None:
            return None

        def execute(self, statement: Any) -> None:
            self.executed.append(str(statement))

        def commit(self) -> None:
            self.committed = True

    session = FakeSession()
    record = _record(session=session)
    record.dag_run_ids.add(5)

    dag_module._cleanup_dag(record)

    assert session.deleted[0].endswith(":5")
    assert any("serialized_dag" in statement for statement in session.executed)
    assert session.deleted[-1].endswith(":fake_dag")
    assert session.committed is True


@pytest.mark.usefixtures("v2_capabilities")
def test_cleanup_dag_tolerates_an_absent_dag_model() -> None:
    """Commit the v2 cleanup even when the `DagModel` row is already gone."""

    class FakeSession:
        """Report every metadata row as absent."""

        def __init__(self) -> None:
            self.deleted: list[Any] = []
            self.executed: list[str] = []
            self.committed = False

        def rollback(self) -> None:
            return None

        def get(self, model: Any, key: Any) -> None:
            del model, key
            return

        def delete(self, row: Any) -> None:
            self.deleted.append(row)

        def flush(self) -> None:
            return None

        def execute(self, statement: Any) -> None:
            self.executed.append(str(statement))

        def commit(self) -> None:
            self.committed = True

    session = FakeSession()
    record = _record(session=session)

    dag_module._cleanup_dag(record)

    assert session.deleted == []
    assert any("serialized_dag" in statement for statement in session.executed)
    assert session.committed is True


def test_register_v2_orm_models_falls_back_and_tolerates_absence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Try the provider path first, the legacy path second, and give up quietly."""

    attempts: list[str] = []

    def fake_import(name: str) -> Any:
        attempts.append(name)
        if name.endswith("fab_security.sqla.models"):
            return SimpleNamespace()
        raise ImportError(name)

    monkeypatch.setattr(dag_module, "import_module", fake_import)
    dag_module._register_v2_orm_models()
    assert attempts == [
        "airflow.providers.fab.auth_manager.models",
        "airflow.www.fab_security.sqla.models",
    ]

    attempts.clear()
    monkeypatch.setattr(
        dag_module, "import_module", lambda name: (_ for _ in ()).throw(ImportError(name))
    )
    dag_module._register_v2_orm_models()


def test_is_v2_reports_the_v3_family(monkeypatch: pytest.MonkeyPatch) -> None:
    """Report False when the resolved contract is the 3.x family."""

    capabilities = replace(_CERTIFIED_CAPABILITIES[(3, 3, 0)])
    monkeypatch.setattr(dag_module, "resolve_capabilities", lambda: capabilities)

    assert capabilities.family is AirflowFamily.V3
    assert dag_module._is_v2() is False


@pytest.mark.usefixtures("v2_capabilities")
def test_open_dag_session_registers_v2_orm_models(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Register the FAB tables before handing out a 2.x metadata session."""

    registered: list[bool] = []
    monkeypatch.setattr(dag_module, "_register_v2_orm_models", lambda: registered.append(True))

    class FakeSessionFactory:
        """Expose the nested factory shape `airflow.settings.Session` carries."""

        @staticmethod
        def session_factory() -> str:
            return "session-token"

    monkeypatch.setitem(
        sys.modules,
        "airflow",
        SimpleNamespace(settings=SimpleNamespace(Session=FakeSessionFactory)),
    )

    session = dag_module.open_dag_session("fake_dag")

    assert session == "session-token"
    assert registered == [True]
