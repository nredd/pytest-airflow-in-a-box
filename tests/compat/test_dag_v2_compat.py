"""Probe-double coverage for the Airflow 2.x branches of `_compat.dag`.

The 2.x branches are unreachable from a 3.x install, so these tests hold the
100% gate with fake capability contracts and fake `airflow` modules -- the real
behavior is certified end to end by the 2.x compat CI legs.
"""

from __future__ import annotations

import sys
from dataclasses import replace
from datetime import datetime, timezone
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


@pytest.fixture
def v2_7_capabilities(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Pin `_compat.dag` to the certified 2.7.3 contract.

    2.7.3 is the one certified release whose `DAG.add_task` rejects a Dag with no
    `start_date` on it or its tasks, scheduled or not.

    Parameters:
        monkeypatch: pytest.MonkeyPatch replacing the resolver.

    Returns:
        AirflowCapabilities containing the certified 2.7.3 contract.
    """

    capabilities = _CERTIFIED_CAPABILITIES[(2, 7, 3)]
    monkeypatch.setattr(dag_module, "resolve_capabilities", lambda: capabilities)
    return capabilities


@pytest.mark.usefixtures("v2_7_capabilities")
def test_build_dag_supplies_a_start_date_below_2_8(monkeypatch: pytest.MonkeyPatch) -> None:
    """Inject the implicit `start_date` on the releases that demand one.

    Parameters:
        monkeypatch: pytest.MonkeyPatch installing the fake authoring class.
    """

    constructed: dict[str, Any] = {}

    class FakeDag:
        """Record 2.x authoring construction."""

        def __init__(self, dag_id: str, schedule: Any = None, **kwargs: Any) -> None:
            constructed.update({"dag_id": dag_id, "schedule": schedule, **kwargs})

    monkeypatch.setitem(sys.modules, "airflow.models.dag", SimpleNamespace(DAG=FakeDag))

    dag_module.build_dag("fake_dag", "/suite/test_module.py", {})

    assert constructed["start_date"] == dag_module.IMPLICIT_V2_START_DATE


@pytest.mark.parametrize(
    ("release", "schedule", "dag_kwargs", "expected"),
    [
        ((2, 7, 3), None, {}, True),
        # A scheduled Dag without a `start_date` is a real authoring error on every
        # certified release, so the shim leaves it to raise.
        ((2, 7, 3), "@daily", {}, False),
        ((2, 7, 3), None, {"start_date": datetime(2024, 1, 1, tzinfo=timezone.utc)}, False),
        (
            (2, 7, 3),
            None,
            {"default_args": {"start_date": datetime(2024, 1, 1, tzinfo=timezone.utc)}},
            False,
        ),
        ((2, 7, 3), None, {"default_args": {}}, True),
        ((2, 8, 4), None, {}, False),
        ((2, 11, 2), None, {}, False),
        ((3, 3, 1), None, {}, False),
    ],
)
def test_needs_implicit_start_date_matches_the_2_8_rule(
    monkeypatch: pytest.MonkeyPatch,
    release: tuple[int, int, int],
    schedule: Any,
    dag_kwargs: dict[str, Any],
    expected: bool,
) -> None:
    """Inject only where Airflow 2.8 stopped rejecting a `start_date`-free Dag.

    Parameters:
        monkeypatch: pytest.MonkeyPatch replacing the resolver.
        release: tuple[int, int, int] selecting the certified contract to pin.
        schedule: Any standing in for the popped scheduling argument.
        dag_kwargs: dict[str, Any] forwarded to the authoring constructor.
        expected: bool expected from the probe.
    """

    capabilities = _CERTIFIED_CAPABILITIES[release]
    monkeypatch.setattr(dag_module, "resolve_capabilities", lambda: capabilities)

    assert dag_module._needs_implicit_start_date(schedule, dag_kwargs) is expected


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
    """Create the run with `execution_date`, no `triggered_by`, and no second integrity pass.

    The fake mirrors real 2.x behavior: `DAG.create_dagrun` itself ends with
    `run.verify_integrity(session=...)`, so the plugin must NOT repeat the pass -- the
    fake raises if anything calls `verify_integrity` again after creation returns.
    """

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
        """Reject repeated integrity verification and expose owned identities."""

        id = 77

        def verify_integrity(self, session: Any = None) -> None:
            del session
            raise AssertionError(
                "2.x `DAG.create_dagrun` already verified integrity; the plugin must "
                "not run a second pass"
            )

        def get_task_instances(self, session: Any = None) -> list[Any]:
            del session
            return []

    class FakeSchedulerDag:
        """Record the 2.x `create_dagrun` call shape, verifying like real 2.x."""

        timetable = SimpleNamespace(
            infer_manual_data_interval=lambda run_after: ("interval", run_after)
        )

        def create_dagrun(self, **kwargs: Any) -> FakeDagRun:
            created["kwargs"] = kwargs
            # Real 2.x verifies inside `create_dagrun`; record it the same way so the
            # test also proves the session the run would verify against.
            created["verified_session"] = kwargs.get("session")
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
def test_create_dag_run_rejects_run_after_on_v2() -> None:
    """Refuse the 3.x-only `run_after` kwarg instead of silently ignoring it."""

    record = _record(session=None)
    scheduler_dag: Any = SimpleNamespace()
    authoring_dag: Any = SimpleNamespace()

    with pytest.raises(ValueError, match=r"no 2\.x equivalent"):
        dag_module.create_dag_run(
            scheduler_dag,
            authoring_dag,
            record,
            run_id="run-1",
            logical_date=None,
            run_after=datetime(2021, 5, 5, tzinfo=timezone.utc),
            start_date=None,
            dag_run_kwargs={},
        )


@pytest.mark.usefixtures("v2_capabilities")
def test_expand_mapped_task_uses_the_operator_method_on_v2(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Detect 2.x mapped operators by class and expand through their own method.

    The 2.x `MappedOperator` carries no `is_mapped` attribute, so the fake must
    pass the real `isinstance` probe -- an attribute-only fake would paper over
    the detection path this test exists to hold.
    """

    calls: list[tuple[str, Any]] = []

    class FakeSession:
        """Record the commit closing the expansion."""

        committed = False

        def commit(self) -> None:
            self.committed = True

    class FakeMappedOperator:
        """Expose the 2.x expansion entry point without `is_mapped`."""

        def expand_mapped_task(self, run_id: str, session: Any = None) -> None:
            calls.append((run_id, session))

    monkeypatch.setitem(
        sys.modules,
        "airflow.models.mappedoperator",
        SimpleNamespace(MappedOperator=FakeMappedOperator),
    )
    session = FakeSession()

    mapped_task: Any = FakeMappedOperator()
    fake_session: Any = session
    dag_module.expand_mapped_task_instances(mapped_task, "run-1", fake_session)

    assert calls == [("run-1", session)]
    assert session.committed is True

    calls.clear()
    unmapped_task: Any = SimpleNamespace(is_mapped=True)
    dag_module.expand_mapped_task_instances(unmapped_task, "run-1", fake_session)

    assert calls == []


class _FakeV2CleanupSession:
    """Record ORM and core delete traffic for v2 `_cleanup_dag` probes.

    Parameters:
        fileloc: str | None reported as the Dag model's source location.
        fileloc_still_used: bool reporting another Dag model sharing the fileloc.
    """

    def __init__(self, fileloc: str | None, fileloc_still_used: bool) -> None:
        self.fileloc = fileloc
        self.fileloc_still_used = fileloc_still_used
        self.deleted: list[Any] = []
        self.executed: list[str] = []
        self.committed = False

    def rollback(self) -> None:
        return None

    def get(self, model: Any, key: Any) -> Any:
        name = getattr(model, "__name__", str(model))
        if name == "DagModel":
            return SimpleNamespace(kind=f"{name}:{key}", fileloc=self.fileloc)
        return f"{name}:{key}"

    def delete(self, row: Any) -> None:
        self.deleted.append(row)

    def flush(self) -> None:
        return None

    def execute(self, statement: Any) -> None:
        self.executed.append(str(statement))

    def scalars(self, statement: Any) -> Any:
        del statement
        return SimpleNamespace(first=lambda: "other_dag" if self.fileloc_still_used else None)

    def commit(self) -> None:
        self.committed = True


@pytest.mark.usefixtures("v2_capabilities")
def test_cleanup_dag_deletes_the_v2_row_set() -> None:
    """Delete runs, serialized rows, the Dag model, and the orphaned code row."""

    session = _FakeV2CleanupSession(fileloc="/suite/test_module.py", fileloc_still_used=False)
    record = _record(session=session)
    record.dag_run_ids.add(5)

    dag_module._cleanup_dag(record)

    assert session.deleted[0].endswith(":5")
    assert any("serialized_dag" in statement for statement in session.executed)
    assert session.deleted[-1].kind.endswith(":fake_dag")
    assert any("dag_code" in statement for statement in session.executed)
    assert session.committed is True


@pytest.mark.usefixtures("v2_capabilities")
def test_cleanup_dag_keeps_a_shared_v2_code_row() -> None:
    """Leave the `dag_code` row alone while another Dag still shares the fileloc."""

    session = _FakeV2CleanupSession(fileloc="/suite/test_module.py", fileloc_still_used=True)
    record = _record(session=session)

    dag_module._cleanup_dag(record)

    assert not any("dag_code" in statement for statement in session.executed)
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


def _fab_import_probe(monkeypatch: pytest.MonkeyPatch, importable: frozenset[str]) -> list[str]:
    """Replace `_compat.dag`'s importer with one resolving only the named modules.

    Parameters:
        monkeypatch: pytest.MonkeyPatch replacing `import_module`.
        importable: frozenset[str] naming the module paths the probe resolves.

    Returns:
        list[str] recording every attempted module path, in order.
    """

    attempts: list[str] = []

    def fake_import(name: str) -> Any:
        """Record the attempt, then resolve or raise like `importlib.import_module`."""

        attempts.append(name)
        if name not in importable:
            raise ModuleNotFoundError(f"No module named '{name}'", name=name)
        return SimpleNamespace()

    monkeypatch.setattr(dag_module, "import_module", fake_import)
    return attempts


def test_register_v2_orm_models_imports_the_fab_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stop at the provider models on 2.9 and later, never probing the in-tree path.

    Parameters:
        monkeypatch: pytest.MonkeyPatch replacing `import_module`.
    """

    attempts = _fab_import_probe(
        monkeypatch, frozenset({"airflow.providers.fab.auth_manager.models"})
    )

    dag_module._register_v2_orm_models()

    assert attempts == ["airflow.providers.fab.auth_manager.models"]


def test_register_v2_orm_models_falls_back_to_the_in_tree_models(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Register the in-tree FAB models on 2.7/2.8, where the provider does not exist.

    FAB's auth-manager models were extracted into `apache-airflow-providers-fab` for
    2.9; below that they live at `airflow.auth.managers.fab.models` and the provider is
    absent from those releases' constraints files entirely, so a provider-only import
    left the shim dead on exactly the releases it was silent about.

    Parameters:
        monkeypatch: pytest.MonkeyPatch replacing `import_module`.
        caplog: pytest.LogCaptureFixture proving the fallback is not the failure path.
    """

    attempts = _fab_import_probe(monkeypatch, frozenset({"airflow.auth.managers.fab.models"}))

    with caplog.at_level("INFO", logger=dag_module.__name__):
        dag_module._register_v2_orm_models()

    assert attempts == [
        "airflow.providers.fab.auth_manager.models",
        "airflow.auth.managers.fab.models",
    ]
    assert caplog.messages == []


def test_register_v2_orm_models_logs_a_stripped_install(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Name both missing FAB models modules instead of failing the fixture.

    Every certified 2.x release carries the models at one of the two locations, so this
    only fires on a hand-stripped install -- the log line is the diagnostic breadcrumb
    for the mapper failure that follows.

    Parameters:
        monkeypatch: pytest.MonkeyPatch replacing `import_module`.
        caplog: pytest.LogCaptureFixture capturing the INFO diagnostic.
    """

    attempts = _fab_import_probe(monkeypatch, frozenset())

    with caplog.at_level("INFO", logger=dag_module.__name__):
        dag_module._register_v2_orm_models()

    assert attempts == list(dag_module._V2_FAB_MODEL_MODULES)
    assert any("`ab_user`" in message for message in caplog.messages)
    assert any("airflow.auth.managers.fab.models" in message for message in caplog.messages)


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
