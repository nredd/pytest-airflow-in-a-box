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
from sqlalchemy.exc import IntegrityError

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
        # certified release, so the shim leaves it to raise -- including through the
        # deprecated spellings 2.x's own scheduling check counts.
        ((2, 7, 3), "@daily", {}, False),
        ((2, 7, 3), None, {"schedule_interval": "@daily"}, False),
        ((2, 7, 3), None, {"timetable": object()}, False),
        ((2, 7, 3), None, {"schedule_interval": None, "timetable": None}, True),
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


class _RollbackRecordingSession:
    """Count `rollback` and `commit` calls and expose an empty pending-state surface."""

    def __init__(self) -> None:
        """Initialize the lifecycle counters and the empty identity sets."""

        self.rollbacks = 0
        self.commits = 0
        self.new: set[Any] = set()
        self.dirty: set[Any] = set()
        self.deleted: set[Any] = set()

    def rollback(self) -> None:
        """Record one rollback."""

        self.rollbacks += 1

    def commit(self) -> None:
        """Record one commit."""

        self.commits += 1

    def expire_all(self) -> None:
        """Accept the identity-map expiry `resync_dag` issues before reloading."""


def _fake_authoring_dag(failures: list[IntegrityError]) -> tuple[Any, list[tuple[Any, ...]]]:
    """Build a 2.x authoring double raising each queued failure before succeeding.

    Centralizes the 2.x `bulk_write_to_db(dags, session=None)` compat signature so a
    shift in that call shape needs exactly one edit here.

    Parameters:
        failures: list[IntegrityError] consumed one per write call before success.

    Returns:
        tuple[Any, list[tuple[Any, ...]]] containing the Dag double and its call log.
    """

    calls: list[tuple[Any, ...]] = []

    class FakeAuthoringDag:
        """Expose the 2.x class-level writer."""

        @classmethod
        def bulk_write_to_db(cls, dags: list[Any], session: Any = None) -> None:
            calls.append((tuple(dags), session))
            if failures:
                raise failures.pop(0)

    return FakeAuthoringDag(), calls


def _dag_code_integrity_error() -> IntegrityError:
    """Build the UNIQUE-violation shape issue #157's CI leg observed.

    Returns:
        sqlalchemy.exc.IntegrityError carrying a `dag_code.fileloc_hash` cause.
    """

    return IntegrityError(
        "INSERT INTO dag_code",
        None,
        Exception("UNIQUE constraint failed: dag_code.fileloc_hash"),
    )


@pytest.mark.usefixtures("v2_capabilities")
def test_sync_dag_model_uses_the_authoring_writer_on_v2() -> None:
    """Write through the authoring class's bundle-free `bulk_write_to_db`."""

    dag, calls = _fake_authoring_dag([])
    session = _RollbackRecordingSession()

    dag_module._sync_dag_model(dag, _record(session=session))

    assert calls == [((dag,), session)]
    assert session.rollbacks == 0


@pytest.mark.usefixtures("v2_capabilities")
def test_sync_dag_model_ignores_bundle_version_on_v2() -> None:
    """Keep the bundle-free 2.x writer shape when a bundle version is recorded."""

    dag, calls = _fake_authoring_dag([])
    session = _RollbackRecordingSession()
    record = _record(session=session)
    record.bundle_version = "v238"

    dag_module._sync_dag_model(dag, record)

    assert calls == [((dag,), session)]


@pytest.mark.usefixtures("v2_capabilities")
def test_sync_dag_model_retries_a_concurrent_dag_code_insert(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Roll back and retry once another worker wins the `dag_code` insert race.

    Parameters:
        caplog: pytest.LogCaptureFixture capturing the retry diagnostic.
    """

    dag, calls = _fake_authoring_dag([_dag_code_integrity_error()])
    session = _RollbackRecordingSession()

    with caplog.at_level("WARNING", logger=dag_module.__name__):
        dag_module._sync_dag_model(dag, _record(session=session))

    assert calls == [((dag,), session), ((dag,), session)]
    assert session.rollbacks == 1
    assert any("attempt 1" in message for message in caplog.messages)


@pytest.mark.usefixtures("v2_capabilities")
def test_sync_dag_model_raises_after_exhausted_retries() -> None:
    """Surface the `IntegrityError` once every bounded attempt loses the race."""

    attempts = dag_module._V2_DAG_CODE_SYNC_ATTEMPTS
    dag, calls = _fake_authoring_dag([_dag_code_integrity_error() for _ in range(attempts)])
    session = _RollbackRecordingSession()

    with pytest.raises(IntegrityError, match="fileloc_hash"):
        dag_module._sync_dag_model(dag, _record(session=session))

    assert len(calls) == attempts
    # The final attempt raises without a rollback; `persist_dag`'s handler owns it.
    assert session.rollbacks == attempts - 1


@pytest.mark.usefixtures("v2_capabilities")
def test_sync_dag_model_does_not_retry_a_foreign_integrity_error() -> None:
    """Keep a non-`dag_code` violation loud instead of absorbing it as the race.

    A cross-worker `dag_id` collision that slips past `ensure_dag_registrable`'s
    non-locking check must fail the test, not silently take the update path over
    another worker's committed rows.
    """

    collision = IntegrityError(
        "INSERT INTO dag", None, Exception("UNIQUE constraint failed: dag.dag_id")
    )
    dag, calls = _fake_authoring_dag([collision])
    session = _RollbackRecordingSession()

    with pytest.raises(IntegrityError, match=r"dag\.dag_id"):
        dag_module._sync_dag_model(dag, _record(session=session))

    assert len(calls) == 1
    assert session.rollbacks == 0


@pytest.mark.usefixtures("v2_capabilities")
def test_sync_dag_model_does_not_retry_over_pending_user_state() -> None:
    """Never roll back user work staged on the published `dag_maker.session`.

    The session is handed to the test body before persistence runs, so a retry's
    rollback would silently discard rows the user staged inside the `with` block and
    the eventual commit would drop them without a trace.
    """

    dag, calls = _fake_authoring_dag([_dag_code_integrity_error()])
    session = _RollbackRecordingSession()
    session.new = {"staged-user-connection"}

    with pytest.raises(IntegrityError, match="fileloc_hash"):
        dag_module._sync_dag_model(dag, _record(session=session))

    assert len(calls) == 1
    assert session.rollbacks == 0


@pytest.mark.usefixtures("v2_capabilities")
def test_persist_dag_reports_exhausted_dag_code_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report the exhausted race through the existing sync-labelled persistence error.

    Parameters:
        monkeypatch: pytest.MonkeyPatch disarming the cleanup path.
    """

    attempts = dag_module._V2_DAG_CODE_SYNC_ATTEMPTS
    dag, _calls = _fake_authoring_dag([_dag_code_integrity_error() for _ in range(attempts)])
    monkeypatch.setattr(dag_module, "_cleanup_dag", lambda _record: None)
    session = _RollbackRecordingSession()

    with pytest.raises(
        dag_module.DagPersistenceError, match="syncing DagModel metadata"
    ) as caught:
        dag_module.persist_dag(dag, _record(session=session))

    assert isinstance(caught.value.__cause__, IntegrityError)


@pytest.mark.usefixtures("v2_capabilities")
def test_resync_dag_routes_through_the_v2_writers(monkeypatch: pytest.MonkeyPatch) -> None:
    """Re-persist through the bundle-free 2.x writer pair and reload the result."""

    dag, sync_calls = _fake_authoring_dag([])
    write_calls: list[dict[str, Any]] = []
    reloaded: Any = object()

    class FakeSerializedDagModel:
        """Record 2.x `write_dag` calls and serve the reloaded scheduler Dag."""

        @staticmethod
        def write_dag(dag: Any, min_update_interval: int, session: Any) -> None:
            write_calls.append(
                {"dag": dag, "min_update_interval": min_update_interval, "session": session}
            )

        @staticmethod
        def get_dag(dag_id: str, session: Any) -> Any:
            del dag_id, session
            return reloaded

    monkeypatch.setitem(
        sys.modules,
        "airflow.models.serialized_dag",
        SimpleNamespace(SerializedDagModel=FakeSerializedDagModel),
    )
    registered: list[tuple[str, Any]] = []
    monkeypatch.setattr(
        dag_module, "register_authoring_dag", lambda dag_id, dag: registered.append((dag_id, dag))
    )
    session = _RollbackRecordingSession()
    record = _record(session=session)

    result = dag_module.resync_dag(dag, record)

    assert result is reloaded
    assert sync_calls == [((dag,), session)]
    assert write_calls == [{"dag": dag, "min_update_interval": 0, "session": session}]
    assert session.commits == 1
    assert session.rollbacks == 0
    assert registered == [("fake_dag", dag)]


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
        # The relationship attribute `create_dag_run` snapshots for the refresh loop,
        # mirroring the real 2.x `DagRun.task_instances` relationship.
        task_instances: tuple[Any, ...] = ()

        def verify_integrity(self, session: Any = None) -> None:
            del session
            raise AssertionError(
                "2.x `DAG.create_dagrun` already verified integrity; the plugin must "
                "not run a second pass"
            )

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
        logical_date=dag_module.UNSET,
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


def _run_creation_fakes(
    monkeypatch: pytest.MonkeyPatch,
    fixed_now: Any,
) -> tuple[dict[str, Any], Any]:
    """Install the timezone fake and build recording run-creation doubles.

    Parameters:
        monkeypatch: pytest.MonkeyPatch replacing the 2.x timezone module.
        fixed_now: Any returned by the fake ``utcnow``.

    Returns:
        tuple[dict[str, Any], Any] containing the recorded call sink and the
        persistence record around a recording fake session.
    """

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
    session = SimpleNamespace(
        commit=lambda: created.__setitem__("committed", True),
        rollback=lambda: pytest.fail("rollback must not run on success"),
    )
    return created, _record(session=session)


@pytest.mark.usefixtures("v2_capabilities")
def test_create_dag_run_v2_defaults_run_id_and_logical_date(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Default a bare 2.x run to upstream's `test` id and the Dag's start date.

    docs/adr/0003: with neither `run_id` nor `run_type` passed, the run id is the
    fixed `test` and `execution_date` is `default_logical_date` -- `dag_maker`'s
    resolved Dag `start_date` -- rather than the current UTC date.

    Parameters:
        monkeypatch: pytest.MonkeyPatch replacing the 2.x timezone module.
    """

    fixed_now = object()
    created, record = _run_creation_fakes(monkeypatch, fixed_now)
    default_date = datetime(2016, 1, 1, tzinfo=timezone.utc)

    dag_run_stub = SimpleNamespace(id=81, task_instances=())

    def fake_create_dagrun(**kwargs: Any) -> Any:
        """Record the exact kwargs the 2.x scheduler Dag receives."""

        created["kwargs"] = kwargs
        return dag_run_stub

    scheduler_dag: Any = SimpleNamespace(
        timetable=SimpleNamespace(
            infer_manual_data_interval=lambda run_after: ("interval", run_after)
        ),
        create_dagrun=fake_create_dagrun,
    )
    authoring_stub: Any = SimpleNamespace()

    dag_run = dag_module.create_dag_run(
        scheduler_dag,
        authoring_stub,
        record,
        run_id=None,
        logical_date=dag_module.UNSET,
        run_after=None,
        start_date=None,
        dag_run_kwargs={},
        default_logical_date=default_date,
        default_start_date=default_date,
    )

    kwargs = created["kwargs"]
    assert kwargs["run_id"] == dag_module.DEFAULT_RUN_ID
    assert kwargs["execution_date"] is default_date
    assert kwargs["start_date"] is default_date
    assert kwargs["data_interval"] == ("interval", default_date)
    assert dag_run.id == 81


@pytest.mark.usefixtures("v2_capabilities")
def test_create_dag_run_v2_derives_non_manual_defaults_from_the_timetable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Route a 2.x non-manual run through the 2.x scheduling seams.

    The family-specific spellings at once: `next_dagrun_info` takes 2.x's
    `last_automated_dagrun`, the interval comes from the run info it returned, and
    `generate_run_id` takes 2.x's `logical_date` keyword.

    Parameters:
        monkeypatch: pytest.MonkeyPatch replacing the 2.x timezone module.
    """

    from airflow.utils.types import DagRunType

    fixed_now = object()
    created, record = _run_creation_fakes(monkeypatch, fixed_now)
    automated_date = datetime(2016, 2, 3, tzinfo=timezone.utc)

    dag_run_stub = SimpleNamespace(id=82, task_instances=())

    def fake_next_dagrun_info(**kwargs: Any) -> Any:
        """Record the release-specific keyword and yield the automated run info."""

        created["next_kwargs"] = kwargs
        return SimpleNamespace(
            logical_date=automated_date,
            data_interval=("automated", automated_date),
        )

    def fake_generate_run_id(**kwargs: Any) -> str:
        """Record the 2.x `generate_run_id` keyword spelling."""

        created["generate_kwargs"] = kwargs
        return "scheduled__generated"

    scheduler_dag: Any = SimpleNamespace(
        timetable=SimpleNamespace(
            infer_manual_data_interval=lambda _run_after: pytest.fail(
                "a non-manual run must use the timetable's own run-info interval"
            ),
            generate_run_id=fake_generate_run_id,
        ),
        next_dagrun_info=fake_next_dagrun_info,
        create_dagrun=lambda **kwargs: created.__setitem__("kwargs", kwargs) or dag_run_stub,
    )
    authoring_stub: Any = SimpleNamespace()

    dag_module.create_dag_run(
        scheduler_dag,
        authoring_stub,
        record,
        run_id=None,
        logical_date=dag_module.UNSET,
        run_after=None,
        start_date=None,
        dag_run_kwargs={"run_type": "scheduled"},
        default_logical_date=None,
        default_start_date=None,
        upstream_defaults=True,
    )

    assert created["next_kwargs"] == {"last_automated_dagrun": None}
    kwargs = created["kwargs"]
    assert kwargs["execution_date"] is automated_date
    assert kwargs["data_interval"] == ("automated", automated_date)
    assert kwargs["run_id"] == "scheduled__generated"
    assert kwargs["run_type"] is DagRunType.SCHEDULED
    assert created["generate_kwargs"] == {
        "run_type": DagRunType.SCHEDULED,
        "logical_date": automated_date,
        "data_interval": ("automated", automated_date),
    }


@pytest.mark.usefixtures("v2_capabilities")
def test_create_dag_run_v2_non_manual_falls_back_to_now_without_a_schedule(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Degrade to the current UTC date when the timetable schedules nothing.

    Parameters:
        monkeypatch: pytest.MonkeyPatch replacing the 2.x timezone module.
    """

    fixed_now = datetime(2026, 8, 24, tzinfo=timezone.utc)
    created, record = _run_creation_fakes(monkeypatch, fixed_now)

    dag_run_stub = SimpleNamespace(id=83, task_instances=())
    scheduler_dag: Any = SimpleNamespace(
        timetable=SimpleNamespace(
            generate_run_id=lambda **_kwargs: "scheduled__fallback",
        ),
        next_dagrun_info=lambda **_kwargs: None,
        infer_automated_data_interval=lambda logical_date: ("automated", logical_date),
        create_dagrun=lambda **kwargs: created.__setitem__("kwargs", kwargs) or dag_run_stub,
    )
    authoring_stub: Any = SimpleNamespace()

    dag_module.create_dag_run(
        scheduler_dag,
        authoring_stub,
        record,
        run_id=None,
        logical_date=dag_module.UNSET,
        run_after=None,
        start_date=None,
        dag_run_kwargs={"run_type": "scheduled"},
        default_logical_date=None,
        default_start_date=None,
        upstream_defaults=True,
    )

    kwargs = created["kwargs"]
    assert kwargs["execution_date"] is fixed_now
    assert kwargs["data_interval"] == ("automated", fixed_now)
    assert kwargs["run_id"] == "scheduled__fallback"


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
@pytest.mark.parametrize("key", ["run_id", "start_date", "session", "execution_date"])
def test_create_dag_run_rejects_reserved_dag_run_kwargs_on_v2(key: str) -> None:
    """Refuse a `dag_run_kwargs` entry that would double-pass a keyword to Airflow."""

    record = _record(session=None)
    scheduler_dag: Any = SimpleNamespace()
    authoring_dag: Any = SimpleNamespace()

    with pytest.raises(ValueError, match=r"`dag_run_kwargs` cannot set"):
        dag_module.create_dag_run(
            scheduler_dag,
            authoring_dag,
            record,
            run_id="run-1",
            logical_date=dag_module.UNSET,
            run_after=None,
            start_date=None,
            dag_run_kwargs={key: object()},
        )


@pytest.mark.usefixtures("v2_capabilities")
def test_create_dag_run_rejects_execution_date_with_a_logical_date_remedy() -> None:
    """Point a caller-supplied `execution_date` at `logical_date`, not a nonexistent keyword.

    `execution_date` is not a public keyword anywhere in this plugin's API -- its
    2.x-era name for the 3.x-spelled `logical_date`. A generic "use the dedicated
    keyword argument instead" remedy would send a caller looking for something that
    does not exist.
    """

    record = _record(session=None)
    scheduler_dag: Any = SimpleNamespace()
    authoring_dag: Any = SimpleNamespace()

    with pytest.raises(ValueError, match=r"pass `logical_date` instead") as caught:
        dag_module.create_dag_run(
            scheduler_dag,
            authoring_dag,
            record,
            run_id="run-1",
            logical_date=dag_module.UNSET,
            run_after=None,
            start_date=None,
            dag_run_kwargs={"execution_date": object()},
        )

    assert "dedicated keyword argument" not in str(caught.value)


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
    """Record ORM and core delete traffic for v2 `_cleanup_dag` probes."""

    def __init__(self) -> None:
        self.deleted: list[Any] = []
        self.executed: list[str] = []
        self.committed = False

    def rollback(self) -> None:
        return None

    def get(self, model: Any, key: Any) -> Any:
        name = getattr(model, "__name__", str(model))
        if name == "DagModel":
            return SimpleNamespace(kind=f"{name}:{key}")
        return f"{name}:{key}"

    def delete(self, row: Any) -> None:
        self.deleted.append(row)

    def flush(self) -> None:
        return None

    def execute(self, statement: Any) -> None:
        self.executed.append(str(statement))

    def commit(self) -> None:
        self.committed = True


@pytest.mark.usefixtures("v2_capabilities")
def test_cleanup_dag_deletes_the_v2_row_set() -> None:
    """Delete runs, serialized rows, and the Dag model, keeping the shared code row.

    The `dag_code` row keys on `fileloc`, shared across xdist workers testing one
    source file; deleting it would re-arm issue #157's insert race, so cleanup must
    leave it alone.
    """

    session = _FakeV2CleanupSession()
    record = _record(session=session)
    record.dag_run_ids.add(5)

    dag_module._cleanup_dag(record)

    assert session.deleted[0].endswith(":5")
    assert any("serialized_dag" in statement for statement in session.executed)
    assert session.deleted[-1].kind.endswith(":fake_dag")
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


@pytest.mark.usefixtures("v2_capabilities")
def test_purge_dag_metadata_deletes_the_v2_row_set() -> None:
    """Purge a leaked v2 Dag's runs, serialized rows, and model, keeping `dag_code`.

    The 2.x family has no backfill or `DagVersion` rows, and the `dag_code` row --
    keyed on `fileloc`, shared across xdist workers testing one source file -- must
    stay to avoid re-arming issue #157's insert race, matching `_cleanup_dag`.
    """

    class FakeSession:
        """Record ORM and core delete traffic for the v2 purge probe."""

        def __init__(self) -> None:
            self.deleted: list[Any] = []
            self.executed: list[str] = []
            self.committed = False

        def scalars(self, statement: Any) -> list[Any]:
            del statement
            return [SimpleNamespace(id=5, kind="DagRun:5")]

        def delete(self, row: Any) -> None:
            self.deleted.append(row)

        def flush(self) -> None:
            return None

        def execute(self, statement: Any) -> None:
            self.executed.append(str(statement))

        def commit(self) -> None:
            self.committed = True

    session: Any = FakeSession()
    dag_model = SimpleNamespace(kind="DagModel:fake_dag")

    dag_module._purge_dag_metadata("fake_dag", session, dag_model)

    assert session.deleted[0].kind == "DagRun:5"
    assert session.deleted[-1] is dag_model
    assert any("serialized_dag" in statement for statement in session.executed)
    assert not any("dag_code" in statement for statement in session.executed)
    assert not any("backfill" in statement for statement in session.executed)
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


@pytest.mark.usefixtures("v2_capabilities")
def test_infer_automated_data_interval_uses_the_v2_instance_method() -> None:
    """Route automated inference through 2.x's `DAG.infer_automated_data_interval`."""

    scheduler_dag: Any = SimpleNamespace(
        infer_automated_data_interval=lambda logical_date: ("automated", logical_date)
    )

    assert dag_module._infer_automated_data_interval(scheduler_dag, "L") == ("automated", "L")


@pytest.mark.usefixtures("v2_capabilities")
def test_infer_automated_data_interval_degrades_to_manual_inference() -> None:
    """Fall back to the manual shape when the whitelist refuses the timetable."""

    def refuse(logical_date: Any) -> None:
        """Raise 2.x's whitelist rejection for a non-interval timetable."""

        del logical_date
        raise ValueError("Not a valid timetable")

    scheduler_dag: Any = SimpleNamespace(
        infer_automated_data_interval=refuse,
        timetable=SimpleNamespace(
            infer_manual_data_interval=lambda *, run_after: ("manual", run_after)
        ),
    )

    assert dag_module._infer_automated_data_interval(scheduler_dag, "L") == ("manual", "L")
