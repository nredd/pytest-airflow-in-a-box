"""Test guarded Dag compatibility operations and failure reporting."""

from __future__ import annotations

import importlib
from types import SimpleNamespace
from typing import Any, cast

import pytest

from pytest_airflow_in_a_box._compat import dag as dag_compat
from pytest_airflow_in_a_box._compat import registry
from pytest_airflow_in_a_box._compat.capabilities import (
    AirflowFamily,
    SecretsResolution,
    TimezoneLocation,
)
from pytest_airflow_in_a_box._compat.dag import (
    DagCleanupError,
    DagPersistenceError,
    DagPersistenceRecord,
    DagRunCreationError,
    TaskInstanceCreationError,
)


class _Session:
    """Record the session lifecycle needed by compatibility failure tests."""

    def __init__(self) -> None:
        """Initialize lifecycle counters."""

        self.rollbacks = 0
        self.closes = 0
        self.commits = 0

    def rollback(self) -> None:
        """Record one rollback."""

        self.rollbacks += 1

    def close(self) -> None:
        """Record one close."""

        self.closes += 1

    def commit(self) -> None:
        """Record one commit."""

        self.commits += 1


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

    monkeypatch.setattr(
        dag_compat,
        "resolve_capabilities",
        lambda: SimpleNamespace(family=AirflowFamily.V3),
    )
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


def test_sync_dag_model_does_not_retry_on_v3(monkeypatch: pytest.MonkeyPatch) -> None:
    """Propagate a 3.x `IntegrityError` after exactly one write, never retrying.

    3.x keys `dag_code` on the per-Dag `dag_version_id`, so no row is shared across
    xdist workers and issue #157's retry stays scoped to the 2.x branch. Extending it
    to 3.x must be a deliberate decision, not a refactor side effect.

    Parameters:
        monkeypatch: pytest.MonkeyPatch pinning the 3.x family and faking the writer.
    """

    from sqlalchemy.exc import IntegrityError

    calls: list[Any] = []

    class FakeSerializedDag:
        """Raise the constraint violation the 2.x branch would retry."""

        @classmethod
        def bulk_write_to_db(cls, *args: Any, **kwargs: Any) -> None:
            calls.append((args, kwargs))
            raise IntegrityError("INSERT INTO dag_code", None, Exception("boom"))

    monkeypatch.setattr(
        dag_compat,
        "resolve_capabilities",
        lambda: SimpleNamespace(family=AirflowFamily.V3),
    )
    monkeypatch.setattr(dag_compat, "_get_serialized_dag_class", lambda: FakeSerializedDag)
    session = _Session()
    dag: Any = object()

    with pytest.raises(IntegrityError):
        dag_compat._sync_dag_model(dag, _record(session))

    assert len(calls) == 1
    assert session.rollbacks == 0


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


def test_persist_registers_and_cleanup_unregisters_authoring_dag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Register the authoring Dag only after persistence and remove it at cleanup."""

    session = _Session()
    serialized = object()
    monkeypatch.setattr(registry, "_AUTHORING_DAGS", {})
    monkeypatch.setattr(dag_compat, "_ensure_bundle", lambda _record: None)
    monkeypatch.setattr(dag_compat, "_sync_dag_model", lambda _dag, _record: None)
    monkeypatch.setattr(dag_compat, "_write_serialized_dag", lambda _dag, _record: None)
    monkeypatch.setattr(dag_compat, "_load_serialized_dag", lambda _record: serialized)
    monkeypatch.setattr(dag_compat, "_cleanup_dag", lambda _record: None)
    dag: Any = object()
    record = _record(session)

    persisted = dag_compat.persist_dag(dag, record)

    assert persisted is serialized
    assert registry.lookup_authoring_dag("compat_dag") is dag

    dag_compat.cleanup_dag(record)

    assert registry.lookup_authoring_dag("compat_dag") is None
    assert session.closes == 1


def test_failed_persistence_does_not_register_authoring_dag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep the registry empty when persistence fails before commit."""

    session = _Session()

    def fail_bundle(record: DagPersistenceRecord) -> None:
        """Raise the representative persistence failure."""

        del record
        raise OSError("bundle write failed")

    monkeypatch.setattr(registry, "_AUTHORING_DAGS", {})
    monkeypatch.setattr(dag_compat, "_ensure_bundle", fail_bundle)
    monkeypatch.setattr(dag_compat, "_cleanup_dag", lambda _record: None)
    dag: Any = object()

    with pytest.raises(DagPersistenceError, match="creating DagBundleModel"):
        dag_compat.persist_dag(dag, _record(session))

    assert registry.lookup_authoring_dag("compat_dag") is None


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


def test_dagrun_creation_wraps_airflow_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Name the failed DagRun operation and retain its Airflow cause."""

    from airflow.models.dag_version import DagVersion

    session = _Session()
    failure = OSError("DagRun write failed")
    scheduler_dag = SimpleNamespace(
        timetable=SimpleNamespace(infer_manual_data_interval=lambda **_kwargs: None),
    )

    def fail_create(**kwargs: Any) -> None:
        """Raise the representative scheduler-Dag failure."""

        del kwargs
        raise failure

    def latest_version(dag_id: str, *, session: Any) -> SimpleNamespace:
        """Return representative current DagVersion metadata."""

        del dag_id, session
        return SimpleNamespace(id="version")

    scheduler_dag.create_dagrun = fail_create
    monkeypatch.setattr(
        DagVersion,
        "get_latest_version",
        staticmethod(latest_version),
    )
    authoring_dag: Any = object()

    with pytest.raises(DagRunCreationError, match="calling the persisted scheduler Dag") as caught:
        dag_compat.create_dag_run(
            scheduler_dag,
            authoring_dag,
            _record(session),
            run_id="failed_run",
            logical_date=dag_compat.UNSET,
            run_after=None,
            start_date=None,
            dag_run_kwargs={},
        )

    assert caught.value.__cause__ is failure
    assert session.rollbacks == 1


def test_task_instance_selection_wraps_query_failure() -> None:
    """Name task-instance selection and retain its metadata query cause."""

    session = _Session()
    failure = OSError("task-instance query failed")
    record = _record(session)
    record.dag_run_ids.add(7)
    dag: Any = SimpleNamespace(task_dict={"task": object()})

    def fail_query(**kwargs: Any) -> None:
        """Raise the representative task-instance query failure."""

        del kwargs
        raise failure

    dag_run: Any = SimpleNamespace(id=7, run_id="run", get_task_instance=fail_query)

    with pytest.raises(TaskInstanceCreationError, match="selecting task-instance") as caught:
        dag_compat.select_task_instance(
            dag,
            dag_run,
            record,
            task_id="task",
            map_index=-1,
        )

    assert caught.value.__cause__ is failure
    assert session.rollbacks == 1


def test_dagrun_creation_requires_current_version(monkeypatch: pytest.MonkeyPatch) -> None:
    """Wrap an absent current DagVersion before creating any run rows."""

    from airflow.models.dag_version import DagVersion

    session = _Session()

    def missing_version(_dag_id: str, *, session: Any) -> None:
        """Return no current DagVersion metadata."""

        del session

    monkeypatch.setattr(
        DagVersion,
        "get_latest_version",
        staticmethod(missing_version),
    )
    authoring_dag: Any = object()

    with pytest.raises(DagRunCreationError, match="loading the current DagVersion") as caught:
        dag_compat.create_dag_run(
            object(),
            authoring_dag,
            _record(session),
            run_id="missing_version",
            logical_date=dag_compat.UNSET,
            run_after=None,
            start_date=None,
            dag_run_kwargs={},
        )

    assert isinstance(caught.value.__cause__, RuntimeError)


def test_dagrun_creation_rejects_stale_version_linkage(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reject a created DagRun linked to anything except the current DagVersion."""

    from airflow.models.dag_version import DagVersion

    session = _Session()
    scheduler_dag = SimpleNamespace(
        timetable=SimpleNamespace(infer_manual_data_interval=lambda **_kwargs: None),
        create_dagrun=lambda **_kwargs: SimpleNamespace(created_dag_version_id="stale"),
    )

    def current_version(_dag_id: str, *, session: Any) -> SimpleNamespace:
        """Return representative current DagVersion metadata."""

        del session
        return SimpleNamespace(id="current")

    monkeypatch.setattr(
        DagVersion,
        "get_latest_version",
        staticmethod(current_version),
    )
    authoring_dag: Any = object()

    with pytest.raises(DagRunCreationError, match="expected current version") as caught:
        dag_compat.create_dag_run(
            scheduler_dag,
            authoring_dag,
            _record(session),
            run_id="stale_version",
            logical_date=dag_compat.UNSET,
            run_after=None,
            start_date=None,
            dag_run_kwargs={},
        )

    assert isinstance(caught.value.__cause__, RuntimeError)


def test_legacy_refresh_and_explicit_data_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    """Preserve an explicit interval and use the pre-3.3 refresh signature."""

    from airflow.models.dag_version import DagVersion

    session = _Session()
    ti = SimpleNamespace(
        dag_id="compat_dag",
        run_id="compat_run",
        task_id="task",
        map_index=-1,
        refresh_from_task=lambda _task: None,
    )
    dag_run: Any = SimpleNamespace(
        id=9,
        run_id="compat_run",
        created_dag_version_id="current",
        verify_integrity=lambda **_kwargs: None,
        get_task_instances=lambda **_kwargs: [ti],
        get_task_instance=lambda **_kwargs: ti,
    )
    scheduler_dag = SimpleNamespace(
        timetable=SimpleNamespace(
            infer_manual_data_interval=lambda **_kwargs: pytest.fail(
                "explicit interval was ignored"
            )
        ),
        create_dagrun=lambda **_kwargs: dag_run,
    )
    task = object()
    authoring_dag: Any = SimpleNamespace(
        task_dict={"task": task},
        get_task=lambda _task_id: task,
    )

    def current_version(_dag_id: str, *, session: Any) -> SimpleNamespace:
        """Return representative current DagVersion metadata."""

        del session
        return SimpleNamespace(id="current")

    monkeypatch.setattr(
        DagVersion,
        "get_latest_version",
        staticmethod(current_version),
    )
    monkeypatch.setattr(
        dag_compat,
        "resolve_capabilities",
        lambda: SimpleNamespace(
            family=AirflowFamily.V3,
            timezone_location=TimezoneLocation.SDK,
            secrets_resolution=SecretsResolution.SUPERVISOR_COMMS,
            refresh_from_task_supports_dag_run=False,
        ),
    )
    record = _record(session)

    created = dag_compat.create_dag_run(
        scheduler_dag,
        authoring_dag,
        record,
        run_id="compat_run",
        logical_date=dag_compat.UNSET,
        run_after=None,
        start_date=None,
        dag_run_kwargs={"data_interval": (object(), object())},
    )
    selected = dag_compat.select_task_instance(
        authoring_dag,
        dag_run,
        record,
        task_id="task",
        map_index=-1,
    )

    assert created is dag_run
    assert selected is ti
    assert session.commits == 2


def test_mapped_expansion_is_a_noop_for_regular_tasks() -> None:
    """Avoid scheduler expansion for an ordinary serialized task."""

    session: Any = _Session()

    dag_compat.expand_mapped_task_instances(object(), "run", session)

    assert session.commits == 0


def test_mapped_expansion_commits_scheduler_instances(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Expand a mapped scheduler task and commit its generated instances."""

    from airflow.models.taskmap import TaskMap

    calls: list[tuple[Any, str, Any]] = []
    monkeypatch.setattr(
        TaskMap,
        "expand_mapped_task",
        lambda task, run_id, *, session: calls.append((task, run_id, session)),
    )
    task = SimpleNamespace(is_mapped=True)
    session: Any = _Session()

    dag_compat.expand_mapped_task_instances(task, "mapped_run", session)

    assert calls == [(task, "mapped_run", session)]
    assert session.commits == 1


# ---------------------------------------------------------------------------
# is_custom_timetable_instance (#114)
# ---------------------------------------------------------------------------


def test_custom_timetable_predicate_accepts_a_custom_instance() -> None:
    """Mark a live custom `Timetable` instance as needing registration."""

    from airflow.timetables.base import Timetable

    class _CustomTimetable(Timetable):
        """Minimal custom timetable; only its MRO and module matter here."""

    assert dag_compat.is_custom_timetable_instance(_CustomTimetable())


def test_custom_timetable_predicate_rejects_non_timetable_schedules() -> None:
    """Pass every ordinary `schedule` spelling through untouched."""

    from datetime import timedelta

    from airflow.timetables.base import Timetable

    class _CustomTimetable(Timetable):
        """Minimal custom timetable, rejected here because it arrives as a CLASS."""

    assert not dag_compat.is_custom_timetable_instance(None)
    assert not dag_compat.is_custom_timetable_instance("@daily")
    assert not dag_compat.is_custom_timetable_instance(timedelta(days=1))
    assert not dag_compat.is_custom_timetable_instance(_CustomTimetable)
    assert not dag_compat.is_custom_timetable_instance(object())


def test_custom_timetable_predicate_rejects_airflow_builtin_timetables() -> None:
    """Leave built-in timetables alone; they decode without plugin registration."""

    from airflow.timetables.trigger import CronTriggerTimetable

    assert not dag_compat.is_custom_timetable_instance(
        CronTriggerTimetable("@daily", timezone="UTC")
    )


def test_custom_timetable_predicate_short_circuits_on_the_2x_family(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return False on 2.x before touching anything else.

    The component sandbox this predicate feeds is 3.x-only; its own gate would
    `pytest.fail` the test, so the 2.x family must exit first. The custom instance
    that would return True on 3.x proves the family gate is what decided.
    """

    from airflow.timetables.base import Timetable

    class _CustomTimetable(Timetable):
        """Minimal custom timetable; would register on the 3.x family."""

    instance = _CustomTimetable()
    assert dag_compat.is_custom_timetable_instance(instance)

    monkeypatch.setattr(
        dag_compat,
        "resolve_capabilities",
        lambda: SimpleNamespace(family=AirflowFamily.V2),
    )

    assert not dag_compat.is_custom_timetable_instance(instance)


def test_custom_schedule_timetables_collects_a_direct_custom_instance() -> None:
    """Collect a custom timetable passed as `schedule` directly."""

    from airflow.timetables.base import Timetable

    class _CustomTimetable(Timetable):
        """Minimal custom timetable; only its MRO and module matter here."""

    instance = _CustomTimetable()

    assert dag_compat.custom_schedule_timetables(instance) == (instance,)


def test_custom_schedule_timetables_collects_a_nested_custom_instance() -> None:
    """Collect the custom timetable inside a built-in wrapper's `timetable` attribute.

    `AssetOrTimeSchedule` lives under `airflow.timetables.assets` itself, but its
    `serialize()` encodes the INNER timetable, which fails unregistered exactly like
    the direct case -- the shape this collector exists for.
    """

    from airflow.sdk import Asset
    from airflow.timetables.assets import AssetOrTimeSchedule
    from airflow.timetables.base import Timetable

    class _CustomTimetable(Timetable):
        """Minimal custom timetable; only its MRO and module matter here."""

    inner = _CustomTimetable()
    # `cast` because `AssetOrTimeSchedule` annotates `assets` with the serialized
    # asset type; the authoring-time SDK `Asset` is converted at runtime.
    wrapper = AssetOrTimeSchedule(
        timetable=inner, assets=cast("Any", [Asset("compat_wrapped_asset")])
    )

    assert dag_compat.custom_schedule_timetables(wrapper) == (inner,)


def test_custom_schedule_timetables_ignores_ordinary_schedules() -> None:
    """Collect nothing from every schedule spelling that needs no registration."""

    from datetime import timedelta

    from airflow.sdk import Asset
    from airflow.timetables.assets import AssetOrTimeSchedule
    from airflow.timetables.trigger import CronTriggerTimetable

    # `cast` because `AssetOrTimeSchedule` annotates `assets` with the serialized
    # asset type; the authoring-time SDK `Asset` is converted at runtime.
    builtin_wrapper = AssetOrTimeSchedule(
        timetable=CronTriggerTimetable("@daily", timezone="UTC"),
        assets=cast("Any", [Asset("compat_builtin_asset")]),
    )

    assert dag_compat.custom_schedule_timetables(None) == ()
    assert dag_compat.custom_schedule_timetables("@daily") == ()
    assert dag_compat.custom_schedule_timetables(timedelta(days=1)) == ()
    assert dag_compat.custom_schedule_timetables(CronTriggerTimetable) == ()
    assert (
        dag_compat.custom_schedule_timetables(CronTriggerTimetable("@daily", timezone="UTC")) == ()
    )
    assert dag_compat.custom_schedule_timetables(builtin_wrapper) == ()


def test_custom_schedule_timetables_rejects_a_custom_timetable_class() -> None:
    """Name the class-instead-of-instance mistake instead of a later bare `KeyError`."""

    from airflow.timetables.base import Timetable

    class _CustomTimetable(Timetable):
        """Minimal custom timetable; only its MRO and module matter here."""

    with pytest.raises(TypeError, match="instead of the class"):
        dag_compat.custom_schedule_timetables(_CustomTimetable)


def test_custom_schedule_timetables_short_circuits_on_the_2x_family(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Collect nothing on 2.x, even for the class shape the 3.x guard rejects."""

    from airflow.timetables.base import Timetable

    class _CustomTimetable(Timetable):
        """Minimal custom timetable; would raise or collect on the 3.x family."""

    monkeypatch.setattr(
        dag_compat,
        "resolve_capabilities",
        lambda: SimpleNamespace(family=AirflowFamily.V2),
    )

    assert dag_compat.custom_schedule_timetables(_CustomTimetable) == ()
    assert dag_compat.custom_schedule_timetables(_CustomTimetable()) == ()


def test_custom_timetable_predicate_matches_upstream_exemption_exactly() -> None:
    """Exempt only `airflow.timetables.`, mirroring `is_core_timetable_import_path`.

    Airflow's own shipped example timetable (`airflow.example_dags.plugins.workday`)
    and provider timetables resolve through the registered lookup like any custom
    class -- a broader `airflow.` exemption would silently skip their registration
    and reintroduce the raw `TimetableNotRegistered` failure.
    """

    from airflow.timetables.base import Timetable

    class _ExampleDagsShapedTimetable(Timetable):
        """Impersonate an airflow-namespaced, non-core-timetables module."""

    _ExampleDagsShapedTimetable.__module__ = "airflow.example_dags.plugins.workday"

    assert dag_compat.is_custom_timetable_instance(_ExampleDagsShapedTimetable())
    assert dag_compat.custom_schedule_timetables(_ExampleDagsShapedTimetable()) != ()


def test_time_restriction_type_resolves_the_installed_class() -> None:
    """Resolve the installed release's `TimeRestriction` class."""

    timetables_base = importlib.import_module("airflow.timetables.base")

    assert dag_compat.time_restriction_type() is timetables_base.TimeRestriction


def test_unset_sentinel_reprs_as_its_name() -> None:
    """Render the omission sentinel by its canonical spelling."""

    assert repr(dag_compat.UNSET) == "UNSET"
    assert isinstance(dag_compat.UNSET, dag_compat.UnsetType)


@pytest.mark.parametrize(
    ("family", "expected_module"),
    [
        (AirflowFamily.V2, "airflow.operators.empty"),
        (AirflowFamily.V3, "airflow.providers.standard.operators.empty"),
    ],
)
def test_empty_operator_location_branching(
    monkeypatch: pytest.MonkeyPatch,
    family: AirflowFamily,
    expected_module: str,
) -> None:
    """Resolve only the family-specific `EmptyOperator` location.

    Parameters:
        monkeypatch: pytest.MonkeyPatch pinning the classified family and importer.
        family: AirflowFamily selecting the certified branch.
        expected_module: str naming the module the branch must import.
    """

    sentinel = object()
    imported: list[str] = []

    def import_module(module_name: str) -> SimpleNamespace:
        """Record and return one fake operator module."""

        imported.append(module_name)
        return SimpleNamespace(EmptyOperator=sentinel)

    monkeypatch.setattr(
        dag_compat,
        "resolve_capabilities",
        lambda: SimpleNamespace(family=family),
    )
    monkeypatch.setattr(dag_compat, "import_module", import_module)

    assert dag_compat.empty_operator_class() is sentinel
    assert imported == [expected_module]


class _BundleSession(_Session):
    """Fake metadata session driving `ensure_shared_bundle` branch by branch."""

    def __init__(self, *, existing: bool = False, commit_error: Exception | None = None) -> None:
        """Configure the queried row and an optional commit failure.

        Parameters:
            existing: bool answering the absence check with a present row.
            commit_error: Exception | None raised by the first commit call.
        """

        super().__init__()
        self.existing = existing
        self.commit_error = commit_error
        self.added: list[Any] = []

    def get(self, _model: Any, _name: str) -> Any | None:
        """Answer the absence check from the configured state."""

        return object() if self.existing else None

    def add(self, row: Any) -> None:
        """Record one staged bundle row."""

        self.added.append(row)

    def commit(self) -> None:
        """Record one commit, raising the configured failure first."""

        if self.commit_error is not None:
            error, self.commit_error = self.commit_error, None
            raise error
        super().commit()


def test_ensure_shared_bundle_inserts_when_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Insert and commit the shared row exactly once, then close the session."""

    session = _BundleSession()
    monkeypatch.setattr(dag_compat, "open_dag_session", lambda _name: session)

    dag_compat.ensure_shared_bundle("testing")

    assert len(session.added) == 1
    assert session.commits == 1
    assert session.closes == 1


def test_ensure_shared_bundle_returns_on_present_row(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stage nothing when the shared row already exists."""

    session = _BundleSession(existing=True)
    monkeypatch.setattr(dag_compat, "open_dag_session", lambda _name: session)

    dag_compat.ensure_shared_bundle("testing")

    assert session.added == []
    assert session.commits == 0
    assert session.closes == 1


def test_ensure_shared_bundle_absorbs_the_concurrent_insert_race(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Treat a concurrent worker's UNIQUE violation as the goal state."""

    from sqlalchemy.exc import IntegrityError

    session = _BundleSession(
        commit_error=IntegrityError("INSERT INTO dag_bundle", None, Exception("duplicate"))
    )
    monkeypatch.setattr(dag_compat, "open_dag_session", lambda _name: session)

    dag_compat.ensure_shared_bundle("testing")

    assert session.rollbacks == 1
    assert session.closes == 1


def test_ensure_shared_bundle_wraps_unexpected_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Name the operation and retain the cause for any non-race failure."""

    failure = RuntimeError("bundle table is broken")
    session = _BundleSession(commit_error=failure)
    monkeypatch.setattr(dag_compat, "open_dag_session", lambda _name: session)

    with pytest.raises(DagPersistenceError, match="shared Dag bundle 'testing'") as caught:
        dag_compat.ensure_shared_bundle("testing")

    assert caught.value.__cause__ is failure
    assert session.rollbacks == 1
    assert session.closes == 1


def test_create_dag_run_rejects_explicit_none_logical_date_on_v2(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refuse the 3.x no-logical-date run shape loudly on the 2.x family."""

    monkeypatch.setattr(
        dag_compat,
        "resolve_capabilities",
        lambda: SimpleNamespace(family=AirflowFamily.V2),
    )
    authoring_dag: Any = object()

    with pytest.raises(ValueError, match="logical_date=None"):
        dag_compat.create_dag_run(
            object(),
            authoring_dag,
            _record(_Session()),
            run_id="rejected",
            logical_date=None,
            run_after=None,
            start_date=None,
            dag_run_kwargs={},
        )


def test_coerce_run_type_accepts_members_and_strings() -> None:
    """Coerce both a plain string and an existing member to the enum."""

    from airflow.utils.types import DagRunType

    assert dag_compat.coerce_run_type("scheduled") is DagRunType.SCHEDULED
    assert dag_compat.coerce_run_type(DagRunType.MANUAL) is DagRunType.MANUAL
