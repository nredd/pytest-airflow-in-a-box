"""Construct, persist, and clean Dags across certified Airflow releases.

Airflow imports remain deferred until a test calls the fixture.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/public-airflow-interface.html
    https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-serialization.html
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from importlib import import_module
from typing import TYPE_CHECKING, Any

from pytest_airflow_in_a_box._compat.capabilities import (
    AirflowFamily,
    resolve_capabilities,
)
from pytest_airflow_in_a_box._compat.components import _is_timetable
from pytest_airflow_in_a_box._compat.registry import (
    register_authoring_dag,
    unregister_authoring_dag,
)

LOGGER = logging.getLogger(__name__)

# Supplied to releases whose Dag construction demands a `start_date` even without a
# schedule (see `AirflowCapabilities.dag_requires_start_date`). The epoch is chosen so
# it can never fall after a caller's `logical_date` and strand a task instance behind
# its own task's start date.
IMPLICIT_V2_START_DATE = datetime(1970, 1, 1, tzinfo=timezone.utc)
# The scheduling keywords 2.x's `DAG.__init__` treats as interchangeable with
# `schedule`, minus `schedule` itself, which `build_dag` pops before probing.
_V2_SCHEDULING_KEYWORDS = ("schedule_interval", "timetable")
# Bounded retries for 2.x's non-atomic check-then-insert into `dag_code`, whose rows
# key on `fileloc` and are therefore shared across xdist workers testing one source
# file (issue #157). Each retry independently re-checks, so one attempt is consumed
# only by a genuinely concurrent insert.
_V2_DAG_CODE_SYNC_ATTEMPTS = 3
# Keywords `create_dag_run` itself always assigns to the scheduler Dag's `create_dagrun`
# call (never `setdefault`) on every certified release. A caller-supplied `dag_run_kwargs`
# entry with one of these names would double-pass it and blow up three frames down inside
# Airflow with an opaque `TypeError` (issue #239); the family-specific sets add the two
# scheduling keywords whose spelling differs between families.
_RESERVED_DAG_RUN_KWARGS_COMMON = frozenset({"run_id", "start_date", "session"})
_RESERVED_DAG_RUN_KWARGS_V2 = _RESERVED_DAG_RUN_KWARGS_COMMON | {"execution_date"}
_RESERVED_DAG_RUN_KWARGS_V3 = _RESERVED_DAG_RUN_KWARGS_COMMON | {"logical_date", "run_after"}
# Two of the reserved keys have no same-named public keyword to redirect a caller to:
# `session` is fixture-owned and never accepted as a parameter, and 2.x's `execution_date`
# is spelled `logical_date` everywhere in the public API. Every other reserved key already
# matches its own dedicated parameter name one-for-one, so the generic remedy is correct
# for those.
_RESERVED_DAG_RUN_KWARGS_REMEDY = {
    "session": "the run is created on the fixture-owned session -- use `dag_maker.session`",
    "execution_date": "pass `logical_date` instead; this shim maps it to 2.x's `execution_date`",
}

if TYPE_CHECKING:
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance
    from airflow.sdk import DAG
    from sqlalchemy.orm import Session

    from pytest_airflow_in_a_box.types import SerializedDag


class UnsetType:
    """Mark an argument as omitted, distinct from an explicitly-passed ``None``.

    ``create_dag_run`` needs the distinction for ``logical_date``: omission keeps the
    current-UTC default, while an explicit ``None`` requests a 3.x run with no logical
    date at all (the shape asset-triggered runs take).
    """

    def __repr__(self) -> str:
        """Render the module-level singleton's name.

        Returns:
            str containing the canonical ``UNSET`` spelling.
        """

        return "UNSET"


UNSET = UnsetType()


class DagPersistenceError(RuntimeError):
    """Report failure to create or persist fixture-owned Airflow Dag metadata."""


class DagCleanupError(RuntimeError):
    """Report failure to remove fixture-owned Airflow Dag metadata."""


class DagRunCreationError(RuntimeError):
    """Report failure to create fixture-owned Airflow DagRun metadata."""


class TaskInstanceCreationError(RuntimeError):
    """Report failure to select or refresh fixture-owned task-instance metadata."""


@dataclass
class DagPersistenceRecord:
    """Track the exact resources created by one successful Dag context.

    Parameters:
        dag_id: str identifying the fixture-owned Dag.
        bundle_name: str identifying its isolated bundle row.
        session: sqlalchemy.orm.Session used to persist and inspect metadata.
        session_owned: bool indicating whether the fixture opened ``session`` and may
            roll back and close it. ``False`` marks a caller-supplied session the
            fixture must never close; ``cleanup_dag`` replaces it with a fresh owned
            one, because caller fixtures can finalize first.
        bundle_version: str | None recorded on persisted 3.x metadata rows, or
            ``None`` for unversioned. Ignored by the 2.x family, which predates
            bundles.
        bundle_created: bool indicating whether this fixture inserted the bundle row.
        dag_run_ids: set[int] containing exact fixture-owned DagRun primary keys.
        task_instance_keys: set[tuple[str, str, str, int]] containing owned task identities.
    """

    dag_id: str
    bundle_name: str
    session: Session
    session_owned: bool = True
    bundle_version: str | None = None
    bundle_created: bool = False
    dag_run_ids: set[int] = field(default_factory=set)
    task_instance_keys: set[tuple[str, str, str, int]] = field(default_factory=set)


def _is_v2() -> bool:
    """Report whether the installed Airflow is the certified 2.x family.

    Returns:
        bool selecting the 2.x metadata interface.
    """

    return resolve_capabilities().family is AirflowFamily.V2


def _needs_implicit_start_date(schedule: Any, dag_kwargs: dict[str, Any]) -> bool:
    """Report whether this Dag would trip a pre-2.8 `start_date` requirement.

    The injection is scoped to exactly the case Airflow 2.8 stopped rejecting: no
    scheduling argument and no `start_date` anywhere. A scheduled Dag without one still
    raises, identically worded, on every certified release, so the shim never converts a
    real authoring error into a silent default. "Scheduled" is 2.8's own definition,
    which counts the deprecated `schedule_interval` and `timetable` spellings alongside
    `schedule` -- exactly the spellings a 2.7-vintage Dag is most likely to use.

    Parameters:
        schedule: Any containing the scheduling argument already popped from the kwargs.
        dag_kwargs: dict[str, Any] forwarded to the authoring constructor.

    Returns:
        bool marking the Dag as one needing an implicit `start_date`.

    References:
        https://github.com/apache/airflow/blob/2.8.4/airflow/models/dag.py#L552
    """

    if not resolve_capabilities().dag_requires_start_date:
        return False
    if schedule or any(dag_kwargs.get(name) for name in _V2_SCHEDULING_KEYWORDS):
        return False
    if dag_kwargs.get("start_date") is not None:
        return False
    default_args = dag_kwargs.get("default_args") or {}
    return default_args.get("start_date") is None


def is_custom_timetable_instance(schedule: Any) -> bool:
    """Report whether a `schedule` value is a live custom-timetable instance.

    The predicate behind `custom_schedule_timetables` (and through it `dag_maker`'s
    transparent timetable registration), so its gates are ordered to keep every other
    `schedule` spelling exactly as cheap and as 2.x-safe as before: the 2.x family
    exits first (the component sandbox this feeds is 3.x-only, and its own gate would
    `pytest.fail` the test), then `None` and bare classes (the collector raises for a
    custom timetable CLASS separately), then anything `_compat.components._is_timetable`
    does not recognize -- delegated rather than re-implemented, so `dag_maker`'s hook
    and `check_component`'s auto-classification can never disagree about what counts
    as a timetable. What survives is a timetable instance, exempted only when
    `_needs_timetable_registration` says the serializer imports it directly.

    Parameters:
        schedule: Any containing the `schedule` value handed to `dag_maker`.

    Returns:
        bool marking `schedule` as an instance needing registration before
        serialization.
    """

    if _is_v2() or schedule is None or isinstance(schedule, type):
        return False
    if not _is_timetable(schedule):
        return False
    return _needs_timetable_registration(type(schedule))


def _needs_timetable_registration(component_type: type) -> bool:
    """Report whether Airflow's serializer resolves a timetable class only by registry.

    Mirrors upstream's own exemption exactly: `is_core_timetable_import_path` treats
    only the `airflow.timetables.` prefix as importable-without-registration, so
    everything else -- including Airflow's own shipped
    `airflow.example_dags.plugins.workday.AfterWorkdayTimetable` and provider
    timetables whose entry-point plugin is not loaded -- goes through the registered
    lookup and needs registration. A broader `airflow.` exemption would silently skip
    exactly those and reintroduce the raw `TimetableNotRegistered` failure.

    Parameters:
        component_type: type containing the timetable class.

    Returns:
        bool marking the class as one the serializer resolves through the registry.
    """

    return not component_type.__module__.startswith("airflow.timetables.")


def custom_schedule_timetables(schedule: Any) -> tuple[Any, ...]:
    """Collect every custom timetable instance a `schedule` value carries.

    Two shapes need registration before serialization: a custom timetable passed as
    `schedule` directly, and one nested inside a built-in wrapper's `timetable`
    attribute -- `AssetOrTimeSchedule(timetable=CustomTimetable(), assets=[...])`
    lives under `airflow.timetables.assets` itself, but its `serialize()` calls
    `encode_timetable` on the INNER custom timetable, which fails unregistered
    exactly like the direct case. The one-level `timetable` probe covers the only
    nesting shape Airflow ships; the else-branch keeps a custom timetable that
    happens to expose its own `timetable` attribute from double-registering.

    Parameters:
        schedule: Any containing the `schedule` value handed to `dag_maker`.

    Returns:
        tuple[Any, ...] containing the custom timetable instances to register, empty
        when nothing needs registration.

    Raises:
        TypeError: `schedule` is a custom timetable CLASS. Airflow accepts it at Dag
            construction and only dies much later inside metadata sync with a bare
            `KeyError: 'timetable'`, so the one place that already inspects
            `schedule` names the mistake instead.
    """

    if (
        not _is_v2()
        and isinstance(schedule, type)
        and _is_timetable(schedule)
        and _needs_timetable_registration(schedule)
    ):
        raise TypeError(
            f"`schedule` needs a live `{schedule.__name__}` instance -- pass "
            f"`{schedule.__name__}(...)` instead of the class."
        )
    if is_custom_timetable_instance(schedule):
        return (schedule,)
    inner = getattr(schedule, "timetable", None)
    if is_custom_timetable_instance(inner):
        return (inner,)
    return ()


def build_dag(dag_id: str, fileloc: str, dag_kwargs: dict[str, Any]) -> DAG:
    """Construct one public authoring Dag while keeping the Airflow import deferred.

    Parameters:
        dag_id: str containing a validated Dag identifier.
        fileloc: str naming the consumer test module.
        dag_kwargs: dict[str, Any] forwarded to the authoring constructor.

    Returns:
        airflow.sdk.DAG (or the 2.x `airflow.models.dag.DAG`) configured with stable
        file locations.
    """

    # Deferred to preserve pre-bootstrap plugin import safety.
    if _is_v2():
        from airflow.models.dag import DAG
    else:
        from airflow.sdk import DAG

    schedule = dag_kwargs.pop("schedule", None)
    if _needs_implicit_start_date(schedule, dag_kwargs):
        dag_kwargs["start_date"] = IMPLICIT_V2_START_DATE
    dag = DAG(dag_id=dag_id, schedule=schedule, **dag_kwargs)
    dag.fileloc = fileloc
    if not _is_v2():
        # 2.x computes `relative_fileloc` from `fileloc`; the property is read-only.
        dag.relative_fileloc = fileloc.rsplit("/", maxsplit=1)[-1]
    return dag


# FAB's auth-manager models were extracted from Airflow core into
# `apache-airflow-providers-fab` for 2.9; 2.7.3 and 2.8.4 carry them in-tree and their
# constraints files do not pin the provider at all, so the provider-only import failed
# unconditionally below 2.9 and left the shim silently dead. Ordered newest-location
# first so the certified releases that have the provider stop at one import.
_V2_FAB_MODEL_MODULES = (
    "airflow.providers.fab.auth_manager.models",
    "airflow.auth.managers.fab.models",
)


def _register_v2_orm_models() -> None:
    """Register the FAB tables 2.x ORM mapper configuration depends on.

    Flushing a 2.x `TaskInstance` or `DagRun` configures the note mappers, whose
    foreign keys target FAB's `ab_user` table; importing the FAB models registers that
    table so mapper configuration can resolve it regardless of what else the process
    imported first. The blast radius when it does not resolve is narrow but real:
    Airflow's own `initdb()` performs the same import, so any process that migrates
    self-registers `ab_user`, and the processes that do not are exactly the xdist
    workers that lose the `ensure_database` race and jump straight to the ready
    sentinel.

    References:
        https://github.com/apache/airflow/blob/2.9.3/airflow/providers/fab/auth_manager/models/__init__.py
        https://github.com/apache/airflow/blob/2.8.4/airflow/auth/managers/fab/models/__init__.py
    """

    for module_name in _V2_FAB_MODEL_MODULES:
        try:
            import_module(module_name)
        except ImportError:
            continue
        return
    # INFO because Airflow's dictConfig caps handlers at INFO; a DEBUG line would
    # vanish exactly when this diagnostic matters.
    locations = ", ".join(f"`{module_name}`" for module_name in _V2_FAB_MODEL_MODULES)
    LOGGER.info(
        f"No FAB auth-manager models module is importable ({locations}); 2.x "
        f"note-mapper configuration may fail to resolve the `ab_user` table on "
        f"ORM flushes"
    )


def open_dag_session(dag_id: str) -> Session:
    """Open a metadata session after validating the certified Airflow contract.

    Parameters:
        dag_id: str naming the operation for failure diagnostics.

    Returns:
        sqlalchemy.orm.Session connected to Airflow metadata.

    Raises:
        DagPersistenceError: Airflow cannot provide a metadata session.
    """

    try:
        resolve_capabilities()
        if _is_v2():
            _register_v2_orm_models()
        # Deferred because Airflow settings are bootstrap-sensitive.
        from airflow import settings

        session_factory = settings.Session
        if session_factory is None:
            raise RuntimeError("Airflow metadata session factory is not initialized")
        return session_factory.session_factory()
    except Exception as error:
        raise DagPersistenceError(
            f"Could not open an Airflow metadata session for Dag '{dag_id}': {error}"
        ) from error


def ensure_dag_absent(dag_id: str, session: Session) -> None:
    """Refuse to overwrite metadata not owned by this factory.

    Parameters:
        dag_id: str containing the prospective Dag identifier.
        session: sqlalchemy.orm.Session used for the ownership check.

    Raises:
        ValueError: A Dag row already uses the identifier.
        DagPersistenceError: Airflow cannot query the metadata row.
    """

    try:
        # Deferred private model access is isolated in the compatibility package.
        from airflow.models.dag import DagModel

        existing = session.get(DagModel, dag_id)
    except Exception as error:
        raise DagPersistenceError(
            f"Could not check existing Airflow metadata for Dag '{dag_id}': {error}"
        ) from error
    if existing is not None:
        raise ValueError(f"Dag metadata already exists for `dag_id` '{dag_id}'")


def _get_serialized_dag_class() -> Any:
    """Resolve the certified ``SerializedDAG`` location for the installed release.

    Returns:
        Any containing Airflow's release-specific ``SerializedDAG`` class.
    """

    release = resolve_capabilities().release
    module_name = (
        "airflow.serialization.serialized_objects"
        if release < (3, 2, 0)
        else "airflow.serialization.definitions.dag"
    )
    return import_module(module_name).SerializedDAG


def _get_dag_serializer() -> Any:
    """Resolve the class exposing ``serialize_dag``/``deserialize_dag`` for the installed release.

    Airflow 3.1 exposes both on ``serialized_objects.SerializedDAG``. Airflow 3.2 split the
    scheduler-facing ``SerializedDAG`` data class into ``serialization.definitions.dag`` and kept
    the encode/decode methods on a sibling ``serialized_objects.DagSerialization`` class.

    Returns:
        Any containing the release-specific class exposing ``serialize_dag``/``deserialize_dag``.
    """

    release = resolve_capabilities().release
    module = import_module("airflow.serialization.serialized_objects")
    return module.SerializedDAG if release < (3, 2, 0) else module.DagSerialization


def time_restriction_type() -> Any:
    """Resolve Airflow's ``TimeRestriction`` timetable boundary type.

    A plain deferred-import seam, not a capability probe: the type's location is
    stable across every certified release, and this wrapper only centralizes the
    runtime Airflow import behind ``_compat``.

    Returns:
        Any containing the ``airflow.timetables.base.TimeRestriction`` class.

    Raises:
        ImportError: Airflow is not importable in this environment.
    """

    # Deferred to preserve pre-bootstrap plugin import safety.
    from airflow.timetables.base import TimeRestriction

    return TimeRestriction


def empty_operator_class() -> Any:
    """Resolve Airflow's ``EmptyOperator`` for the installed family.

    A plain deferred-import seam, not a capability probe: 3.x relocated the operator
    into the standard provider, and this wrapper only centralizes the family split
    behind ``_compat``.

    Returns:
        Any containing the family-specific ``EmptyOperator`` class.

    Raises:
        ImportError: Airflow (or the standard provider on 3.x) is not importable.
    """

    module_name = (
        "airflow.operators.empty" if _is_v2() else "airflow.providers.standard.operators.empty"
    )
    return import_module(module_name).EmptyOperator


def coerce_run_type(value: Any) -> Any:
    """Coerce a run-type spelling to Airflow's ``DagRunType`` member.

    A plain deferred-import seam, not a capability probe: the enum's location is
    stable across every certified family, and coercion is idempotent for members, so
    upstream-parity fixtures can accept ``DagRunType`` members and plain strings alike
    without importing Airflow outside ``_compat``.

    Parameters:
        value: Any containing a ``DagRunType`` member or its string value.

    Returns:
        Any containing the ``DagRunType`` member.

    Raises:
        ValueError: ``value`` names no ``DagRunType`` member.
    """

    # Deferred to preserve pre-bootstrap plugin import safety.
    from airflow.utils.types import DagRunType

    return DagRunType(value)


def ensure_shared_bundle(name: str) -> None:
    """Create one shared Dag bundle row when absent, never deleting it afterward.

    Unlike ``_ensure_bundle``, which owns an isolated per-Dag bundle row and cleans it
    up, this row is deliberately shared across tests and xdist workers on one metadata
    database and is left in place for the whole run: a conditional teardown delete
    would race another worker's in-flight ``DagModel.bundle_name`` reference, the
    plugin never reads the row back, and the metadata database is disposable per run
    (the same reasoning that keeps 2.x ``dag_code`` rows in place, issue #157).

    Parameters:
        name: str identifying the shared bundle row.

    Raises:
        DagPersistenceError: Airflow cannot query or insert the bundle row.
    """

    # Deferred to preserve pre-bootstrap plugin import safety.
    from sqlalchemy.exc import IntegrityError

    session = open_dag_session(name)
    try:
        # Deferred private model access is isolated in the compatibility package.
        from airflow.models.dagbundle import DagBundleModel

        if session.get(DagBundleModel, name) is not None:
            return
        session.add(DagBundleModel(name=name))
        try:
            session.commit()
        except IntegrityError:
            # Two workers can both pass the absence check; the loser's UNIQUE
            # violation means the row now exists, which is the goal state.
            session.rollback()
    except Exception as error:
        session.rollback()
        raise DagPersistenceError(
            f"Could not ensure the shared Dag bundle '{name}': {error}"
        ) from error
    finally:
        session.close()


def _ensure_bundle(record: DagPersistenceRecord) -> None:
    """Create the fixture's isolated Dag bundle when absent.

    Bundles arrived in Airflow 3; the 2.x branch is a no-op so persistence keeps one
    call sequence across families.

    Parameters:
        record: DagPersistenceRecord receiving bundle ownership state.
    """

    if _is_v2():
        return

    from airflow.models.dagbundle import DagBundleModel

    if record.session.get(DagBundleModel, record.bundle_name) is not None:
        return
    record.session.add(DagBundleModel(name=record.bundle_name))
    record.session.flush()
    record.bundle_created = True


def _sync_dag_model_v2(dag: DAG, record: DagPersistenceRecord) -> None:
    """Sync the Dag through the 2.x authoring writer, retrying the `dag_code` race.

    2.x's `bulk_write_to_db` performs a non-atomic check-then-insert into `dag_code`,
    whose rows key on `fileloc` -- shared by every Dag built from one test module. Two
    xdist workers on one metadata database can both pass the check, and the loser hits
    a `dag_code.fileloc_hash` UNIQUE violation (issue #157). After the winner's commit
    the row exists, so a retried sync takes the update path and succeeds.

    The retry is deliberately narrow. Only errors naming `dag_code` qualify -- any
    other `IntegrityError` (e.g. a cross-worker `dag_id` collision that slipped past
    `ensure_dag_absent`'s non-locking check) must stay loud rather than be absorbed
    as a benign race. And because `dag_maker.session` is published to the test body
    before persistence runs, a session carrying staged user state is never retried:
    the between-attempt rollback would silently discard that state and the eventual
    commit would drop it without a trace. `_ensure_bundle` is a no-op on 2.x, so the
    plugin itself stages nothing the rollback could lose.

    Parameters:
        dag: airflow.sdk.DAG containing the completed task graph.
        record: DagPersistenceRecord identifying the metadata session.

    Raises:
        sqlalchemy.exc.IntegrityError: The failure is not the `dag_code` race, the
            session carries pending user state a retry would discard, or the race
            recurred on every bounded attempt.
    """

    from sqlalchemy.exc import IntegrityError

    # 2.x has no bundle arguments; the authoring class carries the writer. The
    # dynamic access keeps static checking valid against an installed 3.x tree.
    authoring_class: Any = type(dag)
    session = record.session
    # Snapshot before the first attempt: `bulk_write_to_db` stages pending objects of
    # its own, so this test is meaningless once the writer has run.
    has_pending_user_state = bool(session.new or session.dirty or session.deleted)
    attempt = 1
    while True:
        try:
            authoring_class.bulk_write_to_db([dag], session=session)
            return
        except IntegrityError as error:
            if (
                has_pending_user_state
                or "dag_code" not in str(error)
                or attempt >= _V2_DAG_CODE_SYNC_ATTEMPTS
            ):
                raise
            session.rollback()
            LOGGER.warning(
                f"Retrying the 2.x Dag metadata sync for '{record.dag_id}' after a "
                f"concurrent-writer IntegrityError (attempt {attempt} of "
                f"{_V2_DAG_CODE_SYNC_ATTEMPTS}): {error}"
            )
            attempt += 1


def _sync_dag_model(dag: DAG, record: DagPersistenceRecord) -> None:
    """Sync the Dag and its scheduler metadata through Airflow's canonical writer.

    Only the 2.x path retries `IntegrityError` (see `_sync_dag_model_v2`), and only
    for `dag_code` rows: 3.x keys those on the per-Dag `dag_version_id`, so no
    `dag_code` row is shared across workers. Rows keyed on a user-chosen `dag_id`
    (`dag`, `dag_version`, `dag_bundle`) can still collide across workers on either
    family -- that is the documented `RunDag` caveat in `types.py`, out of scope
    here, and a constraint violation there is a real failure, not a race to
    tolerate.

    Parameters:
        dag: airflow.sdk.DAG containing the completed task graph.
        record: DagPersistenceRecord identifying the isolated bundle.
    """

    if _is_v2():
        _sync_dag_model_v2(dag, record)
        return
    serialized_dag_class = _get_serialized_dag_class()
    serialized_dag_class.bulk_write_to_db(
        record.bundle_name,
        record.bundle_version,
        [dag],
        session=record.session,
    )


def _write_serialized_dag(dag: DAG, record: DagPersistenceRecord) -> None:
    """Write DagVersion, SerializedDagModel, and associated Dag code metadata.

    Parameters:
        dag: airflow.sdk.DAG containing the completed task graph.
        record: DagPersistenceRecord identifying the metadata session and bundle.
    """

    from airflow.models.serialized_dag import SerializedDagModel

    if _is_v2():
        # 2.x serializes the authoring Dag directly and has no DagVersion/bundle rows.
        # The dynamic access keeps static checking valid against an installed 3.x tree.
        serialized_model: Any = SerializedDagModel
        serialized_model.write_dag(dag, min_update_interval=0, session=record.session)
        return

    from airflow.serialization.serialized_objects import LazyDeserializedDAG

    lazy_dag = LazyDeserializedDAG.from_dag(dag)
    SerializedDagModel.write_dag(
        lazy_dag,
        bundle_name=record.bundle_name,
        bundle_version=record.bundle_version,
        min_update_interval=0,
        session=record.session,
    )


def _load_serialized_dag(record: DagPersistenceRecord) -> SerializedDag:
    """Load and validate the scheduler representation just persisted.

    Parameters:
        record: DagPersistenceRecord identifying the Dag and session.

    Returns:
        pytest_airflow_in_a_box.types.SerializedDag loaded from metadata.

    Raises:
        RuntimeError: Airflow did not return the required serialized Dag.
    """

    from airflow.models.serialized_dag import SerializedDagModel

    serialized_dag = SerializedDagModel.get_dag(record.dag_id, session=record.session)
    if serialized_dag is None:
        raise RuntimeError("Airflow did not return the persisted serialized Dag")
    return serialized_dag


def persist_dag(
    dag: DAG,
    record: DagPersistenceRecord,
) -> SerializedDag:
    """Persist every metadata row required by certified Airflow releases.

    Successful persistence registers the authoring Dag so task resolution works
    for task instances queried through sessions the factory does not own.

    Commits ``record.session``. On a borrowed session that also commits whatever the
    caller had staged, and the failure path's ``_cleanup_dag`` commits its deletions on
    the same still-live handle -- both deliberate, matching upstream ``dag_maker``.

    Parameters:
        dag: airflow.sdk.DAG containing the completed task graph.
        record: DagPersistenceRecord identifying owned resources.

    Returns:
        pytest_airflow_in_a_box.types.SerializedDag loaded from committed metadata.

    Raises:
        DagPersistenceError: Any Airflow persistence operation fails.
    """

    operation = "creating DagBundleModel metadata"
    try:
        _ensure_bundle(record)
        operation = "syncing DagModel metadata"
        _sync_dag_model(dag, record)
        operation = "writing DagVersion and SerializedDagModel metadata"
        _write_serialized_dag(dag, record)
        operation = "committing Dag metadata"
        record.session.commit()
        operation = "loading persisted serialized Dag metadata"
        serialized_dag = _load_serialized_dag(record)
        register_authoring_dag(record.dag_id, dag)
        return serialized_dag
    except Exception as error:
        record.session.rollback()
        try:
            _cleanup_dag(record)
        except Exception as cleanup_error:
            raise DagPersistenceError(
                f"Could not persist Airflow Dag '{record.dag_id}' while {operation}: {error}; "
                f"cleanup also failed: {cleanup_error}"
            ) from error
        raise DagPersistenceError(
            f"Could not persist Airflow Dag '{record.dag_id}' while {operation}: {error}"
        ) from error


def resync_dag(dag: DAG, record: DagPersistenceRecord) -> SerializedDag:
    """Re-persist an already-persisted authoring Dag's scheduler metadata.

    The upstream ``tests_common`` ``sync_dagbag_to_db`` shape: after the test body
    mutates the authoring Dag, re-run the same sync/serialize sequence
    ``persist_dag`` used, on the same record, so the metadata rows reflect the
    mutation. Unlike ``persist_dag``, failure never runs ``_cleanup_dag`` -- the Dag
    was persisted successfully once, so a transient resync failure must not delete
    rows mid-test; fixture finalization still owns teardown.

    Commits ``record.session``. On a borrowed session that also commits whatever the
    caller had staged -- the same documented semantics as ``persist_dag``. On the 2.x
    family the internal writers already route through that family's equivalents, so
    the method works there even though upstream 2.x never grew it.

    Parameters:
        dag: airflow.sdk.DAG containing the mutated task graph.
        record: DagPersistenceRecord identifying the previously persisted resources.

    Returns:
        pytest_airflow_in_a_box.types.SerializedDag reloaded from committed metadata.

    Raises:
        DagPersistenceError: Any Airflow persistence operation fails. The record's
            rows are left in place for fixture finalization.
    """

    operation = "creating DagBundleModel metadata"
    try:
        _ensure_bundle(record)
        operation = "syncing DagModel metadata"
        _sync_dag_model(dag, record)
        operation = "writing DagVersion and SerializedDagModel metadata"
        _write_serialized_dag(dag, record)
        operation = "committing Dag metadata"
        record.session.commit()
        operation = "loading persisted serialized Dag metadata"
        # 3.x `write_dag` reuses the latest DagVersion when nothing references it and
        # rewrites the serialized row with a bulk UPDATE, which bypasses the session's
        # identity map -- expire cached state so the reload observes the new payload.
        record.session.expire_all()
        serialized_dag = _load_serialized_dag(record)
        register_authoring_dag(record.dag_id, dag)
        return serialized_dag
    except Exception as error:
        record.session.rollback()
        raise DagPersistenceError(
            f"Could not resync Airflow Dag '{record.dag_id}' while {operation}: {error}"
        ) from error


def get_dag_model(record: DagPersistenceRecord) -> Any:
    """Return the live ``DagModel`` metadata row for one persisted Dag.

    Parameters:
        record: DagPersistenceRecord identifying the persisted Dag and its session.

    Returns:
        Any containing the ORM ``DagModel`` row attached to ``record.session``.

    Raises:
        DagPersistenceError: Airflow cannot query the row, or the row no longer
            exists -- a persisted Dag losing its ``DagModel`` row mid-test is a real
            failure, not an empty result.
    """

    try:
        # Deferred private model access is isolated in the compatibility package.
        from airflow.models.dag import DagModel

        dag_model = record.session.get(DagModel, record.dag_id)
    except Exception as error:
        raise DagPersistenceError(
            f"Could not load the DagModel row for Dag '{record.dag_id}': {error}"
        ) from error
    if dag_model is None:
        raise DagPersistenceError(f"No DagModel row exists for Dag '{record.dag_id}'")
    return dag_model


def create_dag_run(
    scheduler_dag: Any,
    authoring_dag: DAG,
    record: DagPersistenceRecord,
    *,
    run_id: str,
    logical_date: datetime | UnsetType | None,
    run_after: datetime | None,
    start_date: datetime | None,
    dag_run_kwargs: dict[str, Any],
) -> DagRun:
    """Create a DagRun through the persisted scheduler Dag contract.

    Parameters:
        scheduler_dag: pytest_airflow_in_a_box.types.SerializedDag persisted for scheduling.
        authoring_dag: airflow.sdk.DAG containing executable task objects.
        record: DagPersistenceRecord receiving exact metadata ownership.
        run_id: str containing the validated collision-safe run identifier.
        logical_date: datetime.datetime | UnsetType | None overriding the current UTC
            logical date, where an explicit ``None`` requests a 3.x run with no logical
            date at all; rejected on the 2.x family, which cannot express one.
        run_after: datetime.datetime | None overriding the current UTC run-after date;
            rejected on the 2.x family, which has no run-after concept.
        start_date: datetime.datetime | None overriding the current UTC start date.
        dag_run_kwargs: dict[str, Any] forwarded to Airflow's scheduler Dag.

    Returns:
        airflow.models.dagrun.DagRun committed with verified task instances.

    Raises:
        ValueError: `run_after` or an explicit ``logical_date=None`` was passed on the
            Airflow 2.x family, or `dag_run_kwargs` sets a key `create_dag_run` already
            assigns from its own parameters.
        DagRunCreationError: Airflow cannot create or verify the DagRun metadata.
    """

    is_v2 = _is_v2()
    if run_after is not None and is_v2:
        raise ValueError(
            "`run_after` is an Airflow 3.x scheduling concept with no 2.x equivalent; "
            "silently ignoring it would change run semantics between families. Pass "
            "`logical_date` on the 2.x family instead."
        )
    if logical_date is None and is_v2:
        raise ValueError(
            "An explicit `logical_date=None` requests a run with no logical date, an "
            "Airflow 3.x concept with no 2.x equivalent; every 2.x run carries an "
            "execution date. Omit the argument to use the current UTC date instead."
        )
    reserved = _RESERVED_DAG_RUN_KWARGS_V2 if is_v2 else _RESERVED_DAG_RUN_KWARGS_V3
    conflicting = sorted(reserved & dag_run_kwargs.keys())
    if conflicting:
        hints = "; ".join(
            f"`{key}`: "
            + _RESERVED_DAG_RUN_KWARGS_REMEDY.get(key, f"pass `{key}` as its own keyword argument")
            for key in conflicting
        )
        raise ValueError(
            f"`dag_run_kwargs` cannot set {conflicting}: pytest-airflow-in-a-box already "
            f"supplies {'these' if len(conflicting) > 1 else 'it'} from create_dag_run's own "
            f"parameters -- passing them again would double-pass the keyword to Airflow's "
            f"`create_dagrun`. {hints}."
        )

    # The 2.x module is dynamically resolved so static checking stays valid against an
    # installed 3.x tree, which has no `airflow.utils.timezone`.
    airflow_timezone: Any = import_module(resolve_capabilities().timezone_location.value)
    coerce_datetime = airflow_timezone.coerce_datetime
    convert_to_utc = airflow_timezone.convert_to_utc
    utcnow = airflow_timezone.utcnow
    from airflow.utils.state import DagRunState
    from airflow.utils.types import DagRunType

    operation = "resolving UTC dates"
    try:
        now = utcnow()
        resolved_logical_date = (
            None
            if logical_date is None
            else convert_to_utc(
                coerce_datetime(now if isinstance(logical_date, UnsetType) else logical_date)
            )
        )
        resolved_start_date = convert_to_utc(coerce_datetime(start_date or now))
        dag_version: Any = None
        if not is_v2:
            from airflow.models.dag_version import DagVersion

            operation = "loading the current DagVersion"
            dag_version = DagVersion.get_latest_version(record.dag_id, session=record.session)
            if dag_version is None:
                raise RuntimeError(
                    f"Airflow did not return a current DagVersion for '{record.dag_id}'"
                )

        kwargs = dict(dag_run_kwargs)
        # A run without a logical date has no interval to infer; leave `data_interval`
        # to the caller (or absent) exactly as Airflow's asset-triggered runs do.
        if "data_interval" not in kwargs and resolved_logical_date is not None:
            kwargs["data_interval"] = scheduler_dag.timetable.infer_manual_data_interval(
                run_after=resolved_logical_date
            )
        kwargs.setdefault("run_type", DagRunType.MANUAL)
        kwargs.setdefault("state", DagRunState.RUNNING)
        operation = "calling the persisted scheduler Dag's create_dagrun"
        if is_v2:
            # 2.x: `execution_date` interface, no `triggered_by`/`run_after`. No
            # explicit `verify_integrity` call: 2.x's `DAG.create_dagrun` already ends
            # with `run.verify_integrity(session=...)`, so task instances have
            # materialized by the time it returns and a second pass would only repeat
            # `_check_for_removed_or_restored_tasks` and mapped-task counting.
            dag_run: Any = scheduler_dag.create_dagrun(
                run_id=run_id,
                execution_date=resolved_logical_date,
                start_date=resolved_start_date,
                session=record.session,
                **kwargs,
            )
        else:
            from airflow.utils.types import DagRunTriggeredByType

            resolved_run_after = convert_to_utc(coerce_datetime(run_after or now))
            kwargs.setdefault("triggered_by", DagRunTriggeredByType.TEST)
            dag_run = scheduler_dag.create_dagrun(
                run_id=run_id,
                logical_date=resolved_logical_date,
                run_after=resolved_run_after,
                start_date=resolved_start_date,
                session=record.session,
                **kwargs,
            )
            if dag_run.created_dag_version_id != dag_version.id:
                raise RuntimeError(
                    f"DagRun '{run_id}' linked DagVersion "
                    f"'{dag_run.created_dag_version_id}', expected current version "
                    f"'{dag_version.id}'"
                )
            operation = "verifying task-instance integrity"
            dag_run.verify_integrity(session=record.session, dag_version_id=dag_version.id)
        capabilities = resolve_capabilities()
        task_instances: list[Any] = dag_run.get_task_instances(session=record.session)
        operation = "refreshing task instances from authoring tasks"
        for ti in task_instances:
            task = authoring_dag.get_task(str(ti.task_id))
            if capabilities.refresh_from_task_supports_dag_run:
                _refresh_from_task(ti, task, dag_run)
            else:
                _refresh_from_task(ti, task)
        operation = "committing DagRun and task-instance metadata"
        record.session.commit()
        record.dag_run_ids.add(dag_run.id)
        record.task_instance_keys |= {
            (str(ti.dag_id), str(ti.run_id), str(ti.task_id), ti.map_index)
            for ti in task_instances
        }
        return dag_run
    except Exception as error:
        record.session.rollback()
        raise DagRunCreationError(
            f"Could not create Airflow DagRun '{run_id}' for Dag '{record.dag_id}' while "
            f"{operation}: {error}"
        ) from error


def select_task_instance(
    authoring_dag: DAG,
    dag_run: DagRun,
    record: DagPersistenceRecord,
    *,
    task_id: str,
    map_index: int,
) -> TaskInstance:
    """Select and refresh one fixture-owned task instance.

    Parameters:
        authoring_dag: airflow.sdk.DAG containing executable task objects.
        dag_run: airflow.models.dagrun.DagRun created by this fixture record.
        record: DagPersistenceRecord containing ownership and session state.
        task_id: str identifying the requested task.
        map_index: int identifying the requested mapped task instance.

    Returns:
        airflow.models.taskinstance.TaskInstance refreshed from the authoring task.

    Raises:
        ValueError: The DagRun is not owned or the task selection is unavailable.
        TaskInstanceCreationError: Airflow cannot query or refresh task-instance metadata.
    """

    available_task_ids = sorted(authoring_dag.task_dict)
    if dag_run.id not in record.dag_run_ids:
        raise ValueError(
            f"DagRun '{dag_run.run_id}' is not owned by `dag_maker` for Dag '{record.dag_id}'"
        )
    if task_id not in authoring_dag.task_dict:
        raise ValueError(
            f"Task '{task_id}' is absent from Dag '{record.dag_id}'; available task IDs: "
            f"{available_task_ids}"
        )

    operation = "selecting task-instance metadata"
    try:
        ti = dag_run.get_task_instance(
            task_id=task_id,
            map_index=map_index,
            session=record.session,
        )
        if ti is None:
            available_instances = sorted(
                (candidate.task_id, candidate.map_index)
                for candidate in dag_run.get_task_instances(session=record.session)
            )
            raise ValueError(
                f"Task instance '{task_id}' with map index '{map_index}' is absent from DagRun "
                f"'{dag_run.run_id}'; available instances: {available_instances}"
            )
        operation = "refreshing the task instance from its authoring task"
        task = authoring_dag.get_task(task_id)
        if resolve_capabilities().refresh_from_task_supports_dag_run:
            _refresh_from_task(ti, task, dag_run)
        else:
            _refresh_from_task(ti, task)
        operation = "committing refreshed task-instance metadata"
        record.session.commit()
        selected: Any = ti
        record.task_instance_keys.add(
            (
                str(selected.dag_id),
                str(selected.run_id),
                str(selected.task_id),
                selected.map_index,
            )
        )
        return ti
    except ValueError:
        raise
    except Exception as error:
        record.session.rollback()
        raise TaskInstanceCreationError(
            f"Could not create task instance '{task_id}' with map index '{map_index}' for "
            f"DagRun '{dag_run.run_id}' while {operation}: {error}"
        ) from error


def task_is_mapped(task: Any) -> bool:
    """Report whether one task participates in dynamic task mapping.

    Parameters:
        task: Any containing an authoring or serialized operator.

    Returns:
        bool marking the task as a mapped operator on the installed family.
    """

    if _is_v2():
        # 2.x `MappedOperator` predates the `is_mapped` attribute, so an attribute
        # probe is always False there; detect by class instead.
        mapped_module = import_module("airflow.models.mappedoperator")
        return isinstance(task, mapped_module.MappedOperator)
    return bool(getattr(task, "is_mapped", False))


def expand_mapped_task_instances(task: Any, run_id: str, session: Session) -> None:
    """Expand a persisted mapped task for one DagRun when mapping applies."""

    if not task_is_mapped(task):
        return

    if _is_v2():
        # 2.x expansion lives on the mapped operator itself.
        task.expand_mapped_task(run_id, session=session)
        session.commit()
        return

    from airflow.models.taskmap import TaskMap

    TaskMap.expand_mapped_task(task, run_id, session=session)
    session.commit()


def _refresh_from_task(ti: Any, task: Any, dag_run: Any = None) -> None:
    """Cross Airflow's authoring/scheduler operator typing boundary.

    Parameters:
        ti: Any containing an ORM TaskInstance.
        task: Any containing an authoring or serialized operator.
        dag_run: Any containing an optional DagRun for mutation hooks.
    """

    if dag_run is None:
        ti.refresh_from_task(task)
    else:
        ti.refresh_from_task(task, dag_run=dag_run)


def _cleanup_dag(record: DagPersistenceRecord) -> None:
    """Delete only metadata carrying this record's Dag and bundle identities.

    Parameters:
        record: DagPersistenceRecord identifying fixture-owned rows.
    """

    from airflow.models.dag import DagModel
    from airflow.models.dagrun import DagRun
    from airflow.models.serialized_dag import SerializedDagModel
    from sqlalchemy import delete, func, select

    session = record.session
    session.rollback()
    backfill_ids: list[int] = []
    if not _is_v2():
        # Backfill rows arrived in 3.x. `BackfillDagRun` FK-references both `dag_run.id`
        # and `backfill.id`, so the join rows must go before either parent. Fixture
        # ownership is not enough of a predicate here: a test driving Airflow's Backfill
        # machinery directly creates backfills for this Dag -- and DagRuns for them --
        # that never pass through `create_dag_run`, so the cleanup is scoped by
        # `record.dag_run_ids` OR membership in this Dag's backfills (issue #258).
        from airflow.models.backfill import Backfill, BackfillDagRun

        backfill_ids = list(
            session.scalars(select(Backfill.id).where(Backfill.dag_id == record.dag_id))
        )
        session.execute(
            delete(BackfillDagRun).where(
                BackfillDagRun.dag_run_id.in_(record.dag_run_ids)
                | BackfillDagRun.backfill_id.in_(backfill_ids)
            )
        )
    for dag_run_id in record.dag_run_ids:
        dag_run = session.get(DagRun, dag_run_id)
        if dag_run is not None:
            session.delete(dag_run)
    session.flush()

    if backfill_ids:
        # `dag_run.backfill_id` FK-references `backfill.id` with no `ondelete` action,
        # so the Backfill parents can only go after every DagRun that might reference
        # them is gone: the fixture-owned runs (the delete loop and flush above) plus
        # the runs Airflow's Backfill machinery created for this Dag's backfills, which
        # are not in `record.dag_run_ids`. Those are ORM-deleted exactly like the owned
        # loop so task-instance rows cascade identically -- and removing them also
        # unblocks the DagVersion delete below. The Backfill parents are deleted too
        # (not just their `BackfillDagRun` children) so a fixture-owned Dag leaves no
        # backfill metadata behind, matching `clear_db`'s `backfill` group.
        from airflow.models.backfill import Backfill

        for dag_run in session.scalars(select(DagRun).where(DagRun.backfill_id.in_(backfill_ids))):
            session.delete(dag_run)
        session.flush()
        session.execute(delete(Backfill).where(Backfill.id.in_(backfill_ids)))

    if _is_v2():
        # 2.x has no DagVersion/bundle rows; serialized rows key on `dag_id`. The
        # `dag_code` row -- keyed on `fileloc` and shared by every Dag in one source
        # file -- is deliberately left in place: deleting it would re-arm issue #157's
        # cross-worker insert race for every later sync in the run, the plugin never
        # reads it back, and the metadata database is disposable per run.
        session.execute(
            delete(SerializedDagModel).where(SerializedDagModel.dag_id == record.dag_id)
        )
        dag_model = session.get(DagModel, record.dag_id)
        if dag_model is not None:
            session.delete(dag_model)
        session.commit()
        return

    from airflow.models.dag_version import DagVersion
    from airflow.models.dagbundle import DagBundleModel
    from airflow.models.dagcode import DagCode

    version_ids = list(
        session.scalars(
            select(DagVersion.id).where(
                DagVersion.dag_id == record.dag_id,
                DagVersion.bundle_name == record.bundle_name,
            )
        )
    )
    if version_ids:
        session.execute(
            delete(SerializedDagModel).where(SerializedDagModel.dag_version_id.in_(version_ids))
        )
        session.execute(delete(DagCode).where(DagCode.dag_version_id.in_(version_ids)))
        session.execute(delete(DagVersion).where(DagVersion.id.in_(version_ids)))

    dag_model = session.get(DagModel, record.dag_id)
    if dag_model is not None and dag_model.bundle_name == record.bundle_name:
        session.delete(dag_model)
        session.flush()

    if record.bundle_created:
        references = session.scalar(
            select(func.count())
            .select_from(DagModel)
            .where(DagModel.bundle_name == record.bundle_name)
        )
        if references == 0:
            session.execute(
                delete(DagBundleModel).where(DagBundleModel.name == record.bundle_name)
            )
    session.commit()


def cleanup_dag(record: DagPersistenceRecord) -> None:
    """Remove one fixture-owned Dag, deregister its authoring Dag, and close its session.

    A borrowed session (``session_owned=False``) is never touched here: the caller's
    fixture may already have finalized it by the time this teardown runs, so cleanup
    replaces it with a fresh owned session first. Every fixture write path commits, so
    the fresh session sees all owned rows. If even opening that session fails, the dead
    borrowed handle still receives no rollback or close.

    Parameters:
        record: DagPersistenceRecord identifying fixture-owned rows.

    Raises:
        DagCleanupError: Airflow cannot remove all owned metadata.
    """

    try:
        if not record.session_owned:
            record.session = open_dag_session(record.dag_id)
            record.session_owned = True
        _cleanup_dag(record)
    except Exception as error:
        if record.session_owned:
            record.session.rollback()
        raise DagCleanupError(
            f"Could not clean Airflow Dag metadata for '{record.dag_id}': {error}"
        ) from error
    finally:
        unregister_authoring_dag(record.dag_id)
        if record.session_owned:
            record.session.close()


__all__ = (
    "UNSET",
    "DagCleanupError",
    "DagPersistenceError",
    "DagPersistenceRecord",
    "DagRunCreationError",
    "TaskInstanceCreationError",
    "UnsetType",
    "build_dag",
    "cleanup_dag",
    "coerce_run_type",
    "create_dag_run",
    "custom_schedule_timetables",
    "empty_operator_class",
    "ensure_dag_absent",
    "ensure_shared_bundle",
    "expand_mapped_task_instances",
    "get_dag_model",
    "is_custom_timetable_instance",
    "open_dag_session",
    "persist_dag",
    "resync_dag",
    "select_task_instance",
    "task_is_mapped",
    "time_restriction_type",
)
