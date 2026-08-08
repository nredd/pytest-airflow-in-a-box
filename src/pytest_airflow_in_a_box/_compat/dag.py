"""Construct, persist, and clean Dags across certified Airflow releases.

Airflow imports remain deferred until a test calls the fixture.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/public-airflow-interface.html
    https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-serialization.html
"""

from __future__ import annotations

from dataclasses import dataclass, field
from importlib import import_module
from typing import TYPE_CHECKING, Any

from pytest_airflow_in_a_box._compat.capabilities import resolve_capabilities

if TYPE_CHECKING:
    from datetime import datetime

    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance
    from airflow.sdk import DAG
    from sqlalchemy.orm import Session

    from pytest_airflow_in_a_box.types import SerializedDag


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
        bundle_created: bool indicating whether this fixture inserted the bundle row.
        dag_run_ids: set[int] containing exact fixture-owned DagRun primary keys.
        task_instance_keys: set[tuple[str, str, str, int]] containing owned task identities.
    """

    dag_id: str
    bundle_name: str
    session: Session
    bundle_created: bool = False
    dag_run_ids: set[int] = field(default_factory=set)
    task_instance_keys: set[tuple[str, str, str, int]] = field(default_factory=set)


def build_dag(dag_id: str, fileloc: str, dag_kwargs: dict[str, Any]) -> DAG:
    """Construct one public SDK Dag while keeping the Airflow import deferred.

    Parameters:
        dag_id: str containing a validated Dag identifier.
        fileloc: str naming the consumer test module.
        dag_kwargs: dict[str, Any] forwarded to the SDK constructor.

    Returns:
        airflow.sdk.DAG configured with stable file locations.
    """

    # Deferred to preserve pre-bootstrap plugin import safety.
    from airflow.sdk import DAG

    schedule = dag_kwargs.pop("schedule", None)
    dag = DAG(dag_id=dag_id, schedule=schedule, **dag_kwargs)
    dag.fileloc = fileloc
    dag.relative_fileloc = fileloc.rsplit("/", maxsplit=1)[-1]
    return dag


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


def _ensure_bundle(record: DagPersistenceRecord) -> None:
    """Create the fixture's isolated Dag bundle when absent.

    Parameters:
        record: DagPersistenceRecord receiving bundle ownership state.
    """

    from airflow.models.dagbundle import DagBundleModel

    if record.session.get(DagBundleModel, record.bundle_name) is not None:
        return
    record.session.add(DagBundleModel(name=record.bundle_name))
    record.session.flush()
    record.bundle_created = True


def _sync_dag_model(dag: DAG, record: DagPersistenceRecord) -> None:
    """Sync the Dag and its scheduler metadata through Airflow's canonical writer.

    Parameters:
        dag: airflow.sdk.DAG containing the completed task graph.
        record: DagPersistenceRecord identifying the isolated bundle.
    """

    serialized_dag_class = _get_serialized_dag_class()
    serialized_dag_class.bulk_write_to_db(
        record.bundle_name,
        None,
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
    from airflow.serialization.serialized_objects import LazyDeserializedDAG

    lazy_dag = LazyDeserializedDAG.from_dag(dag)
    SerializedDagModel.write_dag(
        lazy_dag,
        bundle_name=record.bundle_name,
        bundle_version=None,
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
        return _load_serialized_dag(record)
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


def create_dag_run(
    scheduler_dag: Any,
    authoring_dag: DAG,
    record: DagPersistenceRecord,
    *,
    run_id: str,
    logical_date: datetime | None,
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
        logical_date: datetime.datetime | None overriding the current UTC logical date.
        run_after: datetime.datetime | None overriding the current UTC run-after date.
        start_date: datetime.datetime | None overriding the current UTC start date.
        dag_run_kwargs: dict[str, Any] forwarded to Airflow's scheduler Dag.

    Returns:
        airflow.models.dagrun.DagRun committed with verified task instances.

    Raises:
        DagRunCreationError: Airflow cannot create or verify the DagRun metadata.
    """

    from airflow.models.dag_version import DagVersion
    from airflow.sdk.timezone import coerce_datetime, convert_to_utc, utcnow
    from airflow.utils.state import DagRunState
    from airflow.utils.types import DagRunTriggeredByType, DagRunType

    operation = "resolving UTC dates"
    try:
        now = utcnow()
        resolved_logical_date = convert_to_utc(coerce_datetime(logical_date or now))
        resolved_run_after = convert_to_utc(coerce_datetime(run_after or now))
        resolved_start_date = convert_to_utc(coerce_datetime(start_date or now))
        operation = "loading the current DagVersion"
        dag_version: Any = DagVersion.get_latest_version(record.dag_id, session=record.session)
        if dag_version is None:
            raise RuntimeError(
                f"Airflow did not return a current DagVersion for '{record.dag_id}'"
            )

        kwargs = dict(dag_run_kwargs)
        if "data_interval" not in kwargs:
            kwargs["data_interval"] = scheduler_dag.timetable.infer_manual_data_interval(
                run_after=resolved_logical_date
            )
        kwargs.setdefault("run_type", DagRunType.MANUAL)
        kwargs.setdefault("triggered_by", DagRunTriggeredByType.TEST)
        kwargs.setdefault("state", DagRunState.RUNNING)
        operation = "calling the persisted scheduler Dag's create_dagrun"
        dag_run: Any = scheduler_dag.create_dagrun(
            run_id=run_id,
            logical_date=resolved_logical_date,
            run_after=resolved_run_after,
            start_date=resolved_start_date,
            session=record.session,
            **kwargs,
        )
        if dag_run.created_dag_version_id != dag_version.id:
            raise RuntimeError(
                f"DagRun '{run_id}' linked DagVersion '{dag_run.created_dag_version_id}', "
                f"expected current version '{dag_version.id}'"
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


def expand_mapped_task_instances(task: Any, run_id: str, session: Session) -> None:
    """Expand a persisted mapped task for one DagRun when mapping applies."""

    if not getattr(task, "is_mapped", False):
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
    from airflow.models.dag_version import DagVersion
    from airflow.models.dagbundle import DagBundleModel
    from airflow.models.dagcode import DagCode
    from airflow.models.dagrun import DagRun
    from airflow.models.serialized_dag import SerializedDagModel
    from sqlalchemy import delete, func, select

    session = record.session
    session.rollback()
    for dag_run_id in record.dag_run_ids:
        dag_run = session.get(DagRun, dag_run_id)
        if dag_run is not None:
            session.delete(dag_run)
    session.flush()
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
    """Remove one fixture-owned Dag in foreign-key-safe order and close its session.

    Parameters:
        record: DagPersistenceRecord identifying fixture-owned rows.

    Raises:
        DagCleanupError: Airflow cannot remove all owned metadata.
    """

    try:
        _cleanup_dag(record)
    except Exception as error:
        record.session.rollback()
        raise DagCleanupError(
            f"Could not clean Airflow Dag metadata for '{record.dag_id}': {error}"
        ) from error
    finally:
        record.session.close()


__all__ = (
    "DagCleanupError",
    "DagPersistenceError",
    "DagPersistenceRecord",
    "DagRunCreationError",
    "TaskInstanceCreationError",
    "build_dag",
    "cleanup_dag",
    "create_dag_run",
    "ensure_dag_absent",
    "expand_mapped_task_instances",
    "open_dag_session",
    "persist_dag",
    "select_task_instance",
)
