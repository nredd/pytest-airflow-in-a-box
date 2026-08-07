"""Construct, persist, and clean Dags across certified Airflow releases.

Airflow imports remain deferred until a test calls the fixture.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/public-airflow-interface.html
    https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-serialization.html
"""

from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
from typing import TYPE_CHECKING, Any

from pytest_airflow_in_a_box._compat.capabilities import resolve_capabilities

if TYPE_CHECKING:
    from airflow.sdk import DAG
    from sqlalchemy.orm import Session

    from pytest_airflow_in_a_box.types import SerializedDag


class DagPersistenceError(RuntimeError):
    """Report failure to create or persist fixture-owned Airflow Dag metadata."""


class DagCleanupError(RuntimeError):
    """Report failure to remove fixture-owned Airflow Dag metadata."""


@dataclass
class DagPersistenceRecord:
    """Track the exact resources created by one successful Dag context.

    Parameters:
        dag_id: str identifying the fixture-owned Dag.
        bundle_name: str identifying its isolated bundle row.
        session: sqlalchemy.orm.Session used to persist and inspect metadata.
        bundle_created: bool indicating whether this fixture inserted the bundle row.
    """

    dag_id: str
    bundle_name: str
    session: Session
    bundle_created: bool = False


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


def _cleanup_dag(record: DagPersistenceRecord) -> None:
    """Delete only metadata carrying this record's Dag and bundle identities.

    Parameters:
        record: DagPersistenceRecord identifying fixture-owned rows.
    """

    from airflow.models.dag import DagModel
    from airflow.models.dag_version import DagVersion
    from airflow.models.dagbundle import DagBundleModel
    from airflow.models.dagcode import DagCode
    from airflow.models.serialized_dag import SerializedDagModel
    from sqlalchemy import delete, func, select

    session = record.session
    session.rollback()
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
    "build_dag",
    "cleanup_dag",
    "ensure_dag_absent",
    "open_dag_session",
    "persist_dag",
)
