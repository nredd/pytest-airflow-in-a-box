"""Track authoring SDK Dags persisted by fixture-owned contexts.

The registry maps ``dag_id`` to the executable authoring Dag so task resolution
works for task instances queried through sessions the factory does not own. The
store is per-process, matching pytest-xdist worker and pytester subprocess
isolation.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/public-airflow-interface.html
    https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-serialization.html
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.sdk import DAG

_AUTHORING_DAGS: dict[str, DAG] = {}


def register_authoring_dag(dag_id: str, dag: DAG) -> None:
    """Register one persisted authoring Dag, replacing any existing entry.

    Parameters:
        dag_id: str identifying the persisted Dag.
        dag: airflow.sdk.DAG containing executable task objects.
    """

    _AUTHORING_DAGS[dag_id] = dag


def lookup_authoring_dag(dag_id: str) -> DAG | None:
    """Return the registered authoring Dag when one exists.

    Parameters:
        dag_id: str identifying the requested Dag.

    Returns:
        airflow.sdk.DAG | None registered for ``dag_id``.
    """

    return _AUTHORING_DAGS.get(dag_id)


def unregister_authoring_dag(dag_id: str) -> None:
    """Remove one registered authoring Dag, tolerating absent entries.

    Parameters:
        dag_id: str identifying the Dag to remove.
    """

    _AUTHORING_DAGS.pop(dag_id, None)


__all__ = (
    "lookup_authoring_dag",
    "register_authoring_dag",
    "unregister_authoring_dag",
)
