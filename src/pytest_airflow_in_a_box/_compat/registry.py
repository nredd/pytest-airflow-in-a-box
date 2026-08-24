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
# Every `dag_id` this process has ever persisted -- a history, not a live set, so it is
# never drained: `cleanup_dag`'s `finally` always unregisters the authoring Dag even
# when row deletion fails, which makes `_AUTHORING_DAGS` useless for telling a leaked
# fixture-owned row apart from metadata this plugin never wrote. Per-process on
# purpose: another pytest-xdist worker's registration is indistinguishable from a live
# cross-worker collision and must keep failing loudly.
_PERSISTED_DAG_IDS: set[str] = set()


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


def record_persisted_dag_id(dag_id: str) -> None:
    """Record one ``dag_id`` this process successfully persisted.

    Entries are never removed: the set is ownership history, consumed by
    ``ensure_dag_registrable`` to tell a leaked fixture-owned row from metadata the
    plugin never wrote.

    Parameters:
        dag_id: str identifying the persisted Dag.
    """

    _PERSISTED_DAG_IDS.add(dag_id)


def was_dag_id_persisted(dag_id: str) -> bool:
    """Report whether this process ever persisted ``dag_id``.

    Parameters:
        dag_id: str identifying the prospective Dag.

    Returns:
        bool marking ``dag_id`` as one this process persisted at some point.
    """

    return dag_id in _PERSISTED_DAG_IDS


__all__ = (
    "lookup_authoring_dag",
    "record_persisted_dag_id",
    "register_authoring_dag",
    "unregister_authoring_dag",
    "was_dag_id_persisted",
)
