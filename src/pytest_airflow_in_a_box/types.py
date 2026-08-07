"""Public typing contracts for pytest-airflow-in-a-box fixtures.

References:
    https://docs.python.org/3/library/typing.html#typing.Protocol
    https://airflow.apache.org/docs/apache-airflow/stable/public-airflow-interface.html
"""

from __future__ import annotations

from contextlib import AbstractContextManager
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from airflow.sdk import DAG
    from sqlalchemy.orm import Session


class SerializedDag(Protocol):
    """Public structural view of a persisted scheduler Dag."""

    dag_id: str

    @property
    def task_ids(self) -> list[str]:
        """Return the task identifiers present in the persisted Dag."""


class DagMaker(Protocol):
    """Build and persist isolated Airflow Dags for one pytest test.

    Calling the fixture returns a context manager. The context always yields the mutable
    ``airflow.sdk.DAG`` authoring object so operators and decorated tasks can be defined naturally.
    Metadata is persisted only after a successful context exit. When ``serialized=True`` or the
    ``need_serialized_dag`` marker requests serialization, ``serialized_dag`` exposes Airflow's
    persisted scheduler representation after exit; no mutable proxy or task execution behavior is
    implied. Metadata remains available until the function-scoped fixture is finalized.
    """

    @property
    def dag(self) -> DAG:
        """Return the most recently created mutable SDK Dag."""

    @property
    def session(self) -> Session:
        """Return the metadata session for the active or most recently persisted Dag."""

    @property
    def serialized_dag(self) -> SerializedDag | None:
        """Return the requested persisted scheduler Dag, or ``None`` when not requested."""

    def __call__(
        self,
        dag_id: str | None = None,
        *,
        serialized: bool | None = None,
        **dag_kwargs: Any,
    ) -> AbstractContextManager[DAG]:
        """Create one context manager accepting SDK ``DAG`` keyword arguments.

        Parameters:
            dag_id: str | None containing an explicit bounded identifier, or ``None`` for a
                deterministic test- and worker-specific identifier.
            serialized: bool | None overriding the ``need_serialized_dag`` marker when supplied.
            dag_kwargs: Any containing keyword arguments forwarded to ``airflow.sdk.DAG``.

        Returns:
            contextlib.AbstractContextManager[airflow.sdk.DAG] yielding the mutable authoring Dag.
        """


__all__ = ("DagMaker", "SerializedDag")
