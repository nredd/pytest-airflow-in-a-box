"""Public typing contracts for pytest-airflow-in-a-box fixtures.

References:
    https://docs.python.org/3/library/typing.html#typing.Protocol
    https://airflow.apache.org/docs/apache-airflow/stable/public-airflow-interface.html
"""

from __future__ import annotations

from contextlib import AbstractContextManager
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from datetime import datetime

    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance
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

    def create_dagrun(
        self,
        *,
        run_id: str | None = None,
        logical_date: datetime | None = None,
        run_after: datetime | None = None,
        start_date: datetime | None = None,
        **dag_run_kwargs: Any,
    ) -> DagRun:
        """Create and own one persisted running manual DagRun."""

    def create_ti(
        self,
        task_id: str,
        dag_run: DagRun | None = None,
        *,
        dag_run_kwargs: dict[str, Any] | None = None,
        map_index: int = -1,
    ) -> TaskInstance:
        """Select and refresh one task instance, creating its DagRun when omitted."""

    def run_ti(
        self,
        task_id: str,
        dag_run: DagRun | None = None,
        *,
        dag_run_kwargs: dict[str, Any] | None = None,
        map_index: int = -1,
        ignore_depends_on_past: bool = False,
        ignore_task_deps: bool = False,
        ignore_ti_state: bool = False,
        mark_success: bool = False,
    ) -> TaskInstance:
        """Create and execute one task instance through the compatibility shim."""


class TaskRunResult(Protocol):
    """Outcome of one DB-free in-process task execution."""

    @property
    def state(self) -> Any:
        """Return the terminal ``TaskInstanceState``."""

    @property
    def msg(self) -> Any | None:
        """Return the final supervisor message, when produced."""

    @property
    def error(self) -> BaseException | None:
        """Return the exception raised by the task, when it failed."""

    @property
    def xcoms(self) -> dict[str, Any]:
        """Return XCom values present after execution."""

    @property
    def sent(self) -> tuple[Any, ...]:
        """Return every supervisor message in send order."""


class RunTask(Protocol):
    """Execute one operator in process without a metadata database."""

    def __call__(
        self,
        task: Any,
        *,
        dag_id: str | None = None,
        run_id: str = "in-process-test",
        logical_date: datetime | None = None,
        params: dict[str, Any] | None = None,
        xcoms: dict[str, Any] | None = None,
        variables: dict[str, str] | None = None,
        connections: dict[str, dict[str, Any]] | None = None,
        map_index: int = -1,
    ) -> TaskRunResult:
        """Execute one operator with seeded fake supervisor state.

        Parameters:
            task: Any containing the Airflow operator or bound TaskFlow task.
            dag_id: str | None overriding the Dag identifier, or ``None`` to
                read it from the task's bound Dag.
            run_id: str identifying the synthetic manual run.
            logical_date: datetime | None pinning the run's logical date.
            params: dict[str, Any] | None overriding declared Dag params.
            xcoms: dict[str, Any] | None seeding XCom values by key.
            variables: dict[str, str] | None seeding Variable values by key.
            connections: dict[str, dict[str, Any]] | None seeding connection
                fields by connection id.
            map_index: int selecting the mapped task index.

        Returns:
            TaskRunResult containing terminal state, error, and XCom values.
        """


__all__ = ("DagMaker", "RunTask", "SerializedDag", "TaskRunResult")
