"""Public typing contracts for pytest-airflow-in-a-box fixtures.

References:
    https://docs.python.org/3/library/typing.html#typing.Protocol
    https://airflow.apache.org/docs/apache-airflow/stable/public-airflow-interface.html
"""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING, Any, Protocol

from pytest_airflow_in_a_box._compat.taskrun import DEFAULT_TRIGGER_TIMEOUT

if TYPE_CHECKING:
    from datetime import datetime

    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance
    from airflow.sdk import DAG
    from sqlalchemy.orm import Session

    from pytest_airflow_in_a_box.results import DagRunResult


class AirflowVariables(Protocol):
    """Seed fixture-owned Airflow Variable rows for one database-backed test."""

    def __call__(self, variables: Mapping[str, str]) -> None:
        """Commit one batch of Variables, removed when the test finishes.

        Parameters:
            variables: Mapping[str, str] containing Variable values by key.

        Raises:
            TypeError: The batch, a key, or a value has the wrong type.
            ValueError: A key is malformed, already present in the metadata
                database, or shadowed by an ``AIRFLOW_VAR_*`` variable.
        """


class AirflowConnections(Protocol):
    """Seed fixture-owned Airflow Connection rows for one database-backed test."""

    def __call__(self, connections: Mapping[str, Mapping[str, Any]]) -> None:
        """Commit one batch of Connections, removed when the test finishes.

        Fields are the flat shape ``run_task(connections=...)`` takes, so
        ``conn_type`` defaults to ``generic`` and ``extra`` is a JSON object
        string.

        Parameters:
            connections: Mapping[str, Mapping[str, Any]] containing connection
                fields by connection id.

        Raises:
            TypeError: The batch, a connection id, or a field has the wrong type.
            ValueError: A connection id or field is malformed, already present in
                the metadata database, or shadowed by an ``AIRFLOW_CONN_*``
                variable.
        """


class SerializedDag(Protocol):
    """Public structural view of a persisted scheduler Dag."""

    dag_id: str

    @property
    def task_ids(self) -> list[str]:
        """Return the task identifiers present in the persisted Dag."""

    def get_task(self, task_id: str) -> Any:
        """Return the task with the requested identifier."""


class DagMaker(Protocol):
    """Build and persist isolated Airflow Dags for one pytest test.

    Calling the fixture returns a context manager. The context always yields the mutable
    authoring Dag object -- ``airflow.sdk.DAG`` on the 3.x family,
    ``airflow.models.dag.DAG`` on the certified 2.x family -- so operators and decorated
    tasks can be defined naturally. Metadata is persisted only after a successful context
    exit. When ``serialized=True`` or the ``need_serialized_dag`` marker requests
    serialization, ``serialized_dag`` exposes Airflow's persisted scheduler
    representation after exit; no mutable proxy or task execution behavior is implied.
    Metadata remains available until the function-scoped fixture is finalized.
    """

    @property
    def dag(self) -> DAG:
        """Return the most recently created mutable authoring Dag."""

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
        """Create one context manager accepting authoring ``DAG`` keyword arguments.

        Parameters:
            dag_id: str | None containing an explicit bounded identifier, or ``None`` for a
                deterministic test- and worker-specific identifier.
            serialized: bool | None overriding the ``need_serialized_dag`` marker when supplied.
            dag_kwargs: Any containing keyword arguments forwarded to the installed
                family's authoring ``DAG`` constructor.

        Returns:
            contextlib.AbstractContextManager[airflow.sdk.DAG] yielding the mutable
            authoring Dag (the 2.x ``airflow.models.dag.DAG`` on that family).
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
        """Create and own one persisted running manual DagRun.

        Parameters:
            run_id: str | None containing an explicit identifier, or ``None`` for a
                derived one.
            logical_date: datetime.datetime | None overriding the current UTC logical
                date.
            run_after: datetime.datetime | None overriding the current UTC run-after
                date. Airflow 3.x only -- the 2.x family has no run-after concept, and
                passing it there raises ``ValueError`` rather than silently changing
                run semantics.
            start_date: datetime.datetime | None overriding the current UTC start date.
            dag_run_kwargs: Any forwarded to Airflow's scheduler Dag creation method.

        Raises:
            ValueError: ``run_after`` was passed on the Airflow 2.x family.
        """

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
        run_triggerer: bool = False,
        trigger_timeout: float = DEFAULT_TRIGGER_TIMEOUT,
    ) -> TaskInstance:
        """Create and execute one task instance through the compatibility shim."""

    def run(
        self,
        dag_run: DagRun | None = None,
        *,
        dag_run_kwargs: dict[str, Any] | None = None,
        run_triggerer: bool = False,
        trigger_timeout: float = DEFAULT_TRIGGER_TIMEOUT,
    ) -> DagRunResult:
        """Execute every task instance of one DagRun and return an inert snapshot.

        Parameters:
            dag_run: airflow.models.dagrun.DagRun | None created by this factory,
                or ``None`` to create one.
            dag_run_kwargs: dict[str, Any] | None used when creating an omitted DagRun.
            run_triggerer: bool running persisted trigger events and resuming deferrals.
            trigger_timeout: float seconds allowed for each trigger's first event.

        Returns:
            pytest_airflow_in_a_box.results.DagRunResult containing the settled outcome.
        """


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


class RenderTask(Protocol):
    """Render one operator's template fields in process without a metadata database."""

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
        try_number: int = 1,
        context_overrides: dict[str, Any] | None = None,
    ) -> Any:
        """Render one operator's template fields with seeded fake supervisor state.

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
            try_number: int selecting the synthetic task attempt number.
            context_overrides: dict[str, Any] | None merged into the
                synthesized template context before rendering.

        Returns:
            Any containing the same operator passed as `task`, mutated in
            place with resolved template-field values.
        """


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
        try_number: int = 1,
        run_callbacks: bool = False,
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
            try_number: int selecting the synthetic task attempt number.
            run_callbacks: bool dispatching task callbacks and listeners after
                execution.

        Returns:
            TaskRunResult containing terminal state, error, and XCom values.
        """


__all__ = (
    "AirflowConnections",
    "AirflowVariables",
    "DagMaker",
    "RenderTask",
    "RunTask",
    "SerializedDag",
    "TaskRunResult",
)
