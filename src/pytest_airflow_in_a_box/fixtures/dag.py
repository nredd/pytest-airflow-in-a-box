"""Provide a function-scoped factory for isolated persisted Airflow Dags.

References:
    https://docs.pytest.org/en/stable/how-to/fixtures.html
    https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html
"""

from __future__ import annotations

import hashlib
import os
import re
from contextlib import AbstractContextManager
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from pytest_airflow_in_a_box._compat.dag import (
    DagCleanupError,
    DagPersistenceRecord,
    build_dag,
    cleanup_dag,
    create_dag_run,
    ensure_dag_absent,
    open_dag_session,
    persist_dag,
    select_task_instance,
)
from pytest_airflow_in_a_box._compat.taskrun import run_task_instance
from pytest_airflow_in_a_box.markers import read_bool_marker
from pytest_airflow_in_a_box.types import DagMaker, SerializedDag

if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import datetime
    from types import TracebackType

    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance
    from airflow.sdk import DAG
    from sqlalchemy.orm import Session

DAG_ID_MAX_LENGTH = 250
RUN_ID_MAX_LENGTH = 250
DAG_ID_PATTERN = re.compile(r"^[\w.-]+$")
DAG_ID_SANITIZER = re.compile(r"[^\w.-]+")
BUNDLE_PREFIX = "pytest-airflow-in-a-box"


def _validate_dag_id(dag_id: str) -> str:
    """Validate one explicit identifier before constructing Airflow objects.

    Parameters:
        dag_id: str containing the explicit identifier.

    Returns:
        str containing the validated identifier.

    Raises:
        TypeError: ``dag_id`` is not a string.
        ValueError: ``dag_id`` is empty, too long, or contains invalid characters.
    """

    if not isinstance(dag_id, str):
        raise TypeError(f"`dag_id` must be a string: '{dag_id}'")
    if not dag_id:
        raise ValueError("`dag_id` must be non-empty")
    if len(dag_id) > DAG_ID_MAX_LENGTH:
        raise ValueError(f"`dag_id` must not exceed {DAG_ID_MAX_LENGTH} characters: '{dag_id}'")
    if DAG_ID_PATTERN.fullmatch(dag_id) is None:
        raise ValueError(f"`dag_id` contains invalid characters: '{dag_id}'")
    return dag_id


def _default_dag_id(nodeid: str, worker: str, invocation: int) -> str:
    """Derive a bounded deterministic identifier from test and worker identity.

    Parameters:
        nodeid: str containing pytest's stable test identifier.
        worker: str containing the xdist worker identity or ``master``.
        invocation: int distinguishing repeated factory calls in one test.

    Returns:
        str containing a sanitized identifier no longer than Airflow's limit.
    """

    source = f"{nodeid}-{worker}-{invocation}"
    sanitized = DAG_ID_SANITIZER.sub("_", source).strip("._-") or "dag"
    digest = hashlib.sha256(source.encode()).hexdigest()[:16]
    prefix_length = DAG_ID_MAX_LENGTH - len(digest) - 1
    return f"{sanitized[:prefix_length]}-{digest}"


def _bundle_name(dag_id: str) -> str:
    """Return a bounded isolated bundle identity for one Dag.

    Parameters:
        dag_id: str containing the validated Dag identifier.

    Returns:
        str containing the fixture-owned bundle name.
    """

    digest = hashlib.sha256(dag_id.encode()).hexdigest()
    return f"{BUNDLE_PREFIX}-{digest}"


def _default_run_id(dag_id: str, invocation: int) -> str:
    """Derive a bounded deterministic run identifier for one factory call.

    Parameters:
        dag_id: str identifying the fixture-owned Dag.
        invocation: int distinguishing repeated run creation.

    Returns:
        str containing a collision-safe manual run identifier.
    """

    source = f"{dag_id}-{invocation}"
    digest = hashlib.sha256(source.encode()).hexdigest()[:16]
    prefix = "manual__pytest-airflow-in-a-box"
    available = RUN_ID_MAX_LENGTH - len(prefix) - len(digest) - 2
    return f"{prefix}-{dag_id[:available]}-{digest}"


class _DagContext(AbstractContextManager["DAG"]):
    """Own one Dag authoring context and metadata session."""

    def __init__(
        self,
        factory: _DagFactory,
        dag: DAG,
        dag_id: str,
        bundle_name: str,
        *,
        serialized: bool,
    ) -> None:
        """Store deferred resources for one context entry.

        Parameters:
            factory: _DagFactory receiving current state and successful records.
            dag: airflow.sdk.DAG containing the mutable authoring object.
            dag_id: str identifying the Dag.
            bundle_name: str identifying the isolated metadata bundle.
            serialized: bool indicating whether to expose the scheduler representation.
        """

        self._factory = factory
        self._dag = dag
        self._dag_id = dag_id
        self._bundle_name = bundle_name
        self._serialized = serialized
        self._record: DagPersistenceRecord | None = None

    def __enter__(self) -> DAG:
        """Open metadata ownership checks, then enter the SDK Dag context.

        Returns:
            airflow.sdk.DAG containing the mutable authoring object.

        Raises:
            RuntimeError: The context manager has already been entered.
        """

        if self._record is not None:
            raise RuntimeError("Dag context cannot be entered more than once")
        session = open_dag_session(self._dag_id)
        try:
            ensure_dag_absent(self._dag_id, session)
        except Exception:
            session.close()
            raise
        self._record = DagPersistenceRecord(
            dag_id=self._dag_id,
            bundle_name=self._bundle_name,
            session=session,
        )
        self._factory._set_active(self._dag, session)
        return self._dag.__enter__()

    def __exit__(
        self,
        error_type: type[BaseException] | None,
        error: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Persist a successful Dag context or discard a failed construction.

        Parameters:
            error_type: type[BaseException] | None raised by the context body.
            error: BaseException | None raised by the context body.
            traceback: types.TracebackType | None associated with the error.
        """

        record = self._record
        if record is None:
            raise RuntimeError("Dag context was exited before it was entered")
        try:
            self._dag.__exit__(error_type, error, traceback)
            if error_type is not None:
                return
            scheduler_dag = persist_dag(self._dag, record)
            self._factory._finish(
                record,
                scheduler_dag,
                scheduler_dag if self._serialized else None,
            )
            self._record = None
        finally:
            if self._record is not None:
                self._record.session.rollback()
                self._record.session.close()
                self._record = None


class _DagFactory:
    """Implement the public ``DagMaker`` protocol without importing Airflow eagerly."""

    def __init__(self, nodeid: str, fileloc: str, worker: str, *, serialized: bool) -> None:
        """Store deterministic identity inputs and marker defaults.

        Parameters:
            nodeid: str containing pytest's stable test identifier.
            fileloc: str naming the consumer test module.
            worker: str containing the xdist worker identity.
            serialized: bool containing the marker-derived default.
        """

        self._nodeid = nodeid
        self._fileloc = fileloc
        self._worker = worker
        self._default_serialized = serialized
        self._invocations = 0
        self._run_invocations = 0
        self._dag: DAG | None = None
        self._session: Session | None = None
        self._serialized_dag: SerializedDag | None = None
        self._scheduler_dag: SerializedDag | None = None
        self._current_record: DagPersistenceRecord | None = None
        self._records: list[DagPersistenceRecord] = []

    @property
    def dag(self) -> DAG:
        """Return the latest mutable SDK Dag.

        Returns:
            airflow.sdk.DAG containing the latest authoring object.

        Raises:
            RuntimeError: The factory has not been called.
        """

        if self._dag is None:
            raise RuntimeError("`dag_maker` has not created a Dag")
        return self._dag

    @property
    def session(self) -> Session:
        """Return the latest metadata session.

        Returns:
            sqlalchemy.orm.Session used by the latest entered context.

        Raises:
            RuntimeError: No Dag context has been entered.
        """

        if self._session is None:
            raise RuntimeError("`dag_maker` has not entered a Dag context")
        return self._session

    @property
    def serialized_dag(self) -> SerializedDag | None:
        """Return the requested persisted scheduler Dag.

        Returns:
            pytest_airflow_in_a_box.types.SerializedDag | None for the latest persisted Dag.
        """

        return self._serialized_dag

    def __call__(
        self,
        dag_id: str | None = None,
        *,
        serialized: bool | None = None,
        **dag_kwargs: Any,
    ) -> AbstractContextManager[DAG]:
        """Create one isolated Dag authoring context.

        Parameters:
            dag_id: str | None containing an explicit identifier or ``None`` for a derived one.
            serialized: bool | None overriding the marker-derived behavior.
            dag_kwargs: Any forwarded to ``airflow.sdk.DAG``.

        Returns:
            contextlib.AbstractContextManager[airflow.sdk.DAG] for task definition.

        Raises:
            TypeError: ``serialized`` is not a boolean or ``None``.
            ValueError: An explicit ``dag_id`` is invalid.
        """

        if serialized is not None and not isinstance(serialized, bool):
            raise TypeError(f"`serialized` must be a boolean or `None`: '{serialized}'")
        self._invocations += 1
        resolved_dag_id = (
            _default_dag_id(self._nodeid, self._worker, self._invocations)
            if dag_id is None
            else _validate_dag_id(dag_id)
        )
        dag = build_dag(resolved_dag_id, self._fileloc, dag_kwargs)
        self._dag = dag
        self._serialized_dag = None
        return _DagContext(
            self,
            dag,
            resolved_dag_id,
            _bundle_name(resolved_dag_id),
            serialized=self._default_serialized if serialized is None else serialized,
        )

    def _set_active(self, dag: DAG, session: Session) -> None:
        """Expose resources for the active context.

        Parameters:
            dag: airflow.sdk.DAG containing the active authoring object.
            session: sqlalchemy.orm.Session opened for its metadata.
        """

        self._dag = dag
        self._session = session

    def _finish(
        self,
        record: DagPersistenceRecord,
        scheduler_dag: SerializedDag,
        serialized_dag: SerializedDag | None,
    ) -> None:
        """Track one successful context for fixture finalization.

        Parameters:
            record: DagPersistenceRecord containing owned metadata.
            scheduler_dag: SerializedDag used internally for scheduler operations.
            serialized_dag: SerializedDag | None exposed by requested semantics.
        """

        self._records.append(record)
        self._current_record = record
        self._scheduler_dag = scheduler_dag
        self._serialized_dag = serialized_dag

    def _require_persisted(self) -> tuple[DagPersistenceRecord, SerializedDag]:
        """Return the latest persisted Dag resources.

        Returns:
            tuple[DagPersistenceRecord, SerializedDag] containing metadata ownership and the
            scheduler Dag.

        Raises:
            RuntimeError: No successful Dag context has been persisted.
        """

        if self._current_record is None or self._scheduler_dag is None:
            raise RuntimeError("`dag_maker` has not persisted a Dag")
        return self._current_record, self._scheduler_dag

    def create_dagrun(
        self,
        *,
        run_id: str | None = None,
        logical_date: datetime | None = None,
        run_after: datetime | None = None,
        start_date: datetime | None = None,
        **dag_run_kwargs: Any,
    ) -> DagRun:
        """Create one fixture-owned DagRun through the persisted scheduler Dag.

        Parameters:
            run_id: str | None containing an explicit identifier or ``None`` for a derived one.
            logical_date: datetime.datetime | None overriding the current UTC logical date.
            run_after: datetime.datetime | None overriding the current UTC run-after date.
            start_date: datetime.datetime | None overriding the current UTC start date.
            dag_run_kwargs: Any forwarded to Airflow's scheduler Dag creation method.

        Returns:
            airflow.models.dagrun.DagRun containing committed task instances.
        """

        record, scheduler_dag = self._require_persisted()
        self._run_invocations += 1
        resolved_run_id = (
            _default_run_id(record.dag_id, self._run_invocations) if run_id is None else run_id
        )
        return create_dag_run(
            scheduler_dag,
            self.dag,
            record,
            run_id=resolved_run_id,
            logical_date=logical_date,
            run_after=run_after,
            start_date=start_date,
            dag_run_kwargs=dag_run_kwargs,
        )

    def create_ti(
        self,
        task_id: str,
        dag_run: DagRun | None = None,
        *,
        dag_run_kwargs: dict[str, Any] | None = None,
        map_index: int = -1,
    ) -> TaskInstance:
        """Select and refresh one fixture-owned task instance.

        Parameters:
            task_id: str identifying the requested task.
            dag_run: airflow.models.dagrun.DagRun | None created by this factory.
            dag_run_kwargs: dict[str, Any] | None used when creating an omitted DagRun.
            map_index: int identifying the requested mapped task instance.

        Returns:
            airflow.models.taskinstance.TaskInstance refreshed from its authoring task.

        Raises:
            ValueError: Both ``dag_run`` and ``dag_run_kwargs`` are supplied or selection fails.
        """

        if dag_run is not None and dag_run_kwargs is not None:
            raise ValueError("`dag_run_kwargs` cannot be supplied with an existing `dag_run`")
        resolved_dag_run = dag_run or self.create_dagrun(**(dag_run_kwargs or {}))
        record, _ = self._require_persisted()
        return select_task_instance(
            self.dag,
            resolved_dag_run,
            record,
            task_id=task_id,
            map_index=map_index,
        )

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
        """Create and run one task instance through the compatibility shim.

        Parameters:
            task_id: str identifying the task to execute.
            dag_run: airflow.models.dagrun.DagRun | None created by this factory.
            dag_run_kwargs: dict[str, Any] | None used when creating an omitted DagRun.
            map_index: int identifying the requested mapped task instance.
            ignore_depends_on_past: bool controlling the depends-on-past dependency.
            ignore_task_deps: bool controlling task-specific dependencies.
            ignore_ti_state: bool controlling existing task-instance state checks.
            mark_success: bool marking success without executing the task body.

        Returns:
            airflow.models.taskinstance.TaskInstance containing refreshed persisted state.
        """

        ti = self.create_ti(
            task_id,
            dag_run,
            dag_run_kwargs=dag_run_kwargs,
            map_index=map_index,
        )
        return run_task_instance(
            ti,
            self.dag.get_task(task_id),
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state,
            mark_success=mark_success,
            session=self.session,
        )

    def close(self) -> None:
        """Clean every successfully persisted Dag in reverse creation order.

        Raises:
            DagCleanupError: One or more owned Dags could not be cleaned.
        """

        failures: list[Exception] = []
        while self._records:
            try:
                cleanup_dag(self._records.pop())
            except Exception as error:
                failures.append(error)
        if failures:
            details = "; ".join(str(error) for error in failures)
            raise DagCleanupError(
                f"Could not clean {len(failures)} fixture-owned Airflow Dags: {details}"
            ) from failures[0]


@pytest.fixture
def dag_maker(request: pytest.FixtureRequest) -> Iterator[DagMaker]:
    """Yield a function-scoped factory for isolated persisted Airflow Dags.

    Parameters:
        request: pytest.FixtureRequest containing marker and test identity metadata.

    Yields:
        pytest_airflow_in_a_box.types.DagMaker creating SDK Dag contexts.
    """

    marker_default = read_bool_marker(request.node, "need_serialized_dag", default=False)
    worker = os.environ.get("PYTEST_XDIST_WORKER", "master")
    fileloc = str(Path(str(request.node.path)).resolve())
    factory = _DagFactory(request.node.nodeid, fileloc, worker, serialized=marker_default)
    try:
        yield factory
    finally:
        factory.close()


__all__ = ("dag_maker",)
