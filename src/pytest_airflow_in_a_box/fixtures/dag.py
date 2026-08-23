"""Provide function-scoped factories for isolated persisted Airflow Dags.

``dag_maker`` builds and owns its own Dag; ``run_dag`` adopts one already authored
elsewhere (typically pulled from ``dag_bag``) and drives it through the same
persist/create/execute pipeline.

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

from pytest_airflow_in_a_box._compat import ensure_database
from pytest_airflow_in_a_box._compat.capabilities import require_v3
from pytest_airflow_in_a_box._compat.components import timetable_lookup_resolves
from pytest_airflow_in_a_box._compat.dag import (
    UNSET,
    DagCleanupError,
    DagPersistenceRecord,
    UnsetType,
    build_dag,
    cleanup_dag,
    create_dag_run,
    custom_schedule_timetables,
    ensure_dag_absent,
    expand_mapped_task_instances,
    get_dag_model,
    open_dag_session,
    persist_dag,
    resync_dag,
    select_task_instance,
)
from pytest_airflow_in_a_box._compat.executor import (
    execute_dag_run_via_executor,
    relative_dag_path,
)
from pytest_airflow_in_a_box._compat.taskrun import (
    DEFAULT_TRIGGER_TIMEOUT,
    execute_dag_run,
    run_task_instance,
)
from pytest_airflow_in_a_box.bootstrap import get_bootstrap_state
from pytest_airflow_in_a_box.fixtures.components import register_schedule_timetable
from pytest_airflow_in_a_box.fixtures.dagbag import _dag_folder
from pytest_airflow_in_a_box.markers import read_bool_marker
from pytest_airflow_in_a_box.types import DagMaker, RunDag, SerializedDag

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from datetime import datetime
    from types import TracebackType

    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance
    from airflow.sdk import DAG
    from sqlalchemy.orm import Session

    from pytest_airflow_in_a_box.results import DagRunResult

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


def _close_records(records: list[DagPersistenceRecord]) -> None:
    """Clean every owned Dag in reverse creation order, aggregating failures.

    Shared teardown for both `_DagFactory.close` and `_DagRunner.close`.

    Parameters:
        records: list[DagPersistenceRecord] owned by the caller, drained in place.

    Raises:
        DagCleanupError: One or more owned Dags could not be cleaned.
    """

    failures: list[Exception] = []
    while records:
        try:
            cleanup_dag(records.pop())
        except Exception as error:
            failures.append(error)
    if failures:
        details = "; ".join(str(error) for error in failures)
        raise DagCleanupError(
            f"Could not clean {len(failures)} fixture-owned Airflow Dags: {details}"
        ) from failures[0]


class _DagContext(AbstractContextManager["DAG"]):
    """Own one Dag authoring context and metadata session."""

    def __init__(
        self,
        factory: _DagFactory,
        dag: DAG,
        dag_id: str,
        bundle_name: str,
        *,
        session: Session | None = None,
        bundle_version: str | None = None,
    ) -> None:
        """Store deferred resources for one context entry.

        Parameters:
            factory: _DagFactory receiving current state and successful records.
            dag: airflow.sdk.DAG containing the mutable authoring object.
            dag_id: str identifying the Dag.
            bundle_name: str identifying the metadata bundle.
            session: sqlalchemy.orm.Session | None borrowed from the caller for all
                metadata writes, or ``None`` to open a context-owned one on entry.
            bundle_version: str | None recorded on persisted 3.x metadata rows.
        """

        self._factory = factory
        self._dag = dag
        self._dag_id = dag_id
        self._bundle_name = bundle_name
        self._borrowed_session = session
        self._bundle_version = bundle_version
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
        session_owned = self._borrowed_session is None
        session = self._borrowed_session
        if session is None:
            session = open_dag_session(self._dag_id)
        try:
            ensure_dag_absent(self._dag_id, session)
        except Exception:
            if session_owned:
                session.close()
            raise
        self._record = DagPersistenceRecord(
            dag_id=self._dag_id,
            bundle_name=self._bundle_name,
            session=session,
            session_owned=session_owned,
            bundle_version=self._bundle_version,
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
            self._factory._finish(record, scheduler_dag)
            self._record = None
        finally:
            if self._record is not None:
                if self._record.session_owned:
                    self._record.session.rollback()
                    self._record.session.close()
                self._record = None


class _DagFactory:
    """Implement the public ``DagMaker`` protocol without importing Airflow eagerly."""

    def __init__(
        self,
        nodeid: str,
        fileloc: str,
        worker: str,
        *,
        register_timetable: Callable[[Any], None] | None = None,
    ) -> None:
        """Store deterministic identity inputs.

        Parameters:
            nodeid: str containing pytest's stable test identifier.
            fileloc: str naming the consumer test module.
            worker: str containing the xdist worker identity.
            register_timetable: Callable[[Any], None] | None registering one custom
                timetable instance before its Dag is built, or None to skip
                registration entirely (direct construction in unit tests).
        """

        self._register_timetable = register_timetable
        self._nodeid = nodeid
        self._fileloc = fileloc
        self._worker = worker
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
        """Return the latest persisted scheduler Dag.

        Returns:
            pytest_airflow_in_a_box.types.SerializedDag | None for the latest persisted
            Dag, or ``None`` while no Dag has been persisted yet -- inside an open
            context, or before the first successful context exit.
        """

        return self._serialized_dag

    @property
    def dag_model(self) -> Any:
        """Return the live ``DagModel`` metadata row for the latest persisted Dag.

        Returns:
            pytest_airflow_in_a_box.types.DagModelRow attached to ``session``. Reads
            observe committed scheduler metadata; mutations are visible to Airflow.

        Raises:
            RuntimeError: No successful Dag context has been persisted.
        """

        record, _ = self._require_persisted()
        return get_dag_model(record)

    def sync_dagbag_to_db(self) -> SerializedDag:
        """Re-persist the current authoring Dag's scheduler metadata.

        The upstream ``tests_common`` mutate-then-resync shape: after the test body
        mutates ``dag``, re-run the persistence sequence so metadata rows reflect
        the mutation, then refresh and return ``serialized_dag``. Commits the
        metadata session -- on a borrowed ``session=`` that also commits anything
        the caller had staged. On the Airflow 3.x family each resync records a new
        DagVersion; DagRuns created before the resync keep their original version.

        Returns:
            pytest_airflow_in_a_box.types.SerializedDag reloaded from the
            re-committed metadata.

        Raises:
            RuntimeError: No successful Dag context has been persisted.
            pytest_airflow_in_a_box._compat.dag.DagPersistenceError: A persistence
                operation failed; the Dag's rows stay in place for fixture teardown.
        """

        record, _ = self._require_persisted()
        scheduler_dag = resync_dag(self.dag, record)
        self._scheduler_dag = scheduler_dag
        self._serialized_dag = scheduler_dag
        return scheduler_dag

    def __call__(
        self,
        dag_id: str | None = None,
        *,
        serialized: bool | None = None,
        session: Session | None = None,
        bundle_name: str | None = None,
        bundle_version: str | None = None,
        **dag_kwargs: Any,
    ) -> AbstractContextManager[DAG]:
        """Create one isolated Dag authoring context.

        The ``session``, ``bundle_name``, and ``bundle_version`` keywords mirror
        upstream ``tests_common``'s ``dag_maker`` harness contract: they route to the
        persistence layer and are never forwarded to the ``DAG`` constructor.

        Parameters:
            dag_id: str | None containing an explicit identifier or ``None`` for a derived one.
            serialized: bool | None accepted for upstream ``tests_common`` contract
                compatibility. Every Dag is serialized as part of persistence, so the
                flag (and the ``need_serialized_dag`` marker) no longer changes
                behavior.
            session: sqlalchemy.orm.Session | None used for all of this context's
                metadata writes and exposed as ``dag_maker.session``, or ``None`` to
                open a fixture-owned one. A supplied session is never closed by the
                fixture, but persistence commits on it -- including anything the
                caller had staged.
            bundle_name: str | None overriding the derived per-Dag bundle row name.
                Supplying one opts out of the unique-name xdist mitigation. Ignored
                by the 2.x family, which predates bundles.
            bundle_version: str | None recorded on persisted 3.x metadata rows, or
                ``None`` for unversioned. Ignored by the 2.x family.
            dag_kwargs: Any forwarded to ``airflow.sdk.DAG``.

        Returns:
            contextlib.AbstractContextManager[airflow.sdk.DAG] for task definition.

        Raises:
            TypeError: ``serialized`` is not a boolean or ``None``, ``session`` is not
                a SQLAlchemy session or ``None``, ``bundle_name`` or ``bundle_version``
                is not a string or ``None``, or ``schedule`` is a custom timetable
                class rather than an instance.
            ValueError: An explicit ``dag_id`` is invalid, or ``bundle_name`` is empty.
        """

        if serialized is not None and not isinstance(serialized, bool):
            raise TypeError(f"`serialized` must be a boolean or `None`: '{serialized}'")
        if session is not None:
            # Deferred with the Airflow imports: SQLAlchemy arrives with Airflow, not
            # with this plugin.
            from sqlalchemy.orm import Session as OrmSession

            if not isinstance(session, OrmSession):
                raise TypeError(f"`session` must be a SQLAlchemy session or `None`: '{session}'")
        if bundle_name is not None:
            if not isinstance(bundle_name, str):
                raise TypeError(f"`bundle_name` must be a string or `None`: '{bundle_name}'")
            if not bundle_name:
                raise ValueError("`bundle_name` must be non-empty")
        if bundle_version is not None and not isinstance(bundle_version, str):
            raise TypeError(f"`bundle_version` must be a string or `None`: '{bundle_version}'")
        self._invocations += 1
        resolved_dag_id = (
            _default_dag_id(self._nodeid, self._worker, self._invocations)
            if dag_id is None
            else _validate_dag_id(dag_id)
        )
        # Before `build_dag`, not at context exit: Airflow 3.1's `encode_timetable`
        # already refuses an unregistered custom timetable when `persist_dag` runs.
        # The collector runs even with no hook wired, so its custom-timetable-CLASS
        # guard names that mistake for direct `_DagFactory` users too.
        for timetable in custom_schedule_timetables(dag_kwargs.get("schedule")):
            if self._register_timetable is not None:
                self._register_timetable(timetable)
        dag = build_dag(resolved_dag_id, self._fileloc, dag_kwargs)
        self._dag = dag
        self._serialized_dag = None
        return _DagContext(
            self,
            dag,
            resolved_dag_id,
            bundle_name if bundle_name is not None else _bundle_name(resolved_dag_id),
            session=session,
            bundle_version=bundle_version,
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
    ) -> None:
        """Track one successful context for fixture finalization.

        Parameters:
            record: DagPersistenceRecord containing owned metadata.
            scheduler_dag: SerializedDag persisted at context exit, exposed as
                ``serialized_dag`` and used for scheduler operations.
        """

        self._records.append(record)
        self._current_record = record
        self._scheduler_dag = scheduler_dag
        self._serialized_dag = scheduler_dag

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
        logical_date: datetime | UnsetType | None = UNSET,
        run_after: datetime | None = None,
        start_date: datetime | None = None,
        **dag_run_kwargs: Any,
    ) -> DagRun:
        """Create one fixture-owned DagRun through the persisted scheduler Dag.

        Parameters:
            run_id: str | None containing an explicit identifier or ``None`` for a derived one.
            logical_date: datetime.datetime | UnsetType | None overriding the current
                UTC logical date. An explicit ``None`` requests a run with no logical
                date at all (the shape asset-triggered runs take) -- Airflow 3.x only,
                and no ``data_interval`` is inferred for it; rejected with
                ``ValueError`` on the 2.x family, which cannot express one.
            run_after: datetime.datetime | None overriding the current UTC run-after
                date; rejected with `ValueError` on the Airflow 2.x family, which has
                no run-after concept.
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
        record, scheduler_dag = self._require_persisted()
        if map_index >= 0:
            scheduler_task = scheduler_dag.get_task(task_id)
            expand_mapped_task_instances(
                scheduler_task,
                str(resolved_dag_run.run_id),
                record.session,
            )
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
        run_triggerer: bool = False,
        trigger_timeout: float = DEFAULT_TRIGGER_TIMEOUT,
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
            run_triggerer: bool running one persisted trigger event and resuming deferral.
            trigger_timeout: float seconds allowed for the persisted trigger's first event.

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
            run_triggerer=run_triggerer,
            trigger_timeout=trigger_timeout,
            session=self.session,
        )

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

        Raises:
            ValueError: Both ``dag_run`` and ``dag_run_kwargs`` are supplied, or the
                supplied ``dag_run`` is not owned by the factory's current Dag.
        """

        if dag_run is not None and dag_run_kwargs is not None:
            raise ValueError("`dag_run_kwargs` cannot be supplied with an existing `dag_run`")
        record, _scheduler_dag = self._require_persisted()
        if dag_run is not None and dag_run.id not in record.dag_run_ids:
            raise ValueError(
                f"DagRun '{dag_run.run_id}' is not owned by `dag_maker` for Dag '{record.dag_id}'"
            )
        resolved_dag_run = dag_run or self.create_dagrun(**(dag_run_kwargs or {}))
        return execute_dag_run(
            resolved_dag_run,
            self.dag,
            session=self.session,
            run_triggerer=run_triggerer,
            trigger_timeout=trigger_timeout,
        )

    def close(self) -> None:
        """Clean every successfully persisted Dag in reverse creation order.

        Raises:
            DagCleanupError: One or more owned Dags could not be cleaned.
        """

        _close_records(self._records)


def _executor_timeout(config: pytest.Config) -> float:
    """Read the per-instance settle timeout for an executor-driven run, in seconds.

    `--airflow-executor-timeout` wins over the `airflow_executor_timeout` ini option,
    matching every other paired option this plugin registers.

    Parameters:
        config: pytest.Config containing the option and ini values.

    Returns:
        float containing the settle timeout in seconds.

    Raises:
        pytest.UsageError: The configured value is not a positive number.
    """

    command_line: object = config.getoption("airflow_executor_timeout")
    source = "Option `--airflow-executor-timeout`"
    if command_line is None:
        command_line = config.getini("airflow_executor_timeout")
        source = "Ini option `airflow_executor_timeout`"
    if not isinstance(command_line, str):
        raise pytest.UsageError(f"{source} must be a number")
    try:
        timeout = float(command_line)
    except ValueError as error:
        raise pytest.UsageError(f"{source} must be a number: '{command_line}'") from error
    if timeout <= 0:
        raise pytest.UsageError(f"{source} must be positive: '{command_line}'")
    return timeout


class _DagRunner:
    """Implement the public ``RunDag`` protocol for externally-authored Dags."""

    def __init__(self, request: pytest.FixtureRequest) -> None:
        """Initialize empty invocation and ownership tracking.

        Parameters:
            request: pytest.FixtureRequest used to reach the live api-server fixture
                and the executor timeout option when an executor-driven run asks for
                them. Nothing is looked up here, so a run that never passes
                ``executor`` starts no server and reads no option.
        """

        self._request = request
        self._invocations = 0
        self._records: list[DagPersistenceRecord] = []

    def __call__(
        self,
        dag: DAG,
        *,
        run_id: str | None = None,
        logical_date: datetime | UnsetType | None = UNSET,
        run_after: datetime | None = None,
        start_date: datetime | None = None,
        dag_run_kwargs: dict[str, Any] | None = None,
        executor: str | type | object | None = None,
        run_triggerer: bool = False,
        trigger_timeout: float = DEFAULT_TRIGGER_TIMEOUT,
    ) -> DagRunResult:
        """Persist ``dag``, create a manual DagRun, and execute every task instance.

        Parameters:
            dag: airflow.sdk.DAG containing the completed, externally-authored task graph.
            run_id: str | None containing an explicit identifier, or ``None`` for a
                derived one.
            logical_date: datetime.datetime | UnsetType | None overriding the current
                UTC logical date. An explicit ``None`` requests a run with no logical
                date at all -- Airflow 3.x only; rejected with ``ValueError`` on the
                2.x family.
            run_after: datetime.datetime | None overriding the current UTC run-after date;
                rejected on the 2.x family, which has no run-after concept.
            start_date: datetime.datetime | None overriding the current UTC start date.
            dag_run_kwargs: dict[str, Any] | None forwarded to Airflow's scheduler Dag
                creation method.
            executor: str | type | object | None selecting an executor to run the tasks
                through -- an alias registered via ``airflow_components.executor``, a
                dotted import path, a ``BaseExecutor`` subclass, or an instance.
                ``None``, the default, runs every task in this process instead.
            run_triggerer: bool running persisted trigger events and resuming deferrals.
            trigger_timeout: float seconds allowed for each trigger's first event.

        Returns:
            pytest_airflow_in_a_box.results.DagRunResult containing the settled outcome.

        Raises:
            ValueError: ``dag.dag_id`` already has persisted Dag metadata, or
                ``run_triggerer`` was combined with ``executor``.
            ExecutorRunError: ``executor`` cannot be resolved or driven to a result, or
                ``dag`` is not defined in a file inside the Dag folder.
            ComponentContractError: ``executor``'s class shape is broken.
        """

        if executor is not None:
            if run_triggerer:
                raise ValueError(
                    "`run_triggerer` cannot be combined with `executor`: resuming a "
                    "deferred task is a triggerer's job, not an executor's, and an "
                    "executor-driven run settles a deferring instance as `deferred`. "
                    "Drop one of the two."
                )
            require_v3(
                "run_dag(executor=...)",
                "Airflow 2.x executors predate AIP-72: they take a CLI command list "
                "rather than a workload, and there is no Task Execution API for a task "
                "worker to report to. Drop `executor` to run the tasks in-process.",
            )
            # Ahead of `open_dag_session`, so a Dag no task worker could ever re-import
            # is refused before this run writes any metadata to clean up afterwards.
            relative_dag_path(dag, _dag_folder(self._request.config))
        dag_id = dag.dag_id
        session = open_dag_session(dag_id)
        try:
            ensure_dag_absent(dag_id, session)
        except Exception:
            session.close()
            raise
        record = DagPersistenceRecord(
            dag_id=dag_id,
            bundle_name=_bundle_name(dag_id),
            session=session,
        )
        try:
            scheduler_dag = persist_dag(dag, record)
        except Exception:
            session.close()
            raise
        self._records.append(record)
        self._invocations += 1
        resolved_run_id = _default_run_id(dag_id, self._invocations) if run_id is None else run_id
        dag_run = create_dag_run(
            scheduler_dag,
            dag,
            record,
            run_id=resolved_run_id,
            logical_date=logical_date,
            run_after=run_after,
            start_date=start_date,
            dag_run_kwargs=dag_run_kwargs or {},
        )
        if executor is None:
            return execute_dag_run(
                dag_run,
                dag,
                session=session,
                run_triggerer=run_triggerer,
                trigger_timeout=trigger_timeout,
            )
        return execute_dag_run_via_executor(
            dag_run,
            dag,
            executor=executor,
            # Resolved before the configuration overrides go up: the api-server is a
            # session-scoped subprocess that inherits the environment live at startup,
            # so starting it inside `airflow_config` would leak the overrides into it.
            api_server_url=self._request.getfixturevalue("api_server_url"),
            dags_folder=_dag_folder(self._request.config),
            bundle_name=record.bundle_name,
            session=session,
            timeout=_executor_timeout(self._request.config),
        )

    def close(self) -> None:
        """Clean every successfully persisted Dag in reverse creation order.

        Raises:
            DagCleanupError: One or more owned Dags could not be cleaned.
        """

        _close_records(self._records)


@pytest.fixture
def dag_maker(request: pytest.FixtureRequest) -> Iterator[DagMaker]:
    """Yield a function-scoped factory for isolated persisted Airflow Dags.

    Parameters:
        request: pytest.FixtureRequest containing marker and test identity metadata.

    Yields:
        pytest_airflow_in_a_box.types.DagMaker creating SDK Dag contexts.
    """

    ensure_database(get_bootstrap_state(request.config).root)
    # Exposure no-op since every Dag serializes at persistence (docs/adr/0002), read
    # so a malformed marker argument still fails loudly.
    read_bool_marker(request.node, "need_serialized_dag", default=False)
    worker = os.environ.get("PYTEST_XDIST_WORKER", "master")
    fileloc = str(Path(str(request.node.path)).resolve())

    def register_timetable(timetable: Any) -> None:
        """Register one custom timetable through the lazily-pulled component sandbox.

        A class the registered-timetable lookup ALREADY resolves -- deployed the
        supported way, via the run's plugins folder or a venv entry point -- is left
        alone entirely: no sandbox, no gate, no behavior change for setups that
        worked before this hook existed. Otherwise `getfixturevalue` on purpose, not
        a fixture parameter: pytest caches the resolved fixture, so every `dag_maker`
        call in one test shares a single sandbox (and its snapshot/finalize cleanup),
        while tests passing no custom timetable -- the overwhelming majority -- never
        construct the sandbox at all. Registration then goes through
        `register_schedule_timetable`'s registration-scoped gate rather than the full
        `ComponentRegistry.timetable` conformance gate.

        Parameters:
            timetable: Any containing the custom `Timetable` instance to register.
        """

        if timetable_lookup_resolves(type(timetable)):
            return
        request.getfixturevalue("airflow_components")
        register_schedule_timetable(timetable)

    factory = _DagFactory(
        request.node.nodeid,
        fileloc,
        worker,
        register_timetable=register_timetable,
    )
    try:
        yield factory
    finally:
        factory.close()


@pytest.fixture
def run_dag(request: pytest.FixtureRequest) -> Iterator[RunDag]:
    """Yield a function-scoped runner for externally-authored Airflow Dags.

    Parameters:
        request: pytest.FixtureRequest containing bootstrap state.

    Yields:
        pytest_airflow_in_a_box.types.RunDag executing adopted Dag objects.
    """

    ensure_database(get_bootstrap_state(request.config).root)
    runner = _DagRunner(request)
    try:
        yield runner
    finally:
        runner.close()


__all__ = ("dag_maker", "run_dag")
