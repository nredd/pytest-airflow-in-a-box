"""Public typing contracts for pytest-airflow-in-a-box fixtures.

References:
    https://docs.python.org/3/library/typing.html#typing.Protocol
    https://airflow.apache.org/docs/apache-airflow/stable/public-airflow-interface.html
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING, Any, Protocol

from pytest_airflow_in_a_box._compat.taskrun import DEFAULT_TRIGGER_TIMEOUT
from pytest_airflow_in_a_box.config import ConfigOverrides, EnvOverrides

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


class AirflowConfigure(Protocol):
    """Apply Airflow configuration overrides for the remainder of the test session."""

    def __call__(
        self,
        overrides: ConfigOverrides | None = None,
        *,
        env: EnvOverrides | None = None,
        refresh_settings: bool = False,
    ) -> None:
        """Apply one batch of overrides, restored at session teardown.

        Parameters:
            overrides: ConfigOverrides | None mapping ``(section, key)`` pairs to
                replacement values, where ``None`` requests the option's absence.
            env: EnvOverrides | None mapping plain variable names to replacement
                values, where ``None`` requests the variable's absence.
            refresh_settings: bool recomputing the ``airflow.settings``
                configuration globals after applying and after restoring.

        Raises:
            pytest.UsageError: An argument, name, or value is malformed.
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
            ValueError: ``run_after`` was passed on the Airflow 2.x family, or
                ``dag_run_kwargs`` sets a key ``create_dagrun`` already supplies from its
                own parameters (e.g. ``session``, ``execution_date``) -- see the message
                for the exact remedy.
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


class RunDag(Protocol):
    """Persist and execute one externally-authored Dag through a full DagRun.

    Unlike ``DagMaker``, which always builds its own Dag, ``RunDag`` adopts a Dag the
    caller already authored elsewhere -- typically one pulled from ``dag_bag`` -- and
    drives it through the same persist/create/execute pipeline, keyed on the Dag's own
    ``dag_id`` rather than a synthetic one. Because the real ``dag_id`` is preserved, two
    tests exercising the same ``dag_id`` concurrently on different ``pytest-xdist`` workers
    can race on the shared metadata database -- not always cleanly: the loser may raise
    before persisting, or the two workers may silently share one bundle row and have
    whichever tears down first delete metadata the other's still-running test depends on.
    Keep such tests on the same worker (e.g. via ``pytest.mark.xdist_group``) or accept that
    they run serially. Metadata is cleaned up when the function-scoped fixture is finalized.
    """

    def __call__(
        self,
        dag: DAG,
        *,
        run_id: str | None = None,
        logical_date: datetime | None = None,
        run_after: datetime | None = None,
        start_date: datetime | None = None,
        dag_run_kwargs: dict[str, Any] | None = None,
        executor: str | type | object | None = None,
        run_triggerer: bool = False,
        trigger_timeout: float = DEFAULT_TRIGGER_TIMEOUT,
    ) -> DagRunResult:
        """Persist ``dag``, create a manual DagRun, and execute every task instance.

        Parameters:
            dag: airflow.sdk.DAG containing the completed, externally-authored task graph
                (the 2.x ``airflow.models.dag.DAG`` on that family).
            run_id: str | None containing an explicit identifier, or ``None`` for a
                derived one.
            logical_date: datetime.datetime | None overriding the current UTC logical
                date.
            run_after: datetime.datetime | None overriding the current UTC run-after
                date. Airflow 3.x only -- the 2.x family has no run-after concept, and
                passing it there raises ``ValueError`` rather than silently changing run
                semantics.
            start_date: datetime.datetime | None overriding the current UTC start date.
            dag_run_kwargs: dict[str, Any] | None forwarded to Airflow's scheduler Dag
                creation method.
            executor: str | type | object | None selecting an executor to run the tasks
                through, instead of running them in this process. Accepts an alias
                registered via ``airflow_components.executor``, a dotted import path, a
                ``BaseExecutor`` subclass, or an instance. Airflow 3.x only, and it
                changes three things worth knowing about, all inherent to tasks running
                in another process:

                * ``dag`` must be defined in a file inside the folder ``dag_bag``
                  parses, because each task is re-imported from that file in a worker
                  subprocess. A ``dag_maker`` Dag lives in a test body and cannot
                  qualify.
                * ``result.errors`` carries only what the executor itself attached to a
                  failure. The task's traceback is in the worker's log under the run's
                  logs folder; ``result.states`` stays authoritative either way.
                * Instances are dispatched one at a time, in dependency-safe order, so
                  an executor's own concurrency is not exercised.

                Starting the live api-server the workers report to happens lazily on the
                first executor-driven call, exactly as ``api_client`` starts it.
            run_triggerer: bool running persisted trigger events and resuming deferrals.
                Cannot be combined with ``executor``.
            trigger_timeout: float seconds allowed for each trigger's first event.

        Returns:
            pytest_airflow_in_a_box.results.DagRunResult containing the settled outcome.

        Raises:
            ValueError: ``dag.dag_id`` already has persisted metadata, ``run_after`` was
                passed on the Airflow 2.x family, ``run_triggerer`` was combined with
                ``executor``, or ``dag_run_kwargs`` sets a key this method already
                supplies from its own parameters (e.g. ``session``, ``execution_date``) --
                see the message for the exact remedy.
            ExecutorRunError: ``executor`` cannot be resolved, started, or driven to a
                result within ``--airflow-executor-timeout``, or ``dag`` is not defined
                in a file inside the Dag folder.
            ComponentContractError: ``executor``'s class shape is broken in a way that
                would fail mid-run.
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
            dag_id: str | None overriding a bound Dag's identifier or naming
                the synthetic Dag auto-created and bound in place when the
                task is unbound, or ``None`` to read the bound Dag -- deriving
                a deterministic per-test, xdist-safe identifier when there is
                none.
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
            Any containing the rendered operator: a `prepare_for_execution()` copy
            of `task` for a plain operator, or the concrete unmapped instance for a
            mapped one. Never the exact `task` object passed in -- rendering never
            mutates the original (binding an unbound task in place to a synthetic
            Dag is the one side effect), so always use the return value.
        """


class TaskContextHandle(Protocol):
    """Live handle over one installed in-process task context."""

    @property
    def ti(self) -> Any:
        """Return the real ``RuntimeTaskInstance`` behind ``context["ti"]``."""

    @property
    def context(self) -> Any:
        """Return the synthesized Task SDK template context mapping."""

    @property
    def task(self) -> Any:
        """Return the execution-time operator copy to drive.

        Always drive this copy, never the operator passed in -- for a mapped
        operator this tracks Airflow's own swap to the concrete unmapped instance.
        """

    @property
    def xcoms(self) -> dict[str, Any]:
        """Return a snapshot of XCom values held by the fake supervisor."""

    @property
    def sent(self) -> tuple[Any, ...]:
        """Return a snapshot of every supervisor message in send order."""


class TaskContext(Protocol):
    """Open one Task SDK template context in process without a metadata database."""

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
        render: bool = True,
    ) -> AbstractContextManager[TaskContextHandle]:
        """Open one template context with seeded fake supervisor state.

        Parameters:
            task: Any containing the Airflow operator or bound TaskFlow task.
            dag_id: str | None overriding a bound Dag's identifier or naming
                the synthetic Dag auto-created and bound in place when the
                task is unbound, or ``None`` to read the bound Dag -- deriving
                a deterministic per-test, xdist-safe identifier when there is
                none.
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
            render: bool pre-rendering template fields like a real run. Pass
                ``False`` for operators that call
                ``context["ti"].render_templates()`` inside ``execute()``.

        Returns:
            contextlib.AbstractContextManager[TaskContextHandle] yielding the
            handle. The fake supervisor stays installed only inside the ``with``
            block; the handle's ``xcoms``/``sent`` snapshots remain readable
            after exit.
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
            dag_id: str | None overriding a bound Dag's identifier or naming
                the synthetic Dag auto-created and bound in place when the
                task is unbound, or ``None`` to read the bound Dag -- deriving
                a deterministic per-test, xdist-safe identifier when there is
                none.
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


class ComponentRegistry(Protocol):
    """Register a custom Airflow component for one test, reverting it afterward.

    Every method runs ``pytest_airflow_in_a_box.components.check_component`` first and
    raises ``ComponentContractError`` on any conformance problem, so a broken component
    is never silently registered and left to be wondered about later. A problem with the
    registration itself rather than the component -- an unsupported dual-registration
    request, an executor class with no importable module path, an unknown policy
    hookspec name, or similar -- raises
    ``pytest_airflow_in_a_box.components.ComponentSandboxError`` instead.
    """

    def plugin(self, component: object) -> None:
        """Register one Airflow plugin into every plugins-manager half this release has.

        Parameters:
            component: object containing an ``AirflowPlugin`` subclass or instance.
        """

    def listener(self, component: object, *, core: bool = True, task: bool = True) -> None:
        """Register one listener with the core and/or Task SDK listener manager.

        The conformance check's manager-scope findings are filtered by the requested
        scope: a hookimpl whose hookspec exists on only one manager (core-only
        ``on_dag_run_success``, for example) is accepted whenever that manager is among
        the requested ones, and refused only when it could never fire -- registered
        exclusively with the manager that lacks its hookspec.

        Parameters:
            component: object containing a listener class or instance carrying at least
                one ``@hookimpl``-decorated method.
            core: bool registering with the core ``ListenerManager``.
            task: bool registering with the Task SDK ``ListenerManager``.

        Raises:
            ComponentSandboxError: Neither ``core`` nor ``task`` is True, or ``task=True``
                on a release with no Task SDK listener manager (Airflow 3.1.x).
        """

    def policy(self, **hooks: Callable[..., object]) -> None:
        """Build and register one cluster-policy plugin from hookspec-named callables.

        Registers directly with Airflow's policy plugin manager and never writes an
        ``airflow_local_settings.py`` file, so per-test policies are fully decoupled
        from the ``airflow_local_settings`` collision guard. A registered
        ``task_instance_mutation_hook`` additionally flips the
        ``task_instance_mutation_hook.is_noop`` dispatch gate to False for the test
        (reverted at teardown) -- Airflow short-circuits on that flag, so the hookimpl
        would otherwise register but never fire.

        Parameters:
            hooks: Callable[..., object] values keyed by hookspec name (``task_policy``,
                ``dag_policy``, ``task_instance_mutation_hook``, ``pod_mutation_hook``,
                ``get_airflow_context_vars``, ``get_dagbag_import_timeout``).

        Raises:
            ComponentSandboxError: ``hooks`` is empty.
        """

    def secrets_backend(self, component: object, *, first: bool = True) -> None:
        """Insert one secrets backend into the search path.

        Parameters:
            component: object containing a ``BaseSecretsBackend`` subclass or instance.
            first: bool inserting at the front of the search path when True (checked
                before every other configured backend), the back when False.
        """

    def executor(self, component: object, *, alias: str = "test") -> str:
        """Register one executor class under an alias, resolvable by name.

        Parameters:
            component: object containing a ``BaseExecutor`` subclass, defined at module
                scope somewhere importable -- Airflow resolves it later by dotted import
                path.
            alias: str naming the alias to register the executor under.

        Returns:
            str containing ``alias``, unchanged, for resolving through
            ``ExecutorLoader.load_executor(alias)`` /
            ``ExecutorLoader.lookup_executor_name_by_str(alias)`` within the same
            test. No configuration surface can select the alias -- see the
            custom-components guide.

        Raises:
            ComponentSandboxError: ``component`` is not defined at module scope.
        """

    def timetable(self, component: object) -> None:
        """Register one custom timetable class through a synthesized throwaway plugin.

        Builds a throwaway ``AirflowPlugin`` subclass carrying the class in its
        ``timetables`` attribute and registers it, which is exactly what makes
        Airflow's serialization round trip work: ``encode_timetable`` refuses an
        unregistered custom timetable outright on 3.1.x, and ``decode_timetable``
        resolves the class by qualname through the plugins manager on every certified
        3.x release. Only a module-scope class can register -- Airflow later resolves
        it by dotted import path, and the ``timetable-local-qualname`` conformance
        check enforces that up front. Registering the same class twice is harmless;
        the qualname-keyed lookup mapping dedupes.

        Timetable LOGIC needs none of this: ``next_dagrun_info`` and
        ``infer_manual_data_interval`` are pure functions of a ``DataInterval`` and a
        ``TimeRestriction``, callable directly with no registration, no fixture, and
        no metadata database. Register only for the serialization path.

        Parameters:
            component: object containing a ``Timetable`` subclass or instance;
                an instance registers its class.
        """

    def priority_weight_strategy(self, component: object) -> None:
        """Register one priority weight strategy class through a synthesized plugin.

        Builds a throwaway ``AirflowPlugin`` subclass carrying the class in its
        ``priority_weight_strategies`` attribute and registers it, which is what lets
        Dag serialization accept the strategy at all --
        ``_encode_priority_weight_strategy`` refuses any custom strategy the plugins
        manager cannot resolve by qualname, on every certified 3.x release. Only a
        module-scope class can register, and deserialization re-instantiates it with
        no arguments, so state must live on the class (upstream's own documented
        contract).

        Parameters:
            component: object containing a ``PriorityWeightStrategy`` subclass or
                instance; an instance registers its class.
        """

    def serialization_round_trip(self, component: object) -> None:
        """Register one timetable instance, then assert it survives encode/decode.

        Registers via ``timetable()`` first, then runs
        ``decode_timetable(encode_timetable(...))`` and raises on any asymmetry: the
        decoded instance reconstructing as a different class, reconstructing with a
        different ``serialize()`` payload than the original produced, or (when the
        class defines its own ``__eq__``) comparing unequal to the original. One call
        covers "not registered", serialize/deserialize asymmetry, and equality
        problems in a single assertion.

        Parameters:
            component: object containing the ``Timetable`` instance to round-trip.

        Raises:
            ComponentSandboxError: ``component`` is a class -- encoding needs a live
                instance.
            ComponentContractError: The class fails a timetable conformance check, or
                the round trip reconstructs asymmetrically
                (``timetable-round-trip-mismatch``).
        """

    def round_trip(self, component: object) -> None:
        """Classify one component and register it with the matching method's defaults.

        Classifies ``component`` as exactly one of plugin, listener, executor,
        secrets backend, timetable, or priority weight strategy -- the same classification
        ``pytest_airflow_in_a_box.components.check_component`` uses for auto-detection
        -- and calls that method with its default keyword arguments, except a
        listener's ``task`` flag, which follows the installed release's Task SDK
        availability so a 3.1.x install (no Task SDK listener manager at all)
        round-trips core-only rather than raising. Not a substitute for ``policy()``,
        which has no bare-component form to classify.

        Parameters:
            component: object containing the component to classify and register.

        Raises:
            ComponentSandboxError: ``component`` matches zero or more than one
                registrable kind.
        """


__all__ = (
    "AirflowConfigure",
    "AirflowConnections",
    "AirflowVariables",
    "ComponentRegistry",
    "DagMaker",
    "RenderTask",
    "RunDag",
    "RunTask",
    "SerializedDag",
    "TaskContext",
    "TaskContextHandle",
    "TaskRunResult",
)
