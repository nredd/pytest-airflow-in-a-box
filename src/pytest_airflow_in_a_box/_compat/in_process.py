"""Run or render one task through the Task SDK in process, without a metadata database.

The Task SDK normally executes under a supervisor process connected through
``SUPERVISOR_COMMS``. Substituting an in-memory, protocol-conformant fake for
the supervisor lets ``task_runner.run`` execute real operator code with XCom,
Variable, and Connection traffic answered from seeded dictionaries. The
``parse``/bundle path is never used, so ``bundle_instance`` stays unset.
``task_runner.finalize`` -- task-level callback and listener dispatch -- runs
only on request, because callbacks are side effects most tests do not want.

``render_task_in_process`` reuses the same construction and shares the same fake
supervisor, but stops after ``get_template_context()`` and calls the operator's own
public ``render_template_fields`` instead of ``task_runner.run``. It mirrors
``task_runner.run``'s own preparation step and renders onto a
``prepare_for_execution()`` copy too, so a shared operator (a module-level Dag, a
session-scoped fixture) never contaminates across repeated calls -- rendering never
mutates the caller's `task`, and every result comes back through the return value.

Both entry points accept a task bound to no Dag at all: the Task SDK's own context
construction requires ``task.dag`` unconditionally, so an unbound task is bound IN
PLACE to a synthetic ``DAG(dag_id=..., schedule=None)`` named by the `dag_id`
argument. A bound task is never rebound.

``task_context_in_process`` stops even earlier: it prepares the same real
``RuntimeTaskInstance``-backed template context and then hands control to the
caller, keeping the fake supervisor installed for the duration of the ``with``
block so hand-driven ``execute()`` calls can push XCom, resolve Variables and
Connections, and call ``context["ti"].render_templates()`` exactly like a
supervised run.

References:
    https://airflow.apache.org/docs/task-sdk/stable/
    https://github.com/apache/airflow/blob/main/task-sdk/src/airflow/sdk/execution_time/task_runner.py
    https://github.com/apache/airflow/blob/main/task-sdk/src/airflow/sdk/execution_time/comms.py
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, NamedTuple

from pytest_airflow_in_a_box._compat.capabilities import resolve_capabilities

LOGGER = logging.getLogger(__name__)

DEFAULT_RUN_ID = "in-process-test"


class FakeSupervisorComms:
    """In-memory supervisor endpoint answering Task SDK messages.

    Parameters:
        xcoms: dict[str, Any] | None seeding XCom values by key.
        variables: dict[str, str] | None seeding Variable values by key.
        connections: dict[str, dict[str, Any]] | None seeding connection
            fields by connection id.
    """

    def __init__(
        self,
        *,
        xcoms: dict[str, Any] | None = None,
        variables: dict[str, str] | None = None,
        connections: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self.xcoms: dict[str, Any] = dict(xcoms) if xcoms else {}
        self.variables: dict[str, str] = dict(variables) if variables else {}
        self.connections: dict[str, dict[str, Any]] = (
            {conn_id: dict(fields) for conn_id, fields in connections.items()}
            if connections
            else {}
        )
        self.sent: list[Any] = []

    def send(self, msg: Any) -> Any:
        """Record one message and answer it from in-memory state.

        Unseeded Variable and Connection requests are answered with the same
        not-found ``ErrorResponse`` a real supervisor produces, so tasks fail
        exactly as they would against a live deployment; the seeding hint
        travels in the response ``detail``. Those two lookups delegate to
        ``_lookup_variable`` / ``_lookup_connection`` so a subclass can answer
        them from another store without re-deriving the response shaping.

        Parameters:
            msg: Any containing one Task SDK supervisor message.

        Returns:
            Any containing the response message, or ``None`` for one-way
            messages and unrecognized message types.
        """

        # Deferred to preserve bootstrap safety and avoid Airflow's module import cost.
        from airflow.sdk.execution_time.comms import (
            ConnectionResult,
            ErrorResponse,
            ErrorType,
            InactiveAssetsResult,
            VariableResult,
            XComCountResponse,
            XComResult,
            XComSequenceSliceResult,
        )

        self.sent.append(msg)
        name = type(msg).__name__
        if name == "SetXCom":
            self.xcoms[msg.key] = msg.value
            return None
        if name == "GetXCom":
            return XComResult(key=msg.key, value=self.xcoms.get(msg.key))
        if name == "GetXComSequenceSlice":
            present = msg.key in self.xcoms
            return XComSequenceSliceResult(root=[self.xcoms[msg.key]] if present else [])
        if name == "DeleteXCom":
            self.xcoms.pop(msg.key, None)
            return None
        if name == "GetXComCount":
            return XComCountResponse(len=int(msg.key in self.xcoms))
        if name == "GetVariable":
            value = self._lookup_variable(msg.key)
            if value is None:
                return ErrorResponse(
                    error=ErrorType.VARIABLE_NOT_FOUND,
                    detail={"hint": self._variable_hint(msg.key)},
                )
            return VariableResult(key=msg.key, value=value)
        if name == "GetConnection":
            fields = self._lookup_connection(msg.conn_id)
            if fields is None:
                return ErrorResponse(
                    error=ErrorType.CONNECTION_NOT_FOUND,
                    detail={"hint": self._connection_hint(msg.conn_id)},
                )
            payload = {"conn_type": "generic", **fields, "conn_id": msg.conn_id}
            return ConnectionResult.model_validate(_fill_declared_nones(ConnectionResult, payload))
        if name == "ValidateInletsAndOutlets":
            return InactiveAssetsResult(inactive_assets=[])
        return None

    def _lookup_variable(self, key: str) -> str | None:
        """Resolve one Variable value, or report it absent.

        Subclasses override this to answer from a different store; `send` owns the
        response shaping either way. A Variable value is always a string, so `None`
        unambiguously means absent.

        Parameters:
            key: str naming the requested Variable.

        Returns:
            str | None containing the seeded value, or None when it is unseeded.
        """

        return self.variables.get(key)

    def _lookup_connection(self, conn_id: str) -> dict[str, Any] | None:
        """Resolve one Connection's fields, or report it absent.

        Parameters:
            conn_id: str naming the requested Connection.

        Returns:
            dict[str, Any] | None containing the seeded fields, or None when it is
            unseeded.
        """

        return self.connections.get(conn_id)

    def _variable_hint(self, key: str) -> str:
        """Build the seeding hint carried by an unseeded Variable response.

        Parameters:
            key: str naming the requested Variable.

        Returns:
            str containing the actionable seeding hint.
        """

        return f"Seed it via `run_task(..., variables={{{key!r}: ...}})`"

    def _connection_hint(self, conn_id: str) -> str:
        """Build the seeding hint carried by an unseeded Connection response.

        Parameters:
            conn_id: str naming the requested Connection.

        Returns:
            str containing the actionable seeding hint.
        """

        return f"Seed it via `run_task(..., connections={{{conn_id!r}: {{'conn_type': ...}}}})`"


@dataclass(frozen=True)
class InProcessRunResult:
    """Outcome of one DB-free in-process task execution.

    Parameters:
        state: Any containing the terminal ``TaskInstanceState``.
        msg: Any | None containing the final supervisor message, when produced.
        error: BaseException | None raised by the task, when it failed.
        xcoms: dict[str, Any] containing XCom values after execution.
        sent: tuple[Any, ...] containing every supervisor message in order.
    """

    state: Any
    msg: Any | None
    error: BaseException | None
    xcoms: dict[str, Any]
    sent: tuple[Any, ...]


def _task_runner_module() -> Any:
    """Import the Task SDK runner module without static attribute constraints.

    The module-global ``SUPERVISOR_COMMS`` must be replaced and restored, which
    a statically typed module reference would reject.

    Returns:
        Any containing the ``airflow.sdk.execution_time.task_runner`` module.
    """

    # Deferred to preserve bootstrap safety and avoid Airflow's module import cost.
    from airflow.sdk.execution_time import task_runner

    return task_runner


# Generated-model fields the runner must send as explicit None where declared: 3.3.1
# promoted `end_date` and `partition_key` to required-without-default, while releases
# before 3.3.0 lack `partition_key` entirely and the models forbid extra fields.
_OPTIONAL_DAG_RUN_FIELDS = ("end_date", "partition_key")


def _dag_run_payload_extras(dag_run_model: Any) -> dict[str, None]:
    """Build the None-valued keyword arguments the generated `DagRun` model declares.

    Parameters:
        dag_run_model: Any containing the release's generated `DagRun` Pydantic model.

    Returns:
        dict[str, None] keyed by each declared `_OPTIONAL_DAG_RUN_FIELDS` name.
    """

    return {name: None for name in _OPTIONAL_DAG_RUN_FIELDS if name in dag_run_model.model_fields}


def _fill_declared_nones(model: Any, payload: dict[str, Any]) -> dict[str, Any]:
    """Complete a payload with explicit None for required fields the caller omitted.

    3.3.1 regenerated the comms models with every field required-without-default
    (None-able fields included), so a seeded subset like `{'conn_type': ..., 'host':
    ...}` no longer validates on its own. A genuinely non-null required field that is
    missing still fails validation loudly -- None is not a valid value for it.

    Parameters:
        model: Any containing the release's generated Pydantic model.
        payload: dict[str, Any] containing the caller-provided field values.

    Returns:
        dict[str, Any] containing `payload` plus None for each absent required field,
        keyed by validation alias where one is declared (the generated models alias
        e.g. `schema_` to `schema`).
    """

    return {
        field.alias or name: None
        for name, field in model.model_fields.items()
        if field.is_required()
    } | payload


def bound_dag_or_none(task: Any) -> Any | None:
    """Return the task's bound Dag, or ``None`` when the task is unbound.

    Parameters:
        task: Any containing the Airflow operator or bound TaskFlow task.

    Returns:
        Any | None containing the bound Dag object, or ``None`` when the SDK
        operator's ``dag`` property reports the task as unbound.
    """

    try:
        return task.dag
    except (AttributeError, RuntimeError):
        # The SDK operator's `dag` property raises when the task is unbound.
        return None


class _RuntimeTaskInstanceBuild(NamedTuple):
    """Paired result of constructing one ``RuntimeTaskInstance``.

    Parameters:
        task_runner: Any containing the ``airflow.sdk.execution_time.task_runner`` module.
        runtime_ti: Any containing the constructed ``RuntimeTaskInstance``.
    """

    task_runner: Any
    runtime_ti: Any


def _build_runtime_task_instance(
    task: Any,
    *,
    dag_id: str | None,
    run_id: str,
    logical_date: datetime | None,
    params: dict[str, Any] | None,
    map_index: int,
    try_number: int,
) -> _RuntimeTaskInstanceBuild:
    """Validate arguments and construct one ``RuntimeTaskInstance`` bound to `task`.

    Shared by `run_task_in_process` and `render_task_in_process`, which diverge only
    in what they do with the resulting instance and its template context. An unbound
    `task` is bound IN PLACE to a synthetic ``DAG(dag_id=..., schedule=None)`` named
    by `dag_id`, because the Task SDK's own context construction requires
    ``task.dag`` unconditionally. A bound task is never rebound.

    Parameters:
        task: Any containing the Airflow operator or bound TaskFlow task.
        dag_id: str | None overriding the Dag identifier and naming the synthetic
            Dag auto-bound to an unbound task, or ``None`` to read it from the
            task's bound Dag.
        run_id: str identifying the synthetic manual run.
        logical_date: datetime | None pinning the run's logical date.
        params: dict[str, Any] | None overriding declared Dag params.
        map_index: int selecting the mapped task index.
        try_number: int selecting the synthetic task attempt number.

    Returns:
        _RuntimeTaskInstanceBuild containing the ``airflow.sdk.execution_time.task_runner``
        module and the constructed ``RuntimeTaskInstance``.

    Raises:
        TypeError: The task does not expose a string ``task_id``.
        ValueError: No Dag identifier is available, ``run_id`` is empty, or
            ``try_number`` is less than 1.
        AirflowCompatibilityError: The installed Airflow interface is unsupported.
    """

    task_id = getattr(task, "task_id", None)
    if not isinstance(task_id, str) or not task_id:
        raise TypeError("`task` must be an Airflow operator exposing a string `task_id`")
    if not run_id:
        raise ValueError("`run_id` must be a non-empty run identifier")
    if try_number < 1:
        raise ValueError("`try_number` must be at least 1")
    bound_dag = bound_dag_or_none(task)
    resolved_dag_id = dag_id if dag_id else getattr(bound_dag, "dag_id", None)
    if not isinstance(resolved_dag_id, str) or not resolved_dag_id:
        raise ValueError("`dag_id` is required when the task is not bound to a Dag")

    resolve_capabilities()

    if bound_dag is None:
        # Deferred to preserve bootstrap safety and avoid Airflow's module import cost.
        from airflow.sdk import DAG

        task.dag = DAG(dag_id=resolved_dag_id, schedule=None)

    # Deferred to preserve bootstrap safety and avoid Airflow's module import cost.
    from airflow.sdk.api.datamodels._generated import DagRun as DagRunDataModel
    from airflow.sdk.api.datamodels._generated import TaskInstance as TaskInstanceDataModel
    from airflow.sdk.api.datamodels._generated import TIRunContext
    from uuid6 import uuid7

    task_runner = _task_runner_module()
    now = datetime.now(timezone.utc)
    run_date = logical_date if logical_date is not None else now
    dag_run_extras = _dag_run_payload_extras(DagRunDataModel)
    task_instance = TaskInstanceDataModel(
        id=uuid7(),
        task_id=task_id,
        dag_id=resolved_dag_id,
        run_id=run_id,
        try_number=try_number,
        dag_version_id=uuid7(),
        map_index=map_index,
    )
    context_from_server = TIRunContext(
        dag_run=DagRunDataModel(
            dag_id=resolved_dag_id,
            run_id=run_id,
            logical_date=run_date,
            data_interval_start=run_date,
            data_interval_end=run_date,
            run_after=run_date,
            start_date=now,
            run_type="manual",
            state="running",
            conf=dict(params) if params else None,
            consumed_asset_events=[],
            **dag_run_extras,
        ),
        max_tries=int(task.retries or 0),
        should_retry=try_number <= int(task.retries or 0),
    )
    runtime_ti = task_runner.RuntimeTaskInstance.model_construct(
        **task_instance.model_dump(exclude_unset=True),
        task=task,
        _ti_context_from_server=context_from_server,
        max_tries=int(task.retries or 0),
        start_date=now,
    )
    return _RuntimeTaskInstanceBuild(task_runner, runtime_ti)


@contextlib.contextmanager
def _installed_supervisor_comms(
    task_runner: Any, comms: FakeSupervisorComms | None
) -> Iterator[FakeSupervisorComms]:
    """Install a fake supervisor for one call, then restore whatever was there before.

    Parameters:
        task_runner: Any containing the ``airflow.sdk.execution_time.task_runner`` module.
        comms: FakeSupervisorComms | None to install, or ``None`` for a fresh instance.

    Yields:
        FakeSupervisorComms installed as ``task_runner.SUPERVISOR_COMMS`` for the block.
    """

    active_comms = comms if comms is not None else FakeSupervisorComms()
    absent = object()
    previous_comms = getattr(task_runner, "SUPERVISOR_COMMS", absent)
    task_runner.SUPERVISOR_COMMS = active_comms
    try:
        yield active_comms
    finally:
        if previous_comms is absent:
            del task_runner.SUPERVISOR_COMMS
        else:
            task_runner.SUPERVISOR_COMMS = previous_comms


def run_task_in_process(
    task: Any,
    *,
    dag_id: str | None = None,
    run_id: str = DEFAULT_RUN_ID,
    logical_date: datetime | None = None,
    params: dict[str, Any] | None = None,
    comms: FakeSupervisorComms | None = None,
    map_index: int = -1,
    try_number: int = 1,
    run_callbacks: bool = False,
) -> InProcessRunResult:
    """Execute one operator through ``task_runner.run`` with fake supervision.

    ``params`` are delivered as the synthetic DagRun's ``conf`` and validated
    against the Dag's declared params exactly like a triggered run. An unbound
    `task` is bound in place to a synthetic Dag named by `dag_id` -- an observable
    side effect on the caller's operator; a bound task is never rebound.

    Parameters:
        task: Any containing the Airflow operator or bound TaskFlow task.
        dag_id: str | None overriding the Dag identifier and naming the synthetic
            Dag auto-bound to an unbound task, or ``None`` to read it from the
            task's bound Dag.
        run_id: str identifying the synthetic manual run.
        logical_date: datetime | None pinning the run's logical date.
        params: dict[str, Any] | None overriding declared Dag params.
        comms: FakeSupervisorComms | None carrying seeded supervisor state.
        map_index: int selecting the mapped task index.
        try_number: int selecting the synthetic task attempt number.
        run_callbacks: bool dispatching task callbacks and listeners through
            ``task_runner.finalize`` after execution.

    Returns:
        InProcessRunResult containing terminal state, error, and XCom values.

    Raises:
        TypeError: The task does not expose a string ``task_id``.
        ValueError: No Dag identifier is available, ``run_id`` is empty, or
            ``try_number`` is less than 1.
        AirflowCompatibilityError: The installed Airflow interface is unsupported.
    """

    task_runner, runtime_ti = _build_runtime_task_instance(
        task,
        dag_id=dag_id,
        run_id=run_id,
        logical_date=logical_date,
        params=params,
        map_index=map_index,
        try_number=try_number,
    )

    # Deferred to preserve bootstrap safety and avoid Airflow's module import cost.
    import structlog

    with _installed_supervisor_comms(task_runner, comms) as active_comms:
        context = runtime_ti.get_template_context()
        log = structlog.get_logger("pytest_airflow_in_a_box.run_task")
        state, msg, error = task_runner.run(runtime_ti, context, log)
        if run_callbacks:
            task_runner.finalize(runtime_ti, state, context, log, error)

    return InProcessRunResult(
        state=state,
        msg=msg,
        error=error,
        xcoms=dict(active_comms.xcoms),
        sent=tuple(active_comms.sent),
    )


def render_task_in_process(
    task: Any,
    *,
    dag_id: str | None = None,
    run_id: str = DEFAULT_RUN_ID,
    logical_date: datetime | None = None,
    params: dict[str, Any] | None = None,
    comms: FakeSupervisorComms | None = None,
    map_index: int = -1,
    try_number: int = 1,
    context_overrides: dict[str, Any] | None = None,
) -> Any:
    """Render one operator's template fields in process with fake supervision.

    Stops short of `run_task_in_process`'s execution: no `task_runner.run`, so the
    operator body never runs. Rendering still happens on a
    `task.prepare_for_execution()` copy, exactly like `task_runner.run`'s own
    preparation step -- the caller's `task` is never mutated, so calling this twice
    against the same shared operator (a module-level Dag, a session-scoped fixture)
    renders each call independently instead of the second call silently rendering a
    template that the first call already collapsed to a literal. Always use the
    return value. For a mapped operator, that is the concrete unmapped instance
    Airflow's own unmapping produces for `map_index`, not a copy of the mapped
    operator itself. Rendering never mutates the caller's `task`; binding an
    unbound task in place to a synthetic Dag named by `dag_id` is the one
    observable side effect, and a bound task is never rebound.

    Parameters:
        task: Any containing the Airflow operator or bound TaskFlow task.
        dag_id: str | None overriding the Dag identifier and naming the synthetic
            Dag auto-bound to an unbound task, or ``None`` to read it from the
            task's bound Dag.
        run_id: str identifying the synthetic manual run.
        logical_date: datetime | None pinning the run's logical date.
        params: dict[str, Any] | None overriding declared Dag params.
        comms: FakeSupervisorComms | None carrying seeded supervisor state.
        map_index: int selecting the mapped task index.
        try_number: int selecting the synthetic task attempt number.
        context_overrides: dict[str, Any] | None merged into the synthesized
            template context before rendering.

    Returns:
        Any containing the rendered operator: a `prepare_for_execution()` copy of
        `task` for a plain operator, or the concrete unmapped instance for a mapped
        one. Never the exact `task` object passed in.

    Raises:
        TypeError: The task does not expose a string ``task_id``.
        ValueError: No Dag identifier is available, ``run_id`` is empty, or
            ``try_number`` is less than 1.
        AirflowCompatibilityError: The installed Airflow interface is unsupported.
    """

    # One shared prepare/render sequence on purpose: it hand-mirrors the
    # release-sensitive `task_runner._prepare`, and two copies would silently
    # diverge at the next upstream reordering.
    with task_context_in_process(
        task,
        dag_id=dag_id,
        run_id=run_id,
        logical_date=logical_date,
        params=params,
        comms=comms,
        map_index=map_index,
        try_number=try_number,
        context_overrides=context_overrides,
        render=True,
    ) as handle:
        return handle.task


class InProcessTaskContext:
    """Live handle over one installed in-process task context.

    Parameters:
        ti: Any containing the constructed ``RuntimeTaskInstance``.
        context: Any containing the synthesized Task SDK template context.
        comms: FakeSupervisorComms installed for the surrounding ``with`` block.
    """

    def __init__(self, *, ti: Any, context: Any, comms: FakeSupervisorComms) -> None:
        self._ti = ti
        self._context = context
        self._comms = comms

    @property
    def ti(self) -> Any:
        """Return the task instance behind ``context["ti"]``.

        Returns:
            Any containing the real ``RuntimeTaskInstance``.
        """

        return self._ti

    @property
    def context(self) -> Any:
        """Return the synthesized template context.

        Returns:
            Any containing the Task SDK template context mapping.
        """

        return self._context

    @property
    def task(self) -> Any:
        """Return the execution-time operator copy to drive.

        A property over ``ti.task`` on purpose: rendering a mapped operator swaps
        ``ti.task`` to the concrete unmapped instance through Airflow's own
        ``context["ti"]`` mutation, and a value captured at yield time would go stale.

        Returns:
            Any containing the prepared operator copy, or the concrete unmapped
            instance for a rendered mapped operator.
        """

        return self._ti.task

    @property
    def xcoms(self) -> dict[str, Any]:
        """Return a snapshot of XCom values held by the fake supervisor.

        Returns:
            dict[str, Any] containing XCom values by key.
        """

        return dict(self._comms.xcoms)

    @property
    def sent(self) -> tuple[Any, ...]:
        """Return a snapshot of supervisor traffic.

        Returns:
            tuple[Any, ...] containing every supervisor message in send order.
        """

        return tuple(self._comms.sent)


@contextlib.contextmanager
def task_context_in_process(
    task: Any,
    *,
    dag_id: str | None = None,
    run_id: str = DEFAULT_RUN_ID,
    logical_date: datetime | None = None,
    params: dict[str, Any] | None = None,
    comms: FakeSupervisorComms | None = None,
    map_index: int = -1,
    try_number: int = 1,
    context_overrides: dict[str, Any] | None = None,
    render: bool = True,
) -> Iterator[InProcessTaskContext]:
    """Prepare one operator's template context and yield control with fake supervision.

    Stops short of `render_task_in_process`'s rendering step being the end of the
    story: the fake supervisor stays installed for the whole ``with`` block, so the
    caller can hand-drive ``handle.task.execute(handle.context)``, capture the raw
    return value, and let the operator resolve XCom, Variable, and Connection
    traffic or call ``context["ti"].render_templates()`` mid-execution. Preparation
    happens on a `task.prepare_for_execution()` copy exactly like a real run -- the
    caller's `task` is never mutated, so always drive ``handle.task``, not the
    operator passed in.

    Parameters:
        task: Any containing the Airflow operator or bound TaskFlow task.
        dag_id: str | None overriding the Dag identifier, or ``None`` to read
            it from the task's bound Dag.
        run_id: str identifying the synthetic manual run.
        logical_date: datetime | None pinning the run's logical date.
        params: dict[str, Any] | None overriding declared Dag params.
        comms: FakeSupervisorComms | None carrying seeded supervisor state.
        map_index: int selecting the mapped task index.
        try_number: int selecting the synthetic task attempt number.
        context_overrides: dict[str, Any] | None merged into the synthesized
            template context before rendering.
        render: bool pre-rendering template fields like a real run. Pass ``False``
            for operators that call ``context["ti"].render_templates()`` inside
            ``execute()`` themselves.

    Yields:
        InProcessTaskContext exposing the ``RuntimeTaskInstance``, the template
        context, the prepared operator copy, and supervisor state snapshots.

    Raises:
        TypeError: The task does not expose a string ``task_id``.
        ValueError: No Dag identifier is available, ``run_id`` is empty,
            ``try_number`` is less than 1, or ``render=False`` was passed for a
            mapped operator.
        AirflowCompatibilityError: The installed Airflow interface is unsupported.
    """

    if not render and getattr(task, "is_mapped", False):
        raise ValueError(
            "`render=False` is unsupported for a mapped operator: Airflow unmaps the "
            "task to its concrete instance inside `render_template_fields`, which "
            "`render=False` skips, leaving a `MappedOperator` with no `execute()`. "
            "Use the default `render=True`."
        )
    task_runner, runtime_ti = _build_runtime_task_instance(
        task,
        dag_id=dag_id,
        run_id=run_id,
        logical_date=logical_date,
        params=params,
        map_index=map_index,
        try_number=try_number,
    )

    # Deferred to preserve bootstrap safety and avoid Airflow's module import cost.
    from airflow.sdk.execution_time.context import set_current_context

    with _installed_supervisor_comms(task_runner, comms) as active_comms:
        context = runtime_ti.get_template_context()
        # Mirrors `task_runner._prepare`'s own order: build the context bound to the
        # original task, then swap in the execution-time copy before rendering, same
        # as a real run. A mapped operator's `prepare_for_execution()` is a no-op --
        # rendering below is what swaps `runtime_ti.task` to the concrete unmapped
        # instance, through Airflow's own `context["ti"]` mutation, and the handle's
        # `task` property tracks that swap.
        runtime_ti.task = runtime_ti.task.prepare_for_execution()
        context["task"] = runtime_ti.task
        if context_overrides:
            context |= context_overrides
        if render:
            # The TI's own `render_templates` (not the operator's public
            # `render_template_fields`) also syncs `ti.is_mapped`, matching what a
            # supervised run exposes to code branching on it.
            runtime_ti.render_templates(context)
        # `task_runner.run` wraps execution the same way, so hand-driven `execute()`
        # bodies calling `airflow.sdk.get_current_context()` resolve this context.
        with set_current_context(context):
            yield InProcessTaskContext(ti=runtime_ti, context=context, comms=active_comms)


__all__ = (
    "DEFAULT_RUN_ID",
    "FakeSupervisorComms",
    "InProcessRunResult",
    "InProcessTaskContext",
    "bound_dag_or_none",
    "render_task_in_process",
    "run_task_in_process",
    "task_context_in_process",
)
