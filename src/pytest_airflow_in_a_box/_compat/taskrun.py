#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Run persisted task instances across certified Airflow releases.

``run_task_instance`` is adapted from Apache Airflow and modified to defer every
Airflow import, resolve optional tasks, preserve dependency semantics, implement
``mark_success``, and return the refreshed caller-owned ORM task instance.
``ordered_task_instances``, ``run_trigger``, and ``execute_dag_run`` are locally
authored.

References:
    https://github.com/apache/airflow/blob/2d374f71bc81202204ac0208df07b07c280668fa/devel-common/src/tests_common/test_utils/taskinstance.py
    https://github.com/apache/airflow/pull/59835
    https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html
    https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/triggers/base/index.html
"""

from __future__ import annotations

import asyncio
import logging
from importlib import import_module
from typing import TYPE_CHECKING, Any

from pytest_airflow_in_a_box._compat.capabilities import TaskInstanceRunner, resolve_capabilities
from pytest_airflow_in_a_box._compat.dag import expand_mapped_task_instances
from pytest_airflow_in_a_box._compat.registry import lookup_authoring_dag
from pytest_airflow_in_a_box.results import DagRunResult, TaskResult, task_key

if TYPE_CHECKING:
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance
    from airflow.sdk.types import Operator
    from sqlalchemy.orm import Session

LOGGER = logging.getLogger(__name__)

DEFAULT_TRIGGER_TIMEOUT = 10.0


class TaskResolutionError(RuntimeError):
    """Report failure to resolve an executable task for a task instance."""


class TriggerExecutionError(RuntimeError):
    """Report failure to drive a trigger to its first event."""


def _session_kwargs(session: Session | None) -> dict[str, Session]:
    """Build keyword arguments accepted across certified Airflow releases.

    Parameters:
        session: sqlalchemy.orm.Session | None supplied by the caller.

    Returns:
        dict[str, sqlalchemy.orm.Session] containing a non-null session.
    """

    return {"session": session} if session is not None else {}


def _resolve_task(ti: TaskInstance, task: Operator | None, session: Session | None) -> Any:
    """Resolve an authoring or scheduler task without importing Airflow eagerly.

    Resolution prefers the explicit ``task`` argument, then the transiently attached
    ``ti.task``, then the plugin's registry of persisted authoring Dags, and finally
    the transient Dag attached to the instance's DagRun. The registry step resolves
    ``dag_maker``-owned tasks for instances queried through any consumer session.

    Parameters:
        ti: airflow.models.taskinstance.TaskInstance being executed.
        task: airflow.sdk.types.Operator | None explicitly supplied by the caller.
        session: sqlalchemy.orm.Session | None used to resolve persisted metadata.

    Returns:
        airflow.sdk.types.Operator associated with ``ti``.

    Raises:
        TaskResolutionError: The task cannot be resolved from the instance or its DagRun.
    """

    if task is not None:
        return task
    attached_task = getattr(ti, "task", None)
    if attached_task is not None:
        return attached_task
    authoring_dag = lookup_authoring_dag(str(ti.dag_id))
    if authoring_dag is not None and str(ti.task_id) in authoring_dag.task_dict:
        return authoring_dag.get_task(str(ti.task_id))
    try:
        dag_run = ti.get_dagrun(**_session_kwargs(session))
        return dag_run.get_dag().get_task(str(ti.task_id))
    except Exception as error:
        raise TaskResolutionError(
            f"Could not resolve task '{ti.task_id}' for DagRun '{ti.run_id}': {error}; "
            f"pass `task=dag.get_task('{ti.task_id}')` from the authoring Dag"
        ) from error


def _scheduler_task(
    ti: TaskInstance,
    session: Session | None,
) -> tuple[DagRun, Any]:
    """Load the persisted scheduler task used for dependency evaluation.

    Parameters:
        ti: airflow.models.taskinstance.TaskInstance being evaluated.
        session: sqlalchemy.orm.Session | None used to query metadata.

    Returns:
        tuple[airflow.models.dagrun.DagRun, airflow.sdk.types.Operator] containing persisted data.

    Raises:
        TaskResolutionError: Persisted DagRun or task metadata cannot be loaded.
    """

    try:
        from airflow.models.dagrun import DagRun
        from airflow.models.serialized_dag import SerializedDagModel
        from sqlalchemy import select

        if session is None:
            raise RuntimeError("scheduler task resolution requires a metadata session")
        identity: Any = ti
        dag_run = session.scalar(
            select(DagRun).where(
                DagRun.dag_id == identity.dag_id,
                DagRun.run_id == identity.run_id,
            )
        )
        if dag_run is None:
            raise RuntimeError("Airflow did not return the persisted DagRun")
        scheduler_dag = SerializedDagModel.get_dag(identity.dag_id, session=session)
        if scheduler_dag is None:
            raise RuntimeError("Airflow did not return the persisted scheduler Dag")
        return dag_run, scheduler_dag.get_task(identity.task_id)
    except Exception as error:
        raise TaskResolutionError(
            f"Could not load scheduler task '{ti.task_id}' for DagRun '{ti.run_id}': {error}"
        ) from error


def _refresh_task_instance(ti: TaskInstance, session: Session | None) -> None:
    """Expire session state and refresh the original task instance from metadata.

    Parameters:
        ti: airflow.models.taskinstance.TaskInstance owned by the caller.
        session: sqlalchemy.orm.Session | None used by execution.
    """

    if session is not None:
        # Execution API handlers use separate sessions, so the caller's identity map is stale.
        session.expire_all()
    if session is None:
        ti.refresh_from_db()
    else:
        ti.refresh_from_db(session=session)


def _run_legacy_task_instance(
    ti: TaskInstance,
    task: Any,
    *,
    ignore_depends_on_past: bool,
    ignore_task_deps: bool,
    ignore_ti_state: bool,
    mark_success: bool,
    session: Session | None,
) -> TaskInstance:
    """Execute one task through Airflow 3.1's ``TaskInstance.run``.

    Parameters:
        ti: airflow.models.taskinstance.TaskInstance to execute.
        task: airflow.sdk.types.Operator associated with the instance.
        ignore_depends_on_past: bool forwarded to Airflow's dependency gate.
        ignore_task_deps: bool forwarded to Airflow's dependency gate.
        ignore_ti_state: bool forwarded to Airflow's dependency gate.
        mark_success: bool selecting success without body execution.
        session: sqlalchemy.orm.Session | None used for metadata operations.

    Returns:
        airflow.models.taskinstance.TaskInstance containing refreshed state.
    """

    _refresh_from_task(ti, task)
    try:
        ti.run(
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state,
            mark_success=mark_success,
            session=session,
        )
    finally:
        _refresh_task_instance(ti, session)
    return ti


def _run_sdk_task_instance(
    ti: TaskInstance,
    task: Any,
    *,
    ignore_depends_on_past: bool,
    ignore_task_deps: bool,
    ignore_ti_state: bool,
    mark_success: bool,
    session: Session | None,
) -> TaskInstance:
    """Execute one task through Airflow 3.2+'s private Task SDK runner.

    Parameters:
        ti: airflow.models.taskinstance.TaskInstance to execute.
        task: airflow.sdk.types.Operator containing executable authoring code.
        ignore_depends_on_past: bool forwarded to Airflow's dependency gate.
        ignore_task_deps: bool forwarded to Airflow's dependency gate.
        ignore_ti_state: bool forwarded to Airflow's dependency gate.
        mark_success: bool selecting success without body execution.
        session: sqlalchemy.orm.Session | None used for metadata operations.

    Returns:
        airflow.models.taskinstance.TaskInstance containing refreshed state.

    Raises:
        RuntimeError: Airflow finishes execution without a task-run result.
        BaseException: Airflow reports the task execution error.
    """

    from airflow.models.taskinstance import TaskInstance
    from airflow.sdk.definitions.dag import _run_task
    from airflow.utils.state import TaskInstanceState
    from sqlalchemy.orm import object_session

    active_session = session or object_session(ti)
    if active_session is None:
        raise RuntimeError(
            "Airflow 3.2+ task execution requires a persisted task instance attached to a session"
        )

    dag_run, scheduler_task = _scheduler_task(ti, active_session)
    capabilities = resolve_capabilities()
    if capabilities.refresh_from_task_supports_dag_run:
        _refresh_from_task(ti, scheduler_task, dag_run)
    else:
        _refresh_from_task(ti, scheduler_task)

    # The in-process runner serves the Execution API through separate DB sessions.
    active_session.commit()
    should_run = ti.check_and_change_state_before_execution(
        ignore_depends_on_past=ignore_depends_on_past,
        ignore_task_deps=ignore_task_deps,
        ignore_ti_state=ignore_ti_state,
        mark_success=mark_success,
        session=active_session,
    )
    if not should_run:
        _refresh_task_instance(ti, active_session)
        return ti

    if mark_success:
        try:
            ti.set_state(TaskInstanceState.SUCCESS, session=active_session)
            active_session.commit()
        finally:
            _refresh_task_instance(ti, active_session)
        return ti

    # Session handling is a mess in tests; use a fresh ti to run the task.
    identity: Any = ti
    new_ti = TaskInstance.get_task_instance(
        dag_id=identity.dag_id,
        run_id=identity.run_id,
        task_id=identity.task_id,
        map_index=identity.map_index,
        session=active_session,
    )
    # Some tests don't save the ti at all, in which case new_ti is None.
    taskrun_result: Any = None
    try:
        taskrun_result = _run_task(ti=new_ti or ti, task=task)
    finally:
        _refresh_task_instance(ti, active_session)  # Some tests expect side effects.
    if not taskrun_result:
        raise RuntimeError("task failed to finish with a result")
    if error := taskrun_result.error:
        raise error
    return ti


def run_trigger(trigger: Any, *, timeout: float = DEFAULT_TRIGGER_TIMEOUT) -> Any:
    """Drive one trigger's async ``run`` to its first event and return that event.

    The trigger runs in process on a private event loop, so no triggerer job, metadata
    database, or deferred task instance is required. ``cleanup`` always runs, including
    when the trigger raises or the timeout expires.

    Parameters:
        trigger: airflow.triggers.base.BaseTrigger exposing ``run`` and ``cleanup``.
        timeout: float seconds allowed for the first event to arrive.

    Returns:
        airflow.triggers.base.TriggerEvent yielded first by ``trigger.run()``.

    Raises:
        TypeError: The supplied object is not trigger-shaped.
        ValueError: The supplied timeout is not positive.
        TriggerExecutionError: The trigger times out or completes without an event.
    """

    if timeout <= 0:
        raise ValueError(f"`timeout` must be positive, got '{timeout}'")
    missing = [name for name in ("run", "cleanup") if not callable(getattr(trigger, name, None))]
    if missing:
        missing_names = ", ".join(f"'{name}'" for name in missing)
        raise TypeError(f"`trigger` of type '{type(trigger).__name__}' lacks {missing_names}")

    async def awaited_event() -> Any:
        events = trigger.run()
        if not callable(getattr(events, "__anext__", None)):
            raise TypeError(
                f"`trigger.run()` of type '{type(trigger).__name__}' "
                "did not return an async iterator"
            )
        try:
            # ``asyncio.run`` closes an abandoned generator through ``shutdown_asyncgens``.
            return await asyncio.wait_for(events.__anext__(), timeout)
        except StopAsyncIteration as error:
            raise TriggerExecutionError(
                f"Trigger '{type(trigger).__name__}' completed without an event"
            ) from error
        except asyncio.TimeoutError as error:
            raise TriggerExecutionError(
                f"Trigger '{type(trigger).__name__}' yielded no event within '{timeout}' seconds"
            ) from error
        finally:
            await trigger.cleanup()

    return asyncio.run(awaited_event())


def _resume_deferred_task_instance(
    ti: TaskInstance,
    session: Session | None,
    *,
    trigger_timeout: float = DEFAULT_TRIGGER_TIMEOUT,
) -> None:
    """Run one persisted trigger event and submit it to the deferred task instance.

    Parameters:
        ti: airflow.models.taskinstance.TaskInstance holding the persisted trigger.
        session: sqlalchemy.orm.Session | None owning the persisted metadata.
        trigger_timeout: float seconds allowed for the trigger's first event.

    Raises:
        TriggerExecutionError: The session, trigger relationship, or trigger row is absent.
    """

    if session is None:
        raise TriggerExecutionError("Trigger execution requires a persisted task instance session")

    from airflow.models.trigger import Trigger

    try:
        module_loading: Any = import_module("airflow.sdk._shared.module_loading")
    except ImportError:
        module_loading = import_module("airflow.utils.module_loading")

    _refresh_task_instance(ti, session)
    trigger_id = ti.trigger_id
    if trigger_id is None:
        raise TriggerExecutionError(
            f"Deferred task instance '{ti.task_id}' has no persisted trigger"
        )
    trigger_row = session.get(Trigger, trigger_id)
    if trigger_row is None:
        raise TriggerExecutionError(
            f"Deferred task instance '{ti.task_id}' trigger '{trigger_id}' is absent"
        )
    trigger = module_loading.import_string(str(trigger_row.classpath))(**trigger_row.kwargs)

    event = run_trigger(trigger, timeout=trigger_timeout)
    Trigger.submit_event(trigger_id, event, session=session)
    session.commit()
    _refresh_task_instance(ti, session)


def run_task_instance(
    ti: TaskInstance,
    task: Operator | None = None,
    *,
    ignore_depends_on_past: bool = False,
    ignore_task_deps: bool = False,
    ignore_ti_state: bool = False,
    mark_success: bool = False,
    run_triggerer: bool = False,
    trigger_timeout: float = DEFAULT_TRIGGER_TIMEOUT,
    session: Session | None = None,
) -> TaskInstance:
    """Run one persisted task instance through the certified Airflow entry point.

    Parameters:
        ti: airflow.models.taskinstance.TaskInstance to execute and return.
        task: airflow.sdk.types.Operator | None containing executable task code.
        ignore_depends_on_past: bool controlling the depends-on-past dependency.
        ignore_task_deps: bool controlling task-specific dependencies.
        ignore_ti_state: bool controlling existing task-instance state checks.
        mark_success: bool marking success without executing the task body.
        run_triggerer: bool running one persisted trigger event and resuming a deferred task.
        trigger_timeout: float seconds allowed for the persisted trigger's first event.
        session: sqlalchemy.orm.Session | None used for metadata operations.

    Returns:
        airflow.models.taskinstance.TaskInstance containing refreshed persisted state.

    Raises:
        TaskResolutionError: No executable task can be resolved.
        TriggerExecutionError: A deferred task's trigger cannot be driven to an event.
        RuntimeError: Airflow 3.2+ cannot execute a detached instance or returns no result.
        BaseException: Airflow reports the task execution error.
    """

    resolved_task = _resolve_task(ti, task, session)
    capabilities = resolve_capabilities()
    arguments = {
        "ignore_depends_on_past": ignore_depends_on_past,
        "ignore_task_deps": ignore_task_deps,
        "ignore_ti_state": ignore_ti_state,
        "mark_success": mark_success,
        "session": session,
    }
    if capabilities.task_instance_runner is TaskInstanceRunner.LEGACY_RUN:
        result = _run_legacy_task_instance(ti, resolved_task, **arguments)
    else:
        result = _run_sdk_task_instance(ti, resolved_task, **arguments)
    if not run_triggerer:
        return result

    from airflow.utils.state import TaskInstanceState

    if result.state != TaskInstanceState.DEFERRED:
        return result
    _resume_deferred_task_instance(result, session, trigger_timeout=trigger_timeout)
    arguments["ignore_ti_state"] = True
    if capabilities.task_instance_runner is TaskInstanceRunner.LEGACY_RUN:
        return _run_legacy_task_instance(result, resolved_task, **arguments)
    return _run_sdk_task_instance(result, resolved_task, **arguments)


def ordered_task_instances(
    dag_run: DagRun,
    dag: Any,
    *,
    session: Session | None = None,
) -> list[TaskInstance]:
    """Return DagRun task instances in graph order, then mapped-index order.

    Parameters:
        dag_run: airflow.models.dagrun.DagRun whose task instances are fetched.
        dag: Any exposing Airflow's ``topological_sort`` method.
        session: sqlalchemy.orm.Session | None used to fetch task instances.

    Returns:
        list[airflow.models.taskinstance.TaskInstance] in dependency-safe order.

    Raises:
        ValueError: A fetched task instance is absent from the supplied Dag graph.
    """

    task_instances = (
        dag_run.get_task_instances()
        if session is None
        else dag_run.get_task_instances(session=session)
    )
    ranks = {task.task_id: rank for rank, task in enumerate(dag.topological_sort())}
    missing = sorted({ti.task_id for ti in task_instances if ti.task_id not in ranks})
    if missing:
        missing_ids = ", ".join(f"'{task_id}'" for task_id in missing)
        raise ValueError(
            f"DagRun '{dag_run.run_id}' contains task IDs absent from Dag '{dag.dag_id}': "
            f"{missing_ids}"
        )
    return sorted(task_instances, key=lambda ti: (ranks[ti.task_id], ti.map_index))


def _refresh_from_task(ti: Any, task: Any, dag_run: Any = None) -> None:
    """Cross Airflow's authoring/scheduler operator typing boundary.

    Parameters:
        ti: Any containing an ORM TaskInstance.
        task: Any containing an authoring or serialized operator.
        dag_run: Any containing an optional DagRun for mutation hooks.
    """

    if dag_run is None:
        ti.refresh_from_task(task)
    else:
        ti.refresh_from_task(task, dag_run=dag_run)


def _task_identity(ti: Any) -> tuple[str, int]:
    """Return one task instance's ``(task_id, map_index)`` identity.

    Parameters:
        ti: Any containing an ORM TaskInstance.

    Returns:
        tuple[str, int] identifying the instance within its DagRun.
    """

    return (str(ti.task_id), int(ti.map_index))


def _next_pending_task_instance(
    dag_run: DagRun,
    dag: Any,
    session: Session | None,
    attempted: set[tuple[str, int]],
) -> TaskInstance | None:
    """Return the first unattempted task instance in dependency-safe order.

    The instance list is re-fetched on every call because mapped expansion
    replaces placeholder instances mid-run.

    Parameters:
        dag_run: airflow.models.dagrun.DagRun being executed.
        dag: Any exposing Airflow's ``topological_sort`` method.
        session: sqlalchemy.orm.Session | None used to fetch task instances.
        attempted: set[tuple[str, int]] containing already-processed identities.

    Returns:
        airflow.models.taskinstance.TaskInstance | None when every instance settled.
    """

    for ti in ordered_task_instances(dag_run, dag, session=session):
        if _task_identity(ti) not in attempted:
            return ti
    return None


def _try_expand_mapped_task(dag_run: DagRun, task_id: str, session: Session | None) -> bool:
    """Expand one persisted mapped task once its upstream values exist.

    Expansion failure is expected when an upstream the mapping depends on did
    not succeed; the placeholder instance is left for ``update_state`` to mark
    ``upstream_failed`` exactly as the scheduler would.

    Parameters:
        dag_run: airflow.models.dagrun.DagRun owning the mapped placeholder.
        task_id: str identifying the mapped task.
        session: sqlalchemy.orm.Session | None owning the persisted metadata.

    Returns:
        bool reporting whether expansion succeeded.

    Raises:
        ValueError: ``session`` is absent, so mapped metadata cannot be expanded.
    """

    if session is None:
        raise ValueError(f"Mapped task '{task_id}' requires a metadata `session` to expand")
    try:
        from airflow.models.serialized_dag import SerializedDagModel

        scheduler_dag = SerializedDagModel.get_dag(str(dag_run.dag_id), session=session)
        if scheduler_dag is None:
            raise RuntimeError("Airflow did not return the persisted scheduler Dag")
        expand_mapped_task_instances(scheduler_dag.get_task(task_id), str(dag_run.run_id), session)
    except Exception as error:
        LOGGER.warning(
            f"Could not expand mapped task '{task_id}' for DagRun '{dag_run.run_id}': {error}"
        )
        session.rollback()
        return False
    return True


def _settle_dag_run(dag_run: DagRun, dag: Any, session: Session | None) -> None:
    """Drive DagRun state and unrunnable task instances to a fixed point.

    One ``update_state`` pass marks only the ``upstream_failed`` instances whose
    upstreams are already terminal, so failure chains settle over several passes.
    The pass count is bounded by the instance count plus the final DagRun pass.

    Parameters:
        dag_run: airflow.models.dagrun.DagRun being settled.
        dag: Any exposing Airflow's ``topological_sort`` method.
        session: sqlalchemy.orm.Session | None owning the persisted metadata.
    """

    previous: list[tuple[str, int, Any]] | None = None
    bound = len(ordered_task_instances(dag_run, dag, session=session)) + 2
    for _ in range(bound):
        dag_run.update_state(**_session_kwargs(session), execute_callbacks=False)
        current = [
            (*_task_identity(ti), ti.state)
            for ti in ordered_task_instances(dag_run, dag, session=session)
        ]
        if current == previous:
            return
        previous = current
    LOGGER.warning(
        f"DagRun '{dag_run.run_id}' task states did not stabilize within '{bound}' passes"
    )


def _pull_return_value(
    ti: TaskInstance,
    task_id: str,
    map_index: int,
    session: Session | None,
) -> Any:
    """Pull one instance's default ``return_value`` XCom.

    Parameters:
        ti: airflow.models.taskinstance.TaskInstance whose XCom is pulled.
        task_id: str identifying the task.
        map_index: int identifying the mapped instance, or ``-1`` when unmapped.
        session: sqlalchemy.orm.Session | None used for the metadata query.

    Returns:
        Any containing the pulled value, or ``None`` when nothing was pushed.
    """

    kwargs: dict[str, Any] = {"task_ids": task_id, **_session_kwargs(session)}
    if map_index >= 0:
        kwargs["map_indexes"] = map_index
    return ti.xcom_pull(**kwargs)


def _snapshot_dag_run(
    dag_run: DagRun,
    dag: Any,
    session: Session | None,
    *,
    errors: dict[str, BaseException],
    executed: list[str],
) -> DagRunResult:
    """Capture one settled DagRun into an inert result snapshot.

    Parameters:
        dag_run: airflow.models.dagrun.DagRun that finished settling.
        dag: Any exposing Airflow's ``topological_sort`` method.
        session: sqlalchemy.orm.Session | None owning the persisted metadata.
        errors: dict[str, BaseException] containing captured task-body exceptions.
        executed: list[str] containing task keys in actual execution order.

    Returns:
        pytest_airflow_in_a_box.results.DagRunResult containing the settled outcome.
    """

    if session is not None:
        session.expire_all()
    task_results = []
    for ti in ordered_task_instances(dag_run, dag, session=session):
        task_id, map_index = _task_identity(ti)
        key = task_key(task_id, map_index)
        task_results.append(
            TaskResult(
                task_id=task_id,
                map_index=map_index,
                state=ti.state,
                xcom=_pull_return_value(ti, task_id, map_index, session),
                error=errors.get(key),
                ti=ti,
            )
        )
    state = dag_run.state
    return DagRunResult(
        dag_run=dag_run,
        dag_id=str(dag_run.dag_id),
        run_id=str(dag_run.run_id),
        state=state,
        success=bool(getattr(state, "value", state) == "success"),
        tasks=tuple(task_results),
        executed=tuple(executed),
    )


def execute_dag_run(
    dag_run: DagRun,
    dag: Any,
    *,
    session: Session | None = None,
    run_triggerer: bool = False,
    trigger_timeout: float = DEFAULT_TRIGGER_TIMEOUT,
) -> DagRunResult:
    """Execute every task instance of one DagRun and return an inert snapshot.

    Instances run in dependency-safe order with default dependency semantics,
    one attempt each. A raising task body is captured and execution continues,
    scheduler-shaped: blocked downstreams settle as ``upstream_failed`` and the
    snapshot reports ``success=False``. Mapped tasks expand mid-run once their
    upstream values exist. A deferring task settles ``deferred`` unless
    ``run_triggerer`` resumes it inline.

    Parameters:
        dag_run: airflow.models.dagrun.DagRun whose task instances are executed.
        dag: Any containing the authoring Dag with executable task objects.
        session: sqlalchemy.orm.Session | None owning the persisted metadata.
        run_triggerer: bool running persisted trigger events and resuming deferrals.
        trigger_timeout: float seconds allowed for each persisted trigger's first event.

    Returns:
        pytest_airflow_in_a_box.results.DagRunResult containing the settled outcome.

    Raises:
        ValueError: A mapped task must expand but no ``session`` was supplied, or a
            fetched task instance is absent from the supplied Dag graph.
        TaskResolutionError: No executable task can be resolved for an instance.
    """

    errors: dict[str, BaseException] = {}
    executed: list[str] = []
    attempted: set[tuple[str, int]] = set()
    expansion_attempted: set[str] = set()
    while (ti := _next_pending_task_instance(dag_run, dag, session, attempted)) is not None:
        task_id, map_index = _task_identity(ti)
        state = ti.state
        if state is not None:
            # Pre-settled instances (for example branch-skipped downstreams) never run.
            attempted.add((task_id, map_index))
            continue
        if (
            map_index < 0
            and task_id not in expansion_attempted
            and getattr(dag.get_task(task_id), "is_mapped", False)
        ):
            expansion_attempted.add(task_id)
            if not _try_expand_mapped_task(dag_run, task_id, session):
                attempted.add((task_id, map_index))
            continue
        attempted.add((task_id, map_index))
        key = task_key(task_id, map_index)
        try:
            run_task_instance(
                ti,
                dag.get_task(task_id),
                run_triggerer=run_triggerer,
                trigger_timeout=trigger_timeout,
                session=session,
            )
        except Exception as error:
            errors[key] = error
            executed.append(key)
        else:
            # An instance whose dependencies were unmet keeps its `None` state: not executed.
            if ti.state != state:
                executed.append(key)
    _settle_dag_run(dag_run, dag, session)
    return _snapshot_dag_run(dag_run, dag, session, errors=errors, executed=executed)


__all__ = (
    "DEFAULT_TRIGGER_TIMEOUT",
    "TaskResolutionError",
    "TriggerExecutionError",
    "execute_dag_run",
    "ordered_task_instances",
    "run_task_instance",
    "run_trigger",
)
