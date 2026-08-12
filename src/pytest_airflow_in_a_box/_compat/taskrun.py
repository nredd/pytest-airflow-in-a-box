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
``ordered_task_instances`` and ``run_trigger`` are locally authored.

References:
    https://github.com/apache/airflow/blob/2d374f71bc81202204ac0208df07b07c280668fa/devel-common/src/tests_common/test_utils/taskinstance.py
    https://github.com/apache/airflow/pull/59835
    https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html
    https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/triggers/base/index.html
"""

from __future__ import annotations

import asyncio
import logging
import time
from importlib import import_module
from typing import TYPE_CHECKING, Any

from pytest_airflow_in_a_box._compat.capabilities import TaskInstanceRunner, resolve_capabilities
from pytest_airflow_in_a_box._compat.registry import lookup_authoring_dag

if TYPE_CHECKING:
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance
    from airflow.sdk.types import Operator
    from sqlalchemy.orm import Session

LOGGER = logging.getLogger(__name__)

DEFAULT_TRIGGER_TIMEOUT = 10.0
SDK_TASK_RUN_RETRIES = 3
SDK_TASK_RUN_RETRY_DELAY_SECONDS = 0.5


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

    Retries up to ``SDK_TASK_RUN_RETRIES`` times when Airflow's in-process Execution API
    server swallows an unexpected exception and returns no result, which happens
    intermittently under concurrent xdist load when a pooled metadata connection observes a
    stale pre-commit snapshot of the persisted task instance.

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
        RuntimeError: Airflow finishes every retry without a task-run result.
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
    # NOTE(redd): https://github.com/nredd/pytest-airflow-in-a-box/issues/78
    taskrun_result: Any = None
    try:
        for attempt in range(1, SDK_TASK_RUN_RETRIES + 1):
            taskrun_result = _run_task(ti=new_ti or ti, task=task)
            if taskrun_result is not None:
                break
            if attempt < SDK_TASK_RUN_RETRIES:
                LOGGER.warning(
                    f"Airflow's in-process Execution API returned no result for task instance "
                    f"'{ti.task_id}' on attempt {attempt}/{SDK_TASK_RUN_RETRIES}; retrying "
                    f"after a possible stale metadata read"
                )
                time.sleep(SDK_TASK_RUN_RETRY_DELAY_SECONDS)
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


__all__ = (
    "DEFAULT_TRIGGER_TIMEOUT",
    "TaskResolutionError",
    "TriggerExecutionError",
    "ordered_task_instances",
    "run_task_instance",
    "run_trigger",
)
