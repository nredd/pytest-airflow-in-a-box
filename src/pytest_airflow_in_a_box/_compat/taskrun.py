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
``ordered_task_instances`` is locally authored.

References:
    https://github.com/apache/airflow/blob/2d374f71bc81202204ac0208df07b07c280668fa/devel-common/src/tests_common/test_utils/taskinstance.py
    https://github.com/apache/airflow/pull/59835
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pytest_airflow_in_a_box._compat.capabilities import TaskInstanceRunner, resolve_capabilities

if TYPE_CHECKING:
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance
    from airflow.sdk.types import Operator
    from sqlalchemy.orm import Session


class TaskResolutionError(RuntimeError):
    """Report failure to resolve an executable task for a task instance."""


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
    try:
        dag_run = ti.get_dagrun(**_session_kwargs(session))
        return dag_run.get_dag().get_task(str(ti.task_id))
    except Exception as error:
        raise TaskResolutionError(
            f"Could not resolve task '{ti.task_id}' for DagRun '{ti.run_id}': {error}"
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


def run_task_instance(
    ti: TaskInstance,
    task: Operator | None = None,
    *,
    ignore_depends_on_past: bool = False,
    ignore_task_deps: bool = False,
    ignore_ti_state: bool = False,
    mark_success: bool = False,
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
        session: sqlalchemy.orm.Session | None used for metadata operations.

    Returns:
        airflow.models.taskinstance.TaskInstance containing refreshed persisted state.

    Raises:
        TaskResolutionError: No executable task can be resolved.
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
        return _run_legacy_task_instance(ti, resolved_task, **arguments)
    return _run_sdk_task_instance(ti, resolved_task, **arguments)


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


__all__ = ("TaskResolutionError", "ordered_task_instances", "run_task_instance")
