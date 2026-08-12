"""Test task-run compatibility branches without executing Airflow workers."""

from __future__ import annotations

import asyncio
import re
import sys
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest
from airflow.models.taskinstance import TaskInstance
from airflow.utils.state import TaskInstanceState

from pytest_airflow_in_a_box._compat import taskrun
from pytest_airflow_in_a_box._compat.capabilities import TaskInstanceRunner
from pytest_airflow_in_a_box.taskinstance import (
    DEFAULT_TRIGGER_TIMEOUT,
    TaskResolutionError,
    TriggerExecutionError,
    execute_dag_run,
    ordered_task_instances,
    run_task_instance,
    run_trigger,
)


class _Session:
    """Record transaction and identity-map lifecycle operations."""

    def __init__(self) -> None:
        """Initialize lifecycle counters."""

        self.commits = 0
        self.expirations = 0

    def commit(self) -> None:
        """Record one commit."""

        self.commits += 1

    def expire_all(self) -> None:
        """Record one identity-map expiration."""

        self.expirations += 1


class _TaskInstance:
    """Expose the task-instance behavior required by both runner branches."""

    dag_id = "compat_dag"
    run_id = "compat_run"
    task_id = "compat_task"
    map_index = -1

    def __init__(self) -> None:
        """Initialize recorded calls and representative state."""

        self.task: Any = None
        self.run_kwargs: dict[str, Any] | None = None
        self.check_kwargs: dict[str, Any] | None = None
        self.refreshes = 0
        self.state: Any = None

    def refresh_from_task(self, task: Any, **kwargs: Any) -> None:
        """Attach a task while accepting release-specific keywords."""

        del kwargs
        self.task = task

    def run(self, **kwargs: Any) -> None:
        """Record legacy execution arguments."""

        self.run_kwargs = kwargs

    def refresh_from_db(self, **kwargs: Any) -> None:
        """Record a persisted-state refresh."""

        del kwargs
        self.refreshes += 1

    def check_and_change_state_before_execution(self, **kwargs: Any) -> bool:
        """Record dependency arguments and permit execution."""

        self.check_kwargs = kwargs
        return True

    def set_state(self, state: Any, **kwargs: Any) -> bool:
        """Record one state transition."""

        del kwargs
        self.state = state
        return True


class _EmptyTrigger:
    """Complete without emitting a trigger event."""

    cleaned = False

    async def run(self):
        if False:
            yield None

    async def cleanup(self) -> None:
        type(self).cleaned = True


class _EventTrigger:
    """Emit one event, then record cleanup."""

    def __init__(self, value: int) -> None:
        """Store the payload yielded by ``run``."""

        self.value = value
        self.cleaned = False

    async def run(self):
        """Yield one payload, then a second the caller must never observe."""

        yield {"value": self.value}
        yield {"value": None}

    async def cleanup(self) -> None:
        """Record cleanup."""

        self.cleaned = True


class _SilentTrigger:
    """Await far longer than any test timeout without emitting an event."""

    def __init__(self) -> None:
        """Initialize the cleanup flag."""

        self.cleaned = False

    async def run(self):
        """Sleep, then yield an event no caller waits for."""

        await asyncio.sleep(30)
        yield {"value": "late"}

    async def cleanup(self) -> None:
        """Record cleanup."""

        self.cleaned = True


class _SyncRunTrigger:
    """Return a plain value from ``run`` instead of an async iterator."""

    def run(self) -> None:
        """Return a non-iterable value."""

        return

    async def cleanup(self) -> None:
        """Do nothing."""


def _capabilities(
    runner: TaskInstanceRunner,
    *,
    dag_run_refresh: bool = False,
) -> SimpleNamespace:
    """Build the capability fields consumed by the task-run shim.

    Parameters:
        runner: TaskInstanceRunner selecting the tested branch.
        dag_run_refresh: bool indicating DagRun-aware task refresh support.

    Returns:
        types.SimpleNamespace containing the required capability values.
    """

    return SimpleNamespace(
        task_instance_runner=runner,
        refresh_from_task_supports_dag_run=dag_run_refresh,
    )


def test_legacy_runner_forwards_every_flag_and_refreshes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Forward the complete 3.1 call contract by keyword and return the original TI."""

    ti: Any = _TaskInstance()
    task: Any = object()
    session: Any = _Session()
    monkeypatch.setattr(
        taskrun,
        "resolve_capabilities",
        lambda: _capabilities(TaskInstanceRunner.LEGACY_RUN),
    )

    result = run_task_instance(
        ti,
        task,
        ignore_depends_on_past=True,
        ignore_task_deps=True,
        ignore_ti_state=True,
        mark_success=True,
        session=session,
    )

    assert result is ti
    assert ti.task is task
    assert ti.run_kwargs == {
        "ignore_depends_on_past": True,
        "ignore_task_deps": True,
        "ignore_ti_state": True,
        "mark_success": True,
        "session": session,
    }
    assert session.expirations == 1
    assert ti.refreshes == 1


def test_legacy_runner_resolves_attached_task_without_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use the TI's attached task and refresh through Airflow's implicit session."""

    ti: Any = _TaskInstance()
    attached_task: Any = object()
    ti.task = attached_task
    monkeypatch.setattr(
        taskrun,
        "resolve_capabilities",
        lambda: _capabilities(TaskInstanceRunner.LEGACY_RUN),
    )

    result = run_task_instance(ti)

    assert result is ti
    assert ti.task is attached_task
    assert ti.refreshes == 1


def test_runner_resolves_task_from_persisted_dagrun(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resolve an omitted task from persisted DagRun metadata when none is attached."""

    ti: Any = _TaskInstance()
    persisted_task: Any = object()
    scheduler_dag = SimpleNamespace(get_task=lambda _task_id: persisted_task)
    ti.get_dagrun = lambda: SimpleNamespace(get_dag=lambda: scheduler_dag)
    monkeypatch.setattr(taskrun, "lookup_authoring_dag", lambda _dag_id: None)
    monkeypatch.setattr(
        taskrun,
        "resolve_capabilities",
        lambda: _capabilities(TaskInstanceRunner.LEGACY_RUN),
    )

    result = run_task_instance(ti)

    assert result is ti
    assert ti.task is persisted_task


def test_resolve_task_prefers_registered_authoring_dag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resolve a detached instance through the registered authoring Dag."""

    ti: Any = _TaskInstance()
    registered_task: Any = object()
    authoring_dag = SimpleNamespace(
        task_dict={"compat_task": registered_task},
        get_task=lambda _task_id: registered_task,
    )
    ti.get_dagrun = lambda **_kwargs: pytest.fail("DagRun fallback must not run")
    monkeypatch.setattr(taskrun, "lookup_authoring_dag", lambda _dag_id: authoring_dag)
    monkeypatch.setattr(
        taskrun,
        "resolve_capabilities",
        lambda: _capabilities(TaskInstanceRunner.LEGACY_RUN),
    )

    result = run_task_instance(ti)

    assert result is ti
    assert ti.task is registered_task


def test_resolve_task_falls_back_when_task_absent_from_registered_dag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use the DagRun fallback when the registered Dag lacks the task identifier."""

    ti: Any = _TaskInstance()
    persisted_task: Any = object()
    scheduler_dag = SimpleNamespace(get_task=lambda _task_id: persisted_task)
    ti.get_dagrun = lambda: SimpleNamespace(get_dag=lambda: scheduler_dag)
    monkeypatch.setattr(
        taskrun,
        "lookup_authoring_dag",
        lambda _dag_id: SimpleNamespace(task_dict={}),
    )
    monkeypatch.setattr(
        taskrun,
        "resolve_capabilities",
        lambda: _capabilities(TaskInstanceRunner.LEGACY_RUN),
    )

    result = run_task_instance(ti)

    assert result is ti
    assert ti.task is persisted_task


def _prepare_sdk_runner(
    monkeypatch: pytest.MonkeyPatch,
    ti: _TaskInstance,
    result: Any,
    *,
    dag_run_refresh: bool = False,
) -> list[tuple[Any, Any]]:
    """Install deterministic Airflow 3.2+ task-run internals.

    Parameters:
        monkeypatch: pytest.MonkeyPatch applying replacements.
        ti: _TaskInstance returned by the persisted-instance lookup.
        result: Any returned by Airflow's private task runner.
        dag_run_refresh: bool selecting the Airflow 3.3 refresh signature.

    Returns:
        list[tuple[Any, Any]] recording private runner invocations.
    """

    calls: list[tuple[Any, Any]] = []
    dag_run = object()
    scheduler_task = object()
    monkeypatch.setattr(
        taskrun,
        "resolve_capabilities",
        lambda: _capabilities(
            TaskInstanceRunner.SDK_RUN_TASK,
            dag_run_refresh=dag_run_refresh,
        ),
    )
    monkeypatch.setattr(
        taskrun, "_scheduler_task", lambda _instance, _session: (dag_run, scheduler_task)
    )
    monkeypatch.setattr(TaskInstance, "get_task_instance", lambda **_kwargs: ti)

    def run_task(*, ti: Any, task: Any) -> Any:
        """Record and return one private runner result."""

        calls.append((ti, task))
        return result

    from airflow.sdk.definitions import dag as sdk_dag

    monkeypatch.setattr(sdk_dag, "_run_task", run_task)
    return calls


def test_sdk_runner_checks_dependencies_and_refreshes_original(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Apply every supported dependency flag before private Task SDK execution."""

    ti: Any = _TaskInstance()
    session: Any = _Session()
    task: Any = object()
    calls = _prepare_sdk_runner(monkeypatch, ti, SimpleNamespace(error=None), dag_run_refresh=True)

    result = run_task_instance(
        ti,
        task,
        ignore_depends_on_past=True,
        ignore_task_deps=True,
        ignore_ti_state=True,
        session=session,
    )

    assert result is ti
    assert ti.check_kwargs == {
        "ignore_depends_on_past": True,
        "ignore_task_deps": True,
        "ignore_ti_state": True,
        "mark_success": False,
        "session": session,
    }
    assert calls == [(ti, task)]
    assert session.commits == 1
    assert session.expirations == 1
    assert ti.refreshes == 1


def test_sdk_mark_success_skips_private_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set persisted success without invoking authored task code."""

    ti: Any = _TaskInstance()
    session: Any = _Session()
    task: Any = object()
    calls = _prepare_sdk_runner(monkeypatch, ti, SimpleNamespace(error=None))

    result = run_task_instance(ti, task, mark_success=True, session=session)

    assert result is ti
    assert calls == []
    assert ti.state == TaskInstanceState.SUCCESS
    assert session.commits == 2
    assert session.expirations == 1


def test_sdk_runner_rejects_detached_task_instance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Require a metadata session before Execution API-backed task execution."""

    from sqlalchemy import orm

    ti: Any = _TaskInstance()
    task: Any = object()
    monkeypatch.setattr(
        taskrun,
        "resolve_capabilities",
        lambda: _capabilities(TaskInstanceRunner.SDK_RUN_TASK),
    )
    monkeypatch.setattr(orm, "object_session", lambda _instance: None)

    with pytest.raises(RuntimeError, match="requires a persisted task instance"):
        run_task_instance(ti, task)


@pytest.mark.parametrize("failure_point", ["session", "dag_run", "scheduler_dag"])
def test_scheduler_task_resolution_errors_are_actionable(
    monkeypatch: pytest.MonkeyPatch,
    failure_point: str,
) -> None:
    """Retain each missing persisted scheduler resource as the domain-error cause."""

    from airflow.models.serialized_dag import SerializedDagModel

    ti: Any = _TaskInstance()
    if failure_point == "session":
        session: Any = None
    else:
        dag_run = None if failure_point == "dag_run" else object()
        session = SimpleNamespace(scalar=lambda _query: dag_run)
    scheduler_dag = None if failure_point == "scheduler_dag" else object()

    def get_dag(_dag_id: str, *, session: Any) -> Any:
        """Return or omit representative serialized Dag metadata."""

        del session
        return scheduler_dag

    monkeypatch.setattr(
        SerializedDagModel,
        "get_dag",
        staticmethod(get_dag),
    )
    resolved_session: Any = session

    with pytest.raises(TaskResolutionError, match="scheduler task") as caught:
        taskrun._scheduler_task(ti, resolved_session)

    assert isinstance(caught.value.__cause__, RuntimeError)


def test_sdk_dependency_rejection_returns_refreshed_original(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return without execution when Airflow's dependency gate rejects the TI."""

    ti: Any = _TaskInstance()
    ti.check_and_change_state_before_execution = lambda **_kwargs: False
    session: Any = _Session()
    task: Any = object()
    calls = _prepare_sdk_runner(monkeypatch, ti, SimpleNamespace(error=None))

    result = run_task_instance(ti, task, session=session)

    assert result is ti
    assert calls == []
    assert session.expirations == 1
    assert ti.refreshes == 1


def test_sdk_no_result_raises_after_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    """Report an absent private result only after refreshing persisted side effects."""

    ti: Any = _TaskInstance()
    session: Any = _Session()
    task: Any = object()
    _prepare_sdk_runner(monkeypatch, ti, None)

    with pytest.raises(RuntimeError, match="failed to finish with a result"):
        run_task_instance(ti, task, session=session)

    assert session.expirations == 1
    assert ti.refreshes == 1


def test_sdk_result_error_is_propagated_after_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise Airflow's original task error after refreshing the caller-owned TI."""

    ti: Any = _TaskInstance()
    session: Any = _Session()
    task: Any = object()
    failure = ValueError("task failed")
    _prepare_sdk_runner(monkeypatch, ti, SimpleNamespace(error=failure))

    with pytest.raises(ValueError, match="task failed") as caught:
        run_task_instance(ti, task, session=session)

    assert caught.value is failure
    assert session.expirations == 1
    assert ti.refreshes == 1


def test_run_triggerer_returns_a_non_deferred_task(monkeypatch: pytest.MonkeyPatch) -> None:
    """Leave a terminal task alone when trigger execution was requested."""

    ti: Any = _TaskInstance()
    ti.state = TaskInstanceState.SUCCESS
    monkeypatch.setattr(
        taskrun,
        "resolve_capabilities",
        lambda: _capabilities(TaskInstanceRunner.LEGACY_RUN),
    )

    task: Any = object()
    result = run_task_instance(ti, task, run_triggerer=True)

    assert result is ti


def test_legacy_run_triggerer_resumes_a_deferred_task(monkeypatch: pytest.MonkeyPatch) -> None:
    """Resume through the legacy runner after submitting one trigger event."""

    ti: Any = _TaskInstance()
    states = iter([TaskInstanceState.DEFERRED, TaskInstanceState.SUCCESS])

    def run(**kwargs: Any) -> None:
        ti.run_kwargs = kwargs
        ti.state = next(states)

    ti.run = run
    resumed: list[Any] = []
    monkeypatch.setattr(
        taskrun,
        "resolve_capabilities",
        lambda: _capabilities(TaskInstanceRunner.LEGACY_RUN),
    )
    monkeypatch.setattr(
        taskrun,
        "_resume_deferred_task_instance",
        lambda instance, session, *, trigger_timeout: resumed.append(
            (instance, session, trigger_timeout)
        ),
    )
    session: Any = _Session()

    task: Any = object()
    result = run_task_instance(ti, task, run_triggerer=True, session=session)

    assert result.state == TaskInstanceState.SUCCESS
    assert resumed == [(ti, session, DEFAULT_TRIGGER_TIMEOUT)]
    assert ti.run_kwargs["ignore_ti_state"] is True


def test_resume_deferred_requires_a_session() -> None:
    """Reject trigger execution detached from persisted metadata."""

    ti: Any = _TaskInstance()
    with pytest.raises(RuntimeError, match="requires a persisted task instance session"):
        taskrun._resume_deferred_task_instance(ti, None)


def test_resume_deferred_requires_a_trigger(monkeypatch: pytest.MonkeyPatch) -> None:
    """Report a deferred task whose trigger relationship is missing."""

    ti: Any = _TaskInstance()
    ti.trigger_id = None
    session: Any = _Session()
    legacy_module: Any = ModuleType("airflow.utils.module_loading")
    legacy_module.import_string = lambda _path: None
    monkeypatch.setitem(sys.modules, "airflow.sdk._shared.module_loading", None)
    monkeypatch.setitem(sys.modules, "airflow.utils.module_loading", legacy_module)

    with pytest.raises(RuntimeError, match="has no persisted trigger"):
        taskrun._resume_deferred_task_instance(ti, session)


def test_resume_deferred_requires_the_trigger_row() -> None:
    """Report a trigger identifier with no metadata row."""

    ti: Any = _TaskInstance()
    ti.trigger_id = 7
    session: Any = SimpleNamespace(
        expire_all=lambda: None,
        get=lambda _model, _identity: None,
    )

    with pytest.raises(RuntimeError, match="trigger '7' is absent"):
        taskrun._resume_deferred_task_instance(ti, session)


def test_resume_deferred_requires_one_event() -> None:
    """Report a trigger that finishes without yielding an event."""

    ti: Any = _TaskInstance()
    ti.trigger_id = 8
    row = SimpleNamespace(classpath=f"{__name__}._EmptyTrigger", kwargs={})
    session: Any = SimpleNamespace(
        expire_all=lambda: None,
        get=lambda _model, _identity: row,
    )
    _EmptyTrigger.cleaned = False

    with pytest.raises(RuntimeError, match="completed without an event"):
        taskrun._resume_deferred_task_instance(ti, session)

    assert _EmptyTrigger.cleaned is True


def test_run_trigger_returns_the_first_event() -> None:
    """Stop at the first event and clean the trigger up."""

    trigger = _EventTrigger(42)

    event = run_trigger(trigger)

    assert event == {"value": 42}
    assert trigger.cleaned is True


def test_run_trigger_reports_a_trigger_without_events() -> None:
    """Name a trigger whose generator completes without yielding."""

    _EmptyTrigger.cleaned = False

    with pytest.raises(TriggerExecutionError, match="completed without an event"):
        run_trigger(_EmptyTrigger())

    assert _EmptyTrigger.cleaned is True


def test_run_trigger_reports_a_timeout() -> None:
    """Bound a trigger that never emits and retain the timeout cause."""

    trigger = _SilentTrigger()
    expected = re.escape("yielded no event within '0.01' seconds")

    with pytest.raises(TriggerExecutionError, match=expected) as caught:
        run_trigger(trigger, timeout=0.01)

    assert isinstance(caught.value.__cause__, asyncio.TimeoutError)
    assert trigger.cleaned is True


def test_run_trigger_rejects_a_non_positive_timeout() -> None:
    """Reject a timeout that can never admit an event."""

    with pytest.raises(ValueError, match="`timeout` must be positive, got '0'"):
        run_trigger(_EventTrigger(1), timeout=0)


def test_run_trigger_rejects_a_non_trigger() -> None:
    """Name the trigger methods a supplied object is missing."""

    with pytest.raises(TypeError, match="lacks 'run', 'cleanup'"):
        run_trigger(object())


def test_run_trigger_rejects_a_synchronous_run() -> None:
    """Reject a ``run`` that does not return an async iterator."""

    with pytest.raises(TypeError, match="did not return an async iterator"):
        run_trigger(_SyncRunTrigger())


def test_resume_deferred_forwards_the_trigger_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pass the caller's trigger timeout through to ``run_trigger``."""

    ti: Any = _TaskInstance()
    ti.trigger_id = 9
    row = SimpleNamespace(classpath=f"{__name__}._EventTrigger", kwargs={"value": 7})
    submitted: list[Any] = []
    trigger_module: Any = SimpleNamespace(
        Trigger=SimpleNamespace(
            submit_event=lambda trigger_id, event, session: submitted.append(
                (trigger_id, event, session)
            )
        )
    )
    monkeypatch.setitem(sys.modules, "airflow.models.trigger", trigger_module)
    timeouts: list[float] = []

    def fake_run_trigger(trigger: Any, *, timeout: float) -> Any:
        timeouts.append(timeout)
        return {"value": trigger.value}

    monkeypatch.setattr(taskrun, "run_trigger", fake_run_trigger)
    session: Any = SimpleNamespace(
        commit=lambda: None,
        expire_all=lambda: None,
        get=lambda _model, _identity: row,
    )

    taskrun._resume_deferred_task_instance(ti, session, trigger_timeout=1.5)

    assert timeouts == [1.5]
    assert submitted == [(9, {"value": 7}, session)]


def test_missing_task_resolution_retains_cause(monkeypatch: pytest.MonkeyPatch) -> None:
    """Name missing task metadata, hint at the fix, and retain the Airflow lookup failure."""

    ti: Any = _TaskInstance()
    failure = LookupError("missing DagRun")

    def fail_lookup(**kwargs: Any) -> None:
        """Raise the representative persisted-task lookup failure."""

        del kwargs
        raise failure

    ti.get_dagrun = fail_lookup
    monkeypatch.setattr(taskrun, "lookup_authoring_dag", lambda _dag_id: None)

    with pytest.raises(TaskResolutionError, match="compat_task") as caught:
        run_task_instance(ti)

    assert caught.value.__cause__ is failure
    assert "pass `task=dag.get_task('compat_task')`" in str(caught.value)


def test_ordered_task_instances_sorts_graph_then_map_index() -> None:
    """Keep mapped instances adjacent and ascending within topological rank."""

    task_instances = [
        SimpleNamespace(task_id="a_downstream", map_index=-1),
        SimpleNamespace(task_id="z_upstream", map_index=2),
        SimpleNamespace(task_id="z_upstream", map_index=0),
        SimpleNamespace(task_id="z_upstream", map_index=1),
    ]
    dag_run: Any = SimpleNamespace(
        run_id="ordered",
        get_task_instances=lambda **_kwargs: task_instances,
    )
    dag = SimpleNamespace(
        dag_id="ordered_dag",
        topological_sort=lambda: [
            SimpleNamespace(task_id="z_upstream"),
            SimpleNamespace(task_id="a_downstream"),
        ],
    )

    assert [(ti.task_id, ti.map_index) for ti in ordered_task_instances(dag_run, dag)] == [
        ("z_upstream", 0),
        ("z_upstream", 1),
        ("z_upstream", 2),
        ("a_downstream", -1),
    ]


def test_ordered_task_instances_rejects_unknown_task_ids() -> None:
    """Report every fetched task ID absent from the supplied Dag graph."""

    dag_run: Any = SimpleNamespace(
        run_id="unknown_tasks",
        get_task_instances=lambda **_kwargs: [
            SimpleNamespace(task_id="missing_b", map_index=-1),
            SimpleNamespace(task_id="missing_a", map_index=-1),
        ],
    )
    dag = SimpleNamespace(
        dag_id="known_dag",
        topological_sort=lambda: [SimpleNamespace(task_id="known")],
    )

    session: Any = object()
    with pytest.raises(ValueError, match="'missing_a', 'missing_b'"):
        ordered_task_instances(dag_run, dag, session=session)


class _ExecutionTaskInstance:
    """Expose the instance surface ``execute_dag_run`` reads and mutates."""

    def __init__(
        self,
        task_id: str,
        map_index: int = -1,
        state: Any = None,
        xcom: Any = None,
    ) -> None:
        """Store identity, state, and the value ``xcom_pull`` returns.

        Parameters:
            task_id: str identifying the task.
            map_index: int identifying the mapped instance, or ``-1`` when unmapped.
            state: Any containing the initial persisted state.
            xcom: Any returned by ``xcom_pull``.
        """

        self.task_id = task_id
        self.map_index = map_index
        self.state = state
        self.xcom = xcom
        self.pull_kwargs: dict[str, Any] | None = None

    def xcom_pull(self, **kwargs: Any) -> Any:
        """Record the pull arguments and return the stored value.

        Parameters:
            kwargs: Any containing the pull keyword arguments.

        Returns:
            Any containing the stored xcom value.
        """

        self.pull_kwargs = kwargs
        return self.xcom


class _ExecutionDagRun:
    """Expose the DagRun surface ``execute_dag_run`` drives, with a scripted settle."""

    def __init__(self, tis: list[_ExecutionTaskInstance], settle: Any = None) -> None:
        """Store the mutable instance list and the per-``update_state`` script.

        Parameters:
            tis: list[_ExecutionTaskInstance] returned by ``get_task_instances``.
            settle: Any called with this run on every ``update_state``.
        """

        self.dag_id = "execute_dag"
        self.run_id = "execute_run"
        self.state: Any = "running"
        self.tis = tis
        self.update_calls = 0
        self._settle = settle

    def get_task_instances(self, **kwargs: Any) -> list[_ExecutionTaskInstance]:
        """Return the current instance list.

        Parameters:
            kwargs: Any containing an optional session.

        Returns:
            list[_ExecutionTaskInstance] currently owned by the run.
        """

        del kwargs
        return list(self.tis)

    def update_state(self, **kwargs: Any) -> None:
        """Record one settle pass and apply the scripted state changes.

        Parameters:
            kwargs: Any containing the settle keyword arguments.
        """

        del kwargs
        self.update_calls += 1
        if self._settle is not None:
            self._settle(self)


class _ExecutionSession:
    """Record identity-map and transaction operations."""

    def __init__(self) -> None:
        """Initialize lifecycle counters."""

        self.expirations = 0
        self.rollbacks = 0

    def expire_all(self) -> None:
        """Record one identity-map expiration."""

        self.expirations += 1

    def rollback(self) -> None:
        """Record one transaction rollback."""

        self.rollbacks += 1


def _execution_dag(*tasks: SimpleNamespace) -> SimpleNamespace:
    """Build one Dag fake exposing graph order and task lookup.

    Parameters:
        tasks: SimpleNamespace entries carrying ``task_id`` and ``is_mapped``.

    Returns:
        types.SimpleNamespace exposing ``topological_sort`` and ``get_task``.
    """

    by_id = {task.task_id: task for task in tasks}
    return SimpleNamespace(
        dag_id="execute_dag",
        topological_sort=lambda: list(by_id.values()),
        get_task=lambda task_id: by_id[task_id],
    )


def test_execute_dag_run_runs_every_instance_and_snapshots(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Run instances in graph order, settle, and capture states, xcoms, and order."""

    upstream = _ExecutionTaskInstance("produce", xcom=21)
    downstream = _ExecutionTaskInstance("consume", xcom=42)
    dag_run: Any = _ExecutionDagRun(
        [downstream, upstream],
        settle=lambda run: setattr(run, "state", "success"),
    )
    dag = _execution_dag(
        SimpleNamespace(task_id="produce", is_mapped=False),
        SimpleNamespace(task_id="consume", is_mapped=False),
    )
    session: Any = _ExecutionSession()
    executed: list[str] = []

    def fake_run(ti: Any, task: Any, **kwargs: Any) -> Any:
        del kwargs
        executed.append(str(task.task_id))
        ti.state = "success"
        return ti

    monkeypatch.setattr(taskrun, "run_task_instance", fake_run)

    result = execute_dag_run(dag_run, dag, session=session)

    assert result.success
    assert executed == ["produce", "consume"]
    assert result.order == ["produce", "consume"]
    assert result.states == {"produce": "success", "consume": "success"}
    assert result.xcoms == {"produce": 21, "consume": 42}
    assert result.errors == {}
    assert session.expirations == 1
    assert upstream.pull_kwargs == {"task_ids": "produce", "session": session}


def test_execute_dag_run_captures_failures_and_leaves_blocked_instances(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Capture a raising body, keep going, and let settle mark blocked instances."""

    boom = _ExecutionTaskInstance("boom")
    consume = _ExecutionTaskInstance("consume")

    def settle(run: Any) -> None:
        consume.state = "upstream_failed"
        run.state = "failed"

    dag_run: Any = _ExecutionDagRun([boom, consume], settle=settle)
    dag = _execution_dag(
        SimpleNamespace(task_id="boom", is_mapped=False),
        SimpleNamespace(task_id="consume", is_mapped=False),
    )

    def fake_run(ti: Any, task: Any, **kwargs: Any) -> Any:
        del task, kwargs
        if ti.task_id == "boom":
            ti.state = "failed"
            raise ValueError("nope")
        return ti  # Unmet dependencies: the instance keeps its `None` state.

    monkeypatch.setattr(taskrun, "run_task_instance", fake_run)

    result = execute_dag_run(dag_run, dag)

    assert not result.success
    assert result.order == ["boom"]
    assert result.states == {"boom": "failed", "consume": "upstream_failed"}
    assert isinstance(result.errors["boom"], ValueError)
    assert consume.pull_kwargs == {"task_ids": "consume"}


def test_execute_dag_run_never_reruns_presettled_instances(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Record a pre-settled instance without executing it."""

    skipped_ti = _ExecutionTaskInstance("branch_dropped", state="skipped")
    dag_run: Any = _ExecutionDagRun([skipped_ti])
    dag = _execution_dag(SimpleNamespace(task_id="branch_dropped", is_mapped=False))

    def fail_run(ti: Any, task: Any, **kwargs: Any) -> Any:
        del ti, task, kwargs
        raise AssertionError("pre-settled instances must not run")

    monkeypatch.setattr(taskrun, "run_task_instance", fail_run)

    result = execute_dag_run(dag_run, dag)

    assert result.order == []
    assert result.states == {"branch_dropped": "skipped"}
    assert not result.success


def test_execute_dag_run_expands_mapped_placeholders(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Replace one mapped placeholder mid-run and execute every expanded instance."""

    placeholder = _ExecutionTaskInstance("double")
    dag_run: Any = _ExecutionDagRun(
        [placeholder],
        settle=lambda run: setattr(run, "state", "success"),
    )
    dag = _execution_dag(SimpleNamespace(task_id="double", is_mapped=True))
    session: Any = _ExecutionSession()
    expanded = [
        _ExecutionTaskInstance("double", map_index=0, xcom=10),
        _ExecutionTaskInstance("double", map_index=1, xcom=20),
    ]

    def fake_expand(run: Any, task_id: str, expand_session: Any) -> bool:
        del task_id, expand_session
        run.tis = expanded
        return True

    def fake_run(ti: Any, task: Any, **kwargs: Any) -> Any:
        del task, kwargs
        ti.state = "success"
        return ti

    monkeypatch.setattr(taskrun, "_try_expand_mapped_task", fake_expand)
    monkeypatch.setattr(taskrun, "run_task_instance", fake_run)

    result = execute_dag_run(dag_run, dag, session=session)

    assert result.states == {"double[0]": "success", "double[1]": "success"}
    assert result.xcoms == {"double": [10, 20]}
    assert result.order == ["double[0]", "double[1]"]
    assert expanded[1].pull_kwargs == {"task_ids": "double", "map_indexes": 1, "session": session}


def test_execute_dag_run_leaves_unexpandable_placeholders_to_settle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Record a placeholder whose expansion failed without executing it."""

    placeholder = _ExecutionTaskInstance("double")
    dag_run: Any = _ExecutionDagRun(
        [placeholder],
        settle=lambda _run: setattr(placeholder, "state", "upstream_failed"),
    )
    dag = _execution_dag(SimpleNamespace(task_id="double", is_mapped=True))
    session: Any = _ExecutionSession()

    def fail_run(ti: Any, task: Any, **kwargs: Any) -> Any:
        del ti, task, kwargs
        raise AssertionError("unexpandable placeholders must not run")

    monkeypatch.setattr(taskrun, "_try_expand_mapped_task", lambda *_args: False)
    monkeypatch.setattr(taskrun, "run_task_instance", fail_run)

    result = execute_dag_run(dag_run, dag, session=session)

    assert result.order == []
    assert result.states == {"double": "upstream_failed"}


def test_try_expand_mapped_task_requires_a_session() -> None:
    """Refuse mapped expansion without a metadata session."""

    dag_run: Any = SimpleNamespace(dag_id="execute_dag", run_id="execute_run")

    with pytest.raises(ValueError, match="requires a metadata `session`"):
        taskrun._try_expand_mapped_task(dag_run, "double", None)


def test_try_expand_mapped_task_reports_a_missing_scheduler_dag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Roll back and report failure when the persisted scheduler Dag is absent."""

    from airflow.models.serialized_dag import SerializedDagModel

    monkeypatch.setattr(
        SerializedDagModel,
        "get_dag",
        classmethod(lambda *_args, **_kwargs: None),
    )
    dag_run: Any = SimpleNamespace(dag_id="execute_dag", run_id="execute_run")
    session: Any = _ExecutionSession()

    assert taskrun._try_expand_mapped_task(dag_run, "double", session) is False
    assert session.rollbacks == 1


def test_try_expand_mapped_task_expands_the_scheduler_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Expand through the persisted scheduler Dag's task."""

    from airflow.models.serialized_dag import SerializedDagModel

    scheduler_task = object()
    scheduler_dag = SimpleNamespace(get_task=lambda _task_id: scheduler_task)
    monkeypatch.setattr(
        SerializedDagModel,
        "get_dag",
        classmethod(lambda *_args, **_kwargs: scheduler_dag),
    )
    calls: list[tuple[Any, str, Any]] = []
    monkeypatch.setattr(
        taskrun,
        "expand_mapped_task_instances",
        lambda task, run_id, session: calls.append((task, run_id, session)),
    )
    dag_run: Any = SimpleNamespace(dag_id="execute_dag", run_id="execute_run")
    session: Any = _ExecutionSession()

    assert taskrun._try_expand_mapped_task(dag_run, "double", session) is True
    assert calls == [(scheduler_task, "execute_run", session)]
    assert session.rollbacks == 0


def test_settle_dag_run_bounds_states_that_never_stabilize() -> None:
    """Stop settling after the bounded pass count when states keep changing."""

    wobble = _ExecutionTaskInstance("wobble")
    dag_run: Any = _ExecutionDagRun(
        [wobble],
        settle=lambda run: setattr(wobble, "state", run.update_calls),
    )
    dag = _execution_dag(SimpleNamespace(task_id="wobble", is_mapped=False))

    taskrun._settle_dag_run(dag_run, dag, None)

    assert dag_run.update_calls == 3
