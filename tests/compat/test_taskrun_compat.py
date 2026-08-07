"""Test task-run compatibility branches without executing Airflow workers."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from airflow.models.taskinstance import TaskInstance
from airflow.utils.state import TaskInstanceState

from pytest_airflow_in_a_box._compat import taskrun
from pytest_airflow_in_a_box._compat.capabilities import TaskInstanceRunner
from pytest_airflow_in_a_box.taskinstance import (
    TaskResolutionError,
    ordered_task_instances,
    run_task_instance,
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


def test_missing_task_resolution_retains_cause() -> None:
    """Name missing task metadata and retain the Airflow lookup failure."""

    ti: Any = _TaskInstance()
    failure = LookupError("missing DagRun")

    def fail_lookup(**kwargs: Any) -> None:
        """Raise the representative persisted-task lookup failure."""

        del kwargs
        raise failure

    ti.get_dagrun = fail_lookup

    with pytest.raises(TaskResolutionError, match="compat_task") as caught:
        run_task_instance(ti)

    assert caught.value.__cause__ is failure


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
