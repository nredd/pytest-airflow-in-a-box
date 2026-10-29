"""Keep in-process task side effects out of later tasks and tests.

References:
    https://docs.python.org/3/library/os.html
"""

from __future__ import annotations

import os
from contextlib import nullcontext
from importlib import import_module
from pathlib import Path
from typing import Any

import pytest

from pytest_airflow_in_a_box.types import DagMaker, RunTask, TaskContext

pytestmark = pytest.mark.compat

PythonOperator = (
    import_module("_authoring")
    ._resolve("airflow.providers.standard.operators.python", "airflow.operators.python")
    .PythonOperator
)


@pytest.mark.requires_airflow3
@pytest.mark.parametrize("fails", [False, True])
def test_run_task_restores_process_state(
    run_task: RunTask, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fails: bool
) -> None:
    """Restore added, removed, and changed environment entries and cwd after a task.

    Parameters:
        run_task: RunTask executing the DB-free operator lifecycle.
        tmp_path: Path used as the task's temporary working directory.
        monkeypatch: pytest.MonkeyPatch seeding the caller's environment.
        fails: bool selecting a failing task body.
    """

    monkeypatch.setenv("PIAB_EXISTING", "original")
    monkeypatch.setenv("PIAB_REMOVED", "keep")
    monkeypatch.delenv("PIAB_ADDED", raising=False)
    cwd, env = Path.cwd(), dict(os.environ)

    def mutate() -> None:
        """Change worker state and optionally fail during execution."""

        os.chdir(tmp_path)
        os.environ["PIAB_EXISTING"] = "changed"
        os.environ["PIAB_ADDED"] = "new"
        del os.environ["PIAB_REMOVED"]
        if fails:
            raise RuntimeError("task failure")

    try:
        result = run_task(PythonOperator(task_id="mutate", python_callable=mutate))
        assert result.state == ("failed" if fails else "success")
        assert Path.cwd() == cwd
        assert dict(os.environ) == env
    finally:
        os.chdir(cwd)
        os.environ.clear()
        os.environ.update(env)


@pytest.mark.db_test
@pytest.mark.parametrize("fails", [False, True])
def test_run_ti_restores_process_state(dag_maker: DagMaker, tmp_path: Path, fails: bool) -> None:
    """Restore caller state even when persisted execution raises.

    Parameters:
        dag_maker: DagMaker creating the persisted task run.
        tmp_path: Path used as the task's temporary working directory.
        fails: bool selecting a failing task body.
    """

    def mutate() -> None:
        """Change the process state inside a persisted task body."""

        os.chdir(tmp_path)
        os.environ["PIAB_TASK_LEAK"] = "1"
        if fails:
            raise RuntimeError("task failure")

    with dag_maker(dag_id=f"process_state_{fails}"):
        PythonOperator(task_id="mutate", python_callable=mutate)
    cwd, env = Path.cwd(), dict(os.environ)
    try:
        if fails:
            with pytest.raises(RuntimeError, match="task failure"):
                dag_maker.run_ti("mutate")
        else:
            assert dag_maker.run_ti("mutate").state == "success"
        assert Path.cwd() == cwd
        assert dict(os.environ) == env
    finally:
        os.chdir(cwd)
        os.environ.clear()
        os.environ.update(env)


@pytest.mark.db_test
def test_whole_dag_isolates_neighboring_tasks(dag_maker: DagMaker, tmp_path: Path) -> None:
    """Keep a task's mutations out of its downstream neighbor in the same run.

    Parameters:
        dag_maker: DagMaker driving the two-task DagRun.
        tmp_path: Path used as the first task's working directory.
    """

    cwd, env = Path.cwd(), dict(os.environ)

    def mutate() -> None:
        """Contaminate process state before the downstream task runs."""

        os.chdir(tmp_path)
        os.environ["PIAB_TASK_LEAK"] = "1"

    def observe() -> None:
        """Assert that the previous task's process mutations were discarded."""

        assert Path.cwd() == cwd
        assert "PIAB_TASK_LEAK" not in os.environ

    with dag_maker(dag_id="process_state_neighbors"):
        first = PythonOperator(task_id="first", python_callable=mutate)
        second = PythonOperator(task_id="second", python_callable=observe)
        first >> second
    try:
        result = dag_maker.run()
        assert result.success
        assert not result.errors
    finally:
        os.chdir(cwd)
        os.environ.clear()
        os.environ.update(env)


@pytest.mark.requires_airflow3
@pytest.mark.parametrize("fails", [False, True])
def test_task_context_restores_process_state(
    task_context: TaskContext, tmp_path: Path, fails: bool
) -> None:
    """Scope manually driven bodies to the task-context block, including exceptions.

    Parameters:
        task_context: TaskContext preparing and scoping hand-driven execution.
        tmp_path: Path used as the task's temporary working directory.
        fails: bool selecting an exception inside the context block.
    """

    def mutate() -> None:
        """Change worker state while hand-driving a task."""

        os.chdir(tmp_path)
        os.environ["PIAB_TASK_LEAK"] = "1"
        if fails:
            raise RuntimeError("task failure")

    operator = PythonOperator(task_id="operator", python_callable=mutate)
    cwd, env = Path.cwd(), dict(os.environ)
    try:
        with (
            pytest.raises(RuntimeError, match="task failure") if fails else nullcontext(),
            task_context(operator) as handle,
        ):
            task: Any = handle.task
            task.execute(handle.context)
        assert Path.cwd() == cwd
        assert dict(os.environ) == env
    finally:
        os.chdir(cwd)
        os.environ.clear()
        os.environ.update(env)
