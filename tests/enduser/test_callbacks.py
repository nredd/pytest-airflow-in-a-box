"""Exercise task state callbacks through both public runners."""

from __future__ import annotations

from pathlib import Path
from typing import Any, ClassVar

import pytest
from airflow.listeners import hookimpl
from airflow.sdk import DAG, BaseOperator
from airflow.sdk.listener import get_listener_manager
from airflow.utils.state import TaskInstanceState

from pytest_airflow_in_a_box.types import DagMaker, RunTask

pytestmark = pytest.mark.compat


def _record(context: Any, outcome: str) -> None:
    Path(context["params"]["callback_path"]).write_text(outcome, encoding="utf-8")


def _success(context: Any) -> None:
    _record(context, "success")


def _failure(context: Any) -> None:
    _record(context, "failure")


def _retry(context: Any) -> None:
    _record(context, "retry")


class CallbackOperator(BaseOperator):
    """Succeed or fail while recording in-process callbacks."""

    calls: ClassVar[list[str]] = []

    def execute(self, context: Any) -> str:
        del context
        if self.params.get("fail"):
            raise ValueError("callback failure")
        return "done"


def _memory_success(context: Any) -> None:
    del context
    CallbackOperator.calls.append("success")


def _memory_failure(context: Any) -> None:
    del context
    CallbackOperator.calls.append("failure")


def _memory_retry(context: Any) -> None:
    del context
    CallbackOperator.calls.append("retry")


class RecordingListener:
    """Record public task-instance success hooks."""

    calls: ClassVar[list[str]] = []

    @hookimpl
    def on_task_instance_success(self, previous_state: Any, task_instance: Any) -> None:
        del previous_state, task_instance
        self.calls.append("success")


@pytest.mark.parametrize(
    ("outcome", "retries", "state"),
    [
        ("success", 0, TaskInstanceState.SUCCESS),
        ("failure", 0, TaskInstanceState.FAILED),
        ("retry", 1, TaskInstanceState.UP_FOR_RETRY),
    ],
)
def test_db_free_callbacks_follow_task_state(
    run_task: RunTask, outcome: str, retries: int, state: TaskInstanceState
) -> None:
    """Dispatch success, failure, and retry callbacks in process."""

    CallbackOperator.calls = []
    with DAG(dag_id=f"compat_callback_free_{outcome}", schedule=None) as dag:
        CallbackOperator(
            task_id="callback",
            params={"fail": outcome != "success"},
            retries=retries,
            on_success_callback=_memory_success,
            on_failure_callback=_memory_failure,
            on_retry_callback=_memory_retry,
        )

    result = run_task(dag.get_task("callback"), run_callbacks=True)

    assert result.state == state
    assert CallbackOperator.calls == [outcome]


@pytest.mark.db_test
@pytest.mark.parametrize(("outcome", "retries"), [("success", 0), ("failure", 0), ("retry", 1)])
def test_persisted_callbacks_follow_task_state(
    dag_maker: DagMaker, tmp_path: Path, outcome: str, retries: int
) -> None:
    """Dispatch callbacks from persisted task execution."""

    callback_path = tmp_path / "callback.txt"
    with dag_maker(
        dag_id=f"compat_callback_db_{outcome}",
        params={"callback_path": str(callback_path)},
    ):
        CallbackOperator(
            task_id="callback",
            params={"fail": outcome != "success"},
            retries=retries,
            on_success_callback=_success,
            on_failure_callback=_failure,
            on_retry_callback=_retry,
        )

    if outcome == "success":
        dag_maker.run_ti("callback")
    else:
        with pytest.raises(ValueError, match="callback failure"):
            dag_maker.run_ti("callback")

    assert callback_path.read_text(encoding="utf-8") == outcome


def test_db_free_runner_dispatches_listeners(run_task: RunTask) -> None:
    """Notify a registered listener when finalization is requested."""

    listener = RecordingListener()
    RecordingListener.calls = []
    manager = get_listener_manager()
    manager.add_listener(listener)
    try:
        with DAG(dag_id="compat_listener", schedule=None) as dag:
            CallbackOperator(task_id="callback")

        result = run_task(dag.get_task("callback"), run_callbacks=True)
    finally:
        manager.pm.unregister(listener)

    assert result.state == TaskInstanceState.SUCCESS
    assert RecordingListener.calls == ["success"]
