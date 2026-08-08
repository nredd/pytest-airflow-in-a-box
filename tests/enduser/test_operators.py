"""Exercise representative custom operator unit tests."""

from __future__ import annotations

from pathlib import Path
from typing import Any, ClassVar

import pytest
from airflow.sdk import DAG, BaseOperator
from airflow.sdk.exceptions import AirflowException
from airflow.utils.state import TaskInstanceState

from pytest_airflow_in_a_box.types import DagMaker, RunTask

pytestmark = pytest.mark.compat

try:
    from airflow.sdk.exceptions import AirflowSkipException
except ImportError:
    from airflow.exceptions import AirflowSkipException


class ContextOperator(BaseOperator):
    """Return high-churn runtime context values."""

    def execute(self, context: Any) -> dict[str, Any]:
        return {
            "ds": context["ds"],
            "data_interval_start": context["data_interval_start"].isoformat(),
            "try_number": context["ti"].try_number,
        }


class TemplateOperator(BaseOperator):
    """Render nested and file-backed template fields."""

    template_fields = ("payload", "query")
    template_ext = (".sql",)

    def __init__(self, *, payload: Any, query: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.payload = payload
        self.query = query

    def execute(self, context: Any) -> dict[str, Any]:
        del context
        return {"payload": self.payload, "query": self.query}


class HookOrderOperator(BaseOperator):
    """Record lifecycle hook ordering."""

    calls: ClassVar[list[str]] = []

    def pre_execute(self, context: Any) -> None:
        del context
        self.calls.append("pre")

    def execute(self, context: Any) -> str:
        del context
        self.calls.append("execute")
        return "value"

    def post_execute(self, context: Any, result: Any = None) -> None:
        del context, result
        self.calls.append("post")


class RaiseOperator(BaseOperator):
    """Raise an Airflow or plain exception."""

    def __init__(self, *, airflow_error: bool, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.airflow_error = airflow_error

    def execute(self, context: Any) -> None:
        del context
        if self.airflow_error:
            raise AirflowException("airflow failure")
        raise ValueError("plain failure")


class SkipOperator(BaseOperator):
    """Skip itself through Airflow's public task exception."""

    def execute(self, context: Any) -> None:
        del context
        raise AirflowSkipException("not applicable")


@pytest.mark.db_test
def test_custom_operator_reads_runtime_context(dag_maker: DagMaker) -> None:
    """Read macros, data interval, and attempt number through the DB runner."""

    with dag_maker(dag_id="compat_operator_context"):
        ContextOperator(task_id="inspect")

    ti = dag_maker.run_ti("inspect")
    value = ti.xcom_pull(task_ids="inspect", session=dag_maker.session)

    assert ti.state == TaskInstanceState.SUCCESS
    assert value["ds"] in value["data_interval_start"]
    assert value["try_number"] >= 0


def test_db_free_context_uses_requested_attempt(run_task: RunTask) -> None:
    """Populate logical interval and attempt context without metadata."""

    with DAG(dag_id="compat_operator_context_free", schedule=None) as dag:
        ContextOperator(task_id="inspect", retries=2)

    result = run_task(dag.get_task("inspect"), try_number=2)

    assert result.xcoms["return_value"]["try_number"] == 2
    assert (
        result.xcoms["return_value"]["ds"] in result.xcoms["return_value"]["data_interval_start"]
    )


def test_nested_and_file_templates_render(run_task: RunTask, tmp_path: Path) -> None:
    """Render dict/list values and a template-extension file without metadata."""

    (tmp_path / "query.sql").write_text("SELECT {{ params.value }}", encoding="utf-8")
    with DAG(
        dag_id="compat_operator_templates", schedule=None, template_searchpath=[str(tmp_path)]
    ) as dag:
        TemplateOperator(
            task_id="render",
            payload={"items": ["{{ params.value }}"]},
            query="query.sql",
        )

    result = run_task(dag.get_task("render"), params={"value": "42"})

    assert result.xcoms["return_value"] == {
        "payload": {"items": ["42"]},
        "query": "SELECT 42",
    }


def test_operator_lifecycle_hooks_run_in_order(run_task: RunTask) -> None:
    """Invoke pre-execute, execute, and post-execute in author order."""

    HookOrderOperator.calls = []
    with DAG(dag_id="compat_operator_hooks", schedule=None) as dag:
        HookOrderOperator(task_id="record")

    result = run_task(dag.get_task("record"))

    assert result.state == TaskInstanceState.SUCCESS
    assert HookOrderOperator.calls == ["pre", "execute", "post"]


def test_custom_operator_constructs_without_metadata() -> None:
    """Construct a custom operator with no Dag or metadata fixtures."""

    operator = ContextOperator(task_id="standalone")

    assert operator.task_id == "standalone"
    assert operator.retries == 0


@pytest.mark.parametrize(
    ("airflow_error", "error_type"), [(True, AirflowException), (False, ValueError)]
)
def test_operator_exception_types_reach_failed_state(
    run_task: RunTask,
    airflow_error: bool,
    error_type: type[Exception],
) -> None:
    """Preserve framework and user exceptions while producing state parity."""

    with DAG(dag_id=f"compat_exception_{airflow_error}", schedule=None) as dag:
        RaiseOperator(task_id="fail", airflow_error=airflow_error)

    result = run_task(dag.get_task("fail"))

    assert result.state == TaskInstanceState.FAILED
    assert isinstance(result.error, error_type)


@pytest.mark.db_test
@pytest.mark.parametrize(
    ("airflow_error", "error_type"), [(True, AirflowException), (False, ValueError)]
)
def test_operator_exception_types_match_persisted_state(
    dag_maker: DagMaker,
    airflow_error: bool,
    error_type: type[Exception],
) -> None:
    """Preserve each exception and FAILED state through persisted execution."""

    with dag_maker(dag_id=f"compat_db_exception_{airflow_error}"):
        RaiseOperator(task_id="fail", airflow_error=airflow_error)
    dag_run = dag_maker.create_dagrun()

    with pytest.raises(error_type):
        dag_maker.run_ti("fail", dag_run)

    assert dag_maker.create_ti("fail", dag_run).state == TaskInstanceState.FAILED


@pytest.mark.db_test
def test_skip_exception_reaches_persisted_skipped_state(dag_maker: DagMaker) -> None:
    """Translate a task-authored skip into persisted SKIPPED state."""

    with dag_maker(dag_id="compat_skip_exception"):
        SkipOperator(task_id="skip")

    ti = dag_maker.run_ti("skip")

    assert ti.state == TaskInstanceState.SKIPPED
