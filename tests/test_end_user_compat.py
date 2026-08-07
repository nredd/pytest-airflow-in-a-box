"""End-user compat suite: drive the public plugin surface as a Dag author would.

Every test here is newly authored against the bundled ``tests/dags`` corpus and
in-test operators, so a green run on one matrix cell proves the public surface
works in that runtime. These are the tests the version matrix exists for.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html
    https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/taskflow.html
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
import structlog
from airflow.sdk import BaseOperator, task
from airflow.utils.state import DagRunState, TaskInstanceState

from pytest_airflow_in_a_box.logging import StructlogCapture
from pytest_airflow_in_a_box.taskinstance import ordered_task_instances, run_task_instance
from pytest_airflow_in_a_box.types import DagMaker

try:
    from airflow.sdk.exceptions import AirflowSkipException
except ImportError:
    from airflow.exceptions import AirflowSkipException

pytestmark = pytest.mark.compat

CORPUS = Path(__file__).parent / "dags"
DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class RenderDateOperator(BaseOperator):
    """Return one templated date expression from ``execute``."""

    template_fields = ("date_expression",)

    def __init__(self, *, date_expression: str = "{{ ds }}", **kwargs: Any) -> None:
        """Store the templated expression.

        Parameters:
            date_expression: str containing a templated date expression.
            kwargs: Any forwarded to ``BaseOperator``.
        """

        super().__init__(**kwargs)
        self.date_expression = date_expression

    def execute(self, context: Any) -> str:
        """Return the rendered expression.

        Parameters:
            context: Any containing the task execution context.

        Returns:
            str containing the rendered expression.
        """

        del context
        return self.date_expression


def test_dagbag_imports_example_dags(pytester: pytest.Pytester) -> None:
    """Parse the bundled corpus with `full_dag_bag` and report the broken file."""

    pytester.makepyfile(
        """
        def test_imports(full_dag_bag):
            assert set(full_dag_bag.dags) == {"happy_path", "chained"}
            assert len(full_dag_bag.import_errors) == 1
            assert next(iter(full_dag_bag.import_errors)).endswith("broken.py")
        """
    )

    result = pytester.runpytest_subprocess("-q", "--dag-folder", str(CORPUS))

    result.assert_outcomes(passed=1)


def test_dagbag_structure(pytester: pytest.Pytester) -> None:
    """Assert parsed task topology through `full_dag_bag`."""

    pytester.makepyfile(
        """
        def test_structure(full_dag_bag):
            chained = full_dag_bag.dags["chained"]
            assert {task.task_id for task in chained.tasks} == {"produce", "consume"}
            assert chained.get_task("consume").upstream_task_ids == {"produce"}
            assert chained.get_task("produce").downstream_task_ids == {"consume"}
        """
    )

    result = pytester.runpytest_subprocess("-q", "--dag-folder", str(CORPUS))

    result.assert_outcomes(passed=1)


@pytest.mark.db_test
def test_custom_operator_executes(dag_maker: DagMaker) -> None:
    """Render a templated field and execute an in-test operator subclass."""

    with dag_maker(dag_id="compat_custom_operator"):
        RenderDateOperator(task_id="render")

    ti = dag_maker.run_ti("render")

    assert ti.state == TaskInstanceState.SUCCESS
    rendered = ti.xcom_pull(task_ids="render", session=dag_maker.session)
    assert isinstance(rendered, str)
    assert DATE_PATTERN.fullmatch(rendered)


def test_custom_operator_init_defaults() -> None:
    """Construct the operator subclass without any fixture or database."""

    operator = RenderDateOperator(task_id="standalone")

    assert operator.task_id == "standalone"
    assert operator.date_expression == "{{ ds }}"
    assert operator.template_fields == ("date_expression",)


@pytest.mark.db_test
def test_taskflow_task_runs_and_xcoms(dag_maker: DagMaker) -> None:
    """Run a benign TaskFlow task to SUCCESS and pull its XCom."""

    with dag_maker(dag_id="compat_taskflow"):

        @task
        def answer() -> int:
            """Return a deterministic XCom value.

            Returns:
                int containing the deterministic value.
            """

            return 42

        answer()

    ti = dag_maker.run_ti("answer")

    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids="answer", session=dag_maker.session) == 42


@pytest.mark.db_test
def test_inline_dag_runs_to_success(dag_maker: DagMaker) -> None:
    """Run every task instance of a multi-task Dag and finish the DagRun."""

    with dag_maker(dag_id="compat_inline") as dag:

        @task
        def produce() -> int:
            """Return the produced value.

            Returns:
                int containing the produced value.
            """

            return 21

        @task
        def consume(value: int) -> int:
            """Double the produced value.

            Parameters:
                value: int received from the producer.

            Returns:
                int containing the doubled value.
            """

            return value * 2

        produced: Any = produce()
        consume(produced)

    dag_run = dag_maker.create_dagrun()
    instances: list[Any] = list(ordered_task_instances(dag_run, dag, session=dag_maker.session))
    for ti in instances:
        run_task_instance(ti, dag.get_task(ti.task_id), session=dag_maker.session)
    dag_run.update_state(session=dag_maker.session, execute_callbacks=False)

    assert dag_run.state == DagRunState.SUCCESS
    consume_ti = dag_maker.create_ti("consume", dag_run)
    assert consume_ti.xcom_pull(task_ids="consume", session=dag_maker.session) == 42


@pytest.mark.db_test
def test_skipped_task_state(dag_maker: DagMaker) -> None:
    """Translate the skip exception into the persisted SKIPPED state."""

    with dag_maker(dag_id="compat_skip"):

        @task
        def not_applicable() -> None:
            """Skip the representative task."""

            raise AirflowSkipException("not applicable in this run")

        not_applicable()

    ti = dag_maker.run_ti("not_applicable")

    assert ti.state == TaskInstanceState.SKIPPED


@pytest.mark.db_test
def test_structlog_capture_sees_task_events(
    cap_structlog: StructlogCapture,
    dag_maker: DagMaker,
) -> None:
    """Capture a structlog event emitted from executed task code."""

    with dag_maker(dag_id="compat_structlog"):

        @task
        def speak() -> None:
            """Emit one structlog event from task code."""

            structlog.get_logger("compat_task").warning("compat_event", answer=42)

        speak()

    dag_maker.run_ti("speak")

    assert "compat_event" in cap_structlog
    assert {"answer": 42} in cap_structlog
