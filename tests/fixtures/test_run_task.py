"""Test the public DB-free ``run_task`` fixture.

References:
    https://airflow.apache.org/docs/task-sdk/stable/
"""

from __future__ import annotations

from typing import Any

from airflow.sdk import DAG, BaseHook, BaseOperator, Variable
from airflow.utils.state import TaskInstanceState

from pytest_airflow_in_a_box.types import RunTask


class VariableOperator(BaseOperator):
    """Return one Airflow Variable value."""

    def execute(self, context: Any) -> str:
        """Return the requested Variable value.

        Parameters:
            context: Any containing the task execution context.

        Returns:
            str containing the Variable value.
        """

        del context
        return Variable.get("answer")


class ConnectionOperator(BaseOperator):
    """Return one Airflow Connection host."""

    def __init__(self, *, conn_id: str, **kwargs: Any) -> None:
        """Store the requested connection id.

        Parameters:
            conn_id: str identifying the connection to read.
            kwargs: Any forwarded to ``BaseOperator``.
        """

        super().__init__(**kwargs)
        self.conn_id = conn_id

    def execute(self, context: Any) -> str | None:
        """Return the requested Connection host.

        Parameters:
            context: Any containing the task execution context.

        Returns:
            str | None containing the Connection host.
        """

        del context
        return BaseHook.get_connection(self.conn_id).host


class SeededXComOperator(BaseOperator):
    """Return one seeded XCom value."""

    def execute(self, context: Any) -> Any:
        """Pull the seeded XCom value.

        Parameters:
            context: Any containing the task execution context.

        Returns:
            Any containing the pulled XCom value.
        """

        ti = context["ti"]
        return ti.xcom_pull(task_ids=ti.task_id, key="seeded")


def _build(operator_type: type[BaseOperator], dag_id: str, **kwargs: Any) -> Any:
    """Create one operator bound to a fresh SDK Dag.

    Parameters:
        operator_type: type[BaseOperator] to instantiate.
        dag_id: str identifying the fresh Dag.
        kwargs: Any forwarded to the operator constructor.

    Returns:
        Any containing the operator bound to the new Dag.
    """

    with DAG(dag_id=dag_id, schedule=None) as dag:
        operator_type(task_id="probe", **kwargs)
    return dag.get_task("probe")


def test_run_task_serves_seeded_variables(run_task: RunTask) -> None:
    """Serve Variable reads from the per-call seed."""

    operator = _build(VariableOperator, "run_task_variables")

    result = run_task(operator, variables={"answer": "42"})

    assert result.state == TaskInstanceState.SUCCESS
    assert result.xcoms["return_value"] == "42"


def test_run_task_serves_seeded_connections(run_task: RunTask) -> None:
    """Serve Connection reads from the per-call seed."""

    operator = _build(ConnectionOperator, "run_task_connections", conn_id="db")

    result = run_task(
        operator,
        connections={"db": {"conn_type": "postgres", "host": "example.com"}},
    )

    assert result.state == TaskInstanceState.SUCCESS
    assert result.xcoms["return_value"] == "example.com"


def test_run_task_fails_naturally_for_unseeded_connection(run_task: RunTask) -> None:
    """Fail exactly like a live deployment when a connection is not seeded."""

    operator = _build(ConnectionOperator, "run_task_unseeded", conn_id="missing")

    result = run_task(operator)

    assert result.state == TaskInstanceState.FAILED
    assert result.error is not None
    assert "missing" in str(result.error)


def test_run_task_seeds_are_isolated_per_call(run_task: RunTask) -> None:
    """Give every call its own seeded XCom state."""

    operator = _build(SeededXComOperator, "run_task_isolated")

    first = run_task(operator, xcoms={"seeded": "first"})
    second = run_task(operator, xcoms={"seeded": "second"})

    assert first.xcoms["return_value"] == "first"
    assert second.xcoms["return_value"] == "second"


def test_run_task_records_supervisor_traffic_in_order(run_task: RunTask) -> None:
    """Record the Variable read before the XCom push and success message."""

    operator = _build(VariableOperator, "run_task_traffic")

    result = run_task(operator, variables={"answer": "42"})

    names = [type(msg).__name__ for msg in result.sent]
    assert names.index("GetVariable") < names.index("SetXCom")
    assert names[-1] == "SucceedTask"
