"""Test the public DB-free ``run_task`` fixture.

References:
    https://airflow.apache.org/docs/task-sdk/stable/
"""

from __future__ import annotations

import os
import re
from typing import Any, ClassVar

import pytest
from airflow.sdk import DAG, BaseHook, BaseOperator, Variable
from airflow.utils.state import TaskInstanceState

from pytest_airflow_in_a_box.fixtures.dag import _default_dag_id
from pytest_airflow_in_a_box.types import RunTask

DERIVED_DAG_ID_PATTERN = re.compile(r"^[\w.-]+-[0-9a-f]{16}$")


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


class EchoDagIdOperator(BaseOperator):
    """Return the bound Dag's identifier from the template context."""

    template_fields = ("expression",)

    def __init__(self, **kwargs: Any) -> None:
        """Template the Dag identifier into ``expression``.

        Parameters:
            kwargs: Any forwarded to ``BaseOperator``.
        """

        super().__init__(**kwargs)
        self.expression = "{{ dag.dag_id }}"

    def execute(self, context: Any) -> Any:
        """Return the rendered Dag identifier.

        Parameters:
            context: Any containing the task execution context.

        Returns:
            Any containing the rendered Dag identifier.
        """

        del context
        return self.expression


def test_run_task_runs_unbound_operator_with_derived_dag_id(
    run_task: RunTask, request: pytest.FixtureRequest
) -> None:
    """Bind an unbound operator to a synthetic Dag with the derived per-test id."""

    operator = EchoDagIdOperator(task_id="floating")

    result = run_task(operator)

    worker = os.environ.get("PYTEST_XDIST_WORKER", "master")
    expected = _default_dag_id(f"{request.node.nodeid}::run_task", worker, 1)
    assert result.state == TaskInstanceState.SUCCESS
    assert operator.dag.dag_id == expected
    assert DERIVED_DAG_ID_PATTERN.fullmatch(expected) is not None
    assert result.xcoms["return_value"] == expected


def test_run_task_derives_distinct_ids_per_unbound_operator(run_task: RunTask) -> None:
    """Give each unbound operator its own derived Dag identifier."""

    first = EchoDagIdOperator(task_id="first_floating")
    second = EchoDagIdOperator(task_id="second_floating")

    run_task(first)
    run_task(second)

    assert first.dag.dag_id != second.dag.dag_id


def test_run_task_reuses_binding_on_repeated_calls(run_task: RunTask) -> None:
    """Keep the first synthetic binding when the same operator runs again."""

    operator = EchoDagIdOperator(task_id="floating")

    first = run_task(operator)
    bound_dag = operator.dag
    second = run_task(operator)

    assert operator.dag is bound_dag
    assert first.xcoms["return_value"] == second.xcoms["return_value"]


def test_run_task_explicit_dag_id_binds_unbound_operator(run_task: RunTask) -> None:
    """Name the synthetic Dag with the call's explicit ``dag_id``."""

    operator = EchoDagIdOperator(task_id="floating")

    result = run_task(operator, dag_id="explicit_id")

    assert result.state == TaskInstanceState.SUCCESS
    assert operator.dag.dag_id == "explicit_id"
    assert result.xcoms["return_value"] == "explicit_id"


def test_run_task_derived_id_differs_from_dag_maker_derivation(
    run_task: RunTask, request: pytest.FixtureRequest
) -> None:
    """Never collide with `dag_maker`'s default id for the same test and worker."""

    operator = EchoDagIdOperator(task_id="floating")

    run_task(operator)

    worker = os.environ.get("PYTEST_XDIST_WORKER", "master")
    assert operator.dag.dag_id != _default_dag_id(request.node.nodeid, worker, 1)


class CallbackOperator(BaseOperator):
    """Record task callback invocations in process memory."""

    invocations: ClassVar[list[str]] = []

    def __init__(self, *, fail: bool = False, **kwargs: Any) -> None:
        """Configure the failure mode and register recording callbacks.

        Parameters:
            fail: bool raising from ``execute`` when true.
            kwargs: Any forwarded to ``BaseOperator``.
        """

        super().__init__(
            on_success_callback=[lambda _context: type(self).invocations.append("success")],
            on_failure_callback=[lambda _context: type(self).invocations.append("failure")],
            **kwargs,
        )
        self.fail = fail

    def execute(self, context: Any) -> str:
        """Return or raise per configuration.

        Parameters:
            context: Any containing the task execution context.

        Returns:
            str containing a benign value.

        Raises:
            ValueError: The operator is configured to fail.
        """

        del context
        if self.fail:
            raise ValueError("configured failure")
        return "done"


def test_run_task_skips_callbacks_by_default(run_task: RunTask) -> None:
    """Leave task callbacks undispatched unless requested."""

    CallbackOperator.invocations = []
    operator = _build(CallbackOperator, "run_task_no_callbacks")

    result = run_task(operator)

    assert result.state == TaskInstanceState.SUCCESS
    assert CallbackOperator.invocations == []


def test_run_task_dispatches_success_callbacks(run_task: RunTask) -> None:
    """Invoke success callbacks through the SDK finalize path."""

    CallbackOperator.invocations = []
    operator = _build(CallbackOperator, "run_task_success_callbacks")

    result = run_task(operator, run_callbacks=True)

    assert result.state == TaskInstanceState.SUCCESS
    assert CallbackOperator.invocations == ["success"]


def test_run_task_dispatches_failure_callbacks(run_task: RunTask) -> None:
    """Invoke failure callbacks through the SDK finalize path."""

    CallbackOperator.invocations = []
    operator = _build(CallbackOperator, "run_task_failure_callbacks", fail=True)

    result = run_task(operator, run_callbacks=True)

    assert result.state == TaskInstanceState.FAILED
    assert CallbackOperator.invocations == ["failure"]
