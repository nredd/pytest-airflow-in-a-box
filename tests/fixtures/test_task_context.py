"""Test the public DB-free ``task_context`` fixture.

References:
    https://airflow.apache.org/docs/task-sdk/stable/
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest
from airflow.sdk import DAG, BaseOperator, get_current_context

from pytest_airflow_in_a_box.fixtures.dag import _default_dag_id
from pytest_airflow_in_a_box.types import TaskContext


class TemplatedReturnOperator(BaseOperator):
    """Return a templated expression from ``execute``."""

    template_fields = ("expression",)

    def __init__(self, *, expression: Any, **kwargs: Any) -> None:
        """Store the templated expression.

        Parameters:
            expression: Any containing a possibly templated return value.
            kwargs: Any forwarded to ``BaseOperator``.
        """

        super().__init__(**kwargs)
        self.expression = expression

    def execute(self, context: Any) -> Any:
        """Return the stored expression.

        Parameters:
            context: Any containing the task execution context.

        Returns:
            Any containing the stored expression.
        """

        del context
        return self.expression


class DeferredRenderOperator(BaseOperator):
    """Render its own template fields from inside ``execute``."""

    template_fields = ("expression",)

    def __init__(self, *, expression: Any, **kwargs: Any) -> None:
        """Store the templated expression.

        Parameters:
            expression: Any containing a possibly templated value rendered
                mid-execution.
            kwargs: Any forwarded to ``BaseOperator``.
        """

        super().__init__(**kwargs)
        self.expression = expression

    def execute(self, context: Any) -> Any:
        """Render templates through ``context["ti"]`` and return the result.

        Parameters:
            context: Any containing the task execution context.

        Returns:
            Any containing the expression rendered mid-execution.
        """

        context["ti"].render_templates(context)
        return self.expression


class CurrentContextOperator(BaseOperator):
    """Read the active context through ``airflow.sdk.get_current_context``."""

    def execute(self, context: Any) -> str:
        """Return the current context's ``task_id``.

        Parameters:
            context: Any containing the task execution context.

        Returns:
            str containing the ``task_id`` read from the ambient context.
        """

        del context
        return get_current_context()["ti"].task_id


class PushXComOperator(BaseOperator):
    """Push one manual XCom through ``context["ti"]``."""

    def execute(self, context: Any) -> str:
        """Push one XCom value and return a marker.

        Parameters:
            context: Any containing the task execution context.

        Returns:
            str containing a benign marker value.
        """

        context["ti"].xcom_push(key="manual", value={"a": 1})
        return "done"


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


def test_task_context_hands_back_the_raw_execute_return_value(
    task_context: TaskContext,
) -> None:
    """Capture ``execute``'s raw return value, pre-rendered like a real run."""

    operator = _build(TemplatedReturnOperator, "task_context_basic", expression="{{ 21 * 2 }}")

    with task_context(operator) as handle:
        result = handle.task.execute(handle.context)

    assert result == "42"
    assert operator.expression == "{{ 21 * 2 }}"


def test_task_context_ti_is_the_context_task_instance(task_context: TaskContext) -> None:
    """Expose the same real ``RuntimeTaskInstance`` as ``context["ti"]``."""

    operator = _build(TemplatedReturnOperator, "task_context_identity", expression="x")

    with task_context(operator) as handle:
        assert handle.context["ti"] is handle.ti
        assert handle.context["task"] is handle.task


def test_task_context_supports_rendering_from_inside_execute(
    task_context: TaskContext,
) -> None:
    """Leave fields raw with ``render=False`` so ``execute`` can render them itself."""

    operator = _build(DeferredRenderOperator, "task_context_deferred", expression="{{ 21 * 2 }}")

    with task_context(operator, render=False) as handle:
        assert handle.task.expression == "{{ 21 * 2 }}"
        result = handle.task.execute(handle.context)

    assert result == "42"
    assert operator.expression == "{{ 21 * 2 }}"


def test_task_context_exposes_the_task_sdk_attribute_surface(
    task_context: TaskContext,
) -> None:
    """Expose the real Task SDK identity attributes, including ``log_url``."""

    operator = _build(TemplatedReturnOperator, "task_context_shape", expression="x")

    with task_context(operator, run_id="shape-run", try_number=2) as handle:
        ti = handle.ti

        assert ti.task_id == "probe"
        assert ti.dag_id == "task_context_shape"
        assert ti.run_id == "shape-run"
        assert ti.try_number == 2
        assert ti.map_index == -1
        assert isinstance(ti.log_url, str)
        assert "task_context_shape" in ti.log_url


def test_task_context_does_not_mutate_the_original_operator(
    task_context: TaskContext,
) -> None:
    """Prepare a fresh execution copy, leaving the caller's operator untouched."""

    operator = _build(TemplatedReturnOperator, "task_context_copy", expression="{{ 1 + 1 }}")

    with task_context(operator) as handle:
        assert handle.task is not operator
        assert handle.task.expression == "2"

    assert operator.expression == "{{ 1 + 1 }}"


def test_task_context_serves_seeded_variables_and_connections(
    task_context: TaskContext,
) -> None:
    """Serve seeded Variable and Connection reads through the fake supervisor."""

    operator = _build(
        TemplatedReturnOperator,
        "task_context_seeded",
        expression="{{ var.value.answer }}-{{ conn.db.host }}",
    )

    with task_context(
        operator,
        variables={"answer": "42"},
        connections={"db": {"conn_type": "postgres", "host": "example.com"}},
    ) as handle:
        result = handle.task.execute(handle.context)

    assert result == "42-example.com"


def test_task_context_records_xcom_pushes_and_supervisor_traffic(
    task_context: TaskContext,
) -> None:
    """Keep XCom and message snapshots readable after the block exits."""

    operator = _build(PushXComOperator, "task_context_traffic")

    with task_context(operator) as handle:
        handle.task.execute(handle.context)

    assert handle.xcoms["manual"] == {"a": 1}
    assert "SetXCom" in [type(msg).__name__ for msg in handle.sent]


def test_task_context_serves_get_current_context(task_context: TaskContext) -> None:
    """Resolve ``airflow.sdk.get_current_context`` inside a hand-driven ``execute``."""

    operator = _build(CurrentContextOperator, "task_context_current")

    with task_context(operator) as handle:
        result = handle.task.execute(handle.context)

    assert result == "probe"


def test_task_context_applies_params_and_context_overrides(
    task_context: TaskContext,
) -> None:
    """Deliver call ``params`` and merge caller-supplied context keys."""

    declared_params: Any = {"left": "declared"}
    with DAG(dag_id="task_context_params", schedule=None, params=declared_params) as dag:
        TemplatedReturnOperator(task_id="probe", expression="x")
    operator: Any = dag.get_task("probe")

    with task_context(
        operator, params={"left": "overridden"}, context_overrides={"custom": "extra"}
    ) as handle:
        assert handle.context["params"]["left"] == "overridden"
        assert handle.context["custom"] == "extra"


def test_task_context_opens_for_an_unbound_operator_with_derived_dag_id(
    task_context: TaskContext, request: pytest.FixtureRequest
) -> None:
    """Bind an unbound operator to a synthetic Dag with the derived per-test id."""

    operator = TemplatedReturnOperator(task_id="floating", expression="{{ dag.dag_id }}")

    with task_context(operator) as handle:
        result = handle.task.execute(handle.context)

    worker = os.environ.get("PYTEST_XDIST_WORKER", "master")
    expected = _default_dag_id(f"{request.node.nodeid}::task_context", worker, 1)
    assert result == expected
    assert operator.dag.dag_id == expected
    assert operator.dag.fileloc == str(Path(str(request.node.path)).resolve())


def test_task_context_explicit_dag_id_binds_unbound_operator(
    task_context: TaskContext,
) -> None:
    """Name the synthetic Dag with the call's explicit ``dag_id``."""

    operator = TemplatedReturnOperator(task_id="floating", expression="{{ dag.dag_id }}")

    with task_context(operator, dag_id="explicit_context_id") as handle:
        result = handle.task.execute(handle.context)

    assert result == "explicit_context_id"
    assert operator.dag.dag_id == "explicit_context_id"


def test_task_context_bound_operator_keeps_its_dag(task_context: TaskContext) -> None:
    """Leave a bound operator's Dag untouched, deriving nothing."""

    with DAG(dag_id="task_context_bound", schedule=None) as dag:
        TemplatedReturnOperator(task_id="probe", expression="{{ dag.dag_id }}")
    operator: Any = dag.get_task("probe")

    with task_context(operator) as handle:
        result = handle.task.execute(handle.context)

    assert result == "task_context_bound"
    assert operator.dag is dag
