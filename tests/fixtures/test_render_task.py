"""Test the public DB-free ``render_task`` fixture.

References:
    https://airflow.apache.org/docs/task-sdk/stable/
"""

from __future__ import annotations

from typing import Any

from airflow.sdk import DAG, BaseOperator

from pytest_airflow_in_a_box.types import RenderTask


class TemplatedOperator(BaseOperator):
    """Carry one templated field and record whether ``execute`` ran."""

    template_fields = ("expression",)

    def __init__(self, *, expression: Any, **kwargs: Any) -> None:
        """Store the templated expression and reset the execution flag.

        Parameters:
            expression: Any containing a possibly templated value.
            kwargs: Any forwarded to ``BaseOperator``.
        """

        super().__init__(**kwargs)
        self.expression = expression
        self.executed = False

    def execute(self, context: Any) -> Any:
        """Record that execution happened and return the expression.

        Parameters:
            context: Any containing the task execution context.

        Returns:
            Any containing the stored expression.
        """

        del context
        self.executed = True
        return self.expression


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


def test_render_task_returns_the_same_operator_mutated(render_task: RenderTask) -> None:
    """Resolve the templated field on the caller's own operator object."""

    operator = _build(TemplatedOperator, "render_task_basic", expression="{{ 21 * 2 }}")

    rendered_operator = render_task(operator)

    assert rendered_operator is operator
    assert operator.expression == "42"


def test_render_task_does_not_execute_the_operator(render_task: RenderTask) -> None:
    """Render fields without ever calling ``execute``."""

    operator = _build(TemplatedOperator, "render_task_no_run", expression="x")

    render_task(operator)

    assert operator.executed is False


def test_render_task_serves_seeded_variables_and_connections(render_task: RenderTask) -> None:
    """Serve seeded Variable and Connection reads while rendering templates."""

    operator = _build(
        TemplatedOperator,
        "render_task_seeded",
        expression="{{ var.value.answer }}-{{ conn.db.host }}",
    )

    render_task(
        operator,
        variables={"answer": "42"},
        connections={"db": {"conn_type": "postgres", "host": "example.com"}},
    )

    assert operator.expression == "42-example.com"


def test_render_task_applies_context_overrides(render_task: RenderTask) -> None:
    """Merge caller-supplied context keys into the synthesized context."""

    operator = _build(TemplatedOperator, "render_task_overrides", expression="{{ custom }}")

    render_task(operator, context_overrides={"custom": "override-value"})

    assert operator.expression == "override-value"
