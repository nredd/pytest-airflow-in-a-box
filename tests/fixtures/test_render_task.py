"""Test the public DB-free ``render_task`` fixture.

References:
    https://airflow.apache.org/docs/task-sdk/stable/
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import pytest
from airflow.sdk import DAG, BaseOperator

from pytest_airflow_in_a_box.fixtures.dag import _default_dag_id
from pytest_airflow_in_a_box.types import RenderTask

DERIVED_DAG_ID_PATTERN = re.compile(r"^[\w.-]+-[0-9a-f]{16}$")


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


def test_render_task_returns_a_rendered_copy_without_mutating_the_original(
    render_task: RenderTask,
) -> None:
    """Resolve the templated field on a fresh copy, leaving the original untouched."""

    operator = _build(TemplatedOperator, "render_task_basic", expression="{{ 21 * 2 }}")

    rendered_operator = render_task(operator)

    assert rendered_operator is not operator
    assert rendered_operator.expression == "42"
    assert operator.expression == "{{ 21 * 2 }}"


def test_render_task_does_not_contaminate_a_shared_operator_across_calls(
    render_task: RenderTask,
) -> None:
    """Render the same shared operator twice, each call independent of the other."""

    operator = _build(TemplatedOperator, "render_task_reuse", expression="{{ custom }}")

    first = render_task(operator, context_overrides={"custom": "first-value"})
    second = render_task(operator, context_overrides={"custom": "second-value"})

    assert first.expression == "first-value"
    assert second.expression == "second-value"


def test_render_task_does_not_execute_the_operator(render_task: RenderTask) -> None:
    """Render fields without ever calling ``execute``."""

    operator = _build(TemplatedOperator, "render_task_no_run", expression="x")

    rendered_operator = render_task(operator)

    assert rendered_operator.executed is False
    assert operator.executed is False


def test_render_task_renders_unbound_operator_with_derived_dag_id(
    render_task: RenderTask, request: pytest.FixtureRequest
) -> None:
    """Bind an unbound operator to a synthetic Dag with the derived per-test id."""

    operator = TemplatedOperator(task_id="floating", expression="{{ dag.dag_id }}")

    rendered_operator = render_task(operator)

    worker = os.environ.get("PYTEST_XDIST_WORKER", "master")
    expected = _default_dag_id(f"{request.node.nodeid}::render_task", worker, 1)
    assert rendered_operator.expression == expected
    assert operator.dag.dag_id == expected
    assert DERIVED_DAG_ID_PATTERN.fullmatch(expected) is not None
    assert operator.executed is False
    assert operator.dag.fileloc == str(Path(str(request.node.path)).resolve())


def test_render_task_explicit_dag_id_binds_unbound_operator(render_task: RenderTask) -> None:
    """Name the synthetic Dag with the call's explicit ``dag_id``."""

    operator = TemplatedOperator(task_id="floating", expression="{{ dag.dag_id }}")

    rendered_operator = render_task(operator, dag_id="explicit_render_id")

    assert rendered_operator.expression == "explicit_render_id"
    assert operator.dag.dag_id == "explicit_render_id"


def test_render_task_bound_operator_keeps_its_dag(render_task: RenderTask) -> None:
    """Leave a bound operator's Dag untouched, deriving nothing."""

    operator = _build(TemplatedOperator, "render_task_bound", expression="{{ dag.dag_id }}")
    original_dag = operator.dag

    rendered_operator = render_task(operator)

    assert rendered_operator.expression == "render_task_bound"
    assert operator.dag is original_dag


def test_render_task_serves_seeded_variables_and_connections(render_task: RenderTask) -> None:
    """Serve seeded Variable and Connection reads while rendering templates."""

    operator = _build(
        TemplatedOperator,
        "render_task_seeded",
        expression="{{ var.value.answer }}-{{ conn.db.host }}",
    )

    rendered_operator = render_task(
        operator,
        variables={"answer": "42"},
        connections={"db": {"conn_type": "postgres", "host": "example.com"}},
    )

    assert rendered_operator.expression == "42-example.com"


def test_render_task_applies_context_overrides(render_task: RenderTask) -> None:
    """Merge caller-supplied context keys into the synthesized context."""

    operator = _build(TemplatedOperator, "render_task_overrides", expression="{{ custom }}")

    rendered_operator = render_task(operator, context_overrides={"custom": "override-value"})

    assert rendered_operator.expression == "override-value"
