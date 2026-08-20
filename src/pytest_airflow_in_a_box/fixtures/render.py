"""Provide DB-free, xdist-safe in-process template-field rendering.

References:
    https://airflow.apache.org/docs/task-sdk/stable/
    https://docs.pytest.org/en/stable/how-to/fixtures.html
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

import pytest

from pytest_airflow_in_a_box._compat.capabilities import v2_gate_message
from pytest_airflow_in_a_box._compat.in_process import (
    DEFAULT_RUN_ID,
    FakeSupervisorComms,
    bound_dag_or_none,
    render_task_in_process,
)
from pytest_airflow_in_a_box.fixtures.dag import _default_dag_id

if TYPE_CHECKING:
    from datetime import datetime

    from pytest_airflow_in_a_box.types import RenderTask


def _render_task(
    task: Any,
    *,
    dag_id: str | None = None,
    run_id: str = DEFAULT_RUN_ID,
    logical_date: datetime | None = None,
    params: dict[str, Any] | None = None,
    xcoms: dict[str, Any] | None = None,
    variables: dict[str, str] | None = None,
    connections: dict[str, dict[str, Any]] | None = None,
    map_index: int = -1,
    try_number: int = 1,
    context_overrides: dict[str, Any] | None = None,
) -> Any:
    """Render one operator's template fields in process with seeded fake supervisor state.

    Parameters:
        task: Any containing the Airflow operator or bound TaskFlow task.
        dag_id: str | None overriding the Dag identifier and naming the
            synthetic Dag auto-bound in place to an unbound task, or ``None``
            to read it from the task's bound Dag.
        run_id: str identifying the synthetic manual run.
        logical_date: datetime | None pinning the run's logical date.
        params: dict[str, Any] | None overriding declared Dag params.
        xcoms: dict[str, Any] | None seeding XCom values by key.
        variables: dict[str, str] | None seeding Variable values by key.
        connections: dict[str, dict[str, Any]] | None seeding connection
            fields by connection id.
        map_index: int selecting the mapped task index.
        try_number: int selecting the synthetic task attempt number.
        context_overrides: dict[str, Any] | None merged into the synthesized
            template context before rendering.

    Returns:
        Any containing the rendered operator: a `prepare_for_execution()` copy of
        `task` for a plain operator, or the concrete unmapped instance for a mapped
        one. Never the exact `task` object passed in -- rendering never mutates the
        original (binding an unbound task in place to a synthetic Dag is the one
        side effect), so always use the return value.
    """

    comms = FakeSupervisorComms(xcoms=xcoms, variables=variables, connections=connections)
    return render_task_in_process(
        task,
        dag_id=dag_id,
        run_id=run_id,
        logical_date=logical_date,
        params=params,
        comms=comms,
        map_index=map_index,
        try_number=try_number,
        context_overrides=context_overrides,
    )


@pytest.fixture
def render_task(request: pytest.FixtureRequest) -> RenderTask:
    """Return a DB-free in-process Task SDK template renderer.

    A task bound to no Dag is bound in place to a synthetic Dag whose
    identifier is either the call's explicit ``dag_id`` or a deterministic,
    bounded, xdist-safe identifier derived from the test's nodeid, the xdist
    worker, and a per-fixture invocation counter -- the same derivation
    `dag_maker` uses for its default Dag ids, salted so the two never collide.

    Parameters:
        request: pytest.FixtureRequest identifying the requesting test.

    Returns:
        RenderTask rendering one operator's template fields per call with
        isolated seeded state, without executing the operator.
    """

    message = v2_gate_message(
        "render_task",
        "it drives the Task SDK's `render_template_fields`, which 2.x predates. Use "
        "`dag_maker.run_ti` plus `RenderedTaskInstanceFields` for DB-backed rendering "
        "instead.",
    )
    if message is not None:
        pytest.fail(message, pytrace=False)

    nodeid = request.node.nodeid
    worker = os.environ.get("PYTEST_XDIST_WORKER", "master")
    invocations = 0

    def _fixture_render_task(
        task: Any,
        *,
        dag_id: str | None = None,
        run_id: str = DEFAULT_RUN_ID,
        logical_date: datetime | None = None,
        params: dict[str, Any] | None = None,
        xcoms: dict[str, Any] | None = None,
        variables: dict[str, str] | None = None,
        connections: dict[str, dict[str, Any]] | None = None,
        map_index: int = -1,
        try_number: int = 1,
        context_overrides: dict[str, Any] | None = None,
    ) -> Any:
        """Render one operator in process, deriving a Dag id when none exists.

        Parameters:
            task: Any containing the Airflow operator or bound TaskFlow task.
            dag_id: str | None overriding the Dag identifier and naming the
                synthetic Dag auto-bound in place to an unbound task, or
                ``None`` to read it from the task's bound Dag -- deriving a
                deterministic per-test identifier when there is none.
            run_id: str identifying the synthetic manual run.
            logical_date: datetime | None pinning the run's logical date.
            params: dict[str, Any] | None overriding declared Dag params.
            xcoms: dict[str, Any] | None seeding XCom values by key.
            variables: dict[str, str] | None seeding Variable values by key.
            connections: dict[str, dict[str, Any]] | None seeding connection
                fields by connection id.
            map_index: int selecting the mapped task index.
            try_number: int selecting the synthetic task attempt number.
            context_overrides: dict[str, Any] | None merged into the synthesized
                template context before rendering.

        Returns:
            Any containing the rendered operator copy, exactly as `_render_task`
            returns it.
        """

        nonlocal invocations
        resolved_dag_id = dag_id
        if resolved_dag_id is None and bound_dag_or_none(task) is None:
            invocations += 1
            resolved_dag_id = _default_dag_id(f"{nodeid}::render_task", worker, invocations)
        return _render_task(
            task,
            dag_id=resolved_dag_id,
            run_id=run_id,
            logical_date=logical_date,
            params=params,
            xcoms=xcoms,
            variables=variables,
            connections=connections,
            map_index=map_index,
            try_number=try_number,
            context_overrides=context_overrides,
        )

    return _fixture_render_task


__all__ = ("render_task",)
