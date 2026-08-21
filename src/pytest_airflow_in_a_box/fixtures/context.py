"""Provide a DB-free, xdist-safe Task SDK template context for hand-driven execution.

References:
    https://airflow.apache.org/docs/task-sdk/stable/
    https://docs.pytest.org/en/stable/how-to/fixtures.html
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from pytest_airflow_in_a_box._compat.capabilities import require_v3
from pytest_airflow_in_a_box._compat.in_process import (
    DEFAULT_RUN_ID,
    FakeSupervisorComms,
    bound_dag_or_none,
    task_context_in_process,
)
from pytest_airflow_in_a_box.fixtures.dag import _default_dag_id

if TYPE_CHECKING:
    from contextlib import AbstractContextManager
    from datetime import datetime

    from pytest_airflow_in_a_box.types import TaskContext, TaskContextHandle


def _task_context(
    task: Any,
    *,
    dag_id: str | None = None,
    fileloc: str | None = None,
    run_id: str = DEFAULT_RUN_ID,
    logical_date: datetime | None = None,
    params: dict[str, Any] | None = None,
    xcoms: dict[str, Any] | None = None,
    variables: dict[str, str] | None = None,
    connections: dict[str, dict[str, Any]] | None = None,
    map_index: int = -1,
    try_number: int = 1,
    context_overrides: dict[str, Any] | None = None,
    render: bool = True,
) -> AbstractContextManager[TaskContextHandle]:
    """Open one Task SDK template context with seeded fake supervisor state.

    Parameters:
        task: Any containing the Airflow operator or bound TaskFlow task.
        dag_id: str | None overriding the Dag identifier and naming the
            synthetic Dag auto-bound in place to an unbound task, or ``None``
            to read it from the task's bound Dag.
        fileloc: str | None naming the consumer test module, stamped on the
            synthetic Dag so ``template_ext`` files resolve next to the test,
            or ``None`` to keep Airflow's own default.
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
        render: bool pre-rendering template fields like a real run. Pass
            ``False`` for operators that call ``context["ti"].render_templates()``
            inside ``execute()`` themselves.

    Returns:
        contextlib.AbstractContextManager[TaskContextHandle] yielding a handle whose
        ``task`` is a `prepare_for_execution()` copy to drive -- the caller's `task`
        is never mutated by preparation (binding an unbound task in place to a
        synthetic Dag is the one side effect) -- and whose ``ti`` is the real
        ``RuntimeTaskInstance`` behind ``context["ti"]``. The fake supervisor stays
        installed only inside the ``with`` block; the handle's ``xcoms``/``sent``
        snapshots remain readable after exit.
    """

    comms = FakeSupervisorComms(xcoms=xcoms, variables=variables, connections=connections)
    return task_context_in_process(
        task,
        dag_id=dag_id,
        fileloc=fileloc,
        run_id=run_id,
        logical_date=logical_date,
        params=params,
        comms=comms,
        map_index=map_index,
        try_number=try_number,
        context_overrides=context_overrides,
        render=render,
    )


@pytest.fixture
def task_context(request: pytest.FixtureRequest) -> TaskContext:
    """Return a DB-free in-process Task SDK template context factory.

    A task bound to no Dag is bound in place to a synthetic Dag whose
    identifier is either the call's explicit ``dag_id`` or a deterministic,
    bounded, xdist-safe identifier derived from the test's nodeid, the xdist
    worker, and a per-fixture invocation counter -- the same derivation
    `dag_maker` uses for its default Dag ids, salted so the two never collide.

    Parameters:
        request: pytest.FixtureRequest identifying the requesting test.

    Returns:
        TaskContext opening one isolated, seeded template context per call for
        hand-driven ``execute()`` and ``post_execute()`` testing.
    """

    require_v3(
        "task_context",
        "it builds a Task SDK `RuntimeTaskInstance`-backed template context, which "
        "2.x predates. Use `dag_maker.run_ti` plus `TaskInstance.get_template_context` "
        "for DB-backed context construction instead.",
    )

    nodeid = request.node.nodeid
    fileloc = str(Path(str(request.node.path)).resolve())
    worker = os.environ.get("PYTEST_XDIST_WORKER", "master")
    invocations = 0

    def _fixture_task_context(
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
        render: bool = True,
    ) -> AbstractContextManager[TaskContextHandle]:
        """Open one template context, deriving a Dag id when none exists.

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
            render: bool pre-rendering template fields like a real run.

        Returns:
            contextlib.AbstractContextManager[TaskContextHandle] yielding the
            handle, exactly as `_task_context` returns it.
        """

        nonlocal invocations
        resolved_dag_id = dag_id
        if resolved_dag_id is None and bound_dag_or_none(task) is None:
            invocations += 1
            resolved_dag_id = _default_dag_id(f"{nodeid}::task_context", worker, invocations)
        return _task_context(
            task,
            dag_id=resolved_dag_id,
            fileloc=fileloc,
            run_id=run_id,
            logical_date=logical_date,
            params=params,
            xcoms=xcoms,
            variables=variables,
            connections=connections,
            map_index=map_index,
            try_number=try_number,
            context_overrides=context_overrides,
            render=render,
        )

    return _fixture_task_context


__all__ = ("task_context",)
