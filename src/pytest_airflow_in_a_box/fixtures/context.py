"""Provide a DB-free, xdist-safe Task SDK template context for hand-driven execution.

References:
    https://airflow.apache.org/docs/task-sdk/stable/
    https://docs.pytest.org/en/stable/how-to/fixtures.html
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from pytest_airflow_in_a_box._compat.capabilities import v2_gate_message
from pytest_airflow_in_a_box._compat.in_process import (
    DEFAULT_RUN_ID,
    FakeSupervisorComms,
    task_context_in_process,
)

if TYPE_CHECKING:
    from contextlib import AbstractContextManager
    from datetime import datetime

    from pytest_airflow_in_a_box.types import TaskContext, TaskContextHandle


def _task_context(
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
    """Open one Task SDK template context with seeded fake supervisor state.

    Parameters:
        task: Any containing the Airflow operator or bound TaskFlow task.
        dag_id: str | None overriding the Dag identifier, or ``None`` to read
            it from the task's bound Dag.
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
        is never mutated -- and whose ``ti`` is the real ``RuntimeTaskInstance``
        behind ``context["ti"]``. The fake supervisor stays installed only inside
        the ``with`` block; the handle's ``xcoms``/``sent`` snapshots remain
        readable after exit.
    """

    comms = FakeSupervisorComms(xcoms=xcoms, variables=variables, connections=connections)
    return task_context_in_process(
        task,
        dag_id=dag_id,
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
def task_context() -> TaskContext:
    """Return a DB-free in-process Task SDK template context factory.

    Returns:
        TaskContext opening one isolated, seeded template context per call for
        hand-driven ``execute()`` and ``post_execute()`` testing.
    """

    message = v2_gate_message(
        "task_context",
        "it builds a Task SDK `RuntimeTaskInstance`-backed template context, which "
        "2.x predates. Use `dag_maker.run_ti` plus `TaskInstance.get_template_context` "
        "for DB-backed context construction instead.",
    )
    if message is not None:
        pytest.fail(message, pytrace=False)
    return _task_context


__all__ = ("task_context",)
