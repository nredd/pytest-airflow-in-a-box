"""Exercise a custom trigger and deferrable operator.

`run_trigger` and `run_task_instance` are family-branched in `_compat.taskrun`, and
`BaseTrigger`/`TriggerEvent` live at the same `airflow.triggers.base` path on both
Airflow families, so the whole module runs on both without a `requires_airflow3`
marker. Only `BaseOperator` moved (from `airflow.models` to `airflow.sdk`), so it
resolves dynamically ON PURPOSE: a static `from airflow.sdk import BaseOperator`
would fail to import on the 2.x family.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from importlib import import_module
from typing import Any

import pytest
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.state import TaskInstanceState

from pytest_airflow_in_a_box.taskinstance import TriggerExecutionError, run_trigger
from pytest_airflow_in_a_box.types import DagMaker

pytestmark = pytest.mark.compat


# Shared with the six sibling contract modules; see `tests/enduser/_authoring.py`.
_resolve = import_module("_authoring")._resolve


BaseOperator = _resolve("airflow.sdk", "airflow.models").BaseOperator


class ImmediateTrigger(BaseTrigger):
    """Emit one deterministic event."""

    def __init__(self, value: int) -> None:
        super().__init__()
        self.value = value

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return f"{type(self).__module__}.{type(self).__qualname__}", {"value": self.value}

    async def run(self) -> AsyncIterator[TriggerEvent]:
        yield TriggerEvent({"value": self.value})


class SilentTrigger(BaseTrigger):
    """Never emit an event."""

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return f"{type(self).__module__}.{type(self).__qualname__}", {}

    async def run(self) -> AsyncIterator[TriggerEvent]:
        await asyncio.sleep(30)
        yield TriggerEvent({"value": "late"})


class DeferredOperator(BaseOperator):
    """Defer once, then return the trigger payload."""

    def execute(self, context: Any) -> None:
        del context
        self.defer(trigger=ImmediateTrigger(42), method_name="execute_complete")

    def execute_complete(self, context: Any, event: dict[str, Any]) -> int:
        del context
        return event["value"]


def test_run_trigger_drives_a_trigger_without_a_database() -> None:
    """Return a custom trigger's first event with no DagRun, task, or metadata database."""

    event = run_trigger(ImmediateTrigger(42))

    assert isinstance(event, TriggerEvent)
    assert event.payload == {"value": 42}


def test_run_trigger_bounds_a_trigger_that_never_fires() -> None:
    """Fail fast instead of hanging when a trigger emits nothing."""

    with pytest.raises(TriggerExecutionError, match="yielded no event"):
        run_trigger(SilentTrigger(), timeout=0.05)


@pytest.mark.db_test
def test_custom_trigger_resumes_a_deferred_operator(dag_maker: DagMaker) -> None:
    """Persist, run, submit, and resume one trigger event."""

    with dag_maker(dag_id="compat_deferred"):
        DeferredOperator(task_id="deferred")

    ti = dag_maker.run_ti("deferred", run_triggerer=True)

    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.trigger_id is None
    assert ti.xcom_pull(task_ids="deferred", session=dag_maker.session) == 42


@pytest.mark.db_test
def test_defer_fire_and_resume_is_one_call(dag_maker: DagMaker) -> None:
    """Compose deferral, trigger firing, and resumption under an explicit timeout."""

    with dag_maker(dag_id="compat_deferred_timeout"):
        DeferredOperator(task_id="deferred")

    ti = dag_maker.run_ti("deferred", run_triggerer=True, trigger_timeout=5.0)

    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids="deferred", session=dag_maker.session) == 42
