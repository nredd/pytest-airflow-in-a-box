"""Exercise a custom trigger and deferrable operator."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest
from airflow.sdk import BaseOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.state import TaskInstanceState

from pytest_airflow_in_a_box.types import DagMaker

pytestmark = [pytest.mark.compat, pytest.mark.db_test]


class ImmediateTrigger(BaseTrigger):
    """Emit one deterministic event."""

    def __init__(self, value: int) -> None:
        super().__init__()
        self.value = value

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return f"{type(self).__module__}.{type(self).__qualname__}", {"value": self.value}

    async def run(self) -> AsyncIterator[TriggerEvent]:
        yield TriggerEvent({"value": self.value})


class DeferredOperator(BaseOperator):
    """Defer once, then return the trigger payload."""

    def execute(self, context: Any) -> None:
        del context
        self.defer(trigger=ImmediateTrigger(42), method_name="execute_complete")

    def execute_complete(self, context: Any, event: dict[str, Any]) -> int:
        del context
        return event["value"]


def test_custom_trigger_resumes_a_deferred_operator(dag_maker: DagMaker) -> None:
    """Persist, run, submit, and resume one trigger event."""

    with dag_maker(dag_id="compat_deferred"):
        DeferredOperator(task_id="deferred")

    ti = dag_maker.run_ti("deferred", run_triggerer=True)

    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.trigger_id is None
    assert ti.xcom_pull(task_ids="deferred", session=dag_maker.session) == 42
