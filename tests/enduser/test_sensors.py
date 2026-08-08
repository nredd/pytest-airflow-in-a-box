"""Exercise custom sensors in poke and reschedule modes."""

from __future__ import annotations

from typing import Any

import pytest
from airflow.models.taskreschedule import TaskReschedule
from airflow.sdk import DAG, BaseSensorOperator, PokeReturnValue
from airflow.utils.state import TaskInstanceState
from sqlalchemy import select

from pytest_airflow_in_a_box.types import DagMaker, RunTask

pytestmark = pytest.mark.compat


class ReturningSensor(BaseSensorOperator):
    """Succeed with an XCom payload."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.pokes = 0

    def poke(self, context: Any) -> PokeReturnValue:
        del context
        self.pokes += 1
        return PokeReturnValue(
            is_done=self.pokes == 3,
            xcom_value={"pokes": self.pokes},
        )


class WaitingSensor(BaseSensorOperator):
    """Request another sensor attempt."""

    def poke(self, context: Any) -> bool:
        del context
        return False


def test_poke_return_value_runs_without_metadata(run_task: RunTask) -> None:
    """Return a sensor XCom payload through the DB-free runner."""

    with DAG(dag_id="compat_sensor_free", schedule=None) as dag:
        ReturningSensor(task_id="wait", mode="poke", poke_interval=0)

    result = run_task(dag.get_task("wait"))

    assert result.state == TaskInstanceState.SUCCESS
    assert result.xcoms["return_value"] == {"pokes": 3}


@pytest.mark.db_test
def test_reschedule_sensor_persists_task_reschedule(dag_maker: DagMaker) -> None:
    """Persist the first reschedule request and its next run date."""

    with dag_maker(dag_id="compat_sensor_reschedule"):
        WaitingSensor(task_id="wait", mode="reschedule", poke_interval=60, timeout=300)

    ti = dag_maker.run_ti("wait")
    row = dag_maker.session.scalar(select(TaskReschedule).where(TaskReschedule.ti_id == ti.id))

    assert ti.state == TaskInstanceState.UP_FOR_RESCHEDULE
    assert row is not None
    assert row.reschedule_date > row.start_date
