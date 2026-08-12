"""Exercise custom sensors in poke and reschedule modes.

`DAG`, `BaseSensorOperator`, and `PokeReturnValue` resolve dynamically ON PURPOSE:
`DAG` moved from `airflow.models` (2.x) to `airflow.sdk` (3.x), and while
`BaseSensorOperator`/`PokeReturnValue` are reachable from `airflow.sdk` on 3.x,
their canonical 2.x home is `airflow.sensors.base`. Only the DB-free poke test
needs the Task SDK's `run_task` runner, so it alone carries `requires_airflow3`.

The reschedule test pins `core.executor` to `SequentialExecutor` for its run: 2.x's
`unit_test_mode` overlays Airflow's own `unit_tests.cfg`, which hard-codes
`executor = LocalExecutor`, and the `ready_to_reschedule` dependency rejects that
combination with SQLite. `SequentialExecutor` is single-threaded and Airflow
allows it with any backend, so pinning it changes no test semantics. 2.x's
`ExecutorLoader` also caches the resolved executor in a module-level list on first
use, so the pin clears that cache too -- otherwise an earlier test's default-executor
lookup elsewhere in the same process would freeze the override out, order-dependently.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

import pytest
from airflow.models.taskreschedule import TaskReschedule
from airflow.utils.state import TaskInstanceState
from sqlalchemy import select

from pytest_airflow_in_a_box._compat.capabilities import AirflowFamily, installed_family
from pytest_airflow_in_a_box.config import airflow_config
from pytest_airflow_in_a_box.types import DagMaker, RunTask

pytestmark = pytest.mark.compat


def _resolve(*candidates: str) -> Any:
    """Import the first available module; the module collects on both Airflow families.

    Parameters:
        candidates: str module paths ordered newest family first.

    Returns:
        Any containing the first importable module.
    """

    for name in candidates[:-1]:
        try:
            return import_module(name)
        except ImportError:
            continue
    return import_module(candidates[-1])


DAG = _resolve("airflow.sdk", "airflow.models").DAG
_sensors = _resolve("airflow.sdk", "airflow.sensors.base")
BaseSensorOperator = _sensors.BaseSensorOperator
PokeReturnValue = _sensors.PokeReturnValue


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


@pytest.mark.requires_airflow3
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

    is_v2 = installed_family() is AirflowFamily.V2
    overrides = {("core", "executor"): "SequentialExecutor"} if is_v2 else {}
    with airflow_config(overrides):
        if is_v2:
            # Force a fresh lookup under the pinned override; an earlier test's
            # default-executor lookup may have already frozen the stale cache.
            executor_loader = import_module("airflow.executors.executor_loader")
            executor_loader._executor_names = []
        try:
            ti = dag_maker.run_ti("wait")
        finally:
            if is_v2:
                executor_loader._executor_names = []
    # 2.x keys `TaskReschedule` by the composite task instance identity; 3.x collapsed
    # that to a single `ti_id` foreign key.
    condition = (
        TaskReschedule.ti_id == ti.id
        if hasattr(TaskReschedule, "ti_id")
        else (
            (TaskReschedule.dag_id == ti.dag_id)
            & (TaskReschedule.task_id == ti.task_id)
            & (TaskReschedule.run_id == ti.run_id)
            & (TaskReschedule.map_index == ti.map_index)
        )
    )
    row = dag_maker.session.scalar(select(TaskReschedule).where(condition))

    assert ti.state == TaskInstanceState.UP_FOR_RESCHEDULE
    assert row is not None
    assert row.reschedule_date > row.start_date
