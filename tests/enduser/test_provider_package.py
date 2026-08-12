"""Exercise composition with a provider-shaped user package.

`DAG` resolves dynamically ON PURPOSE: it lives at `airflow.sdk` on 3.x and
`airflow.models` on 2.x, and the corpus round-trip test collects and runs on both
families. Only the in-process execution test needs the Task SDK's DB-free
`run_task` runner, so it alone carries `requires_airflow3`.
"""

from __future__ import annotations

from importlib import import_module
from pathlib import Path
from typing import Any

import pytest
from airflow.utils.state import TaskInstanceState

from pytest_airflow_in_a_box.types import RunTask

pytestmark = pytest.mark.compat

CORPUS = Path(__file__).parents[1] / "dags"


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


def test_provider_package_composes_in_a_corpus_dag(pytester: pytest.Pytester) -> None:
    """Import adjacent hook/operator/sensor modules through a Dag file."""

    pytester.makepyfile(
        """
        def test_provider(full_dag_bag):
            dag = full_dag_bag.dags["provider_composition"]
            assert {task.task_id for task in dag.tasks} == {"produce", "confirm"}
            assert dag.get_task("confirm").upstream_task_ids == {"produce"}
        """
    )

    result = pytester.runpytest_subprocess("-q", f"--dag-folder={CORPUS}")

    result.assert_outcomes(passed=1)


@pytest.mark.requires_airflow3
def test_provider_package_operator_and_sensor_execute(
    run_task: RunTask, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Execute the package's hook-backed operator and custom sensor."""

    monkeypatch.syspath_prepend(str(CORPUS))
    provider: Any = import_module("provider_package")
    with DAG(dag_id="compat_provider_execution", schedule=None) as dag:
        provider.ExampleOperator(task_id="produce")
        provider.ExampleSensor(task_id="confirm", poke_interval=0)

    produced = run_task(dag.get_task("produce"))
    confirmed = run_task(dag.get_task("confirm"))

    assert produced.xcoms["return_value"] == {"connected": True}
    assert confirmed.state == TaskInstanceState.SUCCESS
