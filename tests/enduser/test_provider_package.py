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


# Shared with the six sibling contract modules; see `tests/enduser/_authoring.py`.
_resolve = import_module("_authoring")._resolve


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
