"""TaskFlow mapping corpus Dag."""

from __future__ import annotations

from airflow.sdk import dag, task

PYTEST_DAG_CASES = {"smoke": {"factor": 2}}


@dag(schedule=None, params={"factor": 2})
def mapped() -> None:
    """Map one pure transformation over literal values."""

    @task
    def multiply(value: int) -> int:
        return value * 2

    multiply.expand(value=[1, 2, 3])


mapped()
