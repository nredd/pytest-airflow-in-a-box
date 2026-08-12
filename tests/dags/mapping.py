"""TaskFlow mapping corpus Dag."""

from __future__ import annotations

from importlib import import_module
from typing import Any


def _resolve(*candidates: str) -> Any:
    """Import the first available module; the corpus parses on both Airflow families.

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


_authoring = _resolve("airflow.sdk", "airflow.decorators")
dag = _authoring.dag
task = _authoring.task

PYTEST_DAG_CASES = {"smoke": {"factor": 2}}


@dag(schedule=None, params={"factor": 2})
def mapped() -> None:
    """Map one pure transformation over literal values."""

    @task
    def multiply(value: int) -> int:
        return value * 2

    multiply.expand(value=[1, 2, 3])


mapped()
