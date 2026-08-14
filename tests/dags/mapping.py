"""TaskFlow mapping corpus Dag."""

from __future__ import annotations

import sys
from importlib import import_module
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
_resolve = import_module("_family")._resolve

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
