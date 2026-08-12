"""Sensor corpus Dag."""

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
_sensors = _resolve("airflow.providers.standard.sensors.python", "airflow.sensors.python")
dag = _authoring.dag
PythonSensor = _sensors.PythonSensor


@dag(schedule=None)
def sensor() -> None:
    """Define one deterministic Python sensor."""

    PythonSensor(task_id="ready", python_callable=lambda: True, poke_interval=0)


sensor()
