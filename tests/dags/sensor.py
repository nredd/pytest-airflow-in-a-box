"""Sensor corpus Dag."""

from __future__ import annotations

import sys
from importlib import import_module
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
_resolve = import_module("_family")._resolve

_authoring = _resolve("airflow.sdk", "airflow.decorators")
_sensors = _resolve("airflow.providers.standard.sensors.python", "airflow.sensors.python")
dag = _authoring.dag
PythonSensor = _sensors.PythonSensor


@dag(schedule=None)
def sensor() -> None:
    """Define one deterministic Python sensor."""

    PythonSensor(task_id="ready", python_callable=lambda: True, poke_interval=0)


sensor()
