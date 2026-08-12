"""Sensor corpus Dag."""

from __future__ import annotations

from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import dag


@dag(schedule=None)
def sensor() -> None:
    """Define one deterministic Python sensor."""

    PythonSensor(task_id="ready", python_callable=lambda: True, poke_interval=0)


sensor()
