"""Compose a provider-shaped package in one corpus Dag."""

from __future__ import annotations

import sys
from importlib import import_module
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
_family = import_module("_family")
_resolve = _family._resolve
_dag_kwargs = _family._dag_kwargs

_authoring = _resolve("airflow.sdk", "airflow.decorators")
dag = _authoring.dag

provider_package = import_module("provider_package")
ExampleOperator = provider_package.ExampleOperator
ExampleSensor = provider_package.ExampleSensor


@dag(schedule=None, **_dag_kwargs())
def provider_composition() -> None:
    """Chain a custom provider operator and sensor."""

    ExampleOperator(task_id="produce") >> ExampleSensor(task_id="confirm", poke_interval=0)


provider_composition()
