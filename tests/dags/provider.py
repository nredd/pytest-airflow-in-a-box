"""Compose a provider-shaped package in one corpus Dag."""

from __future__ import annotations

import sys
from importlib import import_module
from pathlib import Path
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

sys.path.insert(0, str(Path(__file__).parent))
provider_package = import_module("provider_package")
ExampleOperator = provider_package.ExampleOperator
ExampleSensor = provider_package.ExampleSensor


@dag(schedule=None)
def provider_composition() -> None:
    """Chain a custom provider operator and sensor."""

    ExampleOperator(task_id="produce") >> ExampleSensor(task_id="confirm", poke_interval=0)


provider_composition()
