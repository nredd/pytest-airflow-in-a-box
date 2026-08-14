"""Provider-shaped custom operator."""

from __future__ import annotations

from importlib import import_module
from typing import Any

from provider_package._hook import ExampleHook


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


# Airflow 2.x carries the base class in `airflow.models`.
BaseOperator = _resolve("airflow.sdk", "airflow.models").BaseOperator


class ExampleOperator(BaseOperator):
    """Execute through the adjacent custom hook."""

    def execute(self, context: Any) -> dict[str, bool]:
        del context
        return ExampleHook().get_conn()
