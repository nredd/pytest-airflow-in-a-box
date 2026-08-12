"""Provider-shaped custom operator."""

from __future__ import annotations

from importlib import import_module
from typing import Any

from provider_package._hook import ExampleHook

# `provider.py` puts `tests/dags` on `sys.path` before importing this package, the same
# path `_family` itself is reached through.
_resolve = import_module("_family")._resolve

# Airflow 2.x carries the base class in `airflow.models`.
BaseOperator = _resolve("airflow.sdk", "airflow.models").BaseOperator


class ExampleOperator(BaseOperator):
    """Execute through the adjacent custom hook."""

    def execute(self, context: Any) -> dict[str, bool]:
        del context
        return ExampleHook().get_conn()
