"""Provider-shaped custom operator."""

from __future__ import annotations

from importlib import import_module
from typing import Any

from provider_package._hook import ExampleHook

# Both entry points into this package prime `sys.path` with `tests/dags` first --
# `provider.py` for DagBag parsing, and `tests/enduser/test_provider_package.py`'s
# direct `syspath_prepend` + `import_module("provider_package")` -- which is also how
# `_family` itself is reached.
_resolve = import_module("_family")._resolve

# Airflow 2.x carries the base class in `airflow.models`.
BaseOperator = _resolve("airflow.sdk", "airflow.models").BaseOperator


class ExampleOperator(BaseOperator):
    """Execute through the adjacent custom hook."""

    def execute(self, context: Any) -> dict[str, bool]:
        del context
        return ExampleHook().get_conn()
