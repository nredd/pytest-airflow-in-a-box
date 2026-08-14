"""Provider-shaped custom hook."""

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


# Airflow 2.x carries the base class in `airflow.hooks.base`.
BaseHook = _resolve("airflow.sdk", "airflow.hooks.base").BaseHook


class ExampleHook(BaseHook):
    """Return one deterministic provider value."""

    conn_name_attr = "conn_id"
    default_conn_name = "provider_example"
    conn_type = "example"
    hook_name = "Example"

    def get_conn(self) -> dict[str, bool]:
        return {"connected": True}
