"""Provider-shaped custom hook."""

from importlib import import_module

# `provider.py` puts `tests/dags` on `sys.path` before importing this package, the same
# path `_family` itself is reached through.
_resolve = import_module("_family")._resolve

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
