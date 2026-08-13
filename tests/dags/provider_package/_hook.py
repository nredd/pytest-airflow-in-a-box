"""Provider-shaped custom hook."""

from importlib import import_module

# Both entry points into this package prime `sys.path` with `tests/dags` first --
# `provider.py` for DagBag parsing, and `tests/enduser/test_provider_package.py`'s
# direct `syspath_prepend` + `import_module("provider_package")` -- which is also how
# `_family` itself is reached.
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
