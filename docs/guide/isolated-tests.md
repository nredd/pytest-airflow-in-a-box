# Isolated entry-point tests

Anything Airflow discovers through a setuptools entry point -- `airflow.plugins`,
`apache_airflow_provider`, `airflow.policy` -- cannot be honestly tested in process.
Discovery reads installed distribution metadata through a `functools.cache`d helper
(vendored in two copies on Airflow 3.2+), and a custom XCom backend binds at module
import time. Monkeypatching those caches would test the patch, not your packaging.

`@pytest.mark.airflow_isolated` runs the marked tests in a one-shot child pytest
process instead:

```python
import pytest


@pytest.mark.airflow_isolated(
    entry_points={"airflow.plugins": "my_plugin = my_pkg.plugin:MyPlugin"}
)
def test_my_plugin_is_discovered():
    from airflow.plugins_manager import get_plugin_info

    assert "my_plugin" in [info["name"] for info in get_plugin_info()]
```

The parent builds a synthetic distribution -- a real `dist-info` directory with an
`entry_points.txt` -- from the marker's `entry_points`, prepends it to the child's
`PYTHONPATH`, and re-invokes `sys.executable -m pytest` for the marked tests. Airflow's
own `importlib.metadata` resolution then finds the entry point exactly as it would a
`pip`-installed package. The child inherits the parent's bootstrap state -- the same
`AIRFLOW_HOME`, metadata database, Fernet key, and JWT secret -- through the channel an
xdist worker uses, so nothing bootstraps twice. The child's reports are replayed as the
parent's own: outcomes, failure tracebacks, skips, and xfails all render normally.

## Marker keywords

The marker is legal at function, class, and module scope, takes keywords only, and
requires at least one of `entry_points` / `environment`:

- `entry_points`: mapping of entry-point group to one `name = module:attr` line or a
  list of lines. The referenced module must be importable in the child (a module next
  to your tests, or an installed package)
- `environment`: mapping of `AIRFLOW__*` variables applied to the child before the
  first Airflow import -- the escape hatch for settings that bind at import time, e.g.
  `{"AIRFLOW__CORE__XCOM_BACKEND": "my_pkg.xcom:MyXComBackend"}`. Variables the
  bootstrap itself owns (`AIRFLOW__CORE__FERNET_KEY`, the database connection, ...) are
  rejected; configure those through the plugin's ini options instead
- `name`: the synthetic distribution's name (default: a payload-derived
  `pytest-airflow-in-a-box-isolated-<hash>`). Set it when the name is load-bearing --
  `ProvidersManager` cross-checks a provider's `package-name` against it. The
  distribution's version is pinned at `0.0.0`
- `timeout`: seconds before the child is killed and the batch failed (default 300)

## Batching

All marked tests in one module sharing an identical marker payload run in a single
child invocation: cost scales with distinct isolation environments, not with tests. A
different payload, or the same payload in a different module, gets its own child.

## What runs where

The child is a full pytest session: your conftests load, fixtures resolve, the family
and environment gates apply, and the shared metadata database is reused (its
initialization sentinel is inherited). `addopts` is deliberately cleared in the child,
so flags configured there -- `-n`, coverage plugins, report destinations -- do not
apply inside it. A child that crashes, times out, or writes an unreadable report fails
every batched test with the child's log tail in the failure message.

Two refusals keep the process tree honest: the marker is rejected under pytest-xdist
(run marked tests without `-n`), and inside a child the marker is inert, so a
grandchild can never spawn.

## When not to reach for it

Component *logic* needs none of this: a timetable's `next_dagrun_info`, an XCom
backend's `serialize_value`, or a policy callable are directly callable in process, and
[`check_component`](custom-components.md) plus the `airflow_components` registration
sandbox cover conformance and reversible registration without a subprocess. Reserve
`airflow_isolated` for the part only real discovery can prove: the entry point itself.
