# Entry points

Anything Airflow discovers through a setuptools entry point -- `airflow.plugins`,
`apache_airflow_provider`, `airflow.policy` -- cannot be honestly tested in process.
Discovery reads installed distribution metadata through a `functools.cache`d helper
(vendored in two copies on Airflow 3.2+), and a custom XCom backend binds at module import
time. Monkeypatching those caches would test the patch, not your packaging.

Reach for the real thing first. For a package you actually ship, installing it with
`uv pip install -e .` into the test environment and then asserting discovery is the
more faithful test: it exercises the entry points your own `pyproject.toml` declares. The marker below
synthesizes `entry_points.txt` from a string literal in the test, so a typo in the real
`pyproject.toml` still goes green.

Two cases the marker wins outright:

- **Mutually exclusive registrations in one suite.** One installed distribution declares
  one set of entry points. A test asserting your plugin is discovered and another asserting
  the fallback when it is not cannot both hold in a single process
- **Code that is not packaged yet.** A plugin or policy module living next to your tests,
  with no distribution to install

## The marker

`@pytest.mark.airflow_isolated` runs the marked tests in a one-shot child pytest process:

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
`PYTHONPATH`, and re-invokes `sys.executable -m pytest` for the marked tests. Airflow's own
`importlib.metadata` resolution then finds the entry point exactly as it would a
`pip`-installed package. The child inherits the parent's bootstrap state -- the same
`AIRFLOW_HOME`, metadata database, Fernet key, and JWT secret -- through the channel an
xdist worker uses, so nothing bootstraps twice. The child's reports are replayed as the
parent's own: outcomes, failure tracebacks, skips, and xfails all render normally.

## Marker keywords

Legal at function, class, and module scope, keywords only, at least one of
`entry_points` / `environment` required:

- `entry_points`: mapping of entry-point group to one `name = module:attr` line or a list
  of lines. The referenced module must be importable in the child -- a module next to your
  tests, or an installed package
- `environment`: mapping of `AIRFLOW__*` variables applied to the child before the first
  Airflow import, the escape hatch for settings that bind at import time, e.g.
  `{"AIRFLOW__CORE__XCOM_BACKEND": "my_pkg.xcom:MyXComBackend"}`. Variables the bootstrap
  itself owns (`AIRFLOW__CORE__FERNET_KEY`, the database connection, ...) are rejected;
  configure those through [the plugin's ini
  options](custom-components-wiring.md) instead
- `name`: the synthetic distribution's name (default: a payload-derived
  `pytest-airflow-in-a-box-isolated-<hash>`). Set it when the name is load-bearing --
  `ProvidersManager` cross-checks a provider's `package-name` against it. The
  distribution's version is pinned at `0.0.0`
- `timeout`: seconds before the child is killed and the batch failed (default 300)

## Batching

All marked tests in one module sharing an identical marker payload run in a single child
invocation: cost scales with distinct isolation environments, not with tests. A different
payload, or the same payload in a different module, gets its own child.

## What runs where

The child is a full pytest session: your conftests load, fixtures resolve, the family and
environment gates apply, and the shared metadata database is reused (its initialization
sentinel is inherited). `addopts` and an ambient `PYTEST_ADDOPTS` are deliberately cleared
in the child, so flags configured there -- `-n`, coverage plugins, report destinations --
do not apply inside it. The same holds for options passed only on the parent's command
line (`--airflow-record`, `--airflow-baseline*`, `-k`, ...): the child sees the ini
configuration, not the parent's argv, though replayed outcomes still land in the parent's
own record and baseline accounting. A child that crashes, times out, or writes an
unreadable report fails every batched test with the child's log tail in the failure
message.

One caveat cuts the other way: the child's environment -- including the plugin's
bootstrap-state and isolation variables -- is inherited by any process your test spawns. A
test that itself launches pytest must scrub the `PYTEST_AIRFLOW_IN_A_BOX_*` variables
first, or the grandchild will silently reuse the outer run's `AIRFLOW_HOME` and metadata
database (the same care an xdist worker already demands).

## The xdist tax

The marker is rejected under pytest-xdist with a `pytest.UsageError`, and inside a child it
is inert, so a grandchild can never spawn. That is a real cost, not a footnote: CI runs
`-n auto`, so marked tests need their own serial invocation or a deselection in the
parallel one. Budget for it before scattering the marker across a suite.

## When not to reach for it

Component *logic* needs none of this: a timetable's `next_dagrun_info`, an XCom backend's
`serialize_value`, or a policy callable are directly callable in process, and
[`check_component`](custom-components.md) plus the
[`airflow_components`](custom-components-wiring.md#runtime-component-registration)
registration sandbox cover conformance and reversible registration with no subprocess.
Reserve `airflow_isolated` for the part only real discovery can prove: the entry point
itself.
