# Registration and packaging

A clean [`check_component`](custom-components.md) report proves shape, not discovery.
Production loads components through configuration, plugins, or package metadata. Tests have
three matching channels: session configuration, reversible per-test registration, and an
isolated process for real entry-point discovery.

## Session configuration

Five ini options establish the substrate before the first Airflow import:

| Option | Writes |
| --- | --- |
| `airflow_plugins_folder` | Entries symlinked into the run's generated `plugins/` directory |
| `airflow_executor` | `[core] executor` |
| `airflow_xcom_backend` | `[core] xcom_backend` |
| `airflow_secrets_backend` | `[secrets] backend` |
| `airflow_secrets_backend_kwargs` | `[secrets] backend_kwargs` when a backend is configured |

```ini
[pytest]
airflow_plugins_folder = plugins
airflow_executor = tests.support.executors.FakeExecutor
airflow_xcom_backend = tests.support.xcom.RecordingXCom
```

These are ini-only because they are project facts, not per-invocation switches. The timing is
essential for XCom backends: the Task SDK resolves that setting at module import. The full
option contract is in [INI options](../reference/ini-options.md).

## Runtime component registration

`airflow_components` overlays one test on the session substrate, then restores the exact prior
process-global registries:

```python
def test_my_listener_fires(airflow_components, dag_maker):
    airflow_components.listener(MyListener)
    with dag_maker():
        ...

    dag_maker.run()
    assert MyListener.calls
```

Every method calls `check_component` first. A bad component raises `ComponentContractError`; a
registration failure raises `ComponentSandboxError`.

| Method | Registers |
| --- | --- |
| `plugin(component)` | Every plugins-manager half available on the installed release |
| `listener(component, *, core=True, task=True)` | Core and/or Task SDK listener managers |
| `policy(**hooks)` | A temporary policy plugin from hookspec-named callables |
| `secrets_backend(component, *, first=True)` | The secrets backend search path |
| `executor(component, *, alias="test")` | An importable executor class and returns its alias |
| `timetable(component)` | A timetable class through a synthetic plugin |
| `priority_weight_strategy(component)` | A strategy class through a synthetic plugin |
| `serialization_round_trip(instance)` | Timetable registration plus encode/decode assertion |
| `round_trip(component)` | Auto-detects and registers one supported bare component |

Executor classes and registered timetable classes must be importable at module scope. A policy
uses explicit `policy(task_policy=..., dag_policy=..., ...)` hooks because it has no useful bare
component form. On Airflow 3.1, request `task=False` for a listener intentionally limited to the
core manager.

No Airflow configuration option can select a per-test executor alias: configuration is read
before the alias exists. Pass the alias directly to
[`run_dag(..., executor=alias)`](ladder.md#executor-driven-runs).

The fixture is unavailable on Airflow 2.x. On an uncertified 3.x release it degrades to live
capability probing, warns once, and still restores state exactly; `--airflow-doctor` reports the
degraded tier.

## How the channels compose

The ini options are fixed session substrate. `airflow_components` snapshots that state, adds a
per-test overlay, and restores the snapshot—not an empty registry. `airflow_config` environment
overrides sit between them in precedence: they change what `conf.get()` reads but do not mutate
the registries.

If both `airflow_executor` and an `airflow_config` line for `core.executor` exist, the
environment-backed `airflow_config` value wins. Prefer one source.

## Isolated entry-point discovery

Anything discovered through installed distribution metadata—`airflow.plugins`,
`apache_airflow_provider`, or `airflow.policy`—cannot be honestly fabricated in the current
process. Metadata discovery is cached, and XCom backends bind at import time.

For a package you ship, the most faithful test is to install it editable and assert its real
entry points. Use `airflow_isolated` when the suite needs mutually exclusive registrations or
the component is not packaged yet:

```python
import pytest


@pytest.mark.airflow_isolated(
    entry_points={"airflow.plugins": "my_plugin = my_pkg.plugin:MyPlugin"}
)
def test_my_plugin_is_discovered():
    from airflow.plugins_manager import get_plugin_info

    assert "my_plugin" in [info["name"] for info in get_plugin_info()]
```

The marker starts a one-shot child pytest process with a real synthetic `dist-info` directory
on `PYTHONPATH`. The child inherits the isolated `AIRFLOW_HOME`, database, and secrets; its
outcomes and tracebacks replay as the parent's own.

Marker keywords:

- `entry_points`: group to one `name = module:attr` line or a list of lines.
- `environment`: `AIRFLOW__*` overrides applied before import. Bootstrap-owned names are
  rejected.
- `name`: synthetic distribution name, needed when provider package identity matters.
- `timeout`: child deadline in seconds, default 300.

Tests in one module with an identical payload share a child. The child loads conftests and
fixtures but clears command-line-only selection, xdist, coverage, and report destinations.
The marker is rejected on an xdist worker, so run isolated tests in a separate serial
invocation.

Reserve this mechanism for discovery. Component logic is directly callable, and
`check_component` plus `airflow_components` cover shape and reversible registration without a
subprocess.

## Sandbox edges

- Secrets teardown restores the exact prior backend instances, including their accumulated
  state.
- Clearing plugin caches can reload plugins-folder modules; compare objects by name across
  tests, not identity.
- Each sandboxed test may rescan the plugins folder at construction and teardown.
- An ini-seeded listener survives teardown because it is part of the snapshot.

Those costs are why session configuration is preferable when every test needs the same
component, and the per-test sandbox is preferable when isolation is the point.
