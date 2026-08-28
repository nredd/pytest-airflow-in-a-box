# Registration and packaging

Test that Airflow can discover your component through the same channel your deployment uses. A
clean [`check_component`](custom-components.md) report proves shape; this page proves that
Airflow can find the component.

| Production channel | Test with | Scope |
| --- | --- | --- |
| Airflow configuration | The matching pytest ini option | Session |
| Airflow plugins folder | `airflow_plugins_folder` | Session |
| Installed distribution metadata | The installed package, or `airflow_isolated` while developing it | Isolated process |
| Temporary registry entry | `airflow_components` | One test; not a production-discovery test |

## Session configuration

Configure components that production selects through `airflow.cfg` before Airflow's first
import:

| Option | Production setting |
| --- | --- |
| `airflow_executor` | `[core] executor` |
| `airflow_xcom_backend` | `[core] xcom_backend` |
| `airflow_secrets_backend` | `[secrets] backend` |
| `airflow_secrets_backend_kwargs` | `[secrets] backend_kwargs` |

```ini
[pytest]
airflow_executor = tests.support.executors.FakeExecutor
airflow_xcom_backend = tests.support.xcom.RecordingXCom
```

These settings must precede Airflow's first import. XCom backends are especially strict: the
Task SDK resolves one when its module loads. The
[INI reference](../reference/ini-options.md) defines every option.

For file-based `AirflowPlugin` discovery, point `airflow_plugins_folder` at the same directory
layout your deployment receives:

```ini
[pytest]
airflow_plugins_folder = plugins
```

A relative path resolves from pytest's root. Each source entry is symlinked into the isolated
`AIRFLOW_HOME/plugins`, so edits remain live. A missing or non-directory path stops the
session with a usage error.

## Runtime component registration

Use `airflow_components` when the test needs a component registered temporarily, not when the
claim is that production will discover it:

```python
def test_my_listener_fires(airflow_components, dag_maker):
    airflow_components.listener(MyListener)
    with dag_maker():
        ...

    dag_maker.run()

    assert MyListener.calls
```

It registers plugins, listeners, policies, secrets backends, executors, timetables, and
priority-weight strategies. Use the named method when scope matters; `round_trip()` detects an
unambiguous bare component. Timetable state uses `serialization_round_trip()`; see
[Timetables](custom-timetables.md).

Every registration runs `check_component` first. Contract failures raise
`ComponentContractError`; registry failures raise `ComponentSandboxError`. Executor and
timetable classes must be importable at module scope. Airflow 3.1 has no Task SDK listener
manager, so a core-only listener must use `listener(..., task=False)`.

It does not install XCom backends, notifiers, or providers. Configure XCom for the session,
reference notifiers from Dag code, and test provider metadata in an isolated process.

The fixture is Airflow 3 only. On an uncertified 3.x release it probes the available registry
surfaces, warns once, and still restores them after the test.

## How the channels compose

Session configuration and plugins-folder discovery form the baseline. `airflow_components`
adds a test overlay, then restores that baseline -- not an empty registry.

`airflow_config` changes `conf.get()` but not live registries. If it and `airflow_executor`
both set `core.executor`, the environment-backed `airflow_config` value wins. Prefer one.

Configuration cannot select an alias registered later in one test. Pass the alias from
`airflow_components.executor()` to
[`run_dag(..., executor=alias)`](ladder.md#executor-driven-runs).

## Isolated entry-point discovery

Install a real package into the test environment and assert that Airflow's manager finds it.
Only this path verifies the entry points in the built package.

Before packaging -- or for mutually exclusive distributions -- use `airflow_isolated` to create
synthetic distribution metadata in a one-shot child:

```python
import pytest


@pytest.mark.airflow_isolated(
    entry_points={"apache_airflow_provider": "provider_info = my_pkg.provider:get_provider_info"},
    name="my-provider",
)
def test_provider_is_discovered():
    from airflow.providers_manager import ProvidersManager

    assert "my-provider" in ProvidersManager().providers
```

Use it for Airflow groups such as `airflow.plugins`, `apache_airflow_provider`, and
`airflow.policy`. It proves that the declaration resolves through the installed Airflow; its
synthetic metadata cannot prove that `pyproject.toml` contains the declaration.

The child inherits the isolated `AIRFLOW_HOME`, database, and secrets; outcomes and tracebacks
replay in the parent. Same-module tests with identical payloads share a child, so a timeout or
crash fails the batch. Run them serially: xdist workers reject the marker. See the
[`airflow_isolated` reference](../reference/markers.md#isolation-airflow_isolated).

## Sandbox edges

- Sandboxed teardown restores the exact prior registry objects, including stateful secrets
  backend instances.
- Plugin-cache resets can reload plugins-folder modules. Compare components by stable names,
  not class or object identity, across tests.
- The sandbox may rescan the plugins folder. Prefer session configuration when every test
  needs the component.
- Session configuration and plugins-folder discovery work on Airflow 2 and 3. The component
  sandbox requires Airflow 3. Entry-point tests exercise whichever groups the installed
  Airflow family supports.
