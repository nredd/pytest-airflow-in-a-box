# pytest-airflow-in-a-box

[![CI](https://github.com/nredd/pytest-airflow-in-a-box/actions/workflows/ci.yml/badge.svg)](https://github.com/nredd/pytest-airflow-in-a-box/actions/workflows/ci.yml)
[![coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/nredd/pytest-airflow-in-a-box/badges/coverage.json)](https://github.com/nredd/pytest-airflow-in-a-box/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/pytest-airflow-in-a-box)](https://pypi.org/project/pytest-airflow-in-a-box/)
[![Python versions](https://img.shields.io/pypi/pyversions/pytest-airflow-in-a-box)](https://pypi.org/project/pytest-airflow-in-a-box/)
[![License](https://img.shields.io/pypi/l/pytest-airflow-in-a-box)](https://github.com/nredd/pytest-airflow-in-a-box/blob/main/LICENSE)

`pytest-airflow-in-a-box` is a pytest plugin for testing Apache Airflow DAGs without a live
Airflow deployment. It targets Airflow 3 and provides the package and plugin foundation for a
small, typed testing surface.

The package auto-registers with pytest, creates an isolated metadata database, and provides typed
fixtures for persisted Dags, DagRuns, task instances, sessions, and Dag bags.

## Requirements

- CPython 3.10 through 3.14
- Apache Airflow 3.1 or newer, below 4
- Linux or macOS for Airflow-backed tests

Apache Airflow does not support native Windows installations. Windows development should use WSL2
or the included devcontainer; platform-independent package checks alone do not imply full Windows
Airflow support.

The released compatibility matrix is exercised against Airflow 3.1.0, 3.1.8, 3.2.0, 3.2.2, and
3.3.0 across CPython 3.10 through 3.14 using Airflow's published constraints files.

## Installation

```console
uv add --dev pytest-airflow-in-a-box
```

The `pytest11` entry point loads the plugin automatically. Consumer projects do not need to add a
`pytest_plugins` declaration.

## Development

```console
uv sync
uv run prek install
make all
```

Run the GitHub Actions workflow locally on Linux with [act](https://nektosact.com/):

```console
act pull_request
```

`act` cannot reproduce native macOS or Windows behavior.

## Task execution

```python
from airflow.sdk import task
from airflow.utils.state import TaskInstanceState

from pytest_airflow_in_a_box.taskinstance import ordered_task_instances


def test_task(dag_maker):
    with dag_maker() as dag:

        @task
        def answer():
            return 42

        answer()

    dag_run = dag_maker.create_dagrun()
    ti = dag_maker.run_ti("answer", dag_run)

    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids="answer", session=dag_maker.session) == 42
    assert ordered_task_instances(dag_run, dag, session=dag_maker.session) == [ti]
```

Public task helpers live in `pytest_airflow_in_a_box.taskinstance`: `run_task_instance`,
`ordered_task_instances`, and `TaskResolutionError`. The `DagMaker` protocol additionally exposes
`create_dagrun`, `create_ti`, and `run_ti`.

## DB-free task execution

`run_task` executes one operator through the Task SDK in process, with no metadata database. XCom,
Variable, and Connection traffic is answered from seeded dictionaries; unseeded lookups fail
exactly like a live deployment. Task callbacks and listeners stay silent unless the call passes
`run_callbacks=True`.

```python
def test_operator(run_task):
    result = run_task(
        my_operator,
        variables={"answer": "42"},
        connections={"db": {"conn_type": "postgres", "host": "example.com"}},
    )

    assert result.state == TaskInstanceState.SUCCESS
    assert result.xcoms["return_value"] == "expected"
```

## Structlog capture

Airflow 3 logs through structlog, where pytest's builtin `caplog` cannot see records. The
`cap_structlog` fixture records every event emitted during the test:

```python
def test_logging(cap_structlog, dag_maker):
    ...
    assert "task_event" in cap_structlog
    assert {"answer": 42, "log_level": "warning"} in cap_structlog
```

## Dag-file collection

Point the collector at a directory of real Dag files and every `*.py` file below it is collected
as a `dag-import` test item that fails on import errors or a Dag-free file. Off unless configured:

```console
pytest --collect-dag-folder=dags/
```

or persistently via the `airflow_collect_dags_folder` ini option. Collected items are auto-marked
`db_test`; files also matching `test_*.py` naming are deduplicated against pytest's default Python
collector.

A Dag file may pin param cases through a module-level literal, read without importing the file:

```python
PYTEST_DAG_CASES = {
    "dev": {"environment": "dev"},
    "prod": {"environment": "prod"},
}
```

Each case collects as a sibling `dag-params[...]` item that validates the pinned values against
every Dag the file declares -- undeclared keys and schema violations fail the case.

## Airflow configuration

`airflow_config` overrides Airflow configuration options and plain environment variables through
one code path, as a context manager or a decorator. Options are applied as `AIRFLOW__SECTION__KEY`
environment variables -- the same pre-import-safe mechanism bootstrap uses -- so they reach every
Airflow configuration parser in the process, including the Task SDK parser added in Airflow 3.2:

```python
from pytest_airflow_in_a_box.config import airflow_config


def test_with_overrides():
    with airflow_config({("core", "unit_test_mode"): "False"}, env={"MY_FLAG": "1"}):
        ...


@airflow_config({("core", "dagbag_import_timeout"): "120"})
def test_decorated():
    ...
```

Every name is restored exactly on exit, and a name that was absent beforehand is deleted rather
than emptied. Nesting restores last-in-first-out. A `None` value makes a name absent for the
duration of the context, so Airflow falls back to `airflow.cfg` and then to its own default:

```python
with airflow_config({("core", "dagbag_import_timeout"): None}):
    ...  # conf.get returns Airflow's default
```

Both mappings are validated before anything is assigned, so a malformed argument cannot leave the
environment partly modified. Validation runs on context entry, so a bad argument to the decorator
form surfaces as a test failure rather than a collection error. `env` names may not start with
`AIRFLOW__` -- pass configuration options through `overrides` instead -- but `AIRFLOW_HOME` and
other single-underscore names are fine.

Airflow resolves `SQL_ALCHEMY_CONN`, `DAGS_FOLDER`, and `PLUGINS_FOLDER` into `airflow.settings`
globals once at import, and those do not follow an environment assignment. Pass
`refresh_settings=True` for options read through `settings` rather than through the config parser:

```python
with airflow_config({("core", "plugins_folder"): str(tmp_path)}, refresh_settings=True):
    ...  # airflow.settings.PLUGINS_FOLDER now agrees
```

It defaults to off because it imports Airflow and rewrites process-global state bootstrap owns,
and it is a partial remedy: a module that re-exported a settings value *by value* froze that
binding at import and no refresh can update it.

Three things worth knowing:

- **Values are expanded when Airflow reads them.** `conf.get` runs the raw variable through
  `expandvars` then `expanduser`, so a value containing `~` or `$` does not round-trip --
  `os.environ` holds the literal while `conf.get` returns the expansion.
- **A `None` override does not hide a `_CMD`/`_SECRET` sibling.** Setting a plain value always
  wins, but `None` means "fall back to whatever Airflow would otherwise do", and an already-set
  sibling variable is one of those things.
- **Do not wrap the first use of `api_client`/`api_server_url`.** Those fixtures launch a
  session-scoped subprocess that inherits the environment live at startup, so an override would
  outlive the context inside that server.

`conf_vars` ships as a deprecated alias under the name public Airflow docs teach. It emits a
`DeprecationWarning` and carries this plugin's semantics, so it does not recompute the settings
globals the way upstream's does -- use `airflow_config(..., refresh_settings=True)` for that.

## Database cleanup

`clear_db` is a registry-driven whole-database reset for serial setup and teardown contexts:

```python
from pytest_airflow_in_a_box.db import TableGroup, clear_db

clear_db()  # every group
clear_db(tables={TableGroup.VARIABLES})  # one group
```

Requesting a group also clears the groups whose rows reference it (`RUNS` clears task instances
and XCom rows), and clearing `CONNECTIONS` recreates Airflow's default connections.

## Live REST API

`api_client` lazily starts one isolated `airflow api-server` per test process on a loopback
ephemeral port and returns a typed client authenticated through SimpleAuthManager:

```python
import pytest


@pytest.mark.api_test
def test_api(api_client, dag_maker):
    with dag_maker(dag_id="visible"):
        ...

    response = api_client.get("/api/v2/dags/visible")

    assert response.status == 200
    assert response.body["dag_id"] == "visible"
```

## Markers

- `db_test`: requires the isolated metadata database
- `api_test`: requires the isolated REST API server
- `compat`: end-user tests exercised across the version matrix
- `need_serialized_dag([enabled])`: request serialized Dag behavior from `dag_maker`
- `environment(name)`: run only when the named environment's sentinel path exists, configured via
  the `airflow_environments` ini line list (`lab = /opt/lab/sentinel`)

## Defaults

The plugin needs zero ini configuration. It applies `--tb=short`, `-ra`, `--durations=20`, and
failed-only `tmp_path` retention, but only where the user has not chosen a value -- explicit flags
and ini settings always win. Warning filters silence traced third-party deprecation noise
(`flask_appbuilder`, `flask_sqlalchemy`, `starlette`) while keeping Airflow's own deprecation
warnings visible, and promote pytest's collection and unraisable warnings to errors. User-supplied
`filterwarnings` lines take precedence.

## License

Apache License 2.0. See `LICENSE`, `NOTICE`, and `PROVENANCE.md`.
