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
