# pytest-airflow-in-a-box

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

## License

Apache License 2.0. See `LICENSE`, `NOTICE`, and `PROVENANCE.md`.
