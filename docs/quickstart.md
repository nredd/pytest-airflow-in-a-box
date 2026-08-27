# Quickstart

You have a `dags/` folder, custom operators, and CI on Airflow 3. This gets a real `DagRun`
into your suite in one file and one flag.

## Install

With `uv`:

```console
uv add --dev "pytest-airflow-in-a-box[airflow3]"
```

With `pip`:

```console
pip install "pytest-airflow-in-a-box[airflow3]"
```

The `pytest11` entry point registers the plugin automatically. Verify the environment before
writing a test:

```console
pytest --airflow-doctor
```

Install the plugin without an Airflow extra when your project already pins Airflow. For
Airflow 2, Postgres, parallel testing, and common extra combinations, see
[Dependencies and extras](reference/dependencies.md). Linux and macOS are supported; on
Windows, use WSL2 or the devcontainer.

## Run a Dag from your own `dags/` folder

<!-- --8<-- [start:quickstart] -->
```python
def test_my_dag(dag_bag, run_dag):
    dag = dag_bag.dags["my_dag_id"]

    result = run_dag(dag)

    assert result.success
    assert result.order == ["extract", "load"]
```

```console
pytest --dag-folder=dags
```
<!-- --8<-- [end:quickstart] -->

`dag_bag` parses that folder once per worker process. Set `airflow_dags_folder` in pytest's ini
configuration when the path is a repository default. `run_dag` proves your real file, under
its real `dag_id`, finishes in the states you expect; `result.order` is executed order, not
graph topology.

## Author a Dag in the test

`dag_maker` builds and persists a Dag written in the test body:

```python
from airflow.sdk import task


def test_dag(dag_maker):
    with dag_maker():

        @task
        def produce() -> int:
            return 21

        @task
        def consume(value: int) -> int:
            return value * 2

        consume(produce())

    result = dag_maker.run()

    assert result.success
    assert result.xcoms == {"produce": 21, "consume": 42}
    assert result.order == ["produce", "consume"]
```

`run_dag()` and `dag_maker.run()` return the same inert `DagRunResult` snapshot. For concise
whole-run assertions, use the outcome matchers:

```python
from pytest_airflow_in_a_box.matchers import succeeded

assert result == {"produce": succeeded(21), "consume": succeeded(42)}
```

## Catch the gap a callable test misses

A branch skip is invisible when you only parse the file and call each Python function:

```python
from airflow.sdk import task

from pytest_airflow_in_a_box.matchers import skipped


def test_branch_skips_the_unselected_path(dag_maker):
    with dag_maker(dag_id="branching"):

        @task.branch
        def choose() -> str:
            return "chosen"

        @task
        def chosen() -> None: ...

        @task
        def rejected() -> None: ...

        choose() >> [chosen(), rejected()]

    result = dag_maker.run()

    assert result.order == ["choose", "chosen"]
    assert result["rejected"] == skipped()
```

The branch, trigger rules, and execution order only exist once a `DagRun` exists. See
[Whose fail is it anyway?](guide/testing-scope.md) for the boundary around worthwhile tests.

## Run one operator without a database

`run_task` executes one operator in process through the Task SDK runner. No metadata database,
`DagRun`, or migration; Airflow 3.x only:

```python
from airflow.sdk import task


@task
def add(x: int, y: int) -> int:
    return x + y


def test_add(run_task):
    result = run_task(add(1, 2).operator)

    assert result.xcoms["return_value"] == 3
```

`render_task` and `task_context` stop earlier on the same machinery. The
[fidelity ladder](guide/ladder.md) compares every runner and its limits.

## Where next?

- [The fidelity ladder](guide/ladder.md) chooses the cheapest runner that can prove your claim.
- [Cookbook](guide/cookbook.md) covers assets, hooks, templates, and retries.
- [Smoke Tests](guide/smoke-tests.md) check properties of the whole Dag corpus.
- [GitHub Actions and reports](guide/ci/github-action.md) puts the suite into CI.

Disable the plugin for one run with `pytest -p no:pytest_airflow_in_a_box`. Airflow imports and
database migration remain lazy, so unrelated tests in a shared environment do not pay either
cost.
