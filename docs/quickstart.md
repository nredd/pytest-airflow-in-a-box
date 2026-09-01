# Quickstart

Install the plugin, point pytest at your `dags/` folder, and verify a real `DagRun` in one
test.

## Install

With `uv`:

```console
uv add --dev "pytest-airflow-in-a-box[airflow3]"
```

With `pip`:

```console
pip install "pytest-airflow-in-a-box[airflow3]"
```

The `pytest11` entry point registers the plugin automatically. Confirm the installation:

```console
pytest --airflow-doctor
```

If your project already pins Airflow, install the plugin without an Airflow extra. For Airflow
2, Postgres, parallel testing, and common extra combinations, see
[Dependencies and extras](reference/dependencies.md). Linux and macOS are supported; on
Windows, use WSL2 or the devcontainer.

## Run a Dag from your repository

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

`dag_bag` parses the folder once per worker process. `run_dag` executes the selected Dag under
its real `dag_id`; `result.order` records execution order, not graph topology. Set
`airflow_dags_folder` in pytest's ini configuration when `dags/` is your repository default.

## Author a Dag in the test

Use `dag_maker` when the Dag belongs in the test rather than a repository file:

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

`run_dag()` and `dag_maker.run()` return the same inert `DagRunResult` snapshot. Outcome
matchers keep whole-run assertions concise. `piab` is a supported alias package shipped in
the same wheel (the pattern `attrs` uses for `attr`/`attrs`): every public module of
`pytest_airflow_in_a_box` is importable under the short name and resolves the same objects,
and `import pytest_airflow_in_a_box as piab` attribute access works too:

```python
from piab.matchers import succeeded

assert result == {"produce": succeeded(21), "consume": succeeded(42)}
```

## Verify branching behavior

Parsing a Dag and calling its Python functions cannot prove which branch runs or which tasks
Airflow skips:

```python
from airflow.sdk import task

from piab.matchers import skipped


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

This test verifies the branch choice, skip state, and execution order together. See
[Whose fail is it anyway?](guide/testing-scope.md) for the boundary around worthwhile tests.

## Run one operator without a database

On Airflow 3, `run_task` executes one operator through the Task SDK without a metadata
database, `DagRun`, or migration:

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
[fidelity ladder](guide/ladder.md) compares the runners and their limits.

## Where next?

- [The fidelity ladder](guide/ladder.md) chooses the cheapest sufficient runner.
- [Cookbook](guide/cookbook.md) covers assets, hooks, templates, and retries.
- [Smoke Tests](guide/smoke-tests.md) check properties of the whole Dag corpus.
- [GitHub Actions and reports](guide/ci/github-action.md) puts the suite into CI.

Disable the plugin for one run with `pytest -p no:pytest_airflow_in_a_box`. Airflow import and
database migration remain lazy, so unrelated tests do not pay either cost.
