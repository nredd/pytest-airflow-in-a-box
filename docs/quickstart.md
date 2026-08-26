# Quickstart

You have a `dags/` folder, custom operators, and CI on Airflow 3. This gets you a real DagRun
in your suite in one file and one flag.

Install first if you have not: [Installing the plugin](install.md).

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

`dag_bag` parses that folder once per worker process. `--dag-folder` has an ini twin,
`airflow_dags_folder`, so you set it once instead of on every invocation -- see
[Where the run lives](internals/test-environments.md#the-isolated-airflow_home).

`run_dag` proves your *real file*, under its real `dag_id`, actually finishes in the states
you think it does. `result.order` is the executed order, not graph topology.

## Dags authored in the test

`dag_maker` builds and persists a Dag written in the test body, so nothing has to exist on
disk:

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

```console
pytest
```

`run_dag()` and `dag_maker.run()` return the same inert `DagRunResult` snapshot.
`pytest_airflow_in_a_box.matchers` collapses it into one expression:

```python
from pytest_airflow_in_a_box.matchers import succeeded

assert result == {"produce": succeeded(21), "consume": succeeded(42)}
```

The full API: [Real DagRuns and real state](guide/task-execution.md).

## Catch a branch skip

A branch skip is invisible to both halves of the usual suite: the file parses, every callable
returns the right value, and `rejected` still runs when it should not have.

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

    assert result.success
    assert result.order == ["choose", "chosen"]
    assert result["rejected"] == skipped()
```

`dag.test()` cannot phrase either assertion -- see
[why not `dag.test()`](why/index.md#why-not).

## Run one operator without a database

`run_task` executes a single operator in process through the Task SDK runner. No metadata DB,
no DagRun, no migration. Airflow 3.x only:

```python
from airflow.sdk import task


@task
def add(x: int, y: int) -> int:
    return x + y


def test_add(run_task):
    result = run_task(add(1, 2).operator)

    assert result.xcoms["return_value"] == 3
```

`render_task` and `task_context` stop earlier on the same machinery -- see
[One operator, no database](guide/db-free-execution.md).

## How deep do you go?

The decision behind every test on this page: how much Airflow machinery does the assertion
need? Rendered `template_fields` only, one operator in process, or a real `DagRun` backed by a
metadata DB? Each step up costs more setup and runtime. The full cost and capability
comparison -- and what each rung *cannot* prove -- is
[The fidelity ladder](guide/ladder.md).

Next:

- [Deciding which failures are yours](guide/testing-scope.md)
- [Recipes for the handoffs between tasks](guide/cookbook.md)
- [Smoke checks over every Dag](guide/smoke-tests.md) -- properties of the whole corpus, not
  one Dag
- [The GitHub Action](guide/ci/github-action.md)
