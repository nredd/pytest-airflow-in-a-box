# Quickstart

You have a `dags/` folder, custom operators, and CI on Airflow 3. This gets you a real DagRun
in your suite in one file and one flag.

Install first if you have not:
[Installing the plugin](install.md).

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
[Where the run lives](guide/airflow-home.md).

`run_dag` proves your *real file*, under its real `dag_id`, actually settles the way you
think. `result.order` is the executed order, not graph topology.

## Author the Dag in the test instead

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

`run_dag()` and `dag_maker.run()` return the same inert `DagRunResult` snapshot: `states`,
`xcoms`, `errors`, `order`, and per-task access via `result["task_id"]`. One task at a time is
`dag_maker.run_ti("produce")`. `pytest_airflow_in_a_box.matchers` collapses the whole snapshot
into one expression:

```python
from pytest_airflow_in_a_box.matchers import succeeded

assert result == {"produce": succeeded(21), "consume": succeeded(42)}
```

Full surface: [Real DagRuns and real state](guide/task-execution.md).

## Run one operator with no database at all

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

Same family, different jobs: `render_task` resolves `template_fields` without calling
`execute()`, and `task_context` hands you a real Task SDK context for a hand-driven
`execute()`. See [One operator, no database](guide/db-free-execution.md).

## Which rung do I want

Stand on the lowest rung that can still fail for the reason you care about. The full cost and
capability of each -- and what each one structurally *cannot* prove -- is
[The fidelity ladder](guide/ladder.md).

Next:

- [Deciding which failures are yours](guide/testing-scope.md)
- [Recipes for the seams between tasks](guide/cookbook.md)
- [Smoke checks over every Dag](guide/smoke-tests.md) -- properties of the whole corpus, not
  one Dag
- [The GitHub Action](guide/ci/github-action.md)
