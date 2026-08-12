# Task execution

## Whole-DagRun execution

`dag_maker.run()` creates the DagRun (when you did not), executes every task instance in
dependency order, and returns an inert `DagRunResult` snapshot:

```python
from airflow.sdk import task


def test_dag(dag_maker):
    with dag_maker():

        @task
        def produce():
            return 21

        @task
        def consume(value):
            return value * 2

        consume(produce())

    result = dag_maker.run()

    assert result.success
    assert result.xcoms == {"produce": 21, "consume": 42}
    assert result.order == ["produce", "consume"]
```

`DagRunResult` exposes `success`, `state`, `states` (per-task-key `TaskInstanceState`),
`xcoms` (per-task `return_value`, mapped tasks as index-ordered lists), `errors` (captured
task-body exceptions), `order` (the sequence tasks *actually* executed in, not just graph
topology), `tis`, `dag_run`, and per-task access via `result["task_id"]` or
`result["mapped_task", map_index]`. Mapped tasks expand mid-run once their upstream values
exist, and keys expand with them: `states` gains `"double[0]"`, `"double[1]"`, and so on.

Failure is captured scheduler-shaped: a raising task body lands in `result.errors`, blocked
downstreams settle as `upstream_failed`, and `result.success` reports `False` -- testing an
intentional-failure Dag needs no extra flag:

```python
result = dag_maker.run()

assert not result.success
assert result.states == {
    "produce": TaskInstanceState.SUCCESS,
    "boom": TaskInstanceState.FAILED,
    "consume": TaskInstanceState.UPSTREAM_FAILED,
}
assert isinstance(result.errors["boom"], ValueError)
```

A deferring task settles as `deferred` (and the DagRun stays `running`) unless
`run_triggerer=True` fires its persisted trigger inline. An explicit
`dag_maker.create_dagrun(logical_date=...)` composes: pass it as `dag_maker.run(dag_run)`.

Every task instance is attempted exactly *once* -- `retries` are never re-attempted. A
retry-configured task that fails settles as `up_for_retry`, the DagRun stays `running`, and
a warning names the stranded instances; drop `retries` from Dags under test (or assert
`up_for_retry` deliberately).

## Bulk outcome matchers

`pytest_airflow_in_a_box.matchers` asserts a whole DagRun in one expression. The mapping
must cover every task key, and a mismatch renders a per-task diff:

```python
from pytest_airflow_in_a_box.matchers import failed, succeeded, upstream_failed

assert result == {
    "produce": succeeded(21),
    "boom": failed(ValueError),
    "consume": upstream_failed(),
}
```

`succeeded(xcom=ANY)`, `failed(error_type=None)`, `skipped()`, `deferred()`, and
`upstream_failed()` are the built-in outcomes; `TaskOutcome` builds custom ones.

## Single-task execution

`dag_maker.run_ti` executes exactly one task instance and returns it:

```python
from airflow.utils.state import TaskInstanceState


def test_task(dag_maker):
    with dag_maker():

        @task
        def answer():
            return 42

        answer()

    ti = dag_maker.run_ti("answer")

    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids="answer", session=dag_maker.session) == 42
```

Public task helpers live in `pytest_airflow_in_a_box.taskinstance`: `execute_dag_run`,
`run_task_instance`, `ordered_task_instances`, `run_trigger`, `TaskResolutionError`, and
`TriggerExecutionError`. `run_task_instance` resolves the executable task automatically for
any `dag_maker`-persisted Dag, including task instances queried through a different session
(e.g. the `session` fixture); pass `task=` only for Dags the plugin does not own. The
`DagMaker` protocol additionally exposes `run`, `create_dagrun`, `create_ti`, and `run_ti`.
Passing `map_index` expands a mapped task on demand; upstream-XCom mapping works after its
producer has run in the same DagRun. Passing `run_triggerer=True` runs the persisted trigger
event and resumes a deferred task inline, bounded by `trigger_timeout` seconds.
