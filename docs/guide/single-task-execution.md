# One task, real state

This is rung 2 on [the fidelity ladder](ladder.md): one task instance against a real
`DagRun` row, real `XCom` table, real mapped expansion at a given `map_index`, real
deferral through a persisted `Trigger` row. Reach for it when an assertion needs real
metadata but not a second task or an ordering -- [a whole `DagRun`](dagrun-execution.md)
is one rung up for that.

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

Passing `map_index` expands a mapped task on demand; upstream-`XCom` mapping works after its
producer has run in the same `DagRun`. Passing `run_triggerer=True` resumes a deferred task
inline ([deferred tasks and your own triggers](deferrable-operators.md)).
`run_ti(session=...)` mirrors upstream `tests_common`'s routing: the supplied session is used
for the task-execution step only, while `DagRun` creation and task-instance selection stay on
`dag_maker.session` exactly as upstream's do.

Public task helpers live in `pytest_airflow_in_a_box.taskinstance`: `execute_dag_run`,
`execute_dag_run_via_executor`, `run_task_instance`, `ordered_task_instances`, `run_trigger`,
`DagRunDriveError`, `TaskResolutionError`, `TriggerExecutionError`, and `ExecutorRunError`.
`run_task_instance` resolves the executable task automatically for any `dag_maker`-persisted
Dag, including task instances queried through a different session (e.g. the `session`
fixture); pass `task=` only for Dags the plugin does not own -- for that whole-`DagRun` case use
[`run_dag`](dagrun-execution.md#testing-a-dag-defined-elsewhere) directly. The `DagMaker` protocol additionally
exposes `run`, `create_dagrun`, `create_ti`, and `run_ti`.
