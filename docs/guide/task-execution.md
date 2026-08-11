# Task execution

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
`ordered_task_instances`, `run_trigger`, `TaskResolutionError`, and `TriggerExecutionError`.
`run_task_instance` resolves the executable task automatically for any `dag_maker`-persisted Dag,
including task instances queried through a different session (e.g. the `session` fixture); pass
`task=` only for Dags the plugin does not own. The
`DagMaker` protocol additionally exposes `create_dagrun`, `create_ti`, and `run_ti`. Passing
`map_index` expands a mapped task on demand; upstream-XCom mapping works after its producer has run
in the same DagRun. Passing `run_triggerer=True` runs the persisted trigger event and resumes a
deferred task inline, bounded by `trigger_timeout` seconds.
