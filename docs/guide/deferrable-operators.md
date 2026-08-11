# Deferrable operators

`run_trigger` drives one trigger's async `run()` to its first `TriggerEvent` on a private event
loop, with no triggerer job, DagRun, or metadata database. `cleanup()` always runs, and a trigger
that never fires raises `TriggerExecutionError` instead of hanging the suite.

```python
from pytest_airflow_in_a_box.taskinstance import run_trigger


def test_trigger_fires():
    event = run_trigger(MyTrigger(target=42), timeout=5.0)

    assert event.payload == {"value": 42}
```

Compose the two halves to cover defer -> fire -> resume in one test:

```python
def test_operator_resumes(dag_maker):
    with dag_maker():
        MyDeferrableOperator(task_id="wait")

    ti = dag_maker.run_ti("wait", run_triggerer=True, trigger_timeout=5.0)

    assert ti.state == TaskInstanceState.SUCCESS
```
