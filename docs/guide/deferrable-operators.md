# Deferred tasks and your own triggers

Rename a key in your trigger's `TriggerEvent` payload and an ordinary suite stays green:

- The `DagBag` test passes. The file still imports
- Calling `task.execute(context)` passes. `self.defer(...)` raises `TaskDeferred` before your
  `execute_complete` is ever reached, so the resume half is never executed
- A plain `dag_maker` run passes. The instance settles `deferred`, nothing resumes it, and no
  error is recorded

The triggerer fires the real event in prod hours later and `execute_complete` raises
`KeyError: 'value'`.

`run_triggerer=True` is the assertion that catches it. It fires the *persisted* trigger and
feeds the event back into the deferred instance, so `execute_complete` actually runs:

```python
import pytest
from airflow.utils.state import TaskInstanceState


@pytest.mark.db_test
def test_deferred_operator_resumes(dag_maker):
    with dag_maker(dag_id="deferred_demo"):
        MyDeferrableOperator(task_id="wait")

    ti = dag_maker.run_ti("wait", run_triggerer=True, trigger_timeout=5.0)

    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.trigger_id is None
    assert ti.xcom_pull(task_ids="wait", session=dag_maker.session) == 42
```

A `KeyError` in `execute_complete` propagates out of `run_ti` and fails the test. Same keyword
on `dag_maker.run(...)` and [`run_dag(...)`](ladder.md#a-whole-dagrun-real-state), which resume every deferring
instance in the run.

This also exercises your trigger's *serialization*: the resume rehydrates the trigger from the
persisted `Trigger` row's `classpath` and `kwargs`, not from the object your `execute` built.
A `serialize()` that drops a constructor argument fails here. That is the "your own component"
carve-out in [What to test](testing-scope.md#out-of-scope).

## The trigger alone, no database

`run_trigger` drives one trigger's async `run()` to its first `TriggerEvent` on a private event
loop -- no triggerer job, no `DagRun`, no metadata database:

```python
from pytest_airflow_in_a_box.taskinstance import run_trigger


def test_trigger_fires():
    event = run_trigger(MyTrigger(value=42), timeout=5.0)

    assert event.payload == {"value": 42}
```

`cleanup()` always runs, including on timeout and on a raising trigger. A trigger that emits
nothing raises `TriggerExecutionError` naming the trigger class instead of hanging the suite.
`timeout` defaults to 10 seconds (`DEFAULT_TRIGGER_TIMEOUT`).

## What is not modeled

- **Single-shot.** `run_trigger` takes the *first* event only, and the resume runs exactly
  once. A task that defers a second time comes back `deferred`, with no error and no second
  trigger run. A poll-loop trigger -- one that yields nothing until the Nth iteration -- is not
  modeled; test its loop directly with `run_trigger` and a trigger constructed to fire
- **No triggerer semantics.** Trigger timeouts, `TriggerFailureReason`, high-availability
  assignment, and multiple triggers sharing one loop are the triggerer's job, not this
- **`run_triggerer=` cannot be combined with `executor=`.** See
  [executor-driven runs](ladder.md#executor-driven-runs)

## Why not `dag.test(run_triggerer=True)`

[Why not `dag.test()`](../why/index.md#dagtest) covers the general case. The trigger-specific
part: upstream runs the trigger as a bare `asyncio.run(anext(trigger.run(), None))` -- no
timeout, no `cleanup()`, and the resume exception is logged, not raised. Here the trigger is
bounded, `cleanup()` is guaranteed, and the failure is an exception in your test.
