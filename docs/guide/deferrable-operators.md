# Deferred tasks

Choose the test by the handoff you need to verify:

| Test | Use | What it proves |
| --- | --- | --- |
| One trigger | `run_trigger(...)` | The trigger emits the expected first event |
| One deferrable operator | `dag_maker.run_ti(..., run_triggerer=True)` | Airflow persists and reconstructs the trigger, then passes its event to the resume method |
| A whole Dag | `dag_maker.run(run_triggerer=True)` or `run_dag(..., run_triggerer=True)` | Resumed tasks and their downstream dependencies settle together |

Use the persisted path for any contract crossing from `defer()` to `execute_complete()`.
Trigger-only tests cannot catch broken serialization or a mismatched resume payload.

## The trigger alone, no database

`run_trigger` drives one trigger's async `run()` to its first `TriggerEvent` on a private event
loop. It needs no triggerer job, `DagRun`, or metadata database:

```python
from piab.taskinstance import run_trigger


def test_trigger_fires():
    event = run_trigger(MyTrigger(value=42), timeout=5.0)

    assert event.payload == {"value": 42}
```

Use this when polling and the event payload are the subject. `cleanup()` always runs, even when
the trigger raises or times out. Completing without an event raises `TriggerExecutionError`
naming the trigger class. `timeout` must be positive and defaults to 10 seconds
(`DEFAULT_TRIGGER_TIMEOUT`).

## The complete defer-and-resume handoff

Pass `run_triggerer=True` to persist the trigger, fire it, submit its first event, and resume the
task through `execute_complete()`:

```python
from airflow.utils.state import TaskInstanceState


def test_deferred_operator_resumes(dag_maker):
    with dag_maker(dag_id="deferred_demo"):
        MyDeferrableOperator(task_id="wait")

    ti = dag_maker.run_ti("wait", run_triggerer=True, trigger_timeout=5.0)

    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.trigger_id is None
    assert ti.xcom_pull(task_ids="wait", session=dag_maker.session) == 42
```

The runner reconstructs the trigger from the persisted `Trigger` row's `classpath` and
`kwargs`; it does not reuse the object created by `execute()`. Dropped constructor arguments,
payload mismatches, and exceptions from the trigger or resume method therefore fail the test.

Without `run_triggerer=True`, a deferring task simply settles `deferred`. For a whole-Dag
assertion, pass the same keyword to `dag_maker.run(...)` or
[`run_dag(...)`](ladder.md#a-whole-dagrun-real-state); the runner resumes each deferring task
before settling downstream states.

## What is not modeled

- **One event and one resume.** The trigger may poll internally until its first event, but later
  events are ignored. If `execute_complete()` defers again, the task remains `deferred`; the
  runner does not fire a second trigger.
- **A production triggerer.** High-availability assignment, multiple triggers sharing an event
  loop, task deferral deadlines, and `TriggerFailureReason` handling remain Airflow's job.
  `trigger_timeout` only bounds how long this test waits for the first event.
- **An executor and triggerer together.** `run_triggerer=True` cannot be combined with
  `executor=`. Use an [executor-driven run](ladder.md#executor-driven-runs) to test the worker
  boundary, or this page's persisted path to test resumption.

For the broader boundary between testing your component and retesting Airflow itself, see
[Whose fail is it anyway?](testing-scope.md#out-of-scope).
