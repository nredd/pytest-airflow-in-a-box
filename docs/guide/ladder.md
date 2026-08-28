# The fidelity ladder

Choose the lowest rung that exposes the state your assertion needs. Climb only when the test
requires persisted metadata, relationships between tasks, or a worker-process boundary.

| Rung | Use case | Database | What it proves | Primary cost or limit |
| --- | --- | --- | --- | --- |
| 0 | Render or inspect one task | No | Rendering or hand-driven `execute()` in a Task SDK context | No task lifecycle or persisted state |
| 1 | Run one task | No | One operator through the in-process Task SDK lifecycle | No real ORM state or other tasks |
| 2 | Persist one task | Yes | One real `TaskInstance`, including XCom, mapping, and persisted deferral | No ordering or whole-run settlement |
| 3 | Run a whole Dag | Yes | A whole `DagRun`, task relationships, final states, and execution order | No automatic retries or process re-import |
| 4 | Cross the worker boundary | Yes + API | The same run after workers re-import the Dag and execute through a real executor | Serial dispatch; no executor-concurrency coverage |

In practice: use rungs 0-1 for operator logic, rung 2 for one task's Airflow metadata, rung 3
for Dag behavior, and rung 4 only for executor or worker-boundary behavior.

## One operator, no database

Airflow 3's Task SDK can prepare and execute an operator without a metadata database. The
three DB-free fixtures share one in-process runner and stop at different points:

- `render_task(operator, ...)` returns a prepared copy with `template_fields` resolved; it
  never calls `execute()`.
- `task_context(operator, ...)` yields the prepared task, runtime task instance, context,
  supervisor messages, and captured XComs for a test that calls `execute()` itself.
- `run_task(operator, ...)` completes the lifecycle and returns a `TaskRunResult` containing
  state, error, XComs, and supervisor messages. Set `run_callbacks=True` when callbacks or
  listeners are part of the assertion.

Seed `params`, XComs, Variables, Connections, `map_index`, and `try_number` directly instead of
creating ORM rows. An unbound operator is bound in place to a deterministic synthetic Dag, so
construct unbound operators inside the test.

### Rendering template fields without running

Always assert against the copy returned by `render_task`, not the original operator:

```python
rendered_operator = render_task(operator, params={"value": "42"})

assert rendered_operator.query == "SELECT 42"
```

Use `context_overrides` for context keys outside the dedicated seed arguments. Use
`task_context(..., render=False)` when the operator renders inside `execute()`, and always call
the prepared `handle.task` rather than the original operator.

### Where this rung stops

The fake supervisor implements the Task SDK message loop; it does not create ORM rows. These
fixtures cannot prove cross-task XCom, asset persistence, dependencies, deferral persistence,
or scheduler decisions. An unsupported supervisor request may return `None` and fail when the
SDK uses it. Move to `dag_maker.run_ti` when the assertion needs real metadata.

All three fixtures are Airflow 3.x only. On Airflow 2.x, use `dag_maker.run_ti`.

## One task, real state

`dag_maker.run_ti` executes one task instance against the metadata database. Use it when the
assertion needs a persisted `DagRun`, ORM `TaskInstance`, real XCom rows, a selected mapped
instance, or a persisted `Trigger` row.

Pass `run_triggerer=True` to fire and resume one persisted custom trigger. Pass an existing
`DagRun` when logical dates or relationships between runs matter. This rung still executes
only the selected instance: it cannot prove task ordering or how downstream states settle.

## A whole DagRun, real state

Use `dag_maker.run()` for a Dag authored inside the test. Use `run_dag()` for a Dag loaded from
the repository with `dag_bag`. Both persist and execute a real `DagRun`, then return the same
inert `DagRunResult` with:

- final run `state` and `success`;
- per-task states, XComs, errors, and ORM task instances;
- `order`, the sequence of task instances actually executed;
- `dag_run` and `result[task_id]` access.

Mapped instances use keys such as `double[0]`. A raising task is recorded in `errors`, blocked
downstream tasks settle through Airflow's trigger-rule semantics, and `success` follows
Airflow's leaf-task rule. Assert `not result.errors` when the contract is specifically that no
task body raised.

Outcome matchers keep a complete run contract compact:

```python
from pytest_airflow_in_a_box.matchers import failed, succeeded, upstream_failed

assert result == {
    "produce": succeeded(21),
    "boom": failed(ValueError),
    "consume": upstream_failed(),
}
```

The mapping must cover every task instance. `skipped()`, `deferred()`, and `not_run()` are also
available.

### Whole-DagRun execution

Each task instance receives at most one attempt. A retry-configured failure settles
`up_for_retry`, is not requeued, and leaves the run non-terminal. Use the
[retry recipe](cookbook.md#retry-behavior) to drive another attempt explicitly, or seed
`try_number` with `run_task` when only attempt-dependent logic matters.

`dag_maker` supplies deterministic defaults, including a UTC start date and `run_id="test"`.
Give multi-run tests distinct run IDs and logical dates. See [Fixtures](../reference/fixtures.md)
for the full signatures and return contracts.

The lower-level helpers in `pytest_airflow_in_a_box.taskinstance` expose four public failures:
`DagRunDriveError` is the driver base class, `TaskResolutionError` means a runnable task could
not be resolved, `TriggerExecutionError` means a persisted trigger failed, and
`ExecutorRunError` means executor startup, dispatch, or settlement failed.

### Testing a Dag defined elsewhere

Load the repository once with `dag_bag`, then pass the selected Dag to `run_dag`:

```python
def test_orders_dag(dag_bag, run_dag):
    result = run_dag(dag_bag.dags["orders"])

    assert result.success
    assert result.order == ["extract", "load"]
```

`run_dag` persists the Dag under its real `dag_id`. Tests that run the same ID concurrently on
different xdist workers can collide, surfacing later as `DagPersistenceError`,
`DagRunCreationError`, or a task-resolution failure. Group them with
`pytest.mark.xdist_group` and use `--dist loadgroup`. The marker has no effect under plain
`-n auto`. Serial execution is safe because teardown completes before the next test starts.

## Executor-driven runs

Pass `executor=` only when the test must prove that workers can re-import the Dag and round-trip
workloads through Airflow's Task Execution API:

```python
result = run_dag(dag, executor="my_company.executors.MyExecutor")
```

The value may be a registered alias, dotted path, executor class, or instance. The plugin
starts the API server lazily and dispatches instances one at a time in dependency order.

Keep these boundaries explicit:

- The Dag must come from a file inside the configured Dag folder. A `dag_maker` Dag defined in
  the test cannot be re-imported by a worker.
- `result.states` is authoritative. `result.errors` is best-effort because worker exceptions
  live primarily in the retained task logs.
- Serial dispatch does not exercise the executor's concurrency.

`run_triggerer=` and `executor=` cannot be combined. The per-instance timeout defaults to 300
seconds and is configured with `--airflow-executor-timeout`. Executor-driven runs require
Airflow 3.x.

## Off the ladder

[`run_trigger`](deferrable-operators.md) executes one trigger without a database;
`run_triggerer=True` resumes a persisted deferred task. The [REST API](rest-api.md) is a
separate axis for code that calls a live Airflow endpoint. [Smoke Tests](smoke-tests.md) expand
breadth rather than fidelity by checking the entire Dag corpus at parse time.
