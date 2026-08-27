# A whole DagRun, real state

This is rung 3 on [the fidelity ladder](ladder.md): every task instance in a `DagRun`
executed in dependency order, with real ordering, real state propagation, and
`upstream_failed` propagation a single task instance can't exercise. Reach for it when
the assertion needs a second task or an ordering that only a scheduler produces.

## Whole-`DagRun` execution

For a Dag authored directly in the test, rather than loaded from your `dags/` folder,
`dag_maker.run()` creates the `DagRun` (when you did not) and runs it to completion, returning
an inert `DagRunResult` snapshot:

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

Failure is captured the way the scheduler captures it: a raising task body lands in
`result.errors`, blocked downstreams end as `upstream_failed`, and with default trigger rules
`result.success` reports `False` -- testing an intentional-failure Dag needs no extra flag.
`success` mirrors Airflow's leaf-task `DagRun` semantics, so a failure absorbed by an
`all_done`-style leaf still counts as `success`; assert `not result.errors` for "no task
raised":

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

A deferring task settles as `deferred` (and the `DagRun` stays `running`) unless
[`run_triggerer=True`](deferrable-operators.md) fires its persisted trigger inline. An explicit
`dag_maker.create_dagrun(logical_date=...)` composes: pass it as `dag_maker.run(dag_run)`.

Every task instance is attempted exactly *once* -- `retries` are never re-attempted. A
retry-configured task that fails lands in `up_for_retry`, the `DagRun` stays `running`, and
a warning names the stranded instances; drop `retries` from Dags under test, or assert
`up_for_retry` on purpose. Retry *classification* is reachable one rung down, through
[`run_task(..., try_number=)`](db-free-execution.md#running-one-operator).

### Deterministic run defaults

`dag_maker` follows upstream `tests_common`'s deterministic defaults
([ADR 0003](../adr/0003-dag-maker-run-default-parity.md)):

- Every Dag gets a default `start_date` -- explicit kwarg, then
  `default_args["start_date"]`, then the test module's `DEFAULT_DATE` attribute, then the
  `2016-01-01` UTC epoch -- exposed as `dag_maker.start_date`. Pass an explicit
  `start_date=None` to opt out
- A bare `create_dagrun()` gets `run_id="test"` and `logical_date=dag_maker.start_date`,
  so `session.query(DagRun).filter_by(run_id="test")` finds it. A second bare call on one
  Dag collides on `(dag_id, "test")` and fails loudly, exactly as upstream -- and because
  runs are also unique per `(dag_id, logical_date)`, multi-run tests need distinct
  `logical_date`s alongside explicit `run_id`s
- Passing `run_type=` (even a manual one) switches the default `run_id` to the
  timetable-generated `manual__...` / `scheduled__...` form; non-manual run types derive
  the default `logical_date` from `next_dagrun_info` and infer an automated (rather than
  manual) `data_interval`

`run_dag` keeps its derived `manual__pytest-airflow-in-a-box-...` ids and current-UTC
dating: it adopts externally-authored Dags whose `start_date` the plugin does not control.

## Testing a Dag defined elsewhere

Point `dag_bag` at your repo's Dag folder -- via `--dag-folder=PATH` or the
`airflow_dags_folder` ini option -- grab a Dag by id, and drive it with `run_dag`:

```python
def test_orders_dag(dag_bag, run_dag):
    dag = dag_bag.dags["orders"]

    result = run_dag(dag)

    assert result.success
    assert result["extract"].xcom == {"rows": 3}
```

`run_dag` persists the Dag, creates a manual `DagRun`, and runs it the same way `dag_maker.run()`
does (see above), returning the same `DagRunResult` snapshot -- keyed on the Dag's own `dag_id`,
not a synthetic one, so `result.dag_id` matches what your real Dag declares.
`--dag-folder`/`airflow_dags_folder` is a different option from
`--collect-dag-folder`/`airflow_collect_dags_folder`, which drives
[Dag-file collection](smoke-tests.md#one-pytest-item-per-dag-file) instead. The `airflow_dags_folder` fixture returns
whichever directory that ladder resolved, as a `pathlib.Path`, when a test needs the folder
itself rather than the parsed bag.

Because the persisted `dag_id` is the real one, two `pytest-xdist` workers running the same
`dag_id` at the same time race on the shared metadata database, and the race does not fail
cleanly: if both pass the absence check before either commits, they silently share one
bundle/`DagModel` row, and whichever tears down first deletes metadata the other worker's
still-running test depends on. It shows up later as a `DagPersistenceError`,
`DagRunCreationError`, or a task-resolution failure with no obvious link back to the race.

The only mitigation is keeping such tests on one worker with `pytest.mark.xdist_group` -- which
is **inert outside `--dist loadgroup`**. Under `-n auto` alone, the marker does nothing. A
single worker process never hits this: one test's metadata is fully cleaned up before the next
test's setup runs, and re-registering a `dag_id` the process itself persisted replaces the
leftover instead of raising. On an xdist worker a leftover is indistinguishable from another
worker's live row, so the guard stays loud there.

The window already exists for `dag_maker(dag_id="fixed")` with an explicit pinned id. `run_dag`
just makes a fixed real `dag_id` the only mode, which is why it is called out here.

What cleanup will *not* do, since it is the obvious next worry: it never deletes a row by
primary key alone. Airflow's `dag_run.id` is a plain `Integer` primary key with no
`sqlite_autoincrement`, so on SQLite it is a rowid alias and the value is reused as soon as
the highest row is deleted -- on a database every worker shares, an id one test owned can
name another worker's live run by the time teardown runs. Per-Dag cleanup therefore
re-checks `dag_id` on every run it deletes, and seeded Variables and Connections are matched
on their `key`/`conn_id` as well as their id. Distinct identifiers are still yours to pick;
this only guarantees that *unrelated* tests cannot delete each other.

## Bulk outcome matchers

`pytest_airflow_in_a_box.matchers` asserts a whole `DagRun` in one expression. The mapping
must cover every task key, and a mismatch renders a per-task diff:

```python
from pytest_airflow_in_a_box.matchers import failed, succeeded, upstream_failed

assert result == {
    "produce": succeeded(21),
    "boom": failed(ValueError),
    "consume": upstream_failed(),
}
```

`succeeded(xcom=ANY)`, `failed(error_type=None)`, `skipped()`, `deferred()`,
`upstream_failed()`, and `not_run()` (an instance that never ran, e.g. blocked behind a
still-deferred upstream) are the built-in outcomes; `TaskOutcome` builds custom ones. Mapped
instances address as `"double[0]"` keys or `("double", 0)` tuples.
