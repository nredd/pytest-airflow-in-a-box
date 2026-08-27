# The fidelity ladder

Use the least Airflow machinery that can prove your claim. More fidelity buys real state and
real process boundaries; it also buys migrations, subprocess startup, and more ways for tests
to contend.

| Rung | Use | Database | Proves | Does not prove |
| --- | --- | --- | --- | --- |
| 0 | `render_task`, `task_context` | No | Rendering or hand-driven `execute()` against a Task SDK context | Task state, relations, persistence |
| 1 | `run_task` | No | One operator through the in-process Task SDK runner | Real ORM state or another task |
| 2 | `dag_maker.run_ti` | Yes | One real `TaskInstance`, XCom, mapping, deferral | Ordering or whole-run settlement |
| 3 | `dag_maker.run()` / `run_dag` | Yes | A whole `DagRun`, task relations, executed order | Automatic retries or process re-import |
| 4 | `run_dag(..., executor=...)` | Yes + API | Re-import and execution through a real executor | Executor concurrency |

If a cheaper rung exposes the value you need, stop there.

## One operator, no database

Airflow 3's Task SDK can run an operator without a metadata database. These fixtures share one
in-process runner and differ only in where they stop:

- `render_task(operator, ...)` returns a fresh copy after resolving `template_fields`; it never
  calls `execute()`.
- `task_context(operator, ...)` yields the prepared task, runtime task instance, context, sent
  messages, and captured XComs so the test can call `execute()` itself.
- `run_task(operator, ...)` completes the SDK lifecycle and returns a `TaskRunResult` snapshot.

```python
def test_operator(run_task):
    result = run_task(MyOperator(task_id="answer", value=21))

    assert result.success
    assert result.xcoms["return_value"] == 42
```

Pass `params`, Variables, Connections, `map_index`, `try_number`, and context overrides directly
instead of building ORM rows. An operator with no Dag is bound in place to a deterministic
synthetic Dag; build unbound operators inside the test so that binding does not leak between
tests.

### Rendering template fields without running

Assert against the copy returned by `render_task`, not the original operator:

```python
from pytest_airflow_in_a_box.matchers import rendered

rendered_operator = render_task(operator, params={"value": "42"})

assert rendered(query="SELECT 42") == rendered_operator
```

Use `context_overrides` for keys outside the usual params, Variables, and Connections. Use
`task_context(..., render=False)` when the operator deliberately renders inside `execute()`;
always drive `tc.task`, which is the prepared execution copy.

### Where this rung stops

The runner supplies a faithful Task SDK message loop, not a database in disguise. It does not
provide real ORM rows, cross-task XCom, asset persistence, task dependencies, callbacks,
deferral persistence, or scheduler decisions. Unsupported supervisor calls may resolve to
`None` and fail where the SDK dereferences them. When the assertion needs real state, climb to
`dag_maker.run_ti`.

All three fixtures are Airflow 3.x only. On Airflow 2.x, use `dag_maker.run_ti`.

## One task, real state

`dag_maker.run_ti` executes one task instance against real metadata: a persisted `DagRun`, real
XCom rows, mapped expansion at a chosen `map_index`, and deferral through a `Trigger` row.

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

Pass `run_triggerer=True` to fire and resume a persisted custom trigger. Pass a previously
created `DagRun` when the test needs explicit logical dates or relations between runs. This
rung still executes one instance, so it cannot establish ordering or show how downstream
states settle.

## A whole DagRun, real state

`dag_maker.run()` executes a Dag authored in the test. `run_dag()` adopts a Dag from `dag_bag`
and proves the real file under its real `dag_id`. Both return the same inert `DagRunResult`:

- `success` and final `state`
- per-task `states`, `xcoms`, and captured `errors`
- `order`, the sequence tasks actually executed
- `tis`, `dag_run`, and `result[task_id]` access

Mapped task keys expand as `double[0]`, `double[1]`, and so on. Failure is captured the way the
scheduler captures it: a raising task lands in `errors`, blocked downstream tasks become
`upstream_failed`, and `success` follows Airflow's leaf-task semantics. Assert `not
result.errors` when “no task raised” is the requirement.

The matchers make a whole-run contract readable:

```python
from pytest_airflow_in_a_box.matchers import failed, succeeded, upstream_failed

assert result == {
    "produce": succeeded(21),
    "boom": failed(ValueError),
    "consume": upstream_failed(),
}
```

Also available: `skipped()`, `deferred()`, and `not_run()`. The mapping must cover every task
instance.

### Whole-DagRun execution

Every task instance is attempted exactly once. A retry-configured failure settles
`up_for_retry`; it is not automatically requeued, and the run remains `running`. Use the
[retry recipe](cookbook.md#retry-behavior) when retry behavior itself is the subject, or seed a
synthetic `try_number` with `run_task` for attempt-dependent logic.

`dag_maker` uses deterministic upstream-compatible defaults: a default UTC start date, bare
`run_id="test"`, and the corresponding logical date. Multi-run tests must supply distinct run
IDs and logical dates. The exact constructor and return contracts live in
[Fixtures](../reference/fixtures.md) and the typed protocols in
`pytest_airflow_in_a_box.types`.

### Testing a Dag defined elsewhere

Point `dag_bag` at the repository's Dag folder and pass one of its Dags to `run_dag`:

```python
def test_orders_dag(dag_bag, run_dag):
    result = run_dag(dag_bag.dags["orders"])

    assert result.success
    assert result["extract"].xcom == {"rows": 3}
```

Because the persisted `dag_id` is real, tests running the same Dag concurrently on separate
xdist workers can collide. Group them with `pytest.mark.xdist_group` and run
`--dist loadgroup`; the marker is inert under plain `-n auto`. A serial process is safe because
teardown completes before the next test starts. The database and cleanup model is documented
under [Test Environments](../internals/test-environments.md#the-disposable-metadata-database).

## Executor-driven runs

Pass `executor=` to `run_dag` when the test must prove that a task body survives re-import in a
worker process and that a custom executor round-trips through Airflow's Task Execution API:

```python
result = run_dag(dag, executor="my_company.executors.MyExecutor")
```

The value may be a registered alias, dotted path, executor class, or instance. The plugin
starts its API server lazily and drives instances one at a time in dependency order so
`result.order` remains meaningful.

Three boundaries matter:

- The Dag must come from a file in the configured Dag folder; a test-body `dag_maker` Dag cannot
  be imported by a subprocess.
- `result.states` is authoritative, but `result.errors` is best-effort because exceptions occur
  in the worker. Retain the run's log directory when you need the traceback.
- Serial dispatch does not test the executor's own concurrency.

`run_triggerer=` and `executor=` cannot be combined. A per-instance timeout defaults to 300
seconds and is configured with `--airflow-executor-timeout`. This rung is Airflow 3.x only.

## Off the ladder

[`run_trigger`](deferrable-operators.md) spans the DB-free and real-state rungs: it fires one
trigger, while `run_triggerer=True` resumes a persisted task. The [REST API](rest-api.md) is a
different axis for code that calls a live Airflow endpoint.

[Smoke Tests](smoke-tests.md) vary breadth, not fidelity: they assert properties of the entire
Dag corpus at parse time. Upstream harness compatibility is documented in
[Us vs Them](../internals/tests-common-parity.md).
