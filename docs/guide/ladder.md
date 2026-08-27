# The fidelity ladder

Deciding how to run one unit of your Dag code is a cost question, not a taste question. Each
rung buys a class of assertion the rung below structurally cannot make, and charges for it in
setup, runtime, and blast radius.

The climbing rule, one sentence: **stand on the lowest rung that can still fail for the reason
you care about.** If the test would fail identically one rung down, you are paying for fidelity
you are not asserting on.

| Rung | Fixture | Metadata DB | Section |
| --- | --- | --- | --- |
| 0 | `render_task` | no | [One operator, no database](#one-operator-no-database) |
| 1 | `run_task` / `task_context` | no | [One operator, no database](#one-operator-no-database) |
| 2 | `dag_maker` + `run_ti` | yes | [One task, real state](#one-task-real-state) |
| 3 | `dag_maker.run()` / `run_dag` | yes | [A whole DagRun, real state](#a-whole-dagrun-real-state) |
| 4 | `run_dag(..., executor=...)` | yes | [Executor-driven runs](#executor-driven-runs) |

## One operator, no database

**Rung 0 -- `render_task`.** Proves: your `template_fields` resolve to the string you expect.
Costs: nothing -- no DB, no `execute()`, no Airflow ORM import. Cannot prove: anything about the
operator body -- `render_task` never calls `execute()`.

**Rung 1 -- `run_task` / `task_context`.** Proves: your `execute()` runs against a real
`RuntimeTaskInstance`, with real template rendering and a real `airflow.sdk.get_current_context()`.
Retry *classification* is reachable only here, via `try_number=` plus the operator's own
`retries`. Costs: the Task SDK in-process runner, Airflow 3.x only. Cannot prove: anything
involving a second task or real metadata -- [where this rung stops](#where-this-rung-stops)
itemizes the gaps.

The usual way to test one operator is to build a `dict` context, stub `ti` with a mock, and
call `op.execute(context)`. That harness gets three things wrong, and two of them stay green.

**1. Template fields are never rendered.** Rendering happens in the task runner, not in
`execute()`. A hand-rolled call hands your operator the literal string, so an operator whose
`sql="SELECT {{ ds }}"` -- which ships `{{ ds }}` to the warehouse -- passes a test that
asserts on the same literal. `run_task`, `render_task`, and `task_context` render exactly like
a real run -- `"{{ 21 * 2 }}"` arrives at `execute()` as `"42"`.

**2. There is no active `get_current_context()`.** Passing a context as an argument does not
publish it. Any code the operator calls that reaches for
`airflow.sdk.get_current_context()` raises `RuntimeError: Current context was requested but no
context was found!`, which reads like a harness bug, so it gets patched -- and from then on the
ambient context is whatever the test typed. Here it is the real one, published by the Task SDK.

**3. A mock `ti` answers every question wrong.** Every attribute is another mock: truthy under
`if`, unequal to everything under `==`. `if ti.try_number:` takes the retry branch on the first
attempt, `ti.try_number == 1` is `False`, and `ti.try_number > 1` raises `TypeError`. In
`task_context` and `run_task`, `context["ti"]` is a real `RuntimeTaskInstance` -- the exact
class Airflow uses at runtime -- and `try_number=` selects a real integer attempt.

All three are pinned in `tests/fixtures/test_task_context.py`.

### Why not something else

- `dag.test()` -- see [why not `dag.test()`](../why/index.md#dagtest)
- Airflow's own `tests_common` harness targets core and provider development, not Dag-author
  code in your repo. See [Deciding which failures are yours](testing-scope.md)
- Hand-rolling the runner means answering eight Task SDK supervisor message types and tracking
  `airflow.sdk.api.datamodels._generated` across minors. That is what
  [`_compat/`](../internals/compat-layer.md) is for

### Running one operator

`run_task` executes one operator through the Task SDK in process, with no metadata database.
`XCom`, Variable, and Connection traffic is answered from seeded dictionaries; unseeded lookups
fail exactly like a live deployment, with a hint naming the seeding keyword. Task callbacks and
listeners stay silent unless the call passes `run_callbacks=True`.

```python
def test_operator(run_task):
    result = run_task(
        my_operator,
        variables={"answer": "42"},
        connections={"db": {"conn_type": "postgres", "host": "example.com"}},
    )

    assert result.state == TaskInstanceState.SUCCESS
    assert result.xcoms["return_value"] == "expected"
```

`TaskRunResult` carries `state`, `error`, `xcoms`, `msg`, and `sent`.

`try_number=` selects the synthetic attempt, and the operator's own `retries` decides whether a
failure classifies as `UP_FOR_RETRY` and fires its retry callback -- retry classification is
reachable only on this rung.

### Where this rung stops

`run_task` cannot prove anything that needs a second task or real metadata:

- `XCom` is keyed by `XCom` key **alone**. There is no `task_ids`, `run_id`, or `map_index`
  scoping, so a green `xcom_pull` does not prove data flowed from A to B
- Asset inlet/outlet validation is rubber-stamped: `ValidateInletsAndOutlets` always answers
  `inactive_assets=[]`, so an inactive asset never shows up here
- Comms-backed `RuntimeTaskInstance` statics the fake supervisor does not answer (e.g.
  `get_dagrun_state`, `get_dr_count`) resolve to `None`, which the SDK then dereferences --
  expect an `AttributeError`, not a clean `None`

When an assertion needs any of those, climb to [`dag_maker.run_ti`](#one-task-real-state).

On Airflow 2.x all three fixtures fail with an actionable error -- the Task SDK in-process
runner is a 3.x thing; use [`dag_maker.run_ti`](#one-task-real-state) there.

### Operators without a Dag

An operator never bound to any Dag works as-is -- no `dag=DAG(...)` boilerplate:

```python
def test_floating_operator(run_task):
    operator = BashOperator(task_id="my_task", bash_command="echo {{ dag.dag_id }}")

    result = run_task(operator)

    assert result.state == TaskInstanceState.SUCCESS
```

The same goes for a standalone `@task`: calling the decorated function outside any Dag
returns an XComArg, and its `.operator` runs directly:

```python
from airflow.sdk import task


@task
def add(x: int, y: int) -> int:
    return x + y


def test_add(run_task):
    result = run_task(add(1, 2).operator)

    assert result.xcoms["return_value"] == 3
```

The Task SDK requires every executing task to have a bound Dag, so `run_task`, `render_task`,
and `task_context` bind an unbound operator IN PLACE to a synthetic
`DAG(dag_id=..., schedule=None)`. Pass `dag_id="..."` to name it, or leave it off for a
deterministic, xdist-safe identifier derived from the test's nodeid, visible as
`{{ dag.dag_id }}` in templates and as
`operator.dag.dag_id` after the call. The synthetic Dag is stamped with the test module's
location, so `template_ext` files (`.sql`, `.sh`, ...) resolve next to the test.

Two footguns follow from binding in place:

- A bound operator is never rebound (the SDK forbids it). Repeated calls reuse the first
  binding, and a later call passing a *different* explicit `dag_id` fails loudly rather than
  half-applying
- Build unbound operators inside the test body. A module-level operator shared across tests
  carries the first test's binding into the others

### Rendering template fields without running

`render_task` shares `run_task`'s machinery but stops after resolving `template_fields` -- it
never calls `execute()`. Use it when a test only needs an operator's resolved attributes. It
renders onto a fresh copy and returns that copy, so a Dag built once and reused across tests
renders independently every call. Always assert against the return value, and note the
`rendered(...)` matcher goes on the LEFT (its docstring says why):

```python
from airflow.sdk import DAG

from pytest_airflow_in_a_box.matchers import rendered


def test_operator_renders_its_query(render_task):
    with DAG(dag_id="probe_dag", schedule=None) as dag:
        MyOperator(task_id="probe", query="SELECT {{ params.value }}")

    rendered_operator = render_task(dag.get_task("probe"), params={"value": "42"})

    assert rendered(query="SELECT 42") == rendered_operator
```

`context_overrides` merges extra keys into the synthesized template context before rendering,
for fields referencing something the usual `params`/`variables`/`connections` seeding does not
cover.

### Getting the raw return value of execute()

`task_context` prepares the same runner and then hands control back, so `execute()` and
`post_execute()` can be driven by hand. Its job is the value `execute()` *returns* -- which
`run_task` pushes to `XCom` and never shows you:

```python
def test_operator_execute_result(task_context):
    with task_context(my_operator, params={"value": 42}) as tc:
        result = tc.task.execute(tc.context)

    assert result.exit_code == 0
    assert tc.ti.log_url.startswith("http")
```

Also reach for it when an operator renders its templates from *inside* `execute()`, or when
application code reads incidental attributes off `context["ti"]`. Since the test drives
`execute()` itself, `tc.xcoms` reflects only explicit `context["ti"].xcom_push(...)` calls.

Always drive `tc.task`, not the operator you passed in: preparation happens on a
`prepare_for_execution()` copy, and an in-execute `render_templates()` renders `ti.task`, which
must be the object whose `execute()` is running. For a mapped operator, `tc.task` is the
concrete unmapped instance for `map_index` -- but only with the default `render=True`, since
Airflow unmaps inside `render_template_fields`. A mapped operator with `render=False` raises
`ValueError`.

Template fields are pre-rendered like a real run by default. Pass `render=False` for the
deferred-rendering pattern:

```python
def test_deferred_rendering(task_context):
    with task_context(my_operator, render=False) as tc:
        result = tc.task.execute(tc.context)  # execute() renders via context["ti"]
```

The fake supervisor is installed only inside the `with` block. `tc.xcoms` and `tc.sent` stay
readable after exit; `tc.ti` and `tc.context` do not outlive it.

## One task, real state

**Rung 2 -- `dag_maker` + `run_ti`.** Proves: one task instance against real metadata -- real
`DagRun` row, real `XCom` table, real mapped expansion at a given `map_index`, real deferral
through a persisted `Trigger` row. Costs: a lazy DB migration on first request, plus an
authored Dag in the test body. Cannot prove: ordering, or how a run settles -- one instance is
one instance. Reach for it when an assertion needs real metadata but not a second task or an
ordering -- [a whole `DagRun`](#a-whole-dagrun-real-state) is one rung up for that.

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
[`run_dag`](#testing-a-dag-defined-elsewhere) directly. The `DagMaker` protocol additionally
exposes `run`, `create_dagrun`, `create_ti`, and `run_ti`.

## A whole DagRun, real state

**Rung 3 -- `dag_maker.run()` / `run_dag`.** Proves: `result.order`, `result.states` including
`upstream_failed` propagation, and mid-run mapped expansion. `run_dag` additionally proves your
*real* file in `dags/` does this, under its real `dag_id`. Costs: with `run_dag` the real
`dag_id` becomes a shared metadata key, so two `pytest-xdist` workers running the same Dag can
tear each other's metadata down -- see [the xdist caveat](#testing-a-dag-defined-elsewhere).
Cannot prove: retries -- every instance is attempted exactly once
([whole-`DagRun` execution](#whole-dagrun-execution) has the consequences).

### Whole-`DagRun` execution

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
[`run_task(..., try_number=)`](#running-one-operator).

#### Deterministic run defaults

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

### Testing a Dag defined elsewhere

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

### Bulk outcome matchers

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

## Executor-driven runs

**Rung 4 -- `executor=`.** Proves: your task body survives re-import in a subprocess, and your
executor round-trips through a live Task Execution API. `dag.test(use_executor=True)` cannot
reach this rung -- see [why not `dag.test()`](../why/index.md#dagtest). Costs: the Dag must be
a file in your Dag folder, `result.errors` degrades to best-effort, and each instance carries a
timeout, all three below in full. Cannot prove: an executor's own concurrency -- instances are
dispatched one at a time to keep `result.order` meaningful.

Pass `executor=` to run your task bodies through a real Airflow executor instead of the pytest
process itself -- workloads queued, heartbeats pumped, task bodies executing in supervised
worker subprocesses:

```python
def test_orders_dag_through_our_executor(dag_bag, run_dag):
    dag = dag_bag.dags["orders"]

    result = run_dag(dag, executor="my_company.executors.MyExecutor")

    assert result.success
```

`executor=` takes an alias registered through
[`airflow_components.executor`](custom-components-wiring.md#runtime-component-registration), a dotted
import path, a `BaseExecutor` subclass, or an instance you built yourself. The api-server the
workers report to starts lazily on the first executor-driven call, exactly as `api_client`
starts it, and `--apps core,execution` means it serves `/execution` as well as `/api/v2`.

This plugin already ships a live api-server, so pointing an executor at it is the whole trick.

The result is the same `DagRunResult`, with the same ordering, mapped-task expansion, and
final-state semantics -- both paths share one driver. Three things differ, all of them inherent
to tasks running in another process:

- **The Dag must be a file inside your Dag folder.** Each task is re-imported from that file
  in a worker subprocess, so a `dag_maker` Dag -- defined in a test body -- can never qualify
  and is refused by name, before any metadata is written. Use `dag_bag`.
- **`result.errors` is best-effort.** A task's exception is raised inside the worker, so only
  what the executor itself attaches to a failure reaches your test. `result.states` stays
  authoritative, and the traceback is in the worker's log under the run's logs folder --
  [`--airflow-home-retention=all`](../internals/test-environments.md#the-isolated-airflow_home) keeps it around.
- **Instances are dispatched one at a time**, in dependency order, so an executor's own
  concurrency is not exercised. This is what keeps `result.order` meaningful.

`run_triggerer=` cannot be combined with `executor=`: resuming a deferred task is a
triggerer's job, and an executor-driven run leaves a deferring instance `deferred`.

An instance that never reaches a final state fails the run naming the stuck task, rather than hanging.
`--airflow-executor-timeout` (or the `airflow_executor_timeout` ini option) sets that budget
per instance; it defaults to 300 seconds, which is generous for a worker subprocess that has
to start up and parse a Dag file.

On Airflow 2.x `executor=` fails with an actionable error (the Task Execution API is an
AIP-72 / 3.x thing); drop it and the in-process path works there unchanged.

Writing an executor to test is easier than it sounds -- Airflow 3 removed `SequentialExecutor`
from core, so a serial one is about fifteen lines. See
[Custom components](custom-components-execution.md#a-worked-executor) for the whole thing.

## Off the ladder

Two tools are not fidelity increments:

- [`run_trigger`](deferrable-operators.md) -- defer, fire, resume. Spans rungs 1 and 2;
  that page lists what is not modeled
- [The live REST API](rest-api.md) -- a different thing entirely, for code *you* wrote that
  resolves `conf.get("api", "base_url")` or calls `/api/v2`

## Corpus checking is a different axis

The ladder varies fidelity over one unit of code. [Smoke checks](smoke-tests.md),
[per-file collection](smoke-tests.md#one-pytest-item-per-dag-file), and [Dag coverage](smoke-tests.md#dag-coverage) vary *breadth*
over every unit at fixed parse-only fidelity, asserting whole-corpus properties no single-Dag
test can phrase at any rung.

## Coming from upstream's `tests_common`?

`dag_maker` also accepts upstream's harness keywords, exposes scheduler-side handles, and
ships the `create_task_instance` / `create_dummy_dag` one-call factories. The call-site
parity contract lives in [Us vs Them](../internals/tests-common-parity.md).
