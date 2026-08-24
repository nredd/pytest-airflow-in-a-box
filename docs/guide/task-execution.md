# Task execution

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

`run_dag` persists the Dag, creates a manual DagRun, executes every task instance in
dependency order, and returns the same `DagRunResult` snapshot `dag_maker.run()` does (see
below) -- keyed on the Dag's own `dag_id`, not a synthetic one, so `result.dag_id` matches
what your real Dag declares. `--dag-folder`/`airflow_dags_folder` is a different option from
`--collect-dag-folder`/`airflow_collect_dags_folder`, which drives
[Dag-file collection](dag-collection.md) instead -- see
[the two Dag folder options](dag-coverage.md#footguns) if you're wiring both up. The
`airflow_dags_folder` fixture returns whichever directory that ladder resolved, as a
`pathlib.Path`, when a test needs the folder itself rather than the parsed bag.

Because the persisted `dag_id` is the real one, running the same `dag_id` through `run_dag`
from two different tests scheduled onto different `pytest-xdist` workers at the same time can
race on the shared metadata database. That race is not guaranteed to fail cleanly: if both
workers pass the absence check before either commits, they silently share one bundle/`DagModel`
row instead, and whichever worker tears down first deletes metadata the other worker's
still-running test depends on -- surfacing later as a `DagPersistenceError`,
`DagRunCreationError`, or a task-resolution failure with no obvious link back to the race. Keep
such tests on one worker (e.g. `pytest.mark.xdist_group`) or accept that they run serially; a
single worker process never hits this, since one test's metadata is fully cleaned up before the
next test's setup runs -- and on a serial run, even when a cleanup leaks rows, re-registering a
`dag_id` the process itself persisted earlier replaces the leftover instead of raising (on an
xdist worker a leftover is indistinguishable from another worker's live row, so the guard stays
loud there). This window
already exists for `dag_maker(dag_id="fixed")` with an explicit pinned id -- `run_dag` just makes a fixed real `dag_id` the only mode, which is why it
is called out here.

## Executor-driven runs

Everything above runs your task bodies in the pytest process. Pass `executor=` to run them
through a real Airflow executor instead -- workloads queued, heartbeats pumped, task bodies
executing in supervised worker subprocesses that report back to a live Task Execution API:

```python
def test_orders_dag_through_our_executor(dag_bag, run_dag):
    dag = dag_bag.dags["orders"]

    result = run_dag(dag, executor="my_company.executors.MyExecutor")

    assert result.success
```

`executor=` takes an alias registered through
[`airflow_components.executor`](custom-components.md#runtime-component-registration), a dotted
import path, a `BaseExecutor` subclass, or an instance you built yourself. The api-server the
workers report to starts lazily on the first executor-driven call, exactly as `api_client`
starts it, and `--apps core,execution` means it serves `/execution` as well as `/api/v2`.

This is the piece upstream cannot offer. `dag.test(use_executor=True)` queues real workloads,
but nothing in a test process serves the `/execution` API they need
([apache/airflow#59074](https://github.com/apache/airflow/issues/59074)); this plugin already
ships a live api-server, so pointing an executor at it is the whole trick. Nothing here is
built on `dag.test`, which is mid-move upstream
([#61803](https://github.com/apache/airflow/issues/61803),
[#54658](https://github.com/apache/airflow/issues/54658)).

The result is the same `DagRunResult`, with the same ordering, mapped-task expansion, and
settling semantics -- both paths share one driver. Three things differ, all of them inherent
to tasks running in another process:

- **The Dag must be a file inside your Dag folder.** Each task is re-imported from that file
  in a worker subprocess, so a `dag_maker` Dag -- defined in a test body -- can never qualify
  and is refused by name, before any metadata is written. Use `dag_bag`.
- **`result.errors` is best-effort.** A task's exception is raised inside the worker, so only
  what the executor itself attaches to a failure reaches your test. `result.states` stays
  authoritative, and the traceback is in the worker's log under the run's logs folder --
  [`--airflow-home-keep`](airflow-home.md) will keep it around.
- **Instances are dispatched one at a time**, in dependency order, so an executor's own
  concurrency is not exercised. This is what keeps `result.order` meaningful.

`run_triggerer=` cannot be combined with `executor=`: resuming a deferred task is a
triggerer's job, and an executor-driven run settles a deferring instance as `deferred`.

An instance that never settles fails the run naming the stuck task, rather than hanging.
`--airflow-executor-timeout` (or the `airflow_executor_timeout` ini option) sets that budget
per instance; it defaults to 300 seconds, which is generous for a worker subprocess that has
to start up and parse a Dag file.

Airflow 3.x only. `queue_workload`, `workloads.ExecuteTask` and the Task Execution API all
arrived with AIP-72, so on the 2.x family `executor=` fails with an actionable error; drop it
and the in-process path works there unchanged.

Writing an executor to test is easier than it sounds -- Airflow 3 removed `SequentialExecutor`
from core, so a serial one is about fifteen lines. See
[Custom components](custom-components.md#a-worked-executor) for the whole thing.

## Whole-DagRun execution

For a Dag authored directly in the test, rather than loaded from your `dags/` folder,
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
downstreams settle as `upstream_failed`, and with default trigger rules `result.success`
reports `False` -- testing an intentional-failure Dag needs no extra flag. `success` mirrors
Airflow's leaf-task DagRun semantics, so a failure absorbed by an `all_done`-style leaf still
settles `success`; assert `not result.errors` for "no task raised":

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

`succeeded(xcom=ANY)`, `failed(error_type=None)`, `skipped()`, `deferred()`,
`upstream_failed()`, and `not_run()` (an instance that never ran, e.g. blocked behind a
still-deferred upstream) are the built-in outcomes; `TaskOutcome` builds custom ones. Mapped
instances address as `"double[0]"` keys or `("double", 0)` tuples.

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
`execute_dag_run_via_executor`, `run_task_instance`, `ordered_task_instances`, `run_trigger`,
`TaskResolutionError`, `TriggerExecutionError`, and `ExecutorRunError`. `run_task_instance` resolves the executable task automatically for
any `dag_maker`-persisted Dag, including task instances queried through a different session
(e.g. the `session` fixture); pass `task=` only for Dags the plugin does not own -- see
[Testing a Dag defined elsewhere](#testing-a-dag-defined-elsewhere) for `run_dag`, which
covers that whole-DagRun case directly. The `DagMaker` protocol additionally exposes `run`,
`create_dagrun`, `create_ti`, and `run_ti`. Passing `map_index` expands a mapped task on
demand; upstream-XCom mapping works after its producer has run in the same DagRun. Passing
`run_triggerer=True` runs the persisted trigger event and resumes a deferred task inline,
bounded by `trigger_timeout` seconds. `run_ti(session=...)` mirrors upstream
`tests_common`'s routing: the supplied session is used for the task-execution step only,
while DagRun creation and task-instance selection stay on `dag_maker.session` exactly as
upstream's do.

## Upstream harness keywords

`dag_maker(...)` forwards unknown keyword arguments to the authoring `DAG` constructor, with
three exceptions mirroring upstream `tests_common`'s `dag_maker` harness contract:
`session`, `bundle_name`, and `bundle_version` route to the persistence layer instead of the
constructor. A suite migrating off Airflow's internal test harness keeps those call sites
unchanged:

```python
from airflow.models.dag import DagModel


def test_upstream_style(dag_maker, session):
    with dag_maker("upstream_style", session=session):

        @task
        def answer():
            return 42

        answer()

    assert dag_maker.session is session
    assert session.get(DagModel, "upstream_style") is not None
```

- `session=` supplies the metadata session used for every write the context makes
  (persistence, `create_dagrun`, `create_ti`), and `dag_maker.session` returns it. The
  fixture never closes a supplied session; teardown cleanup opens its own. Persistence
  *commits* on it, though -- anything already staged commits too, so combining it with the
  rollback-isolated `session` fixture narrows that fixture's everything-rolls-back
  guarantee to state staged after the last `dag_maker` commit (see
  [Database](database.md#sessions)).
- `bundle_name=` overrides the derived per-Dag bundle row name. The derived name is unique
  per Dag on purpose -- it is the mitigation for cross-worker bundle-row contention under
  `pytest-xdist` -- so supplying your own opts out of that isolation. A shared row is still
  cleaned up once the last Dag referencing it is gone.
- `bundle_version=` is recorded on the persisted 3.x metadata rows (`dag`, `dag_version`).
  Both bundle keywords are accepted and ignored on the certified 2.x family, which predates
  bundles.

## Scheduler-side handles

The context always yields the mutable *authoring* Dag; scheduler-side state is exposed
through opt-in handles on the factory instead (the design decision is recorded in
[ADR 0002](https://github.com/nredd/pytest-airflow-in-a-box/blob/main/docs/adr/0002-authoring-yield-with-scheduler-handles.md)):

```python
def test_scheduler_state(dag_maker):
    with dag_maker("scheduler_state") as dag:
        EmptyOperator(task_id="original")

    assert dag_maker.serialized_dag.task_ids == ["original"]
    assert dag_maker.dag_model.is_paused is False

    EmptyOperator(task_id="added", dag=dag)
    reloaded = dag_maker.sync_dagbag_to_db()

    assert sorted(reloaded.task_ids) == ["added", "original"]
```

- `serialized_dag` returns the persisted scheduler representation after every successful
  context exit. Every Dag serializes as part of persistence, so the `serialized=` keyword
  and the `need_serialized_dag` marker are accepted for upstream compatibility but no
  longer change behavior
- `dag_model` returns the live `DagModel` ORM row on `dag_maker.session` (typed as the
  structural `DagModelRow` protocol). Reads observe committed scheduler metadata --
  `is_paused`, the `next_dagrun*` columns -- and mutations are visible to Airflow
- `sync_dagbag_to_db()` mirrors upstream `tests_common`'s mutate-then-resync shape:
  re-persists the current authoring Dag, refreshes `serialized_dag`, and returns it.
  It commits the metadata session (on a borrowed `session=` that includes anything the
  caller had staged, exactly like persistence at context exit). On 3.x a resync may
  record a new DagVersion; DagRuns created before the resync keep their original version.
  Works on the certified 2.x family too, through that family's writers
- `timetable` returns the persisted scheduler Dag's timetable (typed as the structural
  `SchedulerTimetable` protocol) -- the exact object `create_dagrun` infers
  `data_interval` through. On Airflow 3.2+ the *authoring* Dag's timetable lost the
  scheduling methods, so `dag.timetable.infer_manual_data_interval(...)` raises
  `AttributeError` on the yielded Dag; this handle carries the method on every
  certified release

### Migrating scheduler-side Dag calls

Upstream's `dag_maker` yields a proxy over the serialized Dag, so upstream tests call
scheduler-side methods directly on the yield. This plugin's context always yields the
authoring Dag (ADR 0002), so those call sites move to a factory handle -- each is a
one-line rewrite:

| Upstream pattern on the yield | Migration target |
| --- | --- |
| `dag.timetable.infer_manual_data_interval(...)` | `dag_maker.timetable.infer_manual_data_interval(...)` |
| `dag.create_dagrun(...)` | `dag_maker.create_dagrun(...)` |
| `dag.clear(...)` | `dag_maker.serialized_dag.clear(..., session=dag_maker.session)` |
| `dag.partial_subset(...)` | `dag_maker.serialized_dag.partial_subset(...)` |
| `dag.set_task_instance_state(...)` | `dag_maker.serialized_dag.set_task_instance_state(..., session=dag_maker.session)` |

`serialized_dag` is the installed release's real scheduler Dag object, so everything on
that class is reachable -- the rows above are the patterns upstream suites actually hit.
Signatures beyond `infer_manual_data_interval` follow the installed Airflow release
(e.g. `partial_subset(exclude_original=...)` exists only where upstream added it);
private attributes such as `_time_restriction` are deliberately not part of any plugin
contract. `create_dagrun_after` has no equivalent -- deliberately deferred in
[#261](https://github.com/nredd/pytest-airflow-in-a-box/issues/261) until consumer
demand justifies its per-version run-info machinery (see ADR 0002's amendment).

## Upstream one-call factories

`create_task_instance` and `create_dummy_dag` mirror upstream Airflow's
`tests_common.pytest_plugin` fixtures of the same names -- same parameters and defaults --
so upstream-style tests call them the same way, and they double as the shortest path to
"give me a task instance" when the Dag's content does not matter:

```python
def test_one_call(create_task_instance):
    ti = create_task_instance(dag_id="one_call", state="queued", pool="default_pool")

    assert ti.task_id == "op1"
    assert ti.pool == "default_pool"
```

Both are composition over `dag_maker`: the Dag, DagRun, and task-instance rows are owned
and cleaned up exactly as `dag_maker`'s are, and `**dag_kwargs` (including `serialized=`)
route to `dag_maker` unchanged. `testing_dag_bundle` registers the shared `testing` Dag
bundle row upstream core tests bulk-write metadata against (Airflow 3.x only).

Deliberate deviations from upstream, all rooted in this plugin's own persistence
machinery rather than upstream's:

- `create_task_instance` returns the plain ORM `TaskInstance` with `ti.task` carrying the
  *authoring* operator -- there is no `ti.run()` wrapper; execute through
  `dag_maker.run_ti` or `run_task` instead
- `testing_dag_bundle` never deletes the shared row at teardown: a conditional delete
  would race another `pytest-xdist` worker's in-flight `DagModel.bundle_name` reference,
  and the per-run metadata database is disposable anyway
- An explicit `start_date=None` to `dag_maker(...)` opts out of the default-`start_date`
  injection entirely; upstream silently replaces it with `DEFAULT_DATE`
- A non-manual run whose timetable schedules nothing (`schedule=None`) degrades its
  default `logical_date` to the Dag's `start_date` and then the current UTC date;
  upstream crashes on the `None` run info there. Likewise a non-manual run on a
  trigger-style or custom timetable degrades whitelist-refused automated
  `data_interval` inference to the manual shape every timetable implements
- On a serial run, reusing one `dag_id` across factory calls -- in the same test, or
  after a previous test in the same process leaked its cleanup -- replaces the earlier
  metadata, matching upstream's silent re-sync. `ValueError` remains for a `dag_id`
  whose metadata this process never persisted (foreign rows, another worker's live
  registration) and for any collision on a `pytest-xdist` worker, where a leftover is
  indistinguishable from another worker's in-flight row
- `run_after` on the Airflow 2.x family raises `ValueError` instead of upstream's silent
  drop, matching `dag_maker.create_dagrun`
- `create_task_instance(execution_date=...)` -- the Airflow 2 spelling 2.x-era upstream
  suites use -- is accepted on both families and mapped onto `logical_date` with a
  `DeprecationWarning`, exactly as upstream `tests_common` preserves it; passing both
  spellings raises `ValueError`. `dag_maker.create_dagrun` deliberately does *not* grow
  the alias: `dag_run_kwargs={"execution_date": ...}` keeps its loud rejection, whose
  message names the `logical_date` remedy
- `dag_maker`-routed keywords upstream supports (`session=`, `bundle_name=`,
  `bundle_version=`) follow whatever `dag_maker(...)` itself accepts

Upstream's `dag_id="dag"` default is kept verbatim, so two concurrent tests relying on it
contend on the shared metadata database exactly like any repeated `dag_id` -- keep such
tests on one worker via `pytest.mark.xdist_group`, or pass explicit identifiers.
