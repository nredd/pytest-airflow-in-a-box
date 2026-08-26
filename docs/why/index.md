# The Wall

## The failures that need a DagRun to exist

Your `DagBag` has no import errors and `Callable`s pass. Your `DagRun`s are green. Your `TaskInstance`s still fail -- what the hell?

- A `trigger_rule` that never fires because the branch upstream skipped
- A `template_fields` entry added to a subclass but never wired, so `{{ ds }}` ships literally
- A constructor arg that is not JSON-serializable, so the scheduler drops the Dag entirely
- A producer whose outlet does not actually trigger your consumer
- `depends_on_past` on a task whose first run has no predecessor
- A top-level `Variable.get()`: fine on your laptop, a parse failure in the scheduler loop

All of these need a real `DagRun` to exist, so they hit at 03:00 on a scheduler you cannot
attach a debugger to.

Most Dag repos have exactly two kinds of test against them: a `DagBag` import test asserting
`import_errors == {}`, and unit tests that pull a Dag out of a `dag_bag` fixture and call
`task_name.function(...)` directly. That proves "the Dag parses" and "the callable works".
Everything between those two statements is untested, and it is where DagRun-shaped bugs live.

The next four sections walk one realistic `ingest` Dag seam by seam and show what the
two-stage workflow cannot reach: task relations, cross-Dag asset triggering,
DagRun-to-DagRun relations, and attempt-dependent logic. Every example is a real, passing
test in `tests/enduser/test_cookbook_ingest.py` or `tests/enduser/test_cookbook_digest.py`,
run on all 24 compatibility legs.

Recipes for questions that come up often live in the [cookbook](../guide/cookbook.md). Which of these
assertions are *yours* to make is [testing scope](../guide/testing-scope.md).

## Task relations: trigger rules, branching, and cross-task `XCom`

`ingest` extracts a payload, branches on it, and both branches feed a `notify` leaf with
`trigger_rule="all_done"`. A callable test can call `notify.function(payload)` directly and
it always "passes" -- it can never show you that with the *default* trigger rule, `notify`
would silently never run once `quarantine` is skipped; only `all_done` rescues it. One
`dag_maker.run()` settles the whole run, asserted in one shot with
`pytest_airflow_in_a_box.matchers`:

```python
from typing import Any

from airflow.sdk import task

from pytest_airflow_in_a_box.matchers import skipped, succeeded


def test_ingest_shows_branch_skip_trigger_rule_and_cross_task_xcom(dag_maker) -> None:
    with dag_maker(dag_id="ingest"):

        @task
        def extract() -> dict[str, Any]:
            return {"rows": 3, "batch": "2026-08-17"}

        @task.branch
        def validate(payload: dict[str, Any]) -> str:
            return "load" if payload["rows"] > 0 else "quarantine"

        @task
        def load(payload: dict[str, Any]) -> int:
            return payload["rows"]

        @task
        def quarantine(payload: dict[str, Any]) -> None:
            del payload

        @task(trigger_rule="all_done")
        def notify(payload: dict[str, Any]) -> str:
            return f"processed {payload['rows']} rows"

        payload: Any = extract()
        choice: Any = validate(payload)
        loaded: Any = load(payload)
        quarantined: Any = quarantine(payload)
        choice >> [loaded, quarantined]
        [loaded, quarantined] >> notify(payload)

    result = dag_maker.run()

    assert result.success
    assert result == {
        "extract": succeeded({"rows": 3, "batch": "2026-08-17"}),
        "validate": succeeded(),
        "load": succeeded(3),
        "quarantine": skipped(),
        "notify": succeeded("processed 3 rows"),
    }
```

`validate`'s own `XCom` is not the branch string -- `@task.branch` routes execution rather
than pushing `return_value`, so `result.xcoms` has no `validate` key at all; assert
`succeeded()` with no argument when you only care that it ran.

## Cross-Dag relations: asset-triggered downstream Dags

A minimal producer Dag -- `ingest`'s `notify` step, except it emits an outlet asset
(`asset://warehouse/ingest-report`) instead of just returning a string -- pairs with a
second, minimal `digest` Dag scheduled on that asset. `dag_bag` can already prove a
consumer's *schedule* is wired to the right asset, but neither that check nor a callable
test ever sees a real `AssetEvent` or the data it carries. Run the producer for real, hand
its `AssetEvent` to a `digest` DagRun the way the scheduler would, and let `digest`'s task
read it back through the real `triggering_asset_events` runtime context:

```python
from typing import Any

from airflow.models.asset import AssetEvent
from airflow.sdk import Asset, BaseOperator
from sqlalchemy import select

REPORT = Asset(uri="asset://warehouse/ingest-report")


class Notify(BaseOperator):
    def execute(self, context: Any) -> str:
        summary = "processed 3 rows"
        context["outlet_events"][self.outlets[0]].extra = {"summary": summary}
        return summary


class Summarize(BaseOperator):
    def execute(self, context: Any) -> str:
        events = context["triggering_asset_events"][REPORT]
        return events[0].extra["summary"]


def test_digest_reads_the_ingest_run_that_triggered_it(dag_maker) -> None:
    with dag_maker(dag_id="ingest_asset"):
        Notify(task_id="notify", outlets=[REPORT])

    producer_ti = dag_maker.run_ti("notify")

    event_id = dag_maker.session.scalar(
        select(AssetEvent.id).where(
            AssetEvent.source_dag_id == producer_ti.dag_id,
            AssetEvent.source_run_id == producer_ti.run_id,
            AssetEvent.source_task_id == producer_ti.task_id,
        )
    )

    with dag_maker(dag_id="digest", schedule=[REPORT]):
        Summarize(task_id="summarize")

    digest_run = dag_maker.create_dagrun()
    event = dag_maker.session.get(AssetEvent, event_id)
    digest_run.consumed_asset_events.append(event)  # what the scheduler does for you, live
    dag_maker.session.commit()

    consumer_ti = dag_maker.run_ti("summarize", digest_run)

    assert consumer_ti.xcom_pull(task_ids="summarize", session=dag_maker.session) == (
        "processed 3 rows"
    )
```

`DagRun.consumed_asset_events` is that same association; wiring it by hand runs `summarize`
against the actual triggering event instead of a fabricated one.

To assert instead that the producer's event *causes* the consumer run to be created at all,
use `evaluate_asset_schedules` -- see
[Scheduling a consumer off a producer's outlet](../guide/cookbook.md#scheduling-a-consumer-off-a-producers-outlet).

## DagRun relations: depends-on-past and backfill-ish sequences

`extract` carries `depends_on_past=True`. Two sequential
`dag_maker.create_dagrun(logical_date=...)` runs stand in for a backfill: day two's
`extract` never even starts (`ti.state is None`, not a failure) until day one's has
succeeded, and `ignore_depends_on_past=True` on `run_ti` rescues it -- the same escape hatch
a real backfill replay needs. `catchup=False` is not cosmetic: Airflow's dependency check
takes a different code path depending on it, and the 2.x family defaults `catchup_by_default`
to `True` -- without it, the check silently never sees day one's run at all, since
`dag_maker` runs are always manual runs:

```python
from datetime import datetime, timezone
from typing import Any

from airflow.sdk import task
from airflow.utils.state import TaskInstanceState


def test_ingest_second_run_waits_on_the_first_days_extract(dag_maker) -> None:
    with dag_maker(dag_id="ingest_backfill", catchup=False):

        @task(depends_on_past=True)
        def extract() -> dict[str, Any]:
            return {"rows": 1}

        extract()

    day_one = datetime(2026, 8, 16, tzinfo=timezone.utc)
    day_two = datetime(2026, 8, 17, tzinfo=timezone.utc)
    dag_maker.create_dagrun(logical_date=day_one, run_id="ingest_backfill__day1")

    day_two_run = dag_maker.create_dagrun(logical_date=day_two, run_id="ingest_backfill__day2")
    blocked = dag_maker.run_ti("extract", day_two_run)

    assert blocked.state is None

    rescued = dag_maker.run_ti("extract", day_two_run, ignore_depends_on_past=True)

    assert rescued.state == TaskInstanceState.SUCCESS
```

## Attempt-dependent logic: `try_number` without a real retry

"Only page after the first failure" is a branch a callable test cannot reach: calling
`load.function()` gives it no task instance to seed `try_number` on. `run_task(...,
try_number=...)` needs neither a DagRun nor a real retry:

```python
from typing import Any

from airflow.sdk import DAG, BaseOperator


class PageOnRepeatedFailure(BaseOperator):
    def execute(self, context: Any) -> dict[str, Any]:
        return {"paged": context["ti"].try_number >= 2}


def test_load_only_pages_on_the_second_attempt(run_task) -> None:
    with DAG(dag_id="d", schedule=None) as dag:
        PageOnRepeatedFailure(task_id="load")

    first = run_task(dag.get_task("load"), try_number=1)
    assert first.xcoms["return_value"] == {"paged": False}

    second = run_task(dag.get_task("load"), try_number=2)
    assert second.xcoms["return_value"] == {"paged": True}
```

That is a synthetic attempt counter, not a retry -- `dag_maker.run()` never re-queues one
(see [Whole-DagRun execution](../guide/task-execution.md#whole-dagrun-execution)). Driving a
real `up_for_retry` failure the rest of the way to success, and asserting `retry_delay` and
`on_retry_callback` along the way, is the
[retry recipe](../guide/cookbook.md#retry-behavior).

## Scaling a Dags repo

Scale does not just add more of the same failures. It adds failures that are properties of
the *set*, which no single-Dag test can phrase at any fidelity: two templates rendering the
same `dag_id`, one template doing import-time I/O multiplied by every file it generates and
paid on every scheduler parse loop, an `.expand()` over runtime data that one oversized
upstream result fans out unbounded. Those ship as `--airflow-smoke`, a catalog you opt into
rather than write. See [smoke checks over every Dag](../guide/smoke-tests.md).

Scale also costs wall clock, because every one of those checks needs the whole folder parsed;
the plugin parses the corpus once per run and can shard that parse across subprocess workers
-- details in [corpus parsing and parallelism](../guide/smoke-tests.md#corpus-parsing-and-parallelism).

Not sure this plugin is for you? The profile lives on the [docs index](../index.md).

## Why not...

Three things stand between a Dag repo and a real test suite. Two of them are what people
reach for first, and one of them is what you end up maintaining.

### `dag.test()`

Airflow's own debug entry point. It runs one Dag end to end in-process, and it is a debug
harness, not a test harness.

What it does, from `DAG.test` on Airflow 3:

- It clears existing task instances for the logical date before running
  (`SerializedDAG.clear_dags(..., dag_run_state=False)`). Anything you set up on that run
  is gone
- It catches every exception a task body raises, logs it, and keeps looping
  (`except Exception: log.exception("Task failed; ti=%s", ti)`). The call itself does not
  fail, so a bare `dag.test()` in a test body asserts nothing. You have to fish the state
  out of the returned `DagRun` yourself
- It returns that ORM `DagRun` and nothing else. No execution order, no per-task drill-down,
  no pulled XComs

It is also not a pytest plugin: no fixtures, no isolated `AIRFLOW_HOME`, no disposable
metadata DB, no xdist support.

`dag.test(use_executor=True)` looks like the exception, and is not. It queues a real
`workloads.ExecuteTask` onto a real executor, but nothing inside the test process serves the
Task Execution API those supervised workers report to, so the workloads have nowhere to land
(apache/airflow#59074). That is the gap `executor=` fills here, by standing the api-server up
itself -- see [real DagRuns and real state](../guide/task-execution.md).

It is also mid-move upstream
([#61803](https://github.com/apache/airflow/issues/61803),
[#54658](https://github.com/apache/airflow/issues/54658)) -- nothing in this plugin is built
on it.

### `DebugExecutor`

It does not exist on Airflow 3. `airflow.executors.debug_executor` is gone; importing it
raises `ModuleNotFoundError`.

Every 2.x-era blog post recommending `AIRFLOW__CORE__EXECUTOR=DebugExecutor` plus a manual
run is describing an interface that is no longer there. The name survives in this plugin in
exactly one place, `--airflow-doctor`'s Airflow 2.x SQLite compatibility check, because 2.x
still gates SQLite on a single-threaded executor.

If you landed here from that search: the Airflow 3 equivalent of "run my task in-process
under a debugger" is [`run_task`](../guide/db-free-execution.md), and the equivalent of
"run my whole Dag" is [`run_dag`](../guide/task-execution.md).

### A hand-rolled `conftest.py`

The real competitor. Everyone writes one, and the reason it does not work is timing, not
features.

Airflow reads `AIRFLOW_HOME`, its cfg file, and `AIRFLOW__*` at first import. Set them any
later and you are configuring an Airflow that already booted. So the question is only which
code runs first:

- This plugin bootstraps from `pytest_load_initial_conftests`, during argument parsing
- pytest's own conftest collector is `@hookimpl(trylast=True)` on the *same* hook
  (`_pytest/config/__init__.py`), so it runs after every other implementation of it

Your `conftest.py` is imported by that collector. It cannot win the race. The
usual workarounds -- a `pytest.ini` `env` block, a wrapper shell script, `-p` a local plugin
module -- each move the problem rather than solve it, and none of them survives someone
importing `airflow` at the top of a test module. This plugin fails that case loudly instead:
`load_initial_state` raises a `pytest.UsageError` when `airflow` is already in `sys.modules`.
See [`pytest-xdist`](../internals/bootstrap-env-ownership.md).

The cost of the hand-roll is not "impossible". It is that you now own a compat layer. Airflow
3 has no public testing API: `_compat/` here is 12.5k lines of shims over private modules
across Airflow 3.1-3.3, each gated by a capability probe, and it is the only reason anything
above it survives a minor bump. See [what `_compat/` absorbs](../internals/compat-layer.md).
You find out your hand-roll broke when the upgrade lands.

## Where to go next

- [Deciding which failures are yours](../guide/testing-scope.md) -- the scope boundary
- [The fidelity ladder](../guide/ladder.md) -- how much of Airflow each assertion costs
- [Why not `dag.test()`, `DebugExecutor`, or your own `conftest.py`](#why-not)
