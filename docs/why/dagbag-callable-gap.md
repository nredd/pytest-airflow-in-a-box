# What a dagbag test and a callable test miss

Most Dag repos have exactly two kinds of test: a `DagBag` import test asserting
`import_errors == {}`, and unit tests that pull a Dag out of a dagbag fixture and call
`task_name.function(...)` directly.

That proves "the Dag parses" and "the callable works". Everything between those two
statements is untested, and it is where DagRun-shaped bugs live.

This page walks one realistic `ingest` Dag seam by seam and shows what the two-stage
workflow cannot reach: task relations, cross-Dag asset triggering, DagRun-to-DagRun
relations, and attempt-dependent logic. Every example is a real, passing test in
`tests/enduser/test_cookbook_ingest.py` or `tests/enduser/test_cookbook_digest.py`, run on
all 24 compatibility legs.

Lookup-shaped recipes live in the [cookbook](../guide/cookbook.md). Which of these
assertions are *yours* to make is [testing scope](../guide/testing-scope.md).

## Task relations: trigger rules, branching, and cross-task xcom

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

`validate`'s own XCom is not the branch string -- `@task.branch` routes execution rather
than pushing `return_value`, so `result.xcoms` has no `validate` key at all; assert
`succeeded()` with no argument when you only care that it ran.

## Cross-Dag relations: asset-triggered downstream Dags

A minimal producer Dag -- an `ingest`-shaped `notify` step that emits an outlet asset
(`asset://warehouse/ingest-report`) instead of just returning a string -- pairs with a
second, minimal `digest` Dag scheduled on that asset. `dag_bag` can already prove a
consumer's *schedule* is wired to the right asset, but neither that check nor a callable
test ever sees a real `AssetEvent` or the data it carries. Run the producer for real, hand
its `AssetEvent` to a `digest` DagRun the way the scheduler would, and let `digest`'s task
read it back through the genuine `triggering_asset_events` runtime context:

```python
from typing import Any

from airflow.models.asset import AssetEvent, AssetModel
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
        select(AssetEvent.id)
        .join(AssetModel, AssetModel.id == AssetEvent.asset_id)
        .where(
            AssetModel.uri == REPORT.uri,
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

`DagRun.consumed_asset_events` is the real association the scheduler populates before
running a consumer's DagRun; wiring it by hand is exactly what a test needs to exercise a
consumer task against the actual triggering event rather than a fabricated one.

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

That is a synthetic attempt counter, not a retry. `dag_maker.run()` attempts every task
instance exactly once -- there is no scheduler loop behind it to re-queue a retry, so a
retry-configured `load` failure strands at `up_for_retry` and the DagRun stays `running`.
Driving it the rest of the way to success, and asserting `retry_delay` and
`on_retry_callback` along the way, is the
[retry recipe](../guide/cookbook.md#retry-behavior).
