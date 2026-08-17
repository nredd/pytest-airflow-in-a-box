# Cookbook

## What a dagbag + callable test misses

The common Dag-testing workflow is two-stage: a dagbag import test, then a unit test that
pulls a Dag from a dagbag fixture and calls `task_name.function` directly. That proves "the
Dag parses" and "the callable works" -- and nothing in between. The four recipes below center
on one realistic multi-task `ingest` Dag and show, one seam at a time, what the two-stage
workflow cannot reach: task relations, cross-Dag asset triggering, DagRun-to-DagRun relations,
and retry behavior (the last of these also reaches for the DB-free `run_task` fixture, on a
throwaway single-task Dag, to test attempt-dependent logic without a real retry). Every recipe
is backed by a real, passing test in `tests/enduser/test_cookbook_ingest.py` or
`test_cookbook_digest.py`.

### Task relations: trigger rules, branching, and cross-task xcom

`ingest` extracts a payload, branches on it, and both branches feed a `notify` leaf with
`trigger_rule="all_done"`. A callable test can call `notify.function(payload)` directly and it
always "passes" -- it can never show you that with the *default* trigger rule, `notify` would
silently never run once `quarantine` is skipped; only `all_done` rescues it. One
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

`validate`'s own XCom is not the branch string -- `@task.branch` routes execution rather than
pushing `return_value`, so `result.xcoms` has no `validate` key at all; assert `succeeded()`
with no argument when you only care that it ran.

### Cross-Dag relations: asset-triggered downstream Dags

A minimal producer Dag -- an `ingest`-shaped `notify` step that emits an outlet asset
(`asset://warehouse/ingest-report`) instead of just returning a string -- pairs with a second,
minimal `digest` Dag scheduled on that asset. `full_dag_bag` can already prove a consumer's
*schedule* is wired to the right asset (see
[Assets: outlet/consumer testing](#assets-outletconsumer-testing) below), but neither that check
nor a callable test ever sees a real `AssetEvent` or the data it carries. Run the producer for
real, hand its `AssetEvent` to a `digest` DagRun the way the scheduler would, and let `digest`'s
task read it back through the genuine `triggering_asset_events` runtime context:

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

`DagRun.consumed_asset_events` is the real association the scheduler populates before running a
consumer's DagRun; wiring it by hand is exactly what a test needs to exercise a consumer task
against the actual triggering event rather than a fabricated one.

### DagRun relations: depends-on-past and backfill-ish sequences

`extract` carries `depends_on_past=True`. Two sequential
`dag_maker.create_dagrun(logical_date=...)` runs stand in for a backfill: day two's `extract`
never even starts (`ti.state is None`, not a failure) until day one's has succeeded, and
`ignore_depends_on_past=True` on `run_ti` rescues it -- the same escape hatch a real backfill
replay needs. `catchup=False` is not cosmetic: Airflow's dependency check takes a different
code path depending on it, and the 2.x family defaults `catchup_by_default` to `True` --
without it, the check silently never sees day one's run at all, since `dag_maker` runs are
always manual runs:

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

### Retry behavior: `up_for_retry` and `try_number`

`dag_maker.run()` attempts every task instance exactly once -- there is no scheduler loop
behind it to re-queue a retry, so a retry-configured `load` failure strands at `up_for_retry`
and the DagRun stays `running` rather than eventually recovering (see
[Whole-DagRun execution](task-execution.md#whole-dagrun-execution)). To test *attempt-dependent*
logic itself -- "only page after the first failure" -- seed a synthetic `try_number` with the
DB-free `run_task` fixture instead. A callable test has no task instance to seed `try_number`
on when it calls `load.function()`; `run_task(..., try_number=...)` needs neither a DagRun nor
a real retry:

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

## Community recipes

Recipes for testing questions that come up often, distilled from
[apache/airflow#63941](https://github.com/apache/airflow/discussions/63941). Four are adapted
from a real test in `tests/enduser/`. Two are already covered elsewhere in this guide and are
cross-referenced rather than duplicated.

### SQL operators with mocked connections

Point a real provider operator at a synthetic SQLite file instead of a live warehouse, via
`airflow_connections` (`tests/enduser/test_hooks.py`):

```python
import sqlite3

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


def test_sql_operator_against_a_fake_warehouse(airflow_connections, dag_maker, tmp_path):
    database = tmp_path / "warehouse.db"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE answers (value INTEGER NOT NULL)")
        connection.execute("INSERT INTO answers VALUES (42)")
    with dag_maker():
        SQLExecuteQueryOperator(
            task_id="query",
            conn_id="warehouse",
            sql="SELECT value FROM answers",
            handler=lambda cursor: cursor.fetchone()[0],
        )
    airflow_connections({"warehouse": {"conn_type": "sqlite", "host": str(database)}})

    ti = dag_maker.run_ti("query")

    assert ti.xcom_pull(task_ids="query", session=dag_maker.session) == 42
```

### Mocking your own hooks with `unittest.mock`

Patch the hook directly and skip the metastore entirely -- plain `unittest.mock`/`monkeypatch`
layers on top of `run_task` like any other Python attribute patch. `MyOperator.execute` must
actually construct `MyHook` and return `get_conn()` for the patch to reach it:

```python
from unittest import mock

from airflow.sdk import DAG


def test_hook_is_mocked(run_task):
    with DAG(dag_id="d", schedule=None) as dag:
        MyOperator(task_id="connect", conn_id="unused")

    with mock.patch.object(MyHook, "get_conn", return_value={"region": "mocked"}):
        result = run_task(dag.get_task("connect"))

    assert result.xcoms["return_value"] == {"region": "mocked"}
```

### Asserting rendered templates

Run the task, then read its rendered fields back from Airflow's `RenderedTaskInstanceFields`
table instead of the operator's XCom output. The Airflow 2.x idiom (`ti.get_template_context()` +
`ti.render_templates()` on the ORM `TaskInstance`) does not carry over -- template rendering moved
into the Task SDK's execution-time `RuntimeTaskInstance`, which this table is populated from.
`MyOperator` must declare `query` in `template_fields` (and `".sql"` in `template_ext` for a
file-backed field) for it to render at all:

```python
from airflow.models.renderedtifields import RenderedTaskInstanceFields


def test_rendered_query(dag_maker, tmp_path):
    (tmp_path / "query.sql").write_text("SELECT {{ params.value }}")
    with dag_maker(template_searchpath=[str(tmp_path)], params={"value": "42"}):
        MyOperator(task_id="render", query="query.sql")

    dag_run = dag_maker.create_dagrun()
    ti = dag_maker.run_ti("render", dag_run)
    rendered = RenderedTaskInstanceFields.get_templated_fields(ti, session=dag_maker.session)

    assert rendered["query"] == "SELECT 42"
```

### Pinned-`Param` cases

Already covered end to end -- see [Dag-file collection](dag-collection.md), collected and
validated by `tests/test_collection.py` against the `PYTEST_DAG_CASES` declared in
`tests/dags/mapping.py`.

### Deferrable operators

Already covered end to end -- see [Deferrable operators](deferrable-operators.md), exercised by
`tests/enduser/test_triggers.py`, including the composed defer -> fire -> resume path via
`dag_maker.run_ti(..., run_triggerer=True)`.

### Assets: outlet/consumer testing

Emit an outlet event through `dag_maker` and query it back scoped to the run that produced it --
`AssetEvent` is a database-global, accumulating table (see
[Seeded names are database-global](seeding.md)), so an unscoped query can match a different
test's event. `EmitAssetOperator.execute` attaches the metadata via
`context["outlet_events"][self.outlets[0]].extra` (`tests/enduser/test_assets.py`):

```python
from airflow.models.asset import AssetEvent, AssetModel
from airflow.sdk import Asset
from sqlalchemy import select


def test_outlet_event_is_persisted(dag_maker):
    produced = Asset(uri="asset://warehouse/answers")
    with dag_maker():
        EmitAssetOperator(task_id="emit", outlets=[produced])

    ti = dag_maker.run_ti("emit")
    event = dag_maker.session.scalar(
        select(AssetEvent)
        .join(AssetModel, AssetModel.id == AssetEvent.asset_id)
        .where(
            AssetModel.uri == produced.uri,
            AssetEvent.source_dag_id == ti.dag_id,
            AssetEvent.source_run_id == ti.run_id,
            AssetEvent.source_task_id == ti.task_id,
        )
    )

    assert event is not None
    assert event.extra == {"rows": 3}
```

Consumer/schedule assertions (`consumer.timetable.asset_condition`) go through `full_dag_bag`
against a real Dag folder -- see `test_asset_dags_survive_serialization` in the same file.
