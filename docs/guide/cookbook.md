# Cookbook

Recipes for testing questions that come up often, distilled from
[apache/airflow#63941](https://github.com/apache/airflow/discussions/63941). Five are adapted
from a real test in `tests/enduser/`. Two are already covered elsewhere in this guide and are
cross-referenced rather than duplicated.

## SQL operators with mocked connections

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

## Mocking your own hooks with `unittest.mock`

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

## Asserting rendered templates

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

## Pinned-`Param` cases

Already covered end to end -- see [Dag-file collection](dag-collection.md), collected and
validated by `tests/test_collection.py` against the `PYTEST_DAG_CASES` declared in
`tests/dags/mapping.py`.

## Deferrable operators

Already covered end to end -- see [Deferrable operators](deferrable-operators.md), exercised by
`tests/enduser/test_triggers.py`, including the composed defer -> fire -> resume path via
`dag_maker.run_ti(..., run_triggerer=True)`.

## Assets: outlet/consumer testing

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

## Retry behavior

`dag_maker.run()`/`dag_maker.run_ti()` execute a `TaskInstance` once, scheduler-shaped: a
retry-configured failure settles `up_for_retry` rather than being re-attempted (see
[Task execution](task-execution.md)). Drive it the rest of the way with a second, explicit
`run_ti(..., ignore_ti_state=True, ignore_task_deps=True)` call against the same persisted
instance -- `ignore_task_deps` bypasses Airflow's "Not In Retry Period" dependency instead of
waiting out `retry_delay` for real, and bumping `try_number` beforehand mirrors the same
scheduler-shaped step Airflow's own `Dag.test()` takes when it requeues a retrying instance,
since a direct `run_ti` call does not (`tests/enduser/test_dag_run_result.py`):

```python
from datetime import timedelta

import pytest
from airflow.sdk import task
from airflow.utils.state import TaskInstanceState


def test_flaky_task_retries_to_success(dag_maker, tmp_path):
    retried_marker = tmp_path / "retried"

    def mark_retried(context):
        retried_marker.write_text("retried")

    with dag_maker():

        @task(retries=1, retry_delay=timedelta(minutes=5), on_retry_callback=mark_retried)
        def flaky():
            if not retried_marker.exists():
                raise ValueError("nope")
            return "done"

        flaky()

    dag_run = dag_maker.create_dagrun()

    with pytest.raises(ValueError, match="nope"):
        dag_maker.run_ti("flaky", dag_run)

    ti = dag_maker.create_ti("flaky", dag_run)
    assert ti.try_number == 0
    assert ti.state == TaskInstanceState.UP_FOR_RETRY
    assert ti.next_retry_datetime() == ti.end_date + timedelta(minutes=5)
    assert retried_marker.exists()  # the user's `on_retry_callback` ran

    ti.try_number += 1
    dag_maker.session.commit()
    ti = dag_maker.run_ti("flaky", dag_run, ignore_ti_state=True, ignore_task_deps=True)

    assert ti.try_number == 1
    assert ti.state == TaskInstanceState.SUCCESS
```
