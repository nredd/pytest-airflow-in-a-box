# Cookbook

Recipes for testing questions that come up often -- most distilled from
[apache/airflow#63941](https://github.com/apache/airflow/discussions/63941), plus retry
behavior ([#167](https://github.com/nredd/pytest-airflow-in-a-box/issues/167)). Each names
the test in `tests/enduser/` that keeps it honest. For checks over the whole corpus rather
than one Dag, see [Smoke Tests](smoke-tests.md).

For the argument -- what a `DagBag` import test plus a direct `task.function` call
cannot reach -- read [Whose fail is it anyway?](testing-scope.md).

## Scheduling a consumer off a producer's outlet

`evaluate_asset_schedules` does, without a scheduler, what the scheduler loop does once a
producer task's outlet events are persisted: check whether a consumer Dag's asset condition
is satisfied and create its `QUEUED` `DagRun` with the satisfying events attached. It is the
only way to assert that your producer actually *triggers* your consumer, rather than that the
consumer's `schedule=` mentions the right asset.

The consumer must be persisted *before* the producer's task runs -- Airflow queues an asset
event only against Dags already naming themselves as subscribers, the same ordering a live
deployment requires (`tests/enduser/test_asset_scheduling.py`):

```python
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

from pytest_airflow_in_a_box.assets import evaluate_asset_schedules


def test_consumer_dagrun_is_created(dag_maker):
    asset = Asset(uri="asset://warehouse/answers")
    with dag_maker(dag_id="consumer", schedule=[asset]):
        EmptyOperator(task_id="consume")
    with dag_maker(dag_id="producer"):
        EmitAssetOperator(task_id="emit", outlets=[asset])

    dag_maker.run_ti("emit")

    (consumer_run,) = evaluate_asset_schedules("consumer", session=dag_maker.session)

    assert consumer_run.run_type == DagRunType.ASSET_TRIGGERED
    assert consumer_run.state == DagRunState.QUEUED
```

Signature and scope:

- `evaluate_asset_schedules(dag_ids=None, *, session)`. `dag_ids` takes one `dag_id` or a
  collection; `session` is required and raises `ValueError` when omitted
- Returns `tuple[DagRun, ...]`, one entry per consumer whose condition was satisfied, in
  evaluation order. An unsatisfied condition contributes no entry -- an empty tuple is the
  assertion for "not yet triggered"
- The `DagRun` comes back unrun. Pass it to
  `pytest_airflow_in_a_box.taskinstance.execute_dag_run` to run the consumer too
- `dag_ids=None` sweeps every Dag with a pending queue row database-wide. That is serial-only
  for the same reason [`clear_db`](../internals/test-environments.md#the-disposable-metadata-database) is: another xdist worker's pending rows are
  indistinguishable from yours
- Raises `ValueError` for a named Dag with no persisted serialized representation, or one not
  scheduled by an `Asset`/`Dataset` at all

## Persisting and querying an outlet event

Emit an outlet event through `dag_maker` and query it back scoped to the run that produced it
-- `AssetEvent` is a database-global, accumulating table (see
[Seeded names are database-global](../internals/test-environments.md#seeding-variables-and-connections)), so an unscoped query can match a different
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

Static schedule assertions (`consumer.timetable.asset_condition`) go through `dag_bag`
against a real Dag folder -- see `test_asset_dags_survive_serialization` in the same file.
Reading an event back from *inside* the consumer's own execution is the
[cross-Dag relations](testing-scope.md#the-failures-worth-catching).

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
actually construct `MyHook` and return `get_conn()` for the patch to reach it
(`tests/enduser/test_hooks.py`):

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
table instead of the operator's `XCom` output. The Airflow 2.x idiom
(`ti.get_template_context()` + `ti.render_templates()` on the ORM `TaskInstance`) does not
carry over -- template rendering moved into the Task SDK's execution-time
`RuntimeTaskInstance`, which this table is populated from. `MyOperator` must declare `query`
in `template_fields` (and `".sql"` in `template_ext` for a file-backed field) for it to render
at all (`tests/enduser/test_operators.py`):

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

Cheaper alternatives when the database is not the point: the DB-free
[`render_task`](ladder.md#rendering-template-fields-without-running) returns the
rendered values with no run at all, and [`task_context`](ladder.md#one-operator-no-database) covers
operators that render from *inside* `execute()`.

## Retry behavior

`dag_maker.run()` / `dag_maker.run_ti()` execute a `TaskInstance` once, the way the scheduler would: a
retry-configured failure settles `up_for_retry` rather than being re-attempted (see
[a whole `DagRun`, real state](ladder.md#a-whole-dagrun-real-state)). Drive it the rest of the way with a second, explicit
`run_ti(..., ignore_ti_state=True, ignore_task_deps=True)` call against the same persisted
instance -- `ignore_task_deps` bypasses Airflow's "Not In Retry Period" dependency instead of
waiting out `retry_delay` for real. Bump `try_number` before each `run_ti` call, including
the first. That mirrors the scheduler-side step Airflow's own `Dag.test()` takes before
every attempt -- a step a direct `run_ti` call does not take on its own. Skip the first
bump and you understate how close the retry is to exhausting `max_tries`. Airflow 2.x's pre-2.10
`try_number` is a read-only derived property rather than a plain column, so this recipe is
3.x-only (`tests/enduser/test_dag_run_result.py`):

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
    ti = dag_maker.create_ti("flaky", dag_run)
    ti.try_number += 1
    dag_maker.session.commit()

    with pytest.raises(ValueError, match="nope"):
        dag_maker.run_ti("flaky", dag_run)

    ti = dag_maker.create_ti("flaky", dag_run)
    assert ti.try_number == 1
    assert ti.state == TaskInstanceState.UP_FOR_RETRY
    assert ti.next_retry_datetime() == ti.end_date + timedelta(minutes=5)
    assert retried_marker.exists()  # the user's `on_retry_callback` ran

    ti.try_number += 1
    dag_maker.session.commit()
    ti = dag_maker.run_ti("flaky", dag_run, ignore_ti_state=True, ignore_task_deps=True)

    assert ti.try_number == 2
    assert ti.state == TaskInstanceState.SUCCESS
```

To test *attempt-dependent* logic rather than the retry itself, seed a synthetic `try_number`
with the DB-free `run_task` fixture instead --
[`run_task`](ladder.md#one-operator-no-database).

## A minimal serial executor

Airflow 3 removed `SequentialExecutor` from core. A small custom executor can still drive one
workload at a time:

```python
from airflow.executors.base_executor import BaseExecutor


class SerialExecutor(BaseExecutor):
    is_local = True

    def sync(self) -> None:
        """Nothing async to reconcile."""

    def _process_workloads(self, workload_items) -> None:
        for workload in workload_items:
            key = workload.ti.key
            self.queued_tasks.pop(key, None)
            try:
                BaseExecutor.run_workload(workload)
            except Exception as error:
                self.fail(key, error)
            else:
                self.success(key)

    def end(self) -> None:
        """No workload is left in flight."""

    def terminate(self) -> None:
        """No workload is left to kill."""
```

Key on `workload.ti.key`, not `workload.key`, for compatibility across Airflow 3.1–3.3. On
3.1 and 3.2, `BaseExecutor.run_workload` is unavailable; call the Task SDK supervisor directly
with the workload's task instance, Dag path, bundle, token, server, and log path. Validate the
class with [`check_component`](custom-components.md#execution-components), then exercise it
through [`run_dag(..., executor=...)`](ladder.md#executor-driven-runs).

## Elsewhere in this guide

- Pinned-`Param` cases -- [Dag-file collection](smoke-tests.md#pinned-param-cases)
- Deferrable operators -- [Deferrable operators](deferrable-operators.md)
- Locating a test's own data files -- the plugin ships `airflow_home` and
  `airflow_dags_folder` ([Where the run lives](../internals/test-environments.md#the-isolated-airflow_home)) and nothing for your repo's
  `tests/` folder; use `request.path.parent`, or `pytestconfig.rootpath` /
  `pytestconfig.inipath` for the repo-root and config-file cases
