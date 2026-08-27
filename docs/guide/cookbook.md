# Cookbook

Use these recipes for targeted integration tests: asset handoffs, real or mocked connections,
rendered fields, retries, and executor workloads. Start with the
[fidelity ladder](ladder.md); use [Smoke Tests](smoke-tests.md) for corpus-wide policies.

## Scheduling a consumer off a producer's outlet

Use `evaluate_asset_schedules` to prove that a producer event creates a consumer `DagRun`, not
merely that the consumer names the asset in `schedule=`. Persist the consumer before running
the producer; Airflow queues events only for consumers already registered as subscribers.

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

The example uses Airflow 3 names. On Airflow 2.10 or newer, use `Dataset` and
`DagRunType.DATASET_TRIGGERED` instead.

The evaluator follows the scheduler's asset-condition path without starting a scheduler:

- Pass one `dag_id`, a collection, or `None`; `session=` is required.
- It returns one unrun, queued `DagRun` per satisfied consumer, in evaluation order. An empty
  tuple means no named condition was satisfied.
- Pass a returned run to
  `pytest_airflow_in_a_box.taskinstance.execute_dag_run` to run the consumer too.
- `dag_ids=None` sweeps every pending consumer database-wide. That is serial-only
  for the same reason [`clear_db`](../internals/test-environments.md#the-disposable-metadata-database) is: another xdist worker's pending rows are
  indistinguishable from yours.
- It raises `ValueError` when a named Dag has no persisted serialized representation or is not
  scheduled by an `Asset`/`Dataset`.

## Persisting and querying an outlet event

Query an outlet event by its asset URI and complete task-instance identity. `AssetEvent` is a
database-global, accumulating table (see
[Seeded names are database-global](../internals/test-environments.md#seeding-variables-and-connections)), so an unscoped query can match a different
test's event. Here, `EmitAssetOperator.execute` sets
`context["outlet_events"][self.outlets[0]].extra = {"rows": 3}`:

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

Use `dag_bag` against the real Dag folder for static schedule assertions such as
`consumer.timetable.asset_condition`.

## SQL operators with mocked connections

Seed a temporary SQLite connection with `airflow_connections`, then execute the real provider
operator. This tests hook resolution, execution, and XCom without a warehouse:

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

When connection lookup is not the subject, patch your hook and use DB-free `run_task`.
`MyOperator.execute` must construct `MyHook` for this patch to intercept the call:

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

To verify persisted rendered fields, run the task and query
`RenderedTaskInstanceFields`—not XCom or the original operator. `MyOperator` must include
`query` in `template_fields` and `".sql"` in `template_ext`:

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

When persistence is not the subject, the DB-free
[`render_task`](ladder.md#rendering-template-fields-without-running) returns the
rendered values with no run at all, and [`task_context`](ladder.md#one-operator-no-database) covers
operators that render from *inside* `execute()`.

## Retry behavior

`run()` and `run_ti()` make one attempt. A retryable failure settles `up_for_retry`; they do not
wait and requeue it. On Airflow 3, drive the next attempt against the same `DagRun`:

1. Increment and commit `try_number` before every attempt, including the first. This mirrors
   the scheduler's bookkeeping.
2. Assert the first failure, state, retry deadline, and callback.
3. Increment and commit again, then call `run_ti(..., ignore_ti_state=True,
   ignore_task_deps=True)` to bypass the existing state and “Not In Retry Period” dependency.

This recipe is 3.x-only; earlier Airflow 2 releases expose `try_number` as a read-only derived
property.

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
    assert ti.xcom_pull(task_ids="flaky", session=dag_maker.session) == "done"
```

For attempt-dependent task logic without retry scheduling, seed `try_number` through the
DB-free [`run_task`](ladder.md#one-operator-no-database) fixture.

## A minimal serial executor

Airflow 3 removed `SequentialExecutor` from core. This executor runs workloads serially across
Airflow 3.1–3.3:

```python
from typing import Any

from airflow.executors.base_executor import BaseExecutor


def _run_workload(workload: Any) -> None:
    runner = getattr(BaseExecutor, "run_workload", None)
    if runner is not None:
        runner(workload)
        return

    from airflow.configuration import conf
    from airflow.sdk.execution_time.supervisor import supervise

    base_url = conf.get("api", "base_url", fallback="/")
    if base_url.startswith("/"):
        base_url = f"http://localhost:8080{base_url}"
    supervise(
        ti=workload.ti,
        dag_rel_path=workload.dag_rel_path,
        bundle_info=workload.bundle_info,
        token=workload.token,
        server=conf.get(
            "core",
            "execution_api_server_url",
            fallback=f"{base_url.rstrip('/')}/execution/",
        ),
        log_path=workload.log_path,
    )


class SerialExecutor(BaseExecutor):
    is_local = True

    def sync(self) -> None:
        """Nothing async to reconcile."""

    def _process_workloads(self, workload_items) -> None:
        for workload in workload_items:
            key = workload.ti.key
            self.queued_tasks.pop(key, None)
            try:
                _run_workload(workload)
            except Exception as error:
                self.fail(key, error)
            else:
                self.success(key)

    def end(self) -> None:
        """No workload is left in flight."""

    def terminate(self) -> None:
        """No workload is left to kill."""
```

Key on `workload.ti.key`, not `workload.key`, for compatibility across Airflow 3.1–3.3. The
fallback calls the older Task SDK supervisor when `BaseExecutor.run_workload` is unavailable.
Validate the class with [`check_component`](custom-components.md#execution-components), then
exercise it through [`run_dag(..., executor=...)`](ladder.md#executor-driven-runs).

## Elsewhere in this guide

- Pinned-`Param` cases -- [Dag-file collection](smoke-tests.md#pinned-param-cases)
- Deferred tasks -- [Deferred tasks](deferrable-operators.md)
- Locating a test's own data files -- the plugin ships `airflow_home` and
  `airflow_dags_folder` ([Where the run lives](../internals/test-environments.md#the-isolated-airflow_home)) and nothing for your repo's
  `tests/` folder; use `request.path.parent`, or `pytestconfig.rootpath` /
  `pytestconfig.inipath` for the repo-root and config-file cases
