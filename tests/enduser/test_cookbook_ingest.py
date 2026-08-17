"""Back the `ingest` Dag cookbook recipes: task relations, retries, and backfill.

`BaseOperator` and `task` resolve dynamically ON PURPOSE, matching
`test_dag_run_result.py`: they moved from `airflow.models`/`airflow.decorators` (2.x) to
`airflow.sdk` (3.x), and `dag_maker.run()`/`run_ti()` are family-branched in
`_compat.taskrun`, so the whole module collects and runs on both families.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from importlib import import_module
from typing import Any

import pytest
from airflow.utils.state import DagRunState, TaskInstanceState

from pytest_airflow_in_a_box.matchers import skipped, succeeded
from pytest_airflow_in_a_box.types import DagMaker, RunTask

pytestmark = pytest.mark.compat

_resolve = import_module("_authoring")._resolve

_authoring = _resolve("airflow.sdk", "airflow.models")
DAG = _authoring.DAG
BaseOperator = _authoring.BaseOperator
task = _resolve("airflow.sdk", "airflow.decorators").task


class PageOnRepeatedFailure(BaseOperator):
    """Escalate to paging only once a task is already on a retry."""

    def execute(self, context: Any) -> dict[str, Any]:
        return {"paged": context["ti"].try_number >= 2}


@pytest.mark.db_test
def test_ingest_shows_branch_skip_trigger_rule_and_cross_task_xcom(
    dag_maker: DagMaker,
) -> None:
    """Run the whole `ingest` Dag once and settle every seam in one snapshot."""

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


@pytest.mark.db_test
def test_ingest_strands_a_retrying_load_visibly(dag_maker: DagMaker) -> None:
    """Settle a retry-configured `load` failure as `up_for_retry` and keep the DagRun running.

    `dag_maker.run()` attempts every task instance exactly once -- there is no scheduler
    loop behind it to re-queue a retry, so `load` stays stranded rather than eventually
    succeeding. See `docs/guide/task-execution.md` and the sibling
    `test_run_strands_a_retrying_task_visibly` in `test_dag_run_result.py`.
    """

    with dag_maker(dag_id="ingest_retry"):

        @task
        def extract() -> dict[str, Any]:
            return {"rows": 3, "batch": "2026-08-17"}

        @task(retries=1, retry_delay=timedelta(minutes=5))
        def load(payload: dict[str, Any]) -> int:
            del payload
            raise ValueError("warehouse locked")

        load(extract())

    result = dag_maker.run()

    assert not result.success
    assert result.state == DagRunState.RUNNING
    assert result.states == {
        "extract": TaskInstanceState.SUCCESS,
        "load": TaskInstanceState.UP_FOR_RETRY,
    }


@pytest.mark.requires_airflow3
def test_load_only_pages_on_the_second_attempt(run_task: RunTask) -> None:
    """Seed a synthetic `try_number` to test attempt-dependent logic directly.

    A dagbag+callable test has no task instance to seed `try_number` on when it calls
    `load.function()` -- `run_task(..., try_number=...)` exists precisely for this, no
    real retry (or even a DagRun) required.
    """

    with DAG(dag_id="d", schedule=None) as dag:
        PageOnRepeatedFailure(task_id="load")

    first = run_task(dag.get_task("load"), try_number=1)
    assert first.xcoms["return_value"] == {"paged": False}

    second = run_task(dag.get_task("load"), try_number=2)
    assert second.xcoms["return_value"] == {"paged": True}


@pytest.mark.db_test
def test_ingest_second_run_waits_on_the_first_days_extract(dag_maker: DagMaker) -> None:
    """Block a `depends_on_past` extract until the prior logical date's attempt succeeds.

    ``catchup=False`` is explicit, not cosmetic: it pins the dependency check to Airflow's
    `PrevDagrunDep.get_previous_dagrun` branch on *both* families. The 2.x family defaults
    `catchup_by_default` to `True`, which takes the other branch
    (`get_previous_scheduled_dagrun`) and, since every `dag_maker`-created run is a manual
    run, never sees day one's run at all -- silently passing this test for the wrong reason.
    """

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
    assert rescued.xcom_pull(task_ids="extract", session=dag_maker.session) == {"rows": 1}
