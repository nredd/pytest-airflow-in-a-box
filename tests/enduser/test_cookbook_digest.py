"""Back the `ingest` -> `digest` cross-Dag asset cookbook recipe.

Asset ORM persistence (`airflow.models.asset`) is genuinely 3.x-only, matching
`test_assets.py` -- see `tests/enduser/conftest.py`'s `collect_ignore` for the 2.x family.
"""

from __future__ import annotations

from typing import Any

import pytest
from airflow.models.asset import AssetEvent
from airflow.sdk import Asset, BaseOperator
from sqlalchemy import select

from pytest_airflow_in_a_box.types import DagMaker

pytestmark = pytest.mark.compat

REPORT = Asset(uri="asset://warehouse/ingest-report")


class Notify(BaseOperator):
    """Emit the ingest summary as the outlet event's `extra`."""

    def execute(self, context: Any) -> str:
        summary = "processed 3 rows"
        context["outlet_events"][self.outlets[0]].extra = {"summary": summary}
        return summary


class Summarize(BaseOperator):
    """Read the summary back off the asset event that triggered this run."""

    def execute(self, context: Any) -> str:
        events = context["triggering_asset_events"][REPORT]
        return events[0].extra["summary"]


@pytest.mark.db_test
def test_digest_reads_the_ingest_run_that_triggered_it(dag_maker: DagMaker) -> None:
    """Read a real, queryable `AssetEvent` back through a consumer's own execution."""

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
    assert event_id is not None

    with dag_maker(dag_id="digest", schedule=[REPORT]):
        Summarize(task_id="summarize")

    digest_run = dag_maker.create_dagrun()
    event = dag_maker.session.get(AssetEvent, event_id)
    assert event is not None
    assert event.extra == {"summary": "processed 3 rows"}
    digest_run.consumed_asset_events.append(event)
    dag_maker.session.commit()

    consumer_ti = dag_maker.run_ti("summarize", digest_run)

    assert consumer_ti.xcom_pull(task_ids="summarize", session=dag_maker.session) == (
        "processed 3 rows"
    )
