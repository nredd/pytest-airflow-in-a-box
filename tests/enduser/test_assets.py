"""Exercise Airflow 3 asset authoring and persistence."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from airflow.models.asset import AssetEvent, AssetModel
from airflow.sdk import DAG, Asset, BaseOperator
from airflow.utils.state import TaskInstanceState
from sqlalchemy import select

from pytest_airflow_in_a_box.types import DagMaker, RunTask

pytestmark = pytest.mark.compat


class EmitAssetOperator(BaseOperator):
    """Attach synthetic metadata to one outlet event."""

    def execute(self, context: Any) -> None:
        context["outlet_events"][self.outlets[0]].extra = {"rows": 3}


@pytest.mark.db_test
def test_asset_outlet_event_is_persisted(dag_maker: DagMaker) -> None:
    """Record a produced asset event with its task source and extra."""

    produced = Asset(uri="asset://compat/runtime-outlet")
    with dag_maker(dag_id="compat_asset_outlet"):
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


def test_asset_outlet_runs_without_metadata(run_task: RunTask) -> None:
    """Validate and report an active outlet through fake supervision."""

    produced = Asset(uri="asset://compat/in-process-outlet")
    with DAG(dag_id="compat_asset_free", schedule=None) as dag:
        EmitAssetOperator(task_id="emit", outlets=[produced])

    result = run_task(dag.get_task("emit"))

    assert result.state == TaskInstanceState.SUCCESS
    assert result.msg is not None
    assert result.msg.task_outlets[0].uri == produced.uri


CORPUS = Path(__file__).parents[1] / "dags"


def test_asset_dags_survive_serialization(pytester: pytest.Pytester) -> None:
    """Round-trip the corpus asset-decorator DAG through serialization."""

    pytester.makepyfile(
        """
        def test_assets(full_dag_bag):
            producer = full_dag_bag.dags["asset_outlet"]
            task = producer.get_task("publish")
            consumer = full_dag_bag.dags["asset_consumer"]

            assert producer.schedule is None
            assert task.outlets[0].uri == "asset://compat/corpus-output"
            assert type(consumer.timetable.asset_condition).__name__ == "AssetAll"
            assert consumer.get_task("consume").task_id == "consume"
        """
    )

    result = pytester.runpytest_subprocess("-q", f"--dag-folder={CORPUS}")

    result.assert_outcomes(passed=1)
