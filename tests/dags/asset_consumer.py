"""Asset-scheduled consumer corpus Dag."""

from __future__ import annotations

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, dag


@dag(schedule=[Asset("asset://compat/corpus-output")])
def asset_consumer() -> None:
    """Consume the synthetic corpus asset."""

    EmptyOperator(task_id="consume")


asset_consumer()
