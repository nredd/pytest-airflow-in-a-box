"""Asset-scheduled consumer corpus Dag, Dataset-scheduled on Airflow 2.x.

The authoring surface resolves dynamically because the corpus parses on BOTH Airflow
families and no single environment can statically resolve the other family's modules.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any


def _resolve(*candidates: str) -> Any:
    """Import the first available module; the corpus parses on both Airflow families.

    Parameters:
        candidates: str module paths ordered newest family first.

    Returns:
        Any containing the first importable module.
    """

    for name in candidates[:-1]:
        try:
            return import_module(name)
        except ImportError:
            continue
    return import_module(candidates[-1])


try:
    _sdk = import_module("airflow.sdk")

    dag = _sdk.dag
    Asset = _sdk.Asset
    EXTRA_DAG_KWARGS: dict[str, Any] = {}
except ImportError:  # Airflow 2.x: datasets predate assets and a scheduled Dag
    # requires an explicit `start_date`.
    from datetime import datetime, timezone

    dag = _resolve("airflow.decorators").dag
    Asset = _resolve("airflow.datasets").Dataset
    EXTRA_DAG_KWARGS = {"start_date": datetime(2024, 1, 1, tzinfo=timezone.utc)}

EmptyOperator = _resolve(
    "airflow.providers.standard.operators.empty", "airflow.operators.empty"
).EmptyOperator


@dag(schedule=[Asset("asset://compat/corpus-output")], **EXTRA_DAG_KWARGS)
def asset_consumer() -> None:
    """Consume the synthetic corpus asset."""

    EmptyOperator(task_id="consume")


asset_consumer()
