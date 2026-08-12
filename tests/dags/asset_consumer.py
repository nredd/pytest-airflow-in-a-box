"""Asset-scheduled consumer corpus Dag, Dataset-scheduled on Airflow 2.x.

The authoring surface resolves dynamically because the corpus parses on BOTH Airflow
families and no single environment can statically resolve the other family's modules.
"""

from __future__ import annotations

import sys
from importlib import import_module
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))
_resolve = import_module("_family")._resolve

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
