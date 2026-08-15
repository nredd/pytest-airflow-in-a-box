"""Asset-decorator corpus Dag, expressed as a Dataset producer on Airflow 2.x.

The authoring surface resolves dynamically because the corpus parses on BOTH Airflow
families and no single environment can statically resolve the other family's modules.
"""

from __future__ import annotations

import sys
from importlib import import_module
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
_family = import_module("_family")
_resolve = _family._resolve
_dag_kwargs = _family._dag_kwargs

# Airflow 2.x has no `@asset`; an outlet-producing task is the analogue.
_asset = getattr(_resolve("airflow.sdk", "airflow.decorators"), "asset", None)

if _asset is not None:

    @_asset(
        schedule=None,
        dag_id="asset_outlet",
        name="publish",
        uri="asset://compat/corpus-output",
    )
    def publish() -> None:
        """Publish one synthetic corpus asset."""

else:
    _authoring = _resolve("airflow.decorators")
    _dataset_class = _resolve("airflow.datasets").Dataset

    @_authoring.dag(schedule=None, dag_id="asset_outlet", **_dag_kwargs())
    def asset_outlet() -> None:
        """Publish one synthetic corpus dataset."""

        @_authoring.task(outlets=[_dataset_class("asset://compat/corpus-output")])
        def publish() -> None:
            """Publish one synthetic corpus dataset."""

        publish()

    asset_outlet()
