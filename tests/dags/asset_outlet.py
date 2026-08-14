"""Asset-decorator corpus Dag, expressed as a Dataset producer on Airflow 2.x.

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

    @_authoring.dag(schedule=None, dag_id="asset_outlet")
    def asset_outlet() -> None:
        """Publish one synthetic corpus dataset."""

        @_authoring.task(outlets=[_dataset_class("asset://compat/corpus-output")])
        def publish() -> None:
            """Publish one synthetic corpus dataset."""

        publish()

    asset_outlet()
