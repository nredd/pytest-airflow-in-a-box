"""Asset-decorator corpus Dag."""

from __future__ import annotations

from airflow.sdk import asset


@asset(
    schedule=None,
    dag_id="asset_outlet",
    name="publish",
    uri="asset://compat/corpus-output",
)
def publish() -> None:
    """Publish one synthetic corpus asset."""
