"""Fan-out fixture Dag: team_a's single scheduled Dag.

Bundled fixture data for `tests/test_dag_bag_fanout.py` (issue #243), not enduser
compat-suite data: it exercises the fan-out subprocess mechanism on whichever Airflow 3.x
release this run installs, and is not asserted identical across the 2.x family.
"""

from __future__ import annotations

from airflow.sdk import dag, task


@dag(schedule=None)
def fanout_alpha() -> None:
    """Emit one deterministic value."""

    @task
    def emit() -> str:
        """Return a deterministic value.

        Returns:
            str containing the value.
        """

        return "alpha"

    emit()


fanout_alpha()
