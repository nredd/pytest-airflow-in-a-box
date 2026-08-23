"""Fan-out fixture Dag: team_b's single scheduled Dag.

Bundled fixture data for `tests/test_dag_bag_fanout.py` (issue #243); see `team_a/alpha.py`
for the corpus-wide scope note.
"""

from __future__ import annotations

from airflow.sdk import dag, task


@dag(schedule=None)
def fanout_beta() -> None:
    """Emit one deterministic value."""

    @task
    def emit() -> str:
        """Return a deterministic value.

        Returns:
            str containing the value.
        """

        return "beta"

    emit()


fanout_beta()
