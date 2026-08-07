"""Two-task example Dag with one inter-task dependency.

Bundled fixture data: parsed by the end-user compat suite and reusable as a
Dag-file collection corpus.
"""

from __future__ import annotations

from typing import Any

from airflow.sdk import dag, task


@dag(schedule=None)
def chained() -> None:
    """Chain one producer into one consumer."""

    @task
    def produce() -> int:
        """Return the produced value.

        Returns:
            int containing the produced value.
        """

        return 21

    @task
    def consume(value: int) -> int:
        """Double the produced value.

        Parameters:
            value: int received from the producer.

        Returns:
            int containing the doubled value.
        """

        return value * 2

    produced: Any = produce()
    consume(produced)


chained()
