"""Single-task happy-path example Dag.

Bundled fixture data: parsed by the end-user compat suite and reusable as a
Dag-file collection corpus.
"""

from __future__ import annotations

from airflow.sdk import dag, task


@dag(schedule=None)
def happy_path() -> None:
    """Emit one deterministic greeting."""

    @task
    def greet() -> str:
        """Return a deterministic greeting.

        Returns:
            str containing the greeting.
        """

        return "hello"

    greet()


happy_path()
