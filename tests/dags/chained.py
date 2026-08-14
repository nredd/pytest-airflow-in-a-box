"""Two-task example Dag with one inter-task dependency.

Bundled fixture data: parsed by the end-user compat suite and reusable as a
Dag-file collection corpus.
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


_authoring = _resolve("airflow.sdk", "airflow.decorators")
dag = _authoring.dag
task = _authoring.task


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
