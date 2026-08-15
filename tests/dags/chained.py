"""Two-task example Dag with one inter-task dependency.

Bundled fixture data: parsed by the end-user compat suite and reusable as a
Dag-file collection corpus.
"""

from __future__ import annotations

import sys
from importlib import import_module
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))
_family = import_module("_family")
_resolve = _family._resolve
_dag_kwargs = _family._dag_kwargs

_authoring = _resolve("airflow.sdk", "airflow.decorators")
dag = _authoring.dag
task = _authoring.task


@dag(schedule=None, **_dag_kwargs())
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
