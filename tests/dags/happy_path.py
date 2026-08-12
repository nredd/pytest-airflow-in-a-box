"""Single-task happy-path example Dag.

Bundled fixture data: parsed by the end-user compat suite and reusable as a
Dag-file collection corpus. The authoring surface resolves dynamically because the
corpus parses on BOTH Airflow families and no single environment can statically
resolve the other family's modules.
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
