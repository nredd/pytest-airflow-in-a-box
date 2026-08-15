"""Single-task happy-path example Dag.

Bundled fixture data: parsed by the end-user compat suite and reusable as a
Dag-file collection corpus. The authoring surface resolves dynamically because the
corpus parses on BOTH Airflow families and no single environment can statically
resolve the other family's modules.
"""

from __future__ import annotations

import sys
from importlib import import_module
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
_family = import_module("_family")
_resolve = _family._resolve
_dag_kwargs = _family._dag_kwargs

_authoring = _resolve("airflow.sdk", "airflow.decorators")
dag = _authoring.dag
task = _authoring.task


@dag(schedule=None, **_dag_kwargs())
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
