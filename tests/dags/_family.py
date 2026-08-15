"""Shared Airflow-family resolution helper for the corpus Dag files.

Bundled fixture data, not a Dag module -- `tests/dags/.airflowignore` excludes it from
DagBag discovery. DagBag imports every corpus file as a standalone module, so a plain
relative import will not survive that; sibling corpus files reach this helper the same
way `provider.py` reaches `provider_package`: insert `tests/dags` onto `sys.path`, then
`import_module("_family")`.
"""

from __future__ import annotations

from datetime import datetime, timezone
from importlib import import_module
from typing import Any

from packaging.version import Version

# Airflow below 2.8 rejects a Dag whose tasks carry no `start_date`, scheduled or not.
_START_DATE_REQUIRED_BELOW = (2, 8)
_CORPUS_START_DATE = datetime(2024, 1, 1, tzinfo=timezone.utc)


def _absent(name: str, error: ModuleNotFoundError) -> bool:
    """Report whether the failure means the candidate module itself is absent.

    A bare `except ImportError` would also swallow a failure raised INSIDE a present
    module (a broken transitive import), silently misattributing it to the family
    probe; only a `ModuleNotFoundError` naming the candidate or one of its parent
    packages is a real family miss.

    Parameters:
        name: str containing the probed candidate module path.
        error: ModuleNotFoundError raised by the candidate import.

    Returns:
        bool marking the candidate (or a parent package) as the missing module.
    """

    return error.name is not None and (name == error.name or name.startswith(f"{error.name}."))


def _resolve(*candidates: str) -> Any:
    """Import the first available module; the corpus parses on both Airflow families.

    Parameters:
        candidates: str module paths ordered newest family first.

    Returns:
        Any containing the first importable module.

    Raises:
        ModuleNotFoundError: A candidate failed for a reason other than its own
            absence, or the final candidate is absent too.
    """

    for name in candidates[:-1]:
        try:
            return import_module(name)
        except ModuleNotFoundError as error:
            if not _absent(name, error):
                raise
            continue
    return import_module(candidates[-1])


def _dag_kwargs() -> dict[str, Any]:
    """Return the extra Dag keyword arguments the installed Airflow release demands.

    Airflow below 2.8 raises `DAG is missing the start_date parameter` from
    `DAG.add_task` whenever neither the Dag nor the task carries a `start_date`, even
    for an unscheduled Dag; 2.8 narrowed that to Dags declaring a schedule. Every later
    release therefore parses these files unchanged, so the corpus keeps its authoring
    surface minimal where it can and pays the argument only where it must.

    Returns:
        dict[str, Any] containing `start_date` on releases that require one, else an
        empty mapping.
    """

    release = Version(import_module("airflow").__version__).release
    if release[:2] < _START_DATE_REQUIRED_BELOW:
        return {"start_date": _CORPUS_START_DATE}
    return {}
