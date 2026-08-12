"""Shared Airflow-family resolution helper for the corpus Dag files.

Bundled fixture data, not a Dag module -- `tests/dags/.airflowignore` excludes it from
DagBag discovery. DagBag imports every corpus file as a standalone module, so a plain
relative import will not survive that; sibling corpus files reach this helper the same
way `provider.py` reaches `provider_package`: insert `tests/dags` onto `sys.path`, then
`import_module("_family")`.
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
