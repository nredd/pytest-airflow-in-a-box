"""Shared dual-family authoring resolver for the end-user contract modules.

pytest's default prepend import mode puts `tests/enduser` on `sys.path` before importing
any sibling module, so contract modules reach this helper the same way the corpus
reaches `tests/dags/_family.py`: `import_module("_authoring")`. The name is `_authoring`
rather than `_family` ON PURPOSE -- both directories' helpers load as top-level modules
in one process, and two same-named top-level modules would collide in `sys.modules`.

References:
    https://docs.pytest.org/en/stable/explanation/pythonpath.html#import-modes
"""

from __future__ import annotations

from importlib import import_module
from typing import Any


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
    """Import the first available module; the contract collects on both Airflow families.

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


def _resolve_exception(name: str) -> Any:
    """Resolve one exception class, preferring the Task SDK's re-export.

    `airflow.sdk.exceptions` (3.x) has not always re-exported every exception, so an
    absent module or attribute both fall back to the classic `airflow.exceptions`
    location, which every certified 2.x and 3.x release has.

    Parameters:
        name: str containing the exception class name.

    Returns:
        Any containing the resolved exception class.
    """

    try:
        return getattr(import_module("airflow.sdk.exceptions"), name)
    except (ImportError, AttributeError):
        return getattr(import_module("airflow.exceptions"), name)
