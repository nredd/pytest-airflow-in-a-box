"""Alias package mirroring `pytest_airflow_in_a_box`, attrs-style.

Every public module of `pytest_airflow_in_a_box` has a thin re-export shim here, so
`from piab.matchers import succeeded` resolves the same objects as the canonical package --
statically visible to type checkers, no `sys.modules` tricks, no double-imported module
state. `tests/test_piab.py` keeps the mirror honest. Importing `piab` itself stays inert,
matching the canonical package's import-light contract.

References:
    https://github.com/nredd/pytest-airflow-in-a-box/issues/336
"""

from __future__ import annotations

import importlib
import importlib.util
from types import ModuleType

from pytest_airflow_in_a_box import __version__

__all__ = ("__version__",)


def __getattr__(name: str) -> ModuleType:
    """Resolve mirror submodules lazily so `import piab; piab.matchers` works.

    Parameters:
        name: str attribute requested on the package.

    Returns:
        ModuleType mirror submodule imported on first access.

    Raises:
        AttributeError: `name` is underscore-prefixed or names no submodule.

    References:
        https://peps.python.org/pep-0562/
    """
    if name.startswith("_") or importlib.util.find_spec(f"{__name__}.{name}") is None:
        message = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(message)
    return importlib.import_module(f"{__name__}.{name}")
