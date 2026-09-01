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
from typing import TYPE_CHECKING

from pytest_airflow_in_a_box import __version__

if TYPE_CHECKING:
    from piab import (
        airflow_cfg as airflow_cfg,
    )
    from piab import (
        antipatterns as antipatterns,
    )
    from piab import (
        artifact as artifact,
    )
    from piab import (
        assets as assets,
    )
    from piab import (
        baseline as baseline,
    )
    from piab import (
        bootstrap as bootstrap,
    )
    from piab import (
        certification as certification,
    )
    from piab import (
        collection as collection,
    )
    from piab import (
        components as components,
    )
    from piab import (
        config as config,
    )
    from piab import (
        dagcorpus as dagcorpus,
    )
    from piab import (
        db as db,
    )
    from piab import (
        defaults as defaults,
    )
    from piab import (
        doctor as doctor,
    )
    from piab import (
        fixtures as fixtures,
    )
    from piab import (
        ini_config as ini_config,
    )
    from piab import (
        isolated as isolated,
    )
    from piab import (
        isolated_child as isolated_child,
    )
    from piab import (
        logging as logging,
    )
    from piab import (
        markers as markers,
    )
    from piab import (
        matchers as matchers,
    )
    from piab import (
        migration as migration,
    )
    from piab import (
        migration_strict as migration_strict,
    )
    from piab import (
        parallel_dagbag as parallel_dagbag,
    )
    from piab import (
        parallel_dagbag_child as parallel_dagbag_child,
    )
    from piab import (
        parse_secrets as parse_secrets,
    )
    from piab import (
        plugin as plugin,
    )
    from piab import (
        record as record,
    )
    from piab import (
        reporting as reporting,
    )
    from piab import (
        results as results,
    )
    from piab import (
        smoke as smoke,
    )
    from piab import (
        storage as storage,
    )
    from piab import (
        taskinstance as taskinstance,
    )
    from piab import (
        types as types,
    )

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
