"""Public package surface for pytest-airflow-in-a-box.

References:
    https://docs.pytest.org/en/stable/how-to/writing_plugins.html
"""

from __future__ import annotations

import importlib
import importlib.util
from types import ModuleType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pytest_airflow_in_a_box import (
        airflow_cfg as airflow_cfg,
    )
    from pytest_airflow_in_a_box import (
        antipatterns as antipatterns,
    )
    from pytest_airflow_in_a_box import (
        artifact as artifact,
    )
    from pytest_airflow_in_a_box import (
        assets as assets,
    )
    from pytest_airflow_in_a_box import (
        baseline as baseline,
    )
    from pytest_airflow_in_a_box import (
        bootstrap as bootstrap,
    )
    from pytest_airflow_in_a_box import (
        certification as certification,
    )
    from pytest_airflow_in_a_box import (
        collection as collection,
    )
    from pytest_airflow_in_a_box import (
        components as components,
    )
    from pytest_airflow_in_a_box import (
        config as config,
    )
    from pytest_airflow_in_a_box import (
        dagcorpus as dagcorpus,
    )
    from pytest_airflow_in_a_box import (
        db as db,
    )
    from pytest_airflow_in_a_box import (
        defaults as defaults,
    )
    from pytest_airflow_in_a_box import (
        doctor as doctor,
    )
    from pytest_airflow_in_a_box import (
        fixtures as fixtures,
    )
    from pytest_airflow_in_a_box import (
        ini_config as ini_config,
    )
    from pytest_airflow_in_a_box import (
        isolated as isolated,
    )
    from pytest_airflow_in_a_box import (
        isolated_child as isolated_child,
    )
    from pytest_airflow_in_a_box import (
        logging as logging,
    )
    from pytest_airflow_in_a_box import (
        markers as markers,
    )
    from pytest_airflow_in_a_box import (
        matchers as matchers,
    )
    from pytest_airflow_in_a_box import (
        migration as migration,
    )
    from pytest_airflow_in_a_box import (
        migration_strict as migration_strict,
    )
    from pytest_airflow_in_a_box import (
        parallel_dagbag as parallel_dagbag,
    )
    from pytest_airflow_in_a_box import (
        parallel_dagbag_child as parallel_dagbag_child,
    )
    from pytest_airflow_in_a_box import (
        parse_secrets as parse_secrets,
    )
    from pytest_airflow_in_a_box import (
        plugin as plugin,
    )
    from pytest_airflow_in_a_box import (
        record as record,
    )
    from pytest_airflow_in_a_box import (
        reporting as reporting,
    )
    from pytest_airflow_in_a_box import (
        results as results,
    )
    from pytest_airflow_in_a_box import (
        smoke as smoke,
    )
    from pytest_airflow_in_a_box import (
        storage as storage,
    )
    from pytest_airflow_in_a_box import (
        taskinstance as taskinstance,
    )
    from pytest_airflow_in_a_box import (
        types as types,
    )

__version__ = "0.13.1"

__all__ = ("__version__",)


def __getattr__(name: str) -> ModuleType:
    """Resolve public submodules lazily so `import pytest_airflow_in_a_box as piab` works.

    Eagerly importing submodules here would defeat the import-light contract, and
    `from piab.module import name` off a bare alias cannot work at all -- `as` only binds a
    local name while `from X import Y` re-resolves `X` through `sys.modules` by its literal
    string (the shipped `piab` mirror package covers that spelling instead). Attribute
    access off the alias (`piab.matchers.succeeded`) is what this supports, per PEP 562;
    the `TYPE_CHECKING` re-exports above give the same attributes real static types.

    Parameters:
        name: str attribute requested on the package.

    Returns:
        ModuleType public submodule imported on first access.

    Raises:
        AttributeError: `name` is underscore-prefixed or names no submodule.

    References:
        https://peps.python.org/pep-0562/
        https://github.com/nredd/pytest-airflow-in-a-box/issues/336
    """
    if name.startswith("_") or importlib.util.find_spec(f"{__name__}.{name}") is None:
        message = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(message)
    return importlib.import_module(f"{__name__}.{name}")
