"""Mirror of `pytest_airflow_in_a_box.migration` for the `piab` alias package.

References:
    https://github.com/nredd/pytest-airflow-in-a-box/issues/336
"""

from __future__ import annotations

# Bind the submodules eagerly, matching the attribute surface the canonical package's
# own submodule imports create as a side effect.
from piab.migration import cli as cli
from piab.migration import provision as provision
from piab.migration import render as render
from piab.migration import run as run
from piab.migration import types as types
from pytest_airflow_in_a_box.migration import *
from pytest_airflow_in_a_box.migration import __all__ as __all__
