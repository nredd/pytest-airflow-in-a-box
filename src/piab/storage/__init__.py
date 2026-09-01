"""Mirror of `pytest_airflow_in_a_box.storage` for the `piab` alias package.

References:
    https://github.com/nredd/pytest-airflow-in-a-box/issues/336
"""

from __future__ import annotations

# Bind the submodules eagerly, matching the attribute surface the canonical package's
# own submodule imports create as a side effect.
from piab.storage import locate as locate
from piab.storage import postgres as postgres
from piab.storage import provision as provision
from piab.storage import sqlite as sqlite
from pytest_airflow_in_a_box.storage import *
from pytest_airflow_in_a_box.storage import __all__ as __all__
