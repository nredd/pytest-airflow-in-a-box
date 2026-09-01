"""Mirror of `pytest_airflow_in_a_box.fixtures` for the `piab` alias package.

References:
    https://github.com/nredd/pytest-airflow-in-a-box/issues/336
"""

from __future__ import annotations

# Bind the submodules eagerly, matching the attribute surface the canonical package's
# own submodule imports create as a side effect.
from piab.fixtures import api as api
from piab.fixtures import components as components
from piab.fixtures import configure as configure
from piab.fixtures import context as context
from piab.fixtures import dag as dag
from piab.fixtures import dagbag as dagbag
from piab.fixtures import dagcorpus as dagcorpus
from piab.fixtures import logging as logging
from piab.fixtures import paths as paths
from piab.fixtures import render as render
from piab.fixtures import seed as seed
from piab.fixtures import session as session
from piab.fixtures import taskrun as taskrun
from piab.fixtures import upstream as upstream
from pytest_airflow_in_a_box.fixtures import *
from pytest_airflow_in_a_box.fixtures import __all__ as __all__
