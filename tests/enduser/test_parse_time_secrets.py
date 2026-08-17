"""Exercise Variable and Connection lookups written at Dag top level.

Airflow 3 routes both through a supervisor the plugin has to stand in for during a
parse; Airflow 2 reads the metastore directly. The contract is the same on both, which
is why these tests author dual-family and run across the whole compatibility matrix.

References:
    https://github.com/apache/airflow/issues/51816
    https://github.com/nredd/pytest-airflow-in-a-box/issues/117
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.compat

CORPUS = Path(__file__).parents[1] / "dags"

DAG_SOURCE = '''
"""Dag whose Variable and Connection lookups run at import time, not in a task."""

import sys
from importlib import import_module

sys.path.insert(0, {corpus!r})
_family = import_module("_family")

_dag = _family._resolve("airflow.sdk", "airflow.models.dag")
_variable = _family._resolve("airflow.sdk", "airflow.models.variable")
_hook = _family._resolve("airflow.sdk.bases.hook", "airflow.hooks.base")

TOP_LEVEL_VARIABLE = _variable.Variable.get("parse_time_var")
TOP_LEVEL_HOST = _hook.BaseHook.get_connection("parse_time_conn").host

with _dag.DAG("parse_time_secrets", schedule=None, **_family._dag_kwargs()):
    pass
'''

SESSION_SEED_CONFTEST = '''
"""Commit the rows the Dag folder's top-level lookups need, before anything parses.

Collected Dag items are plain `pytest.Item`s with no fixture support, so seeding for
them belongs in `pytest_sessionstart` rather than an autouse fixture.
"""


def pytest_sessionstart(session):
    """Initialize the metadata database and commit the rows the Dag folder reads."""

    from airflow.models.connection import Connection
    from airflow.models.variable import Variable
    from airflow.utils.session import create_session

    from pytest_airflow_in_a_box._compat import ensure_database
    from pytest_airflow_in_a_box.bootstrap import get_bootstrap_state

    ensure_database(get_bootstrap_state(session.config).root)
    with create_session() as db_session:
        db_session.add(Variable(key="parse_time_var", val="from-metastore"))
        db_session.add(
            Connection(conn_id="parse_time_conn", conn_type="http", host="metastore-host")
        )
'''


def _dag_folder(pytester: pytest.Pytester) -> Path:
    """Write the top-level-lookup Dag into a folder of its own.

    Kept out of `tests/dags/` on purpose: the shared corpus has pinned Dag and item
    counts asserted across several modules, and its files must parse with no seeding.

    Parameters:
        pytester: pytest.Pytester owning the temporary directory.

    Returns:
        pathlib.Path containing the written Dag folder.
    """

    folder = pytester.path / "parse_time_dags"
    folder.mkdir()
    (folder / "toplevel_secrets.py").write_text(
        DAG_SOURCE.format(corpus=str(CORPUS)), encoding="utf-8"
    )
    return folder


def test_full_dag_bag_resolves_top_level_lookups(pytester: pytest.Pytester) -> None:
    """Parse a Dag whose Variable and Connection reads run at module scope.

    Seeding is function-scoped while `full_dag_bag` is session-scoped, so the parse is
    requested through `getfixturevalue` after the rows are committed rather than
    through the signature, which pytest would instantiate first.

    Parameters:
        pytester: pytest.Pytester running the nested session.
    """

    folder = _dag_folder(pytester)
    pytester.makepyfile(
        """
        import sys


        def test_top_level_lookups(request, airflow_variables, airflow_connections):
            airflow_variables({"parse_time_var": "from-metastore"})
            airflow_connections(
                {"parse_time_conn": {"conn_type": "http", "host": "metastore-host"}}
            )
            dag_bag = request.getfixturevalue("full_dag_bag")
            assert dag_bag.import_errors == {}
            fileloc = dag_bag.dags["parse_time_secrets"].fileloc
            parsed = [m for m in sys.modules.values() if getattr(m, "__file__", None) == fileloc]
            assert parsed, "the parsed Dag module is not in `sys.modules`"
            assert parsed[0].TOP_LEVEL_VARIABLE == "from-metastore"
            assert parsed[0].TOP_LEVEL_HOST == "metastore-host"
        """
    )

    result = pytester.runpytest_subprocess("-q", f"--dag-folder={folder}")

    result.assert_outcomes(passed=1)


def test_opting_out_reproduces_the_unshimmed_failure(pytester: pytest.Pytester) -> None:
    """Leave Airflow's own resolution in place under `--airflow-parse-secrets=off`.

    This is the regression canary for the day upstream lands its own fix: the parse
    fails here for as long as a supervisor-free Airflow 3 cannot resolve a seeded row.

    Parameters:
        pytester: pytest.Pytester running the nested session.
    """

    folder = _dag_folder(pytester)
    pytester.makepyfile(
        """
        import pytest

        from pytest_airflow_in_a_box._compat.capabilities import AirflowFamily, installed_family


        @pytest.mark.skipif(
            installed_family() is AirflowFamily.V2,
            reason="Airflow 2.x resolves top-level lookups from the metastore with no shim",
        )
        def test_top_level_lookups_fail(request, airflow_variables, airflow_connections):
            airflow_variables({"parse_time_var": "from-metastore"})
            airflow_connections(
                {"parse_time_conn": {"conn_type": "http", "host": "metastore-host"}}
            )
            dag_bag = request.getfixturevalue("full_dag_bag")
            assert len(dag_bag.import_errors) == 1
            assert next(iter(dag_bag.import_errors)).endswith("toplevel_secrets.py")
        """
    )

    result = pytester.runpytest_subprocess(
        "-q", f"--dag-folder={folder}", "--airflow-parse-secrets=off"
    )

    result.assert_outcomes(passed=1)


def test_collected_dag_items_resolve_top_level_lookups(pytester: pytest.Pytester) -> None:
    """Import-check a Dag whose top-level lookups need rows committed before collection.

    Collected Dag items run outside any seeding fixture, so the consumer commits the
    rows themselves from a session-scoped fixture; the shim reads them per lookup.

    Parameters:
        pytester: pytest.Pytester running the nested session.
    """

    folder = _dag_folder(pytester)
    pytester.makeconftest(SESSION_SEED_CONFTEST)

    result = pytester.runpytest_subprocess("-q", f"--collect-dag-folder={folder}")

    result.assert_outcomes(passed=1)


def test_fixture_resolves_lookups_outside_a_dag_parse(pytester: pytest.Pytester) -> None:
    """Resolve a lookup written in the test body, where no Dag parse is running.

    Parameters:
        pytester: pytest.Pytester running the nested session.
    """

    pytester.makepyfile(
        """
        from importlib import import_module


        def test_body_lookup(airflow_variables, airflow_parse_secrets):
            airflow_variables({"body_var": "body-value"})
            try:
                variable = import_module("airflow.sdk").Variable
            except ModuleNotFoundError:
                variable = import_module("airflow.models.variable").Variable
            assert variable.get("body_var") == "body-value"
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)
