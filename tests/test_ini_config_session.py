"""Drive the `airflow_config` ini option through real plugin-bootstrapped sessions.

`tests/test_ini_config.py` covers the grammar against a configuration double. These tests
cover the things only a real session can show: that overrides land before a consumer conftest
is imported, that they survive to an xdist worker without tripping the inherited-state drift
check, and that a denied option aborts with the remedy visible.

References:
    https://github.com/nredd/pytest-airflow-in-a-box/issues/202
"""

from __future__ import annotations

import pytest

INI = """
[pytest]
airflow_config =
    core.dagbag_import_timeout = 12.5
    core.default_task_retries = 7
"""


def test_overrides_reach_the_configuration_parser(pytester: pytest.Pytester) -> None:
    """Resolve a declared override through Airflow's own parser, not just `os.environ`."""

    pytester.makefile(".ini", pytest=INI)
    pytester.makepyfile(
        """
        from airflow.configuration import conf


        def test_declared_override_is_visible():
            assert conf.get("core", "dagbag_import_timeout") == "12.5"
            assert conf.get("core", "default_task_retries") == "7"
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)


def test_overrides_precede_consumer_conftest_import(pytester: pytest.Pytester) -> None:
    """Assign every declared variable before pytest imports a single consumer conftest.

    The assertion runs at conftest module scope, which is the earliest consumer code any
    session executes and strictly earlier than any Dag parse.
    """

    pytester.makefile(".ini", pytest=INI)
    pytester.makeconftest(
        """
        import os

        assert os.environ["AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT"] == "12.5"
        """
    )
    pytester.makepyfile("def test_session_ran(): pass")

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)


def test_overrides_precede_the_dag_bag_parse(pytester: pytest.Pytester) -> None:
    """Apply overrides before `dag_bag` builds its one-per-process Dag bag.

    The Dag file reads the option through Airflow's own parser at import time and encodes the
    answer in its `dag_id`, so the assertion can only pass if the override was live during the
    parse rather than merely by the time the test body ran.
    """

    pytester.makefile(
        ".ini",
        pytest="""
        [pytest]
        airflow_dags_folder = dags
        airflow_config =
            core.dagbag_import_timeout = 12.5
        """,
    )
    dags = pytester.mkdir("dags")
    (dags / "probe.py").write_text(
        "from airflow.configuration import conf\n"
        "from airflow.sdk import DAG\n"
        "\n"
        "timeout = conf.get('core', 'dagbag_import_timeout').replace('.', '_')\n"
        "probe = DAG(dag_id=f'probe_{timeout}')\n",
        encoding="utf-8",
    )
    pytester.makepyfile(
        """
        def test_override_was_live_during_the_parse(dag_bag):
            assert not dag_bag.import_errors
            assert "probe_12_5" in dag_bag.dags
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)


def test_overrides_survive_to_an_xdist_worker(pytester: pytest.Pytester) -> None:
    """Reach a worker process without desynchronizing it from the controller's state."""

    pytester.makefile(".ini", pytest=INI)
    pytester.makepyfile(
        """
        from airflow.configuration import conf


        def test_worker_sees_the_override():
            assert conf.get("core", "dagbag_import_timeout") == "12.5"
        """
    )

    result = pytester.runpytest_subprocess("-q", "-p", "xdist", "-n", "2")

    result.assert_outcomes(passed=1)
    assert "disagrees with state" not in "\n".join(result.outlines + result.errlines)


def test_a_denied_option_aborts_with_its_remedy(pytester: pytest.Pytester) -> None:
    """Fail the session naming the supported knob instead of breaking database isolation."""

    pytester.makepyfile("def test_never_runs(): pass")

    result = pytester.runpytest_subprocess(
        "-q", "-o", "airflow_config=database.sql_alchemy_conn = postgresql://nope"
    )

    assert result.ret != 0
    result.stderr.fnmatch_lines(["*may not set `database.sql_alchemy_conn`*"])
    result.stderr.fnmatch_lines(["*--airflow-db-backend*"])


def test_a_command_line_override_ini_applies(pytester: pytest.Pytester) -> None:
    """Accept the option through `-o`, which pytest folds in before this plugin reads it."""

    pytester.makepyfile(
        """
        from airflow.configuration import conf


        def test_command_line_override_is_visible():
            assert conf.get("core", "dagbag_import_timeout") == "9.5"
        """
    )

    result = pytester.runpytest_subprocess(
        "-q", "-o", "airflow_config=core.dagbag_import_timeout = 9.5"
    )

    result.assert_outcomes(passed=1)


def test_the_parse_timeout_conflicts_with_an_enabled_smoke_catalog(
    pytester: pytest.Pytester,
) -> None:
    """Refuse a declared parse timeout the catalog would pin over, naming its own knob."""

    pytester.makepyfile("def test_never_runs(): pass")

    result = pytester.runpytest_subprocess(
        "-q", "--airflow-smoke", "-o", "airflow_config=core.dagbag_import_timeout = 120"
    )

    assert result.ret != 0
    result.stderr.fnmatch_lines(["*airflow_dag_parse_timeout*"])


def test_the_conflict_check_leaves_the_catalog_enabled(pytester: pytest.Pytester) -> None:
    """Resolve smoke enablement without poisoning the catalog's own memoized answer.

    Reading `--airflow-smoke` during the initial parse would cache a wrong `False` and silently
    drop every bundled item, which no assertion about the overrides themselves would catch.
    """

    pytester.makefile(
        ".ini",
        pytest="""
        [pytest]
        airflow_config =
            core.default_task_retries = 7
        """,
    )
    dags = pytester.mkdir("dags")
    (dags / "one.py").write_text(
        "from airflow.sdk import DAG\n\none = DAG(dag_id='one', schedule=None)\n",
        encoding="utf-8",
    )
    pytester.makepyfile("def test_own_item(): pass")

    result = pytester.runpytest_subprocess(
        "-q", "--airflow-smoke", f"--dag-folder={dags}", "--collect-only"
    )

    assert result.ret == 0
    result.stdout.fnmatch_lines(["*test_dag_bag_integrity*"])
