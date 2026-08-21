"""Test the `airflow_home` and `airflow_dags_folder` fixtures.

Both are driven through nested sessions rather than requested directly, because the values
under test are properties of a bootstrapped run: the resolved run root, and the Dag folder
selected from the CLI option, the ini option, or the bootstrap scratch fallback.

References:
    https://github.com/nredd/pytest-airflow-in-a-box/issues/202
"""

from __future__ import annotations

import pytest


def test_airflow_home_is_the_run_root(pytester: pytest.Pytester) -> None:
    """Return the same directory bootstrap installed as `AIRFLOW_HOME`."""

    pytester.makepyfile(
        """
        import os
        from pathlib import Path


        def test_home(airflow_home):
            assert isinstance(airflow_home, Path)
            assert airflow_home == Path(os.environ["AIRFLOW_HOME"])
            assert (airflow_home / "airflow.cfg").is_file()
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)


def test_airflow_home_needs_no_database(pytester: pytest.Pytester) -> None:
    """Resolve the run root without importing Airflow or migrating the metadata database."""

    pytester.makepyfile(
        """
        import sys


        def test_home(airflow_home):
            assert airflow_home.is_dir()
            assert "airflow" not in sys.modules
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)


def test_dags_folder_path_falls_back_to_the_bootstrap_scratch_folder(
    pytester: pytest.Pytester,
) -> None:
    """Report the disposable scratch folder when neither option is configured."""

    pytester.makepyfile(
        """
        def test_folder(airflow_home, airflow_dags_folder):
            assert airflow_dags_folder == airflow_home / "dags"
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)


def test_dags_folder_path_follows_the_ini_option(pytester: pytest.Pytester) -> None:
    """Resolve a relative ini value against the pytest root, as `dag_bag` does."""

    pytester.mkdir("dags")
    pytester.makefile(
        ".ini",
        pytest="""
        [pytest]
        airflow_dags_folder = dags
        """,
    )
    pytester.makepyfile(
        """
        def test_folder(pytestconfig, airflow_dags_folder):
            assert airflow_dags_folder == pytestconfig.rootpath / "dags"
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)


def test_dags_folder_path_follows_the_command_line_option(pytester: pytest.Pytester) -> None:
    """Let `--dag-folder` win over the ini option, matching `dag_bag`'s ladder."""

    chosen = pytester.mkdir("elsewhere")
    pytester.mkdir("dags")
    pytester.makefile(
        ".ini",
        pytest="""
        [pytest]
        airflow_dags_folder = dags
        """,
    )
    pytester.makepyfile(
        f"""
        from pathlib import Path


        def test_folder(airflow_dags_folder):
            assert airflow_dags_folder == Path({str(chosen)!r})
        """
    )

    result = pytester.runpytest_subprocess("-q", f"--dag-folder={chosen}")

    result.assert_outcomes(passed=1)


def test_dags_folder_path_is_what_dag_bag_parsed(pytester: pytest.Pytester) -> None:
    """Agree with the Dag bag rather than re-deriving a folder that could drift."""

    dags = pytester.mkdir("dags")
    (dags / "one.py").write_text(
        "from airflow.sdk import DAG\n\none = DAG(dag_id='one')\n", encoding="utf-8"
    )
    pytester.makefile(
        ".ini",
        pytest="""
        [pytest]
        airflow_dags_folder = dags
        """,
    )
    pytester.makepyfile(
        """
        def test_folder(dag_bag, airflow_dags_folder):
            assert "one" in dag_bag.dags
            assert (airflow_dags_folder / "one.py").is_file()
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)
