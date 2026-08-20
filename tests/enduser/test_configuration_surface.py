"""Exercise the repo-wide configuration surface as a consumer sees it.

Three pieces ship together and are contract-tested together across the whole compatibility
matrix: the `airflow_config` ini option, the session-scoped `airflow_configure` fixture, and
the `airflow_home_path` / `airflow_dags_folder_path` path fixtures. All three are
family-independent -- they read pytest configuration and the plugin's own bootstrap state, and
touch neither the metadata database nor any versioned Airflow internal -- so nothing here is
gated behind `requires_airflow3`.

This repo deliberately ships no pytest ini section of its own (`defaults.py` supplies zero-ini
defaults), so the declarative half is exercised through a nested session with a real ini file
rather than by declaring overrides here.

References:
    https://github.com/nredd/pytest-airflow-in-a-box/issues/202
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from pytest_airflow_in_a_box.types import AirflowConfigure

pytestmark = pytest.mark.compat


def test_the_ini_option_is_registered(pytestconfig: pytest.Config) -> None:
    """Register `airflow_config` as a line list defaulting to no overrides."""

    assert pytestconfig.getini("airflow_config") == []


def test_declared_ini_overrides_reach_the_configuration_parser(
    pytester: pytest.Pytester,
) -> None:
    """Resolve a consumer's committed ini overrides through Airflow's own parser."""

    pytester.makefile(
        ".ini",
        pytest="""
        [pytest]
        airflow_config =
            core.dagbag_import_timeout = 45.5
        """,
    )
    pytester.makepyfile(
        """
        import os

        from airflow.configuration import conf


        def test_declared_override_is_visible():
            assert os.environ["AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT"] == "45.5"
            assert conf.get("core", "dagbag_import_timeout") == "45.5"
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)


def test_airflow_configure_applies_a_runtime_batch(
    airflow_configure: AirflowConfigure,
) -> None:
    """Apply a batch that only a running session could compute, and validate its arguments.

    The probe option is deliberately one no Airflow code path reads. This fixture's overrides
    are held until session teardown by design, so a batch applied here outlives this test, and
    an option with real behavior would silently reconfigure every later test in the worker.
    """

    airflow_configure({("pytest_airflow_in_a_box", "probe"): "applied"})

    assert os.environ["AIRFLOW__PYTEST_AIRFLOW_IN_A_BOX__PROBE"] == "applied"
    with pytest.raises(pytest.UsageError):
        airflow_configure({("core", "bad__key"): "1"})


def test_path_fixtures_resolve_without_reaching_into_bootstrap(
    airflow_home_path: Path, airflow_dags_folder_path: Path
) -> None:
    """Hand back both directories the plugin already resolves, as plain paths."""

    assert airflow_home_path == Path(os.environ["AIRFLOW_HOME"])
    assert (airflow_home_path / "airflow.cfg").is_file()
    assert airflow_dags_folder_path.is_dir()
