"""Test package metadata and the public surface."""

from __future__ import annotations

import subprocess
import sys
from importlib.metadata import entry_points, version

import pytest

import pytest_airflow_in_a_box
from pytest_airflow_in_a_box import __version__


def test_version_matches_distribution_metadata() -> None:
    """Keep the package and distribution versions synchronized."""
    assert __version__ == version("pytest-airflow-in-a-box")


def test_public_surface_is_explicit() -> None:
    """Expose only the intentionally supported package symbols."""
    assert pytest_airflow_in_a_box.__all__ == ("__version__",)


def test_pytest_entry_point_loads_plugin() -> None:
    """Publish one loadable pytest entry point."""
    matching = entry_points(group="pytest11", name="pytest_airflow_in_a_box")
    entry_point = next(iter(matching))

    assert len(matching) == 1
    assert entry_point.value == "pytest_airflow_in_a_box.plugin"
    assert entry_point.load().__name__ == "pytest_airflow_in_a_box.plugin"


def test_plugin_import_does_not_import_airflow() -> None:
    """Keep the registered plugin safe to load before Airflow configuration."""
    script = (
        "import sys; import pytest_airflow_in_a_box.plugin; "
        "raise SystemExit('airflow' in sys.modules)"
    )

    subprocess.check_output([sys.executable, "-c", script], text=True)


def test_pytest_auto_loads_entry_point(pytester: pytest.Pytester) -> None:
    """Load the installed plugin without consumer conftest configuration."""
    pytester.makepyfile(
        """
        def test_plugin_is_registered(pytestconfig):
            assert pytestconfig.pluginmanager.hasplugin("pytest_airflow_in_a_box")
        """
    )

    result = pytester.runpytest_inprocess("-q")

    result.assert_outcomes(passed=1)
