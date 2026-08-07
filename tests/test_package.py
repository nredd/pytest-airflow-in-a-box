"""Test package metadata and the public surface."""

from __future__ import annotations

import subprocess
import sys
from importlib.metadata import entry_points, version

import pytest

import pytest_airflow_in_a_box
from pytest_airflow_in_a_box import (
    __version__,
    _compat,
    collection,
    db,
    defaults,
    fixtures,
    logging,
    markers,
    plugin,
    reporting,
    taskinstance,
    types,
)


def test_version_matches_distribution_metadata() -> None:
    """Keep the package and distribution versions synchronized."""
    assert __version__ == version("pytest-airflow-in-a-box")


def test_public_surface_is_explicit() -> None:
    """Expose only the intentionally supported package symbols."""
    assert pytest_airflow_in_a_box.__all__ == ("__version__",)
    assert collection.__all__ == (
        "DagFile",
        "DagFileImportError",
        "DagImportItem",
        "collect_dag_file",
        "collection_folder",
        "format_import_errors",
        "prune_duplicate_items",
    )
    assert db.__all__ == ("DatabaseCleanupError", "TableGroup", "clear_db")
    assert defaults.__all__ == (
        "FILTERWARNINGS",
        "INI_DEFAULTS",
        "OPTION_DEFAULTS",
        "OptionDefault",
        "apply_filterwarnings",
        "apply_option_defaults",
        "register_ini_defaults",
    )
    assert fixtures.__all__ == (
        "cap_structlog",
        "dag_maker",
        "full_dag_bag",
        "run_task",
        "session",
    )
    assert logging.__all__ == ("StructlogCapture", "TestContextFilter", "ensure_handlers")
    assert markers.__all__ == ("MarkedNode", "read_bool_marker", "register_markers")
    assert plugin.__all__ == (
        "cap_structlog",
        "dag_maker",
        "full_dag_bag",
        "get_bootstrap_state",
        "run_task",
        "session",
    )
    assert reporting.__all__ == (
        "configure_reporting",
        "worker_coverage_file",
        "worker_suffixed_path",
    )
    assert taskinstance.__all__ == (
        "TaskResolutionError",
        "ordered_task_instances",
        "run_task_instance",
    )
    assert types.__all__ == ("DagMaker", "RunTask", "SerializedDag", "TaskRunResult")
    assert _compat.__all__ == (
        "AirflowCapabilities",
        "AirflowCompatibilityError",
        "DagBagConstructionError",
        "DagBagLocation",
        "DatabaseCleanupError",
        "DatabaseInitializationError",
        "FakeSupervisorComms",
        "InProcessRunResult",
        "TaskInstanceRunner",
        "build_dag_bag",
        "clear_tables",
        "implied_groups",
        "initialize_database",
        "resolve_capabilities",
        "run_task_in_process",
    )


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


def test_taskinstance_helpers_do_not_import_airflow() -> None:
    """Keep the public compatibility helpers safe before Airflow bootstrap."""

    script = (
        "import sys; import pytest_airflow_in_a_box.taskinstance; "
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

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)
