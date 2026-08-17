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
    artifact,
    baseline,
    collection,
    config,
    db,
    defaults,
    fixtures,
    logging,
    markers,
    matchers,
    plugin,
    record,
    reporting,
    results,
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
        "DagCasesDeclarationError",
        "DagFile",
        "DagFileImportError",
        "DagImportItem",
        "DagParamsCaseItem",
        "collect_dag_file",
        "collection_folder",
        "format_import_errors",
        "prune_duplicate_items",
        "read_declared_cases",
    )
    assert config.__all__ == (
        "ENV_VAR_PREFIX",
        "airflow_config",
        "conf_vars",
        "env_var_name",
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
        "airflow_connections",
        "airflow_parse_secrets",
        "airflow_variables",
        "api_base_url",
        "api_client",
        "api_server_url",
        "cap_structlog",
        "dag_maker",
        "full_dag_bag",
        "render_task",
        "run_dag",
        "run_task",
        "session",
    )
    assert logging.__all__ == ("StructlogCapture", "TestContextFilter", "ensure_handlers")
    assert markers.__all__ == (
        "MarkedNode",
        "apply_environment_gate",
        "apply_family_gate",
        "environment_sentinels",
        "read_bool_marker",
        "register_markers",
        "would_family_gate",
    )
    assert artifact.__all__ == (
        "ARTIFACT_SCHEMA_VERSION",
        "UNKNOWN",
        "Artifact",
        "Outcome",
        "OutcomeEntry",
        "load_artifact",
        "write_artifact",
    )
    assert record.__all__ == (
        "GATED_PROPERTY_NAME",
        "build_artifact",
        "build_live_artifact",
        "configure",
        "handle_logreport",
        "live_outcomes",
        "register_options",
        "stash_gated_property",
        "unconfigure",
        "write_recorded_artifact",
    )
    assert baseline.__all__ == (
        "SELECTORS",
        "CategoryBuckets",
        "apply_selection_and_xfail",
        "compute_categories",
        "register_options",
        "render_terminal_summary",
    )
    assert plugin.__all__ == (
        "airflow_connections",
        "airflow_parse_secrets",
        "airflow_variables",
        "api_base_url",
        "api_client",
        "api_server_url",
        "cap_structlog",
        "dag_maker",
        "full_dag_bag",
        "get_bootstrap_state",
        "render_task",
        "run_dag",
        "run_task",
        "session",
    )
    assert reporting.__all__ == (
        "configure_report_dir",
        "configure_reporting",
        "worker_coverage_file",
        "worker_suffixed_path",
    )
    assert taskinstance.__all__ == (
        "DEFAULT_TRIGGER_TIMEOUT",
        "TaskResolutionError",
        "TriggerExecutionError",
        "execute_dag_run",
        "ordered_task_instances",
        "run_task_instance",
        "run_trigger",
    )
    assert results.__all__ == (
        "DagRunResult",
        "TaskResult",
        "assertrepr_compare",
        "task_key",
    )
    assert matchers.__all__ == (
        "ANY",
        "RenderedFields",
        "TaskOutcome",
        "deferred",
        "failed",
        "not_run",
        "rendered",
        "skipped",
        "succeeded",
        "upstream_failed",
    )
    assert types.__all__ == (
        "AirflowConnections",
        "AirflowVariables",
        "DagMaker",
        "RenderTask",
        "RunDag",
        "RunTask",
        "SerializedDag",
        "TaskRunResult",
    )
    assert _compat.__all__ == (
        "AirflowCapabilities",
        "AirflowCompatibilityError",
        "DagBagConstructionError",
        "DagBagLocation",
        "DatabaseCleanupError",
        "DatabaseInitializationError",
        "FakeSupervisorComms",
        "InProcessRunResult",
        "ParamsCaseError",
        "ParseTimeComms",
        "SeedCleanupError",
        "SeedPersistenceError",
        "SeedRecord",
        "TaskInstanceRunner",
        "build_dag_bag",
        "cleanup_seeds",
        "clear_tables",
        "ensure_database",
        "implied_groups",
        "initialize_database",
        "open_seed_session",
        "parse_time_supervision",
        "render_task_in_process",
        "resolve_capabilities",
        "run_task_in_process",
        "seed_connections",
        "seed_variables",
        "validate_connections",
        "validate_dag_params",
        "validate_variables",
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


def test_config_helpers_do_not_import_airflow() -> None:
    """Keep the configuration override helpers safe before Airflow bootstrap."""

    script = (
        "import sys; import pytest_airflow_in_a_box.config; "
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
