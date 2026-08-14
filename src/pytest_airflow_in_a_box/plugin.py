"""Import-light pytest plugin entry point.

This module must remain safe to import before Apache Airflow.

References:
    https://docs.pytest.org/en/stable/reference/reference.html#hooks
    https://docs.pytest.org/en/stable/how-to/writing_plugins.html
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

from pytest_airflow_in_a_box._compat import AirflowCompatibilityError, ensure_database
from pytest_airflow_in_a_box.bootstrap import (
    STATE_KEY,
    XDIST_WORKER_ENVIRONMENT_VARIABLE,
    XdistNode,
    configure_node,
    get_bootstrap_state,
    load_initial_state,
    validate_configure,
)
from pytest_airflow_in_a_box.collection import (
    DagFile,
    collect_dag_file,
    prune_duplicate_items,
)
from pytest_airflow_in_a_box.defaults import (
    apply_filterwarnings,
    apply_option_defaults,
    register_ini_defaults,
)
from pytest_airflow_in_a_box.doctor import render_doctor_report
from pytest_airflow_in_a_box.fixtures import (
    DATABASE_FIXTURE_NAMES,
    airflow_connections,
    airflow_variables,
    api_base_url,
    api_client,
    api_server_url,
    cap_structlog,
    dag_maker,
    full_dag_bag,
    run_task,
    session,
)
from pytest_airflow_in_a_box.logging import (
    _install_dict_config_interceptor,
    _uninstall_dict_config_interceptor,
)
from pytest_airflow_in_a_box.markers import (
    DATABASE_MARKER_NAMES,
    apply_environment_gate,
    register_markers,
)
from pytest_airflow_in_a_box.reporting import configure_reporting
from pytest_airflow_in_a_box.results import assertrepr_compare
from pytest_airflow_in_a_box.smoke import collect_smoke_items

__all__ = (
    "airflow_connections",
    "airflow_variables",
    "api_base_url",
    "api_client",
    "api_server_url",
    "cap_structlog",
    "dag_maker",
    "full_dag_bag",
    "get_bootstrap_state",
    "run_task",
    "session",
)


@pytest.hookimpl(trylast=True)
def pytest_addoption(parser: pytest.Parser) -> None:
    """Register bootstrap options before pytest's early command-line parse.

    Runs ``trylast`` so re-registered builtin ini defaults land after
    pytest's own registrations; the last registration for a name wins.

    Parameters:
        parser: pytest.Parser receiving command-line and ini options.
    """

    group = parser.getgroup("airflow-in-a-box")
    group.addoption(
        "--dag-folder",
        action="store",
        default=None,
        dest="dag_folder",
        metavar="PATH",
        help="Parse Dags from PATH for the full_dag_bag fixture.",
    )
    group.addoption(
        "--airflow-home",
        action="store",
        default=None,
        dest="airflow_home",
        metavar="PATH",
        help="Create the isolated Airflow run directory below PATH.",
    )
    group.addoption(
        "--allow-network-airflow-home",
        action="store_true",
        default=None,
        dest="allow_network_airflow_home",
        help="Allow an explicit Airflow storage base on a network filesystem.",
    )
    parser.addini("airflow_home", "Base directory for isolated Airflow run storage.", default="")
    group.addoption(
        "--airflow-db-backend",
        action="store",
        default=None,
        dest="airflow_db_backend",
        choices=("sqlite", "postgres"),
        metavar="BACKEND",
        help="Select the isolated Airflow metadata database backend.",
    )
    parser.addini(
        "airflow_db_backend",
        "Metadata database backend: `sqlite` or `postgres`.",
        default="sqlite",
    )
    group.addoption(
        "--collect-dag-folder",
        action="store",
        default=None,
        dest="collect_dag_folder",
        metavar="PATH",
        help="Collect Dag files below PATH as import-check test items.",
    )
    parser.addini(
        "airflow_dags_folder",
        "Directory parsed by the full_dag_bag fixture.",
        default="",
    )
    parser.addini(
        "airflow_collect_dags_folder",
        "Directory whose Dag files are collected as import-check test items.",
        default="",
    )
    parser.addini(
        "airflow_environments",
        "Test environment sentinel paths as `name = path` lines.",
        type="linelist",
        default=[],
    )
    parser.addini(
        "airflow_pools",
        "Pools seeded before `test_pool_references_exist` runs, as `name = slots` lines.",
        type="linelist",
        default=[],
    )
    parser.addini(
        "allow_network_airflow_home",
        "Allow explicit Airflow storage on a network filesystem.",
        type="bool",
        default=False,
    )
    group.addoption(
        "--airflow-smoke",
        action="store_true",
        default=None,
        dest="airflow_smoke",
        help="Enable the bundled opt-in `smoke` test catalog.",
    )
    parser.addini(
        "airflow_smoke",
        "Enable the bundled opt-in `smoke` test catalog.",
        type="bool",
        default=False,
    )
    parser.addini(
        "airflow_dag_parse_timeout",
        "Per-file Dag parse timeout in seconds for the smoke integrity test.",
        default="30",
    )
    parser.addini(
        "airflow_dag_parse_slowpoke_ratio",
        "Fraction of the parse timeout above which a file is a slowpoke.",
        default="0.75",
    )
    parser.addini(
        "airflow_dag_id_pattern",
        "Regex every collected dag_id must match, checked by the smoke catalog.",
        default="",
    )
    parser.addini(
        "airflow_required_dag_tags",
        "Tags every collected Dag must carry, checked by the smoke catalog.",
        type="linelist",
        default=[],
    )
    parser.addini(
        "airflow_forbid_default_owner",
        "Fail Dags whose tasks are owned by the stock `airflow` owner.",
        type="bool",
        default=False,
    )
    group.addoption(
        "--airflow-smoke-update",
        action="store_true",
        default=None,
        dest="airflow_smoke_update",
        help="Regenerate committed Dag serialization snapshots instead of diffing them.",
    )
    parser.addini(
        "airflow_dag_snapshot_dir",
        "Directory of committed Dag serialization snapshots, checked by the smoke catalog.",
        default="",
    )
    parser.addini(
        "airflow_serialization_sample_size",
        "Number of Dags the serialization smoke checks cover; 0 covers every Dag.",
        default="0",
    )
    parser.addini(
        "airflow_serialization_sample_seed",
        "Seed for deterministic selection of the serialization smoke sample.",
        default="0",
    )
    group.addoption(
        "--airflow-doctor",
        action="store_true",
        default=False,
        dest="airflow_doctor",
        help="Print a one-shot diagnostics report and exit without running tests.",
    )
    register_ini_defaults(parser)


def pytest_load_initial_conftests(
    early_config: pytest.Config,
    parser: pytest.Parser,
    args: list[str],
) -> None:
    """Install Airflow paths and configuration before importing consumer conftests.

    Parameters:
        early_config: pytest.Config for initial command-line parsing.
        parser: pytest.Parser used during initial command-line parsing.
        args: list[str] containing the command-line arguments.
    """
    del parser
    early_config.stash[STATE_KEY] = load_initial_state(early_config, args)


@pytest.hookimpl(tryfirst=True)
def pytest_cmdline_main(config: pytest.Config) -> int | None:
    """Print the diagnostics report and exit when `--airflow-doctor` is requested.

    Runs ``tryfirst`` so the report short-circuits the ordinary test session: no
    workers spawn and no collection happens. Bootstrap state is already available
    here because ``pytest_load_initial_conftests`` runs during argument parsing,
    which completes before pytest dispatches this hook.

    Parameters:
        config: pytest.Config for the active invocation.

    Returns:
        int | None containing exit code 0 when the report was printed.
    """

    if not config.option.airflow_doctor:
        return None
    sys.stdout.write(render_doctor_report(config))
    return 0


@pytest.hookimpl(tryfirst=True)
def pytest_configure(config: pytest.Config) -> None:
    """Register markers, validate xdist state, and scope worker report artifacts.

    Runs ``tryfirst`` so worker artifact paths are rewritten before pytest's
    logging plugin reads the ``log_file`` option during its own configuration.

    Parameters:
        config: pytest.Config for the active test session.
    """

    register_markers(config)
    validate_configure(config)
    configure_reporting(config)
    apply_option_defaults(config)
    apply_filterwarnings(config)


def pytest_unconfigure(config: pytest.Config) -> None:
    """Restore process-global logging state when pytest shuts down.

    Parameters:
        config: pytest.Config for the completed test session.
    """

    del config
    _uninstall_dict_config_interceptor()


@pytest.hookimpl(tryfirst=True)
def pytest_sessionstart(session: pytest.Session) -> None:
    """Install logging protection before any test or plugin configures logging.

    Database initialization is deliberately absent: it is deferred to the first
    test that requires the metadata database (see ``pytest_runtest_setup``), so
    sessions without Airflow-facing tests never import Airflow.

    Parameters:
        session: pytest.Session about to start collecting and running tests.
    """

    del session
    _install_dict_config_interceptor()


@pytest.hookimpl(tryfirst=True)
def pytest_collect_file(file_path: Path, parent: pytest.Collector) -> DagFile | None:
    """Collect one file below the opt-in Dag collection directory.

    Parameters:
        file_path: pathlib.Path visited by pytest's collection walk.
        parent: pytest.Collector owning the new file node.

    Returns:
        DagFile | None containing the collector for an eligible Dag file.
    """

    return collect_dag_file(file_path, parent)


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(
    session: pytest.Session,
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:
    """Drop duplicate Dag-file items and append the bundled smoke catalog.

    Parameters:
        session: pytest.Session that owns the synthetic smoke collector.
        config: pytest.Config containing plugin options and ini values.
        items: list[pytest.Item] mutated to exclude duplicates and include smoke items.
    """

    prune_duplicate_items(config, items)
    collect_smoke_items(session, config, items)


def _requires_database(item: pytest.Item) -> bool:
    """Report whether one collected test declares or implies metadata database use.

    Parameters:
        item: pytest.Item inspected for database markers and fixture usage.

    Returns:
        bool reporting whether the item requires the metadata database.
    """

    if any(item.get_closest_marker(name) is not None for name in DATABASE_MARKER_NAMES):
        return True
    fixturenames: tuple[str, ...] = tuple(getattr(item, "fixturenames", ()))
    return not DATABASE_FIXTURE_NAMES.isdisjoint(fixturenames)


def _requires_database_at_collection(item: pytest.Item) -> bool:
    """Report whether one collected item will reach its setup phase needing the database.

    Parameters:
        item: pytest.Item surviving deselection at the end of collection.

    Returns:
        bool reporting whether the item requires the metadata database.
    """

    if not _requires_database(item):
        return False
    try:
        apply_environment_gate(item)
    except pytest.skip.Exception:
        return False
    except pytest.UsageError:
        # A malformed or unknown environment marker must fail at setup, not
        # abort collection; the database still initializes for the run.
        return True
    return True


def pytest_collection_finish(session: pytest.Session) -> None:
    """Initialize the database when the surviving collection requires it.

    Runs after deselection so `pytest -k unrelated` stays free, and before the
    run phase so test execution never absorbs the one-time migration cost.
    Environment-gated items that
    will skip do not trigger initialization.

    On an xdist worker the eager initialization is skipped: a `pytest.UsageError`
    raised from this hook escapes through execnet as a crashed-node traceback wall,
    once per worker, while the `pytest_runtest_setup` safety net renders the same
    message as ordinary per-test errors. Workers initialize on first database use
    through that path instead.

    Parameters:
        session: pytest.Session whose deselected item list is final.
    """

    if os.environ.get(XDIST_WORKER_ENVIRONMENT_VARIABLE) is not None:
        return
    if any(_requires_database_at_collection(item) for item in session.items):
        _ensure_database_or_usage_error(get_bootstrap_state(session.config).root)


def _ensure_database_or_usage_error(root: Path) -> None:
    """Initialize the metadata database, rendering incompatibility as a usage error.

    `AirflowCompatibilityError` describes an installation problem the user must fix
    (no Airflow, an unsupported family, or a corrupt environment). Left unhandled it
    surfaces as a pytest `INTERNALERROR` traceback wall. In a single-process run
    `pytest.UsageError` renders it as a single actionable `ERROR:` line from
    `pytest_collection_finish`; on xdist workers that hook defers to the
    `pytest_runtest_setup` safety net, where the same message renders as per-test
    errors instead of a crashed node.

    Parameters:
        root: Path containing the bootstrap run directory.

    Raises:
        pytest.UsageError: The installed Airflow environment is unusable.
    """

    try:
        ensure_database(root)
    except AirflowCompatibilityError as error:
        raise pytest.UsageError(str(error)) from error


def pytest_runtest_setup(item: pytest.Item) -> None:
    """Gate environment-marked tests, then lazily initialize the database.

    The environment gate runs first so a skipped test never pays the Airflow
    import and migration cost. The database check is a safety net for items
    injected after collection; ordinary runs initialize during
    ``pytest_collection_finish``.

    Parameters:
        item: pytest.Item about to enter its setup phase.
    """

    apply_environment_gate(item)
    if _requires_database(item):
        _ensure_database_or_usage_error(get_bootstrap_state(item.config).root)


def pytest_assertrepr_compare(
    config: pytest.Config,
    op: str,
    left: object,
    right: object,
) -> list[str] | None:
    """Render a per-task diff for one failed ``DagRunResult == Mapping`` assertion.

    Parameters:
        config: pytest.Config for the active test session.
        op: str containing the comparison operator pytest evaluated.
        left: object containing one side of the failed comparison.
        right: object containing the other side of the failed comparison.

    Returns:
        list[str] | None containing explanation lines for a bulk-outcome
        comparison, or ``None`` to keep pytest's default rendering.
    """

    del config
    return assertrepr_compare(op, left, right)


@pytest.hookimpl(optionalhook=True)
def pytest_configure_node(node: XdistNode) -> None:
    """Send controller bootstrap state to one local xdist worker.

    Parameters:
        node: XdistNode representing one worker controller.
    """

    configure_node(node)
