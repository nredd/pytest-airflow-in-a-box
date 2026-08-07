"""Import-light pytest plugin entry point.

This module must remain safe to import before Apache Airflow.

References:
    https://docs.pytest.org/en/stable/reference/reference.html#hooks
    https://docs.pytest.org/en/stable/how-to/writing_plugins.html
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from pytest_airflow_in_a_box._compat import initialize_database
from pytest_airflow_in_a_box.bootstrap import (
    STATE_KEY,
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
from pytest_airflow_in_a_box.fixtures import (
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
from pytest_airflow_in_a_box.markers import register_markers
from pytest_airflow_in_a_box.reporting import configure_reporting

__all__ = (
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
        "allow_network_airflow_home",
        "Allow explicit Airflow storage on a network filesystem.",
        type="bool",
        default=False,
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
    """Install logging protection and initialize controller-owned metadata.

    Parameters:
        session: pytest.Session whose configuration contains bootstrap state.
    """

    _install_dict_config_interceptor()
    state = get_bootstrap_state(session.config)
    if state.owner_pid == os.getpid():
        initialize_database()


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
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Drop default-collector items that duplicate Dag-file collection.

    Parameters:
        config: pytest.Config containing plugin options and ini values.
        items: list[pytest.Item] mutated to exclude duplicate items.
    """

    prune_duplicate_items(config, items)


@pytest.hookimpl(optionalhook=True)
def pytest_configure_node(node: XdistNode) -> None:
    """Send controller bootstrap state to one local xdist worker.

    Parameters:
        node: XdistNode representing one worker controller.
    """

    configure_node(node)
