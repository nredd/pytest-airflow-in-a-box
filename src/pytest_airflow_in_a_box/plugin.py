"""Import-light pytest plugin entry point.

This module must remain safe to import before Apache Airflow.

References:
    https://docs.pytest.org/en/stable/reference/reference.html#hooks
    https://docs.pytest.org/en/stable/how-to/writing_plugins.html
"""

from __future__ import annotations

import os
import sys
import warnings
from pathlib import Path
from typing import TYPE_CHECKING, Final

import pytest

from pytest_airflow_in_a_box import _airflow_home, baseline, record, smoke
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
from pytest_airflow_in_a_box.certification import warn_if_airflow_uncertified
from pytest_airflow_in_a_box.collection import (
    DagFile,
    collect_dag_file,
    prune_duplicate_items,
)
from pytest_airflow_in_a_box.defaults import (
    apply_bootstrap_warning_filters,
    apply_default_filterwarnings,
    apply_filterwarnings,
    apply_option_defaults,
    register_default_filterwarnings_option,
    register_ini_defaults,
)
from pytest_airflow_in_a_box.doctor import render_doctor_report
from pytest_airflow_in_a_box.fixtures import (
    DATABASE_FIXTURE_NAMES,
    airflow_components,
    airflow_configure,
    airflow_connections,
    airflow_dags_folder,
    airflow_home,
    airflow_parse_secrets,
    airflow_variables,
    api_base_url,
    api_client,
    api_server_url,
    cap_structlog,
    dag_bag,
    dag_maker,
    render_task,
    run_dag,
    run_task,
    session,
    task_context,
)
from pytest_airflow_in_a_box.fixtures.dagbag import (
    DAG_BAG_FIXTURE_NAME,
    DAG_BAG_XDIST_GROUP,
)
from pytest_airflow_in_a_box.ini_config import apply_ini_overrides, validate_smoke_conflict
from pytest_airflow_in_a_box.isolated import (
    apply_xdist_refusal,
    store_batches,
)
from pytest_airflow_in_a_box.isolated import (
    runtest_protocol as isolated_runtest_protocol,
)
from pytest_airflow_in_a_box.logging import (
    _install_dict_config_interceptor,
    _uninstall_dict_config_interceptor,
)
from pytest_airflow_in_a_box.markers import (
    DATABASE_MARKER_NAMES,
    apply_environment_gate,
    apply_family_gate,
    register_markers,
)
from pytest_airflow_in_a_box.migration_strict import (
    apply_migration_strict_filterwarnings,
    warn_if_migration_strict_is_a_noop,
)
from pytest_airflow_in_a_box.reporting import configure_report_dir, configure_reporting
from pytest_airflow_in_a_box.results import assertrepr_compare

if TYPE_CHECKING:
    from collections.abc import Generator, Sequence

__all__ = (
    "airflow_components",
    "airflow_configure",
    "airflow_connections",
    "airflow_dags_folder",
    "airflow_home",
    "airflow_parse_secrets",
    "airflow_variables",
    "api_base_url",
    "api_client",
    "api_server_url",
    "cap_structlog",
    "dag_bag",
    "dag_maker",
    "get_bootstrap_state",
    "render_task",
    "run_dag",
    "run_task",
    "session",
    "task_context",
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
        help="Parse Dags from PATH for the dag_bag fixture.",
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
    parser.addini(
        "airflow_local_settings",
        "Dotted module path composed into the generated `airflow_local_settings.py`.",
        default="",
    )
    parser.addini(
        "airflow_plugins_folder",
        "Directory whose entries are symlinked into the run's `plugins/` directory.",
        default="",
    )
    parser.addini(
        "airflow_executor",
        "Executor written to `[core] executor` before the first Airflow import.",
        default="",
    )
    parser.addini(
        "airflow_executor_timeout",
        "Seconds one task instance may take to settle during an executor-driven "
        "`run_dag`, before the run fails naming the stuck instance.",
        default="300",
    )
    parser.addini(
        "airflow_xcom_backend",
        "XCom backend written to `[core] xcom_backend` before the first Airflow import.",
        default="",
    )
    parser.addini(
        "airflow_secrets_backend",
        "Secrets backend written to `[secrets] backend` before the first Airflow import.",
        default="",
    )
    parser.addini(
        "airflow_secrets_backend_kwargs",
        "Secrets backend kwargs written to `[secrets] backend_kwargs`.",
        default="",
    )
    group.addoption(
        "--airflow-executor-timeout",
        action="store",
        default=None,
        dest="airflow_executor_timeout",
        metavar="SECONDS",
        help="Seconds one task instance may take to settle during an executor-driven run.",
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
        "Directory parsed by the dag_bag fixture.",
        default="",
    )
    parser.addini(
        "airflow_collect_dags_folder",
        "Directory whose Dag files are collected as import-check test items.",
        default="",
    )
    group.addoption(
        "--airflow-parse-secrets",
        action="store",
        default=None,
        dest="airflow_parse_secrets",
        choices=("metastore", "off"),
        metavar="POLICY",
        help="Resolve top-level Dag Variable and Connection lookups during a parse.",
    )
    parser.addini(
        "airflow_parse_secrets",
        "Parse-time Variable and Connection resolution: `metastore` or `off`.",
        default="metastore",
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
        "airflow_config",
        "Airflow configuration applied process-wide, as `section.key = value` lines.",
        type="linelist",
        default=[],
    )
    parser.addini(
        "allow_network_airflow_home",
        "Allow explicit Airflow storage on a network filesystem.",
        type="bool",
        default=False,
    )
    smoke.register_options(parser)
    group.addoption(
        "--airflow-doctor",
        action="store_true",
        default=False,
        dest="airflow_doctor",
        help="Print a one-shot diagnostics report and exit without running tests.",
    )
    group.addoption(
        "--airflow-migration-strict",
        action="store_true",
        default=None,
        dest="airflow_migration_strict",
        help=(
            "Promote Airflow's own RemovedInAirflow3Warning and "
            "AirflowProviderDeprecationWarning to test-phase errors on a 2.x run; a "
            "no-op on 3.x."
        ),
    )
    parser.addini(
        "airflow_migration_strict",
        "Promote Airflow's 2->3 deprecation categories to test-phase errors on a 2.x run.",
        type="bool",
        default=False,
    )
    _airflow_home.register_options(parser)
    group.addoption(
        "--airflow-report-dir",
        action="store",
        default=None,
        dest="airflow_report_dir",
        metavar="PATH",
        help="Write `pytest.log` and `pytest.xml` report artifacts into PATH.",
    )
    parser.addini(
        "airflow_report_dir",
        "Directory receiving the `pytest.log` and `pytest.xml` report artifacts.",
        default="",
    )
    record.register_options(parser)
    baseline.register_options(parser)
    register_default_filterwarnings_option(parser)
    register_ini_defaults(parser)


def pytest_load_initial_conftests(
    early_config: pytest.Config,
    parser: pytest.Parser,
    args: list[str],
) -> None:
    """Install Airflow paths and configuration before importing consumer conftests.

    Bootstrap runs first and owns its own environment surface, then the ``airflow_config``
    ini option is applied on top. pytest has already parsed the ini file and folded any
    ``-o`` override into it by the time this hook is dispatched, and its own
    conftest-collecting hookimpl is ``trylast``, so declared overrides are in place before
    a single consumer conftest is imported -- and therefore before any Dag parse.

    Parameters:
        early_config: pytest.Config for initial command-line parsing.
        parser: pytest.Parser used during initial command-line parsing.
        args: list[str] containing the command-line arguments.

    Raises:
        pytest.UsageError: The `airflow_config` ini option is malformed.
    """
    del parser
    early_config.stash[STATE_KEY] = load_initial_state(early_config, args)
    apply_ini_overrides(early_config)


@pytest.hookimpl(tryfirst=True)
def pytest_cmdline_main(config: pytest.Config) -> int | None:
    """Print the diagnostics report and exit when `--airflow-doctor` is requested.

    Runs ``tryfirst`` so the report short-circuits the ordinary test session: no
    workers spawn and no collection happens. Bootstrap state is already available
    here because ``pytest_load_initial_conftests`` runs during argument parsing,
    which completes before pytest dispatches this hook.

    Short-circuiting also skips ``pytest_configure``, so the retention policy is resolved
    here instead. A diagnostic run is not a failed test run and discards its own bootstrap
    directory under the default policy, but an explicit ``--airflow-home-retention=all``
    still keeps the tree whose path the report just printed.

    Parameters:
        config: pytest.Config for the active invocation.

    Returns:
        int | None containing exit code 0 when the report was printed.
    """

    if not config.option.airflow_doctor:
        return None
    sys.stdout.write(render_doctor_report(config))
    _airflow_home.resolve_retention_policy(config)
    return 0


@pytest.hookimpl(tryfirst=True)
def pytest_configure(config: pytest.Config) -> None:
    """Register markers, validate xdist state, and configure report artifacts.

    Runs ``tryfirst`` so artifact paths are rewritten before pytest's logging and
    junitxml plugins read the ``log_file`` and ``xmlpath`` options during their own
    configuration. ``configure_report_dir`` runs before ``configure_reporting`` so a
    destination derived from ``--airflow-report-dir`` is scoped per xdist worker
    exactly like a user-supplied one.

    The ``AIRFLOW_HOME`` retention policy is resolved first and its value discarded:
    resolution caches onto the config stash, so a malformed ini value aborts the session
    with an actionable usage error instead of raising from the cleanup callback that
    reads it at unconfigure time, and an explicit ``--airflow-home-retention`` still
    governs cleanup when a later configure step fails.

    Parameters:
        config: pytest.Config for the active test session.
    """

    _airflow_home.resolve_retention_policy(config)
    register_markers(config)
    validate_configure(config)
    validate_smoke_conflict(config)
    configure_report_dir(config)
    configure_reporting(config)
    apply_option_defaults(config)
    apply_filterwarnings(config)
    apply_default_filterwarnings(config)
    record.configure(config)
    warn_if_migration_strict_is_a_noop(config)
    warn_if_airflow_uncertified(config)


def pytest_unconfigure(config: pytest.Config) -> None:
    """Restore process-global logging state when pytest shuts down.

    Parameters:
        config: pytest.Config for the completed test session.
    """

    record.unconfigure(config)
    _uninstall_dict_config_interceptor()


@pytest.hookimpl(tryfirst=True)
def pytest_sessionstart(session: pytest.Session) -> None:
    """Mark the session started, then install logging protection before anything else.

    The `AIRFLOW_HOME` mark goes first and this hookimpl runs ``tryfirst``, so a session
    that dies anywhere after startup begins -- a crashing conftest hookimpl, an internal
    error, a killed controller -- still reads as failed and keeps its run directory. An
    invocation that never starts a session at all (``--help``, ``--markers``, an argparse
    usage error, an abort during ``pytest_configure``) leaves the mark unset and its
    bootstrap directory is discarded.

    Database initialization is deliberately absent: it is deferred to the first
    test that requires the metadata database (see ``pytest_runtest_setup``), so
    sessions without Airflow-facing tests never import Airflow.

    Parameters:
        session: pytest.Session about to start collecting and running tests.
    """

    _airflow_home.mark_session_started(session.config)
    _install_dict_config_interceptor()


def pytest_report_header(config: pytest.Config) -> list[str]:
    """Name this run's isolated `AIRFLOW_HOME` in the session header.

    Bootstrap state is stashed eagerly during ``pytest_load_initial_conftests``, so the
    run root already exists by the time pytest collects header lines. ``BootstrapState``
    is a plain dataclass and reading it imports no Airflow, which keeps this module
    import-light. pytest suppresses the header entirely under ``-q`` and
    ``--no-header``, and prints only the controller's lines under xdist.

    Parameters:
        config: pytest.Config for the active test session.

    Returns:
        list[str] containing one header line, or no lines on an xdist worker.
    """

    return _airflow_home.report_header(get_bootstrap_state(config))


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
    """Drop duplicate Dag-file items, append the smoke catalog, then apply the baseline.

    `baseline.apply_selection_and_xfail` and the xdist co-location step run last: both
    must see the final collected item list, not an intermediate one, and co-location
    also needs the baseline's own deselection (`--airflow-baseline-select`) already
    applied so it does not group the catalog with a consumer that baseline just
    dropped.

    Parameters:
        session: pytest.Session that owns the synthetic smoke collector.
        config: pytest.Config containing plugin options and ini values.
        items: list[pytest.Item] mutated to exclude duplicates and include smoke items.
    """

    prune_duplicate_items(config, items)
    smoke_start = len(items)
    smoke.collect_smoke_items(session, config, items)
    # `collect_smoke_items` only ever appends (`items.extend(...)`), so the smoke
    # catalog is exactly this tail slice -- captured by id here, before
    # `apply_selection_and_xfail` can deselect some of it, and re-filtered against the
    # post-deselection `items` below so a deselected smoke item is not still treated as
    # live when co-location decides whether the catalog needs a worker to share.
    smoke_ids = {id(item) for item in items[smoke_start:]}
    baseline.apply_selection_and_xfail(session, config, items)
    smoke_items = [item for item in items if id(item) in smoke_ids]
    _colocate_smoke_catalog_with_dag_bag(items, smoke_items, config)


def _requires_dag_bag(item: pytest.Item) -> bool:
    """Report whether one collected test consumes the `dag_bag` fixture.

    Parameters:
        item: pytest.Item inspected for `dag_bag` fixture usage.

    Returns:
        bool reporting whether the item requires `dag_bag`.
    """

    fixturenames: tuple[str, ...] = tuple(getattr(item, "fixturenames", ()))
    return DAG_BAG_FIXTURE_NAME in fixturenames


def _survives_markexpr(item: pytest.Item, config: pytest.Config) -> bool:
    """Predict whether an active `-m` expression would keep one item selected.

    `_pytest.mark`'s own deselection hook is normal priority, so it has not run yet by
    the time this plugin's `tryfirst` collection hook decides xdist co-location --
    unlike `--airflow-baseline-select`, which this plugin applies itself earlier in the
    same hook. Predicting the `-m` result here avoids grouping the smoke catalog with a
    `dag_bag` consumer that `-m` is about to drop from the run anyway, wasting the
    catalog's own cross-worker distribution for no reuse benefit. Mirrors
    `smoke.py::_markexpr_wants_smoke`'s use of the same private, version-coupled
    symbol, deferred and exception-guarded the same way and for the same reason: an
    unparsable or unsupported expression is handled again, correctly, by pytest's own
    `-m` handling right after this hook returns, so failing open here is safe. `-k`
    keyword deselection is not predicted, matching `_markexpr_wants_smoke`'s own
    documented scope -- it runs later regardless, and replicating `KeywordMatcher`'s
    broader name-matching (item, parents, `extra_keyword_matches`) is not worth the
    extra private-API surface for this one heuristic.

    Parameters:
        item: pytest.Item to test against the active `-m` expression.
        config: pytest.Config containing the parsed `-m` option.

    Returns:
        bool reporting whether the item would likely survive `-m` deselection.
    """

    markexpr: str = config.option.markexpr
    if not markexpr:
        return True
    try:
        # Local import: deferred so a future pytest release relocating this private,
        # version-coupled symbol can't break collection for every plugin user -- only a
        # run that already passed `-m` ever reaches this branch.
        from _pytest.mark.expression import Expression

        mark_names = {mark.name for mark in item.iter_markers()}
        expression = Expression.compile(markexpr)
        return expression.evaluate(lambda name, /, **kwargs: name in mark_names and not kwargs)
    except Exception:
        return True


_PRE_GROUPED_ANCHOR_REASON: Final[str] = "carries an explicit `xdist_group` marker"
_DESELECTED_ANCHOR_REASON: Final[str] = "is about to be deselected by the active `-m` expression"


def _anchor_disqualification(item: pytest.Item, config: pytest.Config) -> str | None:
    """Report why one `dag_bag` consumer cannot anchor the smoke catalog's xdist group.

    Returning the reason rather than a bool is what lets co-location collect both the
    chosen anchor and, when there is none, the distinct reasons worth naming -- in a
    single pass over the collected items.

    Parameters:
        item: pytest.Item already known to require the `dag_bag` fixture.
        config: pytest.Config used to predict `-m` deselection.

    Returns:
        str | None naming the disqualifying reason, or None when the item is eligible.
    """

    if item.get_closest_marker("xdist_group") is not None:
        return _PRE_GROUPED_ANCHOR_REASON
    if not _survives_markexpr(item, config):
        return _DESELECTED_ANCHOR_REASON
    return None


def _xdist_worker_without_loadgroup(config: pytest.Config) -> bool:
    """Report whether this process is an xdist worker whose dist mode ignores groups.

    `xdist.remote.setup_config` resets a worker's `dist` option to `"no"` and its
    `numprocesses` to `None`, preserving only the synthetic `loadgroup` boolean, so the
    worker environment variable is the only remaining evidence that the run is being
    distributed at all. Checking the controller's `dist` string instead would be both
    unreachable and wrong: under real xdist the controller never runs
    `pytest_collection_modifyitems` (each worker collects for itself), and `--dist=load`
    passed without `-n` leaves `dist == "load"` while `tx` stays empty and nothing is
    actually distributed.

    Parameters:
        config: pytest.Config inspected for the surviving `loadgroup` signal.

    Returns:
        bool reporting whether groups are inert on a genuinely distributing worker.

    References:
        https://github.com/pytest-dev/pytest-xdist/blob/v3.8.0/src/xdist/remote.py#L392-L400
    """

    if os.environ.get(XDIST_WORKER_ENVIRONMENT_VARIABLE) is None:
        return False
    return not _loadgroup_dist_active(config)


def _warns_for_this_process() -> bool:
    """Report whether this process is the one that should record a co-location warning.

    Every xdist worker collects in its own process, so an unguarded warning issued from
    a collection hook is recorded once per worker -- N identical entries describing one
    run-wide condition. `gw0` always exists in a distributing run, so electing it loses
    no signal. A serial or in-process run has no worker variable at all and always
    reports.

    Returns:
        bool reporting whether this process should issue the warning.
    """

    worker = os.environ.get(XDIST_WORKER_ENVIRONMENT_VARIABLE)
    return worker is None or worker == "gw0"


def _warn_missing_dag_bag_anchor(reasons: Sequence[str]) -> None:
    """Warn that every `dag_bag` consumer is disqualified from anchoring the catalog.

    Issued from `pytest_collection_modifyitems` so pytest's own collection warnings
    context captures it into the terminal summary; `config.issue_config_time_warning`,
    used elsewhere in this plugin, is configure-time only and cannot reach this hook.
    Deliberately never issued for a run with no `dag_bag` consumer at all: the catalog
    then owns the only Dag parse in the run, so there is nothing to co-locate with and
    nothing lost -- warning there would fire on every ordinary smoke-only run.

    Parameters:
        reasons: Sequence[str] naming each disqualification, in discovery order.

    Returns:
        None. Issues at most one `SmokeColocationWarning`.
    """

    if not _warns_for_this_process():
        return
    joined = " or ".join(dict.fromkeys(reasons))
    warnings.warn(
        smoke.SmokeColocationWarning(
            f"Smoke catalog left ungrouped under `--dist loadgroup`: every test using "
            f"the `{DAG_BAG_FIXTURE_NAME}` fixture in this run {joined}, so none can "
            f"anchor the `{DAG_BAG_XDIST_GROUP}` group. The catalog's corpus builder "
            f"will parse the Dag folder itself, adding one full Dag parse to this run. "
            f"Leave one `{DAG_BAG_FIXTURE_NAME}` consumer ungrouped and selected to "
            f"avoid it, or silence this with "
            f"`-W ignore::pytest_airflow_in_a_box.smoke.SmokeColocationWarning`"
        ),
        stacklevel=1,
    )


def _warn_loadgroup_would_colocate() -> None:
    """Warn that the active dist mode makes the catalog's xdist grouping inert.

    `--dist` defaults to `no`, and a plain `-n auto` promotes it to `load`, never
    `loadgroup` -- so the common parallel invocation silently forgoes co-location even
    though this run has a `dag_bag` consumer to share a worker with.

    Returns:
        None. Issues at most one `SmokeColocationWarning`.

    References:
        https://github.com/pytest-dev/pytest-xdist/blob/v3.8.0/src/xdist/plugin.py#L318-L321
    """

    if not _warns_for_this_process():
        return
    warnings.warn(
        smoke.SmokeColocationWarning(
            f"Smoke catalog cannot share a worker with `{DAG_BAG_FIXTURE_NAME}` under "
            f"this dist mode: `xdist_group` only affects scheduling under "
            f"`--dist loadgroup`, and a plain `-n` run distributes with `--dist load`. "
            f"This run has at least one `{DAG_BAG_FIXTURE_NAME}` consumer to reuse a "
            f"parse from, so the catalog's corpus builder will parse the Dag folder a "
            f"second time. Pass `--dist loadgroup` to avoid it, or silence this with "
            f"`-W ignore::pytest_airflow_in_a_box.smoke.SmokeColocationWarning`"
        ),
        stacklevel=1,
    )


def _colocate_smoke_catalog_with_dag_bag(
    items: list[pytest.Item], smoke_items: list[pytest.Item], config: pytest.Config
) -> None:
    """Force the smoke catalog onto one `dag_bag` consumer's xdist worker.

    Under `--dist loadgroup`, an ungrouped smoke item can land on a different worker
    than a `dag_bag` consumer. Since the process-local live-DagBag cache
    (`fixtures/dagbag.py::LIVE_DAG_BAG_KEY`) only helps when both share a worker, that
    split causes the Dag folder to be parsed twice, in parallel. Grouping with a single
    consumer is enough to fix that: it guarantees the catalog's worker already has a
    cached `DagBag` to reuse, without forcing every other `dag_bag` consumer onto
    that same worker too -- which, for a suite with many such consumers, would trade
    one avoided parse for serializing all of their execution onto a single worker.

    An item that already carries its own explicit `xdist_group` (documented for
    seed-collision avoidance in `_compat/seed.py`) is never chosen or overwritten. Only
    applied when `--dist=loadgroup` is actually in effect: an `xdist_group` marker is
    inert under every other dist mode (including no xdist at all), and `xdist_group` is
    registered as a known marker only when the `xdist` plugin is actually loaded
    (`pytest_configure` in `xdist/plugin.py`), so adding it under `-p no:xdist` with
    `--strict-markers` would abort the run with an unregistered-marker error instead of
    a no-op.

    Consumers are found through pytest's full fixture *closure*, not a test's own
    signature, so a project fixture that itself declares `dag_bag` anchors the catalog
    exactly like a direct consumer. A consumer reaching the bag only through
    `request.getfixturevalue` is outside that closure, is invisible here, and gets no
    warning either. Every case where co-location is wanted but unreachable is reported
    with `SmokeColocationWarning`; a run with no `dag_bag` consumer at all is silent by
    design, because the catalog then owns the only Dag parse in the run and loses
    nothing.

    Parameters:
        items: list[pytest.Item] surviving collection, inspected for `dag_bag` use.
        smoke_items: list[pytest.Item] containing the synthesized smoke catalog items.
        config: pytest.Config used to detect `--dist=loadgroup` and predict `-m`.

    Returns:
        None. Mutates matching items in place by adding an `xdist_group` marker, and
        may issue `SmokeColocationWarning` when co-location is unreachable.
    """

    if not smoke_items:
        return
    if not _loadgroup_dist_active(config):
        if _xdist_worker_without_loadgroup(config) and any(
            _requires_dag_bag(item) for item in items
        ):
            _warn_loadgroup_would_colocate()
        return
    anchor: pytest.Item | None = None
    disqualifications: list[str] = []
    for item in items:
        if not _requires_dag_bag(item):
            continue
        reason = _anchor_disqualification(item, config)
        if reason is None:
            anchor = item
            break
        disqualifications.append(reason)
    if anchor is None:
        if disqualifications:
            _warn_missing_dag_bag_anchor(disqualifications)
        return
    for item in (*smoke_items, anchor):
        item.add_marker(pytest.mark.xdist_group(name=DAG_BAG_XDIST_GROUP))


def _loadgroup_dist_active(config: pytest.Config) -> bool:
    """Report whether the effective run is distributing tests via `--dist=loadgroup`.

    `config.getoption("dist")` alone is not reliable from inside this check: on a real
    xdist worker, `xdist.remote.setup_config` resets `config.option.dist` back to
    `"no"` (dist mode is a controller-only concept) after separately preserving the
    original choice as the synthetic boolean `config.option.loadgroup`. The controller
    (and a plain serial run) never gets that synthetic attribute, so its own
    `dist` option is read directly instead.

    Parameters:
        config: pytest.Config containing plugin options and ini values.

    Returns:
        bool reporting whether `--dist=loadgroup` is in effect for this process.

    References:
        https://github.com/pytest-dev/pytest-xdist/blob/v3.8.0/src/xdist/remote.py#L391-L393
    """

    return config.getoption("dist", default="no") == "loadgroup" or bool(
        config.getoption("loadgroup", default=False)
    )


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
        apply_family_gate(item)
        apply_environment_gate(item)
    except pytest.skip.Exception:
        return False
    except pytest.UsageError:
        # A malformed or unknown environment marker must fail at setup, not
        # abort collection; the database still initializes for the run.
        return True
    return True


def pytest_collection_finish(session: pytest.Session) -> None:
    """Apply migration-strict filters, stash isolated batches, then init the database.

    ``store_batches`` runs against the final deselected item list and aborts the
    session on a malformed `airflow_isolated` marker before any test runs.

    The migration-strict mutation runs first and unconditionally with respect to the
    worker check below it: pytest re-reads the `filterwarnings` ini list per warning
    context, so mutating it here (rather than `pytest_configure`) leaves the plugin's
    error filters absent during collection's own warning context and present for
    every runtest-phase warning context. On an xdist worker this hook still runs once
    per worker process, and each worker parses its own copy of the ini list, so every
    worker needs its own mutation -- unlike the database initialization below, which
    is genuinely once-per-run and skips workers.

    Database initialization runs after deselection so `pytest -k unrelated` stays
    free, and before the run phase so test execution never absorbs the one-time
    migration cost. Family- and environment-gated items that will skip do not
    trigger initialization.

    On an xdist worker the eager initialization is skipped: a `pytest.UsageError`
    raised from this hook escapes through execnet as a crashed-node traceback wall,
    once per worker, while the `pytest_runtest_setup` safety net renders the same
    message as ordinary per-test errors. Workers initialize on first database use
    through that path instead.

    Parameters:
        session: pytest.Session whose deselected item list is final.
    """

    apply_migration_strict_filterwarnings(session.config)
    store_batches(session)
    if os.environ.get(XDIST_WORKER_ENVIRONMENT_VARIABLE) is not None:
        return
    if any(_requires_database_at_collection(item) for item in session.items):
        _ensure_database_or_usage_error(session.config, get_bootstrap_state(session.config).root)


def _ensure_database_or_usage_error(config: pytest.Config, root: Path) -> None:
    """Initialize the metadata database, rendering incompatibility as a usage error.

    `AirflowCompatibilityError` describes an installation problem the user must fix
    (no Airflow, an unsupported family, or a corrupt environment). Left unhandled it
    surfaces as a pytest `INTERNALERROR` traceback wall. In a single-process run
    `pytest.UsageError` renders it as a single actionable `ERROR:` line from
    `pytest_collection_finish`; on xdist workers that hook defers to the
    `pytest_runtest_setup` safety net, where the same message renders as per-test
    errors instead of a crashed node.

    The `ensure_database` call is wrapped in its own default-filter warnings context,
    unconditionally, regardless of `--airflow-migration-strict` or any user-supplied
    `error::` filter. On an xdist worker the eager `pytest_collection_finish`
    initialization is skipped (see that hook's docstring), so this call runs from the
    `pytest_runtest_setup` safety net instead -- inside the runtest phase's own warning
    context. Airflow 2.11.2 raises `RemovedInAirflow3Warning` from
    `airflow.metrics.protocols` on first import, so under any active `error::` filter
    covering that category, every worker's *first* test fails on database
    initialization with a misleading `AirflowCompatibilityError`, misattributed by
    `_resolve_symbol`'s exception wrapping to an installation problem rather than the
    warning-turned-exception it actually is. This is a latent bug independent of
    migration-strict mode -- any consumer with their own `error::` filter over a
    warning Airflow's import-time bootstrap happens to raise hits it today.

    The plugin's own ``airflow_default_filterwarnings`` lines are re-applied on top of
    the reset default so plugin-sourced bootstrap noise (alembic's ``path_separator``
    deprecation) stays suppressed even inside this context; a consumer emptying that
    ini option restores full visibility.

    Parameters:
        config: pytest.Config carrying the cached bootstrap warning filters.
        root: Path containing the bootstrap run directory.

    Raises:
        pytest.UsageError: The installed Airflow environment is unusable.
    """

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("default")
            apply_bootstrap_warning_filters(config)
            ensure_database(root)
    except AirflowCompatibilityError as error:
        raise pytest.UsageError(str(error)) from error


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_protocol(item: pytest.Item, nextitem: pytest.Item | None) -> bool | None:
    """Run one `airflow_isolated` item through its batch's child pytest process.

    Runs ``tryfirst`` so a marked item's protocol is claimed before pytest's default
    ``runtestprotocol`` (or any consumer wrapper) executes it in process. Unmarked
    items -- and marked items inside the isolated child itself -- fall through to
    pytest's own protocol untouched.

    Parameters:
        item: pytest.Item about to enter its runtest protocol.
        nextitem: pytest.Item | None scheduled after this one, bounding the claimed
            protocol's setup-stack teardown.

    Returns:
        bool | None containing ``True`` when the item was replayed from a child run.
    """

    return isolated_runtest_protocol(item, nextitem)


def pytest_runtest_setup(item: pytest.Item) -> None:
    """Gate family- and environment-marked tests, then lazily initialize the database.

    The family and environment gates run first so a skipped test never pays the
    Airflow import and migration cost -- and so a gated `airflow_isolated` test skips
    under xdist exactly as it would serially (where the child applies the same gates),
    instead of tripping the refusal below. The `airflow_isolated`-under-xdist refusal
    runs from this hook rather than the protocol hook so an xdist worker renders it as
    an ordinary per-test error instead of a crashed node. The database check is a
    safety net for items injected after collection and the primary path on xdist
    workers; single-process runs initialize during ``pytest_collection_finish``.

    Parameters:
        item: pytest.Item about to enter its setup phase.

    Raises:
        pytest.UsageError: The item carries `airflow_isolated` on an xdist worker.
    """

    apply_family_gate(item)
    apply_environment_gate(item)
    apply_xdist_refusal(item)
    if _requires_database(item):
        _ensure_database_or_usage_error(item.config, get_bootstrap_state(item.config).root)


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


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(
    item: pytest.Item, call: pytest.CallInfo[None]
) -> Generator[None, pytest.TestReport, pytest.TestReport]:
    """Stash the migration-diff family-gate flag onto every phase report.

    Parameters:
        item: pytest.Item under test.
        call: pytest.CallInfo describing the executed phase.

    Yields:
        None, delegating report construction to inner hookimpls.

    Returns:
        pytest.TestReport carrying the stashed `gated` user property.
    """

    del call
    report = yield
    record.stash_gated_property(item, report)
    return report


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    """Accumulate one migration-diff outcome report, local or forwarded from xdist.

    Parameters:
        report: pytest.TestReport for one setup/call/teardown phase.
    """

    record.handle_logreport(report)


def pytest_terminal_summary(
    terminalreporter: pytest.TerminalReporter, exitstatus: int, config: pytest.Config
) -> None:
    """Render the `--airflow-baseline` migration diff summary and a retained run root.

    Parameters:
        terminalreporter: pytest.TerminalReporter receiving the rendered summary.
        exitstatus: int containing pytest's raw session exit status.
        config: pytest.Config containing plugin options and accumulated outcomes.
    """

    baseline.render_terminal_summary(terminalreporter, exitstatus, config)
    _airflow_home.terminal_summary(terminalreporter, config, get_bootstrap_state(config))


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Write the `--airflow-record` artifact and record the outcome for cleanup.

    The `AIRFLOW_HOME` cleanup closure registered during bootstrap is a
    ``config.add_cleanup`` callback with no view of the session outcome, and this is the
    hook that has one. pytest dispatches it before the terminal reporter's wrapper calls
    ``pytest_terminal_summary`` and long before any cleanup runs, so the recorded
    outcome is available to both.

    Parameters:
        session: pytest.Session that finished collecting and running tests.
        exitstatus: int containing pytest's raw session exit status.
    """

    record.write_recorded_artifact(session, exitstatus)
    _airflow_home.record_session_outcome(session.config, exitstatus)


@pytest.hookimpl(optionalhook=True)
def pytest_configure_node(node: XdistNode) -> None:
    """Send controller bootstrap state to one local xdist worker.

    Parameters:
        node: XdistNode representing one worker controller.
    """

    configure_node(node)
