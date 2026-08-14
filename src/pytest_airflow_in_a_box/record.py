"""Accumulate per-test outcomes and write the `--airflow-record` migration artifact.

Outcome derivation is a `pytest_runtest_logreport` accumulator keyed by nodeid,
finalized when a terminal report for that nodeid arrives (normally `teardown`; see
below for the other terminal case): a failed teardown or setup report always wins as
`error` (carrying the failing `phase`); otherwise the call report decides
`passed`/`xpassed`/`failed`/`skipped`/`xfailed`; a setup-only skip (no call report at
all) is `skipped`. `pytest_runtest_logreport` is the one hook that uniformly receives
every report regardless of xdist: on an xdist controller it receives reports forwarded
(and reserialized) from workers, so nothing beyond `report.user_properties` survives
the hop -- which is exactly why the `gated` flag is stashed there by a
`pytest_runtest_makereport` hookwrapper rather than recomputed from `report.nodeid`
alone.

A crashed xdist worker is the other terminal case: `xdist.dsession.DSession.
handle_crashitem` synthesizes exactly one report with `when="???"` for the item the
worker was running and fires it through `pytest_runtest_logreport`, with no setup/call/
teardown reports to follow. Any report whose `when` is not `setup`/`call` is therefore
treated as terminal, not only `teardown` -- otherwise that nodeid's accumulator would
sit unfinished forever and silently vanish from the artifact's `outcomes` mapping
(rather than merely being misclassified, which would at least be visible).

`pytest_runtest_logreport` is the one lifecycle hook with no `config`/`item` parameter,
so this module tracks the active `pytest.Config` stack for the current process as a
module-level reference, pushed in `configure()`/popped in `unconfigure()`. A stack, not
a single slot: a process running this plugin normally has exactly one active pytest
session at a time (serial runs, and each xdist controller and worker, are all one
`Config` per process for their lifetime), but a nested in-process `pytest.main()` call
pushes a second session strictly within the outer session's `configure`/`unconfigure`
window, and popping back to the outer session on the nested one's `unconfigure` is what
keeps the outer session's own accumulation from silently going dark for its remainder.

References:
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.hookspec.pytest_runtest_logreport
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.hookspec.pytest_runtest_makereport
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.hookspec.pytest_sessionfinish
    https://pytest-xdist.readthedocs.io/en/stable/how-to.html
    https://github.com/pytest-dev/pytest-xdist/blob/v3.8.0/src/xdist/dsession.py#L432-L452
"""

from __future__ import annotations

import logging
import os
import platform
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path

import pytest

from pytest_airflow_in_a_box import __version__
from pytest_airflow_in_a_box._compat.capabilities import installed_family
from pytest_airflow_in_a_box.artifact import (
    UNKNOWN,
    Artifact,
    Outcome,
    OutcomeEntry,
    write_artifact,
)
from pytest_airflow_in_a_box.bootstrap import XDIST_WORKER_ENVIRONMENT_VARIABLE, XdistWorkerConfig
from pytest_airflow_in_a_box.markers import would_family_gate

LOGGER = logging.getLogger(__name__)

GATED_PROPERTY_NAME = "pytest_airflow_in_a_box_gated"

_ACTIVE_KEY = pytest.StashKey[bool]()
_ACCUMULATOR_KEY = pytest.StashKey[dict[str, "_NodeAccumulator"]]()
_OUTCOMES_KEY = pytest.StashKey[dict[str, OutcomeEntry]]()
# The one side-channel `pytest_runtest_logreport` needs; see the module docstring. A
# stack so a nested in-process `pytest.main()` session can push/pop its own entry
# without discarding the outer session's, which is still mid-run underneath it.
_ACTIVE_CONFIG_STACK: list[pytest.Config] = []

# `xdist.dsession.DSession.handle_crashitem` synthesizes a report with this exact
# `when` value for an item whose worker crashed mid-test; see the module docstring.
_CRASH_WHEN = "???"


@dataclass
class _NodeAccumulator:
    """Mutable per-nodeid state accumulated across setup/call/teardown reports.

    Parameters:
        gated: bool recording whether the family gate would apply to this item.
        duration: float summing the duration of every phase report seen so far.
        setup_failed: bool recording a failed setup phase.
        call_outcome: str | None containing the raw call-phase `report.outcome`, or
            `None` when no call report has arrived (the item was skipped or failed
            in setup, so pytest's own protocol never entered the call phase).
        call_wasxfail: bool recording an xfail-marked call phase (`report.wasxfail`).
        teardown_failed: bool recording a failed teardown phase.
        crashed: bool recording an xdist worker crash report (`when=` neither
            `setup`, `call`, nor `teardown`) for this nodeid.
    """

    gated: bool = False
    duration: float = 0.0
    setup_failed: bool = False
    call_outcome: str | None = None
    call_wasxfail: bool = False
    teardown_failed: bool = False
    crashed: bool = False


def register_options(parser: pytest.Parser) -> None:
    """Register `--airflow-record`.

    Parameters:
        parser: pytest.Parser receiving command-line options.
    """

    group = parser.getgroup("airflow-in-a-box")
    group.addoption(
        "--airflow-record",
        action="store",
        default=None,
        dest="airflow_record",
        metavar="PATH",
        help="Write a migration outcome artifact to PATH at session finish.",
    )


def _is_recording_active(config: pytest.Config) -> bool:
    """Report whether any migration-diff option makes outcome accumulation worthwhile.

    Accumulation is otherwise skipped: every test would pay an `installed_family()`
    metadata read per phase for a feature most sessions never use.

    Parameters:
        config: pytest.Config containing parsed command-line options.

    Returns:
        bool indicating whether `--airflow-record` or a `--airflow-baseline*` option
        was supplied.
    """

    return bool(
        config.option.airflow_record
        or config.option.airflow_baseline
        or config.option.airflow_baseline_select
        or config.option.airflow_baseline_xfail
    )


def configure(config: pytest.Config) -> None:
    """Install the per-session outcome accumulator and push the active config.

    Parameters:
        config: pytest.Config for the active test session.
    """

    config.stash[_ACTIVE_KEY] = _is_recording_active(config)
    config.stash[_ACCUMULATOR_KEY] = {}
    config.stash[_OUTCOMES_KEY] = {}
    _ACTIVE_CONFIG_STACK.append(config)


def unconfigure(config: pytest.Config) -> None:
    """Pop the active config when its session ends.

    Parameters:
        config: pytest.Config for the completed test session.
    """

    if _ACTIVE_CONFIG_STACK and _ACTIVE_CONFIG_STACK[-1] is config:
        _ACTIVE_CONFIG_STACK.pop()
    elif config in _ACTIVE_CONFIG_STACK:
        # Defensive: a non-LIFO teardown order should not happen (nested sessions
        # configure/unconfigure strictly within their parent's window), but removing
        # by identity rather than leaving a stale entry keeps the stack honest either
        # way.
        _ACTIVE_CONFIG_STACK.remove(config)


def stash_gated_property(item: pytest.Item, report: pytest.TestReport) -> None:
    """Record whether the family gate would apply to one item on its phase report.

    Called from a `pytest_runtest_makereport` hookwrapper so the flag travels in
    `report.user_properties`, which survives xdist worker-to-controller serialization;
    nothing else about `item` does.

    Parameters:
        item: pytest.Item that produced the report.
        report: pytest.TestReport receiving the stashed property.
    """

    if not item.config.stash.get(_ACTIVE_KEY, False):
        return
    report.user_properties.append((GATED_PROPERTY_NAME, would_family_gate(item)))


def _read_gated(report: pytest.TestReport) -> bool:
    """Read the stashed family-gate flag from one report's user properties.

    Parameters:
        report: pytest.TestReport possibly carrying the stashed flag.

    Returns:
        bool containing the stashed flag, or `False` when absent.
    """

    for name, value in report.user_properties:
        if name == GATED_PROPERTY_NAME:
            return bool(value)
    return False


def _accumulate(accumulators: dict[str, _NodeAccumulator], report: pytest.TestReport) -> None:
    """Fold one phase report into its nodeid's accumulator.

    Parameters:
        accumulators: dict[str, _NodeAccumulator] mapping nodeid to in-progress state.
        report: pytest.TestReport for one setup/call/teardown phase.
    """

    node = accumulators.setdefault(report.nodeid, _NodeAccumulator())
    node.gated = node.gated or _read_gated(report)
    node.duration += report.duration
    if report.when == "setup":
        node.setup_failed = report.failed
    elif report.when == "call":
        node.call_outcome = report.outcome
        node.call_wasxfail = hasattr(report, "wasxfail")
    elif report.when == "teardown":
        node.teardown_failed = report.failed
    else:
        # `report.when == _CRASH_WHEN` (xdist's crashed-worker sentinel) today, and
        # defensively any other non-standard phase pytest might introduce: treated as
        # terminal so the nodeid still gets a real, visible outcome instead of an
        # accumulator entry that never finalizes.
        node.crashed = True


def _finalize(node: _NodeAccumulator) -> OutcomeEntry:
    """Derive one finalized outcome entry from a complete accumulator.

    Parameters:
        node: _NodeAccumulator containing every phase report seen for one nodeid.

    Returns:
        OutcomeEntry containing the derived outcome.
    """

    if node.crashed:
        # An xdist worker crash report is the item's sole report -- no setup/call/
        # teardown data to attribute the failure to a phase, so this is `failed`
        # rather than `error` (which the schema reserves for a `setup`/`teardown`
        # phase specifically).
        return OutcomeEntry(
            outcome=Outcome.FAILED.value, phase=None, gated=node.gated, duration=node.duration
        )
    if node.teardown_failed:
        return OutcomeEntry(
            outcome=Outcome.ERROR.value, phase="teardown", gated=node.gated, duration=node.duration
        )
    if node.setup_failed:
        return OutcomeEntry(
            outcome=Outcome.ERROR.value, phase="setup", gated=node.gated, duration=node.duration
        )
    if node.call_outcome is None:
        # pytest's own setup/call/teardown protocol only reaches here when setup
        # skipped: a failed setup is handled above, and a passing setup always
        # proceeds to the call phase.
        return OutcomeEntry(
            outcome=Outcome.SKIPPED.value, phase=None, gated=node.gated, duration=node.duration
        )
    if node.call_outcome == "passed":
        outcome = Outcome.XPASSED if node.call_wasxfail else Outcome.PASSED
    elif node.call_outcome == "failed":
        # A strict-xfail unexpected pass is already reported `failed` by pytest's own
        # xfail handling, so no extra branching is needed here.
        outcome = Outcome.FAILED
    else:
        outcome = Outcome.XFAILED if node.call_wasxfail else Outcome.SKIPPED
    return OutcomeEntry(
        outcome=outcome.value, phase=None, gated=node.gated, duration=node.duration
    )


def handle_logreport(report: pytest.TestReport) -> None:
    """Accumulate one phase report, finalizing and storing the outcome at teardown.

    Dispatched from `plugin.py`'s `pytest_runtest_logreport` hookimpl. A no-op on an
    xdist worker: the controller receives every worker report forwarded to its own
    `pytest_runtest_logreport`, so accumulating locally on the worker too would be
    redundant work whose result nothing reads.

    Parameters:
        report: pytest.TestReport for one setup/call/teardown phase, from a locally
            executed test or forwarded from an xdist worker.
    """

    if os.environ.get(XDIST_WORKER_ENVIRONMENT_VARIABLE) is not None:
        return
    config = _ACTIVE_CONFIG_STACK[-1] if _ACTIVE_CONFIG_STACK else None
    if config is None or not config.stash.get(_ACTIVE_KEY, False):
        return
    accumulators = config.stash[_ACCUMULATOR_KEY]
    _accumulate(accumulators, report)
    if report.when in ("setup", "call"):
        return
    # `teardown` normally, or the `_CRASH_WHEN` terminal sentinel; see `_accumulate`.
    node = accumulators.pop(report.nodeid)
    config.stash[_OUTCOMES_KEY][report.nodeid] = _finalize(node)


def live_outcomes(config: pytest.Config) -> dict[str, OutcomeEntry]:
    """Return a snapshot of the outcomes accumulated so far this session.

    Parameters:
        config: pytest.Config containing the accumulator stash.

    Returns:
        dict[str, OutcomeEntry] mapping nodeid to its finalized outcome.
    """

    return dict(config.stash.get(_OUTCOMES_KEY, {}))


def _resolve_airflow_version_and_family() -> tuple[str, str]:
    """Resolve the installed Airflow version and family without importing Airflow.

    Returns:
        tuple[str, str] containing the distribution version and the raw
        `AirflowFamily` value, or `UNKNOWN` for either when resolution fails.
    """

    family = installed_family()
    if family is None:
        return UNKNOWN, UNKNOWN
    try:
        version = metadata.version(family.value)
    except metadata.PackageNotFoundError:
        version = UNKNOWN
    return version, family.value


def build_artifact(outcomes: dict[str, OutcomeEntry], *, complete: bool) -> Artifact:
    """Build one artifact payload from accumulated outcomes.

    Parameters:
        outcomes: dict[str, OutcomeEntry] mapping nodeid to its finalized outcome.
        complete: bool recording whether the recording session finished cleanly.

    Returns:
        Artifact containing the complete schema-version-1 payload.
    """

    airflow_version, airflow_family = _resolve_airflow_version_and_family()
    return Artifact(
        schema_version=1,
        plugin_version=__version__,
        airflow_version=airflow_version,
        airflow_family=airflow_family,
        python_version=platform.python_version(),
        pytest_version=pytest.__version__,
        created_at=datetime.now(timezone.utc).isoformat(),
        complete=complete,
        outcomes=outcomes,
    )


def build_live_artifact(config: pytest.Config, exitstatus: int | pytest.ExitCode) -> Artifact:
    """Build the current session's live artifact from its accumulated outcomes.

    Shared by `write_recorded_artifact` (writing `--airflow-record` to disk) and
    `baseline.render_terminal_summary` (comparing in memory), so both compute
    `complete` identically.

    Parameters:
        config: pytest.Config containing the accumulator stash.
        exitstatus: int | pytest.ExitCode containing pytest's raw session exit status.

    Returns:
        Artifact containing the live session's outcomes.
    """

    complete = exitstatus not in (pytest.ExitCode.INTERRUPTED, pytest.ExitCode.INTERNAL_ERROR)
    return build_artifact(live_outcomes(config), complete=complete)


def write_recorded_artifact(session: pytest.Session, exitstatus: int) -> None:
    """Write the `--airflow-record` artifact on the xdist controller or a serial run.

    Dispatched from `plugin.py`'s `pytest_sessionfinish` hookimpl. A no-op on an xdist
    worker (reports already funnel to the controller, which owns the one write) and
    when `--airflow-record` was not requested.

    Parameters:
        session: pytest.Session that finished collecting and running tests.
        exitstatus: int containing pytest's raw session exit status.
    """

    config = session.config
    if isinstance(config, XdistWorkerConfig):
        return
    destination: object = config.option.airflow_record
    if not destination:
        return
    artifact = build_live_artifact(config, exitstatus)
    path = Path(str(destination))
    write_artifact(path, artifact)
    LOGGER.info(
        f"Wrote migration outcome artifact to '{path}' ({len(artifact['outcomes'])} outcomes)."
    )


__all__ = (
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
