"""Select, xfail-mark, and compare against a `--airflow-baseline` migration artifact.

`compute_categories` is the pure, importable seven-bucket comparison over two already
loaded `Artifact` values -- the function a sibling migration-orchestrator plugin
(issue #44) is expected to import directly, so it stays free of any pytest hook
plumbing.

References:
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.hookspec.pytest_collection_modifyitems
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.hookspec.pytest_terminal_summary
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.UsageError
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TypedDict

import pytest

from pytest_airflow_in_a_box import record
from pytest_airflow_in_a_box._compat.capabilities import installed_family
from pytest_airflow_in_a_box.artifact import (
    UNKNOWN,
    Artifact,
    Outcome,
    OutcomeEntry,
    load_artifact,
)
from pytest_airflow_in_a_box.bootstrap import XdistWorkerConfig

LOGGER = logging.getLogger(__name__)

SELECTORS = ("passing", "failing", "new")
_XFAIL_REASON_PREFIX = "pytest-airflow-in-a-box known regression vs baseline"

_BASELINE_KEY = pytest.StashKey[Artifact | None]()
_PRIOR_LIVE_KEY = pytest.StashKey[Artifact | None]()
_KNOWN_REGRESSIONS_KEY = pytest.StashKey[frozenset[str]]()

_PASS_OUTCOMES = frozenset({Outcome.PASSED.value, Outcome.XPASSED.value})
_FAIL_OUTCOMES = frozenset({Outcome.FAILED.value, Outcome.ERROR.value, Outcome.XFAILED.value})
# Every (baseline_projection, live_projection) pair maps to exactly one bucket. Neutral
# (a non-gated skip on either side) never lands in `regression`/`fixed`: it folds
# toward `broken_on_both` when the other side is failing, else toward `still_passing` --
# symmetric in which side is neutral, and the identity used when both sides are neutral.
_CATEGORY_BY_PROJECTION: dict[tuple[str, str], str] = {
    ("pass", "pass"): "still_passing",
    ("pass", "fail"): "regression",
    ("pass", "neutral"): "still_passing",
    ("fail", "pass"): "fixed",
    ("fail", "fail"): "broken_on_both",
    ("fail", "neutral"): "broken_on_both",
    ("neutral", "pass"): "still_passing",
    ("neutral", "fail"): "broken_on_both",
    ("neutral", "neutral"): "still_passing",
}


class CategoryBuckets(TypedDict):
    """Seven-way migration comparison over two loaded artifacts.

    Every bucket contains nodeids common to both artifacts except `missing` (baseline
    only) and `new` (live only). `gated` takes priority over the pass/fail/neutral
    projection: a nodeid gated on either side is never also a regression, a fix, or
    folded toward `still_passing`/`broken_on_both`.

    Parameters:
        missing: tuple[str, ...] nodeids present in the baseline but not collected
            live -- informational, these nodeids do not exist in the live run.
        new: tuple[str, ...] nodeids present live but absent from the baseline.
        gated: tuple[str, ...] nodeids where a `requires_airflow2`/`requires_airflow3`
            family marker gated the baseline side, the live side, or both.
        still_passing: tuple[str, ...] nodeids passing (or neutral-with-pass/neutral)
            on both sides.
        broken_on_both: tuple[str, ...] nodeids failing (or neutral-with-fail) on both
            sides.
        regression: tuple[str, ...] nodeids that passed on the baseline and now fail.
        fixed: tuple[str, ...] nodeids that failed on the baseline and now pass.
    """

    missing: tuple[str, ...]
    new: tuple[str, ...]
    gated: tuple[str, ...]
    still_passing: tuple[str, ...]
    broken_on_both: tuple[str, ...]
    regression: tuple[str, ...]
    fixed: tuple[str, ...]


def _project(entry: OutcomeEntry) -> str:
    """Project one outcome entry onto `{"pass", "fail", "neutral"}`.

    Parameters:
        entry: OutcomeEntry containing one recorded outcome.

    Returns:
        str containing `"pass"` for passed/xpassed, `"fail"` for
        failed/error/xfailed, or `"neutral"` for a non-gated skipped outcome.
    """

    outcome = entry["outcome"]
    if outcome in _PASS_OUTCOMES:
        return "pass"
    if outcome in _FAIL_OUTCOMES:
        return "fail"
    return "neutral"


def compute_categories(baseline: Artifact, live: Artifact) -> CategoryBuckets:
    """Compute the seven-bucket migration comparison over two loaded artifacts.

    Nodeids are matched exactly; there is no fuzzy pairing, so parametrize ids that
    drift between the two recordings surface honestly as a `new`/`missing` pair rather
    than being paired up incorrectly.

    Parameters:
        baseline: Artifact containing the prior (typically Airflow 2.x) recording.
        live: Artifact containing the current (typically Airflow 3.x) recording.

    Returns:
        CategoryBuckets containing nodeids sorted within each of the seven buckets.
    """

    baseline_outcomes = baseline["outcomes"]
    live_outcomes = live["outcomes"]
    baseline_ids = set(baseline_outcomes)
    live_ids = set(live_outcomes)

    missing = tuple(sorted(baseline_ids - live_ids))
    new = tuple(sorted(live_ids - baseline_ids))

    buckets: dict[str, list[str]] = {
        "gated": [],
        "still_passing": [],
        "broken_on_both": [],
        "regression": [],
        "fixed": [],
    }
    for nodeid in sorted(baseline_ids & live_ids):
        baseline_entry = baseline_outcomes[nodeid]
        live_entry = live_outcomes[nodeid]
        if baseline_entry["gated"] or live_entry["gated"]:
            buckets["gated"].append(nodeid)
            continue
        pair = (_project(baseline_entry), _project(live_entry))
        buckets[_CATEGORY_BY_PROJECTION[pair]].append(nodeid)

    return CategoryBuckets(
        missing=missing,
        new=new,
        gated=tuple(buckets["gated"]),
        still_passing=tuple(buckets["still_passing"]),
        broken_on_both=tuple(buckets["broken_on_both"]),
        regression=tuple(buckets["regression"]),
        fixed=tuple(buckets["fixed"]),
    )


def register_options(parser: pytest.Parser) -> None:
    """Register `--airflow-baseline`, its select/xfail modes, and their ini pair.

    Parameters:
        parser: pytest.Parser receiving command-line and ini options.
    """

    group = parser.getgroup("airflow-in-a-box")
    group.addoption(
        "--airflow-baseline",
        action="store",
        default=None,
        dest="airflow_baseline",
        metavar="PATH",
        help="Compare live outcomes against the migration artifact at PATH.",
    )
    group.addoption(
        "--airflow-baseline-select",
        action="store",
        default=None,
        dest="airflow_baseline_select",
        choices=SELECTORS,
        metavar="MODE",
        help="Collect only items in this baseline outcome bucket (requires --airflow-baseline).",
    )
    group.addoption(
        "--airflow-baseline-xfail",
        action="store",
        default=None,
        dest="airflow_baseline_xfail",
        metavar="PATH",
        help=(
            "Non-strict xfail-mark known regressions (baseline pass, prior-live fail) "
            "using a prior live recording at PATH (requires --airflow-baseline)."
        ),
    )
    group.addoption(
        "--airflow-baseline-allow-incomplete",
        action="store_true",
        default=None,
        dest="airflow_baseline_allow_incomplete",
        help="Accept a baseline or prior-live artifact recorded from an incomplete session.",
    )
    parser.addini(
        "airflow_baseline_allow_incomplete",
        "Accept a baseline or prior-live artifact recorded from an incomplete session.",
        type="bool",
        default=False,
    )


def _allow_incomplete(config: pytest.Config) -> bool:
    """Resolve the command-line or ini incomplete-artifact opt-in.

    Parameters:
        config: pytest.Config containing parsed options and ini values.

    Returns:
        bool indicating whether an incomplete artifact is accepted.

    Raises:
        pytest.UsageError: The ini value is not a boolean.
    """

    if config.option.airflow_baseline_allow_incomplete:
        return True
    ini_value: object = config.getini("airflow_baseline_allow_incomplete")
    if not isinstance(ini_value, bool):
        raise pytest.UsageError("Ini option `airflow_baseline_allow_incomplete` must be a boolean")
    return ini_value


def _validate_completeness(
    config: pytest.Config, artifact: Artifact, path: Path, role: str
) -> None:
    """Reject an incomplete artifact unless the override opt-in is set.

    Parameters:
        config: pytest.Config containing the incomplete-artifact opt-in.
        artifact: Artifact loaded from `path`.
        path: pathlib.Path containing the artifact file, named in the error message.
        role: str naming the artifact's role (`baseline` or `prior-live`), named in the
            error message.

    Raises:
        pytest.UsageError: `artifact["complete"]` is `False` and the opt-in is unset.
    """

    if artifact["complete"] or _allow_incomplete(config):
        return
    raise pytest.UsageError(
        f"The {role} artifact `{path}` was recorded from an incomplete session "
        f"(`complete: false`); pass `--airflow-baseline-allow-incomplete` (or set the "
        f"`airflow_baseline_allow_incomplete` ini option) to use it anyway"
    )


def _is_same_family(artifact: Artifact) -> bool:
    """Report whether an artifact's recording family matches the installed family.

    A same-family comparison (e.g. 3.1 -> 3.3) is legitimately useful, so this drives a
    warning, never an error.

    Parameters:
        artifact: Artifact containing the recorded `airflow_family`.

    Returns:
        bool indicating whether the recorded family matches the installed one.
    """

    installed = installed_family()
    installed_value = installed.value if installed is not None else UNKNOWN
    return artifact["airflow_family"] == installed_value


def _resolve_baseline(config: pytest.Config) -> Artifact | None:
    """Load, validate, and cache the `--airflow-baseline` artifact for this session.

    Parameters:
        config: pytest.Config containing the `--airflow-baseline` option and cache stash.

    Returns:
        Artifact | None containing the loaded baseline, or `None` when
        `--airflow-baseline` was not supplied.

    Raises:
        pytest.UsageError: The artifact is malformed, an incompatible schema version,
            or an incomplete session without the override opt-in.
    """

    if _BASELINE_KEY in config.stash:
        return config.stash[_BASELINE_KEY]
    value: object = config.option.airflow_baseline
    if not value:
        config.stash[_BASELINE_KEY] = None
        return None
    path = Path(str(value))
    artifact = load_artifact(path)
    _validate_completeness(config, artifact, path, "baseline")
    if _is_same_family(artifact):
        LOGGER.warning(
            f"Baseline artifact `{path}` was recorded on the Airflow family "
            f"(`{artifact['airflow_family']}`) installed for this run; migration "
            f"diffing is designed for a cross-family comparison, though a same-family "
            f"comparison (e.g. 3.1 -> 3.3) is legitimately useful."
        )
    config.stash[_BASELINE_KEY] = artifact
    return artifact


def _require_baseline(config: pytest.Config, requiring_option: str) -> Artifact:
    """Resolve the baseline artifact, requiring it to be present.

    Parameters:
        config: pytest.Config containing the `--airflow-baseline` option.
        requiring_option: str naming the option that requires a baseline, for the
            error message.

    Returns:
        Artifact containing the loaded baseline.

    Raises:
        pytest.UsageError: `--airflow-baseline` was not supplied, or the artifact is
            invalid (see `_resolve_baseline`).
    """

    artifact = _resolve_baseline(config)
    if artifact is None:
        raise pytest.UsageError(f"`{requiring_option}` requires `--airflow-baseline`")
    return artifact


def _resolve_prior_live(config: pytest.Config) -> Artifact | None:
    """Load, validate, and cache the `--airflow-baseline-xfail` prior-live artifact.

    Parameters:
        config: pytest.Config containing the `--airflow-baseline-xfail` option and
            cache stash.

    Returns:
        Artifact | None containing the loaded prior-live recording, or `None` when
        `--airflow-baseline-xfail` was not supplied.

    Raises:
        pytest.UsageError: The artifact is malformed, an incompatible schema version,
            or an incomplete session without the override opt-in.
    """

    if _PRIOR_LIVE_KEY in config.stash:
        return config.stash[_PRIOR_LIVE_KEY]
    value: object = config.option.airflow_baseline_xfail
    if not value:
        config.stash[_PRIOR_LIVE_KEY] = None
        return None
    path = Path(str(value))
    artifact = load_artifact(path)
    _validate_completeness(config, artifact, path, "prior-live")
    if _is_same_family(artifact):
        LOGGER.warning(
            f"Prior-live artifact `{path}` was recorded on the Airflow family "
            f"(`{artifact['airflow_family']}`) installed for this run."
        )
    config.stash[_PRIOR_LIVE_KEY] = artifact
    return artifact


def _outcome_of(artifact: Artifact, nodeid: str) -> str | None:
    """Read one nodeid's raw outcome string from an artifact, if recorded.

    Parameters:
        artifact: Artifact containing the recorded outcomes.
        nodeid: str naming the nodeid to look up.

    Returns:
        str | None containing the raw `Outcome` value, or `None` when the nodeid was
        not recorded in `artifact`.
    """

    entry = artifact["outcomes"].get(nodeid)
    return entry["outcome"] if entry is not None else None


def _selected(nodeid: str, baseline: Artifact, selector: str) -> bool:
    """Report whether one live nodeid belongs to the selected baseline bucket.

    Selection is computed from the *baseline* outcome, before live outcomes exist:
    `passing` is where every possible regression lives (the migration iteration loop),
    `failing` covers fixed/broken-on-both candidates, `new` covers nodeids absent from
    the baseline. `passing` and `failing` reuse `_PASS_OUTCOMES`/`_FAIL_OUTCOMES`, the
    same projection `compute_categories` uses, so a baseline `xpassed` entry is
    selectable under `passing` and a baseline `xfailed` entry is selectable under
    `failing` -- only a genuinely neutral (non-gated `skipped`) baseline entry is
    eligible for none of the three selectors.

    Parameters:
        nodeid: str containing the live item's nodeid.
        baseline: Artifact containing the loaded baseline.
        selector: str containing one of `SELECTORS`.

    Returns:
        bool indicating whether the item survives selection.
    """

    entry = baseline["outcomes"].get(nodeid)
    if selector == "new":
        return entry is None
    if entry is None:
        return False
    if selector == "passing":
        return entry["outcome"] in _PASS_OUTCOMES
    return entry["outcome"] in _FAIL_OUTCOMES


def _deselect(config: pytest.Config, items: list[pytest.Item], kept: list[pytest.Item]) -> None:
    """Deselect every item not in `kept`, firing `pytest_deselected` for the rest.

    Parameters:
        config: pytest.Config used to fire the `pytest_deselected` hook.
        items: list[pytest.Item] mutated in place to `kept`.
        kept: list[pytest.Item] containing the surviving items, in original order.
    """

    if len(kept) == len(items):
        return
    kept_ids = {id(item) for item in kept}
    deselected = [item for item in items if id(item) not in kept_ids]
    config.hook.pytest_deselected(items=deselected)
    items[:] = kept


def _apply_select(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Deselect every item outside the requested `--airflow-baseline-select` bucket.

    Parameters:
        config: pytest.Config containing the selection options.
        items: list[pytest.Item] mutated in place.

    Raises:
        pytest.UsageError: `--airflow-baseline-select` was supplied without
            `--airflow-baseline`.
    """

    selector: object = config.option.airflow_baseline_select
    if not selector:
        return
    baseline = _require_baseline(config, "--airflow-baseline-select")
    kept = [item for item in items if _selected(item.nodeid, baseline, str(selector))]
    _deselect(config, items, kept)


def _apply_xfail(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Non-strict xfail-mark every known regression, unless already user-xfail-marked.

    A known regression is a nodeid that passed (`_PASS_OUTCOMES`, the same projection
    `compute_categories` uses -- an `xpassed` baseline counts) on the baseline and
    failed or errored (`_FAIL_OUTCOMES`, so a previously auto-marked regression that
    re-recorded as `xfailed` still counts, keeping repeated `--airflow-baseline-xfail`
    runs self-sustaining) in the prior live recording -- computable at collection from
    the two artifacts alone.

    Parameters:
        config: pytest.Config containing the xfail options and cache stash.
        items: list[pytest.Item] mutated in place with added `xfail` markers.

    Raises:
        pytest.UsageError: `--airflow-baseline-xfail` was supplied without
            `--airflow-baseline`.
    """

    xfail_value: object = config.option.airflow_baseline_xfail
    if not xfail_value:
        config.stash[_KNOWN_REGRESSIONS_KEY] = frozenset()
        return
    baseline = _require_baseline(config, "--airflow-baseline-xfail")
    prior_live = _resolve_prior_live(config)
    known_regressions = frozenset(
        nodeid
        for nodeid, baseline_entry in baseline["outcomes"].items()
        if baseline_entry["outcome"] in _PASS_OUTCOMES
        and prior_live is not None
        and _outcome_of(prior_live, nodeid) in _FAIL_OUTCOMES
    )
    config.stash[_KNOWN_REGRESSIONS_KEY] = known_regressions
    for item in items:
        if item.nodeid not in known_regressions:
            continue
        if item.get_closest_marker("xfail") is not None:
            continue
        item.add_marker(
            pytest.mark.xfail(
                reason=(
                    f"{_XFAIL_REASON_PREFIX} `{config.option.airflow_baseline}` "
                    f"(passed there, failed/errored in the prior live recording "
                    f"`{xfail_value}`)"
                ),
                strict=False,
            )
        )


def apply_selection_and_xfail(
    session: pytest.Session, config: pytest.Config, items: list[pytest.Item]
) -> None:
    """Apply `--airflow-baseline-select` deselection and `--airflow-baseline-xfail` marking.

    Dispatched from `plugin.py`'s `pytest_collection_modifyitems` hookimpl, after its
    own `tryfirst` deduplication and smoke-item injection so selection sees the final
    item list.

    Parameters:
        session: pytest.Session that owns the final collected item list.
        config: pytest.Config containing the baseline options.
        items: list[pytest.Item] mutated in place.
    """

    del session
    _apply_select(config, items)
    _apply_xfail(config, items)


def _write_bucket(
    terminalreporter: pytest.TerminalReporter, title: str, nodeids: tuple[str, ...]
) -> None:
    """Write one bucket's count and, for non-count-only buckets, its nodeids.

    Parameters:
        terminalreporter: pytest.TerminalReporter receiving the rendered lines.
        title: str naming the bucket.
        nodeids: tuple[str, ...] containing the bucket's nodeids.
    """

    terminalreporter.write_line(f"{title}: {len(nodeids)}")
    for nodeid in nodeids:
        terminalreporter.write_line(f"  - {nodeid}")


def render_terminal_summary(
    terminalreporter: pytest.TerminalReporter, exitstatus: int, config: pytest.Config
) -> None:
    """Render the seven-bucket migration diff summary for `--airflow-baseline`.

    Dispatched from `plugin.py`'s `pytest_terminal_summary` hookimpl. A no-op on an
    xdist worker and when `--airflow-baseline` was not supplied.

    Parameters:
        terminalreporter: pytest.TerminalReporter receiving the rendered summary.
        exitstatus: int containing pytest's raw session exit status.
        config: pytest.Config containing the baseline options and accumulated outcomes.
    """

    if isinstance(config, XdistWorkerConfig):
        return
    baseline = _resolve_baseline(config)
    if baseline is None:
        return
    live = record.build_live_artifact(config, exitstatus)
    categories = compute_categories(baseline, live)

    terminalreporter.write_sep("=", "airflow migration diff")
    if _is_same_family(baseline):
        terminalreporter.write_line(
            f"WARNING: baseline family `{baseline['airflow_family']}` matches this "
            f"run's installed family -- this is a same-family comparison."
        )
    prior_live = config.stash.get(_PRIOR_LIVE_KEY, None)
    if prior_live is not None and _is_same_family(prior_live):
        terminalreporter.write_line(
            f"WARNING: prior-live family `{prior_live['airflow_family']}` matches "
            f"this run's installed family."
        )

    _write_bucket(terminalreporter, "still-passing", categories["still_passing"])
    _write_bucket(terminalreporter, "broken-on-both", categories["broken_on_both"])
    _write_bucket(terminalreporter, "gated", categories["gated"])
    _write_bucket(terminalreporter, "regression", categories["regression"])
    _write_bucket(terminalreporter, "fixed", categories["fixed"])
    _write_bucket(terminalreporter, "new", categories["new"])
    _write_bucket(terminalreporter, "missing", categories["missing"])

    known_regressions = config.stash.get(_KNOWN_REGRESSIONS_KEY, frozenset())
    now_passing = sorted(
        nodeid
        for nodeid in known_regressions
        if _outcome_of(live, nodeid) == Outcome.XPASSED.value
    )
    if now_passing:
        terminalreporter.write_line(
            f"{len(now_passing)} known regression(s) now XPASS -- re-record "
            f"`--airflow-record` and refresh the `--airflow-baseline-xfail` artifact:"
        )
        for nodeid in now_passing:
            terminalreporter.write_line(f"  - {nodeid}")


__all__ = (
    "SELECTORS",
    "CategoryBuckets",
    "apply_selection_and_xfail",
    "compute_categories",
    "register_options",
    "render_terminal_summary",
)
