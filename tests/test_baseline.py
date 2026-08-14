"""Test `--airflow-baseline` selection, xfail-marking, and the seven-bucket compare."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from pytest_airflow_in_a_box import baseline, record
from pytest_airflow_in_a_box._compat.capabilities import AirflowFamily
from pytest_airflow_in_a_box.artifact import (
    ARTIFACT_SCHEMA_VERSION,
    Artifact,
    OutcomeEntry,
    write_artifact,
)


def _entry(
    *, outcome: str = "passed", gated: bool = False, phase: str | None = None
) -> OutcomeEntry:
    """Build one outcome entry for baseline/live artifact fixtures.

    Parameters:
        outcome: str containing one `Outcome` value.
        gated: bool recording whether the family gate would apply.
        phase: str | None naming `setup`/`teardown` for an `error` outcome.

    Returns:
        OutcomeEntry containing the assembled entry.
    """

    return OutcomeEntry(outcome=outcome, phase=phase, gated=gated, duration=1.0)


def _artifact(
    outcomes: dict[str, OutcomeEntry], *, complete: bool = True, family: str = "apache-airflow"
) -> Artifact:
    """Build one complete artifact fixture.

    Parameters:
        outcomes: dict[str, OutcomeEntry] containing the recorded outcomes.
        complete: bool recording whether the session finished cleanly.
        family: str containing the recorded `airflow_family`.

    Returns:
        Artifact containing the assembled payload.
    """

    return Artifact(
        schema_version=ARTIFACT_SCHEMA_VERSION,
        plugin_version="0.4.0",
        airflow_version="2.11.2",
        airflow_family=family,
        python_version="3.12.0",
        pytest_version="8.4.0",
        created_at="2026-08-13T00:00:00+00:00",
        complete=complete,
        outcomes=outcomes,
    )


def _fake_config(
    *,
    airflow_baseline: str | None = None,
    airflow_baseline_select: str | None = None,
    airflow_baseline_xfail: str | None = None,
    airflow_baseline_allow_incomplete: bool | None = None,
    ini_allow_incomplete: bool | object = False,
    workerinput: dict[str, object] | None = None,
) -> Any:
    """Create a minimal configuration double for `baseline` module unit tests.

    Parameters:
        airflow_baseline: str | None containing the `--airflow-baseline` option value.
        airflow_baseline_select: str | None containing the select-mode option value.
        airflow_baseline_xfail: str | None containing the baseline-xfail option value.
        airflow_baseline_allow_incomplete: bool | None containing the CLI opt-in.
        ini_allow_incomplete: bool | object containing the ini opt-in (or a malformed
            non-boolean value, for the validation-failure test).
        workerinput: dict[str, object] | None marking the double as an xdist worker.

    Returns:
        Any shaped like the configuration surface under test, with a `deselected`
        attribute recording every item passed to `pytest_deselected`.
    """

    deselected: list[object] = []
    config = SimpleNamespace(
        option=SimpleNamespace(
            airflow_record=None,
            airflow_baseline=airflow_baseline,
            airflow_baseline_select=airflow_baseline_select,
            airflow_baseline_xfail=airflow_baseline_xfail,
            airflow_baseline_allow_incomplete=airflow_baseline_allow_incomplete,
        ),
        getini=lambda name: {"airflow_baseline_allow_incomplete": ini_allow_incomplete}[name],
        stash=pytest.Stash(),
        hook=SimpleNamespace(pytest_deselected=lambda items: deselected.extend(items)),
        deselected=deselected,
    )
    if workerinput is not None:
        config.workerinput = workerinput
    return config


def _item(nodeid: str, *, xfail: pytest.Mark | None = None) -> Any:
    """Create a minimal item double for selection/xfail unit tests.

    Parameters:
        nodeid: str containing the fake item's nodeid.
        xfail: pytest.Mark | None returned for an `xfail` marker lookup.

    Returns:
        Any shaped like the item surface under test, with an `added_markers` list
        recording every marker passed to `add_marker`.
    """

    added_markers: list[object] = []
    return SimpleNamespace(
        nodeid=nodeid,
        get_closest_marker=lambda name: xfail if name == "xfail" else None,
        add_marker=lambda marker: added_markers.append(marker),
        added_markers=added_markers,
    )


class _FakeTerminalReporter:
    """Collect rendered lines instead of writing to a real terminal."""

    def __init__(self) -> None:
        self.lines: list[str] = []

    def write_line(self, line: str, **markup: bool) -> None:
        """Record one rendered line, ignoring markup keywords."""

        del markup
        self.lines.append(line)

    def write_sep(self, sep: str, title: str | None = None, **markup: bool) -> None:
        """Record one rendered separator line, ignoring markup keywords."""

        del markup
        self.lines.append(f"{sep} {title}")


def _terminal_reporter() -> Any:
    """Build a `_FakeTerminalReporter` typed as `Any` for `pytest.TerminalReporter` call sites."""

    return _FakeTerminalReporter()


# --- compute_categories -----------------------------------------------------------


def test_compute_categories_covers_every_bucket() -> None:
    """Exercise all nine pass/fail/neutral pairs, gating, and missing/new."""

    baseline_outcomes = {
        "pp": _entry(outcome="passed"),
        "pf": _entry(outcome="xpassed"),
        "pn": _entry(outcome="passed"),
        "fp": _entry(outcome="failed"),
        "ff": _entry(outcome="error"),
        "fn": _entry(outcome="xfailed"),
        "np": _entry(outcome="skipped"),
        "nf": _entry(outcome="skipped"),
        "nn": _entry(outcome="skipped"),
        "gated_baseline": _entry(outcome="passed", gated=True),
        "gated_live": _entry(outcome="passed"),
        "missing_only": _entry(outcome="passed"),
    }
    live_outcomes = {
        "pp": _entry(outcome="passed"),
        "pf": _entry(outcome="failed"),
        "pn": _entry(outcome="skipped"),
        "fp": _entry(outcome="xpassed"),
        "ff": _entry(outcome="failed"),
        "fn": _entry(outcome="skipped"),
        "np": _entry(outcome="passed"),
        "nf": _entry(outcome="error"),
        "nn": _entry(outcome="skipped"),
        "gated_baseline": _entry(outcome="failed"),
        "gated_live": _entry(outcome="failed", gated=True),
        "new_only": _entry(outcome="passed"),
    }

    categories = baseline.compute_categories(
        _artifact(baseline_outcomes), _artifact(live_outcomes)
    )

    assert categories["missing"] == ("missing_only",)
    assert categories["new"] == ("new_only",)
    assert set(categories["gated"]) == {"gated_baseline", "gated_live"}
    assert set(categories["still_passing"]) == {"pp", "pn", "np", "nn"}
    assert set(categories["broken_on_both"]) == {"ff", "fn", "nf"}
    assert categories["regression"] == ("pf",)
    assert categories["fixed"] == ("fp",)


def test_compute_categories_empty_artifacts_yield_empty_buckets() -> None:
    """Return every bucket empty when both artifacts have no outcomes."""

    categories = baseline.compute_categories(_artifact({}), _artifact({}))

    assert categories == baseline.CategoryBuckets(
        missing=(), new=(), gated=(), still_passing=(), broken_on_both=(), regression=(), fixed=()
    )


# --- _allow_incomplete / _validate_completeness -----------------------------------


def test_allow_incomplete_true_from_option() -> None:
    """Short-circuit on the command-line opt-in without consulting ini."""

    config = _fake_config(
        airflow_baseline_allow_incomplete=True, ini_allow_incomplete="not-a-bool"
    )

    assert baseline._allow_incomplete(config) is True


@pytest.mark.parametrize("ini_value", [True, False])
def test_allow_incomplete_from_ini(ini_value: bool) -> None:
    """Fall back to the ini value when the command-line opt-in is unset."""

    config = _fake_config(ini_allow_incomplete=ini_value)

    assert baseline._allow_incomplete(config) is ini_value


def test_allow_incomplete_rejects_non_boolean_ini() -> None:
    """Reject a malformed ini value."""

    config = _fake_config(ini_allow_incomplete="yes")

    with pytest.raises(pytest.UsageError, match="must be a boolean"):
        baseline._allow_incomplete(config)


def test_validate_completeness_accepts_complete_artifact(tmp_path: Path) -> None:
    """Accept a complete artifact regardless of the opt-in."""

    baseline._validate_completeness(
        _fake_config(), _artifact({}, complete=True), tmp_path / "a.json", "baseline"
    )


def test_validate_completeness_accepts_incomplete_with_opt_in(tmp_path: Path) -> None:
    """Accept an incomplete artifact when the opt-in is set."""

    config = _fake_config(airflow_baseline_allow_incomplete=True)

    baseline._validate_completeness(
        config, _artifact({}, complete=False), tmp_path / "a.json", "baseline"
    )


def test_validate_completeness_rejects_incomplete_without_opt_in(tmp_path: Path) -> None:
    """Reject an incomplete artifact without the opt-in."""

    path = tmp_path / "a.json"

    with pytest.raises(pytest.UsageError, match="incomplete session"):
        baseline._validate_completeness(
            _fake_config(), _artifact({}, complete=False), path, "prior-live"
        )


# --- _is_same_family ---------------------------------------------------------------


def test_is_same_family_true_when_matching(monkeypatch: pytest.MonkeyPatch) -> None:
    """Report a match against the installed family."""

    monkeypatch.setattr(baseline, "installed_family", lambda: AirflowFamily.V3)

    assert baseline._is_same_family(_artifact({}, family=AirflowFamily.V3.value)) is True


def test_is_same_family_false_when_different(monkeypatch: pytest.MonkeyPatch) -> None:
    """Report no match against a different installed family."""

    monkeypatch.setattr(baseline, "installed_family", lambda: AirflowFamily.V3)

    assert baseline._is_same_family(_artifact({}, family=AirflowFamily.V2.value)) is False


def test_is_same_family_falls_back_to_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    """Compare against `UNKNOWN` when no family is installed."""

    monkeypatch.setattr(baseline, "installed_family", lambda: None)

    assert baseline._is_same_family(_artifact({}, family="unknown")) is True


# --- _resolve_baseline / _require_baseline -----------------------------------------


def test_resolve_baseline_returns_none_and_caches_when_unset() -> None:
    """Cache `None` when `--airflow-baseline` was not supplied."""

    config = _fake_config()

    assert baseline._resolve_baseline(config) is None
    assert baseline._resolve_baseline(config) is None
    assert baseline._BASELINE_KEY in config.stash


def test_resolve_baseline_loads_validates_caches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Load once, warn on a same-family recording, and cache the result."""

    monkeypatch.setattr(baseline, "installed_family", lambda: AirflowFamily.V3)
    path = tmp_path / "baseline.json"
    write_artifact(
        path, _artifact({"tests/x.py::test_a": _entry()}, family=AirflowFamily.V3.value)
    )
    config = _fake_config(airflow_baseline=str(path))

    with caplog.at_level("WARNING"):
        first = baseline._resolve_baseline(config)
    second = baseline._resolve_baseline(config)

    assert first is not None
    assert first is second
    assert "Baseline artifact" in caplog.text
    assert "was recorded on the Airflow family" in caplog.text


def test_resolve_baseline_wraps_incomplete_without_opt_in(tmp_path: Path) -> None:
    """Propagate the incomplete-artifact `UsageError`."""

    path = tmp_path / "baseline.json"
    write_artifact(path, _artifact({}, complete=False))
    config = _fake_config(airflow_baseline=str(path))

    with pytest.raises(pytest.UsageError, match="incomplete session"):
        baseline._resolve_baseline(config)


def test_require_baseline_raises_when_absent() -> None:
    """Name the requiring option when no baseline was supplied."""

    with pytest.raises(pytest.UsageError, match=r"--airflow-baseline-select.*requires"):
        baseline._require_baseline(_fake_config(), "--airflow-baseline-select")


def test_require_baseline_returns_when_present(tmp_path: Path) -> None:
    """Return the resolved baseline when present."""

    path = tmp_path / "baseline.json"
    write_artifact(path, _artifact({}, family=AirflowFamily.V2.value))
    config = _fake_config(airflow_baseline=str(path))

    artifact = baseline._require_baseline(config, "--airflow-baseline-select")

    assert artifact["airflow_family"] == AirflowFamily.V2.value


# --- _resolve_prior_live ------------------------------------------------------------


def test_resolve_prior_live_returns_none_and_caches_when_unset() -> None:
    """Cache `None` when `--airflow-baseline-xfail` was not supplied."""

    config = _fake_config()

    assert baseline._resolve_prior_live(config) is None
    assert baseline._PRIOR_LIVE_KEY in config.stash


def test_resolve_prior_live_loads_and_warns_on_same_family(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Load the prior-live artifact and warn on a same-family recording."""

    monkeypatch.setattr(baseline, "installed_family", lambda: AirflowFamily.V3)
    path = tmp_path / "prior-live.json"
    write_artifact(path, _artifact({}, family=AirflowFamily.V3.value))
    config = _fake_config(airflow_baseline_xfail=str(path))

    with caplog.at_level("WARNING"):
        artifact = baseline._resolve_prior_live(config)

    assert artifact is not None
    assert "Prior-live artifact" in caplog.text


# --- _outcome_of / _selected ---------------------------------------------------------


def test_outcome_of_present_and_absent() -> None:
    """Read a present outcome and default to `None` for an absent nodeid."""

    artifact = _artifact({"tests/x.py::test_a": _entry(outcome="failed")})

    assert baseline._outcome_of(artifact, "tests/x.py::test_a") == "failed"
    assert baseline._outcome_of(artifact, "tests/x.py::test_missing") is None


@pytest.mark.parametrize(
    ("selector", "nodeid", "expected"),
    [
        ("new", "present", False),
        ("new", "absent", True),
        ("passing", "absent", False),
        ("passing", "present_passed", True),
        ("passing", "present_xpassed", True),
        ("passing", "present_failed", False),
        ("passing", "present_skipped", False),
        ("failing", "absent", False),
        ("failing", "present_failed", True),
        ("failing", "present_error", True),
        ("failing", "present_xfailed", True),
        ("failing", "present_passed", False),
        ("failing", "present_skipped", False),
    ],
)
def test_selected_matches_baseline_bucket(selector: str, nodeid: str, expected: bool) -> None:
    """Select by the same pass/fail projection `_project`/`compute_categories` use.

    A baseline `xpassed` entry is selectable under `passing` and a baseline `xfailed`
    entry is selectable under `failing` -- only a genuinely neutral `skipped` entry is
    eligible for neither.
    """

    artifact = _artifact(
        {
            "present": _entry(outcome="passed"),
            "present_passed": _entry(outcome="passed"),
            "present_xpassed": _entry(outcome="xpassed"),
            "present_failed": _entry(outcome="failed"),
            "present_error": _entry(outcome="error"),
            "present_xfailed": _entry(outcome="xfailed"),
            "present_skipped": _entry(outcome="skipped"),
        }
    )

    assert baseline._selected(nodeid, artifact, selector) is expected


# --- _deselect / _apply_select -------------------------------------------------------


def test_deselect_noop_when_nothing_dropped() -> None:
    """Skip firing `pytest_deselected` when every item is kept."""

    config = _fake_config()
    items: list[Any] = [_item("a"), _item("b")]

    baseline._deselect(config, items, list(items))

    assert config.deselected == []
    assert len(items) == 2


def test_deselect_fires_hook_and_mutates_items() -> None:
    """Fire `pytest_deselected` and mutate `items` down to `kept`."""

    config = _fake_config()
    a, b = _item("a"), _item("b")
    items: list[Any] = [a, b]

    baseline._deselect(config, items, [a])

    assert items == [a]
    assert config.deselected == [b]


def test_apply_select_noop_without_selector() -> None:
    """Skip selection entirely when `--airflow-baseline-select` was not supplied."""

    config = _fake_config()
    only = _item("a")
    items: list[Any] = [only]

    baseline._apply_select(config, items)

    assert items == [only]
    assert config.deselected == []


def test_apply_select_requires_baseline() -> None:
    """Reject `--airflow-baseline-select` supplied without `--airflow-baseline`."""

    config = _fake_config(airflow_baseline_select="passing")

    with pytest.raises(pytest.UsageError, match="requires"):
        baseline._apply_select(config, [])


def test_apply_select_filters_items(tmp_path: Path) -> None:
    """Deselect items outside the requested baseline bucket."""

    path = tmp_path / "baseline.json"
    write_artifact(
        path,
        _artifact(
            {
                "tests/x.py::test_pass": _entry(outcome="passed"),
                "tests/x.py::test_fail": _entry(outcome="failed"),
            }
        ),
    )
    config = _fake_config(airflow_baseline=str(path), airflow_baseline_select="passing")
    keep, drop = _item("tests/x.py::test_pass"), _item("tests/x.py::test_fail")
    items: list[Any] = [keep, drop]

    baseline._apply_select(config, items)

    assert items == [keep]
    assert config.deselected == [drop]


# --- _apply_xfail --------------------------------------------------------------------


def test_apply_xfail_noop_without_option() -> None:
    """Stash an empty known-regression set and leave items untouched."""

    config = _fake_config()
    item = _item("tests/x.py::test_a")
    items: list[Any] = [item]

    baseline._apply_xfail(config, items)

    assert config.stash[baseline._KNOWN_REGRESSIONS_KEY] == frozenset()
    assert item.added_markers == []


def test_apply_xfail_requires_baseline() -> None:
    """Reject `--airflow-baseline-xfail` supplied without `--airflow-baseline`."""

    config = _fake_config(airflow_baseline_xfail="prior.json")

    with pytest.raises(pytest.UsageError, match="requires"):
        baseline._apply_xfail(config, [])


def test_apply_xfail_marks_known_regressions_and_skips_user_marked(tmp_path: Path) -> None:
    """Xfail-mark a known regression, but never override the user's own marker."""

    baseline_path = tmp_path / "baseline.json"
    write_artifact(
        baseline_path,
        _artifact(
            {
                "tests/x.py::test_regressed": _entry(outcome="passed"),
                "tests/x.py::test_user_marked": _entry(outcome="passed"),
                "tests/x.py::test_still_failing": _entry(outcome="failed"),
            }
        ),
    )
    prior_path = tmp_path / "prior-live.json"
    write_artifact(
        prior_path,
        _artifact(
            {
                "tests/x.py::test_regressed": _entry(outcome="failed"),
                "tests/x.py::test_user_marked": _entry(outcome="error", phase="setup"),
            }
        ),
    )
    config = _fake_config(
        airflow_baseline=str(baseline_path), airflow_baseline_xfail=str(prior_path)
    )
    regressed = _item("tests/x.py::test_regressed")
    user_marked = _item("tests/x.py::test_user_marked", xfail=pytest.mark.xfail().mark)
    still_failing = _item("tests/x.py::test_still_failing")
    items: list[Any] = [regressed, user_marked, still_failing]

    baseline._apply_xfail(config, items)

    assert config.stash[baseline._KNOWN_REGRESSIONS_KEY] == {
        "tests/x.py::test_regressed",
        "tests/x.py::test_user_marked",
    }
    assert len(regressed.added_markers) == 1
    added_mark = regressed.added_markers[0]
    assert added_mark.name == "xfail"
    assert added_mark.kwargs["strict"] is False
    assert baseline._XFAIL_REASON_PREFIX in added_mark.kwargs["reason"]
    assert user_marked.added_markers == []
    assert still_failing.added_markers == []


def test_apply_xfail_treats_xpassed_baseline_and_xfailed_prior_live_as_matching(
    tmp_path: Path,
) -> None:
    """Mark a known regression whose baseline is `xpassed` and prior-live is `xfailed`.

    Both count under the same `_PASS_OUTCOMES`/`_FAIL_OUTCOMES` projection
    `compute_categories` uses: an `xpassed` baseline is still a "passed" nodeid for
    this purpose, and an `xfailed` prior-live is what a previously auto-marked
    regression re-records as on a repeated `--airflow-baseline-xfail` run, so it must
    keep counting as a known regression rather than dropping out.
    """

    baseline_path = tmp_path / "baseline.json"
    write_artifact(
        baseline_path, _artifact({"tests/x.py::test_regressed": _entry(outcome="xpassed")})
    )
    prior_path = tmp_path / "prior-live.json"
    write_artifact(
        prior_path, _artifact({"tests/x.py::test_regressed": _entry(outcome="xfailed")})
    )
    config = _fake_config(
        airflow_baseline=str(baseline_path), airflow_baseline_xfail=str(prior_path)
    )
    item = _item("tests/x.py::test_regressed")

    baseline._apply_xfail(config, [item])

    assert config.stash[baseline._KNOWN_REGRESSIONS_KEY] == {"tests/x.py::test_regressed"}
    assert len(item.added_markers) == 1


def test_apply_xfail_ignores_a_cached_none_prior_live(tmp_path: Path) -> None:
    """Compute no known regressions when the prior-live cache holds `None`.

    Defensive coverage for the `prior_live is not None` guard: unreachable through
    `_apply_xfail`'s own early-return alone (it only calls `_resolve_prior_live` once
    its own truthy option guarantees a loaded artifact), so this directly poisons the
    cache the way a future caller sequencing change could.
    """

    baseline_path = tmp_path / "baseline.json"
    write_artifact(baseline_path, _artifact({"tests/x.py::test_a": _entry(outcome="passed")}))
    config = _fake_config(airflow_baseline=str(baseline_path), airflow_baseline_xfail="prior.json")
    config.stash[baseline._PRIOR_LIVE_KEY] = None

    baseline._apply_xfail(config, [])

    assert config.stash[baseline._KNOWN_REGRESSIONS_KEY] == frozenset()


# --- _write_bucket / render_terminal_summary -----------------------------------------


def test_write_bucket_renders_count_and_nodeids() -> None:
    """Render the count line plus one bullet per nodeid."""

    reporter = _terminal_reporter()

    baseline._write_bucket(reporter, "regression", ("tests/x.py::test_a",))

    assert reporter.lines == ["regression: 1", "  - tests/x.py::test_a"]


def test_write_bucket_renders_count_only_when_empty() -> None:
    """Render only the count line for an empty bucket."""

    reporter = _terminal_reporter()

    baseline._write_bucket(reporter, "missing", ())

    assert reporter.lines == ["missing: 0"]


def test_render_terminal_summary_noop_on_xdist_worker() -> None:
    """Skip rendering entirely on an xdist worker."""

    config = _fake_config(airflow_baseline="baseline.json", workerinput={})
    reporter = _terminal_reporter()

    baseline.render_terminal_summary(reporter, pytest.ExitCode.OK, config)

    assert reporter.lines == []


def test_render_terminal_summary_noop_without_baseline() -> None:
    """Skip rendering when `--airflow-baseline` was not supplied."""

    config = _fake_config()
    record.configure(config)
    reporter = _terminal_reporter()

    baseline.render_terminal_summary(reporter, pytest.ExitCode.OK, config)

    assert reporter.lines == []


def test_render_terminal_summary_renders_buckets_and_warnings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Render both same-family warnings, all seven buckets, and the XPASS prompt."""

    monkeypatch.setattr(baseline, "installed_family", lambda: AirflowFamily.V3)
    baseline_path = tmp_path / "baseline.json"
    write_artifact(
        baseline_path,
        _artifact(
            {
                "tests/x.py::test_regressed": _entry(outcome="passed"),
                "tests/x.py::test_still_passing": _entry(outcome="passed"),
            },
            family=AirflowFamily.V3.value,
        ),
    )
    prior_path = tmp_path / "prior-live.json"
    write_artifact(
        prior_path,
        _artifact(
            {"tests/x.py::test_regressed": _entry(outcome="failed")},
            family=AirflowFamily.V3.value,
        ),
    )
    config = _fake_config(
        airflow_baseline=str(baseline_path), airflow_baseline_xfail=str(prior_path)
    )
    record.configure(config)
    # Simulate collection having already resolved the prior-live artifact and known
    # regressions, and the session having recorded live outcomes for both nodeids.
    baseline._apply_xfail(config, [])
    config.stash[record._OUTCOMES_KEY]["tests/x.py::test_regressed"] = _entry(outcome="xpassed")
    config.stash[record._OUTCOMES_KEY]["tests/x.py::test_still_passing"] = _entry(outcome="passed")
    reporter = _terminal_reporter()

    baseline.render_terminal_summary(reporter, pytest.ExitCode.OK, config)

    text = "\n".join(reporter.lines)
    assert "WARNING: baseline family" in text
    assert "WARNING: prior-live family" in text
    assert text.count("matches this run's installed family") == 2
    assert "still-passing: 2" in text
    assert "known regression(s) now XPASS" in text
    assert "tests/x.py::test_regressed" in text


def test_render_terminal_summary_no_xpass_prompt_when_none_recovered(tmp_path: Path) -> None:
    """Omit the XPASS prompt when no known regression newly passed."""

    baseline_path = tmp_path / "baseline.json"
    write_artifact(baseline_path, _artifact({"tests/x.py::test_a": _entry(outcome="passed")}))
    config = _fake_config(airflow_baseline=str(baseline_path))
    record.configure(config)
    reporter = _terminal_reporter()

    baseline.render_terminal_summary(reporter, pytest.ExitCode.OK, config)

    assert "XPASS" not in "\n".join(reporter.lines)


def test_register_options_exposes_selectors() -> None:
    """Expose the closed set of `--airflow-baseline-select` modes."""

    assert baseline.SELECTORS == ("passing", "failing", "new")
