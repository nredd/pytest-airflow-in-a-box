"""Test the plain-text migration diff report."""

from __future__ import annotations

from pathlib import Path

from pytest_airflow_in_a_box._compat.capabilities import AirflowFamily
from pytest_airflow_in_a_box.migration.provision import ProvisionedEnvironment
from pytest_airflow_in_a_box.migration.render import render_diff
from pytest_airflow_in_a_box.migration.types import CategoryCounts, DiffResult

_BASELINE_ENV = ProvisionedEnvironment(
    family=AirflowFamily.V2,
    airflow_version="2.11.2",
    python_version="3.12",
    venv_dir=Path("/work/venv-v2"),
    python_path=Path("/work/venv-v2/bin/python"),
)
_LIVE_ENV = ProvisionedEnvironment(
    family=AirflowFamily.V3,
    airflow_version="3.3.1",
    python_version="3.12",
    venv_dir=Path("/work/venv-v3"),
    python_path=Path("/work/venv-v3/bin/python"),
)


def test_render_diff_reports_no_regressions_and_empty_nodeid_sections() -> None:
    """Render an explicit '(none)' line for every empty nodeid section."""
    diff = DiffResult(
        counts=CategoryCounts(
            regression=0,
            fixed=0,
            broken_on_both=1,
            still_passing=5,
            gated=2,
            new=0,
            missing=0,
        ),
        regression_nodeids=(),
        fixed_nodeids=(),
        new_nodeids=(),
        missing_nodeids=(),
        warnings=(),
    )

    report = render_diff(diff, baseline_env=_BASELINE_ENV, live_env=_LIVE_ENV)

    assert report.startswith("pytest-airflow-in-a-box migration diff\n")
    assert "Airflow 2.11.2 (V2) on Python 3.12" in report
    assert "Airflow 3.3.1 (V3) on Python 3.12" in report
    # Named exactly as docs/guide/migration-diff.md's seven categories.
    assert "- regression: 0" in report
    assert "- still-passing: 5" in report
    assert "- gated: 2" in report
    assert report.count("- (none)") == 4
    assert "## Warnings" not in report
    assert report.rstrip("\n").endswith("Result: no regressions")


def test_render_diff_lists_nodeids_and_warnings_when_present() -> None:
    """Render every nodeid and warning when the diff carries them."""
    diff = DiffResult(
        counts=CategoryCounts(
            regression=1,
            fixed=1,
            broken_on_both=0,
            still_passing=0,
            gated=0,
            new=1,
            missing=1,
        ),
        regression_nodeids=("tests/test_a.py::test_regressed",),
        fixed_nodeids=("tests/test_b.py::test_fixed",),
        new_nodeids=("tests/test_c.py::test_new",),
        missing_nodeids=("tests/test_d.py::test_missing",),
        warnings=("baseline and live are the same Airflow family",),
    )

    report = render_diff(diff, baseline_env=_BASELINE_ENV, live_env=_LIVE_ENV)

    assert "- `tests/test_a.py::test_regressed`" in report
    assert "- `tests/test_b.py::test_fixed`" in report
    assert "- `tests/test_c.py::test_new`" in report
    assert "- `tests/test_d.py::test_missing`" in report
    assert "## Warnings" in report
    assert "- baseline and live are the same Airflow family" in report
    assert report.rstrip("\n").endswith("Result: REGRESSIONS FOUND")


def test_render_diff_counts_use_the_documented_seven_bucket_names() -> None:
    """Match `docs/guide/migration-diff.md`'s seven bucket names exactly, not a paraphrase."""
    diff = DiffResult(
        counts=CategoryCounts(
            regression=1, fixed=2, broken_on_both=3, still_passing=4, gated=5, new=6, missing=7
        ),
        regression_nodeids=(),
        fixed_nodeids=(),
        new_nodeids=(),
        missing_nodeids=(),
        warnings=(),
    )

    report = render_diff(diff, baseline_env=_BASELINE_ENV, live_env=_LIVE_ENV)

    for label, count in (
        ("still-passing", 4),
        ("broken-on-both", 3),
        ("gated", 5),
        ("regression", 1),
        ("fixed", 2),
        ("new", 6),
        ("missing", 7),
    ):
        assert f"- {label}: {count}" in report


def test_render_diff_ends_with_single_trailing_newline() -> None:
    """Newline-terminate the report exactly once."""
    diff = DiffResult(
        counts=CategoryCounts(
            regression=0,
            fixed=0,
            broken_on_both=0,
            still_passing=0,
            gated=0,
            new=0,
            missing=0,
        ),
        regression_nodeids=(),
        fixed_nodeids=(),
        new_nodeids=(),
        missing_nodeids=(),
        warnings=(),
    )

    report = render_diff(diff, baseline_env=_BASELINE_ENV, live_env=_LIVE_ENV)

    assert report.endswith("\n")
    assert not report.endswith("\n\n")
