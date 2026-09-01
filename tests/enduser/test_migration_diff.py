"""End-to-end `--airflow-record`/`--airflow-baseline` migration-diff workflow.

Not marked ``compat``: these tests exercise the plugin's own record/compare/select/xfail
mechanics against a synthetic pass/fail/skip/xfail spread, not Airflow-version-specific
behavior, so running them once against the dev environment is enough. See
``test_migration_diff_compat.py`` for the one thing that genuinely needs the version
matrix: that ``airflow_version``/``airflow_family`` populate from the real installation.

References:
    https://github.com/nredd/pytest-airflow-in-a-box/issues/42
    https://pytest-xdist.readthedocs.io/en/stable/distribution.html
"""

from __future__ import annotations

from textwrap import dedent

import pytest

from piab import artifact


def _suite(*, pass_body: str = "assert True") -> str:
    """Build a small pass/fail/skip/xfail/xpass suite as pytester source.

    Parameters:
        pass_body: str containing `test_pass`'s body, swappable to flip its outcome
            between two recordings of the "same" suite.

    Returns:
        str containing the dedented module source.
    """

    return dedent(
        f"""
        import pytest


        def test_pass():
            {pass_body}


        def test_fail():
            assert False


        def test_skip():
            pytest.skip("skipped")


        @pytest.mark.xfail(reason="known issue", strict=False)
        def test_xfail():
            assert False


        @pytest.mark.xfail(reason="known issue", strict=False)
        def test_xpass():
            assert True
        """
    )


def test_record_writes_a_well_formed_artifact(pytester: pytest.Pytester) -> None:
    """Record one outcome per test, correctly classified.

    Parameters:
        pytester: pytest.Pytester running the recording invocation in a subprocess.
    """

    pytester.makepyfile(_suite())
    destination = pytester.path / "record.json"

    result = pytester.runpytest_subprocess(f"--airflow-record={destination}", "-q")

    result.assert_outcomes(passed=1, failed=1, skipped=1, xfailed=1, xpassed=1)
    recorded = artifact.load_artifact(destination)
    assert recorded["schema_version"] == 1
    assert recorded["complete"] is True
    by_name = {
        nodeid.rsplit("::", 1)[-1]: entry["outcome"]
        for nodeid, entry in recorded["outcomes"].items()
    }
    assert by_name == {
        "test_pass": "passed",
        "test_fail": "failed",
        "test_skip": "skipped",
        "test_xfail": "xfailed",
        "test_xpass": "xpassed",
    }


def test_baseline_self_compare_reports_no_regressions(pytester: pytest.Pytester) -> None:
    """Fold every nodeid into `still-passing`/`broken-on-both` when comparing to itself.

    Parameters:
        pytester: pytest.Pytester running both invocations in subprocesses.
    """

    pytester.makepyfile(_suite())
    baseline_path = pytester.path / "baseline.json"
    pytester.runpytest_subprocess(f"--airflow-record={baseline_path}", "-q")

    live_path = pytester.path / "live.json"
    result = pytester.runpytest_subprocess(
        f"--airflow-baseline={baseline_path}", f"--airflow-record={live_path}", "-q"
    )

    result.assert_outcomes(passed=1, failed=1, skipped=1, xfailed=1, xpassed=1)
    stdout = result.stdout.str()
    assert "airflow migration diff" in stdout
    assert "still-passing: 3" in stdout  # pass, xpass, skip (neutral+neutral)
    assert "broken-on-both: 2" in stdout  # fail, xfail (both project to fail)
    assert "regression: 0" in stdout
    assert "fixed: 0" in stdout
    assert "new: 0" in stdout
    assert "missing: 0" in stdout


def test_xdist_single_write_aggregates_both_workers(pytester: pytest.Pytester) -> None:
    """Write exactly one artifact from the controller, aggregating both workers.

    Parameters:
        pytester: pytest.Pytester running the `-n 2` invocation in a subprocess.
    """

    pytester.makepyfile(_suite())
    destination = pytester.path / "record.json"

    result = pytester.runpytest_subprocess(f"--airflow-record={destination}", "-q", "-n", "2")

    result.assert_outcomes(passed=1, failed=1, skipped=1, xfailed=1, xpassed=1)
    matches = sorted(pytester.path.glob("record*.json"))
    assert matches == [destination]
    recorded = artifact.load_artifact(destination)
    assert len(recorded["outcomes"]) == 5


def test_baseline_select_stays_collection_consistent_under_xdist(
    pytester: pytest.Pytester,
) -> None:
    """Deselect identically on every worker so xdist never sees diverging collections.

    Parameters:
        pytester: pytest.Pytester running both invocations in subprocesses.
    """

    pytester.makepyfile(_suite())
    baseline_path = pytester.path / "baseline.json"
    pytester.runpytest_subprocess(f"--airflow-record={baseline_path}", "-q")

    result = pytester.runpytest_subprocess(
        f"--airflow-baseline={baseline_path}",
        "--airflow-baseline-select=passing",
        "-q",
        "-n",
        "2",
    )

    # `passing` selects both `test_pass` (baseline `passed`) and `test_xpass`
    # (baseline `xpassed`, which counts under the same pass projection
    # `compute_categories` uses); `test_fail`/`test_skip`/`test_xfail` are deselected.
    result.assert_outcomes(passed=1, xpassed=1)
    assert "Different tests were collected" not in result.stdout.str()


def test_baseline_xfail_stays_collection_consistent_under_xdist(
    pytester: pytest.Pytester,
) -> None:
    """Xfail-mark a known regression identically on every worker under `-n 2`.

    Parameters:
        pytester: pytest.Pytester running all three invocations in subprocesses.
    """

    pytester.makepyfile(_suite())
    baseline_path = pytester.path / "baseline.json"
    pytester.runpytest_subprocess(f"--airflow-record={baseline_path}", "-q")

    pytester.makepyfile(_suite(pass_body="assert False"))
    prior_live_path = pytester.path / "prior-live.json"
    pytester.runpytest_subprocess(f"--airflow-record={prior_live_path}", "-q")

    pytester.makepyfile(_suite())  # back to passing, matching the baseline
    result = pytester.runpytest_subprocess(
        f"--airflow-baseline={baseline_path}",
        f"--airflow-baseline-xfail={prior_live_path}",
        "-q",
        "-n",
        "2",
    )

    # `test_pass` is the known regression: baseline passed, prior-live failed, so the
    # plugin non-strict xfail-marks it; it actually passes here, so it XPASSes.
    # `test_xpass` carries its own xfail marker and XPASSes independently.
    result.assert_outcomes(xpassed=2, failed=1, skipped=1, xfailed=1)
    assert "Different tests were collected" not in result.stdout.str()


def test_record_survives_a_crashed_xdist_worker(pytester: pytest.Pytester) -> None:
    """Record a crashed worker's nodeid as `failed`, not silently missing.

    `xdist.dsession.DSession.handle_crashitem` synthesizes exactly one report with
    `when="???"` for the item a crashed worker was running, with no setup/call/
    teardown reports around it; `os._exit` reliably kills the worker process the way a
    segfault or OOM kill would, without pytest's normal exception machinery ever
    running.

    Parameters:
        pytester: pytest.Pytester running the `-n 2` invocation in a subprocess.
    """

    pytester.makepyfile(
        """
        import os


        def test_survivor():
            assert True


        def test_crasher():
            os._exit(1)
        """
    )
    destination = pytester.path / "record.json"

    result = pytester.runpytest_subprocess(f"--airflow-record={destination}", "-q", "-n", "2")

    result.assert_outcomes(passed=1, failed=1)
    recorded = artifact.load_artifact(destination)
    by_name = {nodeid.rsplit("::", 1)[-1]: entry for nodeid, entry in recorded["outcomes"].items()}
    assert by_name["test_survivor"]["outcome"] == "passed"
    assert by_name["test_crasher"]["outcome"] == "failed"
    assert by_name["test_crasher"]["phase"] is None
    assert by_name["test_crasher"]["gated"] is False
