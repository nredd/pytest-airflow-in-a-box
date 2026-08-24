"""Unit and subprocess tests for `plugin`-module error rendering.

References:
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.UsageError
"""

from __future__ import annotations

import json
from pathlib import Path
from textwrap import dedent
from types import SimpleNamespace
from typing import Any

import pytest

from pytest_airflow_in_a_box import plugin, smoke
from pytest_airflow_in_a_box._compat import AirflowCompatibilityError
from pytest_airflow_in_a_box.bootstrap import XDIST_WORKER_ENVIRONMENT_VARIABLE
from pytest_airflow_in_a_box.fixtures.dagbag import DAG_BAG_XDIST_GROUP
from pytest_airflow_in_a_box.fixtures.dagcorpus import DAG_CORPUS_XDIST_GROUP


def test_database_incompatibility_renders_as_usage_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Render `AirflowCompatibilityError` as a single actionable usage error.

    Parameters:
        monkeypatch: pytest.MonkeyPatch replacing the database initialization seam.
        tmp_path: Path providing a throwaway bootstrap root.
    """

    failure = AirflowCompatibilityError("Apache Airflow 2.x is installed")

    def broken_database(root: Path) -> None:
        """Raise the representative installation failure."""

        del root
        raise failure

    monkeypatch.setattr(plugin, "ensure_database", broken_database)
    config: Any = SimpleNamespace(stash=pytest.Stash())

    with pytest.raises(pytest.UsageError, match=r"Airflow 2\.x is installed") as caught:
        plugin._ensure_database_or_usage_error(config, tmp_path)

    assert caught.value.__cause__ is failure


_BROKEN_INSTALLATION_CONFTEST = """
    import pytest_airflow_in_a_box.plugin as plugin
    from pytest_airflow_in_a_box._compat import AirflowCompatibilityError

    def _broken_database(root):
        raise AirflowCompatibilityError(
            "No Airflow distribution is installed (fake probe)"
        )

    plugin.ensure_database = _broken_database
"""

_DATABASE_SUITE = """
    import pytest

    pytestmark = pytest.mark.db_test

    def test_first() -> None:
        assert True

    def test_second() -> None:
        assert True
"""


def test_incompatibility_renders_one_error_line_in_process(
    pytester: pytest.Pytester,
) -> None:
    """Abort a single-process run with one actionable `ERROR:` line, not a traceback.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    pytester.makeconftest(_BROKEN_INSTALLATION_CONFTEST)
    pytester.makepyfile(test_broken=_DATABASE_SUITE)

    result = pytester.runpytest_subprocess()

    assert result.ret == pytest.ExitCode.USAGE_ERROR
    output = result.stdout.str() + result.stderr.str()
    assert "No Airflow distribution is installed (fake probe)" in output
    assert "INTERNALERROR" not in output


def test_incompatibility_renders_per_test_errors_under_xdist(
    pytester: pytest.Pytester,
) -> None:
    """Report the installation problem per test on xdist workers, never a crashed node.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    pytester.makeconftest(_BROKEN_INSTALLATION_CONFTEST)
    pytester.makepyfile(test_broken=_DATABASE_SUITE)

    result = pytester.runpytest_subprocess("-n", "2")

    result.assert_outcomes(errors=2)
    output = result.stdout.str() + result.stderr.str()
    assert "No Airflow distribution is installed (fake probe)" in output
    assert "INTERNALERROR" not in output
    assert "crashed" not in output


_FAKE_WARNING_ENSURE_DATABASE_CONFTEST = """
    import warnings

    import pytest_airflow_in_a_box.plugin as plugin


    class FakeAirflowImportWarning(UserWarning):
        pass


    def _fake_ensure_database(root):
        warnings.warn(
            FakeAirflowImportWarning("simulated Airflow import-time warning")
        )


    plugin.ensure_database = _fake_ensure_database
"""

_ERROR_ON_USER_WARNING_INI = "[pytest]\nfilterwarnings =\n    error::UserWarning\n"


def test_ensure_database_warning_does_not_fail_first_test_under_xdist(
    pytester: pytest.Pytester,
) -> None:
    """Suppress a warning `ensure_database` raises, even under a user `error::` filter.

    Regression test for a latent bug (#43): on an xdist worker, the eager
    `pytest_collection_finish` database initialization is skipped, so
    `_ensure_database_or_usage_error` runs from the `pytest_runtest_setup` safety net
    instead -- inside the runtest phase's own warning context. A warning raised there
    under an active `error::` filter turned into an exception unrelated to
    `AirflowCompatibilityError`, so it went unhandled and failed the worker's first
    test with a misleading error. `_ensure_database_or_usage_error` now wraps the
    `ensure_database` call in its own default-filter warnings context, unconditionally,
    so both workers' first test passes here even with a real user `error::UserWarning`
    filter active.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    pytester.makeconftest(_FAKE_WARNING_ENSURE_DATABASE_CONFTEST)
    pytester.makeini(_ERROR_ON_USER_WARNING_INI)
    pytester.makepyfile(test_warned=_DATABASE_SUITE)

    result = pytester.runpytest_subprocess("-n", "2")

    result.assert_outcomes(passed=2)
    output = result.stdout.str() + result.stderr.str()
    assert "INTERNALERROR" not in output
    assert "crashed" not in output


_XDIST_GROUP_REPORTING_CONFTEST = """
    import json
    from pathlib import Path

    import pytest

    RECORD_DIR = Path({record_dir!r})


    @pytest.hookimpl(trylast=True)
    def pytest_collection_modifyitems(items):
        record = {{}}
        for item in items:
            marker = item.get_closest_marker("xdist_group")
            record[item.nodeid] = marker.kwargs.get("name") if marker else None
        (RECORD_DIR / "groups.json").write_text(json.dumps(record), encoding="utf-8")
"""

_COLOCATION_DAG = """
    from airflow.sdk import DAG, task

    with DAG(dag_id="colocate_dag", schedule=None, tags=["team-a"]) as dag:
        @task
        def work():
            pass

        work()
"""


def test_smoke_catalog_and_dag_bag_consumers_share_one_xdist_group(
    pytester: pytest.Pytester,
) -> None:
    """Group the smoke catalog with one `dag_bag` consumer, leaving the rest alone.

    Regression test for issue #163: under `--dist loadgroup`, an ungrouped smoke item
    could land on a different `pytest-xdist` worker than a `dag_bag` consumer, so
    the Dag folder was parsed twice, concurrently.
    `plugin._colocate_smoke_catalog_with_dag_bag` now forces every synthesized
    smoke item and exactly one `dag_bag` consumer onto the shared
    `DAG_BAG_XDIST_GROUP` -- not every consumer, so a suite with many of them does
    not have all their execution serialized onto a single worker just to save one
    parse -- and never chooses or overwrites an item that already carries its own
    explicit `xdist_group`.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    record_dir = pytester.path / "records"
    record_dir.mkdir()
    dag_folder = pytester.path / "dags"
    dag_folder.mkdir()
    (dag_folder / "colocate.py").write_text(dedent(_COLOCATION_DAG), encoding="utf-8")
    pytester.makeconftest(_XDIST_GROUP_REPORTING_CONFTEST.format(record_dir=str(record_dir)))
    pytester.makepyfile(
        """
        import pytest

        def test_consumer(dag_bag):
            pass

        def test_second_consumer(dag_bag):
            pass

        @pytest.mark.xdist_group(name="user-group")
        def test_pre_grouped(dag_bag):
            pass
        """
    )

    result = pytester.runpytest_subprocess(
        "-q", "--dist=loadgroup", "--airflow-smoke", "--dag-folder", str(dag_folder)
    )

    result.assert_outcomes(passed=13)
    groups = json.loads((record_dir / "groups.json").read_text(encoding="utf-8"))
    smoke_groups = {name: group for name, group in groups.items() if "::smoke::" in name}
    assert smoke_groups
    assert all(group == DAG_BAG_XDIST_GROUP for group in smoke_groups.values())
    consumer_group = next(
        group for name, group in groups.items() if name.endswith("::test_consumer")
    )
    second_consumer_group = next(
        group for name, group in groups.items() if name.endswith("::test_second_consumer")
    )
    pre_grouped_group = next(
        group for name, group in groups.items() if name.endswith("::test_pre_grouped")
    )
    assert consumer_group == DAG_BAG_XDIST_GROUP
    assert second_consumer_group is None
    assert pre_grouped_group == "user-group"


def test_colocation_skips_a_dag_bag_consumer_dropped_by_mark_expression(
    pytester: pytest.Pytester,
) -> None:
    """Do not group the catalog with a `dag_bag` consumer that `-m` will deselect.

    Regression test for issue #163: this plugin's collection hook is `tryfirst`, so it
    runs before `_pytest.mark`'s own `-m`/`-k` deselection (normal priority). Deciding
    co-location without predicting that would group the catalog with a consumer that
    never actually runs in this session, wasting the catalog's normal cross-worker
    distribution (`test_smoke_items_share_one_parse_while_remaining_distributed`,
    `tests/enduser/test_parallel_collection.py`) for zero reuse benefit. `-m smoke` is
    also this plugin's own documented way to run just the catalog
    (`docs/guide/smoke-tests.md`), making this exact combination a realistic case, not
    a corner one.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    record_dir = pytester.path / "records"
    record_dir.mkdir()
    dag_folder = pytester.path / "dags"
    dag_folder.mkdir()
    (dag_folder / "colocate.py").write_text(dedent(_COLOCATION_DAG), encoding="utf-8")
    pytester.makeconftest(_XDIST_GROUP_REPORTING_CONFTEST.format(record_dir=str(record_dir)))
    pytester.makepyfile(
        """
        def test_consumer(dag_bag):
            pass
        """
    )

    result = pytester.runpytest_subprocess(
        "-q",
        "--dist=loadgroup",
        "--airflow-smoke",
        "--dag-folder",
        str(dag_folder),
        "-m",
        "smoke",
    )

    result.assert_outcomes(passed=10)
    groups = json.loads((record_dir / "groups.json").read_text(encoding="utf-8"))
    assert all(group is None for group in groups.values())


def test_colocation_is_a_noop_without_the_xdist_plugin(pytester: pytest.Pytester) -> None:
    """Do not add an unregistered `xdist_group` marker when `xdist` itself is disabled.

    Regression test for issue #163: `xdist_group` is only a known marker because
    `xdist.plugin.pytest_configure` registers it. `-p no:xdist` is an ordinary way to
    force a serial run (the `dev` dependency group installs `pytest-xdist`, so it is
    always importable in this suite but not always loaded), and previously this hook
    still tried to add the marker unconditionally, aborting the run with
    `Failed: 'xdist_group' not found in \\`markers\\` configuration option` under
    `--strict-markers`.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    dag_folder = pytester.path / "dags"
    dag_folder.mkdir()
    (dag_folder / "colocate.py").write_text(dedent(_COLOCATION_DAG), encoding="utf-8")
    pytester.makepyfile(
        """
        def test_consumer(dag_bag):
            pass
        """
    )

    result = pytester.runpytest_subprocess(
        "-q",
        "-p",
        "no:xdist",
        "--strict-markers",
        "--airflow-smoke",
        "--dag-folder",
        str(dag_folder),
    )

    result.assert_outcomes(passed=11)
    output = result.stdout.str() + result.stderr.str()
    assert "INTERNALERROR" not in output


def test_colocation_fails_open_on_an_unparsable_mark_expression(pytester: pytest.Pytester) -> None:
    """Predict `-m` survival optimistically when the expression itself cannot be parsed.

    A malformed `-m` expression is invalid regardless of this plugin -- pytest's own
    `-m` handling raises a clear `UsageError` for it right after this collection hook
    returns, so `_survives_markexpr` failing open (treating the item as surviving)
    instead of raising itself just defers to that existing, correctly-typed error
    rather than crashing collection with an unrelated traceback.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    dag_folder = pytester.path / "dags"
    dag_folder.mkdir()
    (dag_folder / "colocate.py").write_text(dedent(_COLOCATION_DAG), encoding="utf-8")
    pytester.makepyfile(
        """
        def test_consumer(dag_bag):
            pass
        """
    )

    result = pytester.runpytest_subprocess(
        "-q",
        "--dist=loadgroup",
        "--airflow-smoke",
        "--dag-folder",
        str(dag_folder),
        "-m",
        "(",
    )

    assert result.ret == pytest.ExitCode.USAGE_ERROR
    output = result.stdout.str() + result.stderr.str()
    assert "INTERNALERROR" not in output


def test_missing_anchor_warns_when_every_dag_bag_consumer_is_pre_grouped(
    pytester: pytest.Pytester,
) -> None:
    """Warn when every `dag_bag` consumer already carries its own `xdist_group`.

    Regression test for issue #242. `docs/guide/seeding.md` and `_compat/seed.py`
    actively tell users to hand-write `xdist_group` to avoid metadata-database seed
    collisions, so a suite where *every* consumer is pre-grouped is a realistic
    outcome of following the documentation -- and it leaves the catalog with no
    eligible anchor. The pre-existing behavior was to return silently, costing one
    extra full Dag parse with no signal at all.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    record_dir = pytester.path / "records"
    record_dir.mkdir()
    dag_folder = pytester.path / "dags"
    dag_folder.mkdir()
    (dag_folder / "colocate.py").write_text(dedent(_COLOCATION_DAG), encoding="utf-8")
    pytester.makeconftest(_XDIST_GROUP_REPORTING_CONFTEST.format(record_dir=str(record_dir)))
    pytester.makepyfile(
        """
        import pytest

        @pytest.mark.xdist_group(name="user-group")
        def test_pre_grouped(dag_bag):
            pass
        """
    )

    result = pytester.runpytest_subprocess(
        "-q", "--dist=loadgroup", "--airflow-smoke", "--dag-folder", str(dag_folder)
    )

    result.assert_outcomes(passed=11)
    output = result.stdout.str() + result.stderr.str()
    assert "SmokeColocationWarning" in output
    assert "carries an explicit `xdist_group` marker" in output
    assert "adding one full Dag parse to this run" in output
    groups = json.loads((record_dir / "groups.json").read_text(encoding="utf-8"))
    smoke_groups = {name: group for name, group in groups.items() if "::smoke::" in name}
    assert smoke_groups
    assert all(group is None for group in smoke_groups.values())
    pre_grouped_group = next(
        group for name, group in groups.items() if name.endswith("::test_pre_grouped")
    )
    assert pre_grouped_group == "user-group"


def test_missing_anchor_warns_when_the_mark_expression_drops_every_dag_bag_consumer(
    pytester: pytest.Pytester,
) -> None:
    """Warn when `-m` is about to deselect every `dag_bag` consumer in the run.

    Companion to `test_colocation_skips_a_dag_bag_consumer_dropped_by_mark_expression`,
    which pins the *grouping* decision; this pins the diagnostic that decision now
    emits. `-m smoke` is the documented way to run just the catalog, so this is the
    case a user most easily stumbles into.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    dag_folder = pytester.path / "dags"
    dag_folder.mkdir()
    (dag_folder / "colocate.py").write_text(dedent(_COLOCATION_DAG), encoding="utf-8")
    pytester.makepyfile(
        """
        def test_consumer(dag_bag):
            pass
        """
    )

    result = pytester.runpytest_subprocess(
        "-q",
        "--dist=loadgroup",
        "--airflow-smoke",
        "--dag-folder",
        str(dag_folder),
        "-m",
        "smoke",
    )

    result.assert_outcomes(passed=10)
    output = result.stdout.str() + result.stderr.str()
    assert "SmokeColocationWarning" in output
    assert "is about to be deselected by the active `-m` expression" in output


def test_missing_anchor_is_silent_when_the_run_has_no_dag_bag_consumer(
    pytester: pytest.Pytester,
) -> None:
    """Stay silent when the run has no `dag_bag` consumer to co-locate with at all.

    The branch that keeps issue #242's diagnostic from firing on every ordinary
    smoke-only run: with no consumer anywhere, the catalog's corpus builder owns the
    only Dag parse in the run, so there is nothing to share a worker with and nothing
    lost. Warning here would be pure noise.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    dag_folder = pytester.path / "dags"
    dag_folder.mkdir()
    (dag_folder / "colocate.py").write_text(dedent(_COLOCATION_DAG), encoding="utf-8")
    pytester.makepyfile(
        """
        def test_no_fixtures():
            pass
        """
    )

    result = pytester.runpytest_subprocess(
        "-q", "--dist=loadgroup", "--airflow-smoke", "--dag-folder", str(dag_folder)
    )

    result.assert_outcomes(passed=11)
    output = result.stdout.str() + result.stderr.str()
    assert "SmokeColocationWarning" not in output


def test_a_derived_fixture_consumer_still_anchors_the_smoke_catalog(
    pytester: pytest.Pytester,
) -> None:
    """Anchor the catalog on a test reaching `dag_bag` through a project fixture.

    Issue #242 reported that migrating tests off the literal `dag_bag` fixture and
    onto scoped, derived fixtures left the catalog with no anchor. It does not:
    `plugin._requires_dag_bag` reads `item.fixturenames`, which pytest assigns from
    the full fixture *closure* (`fixtureinfo.names_closure`), so a project fixture
    declaring `dag_bag` as a parameter puts `dag_bag` in every consuming test's
    closure. This pins that reported repro as *not* reproducing.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    record_dir = pytester.path / "records"
    record_dir.mkdir()
    dag_folder = pytester.path / "dags"
    dag_folder.mkdir()
    (dag_folder / "colocate.py").write_text(dedent(_COLOCATION_DAG), encoding="utf-8")
    pytester.makeconftest(
        _XDIST_GROUP_REPORTING_CONFTEST.format(record_dir=str(record_dir))
        + """

    @pytest.fixture
    def subdir_bag(dag_bag):
        return dag_bag
"""
    )
    pytester.makepyfile(
        """
        def test_derived_consumer(subdir_bag):
            pass
        """
    )

    result = pytester.runpytest_subprocess(
        "-q", "--dist=loadgroup", "--airflow-smoke", "--dag-folder", str(dag_folder)
    )

    result.assert_outcomes(passed=11)
    output = result.stdout.str() + result.stderr.str()
    assert "SmokeColocationWarning" not in output
    groups = json.loads((record_dir / "groups.json").read_text(encoding="utf-8"))
    smoke_groups = {name: group for name, group in groups.items() if "::smoke::" in name}
    assert smoke_groups
    assert all(group == DAG_BAG_XDIST_GROUP for group in smoke_groups.values())
    derived_group = next(
        group for name, group in groups.items() if name.endswith("::test_derived_consumer")
    )
    assert derived_group == DAG_BAG_XDIST_GROUP


def test_missing_anchor_warning_names_every_distinct_reason_once() -> None:
    """Join distinct disqualification reasons and drop repeats, preserving order.

    A suite can disqualify several consumers for the same reason; repeating it once
    per consumer would make the message scale with suite size instead of with the
    number of distinct problems. `dict.fromkeys` dedupes without sorting, so the
    message reads in the order collection actually hit the reasons.
    """

    with pytest.warns(smoke.SmokeColocationWarning) as caught:
        plugin._warn_missing_dag_bag_anchor(
            (
                plugin._PRE_GROUPED_ANCHOR_REASON,
                plugin._DESELECTED_ANCHOR_REASON,
                plugin._PRE_GROUPED_ANCHOR_REASON,
            )
        )

    message = str(caught[0].message)
    assert message.count(plugin._PRE_GROUPED_ANCHOR_REASON) == 1
    assert message.count(plugin._DESELECTED_ANCHOR_REASON) == 1
    assert f"{plugin._PRE_GROUPED_ANCHOR_REASON} or {plugin._DESELECTED_ANCHOR_REASON}" in message


@pytest.mark.parametrize(
    ("worker", "loadgroup", "expected"),
    [
        (None, False, False),
        (None, True, False),
        ("gw0", False, True),
        ("gw0", True, False),
    ],
)
def test_xdist_worker_without_loadgroup_detects_a_distributing_worker(
    monkeypatch: pytest.MonkeyPatch,
    pytestconfig: pytest.Config,
    worker: str | None,
    loadgroup: bool,
    expected: bool,
) -> None:
    """Detect a distributing worker whose dist mode makes `xdist_group` inert.

    `xdist.remote.setup_config` resets a worker's `dist` option to `"no"`, keeping only
    the synthetic `loadgroup` boolean, so the worker environment variable is the only
    surviving evidence that the run is distributed at all. Both halves matter: without
    the environment variable a plain serial run would be reported as degraded, and
    without the `loadgroup` check a correctly-configured `loadgroup` worker would be.

    Parameters:
        monkeypatch: pytest.MonkeyPatch setting or clearing the xdist worker variable.
        pytestconfig: pytest.Config providing a real config to override options on.
        worker: str | None naming the simulated xdist worker, or None when serial.
        loadgroup: bool simulating xdist's surviving `loadgroup` option on a worker.
        expected: bool naming the expected detection result.
    """

    if worker is None:
        monkeypatch.delenv(XDIST_WORKER_ENVIRONMENT_VARIABLE, raising=False)
    else:
        monkeypatch.setenv(XDIST_WORKER_ENVIRONMENT_VARIABLE, worker)
    # Mirror what `xdist.remote.setup_config` leaves behind on a real worker: `dist`
    # reset to `"no"`, with the original choice surviving only as `loadgroup`.
    monkeypatch.setattr(pytestconfig.option, "dist", "no", raising=False)
    monkeypatch.setattr(pytestconfig.option, "loadgroup", loadgroup, raising=False)

    assert plugin._xdist_worker_without_loadgroup(pytestconfig) is expected


def test_smoke_catalog_and_every_dag_corpus_consumer_share_one_xdist_group(
    pytester: pytest.Pytester,
) -> None:
    """Group the smoke catalog with EVERY `dag_corpus` consumer, unlike `dag_bag`'s anchor.

    Issue #277: `_colocate_dag_corpus_consumers` deliberately groups every surviving
    `dag_corpus` consumer, not just one -- `dag_corpus` consumers are expected to be
    few, cheap, read-only metadata checks, so colocating all of them costs nothing extra
    (contrast `test_smoke_catalog_and_dag_bag_consumers_share_one_xdist_group`, which
    pins the single-anchor behavior for `dag_bag`). An item that already carries its own
    explicit `xdist_group` is still never chosen or overwritten.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    record_dir = pytester.path / "records"
    record_dir.mkdir()
    dag_folder = pytester.path / "dags"
    dag_folder.mkdir()
    (dag_folder / "colocate.py").write_text(dedent(_COLOCATION_DAG), encoding="utf-8")
    pytester.makeconftest(_XDIST_GROUP_REPORTING_CONFTEST.format(record_dir=str(record_dir)))
    pytester.makepyfile(
        """
        import pytest

        def test_consumer(dag_corpus):
            pass

        def test_second_consumer(dag_corpus):
            pass

        @pytest.mark.xdist_group(name="user-group")
        def test_pre_grouped(dag_corpus):
            pass
        """
    )

    result = pytester.runpytest_subprocess(
        "-q", "--dist=loadgroup", "--airflow-smoke", "--dag-folder", str(dag_folder)
    )

    result.assert_outcomes(passed=13)
    groups = json.loads((record_dir / "groups.json").read_text(encoding="utf-8"))
    smoke_groups = {name: group for name, group in groups.items() if "::smoke::" in name}
    assert smoke_groups
    assert all(group == DAG_CORPUS_XDIST_GROUP for group in smoke_groups.values())
    consumer_group = next(
        group for name, group in groups.items() if name.endswith("::test_consumer")
    )
    second_consumer_group = next(
        group for name, group in groups.items() if name.endswith("::test_second_consumer")
    )
    pre_grouped_group = next(
        group for name, group in groups.items() if name.endswith("::test_pre_grouped")
    )
    assert consumer_group == DAG_CORPUS_XDIST_GROUP
    assert second_consumer_group == DAG_CORPUS_XDIST_GROUP
    assert pre_grouped_group == "user-group"


def test_dag_corpus_colocation_takes_precedence_over_dag_bag_colocation(
    pytester: pytest.Pytester,
) -> None:
    """Join the smoke catalog to `DAG_CORPUS_XDIST_GROUP`, never `DAG_BAG_XDIST_GROUP`.

    Pins the precedence rule from issue #277: `pytest_collection_modifyitems` runs the
    `dag_corpus` co-location pass first, and when it groups anything, hands the
    `dag_bag` pass an empty `smoke_items` list -- so a run with both kinds of consumer
    never has the smoke catalog claimed by both groups, and the `dag_bag` pass's own
    anchor search never even runs against `test_bag_consumer` here.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    record_dir = pytester.path / "records"
    record_dir.mkdir()
    dag_folder = pytester.path / "dags"
    dag_folder.mkdir()
    (dag_folder / "colocate.py").write_text(dedent(_COLOCATION_DAG), encoding="utf-8")
    pytester.makeconftest(_XDIST_GROUP_REPORTING_CONFTEST.format(record_dir=str(record_dir)))
    pytester.makepyfile(
        """
        def test_bag_consumer(dag_bag):
            pass

        def test_corpus_consumer(dag_corpus):
            pass
        """
    )

    result = pytester.runpytest_subprocess(
        "-q", "--dist=loadgroup", "--airflow-smoke", "--dag-folder", str(dag_folder)
    )

    result.assert_outcomes(passed=12)
    groups = json.loads((record_dir / "groups.json").read_text(encoding="utf-8"))
    smoke_groups = {name: group for name, group in groups.items() if "::smoke::" in name}
    assert smoke_groups
    assert all(group == DAG_CORPUS_XDIST_GROUP for group in smoke_groups.values())
    bag_group = next(
        group for name, group in groups.items() if name.endswith("::test_bag_consumer")
    )
    corpus_group = next(
        group for name, group in groups.items() if name.endswith("::test_corpus_consumer")
    )
    assert bag_group is None
    assert corpus_group == DAG_CORPUS_XDIST_GROUP


def test_missing_dag_corpus_anchor_warns_when_every_consumer_is_pre_grouped(
    pytester: pytest.Pytester,
) -> None:
    """Warn when every `dag_corpus` consumer already carries its own `xdist_group`.

    Companion to `test_missing_anchor_warns_when_every_dag_bag_consumer_is_pre_grouped`
    for the `dag_corpus` pass: with no eligible consumer to group the catalog onto, the
    corpus builder parses the Dag folder itself, and that now costs a diagnostic instead
    of a silent extra parse.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    record_dir = pytester.path / "records"
    record_dir.mkdir()
    dag_folder = pytester.path / "dags"
    dag_folder.mkdir()
    (dag_folder / "colocate.py").write_text(dedent(_COLOCATION_DAG), encoding="utf-8")
    pytester.makeconftest(_XDIST_GROUP_REPORTING_CONFTEST.format(record_dir=str(record_dir)))
    pytester.makepyfile(
        """
        import pytest

        @pytest.mark.xdist_group(name="user-group")
        def test_pre_grouped(dag_corpus):
            pass
        """
    )

    result = pytester.runpytest_subprocess(
        "-q", "--dist=loadgroup", "--airflow-smoke", "--dag-folder", str(dag_folder)
    )

    result.assert_outcomes(passed=11)
    output = result.stdout.str() + result.stderr.str()
    assert "SmokeColocationWarning" in output
    assert "carries an explicit `xdist_group` marker" in output
    assert "adding one full Dag parse to this run" in output
    groups = json.loads((record_dir / "groups.json").read_text(encoding="utf-8"))
    smoke_groups = {name: group for name, group in groups.items() if "::smoke::" in name}
    assert smoke_groups
    assert all(group is None for group in smoke_groups.values())
    pre_grouped_group = next(
        group for name, group in groups.items() if name.endswith("::test_pre_grouped")
    )
    assert pre_grouped_group == "user-group"


def test_dag_corpus_colocation_is_a_noop_without_loadgroup_dist(
    pytester: pytest.Pytester,
) -> None:
    """Leave every item ungrouped when `--dist=loadgroup` is not in effect.

    Coverage pin for `_colocate_dag_corpus_consumers`'s early return: a `dag_corpus`
    consumer exists, but a plain serial run never even scans for one.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    record_dir = pytester.path / "records"
    record_dir.mkdir()
    dag_folder = pytester.path / "dags"
    dag_folder.mkdir()
    (dag_folder / "colocate.py").write_text(dedent(_COLOCATION_DAG), encoding="utf-8")
    pytester.makeconftest(_XDIST_GROUP_REPORTING_CONFTEST.format(record_dir=str(record_dir)))
    pytester.makepyfile(
        """
        def test_consumer(dag_corpus):
            pass
        """
    )

    result = pytester.runpytest_subprocess(
        "-q", "--airflow-smoke", "--dag-folder", str(dag_folder)
    )

    result.assert_outcomes(passed=11)
    groups = json.loads((record_dir / "groups.json").read_text(encoding="utf-8"))
    assert all(group is None for group in groups.values())


def test_dag_corpus_request_forces_serialization_without_loadgroup_dist(
    pytester: pytest.Pytester,
) -> None:
    """Serialize every Dag for a `dag_corpus` consumer even without `--dist loadgroup`.

    Regression test: `_colocate_dag_corpus_consumers`'s actual xdist *grouping* is
    gated on `--dist loadgroup`, but `dagcorpus.mark_dag_corpus_requested` must not be
    -- `--dist loadgroup` is opt-in and the overwhelming majority of runs never pass it.
    Gating the mark on successful grouping would leave a bare `dag_corpus`-only run
    against a project with `--airflow-smoke` enabled and every serialization-backed
    smoke item disabled exposed to exactly the ini-knob-controls-a-public-fixture gap
    issue #277 set out to close: the corpus would silently come back with every
    `.serialized` field `None`.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    dag_folder = pytester.path / "dags"
    dag_folder.mkdir()
    (dag_folder / "colocate.py").write_text(dedent(_COLOCATION_DAG), encoding="utf-8")
    pytester.makeini(
        "[pytest]\n"
        "airflow_smoke = true\n"
        "airflow_smoke_disable =\n"
        "    test_dag_serialization_roundtrip\n"
        "    test_schedule_sanity\n"
    )
    pytester.makepyfile(
        """
        def test_consumer(dag_corpus):
            dag = dag_corpus.dags["colocate_dag"]
            assert dag.serialized is not None
        """
    )

    result = pytester.runpytest_subprocess("-q", f"--dag-folder={dag_folder}")

    assert result.ret == pytest.ExitCode.OK
    output = result.stdout.str() + result.stderr.str()
    assert "INTERNALERROR" not in output


def test_missing_dag_corpus_anchor_warning_names_every_distinct_reason_once() -> None:
    """Join distinct disqualification reasons and drop repeats, preserving order.

    Mirrors `test_missing_anchor_warning_names_every_distinct_reason_once` for
    `_warn_missing_dag_corpus_anchor`, which reuses the exact same disqualification
    reason constants since both passes share `_anchor_disqualification`.
    """

    with pytest.warns(smoke.SmokeColocationWarning) as caught:
        plugin._warn_missing_dag_corpus_anchor(
            (
                plugin._PRE_GROUPED_ANCHOR_REASON,
                plugin._DESELECTED_ANCHOR_REASON,
                plugin._PRE_GROUPED_ANCHOR_REASON,
            )
        )

    message = str(caught[0].message)
    assert message.count(plugin._PRE_GROUPED_ANCHOR_REASON) == 1
    assert message.count(plugin._DESELECTED_ANCHOR_REASON) == 1
    assert f"{plugin._PRE_GROUPED_ANCHOR_REASON} or {plugin._DESELECTED_ANCHOR_REASON}" in message
