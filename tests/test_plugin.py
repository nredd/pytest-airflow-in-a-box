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

from pytest_airflow_in_a_box import plugin
from pytest_airflow_in_a_box._compat import AirflowCompatibilityError
from pytest_airflow_in_a_box.fixtures.dagbag import FULL_DAG_BAG_XDIST_GROUP


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


def test_smoke_catalog_and_full_dag_bag_consumers_share_one_xdist_group(
    pytester: pytest.Pytester,
) -> None:
    """Group the smoke catalog with one `full_dag_bag` consumer, leaving the rest alone.

    Regression test for issue #163: under `--dist loadgroup`, an ungrouped smoke item
    could land on a different `pytest-xdist` worker than a `full_dag_bag` consumer, so
    the Dag folder was parsed twice, concurrently.
    `plugin._colocate_smoke_catalog_with_full_dag_bag` now forces every synthesized
    smoke item and exactly one `full_dag_bag` consumer onto the shared
    `FULL_DAG_BAG_XDIST_GROUP` -- not every consumer, so a suite with many of them does
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

        def test_consumer(full_dag_bag):
            pass

        def test_second_consumer(full_dag_bag):
            pass

        @pytest.mark.xdist_group(name="user-group")
        def test_pre_grouped(full_dag_bag):
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
    assert all(group == FULL_DAG_BAG_XDIST_GROUP for group in smoke_groups.values())
    consumer_group = next(
        group for name, group in groups.items() if name.endswith("::test_consumer")
    )
    second_consumer_group = next(
        group for name, group in groups.items() if name.endswith("::test_second_consumer")
    )
    pre_grouped_group = next(
        group for name, group in groups.items() if name.endswith("::test_pre_grouped")
    )
    assert consumer_group == FULL_DAG_BAG_XDIST_GROUP
    assert second_consumer_group is None
    assert pre_grouped_group == "user-group"


def test_colocation_skips_a_full_dag_bag_consumer_dropped_by_mark_expression(
    pytester: pytest.Pytester,
) -> None:
    """Do not group the catalog with a `full_dag_bag` consumer that `-m` will deselect.

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
        def test_consumer(full_dag_bag):
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
    force a serial run (`pytest-xdist` is a hard dependency, so it is always importable
    but not always loaded), and previously this hook still tried to add the marker
    unconditionally, aborting the run with `Failed: 'xdist_group' not found in
    \\`markers\\` configuration option` under `--strict-markers`.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    dag_folder = pytester.path / "dags"
    dag_folder.mkdir()
    (dag_folder / "colocate.py").write_text(dedent(_COLOCATION_DAG), encoding="utf-8")
    pytester.makepyfile(
        """
        def test_consumer(full_dag_bag):
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
        def test_consumer(full_dag_bag):
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
