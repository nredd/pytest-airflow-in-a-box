"""Exercise DagBag parsing and synthetic Dag-item collection under pytest-xdist.

xdist hard-fails a run with "Different tests were collected" on any cross-worker
divergence, so for the collection scenario the run itself is the assertion. The
DagBag scenario additionally proves per-worker parse determinism through the
artifact-file pattern from ``tests/bootstrap/test_bootstrap.py``.

References:
    https://pytest-xdist.readthedocs.io/en/stable/distribution.html
    https://github.com/nredd/pytest-airflow-in-a-box/issues/45
"""

from __future__ import annotations

import json
import platform
import sys
from pathlib import Path
from textwrap import dedent

import pytest

pytestmark = pytest.mark.compat

CORPUS = Path(__file__).parents[1] / "dags"
CORPUS_DAG_IDS = [
    "asset_consumer",
    "asset_outlet",
    "chained",
    "happy_path",
    "mapped",
    "provider_composition",
    "sensor",
]

# Every nested run re-bootstraps Airflow from scratch; macOS and musl CI hosts
# need roughly double the glibc-Linux budget.
NESTED_RUN_TIMEOUT_SECONDS = (
    300 if sys.platform == "linux" and platform.libc_ver()[0] == "glibc" else 600
)

_DAG_BAG_SUITE = """
    import json
    import os
    from pathlib import Path

    RECORD_DIR = Path({record_dir!r})


    def test_corpus_parses_deterministically(dag_bag):
        worker = os.environ["PYTEST_XDIST_WORKER"]
        record = {{"worker": worker, "dag_ids": sorted(dag_bag.dags)}}
        (RECORD_DIR / worker).write_text(json.dumps(record), encoding="utf-8")
"""

_SHARED_SMOKE_DAG = """
    import json
    import os
    from pathlib import Path

    try:
        from airflow.sdk import DAG, task

        DAG_KWARGS = {{}}
    except ImportError:  # Airflow 2.x: the pre-Task-SDK authoring surface, and below
        # 2.8 an unscheduled Dag still needs an explicit `start_date`.
        from datetime import datetime, timezone

        from airflow.decorators import task
        from airflow.models.dag import DAG

        DAG_KWARGS = {{"start_date": datetime(2024, 1, 1, tzinfo=timezone.utc)}}

    RECORD_DIR = Path({record_dir!r})
    record = {{"worker": os.environ["PYTEST_XDIST_WORKER"], "pid": os.getpid()}}
    (RECORD_DIR / f"import-{{os.getpid()}}.json").write_text(
        json.dumps(record), encoding="utf-8"
    )

    with DAG(dag_id="shared_smoke", schedule=None, tags=["team-a"], **DAG_KWARGS) as dag:
        @task
        def work():
            pass

        work()
"""

_SMOKE_REPORTING_CONFTST = """
    import json
    import os
    from pathlib import Path

    import pytest

    RECORD_DIR = Path({record_dir!r})


    @pytest.hookimpl(trylast=True)
    def pytest_collection_modifyitems(items):
        user_item = next(item for item in items if item.name == "test_user_smoke")
        assert user_item.get_closest_marker("xdist_group") is None


    def pytest_runtest_logreport(report):
        if report.when != "call" or "::smoke::" not in report.nodeid:
            return
        worker = getattr(report, "worker_id", os.environ.get("PYTEST_XDIST_WORKER", "master"))
        # A grouped item's nodeid gets an `@<xdist_group>` suffix, and this plugin's
        # own group names contain `::` -- `rsplit("::", 1)` would collide every
        # grouped item onto one filename, so every separator is replaced instead.
        name = report.nodeid.replace("/", "_").replace("::", "-").replace("@", "-")
        record = {{"worker": worker, "nodeid": report.nodeid}}
        (RECORD_DIR / f"item-{{worker}}-{{name}}.json").write_text(
            json.dumps(record), encoding="utf-8"
        )
"""


@pytest.mark.timeout(NESTED_RUN_TIMEOUT_SECONDS)
def test_dag_bag_is_identical_on_every_worker(pytester: pytest.Pytester) -> None:
    """Parse the bundled corpus into the same session Dag bag on both workers."""

    record_dir = pytester.path / "records"
    record_dir.mkdir()
    source = _DAG_BAG_SUITE.format(record_dir=str(record_dir))
    pytester.makepyfile(test_dag_bag_first=source, test_dag_bag_second=source)

    result = pytester.runpytest_subprocess(
        "-q", "-n", "2", "--dist=loadfile", f"--dag-folder={CORPUS}"
    )

    result.assert_outcomes(passed=2)
    records = [
        json.loads(path.read_text(encoding="utf-8")) for path in sorted(record_dir.iterdir())
    ]
    assert len(records) == 2
    assert len({record["worker"] for record in records}) == 2
    assert [record["dag_ids"] for record in records] == [CORPUS_DAG_IDS, CORPUS_DAG_IDS]


@pytest.mark.timeout(NESTED_RUN_TIMEOUT_SECONDS)
def test_smoke_items_share_one_parse_and_one_worker(
    pytester: pytest.Pytester,
) -> None:
    """Keep the whole bundled catalog, and its one parse, on a single worker.

    Regression test for issue #327: this run has neither a `dag_bag` nor a
    `dag_corpus` consumer to anchor onto (the DAG file only records its own import,
    the standalone `test_user_smoke` never touches either fixture), so before this
    fix the bundled smoke items were left ungrouped and `--dist loadgroup` load
    balanced them individually -- each worker they landed on independently called
    `dagcorpus.get_dag_corpus`, decoding and retaining the full serialized corpus.
    `_group_smoke_catalog_fallback` now forces every item onto one worker via
    `SMOKE_CATALOG_FALLBACK_XDIST_GROUP`, so both the corpus parse (already shared
    through the on-disk artifact -- `dagcorpus._shared_dag_corpus`) and the retained
    decoded `DagCorpus` are bounded to that single worker.
    """

    record_dir = pytester.path / "smoke-records"
    record_dir.mkdir()
    dag_folder = pytester.path / "dags"
    dag_folder.mkdir()
    (dag_folder / "shared_smoke.py").write_text(
        dedent(_SHARED_SMOKE_DAG.format(record_dir=str(record_dir))), encoding="utf-8"
    )
    pytester.makeconftest(_SMOKE_REPORTING_CONFTST.format(record_dir=str(record_dir)))
    pytester.makepyfile(
        """
        import pytest

        @pytest.mark.smoke
        def test_user_smoke():
            pass
        """
    )

    result = pytester.runpytest_subprocess(
        "-q",
        "-n",
        "4",
        "--dist=loadgroup",
        "--maxschedchunk=1",
        "--airflow-smoke",
        "--dag-folder",
        str(dag_folder),
        "-m",
        "smoke",
    )

    result.assert_outcomes(passed=11)
    import_records = [
        json.loads(path.read_text(encoding="utf-8")) for path in record_dir.glob("import-*.json")
    ]
    item_records = [
        json.loads(path.read_text(encoding="utf-8")) for path in record_dir.glob("item-*.json")
    ]
    assert len(import_records) == 1
    assert len(item_records) == 10
    assert len({record["worker"] for record in item_records}) == 1


_COLOCATION_DAG = """
    from pathlib import Path

    try:
        from airflow.sdk import DAG, task

        DAG_KWARGS = {{}}
    except ImportError:  # Airflow 2.x: the pre-Task-SDK authoring surface, and below
        # 2.8 an unscheduled Dag still needs an explicit `start_date`.
        from datetime import datetime, timezone

        from airflow.decorators import task
        from airflow.models.dag import DAG

        DAG_KWARGS = {{"start_date": datetime(2024, 1, 1, tzinfo=timezone.utc)}}

    with Path({counter!r}).open("a", encoding="utf-8") as handle:
        handle.write("x")

    with DAG(dag_id="colocate_dag", schedule=None, tags=["team-a"], **DAG_KWARGS) as dag:
        @task
        def work():
            pass

        work()
"""

_WORKER_REPORTING_CONFTEST = """
    import json
    import os
    from pathlib import Path

    RECORD_DIR = Path({record_dir!r})


    def pytest_runtest_logreport(report):
        # The controller re-fires this hook for reports relayed up from workers for
        # terminal output; only the worker's own firing has PYTEST_XDIST_WORKER set.
        worker = os.environ.get("PYTEST_XDIST_WORKER")
        if report.when != "call" or worker is None:
            return
        name = report.nodeid.replace("/", "_").replace("::", "-")
        record = {{"worker": worker, "nodeid": report.nodeid}}
        (RECORD_DIR / f"{{worker}}-{{name}}.json").write_text(
            json.dumps(record), encoding="utf-8"
        )
"""


@pytest.mark.timeout(NESTED_RUN_TIMEOUT_SECONDS)
def test_smoke_catalog_and_dag_bag_consumer_share_one_worker_and_parse(
    pytester: pytest.Pytester,
) -> None:
    """Parse the Dag folder once when the catalog and a `dag_bag` consumer coexist.

    Regression test for issue #163: an ungrouped smoke catalog could land on a
    different worker than a `dag_bag` consumer under `--dist loadgroup`, so the
    corpus was parsed twice, concurrently, on separate workers. Both now share
    `DAG_BAG_XDIST_GROUP`, which forces `--dist loadgroup` to schedule them onto
    the same worker, so the process-local live-`DagBag` reuse (`fixtures/dagbag.py`)
    actually triggers.
    """

    counter = pytester.path / "parses.txt"
    record_dir = pytester.path / "records"
    record_dir.mkdir()
    dag_folder = pytester.path / "dags"
    dag_folder.mkdir()
    (dag_folder / "colocate.py").write_text(
        dedent(_COLOCATION_DAG.format(counter=str(counter))), encoding="utf-8"
    )
    pytester.makeconftest(_WORKER_REPORTING_CONFTEST.format(record_dir=str(record_dir)))
    pytester.makepyfile(
        """
        def test_consumer(dag_bag):
            assert set(dag_bag.dags) == {"colocate_dag"}
        """
    )

    result = pytester.runpytest_subprocess(
        "-q",
        "-n",
        "2",
        "--dist=loadgroup",
        "--airflow-smoke",
        "--dag-folder",
        str(dag_folder),
    )

    result.assert_outcomes(passed=11)
    assert counter.read_text(encoding="utf-8").count("x") == 1
    records = [json.loads(path.read_text(encoding="utf-8")) for path in record_dir.iterdir()]
    assert len(records) == 11
    assert len({record["worker"] for record in records}) == 1


@pytest.mark.timeout(NESTED_RUN_TIMEOUT_SECONDS)
def test_smoke_and_dag_folder_collection_are_xdist_consistent(
    pytester: pytest.Pytester,
) -> None:
    """Collect identical synthetic smoke and Dag-import items on both workers.

    The corpus deliberately bundles ``broken.py``, so its `dag-import` item and
    the smoke integrity item fail by design; pinning the exact outcome counts is
    what proves both workers collected and ran the same synthetic items.
    """

    result = pytester.runpytest_subprocess(
        "-q",
        "-n",
        "2",
        "--airflow-smoke",
        f"--dag-folder={CORPUS}",
        f"--collect-dag-folder={CORPUS}",
        str(CORPUS),
    )

    result.assert_outcomes(passed=17, failed=2)
    assert "Different tests were collected" not in result.stdout.str()
    result.stdout.fnmatch_lines(["*dag-import*"])


@pytest.mark.timeout(NESTED_RUN_TIMEOUT_SECONDS)
def test_node_id_positional_drops_smoke_catalog_on_every_worker(
    pytester: pytest.Pytester,
) -> None:
    """Drop the synthetic catalog consistently across workers on explicit node selection.

    Workers re-parse the master's raw command line, so the node-ID positional must
    scope every worker to the one selected test with no cross-worker divergence
    (https://github.com/nredd/pytest-airflow-in-a-box/issues/54).
    """

    pytester.makepyfile(
        test_regular="""
        def test_regular():
            assert True
        """
    )

    result = pytester.runpytest_subprocess(
        "-q",
        "-n",
        "2",
        "--airflow-smoke",
        f"--dag-folder={CORPUS}",
        "test_regular.py::test_regular",
    )

    result.assert_outcomes(passed=1)
    assert "Different tests were collected" not in result.stdout.str()
    assert "::smoke" not in result.stdout.str()


@pytest.mark.timeout(NESTED_RUN_TIMEOUT_SECONDS)
def test_warns_that_loadgroup_would_colocate_the_catalog(pytester: pytest.Pytester) -> None:
    """Advise `--dist loadgroup` when a plain `-n` run forgoes catalog co-location.

    Regression test for issue #242. `--dist` defaults to `no` and a bare `-n` promotes
    it to `load`, never `loadgroup`, so the most common parallel invocation makes
    `xdist_group` inert and co-location is never even attempted -- costing one extra
    full Dag parse whenever the run does have a `dag_bag` consumer to reuse. Previously
    silent. The warning is emitted from `gw0` only, so a run with more workers does not
    record one identical copy per worker.
    """

    counter = pytester.path / "parses.txt"
    dag_folder = pytester.path / "dags"
    dag_folder.mkdir()
    (dag_folder / "colocate.py").write_text(
        dedent(_COLOCATION_DAG.format(counter=str(counter))), encoding="utf-8"
    )
    pytester.makepyfile(
        """
        def test_consumer(dag_bag):
            assert set(dag_bag.dags) == {"colocate_dag"}
        """
    )

    result = pytester.runpytest_subprocess(
        "-q", "-n", "2", "--airflow-smoke", "--dag-folder", str(dag_folder)
    )

    result.assert_outcomes(passed=11)
    output = result.stdout.str() + result.stderr.str()
    assert "SmokeColocationWarning" in output
    assert "Pass `--dist loadgroup` to avoid it" in output
    assert output.count("SmokeColocationWarning:") == 1


@pytest.mark.timeout(NESTED_RUN_TIMEOUT_SECONDS)
def test_loadgroup_advice_is_silent_without_a_dag_bag_consumer(
    pytester: pytest.Pytester,
) -> None:
    """Stay silent on a plain `-n` run whose suite has no `dag_bag` consumer.

    The counterpart branch to `test_warns_that_loadgroup_would_colocate_the_catalog`:
    with nothing to co-locate with, `--dist loadgroup` would save nothing, so advising
    it would be noise on every ordinary `-n auto` smoke run.
    """

    counter = pytester.path / "parses.txt"
    dag_folder = pytester.path / "dags"
    dag_folder.mkdir()
    (dag_folder / "colocate.py").write_text(
        dedent(_COLOCATION_DAG.format(counter=str(counter))), encoding="utf-8"
    )
    pytester.makepyfile(
        """
        def test_no_fixtures():
            pass
        """
    )

    result = pytester.runpytest_subprocess(
        "-q", "-n", "2", "--airflow-smoke", "--dag-folder", str(dag_folder)
    )

    result.assert_outcomes(passed=11)
    output = result.stdout.str() + result.stderr.str()
    assert "SmokeColocationWarning" not in output


@pytest.mark.timeout(NESTED_RUN_TIMEOUT_SECONDS)
def test_missing_anchor_warning_is_recorded_once_across_workers(
    pytester: pytest.Pytester,
) -> None:
    """Record the missing-anchor advisory once, not once per xdist worker.

    Every worker collects in its own process and reaches the same run-wide conclusion,
    so an unguarded `warnings.warn` in a collection hook produces N identical entries
    describing one condition. `plugin._warns_for_this_process` elects `gw0`, which
    always exists in a distributing run.
    """

    counter = pytester.path / "parses.txt"
    dag_folder = pytester.path / "dags"
    dag_folder.mkdir()
    (dag_folder / "colocate.py").write_text(
        dedent(_COLOCATION_DAG.format(counter=str(counter))), encoding="utf-8"
    )
    pytester.makepyfile(
        """
        import pytest

        @pytest.mark.xdist_group(name="user-group")
        def test_pre_grouped(dag_bag):
            assert set(dag_bag.dags) == {"colocate_dag"}
        """
    )

    result = pytester.runpytest_subprocess(
        "-q",
        "-n",
        "2",
        "--dist=loadgroup",
        "--airflow-smoke",
        "--dag-folder",
        str(dag_folder),
    )

    result.assert_outcomes(passed=11)
    output = result.stdout.str() + result.stderr.str()
    assert "carries an explicit `xdist_group` marker" in output
    assert output.count("SmokeColocationWarning:") == 1


def test_missing_dag_corpus_anchor_warning_is_recorded_once_across_workers(
    pytester: pytest.Pytester,
) -> None:
    """Record the missing-`dag_corpus`-anchor advisory once, not once per xdist worker.

    Companion to `test_missing_anchor_warning_is_recorded_once_across_workers` for the
    `dag_corpus` pass's call into `_warn_missing_anchor`: every worker collects in its
    own process and reaches the same run-wide conclusion, so an unguarded
    `warnings.warn` in a collection hook would produce N identical entries describing
    one condition.
    `plugin._warns_for_this_process` elects `gw0`, which always exists in a
    distributing run.
    """

    counter = pytester.path / "parses.txt"
    dag_folder = pytester.path / "dags"
    dag_folder.mkdir()
    (dag_folder / "colocate.py").write_text(
        dedent(_COLOCATION_DAG.format(counter=str(counter))), encoding="utf-8"
    )
    pytester.makepyfile(
        """
        import pytest

        @pytest.mark.xdist_group(name="user-group")
        def test_pre_grouped(dag_corpus):
            assert set(dag_corpus.dags) == {"colocate_dag"}
        """
    )

    result = pytester.runpytest_subprocess(
        "-q",
        "-n",
        "2",
        "--dist=loadgroup",
        "--airflow-smoke",
        "--dag-folder",
        str(dag_folder),
    )

    result.assert_outcomes(passed=11)
    output = result.stdout.str() + result.stderr.str()
    assert "carries an explicit `xdist_group` marker" in output
    assert output.count("SmokeColocationWarning:") == 1
