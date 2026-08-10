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


    def test_corpus_parses_deterministically(full_dag_bag):
        worker = os.environ["PYTEST_XDIST_WORKER"]
        record = {{"worker": worker, "dag_ids": sorted(full_dag_bag.dags)}}
        (RECORD_DIR / worker).write_text(json.dumps(record), encoding="utf-8")
"""


@pytest.mark.timeout(NESTED_RUN_TIMEOUT_SECONDS)
def test_full_dag_bag_is_identical_on_every_worker(pytester: pytest.Pytester) -> None:
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

    result.assert_outcomes(passed=12, failed=2)
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
