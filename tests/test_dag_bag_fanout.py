"""Exercise Dag bag fan-out (issue #243) end to end, including real subprocess spawn.

Not marked `pytest.mark.compat`: fan-out shares its Dag-parsing primitives with
`build_dag_bag` (the already-certified cross-family entry point), but the subprocess
orchestration itself was verified only against the installed Airflow 3.x release. These
tests still run on every 3.x compat leg (the whole suite does), just not on a 2.x leg,
which only runs `tests/enduser`.

References:
    https://github.com/nredd/pytest-airflow-in-a-box/issues/243
"""

from __future__ import annotations

import platform
import sys
from pathlib import Path
from textwrap import dedent

import pytest

CORPUS = Path(__file__).parent / "dags_large"

# `airflow_dag_bag_fanout_workers`/`_min_files` have no CLI companion, matching this
# repo's convention for numeric smoke-catalog knobs (e.g. `airflow_dag_parse_timeout`);
# every test below opts in through this ini fragment instead.
_FANOUT_INI = (
    "[pytest]\nairflow_dag_bag_fanout_min_files = 0\nairflow_dag_bag_fanout_workers = 2\n"
)

# Every nested run re-bootstraps Airflow from scratch; macOS and musl CI hosts need
# roughly double the glibc-Linux budget, matching `tests/enduser/test_parallel_collection.py`.
NESTED_RUN_TIMEOUT_SECONDS = (
    300 if sys.platform == "linux" and platform.libc_ver()[0] == "glibc" else 600
)


@pytest.mark.timeout(NESTED_RUN_TIMEOUT_SECONDS)
def test_fanout_matches_serial_and_respects_ancestor_ignore(pytester: pytest.Pytester) -> None:
    """Fan out over `dags_large`, passing every smoke item and never seeing the ignored Dag.

    `team_a/templates.py` declares a `should_be_ignored` Dag but is excluded by the
    root-level `tests/dags_large/.airflowignore`; a fan-out design that re-rooted a
    sub-parse at `team_a/` directly (issue #243's rejected option) would miss that
    ancestor ignore file and wrongly include it. Fan-out here shards an already-walked,
    already-correct file list, so the exclusion holds regardless of shard placement.
    """

    pytester.makeini(_FANOUT_INI)

    result = pytester.runpytest_subprocess(
        "-q", "--airflow-smoke", f"--dag-folder={CORPUS}", "--airflow-dag-bag-fanout"
    )

    result.assert_outcomes(passed=10)
    assert "should_be_ignored" not in result.stdout.str()


@pytest.mark.timeout(NESTED_RUN_TIMEOUT_SECONDS)
def test_fanout_disabled_produces_the_same_outcome(pytester: pytest.Pytester) -> None:
    """Serially parse the same corpus, as a control for the fan-out run above."""

    result = pytester.runpytest_subprocess("-q", "--airflow-smoke", f"--dag-folder={CORPUS}")

    result.assert_outcomes(passed=10)
    assert "should_be_ignored" not in result.stdout.str()


_DUPLICATE_DAG = """
    from airflow.sdk import dag


    @dag(schedule=None)
    def dup_across_shards() -> None:
        pass


    dup_across_shards()
"""


@pytest.mark.timeout(NESTED_RUN_TIMEOUT_SECONDS)
def test_fanout_detects_duplicate_dag_id_spanning_two_shards(pytester: pytest.Pytester) -> None:
    """Catch a `dag_id` collision even when each copy is parsed by a different shard.

    Regression for the correctness gap fan-out introduces by construction: two separate
    `DagBag` instances (one per shard/subprocess) never see each other's Dags, so
    Airflow's own `DagBag.bag_dag` collision detection cannot fire across a shard
    boundary. `parallel_dagbag.merge_shard_payloads` has to reproduce that detection
    itself. Two files, two workers: round-robin sharding guarantees each file lands in
    a separate shard.
    """

    pytester.makeini(_FANOUT_INI)
    dag_folder = pytester.path / "dags"
    (dag_folder / "team_a").mkdir(parents=True)
    (dag_folder / "team_b").mkdir(parents=True)
    (dag_folder / "team_a" / "one.py").write_text(dedent(_DUPLICATE_DAG), encoding="utf-8")
    (dag_folder / "team_b" / "two.py").write_text(dedent(_DUPLICATE_DAG), encoding="utf-8")

    result = pytester.runpytest_subprocess(
        "-q",
        "--airflow-smoke",
        f"--dag-folder={dag_folder}",
        "--airflow-dag-bag-fanout",
        "-m",
        "smoke",
    )

    # Matches `test_smoke.py::test_duplicate_dag_ids_fail_dedicated_item`'s real,
    # serial-parse outcome shape exactly: `test_dag_bag_integrity` also treats a
    # non-empty `import_errors` as a failure, on top of the dedicated item.
    result.assert_outcomes(passed=8, failed=2)
    result.stdout.fnmatch_lines(["*test_no_duplicate_dag_ids*"])
    result.stdout.fnmatch_lines(["*AirflowDagDuplicatedIdException*"])
    assert "also found in" in result.stdout.str()


@pytest.mark.timeout(NESTED_RUN_TIMEOUT_SECONDS)
def test_fanout_pool_spawns_exactly_once_under_xdist(pytester: pytest.Pytester) -> None:
    """Spawn exactly `fanout-workers` shard subprocesses total under `-n 2 --dist loadgroup`.

    `dagcorpus._shared_dag_corpus`'s existing cross-worker `fcntl.flock` election already
    guarantees only one xdist worker ever builds the corpus at all; fan-out lives
    entirely inside that elected build, so it must never spawn
    `fanout-workers * xdist-worker-count` processes -- only `fanout-workers`, once.

    Asserted against the per-electing-worker-PID-qualified shard *logs*
    (`dagbag-fanout-shard-{index}-{electing_pid}.log`), not the shard output
    artifacts: those are named `shard-{index}-output.json` with no producer
    qualifier, so a regressed election (both workers fanning out) would have the
    second producer silently overwrite the first's `shard-0`/`shard-1` files and
    this test would still see exactly 2 -- the logs are the one artifact that
    actually distinguishes which worker produced them.
    """

    pytester.makeini(_FANOUT_INI)
    home = pytester.path / "airflow-home"
    home.mkdir()

    result = pytester.runpytest_subprocess(
        "-q",
        "-n",
        "2",
        "--dist=loadgroup",
        "--airflow-smoke",
        f"--dag-folder={CORPUS}",
        "--airflow-dag-bag-fanout",
        f"--airflow-home={home}",
        "--airflow-home-retention=all",
    )

    result.assert_outcomes(passed=10)
    # `--airflow-home` names a base directory; the run root is a generated subdirectory
    # of it (`_airflow_home.locate_storage`'s explicit-base behavior).
    logs = sorted(home.glob("*/logs/dagbag-fanout-shard-*.log"))
    assert len(logs) == 2


@pytest.mark.timeout(NESTED_RUN_TIMEOUT_SECONDS)
def test_fanout_pool_spawns_exactly_once_with_dag_corpus_and_smoke_mixed(
    pytester: pytest.Pytester,
) -> None:
    """Spawn exactly `fanout-workers` shard subprocesses when `dag_corpus` and smoke mix.

    Companion to `test_fanout_pool_spawns_exactly_once_under_xdist`, which pins the
    single-shard-pool guarantee for a `--airflow-smoke`-only run. Issue #277 adds a
    second, independent `dag_corpus` consumer that
    `plugin._colocate_dag_corpus_consumers` now also joins to the shared
    `DAG_CORPUS_XDIST_GROUP`; `dagcorpus._shared_dag_corpus`'s cross-worker flock
    election is what keeps this at exactly one shard pool regardless of how many
    consumers request the corpus.
    """

    pytester.makeini(_FANOUT_INI)
    home = pytester.path / "airflow-home"
    home.mkdir()
    pytester.makepyfile(
        """
        def test_corpus_consumer(dag_corpus):
            assert dag_corpus.dags
        """
    )

    result = pytester.runpytest_subprocess(
        "-q",
        "-n",
        "2",
        "--dist=loadgroup",
        "--airflow-smoke",
        f"--dag-folder={CORPUS}",
        "--airflow-dag-bag-fanout",
        f"--airflow-home={home}",
        "--airflow-home-retention=all",
    )

    result.assert_outcomes(passed=11)
    # `--airflow-home` names a base directory; the run root is a generated subdirectory
    # of it (`_airflow_home.locate_storage`'s explicit-base behavior).
    logs = sorted(home.glob("*/logs/dagbag-fanout-shard-*.log"))
    assert len(logs) == 2


_DAG_CORPUS_DUMP_TEST = """
    import json
    from pathlib import Path

    DUMP_PATH = Path({dump_path!r})


    def test_dump_dag_corpus(dag_corpus):
        payload = {{
            dag_id: {{
                "tags": sorted(dag.tags),
                "fileloc": dag.fileloc,
                "task_ids": sorted(task.task_id for task in dag.tasks),
                "can_be_scheduled": dag.can_be_scheduled,
                "catchup": dag.catchup,
            }}
            for dag_id, dag in dag_corpus.dags.items()
        }}
        DUMP_PATH.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
"""


@pytest.mark.timeout(NESTED_RUN_TIMEOUT_SECONDS)
def test_fanout_and_serial_dag_corpus_metadata_match(pytester: pytest.Pytester) -> None:
    """Build identical `dag_corpus` metadata whether or not the parse was fanned out.

    The corpus's portable, non-serialized fields (`dags`/`tags`/`fileloc`/task ids/
    `can_be_scheduled`/`catchup`) must not depend on whether the parse was sharded
    across subprocesses or done in one process -- the "equivalent serial and fan-out
    runs" acceptance criterion from issue #277, made concrete. `serialized`/
    `serialization_error`/`serialization_seconds` are deliberately excluded from the
    comparison: neither run here has a consumer needing a serialized Dag, so both
    already leave every Dag unserialized, which would make comparing those fields
    vacuous either way.
    """

    fanout_dump = pytester.path / "fanout.json"
    pytester.makeini(_FANOUT_INI)
    pytester.makepyfile(_DAG_CORPUS_DUMP_TEST.format(dump_path=str(fanout_dump)))

    fanout_result = pytester.runpytest_subprocess(
        "-q", f"--dag-folder={CORPUS}", "--airflow-dag-bag-fanout"
    )

    fanout_result.assert_outcomes(passed=1)

    serial_dump = pytester.path / "serial.json"
    pytester.makepyfile(_DAG_CORPUS_DUMP_TEST.format(dump_path=str(serial_dump)))

    serial_result = pytester.runpytest_subprocess("-q", f"--dag-folder={CORPUS}")

    serial_result.assert_outcomes(passed=1)
    assert fanout_dump.read_text(encoding="utf-8") == serial_dump.read_text(encoding="utf-8")


_USES_SIBLING_PACKAGE_DAG = """
    from shared_util import CONSTANT
    from airflow.sdk import dag, task


    @dag(schedule=None)
    def uses_sibling_package() -> None:
        @task
        def emit() -> str:
            return CONSTANT

        emit()


    uses_sibling_package()
"""


@pytest.mark.timeout(NESTED_RUN_TIMEOUT_SECONDS)
def test_fanout_children_inherit_the_parents_sys_path(pytester: pytest.Pytester) -> None:
    """Import a helper package reachable only via a `pythonpath` ini entry, in a shard.

    Regression: a fresh child interpreter's own `sys.path` carries none of the
    rootdir/conftest/`pythonpath`-ini entries pytest inserted into the parent's, so a
    Dag file importing a sibling package -- a common `dags/` + shared-package repo
    layout -- would import cleanly in a serial parse and fail only under fan-out,
    unless the parent's `sys.path` is threaded through `ShardSpec`.
    """

    helpers_dir = pytester.path / "helpers"
    helpers_dir.mkdir()
    (helpers_dir / "shared_util.py").write_text("CONSTANT = 'ok'\n", encoding="utf-8")
    dag_folder = pytester.path / "dags"
    dag_folder.mkdir()
    (dag_folder / "uses_helper.py").write_text(dedent(_USES_SIBLING_PACKAGE_DAG), encoding="utf-8")
    pytester.makeini(
        "[pytest]\n"
        "pythonpath = helpers\n"
        "airflow_dag_bag_fanout_min_files = 0\n"
        "airflow_dag_bag_fanout_workers = 2\n"
    )

    result = pytester.runpytest_subprocess(
        "-q", "--airflow-smoke", f"--dag-folder={dag_folder}", "--airflow-dag-bag-fanout"
    )

    result.assert_outcomes(passed=10)
    assert "ModuleNotFoundError" not in result.stdout.str()
