"""Test the portable, fan-out-eligible Dag corpus builder and its cross-process cache."""

from __future__ import annotations

import contextlib
import dataclasses
import logging
import os
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from pytest_airflow_in_a_box import dagcorpus, smoke
from pytest_airflow_in_a_box._compat.introspection import SecretsLookup
from pytest_airflow_in_a_box.fixtures import dagbag


def _config(
    *,
    parse_timeout: object = "30",
    snapshot_dir: object = "",
    sample_size: object = "0",
    sample_seed: object = "0",
    disable: object = (),
    airflow_smoke: object = None,
    ini_smoke: object = False,
    fanout: object = None,
    ini_fanout: object = False,
    fanout_workers: object = "0",
    fanout_min_files: object = "200",
    fanout_timeout: object = "600",
    rootpath: Path = Path("/repo"),
) -> Any:
    """Create a minimal configuration double for corpus-builder tests.

    Defaults `airflow_smoke`/`ini_smoke` to disabled, matching a `dag_corpus`-only run
    with the bundled smoke catalog off: `_corpus_serialization_needed` then falls through
    to its own "serialize everything" default, exactly like every corpus-build test here
    exercised before the smoke catalog gained a say in the matter.

    Parameters:
        parse_timeout: object containing the ``airflow_dag_parse_timeout`` ini value.
        snapshot_dir: object containing the ``airflow_dag_snapshot_dir`` ini value.
        sample_size: object containing the ``airflow_serialization_sample_size`` ini value.
        sample_seed: object containing the ``airflow_serialization_sample_seed`` ini value.
        disable: object containing the ``airflow_smoke_disable`` ini value.
        airflow_smoke: object containing the parsed ``--airflow-smoke`` option value.
        ini_smoke: object containing the ``airflow_smoke`` ini value.
        fanout: object containing the parsed ``--airflow-dag-bag-fanout`` option value.
        ini_fanout: object containing the ``airflow_dag_bag_fanout`` ini value.
        fanout_workers: object containing the ``airflow_dag_bag_fanout_workers`` ini value.
        fanout_min_files: object containing the ``airflow_dag_bag_fanout_min_files`` ini value.
        fanout_timeout: object containing the ``airflow_dag_bag_fanout_timeout`` ini value.
        rootpath: pathlib.Path used to resolve a relative snapshot directory.

    Returns:
        types.SimpleNamespace shaped like the configuration surface under test.
    """

    ini_values = {
        "airflow_dag_parse_timeout": parse_timeout,
        "airflow_dag_snapshot_dir": snapshot_dir,
        "airflow_serialization_sample_size": sample_size,
        "airflow_serialization_sample_seed": sample_seed,
        "airflow_smoke_disable": list(disable) if isinstance(disable, (list, tuple)) else disable,
        "airflow_smoke": ini_smoke,
        # Parse-time secrets resolution has its own end-to-end coverage in
        # `tests/enduser/test_parse_time_secrets.py`; the corpus builder is under test
        # here, so the shim stays out of the way.
        "airflow_parse_secrets": "off",
        "airflow_dag_bag_fanout": ini_fanout,
        "airflow_dag_bag_fanout_workers": fanout_workers,
        "airflow_dag_bag_fanout_min_files": fanout_min_files,
        "airflow_dag_bag_fanout_timeout": fanout_timeout,
    }
    option_values = {
        "airflow_smoke": airflow_smoke,
        "airflow_parse_secrets": None,
        "airflow_dag_bag_fanout": fanout,
    }
    return SimpleNamespace(
        getoption=lambda name: option_values[name],
        getini=lambda name: ini_values[name],
        stash=pytest.Stash(),
        rootpath=rootpath,
    )


def _stat(file: str, seconds: float, *, dags: int = 1, tasks: int = 1) -> Any:
    """Create a minimal ``FileLoadStat``-shaped double for parse-statistics tests.

    Parameters:
        file: str containing the Dag file name.
        seconds: float containing the parse duration in seconds.
        dags: int containing the Dag count for the file.
        tasks: int containing the task count for the file.

    Returns:
        Any shaped like Airflow's ``FileLoadStat`` NamedTuple.
    """

    return SimpleNamespace(
        file=file,
        duration=timedelta(seconds=seconds),
        dag_num=dags,
        task_num=tasks,
        dags="[]",
        warning_num=0,
        bundle_path=None,
        bundle_name=None,
    )


def _task(task_id: str, *, owner: str = "someone", pool: str = "default_pool") -> Any:
    """Create a minimal task double exposing only what the corpus builder reads.

    Parameters:
        task_id: str identifying the task.
        owner: str naming the task's declared owner.
        pool: str naming the task's declared pool.

    Returns:
        Any shaped like the task surface under test.
    """

    return SimpleNamespace(task_id=task_id, owner=owner, pool=pool)


def _fake_recorder(lookups: list[SecretsLookup]) -> Any:
    """Create a `record_secrets_lookups` double yielding the given lookups.

    Parameters:
        lookups: list[SecretsLookup] the double reports as recorded.

    Returns:
        Any shaped like the `record_secrets_lookups` context manager factory.
    """

    @contextlib.contextmanager
    def recorder(_folder: Path) -> Any:
        """Yield the canned lookup list without patching anything.

        Parameters:
            _folder: pathlib.Path containing the unused Dag folder.

        Returns:
            Any yielding the canned lookup list.
        """

        yield lookups

    return recorder


def _sample_corpus() -> dagcorpus.DagCorpus:
    """Create one portable corpus for cache and artifact tests.

    Returns:
        pytest_airflow_in_a_box.dagcorpus.DagCorpus containing one Dag.
    """

    return dagcorpus.DagCorpus(
        dags={
            "sample": dagcorpus.CorpusDag(
                dag_id="sample",
                tags=frozenset({"team-a"}),
                tasks=(
                    dagcorpus.CorpusTask(
                        task_id="task",
                        owner="team-a",
                        pool="default_pool",
                        is_mapped=True,
                        mapped_over_runtime_data=True,
                        max_active_tis_per_dag=4,
                    ),
                ),
                can_be_scheduled=False,
                catchup=True,
                fileloc="/dags/sample.py",
                serialized={"dag_id": "sample"},
                serialization_error=None,
                serialization_seconds=0.1,
            )
        },
        import_errors={"broken.py": "traceback"},
        dagbag_stats=(
            dagcorpus.CorpusDagFileStat(
                file="sample.py", duration=timedelta(seconds=0.25), dag_num=1, task_num=1
            ),
        ),
        runtime_lookups=(SecretsLookup(kind="variable", key="k", file="/dags/sample.py", line=3),),
        producer_pid=123,
        producer_worker="gw1",
    )


def test_parse_timeout_reads_default() -> None:
    """Parse the default timeout string into a float."""

    assert dagcorpus._parse_timeout(_config()) == 30.0


@pytest.mark.parametrize(
    ("value", "match"),
    [
        (7, "must be a number"),
        ("oops", "must be a number: 'oops'"),
        ("0", "must be positive"),
        ("-1", "must be positive"),
    ],
)
def test_parse_timeout_rejects_malformed_values(value: object, match: str) -> None:
    """Reject non-string, non-numeric, and non-positive timeout values."""

    with pytest.raises(pytest.UsageError, match=match):
        dagcorpus._parse_timeout(_config(parse_timeout=value))


def test_serialization_sample_size_reads_default() -> None:
    """Parse the default sample size string into the exhaustive sentinel."""

    assert dagcorpus._serialization_sample_size(_config()) == 0


def test_serialization_sample_size_reads_configured_value() -> None:
    """Parse a configured sample size string into an int."""

    assert dagcorpus._serialization_sample_size(_config(sample_size="25")) == 25


@pytest.mark.parametrize(
    "value",
    [7, "oops", "1.5", "-1"],
)
def test_serialization_sample_size_rejects_malformed_values(value: object) -> None:
    """Reject non-string, non-integer, and negative sample sizes."""

    with pytest.raises(pytest.UsageError, match="must be a non-negative integer"):
        dagcorpus._serialization_sample_size(_config(sample_size=value))


def test_serialization_sample_seed_reads_configured_value() -> None:
    """Return the configured seed string, defaulting to '0'."""

    assert dagcorpus._serialization_sample_seed(_config()) == "0"
    assert dagcorpus._serialization_sample_seed(_config(sample_seed="release-42")) == "release-42"


def test_serialization_sample_seed_rejects_non_string() -> None:
    """Reject a non-string seed value."""

    with pytest.raises(pytest.UsageError, match="`airflow_serialization_sample_seed` must be"):
        dagcorpus._serialization_sample_seed(_config(sample_seed=7))


def test_sampled_dag_ids_returns_all_sorted_when_exhaustive() -> None:
    """Return every id in lexical order when sampling is off or covers the corpus."""

    ids = ["bravo", "alpha", "charlie"]

    assert dagcorpus._sampled_dag_ids(ids, sample_size=0, seed="0") == [
        "alpha",
        "bravo",
        "charlie",
    ]
    assert dagcorpus._sampled_dag_ids(ids, sample_size=9, seed="0") == [
        "alpha",
        "bravo",
        "charlie",
    ]


def test_sampled_dag_ids_selects_a_deterministic_subset() -> None:
    """Select the same lexically sorted subset on every call for a fixed seed."""

    ids = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot"]

    first = dagcorpus._sampled_dag_ids(ids, sample_size=3, seed="0")
    second = dagcorpus._sampled_dag_ids(reversed(ids), sample_size=3, seed="0")

    assert first == ["alpha", "bravo", "echo"]
    assert second == first


def test_sampled_dag_ids_varies_with_seed() -> None:
    """Select a different subset when the seed changes."""

    ids = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot"]

    assert dagcorpus._sampled_dag_ids(ids, sample_size=3, seed="other") == [
        "charlie",
        "delta",
        "echo",
    ]


def test_sampled_dag_ids_rejects_negative_sample_size() -> None:
    """Reject a negative sample size."""

    with pytest.raises(ValueError, match="`sample_size` must be non-negative"):
        dagcorpus._sampled_dag_ids(["alpha"], sample_size=-1, seed="0")


def test_mark_dag_corpus_requested_stashes_the_marker() -> None:
    """Record the request marker on the config stash."""

    config = _config()

    assert dagcorpus.DAG_CORPUS_WANTS_SERIALIZATION_KEY not in config.stash

    dagcorpus.mark_dag_corpus_requested(config)

    assert config.stash[dagcorpus.DAG_CORPUS_WANTS_SERIALIZATION_KEY] is True


def test_corpus_serialization_needed_true_when_request_marker_set() -> None:
    """Serialize everything once any collected item requires `dag_corpus`."""

    config = _config()
    dagcorpus.mark_dag_corpus_requested(config)

    assert dagcorpus._corpus_serialization_needed(config) is True


def test_corpus_serialization_needed_delegates_when_smoke_enabled_and_in_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Defer to the bundled smoke catalog's own serialization need when in scope."""

    config = _config()
    monkeypatch.setattr(smoke, "_smoke_enabled", lambda _config: True)
    monkeypatch.setattr(smoke, "_smoke_in_scope", lambda _config: True)
    monkeypatch.setattr(smoke, "_smoke_serialization_needed", lambda _config: False)

    assert dagcorpus._corpus_serialization_needed(config) is False


def test_corpus_serialization_needed_defaults_to_true_when_smoke_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Serialize everything when the bundled smoke catalog is disabled."""

    config = _config()
    monkeypatch.setattr(smoke, "_smoke_enabled", lambda _config: False)

    assert dagcorpus._corpus_serialization_needed(config) is True


def test_corpus_serialization_needed_defaults_to_true_when_smoke_out_of_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Serialize everything when smoke is enabled but out of scope for this run."""

    config = _config()
    monkeypatch.setattr(smoke, "_smoke_enabled", lambda _config: True)
    monkeypatch.setattr(smoke, "_smoke_in_scope", lambda _config: False)

    assert dagcorpus._corpus_serialization_needed(config) is True


def test_dag_corpus_artifact_round_trips() -> None:
    """Preserve every portable field through the shared JSON representation."""

    corpus = _sample_corpus()

    assert dagcorpus._dag_corpus_from_payload(dagcorpus._dag_corpus_payload(corpus)) == corpus


def test_dag_corpus_artifact_round_trips_absent_runtime_lookups() -> None:
    """Preserve the `None` (uninstrumented) runtime-lookup marker through the artifact."""

    corpus = dataclasses.replace(_sample_corpus(), runtime_lookups=None)

    assert dagcorpus._dag_corpus_from_payload(dagcorpus._dag_corpus_payload(corpus)) == corpus


def test_dag_corpus_rejects_an_unknown_artifact_version() -> None:
    """Reject a shared artifact written by an incompatible plugin schema."""

    with pytest.raises(ValueError, match="Unsupported Dag corpus version"):
        dagcorpus._dag_corpus_from_payload({"version": -1})


def test_dag_corpus_build_extracts_portable_data(monkeypatch: pytest.MonkeyPatch) -> None:
    """Extract task metadata and retain one Dag's serialization failure."""

    good = SimpleNamespace(
        tags={"team-a"},
        tasks=[_task("good")],
        timetable=SimpleNamespace(can_be_scheduled=False),
    )
    broken = SimpleNamespace(
        tags=set(),
        tasks=[_task("broken", owner="airflow", pool="custom")],
        timetable=SimpleNamespace(can_be_scheduled=True),
    )
    dag_bag = SimpleNamespace(
        dags={"good": good, "broken": broken},
        import_errors={"bad.py": "boom"},
        dagbag_stats=[_stat("dags.py", 0.5, dags=2, tasks=2)],
    )

    def serialize(dag: object) -> dict[str, Any]:
        if dag is broken:
            raise ValueError("cannot serialize callback")
        return {"dag_id": "good"}

    monkeypatch.setenv("AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT", "999")
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw2")
    monkeypatch.setattr(dagcorpus, "_dag_folder", lambda _config: Path("dags"))
    monkeypatch.setattr(dagcorpus, "build_dag_bag", lambda _folder, **_kwargs: dag_bag)
    monkeypatch.setattr(dagcorpus, "record_secrets_lookups", _fake_recorder([]))
    monkeypatch.setattr(
        dagcorpus, "_get_dag_serializer", lambda: SimpleNamespace(serialize_dag=serialize)
    )

    session: Any = SimpleNamespace(stash=pytest.Stash())
    corpus = dagcorpus._build_dag_corpus(session, _config(parse_timeout="12.5"))

    assert corpus.dags["good"].serialized == {"dag_id": "good"}
    assert corpus.dags["broken"].serialization_error == "cannot serialize callback"
    assert corpus.dags["broken"].tasks[0].pool == "custom"
    assert corpus.producer_worker == "gw2"
    assert os.environ["AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT"] == "12.5"


def test_dag_corpus_falls_back_to_serial_parse_when_fanout_fails(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Log a warning and still build a correct corpus when fan-out itself fails."""

    good = SimpleNamespace(tags=set(), tasks=[], timetable=SimpleNamespace(can_be_scheduled=True))
    dag_bag = SimpleNamespace(dags={"good": good}, import_errors={}, dagbag_stats=[])

    def explode(**_kwargs: object) -> Any:
        raise dagcorpus.DagBagFanoutError("no workers available")

    monkeypatch.setattr(dagcorpus, "_dag_folder", lambda _config: Path("dags"))
    monkeypatch.setattr(
        dagcorpus, "get_bootstrap_state", lambda _config: SimpleNamespace(root=tmp_path)
    )
    monkeypatch.setattr(dagcorpus, "list_dag_file_paths", lambda _folder: ["dags/good.py"])
    monkeypatch.setattr(dagcorpus, "fan_out_dag_bag", explode)
    monkeypatch.setattr(dagcorpus, "build_dag_bag", lambda _folder, **_kwargs: dag_bag)
    monkeypatch.setattr(dagcorpus, "record_secrets_lookups", _fake_recorder([]))
    monkeypatch.setattr(
        dagcorpus, "_get_dag_serializer", lambda: SimpleNamespace(serialize_dag=lambda _dag: {})
    )

    session: Any = SimpleNamespace(stash=pytest.Stash())
    with caplog.at_level(logging.WARNING):
        corpus = dagcorpus._build_dag_corpus(
            session, _config(parse_timeout="1", fanout=True, fanout_min_files="0")
        )

    assert set(corpus.dags) == {"good"}
    assert "no workers available" in caplog.text


def test_dag_corpus_skips_fanout_below_the_minimum_file_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Parse serially, without ever calling `fan_out_dag_bag`, below the file-count floor."""

    good = SimpleNamespace(tags=set(), tasks=[], timetable=SimpleNamespace(can_be_scheduled=True))
    dag_bag = SimpleNamespace(dags={"good": good}, import_errors={}, dagbag_stats=[])

    def _fail_if_called(**_kwargs: object) -> Any:
        raise AssertionError("fan_out_dag_bag must not run below the minimum file count")

    monkeypatch.setattr(dagcorpus, "_dag_folder", lambda _config: Path("dags"))
    monkeypatch.setattr(
        dagcorpus, "get_bootstrap_state", lambda _config: SimpleNamespace(root=tmp_path)
    )
    monkeypatch.setattr(dagcorpus, "list_dag_file_paths", lambda _folder: ["dags/good.py"])
    monkeypatch.setattr(dagcorpus, "fan_out_dag_bag", _fail_if_called)
    monkeypatch.setattr(dagcorpus, "build_dag_bag", lambda _folder, **_kwargs: dag_bag)
    monkeypatch.setattr(dagcorpus, "record_secrets_lookups", _fake_recorder([]))
    monkeypatch.setattr(
        dagcorpus, "_get_dag_serializer", lambda: SimpleNamespace(serialize_dag=lambda _dag: {})
    )

    session: Any = SimpleNamespace(stash=pytest.Stash())
    corpus = dagcorpus._build_dag_corpus(
        session, _config(parse_timeout="1", fanout=True, fanout_min_files="2")
    )

    assert set(corpus.dags) == {"good"}


def test_dag_corpus_build_records_runtime_secrets_lookups(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Capture deduplicated runtime secrets lookups on a fresh corpus parse."""

    lookup = SecretsLookup(kind="variable", key="k", file="/dags/a.py", line=3)
    dag_bag = SimpleNamespace(dags={}, import_errors={}, dagbag_stats=[])
    monkeypatch.setenv("AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT", "999")
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "master")
    monkeypatch.setattr(dagcorpus, "_dag_folder", lambda _config: Path("dags"))
    monkeypatch.setattr(dagcorpus, "build_dag_bag", lambda _folder, **_kwargs: dag_bag)
    monkeypatch.setattr(dagcorpus, "record_secrets_lookups", _fake_recorder([lookup, lookup]))
    monkeypatch.setattr(
        dagcorpus, "_get_dag_serializer", lambda: SimpleNamespace(serialize_dag=lambda _dag: {})
    )

    session: Any = SimpleNamespace(stash=pytest.Stash())
    corpus = dagcorpus._build_dag_corpus(session, _config(parse_timeout="1"))

    assert corpus.runtime_lookups == (lookup,)


def test_dag_corpus_reuses_dag_bag_parsed_by_dag_bag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reuse a DagBag already parsed by `dag_bag` in this process, per issue #85."""

    good = SimpleNamespace(
        tags=set(),
        tasks=[],
        timetable=SimpleNamespace(can_be_scheduled=True),
    )
    dag_bag = SimpleNamespace(dags={"good": good}, import_errors={}, dagbag_stats=[])
    session: Any = SimpleNamespace(stash=pytest.Stash())
    session.stash[dagbag.LIVE_DAG_BAG_KEY] = dag_bag

    def _fail_if_called(*_args: object, **_kwargs: object) -> Any:
        raise AssertionError("build_dag_bag must not run a second time in this process")

    monkeypatch.setenv("AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT", "999")
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "master")
    monkeypatch.setattr(dagcorpus, "build_dag_bag", _fail_if_called)
    monkeypatch.setattr(
        dagcorpus,
        "_get_dag_serializer",
        lambda: SimpleNamespace(serialize_dag=lambda _dag: {}),
    )

    corpus = dagcorpus._build_dag_corpus(session, _config(parse_timeout="1"))

    assert set(corpus.dags) == {"good"}
    assert corpus.runtime_lookups is None


def test_dag_corpus_reuse_adopts_lookups_recorded_by_dag_bag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Adopt the lookups `_cached_dag_bag` recorded when reusing its DagBag."""

    good = SimpleNamespace(
        tags=set(),
        tasks=[],
        timetable=SimpleNamespace(can_be_scheduled=True),
    )
    dag_bag = SimpleNamespace(dags={"good": good}, import_errors={}, dagbag_stats=[])
    lookup = SecretsLookup(kind="variable", key="k", file=None, line=None)
    session: Any = SimpleNamespace(stash=pytest.Stash())
    session.stash[dagbag.LIVE_DAG_BAG_KEY] = dag_bag
    session.stash[dagbag.LIVE_DAG_BAG_LOOKUPS_KEY] = (lookup,)
    monkeypatch.setenv("AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT", "999")
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "master")
    monkeypatch.setattr(
        dagcorpus,
        "_get_dag_serializer",
        lambda: SimpleNamespace(serialize_dag=lambda _dag: {}),
    )

    corpus = dagcorpus._build_dag_corpus(session, _config(parse_timeout="1"))

    assert corpus.runtime_lookups == (lookup,)


def test_dag_corpus_does_not_pin_dag_bag_when_dag_bag_never_ran(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Leave the session's live-DagBag cache empty for a corpus-only run.

    Regression test for issue #85: the shared cache exists so `dag_bag` and the
    corpus builder can hand off a parse to each other, not so the corpus builder's
    own fresh parse gets pinned on the session for the rest of a run that never touches
    `dag_bag` -- that would keep a large corpus's live Dag objects alive for nothing.
    """

    good = SimpleNamespace(tags=set(), tasks=[], timetable=SimpleNamespace(can_be_scheduled=True))
    dag_bag = SimpleNamespace(dags={"good": good}, import_errors={}, dagbag_stats=[])
    session: Any = SimpleNamespace(stash=pytest.Stash())

    monkeypatch.setenv("AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT", "999")
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "master")
    monkeypatch.setattr(dagcorpus, "_dag_folder", lambda _config: Path("dags"))
    monkeypatch.setattr(dagcorpus, "build_dag_bag", lambda _folder, **_kwargs: dag_bag)
    monkeypatch.setattr(dagcorpus, "record_secrets_lookups", _fake_recorder([]))
    monkeypatch.setattr(
        dagcorpus,
        "_get_dag_serializer",
        lambda: SimpleNamespace(serialize_dag=lambda _dag: {}),
    )

    dagcorpus._build_dag_corpus(session, _config(parse_timeout="1"))

    assert dagbag.LIVE_DAG_BAG_KEY not in session.stash


def test_dag_corpus_is_built_once_and_cached_per_process(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Build one artifact, then load it from another process-shaped session."""

    builds: list[object] = []
    corpus = _sample_corpus()
    config = _config()
    monkeypatch.setattr(
        dagcorpus, "get_bootstrap_state", lambda _config: SimpleNamespace(root=tmp_path)
    )
    monkeypatch.setattr(
        dagcorpus,
        "_build_dag_corpus",
        lambda _session, config: (builds.append(config), corpus)[1],
    )
    first_session: Any = SimpleNamespace(stash=pytest.Stash())

    assert dagcorpus.get_dag_corpus(first_session, config) is corpus
    assert dagcorpus.get_dag_corpus(first_session, config) is corpus

    second_session: Any = SimpleNamespace(stash=pytest.Stash())
    loaded = dagcorpus.get_dag_corpus(second_session, config)

    assert builds == [config]
    assert loaded == corpus
    assert loaded is not corpus
