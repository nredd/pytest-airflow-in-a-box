"""Test the portable, fan-out-eligible Dag corpus builder and its cross-process cache."""

from __future__ import annotations

import contextlib
import dataclasses
import logging
import os
from datetime import timedelta
from pathlib import Path
from types import MappingProxyType, SimpleNamespace
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
        dags=MappingProxyType(
            {
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
            }
        ),
        import_errors=MappingProxyType({"broken.py": "traceback"}),
        dagbag_stats=(
            dagcorpus.CorpusDagFileStat(
                file="sample.py", duration=timedelta(seconds=0.25), dag_num=1, task_num=1
            ),
        ),
        runtime_lookups=(SecretsLookup(kind="variable", key="k", file="/dags/sample.py", line=3),),
        producer_pid=123,
        producer_worker="gw1",
    )


def _corpus_with_filelocs(filelocs: dict[str, str]) -> dagcorpus.DagCorpus:
    """Build a corpus containing one minimal Dag per entry, keyed by `dag_id`.

    Parameters:
        filelocs: dict[str, str] mapping `dag_id` to its `fileloc`.

    Returns:
        pytest_airflow_in_a_box.dagcorpus.DagCorpus containing one Dag per entry, with
        every other field set to a minimal placeholder value.
    """

    return dagcorpus.DagCorpus(
        dags=MappingProxyType(
            {
                dag_id: dagcorpus.CorpusDag(
                    dag_id=dag_id,
                    tags=frozenset(),
                    tasks=(),
                    can_be_scheduled=True,
                    catchup=False,
                    fileloc=fileloc,
                    serialized=None,
                    serialization_error=None,
                    serialization_seconds=0.0,
                )
                for dag_id, fileloc in filelocs.items()
            }
        ),
        import_errors=MappingProxyType({}),
        dagbag_stats=(),
        runtime_lookups=(),
        producer_pid=1,
        producer_worker="master",
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


def test_effective_sample_size_passes_through_when_dag_corpus_not_requested() -> None:
    """Return the raw configured sample size absent a `dag_corpus` request.

    Matches `_serialization_sample_size` exactly in this case -- the smoke-only sampling
    optimization is unaffected by `_effective_sample_size`'s existence.
    """

    assert dagcorpus._effective_sample_size(_config()) == 0
    assert dagcorpus._effective_sample_size(_config(sample_size="5")) == 5


def test_effective_sample_size_collapses_to_zero_when_dag_corpus_requested() -> None:
    """Ignore a nonzero configured sample size once `dag_corpus` is requested.

    `_corpus_serialization_needed` already treats a `dag_corpus` request as "serialize
    every Dag"; `_effective_sample_size` must agree, or `_select_serialization_sample`
    would silently honor a smoke-catalog sample size instead.
    """

    config = _config(sample_size="2")
    dagcorpus.mark_dag_corpus_requested(config)

    assert dagcorpus._effective_sample_size(config) == 0


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

    assert dagcorpus.DAG_CORPUS_REQUESTED_KEY not in config.stash

    dagcorpus.mark_dag_corpus_requested(config)

    assert config.stash[dagcorpus.DAG_CORPUS_REQUESTED_KEY] is True


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


def test_dag_corpus_dags_and_import_errors_are_read_only() -> None:
    """Reject mutation of `dags`/`import_errors`, matching the issue's immutable-model ask.

    `DagCorpus` is `frozen=True`, but that alone only blocks reassigning an attribute --
    a plain `dict` field would still let a consumer mutate the shared, cached corpus out
    from under every other consumer sharing it in the same worker process. `dags`/
    `import_errors` are `MappingProxyType` views specifically to close that gap.
    """

    corpus = _sample_corpus()
    # `Mapping` (the field's static type) has no `__setitem__` at all -- typed `Any` here
    # deliberately, to drive the *runtime* `MappingProxyType` rejection under test rather
    # than a static one `ty` would otherwise catch first.
    dags: Any = corpus.dags
    import_errors: Any = corpus.import_errors

    with pytest.raises(TypeError):
        dags["new"] = corpus.dags["sample"]
    with pytest.raises(TypeError):
        import_errors["new.py"] = "traceback"


def test_dag_corpus_from_payload_produces_read_only_mappings() -> None:
    """Reject mutation of a `dags`/`import_errors` decoded from a shared artifact."""

    payload = dagcorpus._dag_corpus_payload(_sample_corpus())

    corpus = dagcorpus._dag_corpus_from_payload(payload)
    dags: Any = corpus.dags
    import_errors: Any = corpus.import_errors

    with pytest.raises(TypeError):
        dags["new"] = corpus.dags["sample"]
    with pytest.raises(TypeError):
        import_errors["new.py"] = "traceback"


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


def test_dag_corpus_serializes_every_dag_despite_a_configured_sample_size_when_requested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Serialize every Dag once `dag_corpus` is requested, regardless of the sample size.

    Literal repro for the `_effective_sample_size` fix: 8 Dags, `airflow_serialization_
    sample_size` configured to 2, smoke disabled. Before the fix, `_select_serialization_
    sample` read the raw configured sample size unconditionally once past the all-or-
    nothing `_corpus_serialization_needed` gate, so only 2 of the 8 Dags ended up
    serialized -- silently contradicting the documented guarantee that requesting
    `dag_corpus` at all serializes every Dag.
    """

    dags = {
        f"dag_{i}": SimpleNamespace(
            tags=set(), tasks=[], timetable=SimpleNamespace(can_be_scheduled=True)
        )
        for i in range(8)
    }
    dag_bag = SimpleNamespace(dags=dags, import_errors={}, dagbag_stats=[])

    monkeypatch.setattr(dagcorpus, "_dag_folder", lambda _config: Path("dags"))
    monkeypatch.setattr(dagcorpus, "build_dag_bag", lambda _folder, **_kwargs: dag_bag)
    monkeypatch.setattr(dagcorpus, "record_secrets_lookups", _fake_recorder([]))
    monkeypatch.setattr(
        dagcorpus,
        "_get_dag_serializer",
        lambda: SimpleNamespace(serialize_dag=lambda _dag: {"dag_id": "x"}),
    )

    config = _config(parse_timeout="1", sample_size="2")
    dagcorpus.mark_dag_corpus_requested(config)
    session: Any = SimpleNamespace(stash=pytest.Stash())

    corpus = dagcorpus._build_dag_corpus(session, config)

    assert len(corpus.dags) == 8
    assert all(dag.serialized is not None for dag in corpus.dags.values())


def test_dag_corpus_build_produces_read_only_mappings(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin the BUILD path's read-only guarantee, not just the payload-decode path.

    `test_dag_corpus_dags_and_import_errors_are_read_only` asserts immutability against
    `_sample_corpus()`, a test helper that already constructs `dags`/`import_errors` as
    `MappingProxyType` -- it would stay green even if `_build_dag_corpus` itself regressed
    back to returning plain, mutable `dict`s. This exercises the real builder instead, the
    same way `test_dag_corpus_from_payload_produces_read_only_mappings` pins the decode path.
    """

    good = SimpleNamespace(tags=set(), tasks=[], timetable=SimpleNamespace(can_be_scheduled=True))
    dag_bag = SimpleNamespace(
        dags={"good": good}, import_errors={"bad.py": "boom"}, dagbag_stats=[]
    )

    monkeypatch.setattr(dagcorpus, "_dag_folder", lambda _config: Path("dags"))
    monkeypatch.setattr(dagcorpus, "build_dag_bag", lambda _folder, **_kwargs: dag_bag)
    monkeypatch.setattr(dagcorpus, "record_secrets_lookups", _fake_recorder([]))
    monkeypatch.setattr(
        dagcorpus, "_get_dag_serializer", lambda: SimpleNamespace(serialize_dag=lambda _dag: {})
    )

    session: Any = SimpleNamespace(stash=pytest.Stash())
    corpus = dagcorpus._build_dag_corpus(session, _config(parse_timeout="1"))

    assert isinstance(corpus.dags, MappingProxyType)
    assert isinstance(corpus.import_errors, MappingProxyType)
    dags: Any = corpus.dags
    import_errors: Any = corpus.import_errors
    with pytest.raises(TypeError):
        dags["new"] = corpus.dags["good"]
    with pytest.raises(TypeError):
        import_errors["new.py"] = "traceback"


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


def test_dag_corpus_fanout_not_blocked_by_sample_size_when_dag_corpus_requested(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Do not let a nonzero configured sample size block fan-out once `dag_corpus` is requested.

    Repro for the `_effective_sample_size` fix on the fan-out-gating side: before it, the
    gate in `_build_dag_corpus` read the raw `airflow_serialization_sample_size`
    unconditionally, so a `dag_corpus` consumer plus a nonzero configured sample size
    blocked fan-out entirely -- even though the reasoning behind the gate (a single shard
    cannot know whether one of its Dags falls inside a corpus-wide sample) does not apply
    once `dag_corpus`'s "serialize everything" guarantee means nothing is actually being
    sampled.
    """

    called = False

    def fake_fan_out(**_kwargs: object) -> dict[str, Any]:
        nonlocal called
        called = True
        return {"import_errors": {}, "dagbag_stats": [], "runtime_lookups": [], "dags": {}}

    def _fail_if_called(*_args: object, **_kwargs: object) -> Any:
        raise AssertionError("must not fall back to a serial parse")

    monkeypatch.setattr(dagcorpus, "_dag_folder", lambda _config: Path("dags"))
    monkeypatch.setattr(
        dagcorpus, "get_bootstrap_state", lambda _config: SimpleNamespace(root=tmp_path)
    )
    monkeypatch.setattr(dagcorpus, "list_dag_file_paths", lambda _folder: ["dags/good.py"])
    monkeypatch.setattr(dagcorpus, "fan_out_dag_bag", fake_fan_out)
    monkeypatch.setattr(dagcorpus, "build_dag_bag", _fail_if_called)

    config = _config(parse_timeout="1", fanout=True, fanout_min_files="0", sample_size="2")
    dagcorpus.mark_dag_corpus_requested(config)
    session: Any = SimpleNamespace(stash=pytest.Stash())

    corpus = dagcorpus._build_dag_corpus(session, config)

    assert called is True
    assert corpus.dags == {}


def test_dag_corpus_fanout_still_blocked_by_sample_size_absent_a_dag_corpus_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Preserve the original smoke-only sampling/fan-out-blocking behavior.

    Pins that the `_effective_sample_size` fix only changes behavior once a `dag_corpus`
    consumer exists: absent a request marker, `_effective_sample_size` passes the
    configured `airflow_serialization_sample_size` straight through, so a nonzero value
    still blocks fan-out entirely and the smoke catalog's own sampling optimization keeps
    applying, exactly as before fix #2.
    """

    def _fail_if_called(**_kwargs: object) -> Any:
        raise AssertionError("fan-out must stay blocked by a nonzero sample size")

    dags = {
        f"dag_{i}": SimpleNamespace(
            tags=set(), tasks=[], timetable=SimpleNamespace(can_be_scheduled=True)
        )
        for i in range(4)
    }
    dag_bag = SimpleNamespace(dags=dags, import_errors={}, dagbag_stats=[])

    monkeypatch.setattr(dagcorpus, "_dag_folder", lambda _config: Path("dags"))
    monkeypatch.setattr(
        dagcorpus, "get_bootstrap_state", lambda _config: SimpleNamespace(root=tmp_path)
    )
    monkeypatch.setattr(dagcorpus, "list_dag_file_paths", lambda _folder: ["dags/good.py"])
    monkeypatch.setattr(dagcorpus, "fan_out_dag_bag", _fail_if_called)
    monkeypatch.setattr(dagcorpus, "build_dag_bag", lambda _folder, **_kwargs: dag_bag)
    monkeypatch.setattr(dagcorpus, "record_secrets_lookups", _fake_recorder([]))
    monkeypatch.setattr(
        dagcorpus,
        "_get_dag_serializer",
        lambda: SimpleNamespace(serialize_dag=lambda _dag: {"dag_id": "x"}),
    )
    monkeypatch.setattr(smoke, "_smoke_enabled", lambda _config: True)
    monkeypatch.setattr(smoke, "_smoke_in_scope", lambda _config: True)
    monkeypatch.setattr(smoke, "_smoke_serialization_needed", lambda _config: True)

    config = _config(parse_timeout="1", fanout=True, fanout_min_files="0", sample_size="2")
    session: Any = SimpleNamespace(stash=pytest.Stash())

    corpus = dagcorpus._build_dag_corpus(session, config)

    serialized = [dag_id for dag_id, dag in corpus.dags.items() if dag.serialized is not None]
    assert len(serialized) == 2


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


def test_dags_under_filters_by_subdirectory(tmp_path: Path) -> None:
    """Keep only Dags whose file lives under the requested subdirectory.

    Uses real on-disk paths (unlike most `dags_under` tests below, which exercise the
    nonexistent-path branch of `Path.resolve()` deliberately): a real directory pins the
    common case where `resolve()` walks actual filesystem entries, not just syntax.
    """

    dag_folder = tmp_path / "dags"
    (dag_folder / "a").mkdir(parents=True)
    (dag_folder / "b").mkdir(parents=True)
    (dag_folder / "a" / "one.py").touch()
    (dag_folder / "b" / "two.py").touch()
    corpus = _corpus_with_filelocs(
        {
            "in_a": str(dag_folder / "a" / "one.py"),
            "in_b": str(dag_folder / "b" / "two.py"),
        }
    )

    result = dagcorpus.dags_under(corpus, dag_folder, "a")

    assert dict(result) == {"in_a": corpus.dags["in_a"]}


def test_dags_under_does_not_match_a_sibling_with_a_shared_prefix(tmp_path: Path) -> None:
    """Reject `dags/ab` when filtering for `dags/a`.

    Guards the reason this filter resolves and compares path *parts* via
    `Path.is_relative_to` instead of a `str.startswith` prefix check, which a naive
    reimplementation could regress to.
    """

    dag_folder = tmp_path / "dags"
    corpus = _corpus_with_filelocs(
        {
            "in_a": str(dag_folder / "a" / "one.py"),
            "in_ab": str(dag_folder / "ab" / "two.py"),
        }
    )

    result = dagcorpus.dags_under(corpus, dag_folder, "a")

    assert dict(result) == {"in_a": corpus.dags["in_a"]}


def test_dags_under_resolves_through_a_symlinked_subdirectory(tmp_path: Path) -> None:
    """Follow a symlinked directory component the same way `Path.resolve()` does.

    `resolve()` is what makes the containment check filesystem-aware rather than
    syntactic; this only exercises that when the symlink is real, unlike the
    nonexistent-path cases elsewhere in this file.
    """

    dag_folder = tmp_path / "dags"
    real_dir = tmp_path / "real_a"
    real_dir.mkdir()
    (real_dir / "one.py").touch()
    dag_folder.mkdir()
    (dag_folder / "a").symlink_to(real_dir)
    corpus = _corpus_with_filelocs({"in_a": str(dag_folder / "a" / "one.py")})

    result = dagcorpus.dags_under(corpus, dag_folder, "a")

    assert dict(result) == {"in_a": corpus.dags["in_a"]}


def test_dags_under_includes_nested_subdirectories(tmp_path: Path) -> None:
    """Keep Dags nested arbitrarily deep under the requested subdirectory."""

    dag_folder = tmp_path / "dags"
    corpus = _corpus_with_filelocs(
        {
            "shallow": str(dag_folder / "a" / "one.py"),
            "deep": str(dag_folder / "a" / "nested" / "further" / "two.py"),
            "sibling": str(dag_folder / "b" / "three.py"),
        }
    )

    result = dagcorpus.dags_under(corpus, dag_folder, "a")

    assert set(result) == {"shallow", "deep"}


def test_dags_under_returns_empty_mapping_when_nothing_matches(tmp_path: Path) -> None:
    """Match nothing when an existing, on-disk subdirectory holds no Dags.

    Distinct from `test_dags_under_subdir_absent_from_disk_still_matches_nothing` below:
    `other` is real here, so this pins the "exists but empty" case rather than the
    "does not exist at all" one.
    """

    dag_folder = tmp_path / "dags"
    (dag_folder / "other").mkdir(parents=True)
    corpus = _corpus_with_filelocs({"in_a": str(dag_folder / "a" / "one.py")})

    assert dict(dagcorpus.dags_under(corpus, dag_folder, "other")) == {}


def test_dags_under_subdir_absent_from_disk_still_matches_nothing(tmp_path: Path) -> None:
    """Stay a pure in-memory filter: a `subdir` that was never created still resolves."""

    dag_folder = tmp_path / "dags"
    assert not dag_folder.exists()
    corpus = _corpus_with_filelocs({"in_a": str(dag_folder / "a" / "one.py")})

    result = dagcorpus.dags_under(corpus, dag_folder, "nope")

    assert dict(result) == {}
    assert not dag_folder.exists()


def test_dags_under_relative_and_absolute_subdir_match(tmp_path: Path) -> None:
    """Give identical results for a relative and an equivalent absolute `subdir`."""

    dag_folder = tmp_path / "dags"
    corpus = _corpus_with_filelocs({"in_a": str(dag_folder / "a" / "one.py")})

    relative_result = dagcorpus.dags_under(corpus, dag_folder, "a")
    absolute_result = dagcorpus.dags_under(corpus, dag_folder, dag_folder / "a")

    expected = {"in_a": corpus.dags["in_a"]}
    assert dict(relative_result) == dict(absolute_result) == expected


def test_dags_under_normalizes_trailing_slash_dot_and_dotdot(tmp_path: Path) -> None:
    """Normalize a trailing slash, `.`, and `..` in `subdir` to the same result."""

    dag_folder = tmp_path / "dags"
    corpus = _corpus_with_filelocs({"in_a": str(dag_folder / "a" / "one.py")})
    expected = {"in_a": corpus.dags["in_a"]}

    assert dict(dagcorpus.dags_under(corpus, dag_folder, "a/")) == expected
    assert dict(dagcorpus.dags_under(corpus, dag_folder, "./a")) == expected
    assert dict(dagcorpus.dags_under(corpus, dag_folder, "b/../a")) == expected


@pytest.mark.parametrize("subdir", [".", ""])
def test_dags_under_dot_or_empty_subdir_returns_everything(subdir: str, tmp_path: Path) -> None:
    """Return every Dag when `subdir` names the Dag folder itself."""

    dag_folder = tmp_path / "dags"
    corpus = _corpus_with_filelocs(
        {
            "in_a": str(dag_folder / "a" / "one.py"),
            "in_b": str(dag_folder / "b" / "two.py"),
        }
    )

    assert dict(dagcorpus.dags_under(corpus, dag_folder, subdir)) == corpus.dags


def test_dags_under_ignores_fileloc_outside_dag_folder(tmp_path: Path) -> None:
    """Do not crash on a `fileloc` that lives outside `dag_folder` entirely."""

    dag_folder = tmp_path / "dags"
    outside = tmp_path / "elsewhere" / "rogue.py"
    corpus = _corpus_with_filelocs({"rogue": str(outside)})

    assert dict(dagcorpus.dags_under(corpus, dag_folder, ".")) == {}


def test_dags_under_return_value_is_read_only(tmp_path: Path) -> None:
    """Reject mutation of the returned mapping so a shared corpus stays intact."""

    dag_folder = tmp_path / "dags"
    corpus = _corpus_with_filelocs({"in_a": str(dag_folder / "a" / "one.py")})
    result = dagcorpus.dags_under(corpus, dag_folder, "a")
    mutable: Any = result

    with pytest.raises(TypeError):
        mutable["new"] = corpus.dags["in_a"]


def test_dags_under_empty_corpus_returns_empty_mapping(tmp_path: Path) -> None:
    """Return an empty mapping, without raising, for a corpus with no Dags."""

    corpus = _corpus_with_filelocs({})

    assert dict(dagcorpus.dags_under(corpus, tmp_path / "dags", ".")) == {}
