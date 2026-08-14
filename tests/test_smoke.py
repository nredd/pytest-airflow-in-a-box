"""Test the opt-in bundled smoke catalog: config guards, table rendering, end-to-end behavior."""

from __future__ import annotations

import logging
import os
import re
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from pytest_airflow_in_a_box import smoke
from pytest_airflow_in_a_box.fixtures import dagbag


def _config(
    *,
    airflow_smoke: object = None,
    ini_smoke: object = False,
    parse_timeout: object = "30",
    slowpoke_ratio: object = "0.75",
    dag_id_pattern: object = "",
    required_dag_tags: object = (),
    forbid_default_owner: object = False,
    airflow_smoke_update: object = None,
    snapshot_dir: object = "",
    sample_size: object = "0",
    sample_seed: object = "0",
    pools: object = (),
    rootpath: Path = Path("/repo"),
) -> Any:
    """Create a minimal configuration double for smoke config-reader tests.

    Parameters:
        airflow_smoke: object containing the parsed ``--airflow-smoke`` option value.
        ini_smoke: object containing the ``airflow_smoke`` ini value.
        parse_timeout: object containing the ``airflow_dag_parse_timeout`` ini value.
        slowpoke_ratio: object containing the ``airflow_dag_parse_slowpoke_ratio`` ini value.
        dag_id_pattern: object containing the ``airflow_dag_id_pattern`` ini value.
        required_dag_tags: object containing the ``airflow_required_dag_tags`` ini value.
        forbid_default_owner: object containing the ``airflow_forbid_default_owner`` ini value.
        airflow_smoke_update: object containing the parsed ``--airflow-smoke-update`` value.
        snapshot_dir: object containing the ``airflow_dag_snapshot_dir`` ini value.
        sample_size: object containing the ``airflow_serialization_sample_size`` ini value.
        sample_seed: object containing the ``airflow_serialization_sample_seed`` ini value.
        pools: object containing the ``airflow_pools`` ini value.
        rootpath: pathlib.Path used to resolve a relative snapshot directory.

    Returns:
        types.SimpleNamespace shaped like the configuration surface under test.
    """

    ini_values = {
        "airflow_smoke": ini_smoke,
        "airflow_dag_parse_timeout": parse_timeout,
        "airflow_dag_parse_slowpoke_ratio": slowpoke_ratio,
        "airflow_dag_id_pattern": dag_id_pattern,
        "airflow_required_dag_tags": list(required_dag_tags)
        if isinstance(required_dag_tags, (list, tuple))
        else required_dag_tags,
        "airflow_forbid_default_owner": forbid_default_owner,
        "airflow_dag_snapshot_dir": snapshot_dir,
        "airflow_serialization_sample_size": sample_size,
        "airflow_serialization_sample_seed": sample_seed,
        "airflow_pools": list(pools) if isinstance(pools, (list, tuple)) else pools,
    }
    option_values = {
        "airflow_smoke": airflow_smoke,
        "airflow_smoke_update": airflow_smoke_update,
    }
    return SimpleNamespace(
        getoption=lambda name: option_values[name],
        getini=lambda name: ini_values[name],
        stash=pytest.Stash(),
        rootpath=rootpath,
    )


def test_smoke_enabled_prefers_cli_option() -> None:
    """Give the CLI option precedence over the ini value."""

    config = _config(airflow_smoke=True, ini_smoke=False)

    assert smoke._smoke_enabled(config) is True


def test_smoke_enabled_reads_ini_when_cli_absent() -> None:
    """Fall back to the ini value when the CLI option is unset."""

    config = _config(airflow_smoke=None, ini_smoke=True)

    assert smoke._smoke_enabled(config) is True


def test_smoke_enabled_defaults_to_disabled() -> None:
    """Return disabled when neither the CLI option nor the ini value is set."""

    config = _config()

    assert smoke._smoke_enabled(config) is False


def test_smoke_enabled_caches_resolution() -> None:
    """Resolve the enabled flag once and serve later calls from the stash."""

    reads: list[str] = []
    config = _config(airflow_smoke=True)
    original_getoption = config.getoption
    config.getoption = lambda name: (reads.append(name), original_getoption(name))[1]

    assert smoke._smoke_enabled(config) is True
    assert smoke._smoke_enabled(config) is True
    assert reads == ["airflow_smoke"]


def test_smoke_enabled_rejects_non_boolean_ini() -> None:
    """Reject a non-boolean ini value."""

    config = _config(ini_smoke="yes")

    with pytest.raises(pytest.UsageError, match="`airflow_smoke` must be a boolean"):
        smoke._smoke_enabled(config)


def _scope_config(
    *,
    args_source: pytest.Config.ArgsSource,
    args: list[str],
    invocation_dir: Path,
    markexpr: str = "",
) -> Any:
    """Create a minimal configuration double for `_smoke_in_scope` tests.

    Parameters:
        args_source: pytest.Config.ArgsSource describing where the positional args came from.
        args: list[str] of resolved positional args.
        invocation_dir: pathlib.Path of the invocation directory.
        markexpr: str resolved `-m` mark expression.

    Returns:
        types.SimpleNamespace shaped like the configuration surface under test.
    """

    return SimpleNamespace(
        args_source=args_source,
        args=args,
        invocation_params=SimpleNamespace(dir=invocation_dir),
        option=SimpleNamespace(markexpr=markexpr),
    )


def test_smoke_in_scope_without_explicit_positionals(tmp_path: Path) -> None:
    """Keep the catalog in scope for bare and testpaths-driven runs."""

    for source in (
        pytest.Config.ArgsSource.INVOCATION_DIR,
        pytest.Config.ArgsSource.TESTPATHS,
    ):
        config = _scope_config(args_source=source, args=["test_x.py"], invocation_dir=tmp_path)
        assert smoke._smoke_in_scope(config) is True


def test_smoke_in_scope_drops_catalog_for_node_id_and_file_args(tmp_path: Path) -> None:
    """Drop the catalog when every explicit positional names a file or node ID."""

    (tmp_path / "test_x.py").write_text("", encoding="utf-8")
    config = _scope_config(
        args_source=pytest.Config.ArgsSource.ARGS,
        args=["test_x.py::test_one", "test_x.py"],
        invocation_dir=tmp_path,
    )

    assert smoke._smoke_in_scope(config) is False


def test_smoke_in_scope_keeps_catalog_for_directory_arg(tmp_path: Path) -> None:
    """Keep the catalog when any explicit positional is a directory."""

    (tmp_path / "sub").mkdir()
    config = _scope_config(
        args_source=pytest.Config.ArgsSource.ARGS,
        args=["missing.py::test_one", "sub"],
        invocation_dir=tmp_path,
    )

    assert smoke._smoke_in_scope(config) is True


@pytest.mark.parametrize(
    ("markexpr", "expected"),
    [
        ("", False),
        ("smoke", True),
        ("not smoke", False),
        ("smoke or unrelated_marker", True),
        ("unrelated_marker", False),
        # No literal `smoke` token: must NOT force the catalog into scope even though the
        # expression happens to match real smoke-item marks once vacuously true (absence of
        # an unrelated marker) or by name alone. Forcing scope here would silently balloon
        # any explicitly-scoped run that merely deselects by an unrelated marker.
        ("not slow", False),
        ("timeout", False),
        ("db_test", False),
        # `timeout` and `db_test` are real markers every/some smoke items carry too -- once
        # `smoke` is explicitly mentioned, conjoining with either must still resolve.
        ("smoke and timeout", True),
        ("smoke and db_test", True),
        # Only the 7 non-`db_test` items match; a flat union-of-names matcher would
        # wrongly return False here since `db_test` is present on *some* smoke item.
        ("smoke and not db_test", True),
        # No real smoke item carries `db_test` without `timeout`.
        ("smoke and db_test and not timeout", False),
        # No real smoke item's `smoke` mark takes keyword arguments.
        ("smoke(iteration=1)", False),
        ("smoke(", False),
    ],
    ids=[
        "empty",
        "bare",
        "negated",
        "or-clause",
        "unrelated",
        "negated-unrelated",
        "timeout-alone",
        "db-test-alone",
        "smoke-and-timeout",
        "smoke-and-db-test",
        "smoke-and-not-db-test",
        "impossible-combo",
        "kwargs-blind",
        "malformed",
    ],
)
def test_markexpr_wants_smoke(markexpr: str, expected: bool, tmp_path: Path) -> None:
    """Resolve whether a `-m` expression explicitly opts into the smoke catalog."""

    config = _scope_config(
        args_source=pytest.Config.ArgsSource.ARGS,
        args=[],
        invocation_dir=tmp_path,
        markexpr=markexpr,
    )

    assert smoke._markexpr_wants_smoke(config) is expected


def test_smoke_in_scope_mark_expression_overrides_node_id_and_file_args(
    tmp_path: Path,
) -> None:
    """Keep the catalog when `-m` explicitly selects `smoke`, even with file/node-ID args.

    Regression test for https://github.com/nredd/pytest-airflow-in-a-box/issues/133.
    """

    (tmp_path / "test_x.py").write_text("", encoding="utf-8")
    config = _scope_config(
        args_source=pytest.Config.ArgsSource.ARGS,
        args=["test_x.py::test_one", "test_x.py"],
        invocation_dir=tmp_path,
        markexpr="smoke",
    )

    assert smoke._smoke_in_scope(config) is True


def test_parse_timeout_reads_default() -> None:
    """Parse the default timeout string into a float."""

    assert smoke._parse_timeout(_config()) == 30.0


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
        smoke._parse_timeout(_config(parse_timeout=value))


def test_slowpoke_ratio_reads_default() -> None:
    """Parse the default ratio string into a float."""

    assert smoke._slowpoke_ratio(_config()) == 0.75


@pytest.mark.parametrize(
    ("value", "match"),
    [
        (7, "must be a number"),
        ("oops", "must be a number: 'oops'"),
        ("0", r"must be in \(0, 1\]"),
        ("1.5", r"must be in \(0, 1\]"),
        ("-0.1", r"must be in \(0, 1\]"),
    ],
)
def test_slowpoke_ratio_rejects_malformed_values(value: object, match: str) -> None:
    """Reject non-string, non-numeric, and out-of-range ratio values."""

    with pytest.raises(pytest.UsageError, match=match):
        smoke._slowpoke_ratio(_config(slowpoke_ratio=value))


def test_dag_id_pattern_returns_none_when_unset() -> None:
    """Return no pattern when the ini value is empty."""

    assert smoke._dag_id_pattern(_config()) is None


def test_dag_id_pattern_compiles_valid_regex() -> None:
    """Compile a valid regex into a usable pattern."""

    pattern = smoke._dag_id_pattern(_config(dag_id_pattern="^team_"))

    assert pattern is not None
    assert pattern.search("team_a")
    assert not pattern.search("other")


@pytest.mark.parametrize(
    ("value", "match"),
    [
        (7, "must be a string"),
        ("[", "must be a valid regex"),
    ],
)
def test_dag_id_pattern_rejects_malformed_values(value: object, match: str) -> None:
    """Reject a non-string value and an invalid regex."""

    with pytest.raises(pytest.UsageError, match=match):
        smoke._dag_id_pattern(_config(dag_id_pattern=value))


def test_required_dag_tags_returns_empty_when_unset() -> None:
    """Return an empty set when no tags are configured."""

    assert smoke._required_dag_tags(_config()) == frozenset()


def test_required_dag_tags_returns_configured_tags() -> None:
    """Return every configured tag as a frozenset."""

    assert smoke._required_dag_tags(_config(required_dag_tags=["a", "b"])) == frozenset({"a", "b"})


@pytest.mark.parametrize(
    "value",
    ["oops", [7]],
)
def test_required_dag_tags_rejects_malformed_values(value: object) -> None:
    """Reject a non-list value and a list containing non-string entries."""

    with pytest.raises(pytest.UsageError, match="must be a list of tags"):
        smoke._required_dag_tags(_config(required_dag_tags=value))


def test_pool_seeds_returns_empty_when_unset() -> None:
    """Return no pools when the ini value is unset."""

    assert smoke._pool_seeds(_config()) == ()


def test_pool_seeds_parses_configured_lines() -> None:
    """Parse every `name = slots` line into a `PoolSeed`."""

    pools = smoke._pool_seeds(_config(pools=["batch = 4", "critical = 1"]))

    assert pools == (
        smoke.PoolSeed(name="batch", slots=4),
        smoke.PoolSeed(name="critical", slots=1),
    )


@pytest.mark.parametrize(
    ("value", "match"),
    [
        (7, "must be a list of lines"),
        ([7], "must be a list of lines"),
        (["batch"], "must be `name = slots`"),
        (["= 4"], "must be `name = slots`"),
        (["batch ="], "must be `name = slots`"),
        (["batch = many"], "slots must be an integer"),
        (["batch = 0"], "slots must be positive"),
        (["batch = -1"], "slots must be positive"),
        (["batch = 4", "batch = 1"], "Duplicate `airflow_pools` pool name: `batch`"),
    ],
)
def test_pool_seeds_rejects_malformed_values(value: object, match: str) -> None:
    """Reject a non-list value and every malformed line shape."""

    with pytest.raises(pytest.UsageError, match=match):
        smoke._pool_seeds(_config(pools=value))


def test_forbid_default_owner_defaults_to_false() -> None:
    """Return disabled when the ini value is unset."""

    assert smoke._forbid_default_owner(_config()) is False


def test_forbid_default_owner_reads_configured_value() -> None:
    """Return the configured boolean value."""

    assert smoke._forbid_default_owner(_config(forbid_default_owner=True)) is True


def test_forbid_default_owner_rejects_non_boolean() -> None:
    """Reject a non-boolean ini value."""

    with pytest.raises(pytest.UsageError, match="must be a boolean"):
        smoke._forbid_default_owner(_config(forbid_default_owner="yes"))


def test_snapshot_dir_returns_none_when_unset() -> None:
    """Return no snapshot directory when the ini value is empty."""

    assert smoke._snapshot_dir(_config()) is None


def test_snapshot_dir_resolves_relative_paths_against_rootpath() -> None:
    """Resolve a relative snapshot directory against the configured rootpath."""

    config = _config(snapshot_dir="snapshots", rootpath=Path("/repo"))

    assert smoke._snapshot_dir(config) == Path("/repo/snapshots")


def test_snapshot_dir_passes_through_absolute_paths() -> None:
    """Leave an absolute snapshot directory unchanged."""

    config = _config(snapshot_dir="/abs/snapshots", rootpath=Path("/repo"))

    assert smoke._snapshot_dir(config) == Path("/abs/snapshots")


def test_snapshot_dir_rejects_non_string() -> None:
    """Reject a non-string ini value."""

    with pytest.raises(pytest.UsageError, match="must be a path string"):
        smoke._snapshot_dir(_config(snapshot_dir=7))


def test_smoke_update_defaults_to_false() -> None:
    """Return disabled when the CLI option is unset."""

    assert smoke._smoke_update(_config()) is False


def test_smoke_update_reads_configured_value() -> None:
    """Return the configured CLI option value."""

    assert smoke._smoke_update(_config(airflow_smoke_update=True)) is True


def test_serialization_sample_size_reads_default() -> None:
    """Parse the default sample size string into the exhaustive sentinel."""

    assert smoke._serialization_sample_size(_config()) == 0


def test_serialization_sample_size_reads_configured_value() -> None:
    """Parse a configured sample size string into an int."""

    assert smoke._serialization_sample_size(_config(sample_size="25")) == 25


@pytest.mark.parametrize(
    "value",
    [7, "oops", "1.5", "-1"],
)
def test_serialization_sample_size_rejects_malformed_values(value: object) -> None:
    """Reject non-string, non-integer, and negative sample sizes."""

    with pytest.raises(pytest.UsageError, match="must be a non-negative integer"):
        smoke._serialization_sample_size(_config(sample_size=value))


def test_serialization_sample_seed_reads_configured_value() -> None:
    """Return the configured seed string, defaulting to '0'."""

    assert smoke._serialization_sample_seed(_config()) == "0"
    assert smoke._serialization_sample_seed(_config(sample_seed="release-42")) == "release-42"


def test_serialization_sample_seed_rejects_non_string() -> None:
    """Reject a non-string seed value."""

    with pytest.raises(pytest.UsageError, match="`airflow_serialization_sample_seed` must be"):
        smoke._serialization_sample_seed(_config(sample_seed=7))


def test_validate_smoke_options_rejects_update_with_sampling() -> None:
    """Reject snapshot update mode combined with serialization sampling."""

    config = _config(airflow_smoke_update=True, snapshot_dir="snapshots", sample_size="1")

    with pytest.raises(pytest.UsageError, match="cannot be combined"):
        smoke._validate_smoke_options(config)


@pytest.mark.parametrize(
    ("update", "snapshot_dir", "sample_size"),
    [
        (None, "snapshots", "1"),
        (True, "", "1"),
        (True, "snapshots", "0"),
    ],
)
def test_validate_smoke_options_passes_partial_combinations(
    update: object, snapshot_dir: object, sample_size: object
) -> None:
    """Raise nothing unless update mode, a snapshot dir, and sampling are all active."""

    config = _config(
        airflow_smoke_update=update, snapshot_dir=snapshot_dir, sample_size=sample_size
    )

    smoke._validate_smoke_options(config)


def test_normalize_serialized_dag_strips_run_dependent_keys() -> None:
    """Drop every run-dependent key while leaving other keys untouched."""

    encoded = {
        "dag_id": "sample",
        "_processor_dags_folder": "/home/user/dags",
        "fileloc": "/home/user/dags/sample.py",
        "relative_fileloc": "sample.py",
        "tags": ["team-a"],
    }

    assert smoke._normalize_serialized_dag(encoded) == {"dag_id": "sample", "tags": ["team-a"]}


def _stat(file: str, seconds: float, *, dags: int = 1, tasks: int = 1) -> Any:
    """Create a minimal ``FileLoadStat``-shaped double for table rendering tests.

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


def test_log_stats_table_marks_ok_and_slowpoke_rows(caplog: pytest.LogCaptureFixture) -> None:
    """Render one row per file with the correct ok/slowpoke status."""

    dag_bag: Any = SimpleNamespace(
        dagbag_stats=[_stat("fast.py", 0.1), _stat("slow.py", 8.0), _stat("dead.py", 12.0)]
    )
    # A failed `logging.config.dictConfig` call elsewhere in the suite can leave this logger
    # disabled process-wide (stdlib `disable_existing_loggers` never gets reverted on failure);
    # `caplog.at_level` only guards against `logging.disable()`, not this per-logger attribute.
    logging.getLogger("pytest_airflow_in_a_box.smoke").disabled = False

    with caplog.at_level("INFO", logger="pytest_airflow_in_a_box.smoke"):
        text = smoke._log_stats_table(dag_bag, timeout=10.0, ratio=0.75)

    assert "fast.py" in text
    assert "ok" in text
    assert "SLOWPOKE (>75% of 10.0s)" in text
    assert "SLOWPOKE (>10.0s timeout)" in text
    assert "Dag bag parse report" in caplog.text


def test_smoke_check_failure_requires_a_message() -> None:
    """Reject construction without a failure body."""

    with pytest.raises(ValueError, match="non-empty failure body"):
        smoke.SmokeCheckFailure("")


def _bare_item(item_class: type, **attributes: Any) -> Any:
    """Create an item without pytest's node constructor, which needs a live session.

    Parameters:
        item_class: type naming the ``pytest.Item`` subclass to instantiate.
        attributes: Any assigned onto the bare instance.

    Returns:
        Any containing the constructed item with only the attributes under test.
    """

    item = object.__new__(item_class)
    for name, value in attributes.items():
        setattr(item, name, value)
    return item


def _dag(dag_id: str, *, tasks: tuple[Any, ...] = (), tags: frozenset[str] = frozenset()) -> Any:
    """Create a minimal Dag double exposing only what the smoke items read.

    Parameters:
        dag_id: str identifying the Dag.
        tasks: tuple[Any, ...] containing task doubles.
        tags: frozenset[str] containing the Dag's tags.

    Returns:
        Any shaped like the Dag surface under test.
    """

    del dag_id
    return SimpleNamespace(tasks=list(tasks), tags=set(tags))


def _task(task_id: str, *, owner: str = "someone", pool: str = "default_pool") -> Any:
    """Create a minimal task double exposing only what the smoke items read.

    Parameters:
        task_id: str identifying the task.
        owner: str naming the task's declared owner.
        pool: str naming the task's declared pool.

    Returns:
        Any shaped like the task surface under test.
    """

    return SimpleNamespace(task_id=task_id, owner=owner, pool=pool)


def _session() -> Any:
    """Create a minimal session double carrying only a stash.

    Returns:
        types.SimpleNamespace shaped like the session surface the smoke caches read.
    """

    return SimpleNamespace(stash=pytest.Stash())


def test_serialized_dag_entry_rejects_inconsistent_outcomes() -> None:
    """Reject entries recording both, or neither, of a payload and an error."""

    with pytest.raises(ValueError, match="Exactly one of `encoded` and `error`"):
        smoke.SerializedDagEntry(encoded={"dag_id": "x"}, error="boom", seconds=0.1)
    with pytest.raises(ValueError, match="Exactly one of `encoded` and `error`"):
        smoke.SerializedDagEntry(encoded=None, error=None, seconds=0.1)


def test_serialized_dag_entry_rejects_negative_seconds() -> None:
    """Reject a negative duration."""

    with pytest.raises(ValueError, match="`seconds` must be non-negative"):
        smoke.SerializedDagEntry(encoded={"dag_id": "x"}, error=None, seconds=-0.1)


def test_serialized_dag_entry_payload_returns_encoded_dag() -> None:
    """Return the serialized payload of a successful entry."""

    entry = smoke.SerializedDagEntry(encoded={"dag_id": "x"}, error=None, seconds=0.1)

    assert entry.payload() == {"dag_id": "x"}


def test_serialized_dag_entry_payload_rejects_failed_entries() -> None:
    """Raise when the payload of a failed entry is requested."""

    entry = smoke.SerializedDagEntry(encoded=None, error="boom", seconds=0.1)

    with pytest.raises(ValueError, match="records a serialization failure: 'boom'"):
        entry.payload()


def test_sampled_dag_ids_returns_all_sorted_when_exhaustive() -> None:
    """Return every id in lexical order when sampling is off or covers the corpus."""

    ids = ["bravo", "alpha", "charlie"]

    assert smoke._sampled_dag_ids(ids, sample_size=0, seed="0") == ["alpha", "bravo", "charlie"]
    assert smoke._sampled_dag_ids(ids, sample_size=9, seed="0") == ["alpha", "bravo", "charlie"]


def test_sampled_dag_ids_selects_a_deterministic_subset() -> None:
    """Select the same lexically sorted subset on every call for a fixed seed."""

    ids = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot"]

    first = smoke._sampled_dag_ids(ids, sample_size=3, seed="0")
    second = smoke._sampled_dag_ids(reversed(ids), sample_size=3, seed="0")

    assert first == ["alpha", "bravo", "echo"]
    assert second == first


def test_sampled_dag_ids_varies_with_seed() -> None:
    """Select a different subset when the seed changes."""

    ids = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot"]

    assert smoke._sampled_dag_ids(ids, sample_size=3, seed="other") == [
        "charlie",
        "delta",
        "echo",
    ]


def test_sampled_dag_ids_rejects_negative_sample_size() -> None:
    """Reject a negative sample size."""

    with pytest.raises(ValueError, match="`sample_size` must be non-negative"):
        smoke._sampled_dag_ids(["alpha"], sample_size=-1, seed="0")


def test_serialized_dag_cache_builds_once_per_session(monkeypatch: pytest.MonkeyPatch) -> None:
    """Serialize the corpus once and serve later calls from the session stash."""

    serialized: list[str] = []
    dag_bag: Any = SimpleNamespace(dags={"one": _dag("one"), "two": _dag("two")})
    monkeypatch.setattr(smoke, "_smoke_dag_bag", lambda _session, _config: dag_bag)
    monkeypatch.setattr(
        smoke,
        "_get_dag_serializer",
        lambda: SimpleNamespace(
            serialize_dag=lambda dag: (serialized.append("call"), {"dag": str(dag)})[1]
        ),
    )
    session = _session()
    config = _config()

    first = smoke._serialized_dag_cache(session, config)
    second = smoke._serialized_dag_cache(session, config)

    assert second is first
    assert sorted(first) == ["one", "two"]
    assert serialized == ["call", "call"]


def test_serialized_dag_cache_records_per_dag_failures(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Record a per-Dag failure entry and warn without raising."""

    def serialize_dag(dag: Any) -> dict[str, Any]:
        if dag == "explode":
            raise ValueError("cannot serialize a lambda")
        return {"dag_id": "fine"}

    dag_bag: Any = SimpleNamespace(dags={"broken": "explode", "fine": "fine"})
    monkeypatch.setattr(smoke, "_smoke_dag_bag", lambda _session, _config: dag_bag)
    monkeypatch.setattr(
        smoke, "_get_dag_serializer", lambda: SimpleNamespace(serialize_dag=serialize_dag)
    )
    logging.getLogger("pytest_airflow_in_a_box.smoke").disabled = False

    with caplog.at_level("WARNING", logger="pytest_airflow_in_a_box.smoke"):
        entries = smoke._serialized_dag_cache(_session(), _config())

    assert entries["broken"].error == "cannot serialize a lambda"
    assert entries["broken"].encoded is None
    assert entries["fine"].payload() == {"dag_id": "fine"}
    assert "Dag `broken` failed to serialize" in caplog.text


def test_serialized_dag_cache_logs_per_dag_progress(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Log a per-Dag progress line as each serialization completes."""

    dag_bag: Any = SimpleNamespace(dags={"one": _dag("one"), "two": _dag("two")})
    monkeypatch.setattr(smoke, "_smoke_dag_bag", lambda _session, _config: dag_bag)
    monkeypatch.setattr(
        smoke,
        "_get_dag_serializer",
        lambda: SimpleNamespace(serialize_dag=lambda _dag: {"dag_id": "x"}),
    )
    logging.getLogger("pytest_airflow_in_a_box.smoke").disabled = False

    with caplog.at_level("INFO", logger="pytest_airflow_in_a_box.smoke"):
        smoke._serialized_dag_cache(_session(), _config())

    assert "Serialized Dag `one`" in caplog.text
    assert "(1/2)" in caplog.text
    assert "(2/2)" in caplog.text


def test_serialized_dag_cache_applies_sampling(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Serialize only the deterministic sample and announce the narrowed coverage."""

    dag_bag: Any = SimpleNamespace(
        dags={dag_id: _dag(dag_id) for dag_id in ("alpha", "bravo", "charlie")}
    )
    monkeypatch.setattr(smoke, "_smoke_dag_bag", lambda _session, _config: dag_bag)
    monkeypatch.setattr(
        smoke,
        "_get_dag_serializer",
        lambda: SimpleNamespace(serialize_dag=lambda _dag: {"dag_id": "x"}),
    )
    logging.getLogger("pytest_airflow_in_a_box.smoke").disabled = False

    with caplog.at_level("INFO", logger="pytest_airflow_in_a_box.smoke"):
        entries = smoke._serialized_dag_cache(_session(), _config(sample_size="1"))

    assert len(entries) == 1
    assert set(entries) == set(
        smoke._sampled_dag_ids(("alpha", "bravo", "charlie"), sample_size=1, seed="0")
    )
    assert "deterministic sample of 1 of 3 Dags (seed '0')" in caplog.text


def test_corpus_timeout_scales_with_file_count(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Multiply the parse timeout by the recursive Dag file count."""

    (tmp_path / "a.py").write_text("", encoding="utf-8")
    nested = tmp_path / "nested"
    nested.mkdir()
    (nested / "b.py").write_text("", encoding="utf-8")
    monkeypatch.setattr(smoke, "_dag_folder", lambda _config: tmp_path)

    assert smoke._corpus_timeout(_config(parse_timeout="30")) == 60.0


def test_corpus_timeout_defaults_to_one_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Fall back to a single-file budget when the Dag folder is missing."""

    monkeypatch.setattr(smoke, "_dag_folder", lambda _config: tmp_path / "missing")

    assert smoke._corpus_timeout(_config(parse_timeout="30")) == 30.0


def test_serialization_timeout_floors_a_tiny_parse_timeout(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Keep the roundtrip deadline at the floor when the parse timeout is tuned down."""

    monkeypatch.setattr(smoke, "_dag_folder", lambda _config: tmp_path / "missing")

    assert (
        smoke._serialization_timeout(_config(parse_timeout="0.05"))
        == smoke.SERIALIZATION_TIMEOUT_FLOOR_SECONDS
    )


def test_serialization_timeout_follows_a_larger_corpus_deadline(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Scale past the floor with the corpus-wide parse deadline."""

    (tmp_path / "a.py").write_text("", encoding="utf-8")
    (tmp_path / "b.py").write_text("", encoding="utf-8")
    monkeypatch.setattr(smoke, "_dag_folder", lambda _config: tmp_path)

    assert smoke._serialization_timeout(_config(parse_timeout="30")) == 60.0


def test_smoke_item_timeout_covers_parse_and_serialization(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Budget both producer phases because any distributed item may elect itself."""

    (tmp_path / "a.py").write_text("", encoding="utf-8")
    (tmp_path / "b.py").write_text("", encoding="utf-8")
    monkeypatch.setattr(smoke, "_dag_folder", lambda _config: tmp_path)

    assert smoke._smoke_item_timeout(_config(parse_timeout="30")) == 120.0


def test_log_serialization_table_marks_ok_and_failed_rows(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Render one row per Dag with timing columns and the correct ok/FAILED status."""

    entries = {
        "fast": smoke.SerializedDagEntry(encoded={"dag_id": "fast"}, error=None, seconds=0.1),
        "broken": smoke.SerializedDagEntry(encoded=None, error="boom", seconds=0.2),
    }
    logging.getLogger("pytest_airflow_in_a_box.smoke").disabled = False

    with caplog.at_level("INFO", logger="pytest_airflow_in_a_box.smoke"):
        text = smoke._log_serialization_table(entries, {"fast": 0.05}, frozenset({"broken"}))

    assert "fast" in text
    assert "0.150s" in text
    assert "ok" in text
    assert "FAILED" in text
    assert "-" in text
    assert "Dag serialization report" in caplog.text


def test_integrity_repr_failure_delegates_foreign_exceptions() -> None:
    """Hand non-smoke exceptions back to pytest's standard representation."""

    item = _bare_item(smoke.DagBagIntegrityItem)
    delegated: list[str] = []
    excinfo: Any = SimpleNamespace(value=RuntimeError("something else"))

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(
            pytest.Item,
            "repr_failure",
            lambda _self, _excinfo: delegated.append("called") or "delegated",
        )

        assert item.repr_failure(excinfo) == "delegated"

    assert delegated == ["called"]


def test_integrity_repr_failure_returns_smoke_message() -> None:
    """Render a smoke failure body without a pytest-internal traceback."""

    item = _bare_item(smoke.DagBagIntegrityItem)
    excinfo: Any = SimpleNamespace(value=smoke.SmokeCheckFailure("the body"))

    assert item.repr_failure(excinfo, style="long") == "the body"


def test_serialization_roundtrip_collects_per_dag_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report every Dag that cannot survive the serialization round trip."""

    dag_bag: Any = SimpleNamespace(dags={"broken": _dag("broken"), "other": _dag("other")})

    def explode(_dag: object) -> dict[str, Any]:
        raise ValueError("cannot serialize a lambda")

    monkeypatch.setattr(smoke, "_smoke_corpus", lambda _session, _config: dag_bag)
    monkeypatch.setattr(
        smoke,
        "_get_dag_serializer",
        lambda: SimpleNamespace(serialize_dag=explode, deserialize_dag=lambda _encoded: None),
    )
    item = _bare_item(smoke.DagSerializationRoundtripItem, session=_session(), config=_config())

    with pytest.raises(smoke.SmokeCheckFailure, match="cannot serialize a lambda") as caught:
        item.runtest()

    assert "Dag `broken`" in str(caught.value)
    assert "Dag `other`" in str(caught.value)


def test_serialization_roundtrip_passes_when_every_dag_survives(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raise nothing when every Dag round-trips."""

    dag_bag: Any = SimpleNamespace(dags={"fine": _dag("fine")})
    monkeypatch.setattr(smoke, "_smoke_corpus", lambda _session, _config: dag_bag)
    monkeypatch.setattr(
        smoke,
        "_get_dag_serializer",
        lambda: SimpleNamespace(
            serialize_dag=lambda _dag: {"dag_id": "fine"},
            deserialize_dag=lambda _encoded: object(),
        ),
    )
    item = _bare_item(smoke.DagSerializationRoundtripItem, session=_session(), config=_config())

    item.runtest()


def test_serialization_roundtrip_reports_deserialize_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report a Dag whose serialized payload cannot be deserialized."""

    dag_bag: Any = SimpleNamespace(dags={"broken": _dag("broken")})

    def explode(_encoded: object) -> object:
        raise ValueError("cannot rebuild the Dag")

    monkeypatch.setattr(smoke, "_smoke_dag_bag", lambda _session, _config: dag_bag)
    monkeypatch.setattr(
        smoke,
        "_get_dag_serializer",
        lambda: SimpleNamespace(
            serialize_dag=lambda _dag: {"dag_id": "broken"}, deserialize_dag=explode
        ),
    )
    item = _bare_item(smoke.DagSerializationRoundtripItem, session=_session(), config=_config())

    with pytest.raises(smoke.SmokeCheckFailure, match="cannot rebuild the Dag") as caught:
        item.runtest()

    assert "Dag `broken` failed to round-trip" in str(caught.value)


def test_serialization_roundtrip_appends_timing_table_on_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Append the slowest-first timing table below the failure bullets."""

    def serialize_dag(dag: Any) -> dict[str, Any]:
        if dag == "explode":
            raise ValueError("cannot serialize a lambda")
        return {"dag_id": "fine"}

    dag_bag: Any = SimpleNamespace(dags={"broken": "explode", "fine": "fine"})
    monkeypatch.setattr(smoke, "_smoke_dag_bag", lambda _session, _config: dag_bag)
    monkeypatch.setattr(
        smoke,
        "_get_dag_serializer",
        lambda: SimpleNamespace(
            serialize_dag=serialize_dag, deserialize_dag=lambda _encoded: object()
        ),
    )
    item = _bare_item(smoke.DagSerializationRoundtripItem, session=_session(), config=_config())

    with pytest.raises(smoke.SmokeCheckFailure) as caught:
        item.runtest()

    body = str(caught.value)
    assert "Dag `broken` failed to round-trip" in body
    assert "FAILED" in body
    assert "fine" in body


def test_serialization_roundtrip_repr_failure_returns_smoke_message() -> None:
    """Render a smoke failure body without a pytest-internal traceback."""

    item = _bare_item(smoke.DagSerializationRoundtripItem)
    excinfo: Any = SimpleNamespace(value=smoke.SmokeCheckFailure("the body"))

    assert item.repr_failure(excinfo, style="long") == "the body"


def test_serialization_roundtrip_repr_failure_delegates_foreign_exceptions() -> None:
    """Hand non-smoke exceptions back to pytest's standard representation."""

    item = _bare_item(smoke.DagSerializationRoundtripItem)
    delegated: list[str] = []
    excinfo: Any = SimpleNamespace(value=RuntimeError("something else"))

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(
            pytest.Item,
            "repr_failure",
            lambda _self, _excinfo: delegated.append("called") or "delegated",
        )

        assert item.repr_failure(excinfo) == "delegated"

    assert delegated == ["called"]


def _scheduled_dag(*, can_be_scheduled: bool, raises: Exception | None = None) -> Any:
    """Create a Dag double whose serialized timetable optionally raises.

    Parameters:
        can_be_scheduled: bool indicating whether the Dag declares a real schedule.
        raises: Exception | None raised when computing the next run.

    Returns:
        Any shaped like the Dag surface `ScheduleSanityItem` reads.
    """

    def next_dagrun_info(**_kwargs: object) -> object:
        if raises is not None:
            raise raises
        return object()

    return SimpleNamespace(
        timetable=SimpleNamespace(
            can_be_scheduled=can_be_scheduled,
            next_dagrun_info=next_dagrun_info,
        ),
        start_date=None,
        end_date=None,
        catchup=False,
    )


def test_schedule_sanity_skips_unscheduled_dags(monkeypatch: pytest.MonkeyPatch) -> None:
    """Leave Dags without a real schedule alone."""

    dag_bag: Any = SimpleNamespace(
        dags={"manual": _scheduled_dag(can_be_scheduled=False, raises=ValueError("never called"))}
    )
    monkeypatch.setattr(smoke, "_smoke_corpus", lambda _session, _config: dag_bag)
    monkeypatch.setattr(
        smoke,
        "_get_dag_serializer",
        lambda: SimpleNamespace(
            serialize_dag=lambda dag: dag,
            deserialize_dag=lambda _dag: pytest.fail("must not deserialize"),
        ),
    )
    item = _bare_item(smoke.ScheduleSanityItem, session=_session(), config=_config())

    item.runtest()


def test_schedule_sanity_reports_broken_timetables(monkeypatch: pytest.MonkeyPatch) -> None:
    """Report a scheduled Dag whose timetable cannot compute its next run."""

    broken = _scheduled_dag(can_be_scheduled=True, raises=ValueError("bad cron"))
    healthy = _scheduled_dag(can_be_scheduled=True)
    dag_bag: Any = SimpleNamespace(dags={"broken": broken, "healthy": healthy})
    monkeypatch.setattr(smoke, "_smoke_corpus", lambda _session, _config: dag_bag)
    monkeypatch.setattr(
        smoke,
        "_get_dag_serializer",
        lambda: SimpleNamespace(
            serialize_dag=lambda dag: dag,
            deserialize_dag=lambda dag: dag,
        ),
    )
    item = _bare_item(smoke.ScheduleSanityItem, session=_session(), config=_config())

    with pytest.raises(smoke.SmokeCheckFailure, match="bad cron") as caught:
        item.runtest()

    assert "Dag `broken`" in str(caught.value)
    assert "Dag `healthy`" not in str(caught.value)


def test_schedule_sanity_skips_serialization_failures_and_unsampled_dags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Skip Dags whose serialization failed or that fell outside the sample."""

    dag_bag: Any = SimpleNamespace(
        dags={
            "broken": _scheduled_dag(can_be_scheduled=True),
            "unsampled": _scheduled_dag(can_be_scheduled=True),
        }
    )
    monkeypatch.setattr(smoke, "_smoke_dag_bag", lambda _session, _config: dag_bag)
    monkeypatch.setattr(
        smoke,
        "_get_dag_serializer",
        lambda: SimpleNamespace(deserialize_dag=lambda _dag: pytest.fail("must not deserialize")),
    )
    session = _session()
    session.stash[smoke.SERIALIZED_DAG_CACHE_KEY] = {
        "broken": smoke.SerializedDagEntry(encoded=None, error="boom", seconds=0.1),
    }
    item = _bare_item(smoke.ScheduleSanityItem, session=session, config=_config())

    item.runtest()


def test_pool_references_report_unknown_pools(monkeypatch: pytest.MonkeyPatch) -> None:
    """Report every task whose declared pool is absent from the database."""

    from airflow.models.pool import Pool

    dag_bag: Any = SimpleNamespace(
        dags={
            "etl": _dag(
                "etl",
                tasks=(_task("known", pool="default_pool"), _task("missing", pool="nope")),
            )
        }
    )
    monkeypatch.setattr(smoke, "_smoke_corpus", lambda _session, _config: dag_bag)
    monkeypatch.setattr(
        Pool,
        "get_pools",
        lambda **_kwargs: [SimpleNamespace(pool="default_pool", slots=128)],
    )
    item = _bare_item(smoke.PoolReferencesExistItem, session=None, config=None, pools=())

    with pytest.raises(smoke.SmokeCheckFailure, match="references unknown pool `nope`") as caught:
        item.runtest()

    assert "task `missing`" in str(caught.value)
    assert "task `known`" not in str(caught.value)


def _fake_pool_session(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    """Replace `create_session` with a fake recording every row added for seeding.

    Parameters:
        monkeypatch: pytest.MonkeyPatch scoped to the calling test.

    Returns:
        list[Any] that accumulates every row passed to `add_all`.
    """

    import contextlib

    from airflow.utils import session as session_module

    added: list[Any] = []

    class _FakeSession:
        """Record rows added by the item without touching a real database."""

        def add_all(self, rows: Any) -> None:
            """Record every row passed for seeding.

            Parameters:
                rows: Any containing the iterable of unsaved model instances.
            """

            added.extend(rows)

    @contextlib.contextmanager
    def _fake_create_session() -> Any:
        """Yield a fake session instead of opening a real database connection."""

        yield _FakeSession()

    monkeypatch.setattr(session_module, "create_session", _fake_create_session)
    return added


def test_pool_references_seeds_configured_pools_before_checking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Seed every configured pool, then pass tasks that reference it."""

    from airflow.models.pool import Pool

    dag_bag: Any = SimpleNamespace(dags={"etl": _dag("etl", tasks=(_task("t", pool="batch"),))})
    monkeypatch.setattr(smoke, "_smoke_corpus", lambda _session, _config: dag_bag)
    monkeypatch.setattr(
        Pool, "get_pools", lambda **_kwargs: [SimpleNamespace(pool="default_pool", slots=128)]
    )
    added = _fake_pool_session(monkeypatch)
    item = _bare_item(
        smoke.PoolReferencesExistItem,
        session=None,
        config=None,
        pools=(smoke.PoolSeed(name="batch", slots=4),),
    )

    item.runtest()

    assert len(added) == 1
    assert added[0].pool == "batch"
    assert added[0].slots == 4


def test_pool_references_seed_matching_existing_slots_is_a_no_op(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Treat a configured pool already seeded with the same slots as already done.

    Covers re-execution of this item against a database another worker already
    seeded, e.g. under `pytest-xdist --dist each` or a test rerun after failure.
    """

    from airflow.models.pool import Pool

    dag_bag: Any = SimpleNamespace(dags={"etl": _dag("etl", tasks=(_task("t", pool="batch"),))})
    monkeypatch.setattr(smoke, "_smoke_corpus", lambda _session, _config: dag_bag)
    monkeypatch.setattr(
        Pool, "get_pools", lambda **_kwargs: [SimpleNamespace(pool="batch", slots=4)]
    )
    added = _fake_pool_session(monkeypatch)
    item = _bare_item(
        smoke.PoolReferencesExistItem,
        session=None,
        config=None,
        pools=(smoke.PoolSeed(name="batch", slots=4),),
    )

    item.runtest()

    assert added == []


def test_pool_references_rejects_seed_colliding_with_existing_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail loudly when a configured pool already exists with a different slot count."""

    from airflow.models.pool import Pool

    dag_bag: Any = SimpleNamespace(dags={})
    monkeypatch.setattr(smoke, "_smoke_corpus", lambda _session, _config: dag_bag)
    monkeypatch.setattr(
        Pool, "get_pools", lambda **_kwargs: [SimpleNamespace(pool="default_pool", slots=128)]
    )
    item = _bare_item(
        smoke.PoolReferencesExistItem,
        session=None,
        config=None,
        pools=(smoke.PoolSeed(name="default_pool", slots=4),),
    )

    with pytest.raises(smoke.SmokeCheckFailure, match="cannot seed `default_pool`"):
        item.runtest()


def test_dag_id_pattern_item_passes_matching_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise nothing when every dag_id matches the configured pattern."""

    dag_bag: Any = SimpleNamespace(dags={"team_a": _dag("team_a")})
    monkeypatch.setattr(smoke, "_smoke_corpus", lambda _session, _config: dag_bag)
    item = _bare_item(
        smoke.DagIdPatternItem, session=None, config=None, pattern=re.compile("^team_")
    )

    item.runtest()


def test_required_dag_tags_item_passes_when_tags_present(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise nothing when every Dag carries the required tags."""

    dag_bag: Any = SimpleNamespace(
        dags={"tagged": _dag("tagged", tags=frozenset({"team-a", "extra"}))}
    )
    monkeypatch.setattr(smoke, "_smoke_corpus", lambda _session, _config: dag_bag)
    item = _bare_item(
        smoke.RequiredDagTagsItem, session=None, config=None, tags=frozenset({"team-a"})
    )

    item.runtest()


def test_forbid_default_owner_item_reports_every_stock_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report each task owned by Airflow's stock default owner."""

    dag_bag: Any = SimpleNamespace(
        dags={
            "etl": _dag(
                "etl",
                tasks=(_task("stock", owner="airflow"), _task("owned", owner="team-a")),
            )
        }
    )
    monkeypatch.setattr(smoke, "_smoke_corpus", lambda _session, _config: dag_bag)
    item = _bare_item(smoke.ForbidDefaultOwnerItem, session=None, config=None)

    with pytest.raises(smoke.SmokeCheckFailure, match="owned by the stock") as caught:
        item.runtest()

    assert "task `stock`" in str(caught.value)
    assert "task `owned`" not in str(caught.value)


def test_forbid_default_owner_item_passes_when_every_task_is_owned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raise nothing when no task carries the stock owner."""

    dag_bag: Any = SimpleNamespace(
        dags={"etl": _dag("etl", tasks=(_task("owned", owner="team"),))}
    )
    monkeypatch.setattr(smoke, "_smoke_corpus", lambda _session, _config: dag_bag)
    item = _bare_item(smoke.ForbidDefaultOwnerItem, session=None, config=None)

    item.runtest()


def _snapshot_item(
    monkeypatch: pytest.MonkeyPatch,
    *,
    dags: dict[str, Any],
    snapshot_dir: Path,
    update: bool,
    serialize_dag: Any = lambda _dag: {"dag_id": "irrelevant"},
) -> Any:
    """Build a bare `SerializedDagSnapshotItem` wired to a fake Dag bag and serializer.

    Parameters:
        monkeypatch: pytest.MonkeyPatch used to stub the shared Dag bag and serializer.
        dags: dict[str, Any] mapping dag_id to a Dag double passed to `serialize_dag`.
        snapshot_dir: pathlib.Path used as the item's committed snapshot directory.
        update: bool indicating whether the item runs in update mode.
        serialize_dag: Any callable serializing a Dag double into a plain dict.

    Returns:
        Any containing the constructed item with only the attributes under test.
    """

    dag_bag: Any = SimpleNamespace(dags=dags)
    monkeypatch.setattr(smoke, "_smoke_corpus", lambda _session, _config: dag_bag)
    monkeypatch.setattr(
        smoke,
        "_get_dag_serializer",
        lambda: SimpleNamespace(serialize_dag=serialize_dag),
    )
    return _bare_item(
        smoke.SerializedDagSnapshotItem,
        session=_session(),
        config=_config(),
        snapshot_dir=snapshot_dir,
        update=update,
    )


def test_snapshot_item_update_mode_writes_new_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Write a new snapshot file when none exists yet."""

    snapshot_dir = tmp_path / "snapshots"
    item = _snapshot_item(
        monkeypatch,
        dags={"sample": _dag("sample")},
        snapshot_dir=snapshot_dir,
        update=True,
        serialize_dag=lambda _dag: {"dag_id": "sample"},
    )

    item.runtest()

    written = (snapshot_dir / "sample.json").read_text(encoding="utf-8")
    assert written == '{\n  "dag_id": "sample"\n}\n'


def test_snapshot_item_update_mode_overwrites_existing_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Overwrite a differing committed snapshot in update mode."""

    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    (snapshot_dir / "sample.json").write_text("stale", encoding="utf-8")
    item = _snapshot_item(
        monkeypatch,
        dags={"sample": _dag("sample")},
        snapshot_dir=snapshot_dir,
        update=True,
        serialize_dag=lambda _dag: {"dag_id": "sample"},
    )

    item.runtest()

    written = (snapshot_dir / "sample.json").read_text(encoding="utf-8")
    assert written == '{\n  "dag_id": "sample"\n}\n'


def test_snapshot_item_diff_mode_fails_when_snapshot_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Fail naming the missing path and the update flag when no snapshot exists."""

    snapshot_dir = tmp_path / "snapshots"
    item = _snapshot_item(
        monkeypatch, dags={"sample": _dag("sample")}, snapshot_dir=snapshot_dir, update=False
    )

    with pytest.raises(smoke.SmokeCheckFailure, match="has no committed snapshot") as caught:
        item.runtest()

    assert "--airflow-smoke-update" in str(caught.value)


def test_snapshot_item_diff_mode_passes_when_snapshot_matches(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Raise nothing when the committed snapshot matches the current serialization."""

    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    (snapshot_dir / "sample.json").write_text('{\n  "dag_id": "sample"\n}\n', encoding="utf-8")
    item = _snapshot_item(
        monkeypatch,
        dags={"sample": _dag("sample")},
        snapshot_dir=snapshot_dir,
        update=False,
        serialize_dag=lambda _dag: {"dag_id": "sample"},
    )

    item.runtest()


def test_snapshot_item_diff_mode_fails_with_diff_on_drift(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Fail with a unified diff body naming the dag_id when content drifted."""

    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    (snapshot_dir / "sample.json").write_text('{\n  "dag_id": "sample"\n}\n', encoding="utf-8")
    item = _snapshot_item(
        monkeypatch,
        dags={"sample": _dag("sample")},
        snapshot_dir=snapshot_dir,
        update=False,
        serialize_dag=lambda _dag: {"dag_id": "sample", "tags": ["new-tag"]},
    )

    with pytest.raises(smoke.SmokeCheckFailure, match=r"Dag `sample` drifted") as caught:
        item.runtest()

    assert "-{" in str(caught.value) or "+  " in str(caught.value)


def test_snapshot_item_aggregates_serialize_failures_without_blocking_other_dags(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Report a per-Dag serialize failure while still processing every other Dag."""

    snapshot_dir = tmp_path / "snapshots"

    def serialize_dag(dag: Any) -> dict[str, Any]:
        if dag == "explode":
            raise ValueError("cannot serialize a lambda")
        return {"dag_id": "fine"}

    item = _snapshot_item(
        monkeypatch,
        dags={"broken": "explode", "fine": "fine"},
        snapshot_dir=snapshot_dir,
        update=True,
        serialize_dag=serialize_dag,
    )

    with pytest.raises(smoke.SmokeCheckFailure, match="cannot serialize a lambda") as caught:
        item.runtest()

    assert "Dag `broken`" in str(caught.value)
    assert (snapshot_dir / "fine.json").is_file()


def _sample_corpus() -> smoke.SmokeCorpus:
    """Create one portable corpus for cache and artifact tests.

    Returns:
        pytest_airflow_in_a_box.smoke.SmokeCorpus containing one Dag.
    """

    return smoke.SmokeCorpus(
        dags={
            "sample": smoke.SmokeDag(
                dag_id="sample",
                tags=frozenset({"team-a"}),
                tasks=(smoke.SmokeTask(task_id="task", owner="team-a", pool="default_pool"),),
                can_be_scheduled=False,
                serialized={"dag_id": "sample"},
                serialization_error=None,
                serialization_seconds=0.1,
            )
        },
        import_errors={"broken.py": "traceback"},
        dagbag_stats=(
            smoke.SmokeDagFileStat(
                file="sample.py", duration=timedelta(seconds=0.25), dag_num=1, task_num=1
            ),
        ),
        producer_pid=123,
        producer_worker="gw1",
    )


def test_smoke_corpus_artifact_round_trips() -> None:
    """Preserve every portable field through the shared JSON representation."""

    corpus = _sample_corpus()

    assert smoke._smoke_corpus_from_payload(smoke._smoke_corpus_payload(corpus)) == corpus


def test_smoke_corpus_rejects_an_unknown_artifact_version() -> None:
    """Reject a shared artifact written by an incompatible plugin schema."""

    with pytest.raises(ValueError, match="Unsupported smoke corpus version"):
        smoke._smoke_corpus_from_payload({"version": -1})


def test_smoke_corpus_build_extracts_portable_data(monkeypatch: pytest.MonkeyPatch) -> None:
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
    monkeypatch.setattr(smoke, "_dag_folder", lambda _config: Path("dags"))
    monkeypatch.setattr(smoke, "build_dag_bag", lambda _folder: dag_bag)
    monkeypatch.setattr(
        smoke, "_get_dag_serializer", lambda: SimpleNamespace(serialize_dag=serialize)
    )

    session: Any = SimpleNamespace(stash=pytest.Stash())
    corpus = smoke._build_smoke_corpus(session, _config(parse_timeout="12.5"))

    assert corpus.dags["good"].serialized == {"dag_id": "good"}
    assert corpus.dags["broken"].serialization_error == "cannot serialize callback"
    assert corpus.dags["broken"].tasks[0].pool == "custom"
    assert corpus.producer_worker == "gw2"
    assert os.environ["AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT"] == "12.5"


def test_smoke_corpus_reuses_dag_bag_parsed_by_full_dag_bag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reuse a DagBag already parsed by `full_dag_bag` in this process, per issue #85."""

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
    monkeypatch.setattr(smoke, "build_dag_bag", _fail_if_called)
    monkeypatch.setattr(
        smoke,
        "_get_dag_serializer",
        lambda: SimpleNamespace(serialize_dag=lambda _dag: {}),
    )

    corpus = smoke._build_smoke_corpus(session, _config(parse_timeout="1"))

    assert set(corpus.dags) == {"good"}


def test_smoke_corpus_does_not_pin_dag_bag_when_full_dag_bag_never_ran(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Leave the session's live-DagBag cache empty for a smoke-only run.

    Regression test for issue #85: the shared cache exists so `full_dag_bag` and the
    smoke corpus builder can hand off a parse to each other, not so the smoke builder's
    own fresh parse gets pinned on the session for the rest of a run that never touches
    `full_dag_bag` -- that would keep a large corpus's live Dag objects alive for nothing.
    """

    good = SimpleNamespace(tags=set(), tasks=[], timetable=SimpleNamespace(can_be_scheduled=True))
    dag_bag = SimpleNamespace(dags={"good": good}, import_errors={}, dagbag_stats=[])
    session: Any = SimpleNamespace(stash=pytest.Stash())

    monkeypatch.setenv("AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT", "999")
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "master")
    monkeypatch.setattr(smoke, "_dag_folder", lambda _config: Path("dags"))
    monkeypatch.setattr(smoke, "build_dag_bag", lambda _folder: dag_bag)
    monkeypatch.setattr(
        smoke,
        "_get_dag_serializer",
        lambda: SimpleNamespace(serialize_dag=lambda _dag: {}),
    )

    smoke._build_smoke_corpus(session, _config(parse_timeout="1"))

    assert dagbag.LIVE_DAG_BAG_KEY not in session.stash


def test_smoke_corpus_is_built_once_and_cached_per_process(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Build one artifact, then load it from another process-shaped session."""

    builds: list[object] = []
    corpus = _sample_corpus()
    config = _config()
    monkeypatch.setattr(
        smoke, "get_bootstrap_state", lambda _config: SimpleNamespace(root=tmp_path)
    )
    monkeypatch.setattr(
        smoke,
        "_build_smoke_corpus",
        lambda _session, config: (builds.append(config), corpus)[1],
    )
    first_session: Any = SimpleNamespace(stash=pytest.Stash())

    assert smoke._smoke_corpus(first_session, config) is corpus
    assert smoke._smoke_corpus(first_session, config) is corpus

    second_session: Any = SimpleNamespace(stash=pytest.Stash())
    loaded = smoke._smoke_corpus(second_session, config)

    assert builds == [config]
    assert loaded == corpus
    assert loaded is not corpus


def _write_dags(pytester: pytest.Pytester, **files: str) -> Any:
    """Create a ``dags`` directory containing the provided Dag modules.

    Parameters:
        pytester: pytest.Pytester owning the temporary project directory.
        files: str module contents keyed by module name without suffix.

    Returns:
        pathlib.Path containing the new Dag directory.
    """

    folder = pytester.path / "dags"
    folder.mkdir()
    for name, contents in files.items():
        (folder / f"{name}.py").write_text(contents, encoding="utf-8")
    return folder


VALID_DAG = """
from airflow.sdk import DAG, task

with DAG(dag_id="valid_dag", schedule=None, tags=["team-a"]) as dag:
    @task
    def t():
        pass

    t()
"""

VALID_DAG_WITH_OWNER = """
from airflow.sdk import DAG, task

with DAG(dag_id="valid_dag", schedule=None, tags=["team-a"]) as dag:
    @task(owner="team-a")
    def t():
        pass

    t()
"""

BROKEN_DAG = """
raise RuntimeError("deliberately broken smoke test Dag")
"""

DUPLICATE_A = 'from airflow.sdk import DAG\n\ndag_a = DAG(dag_id="dup_id", schedule=None)\n'
DUPLICATE_B = 'from airflow.sdk import DAG\n\ndag_b = DAG(dag_id="dup_id", schedule=None)\n'

SLOW_DAG = """
import time

time.sleep(0.3)

from airflow.sdk import DAG

slow_dag = DAG(dag_id="slow_dag", schedule=None)
"""

POOL_DAG = """
from airflow.sdk import DAG, task

with DAG(dag_id="pool_dag", schedule=None) as dag:
    @task(pool="batch")
    def t():
        pass

    t()
"""


def test_smoke_disabled_by_default_collects_nothing(pytester: pytest.Pytester) -> None:
    """Leave the Dag folder untouched when the feature is not enabled."""

    _write_dags(pytester, valid=VALID_DAG)

    result = pytester.runpytest_subprocess("-q", "--dag-folder=dags")

    result.assert_outcomes(passed=0, failed=0)
    assert "smoke" not in result.stdout.str()


def test_airflow_smoke_option_enables_the_catalog(pytester: pytest.Pytester) -> None:
    """Collect every core smoke item once the CLI option is passed."""

    _write_dags(pytester, valid=VALID_DAG)

    result = pytester.runpytest_subprocess("-v", "--airflow-smoke", "--dag-folder=dags")

    result.assert_outcomes(passed=5)
    result.stdout.fnmatch_lines(
        [
            "*::smoke::test_dag_bag_integrity PASSED*",
            "*::smoke::test_dag_serialization_roundtrip PASSED*",
            "*::smoke::test_no_duplicate_dag_ids PASSED*",
            "*::smoke::test_schedule_sanity PASSED*",
            "*::smoke::test_pool_references_exist PASSED*",
        ]
    )


def test_smoke_catalog_reuses_full_dag_bag_parse(pytester: pytest.Pytester) -> None:
    """Parse the Dag folder once when a `full_dag_bag` consumer runs with the catalog.

    Regression test for issue #85: a Dag module that records every time it is imported
    proves the folder is not parsed independently by the smoke corpus builder and by
    `full_dag_bag` in the same worker process. The bundled catalog is always collected
    last (`collection.py`), so in this default ordering `full_dag_bag`'s consumer test
    parses first and the smoke corpus builder is the one reusing that DagBag.
    """

    counter = pytester.path / "parses.txt"
    dag_folder = pytester.path / "dags"
    dag_folder.mkdir()
    (dag_folder / "counted.py").write_text(
        f"""
from pathlib import Path
from airflow.sdk import DAG, task

with Path({str(counter)!r}).open("a", encoding="utf-8") as handle:
    handle.write("x")

with DAG(dag_id="counted_dag", schedule=None, tags=["team-a"]) as dag:
    @task
    def t():
        pass

    t()
""",
        encoding="utf-8",
    )
    pytester.makepyfile(
        test_consumer="""
        def test_consumer(full_dag_bag):
            assert set(full_dag_bag.dags) == {"counted_dag"}
        """
    )

    result = pytester.runpytest_subprocess("-q", "--airflow-smoke", "--dag-folder=dags")

    result.assert_outcomes(passed=6)
    assert counter.read_text(encoding="utf-8").count("x") == 1


def test_smoke_integrity_enforces_parse_timeout_after_full_dag_bag_parses(
    pytester: pytest.Pytester,
) -> None:
    """Honor the configured parse timeout even when `full_dag_bag` parses first.

    Regression test for issue #85: before the shared-cache fix, a DagBag `full_dag_bag`
    parsed never had `AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT` applied, so a slow Dag file
    reused by the smoke corpus builder would silently escape the configured timeout.
    """

    _write_dags(pytester, slow=SLOW_DAG)
    pytester.makepyfile(
        test_consumer="""
        def test_consumer(full_dag_bag):
            # The timeout below is short enough that `slow.py` fails to import
            # under `full_dag_bag`'s own parse too; only presence is asserted here.
            assert full_dag_bag is not None
        """
    )
    pytester.makeini("[pytest]\nairflow_smoke = true\nairflow_dag_parse_timeout = 0.05\n")

    result = pytester.runpytest_subprocess("-v", "--dag-folder=dags")

    result.assert_outcomes(passed=5, failed=1)
    result.stdout.fnmatch_lines(["*::smoke::test_dag_bag_integrity FAILED*"])
    # Distinguishes "the parse itself was hard-killed by the applied timeout" (this
    # message, only possible if AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT was set before
    # `full_dag_bag` parsed) from the post-hoc slowpoke/duration check alone, which
    # would fail this item on `slow.py`'s 0.3s sleep regardless of whether the fix works.
    result.stdout.fnmatch_lines(["*Dag file import check failed*slow.py*"])


def test_ini_option_enables_the_catalog(pytester: pytest.Pytester) -> None:
    """Enable the smoke catalog through the ini option alone."""

    _write_dags(pytester, valid=VALID_DAG)
    pytester.makeini("[pytest]\nairflow_smoke = true\n")

    result = pytester.runpytest_subprocess("-q", "--dag-folder=dags")

    result.assert_outcomes(passed=5)


def test_serialization_sampling_bounds_the_catalog(pytester: pytest.Pytester) -> None:
    """Pass the full catalog while serializing only the configured sample."""

    second = VALID_DAG.replace("valid_dag", "second_dag")
    _write_dags(pytester, valid=VALID_DAG, second=second)
    pytester.makeini("[pytest]\nairflow_smoke = true\nairflow_serialization_sample_size = 1\n")

    result = pytester.runpytest_subprocess(
        "-q", "--dag-folder=dags", "-m", "smoke", "--log-cli-level=INFO"
    )

    result.assert_outcomes(passed=5)
    result.stdout.fnmatch_lines(["*deterministic sample of 1 of 2 Dags (seed '0')*"])


def test_smoke_update_rejects_sampling_at_startup(pytester: pytest.Pytester) -> None:
    """Refuse to regenerate snapshots from a serialization sample."""

    _write_dags(pytester, valid=VALID_DAG)
    pytester.makeini(
        "[pytest]\n"
        "airflow_smoke = true\n"
        "airflow_dag_snapshot_dir = snapshots\n"
        "airflow_serialization_sample_size = 1\n"
    )

    result = pytester.runpytest_subprocess("-q", "--dag-folder=dags", "--airflow-smoke-update")

    assert result.ret != 0
    result.stderr.fnmatch_lines(["*`--airflow-smoke-update` cannot be combined*"])


def test_broken_dag_fails_integrity_with_traceback(pytester: pytest.Pytester) -> None:
    """Fail the integrity item and surface the Dag file's own traceback."""

    _write_dags(pytester, broken=BROKEN_DAG)

    result = pytester.runpytest_subprocess("-q", "--airflow-smoke", "--dag-folder=dags")

    result.assert_outcomes(passed=4, failed=1)
    result.stdout.fnmatch_lines(
        ["*Dag file import check failed*", "*deliberately broken smoke test Dag*"]
    )


def test_duplicate_dag_ids_fail_dedicated_item(pytester: pytest.Pytester) -> None:
    """Fail the dedicated duplicate-id item and name both colliding files."""

    _write_dags(pytester, a=DUPLICATE_A, b=DUPLICATE_B)

    result = pytester.runpytest_subprocess("-q", "--airflow-smoke", "--dag-folder=dags")

    result.assert_outcomes(passed=3, failed=2)
    # DagBag file-collection order is not stable across platforms, so the message
    # names the colliding pair in either order -- assert both filelocs appear.
    report = result.stdout.str()
    assert "also found in" in report
    assert re.search(r"a\.py", report)
    assert re.search(r"b\.py", report)


def test_slowpoke_warns_without_failing(pytester: pytest.Pytester) -> None:
    """Warn on a slow file without failing the run."""

    _write_dags(pytester, slow=SLOW_DAG)
    pytester.makeini(
        "[pytest]\n"
        "airflow_smoke = true\n"
        "airflow_dag_parse_timeout = 1.0\n"
        "airflow_dag_parse_slowpoke_ratio = 0.2\n"
    )

    result = pytester.runpytest_subprocess("-q", "--dag-folder=dags", "-m", "smoke")

    result.assert_outcomes(passed=5)
    result.stdout.fnmatch_lines(["*SlowDagParseWarning*"])


def test_timeout_crossing_fails_the_run(pytester: pytest.Pytester) -> None:
    """Fail the integrity item when a file exceeds the hard parse timeout."""

    _write_dags(pytester, slow=SLOW_DAG)
    pytester.makeini("[pytest]\nairflow_smoke = true\nairflow_dag_parse_timeout = 0.05\n")

    result = pytester.runpytest_subprocess("-v", "--dag-folder=dags", "-m", "smoke")

    result.assert_outcomes(passed=4, failed=1)
    result.stdout.fnmatch_lines(["*::smoke::test_dag_bag_integrity FAILED*"])


def test_pool_seed_option_allows_a_configured_pool_reference(pytester: pytest.Pytester) -> None:
    """Seed a custom pool so a task referencing it passes the check."""

    _write_dags(pytester, valid=POOL_DAG)

    unconfigured = pytester.runpytest_subprocess(
        "-q", "--airflow-smoke", "--dag-folder=dags", "-m", "smoke"
    )
    unconfigured.assert_outcomes(passed=4, failed=1)
    unconfigured.stdout.fnmatch_lines(["*references unknown pool `batch`*"])

    pytester.makeini("[pytest]\nairflow_smoke = true\nairflow_pools =\n    batch = 4\n")
    configured = pytester.runpytest_subprocess("-q", "--dag-folder=dags", "-m", "smoke")
    configured.assert_outcomes(passed=5)


def test_pool_seed_option_rejects_malformed_lines(pytester: pytest.Pytester) -> None:
    """Fail collection when an `airflow_pools` line is malformed."""

    _write_dags(pytester, valid=VALID_DAG)
    pytester.makeini("[pytest]\nairflow_smoke = true\nairflow_pools =\n    batch\n")

    result = pytester.runpytest_subprocess("-q", "--dag-folder=dags")

    assert result.ret != 0
    result.stderr.fnmatch_lines(["*must be `name = slots`*"])


def test_pool_seed_option_rejects_collision_with_existing_pool(pytester: pytest.Pytester) -> None:
    """Fail the item when a configured pool collides with an existing one."""

    _write_dags(pytester, valid=VALID_DAG)
    pytester.makeini("[pytest]\nairflow_smoke = true\nairflow_pools =\n    default_pool = 4\n")

    result = pytester.runpytest_subprocess("-q", "--dag-folder=dags", "-m", "smoke")

    result.assert_outcomes(passed=4, failed=1)
    result.stdout.fnmatch_lines(["*cannot seed `default_pool`*"])


def test_dag_id_pattern_policy_appears_only_when_configured(pytester: pytest.Pytester) -> None:
    """Collect the pattern policy item only when its ini is set."""

    _write_dags(pytester, valid=VALID_DAG)

    disabled = pytester.runpytest_subprocess("-q", "--airflow-smoke", "--dag-folder=dags")
    disabled.assert_outcomes(passed=5)
    assert "test_dag_id_pattern" not in disabled.stdout.str()

    pytester.makeini("[pytest]\nairflow_smoke = true\nairflow_dag_id_pattern = ^valid_\n")
    enabled = pytester.runpytest_subprocess("-v", "--dag-folder=dags")
    enabled.assert_outcomes(passed=6)
    enabled.stdout.fnmatch_lines(["*::smoke::test_dag_id_pattern PASSED*"])


def test_dag_id_pattern_policy_fails_on_mismatch(pytester: pytest.Pytester) -> None:
    """Fail the pattern policy item when a dag_id does not match."""

    _write_dags(pytester, valid=VALID_DAG)
    pytester.makeini("[pytest]\nairflow_smoke = true\nairflow_dag_id_pattern = ^nomatch_\n")

    result = pytester.runpytest_subprocess("-q", "--dag-folder=dags", "-m", "smoke")

    result.assert_outcomes(passed=5, failed=1)
    result.stdout.fnmatch_lines(["*does not match pattern*"])


def test_required_dag_tags_policy_appears_only_when_configured(pytester: pytest.Pytester) -> None:
    """Collect the required-tags policy item only when its ini is set."""

    _write_dags(pytester, valid=VALID_DAG)

    disabled = pytester.runpytest_subprocess("-q", "--airflow-smoke", "--dag-folder=dags")
    assert "test_required_dag_tags" not in disabled.stdout.str()

    pytester.makeini("[pytest]\nairflow_smoke = true\nairflow_required_dag_tags =\n    team-a\n")
    enabled = pytester.runpytest_subprocess("-v", "--dag-folder=dags")
    enabled.assert_outcomes(passed=6)
    enabled.stdout.fnmatch_lines(["*::smoke::test_required_dag_tags PASSED*"])


def test_required_dag_tags_policy_fails_on_missing_tag(pytester: pytest.Pytester) -> None:
    """Fail the required-tags policy item when a Dag is missing a required tag."""

    _write_dags(pytester, valid=VALID_DAG)
    pytester.makeini(
        "[pytest]\nairflow_smoke = true\nairflow_required_dag_tags =\n    team-a\n    team-b\n"
    )

    result = pytester.runpytest_subprocess("-q", "--dag-folder=dags", "-m", "smoke")

    result.assert_outcomes(passed=5, failed=1)
    result.stdout.fnmatch_lines(["*missing required tags*'team-b'*"])


def test_forbid_default_owner_policy_appears_only_when_configured(
    pytester: pytest.Pytester,
) -> None:
    """Collect the default-owner policy item only when its ini is set."""

    _write_dags(pytester, valid=VALID_DAG)

    disabled = pytester.runpytest_subprocess("-q", "--airflow-smoke", "--dag-folder=dags")
    assert "test_forbid_default_owner" not in disabled.stdout.str()

    pytester.makeini("[pytest]\nairflow_smoke = true\nairflow_forbid_default_owner = true\n")
    enabled = pytester.runpytest_subprocess("-q", "--dag-folder=dags", "-m", "smoke")
    enabled.assert_outcomes(passed=5, failed=1)
    enabled.stdout.fnmatch_lines(["*owned by the stock*`airflow`*owner*"])


def test_dag_snapshot_policy_appears_only_when_configured(pytester: pytest.Pytester) -> None:
    """Collect the snapshot policy item only when its ini is set."""

    _write_dags(pytester, valid=VALID_DAG)

    disabled = pytester.runpytest_subprocess("-q", "--airflow-smoke", "--dag-folder=dags")
    disabled.assert_outcomes(passed=5)
    assert "test_dag_serialization_snapshot" not in disabled.stdout.str()

    pytester.makeini("[pytest]\nairflow_smoke = true\nairflow_dag_snapshot_dir = snapshots\n")
    enabled = pytester.runpytest_subprocess("-v", "--dag-folder=dags", "--airflow-smoke-update")
    enabled.assert_outcomes(passed=6)
    enabled.stdout.fnmatch_lines(["*::smoke::test_dag_serialization_snapshot PASSED*"])


def test_dag_snapshot_update_flag_writes_snapshot_file(pytester: pytest.Pytester) -> None:
    """Write one JSON snapshot file per Dag when `--airflow-smoke-update` is passed."""

    _write_dags(pytester, valid=VALID_DAG)
    pytester.makeini("[pytest]\nairflow_smoke = true\nairflow_dag_snapshot_dir = snapshots\n")

    result = pytester.runpytest_subprocess(
        "-q", "--dag-folder=dags", "--airflow-smoke-update", "-m", "smoke"
    )

    result.assert_outcomes(passed=6)
    snapshot_path = pytester.path / "snapshots" / "valid_dag.json"
    assert snapshot_path.is_file()
    assert '"dag_id": "valid_dag"' in snapshot_path.read_text(encoding="utf-8")


def test_dag_snapshot_second_run_passes_without_update(pytester: pytest.Pytester) -> None:
    """Pass in diff mode once a snapshot has been committed for an unchanged Dag."""

    _write_dags(pytester, valid=VALID_DAG)
    pytester.makeini("[pytest]\nairflow_smoke = true\nairflow_dag_snapshot_dir = snapshots\n")
    pytester.runpytest_subprocess(
        "-q", "--dag-folder=dags", "--airflow-smoke-update", "-m", "smoke"
    ).assert_outcomes(passed=6)

    result = pytester.runpytest_subprocess("-q", "--dag-folder=dags", "-m", "smoke")

    result.assert_outcomes(passed=6)


def test_dag_snapshot_fails_on_drift(pytester: pytest.Pytester) -> None:
    """Fail in diff mode once the Dag's serialized structure drifts from its snapshot."""

    _write_dags(pytester, valid=VALID_DAG)
    pytester.makeini("[pytest]\nairflow_smoke = true\nairflow_dag_snapshot_dir = snapshots\n")
    pytester.runpytest_subprocess(
        "-q", "--dag-folder=dags", "--airflow-smoke-update", "-m", "smoke"
    ).assert_outcomes(passed=6)

    (pytester.path / "dags" / "valid.py").write_text(
        VALID_DAG.replace('tags=["team-a"]', 'tags=["team-b"]'), encoding="utf-8"
    )
    result = pytester.runpytest_subprocess("-q", "--dag-folder=dags", "-m", "smoke")

    result.assert_outcomes(passed=5, failed=1)
    result.stdout.fnmatch_lines(["*drifted from its committed snapshot*"])


def test_smoke_marker_selects_exactly_the_bundled_items(pytester: pytest.Pytester) -> None:
    """Select exactly the bundled smoke items with `-m smoke`."""

    _write_dags(pytester, valid=VALID_DAG)

    pytester.makepyfile(
        """
        def test_regular():
            assert True
        """
    )

    selected = pytester.runpytest_subprocess(
        "-q", "--airflow-smoke", "--dag-folder=dags", "-m", "smoke"
    )
    selected.assert_outcomes(passed=5, deselected=1)

    deselected = pytester.runpytest_subprocess(
        "-q", "--airflow-smoke", "--dag-folder=dags", "-m", "not smoke"
    )
    deselected.assert_outcomes(passed=1, deselected=5)


def test_explicit_node_id_and_file_args_exclude_smoke_catalog(
    pytester: pytest.Pytester,
) -> None:
    """Honor explicit node-ID and file positionals by dropping the synthetic catalog.

    Regression test for https://github.com/nredd/pytest-airflow-in-a-box/issues/54.
    """

    _write_dags(pytester, valid=VALID_DAG)
    pytester.makeini("[pytest]\nairflow_smoke = true\n")
    pytester.makepyfile(
        test_regular="""
        def test_regular():
            assert True
        """
    )

    node = pytester.runpytest_subprocess(
        "-q", "--dag-folder=dags", "test_regular.py::test_regular"
    )
    node.assert_outcomes(passed=1)
    node.stdout.no_fnmatch_line("*::smoke*")

    file = pytester.runpytest_subprocess("-q", "--dag-folder=dags", "test_regular.py")
    file.assert_outcomes(passed=1)
    file.stdout.no_fnmatch_line("*::smoke*")


def test_explicit_mark_expression_overrides_positional_exclusion(
    pytester: pytest.Pytester,
) -> None:
    """Let an explicit `-m` expression selecting `smoke` win over positional scoping.

    Regression test for https://github.com/nredd/pytest-airflow-in-a-box/issues/133: an
    explicit file or node-ID positional used to drop the smoke catalog unconditionally, so
    `-m smoke` combined with either had nothing to select.
    """

    _write_dags(pytester, valid=VALID_DAG)
    pytester.makepyfile(
        test_regular="""
        def test_regular():
            assert True
        """
    )

    file_selected = pytester.runpytest_subprocess(
        "-q", "--airflow-smoke", "--dag-folder=dags", "-m", "smoke", "test_regular.py"
    )
    file_selected.assert_outcomes(passed=5, deselected=1)

    node_selected = pytester.runpytest_subprocess(
        "-q",
        "--airflow-smoke",
        "--dag-folder=dags",
        "-m",
        "smoke",
        "test_regular.py::test_regular",
    )
    node_selected.assert_outcomes(passed=5, deselected=1)

    file_excluded = pytester.runpytest_subprocess(
        "-q", "--airflow-smoke", "--dag-folder=dags", "-m", "not smoke", "test_regular.py"
    )
    file_excluded.assert_outcomes(passed=1)
    file_excluded.stdout.no_fnmatch_line("*::smoke*")


def test_smoke_and_db_test_mark_expression_overrides_positional_exclusion(
    pytester: pytest.Pytester,
) -> None:
    """Let a conjunction with a non-`smoke` marker a real item carries win too.

    A flat matcher over the union of every known smoke marker name would wrongly resolve
    `-m "smoke and not db_test"`: `db_test` names a marker on *some* smoke items
    (`DagBagIntegrityItem`, `PoolReferencesExistItem`), so the union sees it as "present"
    and negating it evaluates to `False` -- even though the other seven bundled items
    genuinely lack `db_test` and the expression does select them.
    """

    _write_dags(pytester, valid=VALID_DAG)
    pytester.makepyfile(
        test_regular="""
        def test_regular():
            assert True
        """
    )

    with_db_test = pytester.runpytest_subprocess(
        "-q",
        "--airflow-smoke",
        "--dag-folder=dags",
        "-m",
        "smoke and db_test",
        "test_regular.py",
    )
    with_db_test.assert_outcomes(passed=2, deselected=4)

    without_db_test = pytester.runpytest_subprocess(
        "-q",
        "--airflow-smoke",
        "--dag-folder=dags",
        "-m",
        "smoke and not db_test",
        "test_regular.py",
    )
    without_db_test.assert_outcomes(passed=3, deselected=3)


def test_markexpr_override_reaches_every_conditional_smoke_item(
    pytester: pytest.Pytester,
) -> None:
    """Cover `_SMOKE_ITEM_MARK_SETS` against every ini-gated item, not just the bundled 5.

    `_SMOKE_ITEM_MARK_SETS` is hand-synced with the `add_marker` calls scattered across nine
    `pytest.Item` subclasses; the other `-m`-override regression tests only enable the five
    unconditional items. Enabling all four ini-gated policies too (every item, like every
    other, carries only `smoke` + `timeout`) would catch a future item adding a marker
    `_SMOKE_ITEM_MARK_SETS` doesn't yet know about. The Dag sets an explicit non-stock owner
    so `airflow_forbid_default_owner` passes rather than exercising its failure path.
    """

    _write_dags(pytester, valid=VALID_DAG_WITH_OWNER)
    pytester.makeini(
        "[pytest]\n"
        "airflow_smoke = true\n"
        "airflow_dag_id_pattern = ^valid_\n"
        "airflow_required_dag_tags =\n    team-a\n"
        "airflow_forbid_default_owner = true\n"
        "airflow_dag_snapshot_dir = snapshots\n"
    )
    pytester.makepyfile(
        test_regular="""
        def test_regular():
            assert True
        """
    )

    every_item = pytester.runpytest_subprocess(
        "-q",
        "--dag-folder=dags",
        "--airflow-smoke-update",
        "-m",
        "smoke and timeout",
        "test_regular.py",
    )
    every_item.assert_outcomes(passed=9, deselected=1)

    only_db_test = pytester.runpytest_subprocess(
        "-q", "--dag-folder=dags", "-m", "smoke and db_test", "test_regular.py"
    )
    only_db_test.assert_outcomes(passed=2, deselected=8)


def test_directory_positional_keeps_smoke_catalog(pytester: pytest.Pytester) -> None:
    """Keep the catalog when an explicit positional is a directory scan."""

    _write_dags(pytester, valid=VALID_DAG)
    pytester.makeini("[pytest]\nairflow_smoke = true\n")
    pytester.makepyfile(
        test_regular="""
        def test_regular():
            assert True
        """
    )

    result = pytester.runpytest_subprocess("-q", "--dag-folder=dags", ".")

    result.assert_outcomes(passed=6)


def test_keyword_expression_deselects_smoke_items(pytester: pytest.Pytester) -> None:
    """Deselect the whole catalog when `-k` matches only an ordinary test.

    Regression test for https://github.com/nredd/pytest-airflow-in-a-box/issues/54.
    """

    _write_dags(pytester, valid=VALID_DAG)
    pytester.makeini("[pytest]\nairflow_smoke = true\n")
    pytester.makepyfile(
        test_regular="""
        def test_regular():
            assert True
        """
    )

    result = pytester.runpytest_subprocess("-q", "--dag-folder=dags", "-k", "test_regular")

    result.assert_outcomes(passed=1, deselected=5)


def test_testpaths_file_glob_keeps_smoke_catalog(pytester: pytest.Pytester) -> None:
    """Keep the catalog when `testpaths` resolves to file args without user positionals."""

    _write_dags(pytester, valid=VALID_DAG)
    pytester.makeini("[pytest]\nairflow_smoke = true\ntestpaths = test_regular.py\n")
    pytester.makepyfile(
        test_regular="""
        def test_regular():
            assert True
        """
    )

    result = pytester.runpytest_subprocess("-q", "--dag-folder=dags")

    result.assert_outcomes(passed=6)


def test_deselect_prunes_single_smoke_item(pytester: pytest.Pytester) -> None:
    """Prune one smoke item with `--deselect` while the rest of the catalog runs."""

    _write_dags(pytester, valid=VALID_DAG)
    pytester.makeini("[pytest]\nairflow_smoke = true\n")

    result = pytester.runpytest_subprocess(
        "-q", "--dag-folder=dags", "--deselect", "::smoke::test_dag_bag_integrity"
    )

    result.assert_outcomes(passed=4, deselected=1)
