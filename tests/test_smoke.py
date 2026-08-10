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
    *, args_source: pytest.Config.ArgsSource, args: list[str], invocation_dir: Path
) -> Any:
    """Create a minimal configuration double for `_smoke_in_scope` tests.

    Parameters:
        args_source: pytest.Config.ArgsSource describing where the positional args came from.
        args: list[str] of resolved positional args.
        invocation_dir: pathlib.Path of the invocation directory.

    Returns:
        types.SimpleNamespace shaped like the configuration surface under test.
    """

    return SimpleNamespace(
        args_source=args_source, args=args, invocation_params=SimpleNamespace(dir=invocation_dir)
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
    item = _bare_item(smoke.DagSerializationRoundtripItem, session=None, config=None)

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
    item = _bare_item(smoke.DagSerializationRoundtripItem, session=None, config=None)

    item.runtest()


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
            serialize_dag=lambda _dag: pytest.fail("must not serialize"),
            deserialize_dag=lambda _dag: pytest.fail("must not serialize"),
        ),
    )
    item = _bare_item(smoke.ScheduleSanityItem, session=None, config=None)

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
    item = _bare_item(smoke.ScheduleSanityItem, session=None, config=None)

    with pytest.raises(smoke.SmokeCheckFailure, match="bad cron") as caught:
        item.runtest()

    assert "Dag `broken`" in str(caught.value)
    assert "Dag `healthy`" not in str(caught.value)


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
        lambda **_kwargs: [SimpleNamespace(pool="default_pool")],
    )
    item = _bare_item(smoke.PoolReferencesExistItem, session=None, config=None)

    with pytest.raises(smoke.SmokeCheckFailure, match="references unknown pool `nope`") as caught:
        item.runtest()

    assert "task `missing`" in str(caught.value)
    assert "task `known`" not in str(caught.value)


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
        session=None,
        config=None,
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

    monkeypatch.delenv("AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT", raising=False)
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw2")
    monkeypatch.setattr(smoke, "_dag_folder", lambda _config: Path("dags"))
    monkeypatch.setattr(smoke, "build_dag_bag", lambda _folder: dag_bag)
    monkeypatch.setattr(
        smoke, "_get_dag_serializer", lambda: SimpleNamespace(serialize_dag=serialize)
    )

    corpus = smoke._build_smoke_corpus(_config(parse_timeout="12.5"))

    assert corpus.dags["good"].serialized == {"dag_id": "good"}
    assert corpus.dags["broken"].serialization_error == "cannot serialize callback"
    assert corpus.dags["broken"].tasks[0].pool == "custom"
    assert corpus.producer_worker == "gw2"
    assert os.environ["AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT"] == "12.5"


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
        smoke, "_build_smoke_corpus", lambda value: (builds.append(value), corpus)[1]
    )
    first_session: Any = SimpleNamespace(stash=pytest.Stash())

    assert smoke._smoke_corpus(first_session, config) is corpus
    assert smoke._smoke_corpus(first_session, config) is corpus

    second_session: Any = SimpleNamespace(stash=pytest.Stash())
    loaded = smoke._smoke_corpus(second_session, config)

    assert builds == [config]
    assert loaded == corpus
    assert loaded is not corpus


def test_serialize_smoke_dag_uses_shared_payload_and_reports_producer_error() -> None:
    """Avoid reserializing a portable Dag and retain its producer failure."""

    corpus_dag = _sample_corpus().dags["sample"]
    serializer = SimpleNamespace(serialize_dag=lambda _dag: {"fallback": True})

    assert smoke._serialize_smoke_dag(corpus_dag, serializer) == {"dag_id": "sample"}
    assert smoke._serialize_smoke_dag(object(), serializer) == {"fallback": True}

    failed = smoke.SmokeDag(
        dag_id="failed",
        tags=frozenset(),
        tasks=(),
        can_be_scheduled=False,
        serialized={},
        serialization_error="producer failed",
    )
    with pytest.raises(ValueError, match="producer failed"):
        smoke._serialize_smoke_dag(failed, serializer)


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


def test_ini_option_enables_the_catalog(pytester: pytest.Pytester) -> None:
    """Enable the smoke catalog through the ini option alone."""

    _write_dags(pytester, valid=VALID_DAG)
    pytester.makeini("[pytest]\nairflow_smoke = true\n")

    result = pytester.runpytest_subprocess("-q", "--dag-folder=dags")

    result.assert_outcomes(passed=5)


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
