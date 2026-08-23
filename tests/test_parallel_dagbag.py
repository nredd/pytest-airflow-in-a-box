"""Test the pure sharding, merge, and encoding logic behind Dag bag fan-out."""

from __future__ import annotations

import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from pytest_airflow_in_a_box import parallel_dagbag


def _config(
    *,
    fanout: object = None,
    ini_fanout: object = False,
    workers: object = "0",
    min_files: object = "200",
    timeout: object = "600",
    parse_timeout: object = "30",
) -> Any:
    """Create a minimal configuration double for fan-out config-reader tests.

    Parameters:
        fanout: object containing the parsed `--airflow-dag-bag-fanout` option value.
        ini_fanout: object containing the `airflow_dag_bag_fanout` ini value.
        workers: object containing the `airflow_dag_bag_fanout_workers` ini value.
        min_files: object containing the `airflow_dag_bag_fanout_min_files` ini value.
        timeout: object containing the `airflow_dag_bag_fanout_timeout` ini value.
        parse_timeout: object containing the `airflow_dag_parse_timeout` ini value.

    Returns:
        types.SimpleNamespace shaped like the configuration surface under test.
    """

    ini_values = {
        "airflow_dag_bag_fanout": ini_fanout,
        "airflow_dag_bag_fanout_workers": workers,
        "airflow_dag_bag_fanout_min_files": min_files,
        "airflow_dag_bag_fanout_timeout": timeout,
        "airflow_dag_parse_timeout": parse_timeout,
    }
    option_values = {"airflow_dag_bag_fanout": fanout}
    return SimpleNamespace(
        getoption=lambda name: option_values[name],
        getini=lambda name: ini_values[name],
    )


def test_shard_file_paths_distributes_round_robin() -> None:
    """Distribute files round robin over a stably sorted list."""

    result = parallel_dagbag.shard_file_paths(["c.py", "a.py", "b.py", "d.py"], worker_count=2)

    assert result == [["a.py", "c.py"], ["b.py", "d.py"]]


def test_shard_file_paths_leaves_trailing_shards_empty() -> None:
    """Leave later shards empty when there are fewer files than workers."""

    result = parallel_dagbag.shard_file_paths(["a.py"], worker_count=3)

    assert result == [["a.py"], [], []]


def test_shard_file_paths_rejects_non_positive_worker_count() -> None:
    """Reject a non-positive worker count."""

    with pytest.raises(ValueError, match="must be positive"):
        parallel_dagbag.shard_file_paths(["a.py"], worker_count=0)


def test_should_fan_out_prefers_cli_option_over_ini() -> None:
    """Give the CLI option precedence over the ini value."""

    config = _config(fanout=True, ini_fanout=False, min_files="0")

    assert parallel_dagbag.should_fan_out(1, config) is True


def test_should_fan_out_false_when_disabled() -> None:
    """Report `False` when fan-out is disabled regardless of file count."""

    config = _config(fanout=None, ini_fanout=False, min_files="0")

    assert parallel_dagbag.should_fan_out(1000, config) is False


def test_should_fan_out_respects_minimum_file_count() -> None:
    """Skip fan-out below the configured minimum file count."""

    config = _config(fanout=None, ini_fanout=True, min_files="10")

    assert parallel_dagbag.should_fan_out(9, config) is False
    assert parallel_dagbag.should_fan_out(10, config) is True


def test_should_fan_out_rejects_non_boolean_ini() -> None:
    """Reject a non-boolean `airflow_dag_bag_fanout` ini value."""

    config = _config(fanout=None, ini_fanout="yes")

    with pytest.raises(pytest.UsageError, match="must be a boolean"):
        parallel_dagbag.should_fan_out(1000, config)


def test_resolved_worker_count_prefers_explicit_configuration() -> None:
    """Use the configured worker count over the auto-detected default."""

    config = _config(workers="4")

    assert parallel_dagbag.resolved_worker_count(config, file_count=1000) == 4


def test_resolved_worker_count_auto_detects_and_caps_at_file_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Auto-detect from the CPU count, capped at the discovered file count."""

    monkeypatch.setattr(parallel_dagbag, "_cpu_count", lambda: 8)
    config = _config(workers="0")

    assert parallel_dagbag.resolved_worker_count(config, file_count=3) == 3
    assert parallel_dagbag.resolved_worker_count(config, file_count=100) == 8


def test_resolved_worker_count_never_below_one() -> None:
    """Return at least one worker even for an empty corpus."""

    config = _config(workers="0")

    assert parallel_dagbag.resolved_worker_count(config, file_count=0) == 1


def test_non_negative_int_option_rejects_non_string() -> None:
    """Reject a non-string ini value."""

    config = _config(min_files=5, ini_fanout=True)

    with pytest.raises(pytest.UsageError, match="non-negative integer"):
        parallel_dagbag.should_fan_out(1000, config)


def test_non_negative_int_option_rejects_unparseable_value() -> None:
    """Reject an ini value that does not parse as an integer."""

    config = _config(min_files="not-a-number", ini_fanout=True)

    with pytest.raises(pytest.UsageError, match="non-negative integer"):
        parallel_dagbag.should_fan_out(1000, config)


def test_non_negative_int_option_rejects_negative_value() -> None:
    """Reject a negative ini value."""

    config = _config(min_files="-1", ini_fanout=True)

    with pytest.raises(pytest.UsageError, match="non-negative integer"):
        parallel_dagbag.should_fan_out(1000, config)


def test_positive_number_option_rejects_non_string() -> None:
    """Reject a non-string timeout ini value."""

    config = _config(timeout=600)

    with pytest.raises(pytest.UsageError, match="must be a number"):
        parallel_dagbag._fanout_timeout(config)


def test_positive_number_option_rejects_unparseable_value() -> None:
    """Reject a timeout ini value that does not parse as a number."""

    config = _config(timeout="soon")

    with pytest.raises(pytest.UsageError, match="must be a number"):
        parallel_dagbag._fanout_timeout(config)


def test_positive_number_option_rejects_non_positive_value() -> None:
    """Reject a non-positive timeout ini value."""

    config = _config(timeout="0")

    with pytest.raises(pytest.UsageError, match="must be positive"):
        parallel_dagbag._fanout_timeout(config)


def test_dagbag_import_timeout_reads_the_parse_timeout_ini() -> None:
    """Read `airflow_dag_parse_timeout` as the per-file import timeout."""

    config = _config(parse_timeout="45")

    assert parallel_dagbag.dagbag_import_timeout(config) == 45.0


def test_shard_spec_payload_round_trips() -> None:
    """Round-trip a shard spec through its JSON-compatible payload."""

    spec = parallel_dagbag.ShardSpec(
        dag_folder="/dags",
        file_paths=("/dags/a.py", "/dags/b.py"),
        needs_comms=True,
        dagbag_import_timeout=30.0,
    )

    restored = parallel_dagbag.shard_from_payload(spec.to_payload())

    assert restored == spec


class _FakeTimetable:
    """Minimal timetable double exposing only `can_be_scheduled`."""

    def __init__(self, *, can_be_scheduled: bool) -> None:
        """Create a fake timetable.

        Parameters:
            can_be_scheduled: bool controlling `can_be_scheduled`.
        """

        self.can_be_scheduled = can_be_scheduled


class _FakeTask:
    """Minimal operator double exposing only what `_encode_task` reads."""

    def __init__(self, task_id: str) -> None:
        """Create a fake unmapped operator.

        Parameters:
            task_id: str naming the task.
        """

        self.task_id = task_id
        self.owner = "airflow"
        self.pool = "default_pool"
        self.max_active_tis_per_dag = None


class _FakeDag:
    """Minimal Dag double exposing only what `_encode_dag` reads."""

    def __init__(
        self, *, dag_id: str, tasks: tuple[_FakeTask, ...], catchup: bool = False
    ) -> None:
        """Create a fake parsed Dag.

        Parameters:
            dag_id: str naming the Dag.
            tasks: tuple[_FakeTask, ...] containing the Dag's tasks.
            catchup: bool controlling `catchup`.
        """

        self.dag_id = dag_id
        self.tags = frozenset({"team-a"})
        self.tasks = tasks
        self.timetable = _FakeTimetable(can_be_scheduled=True)
        self.catchup = catchup
        self.fileloc = f"/dags/{dag_id}.py"


class _FakeSerializer:
    """Serializer double returning a canned encoding or raising per Dag id."""

    def __init__(self, *, failing_dag_ids: frozenset[str] = frozenset()) -> None:
        """Create a fake serializer.

        Parameters:
            failing_dag_ids: frozenset[str] naming Dag ids whose serialization fails.
        """

        self._failing_dag_ids = failing_dag_ids

    def serialize_dag(self, dag: _FakeDag) -> dict[str, Any]:
        """Return a canned encoding, or raise for a configured failing Dag id.

        Parameters:
            dag: _FakeDag being serialized.

        Returns:
            dict[str, Any] containing a canned encoding.

        Raises:
            RuntimeError: `dag.dag_id` is in `failing_dag_ids`.
        """

        if dag.dag_id in self._failing_dag_ids:
            raise RuntimeError(f"cannot serialize '{dag.dag_id}'")
        return {"dag_id": dag.dag_id}


def test_encode_shard_includes_every_dag_task_and_stat(monkeypatch: pytest.MonkeyPatch) -> None:
    """Encode dags, tasks, stats, and lookups into the portable shard payload shape."""

    monkeypatch.setattr(
        parallel_dagbag,
        "mapped_expansion",
        lambda task: (False, False, task.max_active_tis_per_dag),
    )
    monkeypatch.setattr(parallel_dagbag, "_get_dag_serializer", lambda: _FakeSerializer())
    dag = _FakeDag(dag_id="alpha", tasks=(_FakeTask("t1"),))
    stat = SimpleNamespace(
        file="alpha.py", duration=SimpleNamespace(total_seconds=lambda: 1.5), dag_num=1, task_num=1
    )
    lookup = SimpleNamespace(kind="variable", key="v", file="alpha.py", line=3)

    payload = parallel_dagbag.encode_shard({"alpha": dag}, {"broken.py": "boom"}, [stat], [lookup])

    assert payload["import_errors"] == {"broken.py": "boom"}
    assert payload["dagbag_stats"] == [
        {"file": "alpha.py", "duration": 1.5, "dag_num": 1, "task_num": 1}
    ]
    assert payload["runtime_lookups"] == [
        {"kind": "variable", "key": "v", "file": "alpha.py", "line": 3}
    ]
    encoded_dag = payload["dags"]["alpha"]
    assert encoded_dag["tags"] == ["team-a"]
    assert encoded_dag["tasks"] == [
        {
            "task_id": "t1",
            "owner": "airflow",
            "pool": "default_pool",
            "is_mapped": False,
            "mapped_over_runtime_data": False,
            "max_active_tis_per_dag": None,
        }
    ]
    assert encoded_dag["can_be_scheduled"] is True
    assert encoded_dag["catchup"] is False
    assert encoded_dag["fileloc"] == "/dags/alpha.py"
    assert encoded_dag["serialized"] == {"dag_id": "alpha"}
    assert encoded_dag["serialization_error"] is None
    assert isinstance(encoded_dag["serialization_seconds"], float)


def test_encode_dag_records_serialization_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Record a serialization failure message instead of raising."""

    monkeypatch.setattr(
        parallel_dagbag,
        "_get_dag_serializer",
        lambda: _FakeSerializer(failing_dag_ids=frozenset({"alpha"})),
    )
    dag = _FakeDag(dag_id="alpha", tasks=())

    payload = parallel_dagbag.encode_shard({"alpha": dag}, {}, [], [])

    encoded_dag = payload["dags"]["alpha"]
    assert encoded_dag["serialized"] is None
    assert encoded_dag["serialization_error"] == "cannot serialize 'alpha'"


def test_merge_shard_payloads_with_no_collisions() -> None:
    """Merge disjoint shards without touching either shard's dags or errors."""

    shard_one = parallel_dagbag.encode_shard(
        {"alpha": _FakeDag(dag_id="alpha", tasks=())}, {}, [], []
    )
    shard_two = parallel_dagbag.encode_shard(
        {"beta": _FakeDag(dag_id="beta", tasks=())}, {"broken.py": "boom"}, [], []
    )

    merged = parallel_dagbag.merge_shard_payloads([shard_one, shard_two])

    assert set(merged["dags"]) == {"alpha", "beta"}
    assert merged["import_errors"] == {"broken.py": "boom"}


def test_merge_shard_payloads_synthesizes_duplicate_id_error() -> None:
    """Keep the first-seen `dag_id` and synthesize a marked import error for the loser."""

    first = parallel_dagbag.encode_shard({"dup": _FakeDag(dag_id="dup", tasks=())}, {}, [], [])
    second_dag = _FakeDag(dag_id="dup", tasks=())
    second_dag.fileloc = "/dags/other/dup.py"
    second = parallel_dagbag.encode_shard({"dup": second_dag}, {}, [], [])

    merged = parallel_dagbag.merge_shard_payloads([first, second])

    assert merged["dags"]["dup"]["fileloc"] == "/dags/dup.py"
    message = merged["import_errors"]["/dags/other/dup.py"]
    assert parallel_dagbag.DUPLICATE_ID_MARKER in message
    assert "also found in /dags/dup.py" in message


def test_merge_shard_payloads_concatenates_stats_and_dedupes_lookups() -> None:
    """Concatenate stats across shards and dedupe identical lookups across shards."""

    lookup = {"kind": "variable", "key": "v", "file": "a.py", "line": 1}
    shard_one = {
        "import_errors": {},
        "dagbag_stats": [{"file": "a.py", "duration": 1.0, "dag_num": 1, "task_num": 1}],
        "runtime_lookups": [lookup],
        "dags": {},
    }
    shard_two = {
        "import_errors": {},
        "dagbag_stats": [{"file": "b.py", "duration": 2.0, "dag_num": 1, "task_num": 1}],
        "runtime_lookups": [dict(lookup)],
        "dags": {},
    }

    merged = parallel_dagbag.merge_shard_payloads([shard_one, shard_two])

    assert merged["dagbag_stats"] == [
        {"file": "a.py", "duration": 1.0, "dag_num": 1, "task_num": 1},
        {"file": "b.py", "duration": 2.0, "dag_num": 1, "task_num": 1},
    ]
    assert merged["runtime_lookups"] == [lookup]


def test_merge_shard_payloads_of_empty_sequence() -> None:
    """Merge an empty sequence of shard payloads into an empty corpus."""

    merged = parallel_dagbag.merge_shard_payloads([])

    assert merged == {"import_errors": {}, "dagbag_stats": [], "runtime_lookups": [], "dags": {}}


def test_log_tail_reads_the_last_lines(tmp_path: Path) -> None:
    """Read a log file's content back verbatim when it is short."""

    log_path = tmp_path / "child.log"
    log_path.write_text("line one\nline two\n", encoding="utf-8")

    assert parallel_dagbag._log_tail(log_path) == "line one\nline two"


def test_log_tail_reports_a_placeholder_when_unreadable(tmp_path: Path) -> None:
    """Report a placeholder instead of raising when the log cannot be read."""

    missing = tmp_path / "missing.log"

    assert parallel_dagbag._log_tail(missing) == "<child log unavailable>"


class _FakeProcess:
    """`subprocess.Popen` double for `fan_out_dag_bag` failure-path tests."""

    def __init__(self, *, returncode: int = 0, timeout_first: bool = False) -> None:
        """Create a fake process.

        Parameters:
            returncode: int the fake process reports once it "exits".
            timeout_first: bool making the first `wait()` call raise `TimeoutExpired`.
        """

        self.pid = 999_999
        self.returncode = returncode
        self._timeout_first = timeout_first
        self._waited_once = False

    def wait(self, timeout: float = 0.0) -> int:
        """Report the configured return code, or time out once first.

        Parameters:
            timeout: float naming the caller's wait budget, echoed into the raised
                `TimeoutExpired`; `fan_out_dag_bag` always calls this with a real float.

        Returns:
            int containing `returncode`.

        Raises:
            subprocess.TimeoutExpired: `timeout_first` is set and this is the first call.
        """

        if self._timeout_first and not self._waited_once:
            self._waited_once = True
            raise subprocess.TimeoutExpired(cmd="child", timeout=timeout)
        return self.returncode


def _fake_popen(processes: list[_FakeProcess]) -> Any:
    """Build a `subprocess.Popen` double returning `processes` in call order.

    Parameters:
        processes: list[_FakeProcess] returned one per call, in order.

    Returns:
        Any callable usable as a `subprocess.Popen` monkeypatch target.
    """

    remaining = list(processes)

    def fake(command: Any, **kwargs: Any) -> _FakeProcess:
        del command, kwargs
        return remaining.pop(0)

    return fake


def test_fan_out_dag_bag_returns_empty_payload_for_no_files(tmp_path: Path) -> None:
    """Return an empty merged payload without spawning any subprocess."""

    config = _config()
    state = SimpleNamespace(root=tmp_path, logs_folder=tmp_path)

    result = parallel_dagbag.fan_out_dag_bag(
        config=config, state=state, dag_folder=tmp_path, file_paths=[], comms_needed=False
    )

    assert result == {"import_errors": {}, "dagbag_stats": [], "runtime_lookups": [], "dags": {}}


def test_fan_out_dag_bag_raises_and_kills_siblings_on_nonzero_exit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Raise `DagBagFanoutError` and kill the still-running sibling on a shard failure."""

    config = _config(workers="2")
    logs = tmp_path / "logs"
    logs.mkdir()
    state = SimpleNamespace(root=tmp_path, logs_folder=logs)
    killed: list[int] = []
    monkeypatch.setattr(
        parallel_dagbag.subprocess,
        "Popen",
        _fake_popen(
            [
                _FakeProcess(returncode=1),
                _FakeProcess(returncode=0),
            ]
        ),
    )
    monkeypatch.setattr(parallel_dagbag.os, "killpg", lambda pid, _sig: killed.append(pid))

    with pytest.raises(parallel_dagbag.DagBagFanoutError, match="exited with status 1"):
        parallel_dagbag.fan_out_dag_bag(
            config=config,
            state=state,
            dag_folder=tmp_path,
            file_paths=["a.py", "b.py"],
            comms_needed=False,
        )

    assert killed == [999_999]


def test_fan_out_dag_bag_raises_on_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Raise `DagBagFanoutError` and kill every shard when the deadline expires."""

    config = _config(workers="2", timeout="0.01")
    logs = tmp_path / "logs"
    logs.mkdir()
    state = SimpleNamespace(root=tmp_path, logs_folder=logs)
    killed: list[int] = []
    monkeypatch.setattr(
        parallel_dagbag.subprocess,
        "Popen",
        _fake_popen([_FakeProcess(timeout_first=True), _FakeProcess(returncode=0)]),
    )
    monkeypatch.setattr(parallel_dagbag.os, "killpg", lambda pid, _sig: killed.append(pid))

    with pytest.raises(parallel_dagbag.DagBagFanoutError, match="exceeded its"):
        parallel_dagbag.fan_out_dag_bag(
            config=config,
            state=state,
            dag_folder=tmp_path,
            file_paths=["a.py", "b.py"],
            comms_needed=False,
        )

    assert killed == [999_999, 999_999]


def test_fan_out_dag_bag_raises_on_undecodable_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Raise `DagBagFanoutError` when a shard's output file is missing or malformed."""

    config = _config(workers="1")
    logs = tmp_path / "logs"
    logs.mkdir()
    state = SimpleNamespace(root=tmp_path, logs_folder=logs)
    monkeypatch.setattr(
        parallel_dagbag.subprocess, "Popen", _fake_popen([_FakeProcess(returncode=0)])
    )
    monkeypatch.setattr(parallel_dagbag.os, "killpg", lambda _pid, _sig: None)
    # The fake child never writes its promised output file.

    with pytest.raises(parallel_dagbag.DagBagFanoutError, match="Could not read"):
        parallel_dagbag.fan_out_dag_bag(
            config=config,
            state=state,
            dag_folder=tmp_path,
            file_paths=["a.py"],
            comms_needed=False,
        )
