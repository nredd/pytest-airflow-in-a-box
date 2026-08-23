"""Fan a smoke corpus's whole-Dag-folder parse out across subprocess workers.

Only `smoke.py`'s corpus builder consumes this module. The `dag_bag` fixture's public
contract is the real Airflow ``DagBag`` class -- there is no serialization format that
hands back something still type-checking and behaving as that class across a process
boundary, so fan-out never touches it (issue #243 discusses this at length).

A single walk of the true configured Dag folder (`_compat.list_dag_file_paths`) is the
only place ``.airflowignore`` is ever consulted, so partitioning its already-correct
output across subprocess workers cannot lose or misapply an ancestor ignore file the way
re-rooting a sub-parse at a subdirectory would. Each worker imports only its shard's
files (`_compat.build_partial_dag_bag`); this module owns sharding, the wire format each
worker writes, merging the shards back into one whole-corpus payload -- including
reproducing Airflow's own same-``dag_id`` collision behavior across shard boundaries --
and the subprocess orchestration itself.

References:
    https://github.com/nredd/pytest-airflow-in-a-box/issues/243
"""

from __future__ import annotations

import contextlib
import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

import pytest

from pytest_airflow_in_a_box._compat.dag import _get_dag_serializer
from pytest_airflow_in_a_box._compat.introspection import mapped_expansion

if TYPE_CHECKING:
    from collections.abc import Sequence


@runtime_checkable
class RunRoot(Protocol):
    """The subset of `bootstrap.BootstrapState` `fan_out_dag_bag` actually reads.

    Read-only members on purpose: `BootstrapState` is a frozen dataclass, so a
    read-write protocol (the default for a bare attribute annotation) would reject it
    as structurally incompatible.
    """

    @property
    def root(self) -> Path: ...
    @property
    def logs_folder(self) -> Path: ...


# The exact marker `smoke.NoDuplicateDagIdsItem` filters `import_errors` messages for.
# Owned here, not in `smoke.py`, because this module is the one that has to synthesize a
# collision Airflow's own `DagBag.bag_dag` never sees (each shard's Dag bag is a separate
# instance, so a same-`dag_id` collision spanning two shards is invisible to either one).
DUPLICATE_ID_MARKER = "AirflowDagDuplicatedIdException"

SHARD_SPEC_PATH_ENVIRONMENT_VARIABLE = "PYTEST_AIRFLOW_IN_A_BOX_DAGBAG_SHARD_SPEC_PATH"
SHARD_OUTPUT_PATH_ENVIRONMENT_VARIABLE = "PYTEST_AIRFLOW_IN_A_BOX_DAGBAG_SHARD_OUTPUT_PATH"

_NORMAL_EXIT_STATUS = 0
_FANOUT_SCRATCH_DIRNAME = "dagbag-fanout"
_LOG_TAIL_LINES = 50


class DagBagFanoutError(RuntimeError):
    """Report that fanning a Dag bag parse out across subprocesses did not succeed."""


@dataclass(frozen=True)
class ShardSpec:
    """One subprocess worker's assignment.

    Parameters:
        dag_folder: str naming the Dag corpus's true root.
        file_paths: tuple[str, ...] naming this shard's files.
        needs_comms: bool indicating whether parse-time secrets resolution is active.
        dagbag_import_timeout: float bounding a single file's import in the child.
        sys_path: tuple[str, ...] containing the parent's `sys.path` at fan-out time --
            a fresh child interpreter's own `sys.path` has none of the rootdir/conftest/
            `pythonpath`-ini entries pytest inserted into the parent's, without which a
            Dag file importing a sibling package (a common repo layout) would fail in
            the child while succeeding in a serial parse.
        serialize: bool indicating whether any still-collected smoke item needs a
            serialized Dag; `False` skips the Dag serializer entirely.
    """

    dag_folder: str
    file_paths: tuple[str, ...]
    needs_comms: bool
    dagbag_import_timeout: float
    sys_path: tuple[str, ...]
    serialize: bool

    def to_payload(self) -> dict[str, Any]:
        """Serialize to JSON-compatible primitives.

        Returns:
            dict[str, Any] containing every shard spec field.
        """

        return {
            "dag_folder": self.dag_folder,
            "file_paths": list(self.file_paths),
            "needs_comms": self.needs_comms,
            "dagbag_import_timeout": self.dagbag_import_timeout,
            "sys_path": list(self.sys_path),
            "serialize": self.serialize,
        }


def shard_from_payload(payload: dict[str, Any]) -> ShardSpec:
    """Reconstruct a shard spec written by the parent process.

    Parameters:
        payload: dict[str, Any] decoded from the shard spec JSON file.

    Returns:
        ShardSpec reconstructed from `payload`.
    """

    return ShardSpec(
        dag_folder=payload["dag_folder"],
        file_paths=tuple(payload["file_paths"]),
        needs_comms=payload["needs_comms"],
        dagbag_import_timeout=payload["dagbag_import_timeout"],
        sys_path=tuple(payload["sys_path"]),
        serialize=payload["serialize"],
    )


def shard_file_paths(file_paths: Sequence[str], worker_count: int) -> list[list[str]]:
    """Distribute a discovered file list across `worker_count` shards, round robin.

    Round robin, not contiguous blocks: files Airflow's walk discovers together tend to
    cluster by directory, and a contiguous block over a sorted list would concentrate
    one team's uniformly slow subdirectory onto a single shard.

    Parameters:
        file_paths: Sequence[str] containing every discovered file, from
            `_compat.list_dag_file_paths`.
        worker_count: int naming how many shards to build.

    Returns:
        list[list[str]] containing `worker_count` shards, some possibly empty, in
        round-robin order over a stably sorted `file_paths`.

    Raises:
        ValueError: `worker_count` is not positive.
    """

    if worker_count < 1:
        raise ValueError(f"`worker_count` must be positive: '{worker_count}'")
    shards: list[list[str]] = [[] for _ in range(worker_count)]
    for index, file_path in enumerate(sorted(file_paths)):
        shards[index % worker_count].append(file_path)
    return shards


def _cpu_count() -> int:
    """Detect logical CPUs with a conservative single-connection fallback.

    Returns:
        int containing at least one logical CPU.
    """

    detected = os.cpu_count()
    return detected if detected is not None and detected > 0 else 1


def fanout_enabled(config: pytest.Config) -> bool:
    """Resolve whether Dag bag fan-out is enabled, with no filesystem access.

    Public and cheap on purpose: callers should check this before doing any Dag file
    discovery, so the default (disabled) case never pays for a walk it will not use.

    Parameters:
        config: pytest.Config containing plugin options and ini values.

    Returns:
        bool indicating whether fan-out should be attempted.

    Raises:
        pytest.UsageError: The ini value is not a boolean.
    """

    option_value: object = config.getoption("airflow_dag_bag_fanout")
    if option_value is not None:
        return bool(option_value)
    ini_value: object = config.getini("airflow_dag_bag_fanout")
    if not isinstance(ini_value, bool):
        raise pytest.UsageError("Ini option `airflow_dag_bag_fanout` must be a boolean")
    return ini_value


def _positive_number_option(config: pytest.Config, name: str) -> float:
    """Read one ini option that must be a strictly positive number.

    Parameters:
        config: pytest.Config containing the named ini value.
        name: str naming the ini option.

    Returns:
        float containing the validated value.

    Raises:
        pytest.UsageError: The ini value is not a positive number.
    """

    value: object = config.getini(name)
    if not isinstance(value, str):
        raise pytest.UsageError(f"Ini option `{name}` must be a number")
    try:
        parsed = float(value)
    except ValueError as error:
        raise pytest.UsageError(f"Ini option `{name}` must be a number: '{value}'") from error
    if parsed <= 0:
        raise pytest.UsageError(f"Ini option `{name}` must be positive: '{value}'")
    return parsed


def _non_negative_int_option(config: pytest.Config, name: str) -> int:
    """Read one ini option that must be a non-negative integer.

    Parameters:
        config: pytest.Config containing the named ini value.
        name: str naming the ini option.

    Returns:
        int containing the validated value.

    Raises:
        pytest.UsageError: The ini value is not a non-negative integer.
    """

    value: object = config.getini(name)
    if not isinstance(value, str):
        raise pytest.UsageError(f"Ini option `{name}` must be a non-negative integer")
    try:
        parsed = int(value)
    except ValueError as error:
        raise pytest.UsageError(
            f"Ini option `{name}` must be a non-negative integer: '{value}'"
        ) from error
    if parsed < 0:
        raise pytest.UsageError(f"Ini option `{name}` must be a non-negative integer: '{value}'")
    return parsed


def should_fan_out(file_count: int, config: pytest.Config) -> bool:
    """Decide whether a discovered file count should be fanned out.

    Parameters:
        file_count: int naming how many files `_compat.list_dag_file_paths` discovered.
        config: pytest.Config containing plugin options and ini values.

    Returns:
        bool indicating whether `fan_out_dag_bag` should be attempted.
    """

    if not fanout_enabled(config):
        return False
    return file_count >= _non_negative_int_option(config, "airflow_dag_bag_fanout_min_files")


def resolved_worker_count(config: pytest.Config, file_count: int) -> int:
    """Resolve how many subprocess workers a fan-out run should use.

    Parameters:
        config: pytest.Config containing the `airflow_dag_bag_fanout_workers` ini value.
        file_count: int bounding the worker count to at most one worker per file.

    Returns:
        int containing at least one worker, never more than `file_count`.
    """

    configured = _non_negative_int_option(config, "airflow_dag_bag_fanout_workers")
    workers = configured or min(_cpu_count(), max(file_count, 1))
    return max(1, min(workers, file_count) if file_count > 0 else 1)


def _fanout_timeout(config: pytest.Config) -> float:
    """Read the whole fan-out run's timeout in seconds.

    Parameters:
        config: pytest.Config containing the `airflow_dag_bag_fanout_timeout` ini value.

    Returns:
        float containing the timeout in seconds.
    """

    return _positive_number_option(config, "airflow_dag_bag_fanout_timeout")


def dagbag_import_timeout(config: pytest.Config) -> float:
    """Read the per-file Dag parse timeout a fan-out child enforces.

    Parameters:
        config: pytest.Config containing the `airflow_dag_parse_timeout` ini value.

    Returns:
        float containing the parse timeout in seconds.
    """

    return _positive_number_option(config, "airflow_dag_parse_timeout")


def _encode_task(task: Any) -> dict[str, Any]:
    """Encode one live operator's portable smoke-check metadata.

    Mirrors `smoke.py`'s own task shape exactly, since a fan-out shard's payload must
    decode through the identical `smoke._smoke_corpus_from_payload`.

    Parameters:
        task: Any containing a live Airflow operator.

    Returns:
        dict[str, Any] containing the task's portable metadata.
    """

    is_mapped, over_runtime_data, cap = mapped_expansion(task)
    return {
        "task_id": task.task_id,
        "owner": task.owner,
        "pool": task.pool,
        "is_mapped": is_mapped,
        "mapped_over_runtime_data": over_runtime_data,
        "max_active_tis_per_dag": cap,
    }


def _encode_dag(dag: Any, *, serializer: Any | None) -> dict[str, Any]:
    """Encode one live Dag's portable smoke-check metadata, including serialization.

    Every fan-out shard serializes every Dag it parses whenever serialization is needed
    at all: fan-out only ever serializes when `airflow_serialization_sample_size` is `0`
    (serialize every Dag), since a seed-keyed sample needs the whole corpus's `dag_id`
    set, which no single shard has. `serializer=None` skips serialization entirely,
    matching the serial path's own `_smoke_serialization_needed` short-circuit -- fan-out
    still parallelizes the (usually dominant) import cost even when no still-collected
    smoke item needs a serialized Dag.

    Parameters:
        dag: Any containing the live parsed Dag.
        serializer: Any | None containing the resolved Dag serializer class, or `None`
            when no still-collected smoke item needs a serialized Dag.

    Returns:
        dict[str, Any] matching `smoke._smoke_corpus_payload`'s `dags` entry shape.
    """

    serialized: dict[str, Any] | None = None
    serialization_error: str | None = None
    serialization_seconds = 0.0
    if serializer is not None:
        started = time.perf_counter()
        try:
            encoded = serializer.serialize_dag(dag)
            serialized = json.loads(json.dumps(encoded))
        except Exception as error:
            serialization_error = str(error)
        serialization_seconds = time.perf_counter() - started
    return {
        "tags": sorted(dag.tags),
        "tasks": [_encode_task(task) for task in dag.tasks],
        "can_be_scheduled": dag.timetable.can_be_scheduled,
        "catchup": bool(getattr(dag, "catchup", False)),
        "fileloc": str(getattr(dag, "fileloc", "")),
        "serialized": serialized,
        "serialization_error": serialization_error,
        "serialization_seconds": serialization_seconds,
    }


def encode_shard(
    dags: dict[str, Any],
    import_errors: dict[str, str],
    stats: Sequence[Any],
    runtime_lookups: Sequence[Any],
    *,
    serialize: bool,
) -> dict[str, Any]:
    """Encode one shard's parse results as the JSON-compatible payload a child writes.

    Parameters:
        dags: dict[str, Any] mapping `dag_id` to live parsed Dag, from
            `_compat.build_partial_dag_bag`.
        import_errors: dict[str, str] mapping file path to import failure message.
        stats: Sequence[Any] containing one `_compat.DagBagShardStat`-shaped entry per
            shard file.
        runtime_lookups: Sequence[Any] containing this shard's deduplicated
            `_compat.introspection.SecretsLookup`-shaped secrets lookups.
        serialize: bool indicating whether any still-collected smoke item needs a
            serialized Dag (`smoke._smoke_serialization_needed`); `False` skips
            resolving and calling the Dag serializer entirely, matching the serial
            path's own short-circuit.

    Returns:
        dict[str, Any] containing the shard's portable payload.
    """

    serializer = _get_dag_serializer() if serialize and dags else None
    return {
        "import_errors": dict(import_errors),
        "dagbag_stats": [
            {
                "file": stat.file,
                "duration": stat.duration.total_seconds(),
                "dag_num": stat.dag_num,
                "task_num": stat.task_num,
            }
            for stat in stats
        ],
        "runtime_lookups": [
            {"kind": lookup.kind, "key": lookup.key, "file": lookup.file, "line": lookup.line}
            for lookup in runtime_lookups
        ],
        "dags": {
            dag_id: _encode_dag(dag, serializer=serializer) for dag_id, dag in sorted(dags.items())
        },
    }


def merge_shard_payloads(payloads: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """Merge every shard's encoded payload into one whole-corpus payload.

    Reproduces Airflow's own `DagBag.bag_dag` collision behavior across shards: the
    first-seen `dag_id` wins, and every later shard's colliding entry is dropped with a
    synthesized `DUPLICATE_ID_MARKER` import-errors entry, so
    `smoke.NoDuplicateDagIdsItem` -- which only ever filters `import_errors` for that
    marker string -- keeps working unmodified. `dagbag_stats` concatenates (files are
    disjoint across shards by construction), preserving `smoke.DagParseBudgetItem`'s
    whole-corpus median. `runtime_lookups` dedupes across shards the same way each child
    already dedupes within its own shard.

    Parameters:
        payloads: Sequence[dict[str, Any]] containing every shard's `encode_shard`
            output, in shard order.

    Returns:
        dict[str, Any] containing the merged `import_errors`/`dagbag_stats`/
        `runtime_lookups`/`dags`, shaped for `smoke._smoke_corpus_from_payload` once the
        caller adds `version`/`producer_pid`/`producer_worker`.
    """

    merged_dags: dict[str, dict[str, Any]] = {}
    merged_import_errors: dict[str, str] = {}
    merged_stats: list[dict[str, Any]] = []
    seen_lookups: set[tuple[Any, ...]] = set()
    merged_lookups: list[dict[str, Any]] = []
    for payload in payloads:
        merged_import_errors.update(payload["import_errors"])
        merged_stats.extend(payload["dagbag_stats"])
        for lookup in payload["runtime_lookups"]:
            key = (lookup["kind"], lookup["key"], lookup["file"], lookup["line"])
            if key not in seen_lookups:
                seen_lookups.add(key)
                merged_lookups.append(lookup)
        for dag_id, encoded in payload["dags"].items():
            existing = merged_dags.get(dag_id)
            if existing is None:
                merged_dags[dag_id] = encoded
                continue
            losing_fileloc = encoded["fileloc"]
            winning_fileloc = existing["fileloc"]
            # Message shape matches AirflowDagDuplicatedIdException's own `__str__`,
            # prefixed the same way `DagBag.process_file` prefixes any bagging
            # exception it catches (`f"{type(e).__name__}: {e}"`) -- byte-identical
            # formatting to what a single-process parse would have written. WHICH
            # file lands as winner vs. loser is NOT guaranteed to match a serial
            # parse's choice: shards merge in shard-index order here, which tracks
            # each file's shard assignment, not its original discovery position, so a
            # file discovered earlier can still merge later than a file discovered
            # after it (e.g. worker_count=2, files f0..f3, shard 0 = [f0, f2], shard 1
            # = [f1, f3] -- f2 merges before f1 despite f1 being discovered first).
            # Deliberately not "fixed" by preserving discovery order end to end: doing
            # so would mean either abandoning round-robin sharding (a real
            # load-balancing loss -- see `shard_file_paths`) or tracking each file's
            # global discovery index through to merge time purely to pick a winner
            # that is otherwise inert -- the check that consumes this message
            # (`smoke.NoDuplicateDagIdsItem`) only greps for `DUPLICATE_ID_MARKER`, so
            # both files are equally "the fix" from the user's side regardless of
            # which one is named loser. Both paths promise only that a collision is
            # detected and named, never a specific winner.
            merged_import_errors[losing_fileloc] = (
                f"{DUPLICATE_ID_MARKER}: Ignoring DAG {dag_id} from {losing_fileloc} "
                f"- also found in {winning_fileloc}"
            )
    # Airflow's own `collect_dags()` ends with `sorted(stats, key=..., reverse=True)`;
    # `smoke._log_stats_table` renders `dagbag_stats` as a slowest-first report without
    # re-sorting, so concatenation order alone (shard, then per-shard processing order)
    # would silently break that ordering on a merged corpus.
    merged_stats.sort(key=lambda stat: stat["duration"], reverse=True)
    return {
        "import_errors": merged_import_errors,
        "dagbag_stats": merged_stats,
        "runtime_lookups": merged_lookups,
        "dags": merged_dags,
    }


def _log_tail(path: Path) -> str:
    """Read the last captured child output lines for a failure message.

    Parameters:
        path: pathlib.Path containing the child's combined output log.

    Returns:
        str containing up to the last fifty log lines, or a placeholder when the log
        cannot be read.
    """

    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return "<child log unavailable>"
    return "\n".join(lines[-_LOG_TAIL_LINES:])


def _kill_all(processes: Sequence[subprocess.Popen[bytes]]) -> None:
    """Kill and reap every still-running shard process.

    Skips any process whose `returncode` is already set: `Popen.wait()` reaps on
    success, freeing that PID for OS reuse, so signaling it again would target
    whatever unrelated process (a sibling shard is itself a `start_new_session=True`
    process-group leader) the OS has since assigned that PID to.

    Parameters:
        processes: Sequence[subprocess.Popen[bytes]] naming the shard processes.
    """

    running = [process for process in processes if process.returncode is None]
    for process in running:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
    for process in running:
        process.wait()


def fan_out_dag_bag(
    *,
    config: pytest.Config,
    state: RunRoot,
    dag_folder: Path,
    file_paths: Sequence[str],
    comms_needed: bool,
    serialize: bool,
) -> dict[str, Any]:
    """Fan a whole-corpus Dag parse out across subprocess workers and merge the result.

    Shards `file_paths` -- already discovered by the caller from `dag_folder`, the true
    configured root, so `.airflowignore` resolution is identical to a serial parse --
    across `resolved_worker_count` subprocess workers and merges their results. Callers
    must already have ensured the metadata database is migrated (via `ensure_database`)
    when `comms_needed` is `True`, exactly as the serial path does today: letting each
    child race its own migration would corrupt a fresh SQLite database.

    Each shard inherits the parent's `sys.path` (rootdir/conftest/`pythonpath`-ini
    entries a fresh child interpreter would not otherwise have) and runs with
    `config.rootpath` as its working directory, matching `isolated.py`'s own child-
    process convention -- without this a Dag file importing a sibling package under a
    typical `dags/` + shared-package repo layout would import cleanly in a serial parse
    and fail only under fan-out.

    Parameters:
        config: pytest.Config containing plugin options and ini values.
        state: RunRoot providing the run root and log directory.
        dag_folder: pathlib.Path naming the Dag corpus's true root.
        file_paths: Sequence[str] naming every file to shard, from
            `_compat.list_dag_file_paths(dag_folder)`.
        comms_needed: bool indicating whether parse-time secrets resolution is active.
        serialize: bool indicating whether any still-collected smoke item needs a
            serialized Dag (`smoke._smoke_serialization_needed`).

    Returns:
        dict[str, Any] containing the merged payload, shaped for
        `smoke._smoke_corpus_from_payload` once the caller adds
        `version`/`producer_pid`/`producer_worker`.

    Raises:
        DagBagFanoutError: A shard could not be spawned, or a spawned child process
            failed, timed out, or wrote an undecodable output.
    """

    if not file_paths:
        return merge_shard_payloads([])
    worker_count = resolved_worker_count(config, len(file_paths))
    shards = [shard for shard in shard_file_paths(file_paths, worker_count) if shard]

    scratch = state.root / _FANOUT_SCRATCH_DIRNAME
    scratch.mkdir(parents=True, exist_ok=True)
    import_timeout = dagbag_import_timeout(config)
    parent_sys_path = tuple(sys.path)
    processes: list[tuple[subprocess.Popen[bytes], Path, Path]] = []
    env = os.environ.copy()
    deadline = time.monotonic() + _fanout_timeout(config)
    try:
        for index, shard in enumerate(shards):
            spec = ShardSpec(
                dag_folder=str(dag_folder),
                file_paths=tuple(shard),
                needs_comms=comms_needed,
                dagbag_import_timeout=import_timeout,
                sys_path=parent_sys_path,
                serialize=serialize,
            )
            spec_path = scratch / f"shard-{index}-spec.json"
            output_path = scratch / f"shard-{index}-output.json"
            output_path.unlink(missing_ok=True)
            spec_path.write_text(json.dumps(spec.to_payload()), encoding="utf-8")
            shard_env = env.copy()
            shard_env[SHARD_SPEC_PATH_ENVIRONMENT_VARIABLE] = str(spec_path)
            shard_env[SHARD_OUTPUT_PATH_ENVIRONMENT_VARIABLE] = str(output_path)
            log_path = state.logs_folder / f"dagbag-fanout-shard-{index}-{os.getpid()}.log"
            with log_path.open("wb") as log_stream:
                process = subprocess.Popen(
                    [sys.executable, "-m", "pytest_airflow_in_a_box.parallel_dagbag_child"],
                    stdout=log_stream,
                    stderr=subprocess.STDOUT,
                    env=shard_env,
                    cwd=config.rootpath,
                    start_new_session=True,
                )
            processes.append((process, output_path, log_path))
    except OSError as error:
        _kill_all([item[0] for item in processes])
        raise DagBagFanoutError(f"Could not spawn a Dag bag fan-out shard: {error}") from error

    payloads: list[dict[str, Any]] = []
    for process, output_path, log_path in processes:
        remaining = max(0.0, deadline - time.monotonic())
        try:
            process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            _kill_all([item[0] for item in processes])
            raise DagBagFanoutError(
                f"Dag bag fan-out exceeded its {_fanout_timeout(config)}s timeout"
            ) from None
        if process.returncode != _NORMAL_EXIT_STATUS:
            _kill_all([item[0] for item in processes])
            raise DagBagFanoutError(
                f"A Dag bag fan-out shard exited with status {process.returncode}; last "
                f"output:\n{_log_tail(log_path)}"
            )
        try:
            payloads.append(json.loads(output_path.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError) as error:
            _kill_all([item[0] for item in processes])
            raise DagBagFanoutError(
                f"Could not read Dag bag fan-out shard output '{output_path}': {error}"
            ) from error
    return merge_shard_payloads(payloads)


__all__ = (
    "DUPLICATE_ID_MARKER",
    "SHARD_OUTPUT_PATH_ENVIRONMENT_VARIABLE",
    "SHARD_SPEC_PATH_ENVIRONMENT_VARIABLE",
    "DagBagFanoutError",
    "RunRoot",
    "ShardSpec",
    "dagbag_import_timeout",
    "encode_shard",
    "fan_out_dag_bag",
    "fanout_enabled",
    "merge_shard_payloads",
    "resolved_worker_count",
    "shard_file_paths",
    "shard_from_payload",
    "should_fan_out",
)
