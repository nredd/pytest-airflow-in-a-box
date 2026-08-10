"""Collect an opt-in catalog of zero-boilerplate Dag smoke checks.

Every item is synthesized directly on the pytest ``Session`` rather than anchored to a real file
on disk, so the catalog carries no collection dependency on the user's project layout. Off unless
``airflow_smoke``/``--airflow-smoke`` is enabled; collection cost is zero when disabled. Explicit
file or node-ID positionals scope the run to those tests and drop the catalog; directory
positionals and arg-less runs keep it.

References:
    https://docs.pytest.org/en/stable/example/nonpython.html
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.hookspec.pytest_collection_modifyitems
    https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-serialization.html
"""

from __future__ import annotations

import contextlib
import copy
import difflib
import hashlib
import io
import json
import logging
import os
import re
import time
import warnings
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from pytest_airflow_in_a_box._compat import build_dag_bag
from pytest_airflow_in_a_box._compat.dag import _get_dag_serializer
from pytest_airflow_in_a_box.bootstrap import get_bootstrap_state
from pytest_airflow_in_a_box.fixtures.dagbag import _dag_folder

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from _pytest._code.code import TerminalRepr

LOGGER = logging.getLogger(__name__)

SMOKE_ENABLED_KEY = pytest.StashKey[bool]()
SMOKE_CORPUS_KEY = pytest.StashKey["SmokeCorpus"]()
SMOKE_CORPUS_VERSION = 1
SMOKE_CORPUS_ARTIFACT_NAME = ".airflow-smoke-corpus.json"
SMOKE_CORPUS_LOCK_NAME = ".airflow-smoke-corpus.lock"
SERIALIZED_DAG_CACHE_KEY = pytest.StashKey[dict[str, "SerializedDagEntry"]]()
DEFAULT_OWNER = "airflow"
DUPLICATE_ID_MARKER = "AirflowDagDuplicatedIdException"
RUN_DEPENDENT_SERIALIZED_DAG_KEYS = frozenset(
    {"_processor_dags_folder", "fileloc", "relative_fileloc"}
)
SERIALIZATION_TIMEOUT_FLOOR_SECONDS = 30.0


@dataclass(frozen=True)
class SmokeTask:
    """Task metadata needed by the corpus-wide policy checks."""

    task_id: str
    owner: str
    pool: str


@dataclass(frozen=True)
class SmokeDag:
    """Serialized Dag plus metadata needed without the authoring process."""

    dag_id: str
    tags: frozenset[str]
    tasks: tuple[SmokeTask, ...]
    can_be_scheduled: bool
    serialized: dict[str, Any] | None
    serialization_error: str | None
    serialization_seconds: float


@dataclass(frozen=True)
class SmokeDagFileStat:
    """Portable subset of Airflow's release-specific Dag parse statistics."""

    file: str
    duration: timedelta
    dag_num: int
    task_num: int


@dataclass(frozen=True)
class SmokeCorpus:
    """Cross-process representation of one parsed Dag folder."""

    dags: dict[str, SmokeDag]
    import_errors: dict[str, str]
    dagbag_stats: tuple[SmokeDagFileStat, ...]
    producer_pid: int
    producer_worker: str


class SlowDagParseWarning(RuntimeWarning):
    """Warn that a Dag file's parse duration crossed the slowpoke ratio."""


class SmokeCheckFailure(Exception):
    """Report a bundled smoke check failure with a preformatted message.

    Parameters:
        message: str containing the complete failure body.

    Raises:
        ValueError: The message is empty.
    """

    def __init__(self, message: str) -> None:
        if not message:
            raise ValueError("`message` must be a non-empty failure body")
        super().__init__(message)


@dataclass(frozen=True)
class SerializedDagEntry:
    """Hold one Dag's cached serialization outcome and timing.

    Parameters:
        encoded: dict[str, Any] | None containing the ``serialize_dag`` output, or ``None`` when
            serialization failed.
        error: str | None containing the serialization failure message, or ``None`` on success.
        seconds: float containing the wall-clock duration of ``serialize_dag``.

    Raises:
        ValueError: Both or neither of ``encoded`` and ``error`` are set, or ``seconds`` is
            negative.
    """

    encoded: dict[str, Any] | None
    error: str | None
    seconds: float

    def __post_init__(self) -> None:
        """Validate that the entry records exactly one outcome and a sane duration.

        Raises:
            ValueError: Both or neither of ``encoded`` and ``error`` are set, or ``seconds`` is
                negative.
        """

        if (self.encoded is None) == (self.error is None):
            raise ValueError("Exactly one of `encoded` and `error` must be set")
        if self.seconds < 0:
            raise ValueError(f"`seconds` must be non-negative: '{self.seconds}'")

    def payload(self) -> dict[str, Any]:
        """Return the serialized payload of a successful entry.

        Returns:
            dict[str, Any] containing the ``serialize_dag`` output.

        Raises:
            ValueError: The entry records a serialization failure.
        """

        if self.encoded is None:
            raise ValueError(f"Entry records a serialization failure: '{self.error}'")
        return self.encoded


def _smoke_enabled(config: pytest.Config) -> bool:
    """Resolve and cache whether the bundled smoke catalog is enabled.

    Parameters:
        config: pytest.Config containing plugin options and ini values.

    Returns:
        bool indicating whether smoke items should be collected.
    """

    if SMOKE_ENABLED_KEY in config.stash:
        return config.stash[SMOKE_ENABLED_KEY]

    option_value: object = config.getoption("airflow_smoke")
    if option_value is not None:
        enabled = bool(option_value)
    else:
        ini_value: object = config.getini("airflow_smoke")
        if not isinstance(ini_value, bool):
            raise pytest.UsageError("Ini option `airflow_smoke` must be a boolean")
        enabled = ini_value
    config.stash[SMOKE_ENABLED_KEY] = enabled
    return enabled


def _smoke_in_scope(config: pytest.Config) -> bool:
    """Report whether the run's positional selection leaves the smoke catalog in scope.

    The catalog is synthesized onto the ``Session`` after ``perform_collect`` has already
    honored positional args, so node-ID and file selection must be re-applied here: explicit
    file or node-ID positionals scope the run to those tests only, while directory positionals
    (and arg-less runs, including ``testpaths``-driven ones) keep the session-level catalog.
    Keyword and marker deselection (``-k``/``-m``/``--deselect``) need no handling -- pytest
    applies them after this plugin's ``tryfirst`` collection hook.

    Parameters:
        config: pytest.Config containing resolved positional args and invocation metadata.

    Returns:
        bool indicating whether the bundled catalog should be appended to the collection.
    """

    if config.args_source is not pytest.Config.ArgsSource.ARGS:
        return True

    invocation_dir = config.invocation_params.dir
    for arg in config.args:
        path_part, separator, _ = arg.partition("::")
        if separator:
            continue
        if (invocation_dir / path_part).is_dir():
            return True
    return False


def _parse_timeout(config: pytest.Config) -> float:
    """Read the per-file Dag parse timeout in seconds.

    Parameters:
        config: pytest.Config containing the ``airflow_dag_parse_timeout`` ini value.

    Returns:
        float containing the parse timeout in seconds.

    Raises:
        pytest.UsageError: The ini value is not a positive number.
    """

    value: object = config.getini("airflow_dag_parse_timeout")
    if not isinstance(value, str):
        raise pytest.UsageError("Ini option `airflow_dag_parse_timeout` must be a number")
    try:
        timeout = float(value)
    except ValueError as error:
        raise pytest.UsageError(
            f"Ini option `airflow_dag_parse_timeout` must be a number: '{value}'"
        ) from error
    if timeout <= 0:
        raise pytest.UsageError(
            f"Ini option `airflow_dag_parse_timeout` must be positive: '{value}'"
        )
    return timeout


def _slowpoke_ratio(config: pytest.Config) -> float:
    """Read the slowpoke warning ratio of the parse timeout.

    Parameters:
        config: pytest.Config containing the ``airflow_dag_parse_slowpoke_ratio`` ini value.

    Returns:
        float containing the ratio, in ``(0, 1]``.

    Raises:
        pytest.UsageError: The ini value is not a number in ``(0, 1]``.
    """

    value: object = config.getini("airflow_dag_parse_slowpoke_ratio")
    if not isinstance(value, str):
        raise pytest.UsageError("Ini option `airflow_dag_parse_slowpoke_ratio` must be a number")
    try:
        ratio = float(value)
    except ValueError as error:
        raise pytest.UsageError(
            f"Ini option `airflow_dag_parse_slowpoke_ratio` must be a number: '{value}'"
        ) from error
    if not (0 < ratio <= 1):
        raise pytest.UsageError(
            f"Ini option `airflow_dag_parse_slowpoke_ratio` must be in (0, 1]: '{value}'"
        )
    return ratio


def _serialization_sample_size(config: pytest.Config) -> int:
    """Read how many Dags the serialization smoke checks cover.

    Parameters:
        config: pytest.Config containing the ``airflow_serialization_sample_size`` ini value.

    Returns:
        int containing the sample size; ``0`` means every Dag.

    Raises:
        pytest.UsageError: The ini value is not a non-negative integer.
    """

    value: object = config.getini("airflow_serialization_sample_size")
    if not isinstance(value, str):
        raise pytest.UsageError(
            "Ini option `airflow_serialization_sample_size` must be a non-negative integer"
        )
    try:
        size = int(value)
    except ValueError as error:
        raise pytest.UsageError(
            f"Ini option `airflow_serialization_sample_size` must be a non-negative integer: "
            f"'{value}'"
        ) from error
    if size < 0:
        raise pytest.UsageError(
            f"Ini option `airflow_serialization_sample_size` must be a non-negative integer: "
            f"'{value}'"
        )
    return size


def _serialization_sample_seed(config: pytest.Config) -> str:
    """Read the seed selecting which Dags land in the serialization sample.

    Parameters:
        config: pytest.Config containing the ``airflow_serialization_sample_seed`` ini value.

    Returns:
        str containing the sample seed.

    Raises:
        pytest.UsageError: The ini value is not a string.
    """

    value: object = config.getini("airflow_serialization_sample_seed")
    if not isinstance(value, str):
        raise pytest.UsageError("Ini option `airflow_serialization_sample_seed` must be a string")
    return value


def _dag_id_pattern(config: pytest.Config) -> re.Pattern[str] | None:
    """Read and compile the required ``dag_id`` naming pattern.

    Parameters:
        config: pytest.Config containing the ``airflow_dag_id_pattern`` ini value.

    Returns:
        re.Pattern[str] | None containing the compiled pattern, or ``None`` when unset.

    Raises:
        pytest.UsageError: The ini value is not a string or is not a valid regex.
    """

    value: object = config.getini("airflow_dag_id_pattern")
    if not isinstance(value, str):
        raise pytest.UsageError("Ini option `airflow_dag_id_pattern` must be a string")
    if not value:
        return None
    try:
        return re.compile(value)
    except re.error as error:
        raise pytest.UsageError(
            f"Ini option `airflow_dag_id_pattern` must be a valid regex: '{value}': {error}"
        ) from error


def _required_dag_tags(config: pytest.Config) -> frozenset[str]:
    """Read the tags every collected Dag must carry.

    Parameters:
        config: pytest.Config containing the ``airflow_required_dag_tags`` ini value.

    Returns:
        frozenset[str] containing the required tags; empty when the policy is off.

    Raises:
        pytest.UsageError: The ini value is not a list of strings.
    """

    lines: object = config.getini("airflow_required_dag_tags")
    if not isinstance(lines, list) or any(not isinstance(line, str) for line in lines):
        raise pytest.UsageError("Ini option `airflow_required_dag_tags` must be a list of tags")
    return frozenset(lines)


def _forbid_default_owner(config: pytest.Config) -> bool:
    """Read whether tasks owned by Airflow's stock default owner should fail.

    Parameters:
        config: pytest.Config containing the ``airflow_forbid_default_owner`` ini value.

    Returns:
        bool indicating whether the policy is active.

    Raises:
        pytest.UsageError: The ini value is not a boolean.
    """

    value: object = config.getini("airflow_forbid_default_owner")
    if not isinstance(value, bool):
        raise pytest.UsageError("Ini option `airflow_forbid_default_owner` must be a boolean")
    return value


def _snapshot_dir(config: pytest.Config) -> Path | None:
    """Resolve the committed Dag serialization snapshot directory.

    Parameters:
        config: pytest.Config containing the ``airflow_dag_snapshot_dir`` ini value.

    Returns:
        pathlib.Path | None containing the resolved directory, or ``None`` when unset.

    Raises:
        pytest.UsageError: The ini value is not a string.
    """

    value: object = config.getini("airflow_dag_snapshot_dir")
    if not isinstance(value, str):
        raise pytest.UsageError("Ini option `airflow_dag_snapshot_dir` must be a path string")
    if not value:
        return None
    path = Path(value)
    return path if path.is_absolute() else config.rootpath / path


def _smoke_update(config: pytest.Config) -> bool:
    """Read whether the snapshot policy should regenerate rather than diff.

    Parameters:
        config: pytest.Config containing the ``--airflow-smoke-update`` option value.

    Returns:
        bool indicating whether committed snapshots should be overwritten.
    """

    return bool(config.getoption("airflow_smoke_update"))


def _normalize_serialized_dag(encoded: dict[str, Any]) -> dict[str, Any]:
    """Strip run-dependent, checkout-path-sensitive keys from a serialized Dag.

    Parameters:
        encoded: dict[str, Any] returned by ``serialize_dag``.

    Returns:
        dict[str, Any] with every run-dependent key removed.
    """

    return {
        key: value
        for key, value in encoded.items()
        if key not in RUN_DEPENDENT_SERIALIZED_DAG_KEYS
    }


def _build_smoke_corpus(config: pytest.Config) -> SmokeCorpus:
    """Parse and serialize the configured Dag folder in the elected process.

    Sets ``AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT`` immediately before construction so Airflow
    hard-kills any single file exceeding the configured timeout; the environment variable is read
    per lookup, not cached at import, so this stays safe to set late and per-run.

    Parameters:
        config: pytest.Config containing plugin options and ini values.

    Returns:
        SmokeCorpus containing portable data for every bundled smoke check.
    """

    timeout = _parse_timeout(config)
    os.environ["AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT"] = str(timeout)
    dag_bag = build_dag_bag(_dag_folder(config))
    serializer = _get_dag_serializer()
    sample_size = _serialization_sample_size(config)
    seed = _serialization_sample_seed(config)
    selected = _sampled_dag_ids(dag_bag.dags, sample_size=sample_size, seed=seed)
    if len(selected) < len(dag_bag.dags):
        LOGGER.info(
            f"Serializing a deterministic sample of {len(selected)} of {len(dag_bag.dags)} "
            f"Dags (seed '{seed}')"
        )
    selected_ids = set(selected)
    dags: dict[str, SmokeDag] = {}
    total = len(selected)
    progress = 0
    for dag_id, dag in sorted(dag_bag.dags.items()):
        serialized = None
        serialization_error = None
        serialization_seconds = 0.0
        if dag_id in selected_ids:
            progress += 1
            started = time.perf_counter()
            try:
                encoded = serializer.serialize_dag(dag)
                serialized = json.loads(json.dumps(encoded))
            except Exception as error:
                serialization_seconds = time.perf_counter() - started
                serialization_error = str(error)
                LOGGER.warning(
                    f"Dag `{dag_id}` failed to serialize after {serialization_seconds:.3f}s "
                    f"({progress}/{total}): {error}"
                )
            else:
                serialization_seconds = time.perf_counter() - started
                LOGGER.info(
                    f"Serialized Dag `{dag_id}` in {serialization_seconds:.3f}s "
                    f"({progress}/{total})"
                )
        dags[dag_id] = SmokeDag(
            dag_id=dag_id,
            tags=frozenset(dag.tags),
            tasks=tuple(
                SmokeTask(task_id=task.task_id, owner=task.owner, pool=task.pool)
                for task in dag.tasks
            ),
            can_be_scheduled=dag.timetable.can_be_scheduled,
            serialized=serialized,
            serialization_error=serialization_error,
            serialization_seconds=serialization_seconds,
        )
    stats = tuple(
        SmokeDagFileStat(
            file=stat.file,
            duration=stat.duration,
            dag_num=stat.dag_num,
            task_num=stat.task_num,
        )
        for stat in dag_bag.dagbag_stats
    )
    return SmokeCorpus(
        dags=dags,
        import_errors=dict(dag_bag.import_errors),
        dagbag_stats=stats,
        producer_pid=os.getpid(),
        producer_worker=os.environ.get("PYTEST_XDIST_WORKER", "master"),
    )


def _smoke_corpus_payload(corpus: SmokeCorpus) -> dict[str, Any]:
    """Encode one smoke corpus as JSON-compatible primitives.

    Parameters:
        corpus: SmokeCorpus produced from the configured Dag folder.

    Returns:
        dict[str, Any] containing the versioned artifact payload.
    """

    return {
        "version": SMOKE_CORPUS_VERSION,
        "producer_pid": corpus.producer_pid,
        "producer_worker": corpus.producer_worker,
        "import_errors": corpus.import_errors,
        "dagbag_stats": [
            {
                "file": stat.file,
                "duration": stat.duration.total_seconds(),
                "dag_num": stat.dag_num,
                "task_num": stat.task_num,
            }
            for stat in corpus.dagbag_stats
        ],
        "dags": {
            dag_id: {
                "tags": sorted(dag.tags),
                "tasks": [
                    {"task_id": task.task_id, "owner": task.owner, "pool": task.pool}
                    for task in dag.tasks
                ],
                "can_be_scheduled": dag.can_be_scheduled,
                "serialized": dag.serialized,
                "serialization_error": dag.serialization_error,
                "serialization_seconds": dag.serialization_seconds,
            }
            for dag_id, dag in corpus.dags.items()
        },
    }


def _smoke_corpus_from_payload(payload: dict[str, Any]) -> SmokeCorpus:
    """Decode one versioned smoke corpus artifact.

    Parameters:
        payload: dict[str, Any] loaded from the shared JSON artifact.

    Returns:
        SmokeCorpus reconstructed for the current worker.

    Raises:
        ValueError: The artifact schema version is unsupported.
    """

    version = payload.get("version")
    if version != SMOKE_CORPUS_VERSION:
        raise ValueError(f"Unsupported smoke corpus version: '{version}'")
    return SmokeCorpus(
        dags={
            dag_id: SmokeDag(
                dag_id=dag_id,
                tags=frozenset(value["tags"]),
                tasks=tuple(SmokeTask(**task) for task in value["tasks"]),
                can_be_scheduled=value["can_be_scheduled"],
                serialized=value["serialized"],
                serialization_error=value["serialization_error"],
                serialization_seconds=value["serialization_seconds"],
            )
            for dag_id, value in payload["dags"].items()
        },
        import_errors=payload["import_errors"],
        dagbag_stats=tuple(
            SmokeDagFileStat(
                file=stat["file"],
                duration=timedelta(seconds=stat["duration"]),
                dag_num=stat["dag_num"],
                task_num=stat["task_num"],
            )
            for stat in payload["dagbag_stats"]
        ),
        producer_pid=payload["producer_pid"],
        producer_worker=payload["producer_worker"],
    )


def _write_smoke_corpus(path: Path, corpus: SmokeCorpus) -> None:
    """Atomically publish one complete shared smoke corpus artifact.

    Parameters:
        path: pathlib.Path naming the final artifact.
        corpus: SmokeCorpus to serialize.
    """

    temporary = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(_smoke_corpus_payload(corpus), sort_keys=True), encoding="utf-8"
        )
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _shared_smoke_corpus(config: pytest.Config) -> SmokeCorpus:
    """Elect one process to parse Dags and share the result with local workers.

    Parameters:
        config: pytest.Config carrying the shared bootstrap run root.

    Returns:
        SmokeCorpus loaded from or published to the shared run root.
    """

    # Deferred because Airflow only supports POSIX platforms and the plugin's
    # import-light entry point remains useful to platform-independent tooling.
    import fcntl

    root = get_bootstrap_state(config).root
    artifact = root / SMOKE_CORPUS_ARTIFACT_NAME
    lock_path = root / SMOKE_CORPUS_LOCK_NAME
    with lock_path.open("ab") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            if artifact.is_file():
                payload = json.loads(artifact.read_text(encoding="utf-8"))
                return _smoke_corpus_from_payload(payload)
            corpus = _build_smoke_corpus(config)
            _write_smoke_corpus(artifact, corpus)
            LOGGER.info(
                f"Worker `{corpus.producer_worker}` PID {corpus.producer_pid} "
                f"published shared smoke corpus '{artifact}'"
            )
            return corpus
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _smoke_corpus(session: pytest.Session, config: pytest.Config) -> SmokeCorpus:
    """Load and process-cache the shared smoke corpus for one worker session.

    Parameters:
        session: pytest.Session used to cache the decoded corpus.
        config: pytest.Config carrying the shared bootstrap run root.

    Returns:
        SmokeCorpus containing every parsed Dag's portable smoke-check data.
    """

    if SMOKE_CORPUS_KEY not in session.stash:
        session.stash[SMOKE_CORPUS_KEY] = _shared_smoke_corpus(config)
    return session.stash[SMOKE_CORPUS_KEY]


def _corpus_timeout(config: pytest.Config) -> float:
    """Scale the per-file parse timeout by the Dag folder's file count.

    Parameters:
        config: pytest.Config containing plugin options and ini values.

    Returns:
        float containing the corpus-wide item timeout in seconds.
    """

    timeout = _parse_timeout(config)
    folder = _dag_folder(config)
    file_count = max(1, len(list(folder.rglob("*.py")))) if folder.is_dir() else 1
    return timeout * file_count


def _serialization_timeout(config: pytest.Config) -> float:
    """Bound the serialization roundtrip item without inheriting a tiny parse timeout.

    The corpus-scaled parse deadline is the only pre-parse proxy for corpus size, but a user
    tuning ``airflow_dag_parse_timeout`` down for a fast parse check must not starve the
    serialization pass, whose cost scales with tasks rather than files.

    Parameters:
        config: pytest.Config containing plugin options and ini values.

    Returns:
        float containing the roundtrip item timeout in seconds.
    """

    return max(SERIALIZATION_TIMEOUT_FLOOR_SECONDS, _corpus_timeout(config))


def _smoke_item_timeout(config: pytest.Config) -> float:
    """Bound an item that may become the shared corpus producer.

    Parameters:
        config: pytest.Config containing plugin options and ini values.

    Returns:
        float containing the combined parse and serialization budget in seconds.
    """

    return _corpus_timeout(config) + _serialization_timeout(config)


def _sampled_dag_ids(dag_ids: Iterable[str], *, sample_size: int, seed: str) -> list[str]:
    """Select a deterministic, seed-keyed sample of Dag identifiers.

    Ranks every ``dag_id`` by the SHA-256 digest of ``f"{seed}:{dag_id}"`` and keeps the first
    ``sample_size``, so the selection is reproducible across runs, platforms, and processes.

    Parameters:
        dag_ids: Iterable[str] containing every discovered Dag identifier.
        sample_size: int containing the number of Dags to keep; ``0`` keeps every Dag.
        seed: str keying which Dags land in the sample.

    Returns:
        list[str] containing the selected identifiers in lexical order.

    Raises:
        ValueError: The sample size is negative.
    """

    if sample_size < 0:
        raise ValueError(f"`sample_size` must be non-negative: '{sample_size}'")
    unique_ids = sorted(set(dag_ids))
    if sample_size == 0 or sample_size >= len(unique_ids):
        return unique_ids

    def _rank(dag_id: str) -> str:
        return hashlib.sha256(f"{seed}:{dag_id}".encode()).hexdigest()

    return sorted(sorted(unique_ids, key=_rank)[:sample_size])


def _smoke_dag_bag(session: pytest.Session, config: pytest.Config) -> SmokeCorpus:
    """Return the process-cached portable Dag corpus.

    Parameters:
        session: pytest.Session used to cache the decoded corpus.
        config: pytest.Config carrying the shared bootstrap run root.

    Returns:
        SmokeCorpus containing every parsed Dag's portable smoke-check data.
    """

    return _smoke_corpus(session, config)


def _serialized_dag_cache(
    session: pytest.Session, config: pytest.Config
) -> dict[str, SerializedDagEntry]:
    """Expose the producer's sampled serialization results to every smoke item.

    Portable Dags were serialized by the process that published the shared corpus. Authoring Dag
    test doubles are serialized here so this helper remains independently testable.

    Parameters:
        session: pytest.Session used to cache the serialized corpus.
        config: pytest.Config containing plugin options and ini values.

    Returns:
        dict[str, SerializedDagEntry] mapping each selected ``dag_id`` to its outcome.
    """

    if SERIALIZED_DAG_CACHE_KEY in session.stash:
        return session.stash[SERIALIZED_DAG_CACHE_KEY]

    dag_bag = _smoke_dag_bag(session, config)
    sample_size = _serialization_sample_size(config)
    seed = _serialization_sample_seed(config)
    selected = _sampled_dag_ids(dag_bag.dags, sample_size=sample_size, seed=seed)
    if len(selected) < len(dag_bag.dags):
        LOGGER.info(
            f"Serializing a deterministic sample of {len(selected)} of {len(dag_bag.dags)} "
            f"Dags (seed '{seed}')"
        )

    serialized_dag_class: Any | None = None
    entries: dict[str, SerializedDagEntry] = {}
    total = len(selected)
    for index, dag_id in enumerate(selected, start=1):
        dag = dag_bag.dags[dag_id]
        if isinstance(dag, SmokeDag):
            entries[dag_id] = SerializedDagEntry(
                encoded=dag.serialized,
                error=dag.serialization_error,
                seconds=dag.serialization_seconds,
            )
            continue
        if serialized_dag_class is None:
            serialized_dag_class = _get_dag_serializer()
        started = time.perf_counter()
        try:
            encoded = serialized_dag_class.serialize_dag(dag)
        except Exception as error:
            seconds = time.perf_counter() - started
            LOGGER.warning(
                f"Dag `{dag_id}` failed to serialize after {seconds:.3f}s "
                f"({index}/{total}): {error}"
            )
            entries[dag_id] = SerializedDagEntry(encoded=None, error=str(error), seconds=seconds)
            continue
        seconds = time.perf_counter() - started
        LOGGER.info(f"Serialized Dag `{dag_id}` in {seconds:.3f}s ({index}/{total})")
        entries[dag_id] = SerializedDagEntry(encoded=encoded, error=None, seconds=seconds)

    session.stash[SERIALIZED_DAG_CACHE_KEY] = entries
    return entries


def _validate_smoke_options(config: pytest.Config) -> None:
    """Reject smoke option combinations that would silently narrow coverage.

    Parameters:
        config: pytest.Config containing plugin options and ini values.

    Raises:
        pytest.UsageError: Snapshot update mode is combined with serialization sampling, which
            would regenerate only a subset of the committed snapshots.
    """

    if (
        _smoke_update(config)
        and _snapshot_dir(config) is not None
        and _serialization_sample_size(config) > 0
    ):
        raise pytest.UsageError(
            "`--airflow-smoke-update` cannot be combined with "
            "`airflow_serialization_sample_size`; regenerating snapshots from a sample would "
            "silently drop the unsampled Dags"
        )


def _log_stats_table(dag_bag: Any, *, timeout: float, ratio: float) -> str:
    """Render and log a slowest-first table of every parsed Dag file.

    Parameters:
        dag_bag: Any containing portable Dag parse statistics.
        timeout: float containing the hard per-file parse timeout in seconds.
        ratio: float containing the slowpoke warning ratio of the timeout.

    Returns:
        str containing the rendered table text.
    """

    from airflow.cli.simple_table import AirflowConsole

    threshold = ratio * timeout

    def _status(seconds: float) -> str:
        if seconds > timeout:
            return f"SLOWPOKE (>{timeout:.1f}s timeout)"
        if seconds > threshold:
            return f"SLOWPOKE (>{ratio:.0%} of {timeout:.1f}s)"
        return "ok"

    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        AirflowConsole(width=100).print_as(
            data=dag_bag.dagbag_stats,
            output="table",
            mapper=lambda stat: {
                "file": stat.file,
                "dags": stat.dag_num,
                "tasks": stat.task_num,
                "duration": f"{stat.duration.total_seconds():.3f}s",
                "status": _status(stat.duration.total_seconds()),
            },
        )
    text = buffer.getvalue()
    LOGGER.info(f"Dag bag parse report:\n{text}")
    return text


def _log_serialization_table(
    entries: dict[str, SerializedDagEntry],
    deserialize_seconds: dict[str, float],
    failed_ids: frozenset[str],
) -> str:
    """Render and log a slowest-first table of every serialized Dag.

    Parameters:
        entries: dict[str, SerializedDagEntry] mapping each ``dag_id`` to its serialization
            outcome and timing.
        deserialize_seconds: dict[str, float] mapping each round-tripped ``dag_id`` to its
            deserialization duration; Dags absent from the mapping render a ``-`` column.
        failed_ids: frozenset[str] containing every ``dag_id`` that failed either round-trip
            stage.

    Returns:
        str containing the rendered table text.
    """

    from airflow.cli.simple_table import AirflowConsole

    def _total(dag_id: str) -> float:
        return entries[dag_id].seconds + deserialize_seconds.get(dag_id, 0.0)

    rows = sorted(entries, key=_total, reverse=True)

    def _mapper(dag_id: str) -> dict[str, str]:
        entry = entries[dag_id]
        decode = deserialize_seconds.get(dag_id)
        return {
            "dag_id": dag_id,
            "serialize": f"{entry.seconds:.3f}s",
            "deserialize": "-" if decode is None else f"{decode:.3f}s",
            "total": f"{_total(dag_id):.3f}s",
            "status": "FAILED" if dag_id in failed_ids else "ok",
        }

    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        AirflowConsole(width=100).print_as(data=rows, output="table", mapper=_mapper)
    text = buffer.getvalue()
    LOGGER.info(f"Dag serialization report:\n{text}")
    return text


class DagBagIntegrityItem(pytest.Item):
    """Fail on Dag import errors and per-file parse timeouts; warn on slowpokes."""

    def __init__(self, *, name: str, parent: SmokeCollector) -> None:
        """Create the item and mark it as a metadata-database smoke test.

        Parameters:
            name: str containing the pytest item name.
            parent: SmokeCollector that collected this item.
        """

        super().__init__(name=name, parent=parent)
        self.add_marker(pytest.mark.smoke)
        self.add_marker(pytest.mark.timeout(_smoke_item_timeout(self.config)))
        self.add_marker(pytest.mark.db_test)

    def runtest(self) -> None:
        """Parse the configured Dag folder and enforce timeout and import-error policy.

        Raises:
            SmokeCheckFailure: Any file failed to import or exceeded the parse timeout.
        """

        timeout = _parse_timeout(self.config)
        ratio = _slowpoke_ratio(self.config)
        dag_bag = _smoke_corpus(self.session, self.config)
        table = _log_stats_table(dag_bag, timeout=timeout, ratio=ratio)

        failures: list[str] = []
        if dag_bag.import_errors:
            for path, message in sorted(dag_bag.import_errors.items()):
                failures.append(f"Dag file import check failed: '{path}'\n{message.rstrip()}")

        slowpokes: list[str] = []
        for stat in dag_bag.dagbag_stats:
            seconds = stat.duration.total_seconds()
            if seconds > timeout:
                failures.append(
                    f"Dag file '{stat.file}' took {seconds:.3f}s, exceeding the "
                    f"{timeout:.1f}s parse timeout"
                )
            elif seconds > ratio * timeout:
                slowpokes.append(stat.file)

        for slowpoke in slowpokes:
            warnings.warn(
                SlowDagParseWarning(
                    f"Dag file '{slowpoke}' exceeded {ratio:.0%} of the {timeout:.1f}s "
                    "parse timeout"
                ),
                stacklevel=1,
            )

        if failures:
            raise SmokeCheckFailure("\n\n".join(failures) + f"\n\n{table}")

    def repr_failure(
        self,
        excinfo: pytest.ExceptionInfo[BaseException],
        style: object = None,
    ) -> str | TerminalRepr:
        """Format smoke check failures without a pytest-internal traceback.

        Parameters:
            excinfo: pytest.ExceptionInfo[BaseException] describing the failure.
            style: object containing an unused traceback style override.

        Returns:
            str | TerminalRepr containing the failure representation.
        """

        del style
        if isinstance(excinfo.value, SmokeCheckFailure):
            return str(excinfo.value)
        return super().repr_failure(excinfo)

    def reportinfo(self) -> tuple[str, int, str]:
        """Locate this item for terminal and junit reporting.

        Returns:
            tuple[str, int, str] containing path, line, and title.
        """

        return self.nodeid, 0, self.name


class DagSerializationRoundtripItem(pytest.Item):
    """Round-trip the serialized Dag corpus through Airflow's scheduler serialization."""

    def __init__(self, *, name: str, parent: SmokeCollector) -> None:
        """Create the item and mark it as a bundled smoke test.

        Parameters:
            name: str containing the pytest item name.
            parent: SmokeCollector that collected this item.
        """

        super().__init__(name=name, parent=parent)
        self.add_marker(pytest.mark.smoke)
        self.add_marker(pytest.mark.timeout(_smoke_item_timeout(self.config)))

    def runtest(self) -> None:
        """Round-trip every cached serialized Dag and log a slowest-first timing table.

        Raises:
            SmokeCheckFailure: Any Dag failed to round-trip through serialization.
        """

        entries = _serialized_dag_cache(self.session, self.config)
        serialized_dag_class = _get_dag_serializer()
        failures: list[str] = []
        failed_ids: set[str] = set()
        deserialize_seconds: dict[str, float] = {}
        total = len(entries)
        for index, (dag_id, entry) in enumerate(sorted(entries.items()), start=1):
            if entry.error is not None:
                failures.append(
                    f"Dag `{dag_id}` failed to round-trip through serialization: {entry.error}"
                )
                failed_ids.add(dag_id)
                continue
            started = time.perf_counter()
            try:
                # Deserialization mutates its input, so the shared cache gets a copy.
                serialized_dag_class.deserialize_dag(copy.deepcopy(entry.payload()))
            except Exception as error:
                deserialize_seconds[dag_id] = time.perf_counter() - started
                failures.append(
                    f"Dag `{dag_id}` failed to round-trip through serialization: {error}"
                )
                failed_ids.add(dag_id)
                continue
            deserialize_seconds[dag_id] = time.perf_counter() - started
            LOGGER.info(
                f"Round-tripped Dag `{dag_id}` in {deserialize_seconds[dag_id]:.3f}s "
                f"({index}/{total})"
            )
        table = _log_serialization_table(entries, deserialize_seconds, frozenset(failed_ids))
        if failures:
            raise SmokeCheckFailure("\n\n".join(failures) + f"\n\n{table}")

    def repr_failure(
        self,
        excinfo: pytest.ExceptionInfo[BaseException],
        style: object = None,
    ) -> str | TerminalRepr:
        """Format smoke check failures without a pytest-internal traceback.

        Parameters:
            excinfo: pytest.ExceptionInfo[BaseException] describing the failure.
            style: object containing an unused traceback style override.

        Returns:
            str | TerminalRepr containing the failure representation.
        """

        del style
        if isinstance(excinfo.value, SmokeCheckFailure):
            return str(excinfo.value)
        return super().repr_failure(excinfo)

    def reportinfo(self) -> tuple[str, int, str]:
        """Locate this item for terminal and junit reporting.

        Returns:
            tuple[str, int, str] containing path, line, and title.
        """

        return self.nodeid, 0, self.name


class NoDuplicateDagIdsItem(pytest.Item):
    """Fail when two Dag files declare the same ``dag_id``."""

    def __init__(self, *, name: str, parent: SmokeCollector) -> None:
        """Create the item and mark it as a bundled smoke test.

        Parameters:
            name: str containing the pytest item name.
            parent: SmokeCollector that collected this item.
        """

        super().__init__(name=name, parent=parent)
        self.add_marker(pytest.mark.smoke)
        self.add_marker(pytest.mark.timeout(_smoke_item_timeout(self.config)))

    def runtest(self) -> None:
        """Surface every duplicated ``dag_id`` collision the Dag bag recorded.

        Raises:
            SmokeCheckFailure: Any Dag file was dropped for duplicating a ``dag_id``.
        """

        dag_bag = _smoke_corpus(self.session, self.config)
        failures = [
            f"'{path}': {message}"
            for path, message in sorted(dag_bag.import_errors.items())
            if DUPLICATE_ID_MARKER in message
        ]
        if failures:
            raise SmokeCheckFailure("\n\n".join(failures))

    def reportinfo(self) -> tuple[str, int, str]:
        """Locate this item for terminal and junit reporting.

        Returns:
            tuple[str, int, str] containing path, line, and title.
        """

        return self.nodeid, 0, self.name


class ScheduleSanityItem(pytest.Item):
    """Fail when a scheduled Dag's timetable cannot compute its next run."""

    def __init__(self, *, name: str, parent: SmokeCollector) -> None:
        """Create the item and mark it as a bundled smoke test.

        Parameters:
            name: str containing the pytest item name.
            parent: SmokeCollector that collected this item.
        """

        super().__init__(name=name, parent=parent)
        self.add_marker(pytest.mark.smoke)
        self.add_marker(pytest.mark.timeout(_smoke_item_timeout(self.config)))

    def runtest(self) -> None:
        """Compute the next scheduled run for every scheduled Dag in the serialized cache.

        Dags missing from a sampled cache, or whose serialization already failed, are skipped;
        serialization failures belong to ``test_dag_serialization_roundtrip``.

        Raises:
            SmokeCheckFailure: A scheduled Dag's timetable raised while computing its next run.
        """

        from airflow.timetables.base import TimeRestriction

        dag_bag = _smoke_dag_bag(self.session, self.config)
        entries = _serialized_dag_cache(self.session, self.config)
        serialized_dag_class = _get_dag_serializer()
        failures: list[str] = []
        for dag_id, dag in sorted(dag_bag.dags.items()):
            can_be_scheduled = (
                dag.can_be_scheduled
                if isinstance(dag, SmokeDag)
                else dag.timetable.can_be_scheduled
            )
            if not can_be_scheduled:
                continue
            entry = entries.get(dag_id)
            if entry is None or entry.error is not None:
                continue
            try:
                # Deserialization mutates its input, so the shared cache gets a copy.
                decoded = serialized_dag_class.deserialize_dag(copy.deepcopy(entry.payload()))
                restriction = TimeRestriction(
                    earliest=decoded.start_date,
                    latest=decoded.end_date,
                    catchup=decoded.catchup,
                )
                decoded.timetable.next_dagrun_info(
                    last_automated_data_interval=None,
                    restriction=restriction,
                )
            except Exception as error:
                failures.append(
                    f"Dag `{dag_id}` could not compute its next scheduled run: {error}"
                )
        if failures:
            raise SmokeCheckFailure("\n\n".join(failures))

    def reportinfo(self) -> tuple[str, int, str]:
        """Locate this item for terminal and junit reporting.

        Returns:
            tuple[str, int, str] containing path, line, and title.
        """

        return self.nodeid, 0, self.name


class PoolReferencesExistItem(pytest.Item):
    """Fail when a task references a pool absent from the metadata database."""

    def __init__(self, *, name: str, parent: SmokeCollector) -> None:
        """Create the item and mark it as a metadata-database smoke test.

        Parameters:
            name: str containing the pytest item name.
            parent: SmokeCollector that collected this item.
        """

        super().__init__(name=name, parent=parent)
        self.add_marker(pytest.mark.smoke)
        self.add_marker(pytest.mark.timeout(_smoke_item_timeout(self.config)))
        self.add_marker(pytest.mark.db_test)

    def runtest(self) -> None:
        """Resolve every task's declared pool against the metadata database.

        Raises:
            SmokeCheckFailure: A task references a pool the database does not contain.
        """

        # Deferred to preserve bootstrap safety and avoid Airflow's module import cost.
        from airflow.models.pool import Pool
        from airflow.utils.session import create_session

        dag_bag = _smoke_corpus(self.session, self.config)
        failures: list[str] = []
        with create_session() as database_session:
            known = {pool.pool for pool in Pool.get_pools(session=database_session)}
            for dag_id, dag in sorted(dag_bag.dags.items()):
                for task in dag.tasks:
                    if task.pool not in known:
                        failures.append(
                            f"Dag `{dag_id}` task `{task.task_id}` references unknown pool "
                            f"`{task.pool}`"
                        )
        if failures:
            raise SmokeCheckFailure("\n\n".join(failures))

    def reportinfo(self) -> tuple[str, int, str]:
        """Locate this item for terminal and junit reporting.

        Returns:
            tuple[str, int, str] containing path, line, and title.
        """

        return self.nodeid, 0, self.name


class DagIdPatternItem(pytest.Item):
    """Fail when a ``dag_id`` does not match the configured naming pattern."""

    def __init__(self, *, name: str, parent: SmokeCollector, pattern: re.Pattern[str]) -> None:
        """Create the item and mark it as a bundled smoke test.

        Parameters:
            name: str containing the pytest item name.
            parent: SmokeCollector that collected this item.
            pattern: re.Pattern[str] every ``dag_id`` must match.
        """

        super().__init__(name=name, parent=parent)
        self.pattern = pattern
        self.add_marker(pytest.mark.smoke)
        self.add_marker(pytest.mark.timeout(_smoke_item_timeout(self.config)))

    def runtest(self) -> None:
        """Match every ``dag_id`` against the configured pattern.

        Raises:
            SmokeCheckFailure: A ``dag_id`` does not match the configured pattern.
        """

        dag_bag = _smoke_corpus(self.session, self.config)
        failures = [
            f"Dag id `{dag_id}` does not match pattern `{self.pattern.pattern}`"
            for dag_id in sorted(dag_bag.dags)
            if not self.pattern.search(dag_id)
        ]
        if failures:
            raise SmokeCheckFailure("\n\n".join(failures))

    def reportinfo(self) -> tuple[str, int, str]:
        """Locate this item for terminal and junit reporting.

        Returns:
            tuple[str, int, str] containing path, line, and title.
        """

        return self.nodeid, 0, self.name


class RequiredDagTagsItem(pytest.Item):
    """Fail when a Dag is missing a required tag."""

    def __init__(self, *, name: str, parent: SmokeCollector, tags: frozenset[str]) -> None:
        """Create the item and mark it as a bundled smoke test.

        Parameters:
            name: str containing the pytest item name.
            parent: SmokeCollector that collected this item.
            tags: frozenset[str] every Dag must carry.
        """

        super().__init__(name=name, parent=parent)
        self.tags = tags
        self.add_marker(pytest.mark.smoke)
        self.add_marker(pytest.mark.timeout(_smoke_item_timeout(self.config)))

    def runtest(self) -> None:
        """Check every Dag's tags for the required set.

        Raises:
            SmokeCheckFailure: A Dag is missing one or more required tags.
        """

        dag_bag = _smoke_corpus(self.session, self.config)
        failures: list[str] = []
        for dag_id, dag in sorted(dag_bag.dags.items()):
            missing = self.tags - dag.tags
            if missing:
                failures.append(f"Dag `{dag_id}` is missing required tags: {sorted(missing)}")
        if failures:
            raise SmokeCheckFailure("\n\n".join(failures))

    def reportinfo(self) -> tuple[str, int, str]:
        """Locate this item for terminal and junit reporting.

        Returns:
            tuple[str, int, str] containing path, line, and title.
        """

        return self.nodeid, 0, self.name


class ForbidDefaultOwnerItem(pytest.Item):
    """Fail when a task is owned by Airflow's stock default owner."""

    def __init__(self, *, name: str, parent: SmokeCollector) -> None:
        """Create the item and mark it as a bundled smoke test.

        Parameters:
            name: str containing the pytest item name.
            parent: SmokeCollector that collected this item.
        """

        super().__init__(name=name, parent=parent)
        self.add_marker(pytest.mark.smoke)
        self.add_marker(pytest.mark.timeout(_smoke_item_timeout(self.config)))

    def runtest(self) -> None:
        """Check every task's owner against Airflow's stock default.

        Raises:
            SmokeCheckFailure: A task is owned by the stock `airflow` owner.
        """

        dag_bag = _smoke_corpus(self.session, self.config)
        failures: list[str] = []
        for dag_id, dag in sorted(dag_bag.dags.items()):
            for task in dag.tasks:
                if task.owner == DEFAULT_OWNER:
                    failures.append(
                        f"Dag `{dag_id}` task `{task.task_id}` is owned by the stock "
                        f"`{DEFAULT_OWNER}` owner"
                    )
        if failures:
            raise SmokeCheckFailure("\n\n".join(failures))

    def reportinfo(self) -> tuple[str, int, str]:
        """Locate this item for terminal and junit reporting.

        Returns:
            tuple[str, int, str] containing path, line, and title.
        """

        return self.nodeid, 0, self.name


class SerializedDagSnapshotItem(pytest.Item):
    """Diff every Dag's serialized structure against a committed snapshot file."""

    def __init__(
        self, *, name: str, parent: SmokeCollector, snapshot_dir: Path, update: bool
    ) -> None:
        """Create the item and mark it as a bundled smoke test.

        Parameters:
            name: str containing the pytest item name.
            parent: SmokeCollector that collected this item.
            snapshot_dir: pathlib.Path containing the committed snapshot directory.
            update: bool indicating whether to regenerate rather than diff snapshots.
        """

        super().__init__(name=name, parent=parent)
        self.snapshot_dir = snapshot_dir
        self.update = update
        self.add_marker(pytest.mark.smoke)
        self.add_marker(pytest.mark.timeout(_smoke_item_timeout(self.config)))

    def runtest(self) -> None:
        """Diff, or regenerate, every cached serialized Dag's normalized snapshot.

        Raises:
            SmokeCheckFailure: A Dag failed to serialize, has no committed snapshot, or its
                serialized structure drifted from the committed snapshot.
        """

        entries = _serialized_dag_cache(self.session, self.config)
        failures: list[str] = []
        for dag_id, entry in sorted(entries.items()):
            if entry.error is not None:
                failures.append(f"Dag `{dag_id}` failed to serialize: {entry.error}")
                continue
            current = (
                json.dumps(
                    _normalize_serialized_dag(entry.payload()),
                    indent=2,
                    sort_keys=True,
                )
                + "\n"
            )

            snapshot_path = self.snapshot_dir / f"{dag_id}.json"
            if self.update:
                self.snapshot_dir.mkdir(parents=True, exist_ok=True)
                snapshot_path.write_text(current, encoding="utf-8")
                continue

            if not snapshot_path.is_file():
                failures.append(
                    f"Dag `{dag_id}` has no committed snapshot at '{snapshot_path}'; "
                    "regenerate with `--airflow-smoke-update`"
                )
                continue

            committed = snapshot_path.read_text(encoding="utf-8")
            if committed != current:
                diff = "".join(
                    difflib.unified_diff(
                        committed.splitlines(keepends=True),
                        current.splitlines(keepends=True),
                        fromfile=f"{dag_id}.json (committed)",
                        tofile=f"{dag_id}.json (current)",
                    )
                )
                failures.append(f"Dag `{dag_id}` drifted from its committed snapshot:\n{diff}")

        if failures:
            raise SmokeCheckFailure("\n\n".join(failures))

    def reportinfo(self) -> tuple[str, int, str]:
        """Locate this item for terminal and junit reporting.

        Returns:
            tuple[str, int, str] containing path, line, and title.
        """

        return self.nodeid, 0, self.name


class SmokeCollector(pytest.Collector):
    """Collect the bundled smoke catalog directly on the pytest session."""

    def collect(self) -> Iterator[pytest.Item]:
        """Yield every enabled bundled smoke item.

        Returns:
            Iterator[pytest.Item] containing the collected smoke items.
        """

        yield DagBagIntegrityItem.from_parent(self, name="test_dag_bag_integrity")
        yield DagSerializationRoundtripItem.from_parent(
            self, name="test_dag_serialization_roundtrip"
        )
        yield NoDuplicateDagIdsItem.from_parent(self, name="test_no_duplicate_dag_ids")
        yield ScheduleSanityItem.from_parent(self, name="test_schedule_sanity")
        yield PoolReferencesExistItem.from_parent(self, name="test_pool_references_exist")

        pattern = _dag_id_pattern(self.config)
        if pattern is not None:
            yield DagIdPatternItem.from_parent(
                self,
                name="test_dag_id_pattern",
                pattern=pattern,
            )
        required_tags = _required_dag_tags(self.config)
        if required_tags:
            yield RequiredDagTagsItem.from_parent(
                self,
                name="test_required_dag_tags",
                tags=required_tags,
            )
        if _forbid_default_owner(self.config):
            yield ForbidDefaultOwnerItem.from_parent(self, name="test_forbid_default_owner")
        snapshot_dir = _snapshot_dir(self.config)
        if snapshot_dir is not None:
            yield SerializedDagSnapshotItem.from_parent(
                self,
                name="test_dag_serialization_snapshot",
                snapshot_dir=snapshot_dir,
                update=_smoke_update(self.config),
            )


def collect_smoke_items(
    session: pytest.Session, config: pytest.Config, items: list[pytest.Item]
) -> None:
    """Append the bundled smoke catalog to collected items when enabled and in scope.

    Parameters:
        session: pytest.Session that owns the synthetic smoke collector.
        config: pytest.Config containing plugin options and ini values.
        items: list[pytest.Item] mutated to include enabled smoke items.

    Raises:
        pytest.UsageError: The enabled smoke options form an invalid combination.
    """

    if not _smoke_enabled(config):
        return
    _validate_smoke_options(config)
    if not _smoke_in_scope(config):
        LOGGER.info(f"Skipping smoke catalog: positional selection {config.args} excludes it")
        return
    collector = SmokeCollector.from_parent(session, name="smoke")
    items.extend(collector.collect())


__all__ = (
    "DagBagIntegrityItem",
    "DagIdPatternItem",
    "DagSerializationRoundtripItem",
    "ForbidDefaultOwnerItem",
    "NoDuplicateDagIdsItem",
    "PoolReferencesExistItem",
    "RequiredDagTagsItem",
    "ScheduleSanityItem",
    "SerializedDagEntry",
    "SerializedDagSnapshotItem",
    "SlowDagParseWarning",
    "SmokeCheckFailure",
    "SmokeCollector",
    "collect_smoke_items",
)
