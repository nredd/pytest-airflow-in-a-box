"""Build and process-cache the portable, fan-out-eligible Dag corpus.

Shared foundation for the public `dag_corpus` fixture and the bundled `smoke.py`
catalog's corpus-wide checks: both consume the exact same parsed, optionally serialized
Dag folder, built once per worker process and shared with local xdist workers through a
flock-guarded JSON artifact (`get_dag_corpus`). Dag parsing optionally fans out across
subprocess workers (`parallel_dagbag.fan_out_dag_bag`) for large corpora.

Whether the builder calls the Airflow Dag serializer at all is decided by
`_corpus_serialization_needed`: a run with at least one `dag_corpus` consumer always
serializes every Dag, since there is no cheap way to know a test body's field usage before
it runs. Absent a `dag_corpus` consumer, the bundled smoke catalog's own
`airflow_smoke_disable`/`airflow_dag_snapshot_dir`-driven need decides instead, when the
catalog is enabled and in scope; a run with neither serializes everything, the safe default.

References:
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.Session
    https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-serialization.html
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from pytest_airflow_in_a_box._compat import build_dag_bag, ensure_database, list_dag_file_paths
from pytest_airflow_in_a_box._compat.dag import _get_dag_serializer
from pytest_airflow_in_a_box._compat.introspection import (
    SecretsLookup,
    mapped_expansion,
    record_secrets_lookups,
)
from pytest_airflow_in_a_box.bootstrap import get_bootstrap_state
from pytest_airflow_in_a_box.fixtures.dagbag import (
    LIVE_DAG_BAG_KEY,
    LIVE_DAG_BAG_LOOKUPS_KEY,
    _dag_folder,
)
from pytest_airflow_in_a_box.parallel_dagbag import (
    DagBagFanoutError,
    fan_out_dag_bag,
    fanout_enabled,
    should_fan_out,
)
from pytest_airflow_in_a_box.parse_secrets import parse_time_comms

if TYPE_CHECKING:
    from collections.abc import Iterable

LOGGER = logging.getLogger(__name__)

DAG_CORPUS_KEY = pytest.StashKey["DagCorpus"]()
DAG_CORPUS_VERSION = 2
DAG_CORPUS_ARTIFACT_NAME = ".airflow-dag-corpus.json"
DAG_CORPUS_LOCK_NAME = ".airflow-dag-corpus.lock"
# Stashed by `plugin.py`'s collection hook whenever at least one collected item survives
# with a `dag_corpus` fixture dependency; read by `_corpus_serialization_needed` so a bare
# `dag_corpus` request always gets a fully serialized corpus, independent of whatever the
# bundled smoke catalog's own ini knobs happen to be set to.
DAG_CORPUS_WANTS_SERIALIZATION_KEY = pytest.StashKey[bool]()


@dataclass(frozen=True)
class CorpusTask:
    """Task metadata needed by corpus-wide policy checks.

    Parameters:
        task_id: str containing the task identifier.
        owner: str containing the task owner.
        pool: str containing the task's pool name.
        is_mapped: bool indicating the task is dynamically mapped.
        mapped_over_runtime_data: bool indicating a mapped task expands over runtime data
            (XCom or task output) rather than literals.
        max_active_tis_per_dag: int | None containing the mapped concurrency cap when set.
    """

    task_id: str
    owner: str
    pool: str
    is_mapped: bool
    mapped_over_runtime_data: bool
    max_active_tis_per_dag: int | None


@dataclass(frozen=True)
class CorpusDagFileStat:
    """Portable subset of Airflow's release-specific Dag parse statistics."""

    file: str
    duration: timedelta
    dag_num: int
    task_num: int


@dataclass(frozen=True)
class CorpusDag:
    """Serialized Dag plus metadata needed without the authoring process."""

    dag_id: str
    tags: frozenset[str]
    tasks: tuple[CorpusTask, ...]
    can_be_scheduled: bool
    catchup: bool
    fileloc: str
    serialized: dict[str, Any] | None
    serialization_error: str | None
    serialization_seconds: float


@dataclass(frozen=True)
class DagCorpus:
    """Cross-process representation of one parsed Dag folder.

    Shared by the public `dag_corpus` fixture and the bundled smoke catalog's corpus-wide
    checks -- both consume the exact same parse, built and cached once per worker process
    (see `get_dag_corpus`). ``runtime_lookups`` is deliberately tri-state: ``None`` means
    the producer reused a ``DagBag`` that `dag_bag` had already parsed without
    interception, so runtime secrets findings are unavailable; an empty tuple means the
    parse was observed and no lookup happened.
    """

    dags: dict[str, CorpusDag]
    import_errors: dict[str, str]
    dagbag_stats: tuple[CorpusDagFileStat, ...]
    runtime_lookups: tuple[SecretsLookup, ...] | None
    producer_pid: int
    producer_worker: str


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


def mark_dag_corpus_requested(config: pytest.Config) -> None:
    """Record that at least one collected item this run consumes `dag_corpus`.

    Called from `plugin.py`'s collection hook once per run, alongside the `dag_corpus`
    xdist-grouping scan, whenever a surviving item requires `dag_corpus`.
    `_corpus_serialization_needed` reads the stashed marker so a bare `dag_corpus` request
    always gets a fully serialized corpus, independent of the bundled smoke catalog's own
    ini-driven serialization need.

    Parameters:
        config: pytest.Config receiving the request marker on its stash.
    """

    config.stash[DAG_CORPUS_WANTS_SERIALIZATION_KEY] = True


def _corpus_serialization_needed(config: pytest.Config) -> bool:
    """Report whether the corpus builder must call the Airflow Dag serializer.

    A run with at least one `dag_corpus` consumer always serializes every Dag -- there is
    no cheap way to know in advance which fields a test body's assertions read. Absent
    that, when the bundled smoke catalog is enabled and in scope, its own
    `smoke._smoke_serialization_needed` decides, exactly as before this predicate existed.
    A run with neither builds a corpus with every Dag serialized, the safe default a
    `dag_bag`-equivalent parse would also produce.

    Parameters:
        config: pytest.Config containing plugin options, ini values, and the
            `DAG_CORPUS_WANTS_SERIALIZATION_KEY` request marker.

    Returns:
        bool indicating whether the corpus builder must call the Dag serializer.
    """

    if config.stash.get(DAG_CORPUS_WANTS_SERIALIZATION_KEY, False):
        return True
    # Deferred: smoke.py imports this module at load time, so a module-level import here
    # would cycle. Mirrors `fixtures/dagbag.py::_cached_dag_bag`'s own deferred import of
    # smoke internals, for the same reason.
    from pytest_airflow_in_a_box.smoke import (
        _smoke_enabled,
        _smoke_in_scope,
        _smoke_serialization_needed,
    )

    if _smoke_enabled(config) and _smoke_in_scope(config):
        return _smoke_serialization_needed(config)
    return True


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


def _select_serialization_sample(config: pytest.Config, dag_ids: Iterable[str]) -> list[str]:
    """Select the Dag IDs to serialize, honoring `_corpus_serialization_needed`.

    Skips sampling (and any Airflow DAG serializer call downstream) entirely once
    `_corpus_serialization_needed` reports nothing still needs a serialized Dag.

    Parameters:
        config: pytest.Config containing plugin options and ini values.
        dag_ids: Iterable[str] containing every discovered Dag identifier.

    Returns:
        list[str] containing the selected Dag IDs; empty when serialization is not needed.
    """

    if not _corpus_serialization_needed(config):
        return []
    dag_ids = list(dag_ids)
    sample_size = _serialization_sample_size(config)
    seed = _serialization_sample_seed(config)
    selected = _sampled_dag_ids(dag_ids, sample_size=sample_size, seed=seed)
    if len(selected) < len(dag_ids):
        LOGGER.info(
            f"Serializing a deterministic sample of {len(selected)} of {len(dag_ids)} "
            f"Dags (seed '{seed}')"
        )
    return selected


def _corpus_task(task: Any) -> CorpusTask:
    """Capture one live operator's portable corpus metadata.

    Parameters:
        task: Any containing a live Airflow operator.

    Returns:
        CorpusTask containing the captured task metadata.
    """

    is_mapped, over_runtime_data, cap = mapped_expansion(task)
    return CorpusTask(
        task_id=task.task_id,
        owner=task.owner,
        pool=task.pool,
        is_mapped=is_mapped,
        mapped_over_runtime_data=over_runtime_data,
        max_active_tis_per_dag=cap,
    )


def _build_dag_corpus(session: pytest.Session, config: pytest.Config) -> DagCorpus:
    """Parse and serialize the configured Dag folder in the elected process.

    Sets ``AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT`` immediately before construction so Airflow
    hard-kills any single file exceeding the configured timeout; the environment variable is read
    per lookup, not cached at import, so this stays safe to set late and per-run. Reuses a
    DagBag `dag_bag` already parsed for this process, if one exists, rather than parsing
    the Dag folder a second time. A fresh parse here is deliberately not cached on the session
    the way `dag_bag`'s is -- nothing else in a corpus-only run needs the live DagBag past
    this function returning, only the portable DagCorpus it builds from it.

    Parameters:
        session: pytest.Session used to reach a DagBag already parsed by `dag_bag`.
        config: pytest.Config containing plugin options and ini values.

    Returns:
        DagCorpus containing portable data for every consumer: the public `dag_corpus`
        fixture and the bundled smoke catalog alike.
    """

    timeout = _parse_timeout(config)
    os.environ["AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT"] = str(timeout)
    runtime_lookups: tuple[SecretsLookup, ...] | None = None
    if LIVE_DAG_BAG_KEY in session.stash:
        dag_bag = session.stash[LIVE_DAG_BAG_KEY]
        # `_cached_dag_bag` records lookups during its own parse whenever the catalog is
        # enabled, so reuse normally preserves runtime findings; `None` (uninstrumented)
        # survives only for a bag parsed outside that path.
        runtime_lookups = session.stash.get(LIVE_DAG_BAG_LOOKUPS_KEY, None)
    else:
        dag_folder = _dag_folder(config)
        comms = parse_time_comms(config)
        if comms is not None:
            # Several `dag_corpus`/smoke consumers are deliberately not `db_test`, so
            # nothing has initialized the database by the time they run. A Dag with a
            # top-level Variable or Connection lookup would otherwise charge the whole
            # one-time migration to this item's parse timeout, which budgets for parsing
            # and serialization alone. `--airflow-parse-secrets=off` keeps the build
            # database-free.
            ensure_database(get_bootstrap_state(config).root)
        # `fanout_enabled` is checked first and is filesystem-free, so the default
        # (disabled) case never pays for a Dag file walk it will not use. A seed-keyed
        # serialization sample needs the whole corpus's `dag_id` set, which no single
        # fan-out shard can see, so a nonzero sample size blocks fan-out entirely (a
        # shard cannot decide on its own whether one of its Dags falls inside the
        # sample) -- this is unrelated to whether serialization is needed at all, which
        # `serialize=_corpus_serialization_needed(config)` below decides independently:
        # even at the default sample size (0, "serialize everything"), a smoke-only run
        # with `airflow_smoke_disable` covering every serialization-consuming item means
        # nothing needs a serialized Dag, and fan-out still parallelizes the (usually
        # dominant) import cost without ever resolving or calling the Dag serializer.
        if fanout_enabled(config) and _serialization_sample_size(config) == 0:
            file_paths = list_dag_file_paths(dag_folder)
            if should_fan_out(len(file_paths), config):
                try:
                    payload = fan_out_dag_bag(
                        config=config,
                        state=get_bootstrap_state(config),
                        dag_folder=dag_folder,
                        file_paths=file_paths,
                        comms_needed=comms is not None,
                        serialize=_corpus_serialization_needed(config),
                    )
                except DagBagFanoutError as error:
                    LOGGER.warning(
                        f"Dag bag fan-out failed, falling back to a single-process parse: {error}"
                    )
                else:
                    return _dag_corpus_from_payload(
                        {
                            **payload,
                            "version": DAG_CORPUS_VERSION,
                            "producer_pid": os.getpid(),
                            "producer_worker": os.environ.get("PYTEST_XDIST_WORKER", "master"),
                        }
                    )
        with record_secrets_lookups(dag_folder) as recorded:
            dag_bag = build_dag_bag(dag_folder, comms=comms)
        runtime_lookups = tuple(dict.fromkeys(recorded))
    serializer: Any | None = None
    selected = _select_serialization_sample(config, dag_bag.dags)
    selected_ids = set(selected)
    dags: dict[str, CorpusDag] = {}
    total = len(selected)
    progress = 0
    for dag_id, dag in sorted(dag_bag.dags.items()):
        serialized = None
        serialization_error = None
        serialization_seconds = 0.0
        if dag_id in selected_ids:
            if serializer is None:
                serializer = _get_dag_serializer()
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
        dags[dag_id] = CorpusDag(
            dag_id=dag_id,
            tags=frozenset(dag.tags),
            tasks=tuple(_corpus_task(task) for task in dag.tasks),
            can_be_scheduled=dag.timetable.can_be_scheduled,
            catchup=bool(getattr(dag, "catchup", False)),
            fileloc=str(getattr(dag, "fileloc", "")),
            serialized=serialized,
            serialization_error=serialization_error,
            serialization_seconds=serialization_seconds,
        )
    stats = tuple(
        CorpusDagFileStat(
            file=stat.file,
            duration=stat.duration,
            dag_num=stat.dag_num,
            task_num=stat.task_num,
        )
        for stat in dag_bag.dagbag_stats
    )
    return DagCorpus(
        dags=dags,
        import_errors=dict(dag_bag.import_errors),
        dagbag_stats=stats,
        runtime_lookups=runtime_lookups,
        producer_pid=os.getpid(),
        producer_worker=os.environ.get("PYTEST_XDIST_WORKER", "master"),
    )


def _dag_corpus_payload(corpus: DagCorpus) -> dict[str, Any]:
    """Encode one Dag corpus as JSON-compatible primitives.

    Parameters:
        corpus: DagCorpus produced from the configured Dag folder.

    Returns:
        dict[str, Any] containing the versioned artifact payload.
    """

    return {
        "version": DAG_CORPUS_VERSION,
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
        "runtime_lookups": None
        if corpus.runtime_lookups is None
        else [
            {
                "kind": lookup.kind,
                "key": lookup.key,
                "file": lookup.file,
                "line": lookup.line,
            }
            for lookup in corpus.runtime_lookups
        ],
        "dags": {
            dag_id: {
                "tags": sorted(dag.tags),
                "tasks": [
                    {
                        "task_id": task.task_id,
                        "owner": task.owner,
                        "pool": task.pool,
                        "is_mapped": task.is_mapped,
                        "mapped_over_runtime_data": task.mapped_over_runtime_data,
                        "max_active_tis_per_dag": task.max_active_tis_per_dag,
                    }
                    for task in dag.tasks
                ],
                "can_be_scheduled": dag.can_be_scheduled,
                "catchup": dag.catchup,
                "fileloc": dag.fileloc,
                "serialized": dag.serialized,
                "serialization_error": dag.serialization_error,
                "serialization_seconds": dag.serialization_seconds,
            }
            for dag_id, dag in corpus.dags.items()
        },
    }


def _dag_corpus_from_payload(payload: dict[str, Any]) -> DagCorpus:
    """Decode one versioned Dag corpus artifact.

    Parameters:
        payload: dict[str, Any] loaded from the shared JSON artifact.

    Returns:
        DagCorpus reconstructed for the current worker.

    Raises:
        ValueError: The artifact schema version is unsupported.
    """

    version = payload.get("version")
    if version != DAG_CORPUS_VERSION:
        raise ValueError(f"Unsupported Dag corpus version: '{version}'")
    runtime_lookups = payload["runtime_lookups"]
    return DagCorpus(
        dags={
            dag_id: CorpusDag(
                dag_id=dag_id,
                tags=frozenset(value["tags"]),
                tasks=tuple(CorpusTask(**task) for task in value["tasks"]),
                can_be_scheduled=value["can_be_scheduled"],
                catchup=value["catchup"],
                fileloc=value["fileloc"],
                serialized=value["serialized"],
                serialization_error=value["serialization_error"],
                serialization_seconds=value["serialization_seconds"],
            )
            for dag_id, value in payload["dags"].items()
        },
        import_errors=payload["import_errors"],
        dagbag_stats=tuple(
            CorpusDagFileStat(
                file=stat["file"],
                duration=timedelta(seconds=stat["duration"]),
                dag_num=stat["dag_num"],
                task_num=stat["task_num"],
            )
            for stat in payload["dagbag_stats"]
        ),
        runtime_lookups=None
        if runtime_lookups is None
        else tuple(SecretsLookup(**lookup) for lookup in runtime_lookups),
        producer_pid=payload["producer_pid"],
        producer_worker=payload["producer_worker"],
    )


def _write_dag_corpus(path: Path, corpus: DagCorpus) -> None:
    """Atomically publish one complete shared Dag corpus artifact.

    Parameters:
        path: pathlib.Path naming the final artifact.
        corpus: DagCorpus to serialize.
    """

    temporary = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(_dag_corpus_payload(corpus), sort_keys=True), encoding="utf-8"
        )
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _shared_dag_corpus(session: pytest.Session, config: pytest.Config) -> DagCorpus:
    """Elect one process to parse Dags and share the result with local workers.

    Parameters:
        session: pytest.Session used to reach a DagBag already parsed by `dag_bag`.
        config: pytest.Config carrying the shared bootstrap run root.

    Returns:
        DagCorpus loaded from or published to the shared run root.
    """

    # Deferred because Airflow only supports POSIX platforms and the plugin's
    # import-light entry point remains useful to platform-independent tooling.
    import fcntl

    root = get_bootstrap_state(config).root
    artifact = root / DAG_CORPUS_ARTIFACT_NAME
    lock_path = root / DAG_CORPUS_LOCK_NAME
    with lock_path.open("ab") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            if artifact.is_file():
                payload = json.loads(artifact.read_text(encoding="utf-8"))
                return _dag_corpus_from_payload(payload)
            corpus = _build_dag_corpus(session, config)
            _write_dag_corpus(artifact, corpus)
            LOGGER.info(
                f"Worker `{corpus.producer_worker}` PID {corpus.producer_pid} "
                f"published shared Dag corpus '{artifact}'"
            )
            return corpus
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def get_dag_corpus(session: pytest.Session, config: pytest.Config) -> DagCorpus:
    """Load and process-cache the shared Dag corpus for one worker session.

    Backs both the public `dag_corpus` fixture and the bundled smoke catalog's
    ``SmokeContext.corpus``, so every consumer in one worker process shares the exact same
    parse regardless of which one triggers the build.

    Parameters:
        session: pytest.Session used to cache the decoded corpus.
        config: pytest.Config carrying the shared bootstrap run root.

    Returns:
        DagCorpus containing every parsed Dag's portable metadata.
    """

    if DAG_CORPUS_KEY not in session.stash:
        session.stash[DAG_CORPUS_KEY] = _shared_dag_corpus(session, config)
    return session.stash[DAG_CORPUS_KEY]


__all__ = (
    "CorpusDag",
    "CorpusDagFileStat",
    "CorpusTask",
    "DagCorpus",
    "SecretsLookup",
    "get_dag_corpus",
    "mark_dag_corpus_requested",
)
