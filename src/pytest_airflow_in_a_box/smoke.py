"""Collect an opt-in catalog of zero-boilerplate Dag smoke checks.

Every item is synthesized directly on the pytest ``Session`` rather than anchored to a real file
on disk, so the catalog carries no collection dependency on the user's project layout. Off unless
``airflow_smoke``/``--airflow-smoke`` is enabled; collection cost is zero when disabled. Explicit
file or node-ID positionals scope the run to those tests and drop the catalog; directory
positionals and arg-less runs keep it. An explicit ``-m`` expression that mentions ``smoke``
and would select a real smoke item overrides that positional scoping.

References:
    https://docs.pytest.org/en/stable/example/nonpython.html
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.hookspec.pytest_collection_modifyitems
    https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-serialization.html
"""

from __future__ import annotations

import copy
import difflib
import hashlib
import json
import logging
import os
import re
import statistics
import time
import warnings
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from pytest_airflow_in_a_box._compat import build_dag_bag, ensure_database
from pytest_airflow_in_a_box._compat.dag import _get_dag_serializer, time_restriction_type
from pytest_airflow_in_a_box._compat.db import create_session, get_pool_model
from pytest_airflow_in_a_box._compat.introspection import (
    SecretsLookup,
    mapped_expansion,
    record_secrets_lookups,
)
from pytest_airflow_in_a_box.antipatterns import (
    DEFAULT_TOP_LEVEL_IO_MODULES,
    find_io_calls,
    find_secrets_lookups,
    parse_dag_module,
)
from pytest_airflow_in_a_box.bootstrap import get_bootstrap_state
from pytest_airflow_in_a_box.fixtures.dagbag import (
    LIVE_DAG_BAG_KEY,
    LIVE_DAG_BAG_LOOKUPS_KEY,
    _dag_folder,
)
from pytest_airflow_in_a_box.parse_secrets import parse_time_comms

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping, Sequence

    from _pytest._code.code import TerminalRepr

LOGGER = logging.getLogger(__name__)

SMOKE_ENABLED_KEY = pytest.StashKey[bool]()
SMOKE_CORPUS_KEY = pytest.StashKey["SmokeCorpus"]()
SMOKE_CORPUS_VERSION = 2
SMOKE_CORPUS_ARTIFACT_NAME = ".airflow-smoke-corpus.json"
SMOKE_CORPUS_LOCK_NAME = ".airflow-smoke-corpus.lock"
SERIALIZED_DAG_CACHE_KEY = pytest.StashKey[dict[str, "SerializedDagEntry"]]()
DEFAULT_OWNER = "airflow"
DUPLICATE_ID_MARKER = "AirflowDagDuplicatedIdException"
RUN_DEPENDENT_SERIALIZED_DAG_KEYS = frozenset(
    {"_processor_dags_folder", "fileloc", "relative_fileloc"}
)
SERIALIZATION_TIMEOUT_FLOOR_SECONDS = 30.0
# Absolute floor under the relative parse-budget threshold, so tiny fast corpora with a
# near-zero median do not fail on CI timing jitter.
PARSE_BUDGET_FLOOR_SECONDS = 1.0
# Below this many parsed files a relative-to-median budget is statistical noise.
PARSE_BUDGET_MINIMUM_FILES = 3


@dataclass(frozen=True)
class SmokeTask:
    """Task metadata needed by the corpus-wide policy checks.

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
class PoolSeed:
    """One consumer-defined pool seeded before `test_pool_references_exist` runs.

    Parameters:
        name: str containing the pool name.
        slots: int containing the pool's slot capacity.
    """

    name: str
    slots: int


@dataclass(frozen=True)
class SmokeDag:
    """Serialized Dag plus metadata needed without the authoring process."""

    dag_id: str
    tags: frozenset[str]
    tasks: tuple[SmokeTask, ...]
    can_be_scheduled: bool
    catchup: bool
    fileloc: str
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
    """Cross-process representation of one parsed Dag folder.

    ``runtime_lookups`` is deliberately tri-state: ``None`` means the producer reused a
    ``DagBag`` that `full_dag_bag` had already parsed without interception, so runtime
    secrets findings are unavailable; an empty tuple means the parse was observed and no
    lookup happened.
    """

    dags: dict[str, SmokeDag]
    import_errors: dict[str, str]
    dagbag_stats: tuple[SmokeDagFileStat, ...]
    runtime_lookups: tuple[SecretsLookup, ...] | None
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


# Every bundled item's `name=` as passed to `.from_parent` in `SmokeCollector.collect()` --
# the single source of truth `_disabled_smoke_items` validates `airflow_smoke_disable` against.
_SMOKE_ITEM_NAMES: frozenset[str] = frozenset(
    {
        "test_dag_bag_integrity",
        "test_dag_serialization_roundtrip",
        "test_no_duplicate_dag_ids",
        "test_schedule_sanity",
        "test_pool_references_exist",
        "test_no_top_level_variable_access",
        "test_no_top_level_io",
        "test_dag_parse_budget",
        "test_forbid_catchup",
        "test_no_unbounded_expand",
        "test_dag_id_pattern",
        "test_required_dag_tags",
        "test_forbid_default_owner",
        "test_dag_serialization_snapshot",
    }
)

# The two concrete mark-name combinations every synthesized smoke item actually carries
# (`smoke` + `timeout` always, plus `db_test` on `DagBagIntegrityItem` and
# `PoolReferencesExistItem`). Kept in sync with the `add_marker` calls in each item's
# `__init__`; `test_smoke_and_db_test_mark_expression_overrides_positional_exclusion` and
# `test_markexpr_override_reaches_every_conditional_smoke_item` collect the real catalog
# (the latter with every ini-gated item enabled too) and would start failing their exact
# pass counts if an item's marks ever drifted from this pair.
_SMOKE_ITEM_MARK_SETS: tuple[frozenset[str], ...] = (
    frozenset({"smoke", "timeout"}),
    frozenset({"smoke", "timeout", "db_test"}),
)

# Requires the literal `smoke` identifier somewhere in `-m`, not just an expression a real
# smoke item's marks happen to satisfy: `_SMOKE_ITEM_MARK_SETS` only carries a handful of
# generic, widely-reused names (`timeout`, `db_test`), so e.g. `-m "not slow"` or a bare
# `-m timeout` would otherwise vacuously match too (absence of an unrelated marker), silently
# pulling the whole catalog into a run explicitly scoped to one unrelated file. Mentioning
# `smoke` at all is the actual unambiguous opt-in signal.
_SMOKE_MARKEXPR_TOKEN = re.compile(r"\bsmoke\b")


def _markexpr_wants_smoke(config: pytest.Config) -> bool:
    """Report whether ``-m``/ini ``markexpr`` explicitly opts into the smoke catalog.

    An explicit ``-m`` expression mentioning ``smoke`` is unambiguous opt-in and must win
    over the file/node-ID scoping in `_smoke_in_scope`, or ``-m smoke`` (or any other
    expression a real smoke item's marks would satisfy, e.g. ``-m "smoke and timeout"``)
    combined with an explicit positional silently selects nothing. A single flat matcher
    over the union of known marker names is not enough to resolve the expression once it
    does mention ``smoke``: e.g. ``-m "smoke and not db_test"`` genuinely selects every
    smoke item that lacks ``db_test``, but a union matcher sees ``db_test`` as present
    (some other item carries it) and wrongly evaluates the expression to ``False``.
    Evaluating against each concrete mark set in `_SMOKE_ITEM_MARK_SETS` in turn avoids that.

    Parameters:
        config: pytest.Config carrying the resolved ``-m`` mark expression.

    Returns:
        bool indicating whether the expression mentions `smoke` and matches a real item.
    """

    markexpr: str = config.option.markexpr
    if not markexpr or not _SMOKE_MARKEXPR_TOKEN.search(markexpr):
        return False
    try:
        # Local import: deferred so a future pytest release relocating this private,
        # version-coupled symbol can't break collection for every plugin user -- only a run
        # that already enabled the smoke catalog (`_smoke_enabled` is true by the time
        # `_smoke_in_scope` calls this) ever reaches this branch.
        from _pytest.mark.expression import Expression

        # `Expression.compile` raises `SyntaxError` on pytest >= 9 and the private,
        # version-specific `_pytest.mark.expression.ParseError` (unrelated to `SyntaxError`)
        # on pytest 8.x, the floor this plugin supports; `.evaluate` can itself raise
        # `pytest.UsageError` for an expression form the matcher rejects. Catching broadly
        # here is safe: an unparsable/unsupported `markexpr` is handled again, correctly
        # typed, by pytest's own `-m` handling right after this `tryfirst` hook returns.
        expression = Expression.compile(markexpr)
        return any(
            expression.evaluate(
                lambda name, /, mark_set=mark_set, **kwargs: name in mark_set and not kwargs
            )
            for mark_set in _SMOKE_ITEM_MARK_SETS
        )
    except Exception:
        return False


def _smoke_in_scope(config: pytest.Config) -> bool:
    """Report whether the run's positional selection leaves the smoke catalog in scope.

    The catalog is synthesized onto the ``Session`` after ``perform_collect`` has already
    honored positional args, so node-ID and file selection must be re-applied here: explicit
    file or node-ID positionals scope the run to those tests only, while directory positionals
    (and arg-less runs, including ``testpaths``-driven ones) keep the session-level catalog.
    An explicit ``-m`` expression that would select a smoke item (see
    `_markexpr_wants_smoke`) overrides that positional scoping, since it is unambiguous
    opt-in. Keyword deselection (``-k``/``--deselect``) needs no handling -- pytest applies
    it after this plugin's ``tryfirst`` collection hook.

    Parameters:
        config: pytest.Config containing resolved positional args and invocation metadata.

    Returns:
        bool indicating whether the bundled catalog should be appended to the collection.
    """

    if _markexpr_wants_smoke(config):
        return True

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


def _disabled_smoke_items(config: pytest.Config) -> frozenset[str]:
    """Read the bundled smoke item names dropped from the catalog.

    Parameters:
        config: pytest.Config containing the ``airflow_smoke_disable`` ini value.

    Returns:
        frozenset[str] containing the disabled item names; empty when nothing is disabled.

    Raises:
        pytest.UsageError: The ini value is not a list of strings, or names an item outside
            `_SMOKE_ITEM_NAMES`.
    """

    lines: object = config.getini("airflow_smoke_disable")
    if not isinstance(lines, list) or any(not isinstance(line, str) for line in lines):
        raise pytest.UsageError("Ini option `airflow_smoke_disable` must be a list of names")
    disabled = frozenset(lines)
    unknown = disabled - _SMOKE_ITEM_NAMES
    if unknown:
        raise pytest.UsageError(
            f"Ini option `airflow_smoke_disable` names unknown smoke item(s): "
            f"{sorted(unknown)}; must be a subset of {sorted(_SMOKE_ITEM_NAMES)}"
        )
    return disabled


def _smoke_serialization_needed(config: pytest.Config) -> bool:
    """Report whether any still-collected item requires a serialized Dag.

    Parameters:
        config: pytest.Config containing the ``airflow_smoke_disable`` and
            ``airflow_dag_snapshot_dir`` ini values.

    Returns:
        bool indicating whether the corpus builder must call the Airflow DAG serializer.
    """

    disabled = _disabled_smoke_items(config)
    if "test_dag_serialization_roundtrip" not in disabled:
        return True
    if "test_schedule_sanity" not in disabled:
        return True
    return _snapshot_dir(config) is not None and "test_dag_serialization_snapshot" not in disabled


def _pool_seeds(config: pytest.Config) -> tuple[PoolSeed, ...]:
    """Read the pools seeded before `test_pool_references_exist` runs.

    Parameters:
        config: pytest.Config containing the ``airflow_pools`` ini value.

    Returns:
        tuple[PoolSeed, ...] containing the configured pools; empty when unset.

    Raises:
        pytest.UsageError: A line is malformed, a name repeats, or slots is not a
            positive integer.
    """

    lines: object = config.getini("airflow_pools")
    if not isinstance(lines, list) or any(not isinstance(line, str) for line in lines):
        raise pytest.UsageError("Ini option `airflow_pools` must be a list of lines")
    seen: set[str] = set()
    pools: list[PoolSeed] = []
    for line in lines:
        name, separator, value = (part.strip() for part in line.partition("="))
        if not separator or not name or not value:
            raise pytest.UsageError(
                f"Ini option `airflow_pools` line must be `name = slots`: '{line}'"
            )
        if name in seen:
            raise pytest.UsageError(f"Duplicate `airflow_pools` pool name: `{name}`")
        try:
            slots = int(value)
        except ValueError as error:
            raise pytest.UsageError(
                f"Ini option `airflow_pools` slots must be an integer: '{line}'"
            ) from error
        if slots <= 0:
            raise pytest.UsageError(f"Ini option `airflow_pools` slots must be positive: '{line}'")
        seen.add(name)
        pools.append(PoolSeed(name=name, slots=slots))
    return tuple(pools)


def _bool_ini(config: pytest.Config, name: str) -> bool:
    """Read one boolean ini option shared by the on/off smoke policies.

    Parameters:
        config: pytest.Config containing the ini value.
        name: str naming the registered boolean ini option.

    Returns:
        bool containing the configured value.

    Raises:
        pytest.UsageError: The ini value is not a boolean.
    """

    value: object = config.getini(name)
    if not isinstance(value, bool):
        raise pytest.UsageError(f"Ini option `{name}` must be a boolean")
    return value


def _forbid_default_owner(config: pytest.Config) -> bool:
    """Read whether tasks owned by Airflow's stock default owner should fail.

    Parameters:
        config: pytest.Config containing the ``airflow_forbid_default_owner`` ini value.

    Returns:
        bool indicating whether the policy is active.

    Raises:
        pytest.UsageError: The ini value is not a boolean.
    """

    return _bool_ini(config, "airflow_forbid_default_owner")


def _forbid_top_level_variable_access(config: pytest.Config) -> bool:
    """Read whether import-time Variable and Connection lookups should fail.

    Parameters:
        config: pytest.Config containing the ``airflow_forbid_top_level_variable_access``
            ini value.

    Returns:
        bool indicating whether the check is active.

    Raises:
        pytest.UsageError: The ini value is not a boolean.
    """

    return _bool_ini(config, "airflow_forbid_top_level_variable_access")


def _forbid_top_level_io(config: pytest.Config) -> bool:
    """Read whether import-time calls into known I/O modules should fail.

    Parameters:
        config: pytest.Config containing the ``airflow_forbid_top_level_io`` ini value.

    Returns:
        bool indicating whether the check is active.

    Raises:
        pytest.UsageError: The ini value is not a boolean.
    """

    return _bool_ini(config, "airflow_forbid_top_level_io")


def _top_level_io_modules(config: pytest.Config) -> tuple[str, ...]:
    """Read the module prefixes the top-level I/O check flags.

    A non-empty ``airflow_top_level_io_modules`` list replaces the built-in default list
    rather than extending it, so a consumer who wants "defaults plus one" copies the list.

    Parameters:
        config: pytest.Config containing the ``airflow_top_level_io_modules`` ini value.

    Returns:
        tuple[str, ...] containing the configured prefixes, or the built-in defaults.

    Raises:
        pytest.UsageError: The ini value is not a list of non-empty module names.
    """

    lines: object = config.getini("airflow_top_level_io_modules")
    if not isinstance(lines, list) or any(not isinstance(line, str) for line in lines):
        raise pytest.UsageError(
            "Ini option `airflow_top_level_io_modules` must be a list of module names"
        )
    modules = tuple(line.strip() for line in lines)
    if any(not module for module in modules):
        raise pytest.UsageError(
            "Ini option `airflow_top_level_io_modules` must not contain empty module names"
        )
    return modules or DEFAULT_TOP_LEVEL_IO_MODULES


def _dag_parse_budget_ratio(config: pytest.Config) -> float | None:
    """Read the relative parse-budget multiple of the corpus median parse duration.

    Parameters:
        config: pytest.Config containing the ``airflow_dag_parse_budget_ratio`` ini value.

    Returns:
        float | None containing the positive multiplier, or ``None`` when ``0`` disables
        the check.

    Raises:
        pytest.UsageError: The ini value is not a non-negative number.
    """

    value: object = config.getini("airflow_dag_parse_budget_ratio")
    if not isinstance(value, str):
        raise pytest.UsageError("Ini option `airflow_dag_parse_budget_ratio` must be a number")
    try:
        ratio = float(value)
    except ValueError as error:
        raise pytest.UsageError(
            f"Ini option `airflow_dag_parse_budget_ratio` must be a number: '{value}'"
        ) from error
    if ratio < 0:
        raise pytest.UsageError(
            f"Ini option `airflow_dag_parse_budget_ratio` must be non-negative: '{value}'"
        )
    return ratio if ratio > 0 else None


def _forbid_catchup(config: pytest.Config) -> bool:
    """Read whether Dags that enable ``catchup`` should fail.

    Parameters:
        config: pytest.Config containing the ``airflow_forbid_catchup`` ini value.

    Returns:
        bool indicating whether the check is active.

    Raises:
        pytest.UsageError: The ini value is not a boolean.
    """

    return _bool_ini(config, "airflow_forbid_catchup")


def _forbid_unbounded_expand(config: pytest.Config) -> bool:
    """Read whether uncapped runtime-data ``expand()`` tasks should fail.

    Parameters:
        config: pytest.Config containing the ``airflow_forbid_unbounded_expand`` ini value.

    Returns:
        bool indicating whether the check is active.

    Raises:
        pytest.UsageError: The ini value is not a boolean.
    """

    return _bool_ini(config, "airflow_forbid_unbounded_expand")


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


def _build_smoke_corpus(session: pytest.Session, config: pytest.Config) -> SmokeCorpus:
    """Parse and serialize the configured Dag folder in the elected process.

    Sets ``AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT`` immediately before construction so Airflow
    hard-kills any single file exceeding the configured timeout; the environment variable is read
    per lookup, not cached at import, so this stays safe to set late and per-run. Reuses a
    DagBag `full_dag_bag` already parsed for this process, if one exists, rather than parsing
    the Dag folder a second time. A fresh parse here is deliberately not cached on the session
    the way `full_dag_bag`'s is -- nothing else in a smoke-only run needs the live DagBag past
    this function returning, only the portable SmokeCorpus it builds from it.

    Parameters:
        session: pytest.Session used to reach a DagBag already parsed by `full_dag_bag`.
        config: pytest.Config containing plugin options and ini values.

    Returns:
        SmokeCorpus containing portable data for every bundled smoke check.
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
        comms = parse_time_comms(config)
        if comms is not None:
            # Several smoke items are deliberately not `db_test`, so nothing has
            # initialized the database by the time they run. A Dag with a top-level
            # Variable or Connection lookup would otherwise charge the whole one-time
            # migration to this item's parse timeout, which budgets for parsing and
            # serialization alone. `--airflow-parse-secrets=off` keeps the build
            # database-free.
            ensure_database(get_bootstrap_state(config).root)
        with record_secrets_lookups(_dag_folder(config)) as recorded:
            dag_bag = build_dag_bag(_dag_folder(config), comms=comms)
        runtime_lookups = tuple(dict.fromkeys(recorded))
    serializer: Any | None = None
    selected = _select_serialization_sample(config, dag_bag.dags)
    selected_ids = set(selected)
    dags: dict[str, SmokeDag] = {}
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
        dags[dag_id] = SmokeDag(
            dag_id=dag_id,
            tags=frozenset(dag.tags),
            tasks=tuple(_smoke_task(task) for task in dag.tasks),
            can_be_scheduled=dag.timetable.can_be_scheduled,
            catchup=bool(getattr(dag, "catchup", False)),
            fileloc=str(getattr(dag, "fileloc", "")),
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
        runtime_lookups=runtime_lookups,
        producer_pid=os.getpid(),
        producer_worker=os.environ.get("PYTEST_XDIST_WORKER", "master"),
    )


def _smoke_task(task: Any) -> SmokeTask:
    """Capture one live operator's portable smoke-check metadata.

    Parameters:
        task: Any containing a live Airflow operator.

    Returns:
        SmokeTask containing the captured task metadata.
    """

    is_mapped, over_runtime_data, cap = mapped_expansion(task)
    return SmokeTask(
        task_id=task.task_id,
        owner=task.owner,
        pool=task.pool,
        is_mapped=is_mapped,
        mapped_over_runtime_data=over_runtime_data,
        max_active_tis_per_dag=cap,
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
    runtime_lookups = payload["runtime_lookups"]
    return SmokeCorpus(
        dags={
            dag_id: SmokeDag(
                dag_id=dag_id,
                tags=frozenset(value["tags"]),
                tasks=tuple(SmokeTask(**task) for task in value["tasks"]),
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
            SmokeDagFileStat(
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


def _shared_smoke_corpus(session: pytest.Session, config: pytest.Config) -> SmokeCorpus:
    """Elect one process to parse Dags and share the result with local workers.

    Parameters:
        session: pytest.Session used to reach a DagBag already parsed by `full_dag_bag`.
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
            corpus = _build_smoke_corpus(session, config)
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
        session.stash[SMOKE_CORPUS_KEY] = _shared_smoke_corpus(session, config)
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


def _select_serialization_sample(config: pytest.Config, dag_ids: Iterable[str]) -> list[str]:
    """Select the Dag IDs to serialize, honoring `airflow_smoke_disable`.

    Skips sampling (and any Airflow DAG serializer call downstream) entirely once
    `_smoke_serialization_needed` reports nothing still needs a serialized Dag.

    Parameters:
        config: pytest.Config containing plugin options and ini values.
        dag_ids: Iterable[str] containing every discovered Dag identifier.

    Returns:
        list[str] containing the selected Dag IDs; empty when serialization is not needed.
    """

    if not _smoke_serialization_needed(config):
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
    selected = _select_serialization_sample(config, dag_bag.dags)

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

    Runs unconditionally, before `_smoke_in_scope` decides whether the catalog is even
    collected this invocation, so a malformed `airflow_smoke_disable` is always reported --
    a file/node-ID-scoped run that never reaches `SmokeCollector.collect()` would otherwise
    let a typo'd item name pass through silently.

    Parameters:
        config: pytest.Config containing plugin options and ini values.

    Raises:
        pytest.UsageError: `airflow_smoke_disable` is malformed or names an unknown item, or
            snapshot update mode is combined with serialization sampling, which would
            regenerate only a subset of the committed snapshots.
    """

    disabled = _disabled_smoke_items(config)
    if (
        _smoke_update(config)
        and _snapshot_dir(config) is not None
        and "test_dag_serialization_snapshot" not in disabled
        and _serialization_sample_size(config) > 0
    ):
        raise pytest.UsageError(
            "`--airflow-smoke-update` cannot be combined with "
            "`airflow_serialization_sample_size`; regenerating snapshots from a sample would "
            "silently drop the unsampled Dags"
        )


def _render_table(headers: Sequence[str], rows: Sequence[Mapping[str, str]]) -> str:
    """Render a plain-text table with fixed-width, left-justified columns.

    Owned replacement for ``airflow.cli.simple_table.AirflowConsole`` -- an unstable CLI
    internal this plugin only ever used for report rendering. The header row and its dash
    separator render even when ``rows`` is empty, so an empty report still names its columns.
    Each column is as wide as its widest content (header or cell); the separator spans every
    column's full width while header and data lines carry no trailing padding. Widths are
    measured in code points, so double-width (e.g. CJK) cells render misaligned -- accepted,
    since Dag file paths and ids are overwhelmingly ASCII.

    Parameters:
        headers: Sequence[str] naming the columns, in render order. Each header doubles as
            the key that looks its cell up in every row mapping.
        rows: Sequence[Mapping[str, str]] containing one mapping per table row, keyed by
            header name.

    Returns:
        str containing the rendered table text, terminated by a newline.
    """

    # The literal 0 is not dead: with zero rows the unpacking leaves max() a single int
    # argument, which it would reject as a non-iterable.
    widths = [max(len(header), *(len(row[header]) for row in rows), 0) for header in headers]

    def _line(cells: Iterable[str]) -> str:
        return " | ".join(
            cell.ljust(width) for cell, width in zip(cells, widths, strict=True)
        ).rstrip()

    lines = [_line(headers), "-+-".join("-" * width for width in widths)]
    lines.extend(_line(row[header] for header in headers) for row in rows)
    return "\n".join(lines) + "\n"


def _log_stats_table(dag_bag: Any, *, timeout: float, ratio: float) -> str:
    """Render and log a slowest-first table of every parsed Dag file.

    Parameters:
        dag_bag: Any containing portable Dag parse statistics.
        timeout: float containing the hard per-file parse timeout in seconds.
        ratio: float containing the slowpoke warning ratio of the timeout.

    Returns:
        str containing the rendered table text.
    """

    threshold = ratio * timeout

    def _status(seconds: float) -> str:
        if seconds > timeout:
            return f"SLOWPOKE (>{timeout:.1f}s timeout)"
        if seconds > threshold:
            return f"SLOWPOKE (>{ratio:.0%} of {timeout:.1f}s)"
        return "ok"

    rows = [
        {
            "file": stat.file,
            "dags": str(stat.dag_num),
            "tasks": str(stat.task_num),
            "duration": f"{stat.duration.total_seconds():.3f}s",
            "status": _status(stat.duration.total_seconds()),
        }
        for stat in dag_bag.dagbag_stats
    ]
    text = _render_table(("file", "dags", "tasks", "duration", "status"), rows)
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

    def _total(dag_id: str) -> float:
        return entries[dag_id].seconds + deserialize_seconds.get(dag_id, 0.0)

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

    rows = [_mapper(dag_id) for dag_id in sorted(entries, key=_total, reverse=True)]
    text = _render_table(("dag_id", "serialize", "deserialize", "total", "status"), rows)
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

        Dags missing from a sampled cache are skipped. Serialization failures normally belong
        to ``test_dag_serialization_roundtrip``; when that item is disabled via
        ``airflow_smoke_disable``, this item reports them itself instead, so a Dag the
        scheduler cannot serialize does not silently pass just because its usual reporter
        is gone.

        Raises:
            SmokeCheckFailure: A scheduled Dag's timetable raised while computing its next run,
                or (only when ``test_dag_serialization_roundtrip`` is disabled) a scheduled Dag
                failed to serialize.
        """

        time_restriction_class = time_restriction_type()
        dag_bag = _smoke_dag_bag(self.session, self.config)
        entries = _serialized_dag_cache(self.session, self.config)
        serialized_dag_class = _get_dag_serializer()
        roundtrip_disabled = "test_dag_serialization_roundtrip" in _disabled_smoke_items(
            self.config
        )
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
            if entry is None:
                continue
            if entry.error is not None:
                if roundtrip_disabled:
                    failures.append(f"Dag `{dag_id}` failed to serialize: {entry.error}")
                continue
            try:
                # Deserialization mutates its input, so the shared cache gets a copy.
                decoded = serialized_dag_class.deserialize_dag(copy.deepcopy(entry.payload()))
                restriction = time_restriction_class(
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

    def __init__(
        self,
        *,
        name: str,
        parent: SmokeCollector,
        pools: tuple[PoolSeed, ...] = (),
    ) -> None:
        """Create the item and mark it as a metadata-database smoke test.

        Parameters:
            name: str containing the pytest item name.
            parent: SmokeCollector that collected this item.
            pools: tuple[PoolSeed, ...] containing pools to seed before checking references.
        """

        super().__init__(name=name, parent=parent)
        self.add_marker(pytest.mark.smoke)
        self.add_marker(pytest.mark.timeout(_smoke_item_timeout(self.config)))
        self.add_marker(pytest.mark.db_test)
        self.pools = pools

    def runtest(self) -> None:
        """Seed configured pools, then resolve every task's pool against the database.

        Seeding is idempotent: a configured pool already present with the same slot
        count is treated as already seeded rather than an error, so this item stays
        safe to execute more than once against the same database -- under
        ``pytest-xdist --dist each``, every worker runs this item, and a rerun tool
        may execute it again after a failure.

        Raises:
            SmokeCheckFailure: A configured pool already exists with a different slot
                count, or a task references a pool the database does not contain.
        """

        pool_model = get_pool_model()
        dag_bag = _smoke_corpus(self.session, self.config)
        failures: list[str] = []
        with create_session() as database_session:
            existing: dict[str, int] = {
                name: slots
                for pool in pool_model.get_pools(session=database_session)
                if isinstance(name := pool.pool, str) and isinstance(slots := pool.slots, int)
            }
            conflicts = sorted(
                seed.name
                for seed in self.pools
                if seed.name in existing and existing[seed.name] != seed.slots
            )
            if conflicts:
                names = ", ".join(f"`{name}`" for name in conflicts)
                raise SmokeCheckFailure(
                    f"`airflow_pools` cannot seed {names}; a pool with that name already "
                    "exists with a different slot count. Remove it from `airflow_pools` "
                    "or match its existing slots"
                )
            to_seed = [seed for seed in self.pools if seed.name not in existing]
            if to_seed:
                database_session.add_all(
                    pool_model(pool=seed.name, slots=seed.slots, include_deferred=False)
                    for seed in to_seed
                )
            known = set(existing) | {seed.name for seed in self.pools}
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


def _corpus_source_files(corpus: SmokeCorpus, folder: Path) -> list[tuple[str, Path]]:
    """Resolve each parsed Dag file's display name and absolute path.

    Scanning the statistics paths covers exactly the files Airflow parsed, inheriting
    ``.airflowignore`` and underscore-prefix handling for free -- but releases differ in
    what the path holds. Airflow 3.2+ records a Dag-folder-relative path, while 3.1 and
    2.x strip ``settings.DAGS_FOLDER`` (the bootstrap folder) from the front, which is a
    no-op leaving the path absolute whenever the parsed folder is a configured
    ``--dag-folder``; 3.2+ also falls back to the absolute path for files outside the
    folder. Both shapes are resolved here rather than trusted.

    Parameters:
        corpus: SmokeCorpus containing the parsed Dag folder's file statistics.
        folder: pathlib.Path containing the configured Dag folder.

    Returns:
        list[tuple[str, pathlib.Path]] pairing each display name with its absolute path.
    """

    pairs: list[tuple[str, Path]] = []
    for stat in corpus.dagbag_stats:
        candidate = folder / stat.file.lstrip("/")
        if not candidate.is_file():
            absolute = Path(stat.file)
            if absolute.is_file():
                candidate = absolute
        pairs.append((stat.file, candidate))
    return pairs


class TopLevelVariableAccessItem(pytest.Item):
    """Fail on Variable and Connection lookups that run while a Dag file imports."""

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
        """Merge AST and runtime secrets-lookup findings over the parsed corpus.

        The AST pass reports direct top-level calls with exact locations; the runtime pass
        adds lookups hidden behind helpers, recorded while the corpus producer (or the
        `full_dag_bag` parse it reuses) filled the ``DagBag``. Runtime findings deduplicate
        against AST findings by file and call span, and degrade gracefully to AST-only for
        a ``DagBag`` parsed without instrumentation.

        Raises:
            SmokeCheckFailure: Any Dag file performs an import-time secrets lookup.
        """

        corpus = _smoke_corpus(self.session, self.config)
        folder = _dag_folder(self.config)
        failures: list[str] = []
        spans: dict[str, list[tuple[int, int]]] = {}
        for display, path in _corpus_source_files(corpus, folder):
            parsed = parse_dag_module(path)
            if parsed is None:
                continue
            for finding in find_secrets_lookups(*parsed):
                spans.setdefault(str(path.resolve()), []).append((finding.line, finding.end_line))
                failures.append(
                    f"Dag file '{display}' line {finding.line} calls `{finding.snippet}` at "
                    f"import time; secrets lookups in top-level code run on every scheduler "
                    f"parse loop -- move them into task scope"
                )
        if corpus.runtime_lookups is None:
            LOGGER.info(
                "Runtime secrets interception unavailable: the shared corpus reused a "
                "DagBag parsed without instrumentation; AST findings still apply"
            )
        else:
            for lookup in corpus.runtime_lookups:
                # A frame's reported line for a multi-line call varies across CPython
                # releases (pre-PEP 657 names the attribute's line, not the call's), so
                # runtime findings deduplicate against the AST finding's whole line span.
                if (
                    lookup.file is not None
                    and lookup.line is not None
                    and any(
                        start <= lookup.line <= end
                        for start, end in spans.get(str(Path(lookup.file).resolve()), ())
                    )
                ):
                    continue
                origin = "an unattributed location" if lookup.file is None else f"'{lookup.file}'"
                failures.append(
                    f"Parsing the Dag folder fetched {lookup.kind} '{lookup.key}' from "
                    f"{origin}; secrets lookups in top-level code run on every scheduler "
                    f"parse loop -- move them into task scope"
                )
        if failures:
            raise SmokeCheckFailure("\n\n".join(failures))

    def reportinfo(self) -> tuple[str, int, str]:
        """Locate this item for terminal and junit reporting.

        Returns:
            tuple[str, int, str] containing path, line, and title.
        """

        return self.nodeid, 0, self.name


class TopLevelIOItem(pytest.Item):
    """Fail on calls into known I/O modules that run while a Dag file imports."""

    def __init__(self, *, name: str, parent: SmokeCollector, io_modules: tuple[str, ...]) -> None:
        """Create the item and mark it as a bundled smoke test.

        Parameters:
            name: str containing the pytest item name.
            parent: SmokeCollector that collected this item.
            io_modules: tuple[str, ...] containing the module prefixes to flag.
        """

        super().__init__(name=name, parent=parent)
        self.io_modules = io_modules
        self.add_marker(pytest.mark.smoke)
        self.add_marker(pytest.mark.timeout(_smoke_item_timeout(self.config)))

    def runtest(self) -> None:
        """Scan every parsed Dag file for import-time calls into configured I/O modules.

        Raises:
            SmokeCheckFailure: Any Dag file performs import-time network or database I/O.
        """

        corpus = _smoke_corpus(self.session, self.config)
        folder = _dag_folder(self.config)
        failures: list[str] = []
        for display, path in _corpus_source_files(corpus, folder):
            parsed = parse_dag_module(path)
            if parsed is None:
                continue
            for finding in find_io_calls(*parsed, self.io_modules):
                failures.append(
                    f"Dag file '{display}' line {finding.line} calls `{finding.snippet}` at "
                    f"import time; network or database I/O in top-level code runs on every "
                    f"scheduler parse loop -- move it into task scope"
                )
        if failures:
            raise SmokeCheckFailure("\n\n".join(failures))

    def reportinfo(self) -> tuple[str, int, str]:
        """Locate this item for terminal and junit reporting.

        Returns:
            tuple[str, int, str] containing path, line, and title.
        """

        return self.nodeid, 0, self.name


class DagParseBudgetItem(pytest.Item):
    """Fail Dag files whose parse duration is an outlier against the corpus median."""

    def __init__(self, *, name: str, parent: SmokeCollector, ratio: float) -> None:
        """Create the item and mark it as a bundled smoke test.

        Parameters:
            name: str containing the pytest item name.
            parent: SmokeCollector that collected this item.
            ratio: float containing the budget multiple of the corpus median.
        """

        super().__init__(name=name, parent=parent)
        self.ratio = ratio
        self.add_marker(pytest.mark.smoke)
        self.add_marker(pytest.mark.timeout(_smoke_item_timeout(self.config)))

    def runtest(self) -> None:
        """Compare every file's parse duration against the relative budget threshold.

        The threshold is ``max(ratio * median, PARSE_BUDGET_FLOOR_SECONDS)``, so the check
        is independent of absolute CI speed and a near-zero median on a small fast corpus
        cannot fail on timing jitter. Below `PARSE_BUDGET_MINIMUM_FILES` parsed files the
        median is statistical noise and the check passes trivially.

        Raises:
            SmokeCheckFailure: Any file's parse duration exceeds the budget threshold.
        """

        corpus = _smoke_corpus(self.session, self.config)
        durations = [stat.duration.total_seconds() for stat in corpus.dagbag_stats]
        if len(durations) < PARSE_BUDGET_MINIMUM_FILES:
            LOGGER.info(
                f"Skipping the parse budget over {len(durations)} file(s); a relative "
                f"budget needs at least {PARSE_BUDGET_MINIMUM_FILES}"
            )
            return
        median = statistics.median(durations)
        threshold = max(self.ratio * median, PARSE_BUDGET_FLOOR_SECONDS)
        failures: list[str] = []
        for stat in corpus.dagbag_stats:
            seconds = stat.duration.total_seconds()
            if seconds > threshold:
                failures.append(
                    f"Dag file '{stat.file}' took {seconds:.3f}s to parse, exceeding the "
                    f"{threshold:.3f}s budget ({self.ratio:g} x the {median:.3f}s corpus "
                    f"median, floored at {PARSE_BUDGET_FLOOR_SECONDS:.1f}s); tune "
                    f"`airflow_dag_parse_budget_ratio` or move slow work out of module scope"
                )
        if failures:
            raise SmokeCheckFailure("\n\n".join(failures))

    def reportinfo(self) -> tuple[str, int, str]:
        """Locate this item for terminal and junit reporting.

        Returns:
            tuple[str, int, str] containing path, line, and title.
        """

        return self.nodeid, 0, self.name


class ForbidCatchupItem(pytest.Item):
    """Fail Dags that enable ``catchup`` and would backfill on unpause."""

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
        """Check every scheduled Dag's ``catchup`` flag.

        Unscheduled Dags are skipped: with no timetable producing runs there is nothing
        to backfill, so ``catchup=True`` is inert there rather than a production hazard.

        Raises:
            SmokeCheckFailure: A scheduled Dag enables ``catchup``.
        """

        dag_bag = _smoke_corpus(self.session, self.config)
        failures: list[str] = []
        for dag_id, dag in sorted(dag_bag.dags.items()):
            if not dag.catchup or not dag.can_be_scheduled:
                continue
            failures.append(
                f"Dag `{dag_id}` ('{dag.fileloc}') enables `catchup`; unpausing it "
                f"backfills every missed interval -- set `catchup=False` or disable "
                f"this check with `airflow_forbid_catchup = false`"
            )
        if failures:
            raise SmokeCheckFailure("\n\n".join(failures))

    def reportinfo(self) -> tuple[str, int, str]:
        """Locate this item for terminal and junit reporting.

        Returns:
            tuple[str, int, str] containing path, line, and title.
        """

        return self.nodeid, 0, self.name


class UnboundedExpandItem(pytest.Item):
    """Fail mapped tasks that expand over runtime data without a concurrency cap."""

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
        """Check every mapped task's expansion source and concurrency cap.

        Literal expansions are bounded by construction and pass; a task expanding over
        runtime data (XCom or task output) must carry ``max_active_tis_per_dag``, or one
        oversized upstream result fans out into an unbounded number of concurrent task
        instances.

        Raises:
            SmokeCheckFailure: A mapped task expands over runtime data without a cap.
        """

        dag_bag = _smoke_corpus(self.session, self.config)
        failures: list[str] = []
        for dag_id, dag in sorted(dag_bag.dags.items()):
            for task in dag.tasks:
                if (
                    task.is_mapped
                    and task.mapped_over_runtime_data
                    and task.max_active_tis_per_dag is None
                ):
                    failures.append(
                        f"Dag `{dag_id}` task `{task.task_id}` expands over runtime data "
                        f"without `max_active_tis_per_dag`; one oversized upstream result "
                        f"fans out unbounded -- set a cap on the mapped task"
                    )
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

        disabled = _disabled_smoke_items(self.config)

        if "test_dag_bag_integrity" not in disabled:
            yield DagBagIntegrityItem.from_parent(self, name="test_dag_bag_integrity")
        if "test_dag_serialization_roundtrip" not in disabled:
            yield DagSerializationRoundtripItem.from_parent(
                self, name="test_dag_serialization_roundtrip"
            )
        if "test_no_duplicate_dag_ids" not in disabled:
            yield NoDuplicateDagIdsItem.from_parent(self, name="test_no_duplicate_dag_ids")
        if "test_schedule_sanity" not in disabled:
            yield ScheduleSanityItem.from_parent(self, name="test_schedule_sanity")
        if "test_pool_references_exist" not in disabled:
            yield PoolReferencesExistItem.from_parent(
                self,
                name="test_pool_references_exist",
                pools=_pool_seeds(self.config),
            )

        if (
            _forbid_top_level_variable_access(self.config)
            and "test_no_top_level_variable_access" not in disabled
        ):
            yield TopLevelVariableAccessItem.from_parent(
                self, name="test_no_top_level_variable_access"
            )
        if _forbid_top_level_io(self.config) and "test_no_top_level_io" not in disabled:
            yield TopLevelIOItem.from_parent(
                self,
                name="test_no_top_level_io",
                io_modules=_top_level_io_modules(self.config),
            )
        budget_ratio = _dag_parse_budget_ratio(self.config)
        if budget_ratio is not None and "test_dag_parse_budget" not in disabled:
            yield DagParseBudgetItem.from_parent(
                self,
                name="test_dag_parse_budget",
                ratio=budget_ratio,
            )
        if _forbid_catchup(self.config) and "test_forbid_catchup" not in disabled:
            yield ForbidCatchupItem.from_parent(self, name="test_forbid_catchup")
        if _forbid_unbounded_expand(self.config) and "test_no_unbounded_expand" not in disabled:
            yield UnboundedExpandItem.from_parent(self, name="test_no_unbounded_expand")

        pattern = _dag_id_pattern(self.config)
        if pattern is not None and "test_dag_id_pattern" not in disabled:
            yield DagIdPatternItem.from_parent(
                self,
                name="test_dag_id_pattern",
                pattern=pattern,
            )
        required_tags = _required_dag_tags(self.config)
        if required_tags and "test_required_dag_tags" not in disabled:
            yield RequiredDagTagsItem.from_parent(
                self,
                name="test_required_dag_tags",
                tags=required_tags,
            )
        if _forbid_default_owner(self.config) and "test_forbid_default_owner" not in disabled:
            yield ForbidDefaultOwnerItem.from_parent(self, name="test_forbid_default_owner")
        snapshot_dir = _snapshot_dir(self.config)
        if snapshot_dir is not None and "test_dag_serialization_snapshot" not in disabled:
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
    "DagParseBudgetItem",
    "DagSerializationRoundtripItem",
    "ForbidCatchupItem",
    "ForbidDefaultOwnerItem",
    "NoDuplicateDagIdsItem",
    "PoolReferencesExistItem",
    "PoolSeed",
    "RequiredDagTagsItem",
    "ScheduleSanityItem",
    "SerializedDagEntry",
    "SerializedDagSnapshotItem",
    "SlowDagParseWarning",
    "SmokeCheckFailure",
    "SmokeCollector",
    "TopLevelIOItem",
    "TopLevelVariableAccessItem",
    "UnboundedExpandItem",
    "collect_smoke_items",
)
