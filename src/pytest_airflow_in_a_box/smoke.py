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
import json
import logging
import re
import statistics
import time
import warnings
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, Generic, TypeVar

import pytest

from pytest_airflow_in_a_box._compat.dag import _get_dag_serializer, time_restriction_type
from pytest_airflow_in_a_box._compat.db import create_session, get_pool_model
from pytest_airflow_in_a_box.antipatterns import (
    DEFAULT_TOP_LEVEL_IO_MODULES,
    find_io_calls,
    find_secrets_lookups,
    parse_dag_module,
)
from pytest_airflow_in_a_box.dagcorpus import (
    CorpusDag,
    DagCorpus,
    _parse_timeout,
    _select_serialization_sample,
    _serialization_sample_size,
    get_dag_corpus,
)
from pytest_airflow_in_a_box.fixtures.dagbag import _dag_folder
from pytest_airflow_in_a_box.parallel_dagbag import DUPLICATE_ID_MARKER

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence

    from _pytest._code.code import TerminalRepr

LOGGER = logging.getLogger(__name__)

SMOKE_ENABLED_KEY = pytest.StashKey[bool]()
SERIALIZED_DAG_CACHE_KEY = pytest.StashKey[dict[str, "SerializedDagEntry"]]()
DEFAULT_OWNER = "airflow"
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
class PoolSeed:
    """One consumer-defined pool seeded before `test_pool_references_exist` runs.

    Parameters:
        name: str containing the pool name.
        slots: int containing the pool's slot capacity.
    """

    name: str
    slots: int


@dataclass(frozen=True)
class _SnapshotPayload:
    """Resolved `test_dag_serialization_snapshot` configuration.

    Parameters:
        snapshot_dir: pathlib.Path containing the committed snapshot directory.
        update: bool indicating whether to regenerate rather than diff snapshots.
    """

    snapshot_dir: Path
    update: bool


class SlowDagParseWarning(RuntimeWarning):
    """Warn that a Dag file's parse duration crossed the slowpoke ratio."""


class SmokeColocationWarning(RuntimeWarning):
    """Warn that the smoke catalog could not share an xdist worker with `dag_bag`."""


SMOKE_CATALOG_FALLBACK_XDIST_GROUP: Final[str] = "pytest-airflow-in-a-box::smoke-catalog-fallback"
"""xdist group the smoke catalog falls back to when no `dag_corpus`/`dag_bag` anchor exists.

Keeps every bundled smoke item on one `--dist loadgroup` worker even when this run has no other
consumer to anchor onto, so `dagcorpus.get_dag_corpus`'s decoded `DagCorpus` is retained by at
most one worker instead of one per worker the catalog happens to be scheduled across.
"""


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


T = TypeVar("T")


def register_options(parser: pytest.Parser) -> None:
    """Register the bundled smoke catalog's command-line and ini options.

    Parameters:
        parser: pytest.Parser receiving the command-line and ini registrations.
    """

    group = parser.getgroup("airflow-in-a-box")
    group.addoption(
        "--airflow-smoke",
        action="store_true",
        default=None,
        dest="airflow_smoke",
        help="Enable the bundled opt-in `smoke` test catalog.",
    )
    parser.addini(
        "airflow_smoke",
        "Enable the bundled opt-in `smoke` test catalog.",
        type="bool",
        default=False,
    )
    parser.addini(
        "airflow_dag_parse_timeout",
        "Per-file Dag parse timeout in seconds for the smoke integrity test.",
        default="30",
    )
    parser.addini(
        "airflow_dag_parse_slowpoke_ratio",
        "Fraction of the parse timeout above which a file is a slowpoke.",
        default="0.75",
    )
    parser.addini(
        "airflow_dag_id_pattern",
        "Regex every collected dag_id must match, checked by the smoke catalog.",
        default="",
    )
    parser.addini(
        "airflow_required_dag_tags",
        "Tags every collected Dag must carry, checked by the smoke catalog.",
        type="linelist",
        default=[],
    )
    parser.addini(
        "airflow_smoke_disable",
        "Bundled smoke item names to drop from the catalog (e.g. `test_schedule_sanity`).",
        type="linelist",
        default=[],
    )
    parser.addini(
        "airflow_forbid_default_owner",
        "Fail Dags whose tasks are owned by the stock `airflow` owner.",
        type="bool",
        default=False,
    )
    group.addoption(
        "--airflow-smoke-update",
        action="store_true",
        default=None,
        dest="airflow_smoke_update",
        help="Regenerate committed Dag serialization snapshots instead of diffing them.",
    )
    parser.addini(
        "airflow_dag_snapshot_dir",
        "Directory of committed Dag serialization snapshots, checked by the smoke catalog.",
        default="",
    )
    parser.addini(
        "airflow_serialization_sample_size",
        "Number of Dags the serialization smoke checks cover; 0 covers every Dag.",
        default="0",
    )
    parser.addini(
        "airflow_serialization_sample_seed",
        "Seed for deterministic selection of the serialization smoke sample.",
        default="0",
    )
    parser.addini(
        "airflow_forbid_top_level_variable_access",
        "Fail Dag files that fetch Variables or Connections at import time.",
        type="bool",
        default=True,
    )
    parser.addini(
        "airflow_forbid_top_level_io",
        "Fail Dag files that call into known I/O modules at import time.",
        type="bool",
        default=True,
    )
    parser.addini(
        "airflow_top_level_io_modules",
        "Module prefixes the top-level I/O smoke check flags; replaces the built-in list.",
        type="linelist",
        default=[],
    )
    parser.addini(
        "airflow_dag_parse_budget_ratio",
        "Fail Dag files parsing slower than this multiple of the corpus median; 0 disables.",
        default="10",
    )
    parser.addini(
        "airflow_forbid_catchup",
        "Fail Dags that enable catchup.",
        type="bool",
        default=True,
    )
    parser.addini(
        "airflow_forbid_unbounded_expand",
        "Fail mapped tasks expanding over runtime data without max_active_tis_per_dag.",
        type="bool",
        default=True,
    )
    group.addoption(
        "--airflow-dag-bag-fanout",
        action="store_true",
        default=None,
        dest="airflow_dag_bag_fanout",
        help="Fan the smoke corpus's Dag parse out across subprocess workers.",
    )
    parser.addini(
        "airflow_dag_bag_fanout",
        "Fan the smoke corpus's Dag parse out across subprocess workers for large corpora.",
        type="bool",
        default=False,
    )
    parser.addini(
        "airflow_dag_bag_fanout_workers",
        "Subprocess worker count for Dag bag fan-out; 0 auto-selects a CPU-based default.",
        default="0",
    )
    parser.addini(
        "airflow_dag_bag_fanout_min_files",
        "Minimum discovered Dag file count below which fan-out is skipped even when enabled.",
        default="200",
    )
    parser.addini(
        "airflow_dag_bag_fanout_timeout",
        "Seconds the whole Dag bag fan-out may take before falling back to a serial parse.",
        default="600",
    )


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


# `_SMOKE_ITEM_NAMES` and `_SMOKE_ITEM_MARK_SETS` are derived from `SMOKE_CATALOG` near the
# bottom of this module, after every check's `run`/`enable` is defined. Both are referenced
# only from function bodies below (`_disabled_smoke_items`, `_markexpr_wants_smoke`), so the
# forward reference is safe -- neither is needed until collection time, long after the whole
# module has finished importing.

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

    dag_bag = get_dag_corpus(session, config)
    selected = _select_serialization_sample(config, dag_bag.dags)

    serialized_dag_class: Any | None = None
    entries: dict[str, SerializedDagEntry] = {}
    total = len(selected)
    for index, dag_id in enumerate(selected, start=1):
        dag = dag_bag.dags[dag_id]
        if isinstance(dag, CorpusDag):
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


def _run_dag_bag_integrity(context: SmokeContext, _enabled: bool) -> None:
    """Parse the configured Dag folder and enforce timeout and import-error policy.

    Parameters:
        context: SmokeContext bundling the session and config this check runs against.
        _enabled: bool, unused; `test_dag_bag_integrity` takes no configuration payload.

    Raises:
        SmokeCheckFailure: Any file failed to import or exceeded the parse timeout.
    """

    timeout = _parse_timeout(context.config)
    ratio = _slowpoke_ratio(context.config)
    dag_bag = context.corpus
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
                f"Dag file '{slowpoke}' exceeded {ratio:.0%} of the {timeout:.1f}s parse timeout"
            ),
            stacklevel=1,
        )

    if failures:
        raise SmokeCheckFailure("\n\n".join(failures) + f"\n\n{table}")


def _run_dag_serialization_roundtrip(context: SmokeContext, _enabled: bool) -> None:
    """Round-trip every cached serialized Dag and log a slowest-first timing table.

    Parameters:
        context: SmokeContext bundling the session and config this check runs against.
        _enabled: bool, unused; `test_dag_serialization_roundtrip` takes no payload.

    Raises:
        SmokeCheckFailure: Any Dag failed to round-trip through serialization.
    """

    entries = context.serialized_cache
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
            failures.append(f"Dag `{dag_id}` failed to round-trip through serialization: {error}")
            failed_ids.add(dag_id)
            continue
        deserialize_seconds[dag_id] = time.perf_counter() - started
        LOGGER.info(
            f"Round-tripped Dag `{dag_id}` in {deserialize_seconds[dag_id]:.3f}s ({index}/{total})"
        )
    table = _log_serialization_table(entries, deserialize_seconds, frozenset(failed_ids))
    if failures:
        raise SmokeCheckFailure("\n\n".join(failures) + f"\n\n{table}")


def _run_no_duplicate_dag_ids(context: SmokeContext, _enabled: bool) -> None:
    """Surface every duplicated ``dag_id`` collision the Dag bag recorded.

    Parameters:
        context: SmokeContext bundling the session and config this check runs against.
        _enabled: bool, unused; `test_no_duplicate_dag_ids` takes no payload.

    Raises:
        SmokeCheckFailure: Any Dag file was dropped for duplicating a ``dag_id``.
    """

    dag_bag = context.corpus
    failures = [
        f"'{path}': {message}"
        for path, message in sorted(dag_bag.import_errors.items())
        if DUPLICATE_ID_MARKER in message
    ]
    if failures:
        raise SmokeCheckFailure("\n\n".join(failures))


def _run_schedule_sanity(context: SmokeContext, _enabled: bool) -> None:
    """Compute the next scheduled run for every scheduled Dag in the serialized cache.

    Dags missing from a sampled cache are skipped. Serialization failures normally belong
    to ``test_dag_serialization_roundtrip``; when that item is disabled via
    ``airflow_smoke_disable``, this check reports them itself instead, so a Dag the
    scheduler cannot serialize does not silently pass just because its usual reporter
    is gone.

    Parameters:
        context: SmokeContext bundling the session and config this check runs against.
        _enabled: bool, unused; `test_schedule_sanity` takes no payload.

    Raises:
        SmokeCheckFailure: A scheduled Dag's timetable raised while computing its next run,
            or (only when ``test_dag_serialization_roundtrip`` is disabled) a scheduled Dag
            failed to serialize.
    """

    time_restriction_class = time_restriction_type()
    dag_bag = context.corpus
    entries = context.serialized_cache
    serialized_dag_class = _get_dag_serializer()
    roundtrip_disabled = "test_dag_serialization_roundtrip" in context.disabled
    failures: list[str] = []
    for dag_id, dag in sorted(dag_bag.dags.items()):
        can_be_scheduled = (
            dag.can_be_scheduled if isinstance(dag, CorpusDag) else dag.timetable.can_be_scheduled
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
            failures.append(f"Dag `{dag_id}` could not compute its next scheduled run: {error}")
    if failures:
        raise SmokeCheckFailure("\n\n".join(failures))


def _run_pool_references_exist(context: SmokeContext, pools: tuple[PoolSeed, ...]) -> None:
    """Seed configured pools, then resolve every task's pool against the database.

    Seeding is idempotent: a configured pool already present with the same slot
    count is treated as already seeded rather than an error, so this check stays
    safe to execute more than once against the same database -- under
    ``pytest-xdist --dist each``, every worker runs it, and a rerun tool
    may execute it again after a failure.

    Parameters:
        context: SmokeContext bundling the session and config this check runs against.
        pools: tuple[PoolSeed, ...] containing pools to seed before checking references.

    Raises:
        SmokeCheckFailure: A configured pool already exists with a different slot
            count, or a task references a pool the database does not contain.
    """

    pool_model = get_pool_model()
    dag_bag = context.corpus
    failures: list[str] = []
    with create_session() as database_session:
        existing: dict[str, int] = {
            name: slots
            for pool in pool_model.get_pools(session=database_session)
            if isinstance(name := pool.pool, str) and isinstance(slots := pool.slots, int)
        }
        conflicts = sorted(
            seed.name
            for seed in pools
            if seed.name in existing and existing[seed.name] != seed.slots
        )
        if conflicts:
            names = ", ".join(f"`{name}`" for name in conflicts)
            raise SmokeCheckFailure(
                f"`airflow_pools` cannot seed {names}; a pool with that name already "
                "exists with a different slot count. Remove it from `airflow_pools` "
                "or match its existing slots"
            )
        to_seed = [seed for seed in pools if seed.name not in existing]
        if to_seed:
            database_session.add_all(
                pool_model(pool=seed.name, slots=seed.slots, include_deferred=False)
                for seed in to_seed
            )
        known = set(existing) | {seed.name for seed in pools}
        for dag_id, dag in sorted(dag_bag.dags.items()):
            for task in dag.tasks:
                if task.pool not in known:
                    failures.append(
                        f"Dag `{dag_id}` task `{task.task_id}` references unknown pool "
                        f"`{task.pool}`"
                    )
    if failures:
        raise SmokeCheckFailure("\n\n".join(failures))


def _run_dag_id_pattern(context: SmokeContext, pattern: re.Pattern[str]) -> None:
    """Match every ``dag_id`` against the configured pattern.

    Parameters:
        context: SmokeContext bundling the session and config this check runs against.
        pattern: re.Pattern[str] every ``dag_id`` must match.

    Raises:
        SmokeCheckFailure: A ``dag_id`` does not match the configured pattern.
    """

    dag_bag = context.corpus
    failures = [
        f"Dag id `{dag_id}` does not match pattern `{pattern.pattern}`"
        for dag_id in sorted(dag_bag.dags)
        if not pattern.search(dag_id)
    ]
    if failures:
        raise SmokeCheckFailure("\n\n".join(failures))


def _enable_required_dag_tags(config: pytest.Config) -> frozenset[str] | None:
    """Resolve the required-tags check's payload, or report it disabled.

    Parameters:
        config: pytest.Config containing the ``airflow_required_dag_tags`` ini value.

    Returns:
        frozenset[str] | None containing the required tags, or ``None`` when the set is empty.
    """

    tags = _required_dag_tags(config)
    return tags if tags else None


def _run_required_dag_tags(context: SmokeContext, tags: frozenset[str]) -> None:
    """Check every Dag's tags for the required set.

    Parameters:
        context: SmokeContext bundling the session and config this check runs against.
        tags: frozenset[str] every Dag must carry.

    Raises:
        SmokeCheckFailure: A Dag is missing one or more required tags.
    """

    dag_bag = context.corpus
    failures: list[str] = []
    for dag_id, dag in sorted(dag_bag.dags.items()):
        missing = tags - dag.tags
        if missing:
            failures.append(f"Dag `{dag_id}` is missing required tags: {sorted(missing)}")
    if failures:
        raise SmokeCheckFailure("\n\n".join(failures))


def _enable_forbid_default_owner(config: pytest.Config) -> bool | None:
    """Report whether the default-owner check is enabled.

    Parameters:
        config: pytest.Config containing the ``airflow_forbid_default_owner`` ini value.

    Returns:
        bool | None containing ``True`` when the policy is active, else ``None``.
    """

    return True if _forbid_default_owner(config) else None


def _run_forbid_default_owner(context: SmokeContext, _enabled: bool) -> None:
    """Check every task's owner against Airflow's stock default.

    Parameters:
        context: SmokeContext bundling the session and config this check runs against.
        _enabled: bool, unused; `test_forbid_default_owner` takes no payload.

    Raises:
        SmokeCheckFailure: A task is owned by the stock `airflow` owner.
    """

    dag_bag = context.corpus
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


def _enable_dag_serialization_snapshot(config: pytest.Config) -> _SnapshotPayload | None:
    """Resolve the snapshot check's payload, or report it disabled.

    Parameters:
        config: pytest.Config containing the ``airflow_dag_snapshot_dir`` and
            ``--airflow-smoke-update`` values.

    Returns:
        _SnapshotPayload | None containing the resolved snapshot directory and update flag,
        or ``None`` when no snapshot directory is configured.
    """

    snapshot_dir = _snapshot_dir(config)
    if snapshot_dir is None:
        return None
    return _SnapshotPayload(snapshot_dir=snapshot_dir, update=_smoke_update(config))


def _run_dag_serialization_snapshot(context: SmokeContext, payload: _SnapshotPayload) -> None:
    """Diff, or regenerate, every cached serialized Dag's normalized snapshot.

    Parameters:
        context: SmokeContext bundling the session and config this check runs against.
        payload: _SnapshotPayload containing the committed snapshot directory and update flag.

    Raises:
        SmokeCheckFailure: A Dag failed to serialize, has no committed snapshot, or its
            serialized structure drifted from the committed snapshot.
    """

    entries = context.serialized_cache
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

        snapshot_path = payload.snapshot_dir / f"{dag_id}.json"
        if payload.update:
            payload.snapshot_dir.mkdir(parents=True, exist_ok=True)
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


def _corpus_source_files(corpus: DagCorpus, folder: Path) -> list[tuple[str, Path]]:
    """Resolve each parsed Dag file's display name and absolute path.

    Scanning the statistics paths covers exactly the files Airflow parsed, inheriting
    ``.airflowignore`` and underscore-prefix handling for free -- but releases differ in
    what the path holds. Airflow 3.2+ records a Dag-folder-relative path, while 3.1 and
    2.x strip ``settings.DAGS_FOLDER`` (the bootstrap folder) from the front, which is a
    no-op leaving the path absolute whenever the parsed folder is a configured
    ``--dag-folder``; 3.2+ also falls back to the absolute path for files outside the
    folder. Both shapes are resolved here rather than trusted.

    Parameters:
        corpus: DagCorpus containing the parsed Dag folder's file statistics.
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


def _enable_top_level_variable_access(config: pytest.Config) -> bool | None:
    """Report whether the top-level Variable/Connection access check is enabled.

    Parameters:
        config: pytest.Config containing the ``airflow_forbid_top_level_variable_access``
            ini value.

    Returns:
        bool | None containing ``True`` when the policy is active, else ``None``.
    """

    return True if _forbid_top_level_variable_access(config) else None


def _run_top_level_variable_access(context: SmokeContext, _enabled: bool) -> None:
    """Merge AST and runtime secrets-lookup findings over the parsed corpus.

    The AST pass reports direct top-level calls with exact locations; the runtime pass
    adds lookups hidden behind helpers, recorded while the corpus producer (or the
    `dag_bag` parse it reuses) filled the ``DagBag``. Runtime findings deduplicate
    against AST findings by file and call span, and degrade gracefully to AST-only for
    a ``DagBag`` parsed without instrumentation.

    Parameters:
        context: SmokeContext bundling the session and config this check runs against.
        _enabled: bool, unused; `test_no_top_level_variable_access` takes no payload.

    Raises:
        SmokeCheckFailure: Any Dag file performs an import-time secrets lookup.
    """

    corpus = context.corpus
    folder = context.dag_folder
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


def _enable_top_level_io(config: pytest.Config) -> tuple[str, ...] | None:
    """Resolve the top-level I/O check's payload, or report it disabled.

    Parameters:
        config: pytest.Config containing the ``airflow_forbid_top_level_io`` and
            ``airflow_top_level_io_modules`` values.

    Returns:
        tuple[str, ...] | None containing the configured module prefixes, or ``None`` when
        the policy is off.
    """

    if not _forbid_top_level_io(config):
        return None
    return _top_level_io_modules(config)


def _run_top_level_io(context: SmokeContext, io_modules: tuple[str, ...]) -> None:
    """Scan every parsed Dag file for import-time calls into configured I/O modules.

    Parameters:
        context: SmokeContext bundling the session and config this check runs against.
        io_modules: tuple[str, ...] containing the module prefixes to flag.

    Raises:
        SmokeCheckFailure: Any Dag file performs import-time network or database I/O.
    """

    corpus = context.corpus
    folder = context.dag_folder
    failures: list[str] = []
    for display, path in _corpus_source_files(corpus, folder):
        parsed = parse_dag_module(path)
        if parsed is None:
            continue
        for finding in find_io_calls(*parsed, io_modules):
            failures.append(
                f"Dag file '{display}' line {finding.line} calls `{finding.snippet}` at "
                f"import time; network or database I/O in top-level code runs on every "
                f"scheduler parse loop -- move it into task scope"
            )
    if failures:
        raise SmokeCheckFailure("\n\n".join(failures))


def _run_dag_parse_budget(context: SmokeContext, ratio: float) -> None:
    """Compare every file's parse duration against the relative budget threshold.

    The threshold is ``max(ratio * median, PARSE_BUDGET_FLOOR_SECONDS)``, so the check
    is independent of absolute CI speed and a near-zero median on a small fast corpus
    cannot fail on timing jitter. Below `PARSE_BUDGET_MINIMUM_FILES` parsed files the
    median is statistical noise and the check passes trivially.

    Parameters:
        context: SmokeContext bundling the session and config this check runs against.
        ratio: float containing the budget multiple of the corpus median.

    Raises:
        SmokeCheckFailure: Any file's parse duration exceeds the budget threshold.
    """

    corpus = context.corpus
    durations = [stat.duration.total_seconds() for stat in corpus.dagbag_stats]
    if len(durations) < PARSE_BUDGET_MINIMUM_FILES:
        LOGGER.info(
            f"Skipping the parse budget over {len(durations)} file(s); a relative "
            f"budget needs at least {PARSE_BUDGET_MINIMUM_FILES}"
        )
        return
    median = statistics.median(durations)
    threshold = max(ratio * median, PARSE_BUDGET_FLOOR_SECONDS)
    failures: list[str] = []
    for stat in corpus.dagbag_stats:
        seconds = stat.duration.total_seconds()
        if seconds > threshold:
            failures.append(
                f"Dag file '{stat.file}' took {seconds:.3f}s to parse, exceeding the "
                f"{threshold:.3f}s budget ({ratio:g} x the {median:.3f}s corpus "
                f"median, floored at {PARSE_BUDGET_FLOOR_SECONDS:.1f}s); tune "
                f"`airflow_dag_parse_budget_ratio` or move slow work out of module scope"
            )
    if failures:
        raise SmokeCheckFailure("\n\n".join(failures))


def _enable_forbid_catchup(config: pytest.Config) -> bool | None:
    """Report whether the catchup check is enabled.

    Parameters:
        config: pytest.Config containing the ``airflow_forbid_catchup`` ini value.

    Returns:
        bool | None containing ``True`` when the policy is active, else ``None``.
    """

    return True if _forbid_catchup(config) else None


def _run_forbid_catchup(context: SmokeContext, _enabled: bool) -> None:
    """Check every scheduled Dag's ``catchup`` flag.

    Unscheduled Dags are skipped: with no timetable producing runs there is nothing
    to backfill, so ``catchup=True`` is inert there rather than a production hazard.

    Parameters:
        context: SmokeContext bundling the session and config this check runs against.
        _enabled: bool, unused; `test_forbid_catchup` takes no payload.

    Raises:
        SmokeCheckFailure: A scheduled Dag enables ``catchup``.
    """

    dag_bag = context.corpus
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


def _enable_unbounded_expand(config: pytest.Config) -> bool | None:
    """Report whether the unbounded-expand check is enabled.

    Parameters:
        config: pytest.Config containing the ``airflow_forbid_unbounded_expand`` ini value.

    Returns:
        bool | None containing ``True`` when the policy is active, else ``None``.
    """

    return True if _forbid_unbounded_expand(config) else None


def _run_unbounded_expand(context: SmokeContext, _enabled: bool) -> None:
    """Check every mapped task's expansion source and concurrency cap.

    Literal expansions are bounded by construction and pass; a task expanding over
    runtime data (XCom or task output) must carry ``max_active_tis_per_dag``, or one
    oversized upstream result fans out into an unbounded number of concurrent task
    instances.

    Parameters:
        context: SmokeContext bundling the session and config this check runs against.
        _enabled: bool, unused; `test_no_unbounded_expand` takes no payload.

    Raises:
        SmokeCheckFailure: A mapped task expands over runtime data without a cap.
    """

    dag_bag = context.corpus
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


def _always_enabled(config: pytest.Config) -> bool | None:
    """Report an unconditional smoke check as always enabled.

    Parameters:
        config: pytest.Config, unused; satisfies the `SmokeCheck.enable` signature.

    Returns:
        bool | None always containing ``True``.
    """

    del config
    return True


@dataclass
class SmokeContext:
    """Runtime state one smoke check's `run` needs, computed lazily and cached per instance.

    Every property is computed on first access via the same functions the pre-catalog
    ``runtest`` bodies called directly, so a check that never touches e.g.
    `serialized_cache` never builds it -- unchanged from prior behavior. Overwriting a
    property directly (e.g. ``context.corpus = fake_corpus``) is the intended test seam: it
    replaces the cached value without touching `session`/`config`, no monkeypatching required.

    Parameters:
        session: pytest.Session used to reach cross-item process caches.
        config: pytest.Config containing plugin options and ini values.
    """

    session: pytest.Session
    config: pytest.Config

    @cached_property
    def corpus(self) -> DagCorpus:
        """Return the process-cached portable Dag corpus.

        Returns:
            DagCorpus containing every parsed Dag's portable smoke-check data.
        """

        return get_dag_corpus(self.session, self.config)

    @cached_property
    def dag_folder(self) -> Path:
        """Return the configured Dag folder.

        Returns:
            pathlib.Path containing the resolved Dag folder.
        """

        return _dag_folder(self.config)

    @cached_property
    def serialized_cache(self) -> dict[str, SerializedDagEntry]:
        """Return the sampled serialization outcome for every selected Dag.

        Returns:
            dict[str, SerializedDagEntry] mapping each selected ``dag_id`` to its outcome.
        """

        return _serialized_dag_cache(self.session, self.config)

    @cached_property
    def disabled(self) -> frozenset[str]:
        """Return the bundled smoke item names dropped from the catalog.

        Returns:
            frozenset[str] containing the disabled item names.
        """

        return _disabled_smoke_items(self.config)


@dataclass(frozen=True)
class SmokeCheck(Generic[T]):
    """One catalog entry: how to gate, mark, and run a single bundled smoke check.

    `enable` folds the check's own ini/CLI reading together with whatever gating condition
    (unconditional, a boolean flag, or a truthy/not-None payload check) used to live in
    `SmokeCollector.collect`'s if-chain -- returning ``None`` means the check is disabled this
    run, any other value is both "enabled" and the payload `run` receives.

    Parameters:
        name: str containing the item's collected node name (e.g. `test_forbid_catchup`).
        enable: Callable[[pytest.Config], T | None] resolving the check's payload, or ``None``
            when disabled.
        marks: frozenset[str] naming extra marks beyond the `smoke`/`timeout` pair every item
            carries (e.g. ``{"db_test"}``).
        run: Callable[[SmokeContext, T], None] executing the check; raises `SmokeCheckFailure`
            on failure.
    """

    name: str
    enable: Callable[[pytest.Config], T | None]
    marks: frozenset[str]
    run: Callable[[SmokeContext, T], None]


class _CatalogItem(pytest.Item):
    """Generic pytest Item running one `SmokeCheck` from `SMOKE_CATALOG`."""

    def __init__(
        self, *, name: str, parent: SmokeCollector, check: SmokeCheck[Any], payload: Any
    ) -> None:
        """Create the item, apply its marks, and store its check and resolved payload.

        Parameters:
            name: str containing the pytest item name.
            parent: SmokeCollector that collected this item.
            check: SmokeCheck[Any] naming the catalog entry this item runs.
            payload: Any containing the value `check.enable` resolved for this run.
        """

        super().__init__(name=name, parent=parent)
        self.check = check
        self.payload = payload
        self.add_marker(pytest.mark.smoke)
        self.add_marker(pytest.mark.timeout(_smoke_item_timeout(self.config)))
        for mark in check.marks:
            self.add_marker(getattr(pytest.mark, mark))

    def runtest(self) -> None:
        """Run this item's check against a freshly constructed `SmokeContext`.

        Raises:
            SmokeCheckFailure: The check failed.
        """

        self.check.run(SmokeContext(session=self.session, config=self.config), self.payload)

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


SMOKE_CATALOG: tuple[SmokeCheck[Any], ...] = (
    SmokeCheck(
        name="test_dag_bag_integrity",
        enable=_always_enabled,
        marks=frozenset({"db_test"}),
        run=_run_dag_bag_integrity,
    ),
    SmokeCheck(
        name="test_dag_serialization_roundtrip",
        enable=_always_enabled,
        marks=frozenset(),
        run=_run_dag_serialization_roundtrip,
    ),
    SmokeCheck(
        name="test_no_duplicate_dag_ids",
        enable=_always_enabled,
        marks=frozenset(),
        run=_run_no_duplicate_dag_ids,
    ),
    SmokeCheck(
        name="test_schedule_sanity",
        enable=_always_enabled,
        marks=frozenset(),
        run=_run_schedule_sanity,
    ),
    SmokeCheck(
        name="test_pool_references_exist",
        enable=_pool_seeds,
        marks=frozenset({"db_test"}),
        run=_run_pool_references_exist,
    ),
    SmokeCheck(
        name="test_no_top_level_variable_access",
        enable=_enable_top_level_variable_access,
        marks=frozenset(),
        run=_run_top_level_variable_access,
    ),
    SmokeCheck(
        name="test_no_top_level_io",
        enable=_enable_top_level_io,
        marks=frozenset(),
        run=_run_top_level_io,
    ),
    SmokeCheck(
        name="test_dag_parse_budget",
        enable=_dag_parse_budget_ratio,
        marks=frozenset(),
        run=_run_dag_parse_budget,
    ),
    SmokeCheck(
        name="test_forbid_catchup",
        enable=_enable_forbid_catchup,
        marks=frozenset(),
        run=_run_forbid_catchup,
    ),
    SmokeCheck(
        name="test_no_unbounded_expand",
        enable=_enable_unbounded_expand,
        marks=frozenset(),
        run=_run_unbounded_expand,
    ),
    SmokeCheck(
        name="test_dag_id_pattern",
        enable=_dag_id_pattern,
        marks=frozenset(),
        run=_run_dag_id_pattern,
    ),
    SmokeCheck(
        name="test_required_dag_tags",
        enable=_enable_required_dag_tags,
        marks=frozenset(),
        run=_run_required_dag_tags,
    ),
    SmokeCheck(
        name="test_forbid_default_owner",
        enable=_enable_forbid_default_owner,
        marks=frozenset(),
        run=_run_forbid_default_owner,
    ),
    SmokeCheck(
        name="test_dag_serialization_snapshot",
        enable=_enable_dag_serialization_snapshot,
        marks=frozenset(),
        run=_run_dag_serialization_snapshot,
    ),
)

# Derived from `SMOKE_CATALOG` rather than hand-maintained. `_disabled_smoke_items` validates
# `airflow_smoke_disable` against `_SMOKE_ITEM_NAMES`; `_markexpr_wants_smoke` evaluates a
# mark expression against every concrete mark combination a real item can carry.
_SMOKE_ITEM_NAMES: frozenset[str] = frozenset(check.name for check in SMOKE_CATALOG)

_SMOKE_ITEM_MARK_SETS: frozenset[frozenset[str]] = frozenset(
    frozenset({"smoke", "timeout"} | check.marks) for check in SMOKE_CATALOG
)


class SmokeCollector(pytest.Collector):
    """Collect the bundled smoke catalog directly on the pytest session."""

    def collect(self) -> Iterator[pytest.Item]:
        """Yield one item per enabled `SMOKE_CATALOG` entry.

        `enable` runs before the `disabled` check, not after, even though its result is
        about to be discarded for a disabled entry: `enable` is also this check's only ini
        validation, and a malformed value must be rejected even for a check the user has
        named in `airflow_smoke_disable` -- silently accepting it there would be a worse
        surprise than the wasted read.

        Returns:
            Iterator[pytest.Item] containing the collected smoke items.
        """

        disabled = _disabled_smoke_items(self.config)
        for check in SMOKE_CATALOG:
            payload = check.enable(self.config)
            if check.name in disabled or payload is None:
                continue
            yield _CatalogItem.from_parent(self, name=check.name, check=check, payload=payload)


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
    "SMOKE_CATALOG",
    "SMOKE_CATALOG_FALLBACK_XDIST_GROUP",
    "PoolSeed",
    "SerializedDagEntry",
    "SlowDagParseWarning",
    "SmokeCheck",
    "SmokeCheckFailure",
    "SmokeCollector",
    "SmokeColocationWarning",
    "SmokeContext",
    "collect_smoke_items",
    "register_options",
)
