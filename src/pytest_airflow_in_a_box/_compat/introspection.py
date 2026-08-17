"""Runtime Dag anti-pattern introspection over version-shifting Airflow surfaces.

Two probes back the smoke catalog's anti-pattern checks. ``record_secrets_lookups`` patches
every resolvable Variable/Connection secrets entry point while a ``DagBag`` fill runs and
records each import-time call -- the mechanism a user hand-rolled in apache/airflow#42205.
``mapped_expansion`` reads a mapped operator's expansion source and concurrency cap. Both are
capability-probed rather than release-gated: entry points and attributes that a given Airflow
release lacks are skipped or reported conservatively, never raised, because the module layout
shifts across the supported 2.x and 3.x families.

References:
    https://github.com/apache/airflow/issues/42205
    https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#top-level-python-code
    https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html
"""

from __future__ import annotations

import logging
import sys
import warnings
from contextlib import contextmanager
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

LOGGER = logging.getLogger(__name__)

# Every certified secrets entry point as `(kind, module, class, method)`. Targets that fail to
# resolve on the installed release are skipped per probe: the ORM rows exist on both families,
# the `airflow.sdk` rows only on 3.x, and upstream renames degrade recording rather than break
# the corpus build. The 3.x `BaseHook` home precedes the legacy `airflow.hooks.base` shim so a
# 3.x run patches the canonical class; when both resolve to the same class the duplicate is
# skipped.
SECRETS_PATCH_TARGETS: tuple[tuple[str, str, str, str], ...] = (
    ("variable", "airflow.models.variable", "Variable", "get"),
    ("variable", "airflow.sdk", "Variable", "get"),
    ("connection", "airflow.models.connection", "Connection", "get_connection_from_secrets"),
    ("connection", "airflow.sdk", "Connection", "get"),
    ("connection", "airflow.sdk.bases.hook", "BaseHook", "get_connection"),
    ("connection", "airflow.hooks.base", "BaseHook", "get_connection"),
)
# Keyword names the certified entry points spell their lookup key with.
_LOOKUP_KEY_KEYWORDS = ("key", "conn_id")
# Concrete types whose fan-out is already known when the Dag parses: scalars and containers
# with a length. Expanding over them is bounded by construction regardless of element types
# (a `date` or `Decimal` element does not make the expansion unbounded); anything else
# (`XComArg`, `MappedArgument`) resolves at run time.
_BOUNDED_EXPANSION_TYPES = (
    str,
    bytes,
    int,
    float,
    bool,
    type(None),
    list,
    tuple,
    set,
    frozenset,
    dict,
)
# Where a mapped operator stores its expansion source: classic operators use
# `expand_input`, TaskFlow's `DecoratedMappedOperator` moves the `.expand()` arguments to
# `op_kwargs_expand_input` and leaves `expand_input` empty.
_EXPAND_INPUT_ATTRIBUTES = ("expand_input", "op_kwargs_expand_input")


@dataclass(frozen=True)
class SecretsLookup:
    """One Variable or Connection lookup observed during a Dag folder parse.

    Parameters:
        kind: str naming the lookup kind, ``variable`` or ``connection``.
        key: str containing the looked-up key or connection id, or ``<unknown>``.
        file: str | None naming the Dag file whose import triggered the lookup, or ``None``
            when no stack frame under the Dag folder was found.
        line: int | None containing the triggering line, or ``None`` when unattributed.
    """

    kind: str
    key: str
    file: str | None
    line: int | None


def _resolve_target(
    module_name: str, class_name: str, attribute: str
) -> tuple[type, object] | None:
    """Resolve one patch target's class and its own attribute descriptor.

    The descriptor must live in the class's own ``__dict__``: patching an inherited attribute
    could not be restored faithfully, so such a target is skipped.

    Parameters:
        module_name: str containing the Airflow module path.
        class_name: str naming the class inside the module.
        attribute: str naming the method to patch.

    Returns:
        tuple[type, object] | None containing the class and original descriptor, or ``None``
        when the target does not resolve on the installed release.
    """

    try:
        with warnings.catch_warnings():
            # The legacy `airflow.hooks.base.BaseHook` resolves through a deprecation shim
            # on 3.x; a probe must neither surface that warning into every smoke run nor
            # lose the target under a consumer's `filterwarnings = error`.
            warnings.simplefilter("ignore")
            module = import_module(module_name)
            cls = getattr(module, class_name)
        descriptor = cls.__dict__[attribute]
    except Exception as error:
        LOGGER.debug(
            f"Skipping secrets patch target `{module_name}.{class_name}.{attribute}`: {error}"
        )
        return None
    return cls, descriptor


def _lookup_key(args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
    """Extract the looked-up key from an intercepted secrets call.

    Parameters:
        args: tuple[Any, ...] containing the intercepted positional arguments.
        kwargs: dict[str, Any] containing the intercepted keyword arguments.

    Returns:
        str containing the first string key found, or ``<unknown>``.
    """

    for name in _LOOKUP_KEY_KEYWORDS:
        value = kwargs.get(name)
        if isinstance(value, str):
            return value
    for arg in args:
        if isinstance(arg, str):
            return arg
    return "<unknown>"


def _caller_location(folder: Path, cache: dict[str, str | None]) -> tuple[str | None, int | None]:
    """Attribute an intercepted call to the innermost stack frame under the Dag folder.

    Walks the raw frame chain rather than ``traceback.extract_stack`` -- only file names
    and line numbers are needed, so eagerly fetching source lines for every frame on
    every intercepted call would be pure overhead during a large corpus parse.

    Parameters:
        folder: pathlib.Path containing the parsed Dag folder, already resolved.
        cache: dict[str, str | None] memoizing frame file names to their resolved path
            under the folder, or ``None`` for files outside it.

    Returns:
        tuple[str | None, int | None] containing the resolved Dag file path and line, or
        ``(None, None)`` when no frame under the folder exists.
    """

    frame = sys._getframe(1)
    while frame is not None:
        filename = frame.f_code.co_filename
        if filename not in cache:
            try:
                resolved = Path(filename).resolve()
                resolved.relative_to(folder)
            except (OSError, ValueError):
                cache[filename] = None
            else:
                cache[filename] = str(resolved)
        located = cache[filename]
        if located is not None:
            return located, frame.f_lineno
        frame = frame.f_back
    return None, None


def _recording_wrapper(
    kind: str,
    original: Callable[..., Any],
    folder: Path,
    recorded: list[SecretsLookup],
    cache: dict[str, str | None],
) -> Callable[..., Any]:
    """Build a pass-through wrapper that records each intercepted secrets call.

    Parameters:
        kind: str naming the lookup kind, ``variable`` or ``connection``.
        original: Callable[..., Any] containing the bound original entry point.
        folder: pathlib.Path containing the parsed Dag folder, already resolved.
        recorded: list[SecretsLookup] mutated with one entry per intercepted call.
        cache: dict[str, str | None] shared frame-file resolution memo.

    Returns:
        Callable[..., Any] recording the call, then delegating to the original.
    """

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        """Record the lookup, then call through so a dependent Dag still parses.

        Parameters:
            args: Any positional arguments of the intercepted call.
            kwargs: Any keyword arguments of the intercepted call.

        Returns:
            Any containing the original entry point's result.
        """

        file, line = _caller_location(folder, cache)
        recorded.append(
            SecretsLookup(kind=kind, key=_lookup_key(args, kwargs), file=file, line=line)
        )
        return original(*args, **kwargs)

    return wrapper


@contextmanager
def record_secrets_lookups(dag_folder: Path) -> Iterator[list[SecretsLookup]]:
    """Record every Variable and Connection lookup made while the body parses Dags.

    Every resolvable entry point in `SECRETS_PATCH_TARGETS` is replaced with a recording
    pass-through wrapper for the duration of the block and restored afterwards. Calls are
    recorded and then delegated, so a Dag that depends on the looked-up value still parses --
    the check reports the anti-pattern instead of breaking the corpus build.

    Parameters:
        dag_folder: pathlib.Path containing the Dag folder about to be parsed, used to
            attribute each lookup to a file and line.

    Returns:
        Iterator[list[SecretsLookup]] yielding the live recording list; it is fully populated
        once the block exits.
    """

    recorded: list[SecretsLookup] = []
    patched: list[tuple[type, str, object]] = []
    seen: set[tuple[int, str]] = set()
    folder = dag_folder.resolve()
    cache: dict[str, str | None] = {}
    try:
        for kind, module_name, class_name, attribute in SECRETS_PATCH_TARGETS:
            target = _resolve_target(module_name, class_name, attribute)
            if target is None:
                continue
            cls, descriptor = target
            if (id(cls), attribute) in seen:
                continue
            seen.add((id(cls), attribute))
            wrapper = _recording_wrapper(kind, getattr(cls, attribute), folder, recorded, cache)
            setattr(cls, attribute, staticmethod(wrapper))
            patched.append((cls, attribute, descriptor))
        yield recorded
    finally:
        for cls, attribute, descriptor in patched:
            setattr(cls, attribute, descriptor)


def _expands_over_runtime_data(value: Any) -> bool:
    """Report whether one expansion input resolves at run time rather than parse time.

    A mapped operator's primary expansion input is a mapping of keyword name to source;
    each source is judged on its own. A source that is a concrete container has a known
    length already, so the fan-out is bounded by construction whatever its elements hold;
    only a non-container source such as an ``XComArg`` is runtime data.

    Parameters:
        value: Any containing one expansion input's ``value``.

    Returns:
        bool indicating any expansion source resolves at run time.
    """

    if isinstance(value, dict):
        return any(not isinstance(item, _BOUNDED_EXPANSION_TYPES) for item in value.values())
    return not isinstance(value, _BOUNDED_EXPANSION_TYPES)


def mapped_expansion(task: Any) -> tuple[bool, bool, int | None]:
    """Read one task's dynamic-mapping expansion source and concurrency cap.

    Duck-typed by design: `is_mapped` is stable across the supported releases, while the
    expansion internals (`expand_input`, `partial_kwargs`) may drift, so any inspection
    failure downgrades the task to unmapped with a warning rather than producing a false
    failure.

    Parameters:
        task: Any containing a live Airflow operator.

    Returns:
        tuple[bool, bool, int | None] containing whether the task is mapped, whether it
        expands over runtime data rather than literals, and its ``max_active_tis_per_dag``
        cap when set.
    """

    try:
        if not bool(getattr(task, "is_mapped", False)):
            return False, False, None
        values = []
        for attribute in _EXPAND_INPUT_ATTRIBUTES:
            expand_input = getattr(task, attribute, None)
            if expand_input is not None:
                values.append(getattr(expand_input, "value", None))
        over_runtime_data = any(_expands_over_runtime_data(value) for value in values)
        cap = getattr(task, "max_active_tis_per_dag", None)
        if cap is None:
            partial_kwargs = getattr(task, "partial_kwargs", None)
            if isinstance(partial_kwargs, dict):
                cap = partial_kwargs.get("max_active_tis_per_dag")
        return True, over_runtime_data, cap if isinstance(cap, int) else None
    except Exception as error:
        task_id = getattr(task, "task_id", "<unknown>")
        LOGGER.warning(f"Could not inspect mapped expansion for task `{task_id}`: {error}")
        return False, False, None


__all__ = (
    "SECRETS_PATCH_TARGETS",
    "SecretsLookup",
    "mapped_expansion",
    "record_secrets_lookups",
)
