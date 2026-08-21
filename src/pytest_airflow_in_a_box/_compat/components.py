"""Static conformance checks for custom timetables, listeners, and executors.

`Timetable` is a `typing.Protocol` and `BaseExecutor` is not an ABC, and a listener
carries no base class at all, so nothing about these three shapes is enforced at
class-creation time -- a bug ships and only fails once a scheduler exercises it. Every
checker here is a pure function over a class or instance: no Airflow bootstrap, metadata
database, or cache is touched, and Airflow itself is imported only inside a checker body,
never at module scope.

The registry is a flat, appendable list of `(kind, check_name, checker)` rows.
`pytest_airflow_in_a_box.components.check_component` iterates it generically -- filtering
by an explicit kind, or by `KIND_CLASSIFIERS` when none is given -- so a follow-up phase
adds more checks purely by appending rows here, never by touching the dispatch loop.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/timetable.html
    https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/listeners.html
    https://pluggy.readthedocs.io/en/stable/
"""

from __future__ import annotations

import inspect
import json
from dataclasses import dataclass
from importlib import import_module
from typing import TYPE_CHECKING

from pytest_airflow_in_a_box._compat.capabilities import (
    AirflowCompatibilityError,
    AirflowFamily,
    ExecutorContract,
    installed_family,
    resolve_capabilities,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

# Bare strings, not `pytest_airflow_in_a_box.components.ComponentKind` members: importing
# the public enum here would import this module's own consumer, a cycle. The public
# `ComponentKind` values match these exactly; `tests/test_components.py` pins the match.
TIMETABLE = "timetable"
LISTENER = "listener"
EXECUTOR = "executor"


@dataclass(frozen=True)
class ComponentProblem:
    """One conformance problem found on a checked component.

    Parameters:
        code: str naming the check that found this problem, e.g.
            `timetable-local-qualname`.
        message: str describing what is wrong, specific to the checked component.
        hint: str describing how to fix it.
    """

    code: str
    message: str
    hint: str


def _as_type(component: object) -> type:
    """Normalize a checked component to its class without ever instantiating it.

    Every checker accepts a bare class or an already-built instance interchangeably: a
    check must never construct a `Timetable`, listener, or `BaseExecutor` itself, since
    a real constructor may have side effects or required arguments this module cannot
    guess.

    Parameters:
        component: object containing the class or instance under check.

    Returns:
        type containing `component` itself when it is already a class, else its class.
    """

    return component if isinstance(component, type) else type(component)


def _defining_class(component_type: type, name: str) -> type | None:
    """Find which class in the MRO first defines an attribute in its own `__dict__`.

    Walking the MRO rather than comparing resolved (possibly descriptor-bound) attribute
    identity handles `classmethod`-wrapped attributes like `Timetable.deserialize`
    correctly: accessing a classmethod through the class rebinds it on every access, so
    identity comparison across two accesses is unreliable, while checking which class's
    own `__dict__` owns the name is not.

    Parameters:
        component_type: type containing the class to search.
        name: str naming the attribute to locate.

    Returns:
        type | None containing the first class in MRO order whose own `__dict__`
        defines `name`, or None when no class in the MRO defines it.
    """

    for klass in component_type.__mro__:
        if name in vars(klass):
            return klass
    return None


# ---------------------------------------------------------------------------
# Timetable checks
# ---------------------------------------------------------------------------

TIMETABLE_LOCAL_QUALNAME = "timetable-local-qualname"
TIMETABLE_MISSING_PROTOCOL_METHOD = "timetable-missing-protocol-method"
TIMETABLE_SERIALIZE_PAIR_INCOMPLETE = "timetable-serialize-pair-incomplete"
TIMETABLE_SERIALIZE_NOT_JSON = "timetable-serialize-not-json"

# The two `Timetable` Protocol methods a timetable always needs: their default body
# raises `NotImplementedError` unconditionally, and the scheduler calls both for every
# DagRun regardless of `partitioned`. Verified against `airflow.timetables.base.Timetable`
# on the installed 3.3.0; see `PROVENANCE.md`.
_TIMETABLE_REQUIRED_METHODS = ("infer_manual_data_interval", "next_dagrun_info")
# Required only when the checked timetable sets `partitioned = True`: the scheduler calls
# these two conditionally, but their default body raises `NotImplementedError`
# unconditionally the moment either is actually called, same as the pair above. Verified
# against the installed 3.3.0; see `PROVENANCE.md`.
_TIMETABLE_PARTITIONED_REQUIRED_METHODS = ("get_partition_mapper", "iter_partition_dagrun_infos")
_TIMETABLE_METHOD_HINTS: dict[str, str] = {
    "infer_manual_data_interval": "the scheduler calls it for every manually triggered DagRun.",
    "next_dagrun_info": "the scheduler calls it for every DagRun this timetable schedules.",
    "get_partition_mapper": (
        "the scheduler calls it for every partitioned asset this timetable references, "
        "since `partitioned` is True."
    ),
    "iter_partition_dagrun_infos": (
        "the scheduler calls it to enumerate partition runs, since `partitioned` is True."
    ),
}


def _check_timetable_local_qualname(component: object) -> Iterable[ComponentProblem]:
    """Flag a timetable class defined inside a function, method, or other local scope.

    Parameters:
        component: object containing the timetable class or instance under check.

    Returns:
        Iterable[ComponentProblem] containing at most one problem.
    """

    component_type = _as_type(component)
    if "<locals>" not in component_type.__qualname__:
        return
    yield ComponentProblem(
        code=TIMETABLE_LOCAL_QUALNAME,
        message=(
            f"`{component_type.__qualname__}` is defined inside a function, method, or "
            f"other local scope, so its `__qualname__` contains `<locals>`."
        ),
        hint=(
            "Move the timetable class to module level. Airflow's "
            "`find_registered_custom_timetable` matches a custom timetable by qualified "
            "name; a `<locals>` class can never match, so every DagRun that uses it "
            "raises `TimetableNotRegistered` permanently, not just in this test."
        ),
    )


def _check_timetable_missing_protocol_method(component: object) -> Iterable[ComponentProblem]:
    """Flag a timetable that never overrides a method the Protocol default cannot serve.

    `get_partition_mapper`/`iter_partition_dagrun_infos` join the required set only when
    the checked component sets `partitioned = True`.

    Parameters:
        component: object containing the timetable class or instance under check.

    Returns:
        Iterable[ComponentProblem] containing at most one problem per missing method.
    """

    from airflow.timetables.base import Timetable

    component_type = _as_type(component)
    required = _TIMETABLE_REQUIRED_METHODS
    if getattr(component, "partitioned", False):
        required += _TIMETABLE_PARTITIONED_REQUIRED_METHODS
    for name in required:
        defining = _defining_class(component_type, name)
        if defining is not None and defining is not Timetable:
            continue
        yield ComponentProblem(
            code=TIMETABLE_MISSING_PROTOCOL_METHOD,
            message=f"`{component_type.__name__}` does not override `{name}`.",
            hint=(
                f"`Timetable.{name}`'s default implementation raises "
                f"`NotImplementedError()`. Implement `{name}` on "
                f"`{component_type.__name__}` -- {_TIMETABLE_METHOD_HINTS[name]}"
            ),
        )


def _check_timetable_serialize_pair_incomplete(component: object) -> Iterable[ComponentProblem]:
    """Flag a timetable that overrides exactly one of `serialize`/`deserialize`.

    An override inherited from Airflow's own shipped timetables does not count as a user
    override: `airflow.timetables.simple.NullTimetable`, for example, redefines
    `deserialize` identically to the Protocol default and inherits `serialize` -- a
    complete, correct pair Airflow itself ships, not a gap in a `NullTimetable` subclass.
    Only a definition outside the `airflow.` package (including `airflow.providers.*`)
    counts. Verified against the installed 3.3.0; see `PROVENANCE.md`.

    Parameters:
        component: object containing the timetable class or instance under check.

    Returns:
        Iterable[ComponentProblem] containing at most one problem.
    """

    from airflow.timetables.base import Timetable

    component_type = _as_type(component)

    def _user_overridden(name: str) -> bool:
        defining = _defining_class(component_type, name)
        if defining is None or defining is Timetable:
            return False
        return not defining.__module__.startswith("airflow.")

    serialize_overridden = _user_overridden("serialize")
    deserialize_overridden = _user_overridden("deserialize")
    if serialize_overridden == deserialize_overridden:
        return
    implemented, missing = (
        ("serialize", "deserialize") if serialize_overridden else ("deserialize", "serialize")
    )
    yield ComponentProblem(
        code=TIMETABLE_SERIALIZE_PAIR_INCOMPLETE,
        message=f"`{component_type.__name__}` overrides `{implemented}` but not `{missing}`.",
        hint=(
            f"Implement `{missing}` too, or remove the `{implemented}` override. "
            f"`Timetable.deserialize`'s default reconstructs the class with `cls()`, "
            f"silently dropping whatever state a custom `serialize` emits; the reverse "
            f"gap silently discards a custom `deserialize`'s extra fields on the next "
            f"parse."
        ),
    )


def _check_timetable_serialize_not_json(component: object) -> Iterable[ComponentProblem]:
    """Flag a timetable instance whose `serialize()` does not return a JSON-safe dict.

    Only runs against an already-built instance: calling `serialize()` on a bare class
    would require constructing one, which this module never does. A class-only check
    (`kind=ComponentKind.TIMETABLE` against a class) simply skips this one check.

    Parameters:
        component: object containing the timetable class or instance under check.

    Returns:
        Iterable[ComponentProblem] containing at most one problem.
    """

    if isinstance(component, type):
        return
    serialize = getattr(component, "serialize", None)
    if not callable(serialize):
        return
    try:
        payload = serialize()
        if not isinstance(payload, dict):
            raise TypeError(f"expected a dict, got {type(payload).__name__}")
        json.dumps(payload)
    except Exception as error:
        yield ComponentProblem(
            code=TIMETABLE_SERIALIZE_NOT_JSON,
            message=(
                f"`{type(component).__name__}.serialize()` did not return a "
                f"JSON-serializable dict ({type(error).__name__}: {error})."
            ),
            hint=(
                "`serialize()`'s return value is stored in the metadata database as "
                "JSON and fed into `deserialize(cls, data: dict[str, Any])`. Return a "
                "dict with only JSON-safe values (str, int, float, bool, None, list, "
                "dict)."
            ),
        )


# ---------------------------------------------------------------------------
# Listener checks
# ---------------------------------------------------------------------------

LISTENER_NO_MATCHING_HOOKSPEC = "listener-no-matching-hookspec"
LISTENER_UNKNOWN_ARGUMENT = "listener-unknown-argument"
LISTENER_CORE_MANAGER_ONLY = "listener-core-manager-only"
LISTENER_SDK_MANAGER_ONLY = "listener-sdk-manager-only"

# `airflow.listeners.listener.get_listener_manager` registers all five; the Task SDK's
# `airflow.sdk.listener.get_listener_manager` (3.2+ only -- see
# `AirflowCapabilities.sdk_listener_manager_available`) registers only the first two. On
# 3.2+, `lifecycle` and `taskinstance` are physically duplicated between the core and SDK
# packages (`airflow._shared.listeners.spec.*` vs `airflow.sdk._shared.listeners.spec.*`)
# rather than shared, but their hookspec names and signatures are identical. On 3.1.x the
# `_shared` split does not exist at all -- `airflow._shared.listeners` is not importable --
# and the one and only manager builds all five hookspecs directly from
# `airflow.listeners.spec.*`, `lifecycle`/`taskinstance` included; those two legacy
# candidates are listed after the `_shared` ones and are simply skipped by
# `_listener_hookspecs` wherever the `_shared` split exists instead. Verified against the
# installed 3.3.0 and against 3.1.0/3.2.0 in isolated environments; see `PROVENANCE.md`.
_CORE_LISTENER_SPEC_MODULES = (
    "airflow._shared.listeners.spec.lifecycle",
    "airflow._shared.listeners.spec.taskinstance",
    "airflow.listeners.spec.lifecycle",
    "airflow.listeners.spec.taskinstance",
    "airflow.listeners.spec.dagrun",
    "airflow.listeners.spec.asset",
    "airflow.listeners.spec.importerrors",
)
_SDK_LISTENER_SPEC_MODULES = (
    "airflow.sdk._shared.listeners.spec.lifecycle",
    "airflow.sdk._shared.listeners.spec.taskinstance",
)


def _listener_marker_attribute(name: str) -> str:
    """Build the pluggy marker attribute name pluggy stamps onto a decorated function.

    Both `airflow.listeners.hookimpl` and every `hookspec` in Airflow's own listener spec
    modules are built from a `pluggy.HookimplMarker("airflow")` /
    `pluggy.HookspecMarker("airflow")` pair, so both stamp the project name `airflow` --
    derived here from the real `hookimpl` rather than hardcoded, so a future rename of
    Airflow's pluggy project name changes this too instead of silently going stale.

    Parameters:
        name: str containing `"impl"` or `"spec"`.

    Returns:
        str containing the attribute name pluggy stamps for that marker kind.
    """

    from airflow.listeners import hookimpl

    return f"{hookimpl.project_name}_{name}"


def _pluggy_argnames(func: Callable[..., object]) -> tuple[str, ...]:
    """Extract the parameter names pluggy's own `varnames()` would validate.

    Mirrors `pluggy._hooks.varnames`: only `POSITIONAL_ONLY`/`POSITIONAL_OR_KEYWORD`
    parameters are ever compared against a hookspec by pluggy -- a hookimpl's own
    `**kwargs`, `*args`, and keyword-only parameters are invisible to pluggy's validation,
    and a defaulted parameter is split off into a group pluggy never validates against the
    hookspec at all, so recording it here would false-positive `listener-unknown-argument`
    on pluggy-legal code. `check_component` never binds an instance, so unlike pluggy
    itself this cannot detect a genuinely bound method; it reproduces pluggy's own
    fallback for that case instead -- strip a first parameter literally named `self` on a
    function whose qualified name shows it was defined inside a class body. Verified
    against `pluggy` 1.6.0's `varnames()` source directly.

    Parameters:
        func: Callable[..., object] containing the raw function to inspect.

    Returns:
        tuple[str, ...] containing the parameter names pluggy would require to match a
        hookspec, in declaration order.
    """

    valid_kinds = (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    valid_params = [
        parameter
        for parameter in inspect.signature(func).parameters.values()
        if parameter.kind in valid_kinds
    ]
    required = tuple(
        parameter.name
        for parameter in valid_params
        if parameter.default is inspect.Parameter.empty
    )
    qualname = getattr(func, "__qualname__", "")
    if required and "." in qualname and required[0] == "self":
        required = required[1:]
    return required


def _listener_hookspecs(module_names: tuple[str, ...]) -> dict[str, tuple[str, ...]]:
    """Map each `@hookspec` function name in the given modules to its validated parameters.

    A module that fails to import is skipped rather than raised: these listener checks
    deliberately validate against the light, non-raising `installed_family()` rather than
    a certified release, so an uncertified installed release renaming or removing one of
    these private modules degrades the check conservatively instead of raising out of
    `check_component`, mirroring every other capability probe in `_compat/`.

    Parameters:
        module_names: tuple[str, ...] containing hookspec module import paths.

    Returns:
        dict[str, tuple[str, ...]] mapping hookspec function name to the parameter names
        pluggy would validate, in declaration order.
    """

    marker = _listener_marker_attribute("spec")
    specs: dict[str, tuple[str, ...]] = {}
    for module_name in module_names:
        try:
            module = import_module(module_name)
        except ImportError:
            continue
        for name, value in vars(module).items():
            if callable(value) and hasattr(value, marker):
                specs[name] = _pluggy_argnames(value)
    return specs


@dataclass(frozen=True)
class _HookimplInfo:
    """One hookimpl method's effective hookspec name and pluggy-validated parameters.

    Parameters:
        method_name: str naming the Python method as the class defines it, used in
            problem messages so they point at the code the user actually wrote.
        hookspec_name: str naming the hookspec this method is registered against --
            `@hookimpl(specname=...)` when given, else `method_name`.
        params: tuple[str, ...] containing the parameter names pluggy itself would
            validate against that hookspec; see `_pluggy_argnames`.
    """

    method_name: str
    hookspec_name: str
    params: tuple[str, ...]


def _listener_hookimpls(component_type: type) -> tuple[_HookimplInfo, ...]:
    """Collect every `@hookimpl`-decorated method's effective name and validated parameters.

    Parameters:
        component_type: type containing the listener class under check.

    Returns:
        tuple[_HookimplInfo, ...] containing one entry per hookimpl-marked method, in
        `inspect.getmembers` order (alphabetical by method name).
    """

    marker = _listener_marker_attribute("impl")
    infos: list[_HookimplInfo] = []
    for name, value in inspect.getmembers(component_type, callable):
        opts = getattr(value, marker, None)
        if opts is None:
            continue
        hookspec_name = opts.get("specname") or name
        infos.append(
            _HookimplInfo(
                method_name=name, hookspec_name=hookspec_name, params=_pluggy_argnames(value)
            )
        )
    return tuple(infos)


def _check_listener_no_matching_hookspec(component: object) -> Iterable[ComponentProblem]:
    """Flag a hookimpl method whose name matches no hookspec in either manager.

    Parameters:
        component: object containing the listener class or instance under check.

    Returns:
        Iterable[ComponentProblem] containing at most one problem per unmatched method.
    """

    if installed_family() is not AirflowFamily.V3:
        return
    component_type = _as_type(component)
    impls = _listener_hookimpls(component_type)
    if not impls:
        return
    known = set(_listener_hookspecs(_CORE_LISTENER_SPEC_MODULES)) | set(
        _listener_hookspecs(_SDK_LISTENER_SPEC_MODULES)
    )
    for info in sorted(impls, key=lambda item: item.method_name):
        if info.hookspec_name in known:
            continue
        yield ComponentProblem(
            code=LISTENER_NO_MATCHING_HOOKSPEC,
            message=(
                f"`{component_type.__name__}.{info.method_name}` matches no hookspec "
                f"registered by either `airflow.listeners.listener` or "
                f"`airflow.sdk.listener`."
            ),
            hint=(
                "pluggy silently ignores a hookimpl matching no hookspec -- this method "
                "never fires, with no warning. Check the method name (or `specname=`) "
                "against `airflow.listeners.spec.*` and the Task SDK's listener "
                "hookspecs."
            ),
        )


def _check_listener_unknown_argument(component: object) -> Iterable[ComponentProblem]:
    """Flag a hookimpl method that declares an argument its hookspec does not have.

    Parameters:
        component: object containing the listener class or instance under check.

    Returns:
        Iterable[ComponentProblem] containing at most one problem per offending method.
    """

    if installed_family() is not AirflowFamily.V3:
        return
    component_type = _as_type(component)
    impls = _listener_hookimpls(component_type)
    if not impls:
        return
    specs = {
        **_listener_hookspecs(_SDK_LISTENER_SPEC_MODULES),
        **_listener_hookspecs(_CORE_LISTENER_SPEC_MODULES),
    }
    for info in sorted(impls, key=lambda item: item.method_name):
        hookspec_params = specs.get(info.hookspec_name)
        if hookspec_params is None:
            continue
        unknown = [param for param in info.params if param not in hookspec_params]
        if not unknown:
            continue
        yield ComponentProblem(
            code=LISTENER_UNKNOWN_ARGUMENT,
            message=(
                f"`{component_type.__name__}.{info.method_name}` declares argument(s) "
                f"{unknown} that `{info.hookspec_name}`'s hookspec does not accept."
            ),
            hint=(
                f"pluggy hard-errors at registration time on an unknown hookimpl "
                f"argument name. `{info.hookspec_name}` accepts: {list(hookspec_params)}."
            ),
        )


def _check_listener_manager_scope(
    component: object,
    *,
    this_scope: tuple[str, ...],
    other_scope: tuple[str, ...],
    code: str,
    other_manager: str,
) -> Iterable[ComponentProblem]:
    """Flag a hookimpl method whose hookspec exists only in one manager's registry.

    On 3.1.x, `other_scope` resolves to no hookspecs at all when it names the SDK
    manager's modules, since that manager does not exist there (see
    `AirflowCapabilities.sdk_listener_manager_available`) -- not because it exists but
    happens not to register this particular hookspec, which is the normal 3.2+ case this
    check is for. An empty `other_scope` is therefore treated as "no second manager to be
    unreachable from" and skipped entirely, rather than as every hookspec in `this_scope`
    trivially qualifying.

    Parameters:
        component: object containing the listener class or instance under check.
        this_scope: tuple[str, ...] containing the hookspec modules naming methods that
            are unreachable through `other_manager`.
        other_scope: tuple[str, ...] containing the other manager's hookspec modules.
        code: str naming the problem code to report.
        other_manager: str naming the manager entry point that cannot reach these hooks.

    Returns:
        Iterable[ComponentProblem] containing at most one problem per offending method.
    """

    if installed_family() is not AirflowFamily.V3:
        return
    component_type = _as_type(component)
    impls = _listener_hookimpls(component_type)
    if not impls:
        return
    other_hookspecs = set(_listener_hookspecs(other_scope))
    if not other_hookspecs:
        return
    only_this = set(_listener_hookspecs(this_scope)) - other_hookspecs
    for info in sorted(impls, key=lambda item: item.method_name):
        if info.hookspec_name not in only_this:
            continue
        yield ComponentProblem(
            code=code,
            message=(
                f"`{component_type.__name__}.{info.method_name}` has no hookspec in "
                f"`{other_manager}`."
            ),
            hint=(
                f"Registering `{component_type.__name__}` only with `{other_manager}` "
                f"never fires `{info.method_name}`. `airflow.listeners.listener` "
                f"registers lifecycle, taskinstance, dagrun, asset, and import-error "
                f"hookspecs; `airflow.sdk.listener` registers only lifecycle and "
                f"taskinstance."
            ),
        )


def _check_listener_core_manager_only(component: object) -> Iterable[ComponentProblem]:
    """Flag a hookimpl reachable only through the core listener manager.

    Parameters:
        component: object containing the listener class or instance under check.

    Returns:
        Iterable[ComponentProblem] containing at most one problem per offending method.
    """

    yield from _check_listener_manager_scope(
        component,
        this_scope=_CORE_LISTENER_SPEC_MODULES,
        other_scope=_SDK_LISTENER_SPEC_MODULES,
        code=LISTENER_CORE_MANAGER_ONLY,
        other_manager="airflow.sdk.listener",
    )


def _check_listener_sdk_manager_only(component: object) -> Iterable[ComponentProblem]:
    """Flag a hookimpl reachable only through the Task SDK listener manager.

    Parameters:
        component: object containing the listener class or instance under check.

    Returns:
        Iterable[ComponentProblem] containing at most one problem per offending method.
    """

    yield from _check_listener_manager_scope(
        component,
        this_scope=_SDK_LISTENER_SPEC_MODULES,
        other_scope=_CORE_LISTENER_SPEC_MODULES,
        code=LISTENER_SDK_MANAGER_ONLY,
        other_manager="airflow.listeners.listener",
    )


# ---------------------------------------------------------------------------
# Executor checks
# ---------------------------------------------------------------------------

EXECUTOR_MISSING_OVERRIDE = "executor-missing-override"
EXECUTOR_STALE_ATTRIBUTE = "executor-stale-attribute"
EXECUTOR_FLAG_WRONG_TYPE = "executor-flag-wrong-type"

# `sync` and `_process_workloads` exist on `BaseExecutor` across the whole certified
# 3.1-3.3 range with a body that is either a silent no-op (`sync`) or a
# `raise NotImplementedError` (`_process_workloads`); neither is abstract, so nothing
# forces a subclass to override them. Verified against the installed 3.3.0, plus 3.1.0
# and 3.2.0 in isolated environments; see `PROVENANCE.md`.
_EXECUTOR_REQUIRED_OVERRIDES = ("sync", "_process_workloads")
_OVERRIDE_HINTS: dict[str, str] = {
    "sync": (
        "`BaseExecutor.sync`'s default body does nothing; the heartbeat loop calls it "
        "but this executor never reports status back."
    ),
    "_process_workloads": (
        "`BaseExecutor._process_workloads`'s default body raises `NotImplementedError`; "
        "the scheduler calls it for every batch of queued work."
    ),
}

# Verified absent from `BaseExecutor` on the installed 3.3.0, plus 3.1.0 and 3.2.0 in
# isolated environments (see `PROVENANCE.md`): `is_single_threaded`, `supports_pickling`,
# and `change_sensitivity` are still documented in older material but Airflow silently
# ignores them now. `execute_async` is grouped here rather than under
# `executor-missing-override` even though the issue that scoped this phase named it
# there: it was the pre-3.0 task-dispatch entry point and is confirmed absent from
# `BaseExecutor` on every certified 3.1-3.3 release, superseded by the workload-based
# `queue_workload`/`_process_workloads` pair, so there is nothing on 3.x for a subclass
# to "miss" -- the scheduler never looks for this name at all, which is exactly the
# stale-attribute shape, not the missing-override one.
_EXECUTOR_STALE_ATTRIBUTES: dict[str, str] = {
    "is_single_threaded": (
        "Airflow 2.x gated SQLite compatibility on this flag; 3.x does not read it."
    ),
    "supports_pickling": "No certified 3.x release reads this flag.",
    "change_sensitivity": "No certified 3.x release reads this flag.",
    "execute_async": (
        "This was the pre-3.0 task-dispatch entry point. The scheduler never calls it "
        "on 3.x; implement `_process_workloads` instead."
    ),
}

# `sentry_integration`/`supports_sentry` are distinct from
# `AirflowCapabilities.startup_details_supports_sentry`, which describes a different
# Task SDK model field entirely. Verified against the installed 3.3.0, plus 3.1.0 and
# 3.2.0 in isolated environments; see `PROVENANCE.md`.
_EXECUTOR_SENTRY_FLAG_BY_CONTRACT: dict[ExecutorContract, tuple[str, type]] = {
    ExecutorContract.V3_1: ("supports_sentry", bool),
    ExecutorContract.V3_2: ("sentry_integration", str),
    ExecutorContract.V3_3: ("sentry_integration", str),
}


def _check_executor_missing_override(component: object) -> Iterable[ComponentProblem]:
    """Flag an executor that never overrides a method `BaseExecutor` does not enforce.

    Parameters:
        component: object containing the executor class or instance under check.

    Returns:
        Iterable[ComponentProblem] containing at most one problem per method.
    """

    if installed_family() is not AirflowFamily.V3:
        return
    from airflow.executors.base_executor import BaseExecutor

    component_type = _as_type(component)
    for name in _EXECUTOR_REQUIRED_OVERRIDES:
        defining = _defining_class(component_type, name)
        if defining is not None and defining is not BaseExecutor:
            continue
        yield ComponentProblem(
            code=EXECUTOR_MISSING_OVERRIDE,
            message=f"`{component_type.__name__}` does not override `{name}`.",
            hint=(
                f"`BaseExecutor` is not an ABC, so nothing enforces overriding `{name}`. "
                f"{_OVERRIDE_HINTS[name]}"
            ),
        )


def _check_executor_stale_attribute(component: object) -> Iterable[ComponentProblem]:
    """Flag an executor that sets an attribute `BaseExecutor` no longer reads.

    Parameters:
        component: object containing the executor class or instance under check.

    Returns:
        Iterable[ComponentProblem] containing at most one problem per stale attribute.
    """

    if installed_family() is not AirflowFamily.V3:
        return
    component_type = _as_type(component)
    for name, hint in _EXECUTOR_STALE_ATTRIBUTES.items():
        if not hasattr(component, name):
            continue
        yield ComponentProblem(
            code=EXECUTOR_STALE_ATTRIBUTE,
            message=(
                f"`{component_type.__name__}` sets `{name}`, which does not exist on "
                f"`BaseExecutor` in Airflow 3.1-3.3."
            ),
            hint=hint,
        )


def _check_executor_flag_wrong_type(component: object) -> Iterable[ComponentProblem]:
    """Flag an executor's sentry flag using the wrong name or type for the resolved release.

    Unlike every other checker in this registry, resolving the expected flag needs
    `resolve_capabilities()` rather than the non-raising `installed_family()`, because
    `executor_contract` only exists on the fully validated contract. `resolve_capabilities()`
    validates the whole environment -- symbols this check never touches included -- and can
    raise `AirflowCompatibilityError` for reasons that have nothing to do with the checked
    executor, an installed release that is not certified for example; that must never
    escape this checker, since a wrong or unrelated environment problem cannot be allowed
    to raise instead of reporting a problem.

    Parameters:
        component: object containing the executor class or instance under check.

    Returns:
        Iterable[ComponentProblem] containing at most one problem per sentry flag name
        found.
    """

    if installed_family() is not AirflowFamily.V3:
        return
    try:
        contract = resolve_capabilities().executor_contract
    except AirflowCompatibilityError:
        return
    if contract is None:
        return
    expected_name, expected_type = _EXECUTOR_SENTRY_FLAG_BY_CONTRACT[contract]
    component_type = _as_type(component)
    all_names = dict.fromkeys(
        flag_name for flag_name, _ in _EXECUTOR_SENTRY_FLAG_BY_CONTRACT.values()
    )
    for name in all_names:
        if not hasattr(component, name):
            continue
        if name != expected_name:
            yield ComponentProblem(
                code=EXECUTOR_FLAG_WRONG_TYPE,
                message=(
                    f"`{component_type.__name__}` sets `{name}`, which belongs to a "
                    f"different Airflow contract than the resolved one "
                    f"({contract.value})."
                ),
                hint=(
                    f"Rename to `{expected_name}` and use a `{expected_type.__name__}` "
                    f"value; `{name}` is silently ignored on the resolved contract."
                ),
            )
            continue
        value = getattr(component, name)
        if isinstance(value, expected_type):
            continue
        yield ComponentProblem(
            code=EXECUTOR_FLAG_WRONG_TYPE,
            message=(
                f"`{component_type.__name__}.{name}` is {type(value).__name__}, "
                f"expected {expected_type.__name__} on the resolved contract "
                f"({contract.value})."
            ),
            hint=f"Set `{name}` to a `{expected_type.__name__}` value.",
        )


# ---------------------------------------------------------------------------
# Kind classification and the flat, appendable check registry
# ---------------------------------------------------------------------------


def _is_timetable(component: object) -> bool:
    """Report whether a component nominally inherits `airflow.timetables.base.Timetable`.

    Structural (Protocol) `isinstance`/`issubclass` checks are not used: `Timetable`
    declares data attributes as well as methods, so `issubclass` against it always
    raises `TypeError` regardless of nominal inheritance, and a minimal duck-typed
    timetable that does not redeclare the Protocol's defaulted data attributes fails
    `isinstance` too. A caller whose timetable does not nominally inherit `Timetable`
    can still force these checks with `kind=ComponentKind.TIMETABLE`.

    Parameters:
        component: object containing the class or instance under check.

    Returns:
        bool indicating whether `Timetable` appears in the component's MRO.
    """

    from airflow.timetables.base import Timetable

    return Timetable in _as_type(component).__mro__


def _is_listener(component: object) -> bool:
    """Report whether a component defines at least one `@hookimpl`-marked method.

    Parameters:
        component: object containing the class or instance under check.

    Returns:
        bool indicating whether any callable member carries the hookimpl marker.
    """

    marker = _listener_marker_attribute("impl")
    component_type = _as_type(component)
    return any(hasattr(value, marker) for _, value in inspect.getmembers(component_type, callable))


def _is_executor(component: object) -> bool:
    """Report whether a component is a `BaseExecutor` subclass.

    Parameters:
        component: object containing the class or instance under check.

    Returns:
        bool indicating whether the component's type subclasses `BaseExecutor`.
    """

    from airflow.executors.base_executor import BaseExecutor

    return issubclass(_as_type(component), BaseExecutor)


KIND_CLASSIFIERS: dict[str, Callable[[object], bool]] = {
    TIMETABLE: _is_timetable,
    LISTENER: _is_listener,
    EXECUTOR: _is_executor,
}

# Flat and appendable by design: a follow-up phase adds more checks purely by appending
# rows, never by touching `check_component`'s dispatch loop.
CHECK_REGISTRY: tuple[tuple[str, str, Callable[[object], Iterable[ComponentProblem]]], ...] = (
    (TIMETABLE, TIMETABLE_LOCAL_QUALNAME, _check_timetable_local_qualname),
    (TIMETABLE, TIMETABLE_MISSING_PROTOCOL_METHOD, _check_timetable_missing_protocol_method),
    (TIMETABLE, TIMETABLE_SERIALIZE_PAIR_INCOMPLETE, _check_timetable_serialize_pair_incomplete),
    (TIMETABLE, TIMETABLE_SERIALIZE_NOT_JSON, _check_timetable_serialize_not_json),
    (LISTENER, LISTENER_NO_MATCHING_HOOKSPEC, _check_listener_no_matching_hookspec),
    (LISTENER, LISTENER_UNKNOWN_ARGUMENT, _check_listener_unknown_argument),
    (LISTENER, LISTENER_CORE_MANAGER_ONLY, _check_listener_core_manager_only),
    (LISTENER, LISTENER_SDK_MANAGER_ONLY, _check_listener_sdk_manager_only),
    (EXECUTOR, EXECUTOR_MISSING_OVERRIDE, _check_executor_missing_override),
    (EXECUTOR, EXECUTOR_STALE_ATTRIBUTE, _check_executor_stale_attribute),
    (EXECUTOR, EXECUTOR_FLAG_WRONG_TYPE, _check_executor_flag_wrong_type),
)

__all__ = (
    "CHECK_REGISTRY",
    "EXECUTOR",
    "KIND_CLASSIFIERS",
    "LISTENER",
    "TIMETABLE",
    "ComponentProblem",
)
