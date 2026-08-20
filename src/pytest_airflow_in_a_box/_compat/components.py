"""Static conformance checks for custom Airflow components.

Covers timetables, listeners, executors, XCom backends, weight strategies, notifiers,
secrets backends, policies, plugins, and providers. Most of these carry no enforcement
at class-creation time at all -- `Timetable` is a `typing.Protocol`, `BaseExecutor` is
not an ABC, a listener or policy hookimpl carries no base class whatsoever -- so a bug
ships and only fails once a scheduler, worker, or the Dag processor actually exercises
it. Every checker here is a pure function over a class, an instance, or (for providers) a
plain callable: no Airflow bootstrap or metadata database is touched, and Airflow itself
is imported only inside a checker body, never at module scope. The provider checks are
the one exception to "no cache": attributing a callable to its owning distribution scans
every installed distribution's file manifest, so that index is built once per process and
reused (see `_DISTRIBUTION_INDEX`) rather than rescanned on every `check_component()` call.

The registry is a flat, appendable list of `(kind, check_name, checker)` rows.
`pytest_airflow_in_a_box.components.check_component` iterates it generically -- filtering
by an explicit kind, or by `KIND_CLASSIFIERS` when none is given -- so a follow-up phase
adds more checks purely by appending rows here, never by touching the dispatch loop.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/timetable.html
    https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/listeners.html
    https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/xcoms.html
    https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/notifications.html
    https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/secrets-backend/index.html
    https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/cluster-policies.html
    https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/plugins.html
    https://airflow.apache.org/docs/apache-airflow-providers/index.html
    https://pluggy.readthedocs.io/en/stable/
"""

from __future__ import annotations

import inspect
import json
import sys
from dataclasses import dataclass
from importlib import import_module, metadata
from pathlib import Path
from typing import TYPE_CHECKING, cast, get_args

from packaging.utils import canonicalize_name

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
XCOM = "xcom"
WEIGHT_STRATEGY = "weight-strategy"
NOTIFIER = "notifier"
SECRETS_BACKEND = "secrets-backend"
POLICY = "policy"
PLUGIN = "plugin"
PROVIDER = "provider"


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
# XCom backend checks
# ---------------------------------------------------------------------------

XCOM_ORM_DESERIALIZE_REMOVED = "xcom-orm-deserialize-removed"
XCOM_BACKEND_SIGNATURE = "xcom-backend-signature"

# The real call shape `airflow.sdk.bases.xcom.BaseXCom.set()` uses against
# `cls.serialize_value(...)`: every argument by keyword, `value` included.
# `deserialize_value` is called as `cls.deserialize_value(result)`, one positional
# argument. Both are `@staticmethod` on the real base, called unbound as
# `cls.serialize_value(...)`/`cls.deserialize_value(...)` -- a user override that drops
# the decorator silently shifts every argument by one position, since the class-bound
# access then yields a plain function with `self` as its first parameter. Verified
# against the installed 3.3.0, plus 3.1.0 and 3.2.0 in isolated environments; see
# `PROVENANCE.md`.
_XCOM_SERIALIZE_VALUE_PROBE_KWARGS: dict[str, object] = {
    "value": None,
    "key": None,
    "task_id": None,
    "dag_id": None,
    "run_id": None,
    "map_index": None,
}


def _check_xcom_orm_deserialize_removed(component: object) -> Iterable[ComponentProblem]:
    """Flag a custom XCom backend still defining the removed `orm_deserialize_value`.

    `orm_deserialize_value` does not exist anywhere on `BaseXCom` in Airflow 3 -- no
    caller looks it up on any certified 3.1-3.3 release -- so a backend carrying one
    ships silently inert.

    Parameters:
        component: object containing the XCom backend class or instance under check.

    Returns:
        Iterable[ComponentProblem] containing at most one problem.
    """

    if installed_family() is not AirflowFamily.V3:
        return
    component_type = _as_type(component)
    if not hasattr(component_type, "orm_deserialize_value"):
        return
    yield ComponentProblem(
        code=XCOM_ORM_DESERIALIZE_REMOVED,
        message=f"`{component_type.__name__}` defines `orm_deserialize_value`.",
        hint=(
            "`orm_deserialize_value` was removed in Airflow 3 -- nothing calls it, on "
            "any certified 3.1-3.3 release. Delete it, or fold whatever it does into "
            "`deserialize_value`."
        ),
    )


def _check_xcom_backend_signature(component: object) -> Iterable[ComponentProblem]:
    """Flag an XCom backend that is not a real `BaseXCom` subclass, or a broken override.

    `resolve_xcom_backend` runs `issubclass(cls, BaseXCom)` only once, at import time
    when a worker starts. `BaseXCom.set()`/`get_one()` call `serialize_value`/
    `deserialize_value` with a fixed call shape; an override that does not accept it
    raises `TypeError` on the very first XCom push or pull. `Signature.bind()` checks
    that shape without ever calling the override.

    Parameters:
        component: object containing the XCom backend class or instance under check.

    Returns:
        Iterable[ComponentProblem] containing at most one problem per broken shape.
    """

    if installed_family() is not AirflowFamily.V3:
        return
    try:
        from airflow.sdk.bases.xcom import BaseXCom
    except ImportError:
        # `installed_family()` classifies from `apache-airflow-core`'s own dist-info
        # metadata alone, never this submodule's actual importability -- an uncertified
        # release (a pre-release build, or a future release this project has not
        # certified yet) that has moved or removed it must degrade to reporting nothing,
        # the same conservative-degradation precedent `_listener_hookspecs` already sets,
        # not raise out of `check_component` for a reason unrelated to the checked
        # component.
        return

    component_type = _as_type(component)
    if not issubclass(component_type, BaseXCom):
        yield ComponentProblem(
            code=XCOM_BACKEND_SIGNATURE,
            message=f"`{component_type.__name__}` is not a subclass of `BaseXCom`.",
            hint=(
                "`resolve_xcom_backend` raises `TypeError` at worker startup if "
                "`core.xcom_backend` names a class that is not "
                "`issubclass(cls, airflow.sdk.bases.xcom.BaseXCom)`. Subclass `BaseXCom`."
            ),
        )
        return

    def _user_overridden(name: str) -> bool:
        defining = _defining_class(component_type, name)
        if defining is None or defining is BaseXCom:
            return False
        return not defining.__module__.startswith("airflow.")

    if _user_overridden("serialize_value"):
        try:
            inspect.signature(component_type.serialize_value).bind(
                **_XCOM_SERIALIZE_VALUE_PROBE_KWARGS
            )
        except (TypeError, ValueError) as error:
            # `inspect.signature` itself raises `ValueError`, not `TypeError`, for a
            # callable it cannot introspect at all -- `staticmethod(min)` or another
            # C-accelerated builtin wired in directly, a plausible move for a
            # performance-conscious override. That is exactly as broken an override as
            # one `.bind()` itself rejects; catching only `TypeError` let this shape
            # raise straight out of `check_component` instead of reporting it.
            yield ComponentProblem(
                code=XCOM_BACKEND_SIGNATURE,
                message=(
                    f"`{component_type.__name__}.serialize_value` does not accept "
                    f"`BaseXCom.set()`'s real call shape ({error})."
                ),
                hint=(
                    "`set()` calls `cls.serialize_value(value=..., key=..., "
                    "task_id=..., dag_id=..., run_id=..., map_index=...)` -- every "
                    "argument by keyword. Accept all six (directly or via `**kwargs`), "
                    "and keep the method a `@staticmethod` or `@classmethod`: a plain "
                    "instance method silently shifts every keyword argument, since "
                    "`set()` calls it unbound as `cls.serialize_value(...)`."
                ),
            )
    if _user_overridden("deserialize_value"):
        try:
            inspect.signature(component_type.deserialize_value).bind(None)
        except (TypeError, ValueError) as error:
            yield ComponentProblem(
                code=XCOM_BACKEND_SIGNATURE,
                message=(
                    f"`{component_type.__name__}.deserialize_value` does not accept "
                    f"`BaseXCom.get_one()`'s real call shape ({error})."
                ),
                hint=(
                    "`get_one()`/`get_all()` call `cls.deserialize_value(result)` with "
                    "exactly one positional argument. Keep the method a `@staticmethod` "
                    "or `@classmethod` accepting one required positional parameter."
                ),
            )


# ---------------------------------------------------------------------------
# Weight strategy checks
# ---------------------------------------------------------------------------

WEIGHT_STRATEGY_ABSTRACT = "weight-strategy-abstract"
WEIGHT_STRATEGY_HASH_OF_NONE = "weight-strategy-hash-of-none"


def _check_weight_strategy_abstract(component: object) -> Iterable[ComponentProblem]:
    """Flag a weight strategy that still carries unresolved abstract methods.

    `PriorityWeightStrategy` is a real `abc.ABC`, so Python itself refuses to
    instantiate an incomplete subclass -- but only at instantiation time, which for a
    `weight_rule` class reference can be far downstream of Dag parsing, inside a
    scheduler run. This surfaces the same `TypeError` statically instead, without ever
    instantiating anything itself.

    Parameters:
        component: object containing the weight strategy class or instance under check.

    Returns:
        Iterable[ComponentProblem] containing at most one problem.
    """

    if installed_family() is not AirflowFamily.V3:
        return
    component_type = _as_type(component)
    remaining = getattr(component_type, "__abstractmethods__", frozenset())
    if not remaining:
        return
    yield ComponentProblem(
        code=WEIGHT_STRATEGY_ABSTRACT,
        message=(
            f"`{component_type.__name__}` still has unimplemented abstract method(s): "
            f"{sorted(remaining)}."
        ),
        hint=(
            "`PriorityWeightStrategy` is an `abc.ABC`; Python raises `TypeError` the "
            "moment anything tries to construct this class, which can be long after "
            "Dag parsing, inside a scheduler run. Implement the missing method(s)."
        ),
    )


def _check_weight_strategy_hash_of_none(component: object) -> Iterable[ComponentProblem]:
    """Flag a weight strategy whose effective `__hash__` is unusable.

    `PriorityWeightStrategy` defines `__eq__` without a matching `__hash__`. Python sets
    a class's own `__hash__` to `None` whenever a class body defines `__eq__` without
    `__hash__`. On the certified 3.1.x base that class is `PriorityWeightStrategy`
    itself, making every instance of every subclass that does not fix it unhashable
    (`TypeError` from `hash()`); the same automatic rule just as easily fires on a
    *user's own* subclass that defines `__eq__` for value semantics and does not think
    to also define `__hash__`, independently of which release is installed. The
    certified 3.2+ base instead defines `__hash__` explicitly as `return hash(None)`,
    making every instance hash *equal* instead when nothing overrides it, so a `set` or
    `dict` keyed on strategy instances silently collapses distinct strategies together.
    Either way, relying on the inherited or automatically-nulled behavior breaks
    deduplication.

    Checking whether `__hash__` was "user overridden" is not the same question as
    whether the *effective* `__hash__` still works, since Python's automatic rule can
    plant `None` on a user's own subclass exactly as it does on the base -- so this
    compares the component's own resolved `__hash__` by identity against the
    installed `PriorityWeightStrategy.__hash__` instead of asking who defined it: `None`
    is always broken regardless of which class in the MRO it came from, and a resolved
    `__hash__` identical to the un-subclassed base's is never a real override either.
    Reads the real installed base's own `__hash__` to describe which failure mode
    applies, rather than hardcoding a per-release table, so the message stays accurate
    even if a future release changes which one applies.

    Parameters:
        component: object containing the weight strategy class or instance under check.

    Returns:
        Iterable[ComponentProblem] containing at most one problem.
    """

    if installed_family() is not AirflowFamily.V3:
        return
    try:
        from airflow.task.priority_strategy import PriorityWeightStrategy
    except ImportError:
        return

    component_type = _as_type(component)
    effective_hash = component_type.__hash__
    if effective_hash is not None and effective_hash is not PriorityWeightStrategy.__hash__:
        return
    if effective_hash is None:
        observed = "raises `TypeError: unhashable type`"
    else:
        observed = (
            "returns `hash(None)` on the installed Airflow release, so every instance hashes equal"
        )
    yield ComponentProblem(
        code=WEIGHT_STRATEGY_HASH_OF_NONE,
        message=f"`{component_type.__name__}`'s effective `__hash__` is unusable.",
        hint=(
            f"`PriorityWeightStrategy` defines `__eq__` without `__hash__`; the same "
            f"automatic Python rule that nulls the base's own `__hash__` on some "
            f"releases fires just as easily on a subclass that defines `__eq__` "
            f"without `__hash__` too. The effective `__hash__` here {observed}, so a "
            f"`set` or `dict` cannot dedupe or key on instances correctly. Define "
            f"`__hash__` explicitly, for example by hashing "
            f"`(type(self).__module__, type(self).__qualname__)` -- `serialize()`/"
            f"`deserialize()` exist on `PriorityWeightStrategy` only on 3.1.x; 3.2+ "
            f"identifies a strategy by its qualname instead, so a hash based on "
            f"`serialize()` raises `AttributeError` on the releases where this check "
            f"is most likely to fire (`hash(None)` never raises, so more code "
            f"carrying this bug ships unnoticed there)."
        ),
    )


# ---------------------------------------------------------------------------
# Notifier checks
# ---------------------------------------------------------------------------

NOTIFIER_MISSING_NOTIFY = "notifier-missing-notify"
NOTIFIER_TEMPLATE_FIELDS_UNRESOLVABLE = "notifier-template-fields-unresolvable"


def _check_notifier_missing_notify(component: object) -> Iterable[ComponentProblem]:
    """Flag a notifier that never overrides `notify`.

    `BaseNotifier.notify`'s default body raises `NotImplementedError` unconditionally.
    `on_success_callback`/`on_failure_callback` run on the Dag processor, sync-only, and
    call `notify` directly -- implementing only `async_notify` does not help there.
    Covers apache/airflow#64649, where a minimal `BaseNotifier` used as a callback
    crashed under `airflow dags test`.

    Parameters:
        component: object containing the notifier class or instance under check.

    Returns:
        Iterable[ComponentProblem] containing at most one problem.
    """

    if installed_family() is not AirflowFamily.V3:
        return
    try:
        from airflow.sdk.bases.notifier import BaseNotifier
    except ImportError:
        return

    component_type = _as_type(component)
    defining = _defining_class(component_type, "notify")
    if defining is not None and defining is not BaseNotifier:
        return
    yield ComponentProblem(
        code=NOTIFIER_MISSING_NOTIFY,
        message=f"`{component_type.__name__}` does not override `notify`.",
        hint=(
            "`BaseNotifier.notify`'s default implementation raises "
            "`NotImplementedError()`. `on_success_callback`/`on_failure_callback` run "
            "on the Dag processor and call `notify` synchronously -- implementing only "
            "`async_notify` does not help there."
        ),
    )


def _check_notifier_template_fields_unresolvable(component: object) -> Iterable[ComponentProblem]:
    """Flag a notifier instance naming a `template_fields` entry it does not carry.

    Only runs against an already-built instance: an attribute a `__init__` assigns is
    invisible on the bare class, so a class-only check would false-positive on the
    common, correct case. `BaseNotifier._update_context` does a plain `getattr(self, f)`
    for every name in `template_fields`, raising `AttributeError` the first time the
    notifier actually fires.

    Parameters:
        component: object containing the notifier class or instance under check.

    Returns:
        Iterable[ComponentProblem] containing at most one problem per unresolvable name.
    """

    if installed_family() is not AirflowFamily.V3:
        return
    if isinstance(component, type):
        return
    try:
        template_fields = getattr(component, "template_fields", ())
    except Exception as error:
        # `getattr(..., default)` only substitutes `default` for `AttributeError`; a
        # `template_fields` implemented as a `property` that itself raises (say, before
        # some other required state is configured) raises straight through instead. That
        # is exactly the shape this check exists to catch -- a notifier that will crash
        # the first time Airflow actually reads `template_fields` -- so it is reported as
        # a problem, not silently swallowed the way an unrelated environment failure
        # would be.
        yield ComponentProblem(
            code=NOTIFIER_TEMPLATE_FIELDS_UNRESOLVABLE,
            message=(
                f"`{type(component).__name__}.template_fields` raised "
                f"{type(error).__name__} instead of returning a value ({error})."
            ),
            hint=(
                "`_update_context` reads `template_fields` directly off the instance; "
                "an attribute access that raises breaks the notifier the first time it "
                "actually runs. Make `template_fields` a plain, side-effect-free value."
            ),
        )
        return
    if isinstance(template_fields, str):
        # `str` is technically a `Sequence[str]`, so nothing rejects it structurally --
        # but `_update_context` then iterates it character by character, treating each
        # character as a field name, almost always from a missing trailing comma.
        yield ComponentProblem(
            code=NOTIFIER_TEMPLATE_FIELDS_UNRESOLVABLE,
            message=(
                f"`{type(component).__name__}.template_fields` is the bare string "
                f"{template_fields!r}, not a sequence of field names."
            ),
            hint=(
                "`_update_context` iterates `template_fields` character by character "
                "when it is a bare string, treating each character as a field name. "
                'This is almost always a missing trailing comma: `("message",)`, not '
                '`("message")`.'
            ),
        )
        return
    try:
        names = tuple(template_fields)
    except Exception as error:
        # Broader than `TypeError` on purpose: a non-iterable value raises `TypeError`
        # from `tuple()` itself, but a generator or other lazy iterable that raises
        # partway through iteration can surface any exception type, and both are exactly
        # the same "this notifier's `template_fields` cannot be safely resolved" problem.
        yield ComponentProblem(
            code=NOTIFIER_TEMPLATE_FIELDS_UNRESOLVABLE,
            message=(
                f"`{type(component).__name__}.template_fields` is not usable as a "
                f"sequence of field names ({type(error).__name__}: {error})."
            ),
            hint=(
                "`_update_context` iterates `template_fields` directly; a value that is "
                "not iterable, or that raises while being iterated, breaks the notifier "
                "the first time it actually runs. Set it to a tuple or list of attribute "
                "names."
            ),
        )
        return
    for name in names:
        try:
            resolvable = hasattr(component, name)
        except Exception as error:
            # `hasattr` only swallows `AttributeError`; a non-`str` entry (a missing
            # trailing comma turning `("message")` into a bare string is one way, but a
            # plain typo like `("message", None)` is another) raises `TypeError` from
            # `hasattr` itself, and a `property` that raises something other than
            # `AttributeError` when accessed propagates too. Both are real, checkable
            # problems, not reasons to crash `check_component`.
            yield ComponentProblem(
                code=NOTIFIER_TEMPLATE_FIELDS_UNRESOLVABLE,
                message=(
                    f"`{type(component).__name__}.template_fields` entry {name!r} could "
                    f"not be resolved ({type(error).__name__}: {error})."
                ),
                hint=(
                    "`_update_context` does `getattr(self, f)` for every `template_fields` "
                    "entry; this entry is not a valid attribute name, or resolving it "
                    "raises. Fix the entry, or remove it from `template_fields`."
                ),
            )
            continue
        if resolvable:
            continue
        yield ComponentProblem(
            code=NOTIFIER_TEMPLATE_FIELDS_UNRESOLVABLE,
            message=(
                f"`{type(component).__name__}.template_fields` names `{name}`, which "
                f"this instance does not carry."
            ),
            hint=(
                "`_update_context` does `getattr(self, f)` for every `template_fields` "
                "entry, raising `AttributeError` the first time this notifier actually "
                "runs. Set the attribute, or remove it from `template_fields`."
            ),
        )


# ---------------------------------------------------------------------------
# Secrets backend checks
# ---------------------------------------------------------------------------

SECRETS_BACKEND_RAISES_ON_MISS = "secrets-backend-raises-on-miss"

# The four getters a secrets backend author overrides, all sharing the same `-> str |
# None` (or, for `get_connection`, `Connection | None`) contract. `get_conn_value`'s own
# docstring names it the *default* override point ("If the client your secrets backend
# uses already returns a python dict, you should override `get_connection` instead"),
# and Airflow's own shipped `EnvironmentVariablesBackend` and `ExecutionAPISecretsBackend`
# override `get_conn_value`, not `get_connection` -- so excluding it would miss the more
# commonly overridden of the two connection getters, not a rarely-used one. Verified
# against the installed 3.3.0, plus 3.1.0 and 3.2.0 in isolated environments; see
# `PROVENANCE.md`.
_SECRETS_BACKEND_GETTERS = ("get_conn_value", "get_connection", "get_variable", "get_config")


def _annotation_allows_none(annotation: object) -> bool:
    """Report whether a return annotation admits `None` as a value.

    A real (already-evaluated) annotation object is checked structurally via
    `typing.get_args`, which normalizes `X | None`, `typing.Optional[X]`, and
    `typing.Union[X, None]` identically -- unlike a plain substring search, which finds
    the word `None` in the first spelling but not the other two: `str(typing.Optional
    [str])` renders as `'typing.Optional[str]'`, containing no literal `None` at all.
    A bare `-> None` annotation (the real object `None`, or `type(None)` itself) is
    special-cased ahead of the structural check: `get_args(None)` is `()`, since `None`
    is not a parametrized generic at all, so the structural check alone would wrongly
    reject the one annotation that is trivially, entirely `None`. When the defining
    module uses `from __future__ import annotations`, `inspect.signature` instead
    reports the annotation as an unevaluated string; this module never resolves those
    (doing so can raise for a forward reference it cannot see), so a string annotation
    falls back to a conservative text match covering both the `None` and `Optional[`
    spellings.

    Parameters:
        annotation: object containing the return annotation to inspect.

    Returns:
        bool indicating whether the annotation is recognized as nullable.
    """

    if isinstance(annotation, str):
        return "None" in annotation or "Optional[" in annotation
    if annotation is None or annotation is type(None):
        return True
    return type(None) in get_args(annotation)


def _check_secrets_backend_raises_on_miss(component: object) -> Iterable[ComponentProblem]:
    """Flag a secrets backend getter annotated to never return `None`.

    All four getters must return `None`, not raise, on a miss -- a very common bug is
    raising the backing client's own not-found error instead. `check_component` never
    calls a secrets backend for real (a genuine miss needs real credentials and a real
    backend this module cannot fabricate safely), so this reads the override's own
    declared return annotation instead: one that is present but does not admit `None`
    (see `_annotation_allows_none`) is a strong static signal the author did not design
    for the miss case. An unannotated override is not flagged -- silence, not a false
    positive, on code this cannot judge.

    Parameters:
        component: object containing the secrets backend class or instance under check.

    Returns:
        Iterable[ComponentProblem] containing at most one problem per offending getter.
    """

    if installed_family() is not AirflowFamily.V3:
        return
    try:
        from airflow.secrets.base_secrets import BaseSecretsBackend
    except ImportError:
        return

    component_type = _as_type(component)
    for name in _SECRETS_BACKEND_GETTERS:
        defining = _defining_class(component_type, name)
        if defining is None or defining is BaseSecretsBackend:
            continue
        if defining.__module__.startswith("airflow."):
            continue
        try:
            return_annotation = inspect.signature(getattr(component_type, name)).return_annotation
        except (TypeError, ValueError):
            # `_defining_class` only confirms `name in vars(klass)` for some class in the
            # MRO -- not that the stored value is callable or introspectable. A getter
            # stubbed out as `get_variable = None` (a plausible work-in-progress leftover)
            # makes `inspect.signature` raise `TypeError`; a getter that is callable but
            # unintrospectable (a C-accelerated builtin wired in directly) raises
            # `ValueError` instead. Neither is this check's business to judge -- skip,
            # the same as the "no annotation at all" case just below.
            continue
        if return_annotation is inspect.Signature.empty:
            continue
        if _annotation_allows_none(return_annotation):
            continue
        yield ComponentProblem(
            code=SECRETS_BACKEND_RAISES_ON_MISS,
            message=(
                f"`{component_type.__name__}.{name}` is annotated to return "
                f"`{return_annotation}`, which does not admit `None`."
            ),
            hint=(
                f"`{name}` must return `None` on a miss, not raise. If it genuinely "
                f"always returns a value, ignore this; if it can miss, annotate the "
                f"return type accordingly (for example `str | None`) and return `None` "
                f"there instead of raising."
            ),
        )


# ---------------------------------------------------------------------------
# Policy checks
# ---------------------------------------------------------------------------

POLICY_UNKNOWN_HOOKSPEC = "policy-unknown-hookspec"
POLICY_ARGUMENT_NAME_MISMATCH = "policy-argument-name-mismatch"

_POLICY_HOOKSPEC_MODULES = ("airflow.policies",)


def _policy_marker_attribute(name: str) -> str:
    """Build the pluggy marker attribute name pluggy stamps onto a decorated function.

    Mirrors `_listener_marker_attribute` for `airflow.policies`, whose `hookimpl`/
    `local_settings_hookspec` markers are built from a
    `pluggy.HookimplMarker("airflow.policy")`/`pluggy.HookspecMarker("airflow.policy")`
    pair -- a different pluggy project name than the listener markers use. Derived from
    the real `hookimpl` rather than hardcoded, so a future rename changes this too
    instead of silently going stale.

    Parameters:
        name: str containing `"impl"` or `"spec"`.

    Returns:
        str containing the attribute name pluggy stamps for that marker kind.
    """

    from airflow.policies import hookimpl

    return f"{hookimpl.project_name}_{name}"


def _policy_hookspecs(module_names: tuple[str, ...]) -> dict[str, tuple[str, ...]]:
    """Map each `@local_settings_hookspec` function name to its validated parameters.

    Mirrors `_listener_hookspecs` for `airflow.policies`; a module that fails to import
    is skipped rather than raised, the same conservative-degradation precedent.

    Parameters:
        module_names: tuple[str, ...] containing hookspec module import paths.

    Returns:
        dict[str, tuple[str, ...]] mapping hookspec function name to the parameter names
        pluggy would validate, in declaration order.
    """

    try:
        marker = _policy_marker_attribute("spec")
    except ImportError:
        # Mirrors the per-module `ImportError` tolerance just below: `airflow.policies`
        # itself failing to import (an uncertified release moving or removing it) must
        # degrade to reporting nothing, not raise out of `check_component`.
        return {}
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


def _policy_hookimpls(component_type: type) -> tuple[_HookimplInfo, ...]:
    """Collect every `@hookimpl`-decorated policy method's name and validated parameters.

    Mirrors `_listener_hookimpls` for `airflow.policies.hookimpl`.

    Parameters:
        component_type: type containing the policy class under check.

    Returns:
        tuple[_HookimplInfo, ...] containing one entry per hookimpl-marked method, in
        `inspect.getmembers` order (alphabetical by method name).
    """

    try:
        marker = _policy_marker_attribute("impl")
    except ImportError:
        return ()
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


def _check_policy_unknown_hookspec(component: object) -> Iterable[ComponentProblem]:
    """Flag a policy hookimpl method whose name matches no real `airflow.policies` hookspec.

    Mirrors `_check_listener_no_matching_hookspec` for policies.

    Parameters:
        component: object containing the policy class or instance under check.

    Returns:
        Iterable[ComponentProblem] containing at most one problem per unmatched method.
    """

    if installed_family() is not AirflowFamily.V3:
        return
    component_type = _as_type(component)
    impls = _policy_hookimpls(component_type)
    if not impls:
        return
    known = _policy_hookspecs(_POLICY_HOOKSPEC_MODULES)
    for info in sorted(impls, key=lambda item: item.method_name):
        if info.hookspec_name in known:
            continue
        yield ComponentProblem(
            code=POLICY_UNKNOWN_HOOKSPEC,
            message=(
                f"`{component_type.__name__}.{info.method_name}` matches no hookspec "
                f"registered by `airflow.policies`."
            ),
            hint=(
                "pluggy silently ignores a hookimpl matching no hookspec -- this "
                "method never fires, with no warning. Check the method name (or "
                "`specname=`) against `task_policy`, `dag_policy`, "
                "`task_instance_mutation_hook`, `pod_mutation_hook`, "
                "`get_airflow_context_vars`, and `get_dagbag_import_timeout`."
            ),
        )


def _check_policy_argument_name_mismatch(component: object) -> Iterable[ComponentProblem]:
    """Flag a policy hookimpl method declaring an argument its hookspec does not have.

    `task_instance_mutation_hook` gained a `dag_run` parameter in Airflow 3.3; pluggy
    hard-errors at registration time on an unknown hookimpl argument name, so a hook
    written for -- or copied from -- a newer release breaks registration entirely on an
    older one. Reads the live, installed hookspec rather than a hardcoded per-release
    table, so this reflects whatever the resolved Airflow actually declares.

    Parameters:
        component: object containing the policy class or instance under check.

    Returns:
        Iterable[ComponentProblem] containing at most one problem per offending method.
    """

    if installed_family() is not AirflowFamily.V3:
        return
    component_type = _as_type(component)
    impls = _policy_hookimpls(component_type)
    if not impls:
        return
    specs = _policy_hookspecs(_POLICY_HOOKSPEC_MODULES)
    for info in sorted(impls, key=lambda item: item.method_name):
        hookspec_params = specs.get(info.hookspec_name)
        if hookspec_params is None:
            continue
        unknown = [param for param in info.params if param not in hookspec_params]
        if not unknown:
            continue
        yield ComponentProblem(
            code=POLICY_ARGUMENT_NAME_MISMATCH,
            message=(
                f"`{component_type.__name__}.{info.method_name}` declares argument(s) "
                f"{unknown} that `{info.hookspec_name}`'s hookspec does not accept on "
                f"the installed Airflow release."
            ),
            hint=(
                f"pluggy hard-errors at registration time on an unknown hookimpl "
                f"argument name. `{info.hookspec_name}` accepts: {list(hookspec_params)}."
            ),
        )


# ---------------------------------------------------------------------------
# Plugin checks
# ---------------------------------------------------------------------------

PLUGIN_NAME_MISSING = "plugin-name-missing"


def _check_plugin_name_missing(component: object) -> Iterable[ComponentProblem]:
    """Flag a plugin that does not set `name`.

    `AirflowPlugin.validate()` raises `AirflowPluginException` for exactly this, but
    only when Airflow's own `is_valid_plugin` calls it during real plugin discovery.
    This checker never calls `validate()` -- doing so risks raising out of
    `check_component`, which must never happen -- and reports the same condition
    instead, safely.

    Parameters:
        component: object containing the plugin class or instance under check.

    Returns:
        Iterable[ComponentProblem] containing at most one problem.
    """

    if installed_family() is not AirflowFamily.V3:
        return
    component_type = _as_type(component)
    if getattr(component_type, "name", None):
        return
    yield ComponentProblem(
        code=PLUGIN_NAME_MISSING,
        message=f"`{component_type.__name__}` does not set `name`.",
        hint=(
            "`AirflowPlugin.validate()` raises "
            '`AirflowPluginException("Your plugin needs a name.")` for exactly this, '
            "the moment real discovery reaches it. Set a `name` class attribute."
        ),
    )


# ---------------------------------------------------------------------------
# Provider checks
# ---------------------------------------------------------------------------

PROVIDER_INFO_SCHEMA = "provider-info-schema"
PROVIDER_PACKAGE_NAME_MISMATCH = "provider-package-name-mismatch"
PROVIDER_NO_ENTRY_POINT = "provider-no-entry-point"

_PROVIDER_ENTRY_POINT_GROUP = "apache_airflow_provider"


def _call_provider_info(component: object) -> object:
    """Call an already-verified-callable `get_provider_info`-shaped component.

    Every caller has already checked `callable(component) and not
    isinstance(component, type)` before reaching this; the checked type stays the
    generic `object` every checker in this module accepts, so the runtime-justified cast
    lives in this one place rather than at each of the three call sites.

    Parameters:
        component: object already known to be a non-class callable.

    Returns:
        object containing whatever the callable returns.
    """

    return cast("Callable[[], object]", component)()


def _distribution_editable_roots(dist: metadata.Distribution) -> tuple[Path, ...]:
    """Resolve the real, precise source roots a `.pth`-based editable install exposes.

    A `pip install -e .` / `uv pip install -e .` install -- the standard way a provider
    author develops their own package -- records no real file paths in its RECORD at
    all: only its `.dist-info` metadata and a `.pth` file are actually installed into
    `site-packages`. But that `.pth` file's own content is precisely the directory (or
    directories, one per line) Python's site module adds to `sys.path` for this
    distribution -- the real editable-exposed boundary. The project root PEP 610's
    `direct_url.json` names instead is not that boundary: a package built with a
    `src/` layout (this project included) is editable-exposed only under `src/`, not
    the whole checkout, so attributing every file under the project root -- `tests/`,
    `docs/`, a sibling package in a workspace -- to this one distribution over-attributes
    and can misattribute in a workspace where sibling distributions share a root.

    Parameters:
        dist: importlib.metadata.Distribution to inspect.

    Returns:
        tuple[Path, ...] containing each resolved, existing directory this
        distribution's own `.pth` file(s) add to `sys.path`. Empty when this
        distribution ships no `.pth` file, or none of its lines resolve to a real
        directory.
    """

    roots: list[Path] = []
    for recorded_file in dist.files or ():
        if not str(recorded_file).endswith(".pth"):
            continue
        try:
            content = Path(str(dist.locate_file(recorded_file))).read_text()
        except (OSError, ValueError):
            # `UnicodeDecodeError` (a `ValueError`) is as real a possibility as `OSError`
            # here: a `.pth` file is ordinary text this module does not control the
            # encoding of. `site.addpackage` tolerates the same failure the same way.
            continue
        for line in content.splitlines():
            stripped = line.strip()
            # Mirrors Python's own `site` module: a `.pth` line starting with `import`
            # is an executable hook, not a path, and `#` starts a comment. Neither
            # names a directory this distribution exposes.
            if not stripped or stripped.startswith(("#", "import")):
                continue
            try:
                candidate = Path(stripped).resolve()
            except (OSError, ValueError):
                continue
            if candidate.is_dir():
                roots.append(candidate)
    return tuple(roots)


# Process-local cache: every installed distribution's file manifest and editable
# `.pth` roots, built once. Mirrors `capabilities.py`'s `_CAPABILITIES` caching -- the
# installed set of distributions is a fact about the environment, not about any one
# checked component, so rebuilding it on every `check_component()` call over a
# provider (there can be many, across a whole test session) would repeat an
# `O(distributions x files)` filesystem walk for no benefit.
_DISTRIBUTION_INDEX: tuple[dict[Path, str], tuple[tuple[Path, str], ...]] | None = None


def _build_distribution_index() -> tuple[dict[Path, str], tuple[tuple[Path, str], ...]]:
    """Build the file-manifest and editable-root index for every installed distribution.

    Returns:
        tuple[dict[Path, str], tuple[tuple[Path, str], ...]] containing a mapping from
        each real recorded file's resolved path to its owning distribution's name, and
        a tuple of `(editable root, owning distribution name)` pairs sorted by root
        length descending, so the most specific (deepest) root is checked first when
        more than one contains a given file.
    """

    file_index: dict[Path, str] = {}
    editable_roots: list[tuple[Path, str]] = []
    for dist in metadata.distributions():
        for recorded_file in dist.files or ():
            try:
                located = Path(str(dist.locate_file(recorded_file))).resolve()
            except Exception:
                continue
            file_index.setdefault(located, dist.name)
        for root in _distribution_editable_roots(dist):
            editable_roots.append((root, dist.name))
    editable_roots.sort(key=lambda pair: len(pair[0].parts), reverse=True)
    return file_index, tuple(editable_roots)


def _reset_provider_distribution_index_for_testing() -> None:
    """Clear the cached distribution index so a test can install a fresh fake one."""

    global _DISTRIBUTION_INDEX
    _DISTRIBUTION_INDEX = None


def _provider_owning_distribution(component: object) -> str | None:
    """Resolve the installed distribution that owns a callable's module.

    Every real Airflow provider lives under the shared `airflow.providers.*` namespace
    package, so `importlib.metadata.packages_distributions()` -- which maps by top-level
    import name -- resolves the top-level name `airflow` to every provider distribution
    installed at once, never to exactly one; it is unusable here. Matching the cached
    file-manifest index against the callable's actual module file instead correctly
    attributes a namespace-packaged module to its one real owner, for a normally
    (non-editable) installed provider. A provider under active development is typically
    installed editable instead, whose RECORD contains no real file paths at all --
    `_distribution_editable_roots`' precise `.pth`-derived roots are the fallback for
    that case.

    Parameters:
        component: object containing the `get_provider_info`-shaped callable to trace.

    Returns:
        str | None containing the owning distribution's name, or None when the
        callable carries no resolvable module file, or no installed distribution can be
        attributed to it either way.
    """

    global _DISTRIBUTION_INDEX

    module_name = getattr(component, "__module__", None)
    if not module_name:
        return None
    module = sys.modules.get(module_name)
    module_file = getattr(module, "__file__", None)
    if not module_file:
        return None
    module_path = Path(module_file).resolve()

    if _DISTRIBUTION_INDEX is None:
        _DISTRIBUTION_INDEX = _build_distribution_index()
    file_index, editable_roots = _DISTRIBUTION_INDEX

    exact = file_index.get(module_path)
    if exact is not None:
        return exact
    for root, distribution_name in editable_roots:
        if module_path.is_relative_to(root):
            return distribution_name
    return None


def _check_provider_info_schema(component: object) -> Iterable[ComponentProblem]:
    """Flag a `get_provider_info()` callable whose return value fails the shipped schema.

    Calls `component()`, mirroring how `ProvidersManager` calls the real entry point
    (`entry_point.load()()`) at discovery time; a call that raises is reported here
    too, since a provider whose info cannot even be produced can never be schema
    validated.

    Parameters:
        component: object containing the `get_provider_info`-shaped callable to check.

    Returns:
        Iterable[ComponentProblem] containing at most one problem per schema violation,
        or one problem when the callable itself raises.
    """

    if installed_family() is not AirflowFamily.V3:
        return
    if not callable(component) or isinstance(component, type):
        return
    from importlib.resources import files

    try:
        import jsonschema
    except ImportError:
        # `jsonschema` is not a direct dependency of this project -- it rides in only
        # because the installed `apache-airflow-core` currently requires it, which
        # `installed_family()` never verifies (it classifies from `apache-airflow-core`'s
        # own dist-info metadata alone, not its dependencies' importability). A pruned
        # or `--no-deps` environment can have Airflow's metadata present without
        # `jsonschema` actually installed; a checker must never raise on the checked
        # component for a reason that has nothing to do with it.
        return

    name = getattr(component, "__name__", type(component).__name__)
    try:
        schema = json.loads(files("airflow").joinpath("provider_info.schema.json").read_text())
        validator = jsonschema.validators.validator_for(schema)(schema)
    except (OSError, ValueError, jsonschema.exceptions.SchemaError):
        return
    try:
        provider_info = _call_provider_info(component)
    except Exception as error:
        yield ComponentProblem(
            code=PROVIDER_INFO_SCHEMA,
            message=(
                f"`{name}()` raised {type(error).__name__} instead of returning a "
                f"provider-info dict: {error}."
            ),
            hint=(
                "`ProvidersManager` calls the registered entry point directly as "
                "`entry_point.load()()` -- `get_provider_info` must be a plain, "
                "side-effect-free function returning a dict."
            ),
        )
        return
    for schema_error in validator.iter_errors(provider_info):
        path = "/".join(str(part) for part in schema_error.absolute_path) or "<root>"
        yield ComponentProblem(
            code=PROVIDER_INFO_SCHEMA,
            message=(
                f"`{name}()` does not conform to `provider_info.schema.json` at "
                f"`{path}`: {schema_error.message}."
            ),
            hint=(
                "Validate against the shipped `airflow/provider_info.schema.json`; "
                "`ProvidersManager` rejects a non-conforming dict at discovery time."
            ),
        )


def _check_provider_package_name_mismatch(component: object) -> Iterable[ComponentProblem]:
    """Flag a `package-name` that disagrees with the owning distribution's own name.

    `_discover_all_providers_from_packages` (moved to
    `airflow._shared.providers_discovery.providers_discovery
    .discover_all_providers_from_packages` from 3.2 onward; inline on `ProvidersManager`
    on 3.1.x) raises `ValueError` -- not a warning -- when these disagree.

    Parameters:
        component: object containing the `get_provider_info`-shaped callable to check.

    Returns:
        Iterable[ComponentProblem] containing at most one problem.
    """

    if installed_family() is not AirflowFamily.V3:
        return
    if not callable(component) or isinstance(component, type):
        return
    distribution_name = _provider_owning_distribution(component)
    if distribution_name is None:
        return
    try:
        provider_info = _call_provider_info(component)
    except Exception:
        return
    if not isinstance(provider_info, dict):
        return
    package_name = provider_info.get("package-name")
    if not isinstance(package_name, str):
        return
    canonical_distribution_name = canonicalize_name(distribution_name)
    if canonical_distribution_name == package_name:
        return
    name = getattr(component, "__name__", type(component).__name__)
    yield ComponentProblem(
        code=PROVIDER_PACKAGE_NAME_MISMATCH,
        message=(
            f"`{name}()['package-name']` is {package_name!r}, but the installed "
            f"distribution canonicalizes to {canonical_distribution_name!r}."
        ),
        hint=(
            "`ProvidersManager` raises `ValueError` at discovery when these disagree "
            "-- not a warning. Make `package-name` match the distribution's own "
            "canonical (PEP 503) name."
        ),
    )


def _check_provider_no_entry_point(component: object) -> Iterable[ComponentProblem]:
    """Flag a provider whose owning distribution registers no discovery entry point.

    Without an `apache_airflow_provider` entry point, `ProvidersManager` never calls
    this function at all -- the provider is not discovered, silently.

    Parameters:
        component: object containing the `get_provider_info`-shaped callable to check.

    Returns:
        Iterable[ComponentProblem] containing at most one problem.
    """

    if installed_family() is not AirflowFamily.V3:
        return
    if not callable(component) or isinstance(component, type):
        return
    distribution_name = _provider_owning_distribution(component)
    if distribution_name is None:
        return
    canonical_distribution_name = canonicalize_name(distribution_name)
    registered = {
        canonicalize_name(entry_point.dist.name)
        for entry_point in metadata.entry_points(group=_PROVIDER_ENTRY_POINT_GROUP)
        # `entry_point.dist is not None` only confirms the `Distribution` object exists,
        # not that its own `.name` does: `Distribution.name` reads the `Name` metadata
        # header and is `None` for a distribution with malformed `.dist-info` metadata
        # (a real, if rare, possibility -- `email.message.Message` returns `None` for a
        # missing header rather than raising). `canonicalize_name(None)` raises
        # `AttributeError`, and this scan walks every distribution registering this
        # entry-point group, not just the one being checked -- one unrelated distribution
        # with broken metadata must not crash `check_component` on a provider that has
        # nothing to do with it.
        if entry_point.dist is not None and isinstance(entry_point.dist.name, str)
    }
    if canonical_distribution_name in registered:
        return
    name = getattr(component, "__name__", type(component).__name__)
    yield ComponentProblem(
        code=PROVIDER_NO_ENTRY_POINT,
        message=(
            f"The distribution providing `{name}` ({distribution_name!r}) registers no "
            f"`{_PROVIDER_ENTRY_POINT_GROUP}` entry point."
        ),
        hint=(
            f"Add an `[project.entry-points.{_PROVIDER_ENTRY_POINT_GROUP}]` table "
            f"pointing at `{name}`; without it, `ProvidersManager` never calls this "
            f"function and the provider is not discovered at all."
        ),
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


def _is_xcom(component: object) -> bool:
    """Report whether a component is a `BaseXCom` subclass.

    Gated on `installed_family()` first, before importing anything from `airflow.sdk`:
    that namespace does not exist at all on 2.x, and every classifier here is called
    unconditionally by `check_component`'s auto-detect path regardless of what kind of
    component it is actually looking at, so an unguarded import would turn a 2.x
    environment's classification of an unrelated component (a timetable, say) into a
    hard crash instead of a clean non-match.

    Parameters:
        component: object containing the class or instance under check.

    Returns:
        bool indicating whether the component's type subclasses `BaseXCom`.
    """

    if installed_family() is not AirflowFamily.V3:
        return False
    try:
        from airflow.sdk.bases.xcom import BaseXCom
    except ImportError:
        return False

    return issubclass(_as_type(component), BaseXCom)


def _is_weight_strategy(component: object) -> bool:
    """Report whether a component is a `PriorityWeightStrategy` subclass.

    Parameters:
        component: object containing the class or instance under check.

    Returns:
        bool indicating whether the component's type subclasses `PriorityWeightStrategy`.
    """

    if installed_family() is not AirflowFamily.V3:
        return False
    try:
        from airflow.task.priority_strategy import PriorityWeightStrategy
    except ImportError:
        return False

    return issubclass(_as_type(component), PriorityWeightStrategy)


def _is_notifier(component: object) -> bool:
    """Report whether a component is a `BaseNotifier` subclass.

    Parameters:
        component: object containing the class or instance under check.

    Returns:
        bool indicating whether the component's type subclasses `BaseNotifier`.
    """

    if installed_family() is not AirflowFamily.V3:
        return False
    try:
        from airflow.sdk.bases.notifier import BaseNotifier
    except ImportError:
        return False

    return issubclass(_as_type(component), BaseNotifier)


def _is_secrets_backend(component: object) -> bool:
    """Report whether a component is a `BaseSecretsBackend` subclass.

    Parameters:
        component: object containing the class or instance under check.

    Returns:
        bool indicating whether the component's type subclasses `BaseSecretsBackend`.
    """

    if installed_family() is not AirflowFamily.V3:
        return False
    try:
        from airflow.secrets.base_secrets import BaseSecretsBackend
    except ImportError:
        return False

    return issubclass(_as_type(component), BaseSecretsBackend)


def _is_policy(component: object) -> bool:
    """Report whether a component defines at least one `airflow.policies` hookimpl method.

    Parameters:
        component: object containing the class or instance under check.

    Returns:
        bool indicating whether any callable member carries the policy hookimpl marker.
    """

    if installed_family() is not AirflowFamily.V3:
        return False
    try:
        marker = _policy_marker_attribute("impl")
    except ImportError:
        return False
    component_type = _as_type(component)
    return any(hasattr(value, marker) for _, value in inspect.getmembers(component_type, callable))


def _is_plugin(component: object) -> bool:
    """Report whether a component duck-types as a valid Airflow plugin.

    Matches Airflow's own `is_valid_plugin` exactly, minus the `.validate()` call it
    makes on a match: `validate()` raises `AirflowPluginException` when `name` is unset,
    and a classifier used unconditionally by `check_component`'s auto-detect path must
    never raise. `plugin-name-missing` reports that same condition safely instead. Real
    Airflow matches by MRO member name and module substring rather than `issubclass`
    on purpose (see the real function's own comment): the shared plugin base is
    accessed via different symlinked paths from core and the Task SDK, which Python
    treats as distinct classes, so a provider's plugin genuinely inheriting the SDK's
    `AirflowPlugin` would wrongly fail an `issubclass` check against the core one.

    Parameters:
        component: object containing the class or instance under check.

    Returns:
        bool indicating whether the component duck-types as an `AirflowPlugin` subclass.
    """

    if installed_family() is not AirflowFamily.V3:
        return False
    component_type = _as_type(component)
    if component_type.__name__ == "AirflowPlugin":
        return False
    return any(
        base.__name__ == "AirflowPlugin" and "plugins_manager" in base.__module__
        for base in component_type.__mro__
    )


def _is_provider(component: object) -> bool:
    """Report whether a component looks like a `get_provider_info`-shaped callable.

    Every real Airflow provider names this function `get_provider_info` by convention
    -- it is how `ProvidersManager` looks it up once loaded, and how a human reading the
    package finds it. A bare class is excluded even though classes are callable, since
    `AirflowPlugin`, `BaseExecutor`, and friends would otherwise all match.

    Parameters:
        component: object containing the class or instance under check.

    Returns:
        bool indicating whether the component is a non-class callable named
        `get_provider_info`.
    """

    if installed_family() is not AirflowFamily.V3:
        return False
    return (
        callable(component)
        and not isinstance(component, type)
        and getattr(component, "__name__", None) == "get_provider_info"
    )


KIND_CLASSIFIERS: dict[str, Callable[[object], bool]] = {
    TIMETABLE: _is_timetable,
    LISTENER: _is_listener,
    EXECUTOR: _is_executor,
    XCOM: _is_xcom,
    WEIGHT_STRATEGY: _is_weight_strategy,
    NOTIFIER: _is_notifier,
    SECRETS_BACKEND: _is_secrets_backend,
    POLICY: _is_policy,
    PLUGIN: _is_plugin,
    PROVIDER: _is_provider,
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
    (XCOM, XCOM_ORM_DESERIALIZE_REMOVED, _check_xcom_orm_deserialize_removed),
    (XCOM, XCOM_BACKEND_SIGNATURE, _check_xcom_backend_signature),
    (WEIGHT_STRATEGY, WEIGHT_STRATEGY_ABSTRACT, _check_weight_strategy_abstract),
    (WEIGHT_STRATEGY, WEIGHT_STRATEGY_HASH_OF_NONE, _check_weight_strategy_hash_of_none),
    (NOTIFIER, NOTIFIER_MISSING_NOTIFY, _check_notifier_missing_notify),
    (
        NOTIFIER,
        NOTIFIER_TEMPLATE_FIELDS_UNRESOLVABLE,
        _check_notifier_template_fields_unresolvable,
    ),
    (SECRETS_BACKEND, SECRETS_BACKEND_RAISES_ON_MISS, _check_secrets_backend_raises_on_miss),
    (POLICY, POLICY_UNKNOWN_HOOKSPEC, _check_policy_unknown_hookspec),
    (POLICY, POLICY_ARGUMENT_NAME_MISMATCH, _check_policy_argument_name_mismatch),
    (PLUGIN, PLUGIN_NAME_MISSING, _check_plugin_name_missing),
    (PROVIDER, PROVIDER_INFO_SCHEMA, _check_provider_info_schema),
    (PROVIDER, PROVIDER_PACKAGE_NAME_MISMATCH, _check_provider_package_name_mismatch),
    (PROVIDER, PROVIDER_NO_ENTRY_POINT, _check_provider_no_entry_point),
)

__all__ = (
    "CHECK_REGISTRY",
    "EXECUTOR",
    "KIND_CLASSIFIERS",
    "LISTENER",
    "NOTIFIER",
    "PLUGIN",
    "POLICY",
    "PROVIDER",
    "SECRETS_BACKEND",
    "TIMETABLE",
    "WEIGHT_STRATEGY",
    "XCOM",
    "ComponentProblem",
)
