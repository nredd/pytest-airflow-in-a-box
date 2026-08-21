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
import logging
import sys
from dataclasses import dataclass
from importlib import import_module, metadata
from pathlib import Path
from typing import TYPE_CHECKING, cast, get_args

from packaging.utils import canonicalize_name

from pytest_airflow_in_a_box._compat.capabilities import (
    CERTIFIED_CORE_PLUGINS_MANAGER_CACHES,
    CERTIFIED_SDK_PLUGINS_MANAGER_CACHES,
    CERTIFIED_SHARED_MODULE_LOADING_CACHES,
    AirflowCompatibilityError,
    AirflowFamily,
    CertificationTier,
    CertifiedCaches,
    ExecutorContract,
    PluginsManagerShape,
    SharedModuleLoading,
    installed_family,
    resolve_capabilities,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from types import ModuleType
    from typing import Any

    from airflow.executors.executor_utils import ExecutorName
    from airflow.secrets.base_secrets import BaseSecretsBackend

LOGGER = logging.getLogger(__name__)

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

# =============================================================================
# Runtime component sandbox
# =============================================================================
#
# Everything above is pure and static -- a checker never touches Airflow's live,
# process-global registries. This section backs
# `pytest_airflow_in_a_box.fixtures.components.airflow_components` instead: it
# registers a real component into Airflow's process-global plugin, listener, policy,
# secrets-backend, or executor state for the duration of one test, then reverts every
# one of them, so the next test starts from the same baseline this one did.
#
# LAZY CONTRACT RESOLUTION, a deliberate deviation from `_compat.capabilities`'s
# eager-verification norm: `resolve_capabilities()` probes and verifies its whole
# contract once, unconditionally, the moment any fixture first needs Airflow -- every
# session pays that cost. The cache-enumeration contract this section verifies
# (`clear_plugins_manager_caches` below) instead resolves lazily, on first
# `airflow_components` use, and is never wired into `resolve_capabilities()` or
# `_verify_contract`. Verifying it means importing `airflow.plugins_manager`,
# `airflow.sdk.plugins_manager`, and (for the executor snapshot) the module carrying
# `ExecutorLoader`, which imports `airflow.executors.base_executor` -- real, avoidable
# cost the overwhelming majority of test sessions, which never register a single custom
# component, would otherwise pay for nothing. Constructing `airflow_components` is what
# triggers it, exactly once per test, not once per process: see
# `clear_plugins_manager_caches`'s own docstring for why re-verifying every time (rather
# than caching success the way `resolve_capabilities()` does) is the safer default here.
#
# THE LOAD-BEARING DECISION: cache-clearable callables are enumerated BY INTROSPECTION
# (walking `vars(module)` for anything exposing a callable `.cache_clear`), then the
# observed name set is verified against the certified `CertifiedCaches` row in
# `_compat.capabilities` -- a missing `required` name or an uncertified extra is a hard
# failure (`ComponentSandboxError`), never a silent skip, while a certified-but-absent
# `optional` name (upstream added it mid-release-line) is tolerated. Pure introspection
# with no certified cross-check would under-clear silently the moment upstream adds
# another cache function in a release this plugin has not certified yet -- surfacing
# three files later as unexplained cross-test contamination, the worst failure mode this
# feature can have. A pure hardcoded table (introspection dropped entirely) is
# unmaintainable at eleven names times a dozen releases; certifying by
# `PluginsManagerShape`/`SharedModuleLoading` instead of by release keeps the table at
# two rows apiece. Do NOT simplify this to bare introspection with no certified
# cross-check, and do NOT simplify it to a bare hardcoded table with no introspection --
# either alone reintroduces the exact failure this design exists to prevent.
#
# Upstream's OWN `clear_lru_cache` fixture (`devel-common/src/tests_common/pytest_plugin.py`
# in the Airflow source tree) is DEAD CODE against a pip-installed Airflow: it clears
# caches by walking `airflow_shared.plugins_manager` and `airflow.utils.entry_points`
# unconditionally, and neither is an importable module in a wheel install on any
# certified release here -- `airflow_shared` is a source-tree-only vendoring alias for
# what a wheel install exposes as `airflow._shared`/`airflow.sdk._shared`, and
# `airflow.utils.entry_points` (the genuine pre-3.2 home of `_get_grouped_entry_points`,
# see `SharedModuleLoading`) was renamed away entirely in 3.2. If the clearing logic
# below looks like it should converge with that upstream fixture, it should NOT -- this
# comment is that explanation, so nobody "fixes" the divergence by mistake.


class ComponentSandboxError(Exception):
    """Report that the component sandbox itself refused or could not complete a request.

    Distinct from `ComponentContractError`: that one means the *component* is broken
    (`check_component` found a real conformance problem). This one means the sandbox
    mechanics could not do what was asked -- an unsupported dual-registration request, an
    executor class with no importable module path, an unknown policy hookspec name, or
    the installed release's live cache-clearable names no longer matching what this
    plugin has certified.
    """


def _plugins_manager_modules() -> tuple[Any, Any | None]:
    """Import both plugins-manager modules without static attribute constraints.

    A seam, mirroring `_compat.in_process._task_runner_module()`: a plain function
    returning `Any`, freely monkeypatchable in a test to substitute a fake module and
    make the `PluginsManagerShape.MODULE_GLOBALS` branch (real only on an actual 3.1.x
    install) coverable on this repository's 3.2+ development install.

    Returns:
        tuple[Any, Any | None] containing the core `airflow.plugins_manager` module and
        the Task SDK `airflow.sdk.plugins_manager` module, or None for the second when
        the installed release predates it -- 3.1.x's Task SDK carries no plugin-loading
        surface of its own at all (confirmed absent from the paired
        `apache-airflow-task-sdk==1.1.0` wheel; see PROVENANCE.md).
    """

    from airflow import plugins_manager as core_module

    try:
        from airflow.sdk import plugins_manager as sdk_module
    except (ImportError, AttributeError):
        return core_module, None
    return core_module, sdk_module


def listener_managers() -> tuple[Any, Any | None]:
    """Resolve both listener-manager instances without static attribute constraints.

    Resolves the actual `ListenerManager` instances (calling each release's cached
    `get_listener_manager()` getter), not the getters themselves: the getters must never
    be cleared -- see `clear_plugins_manager_caches`'s docstring -- so there is nothing a
    caller should do with them beyond this one resolution.

    Returns:
        tuple[Any, Any | None] containing the core `ListenerManager` instance and the
        Task SDK one, or None for the second when the installed release predates it
        (3.1.x; confirmed no `airflow.sdk.listener` module in the paired
        `apache-airflow-task-sdk==1.1.0` wheel, matching PROVENANCE.md's existing
        `sdk_listener_manager_available` finding).
    """

    from airflow.listeners.listener import get_listener_manager as get_core_manager

    core_manager = get_core_manager()
    try:
        from airflow.sdk.listener import get_listener_manager as get_sdk_manager
    except (ImportError, AttributeError):
        return core_manager, None
    return core_manager, get_sdk_manager()


def policy_plugin_manager() -> Any:
    """Resolve the policy plugin manager without static attribute constraints.

    Two certified shapes, probed by import failure like `listener_managers`: 3.2+
    exposes the cached getter `airflow.settings.get_policy_plugin_manager()`, while
    3.1.x has only the module global `airflow.settings.POLICY_PLUGIN_MANAGER`, assigned
    by `import_local_settings()` during `settings.initialize()` (confirmed by reading
    the `apache-airflow-core==3.1.0` and `==3.1.8` wheels' `settings.py`; see
    PROVENANCE.md). The 3.2+ getter's cache must never be cleared -- like the listener
    managers, clearing it would rebuild a fresh manager on the next call and lose the
    run's own `airflow_local_settings.py`-derived policy plugin (if any) until something
    re-triggers `import_local_settings()`, which nothing in a running test session does.

    Returns:
        Any containing the live policy `pluggy.PluginManager`.

    Raises:
        ComponentSandboxError: The 3.1.x module global is still None, meaning
            `settings.initialize()` has not run -- impossible through the
            `airflow_components` fixture, whose bootstrap always initializes settings
            first, so this is a loud guard against direct misuse.
    """

    try:
        from airflow.settings import get_policy_plugin_manager
    except ImportError:
        # A dynamic, string-based import on purpose, mirroring
        # `_shared_module_loading_modules`'s SINGLE branch: `POLICY_PLUGIN_MANAGER`
        # does not exist on the certified 3.2+ releases this project is actually
        # developed and type-checked against, so a static attribute access on the
        # statically-imported `airflow.settings` does not resolve under `ty`.
        settings_module = import_module("airflow.settings")

        manager = settings_module.POLICY_PLUGIN_MANAGER
        if manager is None:
            raise ComponentSandboxError(
                "`airflow.settings.POLICY_PLUGIN_MANAGER` is None: "
                "`settings.initialize()` has not run yet, so there is no policy plugin "
                "manager to register with."
                # `from None` on purpose: the swallowed ImportError is the 3.1.x shape
                # probe, not the cause of this failure.
            ) from None
        return manager
    return get_policy_plugin_manager()


def _shared_module_loading_modules(shape: SharedModuleLoading) -> tuple[Any, ...]:
    """Import the module(s) carrying `_get_grouped_entry_points` for one vendoring shape.

    `_get_grouped_entry_points` itself is identical on every certified release; only its
    module location, and on 3.2+ its duplication into an independently-cached Task SDK
    copy, changes. Verified against the installed 3.3.0
    (`airflow._shared.module_loading`, `airflow.sdk._shared.module_loading` -- distinct
    module objects) and the installed `apache-airflow-core==3.1.0` wheel
    (`airflow.utils.entry_points`, byte-for-byte the same function body); see
    PROVENANCE.md.

    Parameters:
        shape: SharedModuleLoading naming the certified vendoring shape.

    Returns:
        tuple[Any, ...] containing one module for SINGLE, two for DUPLICATED.
    """

    if shape is SharedModuleLoading.SINGLE:
        # A dynamic, string-based import on purpose: on the certified 3.2+ releases
        # this project is actually developed and type-checked against,
        # `airflow.utils.entry_points` survives only as a PEP 562 deprecation shim (an
        # essentially empty namespace forwarding to `airflow._shared.module_loading`),
        # so a static `from airflow.utils import entry_points` does not resolve under
        # `ty`. The shim also means `import_module` here SUCCEEDS on 3.2+ -- returning
        # an empty stub with none of the certified cache functions -- which is why the
        # compat test for this branch asserts the resolved function set, not just the
        # module name. The branch is reached only when `shape is SINGLE`, i.e. only on
        # the 3.1.x releases where the real module exists; see PROVENANCE.md.
        module = import_module("airflow.utils.entry_points")

        return (module,)
    from airflow._shared import module_loading as core_module
    from airflow.sdk._shared import module_loading as sdk_module

    return (core_module, sdk_module)


def _cache_clearable_names(module: Any) -> frozenset[str]:
    """Enumerate cache-clearable callable names on one module by introspection.

    A name is cache-clearable when its bound value exposes a callable `.cache_clear` --
    the structural marker every `functools.cache`/`functools.lru_cache` wrapper carries,
    independent of whether the cache happens to be populated right now. This is what
    makes `_verify_and_clear_cache_functions` a real, symmetric-difference-capable
    enumeration rather than a name-matching presence check.

    Parameters:
        module: Any containing the module to introspect.

    Returns:
        frozenset[str] containing every module attribute name exposing `.cache_clear`.
    """

    return frozenset(
        name
        for name, value in vars(module).items()
        if callable(getattr(value, "cache_clear", None))
    )


def _verify_and_clear_cache_functions(
    module: Any,
    certified: CertifiedCaches,
    certification: CertificationTier = CertificationTier.CERTIFIED,
) -> None:
    """Verify observed `functools.cache` names against the certified sets, then clear them.

    On the `CERTIFIED` tier the observed set must be a superset of `certified.required`
    and a subset of `certified.required | certified.optional`: a missing required name
    and an uncertified extra are both hard failures, while a certified-but-absent
    optional name (a cache function upstream added mid-release-line, absent on the
    earlier releases the same `PluginsManagerShape` row covers) is tolerated, and only
    names that are both certified and actually present are cleared.

    On the `PROBED` tier drift degrades instead of failing: every observed
    cache-clearable name is cleared generically -- introspection discovers uncertified
    extras exactly as reliably as certified names, so an unknown upstream cache is
    snapshot-cleared byte-for-byte -- and any missing/extra drift is logged once per
    call so the lost vetting stays visible in test logs. What the degraded tier gives
    up is semantic vetting of the name set, not cache isolation.

    Parameters:
        module: Any containing the module to introspect and clear.
        certified: CertifiedCaches containing the certified cache-clearable names.
        certification: CertificationTier selecting hard-fail or degrade on drift.
            Defaults to `CERTIFIED` so a caller that never resolved a tier stays on
            the strict, fail-closed contract.

    Raises:
        ComponentSandboxError: On the `CERTIFIED` tier, the observed name set is
            missing a required name or contains an uncertified extra.
    """

    observed = _cache_clearable_names(module)
    allowed = certified.required | certified.optional
    missing = sorted(certified.required - observed)
    extra = sorted(observed - allowed)
    if missing or extra:
        if certification is CertificationTier.CERTIFIED:
            raise ComponentSandboxError(
                f"`{module.__name__}`'s cache-clearable callables no longer match the "
                f"certified set for the installed Apache Airflow release: missing "
                f"{missing}, uncertified extra {extra} (certified-but-absent optional "
                f"names are tolerated and not listed). This plugin's `airflow_components` "
                f"snapshot/restore machinery is out of date for this release; file an issue."
            )
        LOGGER.warning(
            f"`{module.__name__}`'s cache-clearable callables drifted from the last "
            f"certified set (missing {missing}, uncertified extra {extra}); the "
            f"installed Apache Airflow release is uncertified, so every observed cache "
            f"is cleared generically instead of failing. Upgrade "
            f"`pytest-airflow-in-a-box` for a certified row."
        )
    names_to_clear = observed if certification is CertificationTier.PROBED else observed & allowed
    for name in names_to_clear:
        getattr(module, name).cache_clear()


# `loaded_plugins`/`import_errors` reset to an empty container of their own declared
# type, never `None` -- every other certified `PluginsManagerShape.MODULE_GLOBALS` name
# resets to `None`, its declared pre-load sentinel. Transcribed by reading the installed
# `apache-airflow-core==3.1.0` wheel's `airflow/plugins_manager.py` module-level
# declarations directly; see PROVENANCE.md.
_MODULE_GLOBAL_EMPTY_CONTAINER_RESETS: dict[str, Callable[[], object]] = {
    "loaded_plugins": set,
    "import_errors": dict,
}


def _verify_and_reset_module_globals(
    module: Any,
    certified: CertifiedCaches,
    certification: CertificationTier = CertificationTier.CERTIFIED,
) -> None:
    """Verify every certified global is present, then reset each to its cache-empty value.

    Only `certified.required` participates -- the MODULE_GLOBALS rows certify a closed,
    fully released 3.1.x line, so their `optional` set is structurally empty.

    No symmetric-difference check runs here, unlike `_verify_and_clear_cache_functions`:
    a plain module-level global carries no structural marker distinguishing lazily-cached
    plugin state from any other module attribute (`log = logging.getLogger(__name__)`
    included), so there is no safe way to *discover* an uncertified one by introspection
    the way `.cache_clear` discovers a `functools.cache` function. `certified` exists
    purely as a permanently fixed, hand-verified fact about a release line Airflow will
    never modify again -- 3.1.x is fully released and closed -- so presence-only
    verification is not a weaker substitute for symmetric difference here; it is the
    complete check, since there is no future 3.1.x release that could add an uncertified
    name for a symmetric-difference check to catch.

    On the `PROBED` tier a missing certified name degrades instead of failing: the
    present names still reset, and the missing ones are logged. Unlike the cache-clear
    path there is no generic discovery of uncertified extras here -- an unknown plain
    global is structurally invisible -- which is exactly the semantic vetting the
    degraded tier gives up.

    Parameters:
        module: Any containing the module to introspect and reset.
        certified: CertifiedCaches containing the certified global names.
        certification: CertificationTier selecting hard-fail or degrade on a missing
            name. Defaults to `CERTIFIED` so a caller that never resolved a tier stays
            on the strict, fail-closed contract.

    Raises:
        ComponentSandboxError: On the `CERTIFIED` tier, a certified name is missing
            from `module`.
    """

    missing = sorted(name for name in certified.required if not hasattr(module, name))
    if missing:
        if certification is CertificationTier.CERTIFIED:
            raise ComponentSandboxError(
                f"`{module.__name__}` no longer defines the certified plugin-cache globals "
                f"{missing}. This plugin's `airflow_components` snapshot/restore machinery "
                f"is out of date for the installed Apache Airflow release; file an issue."
            )
        LOGGER.warning(
            f"`{module.__name__}` no longer defines the certified plugin-cache globals "
            f"{missing}; the installed Apache Airflow release is uncertified, so the "
            f"present globals reset and the missing ones are skipped. Upgrade "
            f"`pytest-airflow-in-a-box` for a certified row."
        )
    for name in certified.required:
        if not hasattr(module, name):
            continue
        factory = _MODULE_GLOBAL_EMPTY_CONTAINER_RESETS.get(name)
        setattr(module, name, factory() if factory is not None else None)


def clear_plugins_manager_caches() -> None:
    """Verify and clear every certified plugins-manager and shared-module-loading cache.

    Called at both sandbox construction and teardown -- on construction, so a stale
    pre-test load cannot win; on teardown, so nothing this test computed lingers for the
    next one. Each call re-verifies the certified names against the live installed
    release rather than trusting a cached prior success: this is deliberately NOT the
    `resolve_capabilities()`-style "verify once, trust forever" shape, because the whole
    point of certifying by capability rather than by hardcoded release table is to catch
    upstream drift the moment it is observed, and a component-heavy test session calling
    this many times over its life is exactly where that vigilance is cheap (every import
    it touches is already `sys.modules`-cached after the first call) and valuable.

    On the `PROBED` tier (an uncertified 3.x release, see `CertificationTier`) shape
    drift degrades instead of failing: an SDK plugins manager missing where the
    derived shape expects one is skipped with a warning, an SDK plugins manager
    present where the derived shape expects none is cleared generically, and the
    per-module verification inside `_verify_and_clear_cache_functions` /
    `_verify_and_reset_module_globals` warns and clears generically rather than
    raising. The 2.x guard stays unconditional -- it is a self-consistency check, not
    release drift.

    Raises:
        ComponentSandboxError: The resolved release is 2.x, which has neither a
            plugins-manager cache mechanism nor a Task SDK to duplicate shared module
            loading into -- `airflow_components` itself never reaches this call on 2.x
            (see `v2_gate_message`), so this guard is a self-consistency check against
            direct misuse, not a real code path. On the `CERTIFIED` tier only: the
            installed release's live cache-clearable names, or
            `_get_grouped_entry_points` module presence, no longer match the certified
            set in `_compat.capabilities` for the resolved `PluginsManagerShape` /
            `SharedModuleLoading`.
    """

    capabilities = resolve_capabilities()
    shape = capabilities.plugins_manager
    shared_shape = capabilities.shared_module_loading
    certification = capabilities.certification
    if shape is None or shared_shape is None:
        raise ComponentSandboxError(
            "the runtime component sandbox is unavailable on the Airflow 2.x family, "
            "which has neither a plugins-manager cache mechanism nor a Task SDK to "
            "duplicate shared module loading into."
        )
    core_module, sdk_module = _plugins_manager_modules()
    core_certified = CERTIFIED_CORE_PLUGINS_MANAGER_CACHES[shape]
    sdk_certified = CERTIFIED_SDK_PLUGINS_MANAGER_CACHES[shape]
    if shape is PluginsManagerShape.CACHED_FUNCTIONS:
        _verify_and_clear_cache_functions(core_module, core_certified, certification)
        if sdk_module is None:
            if certification is CertificationTier.CERTIFIED:
                raise ComponentSandboxError(
                    "certified `cached-functions` expects `airflow.sdk.plugins_manager` to "
                    "exist, but it is not importable on the installed Apache Airflow release."
                )
            LOGGER.warning(
                "`airflow.sdk.plugins_manager` is not importable where the derived "
                "`cached-functions` shape expects it; the installed Apache Airflow "
                "release is uncertified, so the SDK half of the cache clear is skipped."
            )
        else:
            _verify_and_clear_cache_functions(sdk_module, sdk_certified, certification)
    else:
        _verify_and_reset_module_globals(core_module, core_certified, certification)
        if sdk_module is not None:
            if certification is CertificationTier.CERTIFIED:
                raise ComponentSandboxError(
                    "certified `module-globals` expects no `airflow.sdk.plugins_manager`, "
                    "but the installed Apache Airflow release provides one."
                )
            # An unexpected SDK plugins manager on an uncertified release still holds
            # cached state a sandboxed test must not leak; clear it generically. The
            # MODULE_GLOBALS row's SDK table is empty, so every observed name counts
            # as drift and `_verify_and_clear_cache_functions` logs it once.
            _verify_and_clear_cache_functions(sdk_module, sdk_certified, certification)

    shared_certified = CERTIFIED_SHARED_MODULE_LOADING_CACHES[shared_shape]
    for module in _shared_module_loading_modules(shared_shape):
        _verify_and_clear_cache_functions(module, shared_certified, certification)


def listener_manager_snapshot(manager: Any) -> tuple[Any, ...]:
    """Snapshot the listener objects currently registered on one manager.

    Parameters:
        manager: Any containing a `ListenerManager` instance.

    Returns:
        tuple[Any, ...] containing every currently registered listener object.
    """

    return tuple(manager.pm.get_plugins())


def listener_manager_restore(manager: Any, snapshot: tuple[Any, ...]) -> None:
    """Clear one manager and re-register exactly its pre-sandbox listener set.

    Uses `ListenerManager.clear()`, never `get_listener_manager.cache_clear()`: clearing
    the getter's own cache would rebuild a brand-new manager on the next call and re-run
    `integrate_listener_plugins` against whatever the plugins-manager cache holds at that
    later moment -- losing this manager's identity, and its already-registered
    hookspecs, for no reason `clear()` does not already serve.

    Parameters:
        manager: Any containing a `ListenerManager` instance.
        snapshot: tuple[Any, ...] containing the listener objects to restore.
    """

    manager.clear()
    for listener in snapshot:
        manager.add_listener(listener)


def register_listener(component: object, managers: tuple[Any, ...]) -> object:
    """Instantiate a listener class if needed and register it with every given manager.

    Parameters:
        component: object containing the listener class or instance to register.
        managers: tuple[Any, ...] containing every `ListenerManager` to register with.

    Returns:
        object containing the live listener instance that was registered.
    """

    instance = component() if isinstance(component, type) else component
    for manager in managers:
        manager.add_listener(instance)
    return instance


# Every mutable list attribute `airflow._shared.plugins_manager.AirflowPlugin` declares,
# transcribed by reading that class directly (see PROVENANCE.md). Each defaults to a
# class-level `[]` shared across every instance that does not override it, and
# `_get_ui_plugins` (plus its sibling cache functions) mutate several of these in place
# (`.remove()`) while building their own return value.
PLUGIN_LIST_ATTRIBUTES = (
    "macros",
    "admin_views",
    "flask_blueprints",
    "fastapi_apps",
    "fastapi_root_middlewares",
    "external_views",
    "react_apps",
    "menu_links",
    "appbuilder_views",
    "appbuilder_menu_items",
    "global_operator_extra_links",
    "operator_extra_links",
    "timetables",
    "partition_mappers",
    "windows",
    "deadline_references",
    "listeners",
    "hook_lineage_readers",
    "priority_weight_strategies",
)


def _prepare_plugin_instance(component: object) -> object:
    """Instantiate a plugin class if needed and shallow-copy its mutable list attributes.

    `_get_ui_plugins` (and its sibling cache functions) call `.remove()` on a plugin's
    `external_views`/`react_apps`/etc in place while building their own return value.
    Every one of `PLUGIN_LIST_ATTRIBUTES` defaults to the SAME class-level `[]` object
    declared on `AirflowPlugin` itself when a subclass does not override it, and even an
    overridden one may be a class attribute shared across every instance of that class --
    so a second registration of "the same" plugin class (a module-level fixture reused by
    two tests, or a plugin object a test happens to hold onto and register twice) would
    see a list an earlier `.remove()` call already mutated. Rebinding every list
    attribute to a fresh `list(...)` onto THIS instance specifically, at registration
    time, makes every registration start from an unmutated list regardless of what
    happened to any other instance or to the class default.

    A SHALLOW list copy on purpose, never `copy.deepcopy`: the documented failure mode
    is in-place mutation of the list CONTAINER, so copying the container fully defeats
    it, while copying the elements would break real plugins twice over -- the canonical
    `listeners = [some_module]` shape (Airflow's own shipped
    `example_dags/plugins/listener_plugin.py`) cannot deepcopy a module at all
    (`TypeError: cannot pickle 'module' object`), and where elements CAN copy, Airflow
    would silently register the copies, so a test asserting against the original
    objects would observe nothing.

    Parameters:
        component: object containing the plugin class or instance to register.

    Returns:
        object containing a live plugin instance with independent list attributes.
    """

    instance = component() if isinstance(component, type) else component
    for name in PLUGIN_LIST_ATTRIBUTES:
        if hasattr(instance, name):
            setattr(instance, name, list(getattr(instance, name)))
    return instance


def _live_plugin_list(module: Any) -> list[Any]:
    """Resolve one plugins-manager half's live, appendable plugin list.

    Branches on the same structural fact `PluginsManagerShape` certifies, probed
    directly on the module so a seam-substituted fake drives the branch in tests: the
    3.2+ CACHED_FUNCTIONS shape stores the list inside `_get_plugins()`'s cached
    `(plugins, import_errors)` tuple, while the 3.1.x MODULE_GLOBALS shape keeps a plain
    `plugins` module global that is `None` until `ensure_plugins_loaded()` populates it
    (both names certified in `CERTIFIED_CORE_PLUGINS_MANAGER_CACHES`; transcribed from
    the `apache-airflow-core==3.1.0` wheel's `plugins_manager.py`, see PROVENANCE.md).

    Parameters:
        module: Any containing one plugins-manager module.

    Returns:
        list[Any] containing the module's live plugin list, loaded if it was not yet.
    """

    if hasattr(module, "_get_plugins"):
        return module._get_plugins()[0]
    module.ensure_plugins_loaded()
    return module.plugins


def register_plugin(component: object) -> object:
    """Register one plugin into every plugins-manager half the installed release has.

    Appends the SAME prepared instance to both the core and (when it exists) Task SDK
    plugin lists: a real, file-based plugin is independently discovered by both halves'
    own `plugins_folder` scan, so registering the identical object into both accurately
    simulates that -- and the Task SDK's three cache functions have no in-place list
    mutation of their own to guard against, unlike the core side's `_get_ui_plugins`.

    Parameters:
        component: object containing the plugin class or instance to register.

    Returns:
        object containing the live plugin instance that was registered.
    """

    instance = _prepare_plugin_instance(component)
    core_module, sdk_module = _plugins_manager_modules()
    _live_plugin_list(core_module).append(instance)
    if sdk_module is not None:
        _live_plugin_list(sdk_module).append(instance)
    return instance


def build_component_plugin(component_type: type, list_attribute: str) -> type:
    """Build a throwaway `AirflowPlugin` subclass carrying one component class.

    Subclasses the live plugins-manager module's own `AirflowPlugin` rather than
    `type(...)`-ing a bare class the way `build_policy_plugin` does: the plugins-manager
    cache functions iterate EVERY list attribute of EVERY registered plugin
    (`get_timetables_plugins` reads `plugin.timetables`, `_get_ui_plugins` reads
    `plugin.external_views`, and so on), so every one of `PLUGIN_LIST_ATTRIBUTES` must
    resolve on the synthesized plugin -- the base class's own class-level defaults
    provide exactly that.

    Parameters:
        component_type: type containing the component class to expose.
        list_attribute: str naming the `AirflowPlugin` list attribute to expose it on
            (`timetables` or `priority_weight_strategies`).

    Returns:
        type containing the unregistered `AirflowPlugin` subclass.
    """

    core_module, _sdk_module = _plugins_manager_modules()
    return type(
        "ComponentRegistryPlugin",
        (core_module.AirflowPlugin,),
        {
            "name": f"pytest-airflow-in-a-box-{list_attribute}-{component_type.__qualname__}",
            list_attribute: [component_type],
        },
    )


# The derived-lookup caches `invalidate_component_lookup_caches` drops, per
# `PluginsManagerShape`, expressed as data so the "never clear `_get_plugins`"
# invariant is pinnable against `CERTIFIED_CORE_PLUGINS_MANAGER_CACHES` (a compat test
# asserts each set is a strict subset of the matching certified `required` row).
DERIVED_LOOKUP_CACHE_FUNCTIONS = ("get_timetables_plugins", "get_priority_weight_strategy_plugins")
DERIVED_LOOKUP_MODULE_GLOBALS = ("timetable_classes", "priority_weight_strategy_classes")


def invalidate_component_lookup_caches() -> None:
    """Invalidate only the derived timetable and weight-strategy lookup caches.

    Deliberately NOT `clear_plugins_manager_caches()`: that clear also empties
    `_get_plugins` (3.2+) or resets the `plugins` module global (3.1.x), which would
    discard the throwaway plugin `register_plugin` just appended to the live plugin
    list -- exactly the registration this invalidation exists to expose. Only the two
    caches DERIVED from the plugin list are dropped, so the next
    `find_registered_custom_timetable` / `_get_registered_priority_weight_strategy`
    lookup rebuilds its qualname-keyed mapping from the live list, appended plugin
    included, even when an earlier lookup in the same test already populated the cache.

    Core module only: the Task SDK plugins-manager half certifies neither cache (see
    `CERTIFIED_SDK_PLUGINS_MANAGER_CACHES`), and both lookup paths live core-side.
    Branches on the same structural fact `_live_plugin_list` probes -- the 3.2+
    CACHED_FUNCTIONS shape exposes `functools.cache` getters, while the 3.1.x
    MODULE_GLOBALS shape keeps plain module globals whose `None` state is what makes
    `initialize_timetables_plugins()`-style loaders recompute (both name sets certified
    in `CERTIFIED_CORE_PLUGINS_MANAGER_CACHES`; see PROVENANCE.md).
    """

    core_module, _sdk_module = _plugins_manager_modules()
    if hasattr(core_module, "get_timetables_plugins"):
        for name in DERIVED_LOOKUP_CACHE_FUNCTIONS:
            getattr(core_module, name).cache_clear()
        return
    for name in DERIVED_LOOKUP_MODULE_GLOBALS:
        setattr(core_module, name, None)


def lookup_key(component_type: type) -> str:
    """Build the key Airflow's registered-component lookups map a class under.

    Upstream's `qualname()` helper keys a class by `__module__.__name__`, NOT
    `__qualname__` -- identical for the module-level classes real registrations
    require (see PROVENANCE.md).

    Parameters:
        component_type: type containing the component class.

    Returns:
        str containing the `module.name` lookup key.
    """

    return f"{component_type.__module__}.{component_type.__name__}"


def timetable_lookup_resolves(component_type: type) -> bool:
    """Report whether the registered-timetable lookup already resolves one class.

    True means the class is reachable the deployed way -- an `AirflowPlugin` loaded
    from the run's plugins folder or a venv entry point -- and needs no sandbox
    registration at all. Probes the same structural fact `_live_plugin_list` does: the
    3.2+ shape asks the cached getter, while the 3.1.x shape loads the
    `timetable_classes` module global through its own `initialize_timetables_plugins()`
    (a no-op when already populated).

    Parameters:
        component_type: type containing the timetable class to look up.

    Returns:
        bool marking the class as already resolvable by qualname.
    """

    core_module, _sdk_module = _plugins_manager_modules()
    key = lookup_key(component_type)
    if hasattr(core_module, "get_timetables_plugins"):
        return key in core_module.get_timetables_plugins()
    core_module.initialize_timetables_plugins()
    return key in (core_module.timetable_classes or {})


def _register_component(component: object, list_attribute: str) -> type:
    """Register one plugin-list-resolved component class through a throwaway plugin.

    The shared registration sequence behind `register_timetable` and
    `register_weight_strategy`, kept in one place so a change to it (a dedupe, an
    ordering constraint, a third cache to invalidate) cannot land in one kind and
    drift from the other. Appends unconditionally -- a repeated registration of the
    same class is harmless, since the derived lookup mapping is keyed by qualname and
    the sandbox teardown discards every appended plugin anyway.

    Parameters:
        component: object containing the component class or instance to register.
        list_attribute: str naming the `AirflowPlugin` list attribute to expose it on.

    Returns:
        type containing the class that was registered.
    """

    component_type = _as_type(component)
    register_plugin(build_component_plugin(component_type, list_attribute))
    invalidate_component_lookup_caches()
    return component_type


def register_timetable(component: object) -> type:
    """Register one timetable class through a synthesized throwaway plugin.

    Registration is what makes Airflow's serialization round trip resolve the class by
    qualname: `encode_timetable` refuses an unregistered custom timetable outright on
    3.1.x, and `decode_timetable` resolves through the plugins manager's
    qualname-to-class mapping on every certified 3.x release.

    Parameters:
        component: object containing the timetable class or instance to register.

    Returns:
        type containing the class that was registered.
    """

    return _register_component(component, "timetables")


def register_weight_strategy(component: object) -> type:
    """Register one priority weight strategy class through a synthesized throwaway plugin.

    Registration is what lets `_encode_priority_weight_strategy` accept the class at
    Dag serialization time on every certified 3.x release -- it refuses any custom
    strategy `_get_registered_priority_weight_strategy` cannot resolve by qualname.

    Parameters:
        component: object containing the weight strategy class or instance to register.

    Returns:
        type containing the class that was registered.
    """

    return _register_component(component, "priority_weight_strategies")


TIMETABLE_ROUND_TRIP_MISMATCH = "timetable-round-trip-mismatch"


def timetable_round_trip(component: object) -> tuple[ComponentProblem, ...]:
    """Run one timetable instance through `decode_timetable(encode_timetable(...))`.

    The caller registers the timetable first (`register_timetable`), so an exception
    out of either half is a genuine defect and propagates as-is -- an unregistered
    class raises Airflow's own not-registered error, which is precisely the loud
    failure this assertion exists to surface. What returns as problems instead of
    raising is serialize/deserialize ASYMMETRY: the decoded instance reconstructing as
    a different class, or reconstructing with a different `serialize()` payload than
    the original produced. Payloads are compared rather than instances because a
    timetable has no `__eq__` contract; when the class does define its own `__eq__`,
    the instances are additionally compared with it.

    Parameters:
        component: object containing the timetable instance to round-trip.

    Returns:
        tuple[ComponentProblem, ...] containing every asymmetry found, empty on success.
    """

    # Deferred to preserve pre-bootstrap import safety, like every Airflow import in
    # this module.
    from airflow.serialization.serialized_objects import decode_timetable, encode_timetable

    # `encode_timetable` is annotated against Airflow's `Timetable` Protocol, which
    # `component` satisfies only dynamically here -- same rationale as
    # `register_secrets_backend`'s cast: the caller's conformance gate already ran.
    timetable = cast("Any", component)
    decoded = decode_timetable(encode_timetable(timetable))
    if type(decoded) is not type(component):
        return (
            ComponentProblem(
                code=TIMETABLE_ROUND_TRIP_MISMATCH,
                message=(
                    f"`decode_timetable(encode_timetable(...))` reconstructed "
                    f"`{type(decoded).__name__}`, not `{type(component).__name__}`."
                ),
                hint=(
                    "Make `deserialize` a classmethod returning `cls(...)` so the "
                    "round trip reconstructs the class that serialized."
                ),
            ),
        )
    problems: list[ComponentProblem] = []
    original_payload = timetable.serialize()
    decoded_payload = decoded.serialize()
    if decoded_payload != original_payload:
        problems.append(
            ComponentProblem(
                code=TIMETABLE_ROUND_TRIP_MISMATCH,
                message=(
                    f"the reconstructed `{type(component).__name__}` serializes to "
                    f"{decoded_payload!r}, but the original serialized to "
                    f"{original_payload!r}."
                ),
                hint=(
                    "Make `serialize` and `deserialize` a symmetric pair: every key "
                    "`serialize` emits must survive `deserialize` and be emitted again."
                ),
            )
        )
    if _defining_class(type(component), "__eq__") is not object and decoded != component:
        problems.append(
            ComponentProblem(
                code=TIMETABLE_ROUND_TRIP_MISMATCH,
                message=(
                    f"the reconstructed `{type(component).__name__}` compares unequal "
                    f"to the original under the class's own `__eq__`."
                ),
                hint=(
                    "Make `__eq__` agree with the `serialize`/`deserialize` pair: two "
                    "instances carrying the same serialized payload should be equal."
                ),
            )
        )
    return tuple(problems)


def build_policy_plugin(hooks: dict[str, Callable[..., object]]) -> type:
    """Build an unregistered, hookimpl-decorated policy plugin class.

    Mirrors `airflow.policies.make_plugin_from_local_settings`'s own
    `setattr(cls, name, staticmethod(hookimpl(policy, specname=name)))` idiom for turning
    a bare callable into a real pluggy hookimpl (see PROVENANCE.md), but skips that
    function's silent per-name `hasattr(pm.hook, name)` tolerance and its automatic
    argument-mismatch shimming: the caller runs `check_component` against the result
    before anything registers, so an unknown hookspec name or a parameter name mismatch
    must surface as a loud, attributable problem, never a silent skip or an invisible
    shim nobody asked for.

    Parameters:
        hooks: dict[str, Callable[..., object]] mapping hookspec name to implementation.

    Returns:
        type containing the unregistered, hookimpl-decorated plugin class.
    """

    from airflow.policies import hookimpl

    namespace: dict[str, object] = {}
    for name, fn in hooks.items():
        namespace[name] = staticmethod(hookimpl(fn, specname=name))
    return type("ComponentRegistryPolicy", (), namespace)


def register_policy(plugin_class: type, pm: Any) -> object:
    """Instantiate and register one already-validated policy plugin class.

    Parameters:
        plugin_class: type containing a hookimpl-decorated policy plugin class.
        pm: Any containing the policy `pluggy.PluginManager` to register with.

    Returns:
        object containing the live plugin instance that was registered.
    """

    instance = plugin_class()
    pm.register(instance)
    return instance


def restore_policy_plugins(pm: Any, before: tuple[object, ...]) -> None:
    """Unregister every policy plugin not present in the pre-sandbox snapshot.

    Targeted, not a clear-and-restore: pluggy's plain `PluginManager` supports precise
    `unregister()`, unlike `ListenerManager`, so a plugin genuinely registered elsewhere
    (an `airflow_local_settings.py` cluster policy) is never disturbed.

    Parameters:
        pm: Any containing the policy `pluggy.PluginManager` to restore.
        before: tuple[object, ...] containing the pre-sandbox registered plugin objects.
    """

    for plugin in pm.get_plugins():
        if plugin not in before:
            pm.unregister(plugin)


def snapshot_secrets_backend_list() -> list[BaseSecretsBackend]:
    """Snapshot the current secrets backend list.

    Returns:
        list[BaseSecretsBackend] containing a shallow copy of
        `airflow.configuration.secrets_backend_list`.
    """

    from airflow.configuration import secrets_backend_list

    return list(secrets_backend_list)


def restore_secrets_backend_list(before: list[BaseSecretsBackend]) -> None:
    """Restore the secrets backend list by slice assignment.

    Never via `airflow.configuration.ensure_secrets_loaded`: its `len(...) == 2`
    reinitialization heuristic belongs to upstream, may change, and is not a restore
    operation at all -- it rebuilds from configuration rather than returning to a
    snapshot. Slice assignment mutates the existing list object in place rather than
    rebinding the module attribute to a new one, which matters here specifically:
    anything that read `secrets_backend_list` by reference before this call keeps
    working, since it is still the same list object, merely with different contents.

    Parameters:
        before: list[BaseSecretsBackend] containing the snapshot to restore.
    """

    from airflow.configuration import secrets_backend_list

    secrets_backend_list[:] = before


def register_secrets_backend(component: object, *, first: bool) -> BaseSecretsBackend:
    """Instantiate a secrets backend class if needed and insert it into the search path.

    Parameters:
        component: object containing the secrets backend class or instance to register.
        first: bool inserting at the front of the search path when True, the back when
            False.

    Returns:
        BaseSecretsBackend containing the live secrets backend instance that was
        registered. Typed precisely rather than as `object`, unlike `component`'s own
        parameter type: `check_component`'s conformance gate runs before this is ever
        called in practice, so by the time a real caller reaches here `component` is
        already known -- just not statically, to this function -- to actually be one.
    """

    from airflow.configuration import secrets_backend_list

    raw = component() if isinstance(component, type) else component
    instance = cast("BaseSecretsBackend", raw)
    if first:
        secrets_backend_list.insert(0, instance)
    else:
        secrets_backend_list.append(instance)
    return instance


def snapshot_task_instance_mutation_hook_is_noop() -> bool:
    """Snapshot `airflow.policies.task_instance_mutation_hook.is_noop`.

    Returns:
        bool containing the flag's current value.
    """

    from airflow.settings import task_instance_mutation_hook

    # `task_instance_mutation_hook` is a plain function object; `is_noop` is a flag
    # `airflow/settings.py` attaches to it dynamically
    # (`task_instance_mutation_hook.is_noop = True`), invisible to static typing
    # without this cast.
    return cast("Any", task_instance_mutation_hook).is_noop


def restore_task_instance_mutation_hook_is_noop(value: bool) -> None:
    """Restore `airflow.policies.task_instance_mutation_hook.is_noop`.

    `import_local_settings()` flips this module-level function attribute to `False` the
    moment any policy hookimpl for `task_instance_mutation_hook` is registered, and never
    flips it back -- a leaked `False` alters every subsequent test's mutation-hook
    dispatch cost (though not its outcome), so restoring it is cheap insurance against a
    real, if minor, observable difference between a run that used `airflow_components`
    and one that did not.

    Parameters:
        value: bool containing the value to restore.
    """

    from airflow.settings import task_instance_mutation_hook

    cast("Any", task_instance_mutation_hook).is_noop = value


def mark_task_instance_mutation_hook_active() -> None:
    """Flip `task_instance_mutation_hook.is_noop` to False so registered hooks fire.

    `airflow/settings.py` sets `is_noop = True` at import time, and the only upstream
    code that ever flips it is `import_local_settings()` -- which the sandbox's
    `policy()` deliberately bypasses (that decoupling is the whole point of registering
    directly with the policy plugin manager; see #109). Real dispatch sites
    (`DagRun.verify_integrity` among them) short-circuit on the flag and never invoke
    the pluggy hook while it is True, so a `task_instance_mutation_hook` hookimpl
    registered without this call would silently never fire. Mirrors
    `import_local_settings`'s own assignment; the sandbox's is_noop snapshot/restore
    pair reverts it at teardown.
    """

    from airflow.settings import task_instance_mutation_hook

    cast("Any", task_instance_mutation_hook).is_noop = False


def _executor_loader_module() -> Any:
    """Import `airflow.executors.executor_loader` without static attribute constraints.

    A seam, mirroring `_plugins_manager_modules()`: a plain function returning `Any`,
    freely monkeypatchable in a test to substitute a fake module and make the flat 3.1.x
    global shape (real only on an actual 3.1.x install) coverable on this repository's
    3.2+ development install.

    Returns:
        Any containing the `airflow.executors.executor_loader` module.
    """

    from airflow.executors import executor_loader as module

    return module


def _executor_loader_is_per_team(module: Any) -> bool:
    """Probe whether `executor_loader.py` carries the 3.2+ per-team global shape.

    3.2.0 split every flat lookup global (`_alias_to_executors` and siblings, each
    `dict[str, ExecutorName]`, plus scalar-valued `_team_name_to_executors`) into a
    `_per_team`-suffixed dict-of-dicts keyed by team name, with `_team_name_to_executors`
    becoming list-valued. A structural probe on the live module, not a release-table
    lookup, so a seam-substituted fake module drives the branch directly in tests. NOT
    derivable from `ExecutorContract` -- that enum certifies `BaseExecutor` *attribute*
    contracts for `check_component`, a different fact that merely happens to share the
    3.2 boundary today; do not merge the two.

    Parameters:
        module: Any containing the `executor_loader` module to probe.

    Returns:
        bool containing True for the 3.2+ per-team shape, False for the flat 3.1.x one.
    """

    return hasattr(module, "_alias_to_executors_per_team")


@dataclass(frozen=True)
class ExecutorLoaderSnapshot:
    """Immutable snapshot of `airflow.executors.executor_loader`'s mutable global state.

    The 3.2+ per-team shape; `ExecutorLoaderSnapshotV31` is the flat 3.1.x sibling.

    Parameters:
        executors: dict[str, str] containing `ExecutorLoader.executors`, the class dict
            upstream's own reset leaks between test runs.
        alias_to_executors_per_team: dict[str | None, dict[str, ExecutorName]]
            containing `_alias_to_executors_per_team`.
        module_to_executors_per_team: dict[str | None, dict[str, ExecutorName]]
            containing `_module_to_executors_per_team`.
        classname_to_executors_per_team: dict[str | None, dict[str, ExecutorName]]
            containing `_classname_to_executors_per_team`.
        team_name_to_executors: dict[str | None, list[ExecutorName]] containing
            `_team_name_to_executors`.
        executor_names: list[ExecutorName] containing `_executor_names`.
    """

    executors: dict[str, str]
    alias_to_executors_per_team: dict[str | None, dict[str, ExecutorName]]
    module_to_executors_per_team: dict[str | None, dict[str, ExecutorName]]
    classname_to_executors_per_team: dict[str | None, dict[str, ExecutorName]]
    team_name_to_executors: dict[str | None, list[ExecutorName]]
    executor_names: list[ExecutorName]


@dataclass(frozen=True)
class ExecutorLoaderSnapshotV31:
    """Immutable snapshot of the flat, pre-per-team 3.1.x `executor_loader.py` globals.

    Transcribed from the `apache-airflow-core==3.1.0` and `==3.1.8` wheels'
    `airflow/executors/executor_loader.py` (identical shape on both; see PROVENANCE.md):
    the lookup globals carry no `_per_team` suffix and are flat single-level dicts, and
    `_team_name_to_executors` maps each team name to ONE `ExecutorName` (the last one
    resolved for that team), not a list.

    Parameters:
        executors: dict[str, str] containing `ExecutorLoader.executors`.
        alias_to_executors: dict[str, ExecutorName] containing `_alias_to_executors`.
        module_to_executors: dict[str, ExecutorName] containing `_module_to_executors`.
        classname_to_executors: dict[str, ExecutorName] containing
            `_classname_to_executors`.
        team_name_to_executors: dict[str | None, ExecutorName] containing
            `_team_name_to_executors`.
        executor_names: list[ExecutorName] containing `_executor_names`.
    """

    executors: dict[str, str]
    alias_to_executors: dict[str, ExecutorName]
    module_to_executors: dict[str, ExecutorName]
    classname_to_executors: dict[str, ExecutorName]
    team_name_to_executors: dict[str | None, ExecutorName]
    executor_names: list[ExecutorName]


def snapshot_executor_loader() -> ExecutorLoaderSnapshot | ExecutorLoaderSnapshotV31:
    """Force natural executor-name resolution once, then snapshot the resulting state.

    Forcing `ExecutorLoader._get_executor_names()` before snapshotting ensures the
    snapshot captures the real, config-driven baseline exactly once per sandbox,
    regardless of whether anything had already triggered it -- `register_executor` below
    injects directly into these same five globals without going through config parsing
    at all, so nothing else in this sandbox would trigger that natural resolution later.

    Returns:
        ExecutorLoaderSnapshot | ExecutorLoaderSnapshotV31 containing independent copies
        of every mutable global, in the shape matching the installed release.
    """

    module = _executor_loader_module()
    module.ExecutorLoader._get_executor_names()
    if not _executor_loader_is_per_team(module):
        return ExecutorLoaderSnapshotV31(
            executors=dict(module.ExecutorLoader.executors),
            alias_to_executors=dict(module._alias_to_executors),
            module_to_executors=dict(module._module_to_executors),
            classname_to_executors=dict(module._classname_to_executors),
            team_name_to_executors=dict(module._team_name_to_executors),
            executor_names=list(module._executor_names),
        )
    return ExecutorLoaderSnapshot(
        executors=dict(module.ExecutorLoader.executors),
        alias_to_executors_per_team={
            team: dict(names) for team, names in module._alias_to_executors_per_team.items()
        },
        module_to_executors_per_team={
            team: dict(names) for team, names in module._module_to_executors_per_team.items()
        },
        classname_to_executors_per_team={
            team: dict(names) for team, names in module._classname_to_executors_per_team.items()
        },
        team_name_to_executors={
            team: list(names) for team, names in module._team_name_to_executors.items()
        },
        executor_names=list(module._executor_names),
    )


def restore_executor_loader(snapshot: ExecutorLoaderSnapshot | ExecutorLoaderSnapshotV31) -> None:
    """Restore every `executor_loader.py` global from a snapshot, in place.

    Every container is cleared and repopulated rather than rebound to a new object,
    matching `restore_secrets_backend_list`'s reasoning: nothing observed imports these
    specific names by reference today, but mutating in place costs nothing extra and
    stays correct if that ever changes. The snapshot's own type, not a fresh probe,
    selects the branch: the snapshot was built from the live module's actual shape, and
    dispatching on it keeps snapshot and restore structurally paired.

    Parameters:
        snapshot: ExecutorLoaderSnapshot | ExecutorLoaderSnapshotV31 containing the
            state to restore.
    """

    module = _executor_loader_module()
    module.ExecutorLoader.executors.clear()
    module.ExecutorLoader.executors.update(snapshot.executors)
    if isinstance(snapshot, ExecutorLoaderSnapshotV31):
        for flat_target, flat_source in (
            (module._alias_to_executors, snapshot.alias_to_executors),
            (module._module_to_executors, snapshot.module_to_executors),
            (module._classname_to_executors, snapshot.classname_to_executors),
            (module._team_name_to_executors, snapshot.team_name_to_executors),
        ):
            flat_target.clear()
            flat_target.update(flat_source)
        module._executor_names[:] = snapshot.executor_names
        return
    for target, source in (
        (module._alias_to_executors_per_team, snapshot.alias_to_executors_per_team),
        (module._module_to_executors_per_team, snapshot.module_to_executors_per_team),
        (module._classname_to_executors_per_team, snapshot.classname_to_executors_per_team),
    ):
        target.clear()
        target.update({team: dict(names) for team, names in source.items()})
    module._team_name_to_executors.clear()
    module._team_name_to_executors.update(
        {team: list(names) for team, names in snapshot.team_name_to_executors.items()}
    )
    module._executor_names[:] = snapshot.executor_names


def register_executor(component: object, *, alias: str) -> str:
    """Register one executor class under `alias` for the duration of the sandbox.

    Injects directly into `ExecutorLoader`'s lookup globals and its `_executor_names`
    list, bypassing `core.executor` config-string parsing entirely: a test author's
    executor class is never already named in the run's fixed `core.executor` config, so
    there is nothing for the real parsing path to find for it. `component` must be
    defined at module scope somewhere importable -- `ExecutorLoader.load_executor`
    resolves it later by dotted import path, and a class defined inside a test function
    has no such path.

    On the 3.2+ per-team shape the `None`-team buckets are created via `setdefault`
    rather than assumed present, so this function stays independently correct even when
    nothing (snapshot included) has forced natural resolution first. On the flat 3.1.x
    shape every write mirrors `_get_executor_names`'s own population loop, including
    the scalar `_team_name_to_executors[None]` overwrite (upstream's loop assigns, not
    appends -- last resolved executor per team wins) and the classname key, which
    upstream derives as `module_path.split(".")[-1]`, exactly the class `__name__` here.

    Parameters:
        component: object containing the executor class to register.
        alias: str naming the alias `ExecutorLoader.load_executor(alias)` resolves.

    Returns:
        str containing `alias`, unchanged, for the caller to pass into whichever Airflow
        configuration surface selects an executor by name.

    Raises:
        ComponentSandboxError: `component` has no real, importable module-level path.
    """

    from airflow.executors.executor_utils import ExecutorName

    module = _executor_loader_module()
    executor_type = component if isinstance(component, type) else type(component)
    module_path = f"{executor_type.__module__}.{executor_type.__qualname__}"
    if "<locals>" in executor_type.__qualname__:
        raise ComponentSandboxError(
            f"executor() requires a module-level class; `{module_path}` is defined "
            f"inside a function and has no importable path. Move it to module scope."
        )
    name = ExecutorName(alias=alias, module_path=module_path, team_name=None)
    if _executor_loader_is_per_team(module):
        module._alias_to_executors_per_team.setdefault(None, {})[alias] = name
        module._module_to_executors_per_team.setdefault(None, {})[module_path] = name
        module._classname_to_executors_per_team.setdefault(None, {})[executor_type.__name__] = name
        module._team_name_to_executors.setdefault(None, []).append(name)
    else:
        module._alias_to_executors[alias] = name
        module._module_to_executors[module_path] = name
        module._classname_to_executors[executor_type.__name__] = name
        module._team_name_to_executors[None] = name
    module._executor_names.append(name)
    module.ExecutorLoader.executors[alias] = module_path
    return alias


def snapshot_sys_modules() -> dict[str, ModuleType]:
    """Snapshot every currently-loaded module by name and object identity.

    Returns:
        dict[str, ModuleType] containing a shallow copy of `sys.modules`.
    """

    return dict(sys.modules)


def restore_sys_modules(before: dict[str, ModuleType], plugins_folder: Path) -> None:
    """Restore `sys.modules` from a snapshot, TARGETED rather than blanket.

    Blanket restoration -- reverting every key that differs from the snapshot -- breaks
    unrelated lazily-imported libraries that cache a module-level singleton the moment
    they are first imported anywhere during the test, not just by component
    registration. Two things happen instead:

    1. Any PRE-EXISTING key (present in `before`) whose current binding is a different
       object is restored unconditionally, regardless of name -- something rebuilt a
       module this sandbox, or the test, touched, and the rest of the process still
       expects the original.
    2. Any NEW key (absent from `before`) is deleted only when its name is the stem of a
       file written into `plugins_folder` (a real, file-loaded plugin module
       `importlib.util.module_from_spec` installs into `sys.modules` under its bare stem
       name; see `airflow._shared.plugins_manager._load_plugins_from_plugin_directory`),
       or starts with `airflow.sdk.execution_time.macros.` (the per-plugin macros
       submodule `integrate_macros_plugins` installs there; see
       `airflow._shared.plugins_manager.make_module`). Every other new key -- an
       unrelated library the test happened to import for the first time -- is left alone.

    Parameters:
        before: dict[str, ModuleType] containing the `sys.modules` snapshot taken at
            sandbox construction.
        plugins_folder: pathlib.Path containing the run's plugins directory.
    """

    stems = (
        {entry.stem for entry in plugins_folder.iterdir()} if plugins_folder.is_dir() else set()
    )
    for name, original in before.items():
        if sys.modules.get(name) is not original:
            sys.modules[name] = original
    for name in set(sys.modules) - set(before):
        if name in stems or name.startswith("airflow.sdk.execution_time.macros."):
            del sys.modules[name]


def _sdk_macros_module() -> Any | None:
    """Import the macros parent module plugin macros are attached to, if it exists.

    A seam, mirroring `_plugins_manager_modules()`: monkeypatchable in a test to fake
    the absent-module branch. Every certified 3.x release attaches plugin macros to the
    SAME parent, `airflow.sdk.execution_time.macros` -- 3.1.x's core
    `integrate_macros_plugins` imports it directly, and the 3.2+ shared implementation
    receives it as `target_macros_module` from both halves (confirmed by reading the
    3.1.0/3.1.8/3.2.0 core and 1.2.0 task-sdk wheels; see PROVENANCE.md) -- so one seam
    covers both release lines. The None branch is pure defensive structure for an
    upstream relocation this plugin has not certified yet.

    Returns:
        Any | None containing the `airflow.sdk.execution_time.macros` module, or None
        when it is not importable.
    """

    try:
        from airflow.sdk.execution_time import macros
    except (ImportError, AttributeError):
        return None
    return macros


def snapshot_macros_module_keys() -> frozenset[str] | None:
    """Snapshot the attribute names currently defined on the macros parent module.

    Returns:
        frozenset[str] | None containing every attribute name on
        `airflow.sdk.execution_time.macros`, or None when the module is not importable.
    """

    module = _sdk_macros_module()
    if module is None:
        return None
    return frozenset(vars(module))


def restore_macros_module_keys(before: frozenset[str] | None) -> None:
    """Delete any macros-parent attribute not present in a prior snapshot.

    `integrate_macros_plugins` leaks in TWO places per plugin, and they need separate
    handling because their names differ: it installs the per-plugin macros module into
    `sys.modules` under a `make_module`-LOWERCASED dotted name (undone by
    `restore_sys_modules`'s prefix rule), and it `setattr`s the same module object onto
    the macros parent under the RAW `plugin.name` (undone here). Upstream never removes
    the attribute, so without this a later test's `{{ macros.<plugin_name>.f() }}`
    still resolves a module that is no longer in `sys.modules`.

    Parameters:
        before: frozenset[str] | None containing the snapshot taken at sandbox
            construction, or None when the macros parent module was not importable then
            (in which case this is a no-op).
    """

    if before is None:
        return
    module = _sdk_macros_module()
    if module is None:
        return
    for name in set(vars(module)) - before:
        delattr(module, name)


def snapshot_settings_keys() -> frozenset[str]:
    """Snapshot the attribute names currently defined on `airflow.settings`.

    Returns:
        frozenset[str] containing every `airflow.settings` module attribute name.
    """

    from airflow import settings

    return frozenset(vars(settings))


def restore_settings_keys(before: frozenset[str]) -> None:
    """Delete any `airflow.settings` attribute not present in a prior snapshot.

    `import_local_settings()` (Airflow's own bootstrap step, driven by the run's ini-
    configured `airflow_local_settings` module -- see #109) writes arbitrary names from a
    real `airflow_local_settings.py` module straight into `airflow.settings.__dict__` and
    never removes them. That write happens once, at process bootstrap, long before any
    `airflow_components` sandbox snapshot -- so it is already part of `before` and this
    function leaves it untouched. This function guards the mirror case: a new key
    appearing DURING the sandboxed test, from any source, is removed.

    Parameters:
        before: frozenset[str] containing the `airflow.settings` snapshot taken at
            sandbox construction.
    """

    from airflow import settings

    for name in set(vars(settings)) - before:
        delattr(settings, name)


__all__ = (
    "CHECK_REGISTRY",
    "DERIVED_LOOKUP_CACHE_FUNCTIONS",
    "DERIVED_LOOKUP_MODULE_GLOBALS",
    "EXECUTOR",
    "KIND_CLASSIFIERS",
    "LISTENER",
    "NOTIFIER",
    "PLUGIN",
    "PLUGIN_LIST_ATTRIBUTES",
    "POLICY",
    "PROVIDER",
    "SECRETS_BACKEND",
    "TIMETABLE",
    "TIMETABLE_ROUND_TRIP_MISMATCH",
    "WEIGHT_STRATEGY",
    "XCOM",
    "ComponentProblem",
    "ComponentSandboxError",
    "ExecutorLoaderSnapshot",
    "ExecutorLoaderSnapshotV31",
    "build_component_plugin",
    "build_policy_plugin",
    "clear_plugins_manager_caches",
    "invalidate_component_lookup_caches",
    "listener_manager_restore",
    "listener_manager_snapshot",
    "listener_managers",
    "lookup_key",
    "mark_task_instance_mutation_hook_active",
    "policy_plugin_manager",
    "register_executor",
    "register_listener",
    "register_plugin",
    "register_policy",
    "register_secrets_backend",
    "register_timetable",
    "register_weight_strategy",
    "restore_executor_loader",
    "restore_macros_module_keys",
    "restore_policy_plugins",
    "restore_secrets_backend_list",
    "restore_settings_keys",
    "restore_sys_modules",
    "restore_task_instance_mutation_hook_is_noop",
    "snapshot_executor_loader",
    "snapshot_macros_module_keys",
    "snapshot_secrets_backend_list",
    "snapshot_settings_keys",
    "snapshot_sys_modules",
    "snapshot_task_instance_mutation_hook_is_noop",
    "timetable_lookup_resolves",
    "timetable_round_trip",
)
