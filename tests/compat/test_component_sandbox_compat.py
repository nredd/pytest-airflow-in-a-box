"""Test the runtime component sandbox: seams, cache certification, snapshot/restore."""

from __future__ import annotations

import functools
import sys
import types
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest
from airflow.executors.base_executor import BaseExecutor

from pytest_airflow_in_a_box._compat import components as compat_components
from pytest_airflow_in_a_box._compat.capabilities import (
    PluginsManagerShape,
    SharedModuleLoading,
    resolve_capabilities,
)


class _Executor(BaseExecutor):
    """Minimal conformant executor, module-level so `register_executor` can resolve it.

    `register_executor` resolves a component by dotted import path
    (`f"{__module__}.{__qualname__}"`), so a class defined inside a test function --
    whose `__qualname__` contains `<locals>` -- is rejected on purpose; see
    `test_executor_loader_register_rejects_a_locally_defined_class` below. The two
    happy-path registration tests need the opposite shape, hence this module-level twin.
    """

    def sync(self) -> None:
        """Report nothing; this sandbox test never actually runs the executor."""

    def _process_workloads(self, workload_items: Any) -> None:
        """Accept a workload batch and do nothing with it.

        Parameters:
            workload_items: Any containing the queued workload batch.
        """

        del workload_items


@pytest.fixture(autouse=True)
def _clean_plugins_manager_caches() -> None:
    """Clear every real plugins-manager cache before each test.

    Every test in this module either registers into, or introspects, the real
    installed `airflow.plugins_manager`/`airflow.sdk.plugins_manager` caches. A prior
    test (in this file or another) that registered a component and forgot to clean up
    would otherwise leak into the next one's baseline; clearing on entry makes each
    test's own baseline observation-independent of suite ordering, the same reasoning
    `clear_plugins_manager_caches` itself documents for the real fixture.
    """

    compat_components.clear_plugins_manager_caches()


def _fake_cached_module(names: tuple[str, ...], *, module_name: str = "fake_cached_module") -> Any:
    """Build a fake module exposing real `functools.cache`-decorated functions.

    Parameters:
        names: tuple[str, ...] naming the cache-clearable functions to define.
        module_name: str naming the fake module, for diagnostics.

    Returns:
        Any containing a `types.ModuleType` with one trivial `@functools.cache`
        function per name in `names`.
    """

    module = types.ModuleType(module_name)
    for name in names:

        def _make(n: str) -> Any:
            @functools.cache
            def _fn() -> str:
                return n

            _fn.__name__ = n
            return _fn

        setattr(module, name, _make(name))
    return module


def _fake_globals_module(
    values: dict[str, object], *, module_name: str = "fake_globals_module"
) -> Any:
    """Build a fake module exposing plain module-level globals.

    Parameters:
        values: dict[str, object] mapping global name to its initial value.
        module_name: str naming the fake module, for diagnostics.

    Returns:
        Any containing a `types.ModuleType` with one attribute per entry in `values`.
    """

    module = types.ModuleType(module_name)
    for name, value in values.items():
        setattr(module, name, value)
    return module


def test_component_sandbox_compat_import_does_not_import_airflow() -> None:
    """Keep the sandbox mechanics import-safe before Airflow bootstrap."""

    import subprocess

    script = (
        "import sys; import pytest_airflow_in_a_box._compat.components; "
        "raise SystemExit('airflow' in sys.modules)"
    )
    subprocess.check_output([sys.executable, "-c", script], text=True)


# ---------------------------------------------------------------------------
# Seam functions
# ---------------------------------------------------------------------------


def test_plugins_manager_modules_resolves_both_on_the_installed_release() -> None:
    """Resolve both real plugins-manager modules on the certified 3.2+ install."""

    core_module, sdk_module = compat_components._plugins_manager_modules()
    assert core_module.__name__ == "airflow.plugins_manager"
    assert sdk_module is not None
    assert sdk_module.__name__ == "airflow.sdk.plugins_manager"


def test_plugins_manager_modules_reports_no_sdk_half_when_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report `None` for the Task SDK half when `airflow.sdk.plugins_manager` is absent.

    `sys.modules[name] = None` is the standard way to force a fresh `import name` to
    raise `ImportError`, simulating the module's genuine absence on 3.1.x without
    needing a real 3.1.x install. That alone is not reliable here: by the time this
    test runs, something earlier in the suite (this very module's own autouse
    `_clean_plugins_manager_caches` fixture, guaranteed) has already imported
    `airflow.sdk.plugins_manager` for real and bound it as a live attribute on the
    `airflow.sdk` package object -- and `from airflow.sdk import plugins_manager`
    resolves via a plain `getattr` on the already-imported parent package FIRST, never
    consulting `sys.modules` at all once that attribute exists. `delattr` removes that
    shortcut too, so the two together force the real `ImportError` regardless of
    what else in the suite has already imported this module.
    """

    monkeypatch.setitem(sys.modules, "airflow.sdk.plugins_manager", None)
    monkeypatch.delattr(sys.modules["airflow.sdk"], "plugins_manager", raising=False)

    core_module, sdk_module = compat_components._plugins_manager_modules()

    assert core_module.__name__ == "airflow.plugins_manager"
    assert sdk_module is None


def test_listener_managers_resolves_both_on_the_installed_release() -> None:
    """Resolve both real `ListenerManager` instances on the certified 3.2+ install."""

    core_manager, task_manager = compat_components.listener_managers()

    assert type(core_manager).__name__ == "ListenerManager"
    assert task_manager is not None
    assert type(task_manager).__name__ == "ListenerManager"
    assert core_manager is not task_manager


def test_listener_managers_reports_no_task_half_when_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report `None` for the Task SDK half when `airflow.sdk.listener` is absent.

    Both the `sys.modules` poison and the parent-package `delattr` are needed --
    see `test_plugins_manager_modules_reports_no_sdk_half_when_absent`'s docstring for
    why relying on `sys.modules` alone is order-dependent on whatever else in the suite
    happened to import `airflow.sdk.listener` first.
    """

    monkeypatch.setitem(sys.modules, "airflow.sdk.listener", None)
    monkeypatch.delattr(sys.modules["airflow.sdk"], "listener", raising=False)

    core_manager, task_manager = compat_components.listener_managers()

    assert type(core_manager).__name__ == "ListenerManager"
    assert task_manager is None


def test_policy_plugin_manager_resolves_the_real_manager() -> None:
    """Resolve the real, cached policy plugin manager."""

    pm = compat_components.policy_plugin_manager()

    assert pm is compat_components.policy_plugin_manager()
    names = {getattr(plugin, "__name__", type(plugin).__name__) for plugin in pm.get_plugins()}
    assert "DefaultPolicy" in names


def test_shared_module_loading_modules_single_resolves_the_pre_32_module() -> None:
    """Resolve the one pre-3.2 module for the `SINGLE` shape."""

    modules = compat_components._shared_module_loading_modules(SharedModuleLoading.SINGLE)

    assert len(modules) == 1
    assert modules[0].__name__ == "airflow.utils.entry_points"


def test_shared_module_loading_modules_duplicated_resolves_both_modules() -> None:
    """Resolve two distinct modules for the `DUPLICATED` shape."""

    modules = compat_components._shared_module_loading_modules(SharedModuleLoading.DUPLICATED)

    assert len(modules) == 2
    names = {module.__name__ for module in modules}
    assert names == {"airflow._shared.module_loading", "airflow.sdk._shared.module_loading"}
    assert modules[0] is not modules[1]


# ---------------------------------------------------------------------------
# Cache enumeration: functools.cache shape
# ---------------------------------------------------------------------------


def test_cache_clearable_names_finds_only_cache_clear_attributes() -> None:
    """Enumerate exactly the attributes exposing a callable `.cache_clear`."""

    module = _fake_cached_module(("alpha", "beta"))
    module.plain_value = 1
    module.plain_function = lambda: None

    names = compat_components._cache_clearable_names(module)

    assert names == frozenset({"alpha", "beta"})


def test_verify_and_clear_cache_functions_clears_every_certified_name() -> None:
    """Clear every certified `functools.cache` function, real, no mismatch."""

    module = _fake_cached_module(("alpha", "beta"))
    module.alpha()
    module.beta()
    assert module.alpha.cache_info().currsize == 1

    compat_components._verify_and_clear_cache_functions(module, frozenset({"alpha", "beta"}))

    assert module.alpha.cache_info().currsize == 0
    assert module.beta.cache_info().currsize == 0


def test_verify_and_clear_cache_functions_raises_on_missing_name() -> None:
    """Raise when a certified name is missing from the observed set."""

    module = _fake_cached_module(("alpha",))

    with pytest.raises(compat_components.ComponentSandboxError, match="missing"):
        compat_components._verify_and_clear_cache_functions(module, frozenset({"alpha", "beta"}))


def test_verify_and_clear_cache_functions_raises_on_uncertified_extra_name() -> None:
    """Raise when the observed set has an extra name the certified set does not."""

    module = _fake_cached_module(("alpha", "beta"))

    with pytest.raises(compat_components.ComponentSandboxError, match="extra"):
        compat_components._verify_and_clear_cache_functions(module, frozenset({"alpha"}))


def test_verify_and_clear_cache_functions_real_core_module_matches_certified_table() -> None:
    """Verify the real installed `airflow.plugins_manager` matches its certified table."""

    from pytest_airflow_in_a_box._compat.capabilities import CERTIFIED_CORE_PLUGINS_MANAGER_CACHES

    core_module, _sdk_module = compat_components._plugins_manager_modules()
    certified = CERTIFIED_CORE_PLUGINS_MANAGER_CACHES[PluginsManagerShape.CACHED_FUNCTIONS]

    compat_components._verify_and_clear_cache_functions(core_module, certified)


# ---------------------------------------------------------------------------
# Cache enumeration: module-globals shape (3.1.x, faked)
# ---------------------------------------------------------------------------


def test_verify_and_reset_module_globals_resets_every_certified_name() -> None:
    """Reset every certified global to its declared cache-empty value."""

    module = _fake_globals_module(
        {"plugins": ["stale"], "loaded_plugins": {"stale"}, "import_errors": {"stale": "x"}}
    )

    compat_components._verify_and_reset_module_globals(
        module, frozenset({"plugins", "loaded_plugins", "import_errors"})
    )

    assert module.plugins is None
    assert module.loaded_plugins == set()
    assert module.import_errors == {}


def test_verify_and_reset_module_globals_raises_on_missing_name() -> None:
    """Raise when a certified global is absent from the module entirely."""

    module = _fake_globals_module({"plugins": None})

    with pytest.raises(compat_components.ComponentSandboxError, match="plugins_cache"):
        compat_components._verify_and_reset_module_globals(
            module, frozenset({"plugins", "plugins_cache"})
        )


def test_verify_and_reset_module_globals_real_31_shape_matches_certified_table() -> None:
    """Verify the transcribed 3.1.0 certified names against a faithful fake of that shape.

    A real 3.1.x install is not available in this environment; this fake reproduces the
    exact nineteen module-level globals `PROVENANCE.md` records reading directly from the
    installed `apache-airflow-core==3.1.0` wheel, so the certified table itself -- not
    just this function's own mechanics -- gets exercised end to end.
    """

    from pytest_airflow_in_a_box._compat.capabilities import CERTIFIED_CORE_PLUGINS_MANAGER_CACHES

    certified = CERTIFIED_CORE_PLUGINS_MANAGER_CACHES[PluginsManagerShape.MODULE_GLOBALS]
    module = _fake_globals_module(dict.fromkeys(certified, "populated"))

    compat_components._verify_and_reset_module_globals(module, certified)

    assert module.loaded_plugins == set()
    assert module.import_errors == {}
    for name in certified - {"loaded_plugins", "import_errors"}:
        assert getattr(module, name) is None


# ---------------------------------------------------------------------------
# clear_plugins_manager_caches
# ---------------------------------------------------------------------------


def test_clear_plugins_manager_caches_real_installation() -> None:
    """Clear every real cache on the certified, installed 3.2+ release."""

    compat_components.clear_plugins_manager_caches()


def test_clear_plugins_manager_caches_raises_on_a_2x_style_capabilities(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raise a clear, attributable error rather than a bare `KeyError` on 2.x.

    `airflow_components` itself never reaches this call on 2.x -- `v2_gate_message`
    fails the fixture first -- so this is a self-consistency guard against direct
    misuse of `clear_plugins_manager_caches()`, not a real path through the fixture.
    Faking a 2.x-shaped `AirflowCapabilities` (`plugins_manager`/`shared_module_loading`
    both `None`) is simpler and more honest than an actual 2.x install would be here,
    since nothing else about this function's behavior depends on the rest of the
    capabilities contract.
    """

    real_capabilities = resolve_capabilities()
    fake_capabilities = replace(
        real_capabilities, plugins_manager=None, shared_module_loading=None
    )
    monkeypatch.setattr(compat_components, "resolve_capabilities", lambda: fake_capabilities)

    with pytest.raises(compat_components.ComponentSandboxError, match=r"Airflow 2\.x family"):
        compat_components.clear_plugins_manager_caches()


def test_clear_plugins_manager_caches_raises_when_cached_functions_shape_has_no_sdk_module(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raise when the resolved shape is `CACHED_FUNCTIONS` but no SDK module resolves.

    An internally inconsistent installation (the certified contract says 3.2+, but the
    Task SDK half cannot be imported) must fail loudly rather than silently clear the
    core half alone.
    """

    real_capabilities = resolve_capabilities()
    fake_capabilities = replace(
        real_capabilities, plugins_manager=PluginsManagerShape.CACHED_FUNCTIONS
    )
    # Resolve the real core module BEFORE patching: the lambda below replaces
    # `_plugins_manager_modules` itself, so a lambda body that calls
    # `compat_components._plugins_manager_modules()` again would look up and invoke
    # the very same patched attribute, recursing until `RecursionError`. Capturing the
    # real result first and closing over that plain tuple avoids the self-reference.
    real_core, _real_sdk = compat_components._plugins_manager_modules()
    monkeypatch.setattr(compat_components, "resolve_capabilities", lambda: fake_capabilities)
    monkeypatch.setattr(
        compat_components,
        "_plugins_manager_modules",
        lambda: (real_core, None),
    )

    with pytest.raises(compat_components.ComponentSandboxError, match="cached-functions"):
        compat_components.clear_plugins_manager_caches()


def test_clear_plugins_manager_caches_raises_when_module_globals_shape_has_sdk_module(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raise when the resolved shape is `MODULE_GLOBALS` but an SDK module still resolves.

    3.1.x never has a Task SDK plugins-manager module; if one is observed anyway, the
    certified `MODULE_GLOBALS` classification for this release has drifted from reality.
    """

    from pytest_airflow_in_a_box._compat.capabilities import CERTIFIED_CORE_PLUGINS_MANAGER_CACHES

    real_capabilities = resolve_capabilities()
    fake_capabilities = replace(
        real_capabilities, plugins_manager=PluginsManagerShape.MODULE_GLOBALS
    )
    certified = CERTIFIED_CORE_PLUGINS_MANAGER_CACHES[PluginsManagerShape.MODULE_GLOBALS]
    fake_core = _fake_globals_module(dict.fromkeys(certified))
    # Resolve the real SDK module BEFORE patching, for the same reason the sibling
    # `..._has_no_sdk_module` test above does: a lambda that calls
    # `compat_components._plugins_manager_modules()` again would recurse into its own
    # freshly-patched self instead of reaching the real function.
    _real_core, real_sdk = compat_components._plugins_manager_modules()
    monkeypatch.setattr(compat_components, "resolve_capabilities", lambda: fake_capabilities)
    monkeypatch.setattr(
        compat_components,
        "_plugins_manager_modules",
        lambda: (fake_core, real_sdk),
    )

    with pytest.raises(compat_components.ComponentSandboxError, match="module-globals"):
        compat_components.clear_plugins_manager_caches()


def test_clear_plugins_manager_caches_module_globals_shape_happy_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Clear a faked `MODULE_GLOBALS`-shaped installation end to end, with no SDK half."""

    from pytest_airflow_in_a_box._compat.capabilities import CERTIFIED_CORE_PLUGINS_MANAGER_CACHES

    real_capabilities = resolve_capabilities()
    fake_capabilities = replace(
        real_capabilities,
        plugins_manager=PluginsManagerShape.MODULE_GLOBALS,
        shared_module_loading=SharedModuleLoading.SINGLE,
    )
    certified = CERTIFIED_CORE_PLUGINS_MANAGER_CACHES[PluginsManagerShape.MODULE_GLOBALS]
    fake_core = _fake_globals_module(dict.fromkeys(certified, "populated"))
    fake_shared = _fake_cached_module(("_get_grouped_entry_points",), module_name="fake_shared")
    monkeypatch.setattr(compat_components, "resolve_capabilities", lambda: fake_capabilities)
    monkeypatch.setattr(compat_components, "_plugins_manager_modules", lambda: (fake_core, None))
    monkeypatch.setattr(
        compat_components, "_shared_module_loading_modules", lambda _shape: (fake_shared,)
    )

    compat_components.clear_plugins_manager_caches()

    assert fake_core.plugins is None
    assert fake_core.loaded_plugins == set()


# ---------------------------------------------------------------------------
# Listener manager snapshot/register/restore
# ---------------------------------------------------------------------------


def test_listener_manager_snapshot_and_restore_round_trips(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Snapshot, register, then restore a listener manager to its exact pre-state."""
    del monkeypatch
    core_manager, _task_manager = compat_components.listener_managers()
    before = compat_components.listener_manager_snapshot(core_manager)

    from airflow.listeners import hookimpl

    class _Listener:
        @hookimpl
        def on_starting(self, component: object) -> None:
            del component

    instance = compat_components.register_listener(_Listener, (core_manager,))
    assert instance in core_manager.pm.get_plugins()

    compat_components.listener_manager_restore(core_manager, before)

    assert tuple(core_manager.pm.get_plugins()) == before


def test_listener_manager_restore_reregisters_a_nonempty_snapshot() -> None:
    """Restore a snapshot that itself contains a listener, not just the empty case.

    The round-trip test above snapshots BEFORE registering anything, so its own
    `before` is empty on a real installation with no other listener registered --
    `listener_manager_restore`'s `for listener in snapshot: manager.add_listener(...)`
    loop body never runs there. This snapshots AFTER registering instead, so the
    snapshot handed to `listener_manager_restore` is genuinely non-empty.
    """

    from airflow.listeners import hookimpl

    class _PreExistingListener:
        @hookimpl
        def on_starting(self, component: object) -> None:
            del component

    core_manager, _task_manager = compat_components.listener_managers()
    pre_existing = compat_components.register_listener(_PreExistingListener, (core_manager,))
    non_empty_snapshot = compat_components.listener_manager_snapshot(core_manager)
    assert pre_existing in non_empty_snapshot

    core_manager.clear()
    assert pre_existing not in core_manager.pm.get_plugins()

    compat_components.listener_manager_restore(core_manager, non_empty_snapshot)

    assert tuple(core_manager.pm.get_plugins()) == non_empty_snapshot

    compat_components.listener_manager_restore(core_manager, ())


def test_register_listener_accepts_an_already_built_instance() -> None:
    """Register an already-instantiated listener without double-constructing it."""

    core_manager, _task_manager = compat_components.listener_managers()
    before = compat_components.listener_manager_snapshot(core_manager)

    from airflow.listeners import hookimpl

    class _Listener:
        @hookimpl
        def on_starting(self, component: object) -> None:
            del component

    built = _Listener()
    returned = compat_components.register_listener(built, (core_manager,))

    assert returned is built
    compat_components.listener_manager_restore(core_manager, before)


# ---------------------------------------------------------------------------
# Plugin registration
# ---------------------------------------------------------------------------


def test_register_plugin_appends_to_both_halves_and_clears_cleanly() -> None:
    """Register a plugin class into both plugins-manager halves and revert cleanly."""

    from airflow.plugins_manager import AirflowPlugin

    class _Plugin(AirflowPlugin):
        name = "sandbox_compat_test_plugin"

    instance = compat_components.register_plugin(_Plugin)

    core_module, sdk_module = compat_components._plugins_manager_modules()
    assert instance in core_module._get_plugins()[0]
    assert sdk_module is not None
    assert instance in sdk_module._get_plugins()[0]

    compat_components.clear_plugins_manager_caches()

    assert instance not in core_module._get_plugins()[0]


def test_register_plugin_without_an_sdk_half(monkeypatch: pytest.MonkeyPatch) -> None:
    """Register a plugin core-only when no Task SDK plugins-manager module resolves."""

    from airflow.plugins_manager import AirflowPlugin

    class _Plugin(AirflowPlugin):
        name = "sandbox_compat_test_plugin_core_only"

    core_module, _sdk_module = compat_components._plugins_manager_modules()
    monkeypatch.setattr(compat_components, "_plugins_manager_modules", lambda: (core_module, None))

    instance = compat_components.register_plugin(_Plugin)

    assert instance in core_module._get_plugins()[0]

    # Undo the "no SDK half" patch before clearing: the real installed release's
    # certified shape is `CACHED_FUNCTIONS`, which requires an SDK module to exist --
    # `clear_plugins_manager_caches` would otherwise (correctly) treat the still-patched
    # "no SDK half" as a real environment inconsistency and raise `ComponentSandboxError`,
    # exactly as the neighboring `..._has_no_sdk_module` test exercises on purpose. This
    # test's own concern is `register_plugin`'s core-only behavior, not that guard.
    monkeypatch.undo()
    compat_components.clear_plugins_manager_caches()


def test_prepare_plugin_instance_deep_copies_lists_across_two_registrations() -> None:
    """Keep two registrations of the same plugin class independent after a mutation.

    `_get_ui_plugins` mutates `external_views`/`react_apps` in place while building its
    own return value; without a deep copy at registration time, a second registration of
    the same class would inherit whatever the first registration's own downstream
    processing already removed.
    """

    from airflow.plugins_manager import AirflowPlugin

    class _Plugin(AirflowPlugin):
        name = "sandbox_compat_test_plugin_deepcopy"

        def __init__(self) -> None:
            """Set `external_views` per instance rather than as a shared class default."""

            super().__init__()
            self.external_views = [{"url_route": "shared", "label": "shared"}]

    first = cast(_Plugin, compat_components._prepare_plugin_instance(_Plugin))
    first.external_views.remove(first.external_views[0])
    assert first.external_views == []

    second = cast(_Plugin, compat_components._prepare_plugin_instance(_Plugin))

    assert second.external_views == [{"url_route": "shared", "label": "shared"}]
    assert second.external_views is not first.external_views
    assert second.external_views is not _Plugin.external_views


def test_prepare_plugin_instance_skips_attributes_the_component_does_not_have() -> None:
    """Skip a `PLUGIN_LIST_ATTRIBUTES` entry the component does not define at all.

    Exercises the defensive `hasattr` guard directly: every real `AirflowPlugin`
    subclass inherits all of `PLUGIN_LIST_ATTRIBUTES`, so this path is unreachable
    through the normal `check_component`-gated registration flow.
    """

    class _BareObject:
        name = "bare"

    prepared = compat_components._prepare_plugin_instance(_BareObject)

    assert not hasattr(prepared, "external_views")


# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------


def test_build_policy_plugin_with_no_hooks_builds_an_empty_class() -> None:
    """Build a policy plugin class with no hookimpl-marked methods from an empty mapping."""

    plugin_class = compat_components.build_policy_plugin({})

    from pytest_airflow_in_a_box.components import ComponentKind, check_component

    report = check_component(plugin_class, kind=ComponentKind.POLICY)
    assert report.ok


def test_register_policy_and_restore_policy_plugins_round_trips() -> None:
    """Register a policy plugin, observe it fire, then restore the pre-state exactly."""

    pm = compat_components.policy_plugin_manager()
    before = tuple(pm.get_plugins())

    received: dict[str, bool] = {}

    def _hook(task_instance: object, dag_run: object) -> None:
        del task_instance, dag_run
        received["called"] = True

    plugin_class = compat_components.build_policy_plugin({"task_instance_mutation_hook": _hook})
    instance = compat_components.register_policy(plugin_class, pm)

    assert instance in pm.get_plugins()
    pm.hook.task_instance_mutation_hook(task_instance=None, dag_run=None)
    assert received == {"called": True}

    compat_components.restore_policy_plugins(pm, before)

    assert tuple(pm.get_plugins()) == before


# ---------------------------------------------------------------------------
# Secrets backend
# ---------------------------------------------------------------------------


def test_secrets_backend_register_first_true_inserts_at_front() -> None:
    """Insert a secrets backend at the front of the search path by default."""

    from airflow.secrets.environment_variables import EnvironmentVariablesBackend

    before = compat_components.snapshot_secrets_backend_list()

    instance = compat_components.register_secrets_backend(EnvironmentVariablesBackend, first=True)

    current = compat_components.snapshot_secrets_backend_list()
    assert current[0] is instance
    compat_components.restore_secrets_backend_list(before)
    assert compat_components.snapshot_secrets_backend_list() == before


def test_secrets_backend_register_first_false_appends_at_back() -> None:
    """Append a secrets backend at the back of the search path when `first=False`."""

    from airflow.secrets.environment_variables import EnvironmentVariablesBackend

    before = compat_components.snapshot_secrets_backend_list()

    instance = compat_components.register_secrets_backend(EnvironmentVariablesBackend, first=False)

    current = compat_components.snapshot_secrets_backend_list()
    assert current[-1] is instance
    compat_components.restore_secrets_backend_list(before)


def test_secrets_backend_register_accepts_an_already_built_instance() -> None:
    """Register an already-instantiated secrets backend without double-constructing it."""

    from airflow.secrets.environment_variables import EnvironmentVariablesBackend

    before = compat_components.snapshot_secrets_backend_list()
    built = EnvironmentVariablesBackend()

    returned = compat_components.register_secrets_backend(built, first=True)

    assert returned is built
    compat_components.restore_secrets_backend_list(before)


def test_restore_secrets_backend_list_mutates_the_same_object_in_place() -> None:
    """Restore by slice assignment, keeping the same list object identity."""

    from airflow.configuration import secrets_backend_list

    original_id = id(secrets_backend_list)
    before = compat_components.snapshot_secrets_backend_list()

    compat_components.restore_secrets_backend_list(list(reversed(before)))
    compat_components.restore_secrets_backend_list(before)

    from airflow.configuration import secrets_backend_list as after

    assert id(after) == original_id


# ---------------------------------------------------------------------------
# task_instance_mutation_hook.is_noop
# ---------------------------------------------------------------------------


def test_task_instance_mutation_hook_is_noop_snapshot_and_restore() -> None:
    """Snapshot and restore the `is_noop` flag exactly."""

    before = compat_components.snapshot_task_instance_mutation_hook_is_noop()

    compat_components.restore_task_instance_mutation_hook_is_noop(not before)
    from airflow.settings import task_instance_mutation_hook

    # `task_instance_mutation_hook` is a plain function object; `is_noop` is a flag
    # Airflow attaches to it dynamically (`task_instance_mutation_hook.is_noop = True`
    # in `airflow/settings.py`), invisible to static typing without this cast -- the
    # same shape `_compat.components`'s own snapshot/restore functions cast around.
    hook = cast("Any", task_instance_mutation_hook)
    assert hook.is_noop is (not before)

    compat_components.restore_task_instance_mutation_hook_is_noop(before)
    assert hook.is_noop is before


# ---------------------------------------------------------------------------
# Executor loader
# ---------------------------------------------------------------------------


def test_executor_loader_register_and_restore_round_trips() -> None:
    """Register an executor alias, resolve it live, then restore the pre-state exactly."""

    from airflow.executors.executor_loader import ExecutorLoader

    before = compat_components.snapshot_executor_loader()

    alias = compat_components.register_executor(_Executor, alias="sandbox_compat_test_executor")

    assert alias == "sandbox_compat_test_executor"
    loaded = ExecutorLoader.load_executor(alias)
    assert isinstance(loaded, _Executor)

    compat_components.restore_executor_loader(before)

    with pytest.raises(Exception, match=r"[Uu]nknown"):
        ExecutorLoader.lookup_executor_name_by_str(alias)


def test_executor_loader_register_accepts_an_instance() -> None:
    """Register an executor from an already-built instance, using its class's path."""

    before = compat_components.snapshot_executor_loader()

    alias = compat_components.register_executor(
        _Executor(), alias="sandbox_compat_test_executor_2"
    )

    assert alias == "sandbox_compat_test_executor_2"
    compat_components.restore_executor_loader(before)


def test_executor_loader_register_rejects_a_locally_defined_class() -> None:
    """Raise when the executor class has no importable module-level path."""

    from airflow.executors.base_executor import BaseExecutor

    def _make_local_executor() -> type:
        class _LocalExecutor(BaseExecutor):
            def sync(self) -> None:
                pass

        return _LocalExecutor

    local_class = _make_local_executor()

    with pytest.raises(compat_components.ComponentSandboxError, match="module scope"):
        compat_components.register_executor(local_class, alias="sandbox_compat_test_local")


# ---------------------------------------------------------------------------
# sys.modules targeted restoration
# ---------------------------------------------------------------------------


def test_restore_sys_modules_restores_identity_changed_pre_existing_keys(tmp_path: Path) -> None:
    """Restore a pre-existing key whose binding changed identity, regardless of name."""

    sentinel_name = "sandbox_compat_test_identity_module"
    original = types.ModuleType(sentinel_name)
    sys.modules[sentinel_name] = original
    before = compat_components.snapshot_sys_modules()

    sys.modules[sentinel_name] = types.ModuleType(sentinel_name)
    assert sys.modules[sentinel_name] is not original

    compat_components.restore_sys_modules(before, tmp_path)

    assert sys.modules[sentinel_name] is original
    del sys.modules[sentinel_name]


def test_restore_sys_modules_deletes_new_keys_matching_a_plugin_file_stem(tmp_path: Path) -> None:
    """Delete a new `sys.modules` key whose name is the stem of a file in `plugins_folder`."""

    (tmp_path / "my_plugin.py").write_text("", encoding="utf-8")
    before = compat_components.snapshot_sys_modules()

    sys.modules["my_plugin"] = types.ModuleType("my_plugin")

    compat_components.restore_sys_modules(before, tmp_path)

    assert "my_plugin" not in sys.modules


def test_restore_sys_modules_deletes_new_macros_submodule_keys(tmp_path: Path) -> None:
    """Delete a new `sys.modules` key under the per-plugin macros submodule prefix."""

    before = compat_components.snapshot_sys_modules()

    name = "airflow.sdk.execution_time.macros.sandbox_compat_test_plugin"
    sys.modules[name] = types.ModuleType(name)

    compat_components.restore_sys_modules(before, tmp_path)

    assert name not in sys.modules


def test_restore_sys_modules_leaves_unrelated_new_keys_alone(tmp_path: Path) -> None:
    """Leave a new `sys.modules` key alone when it matches neither targeted pattern."""

    before = compat_components.snapshot_sys_modules()

    sys.modules["sandbox_compat_test_unrelated_module"] = types.ModuleType(
        "sandbox_compat_test_unrelated_module"
    )

    compat_components.restore_sys_modules(before, tmp_path)

    assert "sandbox_compat_test_unrelated_module" in sys.modules
    del sys.modules["sandbox_compat_test_unrelated_module"]


def test_restore_sys_modules_tolerates_a_missing_plugins_folder(tmp_path: Path) -> None:
    """Tolerate a `plugins_folder` that does not exist on disk (no stems to match)."""

    missing = tmp_path / "does-not-exist"
    before = compat_components.snapshot_sys_modules()

    sys.modules["sandbox_compat_test_unrelated_module_2"] = types.ModuleType(
        "sandbox_compat_test_unrelated_module_2"
    )

    compat_components.restore_sys_modules(before, missing)

    assert "sandbox_compat_test_unrelated_module_2" in sys.modules
    del sys.modules["sandbox_compat_test_unrelated_module_2"]


# ---------------------------------------------------------------------------
# airflow.settings.__dict__ new-key cleanup
# ---------------------------------------------------------------------------


def test_restore_settings_keys_deletes_only_new_attributes() -> None:
    """Delete a new `airflow.settings` attribute added during the sandboxed test."""

    from airflow import settings

    before = compat_components.snapshot_settings_keys()

    leaked_name = "".join(("SANDBOX", "_COMPAT_TEST_ATTR"))
    setattr(settings, leaked_name, "leaked")
    assert hasattr(settings, leaked_name)

    compat_components.restore_settings_keys(before)

    assert not hasattr(settings, leaked_name)


def test_restore_settings_keys_preserves_bootstrap_time_attributes() -> None:
    """Leave every pre-existing `airflow.settings` attribute untouched."""

    from airflow import settings

    before = compat_components.snapshot_settings_keys()

    compat_components.restore_settings_keys(before)

    assert hasattr(settings, "get_policy_plugin_manager")
