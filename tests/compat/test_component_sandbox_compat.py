"""Test the runtime component sandbox: seams, cache certification, snapshot/restore."""

from __future__ import annotations

import functools
import sys
import types
from dataclasses import replace
from pathlib import Path
from typing import Any, ClassVar, cast

import pytest
from airflow.executors.base_executor import BaseExecutor
from airflow.timetables.base import Timetable

from pytest_airflow_in_a_box._compat import components as compat_components
from pytest_airflow_in_a_box._compat.capabilities import (
    CERTIFIED_CORE_PLUGINS_MANAGER_CACHES,
    CertificationTier,
    CertifiedCaches,
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
    `clear_plugins_manager_caches` itself documents for the real fixture. Entry-only
    on purpose: an exit clear here would race the `monkeypatch`-driven fake-module
    tests, whose fakes can still be patched in when this fixture finalizes. Tests
    that register into the REAL modules clear inline at their own end instead --
    leaking a `ComponentRegistryPlugin` past the test is a real cross-module flake
    (observed against `tests/fixtures/test_dag.py`), not a hypothetical.

    Also clears the once-per-process degraded-warning cache: the PROBED-tier tests
    below assert on `caplog`, and fake modules reuse names across tests, so a warning
    deduplicated by an earlier test would silently blank a later assertion.
    """

    compat_components._reset_degrade_warnings_for_testing()
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
            # Stamp ownership: the PROBED-tier generic clear only touches caches whose
            # `__module__` matches the module carrying them, exactly like a function
            # defined in the real module would.
            _fn.__module__ = module_name
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


@pytest.mark.skipif(
    resolve_capabilities().plugins_manager is not PluginsManagerShape.CACHED_FUNCTIONS,
    reason="airflow.sdk.plugins_manager exists only on the 3.2+ CACHED_FUNCTIONS shape",
)
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


@pytest.mark.skipif(
    not resolve_capabilities().sdk_listener_manager_available,
    reason="the Task SDK listener manager exists only on 3.2+",
)
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


def test_policy_plugin_manager_falls_back_to_the_31_module_global(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fall back to `settings.POLICY_PLUGIN_MANAGER` when the 3.2+ getter is absent.

    `monkeypatch.delattr` on the already-imported `airflow.settings` makes the
    `from airflow.settings import get_policy_plugin_manager` raise `ImportError` (a
    from-import of a missing attribute on an imported module raises `ImportError`, not
    `AttributeError`), simulating the 3.1.x shape; the module global the fallback reads
    is then planted the way 3.1.x's `configure_policy_plugin_manager()` would have.
    """

    from airflow import settings

    sentinel = object()
    # `raising=False`: on a real 3.1.x leg the getter is already genuinely absent.
    monkeypatch.delattr(settings, "get_policy_plugin_manager", raising=False)
    monkeypatch.setattr(settings, "POLICY_PLUGIN_MANAGER", sentinel, raising=False)

    assert compat_components.policy_plugin_manager() is sentinel


def test_policy_plugin_manager_raises_when_the_31_module_global_is_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raise loudly when the 3.1.x module global is still None.

    Unreachable through the `airflow_components` fixture, whose bootstrap always runs
    `settings.initialize()` first -- this is the guard against direct misuse.
    """

    from airflow import settings

    monkeypatch.delattr(settings, "get_policy_plugin_manager", raising=False)
    monkeypatch.setattr(settings, "POLICY_PLUGIN_MANAGER", None, raising=False)

    with pytest.raises(compat_components.ComponentSandboxError, match="initialize"):
        compat_components.policy_plugin_manager()


@pytest.mark.skipif(
    resolve_capabilities().shared_module_loading is not SharedModuleLoading.SINGLE,
    reason=(
        "airflow.utils.entry_points is the real cache home only on 3.1.x; on 3.2+ it "
        "survives as an empty PEP 562 deprecation shim that would satisfy a bare "
        "module-name assertion for the wrong reason"
    ),
)
def test_shared_module_loading_modules_single_resolves_the_certified_module() -> None:
    """Resolve the one pre-3.2 module for the `SINGLE` shape, with its certified caches.

    Asserts the resolved function SET, not just the module name: on 3.2+ the module
    still imports (as the deprecation shim), so a name-only assertion would keep
    passing there -- and would keep passing even after upstream deletes the shim's
    forwarding target. The skipif keeps this test honest to the releases where the
    real module exists; `..._single_imports_the_certified_location` below keeps the
    branch itself covered on every leg.
    """

    from pytest_airflow_in_a_box._compat.capabilities import (
        CERTIFIED_SHARED_MODULE_LOADING_CACHES,
    )

    modules = compat_components._shared_module_loading_modules(SharedModuleLoading.SINGLE)

    assert len(modules) == 1
    assert modules[0].__name__ == "airflow.utils.entry_points"
    certified = CERTIFIED_SHARED_MODULE_LOADING_CACHES[SharedModuleLoading.SINGLE]
    assert compat_components._cache_clearable_names(modules[0]) == certified.required


def test_shared_module_loading_modules_single_imports_the_certified_location(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cover the `SINGLE` branch on every leg by faking `import_module` at the seam.

    The real-module assertion above is honestly skipped on 3.2+, so this keeps the
    branch itself (and the certified SINGLE row's round trip through
    `_verify_and_clear_cache_functions`) exercised regardless of the installed release.
    `import_module` is imported by name into `_compat.components`, so patching the
    module attribute intercepts exactly this seam's dynamic import.
    """

    from pytest_airflow_in_a_box._compat.capabilities import (
        CERTIFIED_SHARED_MODULE_LOADING_CACHES,
    )

    requested: list[str] = []

    def _fake_import_module(name: str) -> Any:
        requested.append(name)
        return _fake_cached_module(("_get_grouped_entry_points",), module_name=name)

    monkeypatch.setattr(compat_components, "import_module", _fake_import_module)

    modules = compat_components._shared_module_loading_modules(SharedModuleLoading.SINGLE)

    assert requested == ["airflow.utils.entry_points"]
    assert len(modules) == 1
    compat_components._verify_and_clear_cache_functions(
        modules[0],
        CERTIFIED_SHARED_MODULE_LOADING_CACHES[SharedModuleLoading.SINGLE],
        certification=CertificationTier.CERTIFIED,
    )


@pytest.mark.skipif(
    resolve_capabilities().shared_module_loading is not SharedModuleLoading.DUPLICATED,
    reason="airflow._shared.module_loading and its Task SDK twin exist only on 3.2+",
)
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


def test_drop_caches_clears_the_named_cache_functions() -> None:
    """Clear exactly the named `functools.cache` functions, leaving others populated."""

    module = _fake_cached_module(("alpha", "beta"))
    module.alpha()
    module.beta()
    assert module.alpha.cache_info().currsize == 1
    assert module.beta.cache_info().currsize == 1

    compat_components._drop_caches(module, cache_functions=("alpha",), module_globals=())

    assert module.alpha.cache_info().currsize == 0
    assert module.beta.cache_info().currsize == 1


def test_drop_caches_rebinds_the_named_module_globals_to_their_empty_values() -> None:
    """Rebind plain globals to `None` and container-sentineled ones to fresh containers."""

    module: Any = types.ModuleType("fake_globals_module")
    module.timetable_classes = {"pkg.Timetable": object}
    module.loaded_plugins = {"a_plugin"}
    module.import_errors = {"plugin.py": "boom"}
    module.untouched = {"left": "alone"}

    compat_components._drop_caches(
        module,
        cache_functions=(),
        module_globals=("timetable_classes", "loaded_plugins", "import_errors"),
    )

    assert module.timetable_classes is None
    assert module.loaded_plugins == set()
    assert module.import_errors == {}
    assert module.untouched == {"left": "alone"}


def test_drop_caches_with_empty_iterables_is_a_no_op() -> None:
    """Touch nothing when both name iterables are empty."""

    module = _fake_cached_module(("alpha",))
    module.alpha()
    module.timetable_classes = {"pkg.Timetable": object}

    compat_components._drop_caches(module, cache_functions=(), module_globals=())

    assert module.alpha.cache_info().currsize == 1
    assert module.timetable_classes == {"pkg.Timetable": object}


def test_verify_and_clear_cache_functions_clears_every_certified_name() -> None:
    """Clear every certified `functools.cache` function, real, no mismatch."""

    module = _fake_cached_module(("alpha", "beta"))
    module.alpha()
    module.beta()
    assert module.alpha.cache_info().currsize == 1

    compat_components._verify_and_clear_cache_functions(
        module,
        CertifiedCaches(required=frozenset({"alpha", "beta"})),
        certification=CertificationTier.CERTIFIED,
    )

    assert module.alpha.cache_info().currsize == 0
    assert module.beta.cache_info().currsize == 0


def test_verify_and_clear_cache_functions_raises_on_missing_name() -> None:
    """Raise when a certified name is missing from the observed set."""

    module = _fake_cached_module(("alpha",))

    with pytest.raises(compat_components.ComponentSandboxError, match="missing"):
        compat_components._verify_and_clear_cache_functions(
            module,
            CertifiedCaches(required=frozenset({"alpha", "beta"})),
            certification=CertificationTier.CERTIFIED,
        )


def test_verify_and_clear_cache_functions_raises_on_uncertified_extra_name() -> None:
    """Raise when the observed set has an extra name the certified set does not."""

    module = _fake_cached_module(("alpha", "beta"))

    with pytest.raises(compat_components.ComponentSandboxError, match="extra"):
        compat_components._verify_and_clear_cache_functions(
            module,
            CertifiedCaches(required=frozenset({"alpha"})),
            certification=CertificationTier.CERTIFIED,
        )


def test_verify_and_clear_cache_functions_tolerates_an_absent_optional_name() -> None:
    """Tolerate a certified optional name missing from the observed set.

    The exact 3.2.0/3.2.1 situation that motivated `CertifiedCaches`: upstream added
    `get_deadline_references_plugins` (3.2.2) and `get_windows_plugins` (3.3.0)
    mid-release-line, so the earlier releases in the same `CACHED_FUNCTIONS` bucket
    legitimately lack them.
    """

    module = _fake_cached_module(("alpha",))

    compat_components._verify_and_clear_cache_functions(
        module,
        CertifiedCaches(required=frozenset({"alpha"}), optional=frozenset({"beta"})),
        certification=CertificationTier.CERTIFIED,
    )


def test_verify_and_clear_cache_functions_clears_a_present_optional_name() -> None:
    """Clear a certified optional name when the installed release does have it."""

    module = _fake_cached_module(("alpha", "beta"))
    module.alpha()
    module.beta()
    assert module.beta.cache_info().currsize == 1

    compat_components._verify_and_clear_cache_functions(
        module,
        CertifiedCaches(required=frozenset({"alpha"}), optional=frozenset({"beta"})),
        certification=CertificationTier.CERTIFIED,
    )

    assert module.alpha.cache_info().currsize == 0
    assert module.beta.cache_info().currsize == 0


def test_verify_and_clear_cache_functions_raises_on_extra_beyond_required_and_optional() -> None:
    """Raise on an observed name outside `required | optional` -- the under-clear guard."""

    module = _fake_cached_module(("alpha", "beta", "gamma"))

    with pytest.raises(compat_components.ComponentSandboxError, match="extra"):
        compat_components._verify_and_clear_cache_functions(
            module,
            CertifiedCaches(required=frozenset({"alpha"}), optional=frozenset({"beta"})),
            certification=CertificationTier.CERTIFIED,
        )


def test_certified_core_cached_functions_table_matches_the_3_2_0_shape() -> None:
    """Verify the transcribed 3.2.0 cache set passes against the real certified row.

    A real 3.2.0 install is not available in this environment; this fake reproduces
    the exact nine `@cache` functions read directly from the
    `apache-airflow-core==3.2.0` wheel's `airflow/plugins_manager.py` (see
    PROVENANCE.md) -- the release whose missing `get_deadline_references_plugins` /
    `get_windows_plugins` previously hard-failed the strict symmetric-difference check.
    """

    from pytest_airflow_in_a_box._compat.capabilities import CERTIFIED_CORE_PLUGINS_MANAGER_CACHES

    module = _fake_cached_module(
        (
            "_get_plugins",
            "_get_ui_plugins",
            "get_flask_plugins",
            "get_fastapi_plugins",
            "_get_extra_operators_links_plugins",
            "get_timetables_plugins",
            "get_partition_mapper_plugins",
            "integrate_macros_plugins",
            "get_priority_weight_strategy_plugins",
        ),
        module_name="fake_airflow_320_plugins_manager",
    )

    compat_components._verify_and_clear_cache_functions(
        module,
        CERTIFIED_CORE_PLUGINS_MANAGER_CACHES[PluginsManagerShape.CACHED_FUNCTIONS],
        certification=CertificationTier.CERTIFIED,
    )


@pytest.mark.skipif(
    resolve_capabilities().plugins_manager is not PluginsManagerShape.CACHED_FUNCTIONS,
    reason="the 3.1.x MODULE_GLOBALS shape has no functools.cache functions to enumerate",
)
def test_verify_and_clear_cache_functions_real_core_module_matches_certified_table() -> None:
    """Verify the real installed `airflow.plugins_manager` matches its certified table."""

    from pytest_airflow_in_a_box._compat.capabilities import CERTIFIED_CORE_PLUGINS_MANAGER_CACHES

    core_module, _sdk_module = compat_components._plugins_manager_modules()
    certified = CERTIFIED_CORE_PLUGINS_MANAGER_CACHES[PluginsManagerShape.CACHED_FUNCTIONS]

    compat_components._verify_and_clear_cache_functions(
        core_module, certified, certification=CertificationTier.CERTIFIED
    )


# ---------------------------------------------------------------------------
# Cache enumeration: module-globals shape (3.1.x, faked)
# ---------------------------------------------------------------------------


def test_verify_and_reset_module_globals_resets_every_certified_name() -> None:
    """Reset every certified global to its declared cache-empty value."""

    module = _fake_globals_module(
        {"plugins": ["stale"], "loaded_plugins": {"stale"}, "import_errors": {"stale": "x"}}
    )

    compat_components._verify_and_reset_module_globals(
        module,
        CertifiedCaches(required=frozenset({"plugins", "loaded_plugins", "import_errors"})),
        certification=CertificationTier.CERTIFIED,
    )

    assert module.plugins is None
    assert module.loaded_plugins == set()
    assert module.import_errors == {}


def test_verify_and_reset_module_globals_raises_on_missing_name() -> None:
    """Raise when a certified global is absent from the module entirely."""

    module = _fake_globals_module({"plugins": None})

    with pytest.raises(compat_components.ComponentSandboxError, match="plugins_cache"):
        compat_components._verify_and_reset_module_globals(
            module,
            CertifiedCaches(required=frozenset({"plugins", "plugins_cache"})),
            certification=CertificationTier.CERTIFIED,
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
    module = _fake_globals_module(dict.fromkeys(certified.required, "populated"))

    compat_components._verify_and_reset_module_globals(
        module, certified, certification=CertificationTier.CERTIFIED
    )

    assert module.loaded_plugins == set()
    assert module.import_errors == {}
    for name in certified.required - {"loaded_plugins", "import_errors"}:
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

    `airflow_components` itself never reaches this call on 2.x -- `require_v3`
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

    from pytest_airflow_in_a_box._compat.capabilities import CERTIFIED_CORE_PLUGINS_MANAGER_CACHES

    real_capabilities = resolve_capabilities()
    fake_capabilities = replace(
        real_capabilities, plugins_manager=PluginsManagerShape.CACHED_FUNCTIONS
    )
    # A fake CACHED_FUNCTIONS-shaped core half rather than the real installed module:
    # the core half must pass its own verification first for the missing-SDK guard to
    # be the thing that raises, and on a real 3.1.x leg the installed core module is
    # MODULE_GLOBALS-shaped and would fail verification with a different message.
    certified = CERTIFIED_CORE_PLUGINS_MANAGER_CACHES[PluginsManagerShape.CACHED_FUNCTIONS]
    fake_core = _fake_cached_module(tuple(certified.required), module_name="fake_cached_core")
    monkeypatch.setattr(compat_components, "resolve_capabilities", lambda: fake_capabilities)
    monkeypatch.setattr(
        compat_components,
        "_plugins_manager_modules",
        lambda: (fake_core, None),
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
    fake_core = _fake_globals_module(dict.fromkeys(certified.required))
    # Any non-None object serves as the unexpectedly-present SDK half -- the guard
    # fires before anything introspects it, and a fake keeps this test independent of
    # whether the installed release (a real 3.1.x leg included) actually has one.
    fake_sdk = _fake_cached_module((), module_name="fake_unexpected_sdk")
    monkeypatch.setattr(compat_components, "resolve_capabilities", lambda: fake_capabilities)
    monkeypatch.setattr(
        compat_components,
        "_plugins_manager_modules",
        lambda: (fake_core, fake_sdk),
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
    fake_core = _fake_globals_module(dict.fromkeys(certified.required, "populated"))
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
# Probe-and-degrade: the PROBED certification tier (#212)
# ---------------------------------------------------------------------------


def test_warn_degraded_once_deduplicates_identical_messages(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Log an identical degraded-tier message once per process, distinct ones each.

    `clear_plugins_manager_caches` runs twice per component test, so without the
    dedup a persistent drift would repeat the same block for every test.

    Parameters:
        caplog: pytest.LogCaptureFixture capturing the warnings.
    """

    with caplog.at_level("WARNING"):
        compat_components._warn_degraded_once("drift alpha")
        compat_components._warn_degraded_once("drift alpha")
        compat_components._warn_degraded_once("drift beta")

    assert caplog.text.count("drift alpha") == 1
    assert caplog.text.count("drift beta") == 1


def test_verify_and_clear_cache_functions_probed_clears_uncertified_extra_generically(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Clear an uncertified extra cache on the `PROBED` tier instead of raising.

    Parameters:
        caplog: pytest.LogCaptureFixture capturing the degradation warning.
    """

    module = _fake_cached_module(("alpha", "beta"))
    module.alpha()
    module.beta()

    with caplog.at_level("WARNING"):
        compat_components._verify_and_clear_cache_functions(
            module,
            CertifiedCaches(required=frozenset({"alpha"})),
            certification=CertificationTier.PROBED,
        )

    assert module.alpha.cache_info().currsize == 0
    assert module.beta.cache_info().currsize == 0
    assert "uncertified extra ['beta']" in caplog.text


def test_verify_and_clear_cache_functions_probed_tolerates_missing_required(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Tolerate a missing required cache name on the `PROBED` tier, clearing the rest.

    Parameters:
        caplog: pytest.LogCaptureFixture capturing the degradation warning.
    """

    module = _fake_cached_module(("alpha",))
    module.alpha()

    with caplog.at_level("WARNING"):
        compat_components._verify_and_clear_cache_functions(
            module,
            CertifiedCaches(required=frozenset({"alpha", "beta"})),
            certification=CertificationTier.PROBED,
        )

    assert module.alpha.cache_info().currsize == 0
    assert "missing ['beta']" in caplog.text


def test_verify_and_clear_cache_functions_probed_leaves_foreign_reexports_untouched(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Skip an uncertified extra whose `__module__` points outside the audited module.

    Reviewer-found hole: an unbounded generic clear would wipe a re-exported
    listener/policy getter cache -- the exact never-clear state the sandbox preserves.

    Parameters:
        caplog: pytest.LogCaptureFixture capturing the re-export warning.
    """

    module = _fake_cached_module(("alpha", "reexported_getter"))
    module.reexported_getter.__module__ = "airflow.settings"
    module.alpha()
    module.reexported_getter()

    with caplog.at_level("WARNING"):
        compat_components._verify_and_clear_cache_functions(
            module,
            CertifiedCaches(required=frozenset({"alpha"})),
            certification=CertificationTier.PROBED,
        )

    assert module.alpha.cache_info().currsize == 0
    assert module.reexported_getter.cache_info().currsize == 1
    assert "re-exports cache-clearable callables owned by other modules" in caplog.text
    assert "reexported_getter" in caplog.text


def test_verify_and_clear_cache_functions_probed_stays_silent_without_drift(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Log nothing on the `PROBED` tier when the observed set matches the certified row.

    Parameters:
        caplog: pytest.LogCaptureFixture asserting no degradation warning fires.
    """

    module = _fake_cached_module(("alpha",))
    module.alpha()

    with caplog.at_level("WARNING"):
        compat_components._verify_and_clear_cache_functions(
            module,
            CertifiedCaches(required=frozenset({"alpha"})),
            certification=CertificationTier.PROBED,
        )

    assert module.alpha.cache_info().currsize == 0
    assert caplog.text == ""


def test_verify_and_reset_module_globals_probed_tolerates_missing_name(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Reset the present certified globals on the `PROBED` tier, logging the missing.

    Parameters:
        caplog: pytest.LogCaptureFixture capturing the degradation warning.
    """

    module = _fake_globals_module({"plugins": "populated"})

    with caplog.at_level("WARNING"):
        compat_components._verify_and_reset_module_globals(
            module,
            CertifiedCaches(required=frozenset({"plugins", "plugins_cache"})),
            certification=CertificationTier.PROBED,
        )

    assert module.plugins is None
    assert not hasattr(module, "plugins_cache")
    assert "plugins_cache" in caplog.text


def test_clear_plugins_manager_caches_probed_skips_missing_sdk_module(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Skip the SDK half with a warning on the `PROBED` tier instead of raising.

    Parameters:
        monkeypatch: pytest.MonkeyPatch faking the capabilities and module seams.
        caplog: pytest.LogCaptureFixture capturing the degradation warning.
    """

    real_capabilities = resolve_capabilities()
    fake_capabilities = replace(
        real_capabilities,
        plugins_manager=PluginsManagerShape.CACHED_FUNCTIONS,
        shared_module_loading=SharedModuleLoading.DUPLICATED,
        certification=CertificationTier.PROBED,
    )
    certified = CERTIFIED_CORE_PLUGINS_MANAGER_CACHES[PluginsManagerShape.CACHED_FUNCTIONS]
    fake_core = _fake_cached_module(tuple(certified.required), module_name="fake_cached_core")
    fake_shared = _fake_cached_module(("_get_grouped_entry_points",), module_name="fake_shared")
    monkeypatch.setattr(compat_components, "resolve_capabilities", lambda: fake_capabilities)
    monkeypatch.setattr(compat_components, "_plugins_manager_modules", lambda: (fake_core, None))
    monkeypatch.setattr(
        compat_components,
        "_shared_module_loading_modules",
        lambda _shape: (fake_shared, fake_shared),
    )

    with caplog.at_level("WARNING"):
        compat_components.clear_plugins_manager_caches()

    assert "SDK half of the cache clear is skipped" in caplog.text


def test_clear_plugins_manager_caches_probed_clears_unexpected_sdk_module(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Clear an unexpectedly present SDK module generically on the `PROBED` tier.

    Parameters:
        monkeypatch: pytest.MonkeyPatch faking the capabilities and module seams.
        caplog: pytest.LogCaptureFixture capturing the degradation warning.
    """

    real_capabilities = resolve_capabilities()
    fake_capabilities = replace(
        real_capabilities,
        plugins_manager=PluginsManagerShape.MODULE_GLOBALS,
        shared_module_loading=SharedModuleLoading.SINGLE,
        certification=CertificationTier.PROBED,
    )
    certified = CERTIFIED_CORE_PLUGINS_MANAGER_CACHES[PluginsManagerShape.MODULE_GLOBALS]
    fake_core = _fake_globals_module(dict.fromkeys(certified.required, "populated"))
    fake_sdk = _fake_cached_module(("surprise_cache",), module_name="fake_unexpected_sdk")
    fake_sdk.surprise_cache()
    fake_shared = _fake_cached_module(("_get_grouped_entry_points",), module_name="fake_shared")
    monkeypatch.setattr(compat_components, "resolve_capabilities", lambda: fake_capabilities)
    monkeypatch.setattr(
        compat_components, "_plugins_manager_modules", lambda: (fake_core, fake_sdk)
    )
    monkeypatch.setattr(
        compat_components, "_shared_module_loading_modules", lambda _shape: (fake_shared,)
    )

    with caplog.at_level("WARNING"):
        compat_components.clear_plugins_manager_caches()

    assert fake_core.plugins is None
    assert fake_sdk.surprise_cache.cache_info().currsize == 0
    assert "uncertified extra ['surprise_cache']" in caplog.text
    assert "cannot be vetted" in caplog.text


def test_clear_plugins_manager_caches_probed_sweeps_core_cache_functions_on_module_globals(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Clear an uncertified `functools.cache` grown on a `MODULE_GLOBALS`-shaped core.

    Reviewer-found hole: the `MODULE_GLOBALS` arm only reset plain globals, so a gap
    release adding a cache function to `airflow.plugins_manager` leaked that cache
    across sandboxes with no log line.

    Parameters:
        monkeypatch: pytest.MonkeyPatch faking the capabilities and module seams.
        caplog: pytest.LogCaptureFixture capturing the drift warning.
    """

    real_capabilities = resolve_capabilities()
    fake_capabilities = replace(
        real_capabilities,
        plugins_manager=PluginsManagerShape.MODULE_GLOBALS,
        shared_module_loading=SharedModuleLoading.SINGLE,
        certification=CertificationTier.PROBED,
    )
    certified = CERTIFIED_CORE_PLUGINS_MANAGER_CACHES[PluginsManagerShape.MODULE_GLOBALS]
    fake_core = _fake_globals_module(dict.fromkeys(certified.required, "populated"))
    fake_core.__name__ = "fake_31_core"
    surprise = _fake_cached_module(("surprise_cache",), module_name="fake_31_core")
    fake_core.surprise_cache = surprise.surprise_cache
    fake_core.surprise_cache()
    fake_shared = _fake_cached_module(("_get_grouped_entry_points",), module_name="fake_shared")
    monkeypatch.setattr(compat_components, "resolve_capabilities", lambda: fake_capabilities)
    monkeypatch.setattr(compat_components, "_plugins_manager_modules", lambda: (fake_core, None))
    monkeypatch.setattr(
        compat_components, "_shared_module_loading_modules", lambda _shape: (fake_shared,)
    )

    with caplog.at_level("WARNING"):
        compat_components.clear_plugins_manager_caches()

    assert fake_core.plugins is None
    assert fake_core.surprise_cache.cache_info().currsize == 0
    assert "uncertified extra ['surprise_cache']" in caplog.text


def test_clear_plugins_manager_caches_probed_flags_an_unclearable_unexpected_sdk_module(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Warn even when the unexpected SDK module exposes nothing generically clearable.

    A module holding only plain globals has no `.cache_clear` markers, so the generic
    clear's own drift log stays silent; the branch-level warning is the only signal
    that un-restorable state exists. Reviewer-found hole: without it, this exact case
    no-opped with no log line at all.

    Parameters:
        monkeypatch: pytest.MonkeyPatch faking the capabilities and module seams.
        caplog: pytest.LogCaptureFixture capturing the degradation warning.
    """

    real_capabilities = resolve_capabilities()
    fake_capabilities = replace(
        real_capabilities,
        plugins_manager=PluginsManagerShape.MODULE_GLOBALS,
        shared_module_loading=SharedModuleLoading.SINGLE,
        certification=CertificationTier.PROBED,
    )
    certified = CERTIFIED_CORE_PLUGINS_MANAGER_CACHES[PluginsManagerShape.MODULE_GLOBALS]
    fake_core = _fake_globals_module(dict.fromkeys(certified.required, "populated"))
    fake_sdk = _fake_globals_module({"plugins": "populated"}, module_name="fake_globals_sdk")
    fake_shared = _fake_cached_module(("_get_grouped_entry_points",), module_name="fake_shared")
    monkeypatch.setattr(compat_components, "resolve_capabilities", lambda: fake_capabilities)
    monkeypatch.setattr(
        compat_components, "_plugins_manager_modules", lambda: (fake_core, fake_sdk)
    )
    monkeypatch.setattr(
        compat_components, "_shared_module_loading_modules", lambda _shape: (fake_shared,)
    )

    with caplog.at_level("WARNING"):
        compat_components.clear_plugins_manager_caches()

    assert fake_sdk.plugins == "populated"
    assert "cannot be vetted" in caplog.text


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
    assert instance in compat_components._live_plugin_list(core_module)
    if sdk_module is not None:
        assert instance in compat_components._live_plugin_list(sdk_module)

    compat_components.clear_plugins_manager_caches()

    assert instance not in compat_components._live_plugin_list(core_module)


def test_register_plugin_without_an_sdk_half(monkeypatch: pytest.MonkeyPatch) -> None:
    """Register a plugin core-only when no Task SDK plugins-manager module resolves."""

    from airflow.plugins_manager import AirflowPlugin

    class _Plugin(AirflowPlugin):
        name = "sandbox_compat_test_plugin_core_only"

    core_module, _sdk_module = compat_components._plugins_manager_modules()
    monkeypatch.setattr(compat_components, "_plugins_manager_modules", lambda: (core_module, None))

    instance = compat_components.register_plugin(_Plugin)

    assert instance in compat_components._live_plugin_list(core_module)

    # Undo the "no SDK half" patch before clearing: on a `CACHED_FUNCTIONS` release
    # that certified shape requires an SDK module to exist -- `clear_plugins_manager_caches`
    # would otherwise (correctly) treat the still-patched "no SDK half" as a real
    # environment inconsistency and raise `ComponentSandboxError`, exactly as the
    # neighboring `..._has_no_sdk_module` test exercises on purpose. This test's own
    # concern is `register_plugin`'s core-only behavior, not that guard.
    monkeypatch.undo()
    compat_components.clear_plugins_manager_caches()


def test_live_plugin_list_module_globals_shape_loads_then_appends() -> None:
    """Resolve the 3.1.x `plugins` module global, loading it via `ensure_plugins_loaded`.

    The fake reproduces the 3.1.0 wheel's shape: `plugins` starts as None and
    `ensure_plugins_loaded()` populates it (see PROVENANCE.md).
    """

    module = _fake_globals_module({"plugins": None}, module_name="fake_31_plugins_manager")

    def _ensure_plugins_loaded() -> None:
        if module.plugins is None:
            module.plugins = []

    module.ensure_plugins_loaded = _ensure_plugins_loaded

    live = compat_components._live_plugin_list(module)
    live.append("sentinel")

    assert module.plugins == ["sentinel"]


def test_register_plugin_module_globals_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    """Register a plugin into a faked 3.1.x `MODULE_GLOBALS`-shaped core half."""

    from airflow.plugins_manager import AirflowPlugin

    class _Plugin(AirflowPlugin):
        name = "sandbox_compat_test_plugin_module_globals"

    fake_core = _fake_globals_module({"plugins": []}, module_name="fake_31_core")
    fake_core.ensure_plugins_loaded = lambda: None
    monkeypatch.setattr(compat_components, "_plugins_manager_modules", lambda: (fake_core, None))

    instance = compat_components.register_plugin(_Plugin)

    assert instance in fake_core.plugins


def test_prepare_plugin_instance_copies_lists_shallowly_across_two_registrations() -> None:
    """Keep two registrations of the same plugin class independent after a mutation.

    `_get_ui_plugins` mutates `external_views`/`react_apps` in place while building its
    own return value; without a fresh list copy at registration time, a second
    registration of the same class would inherit whatever the first registration's own
    downstream processing already removed. The copy is SHALLOW on purpose: the list
    CONTAINER is rebound, but the elements keep their identity -- Airflow must register
    the very objects the test holds references to, not copies it could never observe.
    """

    from airflow.plugins_manager import AirflowPlugin

    shared_view = {"url_route": "shared", "label": "shared"}

    class _Plugin(AirflowPlugin):
        name = "sandbox_compat_test_plugin_shallow_copy"

        def __init__(self) -> None:
            """Set `external_views` per instance, holding the shared element object."""

            super().__init__()
            self.external_views = [shared_view]

    built = _Plugin()
    original_list = built.external_views

    prepared = cast(_Plugin, compat_components._prepare_plugin_instance(built))

    assert prepared is built
    assert prepared.external_views is not original_list
    assert prepared.external_views == [shared_view]
    assert prepared.external_views[0] is shared_view
    prepared.external_views.remove(prepared.external_views[0])
    assert original_list == [shared_view]

    second = cast(_Plugin, compat_components._prepare_plugin_instance(_Plugin))

    assert second.external_views == [shared_view]
    assert second.external_views[0] is shared_view
    assert second.external_views is not prepared.external_views


def test_prepare_plugin_instance_accepts_a_module_valued_listener() -> None:
    """Accept the canonical `listeners = [module]` plugin shape without copying it.

    Airflow's own shipped `example_dags/plugins/listener_plugin.py` sets `listeners`
    to a bare module -- an object `copy.deepcopy` cannot handle at all
    (`TypeError: cannot pickle 'module' object`), which is one of the two reasons
    `_prepare_plugin_instance` copies shallowly.
    """

    from airflow.plugins_manager import AirflowPlugin

    listener_module = types.ModuleType("sandbox_compat_test_listener_module")

    class _Plugin(AirflowPlugin):
        name = "sandbox_compat_test_plugin_module_listener"

        def __init__(self) -> None:
            """Set `listeners` to a bare module, the canonical upstream example shape."""

            super().__init__()
            self.listeners = [listener_module]

    prepared = cast(_Plugin, compat_components._prepare_plugin_instance(_Plugin))

    assert prepared.listeners == [listener_module]
    assert prepared.listeners[0] is listener_module
    assert prepared.listeners is not _Plugin.listeners


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

    # `get_dagbag_import_timeout`, not `task_instance_mutation_hook`: the mutation
    # hook's hookspec signature grew a `dag_run` parameter in 3.3.0, so a hookimpl
    # written against it is not registrable on the 3.1.x/3.2.x legs, while this
    # hookspec's `(dag_file_path)` shape is identical on every certified release.
    def _hook(dag_file_path: str) -> int:
        del dag_file_path
        received["called"] = True
        return 30

    plugin_class = compat_components.build_policy_plugin({"get_dagbag_import_timeout": _hook})
    instance = compat_components.register_policy(plugin_class, pm)

    assert instance in pm.get_plugins()
    # `get_dagbag_import_timeout` is `firstresult=True` and pluggy dispatches
    # last-registered-first, so the freshly registered hook wins over `DefaultPolicy`.
    assert pm.hook.get_dagbag_import_timeout(dag_file_path="a_dag.py") == 30
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


def test_mark_task_instance_mutation_hook_active_flips_the_flag() -> None:
    """Flip `is_noop` to False so a sandbox-registered mutation hook actually fires."""

    from airflow.settings import task_instance_mutation_hook

    hook = cast("Any", task_instance_mutation_hook)
    before = compat_components.snapshot_task_instance_mutation_hook_is_noop()

    compat_components.mark_task_instance_mutation_hook_active()

    assert hook.is_noop is False

    compat_components.restore_task_instance_mutation_hook_is_noop(before)
    assert hook.is_noop is before


# ---------------------------------------------------------------------------
# Executor loader
# ---------------------------------------------------------------------------


def _fake_executor_loader_module_v31() -> Any:
    """Build a fake module reproducing the flat 3.1.x `executor_loader.py` globals.

    Transcribed from the `apache-airflow-core==3.1.0`/`==3.1.8` wheels (identical
    shape; see PROVENANCE.md): no `_per_team` suffixes, flat single-level dicts, and a
    scalar-valued `_team_name_to_executors`.

    Returns:
        Any containing a `types.ModuleType` with the five flat globals and a minimal
        `ExecutorLoader` stand-in exposing `executors` and `_get_executor_names`.
    """

    class _FakeExecutorLoader:
        executors: ClassVar[dict[str, str]] = {}

        @classmethod
        def _get_executor_names(cls) -> list[Any]:
            return []

    return _fake_globals_module(
        {
            "ExecutorLoader": _FakeExecutorLoader,
            "_alias_to_executors": {},
            "_module_to_executors": {},
            "_classname_to_executors": {},
            "_team_name_to_executors": {},
            "_executor_names": [],
        },
        module_name="fake_executor_loader_v31",
    )


@pytest.mark.skipif(
    resolve_capabilities().plugins_manager is not PluginsManagerShape.CACHED_FUNCTIONS,
    reason=(
        "the executor-loader per-team split shipped in the same 3.2.0 break the "
        "CACHED_FUNCTIONS plugins-manager shape certifies"
    ),
)
def test_executor_loader_is_per_team_true_on_the_installed_release() -> None:
    """Probe the real installed `executor_loader` as the 3.2+ per-team shape."""

    assert compat_components._executor_loader_is_per_team(
        compat_components._executor_loader_module()
    )


def test_executor_loader_v31_snapshot_register_restore_round_trips(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Snapshot, register, and restore against the faked flat 3.1.x global shape.

    Mirrors upstream's own `_get_executor_names` population loop: flat dict writes
    keyed by alias/module-path/classname, and a SCALAR `_team_name_to_executors[None]`
    assignment (upstream assigns, never appends -- last resolved executor per team
    wins).
    """

    fake_module = _fake_executor_loader_module_v31()
    monkeypatch.setattr(compat_components, "_executor_loader_module", lambda: fake_module)

    before = compat_components.snapshot_executor_loader()
    assert isinstance(before, compat_components.ExecutorLoaderSnapshotV31)

    alias = compat_components.register_executor(_Executor, alias="sandbox_compat_test_v31")

    assert alias == "sandbox_compat_test_v31"
    module_path = f"{_Executor.__module__}.{_Executor.__qualname__}"
    assert fake_module._alias_to_executors[alias].module_path == module_path
    assert fake_module._module_to_executors[module_path].alias == alias
    assert fake_module._classname_to_executors["_Executor"].alias == alias
    assert fake_module._team_name_to_executors[None].alias == alias
    assert len(fake_module._executor_names) == 1
    assert fake_module.ExecutorLoader.executors == {alias: module_path}

    compat_components.restore_executor_loader(before)

    assert fake_module._alias_to_executors == {}
    assert fake_module._module_to_executors == {}
    assert fake_module._classname_to_executors == {}
    assert fake_module._team_name_to_executors == {}
    assert fake_module._executor_names == []
    assert fake_module.ExecutorLoader.executors == {}


def test_register_executor_creates_the_none_team_buckets_when_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Create the per-team `None` buckets via `setdefault` when nothing populated them.

    `register_executor` must stay independently correct even when nothing (the
    sandbox's own snapshot included) has forced `_get_executor_names()`'s natural
    resolution first -- direct indexing on an empty per-team dict would `KeyError`.
    """

    class _FakeExecutorLoader:
        executors: ClassVar[dict[str, str]] = {}

    module = _fake_globals_module(
        {
            "ExecutorLoader": _FakeExecutorLoader,
            "_alias_to_executors_per_team": {},
            "_module_to_executors_per_team": {},
            "_classname_to_executors_per_team": {},
            "_team_name_to_executors": {},
            "_executor_names": [],
        },
        module_name="fake_executor_loader_per_team_empty",
    )
    monkeypatch.setattr(compat_components, "_executor_loader_module", lambda: module)

    alias = compat_components.register_executor(_Executor, alias="sandbox_compat_test_buckets")

    module_path = f"{_Executor.__module__}.{_Executor.__qualname__}"
    assert module._alias_to_executors_per_team[None][alias].module_path == module_path
    assert module._module_to_executors_per_team[None][module_path].alias == alias
    assert module._classname_to_executors_per_team[None]["_Executor"].alias == alias
    assert module._team_name_to_executors[None][0].alias == alias


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
# Macros parent module new-key cleanup
# ---------------------------------------------------------------------------


def test_sdk_macros_module_resolves_on_the_installed_release() -> None:
    """Resolve the real macros parent module every certified 3.x release shares."""

    module = compat_components._sdk_macros_module()

    assert module is not None
    assert module.__name__ == "airflow.sdk.execution_time.macros"


def test_sdk_macros_module_reports_none_when_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Report None when the macros parent module is not importable at all.

    Both the `sys.modules` poison and the parent-package `delattr` are needed -- see
    `test_plugins_manager_modules_reports_no_sdk_half_when_absent`'s docstring for why
    relying on `sys.modules` alone is order-dependent on prior imports.
    """

    monkeypatch.setitem(sys.modules, "airflow.sdk.execution_time.macros", None)
    monkeypatch.delattr(sys.modules["airflow.sdk.execution_time"], "macros", raising=False)

    assert compat_components._sdk_macros_module() is None


def test_restore_macros_module_keys_deletes_only_new_attributes() -> None:
    """Delete a macros-parent attribute added after the snapshot, keeping the rest.

    Reproduces `integrate_macros_plugins`'s own leak shape: `setattr` of a per-plugin
    macros module onto the parent under the raw plugin name, which upstream never
    removes.
    """

    module = compat_components._sdk_macros_module()
    assert module is not None
    pre_existing = next(iter(vars(module)))
    before = compat_components.snapshot_macros_module_keys()
    assert before is not None

    leaked = types.ModuleType("airflow.sdk.execution_time.macros.sandbox_compat_leak")
    module.sandbox_compat_leak = leaked
    assert hasattr(module, "sandbox_compat_leak")

    compat_components.restore_macros_module_keys(before)

    assert not hasattr(module, "sandbox_compat_leak")
    assert hasattr(module, pre_existing)


def test_snapshot_macros_module_keys_reports_none_when_the_module_is_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report None from the snapshot, and no-op the restore, without the module."""

    monkeypatch.setattr(compat_components, "_sdk_macros_module", lambda: None)

    before = compat_components.snapshot_macros_module_keys()

    assert before is None
    compat_components.restore_macros_module_keys(before)


def test_restore_macros_module_keys_tolerates_the_module_vanishing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No-op the restore when the module resolved at snapshot time but not anymore."""

    before = compat_components.snapshot_macros_module_keys()
    assert before is not None
    monkeypatch.setattr(compat_components, "_sdk_macros_module", lambda: None)

    compat_components.restore_macros_module_keys(before)


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

    # `task_instance_mutation_hook`, not `get_policy_plugin_manager`: the latter is
    # 3.2+-only, while this dispatch function exists on every certified 3.x release.
    assert hasattr(settings, "task_instance_mutation_hook")


# ---------------------------------------------------------------------------
# Timetable and weight-strategy registration (#114)
# ---------------------------------------------------------------------------


class _SymmetricTimetable(Timetable):
    """Round-trip cleanly: symmetric `serialize`/`deserialize`, no `__eq__` of its own.

    Deliberately minimal, not the corpus `ExampleTimetable`: these classes feed the
    gate-free `_compat` functions directly, so only the serialize pair matters, and
    each sibling below mutates exactly one property of this baseline.
    """

    def __init__(self, hours: int = 1) -> None:
        """Carry one piece of round-trippable state.

        Parameters:
            hours: int containing the interval length the serialize pair carries.
        """

        self.hours = hours

    def serialize(self) -> dict[str, Any]:
        """Emit the full state.

        Returns:
            dict[str, Any] containing the interval length.
        """

        return {"hours": self.hours}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> _SymmetricTimetable:
        """Reconstruct the full state.

        Parameters:
            data: dict[str, Any] containing the serialized payload.

        Returns:
            _SymmetricTimetable carrying the payload's state.
        """

        return cls(hours=data["hours"])


class _EqualityTimetable(Timetable):
    """Round-trip cleanly AND define a real `__eq__`, driving the equality comparison."""

    def __init__(self, hours: int = 1) -> None:
        """Carry one piece of round-trippable state.

        Parameters:
            hours: int containing the interval length the serialize pair carries.
        """

        self.hours = hours

    def serialize(self) -> dict[str, Any]:
        """Emit the full state.

        Returns:
            dict[str, Any] containing the interval length.
        """

        return {"hours": self.hours}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> _EqualityTimetable:
        """Reconstruct the full state.

        Parameters:
            data: dict[str, Any] containing the serialized payload.

        Returns:
            _EqualityTimetable carrying the payload's state.
        """

        return cls(hours=data["hours"])

    def __eq__(self, other: object) -> bool:
        """Compare by type and state, agreeing with the serialize pair.

        Parameters:
            other: object containing the comparison target.

        Returns:
            bool marking value equality.
        """

        return type(other) is type(self) and other.hours == self.hours

    def __hash__(self) -> int:
        """Hash consistently with `__eq__`.

        Returns:
            int containing the state hash.
        """

        return hash(self.hours)


class _StateDroppingTimetable(Timetable):
    """Drop state in `deserialize`, so the reconstructed payload differs.

    NOT the fixtures-level `_StateDroppingTimetable` (which additionally overrides the
    protocol methods to pass the full conformance gate); this one stays minimal
    because the gate-free `_compat` functions never inspect them.
    """

    def __init__(self, hours: int = 1) -> None:
        """Carry the one piece of state `deserialize` deliberately drops.

        Parameters:
            hours: int containing the interval length `serialize` emits.
        """

        self.hours = hours

    def serialize(self) -> dict[str, Any]:
        """Emit the state `deserialize` will drop.

        Returns:
            dict[str, Any] containing the interval length.
        """

        return {"hours": self.hours}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> _StateDroppingTimetable:
        """Reconstruct with the default interval, discarding the payload.

        Parameters:
            data: dict[str, Any] containing the ignored serialized payload.

        Returns:
            _StateDroppingTimetable reconstructed with default state.
        """

        del data
        return cls()


class _WrongClassTimetable(Timetable):
    """Reconstruct as a different class entirely, the worst asymmetry."""

    def serialize(self) -> dict[str, Any]:
        """Emit an empty payload.

        Returns:
            dict[str, Any] containing nothing.
        """

        return {}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> _SymmetricTimetable:
        """Reconstruct the WRONG class on purpose.

        Parameters:
            data: dict[str, Any] containing the serialized payload.

        Returns:
            _SymmetricTimetable, never `cls`.
        """

        return _SymmetricTimetable(hours=data.get("hours", 1))


class _NeverEqualTimetable(Timetable):
    """Round-trip a symmetric payload while `__eq__` still refuses every comparison."""

    def serialize(self) -> dict[str, Any]:
        """Emit an empty payload.

        Returns:
            dict[str, Any] containing nothing.
        """

        return {}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> _NeverEqualTimetable:
        """Reconstruct statelessly.

        Parameters:
            data: dict[str, Any] containing the ignored serialized payload.

        Returns:
            _NeverEqualTimetable with no state.
        """

        del data
        return cls()

    def __eq__(self, other: object) -> bool:
        """Refuse every comparison, disagreeing with the symmetric payload.

        Parameters:
            other: object containing the comparison target.

        Returns:
            bool; always False.
        """

        del other
        return False

    def __hash__(self) -> int:
        """Hash by type, keeping the class hashable despite the broken `__eq__`.

        Returns:
            int containing the type-name hash.
        """

        return hash(type(self).__qualname__)


def _lookup_qualname(component_type: type) -> str:
    """Build the qualname key Airflow's registered-component lookups are keyed by.

    Upstream's `qualname()` helper keys a class by `__module__.__name__`, NOT
    `__qualname__` -- identical for the module-level classes real registrations
    require, but a function-local class would key without its `<locals>` segment.

    Parameters:
        component_type: type containing the registered component class.

    Returns:
        str containing the `module.name` lookup key.
    """

    return f"{component_type.__module__}.{component_type.__name__}"


def test_build_component_plugin_subclasses_the_core_airflow_plugin() -> None:
    """Synthesize a real `AirflowPlugin` subclass carrying the class on one attribute.

    Subclassing matters: the plugins-manager cache functions iterate EVERY list
    attribute of EVERY plugin, so all of `PLUGIN_LIST_ATTRIBUTES` must resolve on the
    synthesized plugin through the base class's own defaults.
    """

    from airflow.plugins_manager import AirflowPlugin

    plugin_class = compat_components.build_component_plugin(_SymmetricTimetable, "timetables")

    assert issubclass(plugin_class, AirflowPlugin)
    assert plugin_class.timetables == [_SymmetricTimetable]
    assert plugin_class.name == "pytest-airflow-in-a-box-timetables-_SymmetricTimetable"
    # Subclassing the INSTALLED base is itself the guarantee that every list attribute
    # this release's cache functions iterate resolves on the synthesized plugin --
    # asserted structurally rather than against `PLUGIN_LIST_ATTRIBUTES`, which
    # transcribes the 3.3 superset (`partition_mappers` and friends do not exist on
    # the 3.1/3.2 bases and would false-fail those legs).
    declared = [
        attribute
        for attribute in compat_components.PLUGIN_LIST_ATTRIBUTES
        if hasattr(AirflowPlugin, attribute)
    ]
    assert declared
    for attribute in declared:
        assert hasattr(plugin_class, attribute)


@pytest.mark.skipif(
    resolve_capabilities().plugins_manager is not PluginsManagerShape.CACHED_FUNCTIONS,
    reason="the cached lookup getters exist only on the 3.2+ CACHED_FUNCTIONS shape",
)
def test_register_timetable_resolves_through_a_prewarmed_lookup() -> None:
    """Register a timetable and resolve it by qualname through a PRE-WARMED cache.

    Warming `get_timetables_plugins()` first is the point: it proves
    `invalidate_component_lookup_caches` really drops the derived mapping, rather than
    the registration merely winning because nothing had populated the cache yet.
    """

    core_module, _sdk_module = compat_components._plugins_manager_modules()
    key = _lookup_qualname(_SymmetricTimetable)
    assert key not in core_module.get_timetables_plugins()

    returned = compat_components.register_timetable(_SymmetricTimetable(hours=2))

    assert returned is _SymmetricTimetable
    assert core_module.get_timetables_plugins()[key] is _SymmetricTimetable

    compat_components.clear_plugins_manager_caches()

    assert key not in core_module.get_timetables_plugins()


@pytest.mark.skipif(
    resolve_capabilities().plugins_manager is not PluginsManagerShape.CACHED_FUNCTIONS,
    reason="the cached lookup getters exist only on the 3.2+ CACHED_FUNCTIONS shape",
)
def test_register_weight_strategy_resolves_through_a_prewarmed_lookup() -> None:
    """Register a weight strategy and resolve it by qualname through a pre-warmed cache."""

    from airflow.task.priority_strategy import PriorityWeightStrategy

    class _Strategy(PriorityWeightStrategy):
        def get_weight(self, ti: Any) -> int:
            del ti
            return 1

    core_module, _sdk_module = compat_components._plugins_manager_modules()
    key = _lookup_qualname(_Strategy)
    assert key not in core_module.get_priority_weight_strategy_plugins()

    returned = compat_components.register_weight_strategy(_Strategy)

    assert returned is _Strategy
    assert core_module.get_priority_weight_strategy_plugins()[key] is _Strategy

    compat_components.clear_plugins_manager_caches()

    assert key not in core_module.get_priority_weight_strategy_plugins()


def test_invalidate_component_lookup_caches_module_globals_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reset only the two derived lookup globals on a faked 3.1.x-shaped core half."""

    fake_core = _fake_globals_module(
        {
            "plugins": ["kept"],
            "timetable_classes": {"stale": object},
            "priority_weight_strategy_classes": {"stale": object},
        },
        module_name="fake_31_lookup_core",
    )
    monkeypatch.setattr(compat_components, "_plugins_manager_modules", lambda: (fake_core, None))

    compat_components.invalidate_component_lookup_caches()

    assert fake_core.timetable_classes is None
    assert fake_core.priority_weight_strategy_classes is None
    # The live plugin list survives -- the whole reason this is not
    # `clear_plugins_manager_caches`.
    assert fake_core.plugins == ["kept"]


def test_register_timetable_module_globals_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    """Register a timetable end-to-end against a faked 3.1.x `MODULE_GLOBALS` core half."""

    fake_core = _fake_globals_module(
        {
            "plugins": [],
            "timetable_classes": {"stale": object},
            "priority_weight_strategy_classes": None,
        },
        module_name="fake_31_timetable_core",
    )
    fake_core.ensure_plugins_loaded = lambda: None
    from airflow.plugins_manager import AirflowPlugin

    fake_core.AirflowPlugin = AirflowPlugin
    monkeypatch.setattr(compat_components, "_plugins_manager_modules", lambda: (fake_core, None))

    compat_components.register_timetable(_SymmetricTimetable)

    assert fake_core.timetable_classes is None
    registered = [type(plugin).__name__ for plugin in fake_core.plugins]
    assert registered == ["ComponentRegistryPlugin"]
    assert fake_core.plugins[0].timetables == [_SymmetricTimetable]


def test_timetable_round_trip_returns_no_problems_for_a_symmetric_pair() -> None:
    """Return zero problems for a registered, symmetric timetable without `__eq__`."""

    compat_components.register_timetable(_SymmetricTimetable)

    assert compat_components.timetable_round_trip(_SymmetricTimetable(hours=2)) == ()

    compat_components.clear_plugins_manager_caches()


def test_timetable_round_trip_compares_equal_under_a_real_dunder_eq() -> None:
    """Return zero problems when the class's own `__eq__` also agrees."""

    compat_components.register_timetable(_EqualityTimetable)

    assert compat_components.timetable_round_trip(_EqualityTimetable(hours=3)) == ()

    compat_components.clear_plugins_manager_caches()


def test_timetable_round_trip_flags_a_payload_mismatch() -> None:
    """Flag a `deserialize` that drops state the original `serialize` emitted."""

    compat_components.register_timetable(_StateDroppingTimetable)

    problems = compat_components.timetable_round_trip(_StateDroppingTimetable(hours=5))

    assert [problem.code for problem in problems] == [
        compat_components.TIMETABLE_ROUND_TRIP_MISMATCH
    ]
    assert "serializes to" in problems[0].message

    compat_components.clear_plugins_manager_caches()


def test_timetable_round_trip_flags_a_wrong_class_reconstruction() -> None:
    """Flag a `deserialize` reconstructing a different class, and stop there."""

    compat_components.register_timetable(_WrongClassTimetable)
    compat_components.register_timetable(_SymmetricTimetable)

    problems = compat_components.timetable_round_trip(_WrongClassTimetable())

    assert [problem.code for problem in problems] == [
        compat_components.TIMETABLE_ROUND_TRIP_MISMATCH
    ]
    assert "reconstructed `_SymmetricTimetable`" in problems[0].message

    compat_components.clear_plugins_manager_caches()


def test_timetable_round_trip_flags_an_eq_disagreement() -> None:
    """Flag a class whose own `__eq__` refuses the reconstructed twin.

    The payload comparison passes (both serialize to `{}`), isolating the equality
    problem as the only finding.
    """

    compat_components.register_timetable(_NeverEqualTimetable)

    problems = compat_components.timetable_round_trip(_NeverEqualTimetable())

    assert [problem.code for problem in problems] == [
        compat_components.TIMETABLE_ROUND_TRIP_MISMATCH
    ]
    assert "compares unequal" in problems[0].message

    compat_components.clear_plugins_manager_caches()


def test_derived_lookup_cache_names_are_a_strict_subset_of_the_certified_rows() -> None:
    """Pin the never-clear-`_get_plugins` invariant as data against the certified table.

    `invalidate_component_lookup_caches` must drop ONLY caches derived from the
    plugin list: every name it touches has to be certified for its shape (or the
    certification drifted), and the plugin-list holders themselves
    (`_get_plugins`/`plugins`) must never appear in the derived sets.
    """

    cached = CERTIFIED_CORE_PLUGINS_MANAGER_CACHES[PluginsManagerShape.CACHED_FUNCTIONS]
    module_globals = CERTIFIED_CORE_PLUGINS_MANAGER_CACHES[PluginsManagerShape.MODULE_GLOBALS]

    assert set(compat_components.DERIVED_LOOKUP_CACHE_FUNCTIONS) < cached.required
    assert set(compat_components.DERIVED_LOOKUP_MODULE_GLOBALS) < module_globals.required
    assert "_get_plugins" not in compat_components.DERIVED_LOOKUP_CACHE_FUNCTIONS
    assert "plugins" not in compat_components.DERIVED_LOOKUP_MODULE_GLOBALS


@pytest.mark.skipif(
    resolve_capabilities().plugins_manager is not PluginsManagerShape.CACHED_FUNCTIONS,
    reason="the cached lookup getters exist only on the 3.2+ CACHED_FUNCTIONS shape",
)
def test_timetable_lookup_resolves_tracks_registration() -> None:
    """Resolve False before registration, True after, False again after the clear."""

    assert not compat_components.timetable_lookup_resolves(_SymmetricTimetable)

    compat_components.register_timetable(_SymmetricTimetable)

    assert compat_components.timetable_lookup_resolves(_SymmetricTimetable)

    compat_components.clear_plugins_manager_caches()

    assert not compat_components.timetable_lookup_resolves(_SymmetricTimetable)


def test_timetable_lookup_resolves_module_globals_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resolve through the 3.1.x `timetable_classes` global, loading it when None."""

    key = compat_components.lookup_key(_SymmetricTimetable)
    fake_core = _fake_globals_module(
        {"timetable_classes": None}, module_name="fake_31_lookup_resolves_core"
    )

    def _initialize_timetables_plugins() -> None:
        if fake_core.timetable_classes is None:
            fake_core.timetable_classes = {key: _SymmetricTimetable}

    fake_core.initialize_timetables_plugins = _initialize_timetables_plugins
    monkeypatch.setattr(compat_components, "_plugins_manager_modules", lambda: (fake_core, None))

    assert compat_components.timetable_lookup_resolves(_SymmetricTimetable)
    assert not compat_components.timetable_lookup_resolves(_EqualityTimetable)
