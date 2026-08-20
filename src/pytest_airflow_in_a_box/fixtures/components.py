"""Register a custom Airflow component for one test, reverting it afterward.

See `pytest_airflow_in_a_box._compat.components` for the snapshot/restore mechanics and
the lazy cache-enumeration contract this fixture triggers on first use.

References:
    https://docs.pytest.org/en/stable/how-to/fixtures.html
    https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/plugins.html
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from pytest_airflow_in_a_box._compat import components as sandbox
from pytest_airflow_in_a_box._compat.capabilities import v2_gate_message
from pytest_airflow_in_a_box._compat.components import KIND_CLASSIFIERS, _as_type
from pytest_airflow_in_a_box.bootstrap import get_bootstrap_state
from pytest_airflow_in_a_box.components import ComponentKind, check_component

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

    from pytest_airflow_in_a_box.types import ComponentRegistry

# The four kinds `round_trip()` can classify unambiguously: each takes a bare component
# positionally and nothing else structurally identifies it. `POLICY` is excluded on
# purpose -- it has no bare-component form, only `**hookname_to_callable` -- and so are
# every static-check-only kind `check_component` supports (`TIMETABLE`, `XCOM`,
# `WEIGHT_STRATEGY`, `NOTIFIER`, `PROVIDER`), which `ComponentRegistry` never registers
# at all: a Timetable is passed directly to a Dag's `schedule=`, an XCom backend is
# configured once for the whole run via the `airflow_xcom_backend` ini (#112), and a
# weight strategy/notifier/provider is referenced directly in Dag code, never through a
# process-global registry a sandbox would need to revert.
_ROUND_TRIP_KINDS = (
    ComponentKind.PLUGIN,
    ComponentKind.LISTENER,
    ComponentKind.EXECUTOR,
    ComponentKind.SECRETS_BACKEND,
)


class _ComponentSandbox:
    """Concrete `ComponentRegistry`: snapshots Airflow global state once, restores it once.

    Every snapshot needed by any registration method is taken eagerly at construction,
    not lazily per method -- the fixture itself is already the laziness boundary
    (`pytest_airflow_in_a_box._compat.components` never runs a single line until a test
    requests `airflow_components`), and eager, unconditional snapshotting here keeps
    restoration simple to reason about regardless of which methods a test happens to
    call.

    Parameters:
        plugins_folder: pathlib.Path containing the run's plugins directory, needed to
            target `sys.modules` restoration.
    """

    def __init__(self, plugins_folder: Path) -> None:
        self._plugins_folder = plugins_folder
        # Clear on entry so a stale pre-test load (a leaked prior test, or a real
        # scheduler-shaped caller that ran before this fixture) cannot win; see
        # `clear_plugins_manager_caches`'s own docstring.
        sandbox.clear_plugins_manager_caches()
        self._sys_modules_before = sandbox.snapshot_sys_modules()
        self._settings_keys_before = sandbox.snapshot_settings_keys()
        self._is_noop_before = sandbox.snapshot_task_instance_mutation_hook_is_noop()
        self._secrets_backend_list_before = sandbox.snapshot_secrets_backend_list()
        self._executor_loader_before = sandbox.snapshot_executor_loader()
        self._core_listener, self._task_listener = sandbox.listener_managers()
        self._core_listener_before = sandbox.listener_manager_snapshot(self._core_listener)
        self._task_listener_before = (
            sandbox.listener_manager_snapshot(self._task_listener)
            if self._task_listener is not None
            else ()
        )
        self._policy_pm = sandbox.policy_plugin_manager()
        self._policy_plugins_before = tuple(self._policy_pm.get_plugins())

    def plugin(self, component: object) -> None:
        """See `pytest_airflow_in_a_box.types.ComponentRegistry.plugin`."""

        check_component(component, kind=ComponentKind.PLUGIN).raise_for_problems()
        sandbox.register_plugin(component)

    def listener(self, component: object, *, core: bool = True, task: bool = True) -> None:
        """See `pytest_airflow_in_a_box.types.ComponentRegistry.listener`."""

        check_component(component, kind=ComponentKind.LISTENER).raise_for_problems()
        managers = []
        if core:
            managers.append(self._core_listener)
        if task:
            if self._task_listener is None:
                raise sandbox.ComponentSandboxError(
                    "listener(task=True) requires the Task SDK listener manager, which "
                    "does not exist on the installed Apache Airflow release (3.1.x). "
                    "Pass task=False to register with the core manager only."
                )
            managers.append(self._task_listener)
        if not managers:
            raise sandbox.ComponentSandboxError(
                "listener() requires core=True, task=True, or both."
            )
        sandbox.register_listener(component, tuple(managers))

    def policy(self, **hooks: Callable[..., object]) -> None:
        """See `pytest_airflow_in_a_box.types.ComponentRegistry.policy`."""

        if not hooks:
            raise sandbox.ComponentSandboxError(
                "policy() requires at least one hookname=callable keyword argument."
            )
        plugin_class = sandbox.build_policy_plugin(hooks)
        check_component(plugin_class, kind=ComponentKind.POLICY).raise_for_problems()
        sandbox.register_policy(plugin_class, self._policy_pm)

    def secrets_backend(self, component: object, *, first: bool = True) -> None:
        """See `pytest_airflow_in_a_box.types.ComponentRegistry.secrets_backend`."""

        check_component(component, kind=ComponentKind.SECRETS_BACKEND).raise_for_problems()
        sandbox.register_secrets_backend(component, first=first)

    def executor(self, component: object, *, alias: str = "test") -> str:
        """See `pytest_airflow_in_a_box.types.ComponentRegistry.executor`."""

        check_component(component, kind=ComponentKind.EXECUTOR).raise_for_problems()
        return sandbox.register_executor(component, alias=alias)

    def round_trip(self, component: object) -> None:
        """See `pytest_airflow_in_a_box.types.ComponentRegistry.round_trip`."""

        matched = [kind for kind in _ROUND_TRIP_KINDS if KIND_CLASSIFIERS[kind.value](component)]
        if len(matched) != 1:
            raise sandbox.ComponentSandboxError(
                f"round_trip() could not classify `{_as_type(component).__name__}` as "
                f"exactly one of plugin/listener/executor/secrets_backend (matched "
                f"{[kind.value for kind in matched]}); call the specific method instead."
            )
        kind = matched[0]
        if kind is ComponentKind.PLUGIN:
            self.plugin(component)
        elif kind is ComponentKind.LISTENER:
            self.listener(component)
        elif kind is ComponentKind.EXECUTOR:
            self.executor(component)
        else:
            self.secrets_backend(component)

    def finalize(self) -> None:
        """Restore every snapshot, in roughly reverse registration order.

        Plugins-manager caches are cleared again at the end (not just restored via a
        snapshot -- there is no cache *value* to snapshot, only a clear operation; see
        `clear_plugins_manager_caches`'s docstring), so nothing this test computed
        lingers for the next one.
        """

        sandbox.restore_executor_loader(self._executor_loader_before)
        sandbox.restore_secrets_backend_list(self._secrets_backend_list_before)
        sandbox.restore_task_instance_mutation_hook_is_noop(self._is_noop_before)
        sandbox.listener_manager_restore(self._core_listener, self._core_listener_before)
        if self._task_listener is not None:
            sandbox.listener_manager_restore(self._task_listener, self._task_listener_before)
        sandbox.restore_policy_plugins(self._policy_pm, self._policy_plugins_before)
        sandbox.clear_plugins_manager_caches()
        sandbox.restore_sys_modules(self._sys_modules_before, self._plugins_folder)
        sandbox.restore_settings_keys(self._settings_keys_before)


@pytest.fixture
def airflow_components(pytestconfig: pytest.Config) -> Iterator[ComponentRegistry]:
    """Register a custom plugin, listener, policy, secrets backend, or executor.

    Every registration method runs `check_component` first and raises on any
    conformance problem -- see `pytest_airflow_in_a_box.types.ComponentRegistry` and the
    "Runtime component registration" section of the custom-components guide. Reverts
    every Airflow global registry it touched when the test finishes, regardless of
    which methods were called or whether the test raised.

    Parameters:
        pytestconfig: pytest.Config carrying the session's bootstrap state.

    Yields:
        ComponentRegistry registering components for the duration of one test.
    """

    message = v2_gate_message(
        "airflow_components",
        "it registers into the Task SDK's own plugin and listener managers, which 2.x "
        "predates entirely.",
    )
    if message is not None:
        pytest.fail(message, pytrace=False)
    state = get_bootstrap_state(pytestconfig)
    registry = _ComponentSandbox(state.plugins_folder)
    try:
        yield registry
    finally:
        registry.finalize()


__all__ = ("airflow_components",)
