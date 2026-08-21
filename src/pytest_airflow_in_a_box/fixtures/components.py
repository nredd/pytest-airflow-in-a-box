"""Register a custom Airflow component for one test, reverting it afterward.

See `pytest_airflow_in_a_box._compat.components` for the snapshot/restore mechanics and
the lazy cache-enumeration contract this fixture triggers on first use.

References:
    https://docs.pytest.org/en/stable/how-to/fixtures.html
    https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/plugins.html
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pytest

from pytest_airflow_in_a_box._compat import components as sandbox
from pytest_airflow_in_a_box._compat.capabilities import installed_certification, v2_gate_message
from pytest_airflow_in_a_box._compat.components import (
    KIND_CLASSIFIERS,
    LISTENER_CORE_MANAGER_ONLY,
    LISTENER_SDK_MANAGER_ONLY,
    _as_type,
)
from pytest_airflow_in_a_box.bootstrap import get_bootstrap_state
from pytest_airflow_in_a_box.components import ComponentKind, ComponentReport, check_component

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

    from pytest_airflow_in_a_box.types import ComponentRegistry

LOGGER = logging.getLogger(__name__)

# The kinds `round_trip()` can classify unambiguously: each takes a bare component
# positionally and nothing else structurally identifies it. `POLICY` is excluded on
# purpose -- it has no bare-component form, only `**hookname_to_callable` -- and so are
# the remaining static-check-only kinds (`XCOM`, `NOTIFIER`, `PROVIDER`), which
# `ComponentRegistry` never registers at all: an XCom backend is configured once for
# the whole run via the `airflow_xcom_backend` ini (#112), and a notifier/provider is
# referenced directly in Dag code, never through a process-global registry a sandbox
# would need to revert. `TIMETABLE` and `WEIGHT_STRATEGY` joined in #114: Dag
# serialization resolves both by qualname through the plugins manager, so both have a
# real registration for the sandbox to make and revert.
_ROUND_TRIP_KINDS = (
    ComponentKind.PLUGIN,
    ComponentKind.LISTENER,
    ComponentKind.EXECUTOR,
    ComponentKind.SECRETS_BACKEND,
    ComponentKind.TIMETABLE,
    ComponentKind.WEIGHT_STRATEGY,
)


def _scoped_listener_problems(
    report: ComponentReport, *, core: bool, task: bool
) -> ComponentReport:
    """Drop the manager-scope listener problems the requested registration scope moots.

    `check_component` reports everything wrong with a listener in the abstract,
    including `listener-core-manager-only` / `listener-sdk-manager-only` -- hookimpls
    whose hookspec exists on only one manager. Those are real registration problems
    ONLY when the one manager carrying the hookspec was not requested (the hookimpl
    could never fire); when that manager IS requested, a single-scope hookspec is the
    normal, supported case (`on_dag_run_success` exists on the core manager alone by
    design). Only the registration call knows the requested scope, so the filtering
    lives here rather than in the public, purely static `check_component`. The two
    scope-independent checks (`listener-no-matching-hookspec`,
    `listener-unknown-argument`) always pass through.

    Parameters:
        report: ComponentReport containing the unfiltered `check_component` outcome.
        core: bool mirroring `ComponentRegistry.listener`'s own flag.
        task: bool mirroring `ComponentRegistry.listener`'s own flag.

    Returns:
        ComponentReport containing only the problems relevant to the requested scope.
    """

    mooted: set[str] = set()
    if core:
        mooted.add(LISTENER_CORE_MANAGER_ONLY)
    if task:
        mooted.add(LISTENER_SDK_MANAGER_ONLY)
    return ComponentReport(
        component_name=report.component_name,
        problems=tuple(problem for problem in report.problems if problem.code not in mooted),
        certification=report.certification,
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
        self._macros_keys_before = sandbox.snapshot_macros_module_keys()
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

        report = check_component(component, kind=ComponentKind.LISTENER)
        _scoped_listener_problems(report, core=core, task=task).raise_for_problems()
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
        """See `pytest_airflow_in_a_box.types.ComponentRegistry.policy`.

        A registered `task_instance_mutation_hook` additionally flips
        `task_instance_mutation_hook.is_noop` to False -- without that, Airflow's own
        dispatch sites short-circuit on the flag and the hookimpl never fires (see
        `mark_task_instance_mutation_hook_active`). The construction-time is_noop
        snapshot already covers reverting it.
        """

        if not hooks:
            raise sandbox.ComponentSandboxError(
                "policy() requires at least one hookname=callable keyword argument."
            )
        plugin_class = sandbox.build_policy_plugin(hooks)
        check_component(plugin_class, kind=ComponentKind.POLICY).raise_for_problems()
        sandbox.register_policy(plugin_class, self._policy_pm)
        if "task_instance_mutation_hook" in hooks:
            sandbox.mark_task_instance_mutation_hook_active()

    def secrets_backend(self, component: object, *, first: bool = True) -> None:
        """See `pytest_airflow_in_a_box.types.ComponentRegistry.secrets_backend`."""

        check_component(component, kind=ComponentKind.SECRETS_BACKEND).raise_for_problems()
        sandbox.register_secrets_backend(component, first=first)

    def executor(self, component: object, *, alias: str = "test") -> str:
        """See `pytest_airflow_in_a_box.types.ComponentRegistry.executor`."""

        check_component(component, kind=ComponentKind.EXECUTOR).raise_for_problems()
        return sandbox.register_executor(component, alias=alias)

    def timetable(self, component: object) -> None:
        """See `pytest_airflow_in_a_box.types.ComponentRegistry.timetable`."""

        check_component(component, kind=ComponentKind.TIMETABLE).raise_for_problems()
        sandbox.register_timetable(component)

    def priority_weight_strategy(self, component: object) -> None:
        """See `pytest_airflow_in_a_box.types.ComponentRegistry.priority_weight_strategy`."""

        check_component(component, kind=ComponentKind.WEIGHT_STRATEGY).raise_for_problems()
        sandbox.register_weight_strategy(component)

    def serialization_round_trip(self, component: object) -> None:
        """See `pytest_airflow_in_a_box.types.ComponentRegistry.serialization_round_trip`."""

        if isinstance(component, type):
            raise sandbox.ComponentSandboxError(
                f"serialization_round_trip() needs a live `{component.__name__}` "
                f"instance -- encoding a timetable requires one; pass "
                f"`{component.__name__}(...)` instead of the class."
            )
        self.timetable(component)
        ComponentReport(
            component_name=type(component).__name__,
            problems=sandbox.timetable_round_trip(component),
            certification=installed_certification(),
        ).raise_for_problems()

    def round_trip(self, component: object) -> None:
        """See `pytest_airflow_in_a_box.types.ComponentRegistry.round_trip`."""

        matched = [kind for kind in _ROUND_TRIP_KINDS if KIND_CLASSIFIERS[kind.value](component)]
        if len(matched) != 1:
            raise sandbox.ComponentSandboxError(
                f"round_trip() could not classify `{_as_type(component).__name__}` as "
                f"exactly one of plugin/listener/executor/secrets_backend/timetable/"
                f"priority_weight_strategy (matched "
                f"{[kind.value for kind in matched]}); call the specific method instead."
            )
        kind = matched[0]
        if kind is ComponentKind.PLUGIN:
            self.plugin(component)
        elif kind is ComponentKind.LISTENER:
            # `task` follows the installed release's Task SDK availability rather than
            # `listener()`'s own default, so a 3.1.x install (no Task SDK listener
            # manager at all) round-trips core-only instead of raising.
            self.listener(component, task=self._task_listener is not None)
        elif kind is ComponentKind.EXECUTOR:
            self.executor(component)
        elif kind is ComponentKind.TIMETABLE:
            self.timetable(component)
        elif kind is ComponentKind.WEIGHT_STRATEGY:
            self.priority_weight_strategy(component)
        else:
            self.secrets_backend(component)

    def finalize(self) -> None:
        """Restore every snapshot, isolating the steps from each other's failures.

        Ordering, in three deliberate groups: first the registry restores (executor
        loader, secrets, is_noop, listeners, policy), in roughly reverse registration
        order; then `restore_sys_modules` BEFORE `clear_plugins_manager_caches`, so
        that if anything swapped a plugins-manager module object during the test the
        clear lands on the restored module the next test will actually import, not on
        a discarded replacement; then the attribute-key sweeps (macros parent,
        `airflow.settings`), which depend on nothing above. Plugins-manager caches are
        cleared rather than restored because there is no cache *value* to snapshot,
        only a clear operation -- see `clear_plugins_manager_caches`'s docstring.

        Every step runs even when an earlier one raises: each is guarded individually,
        the first failure is re-raised afterward with its original traceback, and any
        additional failures are logged -- a raise partway through must never skip the
        remaining restores, since a partial restore is exactly the cross-test
        contamination this sandbox exists to prevent.

        Raises:
            Exception: The first failure any restore step raised, re-raised as-is
                after every remaining step has run.
        """

        steps: list[tuple[str, Callable[[], None]]] = [
            (
                "restore_executor_loader",
                lambda: sandbox.restore_executor_loader(self._executor_loader_before),
            ),
            (
                "restore_secrets_backend_list",
                lambda: sandbox.restore_secrets_backend_list(self._secrets_backend_list_before),
            ),
            (
                "restore_task_instance_mutation_hook_is_noop",
                lambda: sandbox.restore_task_instance_mutation_hook_is_noop(self._is_noop_before),
            ),
            (
                "listener_manager_restore (core)",
                lambda: sandbox.listener_manager_restore(
                    self._core_listener, self._core_listener_before
                ),
            ),
        ]
        if self._task_listener is not None:
            steps.append(
                (
                    "listener_manager_restore (task)",
                    lambda: sandbox.listener_manager_restore(
                        self._task_listener, self._task_listener_before
                    ),
                )
            )
        steps.extend(
            (
                (
                    "restore_policy_plugins",
                    lambda: sandbox.restore_policy_plugins(
                        self._policy_pm, self._policy_plugins_before
                    ),
                ),
                (
                    "restore_sys_modules",
                    lambda: sandbox.restore_sys_modules(
                        self._sys_modules_before, self._plugins_folder
                    ),
                ),
                ("clear_plugins_manager_caches", sandbox.clear_plugins_manager_caches),
                (
                    "restore_macros_module_keys",
                    lambda: sandbox.restore_macros_module_keys(self._macros_keys_before),
                ),
                (
                    "restore_settings_keys",
                    lambda: sandbox.restore_settings_keys(self._settings_keys_before),
                ),
            )
        )
        failures: list[tuple[str, Exception]] = []
        for step_name, step in steps:
            try:
                step()
            except Exception as error:
                failures.append((step_name, error))
        if not failures:
            return
        for step_name, error in failures[1:]:
            LOGGER.error(
                f"component sandbox restore step `{step_name}` also failed "
                f"(suppressed in favor of the first failure): {error!r}"
            )
        raise failures[0][1]


# The one conformance problem that makes transparent registration FUTILE rather than
# merely suspect: a `<locals>` class can never resolve by qualname, so registering it
# only defers the failure to a raw `TimetableNotRegistered` deep inside persist.
_SCHEDULE_REGISTRATION_BLOCKING = (sandbox.TIMETABLE_LOCAL_QUALNAME,)


def register_schedule_timetable(timetable: object) -> None:
    """Register a `dag_maker` schedule timetable behind a registration-scoped gate.

    Deliberately more lenient than `ComponentRegistry.timetable`'s full conformance
    gate: the transparent path's job is to make Airflow's own serialization succeed,
    not to enforce conformance on a Dag the user merely scheduled -- and the full gate
    is stricter than Airflow itself (`timetable-serialize-pair-incomplete` hard-fails
    a stateless serialize-only timetable that upstream's default `deserialize`, a bare
    `return cls()`, handles fine). Only the problem that makes registration futile
    raises (`timetable-local-qualname`); everything else is logged as a warning so the
    signal survives without turning a previously-working Dag into a hard failure. The
    explicit `airflow_components.timetable()` call keeps the full gate.

    Parameters:
        timetable: object containing the custom timetable instance to register.

    Raises:
        ComponentContractError: The class can never resolve by qualname
            (`timetable-local-qualname`).
    """

    report = check_component(timetable, kind=ComponentKind.TIMETABLE)
    for problem in report.problems:
        if problem.code not in _SCHEDULE_REGISTRATION_BLOCKING:
            LOGGER.warning(
                f"`{report.component_name}` [{problem.code}]: {problem.message} {problem.hint}"
            )
    ComponentReport(
        component_name=report.component_name,
        problems=tuple(
            problem
            for problem in report.problems
            if problem.code in _SCHEDULE_REGISTRATION_BLOCKING
        ),
        certification=report.certification,
    ).raise_for_problems()
    sandbox.register_timetable(timetable)


@pytest.fixture
def airflow_components(pytestconfig: pytest.Config) -> Iterator[ComponentRegistry]:
    """Register a custom plugin, listener, policy, secrets backend, executor, or timetable.

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


__all__ = ("airflow_components", "register_schedule_timetable")
