"""Test the `airflow_components` registration sandbox fixture.

`pytest_airflow_in_a_box._compat.components` (see `tests/compat/test_component_sandbox_compat.py`)
already exercises every snapshot/restore function's own mechanics exhaustively. This
file's job is different: prove the FIXTURE wires `check_component` gating and those
mechanics together correctly, and that a component registered through it is really live
and really reverted -- the "Done" bullet issue #113 states outright.

References:
    https://docs.pytest.org/en/stable/how-to/fixtures.html
    https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/plugins.html
"""

from __future__ import annotations

import logging
import types
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pytest

# `airflow.plugins_manager`, not `airflow._shared.plugins_manager`: the `_shared`
# vendoring path is 3.2+-only, while the core module re-exports `AirflowPlugin` on
# every certified 3.x release.
from airflow.executors.base_executor import BaseExecutor
from airflow.listeners import hookimpl
from airflow.plugins_manager import AirflowPlugin
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.secrets.base_secrets import BaseSecretsBackend
from airflow.timetables.base import Timetable

from pytest_airflow_in_a_box._compat import components as sandbox
from pytest_airflow_in_a_box._compat.capabilities import resolve_capabilities
from pytest_airflow_in_a_box.components import ComponentContractError, ComponentSandboxError
from pytest_airflow_in_a_box.fixtures import components as fixture_components

# Skipping (not faking) on 3.1.x legs is the honest choice for the tests below that
# genuinely need the real Task SDK listener manager; the 3.1 dispatch branches
# themselves are covered by the seam-faked tests in
# `tests/compat/test_component_sandbox_compat.py` and by
# `test_listener_task_true_without_a_task_manager_raises` /
# `test_round_trip_listener_registers_core_only_when_no_task_manager` here.
_NEEDS_TASK_LISTENER_MANAGER = pytest.mark.skipif(
    not resolve_capabilities().sdk_listener_manager_available,
    reason="the Task SDK listener manager exists only on 3.2+",
)

if TYPE_CHECKING:
    from pytest_airflow_in_a_box.types import ComponentRegistry, DagMaker

# `provider_package` (in this corpus) supplies `ExampleExecutor`, an already-conformant,
# module-level `BaseExecutor` -- `executor()` requires a real importable module path, so
# a corpus class is the correct fit here rather than a fresh local one; see
# `tests/enduser/test_provider_package.py`'s identical `monkeypatch.syspath_prepend`
# idiom, which this mirrors.
CORPUS = Path(__file__).parents[1] / "dags"


class _Listener:
    """Record task-instance success notifications for one test, no shared state."""

    def __init__(self) -> None:
        """Start with an empty call log."""

        self.calls: list[str] = []

    @hookimpl
    def on_task_instance_success(self, previous_state: Any, task_instance: Any) -> None:
        """Record the succeeding task's id.

        Parameters:
            previous_state: Any containing the task instance's prior state.
            task_instance: Any containing the succeeding task instance.
        """

        del previous_state
        self.calls.append(task_instance.task_id)


class _BrokenListener:
    """Carry a hookimpl name that matches no real hookspec, on either manager."""

    @hookimpl
    def not_a_real_hookspec(self) -> None:
        """Never fire; no hookspec named `not_a_real_hookspec` exists."""


class _DagRunListener:
    """Implement a hookspec that exists on the CORE manager alone, by upstream design."""

    @hookimpl
    def on_dag_run_success(self, dag_run: Any, msg: str) -> None:
        """Never actually fire; only registration acceptance is under test.

        Parameters:
            dag_run: Any containing the succeeding Dag run.
            msg: str containing upstream's success message.
        """

        del dag_run, msg


class _Plugin(AirflowPlugin):
    """Minimal conformant plugin: only `name` is required."""

    name = "airflow_components_test_plugin"


class _SecretsBackend(BaseSecretsBackend):
    """Serve one fixed Variable value, conformant per `_check_secrets_backend_raises_on_miss`."""

    def get_variable(self, key: str, team_name: str | None = None) -> str | None:
        """Look the requested key up in a fixed mapping.

        Parameters:
            key: str naming the requested Variable.
            team_name: str | None naming the requesting team, unused by this fixture.

        Returns:
            str | None containing the value, or None on a miss.
        """

        del team_name
        return {"airflow_components_test_answer": "42"}.get(key)


class _AmbiguousExecutorListener(BaseExecutor):
    """Match both the executor and listener classifiers at once, on purpose.

    `round_trip()` must refuse a component matching more than one registrable kind
    rather than guessing; this class exists only to prove that refusal fires.
    """

    def sync(self) -> None:
        """Report nothing; never actually run."""

    def _process_workloads(self, workload_items: Any) -> None:
        """Accept a workload batch and do nothing with it.

        Parameters:
            workload_items: Any containing the queued workload batch.
        """

        del workload_items

    @hookimpl
    def on_task_instance_success(self, previous_state: Any, task_instance: Any) -> None:
        """Never actually fire; this class is never registered as a listener.

        Parameters:
            previous_state: Any containing the task instance's prior state.
            task_instance: Any containing the succeeding task instance.
        """

        del previous_state, task_instance


# ---------------------------------------------------------------------------
# plugin()
# ---------------------------------------------------------------------------


def test_plugin_registers_into_the_core_plugins_manager(
    airflow_components: ComponentRegistry,
) -> None:
    """Register a conformant plugin and observe it land in the live plugin list."""

    airflow_components.plugin(_Plugin)

    core_module, _sdk_module = sandbox._plugins_manager_modules()
    names = {type(candidate).__name__ for candidate in sandbox._live_plugin_list(core_module)}
    assert "_Plugin" in names


def test_plugin_rejects_a_component_missing_a_name(
    airflow_components: ComponentRegistry,
) -> None:
    """Refuse an unconformant plugin before ever touching the live registry."""

    class _Unnamed(AirflowPlugin):
        pass

    with pytest.raises(ComponentContractError, match="plugin-name-missing"):
        airflow_components.plugin(_Unnamed)

    core_module, _sdk_module = sandbox._plugins_manager_modules()
    names = {type(candidate).__name__ for candidate in sandbox._live_plugin_list(core_module)}
    assert "_Unnamed" not in names


def test_plugin_accepts_a_module_valued_listener(
    airflow_components: ComponentRegistry,
) -> None:
    """Register a plugin whose `listeners` holds a bare module, the canonical shape.

    Airflow's own shipped `example_dags/plugins/listener_plugin.py` uses exactly this
    shape; the sandbox must register the module object itself, not a copy.
    """

    listener_module = types.ModuleType("airflow_components_module_listener")

    class _ModuleListenerPlugin(AirflowPlugin):
        name = "airflow_components_module_listener_plugin"

        def __init__(self) -> None:
            """Set `listeners` to a bare module, the canonical upstream example shape."""

            super().__init__()
            self.listeners = [listener_module]

    airflow_components.plugin(_ModuleListenerPlugin)

    core_module, _sdk_module = sandbox._plugins_manager_modules()
    registered = next(
        candidate
        for candidate in sandbox._live_plugin_list(core_module)
        if type(candidate).__name__ == "_ModuleListenerPlugin"
    )
    assert registered.listeners[0] is listener_module


# ---------------------------------------------------------------------------
# listener()
# ---------------------------------------------------------------------------


@_NEEDS_TASK_LISTENER_MANAGER
def test_listener_default_registers_with_both_managers(
    airflow_components: ComponentRegistry,
) -> None:
    """Register with both managers by default."""

    listener = _Listener()

    airflow_components.listener(listener)

    core_manager, task_manager = sandbox.listener_managers()
    assert listener in core_manager.pm.get_plugins()
    assert task_manager is not None
    assert listener in task_manager.pm.get_plugins()


def test_listener_core_only(airflow_components: ComponentRegistry) -> None:
    """Register with only the core manager when `task=False`."""

    listener = _Listener()

    airflow_components.listener(listener, task=False)

    core_manager, task_manager = sandbox.listener_managers()
    assert listener in core_manager.pm.get_plugins()
    if task_manager is not None:
        assert listener not in task_manager.pm.get_plugins()


@_NEEDS_TASK_LISTENER_MANAGER
def test_listener_task_only(airflow_components: ComponentRegistry) -> None:
    """Register with only the Task SDK manager when `core=False`."""

    listener = _Listener()

    airflow_components.listener(listener, core=False)

    core_manager, task_manager = sandbox.listener_managers()
    assert listener not in core_manager.pm.get_plugins()
    assert task_manager is not None
    assert listener in task_manager.pm.get_plugins()


def test_listener_requires_at_least_one_manager(
    airflow_components: ComponentRegistry,
) -> None:
    """Refuse `core=False, task=False`: nothing would ever be registered."""

    with pytest.raises(ComponentSandboxError, match="core=True, task=True, or both"):
        airflow_components.listener(_Listener(), core=False, task=False)


def test_listener_task_true_without_a_task_manager_raises(
    airflow_components: ComponentRegistry,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refuse `task=True` when the sandbox resolved no Task SDK listener manager.

    Simulates the real 3.1.x shape directly on the constructed sandbox instance,
    rather than faking the whole `listener_managers()` seam, since only this one
    attribute matters to `listener()`'s own dispatch. Leaving it patched for the rest
    of the test also exercises `_ComponentSandbox.finalize()`'s `self._task_listener is
    not None` branch as False at teardown -- nothing was ever registered with the real
    Task SDK manager in this test, so skipping its restore is correct, not a leak.
    """

    monkeypatch.setattr(airflow_components, "_task_listener", None)

    with pytest.raises(ComponentSandboxError, match="does not exist"):
        airflow_components.listener(_Listener(), task=True)


def test_listener_core_only_accepts_a_core_only_hookspec_listener(
    airflow_components: ComponentRegistry,
) -> None:
    """Accept a core-only hookspec listener when only the core manager is requested.

    `on_dag_run_success` exists on the core manager alone by upstream design;
    `check_component`'s `listener-core-manager-only` finding is mooted when the core
    manager is the one being registered with, so `task=False` must succeed.
    """

    listener = _DagRunListener()

    airflow_components.listener(listener, task=False)

    core_manager, _task_manager = sandbox.listener_managers()
    assert listener in core_manager.pm.get_plugins()


@_NEEDS_TASK_LISTENER_MANAGER
def test_listener_task_only_still_rejects_a_core_only_hookspec_listener(
    airflow_components: ComponentRegistry,
) -> None:
    """Refuse a core-only hookspec listener when only the Task SDK manager is requested.

    Registered task-only, `on_dag_run_success` could never fire -- the scope filter
    keeps `listener-core-manager-only` for exactly this case. Needs the real Task SDK
    manager: on 3.1.x there is no second manager for the scope checks to compare
    against, so `check_component` reports no manager-scope findings there at all.
    """

    with pytest.raises(ComponentContractError, match="listener-core-manager-only"):
        airflow_components.listener(_DagRunListener(), core=False, task=True)


def test_scoped_listener_problems_filters_by_requested_scope() -> None:
    """Filter each manager-scope problem code by its own requested-scope flag alone."""

    from pytest_airflow_in_a_box.components import ComponentReport
    from pytest_airflow_in_a_box.fixtures.components import _scoped_listener_problems

    problems = (
        sandbox.ComponentProblem(
            code=sandbox.LISTENER_CORE_MANAGER_ONLY, message="core-only", hint="h"
        ),
        sandbox.ComponentProblem(
            code=sandbox.LISTENER_SDK_MANAGER_ONLY, message="sdk-only", hint="h"
        ),
        sandbox.ComponentProblem(
            code=sandbox.LISTENER_NO_MATCHING_HOOKSPEC, message="unmatched", hint="h"
        ),
    )
    report = ComponentReport(component_name="_Probe", problems=problems)

    both = _scoped_listener_problems(report, core=True, task=True)
    assert {problem.code for problem in both.problems} == {sandbox.LISTENER_NO_MATCHING_HOOKSPEC}

    core_only = _scoped_listener_problems(report, core=True, task=False)
    assert {problem.code for problem in core_only.problems} == {
        sandbox.LISTENER_SDK_MANAGER_ONLY,
        sandbox.LISTENER_NO_MATCHING_HOOKSPEC,
    }

    task_only = _scoped_listener_problems(report, core=False, task=True)
    assert {problem.code for problem in task_only.problems} == {
        sandbox.LISTENER_CORE_MANAGER_ONLY,
        sandbox.LISTENER_NO_MATCHING_HOOKSPEC,
    }

    neither = _scoped_listener_problems(report, core=False, task=False)
    assert neither.problems == problems
    assert neither.component_name == "_Probe"


def test_listener_rejects_an_unmatched_hookimpl(
    airflow_components: ComponentRegistry,
) -> None:
    """Refuse a listener whose hookimpl name matches no real hookspec."""

    with pytest.raises(ComponentContractError, match="listener-no-matching-hookspec"):
        airflow_components.listener(_BrokenListener())


def test_listener_fires_through_a_real_dag_run(
    airflow_components: ComponentRegistry,
    dag_maker: DagMaker,
) -> None:
    """Register a listener and see it actually fire during a real task run.

    Mirrors the `docs/guide/custom-components.md` example directly, so the shipped
    docs stay honest about the shape that actually works.
    """

    listener = _Listener()
    # `task` follows the installed release's Task SDK availability so this test runs
    # on the 3.1.x legs too -- the docs' bare `listener(MyListener)` shape is exercised
    # by `test_listener_default_registers_with_both_managers` on 3.2+.
    airflow_components.listener(listener, task=sandbox.listener_managers()[1] is not None)

    with dag_maker(dag_id="airflow_components_listener_fires"):
        EmptyOperator(task_id="probe")

    result = dag_maker.run()

    assert result.success
    assert listener.calls == ["probe"]


# ---------------------------------------------------------------------------
# policy()
# ---------------------------------------------------------------------------


def test_policy_registers_and_fires(airflow_components: ComponentRegistry) -> None:
    """Build and register a policy plugin, then observe its hook actually fire."""

    received: dict[str, str] = {}

    def _get_dagbag_import_timeout(dag_file_path: str) -> int | float:
        received["dag_file_path"] = dag_file_path
        return 30

    airflow_components.policy(get_dagbag_import_timeout=_get_dagbag_import_timeout)

    pm = sandbox.policy_plugin_manager()
    # `get_dagbag_import_timeout` is `firstresult=True`; pluggy returns the single
    # result directly rather than a per-implementation list.
    result = pm.hook.get_dagbag_import_timeout(dag_file_path="a_dag.py")

    assert result == 30
    assert received == {"dag_file_path": "a_dag.py"}


def test_policy_requires_at_least_one_hook(airflow_components: ComponentRegistry) -> None:
    """Refuse an empty `policy()` call: there would be nothing to register."""

    with pytest.raises(ComponentSandboxError, match="at least one"):
        airflow_components.policy()


def test_policy_rejects_an_unknown_hookspec_name(
    airflow_components: ComponentRegistry,
) -> None:
    """Refuse a hook keyed by a name `airflow.policies` does not declare."""

    with pytest.raises(ComponentContractError, match="policy-unknown-hookspec"):
        airflow_components.policy(not_a_real_policy_hook=lambda: None)


def test_policy_task_instance_mutation_hook_fires_through_a_real_dag_run(
    airflow_components: ComponentRegistry,
    dag_maker: DagMaker,
) -> None:
    """Register a mutation-hook policy and see it fire through the real `is_noop` gate.

    The load-bearing half of this test is the flag: `airflow/settings.py` sets
    `task_instance_mutation_hook.is_noop = True` at import, and real dispatch sites
    (`DagRun.verify_integrity` among them) short-circuit on it -- so without
    `policy()`'s own activation step the hookimpl registers but never fires, and only
    a real Dag run (not a direct `pm.hook.<name>(...)` call, which bypasses the gate)
    can prove the difference.
    """

    received: list[str] = []

    # `task_instance` alone, not the 3.3.0 hookspec's full `(task_instance, dag_run)`:
    # pluggy accepts a hookimpl requesting any subset of its hookspec's arguments, and
    # the 3.1.x/3.2.x hookspec has no `dag_run` parameter at all, so the subset form is
    # the one signature valid on every certified release.
    def _mutate(task_instance: Any) -> None:
        received.append(task_instance.task_id)

    airflow_components.policy(task_instance_mutation_hook=_mutate)

    assert sandbox.snapshot_task_instance_mutation_hook_is_noop() is False
    with dag_maker(dag_id="airflow_components_policy_mutation_hook"):
        EmptyOperator(task_id="probe")

    result = dag_maker.run()

    assert result.success
    assert "probe" in received


def test_policy_without_a_mutation_hook_keeps_is_noop_untouched(
    airflow_components: ComponentRegistry,
) -> None:
    """Leave `is_noop` alone when the registered hooks do not include the mutation hook."""

    before = sandbox.snapshot_task_instance_mutation_hook_is_noop()

    def _get_dagbag_import_timeout(dag_file_path: str) -> int:
        del dag_file_path
        return 30

    airflow_components.policy(get_dagbag_import_timeout=_get_dagbag_import_timeout)

    assert sandbox.snapshot_task_instance_mutation_hook_is_noop() is before


# ---------------------------------------------------------------------------
# secrets_backend()
# ---------------------------------------------------------------------------


def test_secrets_backend_first_true_inserts_at_front(
    airflow_components: ComponentRegistry,
) -> None:
    """Insert at the front of the search path by default."""

    airflow_components.secrets_backend(_SecretsBackend)

    current = sandbox.snapshot_secrets_backend_list()
    assert isinstance(current[0], _SecretsBackend)


def test_secrets_backend_first_false_appends_at_back(
    airflow_components: ComponentRegistry,
) -> None:
    """Append at the back of the search path when `first=False`."""

    airflow_components.secrets_backend(_SecretsBackend, first=False)

    current = sandbox.snapshot_secrets_backend_list()
    assert isinstance(current[-1], _SecretsBackend)


# ---------------------------------------------------------------------------
# executor()
# ---------------------------------------------------------------------------


def test_executor_registers_and_resolves_by_alias(
    airflow_components: ComponentRegistry,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Register a real, importable executor class and resolve it by its alias."""

    monkeypatch.syspath_prepend(str(CORPUS))
    # `import_module` on purpose, not a static `from provider_package import ...`:
    # `provider_package` resolves only after `monkeypatch.syspath_prepend` above, a
    # runtime-only fact `ty` cannot see -- matching `_shared_module_loading_modules`'s
    # own dynamic-import precedent for the same "not statically resolvable" shape.
    ExampleExecutor = import_module("provider_package").ExampleExecutor

    returned = airflow_components.executor(ExampleExecutor, alias="airflow_components_test_exec")

    assert returned == "airflow_components_test_exec"
    from airflow.executors.executor_loader import ExecutorLoader

    loaded = ExecutorLoader.load_executor(returned)
    assert isinstance(loaded, ExampleExecutor)


def test_executor_defaults_the_alias_to_test(
    airflow_components: ComponentRegistry,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Default `alias` to `"test"` when the caller does not name one."""

    monkeypatch.syspath_prepend(str(CORPUS))
    # `import_module` on purpose, not a static `from provider_package import ...`:
    # `provider_package` resolves only after `monkeypatch.syspath_prepend` above, a
    # runtime-only fact `ty` cannot see -- matching `_shared_module_loading_modules`'s
    # own dynamic-import precedent for the same "not statically resolvable" shape.
    ExampleExecutor = import_module("provider_package").ExampleExecutor

    returned = airflow_components.executor(ExampleExecutor)

    assert returned == "test"


class _AsymmetricTimetable(Timetable):
    """Drop state in `deserialize`, so the serialization round trip must be flagged."""

    def __init__(self, hours: int = 1) -> None:
        """Carry the one piece of state `deserialize` deliberately drops.

        Parameters:
            hours: int containing the interval length `serialize` emits.
        """

        self.hours = hours

    def infer_manual_data_interval(self, *, run_after: Any) -> Any:
        """Never actually fire; only the conformance gate reads this.

        Parameters:
            run_after: Any containing the manual trigger moment.

        Returns:
            Any; unreachable in these tests.
        """

        raise NotImplementedError

    def next_dagrun_info(self, *, last_automated_data_interval: Any, restriction: Any) -> Any:
        """Never actually fire; only the conformance gate reads this.

        Parameters:
            last_automated_data_interval: Any containing the prior automated interval.
            restriction: Any containing the schedule's time restriction.

        Returns:
            Any; unreachable in these tests.
        """

        del last_automated_data_interval, restriction
        return None

    def serialize(self) -> dict[str, Any]:
        """Emit the state `deserialize` will drop.

        Returns:
            dict[str, Any] containing the interval length.
        """

        return {"hours": self.hours}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> _AsymmetricTimetable:
        """Reconstruct with the default interval, discarding the payload.

        Parameters:
            data: dict[str, Any] containing the ignored serialized payload.

        Returns:
            _AsymmetricTimetable reconstructed with default state.
        """

        del data
        return cls()


# ---------------------------------------------------------------------------
# timetable()
# ---------------------------------------------------------------------------


def test_timetable_registers_and_survives_the_serialization_round_trip(
    airflow_components: ComponentRegistry,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Register a corpus timetable and watch Airflow's own encode/decode resolve it."""

    monkeypatch.syspath_prepend(str(CORPUS))
    # `import_module` on purpose, not a static `from provider_package import ...`:
    # `provider_package` resolves only after `monkeypatch.syspath_prepend` above, a
    # runtime-only fact `ty` cannot see -- matching `_shared_module_loading_modules`'s
    # own dynamic-import precedent for the same "not statically resolvable" shape.
    ExampleTimetable = import_module("provider_package").ExampleTimetable

    airflow_components.timetable(ExampleTimetable)

    from airflow.serialization.serialized_objects import decode_timetable, encode_timetable

    decoded = cast("Any", decode_timetable(encode_timetable(ExampleTimetable(hours=2))))
    assert type(decoded) is ExampleTimetable
    assert decoded.hours == 2


def test_timetable_rejects_a_local_scope_class(
    airflow_components: ComponentRegistry,
) -> None:
    """Refuse a timetable class Airflow could never resolve by dotted import path."""

    class _LocalTimetable(Timetable):
        """Fail `timetable-local-qualname` by construction."""

    with pytest.raises(ComponentContractError, match="timetable-local-qualname"):
        airflow_components.timetable(_LocalTimetable)


# ---------------------------------------------------------------------------
# priority_weight_strategy()
# ---------------------------------------------------------------------------


def test_priority_weight_strategy_registers_into_the_live_plugin_list(
    airflow_components: ComponentRegistry,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Register a corpus weight strategy through a synthesized throwaway plugin."""

    monkeypatch.syspath_prepend(str(CORPUS))
    # `import_module` on purpose, not a static `from provider_package import ...`:
    # `provider_package._weight_strategy` resolves only after the syspath prepend, and
    # is deliberately NOT re-exported from the package `__init__`:
    # `airflow.task.priority_strategy` is 3.x-only, and the package must stay
    # importable on the 2.x family.
    weight_strategy_module = import_module("provider_package._weight_strategy")
    ExampleWeightStrategy = weight_strategy_module.ExampleWeightStrategy

    airflow_components.priority_weight_strategy(ExampleWeightStrategy)

    core_module, _sdk_module = sandbox._plugins_manager_modules()
    registered = [
        strategy
        for candidate in sandbox._live_plugin_list(core_module)
        for strategy in getattr(candidate, "priority_weight_strategies", [])
    ]
    assert ExampleWeightStrategy in registered


def test_priority_weight_strategy_rejects_a_broken_hash(
    airflow_components: ComponentRegistry,
) -> None:
    """Refuse a strategy whose effective `__hash__` is the base's unusable one."""

    from airflow.task.priority_strategy import PriorityWeightStrategy

    class _NoHashStrategy(PriorityWeightStrategy):
        """Fail `weight-strategy-hash-of-none`: `get_weight` without a real `__hash__`."""

        def get_weight(self, ti: Any) -> int:
            del ti
            return 1

    with pytest.raises(ComponentContractError, match="weight-strategy-hash-of-none"):
        airflow_components.priority_weight_strategy(_NoHashStrategy)


# ---------------------------------------------------------------------------
# serialization_round_trip()
# ---------------------------------------------------------------------------


def test_serialization_round_trip_accepts_a_symmetric_corpus_timetable(
    airflow_components: ComponentRegistry,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Register and round-trip a conformant timetable in one call."""

    monkeypatch.syspath_prepend(str(CORPUS))
    # `import_module` on purpose, not a static `from provider_package import ...`:
    # `provider_package` resolves only after `monkeypatch.syspath_prepend` above, a
    # runtime-only fact `ty` cannot see -- matching `_shared_module_loading_modules`'s
    # own dynamic-import precedent for the same "not statically resolvable" shape.
    ExampleTimetable = import_module("provider_package").ExampleTimetable

    airflow_components.serialization_round_trip(ExampleTimetable(hours=3))


def test_serialization_round_trip_rejects_a_class(
    airflow_components: ComponentRegistry,
) -> None:
    """Refuse a bare class; encoding a timetable requires a live instance."""

    with pytest.raises(ComponentSandboxError, match="needs a live"):
        airflow_components.serialization_round_trip(_AsymmetricTimetable)


def test_serialization_round_trip_flags_an_asymmetric_pair(
    airflow_components: ComponentRegistry,
) -> None:
    """Surface a `deserialize` that drops state as a loud contract failure."""

    with pytest.raises(ComponentContractError, match="timetable-round-trip-mismatch"):
        airflow_components.serialization_round_trip(_AsymmetricTimetable(hours=7))


# ---------------------------------------------------------------------------
# round_trip()
# ---------------------------------------------------------------------------


def test_round_trip_classifies_and_registers_a_timetable(
    airflow_components: ComponentRegistry,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Classify a `Timetable` instance as a timetable and register its class."""

    monkeypatch.syspath_prepend(str(CORPUS))
    # `import_module` on purpose, not a static `from provider_package import ...`:
    # `provider_package` resolves only after `monkeypatch.syspath_prepend` above, a
    # runtime-only fact `ty` cannot see -- matching `_shared_module_loading_modules`'s
    # own dynamic-import precedent for the same "not statically resolvable" shape.
    ExampleTimetable = import_module("provider_package").ExampleTimetable

    airflow_components.round_trip(ExampleTimetable(hours=4))

    from airflow.serialization.serialized_objects import decode_timetable, encode_timetable

    decoded = decode_timetable(encode_timetable(ExampleTimetable(hours=4)))
    assert type(decoded) is ExampleTimetable


def test_round_trip_classifies_and_registers_a_weight_strategy(
    airflow_components: ComponentRegistry,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Classify a `PriorityWeightStrategy` subclass and register it."""

    monkeypatch.syspath_prepend(str(CORPUS))
    # `import_module` on purpose; see
    # `test_priority_weight_strategy_registers_into_the_live_plugin_list` for why the
    # module is reached directly rather than through the package `__init__`.
    weight_strategy_module = import_module("provider_package._weight_strategy")
    ExampleWeightStrategy = weight_strategy_module.ExampleWeightStrategy

    airflow_components.round_trip(ExampleWeightStrategy)

    core_module, _sdk_module = sandbox._plugins_manager_modules()
    registered = [
        strategy
        for candidate in sandbox._live_plugin_list(core_module)
        for strategy in getattr(candidate, "priority_weight_strategies", [])
    ]
    assert ExampleWeightStrategy in registered


def test_round_trip_classifies_and_registers_a_plugin(
    airflow_components: ComponentRegistry,
) -> None:
    """Classify a bare `AirflowPlugin` subclass as a plugin and register it."""

    airflow_components.round_trip(_Plugin)

    core_module, _sdk_module = sandbox._plugins_manager_modules()
    names = {type(candidate).__name__ for candidate in sandbox._live_plugin_list(core_module)}
    assert "_Plugin" in names


def test_round_trip_classifies_and_registers_a_listener(
    airflow_components: ComponentRegistry,
) -> None:
    """Classify a hookimpl-carrying instance as a listener and register it."""

    listener = _Listener()

    airflow_components.round_trip(listener)

    core_manager, _task_manager = sandbox.listener_managers()
    assert listener in core_manager.pm.get_plugins()


def test_round_trip_classifies_and_registers_an_executor(
    airflow_components: ComponentRegistry,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Classify a `BaseExecutor` subclass as an executor and register it."""

    monkeypatch.syspath_prepend(str(CORPUS))
    # `import_module` on purpose, not a static `from provider_package import ...`:
    # `provider_package` resolves only after `monkeypatch.syspath_prepend` above, a
    # runtime-only fact `ty` cannot see -- matching `_shared_module_loading_modules`'s
    # own dynamic-import precedent for the same "not statically resolvable" shape.
    ExampleExecutor = import_module("provider_package").ExampleExecutor

    airflow_components.round_trip(ExampleExecutor)

    from airflow.executors.executor_loader import ExecutorLoader

    assert ExecutorLoader.executors["test"] == f"{ExampleExecutor.__module__}.ExampleExecutor"


def test_round_trip_classifies_and_registers_a_secrets_backend(
    airflow_components: ComponentRegistry,
) -> None:
    """Classify a `BaseSecretsBackend` subclass as a secrets backend and register it."""

    airflow_components.round_trip(_SecretsBackend)

    current = sandbox.snapshot_secrets_backend_list()
    assert isinstance(current[0], _SecretsBackend)


def test_round_trip_listener_registers_core_only_when_no_task_manager(
    airflow_components: ComponentRegistry,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Register a round-tripped listener core-only when no Task SDK manager exists.

    On 3.1.x `round_trip()` must not inherit `listener()`'s `task=True` default -- it
    follows the installed release's Task SDK availability instead, so a listener
    round-trips core-only rather than raising. Simulated on the constructed sandbox
    instance, matching `test_listener_task_true_without_a_task_manager_raises`.
    """

    monkeypatch.setattr(airflow_components, "_task_listener", None)
    listener = _Listener()

    airflow_components.round_trip(listener)

    core_manager, _task_manager = sandbox.listener_managers()
    assert listener in core_manager.pm.get_plugins()


def test_round_trip_rejects_an_unclassifiable_component(
    airflow_components: ComponentRegistry,
) -> None:
    """Refuse a bare object matching none of the registrable kinds."""

    with pytest.raises(ComponentSandboxError, match="could not classify"):
        airflow_components.round_trip(object())


def test_round_trip_rejects_an_ambiguous_component(
    airflow_components: ComponentRegistry,
) -> None:
    """Refuse a component matching more than one registrable kind at once."""

    with pytest.raises(ComponentSandboxError, match="could not classify"):
        airflow_components.round_trip(_AmbiguousExecutorListener)


# ---------------------------------------------------------------------------
# finalize() error isolation
# ---------------------------------------------------------------------------


def test_finalize_runs_every_restore_step_when_one_raises(
    airflow_components: ComponentRegistry,
) -> None:
    """Run every later restore step, then re-raise, when an early one fails.

    A raise partway through must never skip the remaining restores -- a partial
    restore is exactly the cross-test contamination the sandbox exists to prevent.
    A scoped `MonkeyPatch.context()` rather than the `monkeypatch` fixture on purpose:
    the fixture's undo runs AFTER `airflow_components`'s own teardown `finalize()`,
    which would re-raise the injected failure a second time from teardown; the context
    manager unpatches before this test returns, so the teardown pass runs clean.
    """

    later_steps: list[str] = []
    real_restore_settings_keys = sandbox.restore_settings_keys

    def _boom(snapshot: Any) -> None:
        del snapshot
        raise RuntimeError("deliberate executor restore failure")

    def _recording_restore_settings_keys(before: Any) -> None:
        later_steps.append("restore_settings_keys")
        real_restore_settings_keys(before)

    with pytest.MonkeyPatch.context() as patcher:
        patcher.setattr(sandbox, "restore_executor_loader", _boom)
        patcher.setattr(sandbox, "restore_settings_keys", _recording_restore_settings_keys)

        with pytest.raises(RuntimeError, match="deliberate executor restore failure"):
            cast("Any", airflow_components).finalize()

    assert later_steps == ["restore_settings_keys"]


def test_finalize_logs_additional_failures_beyond_the_first(
    airflow_components: ComponentRegistry,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Re-raise the FIRST failure and log each later one rather than swallowing it.

    Scoped `MonkeyPatch.context()` for the same teardown-ordering reason
    `test_finalize_runs_every_restore_step_when_one_raises` documents.
    """

    def _boom_executor(snapshot: Any) -> None:
        del snapshot
        raise RuntimeError("first failure")

    def _boom_secrets(before: Any) -> None:
        del before
        raise ValueError("second failure")

    with pytest.MonkeyPatch.context() as patcher:
        patcher.setattr(sandbox, "restore_executor_loader", _boom_executor)
        patcher.setattr(sandbox, "restore_secrets_backend_list", _boom_secrets)

        with (
            caplog.at_level(logging.ERROR, logger="pytest_airflow_in_a_box.fixtures.components"),
            pytest.raises(RuntimeError, match="first failure"),
        ):
            cast("Any", airflow_components).finalize()

    assert "restore_secrets_backend_list" in caplog.text
    assert "also failed" in caplog.text


# ---------------------------------------------------------------------------
# _gate_and_register_timetable: one gate implementation, three policies
# ---------------------------------------------------------------------------


class _PolicyCleanTimetable(Timetable):
    """Fully conformant module-level timetable for the policy-matrix tests."""

    def infer_manual_data_interval(self, *, run_after: Any) -> Any:
        """Never actually run; only the conformance gate is under test.

        Parameters:
            run_after: Any containing the manual trigger time.

        Returns:
            Any; never actually returns.

        Raises:
            NotImplementedError: Always; this timetable never schedules for real.
        """

        del run_after
        raise NotImplementedError

    def next_dagrun_info(self, *, last_automated_data_interval: Any, restriction: Any) -> Any:
        """Schedule nothing.

        Parameters:
            last_automated_data_interval: Any containing the previous interval.
            restriction: Any containing the time restriction.

        Returns:
            Any containing None, meaning no next run.
        """

        del last_automated_data_interval, restriction
        return None

    def serialize(self) -> dict[str, Any]:
        """Emit an empty payload.

        Returns:
            dict[str, Any] containing nothing.
        """

        return {}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> _PolicyCleanTimetable:
        """Reconstruct from an empty payload.

        Parameters:
            data: dict[str, Any] containing the serialized payload.

        Returns:
            _PolicyCleanTimetable containing a fresh instance.
        """

        del data
        return cls()


class _PolicySerializeOnlyTimetable(Timetable):
    """Trip only non-futile problems: missing protocol methods, incomplete pair."""

    def serialize(self) -> dict[str, Any]:
        """Emit an empty payload.

        Returns:
            dict[str, Any] containing nothing.
        """

        return {}


@pytest.fixture
def recorded_registrations(monkeypatch: pytest.MonkeyPatch) -> list[object]:
    """Record `sandbox.register_timetable` calls instead of mutating live state.

    The matrix tests exercise GATE behavior only; the registration mechanics behind
    the seam are covered by `tests/compat/test_component_sandbox_compat.py`, so
    recording keeps these tests free of global plugin-list mutation and of the
    sandbox cleanup that mutation would require.

    Parameters:
        monkeypatch: pytest.MonkeyPatch replacing the seam function for one test.

    Returns:
        list[object] accumulating every timetable passed to the seam.
    """

    registered: list[object] = []
    monkeypatch.setattr(sandbox, "register_timetable", registered.append)
    return registered


def test_gate_strict_policy_registers_a_clean_timetable(
    recorded_registrations: list[object],
) -> None:
    """Register a conformant timetable straight through the strict policy."""

    timetable = _PolicyCleanTimetable()

    fixture_components._gate_and_register_timetable(
        timetable, fixture_components._STRICT_TIMETABLE
    )

    assert recorded_registrations == [timetable]


def test_gate_strict_policy_blocks_every_problem(
    recorded_registrations: list[object],
) -> None:
    """Hard-fail the strict policy on a problem the schedule policy merely warns on."""

    with pytest.raises(ComponentContractError, match="timetable-serialize-pair-incomplete"):
        fixture_components._gate_and_register_timetable(
            _PolicySerializeOnlyTimetable(), fixture_components._STRICT_TIMETABLE
        )

    assert recorded_registrations == []


def test_gate_schedule_policy_warns_and_registers_on_a_nonblocking_problem(
    recorded_registrations: list[object],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Downgrade every non-futile problem to a warning and still register."""

    timetable = _PolicySerializeOnlyTimetable()

    with caplog.at_level(logging.WARNING, logger="pytest_airflow_in_a_box.fixtures.components"):
        fixture_components._gate_and_register_timetable(
            timetable, fixture_components._SCHEDULE_TIMETABLE
        )

    assert recorded_registrations == [timetable]
    assert "timetable-serialize-pair-incomplete" in caplog.text


def test_gate_policy_with_warn_nonblocking_off_stays_silent(
    recorded_registrations: list[object],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Filter without warning when a policy blocks selectively but does not warn.

    No shipped policy combines a blocking set with `warn_nonblocking=False`, but the
    policy type is data and the combination must behave: non-blocking problems are
    silently dropped, registration proceeds.
    """

    timetable = _PolicySerializeOnlyTimetable()
    policy = fixture_components._TimetablePolicy(
        blocking=frozenset({sandbox.TIMETABLE_LOCAL_QUALNAME}),
        warn_nonblocking=False,
        require_instance=False,
    )

    with caplog.at_level(logging.WARNING, logger="pytest_airflow_in_a_box.fixtures.components"):
        fixture_components._gate_and_register_timetable(timetable, policy)

    assert recorded_registrations == [timetable]
    assert caplog.text == ""


def test_gate_schedule_policy_blocks_a_local_qualname(
    recorded_registrations: list[object],
) -> None:
    """Keep the one futile-registration problem a hard failure under the lenient policy."""

    class _LocalTimetable(Timetable):
        """Fail `timetable-local-qualname` by construction."""

    with pytest.raises(ComponentContractError, match="timetable-local-qualname"):
        fixture_components._gate_and_register_timetable(
            _LocalTimetable(), fixture_components._SCHEDULE_TIMETABLE
        )

    assert recorded_registrations == []


def test_gate_round_trip_policy_refuses_a_class(
    recorded_registrations: list[object],
) -> None:
    """Refuse a bare class before the conformance check even runs.

    A class `check_component` DOES flag, on purpose: getting `ComponentSandboxError`
    with the instance-refusal message (not `ComponentContractError` naming the
    conformance problems) proves the isinstance guard genuinely runs first -- a clean
    class would raise the same error under either ordering and pin nothing.
    """

    with pytest.raises(
        ComponentSandboxError,
        match=r"pass `_PolicySerializeOnlyTimetable\(\.\.\.\)` instead of the class",
    ):
        fixture_components._gate_and_register_timetable(
            _PolicySerializeOnlyTimetable, fixture_components._ROUND_TRIP_TIMETABLE
        )

    assert recorded_registrations == []


def test_gate_round_trip_policy_registers_a_clean_instance(
    recorded_registrations: list[object],
) -> None:
    """Accept a live conformant instance through the strict-plus-instance policy."""

    timetable = _PolicyCleanTimetable()

    fixture_components._gate_and_register_timetable(
        timetable, fixture_components._ROUND_TRIP_TIMETABLE
    )

    assert recorded_registrations == [timetable]


# ---------------------------------------------------------------------------
# Cross-test isolation: the issue's own "Done" bullet
# ---------------------------------------------------------------------------


def test_registrations_leave_no_trace_across_tests(
    pytester: pytest.Pytester,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Register one of each kind in one test, see every trace of it gone in the next.

    Runs in a nested `pytester` session (its own fresh `AIRFLOW_HOME`, its own process)
    rather than as two functions in this file: teardown-then-setup ordering between two
    directly-adjacent tests in the SAME session is not guaranteed to run in the same
    order pytest happens to use today, and this is exactly the property issue #113
    states as its acceptance bullet, so it earns a real, order-independent proof.
    """

    pytester.makepyfile(
        """
        import pytest
        from airflow.executors.base_executor import BaseExecutor
        from airflow.listeners import hookimpl
        from airflow.plugins_manager import AirflowPlugin
        from airflow.secrets.base_secrets import BaseSecretsBackend
        from airflow.timetables.base import Timetable

        pytestmark = pytest.mark.db_test


        def _isolation_macro():
            return 42


        class _Plugin(AirflowPlugin):
            name = "pytester_isolation_plugin"
            macros = [_isolation_macro]


        class _Listener:
            @hookimpl
            def on_task_instance_success(self, previous_state, task_instance):
                pass


        class _SecretsBackend(BaseSecretsBackend):
            def get_variable(self, key):
                return None


        class _Executor(BaseExecutor):
            def sync(self):
                pass

            def _process_workloads(self, workload_items):
                pass

            def end(self):
                pass

            def terminate(self):
                pass


        class _IsolationTimetable(Timetable):
            def infer_manual_data_interval(self, *, run_after):
                raise NotImplementedError

            def next_dagrun_info(self, *, last_automated_data_interval, restriction):
                return None

            def serialize(self):
                return {}

            @classmethod
            def deserialize(cls, data):
                return cls()


        def test_register_everything(airflow_components):
            from pytest_airflow_in_a_box._compat import components as sandbox

            airflow_components.plugin(_Plugin)
            task_manager = sandbox.listener_managers()[1]
            airflow_components.listener(_Listener(), task=task_manager is not None)
            airflow_components.secrets_backend(_SecretsBackend)
            airflow_components.executor(_Executor, alias="pytester_isolation_exec")
            airflow_components.policy(get_dagbag_import_timeout=lambda dag_file_path: 30)
            airflow_components.serialization_round_trip(_IsolationTimetable())

            # Simulate the template-render path that integrates plugin macros: it both
            # installs a per-plugin module into sys.modules AND setattrs it onto the
            # macros parent -- the second half is the leak `restore_macros_module_keys`
            # exists for. The CORE half's integrate function exists on every certified
            # release (a plain function on 3.1.x, functools.cache'd on 3.2+) and both
            # target the same parent module.
            from airflow import plugins_manager as core_plugins_manager
            from airflow.sdk.execution_time import macros

            core_plugins_manager.integrate_macros_plugins()
            assert hasattr(macros, "pytester_isolation_plugin")


        def test_nothing_leaked(airflow_components):
            from pytest_airflow_in_a_box._compat import components as sandbox

            core_module, sdk_module = sandbox._plugins_manager_modules()
            plugin_names = {
                type(candidate).__name__
                for candidate in sandbox._live_plugin_list(core_module)
            }
            assert "_Plugin" not in plugin_names
            # The timetable registration's synthesized carrier plugin is always the
            # class named `ComponentRegistryPlugin` (see `build_component_plugin`);
            # its absence proves the timetable registration reverted too.
            assert "ComponentRegistryPlugin" not in plugin_names

            core_manager, task_manager = sandbox.listener_managers()
            listener_types = {
                type(listener).__name__ for listener in core_manager.pm.get_plugins()
            }
            assert "_Listener" not in listener_types

            backend_types = {
                type(backend).__name__ for backend in sandbox.snapshot_secrets_backend_list()
            }
            assert "_SecretsBackend" not in backend_types

            from airflow.executors.executor_loader import ExecutorLoader

            assert "pytester_isolation_exec" not in ExecutorLoader.executors

            # Not a value-based check: Airflow's own built-in default policy already
            # returns a real, unrelated 30.0 for `get_dagbag_import_timeout` with no
            # custom policy registered at all, which would coincidentally equal this
            # test's registered `30` under `==`. The registered policy plugin's class
            # is always named `ComponentRegistryPolicy` (see `build_policy_plugin`);
            # checking for its absence is precise regardless of what it happened to
            # return.
            pm = sandbox.policy_plugin_manager()
            plugin_class_names = {type(plugin).__name__ for plugin in pm.get_plugins()}
            assert "ComponentRegistryPolicy" not in plugin_class_names

            import sys as _sys
            from airflow.sdk.execution_time import macros

            assert not hasattr(macros, "pytester_isolation_plugin")
            assert (
                "airflow.sdk.execution_time.macros.pytester_isolation_plugin"
                not in _sys.modules
            )
        """
    )

    # Avoids a "Plugin already registered under a different name" crash: the plugin is
    # already auto-loaded via its own `pytest11` entry point in this installed dev
    # venv, so the explicit `-p` below would register it a second time under a
    # different name without this. Matches `tests/fixtures/test_dag.py`'s identical
    # nested-pytester idiom.
    monkeypatch.setenv("PYTEST_DISABLE_PLUGIN_AUTOLOAD", "1")
    result = pytester.runpytest_subprocess("-p", "pytest_airflow_in_a_box.plugin", "-q")

    result.assert_outcomes(passed=2)
