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

from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from airflow._shared.plugins_manager import AirflowPlugin
from airflow.executors.base_executor import BaseExecutor
from airflow.listeners import hookimpl
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.secrets.base_secrets import BaseSecretsBackend

from pytest_airflow_in_a_box._compat import components as sandbox
from pytest_airflow_in_a_box.components import ComponentContractError, ComponentSandboxError

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
    names = {type(candidate).__name__ for candidate in core_module._get_plugins()[0]}
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
    names = {type(candidate).__name__ for candidate in core_module._get_plugins()[0]}
    assert "_Unnamed" not in names


# ---------------------------------------------------------------------------
# listener()
# ---------------------------------------------------------------------------


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
    assert task_manager is not None
    assert listener not in task_manager.pm.get_plugins()


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
    airflow_components.listener(listener)

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


# ---------------------------------------------------------------------------
# round_trip()
# ---------------------------------------------------------------------------


def test_round_trip_classifies_and_registers_a_plugin(
    airflow_components: ComponentRegistry,
) -> None:
    """Classify a bare `AirflowPlugin` subclass as a plugin and register it."""

    airflow_components.round_trip(_Plugin)

    core_module, _sdk_module = sandbox._plugins_manager_modules()
    names = {type(candidate).__name__ for candidate in core_module._get_plugins()[0]}
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


def test_round_trip_rejects_an_unclassifiable_component(
    airflow_components: ComponentRegistry,
) -> None:
    """Refuse a bare object matching none of the four registrable kinds."""

    with pytest.raises(ComponentSandboxError, match="could not classify"):
        airflow_components.round_trip(object())


def test_round_trip_rejects_an_ambiguous_component(
    airflow_components: ComponentRegistry,
) -> None:
    """Refuse a component matching more than one registrable kind at once."""

    with pytest.raises(ComponentSandboxError, match="could not classify"):
        airflow_components.round_trip(_AmbiguousExecutorListener)


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
        from airflow._shared.plugins_manager import AirflowPlugin
        from airflow.executors.base_executor import BaseExecutor
        from airflow.listeners import hookimpl
        from airflow.secrets.base_secrets import BaseSecretsBackend

        pytestmark = pytest.mark.db_test


        class _Plugin(AirflowPlugin):
            name = "pytester_isolation_plugin"


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


        def test_register_everything(airflow_components):
            airflow_components.plugin(_Plugin)
            airflow_components.listener(_Listener())
            airflow_components.secrets_backend(_SecretsBackend)
            airflow_components.executor(_Executor, alias="pytester_isolation_exec")
            airflow_components.policy(get_dagbag_import_timeout=lambda dag_file_path: 30)


        def test_nothing_leaked(airflow_components):
            from pytest_airflow_in_a_box._compat import components as sandbox

            core_module, sdk_module = sandbox._plugins_manager_modules()
            plugin_names = {
                type(candidate).__name__ for candidate in core_module._get_plugins()[0]
            }
            assert "_Plugin" not in plugin_names

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
