"""Test how the ini substrate and the `airflow_components` overlay compose.

`docs/guide/custom-components.md`'s "How the two channels compose" section states the
contract; this file is its pin. Each test runs a nested `pytester` session with the
component ini options actually SET (the one combination
`tests/fixtures/test_component_sandbox.py`'s isolation proof leaves uncovered), so
sandbox finalize genuinely restores registries the bootstrap substrate seeded rather
than empty ones.

References:
    https://docs.pytest.org/en/stable/how-to/writing_plugins.html#testing-plugins
    https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/secrets-backend/index.html
"""

from __future__ import annotations

import pytest

# Two support modules per nested session, split by import weight ON PURPOSE. The
# secrets module imports only `airflow.secrets.base_secrets` (an import-light chain):
# `initialize_secrets_backends` imports the configured backend DURING
# `airflow.configuration`'s own module initialization, so a backend module that
# transitively imported `airflow.configuration` (as `BaseExecutor` does) would recurse
# into the partially-initialized configuration module -- the exact constraint a real
# deployed secrets backend package lives under. Executors carry no such constraint:
# `[core] executor` is stored as a string and imported lazily by `ExecutorLoader`.
_SUPPORT_SECRETS_MODULE = '''
"""Import-light secrets backends for the channel-interaction nested sessions."""

from airflow.secrets.base_secrets import BaseSecretsBackend

# Cross-test scratch state: this module is NOT a plugins-folder module, so
# `restore_sys_modules` never unloads it and values stored here survive sandbox
# finalize into the next test of the same nested session.
STATE = {}


class IniSecretsBackend(BaseSecretsBackend):
    def get_variable(self, key, team_name=None):
        return None


class SandboxSecretsBackend(BaseSecretsBackend):
    def get_variable(self, key, team_name=None):
        return None
'''

_SUPPORT_EXECUTORS_MODULE = '''
"""Importable executors for the channel-interaction nested sessions."""

from airflow.executors.base_executor import BaseExecutor


class IniExecutor(BaseExecutor):
    def sync(self):
        pass

    def _process_workloads(self, workload_items):
        pass


class OverrideExecutor(BaseExecutor):
    def sync(self):
        pass

    def _process_workloads(self, workload_items):
        pass


class SandboxExecutor(BaseExecutor):
    def sync(self):
        pass

    def _process_workloads(self, workload_items):
        pass
'''

# A real, file-based plugin for the run's `airflow_plugins_folder`: the plugins-manager
# scan loads it, and `integrate_listener_plugins` registers its listener instance on
# the core manager the first time `get_listener_manager()` builds one.
_INI_PLUGIN = """
from airflow.listeners import hookimpl
from airflow.plugins_manager import AirflowPlugin


class _IniListener:
    @hookimpl
    def on_task_instance_success(self, previous_state, task_instance):
        pass


class IniChannelPlugin(AirflowPlugin):
    name = "ini_channel_plugin"
    listeners = [_IniListener()]
"""


def _run_nested(pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch, passed: int) -> None:
    """Run the prepared nested session and assert its outcome.

    Parameters:
        pytester: pytest.Pytester holding the prepared ini, support, and test files.
        monkeypatch: pytest.MonkeyPatch disabling plugin autoload for the subprocess.
        passed: int containing the expected passed-test count.
    """

    # Avoids a "Plugin already registered under a different name" crash; matches the
    # identical nested-pytester idiom in `tests/fixtures/test_component_sandbox.py`.
    monkeypatch.setenv("PYTEST_DISABLE_PLUGIN_AUTOLOAD", "1")
    result = pytester.runpytest_subprocess("-p", "pytest_airflow_in_a_box.plugin", "-q")

    result.assert_outcomes(passed=passed)


def test_ini_substrate_survives_sandbox_finalize(
    pytester: pytest.Pytester,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """See every ini-seeded component still standing after a sandboxed test finalizes.

    One nested session, two tests in definition order: the first proves the substrate
    is live INSIDE a sandboxed test (non-vacuously -- each assertion observes the
    component, not just its configuration), registers one overlay of each kind on top,
    and proves a sandbox executor alias resolves alongside the ini executor; the
    second, unsandboxed, proves finalize restored the substrate -- ini executor still
    resolving, ini secrets backend restored as the SAME instance object, plugins-folder
    plugin back via rescan, plugins-folder listener surviving through the listener
    snapshot -- with every overlay gone.

    Parameters:
        pytester: pytest.Pytester running the nested session.
        monkeypatch: pytest.MonkeyPatch disabling plugin autoload for the subprocess.
    """

    plugins_source = pytester.path / "shared-plugins"
    plugins_source.mkdir()
    (plugins_source / "ini_channel_plugin.py").write_text(_INI_PLUGIN, encoding="utf-8")
    pytester.makeini(
        "[pytest]\n"
        "airflow_plugins_folder = shared-plugins\n"
        "airflow_executor = support_executors.IniExecutor\n"
        "airflow_secrets_backend = support_secrets.IniSecretsBackend\n"
    )
    pytester.makepyfile(
        support_secrets=_SUPPORT_SECRETS_MODULE,
        support_executors=_SUPPORT_EXECUTORS_MODULE,
    )
    pytester.makepyfile(
        """
        import pytest

        # Imported before the support modules ON PURPOSE: `airflow.configuration`'s
        # own init imports the ini-configured secrets backend, so that first import
        # must be Airflow's, executing `support_secrets` completely -- imported the
        # other way around, the backend module would be mid-execution when the
        # configuration init re-enters it and its classes would not exist yet.
        import airflow.configuration

        import support_executors
        import support_secrets

        pytestmark = pytest.mark.db_test


        def _listener_type_names(manager):
            # By NAME, never identity: plugins-folder modules reload across sandboxed
            # tests, so classes from different loads are distinct objects -- exactly
            # the stale-binding hazard the guide documents.
            return {type(listener).__name__ for listener in manager.pm.get_plugins()}


        def _plugin_names(sandbox):
            core_module, _sdk_module = sandbox._plugins_manager_modules()
            return {plugin.name for plugin in sandbox._live_plugin_list(core_module)}


        def test_substrate_visible_inside_a_sandboxed_test(airflow_components):
            from airflow.configuration import conf, secrets_backend_list
            from airflow.executors.executor_loader import ExecutorLoader

            from pytest_airflow_in_a_box._compat import components as sandbox

            # Ini executor: configured AND naturally resolvable from that config.
            assert conf.get("core", "executor") == "support_executors.IniExecutor"
            default = ExecutorLoader.get_default_executor_name()
            assert default.module_path == "support_executors.IniExecutor"

            # Ini secrets backend: instantiated at the front of the live search path.
            # Stored as the OBJECT, not an `id()`, so the later identity assertion
            # cannot be satisfied by CPython address reuse after a rebuild.
            assert type(secrets_backend_list[0]).__name__ == "IniSecretsBackend"
            support_secrets.STATE["ini_backend"] = secrets_backend_list[0]

            # Ini plugins-folder plugin and its listener: loaded and integrated.
            assert "ini_channel_plugin" in _plugin_names(sandbox)
            core_manager, task_manager = sandbox.listener_managers()
            assert "_IniListener" in _listener_type_names(core_manager)

            # Overlay each substrate kind through the sandbox.
            alias = airflow_components.executor(
                support_executors.SandboxExecutor, alias="overlay_exec"
            )
            airflow_components.secrets_backend(support_secrets.SandboxSecretsBackend)

            from airflow.listeners import hookimpl

            class _SandboxListener:
                @hookimpl
                def on_task_instance_success(self, previous_state, task_instance):
                    pass

            airflow_components.listener(
                _SandboxListener(), task=task_manager is not None
            )

            # The alias resolves WHILE the ini executor stays the configured default:
            # the two channels answer different questions and do not collide.
            resolved = ExecutorLoader.lookup_executor_name_by_str(alias)
            assert resolved.module_path == "support_executors.SandboxExecutor"
            assert conf.get("core", "executor") == "support_executors.IniExecutor"
            assert type(secrets_backend_list[0]).__name__ == "SandboxSecretsBackend"
            assert "_SandboxListener" in _listener_type_names(core_manager)


        def test_substrate_survives_after_finalize():
            from airflow.configuration import conf, secrets_backend_list
            from airflow.executors.executor_loader import ExecutorLoader

            from pytest_airflow_in_a_box._compat import components as sandbox

            # Ini executor: still configured, still resolvable; overlay alias gone.
            assert conf.get("core", "executor") == "support_executors.IniExecutor"
            default = ExecutorLoader.get_default_executor_name()
            assert default.module_path == "support_executors.IniExecutor"
            assert "overlay_exec" not in ExecutorLoader.executors

            # Ini secrets backend: restored as the SAME live instance object, not a
            # freshly-built one -- the instance-resurrection contract; overlay gone.
            backend_names = [type(backend).__name__ for backend in secrets_backend_list]
            assert backend_names[0] == "IniSecretsBackend"
            assert "SandboxSecretsBackend" not in backend_names
            assert secrets_backend_list[0] is support_secrets.STATE["ini_backend"]

            # Ini plugins-folder plugin: back through the post-finalize rescan.
            assert "ini_channel_plugin" in _plugin_names(sandbox)

            # Ini plugins-folder listener: survives because the never-cleared, cached
            # manager was resolved and snapshotted at sandbox construction with the
            # integrated listener already on it; overlay gone.
            core_manager, _task_manager = sandbox.listener_managers()
            names = _listener_type_names(core_manager)
            assert "_IniListener" in names
            assert "_SandboxListener" not in names
        """
    )

    _run_nested(pytester, monkeypatch, passed=2)


@pytest.mark.parametrize("ini_backend", [True, False], ids=["ini-backend", "defaults-only"])
def test_sandbox_secrets_backend_visible_through_ensure_secrets_loaded(
    pytester: pytest.Pytester,
    monkeypatch: pytest.MonkeyPatch,
    ini_backend: bool,
) -> None:
    """Prove a sandbox-registered backend stays visible through `ensure_secrets_loaded()`.

    Upstream's `ensure_secrets_loaded()` rebuilds a fresh list from configuration when
    the live list holds exactly two entries (its `len(...) == 2` heuristic, see
    PROVENANCE.md) -- a rebuild that would NOT contain a sandbox registration. The
    nested test first pins the heuristic's precondition (two default backends, plus
    one when the ini seeds a backend), then registers a backend and asserts the very
    instance is in what `ensure_secrets_loaded()` returns, with and without an ini
    backend beneath it.

    Parameters:
        pytester: pytest.Pytester running the nested session.
        monkeypatch: pytest.MonkeyPatch disabling plugin autoload for the subprocess.
        ini_backend: bool seeding `airflow_secrets_backend` in the nested ini.
    """

    ini_lines = ["[pytest]"]
    if ini_backend:
        ini_lines.append("airflow_secrets_backend = support_secrets.IniSecretsBackend")
    pytester.makeini("\n".join(ini_lines) + "\n")
    pytester.makepyfile(support_secrets=_SUPPORT_SECRETS_MODULE)
    pytester.makepyfile(
        f"""
        import pytest

        # Airflow first; see `test_ini_substrate_survives_sandbox_finalize`'s inner
        # module for why the ini-configured backend module must not be imported ahead
        # of `airflow.configuration`.
        import airflow.configuration

        import support_secrets

        pytestmark = pytest.mark.db_test


        def test_registered_backend_is_visible(airflow_components):
            from airflow.configuration import ensure_secrets_loaded, secrets_backend_list

            # The heuristic's precondition: two built-in defaults, plus the ini
            # backend when configured. A drift here means upstream changed the
            # baseline this visibility guarantee is reasoned from.
            assert len(secrets_backend_list) == {3 if ini_backend else 2}

            backend = support_secrets.SandboxSecretsBackend()
            airflow_components.secrets_backend(backend)

            loaded = ensure_secrets_loaded()
            assert any(candidate is backend for candidate in loaded)
        """
    )

    _run_nested(pytester, monkeypatch, passed=1)


def test_airflow_config_ini_outranks_the_airflow_executor_ini(
    pytester: pytest.Pytester,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pin the `core.executor` last-writer edge: the `airflow_config` line wins.

    `airflow_executor` writes `[core] executor` into the generated `airflow.cfg`,
    while an `airflow_config` ini line becomes the `AIRFLOW__CORE__EXECUTOR`
    environment variable -- and the environment outranks the file on every
    `conf.get()`. The nested test proves both channels genuinely wrote (the file
    carries the `airflow_executor` value, the environment carries the override), so
    the winner assertion cannot pass vacuously.

    Parameters:
        pytester: pytest.Pytester running the nested session.
        monkeypatch: pytest.MonkeyPatch disabling plugin autoload for the subprocess.
    """

    pytester.makeini(
        "[pytest]\n"
        "airflow_executor = support_executors.IniExecutor\n"
        "airflow_config =\n"
        "    core.executor = support_executors.OverrideExecutor\n"
    )
    pytester.makepyfile(support_executors=_SUPPORT_EXECUTORS_MODULE)
    pytester.makepyfile(
        """
        import os

        import pytest

        pytestmark = pytest.mark.db_test


        def test_airflow_config_line_wins(pytestconfig):
            from airflow.configuration import conf

            from pytest_airflow_in_a_box.plugin import get_bootstrap_state

            state = get_bootstrap_state(pytestconfig)
            generated = state.config_path.read_text(encoding="utf-8")
            assert "executor = support_executors.IniExecutor" in generated
            assert (
                os.environ["AIRFLOW__CORE__EXECUTOR"]
                == "support_executors.OverrideExecutor"
            )
            assert conf.get("core", "executor") == "support_executors.OverrideExecutor"
        """
    )

    _run_nested(pytester, monkeypatch, passed=1)
