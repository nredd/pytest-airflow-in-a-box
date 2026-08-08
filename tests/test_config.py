"""Test unified Airflow configuration and environment overrides.

Overrides here target ``core.dagbag_import_timeout`` and ``core.plugins_folder``, neither of which
bootstrap assigns, so a test can observe Airflow's own default outside the context. They
deliberately avoid ``core.dags_folder`` and ``core.unit_test_mode``, which bootstrap owns and which
would change this suite's own Airflow behavior if a test died mid-context.

References:
    https://docs.pytest.org/en/stable/how-to/capture-warnings.html
    https://docs.python.org/3/library/os.html#os.environ
"""

from __future__ import annotations

import os
import warnings
from pathlib import Path
from typing import Any

import pytest

from pytest_airflow_in_a_box.config import (
    ENV_VAR_PREFIX,
    airflow_config,
    conf_vars,
    env_var_name,
)

TIMEOUT_KEY = ("core", "dagbag_import_timeout")
TIMEOUT_NAME = "AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT"
PLUGINS_KEY = ("core", "plugins_folder")
AIRFLOW_DEFAULT_TIMEOUT = "30.0"


def _enter_and_fail(
    overrides: Any = None,
    *,
    env: Any = None,
) -> None:
    """Enter one override context that validation must reject before its body runs.

    Kept at module level so each ``pytest.raises`` block stays a single simple statement.

    Parameters:
        overrides: Any containing the configuration overrides under test.
        env: Any containing the plain environment overrides under test.

    Raises:
        pytest.UsageError: Whenever the arguments are malformed, which is every call here.
        AssertionError: The context body ran, meaning validation wrongly accepted the arguments.
    """

    with airflow_config(overrides, env=env):
        pytest.fail("the context body must not run")


def _raise_inside_context() -> None:
    """Enter one override context and fail inside its body.

    Kept at module level so the ``pytest.raises`` block in the caller stays a single statement.

    Raises:
        RuntimeError: Always, from inside the context body.
    """

    with airflow_config({TIMEOUT_KEY: "99.5"}):
        raise RuntimeError("failure inside the override context")


@airflow_config({TIMEOUT_KEY: "7.5"})
def _read_timeout_while_decorated() -> str | None:
    """Read the overridden option from inside a decorated function.

    Returns:
        str | None containing the environment value observed inside the context.
    """

    return os.environ.get(TIMEOUT_NAME)


@conf_vars({TIMEOUT_KEY: "8.5"})
def _read_timeout_while_deprecated() -> str | None:
    """Read the overridden option from inside a function decorated with the alias.

    Returns:
        str | None containing the environment value observed inside the context.
    """

    return os.environ.get(TIMEOUT_NAME)


def _read_through_conf_vars(overrides: Any) -> dict[str, str | None]:
    """Enter the deprecated alias and report the environment observed inside it.

    Kept at module level so each ``pytest.warns`` block stays a single simple statement.

    Parameters:
        overrides: Any containing the configuration overrides to apply.

    Returns:
        dict[str, str | None] mapping every overridden name to the value seen inside the context.
    """

    with conf_vars(overrides):
        return {env_var_name(*pair): os.environ.get(env_var_name(*pair)) for pair in overrides}


@pytest.mark.parametrize(
    ("section", "key", "expected"),
    [
        ("core", "dags_folder", "AIRFLOW__CORE__DAGS_FOLDER"),
        ("a.b", "k", "AIRFLOW__A_B__K"),
        ("CoRe", "MiXeD", "AIRFLOW__CORE__MIXED"),
        ("a.b.c", "k_2", "AIRFLOW__A_B_C__K_2"),
        ("core2", "k9", "AIRFLOW__CORE2__K9"),
    ],
)
def test_env_var_name_mangles_sections_and_keys(section: str, key: str, expected: str) -> None:
    """Reproduce Airflow's section and key mangling exactly."""

    assert env_var_name(section, key) == expected


@pytest.mark.parametrize(
    ("section", "key", "message"),
    [
        (None, "k", "section must be a non-empty string"),
        (1, "k", "section must be a non-empty string"),
        ("", "k", "section must be a non-empty string"),
        ("a b", "k", "section must contain only letters"),
        ("a-b", "k", "section must contain only letters"),
        ("a__b", "k", "section must contain only letters"),
        ("core", None, "key must be a non-empty string"),
        ("core", 1, "key must be a non-empty string"),
        ("core", "", "key must be a non-empty string"),
        ("core", "a.b", "key must contain only letters"),
        ("core", "a-b", "key must contain only letters"),
        ("core", "a__b", "key must contain only letters"),
    ],
)
def test_env_var_name_rejects_malformed_sections_and_keys(
    section: Any, key: Any, message: str
) -> None:
    """Reject every section or key Airflow's mangling cannot express."""

    with pytest.raises(pytest.UsageError, match=message):
        env_var_name(section, key)


@pytest.mark.parametrize(
    ("overrides", "env"),
    [(None, None), ({}, None), (None, {}), ({}, {})],
)
def test_airflow_config_without_overrides_is_a_noop(
    overrides: Any, env: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Leave the environment untouched for absent and empty mappings alike."""

    monkeypatch.setenv(TIMEOUT_NAME, "kept")
    before = dict(os.environ)

    with airflow_config(overrides, env=env):
        assert dict(os.environ) == before

    assert dict(os.environ) == before


@pytest.mark.parametrize("overrides", ["core", 1, [("core", "k")], (("core", "k"),)])
def test_airflow_config_rejects_non_mapping_overrides(overrides: Any) -> None:
    """Require a mapping of pairs for the configuration overrides."""

    with pytest.raises(pytest.UsageError, match="`overrides` must be a mapping"):
        _enter_and_fail(overrides)


@pytest.mark.parametrize("env", ["NAME", 1, [("NAME", "1")]])
def test_airflow_config_rejects_non_mapping_env(env: Any) -> None:
    """Require a mapping of names for the plain environment overrides."""

    with pytest.raises(pytest.UsageError, match="`env` must be a mapping"):
        _enter_and_fail(env=env)


@pytest.mark.parametrize("pair", ["core", 1, ("core",), ("core", "k", "extra")])
def test_airflow_config_rejects_malformed_override_keys(pair: Any) -> None:
    """Require every override key to be a two-element tuple."""

    with pytest.raises(pytest.UsageError, match="keys must be `\\(section, key\\)` pairs"):
        _enter_and_fail({pair: "value"})


@pytest.mark.parametrize("value", [True, 1, 3.5, [], Path("/tmp")])
def test_airflow_config_rejects_non_string_override_values(value: Any) -> None:
    """Require override values to be a string or None."""

    with pytest.raises(pytest.UsageError, match="`overrides` values must be a string or None"):
        _enter_and_fail({TIMEOUT_KEY: value})


def test_airflow_config_rejects_colliding_override_keys() -> None:
    """Reject two pairs mangling to one environment variable instead of picking a winner."""

    with pytest.raises(pytest.UsageError, match="both name `AIRFLOW__A_B__K`"):
        _enter_and_fail({("a.b", "k"): "1", ("a_b", "k"): "2"})


@pytest.mark.parametrize(
    ("name", "message"),
    [
        (None, "`env` names must be a non-empty string"),
        (1, "`env` names must be a non-empty string"),
        ("", "`env` names must be a non-empty string"),
        ("1BAD", "`env` names must start with a letter"),
        ("A=B", "`env` names must start with a letter"),
        ("A-B", "`env` names must start with a letter"),
        ("A B", "`env` names must start with a letter"),
        ("A\x00B", "`env` names must start with a letter"),
    ],
)
def test_airflow_config_rejects_malformed_env_names(name: Any, message: str) -> None:
    """Reject every environment variable name the platform cannot represent."""

    with pytest.raises(pytest.UsageError, match=message):
        _enter_and_fail(env={name: "value"})


def test_airflow_config_rejects_airflow_config_namespace_env_names() -> None:
    """Route Airflow configuration options through `overrides`, not `env`."""

    with pytest.raises(pytest.UsageError, match=f"must not start with `{ENV_VAR_PREFIX}`"):
        _enter_and_fail(env={TIMEOUT_NAME: "12.5"})


def test_airflow_config_accepts_single_underscore_airflow_env_names() -> None:
    """Keep the namespace guard narrow enough to allow `AIRFLOW_HOME`."""

    with airflow_config(env={"AIRFLOW_HOME_PROBE": "probe"}):
        assert os.environ["AIRFLOW_HOME_PROBE"] == "probe"

    assert "AIRFLOW_HOME_PROBE" not in os.environ


@pytest.mark.parametrize("value", [True, 1, 3.5, [], Path("/tmp")])
def test_airflow_config_rejects_non_string_env_values(value: Any) -> None:
    """Require plain environment values to be a string or None."""

    with pytest.raises(pytest.UsageError, match="`env` values must be a string or None"):
        _enter_and_fail(env={"MY_PROBE_FLAG": value})


def test_airflow_config_restores_a_preexisting_value_exactly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return a previously-present variable to its original value."""

    monkeypatch.setenv(TIMEOUT_NAME, "original")

    with airflow_config({TIMEOUT_KEY: "overridden"}):
        assert os.environ[TIMEOUT_NAME] == "overridden"

    assert os.environ[TIMEOUT_NAME] == "original"


def test_airflow_config_deletes_a_previously_absent_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Delete a previously-absent variable rather than leaving it empty."""

    monkeypatch.delenv(TIMEOUT_NAME, raising=False)

    with airflow_config({TIMEOUT_KEY: "12.5"}):
        assert os.environ[TIMEOUT_NAME] == "12.5"

    assert TIMEOUT_NAME not in os.environ
    assert os.environ.get(TIMEOUT_NAME) != ""


def test_none_override_unsets_the_option_inside_the_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Make an assigned option absent inside the context and restore it after."""

    monkeypatch.setenv(TIMEOUT_NAME, "original")

    with airflow_config({TIMEOUT_KEY: None}):
        assert TIMEOUT_NAME not in os.environ

    assert os.environ[TIMEOUT_NAME] == "original"


def test_none_env_value_deletes_the_variable_inside_the_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Make an assigned plain variable absent inside the context and restore it after."""

    monkeypatch.setenv("MY_PROBE_FLAG", "original")

    with airflow_config(env={"MY_PROBE_FLAG": None}):
        assert "MY_PROBE_FLAG" not in os.environ

    assert os.environ["MY_PROBE_FLAG"] == "original"


def test_nested_contexts_restore_in_lifo_order(monkeypatch: pytest.MonkeyPatch) -> None:
    """Restore each nesting level's own snapshot on the way out."""

    monkeypatch.setenv(TIMEOUT_NAME, "outermost")
    monkeypatch.delenv("MY_PROBE_FLAG", raising=False)

    with airflow_config({TIMEOUT_KEY: "first"}):
        assert os.environ[TIMEOUT_NAME] == "first"
        with airflow_config({TIMEOUT_KEY: "second"}):
            assert os.environ[TIMEOUT_NAME] == "second"
            with airflow_config({TIMEOUT_KEY: None}, env={"MY_PROBE_FLAG": "innermost"}):
                assert TIMEOUT_NAME not in os.environ
                assert os.environ["MY_PROBE_FLAG"] == "innermost"
            assert os.environ[TIMEOUT_NAME] == "second"
            assert "MY_PROBE_FLAG" not in os.environ
        assert os.environ[TIMEOUT_NAME] == "first"
    assert os.environ[TIMEOUT_NAME] == "outermost"


def test_airflow_config_restores_after_an_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    """Restore the environment when the context body raises."""

    monkeypatch.delenv(TIMEOUT_NAME, raising=False)

    with pytest.raises(RuntimeError, match="failure inside the override context"):
        _raise_inside_context()

    assert TIMEOUT_NAME not in os.environ


def test_airflow_config_works_as_a_decorator(monkeypatch: pytest.MonkeyPatch) -> None:
    """Apply and restore the override on every call to a decorated function."""

    monkeypatch.delenv(TIMEOUT_NAME, raising=False)

    assert _read_timeout_while_decorated() == "7.5"
    assert TIMEOUT_NAME not in os.environ
    assert _read_timeout_while_decorated() == "7.5"
    assert TIMEOUT_NAME not in os.environ


def test_overrides_and_env_apply_in_one_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    """Apply configuration options and plain variables through one code path."""

    monkeypatch.delenv(TIMEOUT_NAME, raising=False)
    monkeypatch.setenv("MY_PROBE_FLAG", "original")

    with airflow_config({TIMEOUT_KEY: "12.5"}, env={"MY_PROBE_FLAG": "overridden"}):
        assert os.environ[TIMEOUT_NAME] == "12.5"
        assert os.environ["MY_PROBE_FLAG"] == "overridden"

    assert TIMEOUT_NAME not in os.environ
    assert os.environ["MY_PROBE_FLAG"] == "original"


def test_overrides_are_visible_to_the_core_config_parser(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Expose overrides to Airflow's core parser without any cache invalidation."""

    from airflow.configuration import conf

    monkeypatch.delenv(TIMEOUT_NAME, raising=False)

    # `_lookup_sequence` consults the environment first on every `get`, so this module
    # deliberately calls no Airflow invalidation hook.
    assert conf.get("core", "dagbag_import_timeout") == AIRFLOW_DEFAULT_TIMEOUT
    with airflow_config({TIMEOUT_KEY: "12.5"}):
        assert conf.get("core", "dagbag_import_timeout") == "12.5"
        assert conf.getfloat("core", "dagbag_import_timeout") == 12.5
    assert conf.get("core", "dagbag_import_timeout") == AIRFLOW_DEFAULT_TIMEOUT


def test_overrides_are_visible_to_the_task_sdk_config_parser(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Expose overrides to the Task SDK parser Airflow 3.2 added, through the same path."""

    sdk_configuration = pytest.importorskip(
        "airflow.sdk.configuration", reason="Task SDK configuration parser added in Airflow 3.2"
    )

    monkeypatch.delenv(TIMEOUT_NAME, raising=False)

    with airflow_config({TIMEOUT_KEY: "12.5"}):
        assert sdk_configuration.conf.get("core", "dagbag_import_timeout") == "12.5"
    assert sdk_configuration.conf.get("core", "dagbag_import_timeout") == AIRFLOW_DEFAULT_TIMEOUT


def test_refresh_settings_recomputes_the_airflow_settings_globals(tmp_path: Path) -> None:
    """Recompute the settings globals on entry and restore them on exit."""

    from airflow import settings

    original = settings.PLUGINS_FOLDER
    folder = tmp_path / "plugins"

    with airflow_config({PLUGINS_KEY: str(folder)}, refresh_settings=True):
        assert str(folder) == settings.PLUGINS_FOLDER

    assert original == settings.PLUGINS_FOLDER


def test_refresh_settings_defaults_to_leaving_the_globals_stale(tmp_path: Path) -> None:
    """Leave the settings globals untouched unless a caller opts in."""

    from airflow import settings
    from airflow.configuration import conf

    original = settings.PLUGINS_FOLDER
    folder = tmp_path / "plugins"

    with airflow_config({PLUGINS_KEY: str(folder)}):
        assert conf.get("core", "plugins_folder") == str(folder)
        assert original == settings.PLUGINS_FOLDER

    assert original == settings.PLUGINS_FOLDER


def test_conf_vars_emits_a_deprecation_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    """Warn about the deprecated alias while still applying the override."""

    monkeypatch.delenv(TIMEOUT_NAME, raising=False)

    with pytest.warns(DeprecationWarning, match="`conf_vars` is deprecated"):
        observed = _read_through_conf_vars({TIMEOUT_KEY: "12.5"})

    assert observed == {TIMEOUT_NAME: "12.5"}
    assert TIMEOUT_NAME not in os.environ


def test_conf_vars_warns_on_every_decorated_call(monkeypatch: pytest.MonkeyPatch) -> None:
    """Warn once per context entry rather than once per decoration."""

    monkeypatch.delenv(TIMEOUT_NAME, raising=False)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert _read_timeout_while_deprecated() == "8.5"
        assert _read_timeout_while_deprecated() == "8.5"

    deprecations = [entry for entry in caught if entry.category is DeprecationWarning]

    assert len(deprecations) == 2
    assert TIMEOUT_NAME not in os.environ


def test_conf_vars_restores_exactly(monkeypatch: pytest.MonkeyPatch) -> None:
    """Share the restore path with `airflow_config` for present and absent names alike."""

    monkeypatch.setenv(TIMEOUT_NAME, "original")
    monkeypatch.delenv("AIRFLOW__CORE__PLUGINS_FOLDER", raising=False)

    with pytest.warns(DeprecationWarning, match="`conf_vars` is deprecated"):
        observed = _read_through_conf_vars({TIMEOUT_KEY: "12.5", PLUGINS_KEY: "/tmp/plugins"})

    assert observed == {TIMEOUT_NAME: "12.5", "AIRFLOW__CORE__PLUGINS_FOLDER": "/tmp/plugins"}
    assert os.environ[TIMEOUT_NAME] == "original"
    assert "AIRFLOW__CORE__PLUGINS_FOLDER" not in os.environ


def test_airflow_config_applies_inside_a_bootstrapped_session(pytester: pytest.Pytester) -> None:
    """Drive the decorator form through a real plugin-bootstrapped session."""

    pytester.makepyfile(
        """
        from airflow.configuration import conf

        from pytest_airflow_in_a_box.config import airflow_config


        @airflow_config({("core", "dagbag_import_timeout"): "12.5"})
        def test_override_is_visible():
            assert conf.get("core", "dagbag_import_timeout") == "12.5"


        def test_override_is_restored():
            assert conf.get("core", "dagbag_import_timeout") == "30.0"
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=2)
