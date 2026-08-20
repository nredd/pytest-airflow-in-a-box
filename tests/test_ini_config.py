"""Test the `airflow_config` ini option: its grammar, its denylist, and its restoration.

Probe keys are deliberately ones bootstrap does not own -- `core.dagbag_import_timeout` and
`core.plugins_folder` -- for the same reason `tests/test_config.py` picks them: an owned key
would be rejected by the denylist under test rather than exercising the grammar.

References:
    https://github.com/nredd/pytest-airflow-in-a-box/issues/202
    https://docs.pytest.org/en/stable/reference/reference.html#confval-addopts
"""

from __future__ import annotations

import os
import re
from types import SimpleNamespace
from typing import Any

import pytest

from pytest_airflow_in_a_box.bootstrap import _environment_names
from pytest_airflow_in_a_box.config import ENV_VAR_PREFIX
from pytest_airflow_in_a_box.ini_config import (
    INI_OPTION_NAME,
    INI_OVERRIDES_KEY,
    apply_ini_overrides,
    owned_env_names,
    parse_ini_overrides,
    validate_smoke_conflict,
)

TIMEOUT_KEY = ("core", "dagbag_import_timeout")
TIMEOUT_NAME = "AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT"
PLUGINS_NAME = "AIRFLOW__CORE__PLUGINS_FOLDER"

OWNED_NAMES = frozenset(
    {
        "AIRFLOW__CORE__DAGS_FOLDER",
        "AIRFLOW__CORE__UNIT_TEST_MODE",
        "AIRFLOW__CORE__LOAD_EXAMPLES",
        "AIRFLOW__SCHEDULER__CATCHUP_BY_DEFAULT",
        "AIRFLOW__CORE__AUTH_MANAGER",
        "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS",
        "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE",
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
        "AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_ENABLED",
        "AIRFLOW__LOGGING__BASE_LOG_FOLDER",
        "AIRFLOW__API_AUTH__JWT_SECRET",
        "AIRFLOW__WEBSERVER__SECRET_KEY",
        "AIRFLOW__CORE__FERNET_KEY",
    }
)


def _smoke_config(overrides: dict[tuple[str, str], str], *, smoke: object) -> Any:
    """Create a configuration double carrying parsed overrides and a smoke enablement answer.

    Parameters:
        overrides: dict[tuple[str, str], str] pre-stashed as the parsed ini overrides.
        smoke: object returned for the ``--airflow-smoke`` command-line option.

    Returns:
        types.SimpleNamespace shaped like the configuration surface under test.
    """

    stash = pytest.Stash()
    stash[INI_OVERRIDES_KEY] = overrides
    return SimpleNamespace(
        getoption=lambda name: {"airflow_smoke": smoke}[name],
        getini=lambda name: {"airflow_smoke": False}[name],
        stash=stash,
    )


def _config(lines: object) -> Any:
    """Create a minimal configuration double exposing one ini value and a cleanup stack.

    Parameters:
        lines: object returned for the ``airflow_config`` ini option.

    Returns:
        types.SimpleNamespace shaped like the configuration surface under test, whose
        ``cleanups`` list records every callback registered through ``add_cleanup``.
    """

    cleanups: list[Any] = []
    return SimpleNamespace(
        getini=lambda name: {INI_OPTION_NAME: lines}[name],
        stash=pytest.Stash(),
        add_cleanup=cleanups.append,
        cleanups=cleanups,
    )


def test_rejects_a_non_list_ini_value() -> None:
    """Reject an ini value pytest's line-list type could never produce."""

    with pytest.raises(pytest.UsageError, match="must be a list of lines"):
        parse_ini_overrides(_config("core.k = v"))


def test_an_absent_option_produces_no_overrides() -> None:
    """Return an empty mapping for the registered default, which is an empty list."""

    assert parse_ini_overrides(_config([])) == {}


@pytest.mark.parametrize(
    "line",
    ["core.dagbag_import_timeout=12.5", "core.dagbag_import_timeout = 12.5", "core . k = 12.5"],
    ids=["tight", "spaced", "spaces-around-the-dot"],
)
def test_parses_a_section_key_value_line(line: str) -> None:
    """Strip the section, key, and value regardless of surrounding whitespace."""

    assert list(parse_ini_overrides(_config([line])).values()) == ["12.5"]


def test_splits_the_section_on_the_last_dot() -> None:
    """Keep dots inside a section name, which Airflow permits but keys do not."""

    assert parse_ini_overrides(_config(["a.b.key = v"])) == {("a.b", "key"): "v"}


def test_splits_the_value_on_the_first_equals_sign() -> None:
    """Preserve an `=` inside a value, as connection URLs and query strings carry."""

    overrides = parse_ini_overrides(_config(["core.plugins_folder = a=b=c"]))

    assert overrides == {("core", "plugins_folder"): "a=b=c"}


def test_an_empty_value_is_the_empty_string() -> None:
    """Accept an empty value; the line list has no syntax for requesting absence."""

    assert parse_ini_overrides(_config(["core.plugins_folder ="])) == {
        ("core", "plugins_folder"): ""
    }


def test_rejects_a_non_string_line() -> None:
    """Reject a line pytest's line-list type could never produce."""

    with pytest.raises(pytest.UsageError, match="line must be a string"):
        parse_ini_overrides(_config([object()]))


def test_rejects_a_line_without_a_value_separator() -> None:
    """Reject a line that names an option but assigns nothing."""

    with pytest.raises(pytest.UsageError, match=re.escape("must be `section.key = value`")):
        parse_ini_overrides(_config(["core.dagbag_import_timeout"]))


def test_rejects_a_line_without_a_section_separator() -> None:
    """Reject a bare key, which names no section to put it in."""

    with pytest.raises(pytest.UsageError, match="must name a section and a key"):
        parse_ini_overrides(_config(["dagbag_import_timeout = 12.5"]))


@pytest.mark.parametrize(
    ("line", "message"),
    [
        (".key = v", "section must be a non-empty string"),
        ("core. = v", "key must be a non-empty string"),
        ("core.a__b = v", "key must contain only letters"),
        ("co re.key = v", "section must contain only letters"),
    ],
    ids=["empty-section", "empty-key", "delimiter-in-key", "space-in-section"],
)
def test_rejects_a_malformed_section_or_key(line: str, message: str) -> None:
    """Delegate character validation to `env_var_name`, messages included."""

    with pytest.raises(pytest.UsageError, match=message):
        parse_ini_overrides(_config([line]))


@pytest.mark.parametrize(
    "lines",
    [
        ["core.plugins_folder = a", "core.plugins_folder = b"],
        ["a.b.key = a", "a_b.key = b"],
    ],
    ids=["identical-pairs", "colliding-after-mangling"],
)
def test_rejects_two_lines_naming_one_variable(lines: list[str]) -> None:
    """Reject duplicates on the resolved variable name, not the raw pair."""

    with pytest.raises(pytest.UsageError, match="twice"):
        parse_ini_overrides(_config(lines))


@pytest.mark.parametrize(
    ("line", "remedy"),
    [
        ("core.dags_folder = /dags", "`airflow_dags_folder` ini option"),
        ("database.sql_alchemy_conn = sqlite://", "`airflow_db_backend` ini option"),
        ("database.sql_alchemy_pool_enabled = False", "`airflow_db_backend` ini option"),
        ("logging.base_log_folder = /logs", "`airflow_home` ini option"),
        ("core.unit_test_mode = False", "this run's bootstrap owns it"),
    ],
    ids=["dags-folder", "conn", "pool", "logs", "no-specific-remedy"],
)
def test_rejects_an_option_bootstrap_owns(line: str, remedy: str) -> None:
    """Name the supported knob rather than silently fighting bootstrap."""

    with pytest.raises(pytest.UsageError, match=re.escape(remedy)):
        parse_ini_overrides(_config([line]))


def test_denied_names_are_derived_from_the_bootstrap_surface() -> None:
    """Fail loudly when bootstrap starts owning a variable this module does not deny."""

    assert owned_env_names() == OWNED_NAMES
    assert owned_env_names() == {
        name for name in _environment_names() if name.startswith(ENV_VAR_PREFIX)
    }


@pytest.mark.parametrize(
    "name",
    ["AIRFLOW__CORE__EXECUTOR", TIMEOUT_NAME, PLUGINS_NAME],
    ids=["executor", "dagbag-import-timeout", "plugins-folder"],
)
def test_overridable_options_are_not_denied(name: str) -> None:
    """Keep consumer-owned options overridable; `--airflow-doctor` tells users to set them."""

    assert name not in owned_env_names()


def test_apply_sets_the_environment_and_stashes_the_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Assign every declared variable and record the parsed mapping for diagnostics."""

    monkeypatch.setenv(TIMEOUT_NAME, "original")
    monkeypatch.delenv(PLUGINS_NAME, raising=False)
    config = _config(["core.dagbag_import_timeout = 12.5", "core.plugins_folder = /plugins"])

    apply_ini_overrides(config)

    assert os.environ[TIMEOUT_NAME] == "12.5"
    assert os.environ[PLUGINS_NAME] == "/plugins"
    assert config.stash[INI_OVERRIDES_KEY] == {
        TIMEOUT_KEY: "12.5",
        ("core", "plugins_folder"): "/plugins",
    }


def test_the_registered_cleanup_restores_the_environment_exactly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Restore a pre-existing value and delete a name that was absent beforehand."""

    monkeypatch.setenv(TIMEOUT_NAME, "original")
    monkeypatch.delenv(PLUGINS_NAME, raising=False)
    config = _config(["core.dagbag_import_timeout = 12.5", "core.plugins_folder = /plugins"])
    apply_ini_overrides(config)

    assert len(config.cleanups) == 1
    config.cleanups[0]()

    assert os.environ[TIMEOUT_NAME] == "original"
    assert PLUGINS_NAME not in os.environ


def test_smoke_conflict_is_ignored_when_the_catalog_is_off() -> None:
    """Leave `core.dagbag_import_timeout` alone on a run with no catalog to fight it."""

    validate_smoke_conflict(_smoke_config({TIMEOUT_KEY: "120"}, smoke=None))


def test_smoke_conflict_ignores_an_unrelated_override() -> None:
    """Reject only the options the catalog actually pins."""

    validate_smoke_conflict(_smoke_config({("core", "plugins_folder"): "/p"}, smoke=True))


def test_smoke_conflict_rejects_the_parse_timeout() -> None:
    """Fail rather than let the catalog silently overwrite a declared parse timeout."""

    with pytest.raises(pytest.UsageError, match="airflow_dag_parse_timeout"):
        validate_smoke_conflict(_smoke_config({TIMEOUT_KEY: "120"}, smoke=True))
