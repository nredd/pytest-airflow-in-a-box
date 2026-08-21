"""Test zero-ini defaults and narrowed warning filters."""

from __future__ import annotations

import warnings
from types import SimpleNamespace
from typing import Any

import pytest

from pytest_airflow_in_a_box import defaults


def test_option_defaults_replace_only_parser_defaults() -> None:
    """Rewrite untouched options and preserve every explicit choice."""

    config: Any = SimpleNamespace(
        option=SimpleNamespace(tbstyle="auto", reportchars="fE", durations=None)
    )

    defaults.apply_option_defaults(config)

    assert config.option.tbstyle == "short"
    assert config.option.reportchars == "a"
    assert config.option.durations == 20


def test_option_defaults_preserve_explicit_values() -> None:
    """Keep user-selected option values untouched."""

    config: Any = SimpleNamespace(
        option=SimpleNamespace(tbstyle="long", reportchars="N", durations=3)
    )

    defaults.apply_option_defaults(config)

    assert config.option.tbstyle == "long"
    assert config.option.reportchars == "N"
    assert config.option.durations == 3


def test_ini_defaults_reregister_every_declared_option() -> None:
    """Re-register each declared ini option with the plugin default."""

    registered: list[tuple[str, str, object]] = []

    def addini(name: str, help_text: str, default: object = None) -> None:
        """Record one ini registration.

        Parameters:
            name: str naming the ini option.
            help_text: str describing the ini option.
            default: object containing the registered default.
        """

        registered.append((name, help_text, default))

    parser: Any = SimpleNamespace(addini=addini)

    defaults.register_ini_defaults(parser)

    assert [(name, default) for name, _help, default in registered] == [
        ("tmp_path_retention_policy", "failed"),
        ("tmp_path_retention_count", "3"),
    ]
    assert all(help_text for _name, help_text, _default in registered)


def test_filterwarnings_prepend_below_user_lines() -> None:
    """Insert plugin filters before user lines so later user lines win."""

    lines = ["error::DeprecationWarning:flask_appbuilder"]
    config: Any = SimpleNamespace(getini=lambda name: {"filterwarnings": lines}[name])

    defaults.apply_filterwarnings(config)
    defaults.apply_filterwarnings(config)

    assert lines == [
        *defaults.FILTERWARNINGS,
        "error::DeprecationWarning:flask_appbuilder",
    ]


def test_filterwarnings_reject_non_list_ini_value() -> None:
    """Reject a malformed ``filterwarnings`` ini value."""

    config: Any = SimpleNamespace(getini=lambda name: {"filterwarnings": "oops"}[name])

    with pytest.raises(pytest.UsageError, match="`filterwarnings` must be a list"):
        defaults.apply_filterwarnings(config)


def test_default_filterwarnings_prepend_and_cache() -> None:
    """Cache the parsed bootstrap filters and prepend their lines idempotently."""

    lines = ["error::DeprecationWarning"]
    ini = {
        "filterwarnings": lines,
        "airflow_default_filterwarnings": list(defaults.DEFAULT_FILTERWARNINGS),
    }
    config: Any = SimpleNamespace(getini=lambda name: ini[name], stash=pytest.Stash())

    defaults.apply_default_filterwarnings(config)
    defaults.apply_default_filterwarnings(config)

    assert lines == [*defaults.DEFAULT_FILTERWARNINGS, "error::DeprecationWarning"]
    assert config.stash[defaults.DEFAULT_FILTERWARNINGS_KEY] == (
        ("ignore", "No path_separator found in configuration", DeprecationWarning, "", 0),
    )


def test_default_filterwarnings_reject_non_list_ini_value() -> None:
    """Reject a malformed ``airflow_default_filterwarnings`` ini value."""

    ini = {"airflow_default_filterwarnings": "oops"}
    config: Any = SimpleNamespace(getini=lambda name: ini[name], stash=pytest.Stash())

    with pytest.raises(pytest.UsageError, match="`airflow_default_filterwarnings` must be a list"):
        defaults.apply_default_filterwarnings(config)


def test_default_filterwarnings_reject_invalid_filter_line() -> None:
    """Reject a filter line pytest's own parser cannot understand."""

    ini = {"airflow_default_filterwarnings": ["bogus::Nope"]}
    config: Any = SimpleNamespace(getini=lambda name: ini[name], stash=pytest.Stash())

    with pytest.raises(
        pytest.UsageError,
        match="`airflow_default_filterwarnings` contains an invalid filter line",
    ):
        defaults.apply_default_filterwarnings(config)


def test_default_filterwarnings_option_registered_with_default() -> None:
    """Register the ini option as a linelist carrying the bootstrap default."""

    registered: list[tuple[str, str, str, object]] = []

    def addini(name: str, help_text: str, type: str, default: object) -> None:
        """Record one ini registration.

        Parameters:
            name: str naming the ini option.
            help_text: str describing the ini option.
            type: str naming the declared ini value type.
            default: object containing the registered default.
        """

        registered.append((name, help_text, type, default))

    parser: Any = SimpleNamespace(addini=addini)

    defaults.register_default_filterwarnings_option(parser)

    assert [(name, kind, default) for name, _help, kind, default in registered] == [
        ("airflow_default_filterwarnings", "linelist", list(defaults.DEFAULT_FILTERWARNINGS))
    ]
    assert all(help_text for _name, help_text, _kind, _default in registered)


def test_bootstrap_filters_suppress_inside_default_context() -> None:
    """Silence a cached bootstrap warning inside a reset warnings context."""

    ini = {
        "filterwarnings": [],
        "airflow_default_filterwarnings": list(defaults.DEFAULT_FILTERWARNINGS),
    }
    config: Any = SimpleNamespace(getini=lambda name: ini[name], stash=pytest.Stash())
    defaults.apply_default_filterwarnings(config)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("default")
        defaults.apply_bootstrap_warning_filters(config)
        warnings.warn(
            "No path_separator found in configuration; falling back to legacy splitting",
            DeprecationWarning,
            stacklevel=2,
        )

    assert caught == []


def test_bootstrap_filters_noop_without_stash_entry() -> None:
    """Leave the warnings context untouched when no filters were cached."""

    config: Any = SimpleNamespace(stash=pytest.Stash())

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("default")
        defaults.apply_bootstrap_warning_filters(config)
        warnings.warn(
            "No path_separator found in configuration; falling back to legacy splitting",
            DeprecationWarning,
            stacklevel=2,
        )

    assert len(caught) == 1


def test_defaults_applied_end_to_end(pytester: pytest.Pytester) -> None:
    """Deliver every default through a real run with no ini file."""

    pytester.makepyfile(
        """
        from pytest_airflow_in_a_box.defaults import DEFAULT_FILTERWARNINGS, FILTERWARNINGS

        def test_defaults(request):
            cfg = request.config
            assert cfg.option.tbstyle == "short"
            assert cfg.option.reportchars == "a"
            assert cfg.option.durations == 20
            assert cfg.getini("tmp_path_retention_policy") == "failed"
            assert int(cfg.getini("tmp_path_retention_count")) == 3
            assert tuple(cfg.getini("filterwarnings")) == (
                *DEFAULT_FILTERWARNINGS,
                *FILTERWARNINGS,
            )
        """
    )

    result = pytester.runpytest_subprocess()

    result.assert_outcomes(passed=1)


def test_default_filterwarnings_override_replaces_default(pytester: pytest.Pytester) -> None:
    """Replace the bootstrap default wholesale when the user redefines the option."""

    pytester.makeini(
        "[pytest]\nairflow_default_filterwarnings =\n    ignore:custom noise:UserWarning\n"
    )
    pytester.makepyfile(
        """
        from pytest_airflow_in_a_box.defaults import DEFAULT_FILTERWARNINGS, FILTERWARNINGS

        def test_override(request):
            lines = request.config.getini("filterwarnings")
            assert lines[0] == "ignore:custom noise:UserWarning"
            for default_line in DEFAULT_FILTERWARNINGS:
                assert default_line not in lines
            for plugin_line in FILTERWARNINGS:
                assert plugin_line in lines
        """
    )

    result = pytester.runpytest_subprocess()

    result.assert_outcomes(passed=1)


def test_default_filterwarnings_invalid_line_aborts_the_run(pytester: pytest.Pytester) -> None:
    """Abort the session with a usage error on a malformed filter line."""

    pytester.makeini("[pytest]\nairflow_default_filterwarnings =\n    bogus::Nope\n")
    pytester.makepyfile("def test_never_runs():\n    raise AssertionError\n")

    result = pytester.runpytest_subprocess()

    assert result.ret != 0
    result.stderr.fnmatch_lines(
        ["*Ini option `airflow_default_filterwarnings` contains an invalid filter line*"]
    )


def test_explicit_flags_and_ini_survive(pytester: pytest.Pytester) -> None:
    """Preserve explicit command-line flags and user ini values."""

    pytester.makeini("[pytest]\ntmp_path_retention_policy = none\n")
    pytester.makepyfile(
        """
        def test_explicit(request):
            cfg = request.config
            assert cfg.option.tbstyle == "long"
            assert cfg.option.reportchars == "N"
            assert cfg.option.durations == 3
            assert cfg.getini("tmp_path_retention_policy") == "none"
        """
    )

    result = pytester.runpytest_subprocess("--tb=long", "-rN", "--durations=3")

    result.assert_outcomes(passed=1)


def test_collection_warnings_are_promoted_to_errors(pytester: pytest.Pytester) -> None:
    """Fail collection when pytest cannot collect a test class."""

    pytester.makepyfile(
        test_promoted="""
        class TestHelper:
            def __init__(self):
                pass

        def test_ok():
            assert True
        """
    )

    result = pytester.runpytest_subprocess()

    assert result.ret != 0
    result.stdout.fnmatch_lines(["*PytestCollectionWarning*"])


def test_user_ini_downgrades_plugin_error_filter(pytester: pytest.Pytester) -> None:
    """Let a later user ini line override the plugin's error promotion."""

    pytester.makeini("[pytest]\nfilterwarnings =\n    ignore::pytest.PytestCollectionWarning\n")
    pytester.makepyfile(
        test_downgraded="""
        class TestHelper:
            def __init__(self):
                pass

        def test_ok():
            assert True
        """
    )

    result = pytester.runpytest_subprocess()

    result.assert_outcomes(passed=1)
