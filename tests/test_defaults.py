"""Test zero-ini defaults and narrowed warning filters."""

from __future__ import annotations

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


def test_defaults_applied_end_to_end(pytester: pytest.Pytester) -> None:
    """Deliver every default through a real run with no ini file."""

    pytester.makepyfile(
        """
        from pytest_airflow_in_a_box.defaults import FILTERWARNINGS

        def test_defaults(request):
            cfg = request.config
            assert cfg.option.tbstyle == "short"
            assert cfg.option.reportchars == "a"
            assert cfg.option.durations == 20
            assert cfg.getini("tmp_path_retention_policy") == "failed"
            assert int(cfg.getini("tmp_path_retention_count")) == 3
            assert tuple(cfg.getini("filterwarnings")) == FILTERWARNINGS
        """
    )

    result = pytester.runpytest_subprocess()

    result.assert_outcomes(passed=1)


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
