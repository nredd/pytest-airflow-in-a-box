"""Test `--airflow-migration-strict`: option/ini resolution, filter application, e2e behavior."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from pytest_airflow_in_a_box import migration_strict
from pytest_airflow_in_a_box._compat.capabilities import AirflowFamily


def _config(
    *,
    airflow_migration_strict: object = None,
    ini_migration_strict: object = False,
    filterwarnings: list[str] | None = None,
) -> Any:
    """Create a minimal configuration double for migration-strict config-reader tests.

    Parameters:
        airflow_migration_strict: object containing the parsed
            ``--airflow-migration-strict`` option value.
        ini_migration_strict: object containing the ``airflow_migration_strict`` ini value.
        filterwarnings: list[str] | None containing the mutable ``filterwarnings`` ini
            value; defaults to a fresh empty list.

    Returns:
        types.SimpleNamespace shaped like the configuration surface under test.
    """

    lines: list[str] = [] if filterwarnings is None else filterwarnings
    option_values: dict[str, object] = {"airflow_migration_strict": airflow_migration_strict}
    ini_values: dict[str, object] = {
        "airflow_migration_strict": ini_migration_strict,
        "filterwarnings": lines,
    }
    return SimpleNamespace(
        getoption=lambda name: option_values[name],
        getini=lambda name: ini_values[name],
        stash=pytest.Stash(),
    )


def test_migration_strict_enabled_prefers_cli_option() -> None:
    """Give the CLI option precedence over the ini value."""

    config = _config(airflow_migration_strict=True, ini_migration_strict=False)

    assert migration_strict.migration_strict_enabled(config) is True


def test_migration_strict_enabled_reads_ini_when_cli_absent() -> None:
    """Fall back to the ini value when the CLI option is unset."""

    config = _config(airflow_migration_strict=None, ini_migration_strict=True)

    assert migration_strict.migration_strict_enabled(config) is True


def test_migration_strict_enabled_defaults_to_disabled() -> None:
    """Return disabled when neither the CLI option nor the ini value is set."""

    config = _config()

    assert migration_strict.migration_strict_enabled(config) is False


def test_migration_strict_enabled_caches_resolution() -> None:
    """Resolve the enabled flag once and serve later calls from the stash."""

    reads: list[str] = []
    config = _config(airflow_migration_strict=True)
    original_getoption = config.getoption
    config.getoption = lambda name: (reads.append(name), original_getoption(name))[1]

    assert migration_strict.migration_strict_enabled(config) is True
    assert migration_strict.migration_strict_enabled(config) is True
    assert reads == ["airflow_migration_strict"]


def test_migration_strict_enabled_rejects_non_boolean_ini() -> None:
    """Reject a non-boolean ini value."""

    config = _config(ini_migration_strict="yes")

    with pytest.raises(pytest.UsageError, match="`airflow_migration_strict` must be a boolean"):
        migration_strict.migration_strict_enabled(config)


def test_migration_strict_enabled_rejects_unparseable_real_ini_value(
    pytester: pytest.Pytester,
) -> None:
    """Render pytest's own ini-coercion `ValueError` as a usage error, not a crash.

    Regression test: `parser.addini(..., type="bool")` makes pytest's real
    `config.getini()` coerce the raw ini string itself via `_strtobool`, raising a
    bare `ValueError` for a value like `maybe` before `migration_strict_enabled` ever
    sees it. The unit test above only exercises a duck-typed config double whose
    `getini` returns the raw string untouched, which never touches this path -- only
    a real pytest ini file reproduces it.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    pytester.makeini("[pytest]\nairflow_migration_strict = maybe\n")
    pytester.makepyfile(
        test_ok="""
        def test_ok():
            assert True
        """
    )

    result = pytester.runpytest_subprocess()

    assert result.ret == pytest.ExitCode.USAGE_ERROR
    output = result.stdout.str() + result.stderr.str()
    assert "`airflow_migration_strict` must be a boolean" in output
    assert "INTERNALERROR" not in output


def test_apply_migration_strict_filterwarnings_noop_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Leave the filter list untouched when migration-strict mode is disabled."""

    monkeypatch.setattr(migration_strict, "installed_family", lambda: AirflowFamily.V2)
    lines: list[str] = []
    config = _config(airflow_migration_strict=False, filterwarnings=lines)

    migration_strict.apply_migration_strict_filterwarnings(config)

    assert lines == []


def test_apply_migration_strict_filterwarnings_noop_off_v2_family(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Leave the filter list untouched off the Airflow 2.x family, even when enabled."""

    monkeypatch.setattr(migration_strict, "installed_family", lambda: AirflowFamily.V3)
    lines: list[str] = []
    config = _config(airflow_migration_strict=True, filterwarnings=lines)

    migration_strict.apply_migration_strict_filterwarnings(config)

    assert lines == []


def test_apply_migration_strict_filterwarnings_noop_without_installed_airflow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Leave the filter list untouched when neither Airflow family is installed."""

    monkeypatch.setattr(migration_strict, "installed_family", lambda: None)
    lines: list[str] = []
    config = _config(airflow_migration_strict=True, filterwarnings=lines)

    migration_strict.apply_migration_strict_filterwarnings(config)

    assert lines == []


def test_apply_migration_strict_filterwarnings_prepends_below_user_lines_idempotently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Insert the strict filters before user lines, once, even when called repeatedly."""

    monkeypatch.setattr(migration_strict, "installed_family", lambda: AirflowFamily.V2)
    lines = ["error::DeprecationWarning:flask_appbuilder"]
    config = _config(airflow_migration_strict=True, filterwarnings=lines)

    migration_strict.apply_migration_strict_filterwarnings(config)
    migration_strict.apply_migration_strict_filterwarnings(config)

    assert lines == [
        *migration_strict.MIGRATION_STRICT_FILTERWARNINGS,
        "error::DeprecationWarning:flask_appbuilder",
    ]


def test_warn_if_migration_strict_is_a_noop_silent_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stay silent when migration-strict mode is disabled, regardless of family."""

    monkeypatch.setattr(migration_strict, "installed_family", lambda: AirflowFamily.V3)
    calls: list[Warning] = []
    config = _config(airflow_migration_strict=False)
    config.issue_config_time_warning = lambda warning, _stacklevel: calls.append(warning)

    migration_strict.warn_if_migration_strict_is_a_noop(config)

    assert calls == []


def test_warn_if_migration_strict_is_a_noop_silent_on_v2(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stay silent when enabled on the Airflow 2.x family, where the mode does apply."""

    monkeypatch.setattr(migration_strict, "installed_family", lambda: AirflowFamily.V2)
    calls: list[Warning] = []
    config = _config(airflow_migration_strict=True)
    config.issue_config_time_warning = lambda warning, _stacklevel: calls.append(warning)

    migration_strict.warn_if_migration_strict_is_a_noop(config)

    assert calls == []


def test_warn_if_migration_strict_is_a_noop_warns_off_v2(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue one `MigrationStrictNoOpWarning` when enabled off the Airflow 2.x family."""

    monkeypatch.setattr(migration_strict, "installed_family", lambda: AirflowFamily.V3)
    calls: list[Warning] = []
    config = _config(airflow_migration_strict=True)
    config.issue_config_time_warning = lambda warning, _stacklevel: calls.append(warning)

    migration_strict.warn_if_migration_strict_is_a_noop(config)

    assert len(calls) == 1
    assert isinstance(calls[0], migration_strict.MigrationStrictNoOpWarning)


_FAKE_V2_CONFTEST = """
    import pytest_airflow_in_a_box.migration_strict as migration_strict

    from pytest_airflow_in_a_box._compat.capabilities import AirflowFamily

    migration_strict.installed_family = lambda: AirflowFamily.V2
    migration_strict.MIGRATION_STRICT_FILTERWARNINGS = (
        "error::fake_deprecation.FakeRemovedWarning",
    )
"""

_FAKE_DEPRECATION_MODULE = """
    class FakeRemovedWarning(DeprecationWarning):
        pass
"""


def test_migration_strict_collection_time_warning_is_not_fatal(
    pytester: pytest.Pytester,
) -> None:
    """Report, rather than fail, a strict-category warning raised during collection.

    pytest re-reads the ``filterwarnings`` ini list per warning context; the plugin
    mutates it from ``pytest_collection_finish``, which runs after collection, so a
    warning raised while a test module is imported never sees the error filter.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    pytester.makeconftest(_FAKE_V2_CONFTEST)
    pytester.makepyfile(fake_deprecation=_FAKE_DEPRECATION_MODULE)
    pytester.makepyfile(
        test_collection_warning="""
        import warnings

        from fake_deprecation import FakeRemovedWarning

        warnings.warn(FakeRemovedWarning("simulated 2->3 deprecation at import time"))


        def test_ok():
            assert True
        """
    )

    result = pytester.runpytest_subprocess("--airflow-migration-strict")

    result.assert_outcomes(passed=1)
    result.stdout.fnmatch_lines(["*FakeRemovedWarning*"])


def test_migration_strict_runtest_warning_fails_when_enabled(
    pytester: pytest.Pytester,
) -> None:
    """Fail a test whose body raises a strict-category warning, once enabled.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    pytester.makeconftest(_FAKE_V2_CONFTEST)
    pytester.makepyfile(fake_deprecation=_FAKE_DEPRECATION_MODULE)
    pytester.makepyfile(
        test_runtest_warning="""
        import warnings

        from fake_deprecation import FakeRemovedWarning


        def test_uses_deprecated_thing():
            warnings.warn(FakeRemovedWarning("simulated 2->3 deprecation at runtime"))
        """
    )

    result = pytester.runpytest_subprocess("--airflow-migration-strict")

    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(["*FakeRemovedWarning*"])


def test_migration_strict_runtest_warning_passes_when_disabled(
    pytester: pytest.Pytester,
) -> None:
    """Leave the same runtest warning non-fatal when the flag is not passed.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    pytester.makeconftest(_FAKE_V2_CONFTEST)
    pytester.makepyfile(fake_deprecation=_FAKE_DEPRECATION_MODULE)
    pytester.makepyfile(
        test_runtest_warning="""
        import warnings

        from fake_deprecation import FakeRemovedWarning


        def test_uses_deprecated_thing():
            warnings.warn(FakeRemovedWarning("simulated 2->3 deprecation at runtime"))
        """
    )

    result = pytester.runpytest_subprocess()

    result.assert_outcomes(passed=1)


def test_migration_strict_noop_warns_on_real_installed_family(
    pytester: pytest.Pytester,
) -> None:
    """Warn once, end-to-end, when enabled off this environment's real installed family.

    No fake seam here: this repo's own test environment installs Airflow 3.x, so the
    real `installed_family()` probe genuinely resolves to `AirflowFamily.V3`.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    pytester.makepyfile(
        test_ok="""
        def test_ok():
            assert True
        """
    )

    result = pytester.runpytest_subprocess("--airflow-migration-strict")

    result.assert_outcomes(passed=1)
    result.stdout.fnmatch_lines(["*MigrationStrictNoOpWarning*"])
