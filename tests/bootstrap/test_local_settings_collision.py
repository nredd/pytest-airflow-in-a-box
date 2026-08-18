"""Reproduce the `airflow_local_settings` collision and prove the ini-configured fix."""

from __future__ import annotations

import pytest


def test_foreign_airflow_local_settings_at_rootdir_is_rejected(
    pytester: pytest.Pytester,
) -> None:
    """Fail loudly instead of silently dropping SQLite engine tuning.

    A project-root `airflow_local_settings.py` is exactly the realistic collision: it
    sits earlier on `sys.path` than the plugin's own generated `AIRFLOW_HOME/config`
    entry, so Airflow's plain `import airflow_local_settings` would otherwise resolve
    the foreign module and silently drop this run's SQLite engine tuning.
    """

    (pytester.path / "airflow_local_settings.py").write_text(
        'DECOY = "a real project would put cluster policies here"\n', encoding="utf-8"
    )
    pytester.makepyfile("def test_never_runs():\n    assert False\n")

    result = pytester.runpytest_subprocess("-q")

    assert result.ret != 0
    result.stderr.fnmatch_lines(
        ["*`airflow_local_settings` resolves to*not this run's generated*"]
    )


def test_configured_ini_module_composes_into_generated_local_settings(
    pytester: pytest.Pytester,
) -> None:
    """Compose an `__all__`-scoped cluster-policy module, unioned with our own export."""

    pytester.makepyfile(
        policy_module="""
        MARKER = "cluster-policy-applied"
        __all__ = ("MARKER",)
        """
    )
    pytester.makeini("[pytest]\nairflow_local_settings = policy_module\n")
    pytester.makepyfile(
        test_uses_policy="""
        def test_policy_composed():
            import airflow_local_settings
            assert airflow_local_settings.MARKER == "cluster-policy-applied"
            assert airflow_local_settings.create_metadata_engine is not None
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)


def test_configured_ini_module_without_dunder_all_still_composes(
    pytester: pytest.Pytester,
) -> None:
    """Fall back to every non-dunder attribute when the configured module has no `__all__`."""

    pytester.makepyfile(
        policy_module_no_all="""
        PUBLIC_MARKER = "no-all-cluster-policy"
        """
    )
    pytester.makeini("[pytest]\nairflow_local_settings = policy_module_no_all\n")
    pytester.makepyfile(
        test_uses_policy="""
        def test_policy_composed():
            import airflow_local_settings
            assert airflow_local_settings.PUBLIC_MARKER == "no-all-cluster-policy"
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)


def test_unresolvable_ini_module_fails_loudly(pytester: pytest.Pytester) -> None:
    """Fail before any test runs when the configured module cannot be imported."""

    pytester.makeini("[pytest]\nairflow_local_settings = definitely_not_a_real_module_xyz\n")
    pytester.makepyfile("def test_never_runs():\n    assert False\n")

    result = pytester.runpytest_subprocess("-q")

    assert result.ret != 0
    result.stderr.fnmatch_lines(["*names a module that cannot be imported*"])


def test_ini_module_rejects_file_path_value(pytester: pytest.Pytester) -> None:
    """Fail before any test runs when the ini value is a file path, not a module path."""

    pytester.makeini("[pytest]\nairflow_local_settings = ./policies.py\n")
    pytester.makepyfile("def test_never_runs():\n    assert False\n")

    result = pytester.runpytest_subprocess("-q")

    assert result.ret != 0
    result.stderr.fnmatch_lines(["*must be a dotted module path, not a file path*"])
