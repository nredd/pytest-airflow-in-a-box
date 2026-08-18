"""Reproduce the `airflow_local_settings` collision and prove the ini-configured fix.

`Pytester.popen` (used by `Pytester.run`/`runpytest_subprocess`) unconditionally injects
the current directory into the spawned child's `PYTHONPATH`
(https://github.com/pytest-dev/pytest/blob/main/src/_pytest/pytester.py). `PYTHONPATH`
entries land on `sys.path` at interpreter startup, before pytest -- or this plugin's own
bootstrap hooks -- ever run, which makes a project-root module importable regardless of
pytest's own conftest-loading timing. That silently defeats the entire point of the two
tests below marked "clean invocation": they drive a real subprocess directly, with
`PYTHONPATH` stripped, to actually exercise the timing this feature depends on. The
remaining tests in this file use ordinary `runpytest_subprocess` -- fine for proving
composition and validation *mechanics* work, just not for proving *when* they run.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest


def _run_clean_subprocess(cwd: Path, *args: str) -> subprocess.CompletedProcess[str]:
    """Run the real `pytest` console script with `PYTHONPATH` stripped from the child.

    Bypasses `Pytester.run`/`popen` entirely -- see the module docstring for why.

    Parameters:
        cwd: pathlib.Path to run the console script from.
        args: str containing additional command-line arguments.

    Returns:
        subprocess.CompletedProcess[str] with captured stdout/stderr.
    """

    console_script = Path(sys.executable).parent / "pytest"
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    return subprocess.run(
        [str(console_script), *args],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_foreign_airflow_local_settings_at_rootdir_is_rejected(
    pytester: pytest.Pytester,
) -> None:
    """Fail loudly instead of silently dropping SQLite engine tuning.

    A project-root `airflow_local_settings.py` is exactly the realistic collision: it
    sits earlier on `sys.path` than the plugin's own generated `AIRFLOW_HOME/config`
    entry, so Airflow's plain `import airflow_local_settings` would otherwise resolve
    the foreign module and silently drop this run's SQLite engine tuning. This proves the
    guard mechanism itself catches a real collision once one is visible on `sys.path`;
    see `test_foreign_airflow_local_settings_is_rejected_under_a_clean_invocation` below
    for proof it is wired to run at a point where that visibility actually holds under a
    realistic invocation.
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


def test_foreign_airflow_local_settings_is_rejected_under_a_clean_invocation(
    pytester: pytest.Pytester,
) -> None:
    """Fail loudly under a genuinely realistic invocation, not just via `Pytester`'s helper.

    Reproduced against the pre-fix code (collision check run during bootstrap, before
    pytest loads any conftest): with `PYTHONPATH` cleared, the project root is not yet on
    `sys.path` at that point under a plain `pytest` invocation, so the foreign module
    silently won the import race and no error was raised at all. The fix moves the check
    into `validate_configure` (called from `pytest_configure`), which runs after pytest's
    own conftest loading has made the project importable.
    """

    pytester.makeconftest("")
    (pytester.path / "airflow_local_settings.py").write_text(
        'DECOY = "a real project would put cluster policies here"\n', encoding="utf-8"
    )
    pytester.makepyfile("def test_never_runs():\n    assert False\n")

    result = _run_clean_subprocess(pytester.path, "-q")

    assert result.returncode != 0
    assert "`airflow_local_settings` resolves to" in result.stderr
    assert "not this run's generated" in result.stderr


def test_configured_local_settings_module_resolves_under_a_clean_invocation(
    pytester: pytest.Pytester,
) -> None:
    """Accept a legitimate project-root module under a genuinely realistic invocation.

    Reproduced against the pre-fix code (module resolution run during bootstrap, before
    pytest loads any conftest): with `PYTHONPATH` cleared, a project-root package is not
    yet importable at that point under a plain `pytest` invocation, so a perfectly valid
    `airflow_local_settings = myproject.cluster_policies` was rejected as "cannot be
    imported." The fix defers resolution to `validate_configure`, called after pytest's
    own conftest loading has made the project importable.
    """

    package_dir = pytester.path / "myproject"
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (package_dir / "cluster_policies.py").write_text(
        'MARKER = "policy applied"\n', encoding="utf-8"
    )
    pytester.makeconftest("")
    pytester.makeini("[pytest]\nairflow_local_settings = myproject.cluster_policies\n")
    pytester.makepyfile(
        """
        def test_uses_policy():
            import airflow_local_settings
            assert airflow_local_settings.MARKER == "policy applied"
        """
    )

    result = _run_clean_subprocess(pytester.path, "-q")

    assert result.returncode == 0, result.stderr


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


def test_configured_ini_module_cannot_clobber_create_metadata_engine(
    pytester: pytest.Pytester,
) -> None:
    """Keep the plugin's own metadata-engine factory even if the policy module defines one."""

    pytester.makepyfile(
        policy_clobber="""
        def create_metadata_engine(*args, **kwargs):
            raise AssertionError("user override must never run")
        __all__ = ("create_metadata_engine",)
        """
    )
    pytester.makeini("[pytest]\nairflow_local_settings = policy_clobber\n")
    pytester.makepyfile(
        test_uses_policy="""
        from pytest_airflow_in_a_box.storage import create_metadata_engine as plugin_engine

        def test_engine_not_clobbered():
            import airflow_local_settings
            assert airflow_local_settings.create_metadata_engine is plugin_engine
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
