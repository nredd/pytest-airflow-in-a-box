"""Unit and subprocess tests for `plugin`-module error rendering.

References:
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.UsageError
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pytest_airflow_in_a_box import plugin
from pytest_airflow_in_a_box._compat import AirflowCompatibilityError


def test_database_incompatibility_renders_as_usage_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Render `AirflowCompatibilityError` as a single actionable usage error.

    Parameters:
        monkeypatch: pytest.MonkeyPatch replacing the database initialization seam.
        tmp_path: Path providing a throwaway bootstrap root.
    """

    failure = AirflowCompatibilityError("Apache Airflow 2.x is installed")

    def broken_database(root: Path) -> None:
        """Raise the representative installation failure."""

        del root
        raise failure

    monkeypatch.setattr(plugin, "ensure_database", broken_database)

    with pytest.raises(pytest.UsageError, match=r"Airflow 2\.x is installed") as caught:
        plugin._ensure_database_or_usage_error(tmp_path)

    assert caught.value.__cause__ is failure


_BROKEN_INSTALLATION_CONFTEST = """
    import pytest_airflow_in_a_box.plugin as plugin
    from pytest_airflow_in_a_box._compat import AirflowCompatibilityError

    def _broken_database(root):
        raise AirflowCompatibilityError(
            "No Airflow distribution is installed (fake probe)"
        )

    plugin.ensure_database = _broken_database
"""

_DATABASE_SUITE = """
    import pytest

    pytestmark = pytest.mark.db_test

    def test_first() -> None:
        assert True

    def test_second() -> None:
        assert True
"""


def test_incompatibility_renders_one_error_line_in_process(
    pytester: pytest.Pytester,
) -> None:
    """Abort a single-process run with one actionable `ERROR:` line, not a traceback.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    pytester.makeconftest(_BROKEN_INSTALLATION_CONFTEST)
    pytester.makepyfile(test_broken=_DATABASE_SUITE)

    result = pytester.runpytest_subprocess()

    assert result.ret == pytest.ExitCode.USAGE_ERROR
    output = result.stdout.str() + result.stderr.str()
    assert "No Airflow distribution is installed (fake probe)" in output
    assert "INTERNALERROR" not in output


def test_incompatibility_renders_per_test_errors_under_xdist(
    pytester: pytest.Pytester,
) -> None:
    """Report the installation problem per test on xdist workers, never a crashed node.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    pytester.makeconftest(_BROKEN_INSTALLATION_CONFTEST)
    pytester.makepyfile(test_broken=_DATABASE_SUITE)

    result = pytester.runpytest_subprocess("-n", "2")

    result.assert_outcomes(errors=2)
    output = result.stdout.str() + result.stderr.str()
    assert "No Airflow distribution is installed (fake probe)" in output
    assert "INTERNALERROR" not in output
    assert "crashed" not in output


_FAKE_WARNING_ENSURE_DATABASE_CONFTEST = """
    import warnings

    import pytest_airflow_in_a_box.plugin as plugin


    class FakeAirflowImportWarning(UserWarning):
        pass


    def _fake_ensure_database(root):
        warnings.warn(
            FakeAirflowImportWarning("simulated Airflow import-time warning")
        )


    plugin.ensure_database = _fake_ensure_database
"""

_ERROR_ON_USER_WARNING_INI = "[pytest]\nfilterwarnings =\n    error::UserWarning\n"


def test_ensure_database_warning_does_not_fail_first_test_under_xdist(
    pytester: pytest.Pytester,
) -> None:
    """Suppress a warning `ensure_database` raises, even under a user `error::` filter.

    Regression test for a latent bug (#43): on an xdist worker, the eager
    `pytest_collection_finish` database initialization is skipped, so
    `_ensure_database_or_usage_error` runs from the `pytest_runtest_setup` safety net
    instead -- inside the runtest phase's own warning context. A warning raised there
    under an active `error::` filter turned into an exception unrelated to
    `AirflowCompatibilityError`, so it went unhandled and failed the worker's first
    test with a misleading error. `_ensure_database_or_usage_error` now wraps the
    `ensure_database` call in its own default-filter warnings context, unconditionally,
    so both workers' first test passes here even with a real user `error::UserWarning`
    filter active.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    pytester.makeconftest(_FAKE_WARNING_ENSURE_DATABASE_CONFTEST)
    pytester.makeini(_ERROR_ON_USER_WARNING_INI)
    pytester.makepyfile(test_warned=_DATABASE_SUITE)

    result = pytester.runpytest_subprocess("-n", "2")

    result.assert_outcomes(passed=2)
    output = result.stdout.str() + result.stderr.str()
    assert "INTERNALERROR" not in output
    assert "crashed" not in output
