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
