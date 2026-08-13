"""Verify `--airflow-doctor`'s executor section against a real Airflow install.

`tests/test_doctor.py` proves `_executor_section`'s branch logic against fabricated
`BootstrapState` values and a monkeypatched `_resolve_executor`, but never against a
real Airflow process -- and the 2.x compat CI legs run only this directory, not
`tests/test_doctor.py`. This module closes that gap: this plugin's default bootstrap
sets no `core.executor`, so on a real Airflow 2.x install `unit_test_mode` overlays
`unit_tests.cfg` and resolves `LocalExecutor` under the default SQLite backend, and
the live report must flag it exactly as `--airflow-doctor` promises. 3.x has no
equivalent SQLite/executor gate, so the live report must never flag it there.

References:
    https://github.com/nredd/pytest-airflow-in-a-box/issues/105
"""

from __future__ import annotations

import pytest

from pytest_airflow_in_a_box._compat.capabilities import AirflowFamily, installed_family

pytestmark = pytest.mark.compat


def test_doctor_executor_section_reflects_the_real_installed_family(
    pytester: pytest.Pytester,
) -> None:
    """Report the real resolved `core.executor` and flag 2.x's SQLite conflict live."""

    pytester.makepyfile("def test_never_runs():\n    assert False\n")

    result = pytester.runpytest_subprocess("--airflow-doctor")

    assert result.ret == 0
    output = result.stdout.str()
    assert "## Executor" in output
    assert "could not resolve" not in output
    if installed_family() is AirflowFamily.V2:
        result.stdout.fnmatch_lines(["*INCOMPATIBLE*unit_tests.cfg*ready_to_reschedule*"])
    else:
        assert "INCOMPATIBLE" not in output
