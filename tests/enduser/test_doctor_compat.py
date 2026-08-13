"""Verify `--airflow-doctor`'s executor section against a real Airflow install.

`tests/test_doctor.py` proves `_executor_section`'s branch logic against fabricated
`BootstrapState` values and a monkeypatched `_resolve_executor`, but never against a
real Airflow process -- and the 2.x compat CI legs run only this directory, not
`tests/test_doctor.py`. This module closes that gap: bootstrap env-pins
`AIRFLOW__CORE__EXECUTOR=SequentialExecutor` on the 2.x family, outranking the
`unit_tests.cfg` overlay's hard-coded `LocalExecutor`, so the live report must show
`SequentialExecutor` resolved and NO INCOMPATIBLE flag on a default 2.x run. A consumer
override to a multi-threaded executor is what the flag is for; the second test proves
that path live through `airflow_executor_override`. 3.x has no equivalent
SQLite/executor gate, so the live report must never flag there either.

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
    """Report the env-pinned 2.x executor with no flag, and stay silent on 3.x.

    Parameters:
        pytester: pytest.Pytester running the doctor invocation in a subprocess.
    """

    pytester.makepyfile("def test_never_runs():\n    assert False\n")

    result = pytester.runpytest_subprocess("--airflow-doctor")

    assert result.ret == 0
    output = result.stdout.str()
    assert "## Executor" in output
    assert "could not resolve" not in output
    assert "INCOMPATIBLE" not in output
    if installed_family() is AirflowFamily.V2:
        result.stdout.fnmatch_lines(["*`core.executor`: `SequentialExecutor`*"])


def test_doctor_flags_a_live_2x_multi_threaded_executor_override(
    pytester: pytest.Pytester,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Flag a real 2.x run whose ambient environment overrides the executor pin.

    Bootstrap's `SequentialExecutor` pin defers to an ambient
    `AIRFLOW__CORE__EXECUTOR` (deliberate consumer configuration), so exporting
    `LocalExecutor` into the subprocess environment is exactly the consumer state the
    INCOMPATIBLE diagnostic exists for, and the live report must attribute it.

    Parameters:
        pytester: pytest.Pytester running the doctor invocation in a subprocess.
        monkeypatch: pytest.MonkeyPatch exporting the ambient executor override.
    """

    if installed_family() is not AirflowFamily.V2:
        pytest.skip("the SQLite executor gate exists on the Airflow 2.x family only")

    pytester.makepyfile("def test_never_runs():\n    assert False\n")
    monkeypatch.setenv("AIRFLOW__CORE__EXECUTOR", "LocalExecutor")

    result = pytester.runpytest_subprocess("--airflow-doctor")

    assert result.ret == 0
    result.stdout.fnmatch_lines(["*`core.executor`: `LocalExecutor`*"])
    result.stdout.fnmatch_lines(["*INCOMPATIBLE*ready_to_reschedule*"])
