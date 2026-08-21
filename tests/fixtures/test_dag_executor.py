"""Test how `run_dag` wires its `executor` argument to the executor-driven adapter.

`tests/compat/test_executor_compat.py` covers the adapter's own mechanics and
`tests/enduser/test_executor_run.py` covers real runs. This file is about the fixture
seam: the guards that fire before any metadata is written, and the timeout option the
fixture resolves on the caller's behalf.
"""

from __future__ import annotations

from typing import Any

import pytest

from pytest_airflow_in_a_box.fixtures.dag import _DagRunner, _executor_timeout

pytestmark = pytest.mark.requires_airflow3


def test_run_triggerer_cannot_be_combined_with_an_executor(
    request: pytest.FixtureRequest,
) -> None:
    """Refuse a combination with no coherent meaning, rather than ignoring one half.

    Resuming a deferred task is a triggerer's job; an executor-driven run settles a
    deferring instance as `deferred` and moves on.

    Parameters:
        request: pytest.FixtureRequest the runner holds for executor-driven runs.
    """

    runner = _DagRunner(request)
    # The guard fires before `dag` is looked at, which is the point: no Dag, no
    # metadata session, and no api-server started.
    dag: Any = object()

    with pytest.raises(ValueError, match="cannot be combined with `executor`"):
        runner(dag, executor="LocalExecutor", run_triggerer=True)


def _config(*, option: object = None, ini: object = "300") -> Any:
    """Create a minimal configuration double for the timeout reader.

    `pytest.Pytester.parseconfig` cannot be used here: it re-enters this plugin's
    bootstrap in a process where the outer suite has already imported Airflow, which
    bootstrap refuses by design.

    Parameters:
        option: object containing the parsed `--airflow-executor-timeout` value.
        ini: object containing the `airflow_executor_timeout` ini value.

    Returns:
        Any shaped like the configuration surface under test.
    """

    class _Config:
        """Report exactly the two values the reader consults."""

        def getoption(self, name: str) -> object:
            """Return the parsed command-line value.

            Parameters:
                name: str naming the option, unused.

            Returns:
                object containing the configured command-line value.
            """

            del name
            return option

        def getini(self, name: str) -> object:
            """Return the ini value.

            Parameters:
                name: str naming the ini option, unused.

            Returns:
                object containing the configured ini value.
            """

            del name
            return ini

    return _Config()


def test_executor_timeout_prefers_the_command_line_over_the_ini() -> None:
    """Resolve `--airflow-executor-timeout` ahead of `airflow_executor_timeout`."""

    assert _executor_timeout(_config(option="22", ini="11")) == 22.0


def test_executor_timeout_falls_back_to_the_ini_option() -> None:
    """Resolve the ini option when no command-line value is given."""

    assert _executor_timeout(_config(ini="11")) == 11.0


def test_executor_timeout_carries_a_registered_default() -> None:
    """Keep the registered default and the documented one from drifting apart."""

    from pytest_airflow_in_a_box._compat.executor import DEFAULT_EXECUTOR_TIMEOUT

    assert _executor_timeout(_config()) == DEFAULT_EXECUTOR_TIMEOUT


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("soon", "must be a number"),
        ("0", "must be positive"),
        ("-1", "must be positive"),
        (30, "must be a number"),
    ],
    ids=["not-a-number", "zero", "negative", "wrong-type"],
)
def test_executor_timeout_rejects_an_unusable_command_line_value(
    value: object, expected: str
) -> None:
    """Fail configuration loudly rather than hanging or never waiting at all.

    Parameters:
        value: object containing the command-line value under test.
        expected: str containing the message fragment the failure must carry.
    """

    with pytest.raises(pytest.UsageError, match=rf"`--airflow-executor-timeout`.*{expected}"):
        _executor_timeout(_config(option=value))


def test_executor_timeout_rejects_an_unusable_ini_value() -> None:
    """Name the ini option, not the command-line one, when the ini value is at fault."""

    with pytest.raises(pytest.UsageError, match=r"`airflow_executor_timeout`.*must be a number"):
        _executor_timeout(_config(ini=["300"]))
