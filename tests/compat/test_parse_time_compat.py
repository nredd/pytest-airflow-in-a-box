"""Test parse-time Variable and Connection resolution against the installed release.

References:
    https://github.com/apache/airflow/issues/51816
    https://github.com/apache/airflow/blob/main/task-sdk/src/airflow/sdk/execution_time/context.py
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from airflow.sdk.execution_time.comms import ErrorResponse, ErrorType, GetConnection, GetVariable

from pytest_airflow_in_a_box._compat import parse_time
from pytest_airflow_in_a_box._compat.capabilities import AirflowCapabilities, resolve_capabilities
from pytest_airflow_in_a_box._compat.parse_time import (
    SUPERVISOR_COMMS_ATTRIBUTE,
    ParseTimeComms,
    parse_time_supervision,
)
from pytest_airflow_in_a_box.bootstrap import get_bootstrap_state
from pytest_airflow_in_a_box.types import AirflowConnections, AirflowVariables

pytestmark = [pytest.mark.compat, pytest.mark.db_test]


class _BrokenSession:
    """Stand in for a metadata session whose close fails."""

    def close(self) -> None:
        """Fail the way a session on a dropped connection fails.

        Raises:
            RuntimeError: Always, standing in for a dropped connection.
        """

        raise RuntimeError("session close failed")


def _broken_session() -> Any:
    """Build a metadata session double whose close fails.

    Returns:
        Any standing in for a session on a dropped connection.
    """

    broken: Any = _BrokenSession()
    return broken


@pytest.fixture
def run_root(pytestconfig: pytest.Config) -> Path:
    """Locate the session's disposable run root.

    Parameters:
        pytestconfig: pytest.Config carrying the session's bootstrap state.

    Returns:
        pathlib.Path of the session's disposable run root directory.
    """

    return get_bootstrap_state(pytestconfig).root


def test_lookups_resolve_seeded_metadata_rows(
    run_root: Path,
    airflow_variables: AirflowVariables,
    airflow_connections: AirflowConnections,
) -> None:
    """Resolve a seeded Variable and Connection through supervisor messages.

    Parameters:
        run_root: pathlib.Path of the session's disposable run root directory.
        airflow_variables: AirflowVariables committing the seeded Variable.
        airflow_connections: AirflowConnections committing the seeded Connection.
    """

    airflow_variables({"parse_time_var": "parse-value"})
    airflow_connections({"parse_time_conn": {"conn_type": "http", "host": "parse-host"}})

    comms = ParseTimeComms(run_root=run_root)
    try:
        variable = comms.send(GetVariable(key="parse_time_var"))
        connection = comms.send(GetConnection(conn_id="parse_time_conn"))
    finally:
        comms.close()

    assert variable.value == "parse-value"
    assert connection.host == "parse-host"
    assert connection.conn_type == "http"


def test_lookups_report_absent_rows_with_a_seeding_hint(run_root: Path) -> None:
    """Answer an unseeded lookup the way a real supervisor reports not-found.

    Parameters:
        run_root: pathlib.Path of the session's disposable run root directory.
    """

    comms = ParseTimeComms(run_root=run_root)
    try:
        variable = comms.send(GetVariable(key="parse_time_absent_var"))
        connection = comms.send(GetConnection(conn_id="parse_time_absent_conn"))
    finally:
        comms.close()

    assert isinstance(variable, ErrorResponse)
    assert variable.error is ErrorType.VARIABLE_NOT_FOUND
    assert "airflow_variables" in variable.detail["hint"]
    assert isinstance(connection, ErrorResponse)
    assert connection.error is ErrorType.CONNECTION_NOT_FOUND
    assert "airflow_connections" in connection.detail["hint"]


def test_connection_fields_the_release_does_not_declare_are_dropped(
    run_root: Path,
    airflow_connections: AirflowConnections,
) -> None:
    """Answer with only the fields the release's own `ConnectionResult` accepts.

    `description` is a legal seeding field but is absent from the generated comms
    model, which forbids extras before 3.3.0 -- an unfiltered payload would fail
    validation inside `send` and reach the Dag as a plain not-found.

    Parameters:
        run_root: pathlib.Path of the session's disposable run root directory.
        airflow_connections: AirflowConnections committing the seeded Connection.
    """

    airflow_connections(
        {
            "parse_time_described_conn": {
                "conn_type": "http",
                "host": "described-host",
                "description": "a field the comms model does not declare",
            }
        }
    )

    comms = ParseTimeComms(run_root=run_root)
    try:
        fields = comms._lookup_connection("parse_time_described_conn")
        connection = comms.send(GetConnection(conn_id="parse_time_described_conn"))
    finally:
        comms.close()

    assert fields is not None
    assert "description" not in fields
    assert connection.host == "described-host"


def test_explicit_overrides_answer_without_opening_a_session(run_root: Path) -> None:
    """Answer from the constructor overrides without touching the database at all.

    Parameters:
        run_root: pathlib.Path of the session's disposable run root directory.
    """

    comms = ParseTimeComms(
        run_root=run_root,
        variables={"parse_time_var": "override-value"},
        connections={"parse_time_conn": {"conn_type": "http", "host": "override-host"}},
    )
    try:
        variable = comms.send(GetVariable(key="parse_time_var"))
        connection = comms.send(GetConnection(conn_id="parse_time_conn"))
        opened_session = comms._session
    finally:
        comms.close()

    assert variable.value == "override-value"
    assert connection.host == "override-host"
    assert opened_session is None


def test_close_is_idempotent_before_any_lookup(run_root: Path) -> None:
    """Close without a session, which is the state of every parse that never looked up.

    Parameters:
        run_root: pathlib.Path of the session's disposable run root directory.
    """

    comms = ParseTimeComms(run_root=run_root)
    comms.close()
    comms.close()

    assert comms._session is None


def test_lookup_failures_are_logged_before_they_propagate(
    run_root: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Report the real cause of a lookup failure Airflow would flatten to not-found.

    Airflow's worker secrets backend swallows every exception a supervisor call raises
    and answers not-found instead, so a metadata database that is down looks exactly
    like an unseeded row unless the shim logs it here.

    Parameters:
        run_root: pathlib.Path of the session's disposable run root directory.
        monkeypatch: pytest.MonkeyPatch breaking the metadata session.
        caplog: pytest.LogCaptureFixture capturing the emitted report.
    """

    def _explode() -> object:
        """Fail the way an unreachable metadata database fails.

        Raises:
            RuntimeError: Always, standing in for an unreachable database.
        """

        raise RuntimeError("metadata database is unreachable")

    monkeypatch.setattr(ParseTimeComms, "_metadata_session", lambda _self: _explode())

    comms = ParseTimeComms(run_root=run_root)
    with caplog.at_level("ERROR", logger=parse_time.LOGGER.name):
        with pytest.raises(RuntimeError, match="metadata database is unreachable"):
            comms._lookup_variable("parse_time_var")
        with pytest.raises(RuntimeError, match="metadata database is unreachable"):
            comms._lookup_connection("parse_time_conn")

    assert "Variable `parse_time_var`" in caplog.text
    assert "Connection `parse_time_conn`" in caplog.text
    assert caplog.text.count("metadata database is unreachable") == 2


def test_absent_rows_are_logged_with_the_seeding_hint(
    run_root: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Log the hint Airflow discards before it reaches the failing Dag.

    Airflow's backend drops the `ErrorResponse` detail and raises its own generic
    not-found, so the log is where the seeding hint actually reaches the consumer. It
    is `debug` rather than `warning` because `Variable.get(key, default=...)` makes a
    miss an ordinary, intentional outcome.

    Parameters:
        run_root: pathlib.Path of the session's disposable run root directory.
        caplog: pytest.LogCaptureFixture capturing the emitted hints.
    """

    comms = ParseTimeComms(run_root=run_root)
    try:
        with caplog.at_level("DEBUG", logger=parse_time.LOGGER.name):
            comms.send(GetVariable(key="parse_time_absent_var"))
            comms.send(GetConnection(conn_id="parse_time_absent_conn"))
    finally:
        comms.close()

    assert "airflow_variables({'parse_time_absent_var': ...})" in caplog.text
    assert "airflow_connections({'parse_time_absent_conn': {'conn_type': ...}})" in caplog.text


def test_close_drops_a_session_that_fails_to_close(run_root: Path) -> None:
    """Never keep a half-torn-down session referenced for a later lookup.

    Parameters:
        run_root: pathlib.Path of the session's disposable run root directory.
    """

    comms = ParseTimeComms(run_root=run_root)
    comms._session = _broken_session()

    with pytest.raises(RuntimeError, match="session close failed"):
        comms.close()

    assert comms._session is None


def test_supervision_survives_a_failing_session_close(
    run_root: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Keep a correctly parsed result when only the teardown close fails.

    The caller wraps this block in its own construction error, so letting a teardown
    failure escape would relabel a successful parse as a parse failure.

    Parameters:
        run_root: pathlib.Path of the session's disposable run root directory.
        caplog: pytest.LogCaptureFixture capturing the teardown report.
    """

    comms = ParseTimeComms(run_root=run_root)
    with (
        caplog.at_level("WARNING", logger=parse_time.LOGGER.name),
        parse_time_supervision(comms),
    ):
        comms._session = _broken_session()

    assert "Could not close the parse-time metadata session" in caplog.text
    assert comms._session is None


def test_supervision_installs_and_removes_an_absent_attribute(run_root: Path) -> None:
    """Delete the module global again when the release did not define one.

    Every certified 3.x release annotates `SUPERVISOR_COMMS` without assigning it, so
    the attribute is genuinely absent until something installs it -- which is exactly
    the `ImportError: cannot import name 'SUPERVISOR_COMMS'` this shim answers.

    Parameters:
        run_root: pathlib.Path of the session's disposable run root directory.
    """

    task_runner = parse_time._task_runner_module()
    assert not hasattr(task_runner, SUPERVISOR_COMMS_ATTRIBUTE)

    comms = ParseTimeComms(run_root=run_root)
    with parse_time_supervision(comms):
        assert task_runner.SUPERVISOR_COMMS is comms

    assert not hasattr(task_runner, SUPERVISOR_COMMS_ATTRIBUTE)


def test_supervision_restores_a_previous_none_attribute(
    run_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Restore an explicit None rather than deleting an attribute the release owns.

    Parameters:
        run_root: pathlib.Path of the session's disposable run root directory.
        monkeypatch: pytest.MonkeyPatch installing the pre-existing attribute.
    """

    task_runner = parse_time._task_runner_module()
    monkeypatch.setattr(task_runner, SUPERVISOR_COMMS_ATTRIBUTE, None, raising=False)

    comms = ParseTimeComms(run_root=run_root)
    with parse_time_supervision(comms):
        assert task_runner.SUPERVISOR_COMMS is comms

    assert task_runner.SUPERVISOR_COMMS is None


def test_supervision_leaves_a_live_endpoint_alone(
    run_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Never replace a supervisor endpoint an in-process task run already installed.

    Parameters:
        run_root: pathlib.Path of the session's disposable run root directory.
        monkeypatch: pytest.MonkeyPatch installing the live endpoint.
    """

    task_runner = parse_time._task_runner_module()
    live = object()
    monkeypatch.setattr(task_runner, SUPERVISOR_COMMS_ATTRIBUTE, live, raising=False)

    comms = ParseTimeComms(run_root=run_root)
    with parse_time_supervision(comms):
        assert task_runner.SUPERVISOR_COMMS is live

    assert task_runner.SUPERVISOR_COMMS is live


def test_supervision_restores_the_attribute_after_a_failure(run_root: Path) -> None:
    """Restore the module global even when the enclosed parse raises.

    Parameters:
        run_root: pathlib.Path of the session's disposable run root directory.
    """

    task_runner = parse_time._task_runner_module()
    comms = ParseTimeComms(run_root=run_root)
    with (
        pytest.raises(ValueError, match="representative parse failure"),
        parse_time_supervision(comms),
    ):
        raise ValueError("representative parse failure")

    assert not hasattr(task_runner, SUPERVISOR_COMMS_ATTRIBUTE)


def test_supervision_is_a_noop_without_comms() -> None:
    """Leave Airflow's own resolution untouched when the consumer opted out."""

    task_runner = parse_time._task_runner_module()
    with parse_time_supervision(None):
        assert not hasattr(task_runner, SUPERVISOR_COMMS_ATTRIBUTE)


def test_supervision_is_a_noop_without_a_task_sdk(
    run_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Skip the shim on a family that reads the metastore directly at parse time.

    Parameters:
        run_root: pathlib.Path of the session's disposable run root directory.
        monkeypatch: pytest.MonkeyPatch replacing the resolved capabilities.
    """

    without_sdk = replace(resolve_capabilities(), has_task_sdk=False)

    def _resolve() -> AirflowCapabilities:
        """Report the fake capabilities of a family without a Task SDK.

        Returns:
            AirflowCapabilities carrying `has_task_sdk` False.
        """

        return without_sdk

    monkeypatch.setattr(parse_time, "resolve_capabilities", _resolve)

    task_runner = parse_time._task_runner_module()
    comms = ParseTimeComms(run_root=run_root)
    with parse_time_supervision(comms):
        assert not hasattr(task_runner, SUPERVISOR_COMMS_ATTRIBUTE)


def test_supervision_resets_the_secret_cache(
    run_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Drop Airflow's secrets cache on both edges of the parse.

    Parameters:
        run_root: pathlib.Path of the session's disposable run root directory.
        monkeypatch: pytest.MonkeyPatch counting the cache resets.
    """

    resets: list[Any] = []

    def _record_reset() -> None:
        """Record one cache reset instead of performing it."""

        resets.append(object())

    monkeypatch.setattr(parse_time, "_reset_secret_cache", _record_reset)

    comms = ParseTimeComms(run_root=run_root)
    with parse_time_supervision(comms):
        assert len(resets) == 1

    assert len(resets) == 2


def test_reset_secret_cache_clears_airflows_cache() -> None:
    """Call through to Airflow's own cache reset."""

    # Deferred import: the module is part of the Task SDK's execution-time surface.
    from airflow.sdk.execution_time.cache import SecretCache

    SecretCache._cache = {}
    parse_time._reset_secret_cache()

    assert SecretCache._cache is None
