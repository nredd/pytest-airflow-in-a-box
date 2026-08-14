"""Test the `--airflow-doctor` diagnostics report."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from pytest_airflow_in_a_box import doctor
from pytest_airflow_in_a_box._compat.capabilities import (
    AirflowCapabilities,
    AirflowCompatibilityError,
    AirflowFamily,
    ApiSurface,
    DagBagLocation,
    DagRunInterface,
    ParamsLocation,
    TaskInstanceRunner,
    TimezoneLocation,
)
from pytest_airflow_in_a_box.airflow_cfg import sqlite_url
from pytest_airflow_in_a_box.bootstrap import STATE_VERSION, BootstrapState

CAPABILITIES = AirflowCapabilities(
    release=(3, 3, 0),
    family=AirflowFamily.V3,
    dag_bag_location=DagBagLocation.DAG_PROCESSING,
    dag_bag_supports_include_examples=False,
    task_instance_runner=TaskInstanceRunner.SDK_RUN_TASK,
    refresh_from_task_supports_dag_run=True,
    startup_details_supports_sentry=True,
    runtime_task_instance_supports_queue=True,
    has_task_sdk=True,
    uses_structlog=True,
    has_dag_versioning=True,
    dagrun_interface=DagRunInterface.LOGICAL_DATE,
    api_surface=ApiSurface.API_SERVER,
    params_location=ParamsLocation.SDK,
    timezone_location=TimezoneLocation.SDK,
)


def _state(
    root: Path,
    *,
    storage_reason: str = "shared-memory",
    network_storage: bool = False,
    db_backend: str = "sqlite",
    sql_alchemy_conn: str | None = None,
    family: str = AirflowFamily.V3.value,
) -> BootstrapState:
    """Build a fabricated bootstrap state for report-section unit tests.

    Parameters:
        root: pathlib.Path used as the fake run root.
        storage_reason: str containing the fabricated storage ladder reason.
        network_storage: bool containing the fabricated network filesystem flag.
        db_backend: str containing the fabricated backend tier.
        sql_alchemy_conn: str | None containing the fabricated database URL, defaulting
            to a SQLite URL below `root`.
        family: str containing the fabricated `AirflowFamily` value.

    Returns:
        BootstrapState containing fabricated but internally consistent fields.
    """

    return BootstrapState(
        version=STATE_VERSION,
        owner_pid=1234,
        root=root,
        dags_folder=root / "dags",
        logs_folder=root / "logs",
        database_path=root / "airflow.db",
        password_file=root / "simple_auth_manager_passwords.json",
        config_path=root / "airflow.cfg",
        jwt_secret="secret",
        fernet_key="fernet",
        storage_reason=storage_reason,
        network_storage=network_storage,
        sql_alchemy_conn=sql_alchemy_conn or sqlite_url(root / "airflow.db"),
        db_backend=db_backend,
        family=family,
    )


def test_storage_section_reports_reason_and_network_flag(tmp_path: Path) -> None:
    """Render the storage ladder reason and network filesystem flag."""

    state = _state(tmp_path, storage_reason="caller-temp", network_storage=True)

    lines = doctor._storage_section(state)

    assert "- Reason: `caller-temp`" in lines
    assert "- Network filesystem: `True`" in lines


def test_database_section_reports_home_backend_and_scheme(tmp_path: Path) -> None:
    """Render `AIRFLOW_HOME`, backend tier, and database URL scheme."""

    state = _state(tmp_path, db_backend="postgres", sql_alchemy_conn="postgresql://u:p@h/db")

    lines = doctor._database_section(state)

    assert f"- `AIRFLOW_HOME`: `{tmp_path}`" in lines
    assert "- Backend tier: `postgres`" in lines
    assert "- Database URL scheme: `postgresql`" in lines


def test_executor_section_reports_resolved_executor(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Render the resolved `core.executor` value without flagging a compatible setup."""

    state = _state(tmp_path, db_backend="postgres", family=AirflowFamily.V3.value)
    monkeypatch.setattr(doctor, "_resolve_executor", lambda: "LocalExecutor")

    lines = doctor._executor_section(state)

    assert "- `core.executor`: `LocalExecutor`" in lines
    assert not any("INCOMPATIBLE" in line for line in lines)


def test_executor_section_flags_2x_sqlite_local_executor(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Flag a 2.x SQLite override that resolved past the plugin's default pin."""

    state = _state(tmp_path, db_backend="sqlite", family=AirflowFamily.V2.value)
    monkeypatch.setattr(doctor, "_resolve_executor", lambda: "LocalExecutor")

    lines = doctor._executor_section(state)

    assert any(
        "INCOMPATIBLE" in line and "ready_to_reschedule" in line and "SequentialExecutor" in line
        for line in lines
    )


def test_executor_section_does_not_flag_2x_sqlite_sequential_executor(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Do not flag the 2.x SQLite combination once `SequentialExecutor` is pinned."""

    state = _state(tmp_path, db_backend="sqlite", family=AirflowFamily.V2.value)
    monkeypatch.setattr(doctor, "_resolve_executor", lambda: "SequentialExecutor")

    lines = doctor._executor_section(state)

    assert not any("INCOMPATIBLE" in line for line in lines)


def test_executor_section_does_not_flag_3x_sqlite_local_executor(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Scope the conflict bullet to 2.x only, not the general SQLite + executor case."""

    state = _state(tmp_path, db_backend="sqlite", family=AirflowFamily.V3.value)
    monkeypatch.setattr(doctor, "_resolve_executor", lambda: "LocalExecutor")

    lines = doctor._executor_section(state)

    assert not any("INCOMPATIBLE" in line for line in lines)


def test_executor_section_does_not_flag_2x_postgres_local_executor(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Do not flag 2.x with a non-single-threaded executor once Postgres backs it."""

    state = _state(
        tmp_path,
        db_backend="postgres",
        sql_alchemy_conn="postgresql://u:p@h/db",
        family=AirflowFamily.V2.value,
    )
    monkeypatch.setattr(doctor, "_resolve_executor", lambda: "LocalExecutor")

    lines = doctor._executor_section(state)

    assert not any("INCOMPATIBLE" in line for line in lines)


def test_executor_section_does_not_flag_2x_sqlite_debug_executor(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """`DebugExecutor` is single-threaded too; Airflow allows it with any backend."""

    state = _state(tmp_path, db_backend="sqlite", family=AirflowFamily.V2.value)
    monkeypatch.setattr(doctor, "_resolve_executor", lambda: "DebugExecutor")

    lines = doctor._executor_section(state)

    assert not any("INCOMPATIBLE" in line for line in lines)


def test_executor_section_respects_the_skip_check_environment_variable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Do not flag a combination the consumer already told Airflow's own check to skip."""

    state = _state(tmp_path, db_backend="sqlite", family=AirflowFamily.V2.value)
    monkeypatch.setattr(doctor, "_resolve_executor", lambda: "LocalExecutor")
    monkeypatch.setenv("_AIRFLOW__SKIP_DATABASE_EXECUTOR_COMPATIBILITY_CHECK", "1")

    lines = doctor._executor_section(state)

    assert not any("INCOMPATIBLE" in line for line in lines)


def test_executor_section_handles_fully_qualified_and_multi_executor_values(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Match the primary executor by class name whether given a path or a list."""

    state = _state(tmp_path, db_backend="sqlite", family=AirflowFamily.V2.value)
    monkeypatch.setattr(
        doctor,
        "_resolve_executor",
        lambda: "airflow.executors.local_executor.LocalExecutor,CustomExecutor",
    )

    lines = doctor._executor_section(state)

    assert any("INCOMPATIBLE" in line and "`LocalExecutor`" in line for line in lines)


@pytest.mark.parametrize(
    "resolved",
    [
        "myalias:airflow.executors.sequential_executor.SequentialExecutor",
        "myalias:SequentialExecutor",
    ],
)
def test_executor_section_handles_aliased_multi_executor_values(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, resolved: str
) -> None:
    """Resolve `alias:module.Class` and `alias:CoreName` entries to the bare class name.

    Parameters:
        monkeypatch: pytest.MonkeyPatch replacing the executor resolution seam.
        tmp_path: pathlib.Path providing a throwaway bootstrap root.
        resolved: str containing one aliased `core.executor` primary entry.
    """

    state = _state(tmp_path, db_backend="sqlite", family=AirflowFamily.V2.value)
    monkeypatch.setattr(doctor, "_resolve_executor", lambda: resolved)

    lines = doctor._executor_section(state)

    assert not any("INCOMPATIBLE" in line for line in lines)


def test_executor_section_does_not_flag_an_unparseable_empty_executor(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Render the raw value without a garbage bullet when the executor is blank."""

    state = _state(tmp_path, db_backend="sqlite", family=AirflowFamily.V2.value)
    monkeypatch.setattr(doctor, "_resolve_executor", lambda: "   ")

    lines = doctor._executor_section(state)

    assert lines == ["- `core.executor`: `   `"]
    assert not any("INCOMPATIBLE" in line for line in lines)


def test_executor_section_reports_resolution_failure_without_raising(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Report an unresolvable executor instead of crashing the whole report."""

    state = _state(tmp_path)
    error = RuntimeError("Airflow is not importable")

    def _raise() -> str:
        """Simulate a broken Airflow configuration import."""

        raise error

    monkeypatch.setattr(doctor, "_resolve_executor", _raise)

    lines = doctor._executor_section(state)

    assert any("could not resolve" in line and str(error) in line for line in lines)


def test_format_capability_value_handles_enum() -> None:
    """Format an enum field by its plain value, not its repr."""

    assert doctor._format_capability_value(DagBagLocation.MODELS) == "airflow.models.dagbag"


def test_format_capability_value_handles_release_tuple() -> None:
    """Format a release tuple as a dot-joined version string."""

    assert doctor._format_capability_value((3, 3, 0)) == "3.3.0"


def test_format_capability_value_handles_plain_value() -> None:
    """Format a plain boolean through its default string form."""

    assert doctor._format_capability_value(True) == "True"


def test_capability_lines_render_every_field() -> None:
    """Render one bullet per `AirflowCapabilities` field."""

    lines = doctor._capability_lines(CAPABILITIES)

    assert "- Capability `release`: `3.3.0`" in lines
    assert "- Capability `dag_bag_location`: `airflow.dag_processing.dagbag`" in lines
    assert "- Capability `task_instance_runner`: `airflow.sdk.definitions.dag._run_task`" in lines
    assert "- Capability `dag_bag_supports_include_examples`: `False`" in lines
    assert "- Capability `refresh_from_task_supports_dag_run`: `True`" in lines
    assert "- Capability `startup_details_supports_sentry`: `True`" in lines
    assert "- Capability `runtime_task_instance_supports_queue`: `True`" in lines


def test_version_section_reports_versions_and_capabilities(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Render plugin, pytest, Python versions plus the resolved capability table."""

    monkeypatch.setattr(doctor, "resolve_capabilities", lambda: CAPABILITIES)

    lines = doctor._version_section()

    assert any(line.startswith("- `pytest-airflow-in-a-box`: `") for line in lines)
    assert any(line.startswith("- `pytest`: `") for line in lines)
    assert any(line.startswith("- Python: `") for line in lines)
    assert "- Apache Airflow: `3.3.0`" in lines
    assert "- Capability `release`: `3.3.0`" in lines


def test_version_section_reports_incompatibility_without_raising(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report an incompatible Airflow install instead of crashing the whole report."""

    error = AirflowCompatibilityError("installed version '2.0.0' is not certified")

    def _raise() -> AirflowCapabilities:
        """Simulate an incompatible Airflow installation."""

        raise error

    monkeypatch.setattr(doctor, "resolve_capabilities", _raise)

    lines = doctor._version_section()

    assert any("INCOMPATIBLE" in line and str(error) in line for line in lines)
    assert not any(line.startswith("- Apache Airflow: `") for line in lines)


def test_api_server_section_reports_not_started() -> None:
    """Report that the API server was never requested by this invocation."""

    lines = doctor._api_server_section()

    assert any("Not started" in line and "api_server_url" in line for line in lines)


def test_migration_strict_section_reports_disabled(tmp_path: Path) -> None:
    """Report disabled when migration-strict mode is off."""

    state = _state(tmp_path)
    config: Any = SimpleNamespace(
        getoption=lambda _name: None, getini=lambda _name: False, stash=pytest.Stash()
    )

    lines = doctor._migration_strict_section(config, state)

    assert lines == ["- `--airflow-migration-strict`: disabled"]


def test_migration_strict_section_reports_enabled_on_v2(tmp_path: Path) -> None:
    """Report enabled when migration-strict mode is on for a 2.x family state."""

    state = _state(tmp_path, family=AirflowFamily.V2.value)
    config: Any = SimpleNamespace(
        getoption=lambda _name: True, getini=lambda _name: False, stash=pytest.Stash()
    )

    lines = doctor._migration_strict_section(config, state)

    assert lines == ["- `--airflow-migration-strict`: enabled"]


def test_migration_strict_section_flags_noop_off_v2(tmp_path: Path) -> None:
    """Flag migration-strict mode as a no-op when enabled off the 2.x family."""

    state = _state(tmp_path, family=AirflowFamily.V3.value)
    config: Any = SimpleNamespace(
        getoption=lambda _name: True, getini=lambda _name: False, stash=pytest.Stash()
    )

    lines = doctor._migration_strict_section(config, state)

    assert len(lines) == 1
    assert "no-op" in lines[0]
    assert "Airflow 2.x" in lines[0]


def test_render_doctor_report_combines_every_section(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Combine every section under one titled, copy-pasteable report."""

    state = _state(tmp_path)
    monkeypatch.setattr(doctor, "resolve_capabilities", lambda: CAPABILITIES)
    monkeypatch.setattr(doctor, "_resolve_executor", lambda: "LocalExecutor")

    def _fake_get_bootstrap_state(config: Any) -> Any:
        """Return the fabricated state regardless of the passed configuration."""

        del config
        return state

    monkeypatch.setattr(doctor, "get_bootstrap_state", _fake_get_bootstrap_state)

    fake_config: Any = SimpleNamespace(
        getoption=lambda _name: None, getini=lambda _name: False, stash=pytest.Stash()
    )
    report = doctor.render_doctor_report(config=fake_config)

    assert report.startswith(doctor.REPORT_TITLE)
    assert "## Storage" in report
    assert "## AIRFLOW_HOME and database" in report
    assert "## Executor" in report
    assert "## Migration-strict" in report
    assert "## Versions and capabilities" in report
    assert "## API server" in report
    assert report.endswith("\n")
    assert not report.endswith("\n\n")


def test_airflow_doctor_prints_report_and_exits(pytester: pytest.Pytester) -> None:
    """Print the full report and exit before any test collection or run."""

    pytester.makepyfile("def test_never_runs():\n    assert False\n")

    result = pytester.runpytest_subprocess("--airflow-doctor")

    assert result.ret == 0
    result.stdout.fnmatch_lines(
        [
            "# pytest-airflow-in-a-box diagnostics",
            "*## Storage*",
            "*Reason:*",
            "*## AIRFLOW_HOME and database*",
            "*AIRFLOW_HOME*",
            "*Database URL scheme:*",
            "*## Versions and capabilities*",
            "*pytest-airflow-in-a-box*",
            "*Apache Airflow:*",
            "*## Executor*",
            "*core.executor*: `*`",
            "*## Migration-strict*",
            "*--airflow-migration-strict*: disabled*",
            "*## API server*",
            "*Not started*",
        ]
    )
    assert "test_never_runs" not in result.stdout.str()
    assert "could not resolve" not in result.stdout.str()


def test_airflow_doctor_reports_explicit_storage_reason_and_still_cleans_up(
    pytester: pytest.Pytester,
) -> None:
    """Report the `explicit` storage reason and remove the run directory afterward."""

    explicit_base = pytester.path / "explicit-base"
    explicit_base.mkdir()
    pytester.makepyfile("def test_never_runs():\n    assert False\n")

    result = pytester.runpytest_subprocess(
        "--airflow-doctor", "--airflow-home", str(explicit_base)
    )

    assert result.ret == 0
    result.stdout.fnmatch_lines(["*Reason: `explicit`*"])
    remaining = list(explicit_base.iterdir())
    assert remaining == []


def test_format_capability_value_marks_unprobed_fields() -> None:
    """Render None capability fields as unprobed rather than a bare `None`."""

    assert doctor._format_capability_value(None) == "unprobed on this family"
