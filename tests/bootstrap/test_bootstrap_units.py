"""Unit tests for bootstrap validation and error paths."""

from __future__ import annotations

import base64
import json
import os
import shutil
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from pytest_airflow_in_a_box import bootstrap
from pytest_airflow_in_a_box.airflow_cfg import sqlite_url
from pytest_airflow_in_a_box.bootstrap import (
    STATE_ENVIRONMENT_VARIABLE,
    STATE_KEY,
    WORKER_INPUT_KEY,
    BootstrapState,
    configure_node,
    get_bootstrap_state,
    validate_configure,
)


def _artifact_state(root: Path, *, owner_pid: int = 1234) -> BootstrapState:
    """Create a state whose artifacts genuinely exist below one root.

    Parameters:
        root: pathlib.Path receiving the run artifact layout.
        owner_pid: int identifying the pretend controller process.

    Returns:
        BootstrapState whose file-existence validation passes.
    """

    (root / "dags").mkdir(parents=True)
    (root / "logs").mkdir()
    (root / "config").mkdir()
    (root / "simple_auth_manager_passwords.json").write_text("{}\n", encoding="utf-8")
    (root / "airflow.cfg").write_text("", encoding="utf-8")
    (root / "config" / "airflow_local_settings.py").write_text("", encoding="utf-8")
    return BootstrapState(
        version=bootstrap.STATE_VERSION,
        owner_pid=owner_pid,
        root=root,
        dags_folder=root / "dags",
        logs_folder=root / "logs",
        database_path=root / "airflow.db",
        password_file=root / "simple_auth_manager_passwords.json",
        config_path=root / "airflow.cfg",
        jwt_secret="secret",
        fernet_key="fernet",
        storage_reason="explicit",
        network_storage=False,
        sql_alchemy_conn=sqlite_url(root / "airflow.db"),
        db_backend="sqlite",
        family="apache-airflow-core",
    )


@pytest.mark.parametrize(
    ("payload", "match"),
    [
        ({"key": 7}, "`key` must be a non-empty string"),
        ({"key": ""}, "`key` must be a non-empty string"),
        ({}, "`key` must be a non-empty string"),
    ],
)
def test_require_string_rejects_bad_values(payload: dict[str, object], match: str) -> None:
    """Reject absent, empty, and non-string fields."""

    with pytest.raises(ValueError, match=match):
        bootstrap._require_string(payload, "key")


@pytest.mark.parametrize("payload", [{"key": "7"}, {"key": True}, {}])
def test_require_int_rejects_bad_values(payload: dict[str, object]) -> None:
    """Reject absent, boolean, and non-integer fields."""

    with pytest.raises(ValueError, match="`key` must be an integer"):
        bootstrap._require_int(payload, "key")


@pytest.mark.parametrize("payload", [{"key": 1}, {"key": "true"}, {}])
def test_require_bool_rejects_bad_values(payload: dict[str, object]) -> None:
    """Reject absent and non-boolean fields."""

    with pytest.raises(ValueError, match="`key` must be a boolean"):
        bootstrap._require_bool(payload, "key")


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (lambda _payload: "not a dict", "must be a JSON object with string keys"),
        (lambda payload: {**payload, 7: "x"}, "must be a JSON object with string keys"),
        (
            lambda payload: {k: v for k, v in payload.items() if k != "root"},
            "missing or unexpected",
        ),
        (lambda payload: {**payload, "extra": 1}, "missing or unexpected"),
        (lambda payload: {**payload, "version": 99}, "Unsupported bootstrap state version"),
        (
            lambda payload: {**payload, "family": "apache-hadoop"},
            "`family` must be a supported family",
        ),
        (
            lambda payload: {**payload, "dags_folder": "relative/dags"},
            "paths must be absolute",
        ),
        (
            lambda payload: {**payload, "dags_folder": str(Path(payload["root"]).parent / "dags")},
            "direct children of the run root",
        ),
    ],
)
def test_state_from_payload_rejects_malformed_payloads(
    tmp_path: Path,
    mutate: Any,
    match: str,
) -> None:
    """Reject structurally invalid payload shapes with named reasons."""

    payload = _artifact_state(tmp_path / "run").to_payload()

    with pytest.raises(ValueError, match=match):
        bootstrap._state_from_payload(mutate(dict(payload)), validate_files=False)


def test_v2_environment_swaps_the_auth_surface(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Env-pin the webserver secret and executor and no 3.x auth keys on the 2.x family.

    The executor pin is load-bearing: 2.x's `unit_test_mode` overlays `unit_tests.cfg`
    (`executor = LocalExecutor`), which the `ready_to_reschedule` dependency rejects
    against SQLite, and only environment variables outrank that overlay.

    Parameters:
        tmp_path: pathlib.Path providing isolated run roots.
        monkeypatch: pytest.MonkeyPatch isolating the ambient executor variable.
    """

    monkeypatch.delenv("AIRFLOW__CORE__EXECUTOR", raising=False)
    state = replace(_artifact_state(tmp_path / "run"), family=bootstrap.AirflowFamily.V2.value)

    variables = bootstrap._environment(state)

    assert variables["AIRFLOW__WEBSERVER__SECRET_KEY"] == state.jwt_secret
    assert variables["AIRFLOW__CORE__EXECUTOR"] == "SequentialExecutor"
    assert "AIRFLOW__CORE__AUTH_MANAGER" not in variables
    assert "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS" not in variables
    assert "AIRFLOW__API_AUTH__JWT_SECRET" not in variables
    v3_variables = bootstrap._environment(_artifact_state(tmp_path / "run3"))
    assert "AIRFLOW__WEBSERVER__SECRET_KEY" not in v3_variables
    assert "AIRFLOW__CORE__EXECUTOR" not in v3_variables
    assert "AIRFLOW__CORE__AUTH_MANAGER" in v3_variables


def test_v2_environment_defers_to_an_ambient_executor_choice(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Leave a deliberate ambient `AIRFLOW__CORE__EXECUTOR` in charge on 2.x.

    A consumer running the 2.x tier against Postgres may legitimately want
    `LocalExecutor`; the pin is a safe default, not a mandate, and `--airflow-doctor`
    flags an incompatible choice instead of the plugin silently overriding it.

    Parameters:
        tmp_path: pathlib.Path providing an isolated run root.
        monkeypatch: pytest.MonkeyPatch setting the ambient executor variable.
    """

    monkeypatch.setenv("AIRFLOW__CORE__EXECUTOR", "LocalExecutor")
    state = replace(_artifact_state(tmp_path / "run"), family=bootstrap.AirflowFamily.V2.value)

    variables = bootstrap._environment(state)

    assert "AIRFLOW__CORE__EXECUTOR" not in variables


def test_install_environment_owns_the_other_family_surface(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Evict ambient variables of the family this run does not use.

    `_install_environment` mutates `os.environ` directly, and this suite runs inside
    the plugin's own bootstrapped session, so the complete owned surface is snapshotted
    and restored manually -- monkeypatch only tracks names it set itself.
    """

    owned = (*bootstrap._environment_names(), bootstrap.STATE_ENVIRONMENT_VARIABLE)
    original = {name: os.environ.get(name) for name in owned}
    monkeypatch.setenv("AIRFLOW__CORE__AUTH_MANAGER", "ambient.AuthManager")
    monkeypatch.setenv("AIRFLOW__API_AUTH__JWT_SECRET", "ambient-secret")
    monkeypatch.setenv("AIRFLOW__CORE__EXECUTOR", "ambient.Executor")
    state = replace(_artifact_state(tmp_path / "run"), family=bootstrap.AirflowFamily.V2.value)

    try:
        bootstrap._install_environment(state)

        assert "AIRFLOW__CORE__AUTH_MANAGER" not in os.environ
        assert "AIRFLOW__API_AUTH__JWT_SECRET" not in os.environ
        # The ambient executor is deliberate consumer configuration, not owned surface.
        assert os.environ["AIRFLOW__CORE__EXECUTOR"] == "ambient.Executor"
        assert os.environ["AIRFLOW__WEBSERVER__SECRET_KEY"] == state.jwt_secret
    finally:
        for name, value in original.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def test_state_from_environment_requires_the_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refuse to run a worker that inherited no bootstrap state."""

    monkeypatch.delenv(STATE_ENVIRONMENT_VARIABLE, raising=False)

    with pytest.raises(pytest.UsageError, match="did not inherit bootstrap state"):
        bootstrap._state_from_environment()


def test_argument_value_requires_a_following_value() -> None:
    """Reject a trailing option with no value argument."""

    with pytest.raises(pytest.UsageError, match="`--airflow-home` requires a value"):
        bootstrap._argument_value(["--airflow-home"], "--airflow-home")


def test_allow_network_rejects_non_boolean_ini() -> None:
    """Reject a malformed `allow_network_airflow_home` ini value."""

    config: Any = SimpleNamespace(getini=lambda _name: "yes")

    with pytest.raises(pytest.UsageError, match="must be a boolean"):
        bootstrap._allow_network(config, [])


def test_owner_state_wraps_storage_selection_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Wrap storage-selection failures in one actionable usage error."""

    def explode(**_kwargs: Any) -> Any:
        raise ValueError("no storage for you")

    monkeypatch.setattr(bootstrap, "locate_storage", explode)
    ini_values = {
        "airflow_home": "",
        "allow_network_airflow_home": False,
        "airflow_db_backend": "sqlite",
    }
    config: Any = SimpleNamespace(
        getini=lambda name: ini_values[name], add_cleanup=lambda _callback: None
    )

    with pytest.raises(pytest.UsageError, match="Could not create isolated Airflow storage"):
        bootstrap._owner_state(config, [])


def test_owner_state_cleans_up_after_provisioning_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Remove the partial run directory and restore environment on failure."""

    base = tmp_path / "base"
    base.mkdir()
    cleanups: list[Any] = []

    def fail_write(*_args: Any, **_kwargs: Any) -> None:
        raise ValueError("config write failed")

    monkeypatch.setattr(bootstrap, "write_airflow_config", fail_write)
    monkeypatch.setenv("AIRFLOW_HOME", "pre-existing-value")
    config: Any = SimpleNamespace(
        getini=lambda name: {
            "airflow_home": str(base),
            "allow_network_airflow_home": False,
            "airflow_db_backend": "sqlite",
        }[name],
        add_cleanup=cleanups.append,
    )

    with pytest.raises(pytest.UsageError, match="Could not provision isolated Airflow storage"):
        bootstrap._owner_state(config, [])

    assert list(base.iterdir()) == []
    assert os.environ["AIRFLOW_HOME"] == "pre-existing-value"
    assert len(cleanups) == 1
    cleanups[0]()


def test_owner_state_cleans_up_after_a_provisioner_usage_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Remove the partial run directory when the provisioner raises its own usage error.

    `PostgresProvisioner.start` reports an absent Docker daemon or a failed image pull as
    a `pytest.UsageError`, which subclasses neither `OSError` nor `ValueError`. Left
    uncaught it skipped cleanup entirely and leaked one fully provisioned run root per
    failed `--airflow-db-backend=postgres` attempt. The provisioner's own message must
    survive unwrapped, because it is the actionable one.

    Parameters:
        tmp_path: Path providing an explicit storage base.
        monkeypatch: pytest.MonkeyPatch replacing the provisioner selection seam.
    """

    base = tmp_path / "base"
    base.mkdir()
    cleanups: list[Any] = []

    class ExplodingProvisioner:
        """Fail the way the Postgres provisioner fails when Docker is unreachable."""

        def start(self, *, database_path: Path, database_name: str) -> str:
            """Raise the provisioner's own actionable usage error.

            Parameters:
                database_path: pathlib.Path selecting the SQLite file location.
                database_name: str naming a per-run server-side database.

            Returns:
                str containing the provisioned SQLAlchemy URL; never returned.

            Raises:
                pytest.UsageError: Always, standing in for an unreachable Docker daemon.
            """

            del database_path, database_name
            raise pytest.UsageError("Docker daemon is unreachable (fake probe)")

        def stop(self) -> None:
            """Release nothing; this provisioner never started anything."""

    monkeypatch.setattr(bootstrap, "select_provisioner", lambda _backend: ExplodingProvisioner())
    config: Any = SimpleNamespace(
        getini=lambda name: {
            "airflow_home": str(base),
            "allow_network_airflow_home": False,
            "airflow_db_backend": "sqlite",
        }[name],
        add_cleanup=cleanups.append,
    )

    with pytest.raises(pytest.UsageError, match="Docker daemon is unreachable"):
        bootstrap._owner_state(config, [])

    assert list(base.iterdir()) == []
    assert len(cleanups) == 1


def test_worker_environment_mismatch_fails_loudly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Name every mismatched variable when the worker environment disagrees."""

    state = _artifact_state(tmp_path / "run")
    monkeypatch.setenv(bootstrap.XDIST_WORKER_ENVIRONMENT_VARIABLE, "gw0")
    monkeypatch.setenv(
        STATE_ENVIRONMENT_VARIABLE,
        json.dumps(state.to_payload(), sort_keys=True, separators=(",", ":")),
    )
    for name, value in bootstrap._environment(state).items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("AIRFLOW_HOME", "/somewhere/else")
    # The early-import guard fires first in-process; the worker path under
    # test begins after it.
    monkeypatch.delitem(sys.modules, "airflow", raising=False)

    config: Any = SimpleNamespace()

    with pytest.raises(pytest.UsageError, match="disagrees with state: `AIRFLOW_HOME`"):
        bootstrap.load_initial_state(config, [])


def test_get_bootstrap_state_requires_stashed_state() -> None:
    """Report unavailable bootstrap state as a usage error."""

    config: Any = SimpleNamespace(stash=pytest.Stash())

    with pytest.raises(pytest.UsageError, match="bootstrap state is unavailable"):
        get_bootstrap_state(config)


def test_configure_node_rejects_remote_gateways(tmp_path: Path) -> None:
    """Refuse to send state to a non-popen execnet gateway."""

    state = _artifact_state(tmp_path / "run")
    stash = pytest.Stash()
    stash[STATE_KEY] = state
    node: Any = SimpleNamespace(
        gateway=SimpleNamespace(spec=SimpleNamespace(popen=None)),
        config=SimpleNamespace(stash=stash),
        workerinput={},
    )

    with pytest.raises(pytest.UsageError, match="Remote xdist gateways are unsupported"):
        configure_node(node)


def test_local_controller_handoff_validates_on_the_worker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Send controller state through the inherited environment and worker input.

    Parameters:
        tmp_path: pathlib.Path receiving the controller's run artifacts.
        monkeypatch: pytest.MonkeyPatch installing the simulated worker environment.
    """

    state = _artifact_state(tmp_path / "run")
    controller_stash = pytest.Stash()
    controller_stash[STATE_KEY] = state
    workerinput: dict[str, object] = {}
    node: Any = SimpleNamespace(
        gateway=SimpleNamespace(spec=SimpleNamespace(popen=True)),
        config=SimpleNamespace(stash=controller_stash),
        workerinput=workerinput,
    )

    configure_node(node)

    assert workerinput == {WORKER_INPUT_KEY: state.to_payload()}
    for name in bootstrap._environment_names():
        monkeypatch.setenv(name, os.environ.get(name, ""))
    bootstrap._install_environment(state)
    monkeypatch.setenv(bootstrap.XDIST_WORKER_ENVIRONMENT_VARIABLE, "gw0")
    monkeypatch.delitem(sys.modules, "airflow", raising=False)

    def reject_owner_state(_config: pytest.Config, _args: list[str]) -> BootstrapState:
        """Reject accidental worker-side state provisioning.

        Parameters:
            _config: pytest.Config that must remain unused.
            _args: list[str] that must remain unused.

        Raises:
            AssertionError: Always, because workers must inherit controller state.
        """

        raise AssertionError("an xdist worker must not provision controller state")

    monkeypatch.setattr(bootstrap, "_owner_state", reject_owner_state)
    early_config: Any = SimpleNamespace()

    inherited_state = bootstrap.load_initial_state(early_config, [])

    worker_stash = pytest.Stash()
    worker_stash[STATE_KEY] = inherited_state
    worker_config: Any = SimpleNamespace(stash=worker_stash, workerinput=workerinput)

    validate_configure(worker_config)
    assert inherited_state == state


def test_validate_configure_rejects_malformed_workerinput(tmp_path: Path) -> None:
    """Wrap workerinput payload validation failures with context."""

    state = _artifact_state(tmp_path / "run")
    stash = pytest.Stash()
    stash[STATE_KEY] = state
    config: Any = SimpleNamespace(stash=stash, workerinput={WORKER_INPUT_KEY: {"nope": 1}})

    with pytest.raises(pytest.UsageError, match="Invalid controller bootstrap state"):
        validate_configure(config)


def test_validate_configure_rejects_state_disagreement(tmp_path: Path) -> None:
    """Reject workerinput state that differs from the inherited state."""

    state = _artifact_state(tmp_path / "run")
    other = _artifact_state(tmp_path / "other-run", owner_pid=4321)
    stash = pytest.Stash()
    stash[STATE_KEY] = state
    config: Any = SimpleNamespace(
        stash=stash,
        workerinput={WORKER_INPUT_KEY: other.to_payload()},
    )

    with pytest.raises(pytest.UsageError, match="differs from inherited state"):
        validate_configure(config)


def test_allow_network_honors_the_command_line_flag() -> None:
    """Accept the explicit command-line opt-in without reading ini state."""

    config: Any = SimpleNamespace()

    assert bootstrap._allow_network(config, ["--allow-network-airflow-home"]) is True


def test_db_backend_defaults_to_sqlite() -> None:
    """Default to the SQLite backend when neither flag nor ini is set."""

    config: Any = SimpleNamespace(getini=lambda _name: "")

    assert bootstrap._db_backend(config, []) is bootstrap.DbBackend.SQLITE


def test_db_backend_reads_the_command_line_flag() -> None:
    """Prefer the command-line backend over the ini default."""

    config: Any = SimpleNamespace(getini=lambda _name: "sqlite")

    backend = bootstrap._db_backend(config, ["--airflow-db-backend=postgres"])

    assert backend is bootstrap.DbBackend.POSTGRES


def test_db_backend_reads_the_ini_value() -> None:
    """Fall back to the ini backend when no command-line flag is present."""

    config: Any = SimpleNamespace(getini=lambda _name: "postgres")

    assert bootstrap._db_backend(config, []) is bootstrap.DbBackend.POSTGRES


def test_db_backend_rejects_an_unsupported_value() -> None:
    """Reject an unsupported backend string with a usage error."""

    config: Any = SimpleNamespace(getini=lambda _name: "")

    with pytest.raises(pytest.UsageError, match="must be `sqlite` or `postgres`"):
        bootstrap._db_backend(config, ["--airflow-db-backend=mysql"])


def test_state_from_payload_skips_file_validation_when_asked(tmp_path: Path) -> None:
    """Rebuild state from a payload without touching the filesystem."""

    state = _artifact_state(tmp_path / "run")
    shutil.rmtree(tmp_path / "run")

    rebuilt = bootstrap._state_from_payload(state.to_payload(), validate_files=False)

    assert rebuilt == state


def test_state_from_payload_round_trips_a_postgres_backend(tmp_path: Path) -> None:
    """Preserve a Postgres URL and backend through a payload round-trip."""

    root = tmp_path / "run"
    postgres_url = "postgresql+psycopg2://user:pass@localhost:5432/airflow_test_abc"
    state = _artifact_state(root)
    postgres_state = replace(state, sql_alchemy_conn=postgres_url, db_backend="postgres")

    rebuilt = bootstrap._state_from_payload(postgres_state.to_payload(), validate_files=False)

    assert rebuilt.db_backend == "postgres"
    assert rebuilt.sql_alchemy_conn == postgres_url


def test_state_from_payload_rejects_an_unknown_backend(tmp_path: Path) -> None:
    """Reject a payload whose `db_backend` is not a supported value."""

    payload = _artifact_state(tmp_path / "run").to_payload()
    payload["db_backend"] = "mysql"

    with pytest.raises(ValueError, match="`db_backend` must be a supported backend"):
        bootstrap._state_from_payload(payload, validate_files=False)


def test_state_from_payload_rejects_sqlite_url_disagreement(tmp_path: Path) -> None:
    """Reject a SQLite state whose URL disagrees with its database path."""

    payload = _artifact_state(tmp_path / "run").to_payload()
    payload["sql_alchemy_conn"] = "sqlite:////somewhere/else.db"

    with pytest.raises(ValueError, match="URL disagrees with the database path"):
        bootstrap._state_from_payload(payload, validate_files=False)


def test_install_environment_removes_an_inherited_sqlite_pool_toggle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Prevent an inherited pool override from changing SQLite's default behavior."""

    state = _artifact_state(tmp_path / "run")
    for name in bootstrap._environment_names():
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv(bootstrap.SQL_ALCHEMY_POOL_ENABLED_ENVIRONMENT_VARIABLE, "False")

    bootstrap._install_environment(state)

    assert bootstrap.SQL_ALCHEMY_POOL_ENABLED_ENVIRONMENT_VARIABLE not in os.environ
    assert os.environ[bootstrap.SQL_ALCHEMY_CONN_ENVIRONMENT_VARIABLE] == state.sql_alchemy_conn


def test_install_environment_disables_pooling_for_postgres(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Select NullPool for Postgres by disabling Airflow connection pooling."""

    state = _artifact_state(tmp_path / "run")
    postgres_url = "postgresql+psycopg2://user:pass@localhost:5432/airflow_test_abc"
    postgres_state = replace(state, sql_alchemy_conn=postgres_url, db_backend="postgres")

    for name in bootstrap._environment_names():
        monkeypatch.delenv(name, raising=False)
    bootstrap._install_environment(postgres_state)

    assert os.environ[bootstrap.SQL_ALCHEMY_CONN_ENVIRONMENT_VARIABLE] == postgres_url
    assert os.environ[bootstrap.SQL_ALCHEMY_POOL_ENABLED_ENVIRONMENT_VARIABLE] == "False"


def test_generate_fernet_key_is_a_distinct_valid_key() -> None:
    """Generate a decodable 32-byte Fernet key that differs per call."""

    first = bootstrap.generate_fernet_key()
    second = bootstrap.generate_fernet_key()

    assert first != second
    assert len(base64.urlsafe_b64decode(first)) == 32


def test_install_environment_pins_one_fernet_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pin the run's Fernet key so every Airflow process decrypts the same rows.

    Airflow's `unit_test_mode` generates a fresh random key per process, so
    without this the `api_client` server subprocess cannot read an encrypted
    connection password written by the pytest process.
    """

    state = _artifact_state(tmp_path / "run")
    for name in bootstrap._environment_names():
        monkeypatch.delenv(name, raising=False)

    bootstrap._install_environment(state)

    assert "AIRFLOW__CORE__FERNET_KEY" in bootstrap._environment_names()
    assert os.environ["AIRFLOW__CORE__FERNET_KEY"] == state.fernet_key
