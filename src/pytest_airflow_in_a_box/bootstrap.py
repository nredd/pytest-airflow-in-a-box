"""Bootstrap isolated Airflow state before consumer conftests are imported.

References:
    https://docs.pytest.org/en/stable/reference/reference.html#pytest-load-initial-conftests
    https://docs.pytest.org/en/stable/reference/reference.html#stash
    https://pytest-xdist.readthedocs.io/en/stable/how-to.html
"""

from __future__ import annotations

import base64
import json
import os
import secrets
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable

import pytest

from pytest_airflow_in_a_box._compat.capabilities import AirflowFamily, installed_family
from pytest_airflow_in_a_box.airflow_cfg import (
    SIMPLE_AUTH_MANAGER,
    sqlite_url,
    write_airflow_config,
)
from pytest_airflow_in_a_box.storage import locate_storage, write_local_settings
from pytest_airflow_in_a_box.storage.provision import DbBackend, select_provisioner

STATE_VERSION = 4
STATE_ENVIRONMENT_VARIABLE = "PYTEST_AIRFLOW_IN_A_BOX_BOOTSTRAP_STATE"
WORKER_INPUT_KEY = "pytest_airflow_in_a_box_bootstrap_state"
XDIST_WORKER_ENVIRONMENT_VARIABLE = "PYTEST_XDIST_WORKER"
PASSWORDS = '{"admin": "admin"}\n'
SQL_ALCHEMY_CONN_ENVIRONMENT_VARIABLE = "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
SQL_ALCHEMY_POOL_ENABLED_ENVIRONMENT_VARIABLE = "AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_ENABLED"

JsonPrimitive = str | int | bool
StatePayload = dict[str, JsonPrimitive]


@dataclass(frozen=True)
class BootstrapState:
    """Immutable state shared by one controller and its local workers.

    Parameters:
        version: int identifying the serialized state schema.
        owner_pid: int identifying the controller process.
        root: pathlib.Path containing this run's isolated Airflow home.
        dags_folder: pathlib.Path containing test Dag files.
        logs_folder: pathlib.Path receiving Airflow logs.
        database_path: pathlib.Path reserved for the SQLite metadata database.
        password_file: pathlib.Path containing SimpleAuthManager passwords.
        config_path: pathlib.Path containing ``airflow.cfg``.
        jwt_secret: str shared by all Airflow processes in this run.
        fernet_key: str shared by all Airflow processes encrypting metadata.
        storage_reason: str describing why the storage base was selected.
        network_storage: bool indicating network filesystem semantics.
        sql_alchemy_conn: str containing the metadata database SQLAlchemy URL.
        db_backend: str naming the selected metadata database backend.
        family: str naming the Airflow distribution family (`AirflowFamily` value); on
            2.x the `jwt_secret` doubles as the webserver secret key.
    """

    version: int
    owner_pid: int
    root: Path
    dags_folder: Path
    logs_folder: Path
    database_path: Path
    password_file: Path
    config_path: Path
    jwt_secret: str
    fernet_key: str
    storage_reason: str
    network_storage: bool
    sql_alchemy_conn: str
    db_backend: str
    family: str

    def to_payload(self) -> StatePayload:
        """Serialize state to JSON-compatible primitives.

        Returns:
            StatePayload containing every state field.
        """

        return {
            "version": self.version,
            "owner_pid": self.owner_pid,
            "root": str(self.root),
            "dags_folder": str(self.dags_folder),
            "logs_folder": str(self.logs_folder),
            "database_path": str(self.database_path),
            "password_file": str(self.password_file),
            "config_path": str(self.config_path),
            "jwt_secret": self.jwt_secret,
            "fernet_key": self.fernet_key,
            "storage_reason": self.storage_reason,
            "network_storage": self.network_storage,
            "sql_alchemy_conn": self.sql_alchemy_conn,
            "db_backend": self.db_backend,
            "family": self.family,
        }


STATE_KEY = pytest.StashKey[BootstrapState]()


@runtime_checkable
class XdistWorkerConfig(Protocol):
    """Typed xdist extension attached to worker ``pytest.Config`` objects."""

    workerinput: dict[str, object]


class XdistGatewaySpec(Protocol):
    """Subset of an execnet gateway specification used by the plugin."""

    popen: bool | None


class XdistGateway(Protocol):
    """Subset of an execnet gateway used by the plugin."""

    spec: XdistGatewaySpec


class XdistNode(Protocol):
    """Typed subset of an xdist worker controller."""

    config: pytest.Config
    gateway: XdistGateway
    workerinput: dict[str, object]


def _require_string(payload: dict[str, object], key: str) -> str:
    """Read one required string from a decoded state payload.

    Parameters:
        payload: dict[str, object] containing decoded state.
        key: str naming the required field.

    Returns:
        str containing the field value.

    Raises:
        ValueError: The field is absent or is not a non-empty string.
    """

    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"Bootstrap state field `{key}` must be a non-empty string")
    return value


def _require_int(payload: dict[str, object], key: str) -> int:
    """Read one required non-boolean integer from a decoded state payload.

    Parameters:
        payload: dict[str, object] containing decoded state.
        key: str naming the required field.

    Returns:
        int containing the field value.

    Raises:
        ValueError: The field is absent or is not an integer.
    """

    value = payload.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"Bootstrap state field `{key}` must be an integer")
    return value


def _require_bool(payload: dict[str, object], key: str) -> bool:
    """Read one required boolean from a decoded state payload.

    Parameters:
        payload: dict[str, object] containing decoded state.
        key: str naming the required field.

    Returns:
        bool containing the field value.

    Raises:
        ValueError: The field is absent or is not a boolean.
    """

    value = payload.get(key)
    if not isinstance(value, bool):
        raise ValueError(f"Bootstrap state field `{key}` must be a boolean")
    return value


def _state_from_payload(value: object, *, validate_files: bool) -> BootstrapState:
    """Validate and construct state from decoded JSON-compatible data.

    Parameters:
        value: object containing a candidate state mapping.
        validate_files: bool requiring all bootstrap artifacts to exist.

    Returns:
        BootstrapState containing validated state.

    Raises:
        ValueError: The payload is malformed, incompatible, or stale.
    """

    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError("Bootstrap state must be a JSON object with string keys")
    payload: dict[str, object] = value
    expected_keys = {
        "version",
        "owner_pid",
        "root",
        "dags_folder",
        "logs_folder",
        "database_path",
        "password_file",
        "config_path",
        "jwt_secret",
        "fernet_key",
        "storage_reason",
        "network_storage",
        "sql_alchemy_conn",
        "db_backend",
        "family",
    }
    if set(payload) != expected_keys:
        raise ValueError("Bootstrap state has missing or unexpected fields")

    version = _require_int(payload, "version")
    if version != STATE_VERSION:
        raise ValueError(f"Unsupported bootstrap state version: '{version}'")
    root = Path(_require_string(payload, "root"))
    path_values = {
        "dags_folder": Path(_require_string(payload, "dags_folder")),
        "logs_folder": Path(_require_string(payload, "logs_folder")),
        "database_path": Path(_require_string(payload, "database_path")),
        "password_file": Path(_require_string(payload, "password_file")),
        "config_path": Path(_require_string(payload, "config_path")),
    }
    if not root.is_absolute() or any(not path.is_absolute() for path in path_values.values()):
        raise ValueError("Bootstrap state paths must be absolute")
    if any(path.parent != root for path in path_values.values()):
        raise ValueError("Bootstrap state paths must be direct children of the run root")

    sql_alchemy_conn = _require_string(payload, "sql_alchemy_conn")
    backend_value = _require_string(payload, "db_backend")
    try:
        backend = DbBackend(backend_value)
    except ValueError as error:
        raise ValueError(
            f"Bootstrap state field `db_backend` must be a supported backend: '{backend_value}'"
        ) from error
    if backend is DbBackend.SQLITE and sql_alchemy_conn != sqlite_url(
        path_values["database_path"]
    ):
        raise ValueError("SQLite bootstrap state URL disagrees with the database path")

    family_value = _require_string(payload, "family")
    try:
        AirflowFamily(family_value)
    except ValueError as error:
        raise ValueError(
            f"Bootstrap state field `family` must be a supported family: '{family_value}'"
        ) from error

    state = BootstrapState(
        version=version,
        owner_pid=_require_int(payload, "owner_pid"),
        root=root,
        dags_folder=path_values["dags_folder"],
        logs_folder=path_values["logs_folder"],
        database_path=path_values["database_path"],
        password_file=path_values["password_file"],
        config_path=path_values["config_path"],
        jwt_secret=_require_string(payload, "jwt_secret"),
        fernet_key=_require_string(payload, "fernet_key"),
        storage_reason=_require_string(payload, "storage_reason"),
        network_storage=_require_bool(payload, "network_storage"),
        sql_alchemy_conn=sql_alchemy_conn,
        db_backend=str(backend),
        family=family_value,
    )
    if validate_files:
        local_settings_path = state.root / "config" / "airflow_local_settings.py"
        stale = (
            not state.root.is_dir()
            or not state.dags_folder.is_dir()
            or not state.logs_folder.is_dir()
            or not state.password_file.is_file()
            or not state.config_path.is_file()
            or not local_settings_path.is_file()
        )
        if stale:
            raise ValueError(
                f"Bootstrap state is stale; artifacts are missing below '{state.root}'"
            )
    return state


def _state_from_environment() -> BootstrapState:
    """Load inherited worker state from the process environment.

    Returns:
        BootstrapState containing validated inherited state.

    Raises:
        pytest.UsageError: The environment state is absent, malformed, or stale.
    """

    serialized = os.environ.get(STATE_ENVIRONMENT_VARIABLE)
    if serialized is None:
        raise pytest.UsageError(
            "Local xdist worker did not inherit bootstrap state; remote xdist gateways are "
            "unsupported"
        )
    try:
        decoded: object = json.loads(serialized)
        return _state_from_payload(decoded, validate_files=True)
    except (json.JSONDecodeError, ValueError) as error:
        raise pytest.UsageError(f"Invalid inherited Airflow bootstrap state: {error}") from error


def generate_fernet_key() -> str:
    """Generate one Fernet key without importing Airflow or `cryptography`.

    Airflow's ``unit_test_mode`` generates a fresh random Fernet key in every
    process, so encrypted metadata written by one process is undecryptable in
    another. Pinning one key per run root keeps seeded connection passwords and
    Variable values readable from the ``api_client`` server subprocess.

    Returns:
        str containing a url-safe base64-encoded 32-byte Fernet key.
    """

    return base64.urlsafe_b64encode(secrets.token_bytes(32)).decode("ascii")


def _environment(state: BootstrapState) -> dict[str, str]:
    """Build the minimum pre-import Airflow environment.

    Parameters:
        state: BootstrapState containing run paths and secrets.

    Returns:
        dict[str, str] containing environment variable assignments.
    """

    variables = {
        "AIRFLOW_HOME": str(state.root),
        "AIRFLOW_CONFIG": str(state.config_path),
        "AIRFLOW__CORE__DAGS_FOLDER": str(state.dags_folder),
        "AIRFLOW__CORE__UNIT_TEST_MODE": "True",
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        SQL_ALCHEMY_CONN_ENVIRONMENT_VARIABLE: state.sql_alchemy_conn,
        "AIRFLOW__LOGGING__BASE_LOG_FOLDER": str(state.logs_folder),
        "AIRFLOW__CORE__FERNET_KEY": state.fernet_key,
    }
    if state.family == AirflowFamily.V2.value:
        variables["AIRFLOW__WEBSERVER__SECRET_KEY"] = state.jwt_secret
        variables["AIRFLOW__API__AUTH_BACKENDS"] = (
            "airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session"
        )
    else:
        variables["AIRFLOW__CORE__AUTH_MANAGER"] = SIMPLE_AUTH_MANAGER
        variables["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS"] = "admin:admin"
        variables["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE"] = str(state.password_file)
        variables["AIRFLOW__API_AUTH__JWT_SECRET"] = state.jwt_secret
    if state.db_backend == DbBackend.POSTGRES:
        variables[SQL_ALCHEMY_POOL_ENABLED_ENVIRONMENT_VARIABLE] = "False"
    return variables


def _install_environment(state: BootstrapState) -> None:
    """Install pre-import Airflow settings into the process environment.

    Parameters:
        state: BootstrapState containing run paths and secrets.
    """

    variables = _environment(state)
    os.environ.update(variables)
    if state.db_backend == DbBackend.SQLITE:
        os.environ.pop(SQL_ALCHEMY_POOL_ENABLED_ENVIRONMENT_VARIABLE, None)
    os.environ[STATE_ENVIRONMENT_VARIABLE] = json.dumps(
        state.to_payload(), sort_keys=True, separators=(",", ":")
    )


def _argument_value(args: list[str], option: str) -> str | None:
    """Read the final value of one registered command-line option.

    Parameters:
        args: list[str] containing pytest command-line arguments.
        option: str containing the long option name.

    Returns:
        str | None containing the supplied option value.

    Raises:
        pytest.UsageError: The option has no following value.
    """

    value: str | None = None
    for index, argument in enumerate(args):
        if argument.startswith(f"{option}="):
            value = argument.partition("=")[2]
        elif argument == option:
            if index + 1 >= len(args):
                raise pytest.UsageError(f"Option `{option}` requires a value")
            value = args[index + 1]
    return value


def _option_path(config: pytest.Config, args: list[str]) -> Path | None:
    """Resolve the command-line or ini explicit Airflow storage base.

    Parameters:
        config: pytest.Config containing parsed startup options.
        args: list[str] containing pytest command-line arguments.

    Returns:
        pathlib.Path | None containing the explicit base.
    """

    command_line = _argument_value(args, "--airflow-home")
    ini_value: object = config.getini("airflow_home")
    value = command_line if command_line else ini_value
    if not isinstance(value, str) or not value:
        return None
    return Path(value)


def _allow_network(config: pytest.Config, args: list[str]) -> bool:
    """Resolve the command-line or ini network-storage opt-in.

    Parameters:
        config: pytest.Config containing parsed startup options.
        args: list[str] containing pytest command-line arguments.

    Returns:
        bool indicating whether explicit network storage is accepted.
    """

    if "--allow-network-airflow-home" in args:
        return True
    ini_value: object = config.getini("allow_network_airflow_home")
    if not isinstance(ini_value, bool):
        raise pytest.UsageError("Ini option `allow_network_airflow_home` must be a boolean")
    return ini_value


def _db_backend(config: pytest.Config, args: list[str]) -> DbBackend:
    """Resolve the command-line or ini metadata database backend.

    Parameters:
        config: pytest.Config containing parsed startup options.
        args: list[str] containing pytest command-line arguments.

    Returns:
        DbBackend selecting the metadata database backend.

    Raises:
        pytest.UsageError: The selected backend is not a supported value.
    """

    command_line = _argument_value(args, "--airflow-db-backend")
    ini_value: object = config.getini("airflow_db_backend")
    value = command_line if command_line else ini_value
    if not isinstance(value, str) or not value:
        return DbBackend.SQLITE
    try:
        return DbBackend(value)
    except ValueError as error:
        raise pytest.UsageError(
            f"Ini/option `airflow_db_backend` must be `sqlite` or `postgres`: '{value}'"
        ) from error


def _candidate_path(args: list[str]) -> Path | None:
    """Resolve pytest's temporary-storage candidate without constructing fixtures.

    Parameters:
        args: list[str] containing pytest command-line arguments.

    Returns:
        pathlib.Path | None containing a caller-selected candidate.
    """

    basetemp = _argument_value(args, "--basetemp")
    if basetemp:
        return Path(basetemp).parent
    tmpdir = os.environ.get("TMPDIR")
    return Path(tmpdir) if tmpdir else None


def _owner_state(config: pytest.Config, args: list[str]) -> BootstrapState:
    """Create controller-owned storage, configuration, and cleanup.

    Parameters:
        config: pytest.Config receiving owner cleanup registration.
        args: list[str] containing pytest command-line arguments.

    Returns:
        BootstrapState containing the new run state.

    Raises:
        pytest.UsageError: Storage selection or artifact creation fails.
    """

    backend = _db_backend(config, args)
    provisioner = select_provisioner(backend)
    try:
        location = locate_storage(
            explicit=_option_path(config, args),
            candidate=_candidate_path(args),
            allow_network=_allow_network(config, args),
        )
    except (FileNotFoundError, OSError, ValueError) as error:
        raise pytest.UsageError(f"Could not create isolated Airflow storage: {error}") from error

    root = location.path
    environment_names = set(_environment_names())
    original_environment = {name: os.environ.get(name) for name in environment_names}
    cleaned = False

    def cleanup() -> None:
        """Restore environment and remove only the plugin-created run directory."""

        nonlocal cleaned
        if cleaned:
            return
        cleaned = True
        try:
            provisioner.stop()
        finally:
            for name, value in original_environment.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value
            shutil.rmtree(root, ignore_errors=True)

    config.add_cleanup(cleanup)
    try:
        dags_folder = root / "dags"
        logs_folder = root / "logs"
        database_path = root / "airflow.db"
        password_file = root / "simple_auth_manager_passwords.json"
        config_path = root / "airflow.cfg"
        local_settings_path = root / "config" / "airflow_local_settings.py"
        dags_folder.mkdir()
        logs_folder.mkdir()
        password_file.write_text(PASSWORDS, encoding="utf-8")
        password_file.chmod(0o600)
        write_local_settings(local_settings_path)
        database_name = f"airflow_test_{secrets.token_hex(8)}"
        sql_alchemy_conn = provisioner.start(
            database_path=database_path, database_name=database_name
        )
        family = installed_family() or AirflowFamily.V3
        state = BootstrapState(
            version=STATE_VERSION,
            owner_pid=os.getpid(),
            root=root,
            dags_folder=dags_folder,
            logs_folder=logs_folder,
            database_path=database_path,
            password_file=password_file,
            config_path=config_path,
            jwt_secret=secrets.token_urlsafe(48),
            fernet_key=generate_fernet_key(),
            storage_reason=str(location.reason),
            network_storage=location.network,
            sql_alchemy_conn=sql_alchemy_conn,
            db_backend=str(backend),
            family=family.value,
        )
        write_airflow_config(
            config_path,
            dags_folder=dags_folder,
            logs_folder=logs_folder,
            sql_alchemy_conn=sql_alchemy_conn,
            password_file=password_file,
            jwt_secret=state.jwt_secret,
            fernet_key=state.fernet_key,
            family=family,
        )
    except (OSError, ValueError) as error:
        cleanup()
        raise pytest.UsageError(
            f"Could not provision isolated Airflow storage: {error}"
        ) from error
    _install_environment(state)
    return state


def _environment_names() -> tuple[str, ...]:
    """List every environment variable owned during bootstrap.

    Returns:
        tuple[str, ...] containing Airflow and handoff variable names.
    """

    return (
        "AIRFLOW_HOME",
        "AIRFLOW_CONFIG",
        "AIRFLOW__CORE__DAGS_FOLDER",
        "AIRFLOW__CORE__UNIT_TEST_MODE",
        "AIRFLOW__CORE__LOAD_EXAMPLES",
        "AIRFLOW__CORE__AUTH_MANAGER",
        "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS",
        "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE",
        SQL_ALCHEMY_CONN_ENVIRONMENT_VARIABLE,
        SQL_ALCHEMY_POOL_ENABLED_ENVIRONMENT_VARIABLE,
        "AIRFLOW__LOGGING__BASE_LOG_FOLDER",
        "AIRFLOW__API_AUTH__JWT_SECRET",
        "AIRFLOW__WEBSERVER__SECRET_KEY",
        "AIRFLOW__API__AUTH_BACKENDS",
        "AIRFLOW__CORE__FERNET_KEY",
        STATE_ENVIRONMENT_VARIABLE,
    )


def load_initial_state(config: pytest.Config, args: list[str]) -> BootstrapState:
    """Create owner state or load state inherited by a local xdist worker.

    Parameters:
        config: pytest.Config active during initial conftest loading.
        args: list[str] containing pytest command-line arguments.

    Returns:
        BootstrapState containing this process's run state.

    Raises:
        pytest.UsageError: Airflow was imported too early or state is invalid.
    """

    if "airflow" in sys.modules:
        raise pytest.UsageError(
            "Airflow was imported before pytest-airflow-in-a-box could configure it. Remove the "
            "early import or disable the importing pytest plugin."
        )
    if os.environ.get(XDIST_WORKER_ENVIRONMENT_VARIABLE):
        state = _state_from_environment()
        expected_environment = _environment(state)
        mismatched = [
            name for name, value in expected_environment.items() if os.environ.get(name) != value
        ]
        if mismatched:
            names = ", ".join(f"`{name}`" for name in sorted(mismatched))
            raise pytest.UsageError(
                f"Inherited xdist Airflow environment disagrees with state: {names}"
            )
        return state
    return _owner_state(config, args)


def get_bootstrap_state(config: pytest.Config) -> BootstrapState:
    """Return typed bootstrap state stored on a pytest configuration.

    Parameters:
        config: pytest.Config containing plugin state.

    Returns:
        BootstrapState containing the active run state.

    Raises:
        pytest.UsageError: Bootstrap state is unavailable.
    """

    try:
        return config.stash[STATE_KEY]
    except KeyError as error:
        raise pytest.UsageError("Airflow bootstrap state is unavailable") from error


def configure_node(node: XdistNode) -> None:
    """Validate a local popen gateway and send primitive state to its worker.

    Parameters:
        node: XdistNode representing one worker controller.

    Raises:
        pytest.UsageError: The worker uses a remote gateway.
    """

    if node.gateway.spec.popen is not True:
        raise pytest.UsageError(
            "Remote xdist gateways are unsupported; use local popen workers: "
            f"'{node.gateway.spec}'"
        )
    node.workerinput[WORKER_INPUT_KEY] = get_bootstrap_state(node.config).to_payload()


def validate_configure(config: pytest.Config) -> None:
    """Validate workerinput against inherited early state after Config exists.

    Parameters:
        config: pytest.Config containing stashed and optional xdist state.

    Raises:
        pytest.UsageError: Worker state is absent, malformed, stale, or inconsistent.
    """

    state = get_bootstrap_state(config)
    if not isinstance(config, XdistWorkerConfig):
        return
    payload = config.workerinput.get(WORKER_INPUT_KEY)
    try:
        worker_state = _state_from_payload(payload, validate_files=True)
    except ValueError as error:
        raise pytest.UsageError(
            f"Invalid controller bootstrap state in workerinput: {error}"
        ) from error
    if worker_state != state:
        raise pytest.UsageError("xdist workerinput bootstrap state differs from inherited state")


__all__ = (
    "STATE_ENVIRONMENT_VARIABLE",
    "BootstrapState",
    "configure_node",
    "get_bootstrap_state",
    "load_initial_state",
    "validate_configure",
)
