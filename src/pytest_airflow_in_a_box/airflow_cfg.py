"""Write the minimal deterministic Apache Airflow test configuration.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html
    https://docs.python.org/3/library/configparser.html
"""

from __future__ import annotations

import configparser
from pathlib import Path

from pytest_airflow_in_a_box._compat.capabilities import AirflowFamily

SIMPLE_AUTH_MANAGER = (
    "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager"
)
# The FAB-provider paths, not the deprecated `airflow.api.auth.backend.*` shims: the
# provider is a hard dependency of every certified 2.x monolith and these import without
# a `RemovedInAirflow3Warning`.
BASIC_AUTH_BACKENDS = (
    "airflow.providers.fab.auth_manager.api.auth.backend.basic_auth,"
    "airflow.providers.fab.auth_manager.api.auth.backend.session"
)


def sqlite_url(database_path: Path) -> str:
    """Build an absolute SQLite SQLAlchemy URL.

    Parameters:
        database_path: pathlib.Path containing the SQLite database location.

    Returns:
        str containing an absolute SQLite SQLAlchemy URL.

    Raises:
        ValueError: The database path is not absolute.
    """

    if not database_path.is_absolute():
        raise ValueError(f"`database_path` must be absolute: '{database_path}'")
    return f"sqlite:///{database_path}"


def write_airflow_config(
    config_path: Path,
    *,
    dags_folder: Path,
    logs_folder: Path,
    sql_alchemy_conn: str,
    password_file: Path,
    jwt_secret: str,
    fernet_key: str,
    family: AirflowFamily,
) -> None:
    """Write a deterministic Airflow configuration without importing Airflow.

    On 3.x the auth surface is SimpleAuthManager plus a JWT secret; 2.x predates both,
    so the same run secret becomes the `[webserver] secret_key` and the REST API uses
    the basic-auth + session backends.

    Parameters:
        config_path: pathlib.Path receiving the generated configuration.
        dags_folder: pathlib.Path containing test Dag files.
        logs_folder: pathlib.Path receiving Airflow logs.
        sql_alchemy_conn: str containing the metadata database SQLAlchemy URL.
        password_file: pathlib.Path containing SimpleAuthManager passwords.
        jwt_secret: str used to sign Airflow API tokens (3.x) or as the webserver
            secret key (2.x).
        fernet_key: str shared by every Airflow process encrypting metadata.
        family: AirflowFamily selecting the configuration surface to write.

    Raises:
        ValueError: A path is relative, or the URL, JWT secret, or Fernet key is empty.
        OSError: The configuration cannot be written.
    """

    paths = {
        "config_path": config_path,
        "dags_folder": dags_folder,
        "logs_folder": logs_folder,
        "password_file": password_file,
    }
    for name, path in paths.items():
        if not path.is_absolute():
            raise ValueError(f"`{name}` must be absolute: '{path}'")
    if not sql_alchemy_conn:
        raise ValueError("`sql_alchemy_conn` must not be empty")
    if not jwt_secret:
        raise ValueError("`jwt_secret` must not be empty")
    if not fernet_key:
        raise ValueError("`fernet_key` must not be empty")

    cfg = configparser.ConfigParser(interpolation=None)
    core = {
        "dags_folder": str(dags_folder),
        "unit_test_mode": "True",
        "load_examples": "False",
    }
    if family is AirflowFamily.V3:
        core |= {
            "auth_manager": SIMPLE_AUTH_MANAGER,
            "simple_auth_manager_users": "admin:admin",
            "simple_auth_manager_all_admins": "False",
            "simple_auth_manager_passwords_file": str(password_file),
        }
    # `fernet_key` stays last so the emitted 3.x file remains byte-identical to the
    # pre-family-branch output.
    core["fernet_key"] = fernet_key
    cfg["core"] = core
    cfg["database"] = {"sql_alchemy_conn": sql_alchemy_conn}
    cfg["logging"] = {"base_log_folder": str(logs_folder)}
    if family is AirflowFamily.V3:
        cfg["api_auth"] = {"jwt_secret": jwt_secret}
    else:
        cfg["webserver"] = {"secret_key": jwt_secret}
        cfg["api"] = {"auth_backends": BASIC_AUTH_BACKENDS}

    config_path.parent.mkdir(parents=True, exist_ok=True)
    with config_path.open("w", encoding="utf-8", newline="\n") as config_file:
        cfg.write(config_file)


__all__ = ("BASIC_AUTH_BACKENDS", "SIMPLE_AUTH_MANAGER", "sqlite_url", "write_airflow_config")
