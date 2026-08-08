"""Write the minimal deterministic Apache Airflow test configuration.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html
    https://docs.python.org/3/library/configparser.html
"""

from __future__ import annotations

import configparser
from pathlib import Path

SIMPLE_AUTH_MANAGER = (
    "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager"
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
) -> None:
    """Write a deterministic Airflow configuration without importing Airflow.

    Parameters:
        config_path: pathlib.Path receiving the generated configuration.
        dags_folder: pathlib.Path containing test Dag files.
        logs_folder: pathlib.Path receiving Airflow logs.
        sql_alchemy_conn: str containing the metadata database SQLAlchemy URL.
        password_file: pathlib.Path containing SimpleAuthManager passwords.
        jwt_secret: str used to sign Airflow API tokens.

    Raises:
        ValueError: A path is relative, or the URL or JWT secret is empty.
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

    cfg = configparser.ConfigParser(interpolation=None)
    cfg["core"] = {
        "dags_folder": str(dags_folder),
        "unit_test_mode": "True",
        "load_examples": "False",
        "auth_manager": SIMPLE_AUTH_MANAGER,
        "simple_auth_manager_users": "admin:admin",
        "simple_auth_manager_all_admins": "False",
        "simple_auth_manager_passwords_file": str(password_file),
    }
    cfg["database"] = {"sql_alchemy_conn": sql_alchemy_conn}
    cfg["logging"] = {"base_log_folder": str(logs_folder)}
    cfg["api_auth"] = {"jwt_secret": jwt_secret}

    config_path.parent.mkdir(parents=True, exist_ok=True)
    with config_path.open("w", encoding="utf-8", newline="\n") as config_file:
        cfg.write(config_file)


__all__ = ("SIMPLE_AUTH_MANAGER", "sqlite_url", "write_airflow_config")
