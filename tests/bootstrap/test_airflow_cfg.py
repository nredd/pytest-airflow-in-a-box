"""Test deterministic Airflow configuration generation."""

from __future__ import annotations

import configparser
from pathlib import Path

import pytest

from pytest_airflow_in_a_box._compat.capabilities import AirflowFamily
from pytest_airflow_in_a_box.airflow_cfg import (
    BASIC_AUTH_BACKENDS,
    SIMPLE_AUTH_MANAGER,
    sqlite_url,
    write_airflow_config,
)


def _write_stable_config(config_path: Path, tmp_path: Path) -> None:
    """Write one configuration from fixed inputs for determinism checks.

    Parameters:
        config_path: pathlib.Path receiving the generated configuration.
        tmp_path: pathlib.Path anchoring every fabricated absolute path.
    """

    write_airflow_config(
        config_path,
        dags_folder=tmp_path / "dags",
        logs_folder=tmp_path / "logs",
        sql_alchemy_conn=sqlite_url(tmp_path / "airflow.db"),
        password_file=tmp_path / "passwords.json",
        jwt_secret="stable-secret",
        fernet_key="stable-fernet",
    )


def test_write_airflow_config_is_deterministic(tmp_path: Path) -> None:
    """Write byte-identical configuration for identical inputs."""

    config_path = tmp_path / "airflow.cfg"
    _write_stable_config(config_path, tmp_path)
    first = config_path.read_bytes()
    _write_stable_config(config_path, tmp_path)

    assert config_path.read_bytes() == first
    assert (
        first
        == (
            "[core]\n"
            f"dags_folder = {tmp_path / 'dags'}\n"
            "unit_test_mode = True\n"
            "load_examples = False\n"
            f"auth_manager = {SIMPLE_AUTH_MANAGER}\n"
            "simple_auth_manager_users = admin:admin\n"
            "simple_auth_manager_all_admins = False\n"
            f"simple_auth_manager_passwords_file = {tmp_path / 'passwords.json'}\n"
            "fernet_key = stable-fernet\n"
            "\n"
            "[database]\n"
            f"sql_alchemy_conn = sqlite:///{tmp_path / 'airflow.db'}\n"
            "\n"
            "[logging]\n"
            f"base_log_folder = {tmp_path / 'logs'}\n"
            "\n"
            "[api_auth]\n"
            "jwt_secret = stable-secret\n"
            "\n"
        ).encode()
    )


def test_generated_config_accepts_a_postgres_url(tmp_path: Path) -> None:
    """Write any SQLAlchemy URL verbatim into the database section."""

    config_path = tmp_path / "airflow.cfg"
    postgres_url = "postgresql+psycopg2://airflow:airflow@localhost:5432/airflow_test_abc"
    write_airflow_config(
        config_path,
        dags_folder=tmp_path / "dags",
        logs_folder=tmp_path / "logs",
        sql_alchemy_conn=postgres_url,
        password_file=tmp_path / "passwords.json",
        jwt_secret="jwt-value",
        fernet_key="fernet-value",
    )
    cfg = configparser.ConfigParser()
    cfg.read(config_path)

    assert cfg.get("database", "sql_alchemy_conn") == postgres_url


def test_generated_config_contains_required_airflow_settings(tmp_path: Path) -> None:
    """Configure test mode, paths, SQLite, SimpleAuthManager, and API signing."""

    config_path = tmp_path / "airflow.cfg"
    write_airflow_config(
        config_path,
        dags_folder=tmp_path / "dags",
        logs_folder=tmp_path / "logs",
        sql_alchemy_conn=sqlite_url(tmp_path / "airflow.db"),
        password_file=tmp_path / "passwords.json",
        jwt_secret="jwt-value",
        fernet_key="fernet-value",
    )
    cfg = configparser.ConfigParser()
    cfg.read(config_path)

    assert cfg.getboolean("core", "unit_test_mode")
    assert not cfg.getboolean("core", "load_examples")
    assert cfg.get("core", "auth_manager") == SIMPLE_AUTH_MANAGER
    assert cfg.get("core", "simple_auth_manager_users") == "admin:admin"
    assert cfg.get("database", "sql_alchemy_conn") == sqlite_url(tmp_path / "airflow.db")
    assert cfg.get("api_auth", "jwt_secret") == "jwt-value"
    assert cfg.get("core", "fernet_key") == "fernet-value"


def test_v2_config_swaps_the_auth_surface(tmp_path: Path) -> None:
    """Write webserver + basic-auth settings and no 3.x auth keys on the 2.x family."""

    config_path = tmp_path / "airflow.cfg"
    write_airflow_config(
        config_path,
        dags_folder=tmp_path / "dags",
        logs_folder=tmp_path / "logs",
        sql_alchemy_conn=sqlite_url(tmp_path / "airflow.db"),
        password_file=tmp_path / "passwords.json",
        jwt_secret="run-secret",
        fernet_key="fernet-value",
        family=AirflowFamily.V2,
    )
    cfg = configparser.ConfigParser()
    cfg.read(config_path)

    assert cfg.get("webserver", "secret_key") == "run-secret"
    assert cfg.get("api", "auth_backends") == BASIC_AUTH_BACKENDS
    assert not cfg.has_section("api_auth")
    assert not cfg.has_option("core", "auth_manager")
    assert not cfg.has_option("core", "simple_auth_manager_users")
    assert cfg.getboolean("core", "unit_test_mode")
    assert cfg.get("core", "fernet_key") == "fernet-value"


def test_config_writer_rejects_relative_paths(tmp_path: Path) -> None:
    """Reject ambiguous relative paths before writing configuration."""

    with pytest.raises(ValueError, match="`dags_folder` must be absolute"):
        write_airflow_config(
            tmp_path / "airflow.cfg",
            dags_folder=Path("dags"),
            logs_folder=tmp_path / "logs",
            sql_alchemy_conn=sqlite_url(tmp_path / "airflow.db"),
            password_file=tmp_path / "passwords.json",
            jwt_secret="jwt-value",
            fernet_key="fernet-value",
        )


def test_sqlite_url_rejects_relative_path() -> None:
    """Require a process-independent absolute SQLite location."""

    with pytest.raises(ValueError, match="`database_path` must be absolute"):
        sqlite_url(Path("airflow.db"))


def test_write_airflow_config_requires_a_database_url(tmp_path: Path) -> None:
    """Reject an empty metadata database URL before writing configuration."""

    with pytest.raises(ValueError, match="`sql_alchemy_conn` must not be empty"):
        write_airflow_config(
            tmp_path / "airflow.cfg",
            dags_folder=tmp_path / "dags",
            logs_folder=tmp_path / "logs",
            sql_alchemy_conn="",
            password_file=tmp_path / "passwords.json",
            jwt_secret="jwt-value",
            fernet_key="fernet-value",
        )


def test_write_airflow_config_requires_a_jwt_secret(tmp_path: Path) -> None:
    """Reject an empty JWT secret before writing configuration."""

    with pytest.raises(ValueError, match="`jwt_secret` must not be empty"):
        write_airflow_config(
            tmp_path / "airflow.cfg",
            dags_folder=tmp_path / "dags",
            logs_folder=tmp_path / "logs",
            sql_alchemy_conn=sqlite_url(tmp_path / "airflow.db"),
            password_file=tmp_path / "passwords.json",
            jwt_secret="",
            fernet_key="fernet-value",
        )


def test_write_airflow_config_requires_a_fernet_key(tmp_path: Path) -> None:
    """Reject an empty Fernet key before writing configuration."""

    with pytest.raises(ValueError, match="`fernet_key` must not be empty"):
        write_airflow_config(
            tmp_path / "airflow.cfg",
            dags_folder=tmp_path / "dags",
            logs_folder=tmp_path / "logs",
            sql_alchemy_conn=sqlite_url(tmp_path / "airflow.db"),
            password_file=tmp_path / "passwords.json",
            jwt_secret="jwt-value",
            fernet_key="",
        )
