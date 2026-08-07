"""Test deterministic Airflow configuration generation."""

from __future__ import annotations

import configparser
from pathlib import Path

import pytest

from pytest_airflow_in_a_box.airflow_cfg import (
    SIMPLE_AUTH_MANAGER,
    sqlite_url,
    write_airflow_config,
)


def test_write_airflow_config_is_deterministic(tmp_path: Path) -> None:
    """Write byte-identical configuration for identical inputs."""

    config_path = tmp_path / "airflow.cfg"
    values = {
        "dags_folder": tmp_path / "dags",
        "logs_folder": tmp_path / "logs",
        "database_path": tmp_path / "airflow.db",
        "password_file": tmp_path / "passwords.json",
        "jwt_secret": "stable-secret",
    }

    write_airflow_config(config_path, **values)
    first = config_path.read_bytes()
    write_airflow_config(config_path, **values)

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


def test_generated_config_contains_required_airflow_settings(tmp_path: Path) -> None:
    """Configure test mode, paths, SQLite, SimpleAuthManager, and API signing."""

    config_path = tmp_path / "airflow.cfg"
    write_airflow_config(
        config_path,
        dags_folder=tmp_path / "dags",
        logs_folder=tmp_path / "logs",
        database_path=tmp_path / "airflow.db",
        password_file=tmp_path / "passwords.json",
        jwt_secret="jwt-value",
    )
    cfg = configparser.ConfigParser()
    cfg.read(config_path)

    assert cfg.getboolean("core", "unit_test_mode")
    assert not cfg.getboolean("core", "load_examples")
    assert cfg.get("core", "auth_manager") == SIMPLE_AUTH_MANAGER
    assert cfg.get("core", "simple_auth_manager_users") == "admin:admin"
    assert cfg.get("database", "sql_alchemy_conn") == sqlite_url(tmp_path / "airflow.db")
    assert cfg.get("api_auth", "jwt_secret") == "jwt-value"


def test_config_writer_rejects_relative_paths(tmp_path: Path) -> None:
    """Reject ambiguous relative paths before writing configuration."""

    with pytest.raises(ValueError, match="`dags_folder` must be absolute"):
        write_airflow_config(
            tmp_path / "airflow.cfg",
            dags_folder=Path("dags"),
            logs_folder=tmp_path / "logs",
            database_path=tmp_path / "airflow.db",
            password_file=tmp_path / "passwords.json",
            jwt_secret="jwt-value",
        )


def test_sqlite_url_rejects_relative_path() -> None:
    """Require a process-independent absolute SQLite location."""

    with pytest.raises(ValueError, match="`database_path` must be absolute"):
        sqlite_url(Path("airflow.db"))


def test_write_airflow_config_requires_a_jwt_secret(tmp_path: Path) -> None:
    """Reject an empty JWT secret before writing configuration."""

    with pytest.raises(ValueError, match="`jwt_secret` must not be empty"):
        write_airflow_config(
            tmp_path / "airflow.cfg",
            dags_folder=tmp_path / "dags",
            logs_folder=tmp_path / "logs",
            database_path=tmp_path / "airflow.db",
            password_file=tmp_path / "passwords.json",
            jwt_secret="",
        )
