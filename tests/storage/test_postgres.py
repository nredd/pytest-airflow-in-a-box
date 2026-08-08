"""Test the Postgres provisioner through injected seams without Docker."""

from __future__ import annotations

from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from pytest_airflow_in_a_box.storage import postgres
from pytest_airflow_in_a_box.storage.postgres import (
    DEFAULT_POSTGRES_IMAGE,
    ContainerHandle,
    PostgresProvisioner,
    require_postgres_available,
)

_UNUSED_DATABASE_PATH = Path("/unused/airflow.db")


class _FakeContainer:
    """Record lifecycle calls and hand back a canned connection URL."""

    def __init__(self, image: str, dbname: str) -> None:
        self.image = image
        self.dbname = dbname
        self.started = False
        self.stopped = False

    def start(self) -> _FakeContainer:
        """Mark the container started.

        Returns:
            _FakeContainer containing the started container.
        """

        self.started = True
        return self

    def stop(self) -> None:
        """Mark the container stopped."""

        self.stopped = True

    def get_connection_url(self) -> str:
        """Return a deterministic Postgres URL for assertions.

        Returns:
            str containing a canned Postgres SQLAlchemy URL.
        """

        return f"postgresql+psycopg2://user:pass@localhost:5432/{self.dbname}"


def _fake_factory(image: str, dbname: str) -> ContainerHandle:
    """Build a recording fake container as a provisioner factory.

    Parameters:
        image: str naming the requested container image.
        dbname: str naming the requested per-run database.

    Returns:
        ContainerHandle containing an unstarted fake container.
    """

    return _FakeContainer(image, dbname)


def _module_with_container(container_class: type) -> ModuleType:
    """Wrap a container class in a throwaway module exposing ``PostgresContainer``.

    Parameters:
        container_class: type published as the module's ``PostgresContainer``.

    Returns:
        types.ModuleType exposing the container class.
    """

    module: Any = ModuleType("fake_testcontainers")
    module.PostgresContainer = container_class
    return module


def test_import_postgres_container_uses_the_stable_module() -> None:
    """Return the container class from the first importable module name."""

    seen: list[str] = []

    def import_module(name: str) -> ModuleType:
        seen.append(name)
        return _module_with_container(_FakeContainer)

    container_class = postgres._import_postgres_container(import_module=import_module)

    assert container_class is _FakeContainer
    assert seen == ["testcontainers.postgres"]


def test_import_postgres_container_falls_back_to_community() -> None:
    """Skip a missing stable module and resolve the community namespace."""

    def import_module(name: str) -> ModuleType:
        if name == "testcontainers.postgres":
            raise ImportError("no stable module")
        return _module_with_container(_FakeContainer)

    container_class = postgres._import_postgres_container(import_module=import_module)

    assert container_class is _FakeContainer


def test_import_postgres_container_requires_the_extra() -> None:
    """Raise a usage error naming the extra when every module is absent."""

    def import_module(name: str) -> ModuleType:
        raise ImportError(f"missing {name}")

    with pytest.raises(pytest.UsageError, match=r"postgres.* extra"):
        postgres._import_postgres_container(import_module=import_module)


@pytest.mark.parametrize(
    ("resolved", "expected"),
    [("/usr/bin/docker", True), (None, False)],
)
def test_docker_is_available_reflects_which(resolved: str | None, expected: bool) -> None:
    """Report availability from whether ``which`` resolves ``docker``."""

    assert postgres._docker_is_available(which=lambda _name: resolved) is expected


def test_require_postgres_available_passes_when_ready() -> None:
    """Return cleanly when the extra imports and Docker is present."""

    require_postgres_available(import_probe=lambda: None, docker_probe=lambda: True)


def test_require_postgres_available_rejects_missing_extra() -> None:
    """Propagate the import probe failure for a missing extra."""

    def import_probe() -> None:
        raise pytest.UsageError("install the extra")

    with pytest.raises(pytest.UsageError, match="install the extra"):
        require_postgres_available(import_probe=import_probe, docker_probe=lambda: True)


def test_require_postgres_available_rejects_missing_daemon() -> None:
    """Fail loudly when no Docker client is discoverable."""

    with pytest.raises(pytest.UsageError, match="running Docker daemon"):
        require_postgres_available(import_probe=lambda: None, docker_probe=lambda: False)


def test_default_container_factory_threads_dbname(monkeypatch: pytest.MonkeyPatch) -> None:
    """Build a real container class with the requested image and database name."""

    monkeypatch.setattr(postgres, "_import_postgres_container", lambda: _FakeContainer)

    container = postgres._default_container_factory("postgres:16-alpine", "airflow_test_abc")

    assert isinstance(container, _FakeContainer)
    assert container.image == "postgres:16-alpine"
    assert container.dbname == "airflow_test_abc"


def test_provisioner_uses_the_default_image() -> None:
    """Pass the default image to the container factory."""

    captured: dict[str, str] = {}

    def factory(image: str, dbname: str) -> ContainerHandle:
        captured["image"] = image
        return _FakeContainer(image, dbname)

    provisioner = PostgresProvisioner(container_factory=factory)
    provisioner.start(database_path=_UNUSED_DATABASE_PATH, database_name="airflow_test_abc")

    assert captured["image"] == DEFAULT_POSTGRES_IMAGE


def test_provisioner_start_returns_container_url() -> None:
    """Start the container and return its connection URL."""

    containers: list[_FakeContainer] = []

    def factory(image: str, dbname: str) -> ContainerHandle:
        container = _FakeContainer(image, dbname)
        containers.append(container)
        return container

    provisioner = PostgresProvisioner(container_factory=factory)

    url = provisioner.start(database_path=_UNUSED_DATABASE_PATH, database_name="airflow_test_xyz")

    assert url.endswith("/airflow_test_xyz")
    assert containers[0].started is True


def test_provisioner_rejects_double_start() -> None:
    """Refuse to start a provisioner that already holds a container."""

    provisioner = PostgresProvisioner(container_factory=_fake_factory)
    provisioner.start(database_path=_UNUSED_DATABASE_PATH, database_name="db")

    with pytest.raises(RuntimeError, match="already started"):
        provisioner.start(database_path=_UNUSED_DATABASE_PATH, database_name="db")


def test_provisioner_wraps_container_start_failure() -> None:
    """Wrap a container start failure in an actionable usage error."""

    def failing_start() -> Any:
        raise RuntimeError("no docker socket")

    def factory(image: str, dbname: str) -> ContainerHandle:
        del image, dbname
        handle: Any = SimpleNamespace(start=failing_start)
        return handle

    provisioner = PostgresProvisioner(container_factory=factory)

    with pytest.raises(pytest.UsageError, match="Could not start the Postgres test container"):
        provisioner.start(database_path=_UNUSED_DATABASE_PATH, database_name="db")


def test_provisioner_stop_after_start_releases_the_container() -> None:
    """Stop the running container and clear provisioner state."""

    container = _FakeContainer("image", "db")
    provisioner = PostgresProvisioner(container_factory=lambda _image, _dbname: container)
    provisioner.start(database_path=_UNUSED_DATABASE_PATH, database_name="db")

    provisioner.stop()

    assert container.stopped is True


def test_provisioner_stop_without_start_is_a_no_op() -> None:
    """Release nothing when no container was ever started."""

    provisioner = PostgresProvisioner(container_factory=_fake_factory)

    assert provisioner.stop() is None
