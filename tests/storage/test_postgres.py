"""Test the Postgres provisioner through injected seams without Docker."""

from __future__ import annotations

import subprocess
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


class _FailingStartContainer(_FakeContainer):
    """Start a container before reporting a simulated startup failure."""

    def start(self) -> _FakeContainer:
        """Mark the container started, then fail.

        Returns:
            _FakeContainer containing the started container.
        """

        super().start()
        raise RuntimeError("no docker socket")


class _FailingUrlContainer(_FakeContainer):
    """Fail after a container starts but before its URL can be read."""

    def get_connection_url(self) -> str:
        """Raise a simulated connection-URL failure."""

        raise RuntimeError("container did not publish a connection URL")


class _FailingStopContainer(_FakeContainer):
    """Refuse to stop so lifecycle state handling can be asserted."""

    def stop(self) -> None:
        """Raise a simulated Docker removal failure."""

        raise RuntimeError("container removal failed")


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


def test_import_postgres_container_uses_the_community_module() -> None:
    """Return the container class from the preferred community module."""

    seen: list[str] = []

    def import_module(name: str) -> ModuleType:
        seen.append(name)
        return _module_with_container(_FakeContainer)

    container_class = postgres._import_postgres_container(import_module=import_module)

    assert container_class is _FakeContainer
    assert seen == ["testcontainers.community.postgres"]


def test_import_postgres_container_falls_back_to_the_stable_module() -> None:
    """Skip a missing community module and resolve the stable namespace."""

    def import_module(name: str) -> ModuleType:
        if name == "testcontainers.community.postgres":
            raise ImportError("no community module")
        return _module_with_container(_FakeContainer)

    container_class = postgres._import_postgres_container(import_module=import_module)

    assert container_class is _FakeContainer


def test_import_postgres_container_requires_the_extra() -> None:
    """Raise a usage error naming the extra when every module is absent."""

    def import_module(name: str) -> ModuleType:
        raise ImportError(f"missing {name}")

    with pytest.raises(pytest.UsageError, match=r"postgres.* extra"):
        postgres._import_postgres_container(import_module=import_module)


def test_docker_is_available_rejects_a_missing_client() -> None:
    """Do not run a daemon probe when Docker is absent from ``PATH``."""

    assert (
        postgres._docker_is_available(
            which=lambda _name: None,
            run=lambda **_kwargs: pytest.fail("Docker probe must not run"),
        )
        is False
    )


@pytest.mark.parametrize(("returncode", "expected"), [(0, True), (1, False)])
def test_docker_is_available_checks_the_daemon(returncode: int, expected: bool) -> None:
    """Report Docker availability from the daemon probe's exit status."""

    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def run(*args: object, **kwargs: object) -> Any:
        calls.append((args, kwargs))
        return SimpleNamespace(returncode=returncode)

    available = postgres._docker_is_available(
        which=lambda _name: "/usr/bin/docker",
        run=run,
    )

    assert available is expected
    assert calls == [
        (
            (["docker", "info"],),
            {
                "check": False,
                "stdout": subprocess.DEVNULL,
                "stderr": subprocess.DEVNULL,
                "timeout": 5,
            },
        )
    ]


@pytest.mark.parametrize(
    "error",
    [OSError("no docker socket"), subprocess.TimeoutExpired(["docker", "info"], 5)],
)
def test_docker_is_available_rejects_unreachable_daemons(error: Exception) -> None:
    """Treat Docker command failures and timeouts as an unavailable daemon."""

    def run(*_args: object, **_kwargs: object) -> Any:
        raise error

    assert (
        postgres._docker_is_available(
            which=lambda _name: "/usr/bin/docker",
            run=run,
        )
        is False
    )


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
    """Fail loudly when Docker's daemon cannot be reached."""

    with pytest.raises(pytest.UsageError, match="running Docker daemon"):
        require_postgres_available(import_probe=lambda: None, docker_probe=lambda: False)


def test_default_container_factory_threads_dbname() -> None:
    """Build a real container class with the requested image and database name."""

    container = postgres._default_container_factory(
        "postgres:16-alpine",
        "airflow_test_abc",
        import_container=lambda: _FakeContainer,
    )

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


def test_provisioner_wraps_factory_failure() -> None:
    """Wrap a container factory failure in an actionable usage error."""

    def factory(_image: str, _dbname: str) -> ContainerHandle:
        raise RuntimeError("bad image")

    provisioner = PostgresProvisioner(container_factory=factory)

    with pytest.raises(pytest.UsageError, match="Could not start the Postgres test container"):
        provisioner.start(database_path=_UNUSED_DATABASE_PATH, database_name="db")


def test_provisioner_stops_a_container_after_start_failure() -> None:
    """Stop a partially started container before exposing the startup failure."""

    container = _FailingStartContainer("image", "db")
    provisioner = PostgresProvisioner(container_factory=lambda _image, _dbname: container)

    with pytest.raises(pytest.UsageError, match="Could not start the Postgres test container"):
        provisioner.start(database_path=_UNUSED_DATABASE_PATH, database_name="db")

    assert container.started is True
    assert container.stopped is True


def test_provisioner_stops_a_container_after_url_failure() -> None:
    """Stop a container when it starts but cannot yield a connection URL."""

    container = _FailingUrlContainer("image", "db")
    provisioner = PostgresProvisioner(container_factory=lambda _image, _dbname: container)

    with pytest.raises(pytest.UsageError, match="Could not start the Postgres test container"):
        provisioner.start(database_path=_UNUSED_DATABASE_PATH, database_name="db")

    assert container.started is True
    assert container.stopped is True


def test_provisioner_preserves_start_failure_when_cleanup_fails() -> None:
    """Keep the actionable startup error when failed-container cleanup also fails."""

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


def test_provisioner_stop_clears_lifecycle_state_before_removal() -> None:
    """Remain idempotent after Docker reports a container-removal failure."""

    container = _FailingStopContainer("image", "db")
    provisioner = PostgresProvisioner(container_factory=lambda _image, _dbname: container)
    provisioner.start(database_path=_UNUSED_DATABASE_PATH, database_name="db")

    with pytest.raises(RuntimeError, match="container removal failed"):
        provisioner.stop()

    assert provisioner.stop() is None
