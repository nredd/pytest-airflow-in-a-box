"""Provision a disposable Postgres metadata database for isolated Airflow tests.

The provisioner starts one Docker container per session and hands Airflow the
resulting SQLAlchemy URL. Every ``testcontainers`` touch is deferred behind an
injected seam so this module imports without the ``postgres`` extra installed
and reaches full branch coverage on a host with no Docker daemon.

References:
    https://testcontainers-python.readthedocs.io/en/latest/modules/postgres/README.html
    https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#sql-alchemy-conn
"""

from __future__ import annotations

import importlib
import logging
import shutil
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Protocol

import pytest

LOGGER = logging.getLogger(__name__)

DEFAULT_POSTGRES_IMAGE = "postgres:16-alpine"
_POSTGRES_CONTAINER_MODULES = (
    "testcontainers.postgres",
    "testcontainers.community.postgres",
)


class ContainerHandle(Protocol):
    """Minimal ``testcontainers`` Postgres container surface used by the plugin."""

    def start(self) -> ContainerHandle:
        """Start the container and block until it is ready.

        Returns:
            ContainerHandle containing the started container.
        """

    def stop(self) -> None:
        """Stop and remove the container."""

    def get_connection_url(self) -> str:
        """Return the SQLAlchemy URL for the running container.

        Returns:
            str containing a Postgres SQLAlchemy URL.
        """


class PostgresContainerClass(Protocol):
    """The ``PostgresContainer`` class surface required by the plugin."""

    def __call__(self, image: str, *, dbname: str) -> ContainerHandle:
        """Construct an unstarted Postgres container.

        Parameters:
            image: str naming the Postgres container image.
            dbname: str naming the initial database created inside the container.

        Returns:
            ContainerHandle containing an unstarted Postgres container.
        """


ContainerFactory = Callable[[str, str], ContainerHandle]
AvailabilityProbe = Callable[[], None]
ModuleImporter = Callable[[str], ModuleType]
DockerProbe = Callable[[], bool]


def _import_postgres_container(
    *, import_module: ModuleImporter = importlib.import_module
) -> PostgresContainerClass:
    """Import ``PostgresContainer`` from whichever testcontainers module provides it.

    Parameters:
        import_module: ModuleImporter resolving a module name to a module.

    Returns:
        PostgresContainerClass containing the resolved ``PostgresContainer`` class.

    Raises:
        pytest.UsageError: The ``postgres`` extra is not installed.
    """

    for module_name in _POSTGRES_CONTAINER_MODULES:
        try:
            module = import_module(module_name)
        except ImportError:
            continue
        return module.PostgresContainer
    raise pytest.UsageError(
        "Postgres backend requires the `postgres` extra: install "
        "`pytest-airflow-in-a-box[postgres]` (testcontainers plus a Docker daemon)."
    )


def _docker_is_available(*, which: Callable[[str], str | None] = shutil.which) -> bool:
    """Report whether a ``docker`` client is discoverable on ``PATH``.

    Parameters:
        which: Callable resolving an executable name to a path or ``None``.

    Returns:
        bool indicating whether a ``docker`` executable was found.
    """

    return which("docker") is not None


def require_postgres_available(
    *,
    import_probe: Callable[[], object] = _import_postgres_container,
    docker_probe: DockerProbe = _docker_is_available,
) -> None:
    """Fail loudly unless both the ``postgres`` extra and a Docker client are present.

    Parameters:
        import_probe: Callable importing the container class, raising when absent.
        docker_probe: DockerProbe reporting whether a Docker client is available.

    Raises:
        pytest.UsageError: The extra is missing or no Docker client is available.
    """

    import_probe()
    if not docker_probe():
        raise pytest.UsageError(
            "Postgres backend requires a running Docker daemon; no `docker` client was "
            "found on PATH."
        )


def _default_container_factory(image: str, dbname: str) -> ContainerHandle:
    """Build a real ``PostgresContainer`` for the requested image and database.

    Parameters:
        image: str naming the Postgres container image.
        dbname: str naming the initial database created inside the container.

    Returns:
        ContainerHandle containing an unstarted Postgres container.
    """

    postgres_container = _import_postgres_container()
    return postgres_container(image, dbname=dbname)


class PostgresProvisioner:
    """Provision one shared Postgres container for a controller and its workers."""

    def __init__(
        self,
        *,
        image: str = DEFAULT_POSTGRES_IMAGE,
        container_factory: ContainerFactory | None = None,
    ) -> None:
        """Store the container image and factory without touching Docker.

        Parameters:
            image: str naming the Postgres container image.
            container_factory: ContainerFactory | None building a container, or
                ``None`` to build a real ``PostgresContainer``.
        """

        self._image = image
        self._container_factory = container_factory or _default_container_factory
        self._container: ContainerHandle | None = None

    def start(self, *, database_path: Path, database_name: str) -> str:
        """Start the container and return its SQLAlchemy URL.

        Parameters:
            database_path: pathlib.Path ignored by the Postgres backend.
            database_name: str naming the per-run database inside the container.

        Returns:
            str containing the running container's Postgres SQLAlchemy URL.

        Raises:
            RuntimeError: The provisioner was already started.
            pytest.UsageError: The container could not be started.
        """

        del database_path
        if self._container is not None:
            raise RuntimeError("Postgres provisioner is already started")
        container = self._container_factory(self._image, database_name)
        try:
            container.start()
            url = container.get_connection_url()
        except Exception as error:
            raise pytest.UsageError(
                f"Could not start the Postgres test container: {error}"
            ) from error
        self._container = container
        LOGGER.info(f"Started Postgres test container for database '{database_name}'")
        return url

    def stop(self) -> None:
        """Stop the container if one is running; idempotent."""

        if self._container is None:
            return
        self._container.stop()
        self._container = None


__all__ = (
    "DEFAULT_POSTGRES_IMAGE",
    "AvailabilityProbe",
    "ContainerFactory",
    "ContainerHandle",
    "PostgresProvisioner",
    "require_postgres_available",
)
