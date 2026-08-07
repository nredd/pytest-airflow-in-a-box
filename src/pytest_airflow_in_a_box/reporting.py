"""Per-worker report artifact paths for pytest-xdist runs.

pytest's own ``junitxml`` plugin already skips XML writing on xdist workers, so
JUnit output needs no handling here. The stdlib-logging plugin has no such
guard: every worker opens the configured ``--log-file`` destination and the
writers race. Externally orchestrated coverage through ``COVERAGE_FILE`` has
the same collision. Both destinations are rewritten to per-worker names on
workers only; controller and serial runs are untouched.

References:
    https://docs.pytest.org/en/stable/how-to/logging.html
    https://pytest-xdist.readthedocs.io/en/stable/how-to.html
    https://coverage.readthedocs.io/en/latest/cmd.html#data-file
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import pytest

from pytest_airflow_in_a_box.bootstrap import XdistWorkerConfig

LOGGER = logging.getLogger(__name__)

COVERAGE_FILE_ENVIRONMENT_VARIABLE = "COVERAGE_FILE"
PYTEST_COV_PLUGIN_NAME = "_cov"
WORKER_ID_KEY = "workerid"


def worker_suffixed_path(path: Path, worker: str) -> Path:
    """Insert one worker identity before the final path suffix.

    Parameters:
        path: pathlib.Path containing the shared artifact destination.
        worker: str containing the xdist worker identity.

    Returns:
        pathlib.Path containing the worker-scoped destination.

    Raises:
        ValueError: The worker identity is empty.
    """

    if not worker:
        raise ValueError("`worker` must be a non-empty xdist worker identity")
    if path.suffix:
        return path.with_name(f"{path.stem}.{worker}{path.suffix}")
    return path.with_name(f"{path.name}.{worker}")


def worker_coverage_file(value: str, worker: str) -> str:
    """Append one worker identity using coverage's parallel data-file naming.

    Parameters:
        value: str containing the configured coverage data file.
        worker: str containing the xdist worker identity.

    Returns:
        str containing the worker-scoped coverage data file.

    Raises:
        ValueError: The data file or the worker identity is empty.
    """

    if not value:
        raise ValueError("`value` must be a non-empty coverage data file")
    if not worker:
        raise ValueError("`worker` must be a non-empty xdist worker identity")
    return f"{value}.{worker}"


def _worker_identity(config: pytest.Config) -> str | None:
    """Read the validated xdist worker identity from one configuration.

    Parameters:
        config: pytest.Config possibly extended with xdist worker input.

    Returns:
        str | None containing the worker identity on xdist workers.

    Raises:
        pytest.UsageError: The worker identity is missing or malformed.
    """

    if not isinstance(config, XdistWorkerConfig):
        return None
    worker = config.workerinput.get(WORKER_ID_KEY)
    if not isinstance(worker, str) or not worker:
        raise pytest.UsageError(
            f"xdist workerinput field `{WORKER_ID_KEY}` must be a non-empty string"
        )
    return worker


def _configured_log_file(config: pytest.Config) -> str | None:
    """Resolve the effective pytest log file from option or ini configuration.

    Parameters:
        config: pytest.Config containing parsed options and ini values.

    Returns:
        str | None containing the configured log file destination.
    """

    option_value: object = config.option.log_file
    if isinstance(option_value, str) and option_value:
        return option_value
    ini_value: object = config.getini("log_file")
    if isinstance(ini_value, str) and ini_value:
        return ini_value
    return None


def configure_reporting(config: pytest.Config) -> None:
    """Scope shared report artifacts to one xdist worker.

    Must run before pytest's logging plugin instantiates its file handler,
    which reads the ``log_file`` option exactly once during configuration.

    Parameters:
        config: pytest.Config for the active test process.

    Raises:
        pytest.UsageError: xdist worker input is malformed.
    """

    worker = _worker_identity(config)
    if worker is None:
        return
    log_file = _configured_log_file(config)
    if log_file is not None:
        scoped_log = str(worker_suffixed_path(Path(log_file), worker))
        config.option.log_file = scoped_log
        LOGGER.debug(f"Scoped pytest log file to worker `{worker}`: '{scoped_log}'")
    coverage_file = os.environ.get(COVERAGE_FILE_ENVIRONMENT_VARIABLE)
    if coverage_file and not config.pluginmanager.hasplugin(PYTEST_COV_PLUGIN_NAME):
        scoped_coverage = worker_coverage_file(coverage_file, worker)
        os.environ[COVERAGE_FILE_ENVIRONMENT_VARIABLE] = scoped_coverage
        LOGGER.debug(f"Scoped coverage data file to worker `{worker}`: '{scoped_coverage}'")


__all__ = ("configure_reporting", "worker_coverage_file", "worker_suffixed_path")
