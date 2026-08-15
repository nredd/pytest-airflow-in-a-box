"""Report artifact destinations: one opt-in directory, and per-worker scoping.

``configure_report_dir`` derives pytest's own log-file and JUnit XML
destinations from a single opt-in ``--airflow-report-dir``, so a CI job archives
``pytest.log`` and ``pytest.xml`` without wiring three unrelated pytest flags.
It is inert unless that option (or its ini form) is set: the plugin never writes
report files into a consumer's repository by default.

``configure_reporting`` then scopes shared destinations per xdist worker.
pytest's own ``junitxml`` plugin already skips XML writing on xdist workers, so
JUnit output needs no handling there. The stdlib-logging plugin has no such
guard: every worker opens the configured ``--log-file`` destination and the
writers race. Externally orchestrated coverage through ``COVERAGE_FILE`` has
the same collision. Both destinations are rewritten to per-worker names on
workers only; controller and serial runs are untouched. Deriving before scoping
is what lets a derived log file be worker-suffixed exactly like a user-supplied
one.

References:
    https://docs.pytest.org/en/stable/how-to/logging.html
    https://docs.pytest.org/en/stable/how-to/output.html#creating-junitxml-format-files
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

REPORT_DIR_SETTING = "airflow_report_dir"
REPORT_LOG_FILE_NAME = "pytest.log"
REPORT_LOG_FILE_LEVEL = "DEBUG"
REPORT_JUNIT_XML_NAME = "pytest.xml"


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


def _configured_value(config: pytest.Config, name: str) -> str | None:
    """Resolve one setting whose option destination and ini key share a name.

    Every setting read through this helper -- pytest's own ``log_file`` and
    ``log_file_level``, and the plugin's ``airflow_report_dir`` -- is registered
    as a command-line option and an ini key under the same name, with the parsed
    option winning over the ini value and either possibly absent.

    Parameters:
        config: pytest.Config containing parsed options and ini values.
        name: str naming the option destination and the matching ini key.

    Returns:
        str | None containing the configured value, or ``None`` when unset.
    """

    option_value: object = getattr(config.option, name)
    if isinstance(option_value, str) and option_value:
        return option_value
    ini_value: object = config.getini(name)
    if isinstance(ini_value, str) and ini_value:
        return ini_value
    return None


def configure_report_dir(config: pytest.Config) -> None:
    """Derive pytest's log-file and JUnit XML destinations from one directory.

    Inert unless ``--airflow-report-dir`` or the ``airflow_report_dir`` ini value
    is set. Every derived destination yields to an explicit one, so
    ``--log-file``, ``--log-file-level``, and ``--junit-xml`` (and their ini
    forms) always win. ``junit_family`` is deliberately untouched: pytest already
    defaults it to ``xunit2``.

    Must run before ``configure_reporting`` so a derived log file is scoped per
    xdist worker exactly like a user-supplied one, and inside a ``tryfirst``
    ``pytest_configure`` so both land before pytest's own ``logging`` and
    ``junitxml`` plugins read these settings.

    Parameters:
        config: pytest.Config for the active test process.

    Raises:
        pytest.UsageError: The report directory cannot be created.
    """

    configured = _configured_value(config, REPORT_DIR_SETTING)
    if configured is None:
        return
    report_dir = Path(configured)
    try:
        report_dir.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise pytest.UsageError(
            f"Could not create report directory `{report_dir}`: {error}"
        ) from error
    if _configured_value(config, "log_file") is None:
        config.option.log_file = str(report_dir / REPORT_LOG_FILE_NAME)
    if _configured_value(config, "log_file_level") is None:
        config.option.log_file_level = REPORT_LOG_FILE_LEVEL
    if config.option.xmlpath is None:
        config.option.xmlpath = str(report_dir / REPORT_JUNIT_XML_NAME)
    LOGGER.debug(f"Derived report artifact destinations below '{report_dir}'")


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
    log_file = _configured_value(config, "log_file")
    if log_file is not None:
        scoped_log = str(worker_suffixed_path(Path(log_file), worker))
        config.option.log_file = scoped_log
        LOGGER.debug(f"Scoped pytest log file to worker `{worker}`: '{scoped_log}'")
    coverage_file = os.environ.get(COVERAGE_FILE_ENVIRONMENT_VARIABLE)
    if coverage_file and not config.pluginmanager.hasplugin(PYTEST_COV_PLUGIN_NAME):
        scoped_coverage = worker_coverage_file(coverage_file, worker)
        os.environ[COVERAGE_FILE_ENVIRONMENT_VARIABLE] = scoped_coverage
        LOGGER.debug(f"Scoped coverage data file to worker `{worker}`: '{scoped_coverage}'")


__all__ = (
    "configure_report_dir",
    "configure_reporting",
    "worker_coverage_file",
    "worker_suffixed_path",
)
