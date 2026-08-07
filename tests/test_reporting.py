"""Test per-worker report artifact scoping."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from pytest_airflow_in_a_box import reporting


def _config(
    *,
    workerinput: dict[str, object] | None = None,
    log_file: str | None = None,
    log_file_ini: object = None,
    has_cov_plugin: bool = False,
) -> Any:
    """Create a minimal configuration double for reporting tests.

    Parameters:
        workerinput: dict[str, object] | None marking the double as an xdist worker.
        log_file: str | None containing the parsed ``--log-file`` option value.
        log_file_ini: object containing the ``log_file`` ini value.
        has_cov_plugin: bool registering a fake pytest-cov plugin.

    Returns:
        types.SimpleNamespace shaped like the configuration surface under test.
    """

    ini_values = {"log_file": log_file_ini}
    plugins = {reporting.PYTEST_COV_PLUGIN_NAME} if has_cov_plugin else set()
    config = SimpleNamespace(
        option=SimpleNamespace(log_file=log_file),
        getini=lambda name: ini_values[name],
        pluginmanager=SimpleNamespace(hasplugin=lambda name: name in plugins),
    )
    if workerinput is not None:
        config.workerinput = workerinput
    return config


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        (Path("pytest.log"), Path("pytest.gw0.log")),
        (Path("logfile"), Path("logfile.gw0")),
        (Path("run.report.log"), Path("run.report.gw0.log")),
        (Path("logs/pytest.log"), Path("logs/pytest.gw0.log")),
    ],
)
def test_worker_suffixed_path_inserts_identity(path: Path, expected: Path) -> None:
    """Insert the worker identity before the final suffix only."""

    assert reporting.worker_suffixed_path(path, "gw0") == expected


def test_worker_suffixed_path_rejects_empty_worker() -> None:
    """Reject an empty worker identity before building a path."""

    with pytest.raises(ValueError, match="non-empty xdist worker identity"):
        reporting.worker_suffixed_path(Path("pytest.log"), "")


def test_worker_coverage_file_uses_parallel_naming() -> None:
    """Append the worker identity using coverage's parallel data naming."""

    assert reporting.worker_coverage_file(".coverage", "gw1") == ".coverage.gw1"


@pytest.mark.parametrize(
    ("value", "worker", "match"),
    [
        ("", "gw0", "non-empty coverage data file"),
        (".coverage", "", "non-empty xdist worker identity"),
    ],
)
def test_worker_coverage_file_rejects_empty_arguments(
    value: str,
    worker: str,
    match: str,
) -> None:
    """Reject empty data-file and worker arguments."""

    with pytest.raises(ValueError, match=match):
        reporting.worker_coverage_file(value, worker)


def test_configure_reporting_ignores_controller_configuration() -> None:
    """Leave controller and serial-run configuration untouched."""

    config = _config(log_file="pytest.log")

    reporting.configure_reporting(config)

    assert config.option.log_file == "pytest.log"


def test_configure_reporting_scopes_option_log_file() -> None:
    """Rewrite the parsed ``--log-file`` option on an xdist worker."""

    config = _config(workerinput={"workerid": "gw2"}, log_file="pytest.log")

    reporting.configure_reporting(config)

    assert config.option.log_file == "pytest.gw2.log"


def test_configure_reporting_promotes_ini_log_file() -> None:
    """Rewrite an ini-only ``log_file`` value into the worker's option."""

    config = _config(workerinput={"workerid": "gw0"}, log_file_ini="logs/run.log")

    reporting.configure_reporting(config)

    assert config.option.log_file == str(Path("logs/run.gw0.log"))


def test_configure_reporting_without_log_file_changes_nothing() -> None:
    """Leave the log-file option unset when neither source configures one."""

    config = _config(workerinput={"workerid": "gw0"})

    reporting.configure_reporting(config)

    assert config.option.log_file is None


@pytest.mark.parametrize("workerid", [7, ""])
def test_configure_reporting_rejects_malformed_worker_identity(workerid: object) -> None:
    """Reject a missing or non-string xdist worker identity."""

    config = _config(workerinput={"workerid": workerid}, log_file="pytest.log")

    with pytest.raises(pytest.UsageError, match="`workerid` must be a non-empty string"):
        reporting.configure_reporting(config)


def test_configure_reporting_scopes_external_coverage_file(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Rewrite an externally orchestrated coverage data file per worker."""

    monkeypatch.setenv(reporting.COVERAGE_FILE_ENVIRONMENT_VARIABLE, ".coverage")
    config = _config(workerinput={"workerid": "gw3"})

    reporting.configure_reporting(config)

    assert reporting.os.environ[reporting.COVERAGE_FILE_ENVIRONMENT_VARIABLE] == ".coverage.gw3"


def test_configure_reporting_defers_to_pytest_cov(monkeypatch: pytest.MonkeyPatch) -> None:
    """Leave the coverage data file alone when pytest-cov manages coverage."""

    monkeypatch.setenv(reporting.COVERAGE_FILE_ENVIRONMENT_VARIABLE, ".coverage")
    config = _config(workerinput={"workerid": "gw3"}, has_cov_plugin=True)

    reporting.configure_reporting(config)

    assert reporting.os.environ[reporting.COVERAGE_FILE_ENVIRONMENT_VARIABLE] == ".coverage"


def test_configure_reporting_without_coverage_file_changes_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Skip coverage scoping when ``COVERAGE_FILE`` is unset."""

    monkeypatch.delenv(reporting.COVERAGE_FILE_ENVIRONMENT_VARIABLE, raising=False)
    config = _config(workerinput={"workerid": "gw3"})

    reporting.configure_reporting(config)

    assert reporting.COVERAGE_FILE_ENVIRONMENT_VARIABLE not in reporting.os.environ


def test_xdist_workers_write_separate_log_files(pytester: pytest.Pytester) -> None:
    """Write one suffixed log file per worker in a real xdist run."""

    pytester.makepyfile(
        test_split_logs="""
        import logging

        def test_one():
            logging.getLogger("consumer").warning("from a worker")

        def test_two():
            logging.getLogger("consumer").warning("from a worker")
        """
    )

    result = pytester.runpytest_subprocess(
        "-n",
        "2",
        "--log-file=worker.log",
        "--log-file-level=WARNING",
        "-q",
    )

    result.assert_outcomes(passed=2)
    assert (pytester.path / "worker.log").is_file()
    assert (pytester.path / "worker.gw0.log").is_file()
    assert (pytester.path / "worker.gw1.log").is_file()
