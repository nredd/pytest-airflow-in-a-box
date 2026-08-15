"""Test report artifact destination derivation and per-worker scoping."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any
from xml.etree import ElementTree

import pytest

from pytest_airflow_in_a_box import reporting


def _config(
    *,
    workerinput: dict[str, object] | None = None,
    log_file: str | None = None,
    log_file_ini: object = None,
    log_file_level: str | None = None,
    log_file_level_ini: object = None,
    xmlpath: str | None = None,
    report_dir: str | None = None,
    report_dir_ini: object = None,
    has_cov_plugin: bool = False,
) -> Any:
    """Create a minimal configuration double for reporting tests.

    Parameters:
        workerinput: dict[str, object] | None marking the double as an xdist worker.
        log_file: str | None containing the parsed ``--log-file`` option value.
        log_file_ini: object containing the ``log_file`` ini value.
        log_file_level: str | None containing the parsed ``--log-file-level`` value.
        log_file_level_ini: object containing the ``log_file_level`` ini value.
        xmlpath: str | None containing the parsed ``--junit-xml`` option value.
        report_dir: str | None containing the parsed ``--airflow-report-dir`` value.
        report_dir_ini: object containing the ``airflow_report_dir`` ini value.
        has_cov_plugin: bool registering a fake pytest-cov plugin.

    Returns:
        types.SimpleNamespace shaped like the configuration surface under test.
    """

    ini_values = {
        "log_file": log_file_ini,
        "log_file_level": log_file_level_ini,
        reporting.REPORT_DIR_SETTING: report_dir_ini,
    }
    plugins = {reporting.PYTEST_COV_PLUGIN_NAME} if has_cov_plugin else set()
    config = SimpleNamespace(
        option=SimpleNamespace(
            log_file=log_file,
            log_file_level=log_file_level,
            xmlpath=xmlpath,
            airflow_report_dir=report_dir,
        ),
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


def test_configure_report_dir_without_configuration_changes_nothing(tmp_path: Path) -> None:
    """Stay inert when neither the option nor the ini value names a directory."""

    config = _config()

    reporting.configure_report_dir(config)

    assert config.option.log_file is None
    assert config.option.log_file_level is None
    assert config.option.xmlpath is None
    assert list(tmp_path.iterdir()) == []


def test_configure_report_dir_derives_every_destination(tmp_path: Path) -> None:
    """Derive the log file, its level, and the JUnit XML path from the directory."""

    report_dir = tmp_path / "nested" / "reports"
    config = _config(report_dir=str(report_dir))

    reporting.configure_report_dir(config)

    assert report_dir.is_dir()
    assert config.option.log_file == str(report_dir / "pytest.log")
    assert config.option.log_file_level == "DEBUG"
    assert config.option.xmlpath == str(report_dir / "pytest.xml")


def test_configure_report_dir_promotes_ini_value(tmp_path: Path) -> None:
    """Accept an ini-only ``airflow_report_dir`` value."""

    report_dir = tmp_path / "ini-reports"
    config = _config(report_dir_ini=str(report_dir))

    reporting.configure_report_dir(config)

    assert report_dir.is_dir()
    assert config.option.log_file == str(report_dir / "pytest.log")


def test_configure_report_dir_reuses_an_existing_directory(tmp_path: Path) -> None:
    """Leave an already-present report directory alone."""

    report_dir = tmp_path / "reports"
    report_dir.mkdir()
    (report_dir / "previous.txt").write_text("kept", encoding="utf-8")
    config = _config(report_dir=str(report_dir))

    reporting.configure_report_dir(config)

    assert (report_dir / "previous.txt").read_text(encoding="utf-8") == "kept"


@pytest.mark.parametrize(
    ("option", "ini"),
    [("explicit.log", None), (None, "explicit.log")],
)
def test_configure_report_dir_preserves_explicit_log_file(
    tmp_path: Path,
    option: str | None,
    ini: object,
) -> None:
    """Leave an explicitly configured log file untouched, from either source."""

    config = _config(report_dir=str(tmp_path / "reports"), log_file=option, log_file_ini=ini)

    reporting.configure_report_dir(config)

    assert config.option.log_file == option
    assert config.option.xmlpath == str(tmp_path / "reports" / "pytest.xml")


@pytest.mark.parametrize(
    ("option", "ini"),
    [("WARNING", None), (None, "WARNING")],
)
def test_configure_report_dir_preserves_explicit_log_file_level(
    tmp_path: Path,
    option: str | None,
    ini: object,
) -> None:
    """Leave an explicitly configured log-file level untouched, from either source."""

    config = _config(
        report_dir=str(tmp_path / "reports"),
        log_file_level=option,
        log_file_level_ini=ini,
    )

    reporting.configure_report_dir(config)

    assert config.option.log_file_level == option


def test_configure_report_dir_preserves_explicit_junit_xml(tmp_path: Path) -> None:
    """Leave an explicitly configured ``--junit-xml`` destination untouched."""

    config = _config(report_dir=str(tmp_path / "reports"), xmlpath="explicit.xml")

    reporting.configure_report_dir(config)

    assert config.option.xmlpath == "explicit.xml"


def test_configure_report_dir_rejects_an_uncreatable_directory(tmp_path: Path) -> None:
    """Render a directory that cannot be created as an actionable usage error."""

    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory", encoding="utf-8")
    config = _config(report_dir=str(blocker / "reports"))

    with pytest.raises(pytest.UsageError, match="Could not create report directory"):
        reporting.configure_report_dir(config)


def test_configure_report_dir_composes_with_worker_scoping(tmp_path: Path) -> None:
    """Scope a derived log file per worker, proving the plugin's call order."""

    report_dir = tmp_path / "reports"
    config = _config(report_dir=str(report_dir), workerinput={"workerid": "gw2"})

    reporting.configure_report_dir(config)
    reporting.configure_reporting(config)

    assert config.option.log_file == str(report_dir / "pytest.gw2.log")
    assert config.option.xmlpath == str(report_dir / "pytest.xml")


def test_report_dir_writes_both_artifacts(pytester: pytest.Pytester) -> None:
    """Write a parseable ``pytest.xml`` and a populated ``pytest.log`` in a real run."""

    pytester.makepyfile(
        test_reported="""
        import logging

        def test_one():
            logging.getLogger("consumer").debug("recorded at debug level")
        """
    )

    result = pytester.runpytest_subprocess("--airflow-report-dir=reports", "-q")

    result.assert_outcomes(passed=1)
    log_file = pytester.path / "reports" / "pytest.log"
    xml_file = pytester.path / "reports" / "pytest.xml"
    assert "recorded at debug level" in log_file.read_text(encoding="utf-8")
    testsuite = ElementTree.parse(xml_file).getroot().find("testsuite")
    assert testsuite is not None
    assert testsuite.get("tests") == "1"


def test_report_dir_scopes_log_files_across_xdist_workers(pytester: pytest.Pytester) -> None:
    """Write one suffixed log file per worker below a derived report directory."""

    pytester.makepyfile(
        test_reported_parallel="""
        import logging

        def test_one():
            logging.getLogger("consumer").warning("from a worker")

        def test_two():
            logging.getLogger("consumer").warning("from a worker")
        """
    )

    result = pytester.runpytest_subprocess("--airflow-report-dir=reports", "-n", "2", "-q")

    result.assert_outcomes(passed=2)
    assert (pytester.path / "reports" / "pytest.gw0.log").is_file()
    assert (pytester.path / "reports" / "pytest.gw1.log").is_file()
    assert (pytester.path / "reports" / "pytest.xml").is_file()


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
