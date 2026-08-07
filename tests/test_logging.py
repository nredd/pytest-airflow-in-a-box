"""Test preservation of pytest logging across stdlib reconfiguration."""

from __future__ import annotations

import logging
import logging.config
from pathlib import Path

import pytest
from _pytest.logging import LogCaptureHandler, _FileHandler

import pytest_airflow_in_a_box.logging as logging_support


def _record() -> logging.LogRecord:
    """Create one unadorned logging record.

    Returns:
        logging.LogRecord suitable for direct filter and handler tests.
    """

    return logging.LogRecord("test", logging.INFO, __file__, 1, "message", (), None)


@pytest.mark.parametrize(
    ("current_test", "expected"),
    [
        ("tests/test_logging.py::test_case (call)", "tests/test_logging.py::test_case"),
        ("tests/test_logging.py::test_case", "tests/test_logging.py::test_case"),
        ("tests/test_logging.py::test_case (custom)", "tests/test_logging.py::test_case (custom)"),
    ],
)
def test_context_filter_adds_worker_and_test_name(
    monkeypatch: pytest.MonkeyPatch,
    current_test: str,
    expected: str,
) -> None:
    """Add worker identity and remove only a complete pytest stage suffix."""

    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw3")
    monkeypatch.setenv("PYTEST_CURRENT_TEST", current_test)
    record = _record()

    assert logging_support.TestContextFilter().filter(record)
    assert record.__dict__["worker_id"] == "gw3"
    assert record.__dict__["test_name"] == expected


def test_context_filter_uses_stable_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    """Use useful context when pytest environment variables are absent."""

    monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    record = _record()

    assert logging_support.TestContextFilter().filter(record)
    assert record.__dict__["worker_id"] == "master"
    assert record.__dict__["test_name"] == "unknown"


def test_structlog_capture_passes_events_through_unchanged() -> None:
    """Return the exact event object while recording an enriched copy."""

    capture = logging_support.StructlogCapture()
    event = {"event": "probe", "key": 1}

    returned = capture(object(), "warning", event)

    assert returned is event
    assert event == {"event": "probe", "key": 1}
    assert capture.entries == [{"event": "probe", "key": 1, "log_level": "warning"}]


def test_structlog_capture_membership_by_event_name() -> None:
    """Match string probes against captured event names only."""

    capture = logging_support.StructlogCapture()
    capture(object(), "info", {"event": "present", "detail": "ignored"})

    assert "present" in capture
    assert "absent" not in capture


def test_structlog_capture_membership_by_subset() -> None:
    """Match mapping probes as key-value subsets of any captured event."""

    capture = logging_support.StructlogCapture()
    capture(object(), "info", {"event": "probe", "key": 1, "extra": [1, 2]})

    assert {"key": 1} in capture
    assert {"event": "probe", "extra": [1, 2]} in capture
    assert {"key": 2} not in capture
    assert {"missing": 1} not in capture


def test_structlog_capture_rejects_other_probe_types() -> None:
    """Reject probes that are neither strings nor mappings."""

    capture = logging_support.StructlogCapture()

    with pytest.raises(TypeError, match="Unsupported membership probe type: 'int'"):
        _ = 7 in capture


def test_structlog_capture_text_joins_event_names() -> None:
    """Join event names, tolerating events without a name."""

    capture = logging_support.StructlogCapture()
    capture(object(), "info", {"event": "first"})
    capture(object(), "info", {"unnamed": True})
    capture(object(), "info", {"event": "last"})

    assert capture.text == "first\n\nlast"


def test_ensure_handlers_is_selective_and_idempotent() -> None:
    """Attach one filter to pytest handlers without restoring application handlers."""

    root_logger = logging.getLogger()
    pytest_handler = LogCaptureHandler()
    application_handler = logging.StreamHandler()
    try:
        logging_support.ensure_handlers((pytest_handler, application_handler))
        logging_support.ensure_handlers((pytest_handler, application_handler))

        context_filters = [
            item
            for item in pytest_handler.filters
            if isinstance(item, logging_support.TestContextFilter)
        ]
        assert root_logger.handlers.count(pytest_handler) == 1
        assert application_handler not in root_logger.handlers
        assert len(context_filters) == 1
    finally:
        root_logger.removeHandler(pytest_handler)
        pytest_handler.close()
        application_handler.close()


def test_ensure_handlers_reopens_append_file_handler(tmp_path: Path) -> None:
    """Reopen a preserved pytest file handler without changing append mode."""

    log_path = tmp_path / "pytest.log"
    root_logger = logging.getLogger()
    pytest_handler = _FileHandler(log_path, mode="a", encoding="UTF-8")
    pytest_handler.close()
    try:
        logging_support.ensure_handlers((pytest_handler,))
        pytest_handler.handle(_record())

        assert pytest_handler.mode == "a"
        assert log_path.read_text(encoding="utf-8") == "message\n"
    finally:
        root_logger.removeHandler(pytest_handler)
        pytest_handler.close()


def test_dict_config_preserves_only_active_pytest_handlers() -> None:
    """Reattach active pytest handlers but honor replacement of application handlers."""

    root_logger = logging.getLogger()
    pytest_handler = LogCaptureHandler()
    application_handler = logging.StreamHandler()
    root_logger.addHandler(pytest_handler)
    root_logger.addHandler(application_handler)
    try:
        logging.config.dictConfig(
            {
                "version": 1,
                "disable_existing_loggers": False,
                "root": {"handlers": [], "level": "DEBUG"},
            }
        )
        logging.getLogger("consumer").warning("captured after reconfiguration")

        assert pytest_handler in root_logger.handlers
        assert application_handler not in root_logger.handlers
        assert [record.getMessage() for record in pytest_handler.records] == [
            "captured after reconfiguration"
        ]
    finally:
        root_logger.removeHandler(pytest_handler)
        root_logger.removeHandler(application_handler)
        pytest_handler.close()
        application_handler.close()


def test_dict_config_failure_still_restores_pytest_handlers() -> None:
    """Restore pytest capture after dictConfig partially mutates state and fails."""

    root_logger = logging.getLogger()
    pytest_handler = LogCaptureHandler()
    root_logger.addHandler(pytest_handler)
    try:
        with pytest.raises(ValueError, match="Unable to configure root logger"):
            logging.config.dictConfig(
                {
                    "version": 1,
                    "root": {"handlers": ["missing"]},
                }
            )

        assert pytest_handler in root_logger.handlers
    finally:
        root_logger.removeHandler(pytest_handler)
        pytest_handler.close()


def test_interceptor_installation_is_idempotent_and_restorable() -> None:
    """Install once and restore the exact process-global callable once."""

    installed = logging.config.dictConfig
    real = logging_support._REAL_DICT_CONFIG
    try:
        logging_support._install_dict_config_interceptor()
        assert logging.config.dictConfig is installed

        logging_support._uninstall_dict_config_interceptor()
        assert logging.config.dictConfig is real
        logging_support._uninstall_dict_config_interceptor()
        assert logging.config.dictConfig is real

        logging_support._install_dict_config_interceptor()
        assert logging.config.dictConfig is installed
    finally:
        logging_support._install_dict_config_interceptor()


def test_consumer_dict_config_keeps_caplog_capture(
    pytester: pytest.Pytester,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Preserve per-test caplog and pytest's log file through root replacement."""

    # The serial child run must not inherit an outer xdist worker identity.
    monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)
    log_path = pytester.path / "consumer.log"
    pytester.makeini(f"[pytest]\nlog_file = {log_path}\nlog_file_level = WARNING\n")
    pytester.makeconftest(
        """
        import logging.config
        import pytest

        @pytest.fixture
        def replace_root_logging():
            logging.config.dictConfig(
                {
                    "version": 1,
                    "disable_existing_loggers": False,
                    "root": {"handlers": [], "level": "DEBUG"},
                }
            )
        """
    )
    pytester.makepyfile(
        test_logging_guard="""
        import logging

        def test_capture_survives(replace_root_logging, caplog):
            logging.getLogger("consumer").warning("survived")

            record = next(record for record in caplog.records if record.message == "survived")
            assert record.worker_id == "master"
            assert record.test_name == "test_logging_guard.py::test_capture_survives"
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)
    assert "survived" in log_path.read_text(encoding="utf-8")
