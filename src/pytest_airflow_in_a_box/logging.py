"""Preserve and capture test-visible logging.

Covers stdlib logging (handlers preserved across ``dictConfig`` calls) and
structlog (a pass-through capture processor), because Airflow 3 emits most of
its records through structlog where plain ``caplog`` cannot see them.

References:
    https://docs.python.org/3/library/logging.config.html#logging.config.dictConfig
    https://docs.pytest.org/en/stable/how-to/logging.html
    https://docs.pytest.org/en/stable/reference/reference.html#envvar-PYTEST_CURRENT_TEST
    https://www.structlog.org/en/stable/processors.html
"""

from __future__ import annotations

import logging as stdlib_logging
import logging.config as logging_config
import os
import re
import threading
from collections.abc import Callable, Iterable, MutableMapping
from typing import Any, Protocol, cast

_DictConfig = Callable[[dict[str, Any]], None]


class _LoggingConfigModule(Protocol):
    """Typed mutable surface for the stdlib logging configuration module."""

    dictConfig: _DictConfig


_LOGGING_CONFIG = cast(_LoggingConfigModule, logging_config)

_LOCK = threading.RLock()
_REAL_DICT_CONFIG: _DictConfig = _LOGGING_CONFIG.dictConfig
_INSTALLED = False
_CURRENT_TEST_STAGE_PATTERN = re.compile(r" \((?:setup|call|teardown)\)$")


class TestContextFilter(stdlib_logging.Filter):
    """Add the active pytest worker and test node ID to a log record."""

    def filter(self, record: stdlib_logging.LogRecord) -> bool:
        """Add pytest context to one log record.

        Parameters:
            record: logging.LogRecord receiving pytest context fields.

        Returns:
            bool indicating that the record should be emitted.
        """

        current_test = os.environ.get("PYTEST_CURRENT_TEST", "unknown")
        test_name = _CURRENT_TEST_STAGE_PATTERN.sub("", current_test)
        record.__dict__["worker_id"] = os.environ.get("PYTEST_XDIST_WORKER", "master")
        record.__dict__["test_name"] = test_name
        return True


class StructlogCapture:
    """Record every structlog event while passing it through unchanged."""

    def __init__(self) -> None:
        """Create an empty capture."""

        self.entries: list[dict[str, Any]] = []

    def __call__(
        self,
        logger: object,
        method_name: str,
        event_dict: MutableMapping[str, Any],
    ) -> MutableMapping[str, Any]:
        """Record one event and hand it to the next processor unchanged.

        Parameters:
            logger: object containing the wrapped structlog logger.
            method_name: str naming the invoked logger method.
            event_dict: MutableMapping[str, Any] containing the structlog event.

        Returns:
            MutableMapping[str, Any] containing the unmodified event.
        """

        del logger
        self.entries.append({**event_dict, "log_level": method_name})
        return event_dict

    def __contains__(self, target: object) -> bool:
        """Probe captured events by name or by key-value subset.

        Parameters:
            target: object containing an event name or an event subset mapping.

        Returns:
            bool indicating whether any captured event matches.

        Raises:
            TypeError: The probe is neither a string nor a mapping.
        """

        if isinstance(target, str):
            return any(entry.get("event") == target for entry in self.entries)
        if isinstance(target, dict):
            return any(target.items() <= entry.items() for entry in self.entries)
        raise TypeError(f"Unsupported membership probe type: '{type(target).__name__}'")

    @property
    def text(self) -> str:
        """Join captured event names for substring assertions.

        Returns:
            str containing newline-joined event names.
        """

        return "\n".join(str(entry.get("event", "")) for entry in self.entries)


def _is_pytest_handler(handler: stdlib_logging.Handler) -> bool:
    """Identify a handler implemented by pytest's logging plugin.

    Parameters:
        handler: logging.Handler to inspect.

    Returns:
        bool indicating whether pytest owns the handler implementation.
    """

    return any(cls.__module__ == "_pytest.logging" for cls in type(handler).__mro__)


def _ensure_context_filter(handler: stdlib_logging.Handler) -> None:
    """Attach exactly one test-context filter to a pytest handler.

    Parameters:
        handler: logging.Handler receiving context fields.
    """

    if any(isinstance(item, TestContextFilter) for item in handler.filters):
        return
    handler.addFilter(TestContextFilter())


def _reopen_file_handler(handler: stdlib_logging.Handler) -> None:
    """Reopen a pytest file handler closed by stdlib dictConfig.

    Reopening a ``w`` mode handler in append mode preserves records written before
    reconfiguration without changing its configured mode for final shutdown.

    Parameters:
        handler: logging.Handler that may own a closed file stream.
    """

    if not isinstance(handler, stdlib_logging.FileHandler):
        return
    handler.acquire()
    try:
        if handler.stream is not None:
            return
        original_mode = handler.mode
        try:
            if original_mode == "w":
                handler.mode = "a"
            handler.stream = handler._open()
            handler.__dict__["_closed"] = False
        finally:
            handler.mode = original_mode
    finally:
        handler.release()


def ensure_handlers(handlers: Iterable[stdlib_logging.Handler]) -> None:
    """Idempotently attach active pytest handlers and their context filter.

    Parameters:
        handlers: Iterable[logging.Handler] containing candidate root handlers.
    """

    with _LOCK:
        root_logger = stdlib_logging.getLogger()
        for handler in handlers:
            if not _is_pytest_handler(handler):
                continue
            _ensure_context_filter(handler)
            _reopen_file_handler(handler)
            if handler not in root_logger.handlers:
                root_logger.addHandler(handler)


def _intercept_dict_config(cfg: dict[str, Any]) -> None:
    """Apply logging configuration without losing active pytest handlers.

    Parameters:
        cfg: dict[str, Any] containing a stdlib logging dictionary configuration.
    """

    with _LOCK:
        root_logger = stdlib_logging.getLogger()
        pytest_handlers = tuple(
            handler for handler in root_logger.handlers if _is_pytest_handler(handler)
        )
        try:
            _REAL_DICT_CONFIG(cfg)
        finally:
            ensure_handlers(pytest_handlers)


_DICT_CONFIG_INTERCEPTOR: _DictConfig = _intercept_dict_config


def _install_dict_config_interceptor() -> None:
    """Install the process-global dictConfig interceptor exactly once."""

    global _INSTALLED, _REAL_DICT_CONFIG

    with _LOCK:
        if _INSTALLED:
            return
        _REAL_DICT_CONFIG = _LOGGING_CONFIG.dictConfig
        _LOGGING_CONFIG.dictConfig = _DICT_CONFIG_INTERCEPTOR
        _INSTALLED = True


def _uninstall_dict_config_interceptor() -> None:
    """Restore the dictConfig callable replaced by the interceptor."""

    global _INSTALLED

    with _LOCK:
        if not _INSTALLED:
            return
        _LOGGING_CONFIG.dictConfig = _REAL_DICT_CONFIG
        _INSTALLED = False


__all__ = ("StructlogCapture", "TestContextFilter", "ensure_handlers")
