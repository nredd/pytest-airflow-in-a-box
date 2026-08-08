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


_StructlogConfigure = Callable[..., None]
_StructlogCaptureCallback = Callable[[StructlogCapture], None]


class _StructlogModule(Protocol):
    """Typed mutable surface for the structlog configuration module."""

    configure: _StructlogConfigure
    configure_once: _StructlogConfigure


_ADD_STRUCTLOG_CAPTURE: _StructlogCaptureCallback | None = None
_REMOVE_STRUCTLOG_CAPTURE: _StructlogCaptureCallback | None = None


def _with_captures(
    processors: Iterable[Any],
    captures: Iterable[StructlogCapture],
) -> list[Any]:
    """Insert active capture processors immediately before the final processor.

    Parameters:
        processors: Iterable[Any] containing a structlog processor chain.
        captures: Iterable[StructlogCapture] containing active captures.

    Returns:
        list[Any] containing the chain with each active capture inserted once.
    """

    active = tuple(captures)
    chain = [item for item in processors if not any(item is capture for capture in active)]
    if not chain:
        return [*active]
    return [*chain[:-1], *active, chain[-1]]


def _install_structlog_capture_interceptor(capture: StructlogCapture) -> None:
    """Install a capture processor and intercept mid-test reconfiguration.

    Parameters:
        capture: StructlogCapture recording every event emitted while installed.
    """

    global _ADD_STRUCTLOG_CAPTURE, _REMOVE_STRUCTLOG_CAPTURE

    with _LOCK:
        if _ADD_STRUCTLOG_CAPTURE is not None:
            _ADD_STRUCTLOG_CAPTURE(capture)
            return

        # Deferred to preserve bootstrap safety; structlog arrives with Airflow.
        import structlog

        module = cast(_StructlogModule, structlog)
        saved = structlog.get_config()
        real_configure: _StructlogConfigure = module.configure
        real_configure_once: _StructlogConfigure = module.configure_once
        captures: list[StructlogCapture] = [capture]

        def _intercept_configure(*args: Any, **kwargs: Any) -> None:
            with _LOCK:
                real_configure(*args, **kwargs)
                real_configure(
                    processors=_with_captures(structlog.get_config()["processors"], captures)
                )

        def _intercept_configure_once(*args: Any, **kwargs: Any) -> None:
            with _LOCK:
                real_configure_once(*args, **kwargs)
                real_configure(
                    processors=_with_captures(structlog.get_config()["processors"], captures)
                )

        def _add_capture(new_capture: StructlogCapture) -> None:
            if any(item is new_capture for item in captures):
                return
            captures.append(new_capture)
            real_configure(
                processors=_with_captures(structlog.get_config()["processors"], captures)
            )

        def _remove_capture(old_capture: StructlogCapture) -> None:
            global _ADD_STRUCTLOG_CAPTURE, _REMOVE_STRUCTLOG_CAPTURE

            remaining = [item for item in captures if item is not old_capture]
            if len(remaining) == len(captures):
                return
            captures[:] = remaining
            current_processors = [
                item for item in structlog.get_config()["processors"] if item is not old_capture
            ]
            if captures:
                real_configure(processors=_with_captures(current_processors, captures))
                return

            module.configure = real_configure
            module.configure_once = real_configure_once
            _ADD_STRUCTLOG_CAPTURE = None
            _REMOVE_STRUCTLOG_CAPTURE = None
            real_configure(
                processors=current_processors,
                cache_logger_on_first_use=saved["cache_logger_on_first_use"],
            )

        module.configure = _intercept_configure
        module.configure_once = _intercept_configure_once
        _ADD_STRUCTLOG_CAPTURE = _add_capture
        _REMOVE_STRUCTLOG_CAPTURE = _remove_capture
        real_configure(
            processors=_with_captures(saved["processors"], captures),
            cache_logger_on_first_use=False,
        )


def _uninstall_structlog_capture_interceptor(capture: StructlogCapture) -> None:
    """Remove one capture and restore structlog after the final capture exits.

    Parameters:
        capture: StructlogCapture to remove from the active processor chain.
    """

    with _LOCK:
        if _REMOVE_STRUCTLOG_CAPTURE is None:
            return
        _REMOVE_STRUCTLOG_CAPTURE(capture)


__all__ = ("StructlogCapture", "TestContextFilter", "ensure_handlers")
