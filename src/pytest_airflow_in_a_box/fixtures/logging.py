"""Capture structlog events emitted during one test.

Airflow 3 emits most of its records through structlog, where pytest's builtin
``caplog`` cannot see them. The fixture deliberately keeps the builtin name
untouched: pytest's ``caplog`` constructor is private, so a same-named override
could not delegate to it and would silently change stdlib capture semantics.

Mid-test ``structlog.configure``/``configure_once`` calls are intercepted so the
capture processor survives reconfiguration; teardown restores the exact
callables and re-applies whatever chain the test left behind, minus capture.

Known limit: bound loggers created before the fixture keep their frozen
processor chain (mitigated for new loggers by disabling
``cache_logger_on_first_use`` while installed).

References:
    https://www.structlog.org/en/stable/configuration.html
    https://www.structlog.org/en/stable/processors.html
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from pytest_airflow_in_a_box.logging import (
    StructlogCapture,
    _install_structlog_capture_interceptor,
    _uninstall_structlog_capture_interceptor,
)


def _capture_structlog() -> Iterator[StructlogCapture]:
    """Install a capture processor and restore the reconfigured chain.

    Yields:
        StructlogCapture recording every event emitted while installed.
    """

    capture = StructlogCapture()
    _install_structlog_capture_interceptor(capture)
    try:
        yield capture
    finally:
        _uninstall_structlog_capture_interceptor(capture)


@pytest.fixture
def cap_structlog() -> Iterator[StructlogCapture]:
    """Yield a capture recording structlog events emitted during the test.

    Yields:
        StructlogCapture recording every event emitted while installed.
    """

    yield from _capture_structlog()


__all__ = ("cap_structlog",)
