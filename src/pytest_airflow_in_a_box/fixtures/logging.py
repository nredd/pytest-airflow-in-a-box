"""Capture structlog events emitted during one test.

Airflow 3 emits most of its records through structlog, where pytest's builtin
``caplog`` cannot see them. The fixture deliberately keeps the builtin name
untouched: pytest's ``caplog`` constructor is private, so a same-named override
could not delegate to it and would silently change stdlib capture semantics.

Known limit: code that reconfigures structlog mid-test replaces the capture
processor, and bound loggers created before the fixture keep their frozen
processor chain.

References:
    https://www.structlog.org/en/stable/configuration.html
    https://www.structlog.org/en/stable/processors.html
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from pytest_airflow_in_a_box.logging import StructlogCapture


def _capture_structlog() -> Iterator[StructlogCapture]:
    """Install a capture processor and restore the exact prior configuration.

    Yields:
        StructlogCapture recording every event emitted while installed.
    """

    # Deferred to preserve bootstrap safety; structlog arrives with Airflow.
    import structlog

    capture = StructlogCapture()
    saved = structlog.get_config()
    processors = [*saved["processors"]]
    if processors:
        # The final processor renders the event; capture must run before it.
        processors.insert(len(processors) - 1, capture)
    else:
        processors = [capture]
    structlog.configure(processors=processors, cache_logger_on_first_use=False)
    try:
        yield capture
    finally:
        structlog.configure(**saved)


@pytest.fixture
def cap_structlog() -> Iterator[StructlogCapture]:
    """Yield a capture recording structlog events emitted during the test.

    Yields:
        StructlogCapture recording every event emitted while installed.
    """

    yield from _capture_structlog()


__all__ = ("cap_structlog",)
