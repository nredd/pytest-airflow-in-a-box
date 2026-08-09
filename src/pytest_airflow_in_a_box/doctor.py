"""Render a one-shot diagnostics report for `--airflow-doctor`.

References:
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.hookspec.pytest_cmdline_main
"""

from __future__ import annotations

import platform
import urllib.parse
from dataclasses import fields
from enum import Enum

import pytest

from pytest_airflow_in_a_box import __version__
from pytest_airflow_in_a_box._compat import (
    AirflowCapabilities,
    AirflowCompatibilityError,
    resolve_capabilities,
)
from pytest_airflow_in_a_box.bootstrap import BootstrapState, get_bootstrap_state

REPORT_TITLE = "# pytest-airflow-in-a-box diagnostics"


def _storage_section(state: BootstrapState) -> list[str]:
    """Render the storage ladder decision and its reason.

    Parameters:
        state: BootstrapState containing the resolved run storage.

    Returns:
        list[str] containing Markdown bullet lines.
    """

    return [
        f"- Reason: `{state.storage_reason}`",
        f"- Network filesystem: `{state.network_storage}`",
    ]


def _database_section(state: BootstrapState) -> list[str]:
    """Render the resolved `AIRFLOW_HOME`, database backend, and URL scheme.

    The full SQLAlchemy URL is deliberately omitted: a provisioned Postgres URL carries
    live credentials, and the scheme plus backend tier is all this report needs.

    Parameters:
        state: BootstrapState containing the resolved run configuration.

    Returns:
        list[str] containing Markdown bullet lines.
    """

    scheme = urllib.parse.urlsplit(state.sql_alchemy_conn).scheme
    return [
        f"- `AIRFLOW_HOME`: `{state.root}`",
        f"- Backend tier: `{state.db_backend}`",
        f"- Database URL scheme: `{scheme}`",
    ]


def _format_capability_value(value: object) -> str:
    """Format one capability field value for display.

    Parameters:
        value: object containing one `AirflowCapabilities` field value.

    Returns:
        str containing an enum's plain value, a dot-joined release tuple, or the
        value's default string form.
    """

    if isinstance(value, Enum):
        return str(value.value)
    if isinstance(value, tuple):
        return ".".join(str(part) for part in value)
    return str(value)


def _capability_lines(capabilities: AirflowCapabilities) -> list[str]:
    """Render every `AirflowCapabilities` field as a Markdown bullet.

    Parameters:
        capabilities: AirflowCapabilities containing the resolved contract.

    Returns:
        list[str] containing Markdown bullet lines, one per field.
    """

    lines = []
    for field in fields(capabilities):
        value = _format_capability_value(getattr(capabilities, field.name))
        lines.append(f"- Capability `{field.name}`: `{value}`")
    return lines


def _version_section() -> list[str]:
    """Render plugin, pytest, Python, and Apache Airflow versions plus capabilities.

    Returns:
        list[str] containing Markdown bullet lines.
    """

    lines = [
        f"- `pytest-airflow-in-a-box`: `{__version__}`",
        f"- `pytest`: `{pytest.__version__}`",
        f"- Python: `{platform.python_version()}`",
    ]
    try:
        capabilities = resolve_capabilities()
    except AirflowCompatibilityError as error:
        lines.append(f"- Apache Airflow: INCOMPATIBLE: {error}")
        return lines
    lines.append(f"- Apache Airflow: `{_format_capability_value(capabilities.release)}`")
    lines.extend(_capability_lines(capabilities))
    return lines


def _api_server_section() -> list[str]:
    """Render the API server section, which never has live state for this invocation.

    Returns:
        list[str] containing Markdown bullet lines.
    """

    return [
        "- Not started: this diagnostic run did not request the `api_server_url` fixture. "
        "The API server is a lazy, per-process, session-scoped subprocess with no state "
        "before a test requests it.",
    ]


def render_doctor_report(config: pytest.Config) -> str:
    """Render a one-shot, copy-pasteable diagnostics report.

    Parameters:
        config: pytest.Config for the active invocation.

    Returns:
        str containing the complete Markdown report.

    Raises:
        pytest.UsageError: Airflow bootstrap state is unavailable.
    """

    state = get_bootstrap_state(config)
    sections = (
        ("Storage", _storage_section(state)),
        ("AIRFLOW_HOME and database", _database_section(state)),
        ("Versions and capabilities", _version_section()),
        ("API server", _api_server_section()),
    )
    lines = [REPORT_TITLE, ""]
    for title, bullets in sections:
        lines.append(f"## {title}")
        lines.extend(bullets)
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


__all__ = ("render_doctor_report",)
