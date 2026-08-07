"""Import-light pytest plugin entry point.

This module must remain safe to import before Apache Airflow. Bootstrap and fixture
hooks will be added here as their implementations land.

References:
    https://docs.pytest.org/en/stable/reference/reference.html#hooks
    https://docs.pytest.org/en/stable/how-to/writing_plugins.html
"""

from __future__ import annotations

import pytest


def pytest_load_initial_conftests(
    early_config: pytest.Config,
    parser: pytest.Parser,
    args: list[str],
) -> None:
    """Reserve pytest's earliest initialization hook for environment bootstrap.

    Parameters:
        early_config: pytest.Config for initial command-line parsing.
        parser: pytest.Parser used during initial command-line parsing.
        args: list[str] containing the command-line arguments.
    """
    del early_config, parser, args


def pytest_configure(config: pytest.Config) -> None:
    """Reserve pytest's configuration hook for plugin state registration.

    Parameters:
        config: pytest.Config for the active test session.
    """
    del config
