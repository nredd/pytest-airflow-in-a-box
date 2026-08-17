"""Resolve the parse-time secrets policy from plugin options.

Keeps pytest configuration out of ``_compat``, which stays framework-free: this module
turns ``--airflow-parse-secrets`` / ``airflow_parse_secrets`` into the
``ParseTimeComms`` (or None) that every Dag parse site hands to ``build_dag_bag``.

References:
    https://docs.pytest.org/en/stable/reference/reference.html#config
"""

from __future__ import annotations

import logging
from enum import Enum

import pytest

from pytest_airflow_in_a_box._compat.parse_time import ParseTimeComms
from pytest_airflow_in_a_box.bootstrap import get_bootstrap_state

LOGGER = logging.getLogger(__name__)

OPTION_NAME = "airflow_parse_secrets"


class ParseSecretsPolicy(str, Enum):
    """Closed set of parse-time Variable and Connection resolution policies."""

    METASTORE = "metastore"
    OFF = "off"


def parse_secrets_policy(config: pytest.Config) -> ParseSecretsPolicy:
    """Resolve the parse-time secrets policy, command line ahead of ini.

    Parameters:
        config: pytest.Config containing plugin options and ini values.

    Returns:
        ParseSecretsPolicy selecting how top-level lookups resolve during a Dag parse.

    Raises:
        pytest.UsageError: The option or ini value is not a known policy name.
    """

    command_line: object = config.getoption(OPTION_NAME)
    if command_line is not None:
        value = command_line
        source = "Option `--airflow-parse-secrets`"
    else:
        value = config.getini(OPTION_NAME)
        source = f"Ini option `{OPTION_NAME}`"
    if not isinstance(value, str):
        raise pytest.UsageError(f"{source} must be a string")
    try:
        return ParseSecretsPolicy(value)
    except ValueError as error:
        supported = ", ".join(f"`{policy.value}`" for policy in ParseSecretsPolicy)
        raise pytest.UsageError(f"{source} must be one of {supported}: '{value}'") from error


def parse_time_comms(config: pytest.Config) -> ParseTimeComms | None:
    """Build the supervisor shim a Dag parse should run under, if any.

    A fresh shim per parse keeps the metadata session bound to exactly one parse and
    closed with it; nothing is cached, because the shim itself does no work until a Dag
    issues a lookup.

    Parameters:
        config: pytest.Config containing plugin options and bootstrap state.

    Returns:
        ParseTimeComms | None answering top-level lookups, or None when the consumer
        selected `off`.

    Raises:
        pytest.UsageError: The option or ini value is not a known policy name.
    """

    if parse_secrets_policy(config) is ParseSecretsPolicy.OFF:
        return None
    return ParseTimeComms(run_root=get_bootstrap_state(config).root)


__all__ = ("OPTION_NAME", "ParseSecretsPolicy", "parse_secrets_policy", "parse_time_comms")
