"""Test the parse-time secrets policy resolved from plugin options.

References:
    https://docs.pytest.org/en/stable/reference/reference.html#config
"""

from __future__ import annotations

from typing import Any

import pytest

from pytest_airflow_in_a_box import parse_secrets
from pytest_airflow_in_a_box._compat.parse_time import ParseTimeComms
from pytest_airflow_in_a_box.bootstrap import get_bootstrap_state
from pytest_airflow_in_a_box.parse_secrets import (
    ParseSecretsPolicy,
    parse_secrets_policy,
    parse_time_comms,
)


class _FakeConfig:
    """Stand in for `pytest.Config` with just the two option lookups.

    Parameters:
        option: object returned by `getoption`, or None to fall through to the ini.
        ini: object returned by `getini`.
    """

    def __init__(self, *, option: object = None, ini: object = "metastore") -> None:
        self._option = option
        self._ini = ini

    def getoption(self, name: str) -> object:
        """Report the command-line value.

        Parameters:
            name: str naming the option.

        Returns:
            object containing the configured command-line value.
        """

        assert name == parse_secrets.OPTION_NAME
        return self._option

    def getini(self, name: str) -> object:
        """Report the ini value.

        Parameters:
            name: str naming the ini option.

        Returns:
            object containing the configured ini value.
        """

        assert name == parse_secrets.OPTION_NAME
        return self._ini


def _config(**kwargs: Any) -> pytest.Config:
    """Build a fake config typed as the real one.

    Parameters:
        kwargs: Any forwarded to `_FakeConfig`.

    Returns:
        pytest.Config standing in for a real configuration.
    """

    fake: Any = _FakeConfig(**kwargs)
    return fake


def test_ini_default_selects_metastore_resolution() -> None:
    """Resolve the shipped default, which is on."""

    assert parse_secrets_policy(_config()) is ParseSecretsPolicy.METASTORE


def test_command_line_outranks_the_ini_value() -> None:
    """Prefer an explicit `--airflow-parse-secrets` over the ini value."""

    config = _config(option="off", ini="metastore")

    assert parse_secrets_policy(config) is ParseSecretsPolicy.OFF


def test_ini_value_applies_without_a_command_line_value() -> None:
    """Fall through to the ini value when the command line is silent."""

    assert parse_secrets_policy(_config(ini="off")) is ParseSecretsPolicy.OFF


def test_rejects_an_unknown_policy_name() -> None:
    """Name every supported policy when the configured one is unknown."""

    with pytest.raises(pytest.UsageError, match="`metastore`, `off`: 'sometimes'"):
        parse_secrets_policy(_config(ini="sometimes"))


def test_rejects_a_non_string_ini_value() -> None:
    """Reject an ini value pytest handed back with the wrong type."""

    with pytest.raises(pytest.UsageError, match="Ini option `airflow_parse_secrets` must be"):
        parse_secrets_policy(_config(ini=[]))


def test_rejects_a_non_string_command_line_value() -> None:
    """Reject a command-line value with the wrong type."""

    with pytest.raises(pytest.UsageError, match="Option `--airflow-parse-secrets` must be"):
        parse_secrets_policy(_config(option=1))


def test_off_builds_no_supervisor_shim() -> None:
    """Leave Airflow's own resolution alone under the `off` policy."""

    assert parse_time_comms(_config(option="off")) is None


def test_metastore_builds_a_shim_bound_to_the_run_root(pytestconfig: pytest.Config) -> None:
    """Bind the shim to the session's run root under the shipped default policy.

    Parameters:
        pytestconfig: pytest.Config carrying the session's bootstrap state.
    """

    comms = parse_time_comms(pytestconfig)

    assert isinstance(comms, ParseTimeComms)
    assert comms.run_root == get_bootstrap_state(pytestconfig).root


def test_each_call_builds_its_own_shim(pytestconfig: pytest.Config) -> None:
    """Never share one metadata session across two independent parses.

    Parameters:
        pytestconfig: pytest.Config carrying the session's bootstrap state.
    """

    assert parse_time_comms(pytestconfig) is not parse_time_comms(pytestconfig)
