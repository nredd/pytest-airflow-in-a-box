"""Promote Airflow's own 2->3 deprecation categories to test-phase errors.

``--airflow-migration-strict`` turns an Airflow 2.11 run into a forecast of 3.x
breakage with no 3.x environment needed: 2.11 is deliberately saturated with
``RemovedInAirflow3Warning`` and ``AirflowProviderDeprecationWarning`` emissions, so
error-promoting just those two categories during the runtest phase surfaces exactly
the code paths that will break on an eventual 3.x migration. Off the Airflow 2.x
family there is nothing to forecast, so the option is a no-op there, reported once at
configure time.

Error promotion applies to the runtest phase only, never to collection or bootstrap:
Airflow 2.11 emits the very same deprecations from its own modules during import and
Dag parsing, and an unqualified ``error::`` filter over those categories would abort
the session before a single test ran. ``defaults.apply_filterwarnings`` is reused for
the actual list mutation; this module only decides whether and which lines to add,
and does so from ``pytest_collection_finish`` rather than ``pytest_configure`` so the
filters are absent for pytest's collection-time warning context and present for every
runtest-phase warning context, both of which pytest reconstructs per phase from the
``filterwarnings`` ini list.

Plain ``DeprecationWarning`` is deliberately excluded: third-party noise raised
through Airflow's own call frames is not a migration signal, and only Airflow's own
public deprecation categories are trustworthy enough to fail a test over.

References:
    https://docs.pytest.org/en/stable/how-to/capture-warnings.html
    https://airflow.apache.org/docs/apache-airflow/2.11.2/best-practices.html#testing-guidelines
"""

from __future__ import annotations

import pytest

from pytest_airflow_in_a_box._compat.capabilities import AirflowFamily, installed_family
from pytest_airflow_in_a_box.defaults import apply_filterwarnings

MIGRATION_STRICT_FILTERWARNINGS: tuple[str, ...] = (
    "error::airflow.exceptions.RemovedInAirflow3Warning",
    "error::airflow.exceptions.AirflowProviderDeprecationWarning",
)

MIGRATION_STRICT_ENABLED_KEY = pytest.StashKey[bool]()


class MigrationStrictNoOpWarning(UserWarning):
    """Warn that `--airflow-migration-strict` has nothing to forecast on this family."""


def migration_strict_enabled(config: pytest.Config) -> bool:
    """Resolve and cache whether migration-strict mode is enabled.

    The command-line option wins when set; otherwise the ini value decides, defaulting
    to disabled. Mirrors ``smoke._smoke_enabled``'s resolution and caching pattern.

    Parameters:
        config: pytest.Config containing plugin options and ini values.

    Returns:
        bool indicating whether migration-strict mode is enabled.

    Raises:
        pytest.UsageError: The `airflow_migration_strict` ini value is not a boolean.
    """

    if MIGRATION_STRICT_ENABLED_KEY in config.stash:
        return config.stash[MIGRATION_STRICT_ENABLED_KEY]

    option_value: object = config.getoption("airflow_migration_strict")
    if option_value is not None:
        enabled = bool(option_value)
    else:
        ini_value: object = config.getini("airflow_migration_strict")
        if not isinstance(ini_value, bool):
            raise pytest.UsageError("Ini option `airflow_migration_strict` must be a boolean")
        enabled = ini_value
    config.stash[MIGRATION_STRICT_ENABLED_KEY] = enabled
    return enabled


def warn_if_migration_strict_is_a_noop(config: pytest.Config) -> None:
    """Warn once, at configure time, that migration-strict mode has nothing to forecast.

    Off the Airflow 2.x family there are no 2->3 deprecations to promote, so an enabled
    flag is silently inert without this notice. A bare ``warnings.warn`` from a plugin
    hook is not captured by pytest's warnings machinery; ``issue_config_time_warning``
    is the documented seam for a configure-time warning that pytest's summary picks up.

    Parameters:
        config: pytest.Config for the active test session.
    """

    if not migration_strict_enabled(config):
        return
    if installed_family() is AirflowFamily.V2:
        return
    config.issue_config_time_warning(
        MigrationStrictNoOpWarning(
            "`--airflow-migration-strict` has nothing to forecast off the Airflow 2.x "
            "family; no filters were applied"
        ),
        2,
    )


def apply_migration_strict_filterwarnings(config: pytest.Config) -> None:
    """Prepend the migration-strict error filters when enabled on the Airflow 2.x family.

    A no-op when the mode is disabled or the installed family is not 2.x, matching
    ``warn_if_migration_strict_is_a_noop``'s gate. Must run once per process -- on an
    xdist worker that means once per worker, since each worker parses its own copy of
    the ``filterwarnings`` ini list in its own process.

    Parameters:
        config: pytest.Config containing the `filterwarnings` ini value.

    Raises:
        pytest.UsageError: The `filterwarnings` ini value is not a line list.
    """

    if not migration_strict_enabled(config):
        return
    if installed_family() is not AirflowFamily.V2:
        return
    apply_filterwarnings(config, MIGRATION_STRICT_FILTERWARNINGS)


__all__ = (
    "MIGRATION_STRICT_FILTERWARNINGS",
    "MigrationStrictNoOpWarning",
    "apply_migration_strict_filterwarnings",
    "migration_strict_enabled",
    "warn_if_migration_strict_is_a_noop",
)
