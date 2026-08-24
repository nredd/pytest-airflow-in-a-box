"""Apply repo-wide Airflow configuration declared through the ``airflow_config`` ini option.

``config.airflow_config`` scopes an override to a ``with`` block, which is the wrong shape for
a repo's *defaults*. A consumer that needs ``core.dag_ignore_file_syntax`` set before the first
``DagBag`` build in the process previously had to leak a bare ``__enter__()`` at conftest module
scope, or hand-roll a ``scope="session", autouse=True`` fixture and trust fixture instantiation
order. This module is the declarative answer: ``section.key = value`` lines in the ini file,
applied as ``AIRFLOW__SECTION__KEY`` environment variables.

The application point is ``plugin.pytest_load_initial_conftests``, immediately after
``bootstrap.load_initial_state``. Two properties fall out of that placement:

- **Overrides beat every consumer conftest and every Dag parse.** ``Config._preparse`` populates
  the ini table and folds ``-o`` overrides into it before dispatching the hook, and pytest's own
  conftest-collecting hookimpl is ``trylast``, so this runs first. Nothing a consumer writes has
  executed yet.
- **Bootstrap still owns its own names.** ``bootstrap._install_environment`` scrubs every name in
  ``_environment_names()`` that the run's state did not set, so applying earlier would risk
  having values popped back out.

The two facts together force the denylist. Every ``AIRFLOW__``-prefixed name bootstrap owns is
rejected with an actionable message, because setting one would either be scrubbed, break the
metadata database isolation the plugin exists to provide, or -- worst -- desynchronize an xdist
controller from its workers. Workers re-run ``pytest_load_initial_conftests`` and cross-check the
inherited environment against ``_environment(state)``; an ini line touching an owned name would
abort every worker with a drift error naming a variable the consumer never typed. The list is
*derived* from ``bootstrap._environment_names()`` rather than copied, so it cannot go stale.

``AIRFLOW__CORE__EXECUTOR`` is deliberately not on it: bootstrap does not own that name (only the
2.x branch assigns it, and only when it is absent), and ``--airflow-doctor`` already tells users
to override it.

Bootstrap is not the only writer, though, so a second and *conditional* denial exists.
``fixtures.dagbag._cached_dag_bag`` and ``dagcorpus._build_dag_corpus`` both pin
``AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT`` from the ``airflow_dag_parse_timeout`` ini option
immediately before parsing, and that same value scales the per-file parse watchdog and the
slowpoke budget -- so an ini override of that option would be silently discarded on a smoke run,
and honoring it instead would leave the watchdog shorter than the parser it is supposed to back
up. Neither is acceptable, so declaring it *while the catalog is enabled* is an error naming the
one knob that drives all three. On a run without the catalog nothing else writes the name and
the override applies normally, which is why the denial is conditional rather than absolute.

That conditional check runs from ``pytest_configure``, not from the initial parse. Command-line
options are not reliably readable through ``config.getoption`` during
``pytest_load_initial_conftests`` -- pytest has not finished populating ``config.option`` there,
which is why ``bootstrap`` is handed the raw ``args`` list instead -- and ``smoke._smoke_enabled``
memoizes its answer onto the config stash, so calling it that early would cache a wrong ``False``
and silently disable the catalog for the whole run. Applying the overrides still happens during
the initial parse; only the conflict check waits.

Nothing is written into the generated ``airflow.cfg``. On Airflow 3.x the environment already
outranks every file on each ``conf.get()``, and on 2.x ``core.unit_test_mode`` redirects the
parser to ``unit_tests.cfg`` and never reads ``AIRFLOW_CONFIG`` at all, so the environment is the
only channel that works on both families.

References:
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.hookspec.pytest_load_initial_conftests
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.Config.add_cleanup
    https://airflow.apache.org/docs/apache-airflow/stable/howto/set-config.html
"""

from __future__ import annotations

from contextlib import ExitStack
from typing import Final

import pytest

from pytest_airflow_in_a_box.bootstrap import _environment_names
from pytest_airflow_in_a_box.config import ENV_VAR_PREFIX, airflow_config, env_var_name

INI_OPTION_NAME: Final[str] = "airflow_config"
INI_OVERRIDES_KEY = pytest.StashKey[dict[tuple[str, str], str]]()

SECTION_KEY_SEPARATOR: Final[str] = "."
VALUE_SEPARATOR: Final[str] = "="

_DEFAULT_REMEDY: Final[str] = (
    "this run's bootstrap owns it; override it for a single test with "
    "`pytest_airflow_in_a_box.config.airflow_config`"
)
_REMEDIES: Final[dict[str, str]] = {
    "AIRFLOW__CORE__DAGS_FOLDER": (
        "set the `airflow_dags_folder` ini option or pass `--dag-folder`"
    ),
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": (
        "select the backend with the `airflow_db_backend` ini option or `--airflow-db-backend`"
    ),
    "AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_ENABLED": (
        "select the backend with the `airflow_db_backend` ini option or `--airflow-db-backend`"
    ),
    "AIRFLOW__LOGGING__BASE_LOG_FOLDER": (
        "set the `airflow_home` ini option or pass `--airflow-home`"
    ),
    "AIRFLOW__CORE__PLUGINS_FOLDER": "set the `airflow_plugins_folder` ini option",
    "AIRFLOW__CORE__XCOM_BACKEND": "set the `airflow_xcom_backend` ini option",
    "AIRFLOW__SECRETS__BACKEND": "set the `airflow_secrets_backend` ini option",
    "AIRFLOW__SECRETS__BACKEND_KWARGS": "set the `airflow_secrets_backend_kwargs` ini option",
}

# The catalog pins this from `airflow_dag_parse_timeout`, which also scales the per-file parse
# watchdog and the slowpoke budget, so the two knobs cannot both be authoritative.
_SMOKE_OWNED: Final[dict[str, str]] = {
    "AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT": (
        "the bundled smoke catalog pins it from the `airflow_dag_parse_timeout` ini option, "
        "which also scales the per-file parse watchdog and the slowpoke budget; set that "
        "option instead"
    ),
}


def owned_env_names() -> frozenset[str]:
    """List the Airflow configuration variables bootstrap owns, which the ini may not set.

    Derived from ``bootstrap._environment_names()`` rather than duplicated, so a name bootstrap
    starts owning is denied here on the same commit. Names outside Airflow's configuration
    namespace (``AIRFLOW_HOME``, ``AIRFLOW_CONFIG``, the state handoff variable) are filtered
    out because ``env_var_name`` can never produce one.

    Returns:
        frozenset[str] containing owned ``AIRFLOW__SECTION__KEY`` variable names.
    """

    return frozenset(name for name in _environment_names() if name.startswith(ENV_VAR_PREFIX))


def parse_ini_overrides(config: pytest.Config) -> dict[tuple[str, str], str]:
    """Parse the ``airflow_config`` ini option into Airflow configuration overrides.

    Each line is ``section.key = value``. The value is split on the *first* ``=``, because
    values legitimately contain one (connection URLs, query strings). The section and key are
    split on the *last* ``.``, because Airflow section names may contain dots while keys may
    not. Section, key, and value are each stripped. An empty value is accepted and means the
    empty string -- the line list has no syntax for "make this option absent", which
    ``airflow_config``'s ``None`` expresses in the programmatic form.

    Character validation is delegated wholesale to ``config.env_var_name``, so a malformed
    section or key fails with the same message the programmatic form produces. Duplicates are
    detected on the resolved variable name rather than the raw pair, because ``a.b`` and ``a_b``
    both mangle to ``AIRFLOW__A_B__*`` and Airflow cannot address them separately.

    Every line is validated before any override is returned, so a malformed entry can never
    reach the environment.

    Parameters:
        config: pytest.Config containing the parsed ini configuration.

    Returns:
        dict[tuple[str, str], str] mapping ``(section, key)`` pairs to values.

    Raises:
        pytest.UsageError: The ini value or one of its lines is malformed, two lines name one
            variable, or a line sets an option this plugin's bootstrap owns.
    """

    lines: object = config.getini(INI_OPTION_NAME)
    if not isinstance(lines, list):
        raise pytest.UsageError(f"Ini option `{INI_OPTION_NAME}` must be a list of lines")

    denied = owned_env_names()
    overrides: dict[tuple[str, str], str] = {}
    sources: dict[str, str] = {}
    for line in lines:
        if not isinstance(line, str):
            raise pytest.UsageError(
                f"Ini option `{INI_OPTION_NAME}` line must be a string: '{line}'"
            )
        assignment, value_separator, value = line.partition(VALUE_SEPARATOR)
        if not value_separator:
            raise pytest.UsageError(
                f"Ini option `{INI_OPTION_NAME}` line must be `section.key = value`: '{line}'"
            )
        section, section_separator, key = assignment.rpartition(SECTION_KEY_SEPARATOR)
        if not section_separator:
            raise pytest.UsageError(
                f"Ini option `{INI_OPTION_NAME}` line must name a section and a key as "
                f"`section.key = value`: '{line}'"
            )
        pair = (section.strip(), key.strip())
        name = env_var_name(*pair)
        if name in denied:
            raise pytest.UsageError(
                f"Ini option `{INI_OPTION_NAME}` may not set `{pair[0]}.{pair[1]}`; "
                f"pytest-airflow-in-a-box owns `{name}` -- "
                f"{_REMEDIES.get(name, _DEFAULT_REMEDY)}"
            )
        if name in sources:
            raise pytest.UsageError(
                f"Ini option `{INI_OPTION_NAME}` sets `{name}` twice "
                f"('{sources[name]}' and '{line}'); Airflow cannot address them separately"
            )
        sources[name] = line
        overrides[pair] = value.strip()
    return overrides


def validate_smoke_conflict(config: pytest.Config) -> None:
    """Reject a declared override the enabled smoke catalog would silently overwrite.

    Runs from ``pytest_configure`` rather than from the initial parse, because resolving
    whether the catalog is enabled needs a fully parsed command line and memoizes its answer
    onto the config stash -- see this module's docstring.

    A no-op when the catalog is off, when nothing is declared, or when nothing declared
    collides, which is every ordinary run.

    Parameters:
        config: pytest.Config for the active test session.

    Raises:
        pytest.UsageError: A declared option is one the enabled smoke catalog pins itself.
    """

    # Deferred: `smoke` pulls in `fixtures.dagbag` and the Airflow compatibility layer, which
    # this module must not drag onto the initial-parse import path.
    from pytest_airflow_in_a_box.smoke import _smoke_enabled

    if not _smoke_enabled(config):
        return
    for section, key in config.stash[INI_OVERRIDES_KEY]:
        name = env_var_name(section, key)
        if name in _SMOKE_OWNED:
            raise pytest.UsageError(
                f"Ini option `{INI_OPTION_NAME}` may not set `{section}.{key}` while the "
                f"bundled smoke catalog is enabled; {_SMOKE_OWNED[name]}"
            )


def apply_ini_overrides(config: pytest.Config) -> None:
    """Apply the declared overrides process-wide and restore them when pytest shuts down.

    Applied unconditionally on every process, controller and xdist worker alike. A worker
    inherits the controller's environment through its execnet gateway and then re-applies the
    identical values, which is a no-op; gating on the worker would instead leave the two
    processes disagreeing and the worker without a restore.

    Restoration reuses ``config.airflow_config``'s exact-restore semantics -- a name absent
    beforehand is deleted rather than emptied -- held open on an ``ExitStack`` closed from
    ``pytest.Config.add_cleanup``. That callback stack unwinds last-in-first-out, so this
    restore runs ahead of bootstrap's own environment restore, and it still runs when a later
    step of the initial parse aborts the session.

    Parameters:
        config: pytest.Config for the active invocation.

    Raises:
        pytest.UsageError: The ini value or one of its lines is malformed.
    """

    overrides = parse_ini_overrides(config)
    config.stash[INI_OVERRIDES_KEY] = overrides
    stack = ExitStack()
    stack.enter_context(airflow_config(overrides))
    config.add_cleanup(stack.close)


__all__ = (
    "INI_OPTION_NAME",
    "INI_OVERRIDES_KEY",
    "apply_ini_overrides",
    "owned_env_names",
    "parse_ini_overrides",
    "validate_smoke_conflict",
)
