"""Temporarily override Apache Airflow configuration options and environment variables.

``airflow_config`` is one context manager, usable as a decorator, replacing upstream's three
overlapping helpers (``conf_vars``, ``env_vars``, ``create_fresh_airflow_config``). Airflow
configuration keys and plain environment variables travel through one code path: both are applied
as environment variables and both are restored exactly on exit, including names that were
previously absent.

Probed behavior on Apache Airflow 3.1.8, 3.2.2, and 3.3.0, which shapes the design:

- **No cache invalidation is required.** ``AirflowConfigParser._lookup_sequence`` puts
  ``_get_environment_variables`` first on every ``get()`` and keeps no per-value cache, so ``get``,
  ``getboolean``, ``has_option``, ``getsection``, and ``as_dict`` observe an assignment immediately
  and observe its removal immediately. This module therefore calls no Airflow invalidation hook and
  imports nothing from Airflow unless ``refresh_settings`` is requested.
- **One environment path covers both configuration parsers.** Airflow 3.2 added a second parser
  at ``airflow.sdk.configuration.conf``, absent on 3.1.x, reading the same ``AIRFLOW__*``
  variables. Assigning the variable reaches core and Task SDK parsers alike, with no inspection
  of loaded modules and no in-place mutation of parser state.
- **Values are expanded when Airflow reads them.** ``conf.get`` pipes the raw variable through
  ``expand_env_var``, which applies ``os.path.expandvars`` then ``os.path.expanduser``. A value
  containing ``~`` or ``$`` therefore does not round-trip: ``os.environ`` holds the literal, while
  ``conf.get`` returns the expansion.
- **A plain variable outranks its ``_CMD`` and ``_SECRET`` siblings,** so assigning the plain name
  always overrides. The one asymmetry is a ``None`` override for a key whose ``_CMD`` or
  ``_SECRET`` sibling is already set: the sibling resurfaces inside the context. That is the
  documented behavior rather than a bug, because ``None`` means "let Airflow fall back to whatever
  it would otherwise do", and a sibling variable is one of those things.
- **``getsection`` is only partly covered.** It reflects an override for a key its section already
  declares, but returns ``None`` for a section that exists solely through an environment variable.

Every assignment mutates ``os.environ``, which is process-global and not thread-safe. That suits
xdist, whose workers are separate processes, and the in-process ``run_task`` path, which is
single-threaded, but concurrent threads in one process are not supported.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/howto/set-config.html
    https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html
    https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#testing-a-dag
    https://docs.python.org/3/library/contextlib.html#contextlib.contextmanager
    https://docs.python.org/3/library/os.html#os.environ
"""

from __future__ import annotations

import os
import re
import warnings
from collections.abc import Iterator, Mapping
from contextlib import contextmanager

import pytest

from pytest_airflow_in_a_box._compat.settings import configure_vars

ConfigOverrides = Mapping[tuple[str, str], str | None]
EnvOverrides = Mapping[str, str | None]

ENV_VAR_PREFIX = "AIRFLOW__"
SECTION_KEY_DELIMITER = "__"
SECTION_PATTERN = re.compile(r"^[A-Za-z0-9_.]+$")
KEY_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")
ENV_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def env_var_name(section: str, key: str) -> str:
    """Build the Airflow environment variable name overriding one configuration option.

    Reproduces ``AirflowConfigParser._env_var_name`` without importing Airflow: dots in the
    section become underscores and both parts are upper-cased, so ``("a.b", "k")`` names
    ``AIRFLOW__A_B__K``.

    Parameters:
        section: str naming the configuration section.
        key: str naming the configuration option within the section.

    Returns:
        str containing the ``AIRFLOW__SECTION__KEY`` environment variable name.

    Raises:
        pytest.UsageError: The section or key is empty, not a string, or contains characters
            Airflow's mangling cannot express.
    """

    if not isinstance(section, str) or not section:
        raise pytest.UsageError(f"Configuration section must be a non-empty string: '{section}'")
    if not SECTION_PATTERN.fullmatch(section) or SECTION_KEY_DELIMITER in section:
        raise pytest.UsageError(
            "Configuration section must contain only letters, digits, underscores, and dots, "
            f"and must not contain `{SECTION_KEY_DELIMITER}`: '{section}'"
        )
    if not isinstance(key, str) or not key:
        raise pytest.UsageError(f"Configuration key must be a non-empty string: '{key}'")
    if not KEY_PATTERN.fullmatch(key) or SECTION_KEY_DELIMITER in key:
        raise pytest.UsageError(
            "Configuration key must contain only letters, digits, and underscores, and must not "
            f"contain `{SECTION_KEY_DELIMITER}`: '{key}'"
        )
    mangled_section = section.replace(".", "_").upper()
    return f"{ENV_VAR_PREFIX}{mangled_section}{SECTION_KEY_DELIMITER}{key.upper()}"


def _config_assignments(overrides: ConfigOverrides | None) -> dict[str, str | None]:
    """Validate configuration overrides and resolve them to environment assignments.

    Parameters:
        overrides: ConfigOverrides | None mapping ``(section, key)`` pairs to replacement
            values, where ``None`` requests the option's absence.

    Returns:
        dict[str, str | None] mapping environment variable names to assignments.

    Raises:
        pytest.UsageError: The mapping, one of its keys, or one of its values is malformed, or
            two pairs name the same environment variable.
    """

    if overrides is None:
        return {}
    if not isinstance(overrides, Mapping):
        raise pytest.UsageError(
            f"`overrides` must be a mapping of `(section, key)` pairs to values: '{overrides}'"
        )

    assignments: dict[str, str | None] = {}
    sources: dict[str, tuple[str, str]] = {}
    for pair, value in overrides.items():
        if not isinstance(pair, tuple) or len(pair) != 2:
            raise pytest.UsageError(f"`overrides` keys must be `(section, key)` pairs: '{pair}'")
        section, key = pair
        name = env_var_name(section, key)
        if value is not None and not isinstance(value, str):
            raise pytest.UsageError(
                f"`overrides` values must be a string or None: '{value}' for `{name}`"
            )
        if name in sources:
            raise pytest.UsageError(
                f"`overrides` pairs {sources[name]} and {pair} both name `{name}`; "
                "Airflow cannot address them separately"
            )
        sources[name] = pair
        assignments[name] = value
    return assignments


def _env_assignments(env: EnvOverrides | None) -> dict[str, str | None]:
    """Validate plain environment overrides and resolve them to environment assignments.

    Parameters:
        env: EnvOverrides | None mapping variable names to replacement values, where ``None``
            requests the variable's absence.

    Returns:
        dict[str, str | None] mapping environment variable names to assignments.

    Raises:
        pytest.UsageError: The mapping, one of its names, or one of its values is malformed, or
            a name belongs to Airflow's configuration namespace.
    """

    if env is None:
        return {}
    if not isinstance(env, Mapping):
        raise pytest.UsageError(f"`env` must be a mapping of variable names to values: '{env}'")

    assignments: dict[str, str | None] = {}
    for name, value in env.items():
        if not isinstance(name, str) or not name:
            raise pytest.UsageError(f"`env` names must be a non-empty string: '{name}'")
        if not ENV_NAME_PATTERN.fullmatch(name):
            raise pytest.UsageError(
                "`env` names must start with a letter or underscore and contain only letters, "
                f"digits, and underscores: '{name}'"
            )
        if name.startswith(ENV_VAR_PREFIX):
            raise pytest.UsageError(
                f"`env` names must not start with `{ENV_VAR_PREFIX}`; pass Airflow configuration "
                f"options through `overrides` instead: '{name}'"
            )
        if value is not None and not isinstance(value, str):
            raise pytest.UsageError(
                f"`env` values must be a string or None: '{value}' for `{name}`"
            )
        assignments[name] = value
    return assignments


def _assign(values: Mapping[str, str | None]) -> None:
    """Apply one batch of environment assignments, deleting every ``None``.

    This is the single write primitive shared by application and restoration, so a name absent
    beforehand is deleted rather than assigned an empty string in both directions.

    Parameters:
        values: Mapping[str, str | None] mapping variable names to values, where ``None``
            removes the variable.
    """

    for name, value in values.items():
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = value


def _refresh_settings() -> None:
    """Recompute the Airflow settings globals derived from configuration.

    ``airflow.settings`` resolves ``SQL_ALCHEMY_CONN``, ``DAGS_FOLDER``, and ``PLUGINS_FOLDER``
    once at import, so those globals do not follow an environment assignment on their own.
    ``configure_vars`` derives them entirely from the configuration parser, making it idempotent
    and safe to call again while restoring.
    """

    configure_vars()


@contextmanager
def airflow_config(
    overrides: ConfigOverrides | None = None,
    *,
    env: EnvOverrides | None = None,
    refresh_settings: bool = False,
) -> Iterator[None]:
    """Override Airflow configuration options and environment variables for one context.

    Usable as a context manager or as a decorator on a test function. Configuration options are
    applied as ``AIRFLOW__SECTION__KEY`` environment variables, the same pre-import-safe mechanism
    bootstrap uses, so overrides reach every Airflow configuration parser in the process. A
    ``None`` value makes the name absent for the duration of the context, letting Airflow fall
    back to ``airflow.cfg`` and then to its own default. Every name is restored exactly on exit,
    and a name that was absent beforehand is deleted rather than emptied. Nesting restores in
    last-in-first-out order because each context restores only its own snapshot.

    Both mappings are validated completely before any assignment happens, so a malformed argument
    cannot leave the environment partly modified. Because validation runs on context entry rather
    than at decoration time, a malformed argument to the decorator form surfaces as a test failure.

    Configuration names always start with ``AIRFLOW__`` and ``env`` names are forbidden from
    starting with it, so the two mappings address disjoint namespaces and cannot collide.

    ``refresh_settings`` additionally recomputes the ``airflow.settings`` globals, which is
    required for options such as ``core.dags_folder`` and ``core.plugins_folder`` that Airflow
    reads through ``settings`` rather than through the configuration parser. It defaults to
    ``False`` because it imports Airflow and rewrites process-global state this plugin's bootstrap
    owns. It is a partial remedy: a module that re-exported a settings value by value, as
    ``airflow.serialization.serialized_objects`` does with ``DAGS_FOLDER``, froze that binding at
    import and no refresh can update it.

    Do not wrap the first use of ``api_client`` or ``api_server_url`` in this context. Those
    fixtures launch a session-scoped subprocess that inherits the environment live at startup, so
    an override would outlive the context inside that server.

    Parameters:
        overrides: ConfigOverrides | None mapping ``(section, key)`` pairs to replacement values,
            where ``None`` requests the option's absence.
        env: EnvOverrides | None mapping plain variable names to replacement values, where
            ``None`` requests the variable's absence.
        refresh_settings: bool recomputing the ``airflow.settings`` configuration globals after
            applying and after restoring.

    Yields:
        None after the environment is in place.

    Raises:
        pytest.UsageError: An argument, name, or value is malformed.
    """

    assignments = _config_assignments(overrides) | _env_assignments(env)
    original = {name: os.environ.get(name) for name in assignments}
    try:
        _assign(assignments)
        if refresh_settings:
            _refresh_settings()
        yield
    finally:
        _assign(original)
        if refresh_settings:
            _refresh_settings()


@contextmanager
def conf_vars(overrides: ConfigOverrides) -> Iterator[None]:
    """Override Airflow configuration options, deprecated in favor of ``airflow_config``.

    Provided under the name public Airflow documentation teaches. It delegates to
    ``airflow_config`` with default keyword arguments, so it carries this plugin's semantics
    rather than reproducing upstream's behavior exactly: notably it does not recompute the
    ``airflow.settings`` globals, which ``airflow_config(..., refresh_settings=True)`` does.

    Parameters:
        overrides: ConfigOverrides mapping ``(section, key)`` pairs to replacement values, where
            ``None`` requests the option's absence.

    Yields:
        None after the configuration is in place.

    Raises:
        pytest.UsageError: An argument, name, or value is malformed.
    """

    # NOTE(redd): Reports the direct `with` site. Decorator use reports one frame inside
    # `contextlib`, which no stacklevel can correct for both forms.
    warnings.warn(
        "`conf_vars` is deprecated; use "
        "`pytest_airflow_in_a_box.config.airflow_config` instead, which also accepts plain "
        "environment variables through `env=` and can recompute Airflow's settings globals "
        "through `refresh_settings=True`.",
        DeprecationWarning,
        stacklevel=3,
    )
    with airflow_config(overrides):
        yield


__all__ = ("ENV_VAR_PREFIX", "airflow_config", "conf_vars", "env_var_name")
