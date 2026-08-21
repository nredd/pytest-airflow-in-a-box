"""Zero-ini pytest defaults and narrowed third-party warning filters.

Every default applies only when the user has not chosen a value: options are
rewritten only while they still equal pytest's own parser default, ini values
only while absent, and warning filters are prepended so user-supplied ini
lines, applied later, take precedence. A user explicitly passing pytest's own
default (for example ``--tb=auto``) is indistinguishable from the parser
default and is overridden; any other explicit value survives.

Airflow's own deprecation warnings deliberately stay visible: the filters
silence only traced third-party sources and promote pytest's own collection
and unraisable warnings to errors.

Warnings sourced from the plugin's own bootstrap stack (the metadata-database
initialization alembic performs) live in the ``airflow_default_filterwarnings``
ini option instead of ``FILTERWARNINGS``: the bootstrap runs inside its own
reset warnings context that plain ``filterwarnings`` ini lines cannot reach, so
the option's lines are both prepended into the ini list and replayed inside
that context. Redefining the option -- even to an empty value -- replaces the
default wholesale, which is the escape hatch for warnings-as-errors suites.

References:
    https://docs.pytest.org/en/stable/reference/reference.html#ini-options-ref
    https://docs.pytest.org/en/stable/how-to/capture-warnings.html
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from warnings import _ActionKind


@dataclass(frozen=True)
class OptionDefault:
    """One option rewritten only while it equals pytest's parser default.

    Parameters:
        attribute: str naming the ``config.option`` attribute.
        parser_default: object containing pytest's own parser default.
        plugin_default: object containing the plugin's replacement default.
    """

    attribute: str
    parser_default: object
    plugin_default: object


OPTION_DEFAULTS: tuple[OptionDefault, ...] = (
    OptionDefault("tbstyle", "auto", "short"),
    OptionDefault("reportchars", "fE", "a"),
    OptionDefault("durations", None, 20),
)

INI_DEFAULTS: tuple[tuple[str, str, str], ...] = (
    ("tmp_path_retention_policy", "Which directories to keep pytest temporaries for.", "failed"),
    ("tmp_path_retention_count", "How many pytest temporary directories to keep.", "3"),
)

FILTERWARNINGS: tuple[str, ...] = (
    "ignore::DeprecationWarning:flask_appbuilder",
    "ignore::DeprecationWarning:flask_sqlalchemy",
    "ignore:.*HTTP_422_UNPROCESSABLE_ENTITY.*:DeprecationWarning:starlette",
    "error::pytest.PytestCollectionWarning",
    "error::pytest.PytestUnraisableExceptionWarning",
)

DEFAULT_FILTERWARNINGS: tuple[str, ...] = (
    # alembic warns with a stacklevel, so the attributed module is a caller frame
    # inside Airflow's migration utilities, never `alembic.config` -- the filter must
    # match on the message prefix alone. Inert on alembic versions predating it.
    "ignore:No path_separator found in configuration:DeprecationWarning",
)

ParsedFilter = tuple["_ActionKind", str, type[Warning], str, int]

DEFAULT_FILTERWARNINGS_KEY: pytest.StashKey[tuple[ParsedFilter, ...]] = pytest.StashKey()


def register_ini_defaults(parser: pytest.Parser) -> None:
    """Re-register selected builtin ini options with plugin defaults.

    The last ``addini`` registration for a name wins, and the plugin's
    ``pytest_addoption`` hook runs ``trylast`` so these land after pytest's
    builtin registrations. User ini values always beat registered defaults.

    Parameters:
        parser: pytest.Parser receiving the replacement registrations.
    """

    for name, help_text, value in INI_DEFAULTS:
        parser.addini(name, help_text, default=value)


def apply_option_defaults(config: pytest.Config) -> None:
    """Rewrite parsed options that still carry pytest's parser default.

    Parameters:
        config: pytest.Config containing parsed command-line options.
    """

    for default in OPTION_DEFAULTS:
        if getattr(config.option, default.attribute) == default.parser_default:
            setattr(config.option, default.attribute, default.plugin_default)


def apply_filterwarnings(config: pytest.Config, lines: tuple[str, ...] = FILTERWARNINGS) -> None:
    """Prepend the given warning filters below every user-supplied line.

    pytest applies ``filterwarnings`` ini lines in order and later lines win,
    so prepending keeps user configuration authoritative. Defaults to the
    plugin's own ``FILTERWARNINGS``; a caller such as ``migration_strict`` passes
    a different filter set through the same insertion mechanism.

    Parameters:
        config: pytest.Config containing the ``filterwarnings`` ini value.
        lines: tuple[str, ...] containing the filter lines to prepend, in priority
            order (the first line ends up closest to the user's own lines).

    Raises:
        pytest.UsageError: The ``filterwarnings`` ini value is not a line list.
    """

    ini_lines = config.getini("filterwarnings")
    if not isinstance(ini_lines, list):
        raise pytest.UsageError("Ini option `filterwarnings` must be a list of filter lines")
    for line in reversed(lines):
        if line not in ini_lines:
            ini_lines.insert(0, line)


def register_default_filterwarnings_option(parser: pytest.Parser) -> None:
    """Register the ``airflow_default_filterwarnings`` ini option with its default.

    The default covers warnings verifiably sourced from the plugin's own bootstrap
    stack; a user redefining the option -- even to an empty value -- replaces the
    default wholesale, per linelist ini semantics.

    Parameters:
        parser: pytest.Parser receiving the registration.
    """

    parser.addini(
        "airflow_default_filterwarnings",
        "Warning filters covering the plugin's own bootstrap; redefine (even empty) to replace.",
        type="linelist",
        default=list(DEFAULT_FILTERWARNINGS),
    )


def apply_default_filterwarnings(config: pytest.Config) -> None:
    """Validate, parse, cache, and prepend the bootstrap default warning filters.

    Each line is parsed eagerly with pytest's own ini-line parser so a malformed
    value aborts the session as a usage error instead of exploding mid-bootstrap.
    The parsed filters are cached on the config stash for
    ``apply_bootstrap_warning_filters`` to replay inside the bootstrap's reset
    warnings context, and the raw lines are prepended into the ``filterwarnings``
    ini list so user-supplied lines, applied later, still win.

    Parameters:
        config: pytest.Config containing the ``airflow_default_filterwarnings``
            ini value.

    Raises:
        pytest.UsageError: The ini value is not a line list or a line does not
            parse as a warning filter.
    """

    # Local import: deferred so a future pytest release relocating this private,
    # version-coupled symbol can't break import for every plugin user -- only a
    # configure-time call ever reaches it.
    from _pytest.config import parse_warning_filter

    lines = config.getini("airflow_default_filterwarnings")
    if not isinstance(lines, list):
        raise pytest.UsageError(
            "Ini option `airflow_default_filterwarnings` must be a list of filter lines"
        )
    parsed: list[ParsedFilter] = []
    for line in lines:
        try:
            parsed.append(parse_warning_filter(line, escape=False))
        except pytest.UsageError as error:
            raise pytest.UsageError(
                f"Ini option `airflow_default_filterwarnings` contains an invalid "
                f"filter line: {error}"
            ) from error
    config.stash[DEFAULT_FILTERWARNINGS_KEY] = tuple(parsed)
    apply_filterwarnings(config, tuple(lines))


def apply_bootstrap_warning_filters(config: pytest.Config) -> None:
    """Install the cached bootstrap default filters into the active warnings context.

    Called inside the metadata-database bootstrap's ``catch_warnings`` block after
    ``warnings.simplefilter("default")`` wipes the filter list; ``filterwarnings``
    prepends, so these win over the reset default for the block's duration. A
    missing stash entry is a no-op so duck-typed callers stay safe.

    Parameters:
        config: pytest.Config carrying the cached parsed filters on its stash.
    """

    for parsed_filter in config.stash.get(DEFAULT_FILTERWARNINGS_KEY, ()):
        warnings.filterwarnings(*parsed_filter)


__all__ = (
    "DEFAULT_FILTERWARNINGS",
    "DEFAULT_FILTERWARNINGS_KEY",
    "FILTERWARNINGS",
    "INI_DEFAULTS",
    "OPTION_DEFAULTS",
    "OptionDefault",
    "ParsedFilter",
    "apply_bootstrap_warning_filters",
    "apply_default_filterwarnings",
    "apply_filterwarnings",
    "apply_option_defaults",
    "register_default_filterwarnings_option",
    "register_ini_defaults",
)
