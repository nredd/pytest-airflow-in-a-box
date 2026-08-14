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

References:
    https://docs.pytest.org/en/stable/reference/reference.html#ini-options-ref
    https://docs.pytest.org/en/stable/how-to/capture-warnings.html
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest


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


__all__ = (
    "FILTERWARNINGS",
    "INI_DEFAULTS",
    "OPTION_DEFAULTS",
    "OptionDefault",
    "apply_filterwarnings",
    "apply_option_defaults",
    "register_ini_defaults",
)
