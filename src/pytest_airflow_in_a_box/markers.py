"""Register and read pytest-airflow-in-a-box markers.

References:
    https://docs.pytest.org/en/stable/example/markers.html
    https://docs.pytest.org/en/stable/reference/reference.html#pytest-mark
"""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

import pytest

MARKER_DESCRIPTIONS = (
    "api_test: require the isolated Airflow REST API server",
    "compat: exercise the public plugin surface across certified runtimes",
    "db_test: require the isolated Airflow metadata database",
    "environment(name): run only when the named environment's sentinel path exists",
    "need_serialized_dag([enabled]): request serialized Dag behavior",
    "smoke: run a bundled zero-boilerplate check (opt in with airflow_smoke)",
)

ENVIRONMENT_MARKER_NAME = "environment"
_ENVIRONMENTS_KEY = pytest.StashKey[dict[str, Path]]()


class MarkedNode(Protocol):
    """Typed pytest node surface required for marker lookup."""

    def get_closest_marker(self, name: str) -> pytest.Mark | None:
        """Return the closest marker matching a registered name.

        Parameters:
            name: str containing the registered marker name.

        Returns:
            pytest.Mark | None containing the closest matching marker.
        """


def register_markers(config: pytest.Config) -> None:
    """Register every marker accepted by the plugin.

    Parameters:
        config: pytest.Config receiving marker descriptions.
    """

    for description in MARKER_DESCRIPTIONS:
        config.addinivalue_line("markers", description)


def read_bool_marker(node: MarkedNode, name: str, *, default: bool) -> bool:
    """Read an optional zero- or one-argument boolean marker.

    Parameters:
        node: MarkedNode supporting closest-marker lookup.
        name: str containing the registered marker name.
        default: bool returned when the marker is absent.

    Returns:
        bool containing the marker value or the supplied default.

    Raises:
        pytest.UsageError: The marker contains keyword, multiple, or non-boolean arguments.
    """

    if not name:
        raise ValueError("`name` must be a non-empty marker name")
    marker = node.get_closest_marker(name)
    if marker is None:
        return default
    if marker.kwargs:
        raise pytest.UsageError(f"Marker `{name}` does not accept keyword arguments")
    if len(marker.args) > 1:
        raise pytest.UsageError(f"Marker `{name}` accepts at most one boolean argument")
    value = marker.args[0] if marker.args else True
    if not isinstance(value, bool):
        raise pytest.UsageError(f"Marker `{name}` argument must be a boolean: '{value}'")
    return value


def environment_sentinels(config: pytest.Config) -> dict[str, Path]:
    """Read and cache the configured environment sentinel table.

    Entries come from the ``airflow_environments`` ini line list as
    ``name = path`` pairs; relative paths resolve against the pytest root.

    Parameters:
        config: pytest.Config containing the parsed ini configuration.

    Returns:
        dict[str, pathlib.Path] mapping environment names to sentinel paths.

    Raises:
        pytest.UsageError: An entry is malformed or duplicated.
    """

    if _ENVIRONMENTS_KEY in config.stash:
        return config.stash[_ENVIRONMENTS_KEY]

    lines: object = config.getini("airflow_environments")
    if not isinstance(lines, list):
        raise pytest.UsageError("Ini option `airflow_environments` must be a list of lines")
    table: dict[str, Path] = {}
    for line in lines:
        if not isinstance(line, str):
            raise pytest.UsageError(
                f"Ini option `airflow_environments` line must be a string: '{line}'"
            )
        name, separator, value = (part.strip() for part in line.partition("="))
        if not separator or not name or not value:
            raise pytest.UsageError(
                f"Ini option `airflow_environments` line must be `name = path`: '{line}'"
            )
        if name in table:
            raise pytest.UsageError(f"Duplicate test environment name: `{name}`")
        path = Path(value)
        table[name] = path if path.is_absolute() else config.rootpath / path
    config.stash[_ENVIRONMENTS_KEY] = table
    return table


def apply_environment_gate(item: pytest.Item) -> None:
    """Skip one test whose declared environment sentinel is absent.

    Parameters:
        item: pytest.Item possibly carrying the ``environment`` marker.

    Raises:
        pytest.UsageError: The marker is malformed or names an unknown environment.
        pytest.skip.Exception: The environment's sentinel path does not exist.
    """

    marker = item.get_closest_marker(ENVIRONMENT_MARKER_NAME)
    if marker is None:
        return
    if marker.kwargs or len(marker.args) != 1:
        raise pytest.UsageError(
            f"Marker `{ENVIRONMENT_MARKER_NAME}` requires exactly one environment name"
        )
    name = marker.args[0]
    if not isinstance(name, str) or not name:
        raise pytest.UsageError(
            f"Marker `{ENVIRONMENT_MARKER_NAME}` argument must be a non-empty string: '{name}'"
        )
    sentinels = environment_sentinels(item.config)
    if name not in sentinels:
        known = ", ".join(f"`{key}`" for key in sorted(sentinels)) or "none configured"
        raise pytest.UsageError(
            f"Unknown test environment `{name}`; configured environments: {known}"
        )
    sentinel = sentinels[name]
    if not sentinel.exists():
        pytest.skip(f"environment `{name}` unavailable; sentinel missing: '{sentinel}'")


__all__ = (
    "MarkedNode",
    "apply_environment_gate",
    "environment_sentinels",
    "read_bool_marker",
    "register_markers",
)
