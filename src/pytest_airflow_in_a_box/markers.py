"""Register and read pytest-airflow-in-a-box markers.

References:
    https://docs.pytest.org/en/stable/example/markers.html
    https://docs.pytest.org/en/stable/reference/reference.html#pytest-mark
"""

from __future__ import annotations

from typing import Protocol

import pytest

MARKER_DESCRIPTIONS = (
    "api_test: require the isolated Airflow REST API server",
    "compat: exercise the public plugin surface across certified runtimes",
    "db_test: require the isolated Airflow metadata database",
    "need_serialized_dag([enabled]): request serialized Dag behavior",
)


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


__all__ = ("MarkedNode", "read_bool_marker", "register_markers")
