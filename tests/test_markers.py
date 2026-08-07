"""Test strict marker registration and typed argument access."""

from __future__ import annotations

import pytest

from pytest_airflow_in_a_box.markers import read_bool_marker


class _MarkedNode:
    """Provide a minimal typed marker lookup for direct tests."""

    def __init__(self, marker: pytest.Mark | None) -> None:
        """Store the marker returned by every lookup.

        Parameters:
            marker: pytest.Mark | None returned by ``get_closest_marker``.
        """

        self.marker = marker

    def get_closest_marker(self, name: str) -> pytest.Mark | None:
        """Return the stored marker after validating the requested name.

        Parameters:
            name: str containing the expected marker name.

        Returns:
            pytest.Mark | None containing the stored marker.
        """

        assert name == "need_serialized_dag"
        return self.marker


@pytest.mark.parametrize(
    ("marker", "default", "expected"),
    [
        (None, False, False),
        (None, True, True),
        (pytest.mark.need_serialized_dag.mark, False, True),
        (pytest.mark.need_serialized_dag(True).mark, False, True),
        (pytest.mark.need_serialized_dag(False).mark, True, False),
    ],
)
def test_read_bool_marker(marker: pytest.Mark | None, *, default: bool, expected: bool) -> None:
    """Read absent, bare, true, and false marker forms without coercion."""

    assert (
        read_bool_marker(_MarkedNode(marker), "need_serialized_dag", default=default) is expected
    )


@pytest.mark.parametrize(
    ("marker", "message"),
    [
        (
            pytest.mark.need_serialized_dag(enabled=True).mark,
            "does not accept keyword arguments",
        ),
        (
            pytest.mark.need_serialized_dag(True, False).mark,
            "accepts at most one boolean argument",
        ),
        (
            pytest.mark.need_serialized_dag(1).mark,
            "argument must be a boolean: '1'",
        ),
    ],
)
def test_read_bool_marker_rejects_invalid_arguments(marker: pytest.Mark, message: str) -> None:
    """Reject every marker form outside the documented boolean contract."""

    with pytest.raises(pytest.UsageError, match=message):
        read_bool_marker(_MarkedNode(marker), "need_serialized_dag", default=False)


def test_read_bool_marker_rejects_empty_name() -> None:
    """Require a usable marker name before consulting a node."""

    with pytest.raises(ValueError, match="non-empty marker name"):
        read_bool_marker(_MarkedNode(None), "", default=False)


def test_registered_markers_pass_strict_validation(pytester: pytest.Pytester) -> None:
    """Register every public marker before strict collection validation."""

    pytester.makepyfile(
        """
        import pytest

        @pytest.mark.api_test
        @pytest.mark.compat
        @pytest.mark.db_test
        @pytest.mark.need_serialized_dag(False)
        def test_registered_markers():
            pass
        """
    )

    result = pytester.runpytest_subprocess("--strict-markers", "-q")

    result.assert_outcomes(passed=1)
