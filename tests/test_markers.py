"""Test strict marker registration and typed argument access."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from pytest_airflow_in_a_box import markers
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
        @pytest.mark.smoke
        def test_registered_markers():
            pass
        """
    )

    result = pytester.runpytest_subprocess("--strict-markers", "-q")

    result.assert_outcomes(passed=1)


def _environment_config(lines: object, rootpath: Path) -> Any:
    """Create a minimal configuration double for environment-gate tests.

    Parameters:
        lines: object containing the ``airflow_environments`` ini value.
        rootpath: pathlib.Path used to resolve relative sentinel paths.

    Returns:
        types.SimpleNamespace shaped like the configuration surface under test.
    """

    return SimpleNamespace(
        getini=lambda name: {"airflow_environments": lines}[name],
        rootpath=rootpath,
        stash=pytest.Stash(),
    )


def _environment_item(config: Any, marker: pytest.Mark | None) -> Any:
    """Create a minimal item double carrying one optional marker.

    Parameters:
        config: Any containing the configuration double.
        marker: pytest.Mark | None returned for every marker lookup.

    Returns:
        types.SimpleNamespace shaped like the item surface under test.
    """

    return SimpleNamespace(config=config, get_closest_marker=lambda _name: marker)


def test_environment_sentinels_parse_and_resolve(tmp_path: Path) -> None:
    """Parse `name = path` lines and resolve relative paths against root."""

    config = _environment_config(["lab = /opt/lab/sentinel", "edge = relative/sentinel"], tmp_path)

    sentinels = markers.environment_sentinels(config)

    assert sentinels == {
        "lab": Path("/opt/lab/sentinel"),
        "edge": tmp_path / "relative" / "sentinel",
    }
    assert markers.environment_sentinels(config) is sentinels


@pytest.mark.parametrize(
    ("lines", "match"),
    [
        ("oops", "must be a list of lines"),
        ([7], "line must be a string"),
        (["missing-separator"], "must be `name = path`"),
        (["= /path"], "must be `name = path`"),
        (["name ="], "must be `name = path`"),
        (["dup = /a", "dup = /b"], "Duplicate test environment name: `dup`"),
    ],
)
def test_environment_sentinels_reject_malformed_lines(
    tmp_path: Path,
    lines: object,
    match: str,
) -> None:
    """Reject malformed and duplicated environment lines."""

    with pytest.raises(pytest.UsageError, match=match):
        markers.environment_sentinels(_environment_config(lines, tmp_path))


def test_environment_gate_ignores_unmarked_items(tmp_path: Path) -> None:
    """Leave items without the marker untouched."""

    item = _environment_item(_environment_config([], tmp_path), None)

    markers.apply_environment_gate(item)


@pytest.mark.parametrize(
    ("mark", "match"),
    [
        (pytest.mark.environment().mark, "exactly one environment name"),
        (pytest.mark.environment("a", "b").mark, "exactly one environment name"),
        (pytest.mark.environment(name="a").mark, "exactly one environment name"),
        (pytest.mark.environment(7).mark, "must be a non-empty string"),
        (pytest.mark.environment("").mark, "must be a non-empty string"),
    ],
)
def test_environment_gate_rejects_malformed_markers(
    tmp_path: Path,
    mark: pytest.Mark,
    match: str,
) -> None:
    """Reject malformed marker arguments."""

    item = _environment_item(_environment_config([], tmp_path), mark)

    with pytest.raises(pytest.UsageError, match=match):
        markers.apply_environment_gate(item)


def test_environment_gate_rejects_unknown_names(tmp_path: Path) -> None:
    """Name the configured environments when rejecting an unknown one."""

    config = _environment_config(["lab = /opt/lab/sentinel"], tmp_path)
    item = _environment_item(config, pytest.mark.environment("prod").mark)

    with pytest.raises(pytest.UsageError, match=r"Unknown test environment `prod`.*`lab`"):
        markers.apply_environment_gate(item)


def test_environment_gate_skips_without_sentinel(tmp_path: Path) -> None:
    """Skip when the sentinel path does not exist and run when it does."""

    sentinel = tmp_path / "sentinel"
    config = _environment_config([f"lab = {sentinel}"], tmp_path)
    item = _environment_item(config, pytest.mark.environment("lab").mark)

    with pytest.raises(pytest.skip.Exception, match="environment `lab` unavailable"):
        markers.apply_environment_gate(item)

    sentinel.touch()
    config.stash = pytest.Stash()
    markers.apply_environment_gate(item)


def test_environment_gate_end_to_end(pytester: pytest.Pytester) -> None:
    """Skip and run a gated test based on the sentinel's presence."""

    sentinel = pytester.path / "lab-sentinel"
    pytester.makeini(f"[pytest]\nairflow_environments =\n    lab = {sentinel}\n")
    pytester.makepyfile(
        """
        import pytest

        @pytest.mark.environment("lab")
        def test_needs_lab():
            assert True
        """
    )

    skipped = pytester.runpytest_subprocess("-q")
    skipped.assert_outcomes(skipped=1)

    sentinel.touch()
    ran = pytester.runpytest_subprocess("-q")
    ran.assert_outcomes(passed=1)
