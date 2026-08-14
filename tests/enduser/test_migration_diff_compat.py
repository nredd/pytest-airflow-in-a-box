"""Verify the migration artifact against a real Airflow install.

`tests/test_record.py` and `tests/test_markers.py` prove version resolution and
`would_family_gate` against monkeypatched doubles, but never against a real Airflow
process -- and the 2.x compat CI legs run only this directory, not the unit test
modules. This module closes that gap across the whole certified matrix: real
`airflow_version`/`airflow_family` population, and a real
`requires_airflow2`/`requires_airflow3` gate correctly tagged `gated` in the recorded
artifact.

References:
    https://github.com/nredd/pytest-airflow-in-a-box/issues/42
"""

from __future__ import annotations

from importlib import metadata

import pytest

from pytest_airflow_in_a_box import __version__, artifact
from pytest_airflow_in_a_box._compat.capabilities import AirflowFamily, installed_family

pytestmark = pytest.mark.compat


def test_recorded_artifact_reports_the_real_installed_family(pytester: pytest.Pytester) -> None:
    """Populate `airflow_version`/`airflow_family`/`plugin_version` from the real install.

    Parameters:
        pytester: pytest.Pytester running the recording invocation in a subprocess.
    """

    family = installed_family()
    assert family is not None  # the compat matrix always has a certified Airflow installed
    expected_version = metadata.version(family.value)
    pytester.makepyfile("def test_trivial():\n    assert True\n")
    destination = pytester.path / "record.json"

    result = pytester.runpytest_subprocess(f"--airflow-record={destination}", "-q")

    result.assert_outcomes(passed=1)
    recorded = artifact.load_artifact(destination)
    assert recorded["airflow_family"] == family.value
    assert recorded["airflow_version"] == expected_version
    assert recorded["plugin_version"] == __version__


def test_recorded_artifact_flags_family_gated_tests(pytester: pytest.Pytester) -> None:
    """Tag a cross-family marker `gated` and a same-family marker not gated.

    Parameters:
        pytester: pytest.Pytester running the recording invocation in a subprocess.
    """

    family = installed_family()
    assert family is not None
    other_marker = "requires_airflow2" if family is AirflowFamily.V3 else "requires_airflow3"
    same_marker = "requires_airflow3" if family is AirflowFamily.V3 else "requires_airflow2"
    pytester.makepyfile(
        f"""
        import pytest

        @pytest.mark.{other_marker}
        def test_other_family():
            assert True


        @pytest.mark.{same_marker}
        def test_same_family():
            assert True
        """
    )
    destination = pytester.path / "record.json"

    result = pytester.runpytest_subprocess(f"--airflow-record={destination}", "-q")

    result.assert_outcomes(passed=1, skipped=1)
    recorded = artifact.load_artifact(destination)
    other_entry = next(
        entry for nodeid, entry in recorded["outcomes"].items() if "test_other_family" in nodeid
    )
    same_entry = next(
        entry for nodeid, entry in recorded["outcomes"].items() if "test_same_family" in nodeid
    )
    assert other_entry["outcome"] == "skipped"
    assert other_entry["phase"] is None
    assert other_entry["gated"] is True
    assert same_entry["gated"] is False
    assert same_entry["outcome"] == "passed"
