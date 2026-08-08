"""Exercise end-user DagBag and collection workflows."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.compat

CORPUS = Path(__file__).parents[1] / "dags"


def test_dagbag_imports_the_artifact_corpus(pytester: pytest.Pytester) -> None:
    """Parse every valid corpus Dag and retain the intentional import error."""

    pytester.makepyfile(
        """
        def test_imports(full_dag_bag):
            expected = {
                "asset_consumer", "asset_outlet", "chained", "happy_path", "mapped",
                "provider_composition", "sensor",
            }
            assert set(full_dag_bag.dags) == expected
            assert len(full_dag_bag.import_errors) == 1
            assert next(iter(full_dag_bag.import_errors)).endswith("broken.py")
        """
    )

    result = pytester.runpytest_subprocess("-q", f"--dag-folder={CORPUS}")

    result.assert_outcomes(passed=1)


def test_collection_checks_dags_and_declared_param_cases(pytester: pytest.Pytester) -> None:
    """Collect corpus files as import tests plus pinned parameter cases."""

    result = pytester.runpytest_subprocess("-q", f"--collect-dag-folder={CORPUS}", str(CORPUS))

    result.assert_outcomes(passed=8, failed=1)
    result.stdout.fnmatch_lines(["*dag-import: broken.py*", "*Dag file import check failed*"])
