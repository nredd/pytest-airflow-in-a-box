"""Exercise `run_dag` against a Dag pulled from a real `full_dag_bag` corpus."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.compat

CORPUS = Path(__file__).parents[1] / "dags"


def test_run_dag_executes_a_dag_pulled_from_full_dag_bag(pytester: pytest.Pytester) -> None:
    """Adopt a real corpus Dag through `full_dag_bag` and execute it end to end."""

    pytester.makepyfile(
        """
        def test_chained(full_dag_bag, run_dag):
            dag = full_dag_bag.dags["chained"]

            result = run_dag(dag)

            assert result.success
            assert result.dag_id == "chained"
            assert result.xcoms == {"produce": 21, "consume": 42}
            assert result.order == ["produce", "consume"]
        """
    )

    result = pytester.runpytest_subprocess("-q", f"--dag-folder={CORPUS}")

    result.assert_outcomes(passed=1)
