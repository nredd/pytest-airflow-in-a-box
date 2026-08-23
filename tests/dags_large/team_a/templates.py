"""Non-Dag helper module `tests/dags_large/.airflowignore` excludes from discovery.

Reproduces issue #243's exact ancestor-`.airflowignore` correctness pitfall: this file
declares a Dag, so a subdirectory-rooted parse of `team_a/` alone -- which would never
see the root `.airflowignore` above it -- would wrongly include this as a real Dag.
`tests/test_dag_bag_fanout.py` asserts `should_be_ignored` never appears in a fanned-out
corpus, regardless of which shard would otherwise have processed this file.
"""

from __future__ import annotations

from airflow.sdk import dag


@dag(schedule=None)
def should_be_ignored() -> None:
    """Mark this Dag as never having been discovered, if the test ever sees it."""


should_be_ignored()
