"""Dogfood fixture for `action/action.yml`, exercised by the `action-smoke` CI job.

Mirrors the README quickstart verbatim, so this file doubles as a docs-accuracy check
against the published `pytest-airflow-in-a-box` package the action installs from PyPI.
"""

from __future__ import annotations

from airflow.sdk import task


def test_dag(dag_maker) -> None:
    with dag_maker():

        @task
        def produce():
            return 21

        @task
        def consume(value):
            return value * 2

        consume(produce())

    result = dag_maker.run()

    assert result.success
    assert result.xcoms == {"produce": 21, "consume": 42}
    assert result.order == ["produce", "consume"]
