"""Scope the end-user consumer contract to the installed Airflow family.

The four modules listed below exercise genuinely 3.x-only surfaces at module
scope -- Asset ORM persistence (`airflow.models.asset`), the FastAPI `/api/v2`
server, and structlog task-log capture -- with no 2.x equivalent, so on the 2.x
family they cannot even be collected; `collect_ignore` keeps the shared
remainder of the suite runnable on both families. Every other end-user module,
including the whole-DagRun `dag_maker.run()` contract in
`test_dag_run_result.py`, authors dynamically through the shared
`tests/enduser/_authoring.py` resolver and marks its genuinely 3.x-only tests
(the Task SDK's DB-free `run_task` runner) `requires_airflow3` instead, so
those tests are collected and skipped on 2.x rather than never seen at all. The
2.x-native authoring contract lives in `test_airflow2_authoring.py`, gated by
the `requires_airflow2` marker.

References:
    https://docs.pytest.org/en/stable/example/pythoncollection.html#customizing-test-collection
"""

from __future__ import annotations

from pytest_airflow_in_a_box._compat.capabilities import AirflowFamily, installed_family

if installed_family() is AirflowFamily.V2:
    collect_ignore = [
        "test_assets.py",
        "test_cookbook_digest.py",
        "test_rest_api_compat.py",
        "test_structlog_events.py",
    ]
