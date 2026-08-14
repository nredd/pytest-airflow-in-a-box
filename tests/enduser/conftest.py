"""Scope the end-user consumer contract to the installed Airflow family.

The modules listed below import 3.x-only surfaces (`airflow.sdk` authoring, the
FastAPI `/api/v2` server, structlog task logs) at module scope, so on the 2.x family
they cannot even be collected; `collect_ignore` keeps the shared remainder of the
suite runnable on both families. The 2.x authoring contract lives in
`test_airflow2_authoring.py`, gated by the `requires_airflow2` marker.

References:
    https://docs.pytest.org/en/stable/example/pythoncollection.html#customizing-test-collection
"""

from __future__ import annotations

from pytest_airflow_in_a_box._compat.capabilities import AirflowFamily, installed_family

if installed_family() is AirflowFamily.V2:
    collect_ignore = [
        "test_assets.py",
        "test_callbacks.py",
        "test_dag_run_result.py",
        "test_hooks.py",
        "test_operators.py",
        "test_provider_package.py",
        "test_rest_api_compat.py",
        "test_sensors.py",
        "test_structlog_events.py",
        "test_taskflow.py",
        "test_triggers.py",
    ]
