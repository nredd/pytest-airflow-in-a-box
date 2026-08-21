"""Public task-instance execution and ordering helpers.

`execute_dag_run` runs every task in this process; `execute_dag_run_via_executor` hands
each one to a real executor and its supervised task workers. Both return the same
`DagRunResult` and share one driver, so their ordering, mapped-expansion, and settling
semantics are identical by construction.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html
    https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html
"""

from __future__ import annotations

from pytest_airflow_in_a_box._compat.executor import (
    DEFAULT_EXECUTOR_TIMEOUT,
    ExecutorRunError,
    execute_dag_run_via_executor,
)
from pytest_airflow_in_a_box._compat.taskrun import (
    DEFAULT_TRIGGER_TIMEOUT,
    DagRunDriveError,
    TaskResolutionError,
    TriggerExecutionError,
    execute_dag_run,
    ordered_task_instances,
    run_task_instance,
    run_trigger,
)

__all__ = (
    "DEFAULT_EXECUTOR_TIMEOUT",
    "DEFAULT_TRIGGER_TIMEOUT",
    "DagRunDriveError",
    "ExecutorRunError",
    "TaskResolutionError",
    "TriggerExecutionError",
    "execute_dag_run",
    "execute_dag_run_via_executor",
    "ordered_task_instances",
    "run_task_instance",
    "run_trigger",
)
