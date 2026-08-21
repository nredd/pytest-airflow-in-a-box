"""Provider-shaped custom executors.

Stable at `airflow.executors.base_executor` on both certified families, unlike the Hook/
Operator/Sensor bases in the sibling corpus files, so no `_resolve` dual-family lookup is
needed. `_process_workloads` is a 3.x-only method on the real `BaseExecutor`; defining it
here is harmless on 2.x, which simply never calls it.

`ExampleExecutor` is inert and exists for the conformance and sandbox-registration
tests. `SerialExecutor` actually runs what it is handed, and is what the
executor-driven `run_dag` tests drive a real DagRun through. It is also the worked
example in `docs/guide/custom-components.md`: Airflow 3 deleted `SequentialExecutor`
from core, so "I want tasks to run one at a time" is a genuine reason to write one.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow.executors.base_executor import BaseExecutor

if TYPE_CHECKING:
    from collections.abc import Sequence


class ExampleExecutor(BaseExecutor):
    """Minimal conformant executor for the compatibility corpus."""

    def sync(self) -> None:
        """Report nothing; the corpus never actually runs this executor."""

    def _process_workloads(self, workload_items: Sequence[Any]) -> None:
        del workload_items

    def end(self) -> None:
        """Report nothing; this executor never has work in flight."""

    def terminate(self) -> None:
        """Report nothing; this executor never has work in flight."""


def _run_workload(workload: Any) -> None:
    """Run one workload to completion in the calling process.

    Airflow 3.3 collected this behind `BaseExecutor.run_workload`, which also picks the
    right supervisor for the workload type and resolves the Task Execution API URL from
    configuration. 3.1 and 3.2 have no such helper and go straight to
    `supervisor.supervise`, a name 3.3 keeps but deprecates -- hence preferring the
    classmethod whenever it exists rather than branching on a version number.

    Parameters:
        workload: Any containing the `workloads.ExecuteTask` handed over by the executor.
    """

    runner = getattr(BaseExecutor, "run_workload", None)
    if runner is not None:
        runner(workload)
        return

    from airflow.configuration import conf
    from airflow.sdk.execution_time.supervisor import supervise

    base_url = conf.get("api", "base_url", fallback="/")
    if base_url.startswith("/"):
        base_url = f"http://localhost:8080{base_url}"
    supervise(
        ti=workload.ti,
        dag_rel_path=workload.dag_rel_path,
        bundle_info=workload.bundle_info,
        token=workload.token,
        server=conf.get(
            "core",
            "execution_api_server_url",
            fallback=f"{base_url.rstrip('/')}/execution/",
        ),
        log_path=workload.log_path,
    )


class SerialExecutor(BaseExecutor):
    """Run one workload at a time, to completion, in the calling process.

    The executor Airflow 3 no longer ships. Every workload runs inline in
    `_process_workloads`, so by the time the scheduler's next `sync` lands there is
    nothing asynchronous left to reconcile and the outcome is already in the event
    buffer.

    Sets no attribute from `check_component`'s stale-attribute table -- notably not
    `is_single_threaded`, which `BaseExecutor` stopped reading -- so it passes
    `check_component(SerialExecutor, kind=ComponentKind.EXECUTOR)` clean.
    """

    is_local = True

    def sync(self) -> None:
        """Report nothing: `_process_workloads` already settled every workload."""

    def _process_workloads(self, workload_items: Sequence[Any]) -> None:
        """Run each queued workload inline and record its outcome.

        Parameters:
            workload_items: Sequence[Any] containing the workloads to run.
        """

        for workload in workload_items:
            # `workload.ti.key`, not `workload.key`: `ExecuteTask` grew a `key` property
            # after 3.1, but the underlying task-instance key is there on every certified
            # release, and it is what `BaseExecutor.queue_workload` keys `queued_tasks` on.
            key = workload.ti.key
            self.queued_tasks.pop(key, None)
            try:
                _run_workload(workload)
            except Exception as error:
                self.fail(key, error)
            else:
                self.success(key)

    def end(self) -> None:
        """Report nothing: no workload is ever left in flight to wait for."""

    def terminate(self) -> None:
        """Report nothing: no workload is ever left in flight to kill."""
