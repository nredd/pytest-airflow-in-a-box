"""Provider-shaped custom executor.

Stable at `airflow.executors.base_executor` on both certified families, unlike the Hook/
Operator/Sensor bases in the sibling corpus files, so no `_resolve` dual-family lookup is
needed. `_process_workloads` is a 3.x-only method on the real `BaseExecutor`; defining it
here is harmless on 2.x, which simply never calls it.
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
