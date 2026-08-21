"""Provider-shaped custom priority weight strategy.

Import only inside V3-gated tests: `airflow.task.priority_strategy` is 3.x-only, unlike
the dual-family bases the sibling corpus files resolve.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.task.priority_strategy import PriorityWeightStrategy

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance


class ExampleWeightStrategy(PriorityWeightStrategy):
    """Weight every task instance identically, at a recognizable constant.

    Defines `__eq__` and `__hash__` as a pair: the base's own `__hash__` is unusable
    (`weight-strategy-hash-of-none`), and the strategy is stateless, so type identity
    is the whole value.
    """

    def get_weight(self, ti: TaskInstance) -> int:
        del ti
        return 42

    def __eq__(self, other: object) -> bool:
        return type(other) is type(self)

    def __hash__(self) -> int:
        return hash(type(self).__qualname__)
