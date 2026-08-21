"""Provider-shaped custom timetable.

Stable at `airflow.timetables.base` on both certified families, unlike the Hook/Operator/
Sensor bases in the sibling corpus files, so no `_resolve` dual-family lookup is needed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow.timetables.base import DataInterval, Timetable

if TYPE_CHECKING:
    from airflow.timetables.base import DagRunInfo, TimeRestriction
    from pendulum import DateTime


class ExampleTimetable(Timetable):
    """Schedule one run per fixed-length interval, in whole hours."""

    def __init__(self, hours: int = 1) -> None:
        self.hours = hours

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        return DataInterval(start=run_after.subtract(hours=self.hours), end=run_after)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        del last_automated_data_interval, restriction
        return None

    def serialize(self) -> dict[str, Any]:
        return {"hours": self.hours}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> ExampleTimetable:
        return cls(hours=data["hours"])
