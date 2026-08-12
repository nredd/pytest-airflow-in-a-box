"""Inert snapshots of one fully executed DagRun for compact bulk assertions.

``DagRunResult`` and ``TaskResult`` are plain data holders constructed by
``execute_dag_run`` after every task instance has settled. They import no Airflow
code, so a snapshot stays safe to ``repr`` and compare after fixtures close.

References:
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.hookspec.pytest_assertrepr_compare
    https://docs.dagster.io/api/dagster/execution
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

LOGGER = logging.getLogger(__name__)

DETAIL_MAX_LENGTH = 60


def task_key(task_id: str, map_index: int) -> str:
    """Render the snapshot key for one task instance identity.

    Parameters:
        task_id: str identifying the task.
        map_index: int identifying the mapped instance, or ``-1`` when unmapped.

    Returns:
        str containing ``task_id`` or ``task_id[map_index]`` for mapped instances.
    """

    return task_id if map_index < 0 else f"{task_id}[{map_index}]"


def _state_label(state: Any) -> str:
    """Render one task or DagRun state for display.

    Parameters:
        state: Any containing an Airflow state enum, a plain string, or ``None``.

    Returns:
        str containing the state's enum value, or ``"None"`` for an unrun instance.
    """

    return str(getattr(state, "value", state))


def _truncate(detail: str) -> str:
    """Bound one repr detail so failing-assert output stays scannable.

    Parameters:
        detail: str containing an xcom repr or error message.

    Returns:
        str containing at most ``DETAIL_MAX_LENGTH`` characters plus a marker.
    """

    if len(detail) <= DETAIL_MAX_LENGTH:
        return detail
    return f"{detail[:DETAIL_MAX_LENGTH]}..."


@dataclass(frozen=True)
class TaskResult:
    """Inert snapshot of one executed or settled task instance.

    Parameters:
        task_id: str identifying the task.
        map_index: int identifying the mapped instance, or ``-1`` when unmapped.
        state: Any containing the settled ``TaskInstanceState``, or ``None`` when unrun.
        xcom: Any containing the pulled ``return_value`` XCom, or ``None``.
        error: BaseException | None raised by the task body, when captured.
        ti: Any containing the refreshed ORM task instance for escape-hatch access.
    """

    task_id: str
    map_index: int
    state: Any
    xcom: Any
    error: BaseException | None
    ti: Any

    @property
    def key(self) -> str:
        """Return the snapshot key addressing this instance.

        Returns:
            str containing ``task_id`` or ``task_id[map_index]`` for mapped instances.
        """

        return task_key(self.task_id, self.map_index)

    def __repr__(self) -> str:
        """Render one compact single-line description for assertion output.

        Returns:
            str naming the key, state, and the error or xcom detail when present.
        """

        detail = ""
        if self.error is not None:
            message = _truncate(str(self.error))
            detail = f", error={type(self.error).__name__}({message!r})"
        elif self.xcom is not None:
            detail = f", xcom={_truncate(repr(self.xcom))}"
        return f"TaskResult('{self.key}', state={_state_label(self.state)}{detail})"


@dataclass(frozen=True, eq=False)
class DagRunResult:
    """Inert snapshot of one fully executed DagRun.

    Equality against a ``Mapping`` compares per-task outcomes: the mapping's keys
    must cover exactly the snapshot's task keys, and each value is compared against
    the corresponding ``TaskResult`` (see ``pytest_airflow_in_a_box.matchers``).

    Parameters:
        dag_run: Any containing the settled ORM DagRun for escape-hatch access.
        dag_id: str identifying the executed Dag, safe after the session closes.
        run_id: str identifying the executed DagRun, safe after the session closes.
        state: Any containing the settled ``DagRunState``.
        success: bool reporting whether the DagRun settled as ``success``.
        tasks: tuple[TaskResult, ...] in graph order, then mapped-index order.
        executed: tuple[str, ...] of task keys in actual execution order.
    """

    dag_run: Any
    dag_id: str
    run_id: str
    state: Any
    success: bool
    tasks: tuple[TaskResult, ...]
    executed: tuple[str, ...]

    @property
    def order(self) -> list[str]:
        """Return the task keys in the order their bodies actually executed.

        Instances that never ran (skipped by a branch, blocked by a failed
        upstream, or awaiting a deferred upstream) are absent.

        Returns:
            list[str] containing executed task keys in execution order.
        """

        return list(self.executed)

    @property
    def states(self) -> dict[str, Any]:
        """Return every settled task-instance state keyed by task key.

        Returns:
            dict[str, Any] mapping task keys to ``TaskInstanceState`` values or ``None``.
        """

        return {result.key: result.state for result in self.tasks}

    @property
    def xcoms(self) -> dict[str, Any]:
        """Return pulled ``return_value`` XComs keyed by plain task id.

        Unmapped tasks map to their value. Mapped tasks map to a list in
        mapped-index order. Tasks whose pulled value is ``None`` are absent.

        Returns:
            dict[str, Any] mapping task ids to values or mapped-index-ordered lists.
        """

        grouped: dict[str, list[TaskResult]] = {}
        for result in self.tasks:
            grouped.setdefault(result.task_id, []).append(result)
        xcoms: dict[str, Any] = {}
        for task_id, group in grouped.items():
            if len(group) == 1 and group[0].map_index < 0:
                if group[0].xcom is not None:
                    xcoms[task_id] = group[0].xcom
                continue
            values = [result.xcom for result in sorted(group, key=lambda r: r.map_index)]
            if any(value is not None for value in values):
                xcoms[task_id] = values
        return xcoms

    @property
    def errors(self) -> dict[str, BaseException]:
        """Return every captured task-body exception keyed by task key.

        Returns:
            dict[str, BaseException] mapping failed task keys to their exceptions.
        """

        return {result.key: result.error for result in self.tasks if result.error is not None}

    @property
    def tis(self) -> tuple[Any, ...]:
        """Return the refreshed ORM task instances in graph order.

        Returns:
            tuple[Any, ...] containing each snapshot's task instance.
        """

        return tuple(result.ti for result in self.tasks)

    def __getitem__(self, key: str | tuple[str, int]) -> TaskResult:
        """Return one per-task snapshot by task id or ``(task_id, map_index)``.

        Parameters:
            key: str | tuple[str, int] addressing one unmapped task or mapped instance.

        Returns:
            TaskResult for the addressed instance.

        Raises:
            KeyError: The key is absent, or a bare task id addresses a mapped task.
        """

        if isinstance(key, tuple):
            requested_id, requested_index = key
            for result in self.tasks:
                if result.task_id == requested_id and result.map_index == requested_index:
                    return result
            available = [result.key for result in self.tasks]
            raise KeyError(
                f"Task instance '{requested_id}' with map index '{requested_index}' is absent; "
                f"available keys: {available}"
            )
        matches = [result for result in self.tasks if result.task_id == key]
        if not matches:
            available = [result.key for result in self.tasks]
            raise KeyError(f"Task '{key}' is absent; available keys: {available}")
        if len(matches) == 1 and matches[0].map_index < 0:
            return matches[0]
        expanded = [result.key for result in matches]
        raise KeyError(f"Task '{key}' is mapped; address one instance: {expanded}")

    def __eq__(self, other: object) -> bool:
        """Compare per-task outcomes against a mapping of expected outcomes.

        Parameters:
            other: object containing a ``Mapping`` from task keys to expected outcomes.

        Returns:
            bool reporting whether the mapping covers exactly this snapshot's task
            keys and every expected outcome matches its ``TaskResult``.
        """

        if not isinstance(other, Mapping):
            return NotImplemented
        actual = {result.key: result for result in self.tasks}
        if set(other) != set(actual):
            return False
        return all(other[key] == actual[key] for key in other)

    # Defining `__eq__` sets `__hash__` to `None`; two snapshots only ever compare
    # equal by identity, so the identity hash stays consistent with equality.
    __hash__ = object.__hash__

    def __repr__(self) -> str:
        """Render one aligned per-task table for assertion output.

        Returns:
            str containing a header line plus one row per task instance.
        """

        header = (
            f"<DagRunResult '{self.dag_id}' run_id='{self.run_id}' "
            f"state={_state_label(self.state)}>"
        )
        if not self.tasks:
            return header
        key_width = max(len(result.key) for result in self.tasks)
        state_width = max(len(_state_label(result.state)) for result in self.tasks)
        rows = [header]
        for result in self.tasks:
            detail = ""
            if result.error is not None:
                message = _truncate(str(result.error))
                detail = f"  error={type(result.error).__name__}({message!r})"
            elif result.xcom is not None:
                detail = f"  xcom={_truncate(repr(result.xcom))}"
            state_label = _state_label(result.state)
            rows.append(f"  {result.key:<{key_width}}  {state_label:<{state_width}}{detail}")
        return "\n".join(rows)


def assertrepr_compare(op: str, left: object, right: object) -> list[str] | None:
    """Render a per-task diff for one failed ``DagRunResult == Mapping`` assertion.

    Parameters:
        op: str containing the comparison operator pytest evaluated.
        left: object containing one side of the failed comparison.
        right: object containing the other side of the failed comparison.

    Returns:
        list[str] | None containing explanation lines, or ``None`` when the
        comparison does not involve a ``DagRunResult`` and a ``Mapping``.
    """

    if op != "==":
        return None
    if isinstance(left, DagRunResult) and isinstance(right, Mapping):
        result, expected = left, right
    elif isinstance(right, DagRunResult) and isinstance(left, Mapping):
        result, expected = right, left
    else:
        return None

    actual = {task_result.key: task_result for task_result in result.tasks}
    lines = ["DagRunResult does not match the expected task outcomes:"]
    for key in sorted(set(actual) | set(expected)):
        if key not in expected:
            lines.append(f"  {key}: not in the expected mapping; got {actual[key]!r}")
        elif key not in actual:
            lines.append(f"  {key}: absent from the DagRun; expected {expected[key]!r}")
        elif expected[key] == actual[key]:
            lines.append(f"  {key}: matches {expected[key]!r}")
        else:
            lines.append(f"  {key}: expected {expected[key]!r}, got {actual[key]!r}")
    return lines


__all__ = (
    "DagRunResult",
    "TaskResult",
    "assertrepr_compare",
    "task_key",
)
