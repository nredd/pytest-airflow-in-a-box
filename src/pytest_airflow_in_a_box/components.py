"""Static conformance checks for custom timetables, listeners, and executors.

`BaseExecutor` is not an ABC and `Timetable` is a `typing.Protocol`, so nothing about
either class -- nor a listener, which carries no base class at all -- is enforced at
class-creation time. A shape bug in any of the three ships silently and only fails once a
scheduler actually exercises it. `check_component` runs a battery of pure, additive
checks against a class or an already-built instance and reports every problem found; a
wrong or overly strict check can report a false problem, but it can never raise on the
component itself or break an otherwise-passing suite.

No Airflow bootstrap, metadata database, or cache is touched, and this module -- like its
private registry in `pytest_airflow_in_a_box._compat.components` -- never imports Airflow
at module scope, so it is safe to call from a plain unit test or a pre-commit hook.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/timetable.html
    https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/listeners.html
    https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/index.html
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from pytest_airflow_in_a_box._compat.components import (
    CHECK_REGISTRY,
    KIND_CLASSIFIERS,
    ComponentProblem,
    _as_type,
)


class ComponentKind(str, Enum):
    """Closed set of custom Airflow component kinds `check_component` understands.

    Values match `pytest_airflow_in_a_box._compat.components`'s bare-string registry
    keys exactly; `tests/test_components.py` pins the two from drifting apart.
    """

    TIMETABLE = "timetable"
    LISTENER = "listener"
    EXECUTOR = "executor"


class ComponentContractError(Exception):
    """Report that a checked component failed one or more conformance checks."""


@dataclass(frozen=True)
class ComponentReport:
    """Outcome of running every applicable conformance check against one component.

    Parameters:
        component_name: str naming the checked component's type, for messages.
        problems: tuple[ComponentProblem, ...] containing every problem found, in
            registry order.
    """

    component_name: str
    problems: tuple[ComponentProblem, ...]

    @property
    def ok(self) -> bool:
        """Report whether `check_component` found no problems."""

        return not self.problems

    def summary(self) -> str:
        """Render a human-readable summary, one problem per line.

        Returns:
            str containing one header line plus one `[code] message` line per problem,
            or a one-line clean report when `ok` is True.
        """

        if self.ok:
            return f"`{self.component_name}`: no problems found."
        lines = [f"`{self.component_name}`: {len(self.problems)} problem(s) found:"]
        lines.extend(f"  [{problem.code}] {problem.message}" for problem in self.problems)
        return "\n".join(lines)

    def raise_for_problems(self) -> None:
        """Raise `ComponentContractError` summarizing every problem found, if any.

        Raises:
            ComponentContractError: At least one problem was found.
        """

        if not self.ok:
            raise ComponentContractError(self.summary())


def check_component(component: object, *, kind: ComponentKind | None = None) -> ComponentReport:
    """Run every applicable static conformance check against one component.

    Accepts a bare class or an already-built instance interchangeably and never
    constructs one itself, so it is safe to call on a `Timetable`, listener, or
    `BaseExecutor` subclass whose constructor is not side-effect-free or takes required
    arguments. Checks are additive: each reports the problems it finds and never raises
    on the component itself, so a wrong or overly strict check cannot fail an
    otherwise-passing suite -- only `raise_for_problems()` (or asserting `.ok` yourself)
    turns a report into a test failure.

    Parameters:
        component: object containing the timetable, listener, or executor class or
            instance to check.
        kind: ComponentKind | None selecting which checks to run. None classifies
            `component` itself: by nominal `Timetable` inheritance, by carrying at least
            one `@hookimpl`-decorated method, or by `BaseExecutor` subclassing. Pass an
            explicit kind to force a check set regardless of how `component` classifies
            -- for example, a purely duck-typed listener that does not match any
            classifier on its own.

    Returns:
        ComponentReport containing every problem found, or a clean report when `kind` is
        None and `component` matches no known kind.
    """

    component_type = _as_type(component)
    if kind is not None:
        applicable_kinds = {kind.value}
    else:
        applicable_kinds = {
            kind_value
            for kind_value, classifier in KIND_CLASSIFIERS.items()
            if classifier(component)
        }
    problems = tuple(
        problem
        for kind_value, _check_name, checker in CHECK_REGISTRY
        if kind_value in applicable_kinds
        for problem in checker(component)
    )
    return ComponentReport(component_name=component_type.__name__, problems=problems)


__all__ = (
    "ComponentContractError",
    "ComponentKind",
    "ComponentProblem",
    "ComponentReport",
    "check_component",
)
