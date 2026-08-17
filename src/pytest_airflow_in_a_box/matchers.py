"""Rich-equality matchers for bulk ``DagRunResult`` and rendered-operator assertions.

Each factory returns an object whose ``__eq__`` compares expectations against the
outcome under test, so an assertion reads as one expression:
``assert result == {"answer": succeeded(42)}`` or
``assert rendered(query="SELECT 42") == render_task(op)``. ``TaskOutcome`` follows
``pytest.approx`` and dirty-equals -- the right-hand side carries the comparison --
because it is always nested inside a plain ``dict``. ``RenderedFields`` instead goes
on the LEFT: Airflow's ``BaseOperator.__eq__`` returns ``False`` outright for a
foreign type instead of ``NotImplemented``, so Python never falls back to the
matcher's ``__eq__`` when the operator is the left operand.

References:
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.approx
    https://dirty-equals.helpmanual.io/latest/
"""

from __future__ import annotations

import logging
from typing import Any

LOGGER = logging.getLogger(__name__)

_FACTORY_NAMES = {
    "success": "succeeded",
    "failed": "failed",
    "skipped": "skipped",
    "deferred": "deferred",
    "upstream_failed": "upstream_failed",
    None: "not_run",
}
_MISSING = object()


class _AnyValue:
    """Sentinel matching every xcom value, including ``None``."""

    def __repr__(self) -> str:
        """Render the sentinel for matcher reprs.

        Returns:
            str containing the bare sentinel name.
        """

        return "ANY"


ANY = _AnyValue()


class TaskOutcome:
    """One expected task outcome carrying rich equality against ``TaskResult``."""

    def __init__(
        self,
        state: str | None,
        *,
        xcom: Any = ANY,
        error_type: type[BaseException] | None = None,
    ) -> None:
        """Store one expected outcome.

        Parameters:
            state: str | None containing the expected ``TaskInstanceState`` value,
                or ``None`` for an instance that never ran.
            xcom: Any containing the expected ``return_value`` XCom, or ``ANY``.
            error_type: type[BaseException] | None narrowing the captured exception.

        Raises:
            TypeError: ``state`` is not a string or ``None``, or ``error_type`` is
                not an exception type.
            ValueError: ``state`` is an empty string.
        """

        if state is not None and not isinstance(state, str):
            raise TypeError(f"`state` must be a string or `None`: '{state}'")
        if state == "":
            raise ValueError("`state` must be non-empty")
        if error_type is not None and not (
            isinstance(error_type, type) and issubclass(error_type, BaseException)
        ):
            raise TypeError(f"`error_type` must be an exception type: '{error_type}'")
        self._state = state
        self._xcom = xcom
        self._error_type = error_type

    def __eq__(self, other: object) -> bool:
        """Compare this expected outcome against one task-result-shaped object.

        Parameters:
            other: object exposing ``state``, ``xcom``, and ``error`` attributes.

        Returns:
            bool reporting whether the state, xcom, and error expectations hold.
        """

        state = getattr(other, "state", _MISSING)
        xcom = getattr(other, "xcom", _MISSING)
        error = getattr(other, "error", _MISSING)
        if _MISSING in (state, xcom, error):
            return NotImplemented
        if getattr(state, "value", state) != self._state:
            return False
        if not isinstance(self._xcom, _AnyValue) and xcom != self._xcom:
            return False
        return self._error_type is None or isinstance(error, self._error_type)

    def __repr__(self) -> str:
        """Render the factory call that would rebuild this expected outcome.

        Returns:
            str containing a factory-style call, or the class form for a custom state.
        """

        arguments = []
        if not isinstance(self._xcom, _AnyValue):
            arguments.append(repr(self._xcom))
        if self._error_type is not None:
            arguments.append(self._error_type.__name__)
        rendered = ", ".join(arguments)
        factory = _FACTORY_NAMES.get(self._state)
        if factory is None:
            return f"TaskOutcome('{self._state}'{', ' + rendered if rendered else ''})"
        return f"{factory}({rendered})"


class RenderedFields:
    """One expected set of rendered template-field values, checked against an operator.

    Must be the LEFT operand: ``assert rendered(...) == op``, not ``op == rendered(...)``.
    Airflow's ``BaseOperator.__eq__`` returns ``False`` for a foreign type instead of
    ``NotImplemented``, so putting the operator first never gives this class a chance to
    compare -- see the module docstring.
    """

    def __init__(self, **fields: Any) -> None:
        """Store expected field values.

        Parameters:
            fields: Any containing expected attribute values by field name.
                Pass ``ANY`` for a field whose value should not be checked.

        Raises:
            ValueError: No fields were supplied.
        """

        if not fields:
            raise ValueError("`rendered(...)` requires at least one field")
        self._fields = fields

    def __eq__(self, other: object) -> bool:
        """Compare expected field values against one rendered operator.

        Parameters:
            other: object exposing each expected field as an attribute.

        Returns:
            bool reporting whether every expected field value matches, or
            ``NotImplemented`` when `other` is missing an expected attribute.
        """

        values = {name: getattr(other, name, _MISSING) for name in self._fields}
        if _MISSING in values.values():
            return NotImplemented
        return all(
            isinstance(expected, _AnyValue) or values[name] == expected
            for name, expected in self._fields.items()
        )

    def __repr__(self) -> str:
        """Render the factory call that would rebuild this expectation.

        Returns:
            str containing a factory-style call.
        """

        rendered_args = ", ".join(f"{name}={value!r}" for name, value in self._fields.items())
        return f"rendered({rendered_args})"


def rendered(**fields: Any) -> RenderedFields:
    """Expect one operator whose template fields resolved to given values.

    Parameters:
        fields: Any containing expected attribute values by field name.

    Returns:
        RenderedFields comparing expected field values against a rendered operator.

    Raises:
        ValueError: No fields were supplied.
    """

    return RenderedFields(**fields)


def succeeded(xcom: Any = ANY) -> TaskOutcome:
    """Expect one task instance that settled ``success``.

    Parameters:
        xcom: Any containing the expected ``return_value`` XCom, or ``ANY``.

    Returns:
        TaskOutcome comparing state and xcom against a ``TaskResult``.
    """

    return TaskOutcome("success", xcom=xcom)


def failed(error_type: type[BaseException] | None = None) -> TaskOutcome:
    """Expect one task instance that settled ``failed``.

    Parameters:
        error_type: type[BaseException] | None narrowing the captured exception.

    Returns:
        TaskOutcome comparing state and captured error against a ``TaskResult``.
    """

    return TaskOutcome("failed", error_type=error_type)


def skipped() -> TaskOutcome:
    """Expect one task instance that settled ``skipped``.

    Returns:
        TaskOutcome comparing state against a ``TaskResult``.
    """

    return TaskOutcome("skipped")


def deferred() -> TaskOutcome:
    """Expect one task instance that settled ``deferred``.

    Returns:
        TaskOutcome comparing state against a ``TaskResult``.
    """

    return TaskOutcome("deferred")


def upstream_failed() -> TaskOutcome:
    """Expect one task instance that settled ``upstream_failed``.

    Returns:
        TaskOutcome comparing state against a ``TaskResult``.
    """

    return TaskOutcome("upstream_failed")


def not_run() -> TaskOutcome:
    """Expect one task instance that never ran, keeping its ``None`` state.

    Blocked downstreams of a still-deferred or retrying upstream settle this way.

    Returns:
        TaskOutcome comparing state against a ``TaskResult``.
    """

    return TaskOutcome(None)


__all__ = (
    "ANY",
    "RenderedFields",
    "TaskOutcome",
    "deferred",
    "failed",
    "not_run",
    "rendered",
    "skipped",
    "succeeded",
    "upstream_failed",
)
