"""Provide a function-scoped factory for isolated persisted Airflow Dags.

References:
    https://docs.pytest.org/en/stable/how-to/fixtures.html
    https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html
"""

from __future__ import annotations

import hashlib
import os
import re
from contextlib import AbstractContextManager
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from pytest_airflow_in_a_box._compat.dag import (
    DagCleanupError,
    DagPersistenceRecord,
    build_dag,
    cleanup_dag,
    ensure_dag_absent,
    open_dag_session,
    persist_dag,
)
from pytest_airflow_in_a_box.markers import read_bool_marker
from pytest_airflow_in_a_box.types import DagMaker, SerializedDag

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import TracebackType

    from airflow.sdk import DAG
    from sqlalchemy.orm import Session

DAG_ID_MAX_LENGTH = 250
DAG_ID_PATTERN = re.compile(r"^[\w.-]+$")
DAG_ID_SANITIZER = re.compile(r"[^\w.-]+")
BUNDLE_PREFIX = "pytest-airflow-in-a-box"


def _validate_dag_id(dag_id: str) -> str:
    """Validate one explicit identifier before constructing Airflow objects.

    Parameters:
        dag_id: str containing the explicit identifier.

    Returns:
        str containing the validated identifier.

    Raises:
        TypeError: ``dag_id`` is not a string.
        ValueError: ``dag_id`` is empty, too long, or contains invalid characters.
    """

    if not isinstance(dag_id, str):
        raise TypeError(f"`dag_id` must be a string: '{dag_id}'")
    if not dag_id:
        raise ValueError("`dag_id` must be non-empty")
    if len(dag_id) > DAG_ID_MAX_LENGTH:
        raise ValueError(f"`dag_id` must not exceed {DAG_ID_MAX_LENGTH} characters: '{dag_id}'")
    if DAG_ID_PATTERN.fullmatch(dag_id) is None:
        raise ValueError(f"`dag_id` contains invalid characters: '{dag_id}'")
    return dag_id


def _default_dag_id(nodeid: str, worker: str, invocation: int) -> str:
    """Derive a bounded deterministic identifier from test and worker identity.

    Parameters:
        nodeid: str containing pytest's stable test identifier.
        worker: str containing the xdist worker identity or ``master``.
        invocation: int distinguishing repeated factory calls in one test.

    Returns:
        str containing a sanitized identifier no longer than Airflow's limit.
    """

    source = f"{nodeid}-{worker}-{invocation}"
    sanitized = DAG_ID_SANITIZER.sub("_", source).strip("._-") or "dag"
    digest = hashlib.sha256(source.encode()).hexdigest()[:16]
    prefix_length = DAG_ID_MAX_LENGTH - len(digest) - 1
    return f"{sanitized[:prefix_length]}-{digest}"


def _bundle_name(dag_id: str) -> str:
    """Return a bounded isolated bundle identity for one Dag.

    Parameters:
        dag_id: str containing the validated Dag identifier.

    Returns:
        str containing the fixture-owned bundle name.
    """

    digest = hashlib.sha256(dag_id.encode()).hexdigest()
    return f"{BUNDLE_PREFIX}-{digest}"


class _DagContext(AbstractContextManager["DAG"]):
    """Own one Dag authoring context and metadata session."""

    def __init__(
        self,
        factory: _DagFactory,
        dag: DAG,
        dag_id: str,
        bundle_name: str,
        *,
        serialized: bool,
    ) -> None:
        """Store deferred resources for one context entry.

        Parameters:
            factory: _DagFactory receiving current state and successful records.
            dag: airflow.sdk.DAG containing the mutable authoring object.
            dag_id: str identifying the Dag.
            bundle_name: str identifying the isolated metadata bundle.
            serialized: bool indicating whether to expose the scheduler representation.
        """

        self._factory = factory
        self._dag = dag
        self._dag_id = dag_id
        self._bundle_name = bundle_name
        self._serialized = serialized
        self._record: DagPersistenceRecord | None = None

    def __enter__(self) -> DAG:
        """Open metadata ownership checks, then enter the SDK Dag context.

        Returns:
            airflow.sdk.DAG containing the mutable authoring object.

        Raises:
            RuntimeError: The context manager has already been entered.
        """

        if self._record is not None:
            raise RuntimeError("Dag context cannot be entered more than once")
        session = open_dag_session(self._dag_id)
        try:
            ensure_dag_absent(self._dag_id, session)
        except Exception:
            session.close()
            raise
        self._record = DagPersistenceRecord(
            dag_id=self._dag_id,
            bundle_name=self._bundle_name,
            session=session,
        )
        self._factory._set_active(self._dag, session)
        return self._dag.__enter__()

    def __exit__(
        self,
        error_type: type[BaseException] | None,
        error: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Persist a successful Dag context or discard a failed construction.

        Parameters:
            error_type: type[BaseException] | None raised by the context body.
            error: BaseException | None raised by the context body.
            traceback: types.TracebackType | None associated with the error.
        """

        record = self._record
        if record is None:
            raise RuntimeError("Dag context was exited before it was entered")
        try:
            self._dag.__exit__(error_type, error, traceback)
            if error_type is not None:
                return
            serialized_dag = persist_dag(self._dag, record)
            self._factory._finish(record, serialized_dag if self._serialized else None)
            self._record = None
        finally:
            if self._record is not None:
                self._record.session.rollback()
                self._record.session.close()
                self._record = None


class _DagFactory:
    """Implement the public ``DagMaker`` protocol without importing Airflow eagerly."""

    def __init__(self, nodeid: str, fileloc: str, worker: str, *, serialized: bool) -> None:
        """Store deterministic identity inputs and marker defaults.

        Parameters:
            nodeid: str containing pytest's stable test identifier.
            fileloc: str naming the consumer test module.
            worker: str containing the xdist worker identity.
            serialized: bool containing the marker-derived default.
        """

        self._nodeid = nodeid
        self._fileloc = fileloc
        self._worker = worker
        self._default_serialized = serialized
        self._invocations = 0
        self._dag: DAG | None = None
        self._session: Session | None = None
        self._serialized_dag: SerializedDag | None = None
        self._records: list[DagPersistenceRecord] = []

    @property
    def dag(self) -> DAG:
        """Return the latest mutable SDK Dag.

        Returns:
            airflow.sdk.DAG containing the latest authoring object.

        Raises:
            RuntimeError: The factory has not been called.
        """

        if self._dag is None:
            raise RuntimeError("`dag_maker` has not created a Dag")
        return self._dag

    @property
    def session(self) -> Session:
        """Return the latest metadata session.

        Returns:
            sqlalchemy.orm.Session used by the latest entered context.

        Raises:
            RuntimeError: No Dag context has been entered.
        """

        if self._session is None:
            raise RuntimeError("`dag_maker` has not entered a Dag context")
        return self._session

    @property
    def serialized_dag(self) -> SerializedDag | None:
        """Return the requested persisted scheduler Dag.

        Returns:
            pytest_airflow_in_a_box.types.SerializedDag | None for the latest persisted Dag.
        """

        return self._serialized_dag

    def __call__(
        self,
        dag_id: str | None = None,
        *,
        serialized: bool | None = None,
        **dag_kwargs: Any,
    ) -> AbstractContextManager[DAG]:
        """Create one isolated Dag authoring context.

        Parameters:
            dag_id: str | None containing an explicit identifier or ``None`` for a derived one.
            serialized: bool | None overriding the marker-derived behavior.
            dag_kwargs: Any forwarded to ``airflow.sdk.DAG``.

        Returns:
            contextlib.AbstractContextManager[airflow.sdk.DAG] for task definition.

        Raises:
            TypeError: ``serialized`` is not a boolean or ``None``.
            ValueError: An explicit ``dag_id`` is invalid.
        """

        if serialized is not None and not isinstance(serialized, bool):
            raise TypeError(f"`serialized` must be a boolean or `None`: '{serialized}'")
        self._invocations += 1
        resolved_dag_id = (
            _default_dag_id(self._nodeid, self._worker, self._invocations)
            if dag_id is None
            else _validate_dag_id(dag_id)
        )
        dag = build_dag(resolved_dag_id, self._fileloc, dag_kwargs)
        self._dag = dag
        self._serialized_dag = None
        return _DagContext(
            self,
            dag,
            resolved_dag_id,
            _bundle_name(resolved_dag_id),
            serialized=self._default_serialized if serialized is None else serialized,
        )

    def _set_active(self, dag: DAG, session: Session) -> None:
        """Expose resources for the active context.

        Parameters:
            dag: airflow.sdk.DAG containing the active authoring object.
            session: sqlalchemy.orm.Session opened for its metadata.
        """

        self._dag = dag
        self._session = session

    def _finish(
        self,
        record: DagPersistenceRecord,
        serialized_dag: SerializedDag | None,
    ) -> None:
        """Track one successful context for fixture finalization.

        Parameters:
            record: DagPersistenceRecord containing owned metadata.
            serialized_dag: SerializedDag | None exposed by requested semantics.
        """

        self._records.append(record)
        self._serialized_dag = serialized_dag

    def close(self) -> None:
        """Clean every successfully persisted Dag in reverse creation order.

        Raises:
            DagCleanupError: One or more owned Dags could not be cleaned.
        """

        failures: list[Exception] = []
        while self._records:
            try:
                cleanup_dag(self._records.pop())
            except Exception as error:
                failures.append(error)
        if failures:
            details = "; ".join(str(error) for error in failures)
            raise DagCleanupError(
                f"Could not clean {len(failures)} fixture-owned Airflow Dags: {details}"
            ) from failures[0]


@pytest.fixture
def dag_maker(request: pytest.FixtureRequest) -> Iterator[DagMaker]:
    """Yield a function-scoped factory for isolated persisted Airflow Dags.

    Parameters:
        request: pytest.FixtureRequest containing marker and test identity metadata.

    Yields:
        pytest_airflow_in_a_box.types.DagMaker creating SDK Dag contexts.
    """

    marker_default = read_bool_marker(request.node, "need_serialized_dag", default=False)
    worker = os.environ.get("PYTEST_XDIST_WORKER", "master")
    fileloc = str(Path(str(request.node.path)).resolve())
    factory = _DagFactory(request.node.nodeid, fileloc, worker, serialized=marker_default)
    try:
        yield factory
    finally:
        factory.close()


__all__ = ("dag_maker",)
