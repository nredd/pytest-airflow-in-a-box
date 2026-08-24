"""Provide a session-scoped, fan-out-eligible Dag corpus for repository-defined checks.

References:
    https://docs.pytest.org/en/stable/how-to/fixtures.html
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

import pytest

if TYPE_CHECKING:
    from pytest_airflow_in_a_box.dagcorpus import DagCorpus

DAG_CORPUS_FIXTURE_NAME: Final[str] = "dag_corpus"
DAG_CORPUS_XDIST_GROUP: Final[str] = "pytest-airflow-in-a-box::dag-corpus"


@pytest.fixture(scope="session")
def dag_corpus(request: pytest.FixtureRequest, pytestconfig: pytest.Config) -> DagCorpus:
    """Return the portable, fan-out-eligible Dag corpus, once per worker process.

    Backed by the exact same builder and cross-process cache the bundled smoke catalog
    uses (`dagcorpus.get_dag_corpus`), so a repository-defined whole-corpus check (e.g.
    "every Dag must carry an `owner` tag") reuses fan-out and cross-worker sharing for
    free instead of parsing the Dag folder itself. Unlike `dag_bag`, this fixture never
    calls `ensure_database` unconditionally: the corpus builder already initializes the
    metadata database only when parse-time secrets resolution is active
    (`parse_secrets.parse_time_comms`), matching the smoke-only path's existing cost
    profile exactly -- forcing it here would make a `dag_corpus`-only run needlessly more
    expensive than the equivalent smoke-only run.

    Prefer `dag_bag` instead when a test needs live Airflow objects (executing tasks,
    inspecting a real `DAG`/operator instance) -- `DagCorpus` is read-only portable
    metadata, not something that crosses back into the live Airflow objects `dag_bag`
    hands back.

    Under `--dist loadgroup`, `plugin.py` groups every surviving `dag_corpus` consumer
    plus the bundled smoke catalog onto one xdist worker (`DAG_CORPUS_XDIST_GROUP`), so
    the underlying `get_dag_corpus` cross-worker cache is populated once and reused
    rather than rebuilt per worker.

    Parameters:
        request: pytest.FixtureRequest used to reach the session-scoped corpus cache.
        pytestconfig: pytest.Config containing plugin options and bootstrap state.

    Returns:
        pytest_airflow_in_a_box.dagcorpus.DagCorpus containing every parsed Dag's
        portable metadata.
    """

    # Deferred: `dagcorpus.py` imports `fixtures.dagbag` at module level, which runs
    # this package's `__init__.py` (Python always initializes a parent package before a
    # submodule), which imports this very module -- a module-level import here would
    # cycle back into a still-initializing `dagcorpus.py`. Mirrors
    # `fixtures/dagbag.py::_cached_dag_bag`'s own deferred import of smoke internals,
    # for the same reason.
    from pytest_airflow_in_a_box.dagcorpus import get_dag_corpus

    return get_dag_corpus(request.session, pytestconfig)


__all__ = ("dag_corpus",)
