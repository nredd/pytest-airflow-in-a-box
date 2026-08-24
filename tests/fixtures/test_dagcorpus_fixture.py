"""Test the public, fan-out-eligible Dag corpus fixture.

References:
    https://docs.pytest.org/en/stable/how-to/fixtures.html
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from pytest_airflow_in_a_box import dagcorpus
from pytest_airflow_in_a_box.fixtures import dagcorpus as fixtures_dagcorpus


def test_dag_corpus_fixture_delegates_to_get_dag_corpus(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return whatever `get_dag_corpus` resolves, called with the session and config.

    `fixtures/dagcorpus.py::dag_corpus` imports `get_dag_corpus` locally, deferred to
    avoid a circular import through this package's `__init__.py` -- so the seam to
    patch is the core `dagcorpus` module's attribute, not this fixture module's. The raw
    function runs via `__wrapped__`: a `@pytest.fixture`-decorated callable now fails
    loudly if called directly (see `tests/compat/test_v2_gates.py` for the same idiom).
    """

    calls: list[tuple[object, object]] = []
    sentinel: Any = SimpleNamespace(dags={"a": object()})

    def _fake_get_dag_corpus(session: object, config: object) -> Any:
        calls.append((session, config))
        return sentinel

    monkeypatch.setattr(dagcorpus, "get_dag_corpus", _fake_get_dag_corpus)
    session: Any = SimpleNamespace()
    request: Any = SimpleNamespace(session=session)
    pytestconfig: Any = SimpleNamespace()

    fixture_module: Any = fixtures_dagcorpus
    raw = fixture_module.dag_corpus.__wrapped__
    result = raw(request, pytestconfig)

    assert result is sentinel
    assert calls == [(session, pytestconfig)]


def test_dag_corpus_fixture_does_not_touch_bootstrap_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Never resolve bootstrap state or the database from the fixture wrapper itself.

    Regression pin contrasting with `dag_bag`'s fixture, which calls
    `ensure_database(get_bootstrap_state(pytestconfig).root)` unconditionally before
    delegating (`fixtures/dagbag.py::dag_bag`). `dag_corpus`'s builder already
    initializes the database conditionally, deep inside `_build_dag_corpus`, only when
    parse-time secrets resolution needs it -- forcing it here would make a
    `dag_corpus`-only run pay the one-time migration cost even when nothing needs it. A
    `pytestconfig` double with no `.stash` attribute makes any accidental
    `get_bootstrap_state(pytestconfig)` call raise immediately, since that helper reads
    `config.stash[STATE_KEY]`.
    """

    sentinel: Any = SimpleNamespace(dags={})
    monkeypatch.setattr(dagcorpus, "get_dag_corpus", lambda _session, _config: sentinel)
    request: Any = SimpleNamespace(session=SimpleNamespace())
    pytestconfig: Any = SimpleNamespace()  # deliberately carries no `.stash`

    fixture_module: Any = fixtures_dagcorpus
    raw = fixture_module.dag_corpus.__wrapped__
    result = raw(request, pytestconfig)

    assert result is sentinel


def test_dag_corpus_fixture_exposes_parsed_dags_tags_and_import_errors(
    pytester: pytest.Pytester,
) -> None:
    """Expose parsed Dag metadata and retained import errors.

    The `dag_corpus.dags` membership assertion is the issue's literal acceptance example.
    """

    dag_folder = pytester.path / "dags"
    dag_folder.mkdir()
    (dag_folder / "sample.py").write_text(
        'from airflow.sdk import DAG\n\nsome_dag = DAG(dag_id="some_dag", tags=["team-a"])\n',
        encoding="utf-8",
    )
    (dag_folder / "broken.py").write_text('raise RuntimeError("nope")\n', encoding="utf-8")
    pytester.makepyfile(
        """
        def test_x(dag_corpus):
            assert "some_dag" in dag_corpus.dags
            dag = dag_corpus.dags["some_dag"]
            assert dag.tags == frozenset({"team-a"})
            assert dag.fileloc.endswith("sample.py")
            assert len(dag_corpus.import_errors) == 1
            assert next(iter(dag_corpus.import_errors)).endswith("broken.py")
        """
    )

    result = pytester.runpytest_subprocess("-q", "--dag-folder", str(dag_folder))

    result.assert_outcomes(passed=1)


def test_dag_corpus_fixture_is_shared_within_one_worker_process(
    pytester: pytest.Pytester,
) -> None:
    """Cache the corpus once per worker process across multiple consuming tests."""

    dag_folder = pytester.path / "dags"
    dag_folder.mkdir()
    (dag_folder / "sample.py").write_text(
        'from airflow.sdk import DAG\n\nsome_dag = DAG(dag_id="some_dag")\n', encoding="utf-8"
    )
    pytester.makepyfile(
        """
        seen = []

        def test_one(dag_corpus):
            seen.append(dag_corpus)

        def test_two(dag_corpus):
            seen.append(dag_corpus)
            assert seen[0] is seen[1]
        """
    )

    result = pytester.runpytest_subprocess("-q", "--dag-folder", str(dag_folder))

    result.assert_outcomes(passed=2)
