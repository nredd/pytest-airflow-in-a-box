"""Test the session-scoped `airflow_configure` fixture.

Restoration is observed from an inner `pytest_unconfigure`, which pytest dispatches after every
session-scoped fixture has been finalized. The ini option's restore runs later still, from the
config cleanup stack, which is why `tests/test_ini_config.py` tests that one as a unit instead.

References:
    https://github.com/nredd/pytest-airflow-in-a-box/issues/202
    https://docs.pytest.org/en/stable/how-to/fixtures.html
"""

from __future__ import annotations

import pytest

TIMEOUT_NAME = "AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT"


def test_overrides_apply_for_the_whole_session(pytester: pytest.Pytester) -> None:
    """Hold an override applied from a session fixture across every later test."""

    pytester.makeconftest(
        """
        import pytest


        @pytest.fixture(scope="session", autouse=True)
        def _repo_defaults(airflow_configure):
            airflow_configure({("core", "dagbag_import_timeout"): "12.5"})
        """
    )
    pytester.makepyfile(
        """
        from airflow.configuration import conf


        def test_first():
            assert conf.get("core", "dagbag_import_timeout") == "12.5"


        def test_second():
            assert conf.get("core", "dagbag_import_timeout") == "12.5"
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=2)


def test_an_autouse_session_wrapper_precedes_dag_bag(pytester: pytest.Pytester) -> None:
    """Apply a consumer's autouse session overrides before the one-per-process Dag parse.

    The Dag file encodes the parsed option in its `dag_id`, so a passing assertion proves the
    override was live during the parse rather than merely by the time the test body ran.
    """

    pytester.makefile(
        ".ini",
        pytest="""
        [pytest]
        airflow_dags_folder = dags
        """,
    )
    dags = pytester.mkdir("dags")
    (dags / "probe.py").write_text(
        "from airflow.configuration import conf\n"
        "from airflow.sdk import DAG\n"
        "\n"
        "timeout = conf.get('core', 'dagbag_import_timeout').replace('.', '_')\n"
        "probe = DAG(dag_id=f'probe_{timeout}')\n",
        encoding="utf-8",
    )
    pytester.makeconftest(
        """
        import pytest


        @pytest.fixture(scope="session", autouse=True)
        def _repo_defaults(airflow_configure):
            airflow_configure({("core", "dagbag_import_timeout"): "12.5"})
        """
    )
    pytester.makepyfile(
        """
        def test_override_was_live_during_the_parse(dag_bag):
            assert not dag_bag.import_errors
            assert "probe_12_5" in dag_bag.dags
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)


def test_batches_unwind_last_in_first_out(pytester: pytest.Pytester) -> None:
    """Let a later batch win, exactly as nested `airflow_config` contexts do."""

    pytester.makeconftest(
        """
        import pytest


        @pytest.fixture(scope="session", autouse=True)
        def _repo_defaults(airflow_configure):
            airflow_configure({("core", "dagbag_import_timeout"): "12.5"})
            airflow_configure({("core", "dagbag_import_timeout"): "34.5"})
        """
    )
    pytester.makepyfile(
        """
        from airflow.configuration import conf


        def test_last_batch_wins():
            assert conf.get("core", "dagbag_import_timeout") == "34.5"
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)


def test_plain_environment_names_and_absence_are_supported(pytester: pytest.Pytester) -> None:
    """Pass `env` and a `None` value straight through to `airflow_config`."""

    pytester.makeconftest(
        """
        import pytest


        @pytest.fixture(scope="session", autouse=True)
        def _repo_defaults(airflow_configure):
            airflow_configure(
                {("core", "dagbag_import_timeout"): None},
                env={"PYTEST_AIRFLOW_IN_A_BOX_PROBE": "set"},
            )
        """
    )
    pytester.makepyfile(
        """
        import os


        def test_env_and_absence():
            assert os.environ["PYTEST_AIRFLOW_IN_A_BOX_PROBE"] == "set"
            assert "AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT" not in os.environ
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)


def test_a_malformed_batch_fails_the_requesting_test(pytester: pytest.Pytester) -> None:
    """Surface `airflow_config`'s validation unchanged rather than swallowing it."""

    pytester.makepyfile(
        """
        import pytest


        def test_malformed_overrides(airflow_configure):
            with pytest.raises(pytest.UsageError, match="keys must be"):
                airflow_configure({"core.dagbag_import_timeout": "12.5"})
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)


def test_overrides_are_restored_at_session_teardown(pytester: pytest.Pytester) -> None:
    """Unwind the session stack before pytest shuts the configuration down."""

    sentinel = pytester.path / "restored"
    pytester.makeconftest(
        f"""
        import os
        from pathlib import Path

        import pytest

        SENTINEL = Path({str(sentinel)!r})


        @pytest.fixture(scope="session", autouse=True)
        def _repo_defaults(airflow_configure):
            airflow_configure({{("core", "dagbag_import_timeout"): "12.5"}})


        def pytest_unconfigure(config):
            SENTINEL.write_text(
                os.environ.get("{TIMEOUT_NAME}", "<absent>"), encoding="utf-8"
            )
        """
    )
    pytester.makepyfile(
        """
        import os


        def test_override_is_live():
            assert os.environ["AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT"] == "12.5"
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)
    assert sentinel.read_text(encoding="utf-8") == "<absent>"
