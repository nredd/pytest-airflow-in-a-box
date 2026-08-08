"""Test the opt-in bundled smoke catalog: config guards, table rendering, end-to-end behavior."""

from __future__ import annotations

import logging
from datetime import timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from pytest_airflow_in_a_box import smoke


def _config(
    *,
    airflow_smoke: object = None,
    ini_smoke: object = False,
    parse_timeout: object = "30",
    slowpoke_ratio: object = "0.75",
    dag_id_pattern: object = "",
    required_dag_tags: object = (),
    forbid_default_owner: object = False,
) -> Any:
    """Create a minimal configuration double for smoke config-reader tests.

    Parameters:
        airflow_smoke: object containing the parsed ``--airflow-smoke`` option value.
        ini_smoke: object containing the ``airflow_smoke`` ini value.
        parse_timeout: object containing the ``airflow_dag_parse_timeout`` ini value.
        slowpoke_ratio: object containing the ``airflow_dag_parse_slowpoke_ratio`` ini value.
        dag_id_pattern: object containing the ``airflow_dag_id_pattern`` ini value.
        required_dag_tags: object containing the ``airflow_required_dag_tags`` ini value.
        forbid_default_owner: object containing the ``airflow_forbid_default_owner`` ini value.

    Returns:
        types.SimpleNamespace shaped like the configuration surface under test.
    """

    ini_values = {
        "airflow_smoke": ini_smoke,
        "airflow_dag_parse_timeout": parse_timeout,
        "airflow_dag_parse_slowpoke_ratio": slowpoke_ratio,
        "airflow_dag_id_pattern": dag_id_pattern,
        "airflow_required_dag_tags": list(required_dag_tags)
        if isinstance(required_dag_tags, (list, tuple))
        else required_dag_tags,
        "airflow_forbid_default_owner": forbid_default_owner,
    }
    return SimpleNamespace(
        getoption=lambda name: {"airflow_smoke": airflow_smoke}[name],
        getini=lambda name: ini_values[name],
        stash=pytest.Stash(),
    )


def test_smoke_enabled_prefers_cli_option() -> None:
    """Give the CLI option precedence over the ini value."""

    config = _config(airflow_smoke=True, ini_smoke=False)

    assert smoke._smoke_enabled(config) is True


def test_smoke_enabled_reads_ini_when_cli_absent() -> None:
    """Fall back to the ini value when the CLI option is unset."""

    config = _config(airflow_smoke=None, ini_smoke=True)

    assert smoke._smoke_enabled(config) is True


def test_smoke_enabled_defaults_to_disabled() -> None:
    """Return disabled when neither the CLI option nor the ini value is set."""

    config = _config()

    assert smoke._smoke_enabled(config) is False


def test_smoke_enabled_caches_resolution() -> None:
    """Resolve the enabled flag once and serve later calls from the stash."""

    reads: list[str] = []
    config = _config(airflow_smoke=True)
    original_getoption = config.getoption
    config.getoption = lambda name: (reads.append(name), original_getoption(name))[1]

    assert smoke._smoke_enabled(config) is True
    assert smoke._smoke_enabled(config) is True
    assert reads == ["airflow_smoke"]


def test_smoke_enabled_rejects_non_boolean_ini() -> None:
    """Reject a non-boolean ini value."""

    config = _config(ini_smoke="yes")

    with pytest.raises(pytest.UsageError, match="`airflow_smoke` must be a boolean"):
        smoke._smoke_enabled(config)


def test_parse_timeout_reads_default() -> None:
    """Parse the default timeout string into a float."""

    assert smoke._parse_timeout(_config()) == 30.0


@pytest.mark.parametrize(
    ("value", "match"),
    [
        (7, "must be a number"),
        ("oops", "must be a number: 'oops'"),
        ("0", "must be positive"),
        ("-1", "must be positive"),
    ],
)
def test_parse_timeout_rejects_malformed_values(value: object, match: str) -> None:
    """Reject non-string, non-numeric, and non-positive timeout values."""

    with pytest.raises(pytest.UsageError, match=match):
        smoke._parse_timeout(_config(parse_timeout=value))


def test_slowpoke_ratio_reads_default() -> None:
    """Parse the default ratio string into a float."""

    assert smoke._slowpoke_ratio(_config()) == 0.75


@pytest.mark.parametrize(
    ("value", "match"),
    [
        (7, "must be a number"),
        ("oops", "must be a number: 'oops'"),
        ("0", r"must be in \(0, 1\]"),
        ("1.5", r"must be in \(0, 1\]"),
        ("-0.1", r"must be in \(0, 1\]"),
    ],
)
def test_slowpoke_ratio_rejects_malformed_values(value: object, match: str) -> None:
    """Reject non-string, non-numeric, and out-of-range ratio values."""

    with pytest.raises(pytest.UsageError, match=match):
        smoke._slowpoke_ratio(_config(slowpoke_ratio=value))


def test_dag_id_pattern_returns_none_when_unset() -> None:
    """Return no pattern when the ini value is empty."""

    assert smoke._dag_id_pattern(_config()) is None


def test_dag_id_pattern_compiles_valid_regex() -> None:
    """Compile a valid regex into a usable pattern."""

    pattern = smoke._dag_id_pattern(_config(dag_id_pattern="^team_"))

    assert pattern is not None
    assert pattern.search("team_a")
    assert not pattern.search("other")


@pytest.mark.parametrize(
    ("value", "match"),
    [
        (7, "must be a string"),
        ("[", "must be a valid regex"),
    ],
)
def test_dag_id_pattern_rejects_malformed_values(value: object, match: str) -> None:
    """Reject a non-string value and an invalid regex."""

    with pytest.raises(pytest.UsageError, match=match):
        smoke._dag_id_pattern(_config(dag_id_pattern=value))


def test_required_dag_tags_returns_empty_when_unset() -> None:
    """Return an empty set when no tags are configured."""

    assert smoke._required_dag_tags(_config()) == frozenset()


def test_required_dag_tags_returns_configured_tags() -> None:
    """Return every configured tag as a frozenset."""

    assert smoke._required_dag_tags(_config(required_dag_tags=["a", "b"])) == frozenset({"a", "b"})


@pytest.mark.parametrize(
    "value",
    ["oops", [7]],
)
def test_required_dag_tags_rejects_malformed_values(value: object) -> None:
    """Reject a non-list value and a list containing non-string entries."""

    with pytest.raises(pytest.UsageError, match="must be a list of tags"):
        smoke._required_dag_tags(_config(required_dag_tags=value))


def test_forbid_default_owner_defaults_to_false() -> None:
    """Return disabled when the ini value is unset."""

    assert smoke._forbid_default_owner(_config()) is False


def test_forbid_default_owner_reads_configured_value() -> None:
    """Return the configured boolean value."""

    assert smoke._forbid_default_owner(_config(forbid_default_owner=True)) is True


def test_forbid_default_owner_rejects_non_boolean() -> None:
    """Reject a non-boolean ini value."""

    with pytest.raises(pytest.UsageError, match="must be a boolean"):
        smoke._forbid_default_owner(_config(forbid_default_owner="yes"))


def _stat(file: str, seconds: float, *, dags: int = 1, tasks: int = 1) -> Any:
    """Create a minimal ``FileLoadStat``-shaped double for table rendering tests.

    Parameters:
        file: str containing the Dag file name.
        seconds: float containing the parse duration in seconds.
        dags: int containing the Dag count for the file.
        tasks: int containing the task count for the file.

    Returns:
        Any shaped like Airflow's ``FileLoadStat`` NamedTuple.
    """

    return SimpleNamespace(
        file=file,
        duration=timedelta(seconds=seconds),
        dag_num=dags,
        task_num=tasks,
        dags="[]",
        warning_num=0,
        bundle_path=None,
        bundle_name=None,
    )


def test_log_stats_table_marks_ok_and_slowpoke_rows(caplog: pytest.LogCaptureFixture) -> None:
    """Render one row per file with the correct ok/slowpoke status."""

    dag_bag: Any = SimpleNamespace(
        dagbag_stats=[_stat("fast.py", 0.1), _stat("slow.py", 8.0), _stat("dead.py", 12.0)]
    )
    # A failed `logging.config.dictConfig` call elsewhere in the suite can leave this logger
    # disabled process-wide (stdlib `disable_existing_loggers` never gets reverted on failure);
    # `caplog.at_level` only guards against `logging.disable()`, not this per-logger attribute.
    logging.getLogger("pytest_airflow_in_a_box.smoke").disabled = False

    with caplog.at_level("INFO", logger="pytest_airflow_in_a_box.smoke"):
        text = smoke._log_stats_table(dag_bag, timeout=10.0, ratio=0.75)

    assert "fast.py" in text
    assert "ok" in text
    assert "SLOWPOKE (>75% of 10.0s)" in text
    assert "SLOWPOKE (>10.0s timeout)" in text
    assert "Dag bag parse report" in caplog.text


def _write_dags(pytester: pytest.Pytester, **files: str) -> Any:
    """Create a ``dags`` directory containing the provided Dag modules.

    Parameters:
        pytester: pytest.Pytester owning the temporary project directory.
        files: str module contents keyed by module name without suffix.

    Returns:
        pathlib.Path containing the new Dag directory.
    """

    folder = pytester.path / "dags"
    folder.mkdir()
    for name, contents in files.items():
        (folder / f"{name}.py").write_text(contents, encoding="utf-8")
    return folder


VALID_DAG = """
from airflow.sdk import DAG, task

with DAG(dag_id="valid_dag", schedule=None, tags=["team-a"]) as dag:
    @task
    def t():
        pass

    t()
"""

BROKEN_DAG = """
raise RuntimeError("deliberately broken smoke test Dag")
"""

DUPLICATE_A = 'from airflow.sdk import DAG\n\ndag_a = DAG(dag_id="dup_id", schedule=None)\n'
DUPLICATE_B = 'from airflow.sdk import DAG\n\ndag_b = DAG(dag_id="dup_id", schedule=None)\n'

SLOW_DAG = """
import time

time.sleep(0.3)

from airflow.sdk import DAG

slow_dag = DAG(dag_id="slow_dag", schedule=None)
"""


def test_smoke_disabled_by_default_collects_nothing(pytester: pytest.Pytester) -> None:
    """Leave the Dag folder untouched when the feature is not enabled."""

    _write_dags(pytester, valid=VALID_DAG)

    result = pytester.runpytest_subprocess("-q", "--dag-folder=dags")

    result.assert_outcomes(passed=0, failed=0)
    assert "smoke" not in result.stdout.str()


def test_airflow_smoke_option_enables_the_catalog(pytester: pytest.Pytester) -> None:
    """Collect every core smoke item once the CLI option is passed."""

    _write_dags(pytester, valid=VALID_DAG)

    result = pytester.runpytest_subprocess("-v", "--airflow-smoke", "--dag-folder=dags")

    result.assert_outcomes(passed=5)
    result.stdout.fnmatch_lines(
        [
            "*::smoke::test_dag_bag_integrity PASSED*",
            "*::smoke::test_dag_serialization_roundtrip PASSED*",
            "*::smoke::test_no_duplicate_dag_ids PASSED*",
            "*::smoke::test_schedule_sanity PASSED*",
            "*::smoke::test_pool_references_exist PASSED*",
        ]
    )


def test_ini_option_enables_the_catalog(pytester: pytest.Pytester) -> None:
    """Enable the smoke catalog through the ini option alone."""

    _write_dags(pytester, valid=VALID_DAG)
    pytester.makeini("[pytest]\nairflow_smoke = true\n")

    result = pytester.runpytest_subprocess("-q", "--dag-folder=dags")

    result.assert_outcomes(passed=5)


def test_broken_dag_fails_integrity_with_traceback(pytester: pytest.Pytester) -> None:
    """Fail the integrity item and surface the Dag file's own traceback."""

    _write_dags(pytester, broken=BROKEN_DAG)

    result = pytester.runpytest_subprocess("-q", "--airflow-smoke", "--dag-folder=dags")

    result.assert_outcomes(passed=4, failed=1)
    result.stdout.fnmatch_lines(
        ["*Dag file import check failed*", "*deliberately broken smoke test Dag*"]
    )


def test_duplicate_dag_ids_fail_dedicated_item(pytester: pytest.Pytester) -> None:
    """Fail the dedicated duplicate-id item and name both colliding files."""

    _write_dags(pytester, a=DUPLICATE_A, b=DUPLICATE_B)

    result = pytester.runpytest_subprocess("-q", "--airflow-smoke", "--dag-folder=dags")

    result.assert_outcomes(passed=3, failed=2)
    result.stdout.fnmatch_lines(["*a.py*also found in*b.py*"])


def test_slowpoke_warns_without_failing(pytester: pytest.Pytester) -> None:
    """Warn on a slow file without failing the run."""

    _write_dags(pytester, slow=SLOW_DAG)
    pytester.makeini(
        "[pytest]\n"
        "airflow_smoke = true\n"
        "airflow_dag_parse_timeout = 1.0\n"
        "airflow_dag_parse_slowpoke_ratio = 0.2\n"
    )

    result = pytester.runpytest_subprocess("-q", "--dag-folder=dags", "-m", "smoke")

    result.assert_outcomes(passed=5)
    result.stdout.fnmatch_lines(["*SlowDagParseWarning*"])


def test_timeout_crossing_fails_the_run(pytester: pytest.Pytester) -> None:
    """Fail the integrity item when a file exceeds the hard parse timeout."""

    _write_dags(pytester, slow=SLOW_DAG)
    pytester.makeini("[pytest]\nairflow_smoke = true\nairflow_dag_parse_timeout = 0.05\n")

    result = pytester.runpytest_subprocess("-v", "--dag-folder=dags", "-m", "smoke")

    result.assert_outcomes(passed=4, failed=1)
    result.stdout.fnmatch_lines(["*::smoke::test_dag_bag_integrity FAILED*"])


def test_dag_id_pattern_policy_appears_only_when_configured(pytester: pytest.Pytester) -> None:
    """Collect the pattern policy item only when its ini is set."""

    _write_dags(pytester, valid=VALID_DAG)

    disabled = pytester.runpytest_subprocess("-q", "--airflow-smoke", "--dag-folder=dags")
    disabled.assert_outcomes(passed=5)
    assert "test_dag_id_pattern" not in disabled.stdout.str()

    pytester.makeini("[pytest]\nairflow_smoke = true\nairflow_dag_id_pattern = ^valid_\n")
    enabled = pytester.runpytest_subprocess("-v", "--dag-folder=dags")
    enabled.assert_outcomes(passed=6)
    enabled.stdout.fnmatch_lines(["*::smoke::test_dag_id_pattern PASSED*"])


def test_dag_id_pattern_policy_fails_on_mismatch(pytester: pytest.Pytester) -> None:
    """Fail the pattern policy item when a dag_id does not match."""

    _write_dags(pytester, valid=VALID_DAG)
    pytester.makeini("[pytest]\nairflow_smoke = true\nairflow_dag_id_pattern = ^nomatch_\n")

    result = pytester.runpytest_subprocess("-q", "--dag-folder=dags", "-m", "smoke")

    result.assert_outcomes(passed=5, failed=1)
    result.stdout.fnmatch_lines(["*does not match pattern*"])


def test_required_dag_tags_policy_appears_only_when_configured(pytester: pytest.Pytester) -> None:
    """Collect the required-tags policy item only when its ini is set."""

    _write_dags(pytester, valid=VALID_DAG)

    disabled = pytester.runpytest_subprocess("-q", "--airflow-smoke", "--dag-folder=dags")
    assert "test_required_dag_tags" not in disabled.stdout.str()

    pytester.makeini("[pytest]\nairflow_smoke = true\nairflow_required_dag_tags =\n    team-a\n")
    enabled = pytester.runpytest_subprocess("-v", "--dag-folder=dags")
    enabled.assert_outcomes(passed=6)
    enabled.stdout.fnmatch_lines(["*::smoke::test_required_dag_tags PASSED*"])


def test_required_dag_tags_policy_fails_on_missing_tag(pytester: pytest.Pytester) -> None:
    """Fail the required-tags policy item when a Dag is missing a required tag."""

    _write_dags(pytester, valid=VALID_DAG)
    pytester.makeini(
        "[pytest]\nairflow_smoke = true\nairflow_required_dag_tags =\n    team-a\n    team-b\n"
    )

    result = pytester.runpytest_subprocess("-q", "--dag-folder=dags", "-m", "smoke")

    result.assert_outcomes(passed=5, failed=1)
    result.stdout.fnmatch_lines(["*missing required tags*'team-b'*"])


def test_forbid_default_owner_policy_appears_only_when_configured(
    pytester: pytest.Pytester,
) -> None:
    """Collect the default-owner policy item only when its ini is set."""

    _write_dags(pytester, valid=VALID_DAG)

    disabled = pytester.runpytest_subprocess("-q", "--airflow-smoke", "--dag-folder=dags")
    assert "test_forbid_default_owner" not in disabled.stdout.str()

    pytester.makeini("[pytest]\nairflow_smoke = true\nairflow_forbid_default_owner = true\n")
    enabled = pytester.runpytest_subprocess("-q", "--dag-folder=dags", "-m", "smoke")
    enabled.assert_outcomes(passed=5, failed=1)
    enabled.stdout.fnmatch_lines(["*owned by the stock*`airflow`*owner*"])


def test_smoke_marker_selects_exactly_the_bundled_items(pytester: pytest.Pytester) -> None:
    """Select exactly the bundled smoke items with `-m smoke`."""

    _write_dags(pytester, valid=VALID_DAG)
    pytester.makepyfile(
        """
        def test_regular():
            assert True
        """
    )

    selected = pytester.runpytest_subprocess(
        "-q", "--airflow-smoke", "--dag-folder=dags", "-m", "smoke"
    )
    selected.assert_outcomes(passed=5, deselected=1)

    deselected = pytester.runpytest_subprocess(
        "-q", "--airflow-smoke", "--dag-folder=dags", "-m", "not smoke"
    )
    deselected.assert_outcomes(passed=1, deselected=5)
