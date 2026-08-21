"""Test the `airflow_isolated` marker's validation, batching, and child replay."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from pytest_airflow_in_a_box import isolated, isolated_child

_RESERVED = frozenset({"AIRFLOW_HOME", "AIRFLOW__CORE__FERNET_KEY"})

_CHILD_PID_TEST = """
    import json
    import os

    import pytest


    def _record() -> None:
        with open(os.environ["ISOLATED_FACTS_FILE"], "a", encoding="utf-8") as handle:
            handle.write(json.dumps({{"pid": os.getpid(), "worker": os.environ.get(
                "PYTEST_AIRFLOW_IN_A_BOX_ISOLATED_WORKER", "")}}) + "\\n")


    @pytest.mark.airflow_isolated(environment={first_environment})
    def test_first() -> None:
        _record()


    @pytest.mark.airflow_isolated(environment={second_environment})
    def test_second() -> None:
        _record()
"""


def _node(marker: pytest.MarkDecorator | None) -> Any:
    """Create a minimal marker-lookup double for validation tests.

    Parameters:
        marker: pytest.MarkDecorator | None whose mark the double serves for any
            requested name, or ``None`` for a markerless node.

    Returns:
        types.SimpleNamespace shaped like the `MarkedNode` protocol.
    """

    mark = None if marker is None else marker.mark
    return SimpleNamespace(get_closest_marker=lambda _name: mark)


def _passed_report(nodeid: str) -> pytest.TestReport:
    """Create one passed call-phase report for envelope round-trip tests.

    Parameters:
        nodeid: str identifying the reported test.

    Returns:
        pytest.TestReport containing a passed call-phase outcome.
    """

    return pytest.TestReport(
        nodeid=nodeid,
        location=(nodeid.partition("::")[0], 0, nodeid.rpartition("::")[2]),
        keywords={},
        outcome="passed",
        longrepr=None,
        when="call",
        sections=[],
        duration=0.1,
        user_properties=[],
    )


def test_read_marker_absent_returns_none() -> None:
    """Return ``None`` for a node carrying no `airflow_isolated` marker."""

    assert isolated.read_isolated_marker(_node(None), _RESERVED) is None


def test_read_marker_rejects_positional_arguments() -> None:
    """Reject positional marker arguments with an actionable usage error."""

    node = _node(pytest.mark.airflow_isolated("mypkg"))

    with pytest.raises(pytest.UsageError, match="keyword arguments only"):
        isolated.read_isolated_marker(node, _RESERVED)


def test_read_marker_rejects_unknown_keywords() -> None:
    """Name every unknown keyword argument in the usage error."""

    node = _node(pytest.mark.airflow_isolated(entrypoints={}, retries=3))

    with pytest.raises(
        pytest.UsageError, match="unknown keyword arguments: `entrypoints`, `retries`"
    ):
        isolated.read_isolated_marker(node, _RESERVED)


def test_read_marker_rejects_a_bare_payload() -> None:
    """Reject a marker that would isolate nothing."""

    node = _node(pytest.mark.airflow_isolated(timeout=5))

    with pytest.raises(pytest.UsageError, match="would isolate nothing"):
        isolated.read_isolated_marker(node, _RESERVED)


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"entry_points": "airflow.plugins"}, "must map group names"),
        ({"entry_points": {"": "x = y:Z"}}, "group names must be non-empty strings"),
        ({"entry_points": {"airflow.plugins": 7}}, "must be one line or a list of lines"),
        ({"entry_points": {"airflow.plugins": [7]}}, "must be a string"),
        ({"entry_points": {"airflow.plugins": "x -> y:Z"}}, "must be `name = module:attr`"),
        ({"entry_points": {"airflow.plugins": "= y:Z"}}, "must be `name = module:attr`"),
        ({"environment": "AIRFLOW__A__B"}, "must map `AIRFLOW__\\*` variable names"),
        ({"environment": {"PATH": "/tmp"}}, "must start with `AIRFLOW__`"),
        ({"environment": {"AIRFLOW__A__B": 7}}, "must be a string"),
        ({"environment": {"AIRFLOW_HOME": "/tmp"}}, "must start with `AIRFLOW__`"),
        (
            {"environment": {"AIRFLOW__CORE__FERNET_KEY": "k"}},
            "collides with a bootstrap-owned variable",
        ),
        ({"entry_points": {"g": "x = y:Z"}, "name": 7}, "must be a valid distribution name"),
        ({"entry_points": {"g": "x = y:Z"}, "name": "-bad-"}, "must be a valid distribution name"),
        ({"entry_points": {"g": "x = y:Z"}, "timeout": True}, "must be a positive number"),
        ({"entry_points": {"g": "x = y:Z"}, "timeout": "5"}, "must be a positive number"),
        ({"entry_points": {"g": "x = y:Z"}, "timeout": 0}, "must be a positive number"),
    ],
)
def test_read_marker_rejects_malformed_keywords(kwargs: dict[str, object], match: str) -> None:
    """Reject one malformed marker keyword with a shape-naming usage error.

    Parameters:
        kwargs: dict[str, object] containing the malformed marker keywords.
        match: str matching the expected usage error message.
    """

    node = _node(pytest.mark.airflow_isolated(**kwargs))

    with pytest.raises(pytest.UsageError, match=match):
        isolated.read_isolated_marker(node, _RESERVED)


def test_read_marker_normalizes_the_payload() -> None:
    """Sort groups and lines, normalize the name, and default the timeout."""

    node = _node(
        pytest.mark.airflow_isolated(
            entry_points={
                "airflow.policy": ["b =  pkg:B", "a= pkg:A"],
                "airflow.plugins": "x = pkg:X",
            },
            environment={"AIRFLOW__B__B": "2", "AIRFLOW__A__A": "1"},
            name="My_Provider.Pkg",
        )
    )

    payload = isolated.read_isolated_marker(node, _RESERVED)

    assert payload is not None
    assert payload.entry_points == (
        ("airflow.plugins", ("x = pkg:X",)),
        ("airflow.policy", ("a = pkg:A", "b = pkg:B")),
    )
    assert payload.environment == (("AIRFLOW__A__A", "1"), ("AIRFLOW__B__B", "2"))
    assert payload.name == "my-provider-pkg"
    assert payload.timeout == isolated.DEFAULT_TIMEOUT_SECONDS


def test_payload_key_and_distribution_name_are_stable() -> None:
    """Derive one canonical key and one payload-hashed default distribution name."""

    payload = isolated.IsolatedPayload(
        entry_points=(("g", ("x = y:Z",)),), environment=(), name=None, timeout=300.0
    )
    twin = isolated.IsolatedPayload(
        entry_points=(("g", ("x = y:Z",)),), environment=(), name=None, timeout=300.0
    )

    assert payload.key == twin.key
    assert payload.distribution_name == twin.distribution_name
    assert payload.distribution_name.startswith("pytest-airflow-in-a-box-isolated-")

    named = isolated.IsolatedPayload(
        entry_points=(("g", ("x = y:Z",)),), environment=(), name="demo", timeout=300.0
    )

    assert named.distribution_name == "demo"
    assert named.key != payload.key


def test_build_dist_info_writes_the_synthetic_distribution(tmp_path: Path) -> None:
    """Write `METADATA`, sorted `entry_points.txt` sections, and an empty `RECORD`."""

    payload = isolated.IsolatedPayload(
        entry_points=(
            ("airflow.plugins", ("x = pkg:X",)),
            ("airflow.policy", ("a = pkg:A", "b = pkg:B")),
        ),
        environment=(),
        name="demo-provider",
        timeout=300.0,
    )

    site_dir = isolated.build_dist_info(payload, tmp_path / "site")

    dist_dir = site_dir / "demo_provider-0.0.0.dist-info"
    assert (dist_dir / "METADATA").read_text(encoding="utf-8") == (
        "Metadata-Version: 2.1\nName: demo-provider\nVersion: 0.0.0\n"
    )
    assert (dist_dir / "entry_points.txt").read_text(encoding="utf-8") == (
        "[airflow.plugins]\nx = pkg:X\n\n[airflow.policy]\na = pkg:A\nb = pkg:B\n"
    )
    assert (dist_dir / "RECORD").read_text(encoding="utf-8") == ""


def test_build_dist_info_renders_write_failures_as_usage_errors(tmp_path: Path) -> None:
    """Surface an unwritable scratch directory as a usage error."""

    blocker = tmp_path / "site"
    blocker.write_text("", encoding="utf-8")
    payload = isolated.IsolatedPayload(
        entry_points=(("g", ("x = y:Z",)),), environment=(), name=None, timeout=300.0
    )

    with pytest.raises(pytest.UsageError, match="Could not write synthetic distribution"):
        isolated.build_dist_info(payload, blocker)


def test_log_tail_reads_the_last_lines(tmp_path: Path) -> None:
    """Return only the trailing log lines, and a placeholder for a missing log."""

    log_path = tmp_path / "child.log"
    log_path.write_text("\n".join(str(index) for index in range(80)), encoding="utf-8")

    tail = isolated._log_tail(log_path)

    assert tail.splitlines()[0] == "30"
    assert tail.splitlines()[-1] == "79"
    assert isolated._log_tail(tmp_path / "absent.log") == "<child log unavailable>"


def _envelope(entries: list[object], **overrides: object) -> str:
    """Serialize one child report envelope with optional field overrides.

    Parameters:
        entries: list[object] containing serialized report entries.
        overrides: object values replacing the default envelope fields.

    Returns:
        str containing the serialized envelope JSON.
    """

    payload: dict[str, object] = {
        "format": isolated_child.REPORT_FORMAT,
        "version": isolated_child.REPORT_VERSION,
        "exit_status": 0,
        "reports": entries,
    }
    return json.dumps(payload | overrides)


@pytest.mark.parametrize(
    ("text", "match"),
    [
        (None, "could not read the child report"),
        ("not json", "is not valid JSON"),
        ("[]", "must be a JSON object"),
        (_envelope([], format="other"), "field `format` must be"),
        (_envelope([], version=2), "field `version` must be"),
        (_envelope([], exit_status=True), "field `exit_status` must be an integer"),
        (_envelope([], reports="none"), "field `reports` must be a list"),
        (_envelope(["entry"]), "entries must be JSON objects"),
        (_envelope([{"when": "call"}]), "must carry a non-empty `nodeid`"),
        (
            _envelope([{"nodeid": "test_a.py::test_one", "when": "call"}]),
            "could not be deserialized",
        ),
    ],
)
def test_decode_envelope_rejects_malformed_shapes(
    tmp_path: Path, text: str | None, match: str
) -> None:
    """Reject one malformed envelope shape with a field-naming error.

    Parameters:
        tmp_path: pathlib.Path receiving the envelope file.
        text: str | None containing the envelope content, or ``None`` for no file.
        match: str matching the expected error message.
    """

    path = tmp_path / "report.json"
    if text is not None:
        path.write_text(text, encoding="utf-8")

    with pytest.raises(ValueError, match=match):
        isolated._decode_envelope(path)


def test_decode_envelope_groups_reports_by_nodeid(tmp_path: Path) -> None:
    """Round-trip serialized reports into per-nodeid phase lists."""

    first = _passed_report("test_a.py::test_one")
    second = _passed_report("test_a.py::test_two")
    path = tmp_path / "report.json"
    path.write_text(
        _envelope([first._to_json(), second._to_json(), first._to_json()]), encoding="utf-8"
    )

    reports = isolated._decode_envelope(path)

    assert sorted(reports) == ["test_a.py::test_one", "test_a.py::test_two"]
    assert [report.outcome for report in reports["test_a.py::test_one"]] == ["passed", "passed"]
    assert reports["test_a.py::test_two"][0].nodeid == "test_a.py::test_two"


def test_apply_xdist_refusal_branches(monkeypatch: pytest.MonkeyPatch) -> None:
    """Refuse a marked item only on an xdist worker outside an isolated child.

    Parameters:
        monkeypatch: pytest.MonkeyPatch controlling the worker environment variables.
    """

    marked = _node(pytest.mark.airflow_isolated(entry_points={"g": "x = y:Z"}))

    isolated.apply_xdist_refusal(_node(None))
    isolated.apply_xdist_refusal(marked)

    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw0")
    with pytest.raises(pytest.UsageError, match="unsupported under pytest-xdist"):
        isolated.apply_xdist_refusal(marked)

    monkeypatch.setenv("PYTEST_AIRFLOW_IN_A_BOX_ISOLATED_WORKER", "iso-1234abcd")
    isolated.apply_xdist_refusal(marked)


def test_runtest_protocol_defers_for_inert_items(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return ``None`` for unmarked items, isolated children, and xdist workers.

    Parameters:
        monkeypatch: pytest.MonkeyPatch controlling the worker environment variables.
    """

    marked = _node(pytest.mark.airflow_isolated(entry_points={"g": "x = y:Z"}))

    assert isolated.runtest_protocol(_node(None)) is None

    monkeypatch.setenv("PYTEST_AIRFLOW_IN_A_BOX_ISOLATED_WORKER", "iso-1234abcd")
    assert isolated.runtest_protocol(marked) is None

    monkeypatch.delenv("PYTEST_AIRFLOW_IN_A_BOX_ISOLATED_WORKER")
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw0")
    assert isolated.runtest_protocol(marked) is None


def test_report_writer_writes_the_envelope(tmp_path: Path) -> None:
    """Accumulate serialized reports and write one key-sorted envelope."""

    writer = isolated_child.IsolatedReportWriter(tmp_path / "nested" / "report.json")
    session: Any = SimpleNamespace()

    writer.pytest_runtest_logreport(_passed_report("test_a.py::test_one"))
    writer.pytest_runtest_logreport(_passed_report("test_a.py::test_two"))
    writer.pytest_sessionfinish(session, 1)

    decoded = json.loads((tmp_path / "nested" / "report.json").read_text(encoding="utf-8"))
    assert decoded["format"] == isolated_child.REPORT_FORMAT
    assert decoded["version"] == isolated_child.REPORT_VERSION
    assert decoded["exit_status"] == 1
    assert [entry["nodeid"] for entry in decoded["reports"]] == [
        "test_a.py::test_one",
        "test_a.py::test_two",
    ]


def test_report_writer_renders_write_failures_as_usage_errors(tmp_path: Path) -> None:
    """Surface an unwritable envelope destination as a usage error."""

    blocker = tmp_path / "blocker"
    blocker.write_text("", encoding="utf-8")
    writer = isolated_child.IsolatedReportWriter(blocker / "report.json")
    session: Any = SimpleNamespace()

    with pytest.raises(pytest.UsageError, match="Could not write isolated report"):
        writer.pytest_sessionfinish(session, 0)


def test_child_configure_requires_the_report_destination(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refuse to configure the child reporter without a report destination.

    Parameters:
        monkeypatch: pytest.MonkeyPatch clearing the destination variable.
    """

    monkeypatch.delenv(isolated_child.ISOLATED_REPORT_PATH_ENVIRONMENT_VARIABLE, raising=False)
    config: Any = SimpleNamespace()

    with pytest.raises(pytest.UsageError, match="must name the isolated report destination"):
        isolated_child.pytest_configure(config)


def test_child_configure_registers_the_writer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Register one writer bound to the environment-named destination.

    Parameters:
        tmp_path: pathlib.Path providing the report destination.
        monkeypatch: pytest.MonkeyPatch setting the destination variable.
    """

    monkeypatch.setenv(
        isolated_child.ISOLATED_REPORT_PATH_ENVIRONMENT_VARIABLE,
        str(tmp_path / "report.json"),
    )
    registered: dict[str, object] = {}
    config: Any = SimpleNamespace(
        pluginmanager=SimpleNamespace(
            register=lambda plugin, name: registered.__setitem__(name, plugin)
        )
    )

    isolated_child.pytest_configure(config)

    (writer,) = registered.values()
    assert isinstance(writer, isolated_child.IsolatedReportWriter)


def test_marked_tests_share_one_child_per_identical_payload(
    pytester: pytest.Pytester, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Batch identically-marked module-mates into a single child invocation.

    Also exercises the ambient-``PYTHONPATH`` prepend branch: the variable is set in
    the parent so the child must preserve it behind the synthetic site directory.

    Parameters:
        pytester: pytest.Pytester running the plugin in a subprocess.
        tmp_path: pathlib.Path receiving the child fact file.
        monkeypatch: pytest.MonkeyPatch publishing the fact file and ambient path.
    """

    facts = tmp_path / "facts.jsonl"
    monkeypatch.setenv("ISOLATED_FACTS_FILE", str(facts))
    monkeypatch.setenv("PYTHONPATH", str(tmp_path))
    pytester.makepyfile(
        _CHILD_PID_TEST.format(
            first_environment='{"AIRFLOW__DEMO__FLAG": "1"}',
            second_environment='{"AIRFLOW__DEMO__FLAG": "1"}',
        )
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=2)
    lines = [json.loads(line) for line in facts.read_text(encoding="utf-8").splitlines()]
    assert len(lines) == 2
    assert lines[0]["pid"] == lines[1]["pid"]
    assert lines[0]["worker"].startswith("iso-")


def test_distinct_payloads_run_in_distinct_children(
    pytester: pytest.Pytester, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Split module-mates with differing payloads into separate children.

    Parameters:
        pytester: pytest.Pytester running the plugin in a subprocess.
        tmp_path: pathlib.Path receiving the child fact file.
        monkeypatch: pytest.MonkeyPatch publishing the fact file location.
    """

    facts = tmp_path / "facts.jsonl"
    monkeypatch.setenv("ISOLATED_FACTS_FILE", str(facts))
    pytester.makepyfile(
        _CHILD_PID_TEST.format(
            first_environment='{"AIRFLOW__DEMO__FLAG": "1"}',
            second_environment='{"AIRFLOW__DEMO__FLAG": "2"}',
        )
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=2)
    lines = [json.loads(line) for line in facts.read_text(encoding="utf-8").splitlines()]
    assert len(lines) == 2
    assert lines[0]["pid"] != lines[1]["pid"]


def test_child_outcomes_replay_with_full_fidelity(pytester: pytest.Pytester) -> None:
    """Replay pass, fail, skip, and xfail outcomes with the child's own longrepr.

    Parameters:
        pytester: pytest.Pytester running the plugin in a subprocess.
    """

    pytester.makepyfile(
        """
        import pytest

        isolated = pytest.mark.airflow_isolated(environment={"AIRFLOW__DEMO__FLAG": "1"})


        @isolated
        def test_passes() -> None:
            assert True


        @isolated
        def test_fails() -> None:
            assert False, "deliberate isolated failure"


        @isolated
        def test_skips() -> None:
            pytest.skip("deliberate isolated skip")


        @isolated
        @pytest.mark.xfail(reason="deliberate isolated xfail")
        def test_xfails() -> None:
            assert False


        def test_unmarked_neighbour_runs_in_process() -> None:
            assert True
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=2, failed=1, skipped=1, xfailed=1)
    assert result.ret == pytest.ExitCode.TESTS_FAILED
    output = result.stdout.str() + result.stderr.str()
    assert "deliberate isolated failure" in output
    assert "INTERNALERROR" not in output


def test_plugin_entry_point_is_discovered_by_airflow(pytester: pytest.Pytester) -> None:
    """Discover a real `airflow.plugins` entry point through the synthetic dist-info.

    Parameters:
        pytester: pytest.Pytester running the plugin in a subprocess.
    """

    pytester.makepyfile(
        demo_plugin="""
        from airflow.plugins_manager import AirflowPlugin


        class DemoPlugin(AirflowPlugin):
            name = "isolated_demo"
        """,
        test_demo="""
        import pytest


        @pytest.mark.airflow_isolated(
            entry_points={"airflow.plugins": "isolated_demo = demo_plugin:DemoPlugin"}
        )
        def test_plugin_discovered() -> None:
            from airflow.plugins_manager import get_plugin_info

            names = [info["name"] for info in get_plugin_info()]
            assert "isolated_demo" in names
        """,
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)


def test_provider_entry_point_is_discovered_by_airflow(pytester: pytest.Pytester) -> None:
    """Discover a real `apache_airflow_provider` entry point through a live manager.

    Parameters:
        pytester: pytest.Pytester running the plugin in a subprocess.
    """

    pytester.makepyfile(
        demo_provider="""
        def get_provider_info() -> dict[str, object]:
            return {
                "package-name": "demo-provider",
                "name": "Demo Provider",
                "description": "Synthetic provider for entry-point discovery.",
                "versions": ["0.0.0"],
            }
        """,
        test_provider="""
        import pytest


        @pytest.mark.airflow_isolated(
            entry_points={
                "apache_airflow_provider": "provider_info = demo_provider:get_provider_info"
            },
            name="demo-provider",
        )
        def test_provider_discovered() -> None:
            from airflow.providers_manager import ProvidersManager

            assert "demo-provider" in ProvidersManager().providers
        """,
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)


def test_child_crash_fails_every_batched_test(pytester: pytest.Pytester) -> None:
    """Fail the whole batch when the child dies before writing a report.

    Parameters:
        pytester: pytest.Pytester running the plugin in a subprocess.
    """

    pytester.makeconftest(
        """
        import os


        def pytest_sessionstart(session) -> None:
            if os.environ.get("PYTEST_AIRFLOW_IN_A_BOX_ISOLATED_WORKER"):
                os._exit(3)
        """
    )
    pytester.makepyfile(
        """
        import pytest

        pytestmark = pytest.mark.airflow_isolated(
            environment={"AIRFLOW__DEMO__FLAG": "1"}
        )


        def test_first() -> None:
            assert True


        def test_second() -> None:
            assert True
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(failed=2)
    assert "exited with status 3" in result.stdout.str()


def test_child_timeout_fails_every_batched_test(pytester: pytest.Pytester) -> None:
    """Kill and fail a child exceeding its marker timeout.

    Parameters:
        pytester: pytest.Pytester running the plugin in a subprocess.
    """

    pytester.makepyfile(
        """
        import time

        import pytest


        @pytest.mark.airflow_isolated(
            environment={"AIRFLOW__DEMO__FLAG": "1"}, timeout=2
        )
        def test_sleeps_past_the_timeout() -> None:
            time.sleep(60)
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(failed=1)
    assert "exceeded its 2.0s timeout" in result.stdout.str()


def test_garbled_child_report_fails_every_batched_test(pytester: pytest.Pytester) -> None:
    """Fail the batch with the decode error when the child report is unreadable.

    Parameters:
        pytester: pytest.Pytester running the plugin in a subprocess.
    """

    pytester.makeconftest(
        """
        import os

        import pytest


        @pytest.hookimpl(trylast=True)
        def pytest_sessionfinish(session, exitstatus) -> None:
            path = os.environ.get("PYTEST_AIRFLOW_IN_A_BOX_ISOLATED_REPORT_PATH")
            if path and os.environ.get("PYTEST_AIRFLOW_IN_A_BOX_ISOLATED_WORKER"):
                with open(path, "w", encoding="utf-8") as handle:
                    handle.write("not json")
        """
    )
    pytester.makepyfile(
        """
        import pytest


        @pytest.mark.airflow_isolated(environment={"AIRFLOW__DEMO__FLAG": "1"})
        def test_report_gets_garbled() -> None:
            assert True
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(failed=1)
    output = result.stdout.str()
    assert "could not be replayed" in output
    assert "not valid JSON" in output


def test_missing_child_report_fails_only_the_missing_test(
    pytester: pytest.Pytester,
) -> None:
    """Fail only the batch member the child never reported on.

    Parameters:
        pytester: pytest.Pytester running the plugin in a subprocess.
    """

    pytester.makeconftest(
        """
        import os


        def pytest_collection_modifyitems(session, config, items) -> None:
            if os.environ.get("PYTEST_AIRFLOW_IN_A_BOX_ISOLATED_WORKER"):
                items[:] = [item for item in items if "dropped" not in item.name]
        """
    )
    pytester.makepyfile(
        """
        import pytest

        pytestmark = pytest.mark.airflow_isolated(
            environment={"AIRFLOW__DEMO__FLAG": "1"}
        )


        def test_kept() -> None:
            assert True


        def test_dropped_by_the_child() -> None:
            assert True
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1, failed=1)
    assert "produced no report for this test" in result.stdout.str()


def test_extra_child_reports_are_dropped_with_a_warning(
    pytester: pytest.Pytester,
) -> None:
    """Warn about, and drop, child reports for tests outside the batch.

    Parameters:
        pytester: pytest.Pytester running the plugin in a subprocess.
    """

    pytester.makeconftest(
        """
        import json
        import os

        import pytest


        @pytest.hookimpl(trylast=True)
        def pytest_sessionfinish(session, exitstatus) -> None:
            path = os.environ.get("PYTEST_AIRFLOW_IN_A_BOX_ISOLATED_REPORT_PATH")
            if not path or not os.environ.get("PYTEST_AIRFLOW_IN_A_BOX_ISOLATED_WORKER"):
                return
            with open(path, encoding="utf-8") as handle:
                envelope = json.load(handle)
            phantom = dict(envelope["reports"][0])
            phantom["nodeid"] = "test_extra.py::test_phantom"
            envelope["reports"].append(phantom)
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(envelope, handle)
        """
    )
    pytester.makepyfile(
        test_extra="""
        import pytest


        @pytest.mark.airflow_isolated(environment={"AIRFLOW__DEMO__FLAG": "1"})
        def test_real() -> None:
            assert True
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1, warnings=1)
    assert "outside its batch" in result.stdout.str()


def test_marked_tests_are_refused_under_xdist(pytester: pytest.Pytester) -> None:
    """Render the xdist refusal as an ordinary per-test error, not a crashed node.

    Parameters:
        pytester: pytest.Pytester running the plugin in a subprocess.
    """

    pytester.makepyfile(
        """
        import pytest


        @pytest.mark.airflow_isolated(environment={"AIRFLOW__DEMO__FLAG": "1"})
        def test_marked() -> None:
            assert True
        """
    )

    result = pytester.runpytest_subprocess("-q", "-n", "1")

    result.assert_outcomes(errors=1)
    output = result.stdout.str() + result.stderr.str()
    assert "unsupported under pytest-xdist" in output
    assert "INTERNALERROR" not in output
