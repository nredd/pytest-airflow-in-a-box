"""Collect an opt-in catalog of zero-boilerplate Dag smoke checks.

Every item is synthesized directly on the pytest ``Session`` rather than anchored to a real file
on disk, so the catalog carries no collection dependency on the user's project layout. Off unless
``airflow_smoke``/``--airflow-smoke`` is enabled; collection cost is zero when disabled.

References:
    https://docs.pytest.org/en/stable/example/nonpython.html
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.hookspec.pytest_collection_modifyitems
    https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-serialization.html
"""

from __future__ import annotations

import contextlib
import io
import logging
import os
import re
import warnings
from typing import TYPE_CHECKING

import pytest

from pytest_airflow_in_a_box._compat import build_dag_bag
from pytest_airflow_in_a_box._compat.dag import _get_dag_serializer
from pytest_airflow_in_a_box.fixtures.dagbag import _dag_folder

if TYPE_CHECKING:
    from collections.abc import Iterator

    from _pytest._code.code import TerminalRepr

    from pytest_airflow_in_a_box._compat.dagbag import DagBag

LOGGER = logging.getLogger(__name__)

SMOKE_ENABLED_KEY = pytest.StashKey[bool]()
DAG_BAG_KEY = pytest.StashKey["DagBag"]()
DEFAULT_OWNER = "airflow"
DUPLICATE_ID_MARKER = "AirflowDagDuplicatedIdException"


class SlowDagParseWarning(RuntimeWarning):
    """Warn that a Dag file's parse duration crossed the slowpoke ratio."""


class SmokeCheckFailure(Exception):
    """Report a bundled smoke check failure with a preformatted message.

    Parameters:
        message: str containing the complete failure body.

    Raises:
        ValueError: The message is empty.
    """

    def __init__(self, message: str) -> None:
        if not message:
            raise ValueError("`message` must be a non-empty failure body")
        super().__init__(message)


def _smoke_enabled(config: pytest.Config) -> bool:
    """Resolve and cache whether the bundled smoke catalog is enabled.

    Parameters:
        config: pytest.Config containing plugin options and ini values.

    Returns:
        bool indicating whether smoke items should be collected.
    """

    if SMOKE_ENABLED_KEY in config.stash:
        return config.stash[SMOKE_ENABLED_KEY]

    option_value: object = config.getoption("airflow_smoke")
    if option_value is not None:
        enabled = bool(option_value)
    else:
        ini_value: object = config.getini("airflow_smoke")
        if not isinstance(ini_value, bool):
            raise pytest.UsageError("Ini option `airflow_smoke` must be a boolean")
        enabled = ini_value
    config.stash[SMOKE_ENABLED_KEY] = enabled
    return enabled


def _parse_timeout(config: pytest.Config) -> float:
    """Read the per-file Dag parse timeout in seconds.

    Parameters:
        config: pytest.Config containing the ``airflow_dag_parse_timeout`` ini value.

    Returns:
        float containing the parse timeout in seconds.

    Raises:
        pytest.UsageError: The ini value is not a positive number.
    """

    value: object = config.getini("airflow_dag_parse_timeout")
    if not isinstance(value, str):
        raise pytest.UsageError("Ini option `airflow_dag_parse_timeout` must be a number")
    try:
        timeout = float(value)
    except ValueError as error:
        raise pytest.UsageError(
            f"Ini option `airflow_dag_parse_timeout` must be a number: '{value}'"
        ) from error
    if timeout <= 0:
        raise pytest.UsageError(
            f"Ini option `airflow_dag_parse_timeout` must be positive: '{value}'"
        )
    return timeout


def _slowpoke_ratio(config: pytest.Config) -> float:
    """Read the slowpoke warning ratio of the parse timeout.

    Parameters:
        config: pytest.Config containing the ``airflow_dag_parse_slowpoke_ratio`` ini value.

    Returns:
        float containing the ratio, in ``(0, 1]``.

    Raises:
        pytest.UsageError: The ini value is not a number in ``(0, 1]``.
    """

    value: object = config.getini("airflow_dag_parse_slowpoke_ratio")
    if not isinstance(value, str):
        raise pytest.UsageError("Ini option `airflow_dag_parse_slowpoke_ratio` must be a number")
    try:
        ratio = float(value)
    except ValueError as error:
        raise pytest.UsageError(
            f"Ini option `airflow_dag_parse_slowpoke_ratio` must be a number: '{value}'"
        ) from error
    if not (0 < ratio <= 1):
        raise pytest.UsageError(
            f"Ini option `airflow_dag_parse_slowpoke_ratio` must be in (0, 1]: '{value}'"
        )
    return ratio


def _dag_id_pattern(config: pytest.Config) -> re.Pattern[str] | None:
    """Read and compile the required ``dag_id`` naming pattern.

    Parameters:
        config: pytest.Config containing the ``airflow_dag_id_pattern`` ini value.

    Returns:
        re.Pattern[str] | None containing the compiled pattern, or ``None`` when unset.

    Raises:
        pytest.UsageError: The ini value is not a string or is not a valid regex.
    """

    value: object = config.getini("airflow_dag_id_pattern")
    if not isinstance(value, str):
        raise pytest.UsageError("Ini option `airflow_dag_id_pattern` must be a string")
    if not value:
        return None
    try:
        return re.compile(value)
    except re.error as error:
        raise pytest.UsageError(
            f"Ini option `airflow_dag_id_pattern` must be a valid regex: '{value}': {error}"
        ) from error


def _required_dag_tags(config: pytest.Config) -> frozenset[str]:
    """Read the tags every collected Dag must carry.

    Parameters:
        config: pytest.Config containing the ``airflow_required_dag_tags`` ini value.

    Returns:
        frozenset[str] containing the required tags; empty when the policy is off.

    Raises:
        pytest.UsageError: The ini value is not a list of strings.
    """

    lines: object = config.getini("airflow_required_dag_tags")
    if not isinstance(lines, list) or any(not isinstance(line, str) for line in lines):
        raise pytest.UsageError("Ini option `airflow_required_dag_tags` must be a list of tags")
    return frozenset(lines)


def _forbid_default_owner(config: pytest.Config) -> bool:
    """Read whether tasks owned by Airflow's stock default owner should fail.

    Parameters:
        config: pytest.Config containing the ``airflow_forbid_default_owner`` ini value.

    Returns:
        bool indicating whether the policy is active.

    Raises:
        pytest.UsageError: The ini value is not a boolean.
    """

    value: object = config.getini("airflow_forbid_default_owner")
    if not isinstance(value, bool):
        raise pytest.UsageError("Ini option `airflow_forbid_default_owner` must be a boolean")
    return value


def _smoke_dag_bag(session: pytest.Session, config: pytest.Config) -> DagBag:
    """Build and cache one shared Dag bag for every smoke item this session.

    Sets ``AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT`` immediately before construction so Airflow
    hard-kills any single file exceeding the configured timeout; the environment variable is read
    per lookup, not cached at import, so this stays safe to set late and per-run.

    Parameters:
        session: pytest.Session used to cache the built Dag bag.
        config: pytest.Config containing plugin options and ini values.

    Returns:
        pytest_airflow_in_a_box._compat.dagbag.DagBag containing every parsed Dag.
    """

    if DAG_BAG_KEY in session.stash:
        return session.stash[DAG_BAG_KEY]

    timeout = _parse_timeout(config)
    os.environ["AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT"] = str(timeout)
    dag_bag = build_dag_bag(_dag_folder(config))
    session.stash[DAG_BAG_KEY] = dag_bag
    return dag_bag


def _log_stats_table(dag_bag: DagBag, *, timeout: float, ratio: float) -> str:
    """Render and log a slowest-first table of every parsed Dag file.

    Parameters:
        dag_bag: pytest_airflow_in_a_box._compat.dagbag.DagBag containing parse statistics.
        timeout: float containing the hard per-file parse timeout in seconds.
        ratio: float containing the slowpoke warning ratio of the timeout.

    Returns:
        str containing the rendered table text.
    """

    from airflow.cli.simple_table import AirflowConsole

    threshold = ratio * timeout

    def _status(seconds: float) -> str:
        if seconds > timeout:
            return f"SLOWPOKE (>{timeout:.1f}s timeout)"
        if seconds > threshold:
            return f"SLOWPOKE (>{ratio:.0%} of {timeout:.1f}s)"
        return "ok"

    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        AirflowConsole(width=100).print_as(
            data=dag_bag.dagbag_stats,
            output="table",
            mapper=lambda stat: {
                "file": stat.file,
                "dags": stat.dag_num,
                "tasks": stat.task_num,
                "duration": f"{stat.duration.total_seconds():.3f}s",
                "status": _status(stat.duration.total_seconds()),
            },
        )
    text = buffer.getvalue()
    LOGGER.info(f"Dag bag parse report:\n{text}")
    return text


class DagBagIntegrityItem(pytest.Item):
    """Fail on Dag import errors and per-file parse timeouts; warn on slowpokes."""

    def __init__(self, *, name: str, parent: SmokeCollector) -> None:
        """Create the item and mark it as a metadata-database smoke test.

        Parameters:
            name: str containing the pytest item name.
            parent: SmokeCollector that collected this item.
        """

        super().__init__(name=name, parent=parent)
        self.add_marker(pytest.mark.smoke)
        self.add_marker(pytest.mark.db_test)
        timeout = _parse_timeout(self.config)
        folder = _dag_folder(self.config)
        file_count = max(1, len(list(folder.rglob("*.py")))) if folder.is_dir() else 1
        self.add_marker(pytest.mark.timeout(timeout * file_count))

    def runtest(self) -> None:
        """Parse the configured Dag folder and enforce timeout and import-error policy.

        Raises:
            SmokeCheckFailure: Any file failed to import or exceeded the parse timeout.
        """

        timeout = _parse_timeout(self.config)
        ratio = _slowpoke_ratio(self.config)
        dag_bag = _smoke_dag_bag(self.session, self.config)
        table = _log_stats_table(dag_bag, timeout=timeout, ratio=ratio)

        failures: list[str] = []
        if dag_bag.import_errors:
            for path, message in sorted(dag_bag.import_errors.items()):
                failures.append(f"Dag file import check failed: '{path}'\n{message.rstrip()}")

        slowpokes: list[str] = []
        for stat in dag_bag.dagbag_stats:
            seconds = stat.duration.total_seconds()
            if seconds > timeout:
                failures.append(
                    f"Dag file '{stat.file}' took {seconds:.3f}s, exceeding the "
                    f"{timeout:.1f}s parse timeout"
                )
            elif seconds > ratio * timeout:
                slowpokes.append(stat.file)

        for slowpoke in slowpokes:
            warnings.warn(
                SlowDagParseWarning(
                    f"Dag file '{slowpoke}' exceeded {ratio:.0%} of the {timeout:.1f}s "
                    "parse timeout"
                ),
                stacklevel=1,
            )

        if failures:
            raise SmokeCheckFailure("\n\n".join(failures) + f"\n\n{table}")

    def repr_failure(
        self,
        excinfo: pytest.ExceptionInfo[BaseException],
        style: object = None,
    ) -> str | TerminalRepr:
        """Format smoke check failures without a pytest-internal traceback.

        Parameters:
            excinfo: pytest.ExceptionInfo[BaseException] describing the failure.
            style: object containing an unused traceback style override.

        Returns:
            str | TerminalRepr containing the failure representation.
        """

        del style
        if isinstance(excinfo.value, SmokeCheckFailure):
            return str(excinfo.value)
        return super().repr_failure(excinfo)

    def reportinfo(self) -> tuple[str, int, str]:
        """Locate this item for terminal and junit reporting.

        Returns:
            tuple[str, int, str] containing path, line, and title.
        """

        return self.nodeid, 0, self.name


class DagSerializationRoundtripItem(pytest.Item):
    """Round-trip every parsed Dag through Airflow's scheduler serialization."""

    def __init__(self, *, name: str, parent: SmokeCollector) -> None:
        """Create the item and mark it as a bundled smoke test.

        Parameters:
            name: str containing the pytest item name.
            parent: SmokeCollector that collected this item.
        """

        super().__init__(name=name, parent=parent)
        self.add_marker(pytest.mark.smoke)

    def runtest(self) -> None:
        """Serialize and deserialize every parsed Dag.

        Raises:
            SmokeCheckFailure: Any Dag failed to round-trip through serialization.
        """

        dag_bag = _smoke_dag_bag(self.session, self.config)
        serialized_dag_class = _get_dag_serializer()
        failures: list[str] = []
        for dag_id, dag in sorted(dag_bag.dags.items()):
            try:
                encoded = serialized_dag_class.serialize_dag(dag)
                serialized_dag_class.deserialize_dag(encoded)
            except Exception as error:
                failures.append(
                    f"Dag `{dag_id}` failed to round-trip through serialization: {error}"
                )
        if failures:
            raise SmokeCheckFailure("\n\n".join(failures))

    def reportinfo(self) -> tuple[str, int, str]:
        """Locate this item for terminal and junit reporting.

        Returns:
            tuple[str, int, str] containing path, line, and title.
        """

        return self.nodeid, 0, self.name


class NoDuplicateDagIdsItem(pytest.Item):
    """Fail when two Dag files declare the same ``dag_id``."""

    def __init__(self, *, name: str, parent: SmokeCollector) -> None:
        """Create the item and mark it as a bundled smoke test.

        Parameters:
            name: str containing the pytest item name.
            parent: SmokeCollector that collected this item.
        """

        super().__init__(name=name, parent=parent)
        self.add_marker(pytest.mark.smoke)

    def runtest(self) -> None:
        """Surface every duplicated ``dag_id`` collision the Dag bag recorded.

        Raises:
            SmokeCheckFailure: Any Dag file was dropped for duplicating a ``dag_id``.
        """

        dag_bag = _smoke_dag_bag(self.session, self.config)
        failures = [
            f"'{path}': {message}"
            for path, message in sorted(dag_bag.import_errors.items())
            if DUPLICATE_ID_MARKER in message
        ]
        if failures:
            raise SmokeCheckFailure("\n\n".join(failures))

    def reportinfo(self) -> tuple[str, int, str]:
        """Locate this item for terminal and junit reporting.

        Returns:
            tuple[str, int, str] containing path, line, and title.
        """

        return self.nodeid, 0, self.name


class ScheduleSanityItem(pytest.Item):
    """Fail when a scheduled Dag's timetable cannot compute its next run."""

    def __init__(self, *, name: str, parent: SmokeCollector) -> None:
        """Create the item and mark it as a bundled smoke test.

        Parameters:
            name: str containing the pytest item name.
            parent: SmokeCollector that collected this item.
        """

        super().__init__(name=name, parent=parent)
        self.add_marker(pytest.mark.smoke)

    def runtest(self) -> None:
        """Compute the next scheduled run for every scheduled, serialized Dag.

        Raises:
            SmokeCheckFailure: A scheduled Dag's timetable raised while computing its next run.
        """

        from airflow.timetables.base import TimeRestriction

        dag_bag = _smoke_dag_bag(self.session, self.config)
        serialized_dag_class = _get_dag_serializer()
        failures: list[str] = []
        for dag_id, dag in sorted(dag_bag.dags.items()):
            if not dag.timetable.can_be_scheduled:
                continue
            try:
                decoded = serialized_dag_class.deserialize_dag(
                    serialized_dag_class.serialize_dag(dag)
                )
                restriction = TimeRestriction(
                    earliest=decoded.start_date,
                    latest=decoded.end_date,
                    catchup=decoded.catchup,
                )
                decoded.timetable.next_dagrun_info(
                    last_automated_data_interval=None,
                    restriction=restriction,
                )
            except Exception as error:
                failures.append(
                    f"Dag `{dag_id}` could not compute its next scheduled run: {error}"
                )
        if failures:
            raise SmokeCheckFailure("\n\n".join(failures))

    def reportinfo(self) -> tuple[str, int, str]:
        """Locate this item for terminal and junit reporting.

        Returns:
            tuple[str, int, str] containing path, line, and title.
        """

        return self.nodeid, 0, self.name


class PoolReferencesExistItem(pytest.Item):
    """Fail when a task references a pool absent from the metadata database."""

    def __init__(self, *, name: str, parent: SmokeCollector) -> None:
        """Create the item and mark it as a metadata-database smoke test.

        Parameters:
            name: str containing the pytest item name.
            parent: SmokeCollector that collected this item.
        """

        super().__init__(name=name, parent=parent)
        self.add_marker(pytest.mark.smoke)
        self.add_marker(pytest.mark.db_test)

    def runtest(self) -> None:
        """Resolve every task's declared pool against the metadata database.

        Raises:
            SmokeCheckFailure: A task references a pool the database does not contain.
        """

        # Deferred to preserve bootstrap safety and avoid Airflow's module import cost.
        from airflow.models.pool import Pool
        from airflow.utils.session import create_session

        dag_bag = _smoke_dag_bag(self.session, self.config)
        failures: list[str] = []
        with create_session() as database_session:
            known = {pool.pool for pool in Pool.get_pools(session=database_session)}
            for dag_id, dag in sorted(dag_bag.dags.items()):
                for task in dag.tasks:
                    if task.pool not in known:
                        failures.append(
                            f"Dag `{dag_id}` task `{task.task_id}` references unknown pool "
                            f"`{task.pool}`"
                        )
        if failures:
            raise SmokeCheckFailure("\n\n".join(failures))

    def reportinfo(self) -> tuple[str, int, str]:
        """Locate this item for terminal and junit reporting.

        Returns:
            tuple[str, int, str] containing path, line, and title.
        """

        return self.nodeid, 0, self.name


class DagIdPatternItem(pytest.Item):
    """Fail when a ``dag_id`` does not match the configured naming pattern."""

    def __init__(self, *, name: str, parent: SmokeCollector, pattern: re.Pattern[str]) -> None:
        """Create the item and mark it as a bundled smoke test.

        Parameters:
            name: str containing the pytest item name.
            parent: SmokeCollector that collected this item.
            pattern: re.Pattern[str] every ``dag_id`` must match.
        """

        super().__init__(name=name, parent=parent)
        self.pattern = pattern
        self.add_marker(pytest.mark.smoke)

    def runtest(self) -> None:
        """Match every ``dag_id`` against the configured pattern.

        Raises:
            SmokeCheckFailure: A ``dag_id`` does not match the configured pattern.
        """

        dag_bag = _smoke_dag_bag(self.session, self.config)
        failures = [
            f"Dag id `{dag_id}` does not match pattern `{self.pattern.pattern}`"
            for dag_id in sorted(dag_bag.dags)
            if not self.pattern.search(dag_id)
        ]
        if failures:
            raise SmokeCheckFailure("\n\n".join(failures))

    def reportinfo(self) -> tuple[str, int, str]:
        """Locate this item for terminal and junit reporting.

        Returns:
            tuple[str, int, str] containing path, line, and title.
        """

        return self.nodeid, 0, self.name


class RequiredDagTagsItem(pytest.Item):
    """Fail when a Dag is missing a required tag."""

    def __init__(self, *, name: str, parent: SmokeCollector, tags: frozenset[str]) -> None:
        """Create the item and mark it as a bundled smoke test.

        Parameters:
            name: str containing the pytest item name.
            parent: SmokeCollector that collected this item.
            tags: frozenset[str] every Dag must carry.
        """

        super().__init__(name=name, parent=parent)
        self.tags = tags
        self.add_marker(pytest.mark.smoke)

    def runtest(self) -> None:
        """Check every Dag's tags for the required set.

        Raises:
            SmokeCheckFailure: A Dag is missing one or more required tags.
        """

        dag_bag = _smoke_dag_bag(self.session, self.config)
        failures: list[str] = []
        for dag_id, dag in sorted(dag_bag.dags.items()):
            missing = self.tags - dag.tags
            if missing:
                failures.append(f"Dag `{dag_id}` is missing required tags: {sorted(missing)}")
        if failures:
            raise SmokeCheckFailure("\n\n".join(failures))

    def reportinfo(self) -> tuple[str, int, str]:
        """Locate this item for terminal and junit reporting.

        Returns:
            tuple[str, int, str] containing path, line, and title.
        """

        return self.nodeid, 0, self.name


class ForbidDefaultOwnerItem(pytest.Item):
    """Fail when a task is owned by Airflow's stock default owner."""

    def __init__(self, *, name: str, parent: SmokeCollector) -> None:
        """Create the item and mark it as a bundled smoke test.

        Parameters:
            name: str containing the pytest item name.
            parent: SmokeCollector that collected this item.
        """

        super().__init__(name=name, parent=parent)
        self.add_marker(pytest.mark.smoke)

    def runtest(self) -> None:
        """Check every task's owner against Airflow's stock default.

        Raises:
            SmokeCheckFailure: A task is owned by the stock `airflow` owner.
        """

        dag_bag = _smoke_dag_bag(self.session, self.config)
        failures: list[str] = []
        for dag_id, dag in sorted(dag_bag.dags.items()):
            for task in dag.tasks:
                if task.owner == DEFAULT_OWNER:
                    failures.append(
                        f"Dag `{dag_id}` task `{task.task_id}` is owned by the stock "
                        f"`{DEFAULT_OWNER}` owner"
                    )
        if failures:
            raise SmokeCheckFailure("\n\n".join(failures))

    def reportinfo(self) -> tuple[str, int, str]:
        """Locate this item for terminal and junit reporting.

        Returns:
            tuple[str, int, str] containing path, line, and title.
        """

        return self.nodeid, 0, self.name


class SmokeCollector(pytest.Collector):
    """Collect the bundled smoke catalog directly on the pytest session."""

    def collect(self) -> Iterator[pytest.Item]:
        """Yield every enabled bundled smoke item.

        Returns:
            Iterator[pytest.Item] containing the collected smoke items.
        """

        yield DagBagIntegrityItem.from_parent(self, name="test_dag_bag_integrity")
        yield DagSerializationRoundtripItem.from_parent(
            self, name="test_dag_serialization_roundtrip"
        )
        yield NoDuplicateDagIdsItem.from_parent(self, name="test_no_duplicate_dag_ids")
        yield ScheduleSanityItem.from_parent(self, name="test_schedule_sanity")
        yield PoolReferencesExistItem.from_parent(self, name="test_pool_references_exist")

        pattern = _dag_id_pattern(self.config)
        if pattern is not None:
            yield DagIdPatternItem.from_parent(
                self,
                name="test_dag_id_pattern",
                pattern=pattern,
            )
        required_tags = _required_dag_tags(self.config)
        if required_tags:
            yield RequiredDagTagsItem.from_parent(
                self,
                name="test_required_dag_tags",
                tags=required_tags,
            )
        if _forbid_default_owner(self.config):
            yield ForbidDefaultOwnerItem.from_parent(self, name="test_forbid_default_owner")


def collect_smoke_items(
    session: pytest.Session, config: pytest.Config, items: list[pytest.Item]
) -> None:
    """Append the bundled smoke catalog to collected items when enabled.

    Parameters:
        session: pytest.Session that owns the synthetic smoke collector.
        config: pytest.Config containing plugin options and ini values.
        items: list[pytest.Item] mutated to include enabled smoke items.
    """

    if not _smoke_enabled(config):
        return
    collector = SmokeCollector.from_parent(session, name="smoke")
    items.extend(collector.collect())


__all__ = (
    "DagBagIntegrityItem",
    "DagIdPatternItem",
    "DagSerializationRoundtripItem",
    "ForbidDefaultOwnerItem",
    "NoDuplicateDagIdsItem",
    "PoolReferencesExistItem",
    "RequiredDagTagsItem",
    "ScheduleSanityItem",
    "SlowDagParseWarning",
    "SmokeCheckFailure",
    "SmokeCollector",
    "collect_smoke_items",
)
