"""Apply `airflow_config` overrides for the remainder of a test session.

``config.airflow_config`` is scoped to a ``with`` block. Repo-wide defaults want a longer life,
and consumers were writing the same session-scoped wrapper by hand -- including the ``yield``
that makes teardown actually restore. This fixture is that wrapper, owned by the plugin.

It is the *runtime* half of a pair. Anything expressible as a literal belongs in the
``airflow_config`` ini option instead (see ``ini_config``), which lands before any consumer
conftest is imported and therefore before any Dag parse can observe it. This fixture exists for
the values a config file cannot hold: a path from ``tmp_path_factory``, a port from a fixture,
a credential minted for the run.

References:
    https://docs.pytest.org/en/stable/how-to/fixtures.html
    https://docs.python.org/3/library/contextlib.html#contextlib.ExitStack
"""

from __future__ import annotations

from contextlib import ExitStack
from typing import TYPE_CHECKING

import pytest

from pytest_airflow_in_a_box.config import ConfigOverrides, EnvOverrides, airflow_config

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pytest_airflow_in_a_box.types import AirflowConfigure


@pytest.fixture(scope="session")
def airflow_configure() -> Iterator[AirflowConfigure]:
    """Yield a callable applying `airflow_config` overrides until session teardown.

    Each call enters a fresh ``airflow_config`` context on one session-lifetime
    ``ExitStack``, so batches compose and unwind last-in-first-out at teardown, and every name
    is restored exactly -- one absent beforehand is deleted rather than emptied. Arguments and
    their validation are ``airflow_config``'s, unchanged.

    Ordering, which is the whole reason consumers hand-rolled this: pytest puts autouse names
    ahead of requested ones in a test's fixture closure and instantiates in that order, so a
    consumer's own ``scope="session", autouse=True`` wrapper around this fixture is applied
    before a session-scoped ``full_dag_bag`` that the same test requests. Two caveats follow
    from that being a *per-test* guarantee:

    - A function-scoped wrapper is instantiated *after* every session-scoped fixture, so it
      cannot precede a Dag parse.
    - ``full_dag_bag`` parses once per worker process and caches. A test collected earlier and
      outside the wrapper's conftest scope can win the parse, after which no override reaches
      it.

    For configuration that must precede the first Dag parse unconditionally, use the
    ``airflow_config`` ini option instead. It is applied before any consumer conftest is even
    imported.

    Do not reconfigure anything the API server already inherited: `api_server_url` and
    `api_client` launch a session-scoped subprocess that snapshots the environment live at
    startup, so an override applied afterward never reaches it and an override applied before
    outlives this fixture's teardown inside that process.

    Yields:
        AirflowConfigure applying one batch of overrides per call.
    """

    with ExitStack() as stack:

        def configure(
            overrides: ConfigOverrides | None = None,
            *,
            env: EnvOverrides | None = None,
            refresh_settings: bool = False,
        ) -> None:
            """Apply one batch of overrides onto the session-lifetime stack.

            Parameters:
                overrides: ConfigOverrides | None mapping ``(section, key)`` pairs to
                    replacement values, where ``None`` requests the option's absence.
                env: EnvOverrides | None mapping plain variable names to replacement values,
                    where ``None`` requests the variable's absence.
                refresh_settings: bool recomputing the ``airflow.settings`` configuration
                    globals after applying and after restoring.

            Raises:
                pytest.UsageError: An argument, name, or value is malformed.
            """

            stack.enter_context(
                airflow_config(overrides, env=env, refresh_settings=refresh_settings)
            )

        yield configure


__all__ = ("airflow_configure",)
