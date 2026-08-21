"""Reach Airflow's live configuration parser and settings globals.

Both helpers are plain deferred-import seams, not capability probes: the
``airflow.configuration.conf`` parser and ``airflow.settings.configure_vars``
are stable across every certified release, and this module only centralizes
their runtime Airflow imports behind ``_compat``. Airflow imports remain
deferred until a helper is called, keeping the module import-safe before
bootstrap.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/howto/set-config.html
    https://github.com/apache/airflow/blob/main/airflow-core/src/airflow/settings.py
"""

from __future__ import annotations

from typing import Any


def airflow_conf() -> Any:
    """Resolve Airflow's live global configuration parser.

    Returns:
        Any containing the ``airflow.configuration.conf`` parser instance.

    Raises:
        ImportError: Airflow is not importable in this environment.
    """

    # Deferred to preserve bootstrap safety and avoid Airflow's module import cost.
    from airflow.configuration import conf

    return conf


def configure_vars() -> None:
    """Recompute the Airflow settings globals derived from configuration.

    ``airflow.settings`` resolves ``SQL_ALCHEMY_CONN``, ``DAGS_FOLDER``, and
    ``PLUGINS_FOLDER`` once at import; upstream's ``configure_vars`` re-derives
    them entirely from the configuration parser, making it idempotent and safe
    to call again after an environment override.

    Raises:
        ImportError: Airflow is not importable in this environment.
    """

    # Deferred to preserve bootstrap safety and avoid Airflow's module import cost.
    from airflow import settings

    settings.configure_vars()


__all__ = (
    "airflow_conf",
    "configure_vars",
)
