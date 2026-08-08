"""Provider-shaped custom operator."""

from __future__ import annotations

from typing import Any

from airflow.sdk import BaseOperator

from provider_package._hook import ExampleHook


class ExampleOperator(BaseOperator):
    """Execute through the adjacent custom hook."""

    def execute(self, context: Any) -> dict[str, bool]:
        del context
        return ExampleHook().get_conn()
