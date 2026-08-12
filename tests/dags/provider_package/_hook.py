"""Provider-shaped custom hook."""

from airflow.sdk import BaseHook


class ExampleHook(BaseHook):
    """Return one deterministic provider value."""

    conn_name_attr = "conn_id"
    default_conn_name = "provider_example"
    conn_type = "example"
    hook_name = "Example"

    def get_conn(self) -> dict[str, bool]:
        return {"connected": True}
