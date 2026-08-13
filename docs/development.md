# Development

```console
uv sync
uv run prek install
make all
```

The `dev` dependency group carries Airflow 3.x, so a plain `uv sync` is a working 3.x
environment. To experiment against an Airflow 2.x resolution instead (the `airflow2` extra
conflicts with the default `dev` group by design):

```console
uv sync --no-default-groups --extra airflow2
```

Run the GitHub Actions workflow locally on Linux with [act](https://nektosact.com/):

```console
act pull_request
```

`act` cannot reproduce native macOS or Windows behavior.

## Compatibility suite

The repository's `tests/enduser/` suite is a sanitized consumer-style catalog run on every
certified matrix leg. It covers custom operators, TaskFlow and mapping, hooks and connections,
SQLite provider SQL, sensors, deferral, callbacks and retries, assets, provider-shaped packages,
DagBag/collection, logging, xdist, and REST API CRUD. The provider-shaped corpus verifies user
package composition and execution; registering a real provider distribution entry point remains
out of scope because that is Airflow's packaging surface rather than this plugin's test surface.
