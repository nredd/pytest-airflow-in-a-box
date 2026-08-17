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

## Concurrent local runs

Running two suites from this repo at once on one machine -- two worktrees, or a gate alongside a
scratch run -- can rarely end a session with a non-zero exit code even though every test passed:

```
FileNotFoundError: .../pytest-current
```

raised from `_pytest/pathlib.py::cleanup_dead_symlinks` during `pytest_sessionfinish`. This is
pytest's own tmpdir plugin, not this plugin: both sessions share `$TMPDIR/pytest-of-<user>/` and
both run its numbered-directory garbage collector at session finish, so one process can remove
the `pytest-current` symlink after the other has already stat'd it. The isolated `AIRFLOW_HOME`
is not involved -- that is a per-session `mkdtemp`.

Set `TMPDIR` to a distinct directory per checkout to avoid it. That also affects where
`AIRFLOW_HOME` lands: `TMPDIR` backs the `caller-temp` rung of the storage ladder in
[The isolated AIRFLOW_HOME](guide/airflow-home.md), which outranks the RAM-backed
`shared-memory` rung (`/dev/shm`) that usually wins on Linux -- so a `TMPDIR` override trades
that speed for avoiding the race.
