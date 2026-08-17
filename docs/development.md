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

raised from `_pytest/pathlib.py::cleanup_dead_symlinks`, which both sessions reach via
`pytest_sessionfinish` because both share one `$TMPDIR/pytest-of-<user>/` root. The race itself
is pytest's own -- an unguarded `unlink()` on a symlink two processes both see as dead -- but this
plugin's zero-ini defaults make it far likelier to trip than a bare pytest install: `defaults.py`
sets `tmp_path_retention_policy = "failed"` (pytest's own default is `"all"`), so a *passing*
session removes its own numbered directory in `pytest_sessionfinish`, which is exactly the
directory `pytest-current` points to. That leaves `pytest-current` dangling for whichever
concurrent session's cleanup runs next, and the first of the two to call `unlink()` on it wins.
Under pytest's own `all` default the symlink's target is never removed on a pass, so the
dangling state -- and the race -- essentially doesn't arise. The isolated `AIRFLOW_HOME` is not
involved either way -- that is a per-session `mkdtemp` outside `pytest-of-<user>/`.

Two ways to avoid it, cheapest first:

- `pytest -o tmp_path_retention_policy=all` restores pytest's own default for that run, removing
  the dangling-symlink precondition at no storage cost.
- `PYTEST_DEBUG_TEMPROOT=<dir>` relocates pytest's own `pytest-of-<user>/` root without touching
  `TMPDIR`, so it does not affect where `AIRFLOW_HOME` lands (see below).

`TMPDIR` per checkout also works, but it doubles as the `caller-temp` rung of the `AIRFLOW_HOME`
storage ladder in [The isolated AIRFLOW_HOME](guide/airflow-home.md), which outranks the
RAM-backed `shared-memory` rung (`/dev/shm`) that usually wins on Linux -- so it trades that
speed away too. `--basetemp` does not dodge this tradeoff either: this plugin reads its parent
directory as the same `caller-temp` candidate.
