# The isolated AIRFLOW_HOME

Every run gets its own throwaway `AIRFLOW_HOME`, created before any consumer conftest is
imported and torn down when the session ends. The plugin names it in the session header:

```console
$ pytest
============================= test session starts ==============================
platform linux -- Python 3.12.13, pytest-9.1.1, pluggy-1.6.0
pytest-airflow-in-a-box: AIRFLOW_HOME=/dev/shm/pytest-airflow-in-a-box-8f2a1c (storage: shared-memory, db: sqlite)
rootdir: /home/redd/dags
```

That directory holds `airflow.cfg`, `dags/`, `logs/`, `config/airflow_local_settings.py`, the
SimpleAuthManager password file, and -- on the default backend -- the SQLite metadata database.
pytest suppresses its own header under `-q` and `--no-header`, so the line goes with it; under
`xdist` only the controller prints, because workers inherit the controller's directory rather
than creating their own.

## Where it lands

The base directory comes off a five-rung ladder, and the header's `storage:` field names the
rung that won:

| Rung | Base | When |
| --- | --- | --- |
| `explicit` | `--airflow-home=PATH` or the `airflow_home` ini option | Always, when supplied |
| `caller-temp` | `--basetemp`'s parent, else `$TMPDIR` | The caller already picked a temp base |
| `shared-memory` | `/dev/shm` | Linux, tmpfs, and at least 512 MiB free |
| `system-temp` | `tempfile.gettempdir()` | The ordinary fallback |
| `writable-fallback` | A writable network or unknown-filesystem base | Nothing local was writable; warns loudly |

`shared-memory` is the surprising one. On most Linux hosts it wins, which makes runs fast and
makes "where did my logs go?" a fair question -- the answer is RAM, and it is gone at reboot.
Pin the run somewhere durable when that matters:

```console
pytest --airflow-home=/var/tmp/airflow-runs
```

The explicit base is never removed; only the unique `pytest-airflow-in-a-box-*` child inside it
is. A network or otherwise unclassifiable explicit base is rejected unless you pass
`--allow-network-airflow-home`, because Airflow's SQLite metadata database on a network
filesystem corrupts under lock contention rather than failing cleanly.

## Keeping it after the run

The retention policy mirrors pytest's own `tmp_path_retention_policy`, vocabulary and default
alike:

```console
pytest --airflow-home-retention=all
```

or persistently via the `airflow_home_retention_policy` ini option. The command-line flag wins
when given; otherwise the ini value decides.

| Value | Behavior |
| --- | --- |
| Value | Behavior |
| --- | --- |
| `all` | Always keep the run directory |
| `failed` | Keep it when a session ran and did not end cleanly (the default) |
| `none` | Always remove it |

A kept directory is reported again at the end of the run, next to the failures that made you
want it:

```console
=============================== airflow-in-a-box ===============================
Retained AIRFLOW_HOME (retention policy: failed): /dev/shm/pytest-airflow-in-a-box-8f2a1c
WARNING: '/dev/shm' is RAM-backed, so this directory holds memory until it is removed or the machine reboots. Pass `--airflow-home=PATH` to put the run on durable storage instead.
```

Some runs never get that far. A crash in `pytest_sessionstart`, an internal error -- pytest
prints no header on those, and its terminal summary does not run for every exit status either.
Those are precisely the runs most likely to keep their directory, so the same line goes to
`stderr` instead, prefixed with the plugin name. A kept directory is never kept silently.

Under `failed`, retention is biased toward keeping too much rather than too little: every exit
status other than a clean pass counts, interrupted and internal error included, and a run that
started and died before pytest could report an outcome at all is treated as a failure too --
the run that crashed is exactly the one whose state you want to look at.

What does *not* count is an invocation that never started a session. `pytest --help`,
`pytest --markers`, an argparse usage error, an abort during `pytest_configure`, and
`--airflow-doctor` all bootstrap an `AIRFLOW_HOME` and none of them runs a test, so none of them
keeps one. "No tests collected" is the same idea: nothing ran, so nothing touched the directory.
`--airflow-home-retention=all` still means all, `--airflow-doctor` included, if you want to keep
the tree the diagnostic report just named.

Retention never leaks a database server. The `--airflow-db-backend=postgres` container is
stopped on every policy, `all` included; only the directory removal is conditional. A retained
Postgres run therefore keeps its `airflow.cfg` and logs but not a live database.

Nothing else changes with the policy: cleaning up a retained directory is your job. There is no
retention *count* the way pytest caps `tmp_path` directories at `tmp_path_retention_count`, so a
long CI matrix under `--airflow-home-retention=all`, or a stubbornly red suite under `failed`,
will fill a disk (or, on `/dev/shm`, memory) if nothing prunes it.

One known gap: pytest raises the exit status to `MAX_WARNINGS_ERROR` after every
`pytest_sessionfinish` hook has already run, so a run that fails only because it breached
`--max-warnings` is recorded as the clean status pytest reported at the time and its directory
is removed under `failed`. Pass `--airflow-home-retention=all` when you need to inspect one.

## Reaching it from a test

The `airflow_home_path` fixture returns the directory as a `pathlib.Path`, so a test or a
consumer fixture never needs to reach into the plugin's bootstrap internals or re-read
`AIRFLOW_HOME` from the environment:

```python
def test_local_settings_were_written(airflow_home_path):
    assert (airflow_home_path / "config" / "airflow_local_settings.py").is_file()
```

It is session-scoped and imports no Airflow. Under `xdist` every worker reports the controller's
directory, because workers inherit it rather than creating their own. See
[Airflow configuration](configuration.md#where-the-run-lives) for the companion
`airflow_dags_folder_path`.

## Pairing with report artifacts

`--airflow-report-dir` and a retained `AIRFLOW_HOME` are the two halves of "give me the
artifacts", and they stay independent on purpose -- the home is never copied into the report
directory, because it can be large and, on `/dev/shm`, RAM-backed. Point both at one place when
you want a single archivable path:

```console
pytest --airflow-home=./artifacts --airflow-home-retention=all --airflow-report-dir=./artifacts
```

## Inspecting it without running tests

[`--airflow-doctor`](../reference/diagnostics.md) prints the resolved root, its storage rung,
and the database backend without collecting anything. It bootstraps its own run directory to do
so, so the path it reports is that diagnostic run's, not a previous session's -- use the header
line or the retained-directory summary when you need the path of a run that actually executed
tests.
