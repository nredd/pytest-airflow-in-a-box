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
| `all` | Always keep the run directory |
| `failed` | Keep it when the session did not end cleanly (the default) |
| `none` | Always remove it |

A kept directory is reported again at the end of the run, next to the failures that made you
want it:

```console
=============================== airflow-in-a-box ===============================
Retained AIRFLOW_HOME (airflow_home_retention_policy=failed): /dev/shm/pytest-airflow-in-a-box-8f2a1c
WARNING: '/dev/shm' is RAM-backed, so this directory holds memory until it is removed or the machine reboots. Pass `--airflow-home=PATH` to put the run on durable storage instead.
```

Retention is deliberately biased toward keeping too much rather than too little. Every exit
status other than a clean pass counts as a failure -- interrupted, internal error, usage error
-- and a run that died before pytest could report an outcome at all is treated as a failure
too, because the run that crashed is exactly the one whose state you want to look at. The one
carve-out is "no tests collected": nothing ran, so nothing touched the directory and there is
nothing in it to inspect. `--airflow-doctor` never retains either, since it prints its report
and exits without running a session.

Retention never leaks a database server. The `--airflow-db-backend=postgres` container is
stopped on every policy, `all` included; only the directory removal is conditional. A retained
Postgres run therefore keeps its `airflow.cfg` and logs but not a live database.

Nothing else changes with the policy: cleaning up a retained directory is your job, and
`--airflow-home-retention=all` on a long CI matrix will fill a disk (or, on `/dev/shm`, memory)
if nothing prunes it.

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
