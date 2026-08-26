# Where the run lives

Your own `~/airflow` is never touched. The plugin points `AIRFLOW_HOME` at a fresh
throwaway directory before any consumer `conftest.py` is imported and before Airflow is
imported at all, so no test run migrates your dev metadata database, seeds your Variables,
writes to your `logs/`, or reads a stale `airflow.cfg` you forgot you edited. Laptop and CI
get the same tree.

That directory holds `airflow.cfg`, `dags/`, `logs/`, `plugins/`,
`config/airflow_local_settings.py`, the SimpleAuthManager password file, and -- on the
default backend -- the SQLite metadata database. It is removed when the session ends,
unless you ask for it back.

The plugin names it in the session header:

```console
$ pytest
============================= test session starts ==============================
platform linux -- Python 3.12.13, pytest-9.1.1, pluggy-1.6.0
pytest-airflow-in-a-box: AIRFLOW_HOME=/dev/shm/pytest-airflow-in-a-box-8f2a1c (storage: shared-memory, db: sqlite)
rootdir: /home/redd/dags
```

pytest suppresses its own header under `-q` and `--no-header`, so the line goes with it;
under `xdist` only the controller prints, because workers inherit the controller's
directory rather than creating their own.

## Where it lands

The base directory comes off a five-rung ladder, and the header's `storage:` field names
the rung that won:

| Rung | Base | When |
| --- | --- | --- |
| `explicit` | `--airflow-home=PATH` or the `airflow_home` ini option | Always, when supplied |
| `caller-temp` | `--basetemp`'s parent, else `$TMPDIR` | The caller already picked a temp base |
| `shared-memory` | `/dev/shm` | Linux, tmpfs, and at least 512 MiB free |
| `system-temp` | `tempfile.gettempdir()` | The ordinary fallback |
| `writable-fallback` | A writable network or unknown-filesystem base | Nothing local was writable; warns loudly |

`shared-memory` is the surprising one. On most Linux hosts it wins, which makes runs fast
and makes "where did my logs go?" a fair question -- the answer is RAM, and it is gone at
reboot. Pin the run somewhere durable when that matters:

```console
pytest --airflow-home=/var/tmp/airflow-runs
```

The explicit base is never removed; only the unique `pytest-airflow-in-a-box-*` child
inside it is. A network or otherwise unclassifiable explicit base is rejected unless you
pass `--allow-network-airflow-home`, because Airflow's SQLite metadata database on a
network filesystem corrupts under lock contention rather than failing cleanly.

## Keeping it after the run

The retention policy mirrors pytest's own `tmp_path_retention_policy`, vocabulary and
default alike:

```console
pytest --airflow-home-retention=all
```

or persistently via the `airflow_home_retention_policy` ini option. The command-line flag
wins when given; otherwise the ini value decides.

| Value | Behavior |
| --- | --- |
| `all` | Always keep the run directory |
| `failed` | Keep it when a session ran and did not end cleanly (the default) |
| `none` | Always remove it |

A kept directory is reported again at the end of the run, next to the failures that made
you want it:

```console
=============================== airflow-in-a-box ===============================
Retained AIRFLOW_HOME (retention policy: failed; 2 other retained roots kept): /dev/shm/pytest-airflow-in-a-box-8f2a1c
WARNING: '/dev/shm' is RAM-backed, so this directory holds memory until it is removed or the machine reboots. Pass `--airflow-home=PATH` to put the run on durable storage instead.
```

### How retention decides

- Under `failed`, "did not end cleanly" is read generously: every non-passing exit status
  counts, interrupted and internal error included, and so does a run that died before
  pytest could report an outcome at all. The run that crashed is the one whose state you
  want
- An invocation that never started a session never keeps one. `pytest --help`,
  `--airflow-doctor`, an argparse usage error, an abort in `pytest_configure`, and "no
  tests collected" all bootstrap a directory and run no test. `--airflow-home-retention=all`
  still means all, `--airflow-doctor` included
- A crash before the header, or an exit status whose terminal summary never runs, still
  announces the kept path -- on `stderr`, prefixed with the plugin name. A kept directory
  is never kept silently
- Retained roots are bounded, mirroring `tmp_path_retention_count`:
  `--airflow-home-retention-count=N` or the `airflow_home_retention_count` ini option,
  default `3`. Anything past the `N` most recent retained roots under the same storage base
  and owned by the same user is pruned. In-progress runs and other users' roots are never
  candidates. Without the bound, a red suite or a long `all` matrix fills a disk -- or, on
  `/dev/shm`, memory
- Retention never leaks a database server. The `--airflow-db-backend=postgres` container is
  stopped on every policy, `all` included; only the directory removal is conditional
- One known gap: pytest raises the exit status to `MAX_WARNINGS_ERROR` *after* every
  `pytest_sessionfinish` hook has run, so a run that fails only on `--max-warnings` is
  recorded as the clean status pytest reported at the time, and the default `failed`
  policy removes its directory anyway. Pass `--airflow-home-retention=all` to inspect one

## Reaching it from a test

The session-scoped `airflow_home` fixture returns the directory as a `pathlib.Path`. See
[Fixtures](../reference/fixtures.md) for it and the companion `airflow_dags_folder`.

## Pairing with report artifacts

`--airflow-report-dir` and a retained `AIRFLOW_HOME` are the two halves of "give me the
artifacts", and they stay independent on purpose -- the home is never copied into the
report directory, because it can be large and, on `/dev/shm`, RAM-backed. Point both at one
place when you want a single archivable path:

```console
pytest --airflow-home=./artifacts --airflow-home-retention=all --airflow-report-dir=./artifacts
```

See [Logs and JUnit XML you can trust](reports.md).

## Inspecting it without running tests

[`--airflow-doctor`](../reference/diagnostics.md) prints the resolved root, its storage
rung, and the database backend without collecting anything. It bootstraps its own run
directory to do so, so the path it reports is that diagnostic run's, not a previous
session's -- use the header line or the retained-directory summary when you need the path
of a run that actually executed tests.
