# Supported Airflow and Python versions

Run `pytest --airflow-doctor` and read the answer for the environment you actually have. This
page is the map behind that answer.

## What the pin needs to be

- **Apache Airflow** 3.1 or newer, below 4
- **CPython** 3.10 through 3.14
- **pytest** 8 or newer
- **Linux or macOS.** Airflow has no native Windows support -- use WSL2 or the devcontainer

Thirteen Airflow 3.x releases carry a *certified* capability row: 3.1.0, 3.1.1, 3.1.2, 3.1.3,
3.1.5, 3.1.6, 3.1.7, 3.1.8, 3.2.0, 3.2.1, 3.2.2, 3.3.0, 3.3.1. On those, every capability the
plugin probes for has been byte-verified against the recorded row.

## An uncertified 3.x release degrades, it does not brick

A 3.x release at or above 3.1.0 with no certified row -- a fresh upstream release, or a patch
in a gap -- resolves by *pure probing*. Capabilities become live runtime observations, the
structural floor still has to hold, and the session emits one
`UncertifiedAirflowWarning` naming the last certified release. Your suite still runs.

Below 3.1.0, or outside major 3, the plugin hard-fails. Those are known-unsupported, not
unknown.

A weekly canary workflow runs the probe report against the newest upstream release, so
certification work gets filed before you meet the degraded tier.

## What CI actually exercises

Not the cross product. `compat.yml` is an explicit list of legs, one Python per Airflow
release plus deliberate outliers:

- CPython 3.10: 3.1.0, 3.1.6
- CPython 3.11: 3.1.2, 3.1.7, 3.1.8
- CPython 3.12: 3.1.3, 3.1.8, 3.2.2, 3.3.0
- CPython 3.13: 3.1.5, 3.2.0, 3.2.1, 3.3.1
- CPython 3.14: 3.3.0, the serial reference leg
- Outliers, all on 3.3.0: `pytest==8.0.0` (the floor), macOS, `ubuntu-24.04-arm`, Alpine musl

3.x legs run the whole suite under `-n auto --dist loadgroup`. A separate job runs the
Postgres backend against real Docker. Coverage is enforced as the union across every leg.

So: 3.10 through 3.14 is the range the plugin *supports* and guards at runtime. It is not a
claim that all 65 combinations run in CI.

## Airflow 2.x is a migration bridge, not a second home

Say it plainly, because the rest of this docset used to say it three different ways: **this is
an Airflow 3 plugin.** The 2.x tier exists to get a repo *off* 2.x with one suite that stays
green on both sides of the cut. It is not a permanent harness, and if you have no upgrade
planned it is not what you want.

What is real about it:

- Five certified releases, each with its own Python ceiling: 2.7.3 and 2.8.4 cap at CPython
  3.11 (they publish no 3.12 constraints file); 2.9.3, 2.10.5, and 2.11.2 reach 3.12. Airflow
  2.x never supported 3.13, and the `airflow2` extra carries that marker
- These fixtures run on both families: `dag_maker` (including `dag_maker.run()`), `run_ti`,
  `dag_bag`, `run_dag`, `clear_db`, seeding, the configuration surface (`airflow_config`,
  `airflow_configure`, `airflow_home`, `airflow_dags_folder`), and the bundled smoke checks
- `requires_airflow2` and `requires_airflow3` auto-skip on the other family, so one suite runs
  both sides. See [Markers](reference/markers.md)

What is narrower about it:

- The 2.x legs run **only** `tests/enduser`, the consumer contract, on Linux against SQLite.
  The plugin's own internal suite does not collect on 2.x
- `run_task`, `render_task`, `task_context`, and `cap_structlog` fail on 2.x with an error
  naming the 2.x alternative -- they drive Task SDK machinery 2.x predates
- The REST API fixtures fail on 2.x: there is no `airflow api-server`
- `run_dag(executor=...)` fails on 2.x. Driving a DagRun through an executor is AIP-72
  machinery -- workloads, the Task Execution API -- that Airflow 2.x does not have

If you are on 2.x today, the tier is a means to an end and the end is documented:
[Migrating from Airflow 2 to 3](guide/migration/index.md).

CI runs the 2.x legs serially by default -- they finish in a minute or two, where a per-worker
Airflow bootstrap costs more than the fan-out returns -- with one 2.11.2 leg under `-n 2` to
keep the parallel path honest.
