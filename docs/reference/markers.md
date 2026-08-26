# Markers

Every marker here is a *gate*: it decides whether a test runs at all, and what the run has
to pay for before it does. `pytest --markers` prints the same names with none of that, so
this page is the vocabulary and the ordering.

Gate precedence, the order the plugin applies them in `pytest_runtest_setup`:

1. Family -- `requires_airflow2` / `requires_airflow3`
2. Environment -- `environment(name)`
3. Isolation refusal -- `airflow_isolated` under xdist
4. Database -- `db_test` / `api_test`

The order is the point. Family and environment gates run *before* the database check, so a
test that is going to skip never pays the Airflow import or the metadata migration. The same
ordering runs a second time at the end of collection: `_requires_database_at_collection`
applies the family and environment gates to each database-marked item, and if every one of
them would skip, the run initializes no database at all.

## Family gates: `requires_airflow2`, `requires_airflow3`

Run this test only on the named Airflow family, auto-skip elsewhere. This is the marker pair
that lets one suite stay green on both sides of a 2 -> 3 upgrade -- see
[Migrating from Airflow 2 to 3](../guide/migration/strict.md).

```python
import pytest


@pytest.mark.requires_airflow3
def test_asset_scheduling(dag_maker) -> None: ...


@pytest.mark.requires_airflow2
def test_legacy_smart_sensor() -> None: ...
```

Classification comes from `installed_family()`, an import-free probe keyed on the *core*
distribution: `apache-airflow-core` present means the 3.x family, and only when it is absent
does the probe fall back to `apache-airflow`'s major version. The obvious hand-rolled
version -- `skipif(metadata.version("apache-airflow") < "3", ...)` -- raises
`PackageNotFoundError` on a core-only Airflow 3 install, so it classifies a perfectly good
3.x environment as "no Airflow" and silently skips the tests you wrote for it. An
Airflow-free environment skips *both* directions, since neither requirement can hold.

The probe never imports Airflow, so gating costs nothing.

`would_family_gate()` recomputes the same condition semantically -- it does not parse a skip
reason -- so the [migration outcome diff](../guide/migration/outcome-diff.md)'s
`--airflow-record` can tag an outcome `gated` and never mistake an environment-caused skip
on a family-marked test for a family gate.

## Environment gate: `environment(name)`

Run this test only where a named sentinel path exists. The use case is a test that needs
something the repo cannot provision: a mounted share, a VPN-reachable warehouse, a
credentials file that only lives on the lab box.

Declare the names and their sentinels once, as an `airflow_environments` ini line list:

```ini
[pytest]
airflow_environments =
    lab = /opt/lab/sentinel
    warehouse = fixtures/warehouse/.ready
```

Grammar, each line:

- `name = path`. A line missing the `=`, the name, or the path is a `pytest.UsageError`
- Relative paths resolve against the pytest rootpath, absolute paths are taken as-is
- A duplicated name is a `pytest.UsageError`

Then mark the test with exactly one of those names:

```python
@pytest.mark.environment("lab")
def test_reads_the_lab_share() -> None: ...
```

Behavior:

- Sentinel exists -> the test runs
- Sentinel missing -> skipped, with the reason naming the path it looked for
- Name not in `airflow_environments` -> `pytest.UsageError` listing the configured names.
  A typo is an error, *not* a silent skip
- Zero or more than one argument, a keyword argument, or a non-string name ->
  `pytest.UsageError`

Nothing checks *what* the sentinel is. Existence is the whole contract, so a directory, a
marker file, or a mount point all work.

## Database gates: `db_test`, `api_test`

These are cost gates, not capability declarations. The metadata database is migrated lazily,
once, and only because something asked for it -- these markers are one of the two things
that ask. The other is a database fixture in the test's closure (`dag_maker`, `run_dag`,
`session`, `api_client`, and friends), which the plugin detects without any marker at all.

- `db_test`: require the isolated metadata database. Marking it triggers the lazy
  initialization -- see [the disposable metadata database](../internals/test-environments.md#the-disposable-metadata-database)
- `api_test`: additionally start the isolated REST API server and publish its URL as
  `AIRFLOW__API__BASE_URL` for the duration of the test, so code under test resolves it
  through `conf.get("api", "base_url")` -- see [talking to a live Airflow
  API](../guide/rest-api.md). Requesting the `api_client` or `api_server_url` fixture does
  the same thing; the marker is for tests that never touch the fixture and reach the server
  through configuration instead

You rarely need `db_test` by hand. Reach for it when a test drives the ORM through your own
helper rather than a plugin fixture, so the closure carries no evidence that a database is
coming.

## Isolation: `airflow_isolated`

```python
@pytest.mark.airflow_isolated(
    entry_points={"airflow.plugins": ("my_plugin = my_pkg.plugin:MyPlugin",)},
    environment={"AIRFLOW__CORE__PARALLELISM": "1"},
    name="my-dist",
    timeout=120.0,
)
def test_plugin_is_discovered() -> None: ...
```

Runs the test in a one-shot child pytest process with a synthetic entry-point distribution on
`PYTHONPATH` and `AIRFLOW__*` overrides applied before the first Airflow import. Every
keyword is optional. Tests sharing a payload share one child process, so cost scales with
distinct isolation environments, not with tests. See [entry points and
packaging](../guide/isolated-tests.md).

Note the collision: this marker's `environment=` keyword is `AIRFLOW__*` variables for the
child, unrelated to the `environment(name)` sentinel gate above. Every override name must
start with `AIRFLOW__`, and a name bootstrap already owns (`AIRFLOW__CORE__DAGS_FOLDER`,
`AIRFLOW__CORE__UNIT_TEST_MODE`, and the rest of the bootstrap-owned names) is a
`pytest.UsageError` -- configure those through the plugin's ini options instead.

The marker is refused on an xdist worker with a `pytest.UsageError`: an isolated child
spawned from a worker would race its siblings on batch scratch directories. Run those tests
serially, or gate them out. A *family- or environment-gated* isolated test skips first and
never trips the refusal, so it behaves the same under `-n auto` as it does serially.

## Selection labels

No gating behavior. These exist so `-m` can select or exclude a group.

- `postgres`: this test needs a provisioned Postgres metadata database (the `postgres`
  extra plus Docker). Nothing skips it for you -- select it, or deselect it with
  `-m "not postgres"`. See [the disposable metadata database](../internals/test-environments.md#the-disposable-metadata-database)
- `compat`: exercises the public plugin surface across certified runtimes. Used by this
  repo's own matrix -- see [Certification](../internals/certification.md#what-ci-actually-exercises)
- `smoke`: carried by the bundled zero-boilerplate corpus checks, which are collected only
  when `--airflow-smoke` / `airflow_smoke` is on. See [smoke checks over every
  Dag](../guide/smoke-tests.md)
- `need_serialized_dag([enabled])`: accepted for upstream compatibility and does nothing.
  Every Dag serializes at persistence, so `dag_maker.serialized_dag` no longer needs it. The
  argument is still validated, so a malformed marker fails loudly rather than lying
