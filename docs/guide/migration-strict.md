# Migration-strict mode

`--airflow-migration-strict` turns an Airflow 2.11 test run into a forecast of 3.x breakage,
with no 3.x environment needed. Airflow 2.11 is deliberately saturated with deprecation
warnings pointing at what changes in 3.x; error-promoting the right two categories during the
runtest phase surfaces exactly the code paths a real 3.x migration will break, today, on the
2.x environment already in CI:

```console
pytest --airflow-migration-strict
```

or persistently via the `airflow_migration_strict` ini option. The command-line flag wins when
given; otherwise the ini value decides.

Composes with the Airflow 2.x compatibility tier: predict a 3.x break here, verify the fix
against a real 3.x install separately. Single environment, no second Airflow install, matches
this plugin's existing "deprecations stay visible by design" stance.

## What gets promoted

Exactly two categories, both importable on every certified Airflow 2.x release and both
subclassing `DeprecationWarning`:

- `airflow.exceptions.RemovedInAirflow3Warning`
- `airflow.exceptions.AirflowProviderDeprecationWarning`

Plain `DeprecationWarning` is deliberately excluded. A lot of it reaches a test through
Airflow's own call frames without being an Airflow-authored migration signal -- third-party
library noise, stdlib deprecations, and the like. Only Airflow's own two public deprecation
categories are trustworthy enough to fail a test over.

## Test-phase only

Error promotion applies to the runtest phase only, never to collection or bootstrap. Airflow
2.11 emits the very same two categories from its own modules during import and Dag parsing --
an unqualified `error::` filter over them would abort the session before a single test ran.

pytest re-reads the `filterwarnings` ini list separately per phase, so the plugin adds its
filters after collection finishes rather than at configure time: absent during collection's own
warning context, present for every runtest-phase warning context. A module-level deprecation
warning at Dag-file import time is reported, not fatal; the exact same warning raised inside a
test body fails that test.

## Allowlisting a specific warning

No bespoke allowlist option exists, because pytest's own `filterwarnings` precedence already
does the job: the plugin *prepends* its two error filters below every user-supplied line, and
pytest applies `filterwarnings` lines in order with later lines winning. A later, more specific
line downgrades the plugin's default:

```ini
[pytest]
filterwarnings =
    ignore:some specific known-fine message:airflow.exceptions.RemovedInAirflow3Warning
```

or per test:

```python
@pytest.mark.filterwarnings("ignore::airflow.exceptions.RemovedInAirflow3Warning")
def test_uses_a_known_deprecation(): ...
```

## Airflow 3.x and no-Airflow environments

There is nothing to forecast off the Airflow 2.x family: the flag becomes a no-op, reported
once via a `MigrationStrictNoOpWarning` at configure time so an enabled flag left over from a
2.x-only CI leg is never silently inert. `--airflow-doctor` also reports whether the mode is
enabled and, when it is enabled off 2.x, that it is currently a no-op.
