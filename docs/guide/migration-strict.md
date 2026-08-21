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

## Pairing with ruff's AIR rules

ruff's Airflow ruleset (`AIR301`/`AIR302` for hard 3.0 removals and core-to-provider moves,
`AIR311`/`AIR312` for suggested updates that still have a 3.0 compat layer) attacks the same
2.x -> 3.x problem this plugin does, one layer earlier. The three layers compose as a funnel,
cheapest first:

| Layer                        | Sees                                                       | Misses                                                     |
|------------------------------|------------------------------------------------------------|------------------------------------------------------------|
| `AIR3xx` (ruff)              | Every symbol the pinned ruff knows is bad, executed or not | Provider-issued deprecations, anything spelled dynamically |
| `--airflow-migration-strict` | Airflow's own deprecation warnings on executed paths       | Code no test reaches, breakage Airflow never warned about  |
| `airflow-migration-diff`     | Real pass/fail on a real 3.x install                       | Anything your tests never exercise, or a renamed nodeid    |

Run all three. The funnel does not narrow monotonically, though -- each layer is blind along a
*different* axis, and the last one does not subsume the first two. Only ruff sees code no test
executes, so a fully green [`airflow-migration-diff`](migration-orchestrator.md) proves nothing
about the untested half of your Dags. What that layer costs is two provisioned environments per
run, which is why it goes last rather than first.

Where the layers do overlap, the overlap is confirmation, not conflict: ruff never executes your
code and the plugin never parses your source, so there is no code-level interaction to reason
about. A symbol ruff flags and a warning `--airflow-migration-strict` promotes are two
independent witnesses to the same break. The one real coordination cost is a deprecation you
have deliberately accepted -- it has to be waived once per layer, in `filterwarnings` for the
plugin (see above) and in ruff's own config for the linter.

Start with the removal tier as errors, and leave the suggestion tier off until the cutover (see
the next section for why). `extend-select`, not `select` -- `select` replaces the list rather
than adding to it, and would silently drop the rest of your rules:

```toml
[tool.ruff.lint]
extend-select = ["AIR301", "AIR302"]
```

`AIR301`, `AIR302`, `AIR311`, and `AIR312` are all stable as of ruff 0.16.1 -- none of them
needs `preview = true`. Six other `AIR` rules are still preview-gated (`AIR003`, `AIR004`,
`AIR201`, `AIR202`, `AIR304`, `AIR321`), so a bare `select = ["AIR"]` quietly enables the seven
stable rules only.

## AIR311/AIR312 autofixes break a dual-family suite

The suggestion tier rewrites imports to Airflow 3 spellings, and those spellings do not exist on
2.x. `AIR311`'s `airflow.Dataset` -> `airflow.sdk.Asset` fix is classified *safe*, so a bare
`ruff check --fix` applies it with no `--unsafe-fixes` opt-in:

```console
ruff check --select AIR --fix --diff example.py
--- example.py
+++ example.py
@@ -1,2 +1,3 @@
 from airflow import Dataset
-ds = Dataset("s3://bucket/key")
+from airflow.sdk import Asset
+ds = Asset("s3://bucket/key")

Would fix 1 error.
```

There is no `airflow.sdk` on Airflow 2.x, and the blast radius is wider than the tests you
marked [`requires_airflow2`](../reference/markers.md). A rewritten Dag file fails its own
Dag-file import item and poisons `dag_bag` for every test that parses the whole corpus; a
rewrite that lands in a test module or a shared helper is a plain pytest collection error. The
family markers gate *execution*, not import -- they cannot rescue a file that no longer parses.

Note also that ruff adds the new import rather than rewriting the old one; the now-unused
`from airflow import Dataset` is left for `F401` to clean up, so the file is briefly wrong on
*both* families in between. `AIR302`/`AIR312` fixes are unsafe-only and so need an explicit
opt-in, but setting `unsafe-fixes = true` in `[tool.ruff]` is enough of an opt-in to make them
apply too.

So: `AIR301`/`AIR302` as errors immediately, since a removed symbol is broken on 3.x no matter
when you look at it, and `AIR311`/`AIR312` deferred until the 3.x cutover is the last thing
left. The autofix hazard is the loud reason, but not the real one -- the real one is that the
suggestion tier is *unactionable* on a dual-family codebase. Any compliant rewrite breaks 2.x,
by hand exactly as it does by autofix, so gating on those two rules means a permanently red
build with no legal way to turn it green. Come the cutover both facts invert at once and the
autofixes become the fastest way to land the rewrite.

If you want the suggestion tier *visible* in the meantime without the rewrite risk, select it
and mark it unfixable rather than deselecting it -- the diagnostic still reports, and no `--fix`
run (safe or unsafe) will touch it:

```toml
[tool.ruff.lint]
extend-select = ["AIR301", "AIR302", "AIR311", "AIR312"]
unfixable = ["AIR311", "AIR312"]
```

Keep that one out of the gating job, though; it is an inventory of pending work, not a pass/fail
signal.

This repo's own Dag corpus works around exactly this. Every family-divergent symbol in
`tests/dags/` is imported through a dynamic
[`_resolve()`](https://github.com/nredd/pytest-airflow-in-a-box/blob/main/tests/dags/_family.py)
helper that tries the 3.x module path and falls back to the 2.x one, so the same files parse
under both families. The cost is that `ruff check --select AIR tests/dags/` reports nothing at
all -- not because the corpus is clean, but because no `AIR` rule can see through
`import_module`. That blind spot is precisely what `--airflow-migration-strict` covers: the
dynamic import resolves at runtime, and whatever deprecation the resolved symbol emits gets
promoted like any other.
