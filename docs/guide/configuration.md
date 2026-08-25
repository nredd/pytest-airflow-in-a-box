# Overriding Airflow configuration

A 500-Dag repo needs `core.dagbag_import_timeout` raised and `core.dag_ignore_file_syntax =
glob` honoured *before* the first `DagBag` is built. Nothing you write in a `conftest.py` can
do that: pytest's own conftest-collecting hookimpl is `trylast`, so by the time your code runs
the parse may already have happened. The `airflow_config` ini option is applied from
`pytest_load_initial_conftests`, before any consumer conftest is imported:

```ini
# pytest.ini
[pytest]
airflow_config =
    core.dag_ignore_file_syntax = glob
    core.dagbag_import_timeout = 120
```

In `pyproject.toml` the same option is an array of strings, and in `tox.ini` / `setup.cfg` the
section is `[tool:pytest]`:

```toml
[tool.pytest.ini_options]
airflow_config = [
    "core.dag_ignore_file_syntax = glob",
    "core.dagbag_import_timeout = 120",
]
```

Each line becomes an `AIRFLOW__SECTION__KEY` environment variable, which every Airflow
configuration parser in the process reads -- including the second parser Airflow 3.2 added at
`airflow.sdk.configuration.conf`. That ordering is the *whole* point of the option. A `with
airflow_config(...)` block in a test runs long after `dag_bag` has parsed your Dag folder once
for the worker process, and a bare `.__enter__()` at conftest module scope both runs too late
and never restores.

## The line grammar

One option per line. The value is split on the *first* `=`, so a value may contain more
(`airflow.some_url = https://h/?a=1`); the section and key are split on the *last* `.`, so a
section may contain dots while a key may not. Section, key, and value are each stripped, and an
empty value means the empty string -- there is no line syntax for "make this option absent",
which the programmatic form spells `None`. Two lines resolving to one variable are an error,
including `a.b.k` and `a_b.k`, which both mangle to `AIRFLOW__A_B__K` and which Airflow cannot
address separately.

Every line is validated before any override is assigned, so a malformed entry can never reach
the environment half-applied.

## Options bootstrap owns

Setting an option the plugin's own bootstrap owns is rejected with a message naming the
supported knob, rather than silently fought:

```console
ERROR: Ini option `airflow_config` may not set `database.sql_alchemy_conn`;
pytest-airflow-in-a-box owns `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` -- select the backend with
the `airflow_db_backend` ini option or `--airflow-db-backend`
```

The denied set is *derived* from bootstrap's own name list rather than copied, so it cannot go
stale. It covers the metadata database URL and pool flag, `core.dags_folder`,
`core.plugins_folder`, `core.unit_test_mode`, `core.load_examples`,
`scheduler.catchup_by_default`, `logging.base_log_folder`, `core.xcom_backend`, the
`secrets.backend` pair, `core.fernet_key`, and the auth-manager, JWT, and webserver-secret
surface.

`core.executor` is deliberately *not* on the list -- bootstrap does not own it, and
[`--airflow-doctor`](../reference/diagnostics.md) already tells you to set it. Because it is
settable here while the `airflow_executor` ini writes the same option into the generated
`airflow.cfg`, the two can disagree, and the `airflow_config` line wins for the whole session:
the environment outranks the file on every `conf.get()`. Set one or the other; see [how the two
channels compose](custom-components-wiring.md#how-the-two-channels-compose).

One option is rejected only *conditionally*. `core.dagbag_import_timeout` is fine on an
ordinary run, but with [the smoke catalog](smoke-tests.md) enabled it is an error, because the
catalog pins that same variable from `airflow_dag_parse_timeout` -- which also scales the
per-file parse watchdog and the slowpoke budget. Set `airflow_dag_parse_timeout` instead; it is
the one knob driving all three.

## Nothing lands in airflow.cfg

Every override is an environment variable and only an environment variable. On Airflow 3 the
environment already outranks every file on each `conf.get()`, and on 2.x `core.unit_test_mode`
sends the parser to `unit_tests.cfg` and never reads `AIRFLOW_CONFIG` at all, so the
environment is the only channel that works on both families.

One consequence: a *retained* `AIRFLOW_HOME` inspected with `airflow config list` outside the
pytest process will not show these overrides. Use
[`--airflow-doctor`](../reference/diagnostics.md), which echoes them back -- redacting any value
whose option name reads as a credential, since that report is meant to be pasted into bug
reports.

Under `xdist` the overrides reach every worker: workers inherit the controller's environment
and then re-apply the identical values. A worker also cross-checks what it inherited against
the controller's state, which is where [a foreign plugin's own `AIRFLOW__*`
writes](../internals/bootstrap-env-ownership.md) surface.

## Overriding one test

`airflow_config` is also a context manager and a decorator, for a value only that test wants:

```python
from pytest_airflow_in_a_box.config import airflow_config


def test_with_overrides():
    with airflow_config({("core", "unit_test_mode"): "False"}, env={"MY_FLAG": "1"}):
        ...


@airflow_config({("core", "dagbag_import_timeout"): "120"})
def test_decorated(): ...
```

This is a thin wrapper over `monkeypatch.setenv`, and honest about it: Airflow's
`AirflowConfigParser._lookup_sequence` puts the environment first on every `get()` and keeps no
per-value cache, so there is nothing to invalidate. If you are setting one variable in one test
and you already have `monkeypatch`, `monkeypatch.setenv("AIRFLOW__CORE__X", "y")` is fine and
you do not need this. What the wrapper buys is exact restore, `(section, key)` pairs instead of
hand-mangled names, and validation of both mappings before anything is assigned -- so a
malformed argument cannot leave the environment partly modified. Validation runs on context
entry, so a bad argument to the decorator form surfaces as a test failure rather than a
collection error.

A name that was absent beforehand is deleted rather than emptied, and nesting restores
last-in-first-out. A `None` value makes a name absent for the duration, so Airflow falls back to
`airflow.cfg` and then to its own default:

```python
with airflow_config({("core", "dagbag_import_timeout"): None}):
    ...  # conf.get returns Airflow's default
```

`env` names may not start with `AIRFLOW__` -- pass configuration options through `overrides`
instead -- but `AIRFLOW_HOME` and other single-underscore names are fine.

### Options Airflow reads through settings

Airflow resolves `SQL_ALCHEMY_CONN`, `DAGS_FOLDER`, and `PLUGINS_FOLDER` into
`airflow.settings` globals once at import, and those do not follow an environment assignment.
Pass `refresh_settings=True` for options read through `settings` rather than through the config
parser:

```python
with airflow_config({("core", "plugins_folder"): str(tmp_path)}, refresh_settings=True):
    ...  # airflow.settings.PLUGINS_FOLDER now agrees
```

It defaults to off because it imports Airflow and rewrites process-global state bootstrap owns,
and it is a partial remedy: a module that re-exported a settings value *by value* froze that
binding at import and no refresh can update it.

### Three things worth knowing

- **Values are expanded when Airflow reads them.** `conf.get` runs the raw variable through
  `expandvars` then `expanduser`, so a value containing `~` or `$` does not round-trip --
  `os.environ` holds the literal while `conf.get` returns the expansion.
- **A `None` override does not hide a `_CMD`/`_SECRET` sibling.** Setting a plain value always
  wins, but `None` means "fall back to whatever Airflow would otherwise do", and an already-set
  sibling variable is one of those things.
- **Do not wrap the first use of `api_client`/`api_server_url`.** Those fixtures launch a
  session-scoped subprocess that inherits the environment live at startup, so an override would
  outlive the context inside that server.

`conf_vars` is a deprecated alias for `airflow_config` under the name public Airflow docs teach;
it warns, and it does not recompute the settings globals, so use
`airflow_config(..., refresh_settings=True)` instead.

## Session-scoped overrides

For a value a config file cannot hold -- a path from `tmp_path_factory`, a port, a credential
minted for the run -- request the `airflow_configure` fixture and apply a batch from your own
session fixture:

```python
import pytest


@pytest.fixture(scope="session", autouse=True)
def _repo_defaults(airflow_configure, tmp_path_factory):
    airflow_configure(
        {("core", "plugins_folder"): str(tmp_path_factory.mktemp("plugins"))},
        refresh_settings=True,
    )
```

Arguments are `airflow_config`'s, unchanged: `overrides`, `env=`, `refresh_settings=`, and a
`None` value to make a name absent. Batches compose and unwind last-in-first-out at session
teardown, restoring every name exactly.

Ordering is the caveat. pytest instantiates autouse fixtures ahead of requested ones at the
same scope, so a `scope="session", autouse=True` wrapper like the one above *is* applied before
a session-scoped `dag_bag` that the same test requests. But that guarantee is per test:

- A *function*-scoped wrapper is instantiated after every session-scoped fixture, so it cannot
  precede a Dag parse.
- `dag_bag` parses once per worker process and caches. A test collected earlier and outside
  your wrapper's conftest scope can win the parse, after which no override reaches it.

So use the ini option for anything that must precede the first parse unconditionally, and this
fixture for the rest. And do not reconfigure anything the API server already inherited -- the
warning about `api_client`/`api_server_url` above applies here too, for the whole session.

## Pointing at your own cluster-policy module

`airflow_local_settings` is a separate ini option taking a dotted module path, because the
plugin's generated `AIRFLOW_HOME/config/airflow_local_settings.py` already owns that module
name process-wide:

```ini
[pytest]
airflow_local_settings = myproject.cluster_policies
```

Your module's public names are composed into the generated file rather than replacing it. See
[Cluster policies](cluster-policies.md).

## Defaults the plugin applies with no ini at all

The plugin needs zero ini configuration, and it changes three display options *only* while they
still equal pytest's own parser default:

| Option | pytest default | plugin default |
| --- | --- | --- |
| `--tb` | `auto` | `short` |
| `-r` | `fE` | `a` |
| `--durations` | unset | `20` |

Plus two ini values while absent: `tmp_path_retention_policy = failed` and
`tmp_path_retention_count = 3`.

The lossy case is worth stating plainly: an explicit `--tb=auto` is indistinguishable from the
parser default, so it *is* overridden to `short`. Any other explicit value survives. The same
holds for `-rfE` and for `--durations=0`.

Warning filters are prepended below your own `filterwarnings` lines, so yours always win. They
silence traced third-party deprecation noise (`flask_appbuilder`, `flask_sqlalchemy`,
`starlette`) while keeping Airflow's own deprecation warnings visible, and promote pytest's
collection and unraisable warnings to errors.

No default here writes a file. Report artifacts (`pytest.log`, `pytest.xml`) are opt-in -- see
[Report artifacts](reports.md).

### Warnings from the plugin's own bootstrap

Warnings raised by the plugin's *own* metadata-database initialization need a separate knob,
the `airflow_default_filterwarnings` ini option, because plain `filterwarnings` lines
structurally cannot reach them. That initialization runs inside a `warnings.catch_warnings()`
block that calls `simplefilter("default")`, which wipes the filter list your ini configured.
`apply_bootstrap_warning_filters` replaying this option's parsed filters inside that block is
the only reach-in.

The concrete case: alembic's `No path_separator found in configuration` `DeprecationWarning`,
emitted during that initialization. A repo running warnings-as-errors provably cannot silence
it with a `filterwarnings` line, which is why the option ships with that filter as its default.
The filter matches on message prefix alone, because alembic warns with a `stacklevel` that
attributes the warning to a caller frame inside Airflow's migration utilities rather than to
`alembic.config`.

Lines use pytest's `filterwarnings` syntax (`action:message:category:module:lineno`, message and
module matched as regexes from the start), are prepended below user-supplied `filterwarnings`
lines, *and* are replayed inside the bootstrap's warnings context. Each line is parsed eagerly
at configure time, so a malformed value is a usage error rather than an explosion mid-bootstrap.

Redefining the option replaces the default wholesale, so a warnings-as-errors suite that wants
to see the alembic warning empties it:

```ini
[pytest]
airflow_default_filterwarnings =
```

The isolated `AIRFLOW_HOME` carries its own failed-only retention default under
`airflow_home_retention_policy` / `--airflow-home-retention`; see [the isolated
`AIRFLOW_HOME`](airflow-home.md).

## The `airflow_home` and `airflow_dags_folder` fixtures

`airflow_home` and `airflow_dags_folder` return this run's isolated `AIRFLOW_HOME` and the Dag
directory `dag_bag` parses, as `pathlib.Path`:

```python
def test_paths(airflow_home, airflow_dags_folder):
    assert (airflow_home / "airflow.cfg").is_file()
    assert (airflow_dags_folder / "my_dag.py").is_file()
```

Both are session-scoped and touch neither Airflow nor the metadata database, so asking where a
directory is never triggers an import or a migration. The names deliberately match the
`airflow_home` and `airflow_dags_folder` ini options -- fixtures and ini options live in
separate pytest registries, so the same name works in both contexts. One nuance: the
`airflow_home` *ini option* names the base directory to provision under, while the fixture
returns the disposable per-run root created below it; `airflow_dags_folder` returns exactly the
directory its option configures.

`airflow_dags_folder` follows the same ladder `dag_bag` does -- `--dag-folder`, then the
`airflow_dags_folder` ini option, then the empty `dags/` directory inside the run's
`AIRFLOW_HOME`. That last one is a disposable scratch directory meaning "no Dag folder was
configured", not a corpus.

## When something else writes AIRFLOW__\*

The plugin owns `AIRFLOW__*` through bootstrap and makes no claim over it afterwards. When
another plugin or conftest rewrites those names and your xdist run dies with
`xdist: maximum crashed workers reached`, that is the drift check firing -- see [who owns
`AIRFLOW__*`](../internals/bootstrap-env-ownership.md).
