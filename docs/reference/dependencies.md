# Dependencies and extras

Choose exactly one source for Airflow: your project's existing pin, or one of the plugin's
Airflow-family extras. The base package deliberately installs no Airflow distribution because
the 2.x monolith and 3.x packages provide the same `airflow` import package.

## Choose an installation

| Your environment | Install |
| --- | --- |
| The project already pins Airflow | `uv add --dev pytest-airflow-in-a-box` |
| A self-contained Airflow 3 test environment | `uv add --dev "pytest-airflow-in-a-box[airflow3]"` |
| The Airflow 2 side of a migration suite | `uv add --dev "pytest-airflow-in-a-box[airflow2]"` |
| Any row above, with parallel pytest workers | Add `xdist` |
| Any row above, with disposable Postgres metadata | Add `postgres` and provide Docker |

Use `pip install` with the same requirement string when the project does not use `uv`.
Repositories installing Airflow through its published constraints should normally install the
plugin bare instead of asking a second requirement to choose the Airflow version.

## Runtime requirements

- CPython 3.10 through 3.14 and pytest 8 or newer.
- Linux or macOS. On Windows, use WSL2 or the devcontainer; Airflow has no native Windows
  support.
- All major Apache Airflow versions. Airflow 2 releases have narrower Python ceilings: the
  certified 2.7 and 2.8 releases stop at Python 3.11, while the certified 2.9–2.11 releases
  stop at Python 3.12. See
  [Compatibility and certification](../internals/compat-layer.md#supported-and-certified) for
  the exact release matrix.

## Base dependencies

These install with every copy of `pytest-airflow-in-a-box`:

| Dependency | Constraint | Why it is here |
| --- | --- | --- |
| `pytest` | `>=8` | Plugin host and public fixture surface |
| `pytest-timeout` | `>=2.4` | Bounds Dag-file parsing and smoke checks |
| `packaging` | `>=22` | Distribution and version inspection |
| `sqlalchemy` | `>=1.4.36,<3` | Shared metadata-database interfaces across Airflow families |

## Extras

| Extra | Installs | Use it when |
| --- | --- | --- |
| `airflow3` | `apache-airflow>=3.1,<4`; `apache-airflow-providers-sqlite>=4.1,<5` | Create an Airflow 3 environment ready for the default SQLite metadata backend |
| `airflow2` | `apache-airflow>=2.7,<3` when Python is below 3.13 | Run the Airflow 2 compatibility tier |
| `postgres` | `asyncpg>=0.29,<1`; `psycopg2-binary>=2.9,<3`; `testcontainers>=4.15,<5` | Provision the disposable Postgres metadata backend through Docker |
| `xdist` | `pytest-xdist>=3.8` | Run tests in parallel |

`postgres` and `xdist` are additive. Combine either with one Airflow extra, or install it beside
a project-managed Airflow requirement. Never combine `airflow2` and `airflow3`; their Airflow
requirements conflict. An Airflow extra supplies only the named family, not your Dag
repository's provider packages.

The `<4` bound on `airflow3` is the current `pyproject.toml` dependency restriction, not the
project's support guarantee.

## Common installations

Airflow 3 with parallel workers:

```console
uv add --dev "pytest-airflow-in-a-box[airflow3,xdist]"
```

Airflow 3 with Postgres and parallel workers:

```console
uv add --dev "pytest-airflow-in-a-box[airflow3,postgres,xdist]"
```

Airflow 2 compatibility environment:

```console
uv add --dev "pytest-airflow-in-a-box[airflow2]"
```

Installing an extra only makes its dependencies available. Select Postgres with
`--airflow-db-backend=postgres`; start xdist workers with `-n`. The complete catalog of
plugin-owned pytest flags and their ini equivalents lives in
[CLI and INI options](ini-options.md). For shared-corpus and fixed-`dag_id` coordination, use:

```console
pytest -n auto --dist loadgroup
```

The `dev` and `docs` dependency groups in `pyproject.toml` build this repository itself. They
are not published extras and consumers cannot install them with bracket syntax.
