# Dependencies and extras

The base package installs the pytest plugin, not Apache Airflow. Airflow 2 and 3 both provide
the `airflow` import package through different distribution shapes, so choosing either family
as a mandatory dependency would corrupt environments using the other one.

## Base dependencies

These install with every copy of `pytest-airflow-in-a-box`:

| Dependency | Constraint | Why it is here |
| --- | --- | --- |
| `pytest` | `>=8` | Plugin host and public fixture surface |
| `pytest-timeout` | `>=2.4` | Bounds Dag-file parsing and smoke checks |
| `packaging` | `>=22` | Distribution and version inspection |
| `sqlalchemy` | `>=1.4.36,<3` | Shared metadata-database interfaces across Airflow families |

Airflow remains your project's choice. Install the plugin bare when your environment already
pins Airflow, as repositories using Airflow's published constraints files usually do:

```console
uv add --dev pytest-airflow-in-a-box
```

## Extras

| Extra | Installs | Use it when |
| --- | --- | --- |
| `airflow3` | `apache-airflow>=3.1,<4` and `apache-airflow-providers-sqlite>=4.1,<5` | You need a self-contained Airflow 3 test environment with the default SQLite backend |
| `airflow2` | `apache-airflow>=2.7,<3` on CPython below 3.13 | You are running the Airflow 2 side of a migration or compatibility suite |
| `postgres` | `asyncpg>=0.29,<1`, `psycopg2-binary>=2.9,<3`, and `testcontainers>=4.15,<5` | You run the disposable metadata database with `--airflow-db-backend=postgres`; Docker is also required |
| `xdist` | `pytest-xdist>=3.8` | You run pytest in parallel |

`postgres` and `xdist` are additive: combine either with an Airflow extra or with a project that
already supplies Airflow. Choose at most one of `airflow2` and `airflow3`; their Airflow
requirements are mutually exclusive.

The `<4` bound on `airflow3` is the current `pyproject.toml` dependency restriction, not the
project's support guarantee.

## Common installations

Default Airflow 3 environment:

```console
uv add --dev "pytest-airflow-in-a-box[airflow3]"
```

Airflow 3 with parallel tests:

```console
uv add --dev "pytest-airflow-in-a-box[airflow3,xdist]"
```

Airflow 3 with Postgres and parallel tests:

```console
uv add --dev "pytest-airflow-in-a-box[airflow3,postgres,xdist]"
```

Airflow 2 compatibility environment:

```console
uv add --dev "pytest-airflow-in-a-box[airflow2]"
```

The same bracket syntax works with `pip install`. Installing an extra only makes its packages
available: `postgres` does not select the backend until you pass
`--airflow-db-backend=postgres`, and `xdist` does not create workers until you pass `-n`.
For this plugin's shared-corpus and fixed-`dag_id` coordination, prefer:

```console
pytest -n auto --dist loadgroup
```

The `dev` and `docs` groups in `pyproject.toml` are maintainer environments for this repository,
not consumer extras.
