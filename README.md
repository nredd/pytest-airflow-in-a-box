# pytest-airflow-in-a-box

`pytest-airflow-in-a-box` is a pytest plugin for testing Apache Airflow DAGs without a live
Airflow deployment. It targets Airflow 3 and provides the package and plugin foundation for a
small, typed testing surface.

The repository is in its initial scaffold phase. The package auto-registers with pytest, but the
Airflow bootstrap and fixtures described in the project plan are not implemented yet.

## Requirements

- CPython 3.10 through 3.14
- Apache Airflow 3.1 or newer, below 4
- Linux or macOS for Airflow-backed tests

Apache Airflow does not support native Windows installations. Windows development should use WSL2
or the included devcontainer; platform-independent package checks alone do not imply full Windows
Airflow support.

The released compatibility matrix is exercised against Airflow 3.1.8, 3.2.2, and 3.3.0 using
Airflow's published constraints files.

## Installation

```console
uv add --dev pytest-airflow-in-a-box
```

The `pytest11` entry point loads the plugin automatically. Consumer projects do not need to add a
`pytest_plugins` declaration.

## Development

```console
uv sync
uv run prek install
make all
```

Run the GitHub Actions workflow locally on Linux with [act](https://nektosact.com/):

```console
act pull_request
```

`act` cannot reproduce native macOS or Windows behavior.

## Status

The public Python surface currently contains only `pytest_airflow_in_a_box.__version__`. APIs are
added only when their implementation and Airflow compatibility tests land.

## License

Apache License 2.0. See `LICENSE`, `NOTICE`, and `PROVENANCE.md`.
