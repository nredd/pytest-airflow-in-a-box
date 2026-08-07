# Contributing

Use Python 3.10 or newer and install the locked development environment:

```console
uv sync
uv run prek install
```

Before opening a pull request, run:

```console
make all
uv run prek run --all-files
```

Changes require tests, complete type annotations, and documentation for public behavior. Fix type
and lint failures at the source; inline suppression directives are rejected. Keep the registered
`plugin.py` module import-light and never import Airflow there at module scope.

Report security issues through the process in `SECURITY.md`, not a public issue.
