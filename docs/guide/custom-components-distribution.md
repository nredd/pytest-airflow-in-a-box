# Distribution components

Static [`check_component`](custom-components.md) checks for the three pluggable kinds
Airflow discovers by name or package metadata, rather than by direct reference in your
code: secrets backends, plugins, and providers.

## Secrets backend checks

- `secrets-backend-raises-on-miss` -- an overridden `get_conn_value`, `get_connection`,
  `get_variable`, or `get_config` is annotated to return something that does not admit
  `None` (recognizing `X | None`, `Optional[X]`, and `Union[X, None]` alike). All four
  must return `None` on a miss, not raise -- raising the backing client's own not-found
  error instead is the common bug. `check_component` never calls a secrets backend for
  real, since a genuine miss needs credentials it cannot fabricate safely, so this reads
  the override's declared return annotation; an unannotated override is not flagged

## Plugin checks

- `plugin-name-missing` -- the plugin does not set `name`. `AirflowPlugin.validate()`
  raises `AirflowPluginException` for exactly this, but only when Airflow's own
  `is_valid_plugin` calls it during real discovery; this checker never calls `validate()`
  itself, since doing so risks raising out of `check_component`

## Providers, if you are shipping one

Writing a provider package means shipping Airflow integration code to other people, which
is past the boundary [deciding which failures are yours](testing-scope.md) draws -- that
job wants Breeze and upstream `tests_common`. The checks exist because a provider still
starts life as a directory in your own repo:

- `provider-info-schema` -- the callable's return value fails the shipped
  `provider_info.schema.json`, read from the installed `airflow` package so it always
  matches the resolved release, or the callable raises when called
- `provider-package-name-mismatch` -- the returned dict's `package-name` disagrees with
  the owning distribution's canonical name. `ProvidersManager` raises `ValueError` at
  discovery for this, not a warning
- `provider-no-entry-point` -- the owning distribution registers no
  `apache_airflow_provider` entry point at all, so `ProvidersManager` never calls this
  function and the provider is silently undiscovered

Pass the `get_provider_info` callable itself, not its return value:
`check_component(get_provider_info)`. The last two checks need the callable's module
attributed to a real installed distribution, done by matching the distribution's recorded
file list, falling back to the source root(s) named in its `.pth` file for an editable
install. That root is narrower than the project checkout on purpose: a `src/`-layout
package is exposed only under `src/`, so a sibling `tests/` directory is not attributed
to it. A callable that cannot be attributed -- one defined in a test file, say -- is
silently skipped by both.

To prove the entry point itself *resolves*, rather than that it exists, run the test
under [`airflow_isolated`](isolated-tests.md).
