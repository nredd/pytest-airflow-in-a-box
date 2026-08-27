# Observability components

Static [`check_component`](custom-components.md) checks for the three pluggable kinds that
watch or react to a run without driving it: listeners, notifiers, and policies.

## Listener checks

- `listener-no-matching-hookspec` -- a hookimpl method's name matches no hookspec
  registered by either listener manager. pluggy silently ignores it; the method never
  fires, with no warning. The single most common real-world listener bug
- `listener-unknown-argument` -- a hookimpl method declares an argument name its matching
  hookspec does not have. pluggy hard-errors on this at registration time
- `listener-core-manager-only` / `listener-sdk-manager-only` -- a hookimpl matches a
  hookspec registered by only one manager. `airflow.listeners.listener` registers
  lifecycle, taskinstance, dagrun, asset, and import-error hookspecs;
  `airflow.sdk.listener` registers only lifecycle and taskinstance. Register a listener
  with only one manager and half its hooks are silently unreachable

## Notifier checks

- `notifier-missing-notify` -- `notify` is not overridden. Its default raises
  `NotImplementedError()` unconditionally; `on_success_callback` / `on_failure_callback`
  run on the Dag processor, sync-only, and call `notify` directly, so implementing only
  `async_notify` does not help there. Covers apache/airflow#64649, where a minimal
  `BaseNotifier` used as a callback crashed under `airflow dags test`
- `notifier-template-fields-unresolvable` -- an instance's `template_fields` names an
  attribute the instance does not carry. `_update_context` does a plain `getattr(self, f)`
  for every entry, raising `AttributeError` the first time the notifier actually fires.
  Instance-only, for the same reason as `timetable-serialize-not-json`

## Policy checks

- `policy-unknown-hookspec` -- a policy hookimpl method's name matches no hookspec in
  `airflow.policies`. pluggy silently ignores it; the method never fires
- `policy-argument-name-mismatch` -- a policy hookimpl method declares an argument its
  matching hookspec does not have. pluggy hard-errors on this at registration time.
  `task_instance_mutation_hook` gained a `dag_run` parameter in Airflow 3.3, so a hook
  written for a newer release breaks registration entirely on an older one; this reads
  the live installed hookspec, not a hardcoded table

Both checks model a policy registered as an `@hookimpl`-decorated class through the
`airflow.policy` entry point -- the shape `ComponentKind.POLICY`'s classifier requires. A
plain module-level function in `airflow_local_settings.py`, the older and still-common
way, is a different mechanism: `make_plugin_from_local_settings` loads it through a
generated shim that calls it positionally and deliberately tolerates a name or arity
mismatch. Forcing `kind=ComponentKind.POLICY` on such a function finds nothing either,
since a plain function is never `@hookimpl`-marked. See [keeping your own
`airflow_local_settings.py`](../internals/test-environments.md#cluster-policies-and-airflow_local_settingspy).
