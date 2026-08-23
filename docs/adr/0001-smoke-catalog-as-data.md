# Collapse the smoke catalog's 14 Item classes into one data-driven SmokeCheck catalog

Before this change, one conceptual smoke check was spread across five hand-synced places: a
`pytest.Item` subclass per check (14 total, each repeating the same `__init__`/`reportinfo`
boilerplate), a branch in `SmokeCollector.collect()`'s ~70-line if-chain, a
`_SMOKE_ITEM_NAMES` table claiming to be the "single source of truth" (nothing derived it
from the real yields), a `_SMOKE_ITEM_MARK_SETS` table claiming to be "kept in sync" with
each item's `add_marker` calls (same problem), and the checks' `addini`/`addoption`
registrations inlined in `plugin.py` rather than owned by `smoke.py`. Drift between these was
held off only by exact-pass-count and ordered `fnmatch_lines` assertions in
`tests/test_smoke.py`, and ~79 `monkeypatch.setattr(smoke, "_private_fn", ...)` sites reached
past the module's interface because there was no public seam to construct a check against.

We collapsed this into one `SmokeCheck` frozen dataclass (`name`, `enable(config) -> T |
None`, `marks`, `run(SmokeContext, T)`), a `SMOKE_CATALOG` tuple holding all 14 entries in
their pinned emission order, and one generic `_CatalogItem` running whichever check it wraps.
`_SMOKE_ITEM_NAMES` and `_SMOKE_ITEM_MARK_SETS` are now derived from `SMOKE_CATALOG` instead
of hand-maintained. `smoke.register_options(parser)` now owns the checks' `addini`/
`addoption` registrations, matching the `register_options(parser)` idiom `_airflow_home`,
`record`, and `baseline` already use elsewhere in this plugin -- `plugin.py` just calls it.

Consequences: the 14 public `*Item` classes (`DagBagIntegrityItem`, `ScheduleSanityItem`,
etc.) are removed from `smoke.py`'s public surface -- a breaking change for anyone importing
them directly, documented in `changelog.d/232.removed.md`. Tests construct a `SmokeContext`
and call a check's `run` directly instead of monkeypatching `smoke` internals, retiring ~79
monkeypatch sites.

Reviewed and intentionally left alone in the same pass (2026-08-21 architecture review):
`db.py`, the `_compat` registries, the `assets.py` facade, `_compat/settings.py`, `types.py`,
`storage/locate.py`, and `storage/provision.py` -- not part of this change, noted here so
they aren't re-litigated as "also drifted."
