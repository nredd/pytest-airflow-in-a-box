# Changelog fragments

`CHANGELOG.md`'s `[Unreleased]` section used to be edited directly by every PR, which
meant unrelated PRs constantly conflicted on the same lines. Instead, each PR that changes
`src/` or `tests/` adds one fragment file here; `towncrier` assembles them into
`CHANGELOG.md` at release time (`make release`).

## Naming

`<issue-number>.<type>.md`, e.g. `changelog.d/178.added.md`.

`<type>` is one of: `added`, `changed`, `deprecated`, `removed`, `fixed`, `security` --
matching the Keep a Changelog headers already used in `CHANGELOG.md`.

## Content

Write exactly the bullet body, no leading `-` and no issue link -- towncrier adds both:

```
Support asset/dataset-triggered cross-Dag scheduling in tests.
```

Multi-sentence entries are fine; write them as they should read in `CHANGELOG.md`.

## When a fragment isn't needed

A PR that only touches docs, CI, or dev tooling (no `src/`/`tests/` change) doesn't need
one -- the `changelog-fragment-required` prek hook (`uv run towncrier check`) only blocks
when a change is detected with no matching fragment.

## Preview

`make changelog` renders the draft `Unreleased` section from the current fragments without
writing anything.
