`scripts/check_changelog_fragment.py` now also skips `towncrier check` for commits
authored by `dependabot[bot]`. The check previously flagged any branch with no new
fragment regardless of which files changed, so a Dependabot workflow-file or lockfile-only
bump failed a check meant to gate `src`/`tests` changes, blocking auto-merge.
