# skills/

Vendored, model-agnostic copies of the agent skills this repo expects. Real files, not
symlinks or a git submodule -- self-contained for anyone who clones the repo, whatever
agent tool they run.

- `mattpocock/engineering/` and `mattpocock/productivity/` -- Matt Pocock's engineering and
  productivity skills, vendored verbatim (MIT, `mattpocock/LICENSE`). Source and provenance
  details in `../PROVENANCE.md`
- `redd/` -- skills authored for this project's owner (`get-it-merged`, `ship-issue`)

`.claude/skills/` and `.agents/skills/` symlink into this tree by skill name so that
Claude Code and other agent tools discover them without any extra setup. This directory is
the source of truth; edit skills here, not through either symlink tree. See `AGENTS.md`'s
`## Agent skills` section for which skills this repo expects agents to use and when.
