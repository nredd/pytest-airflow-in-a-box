# skills/

Model-agnostic home for the agent skills this repo expects.

- `mattpocock/` -- Matt Pocock's engineering and productivity skills
  (`mattpocock/skills/engineering/`, `mattpocock/skills/productivity/`), pulled in as a git
  submodule pinned to a specific commit (MIT, `mattpocock/LICENSE`). `git submodule update
  --init` after cloning. See `../PROVENANCE.md` for the pinned commit
- `redd/` -- skills authored for this project's owner (`get-it-merged`, `ship-issue`),
  vendored directly, no submodule

`.claude/skills/` and `.agents/skills/` symlink into this tree by skill name so that
Claude Code and other agent tools discover them without any extra setup. Edit `redd/`
skills here directly; `mattpocock/` changes go upstream, then re-pin the submodule commit.
See `AGENTS.md`'s `## Agent skills` section for which skills this repo expects agents to
use and when.
