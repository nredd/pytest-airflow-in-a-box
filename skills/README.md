# skills/

Model-agnostic home for the agent skills this repo expects. `.claude/skills` and
`.agents/skills` are each a single symlink to this directory, so Claude Code and other
agent tools discover every skill here without any extra setup.

- `mattpocock/` -- a git submodule pinned to a specific commit of
  [`mattpocock/skills`](https://github.com/mattpocock/skills) (MIT,
  `mattpocock/LICENSE`). `git submodule update --init` after cloning. See
  `../PROVENANCE.md` for the pinned commit
- `get-it-merged/`, `ship-issue/` -- skills authored for this project's owner, vendored
  directly, no submodule
- Everything else at this level (`tdd`, `code-review`, `codebase-design`, ...) is a
  symlink into `mattpocock/skills/{engineering,productivity}/<name>`, flattening the
  submodule's category split into the flat-by-name layout agent tools expect

Edit `get-it-merged/` and `ship-issue/` here directly; `mattpocock/` changes go upstream,
then re-pin the submodule commit (the flat symlinks don't need touching -- only the names
change if a skill is added or renamed upstream). See `AGENTS.md`'s `## Agent skills`
section for which skills this repo expects agents to use and when.
