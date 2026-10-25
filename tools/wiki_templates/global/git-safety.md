---
description: Git safety with several agent sessions sharing one working tree (personal, every project)
---

# Git safety across parallel sessions

- The user runs every `git push`. Commit locally, verify the guards, then hand over the exact
  push command. Never retry a denied push. Subagent prompts forbid git entirely.
- Never `git stash`, `git checkout --`, `git restore` or `git reset --hard` while another
  session may hold uncommitted work in the same tree. Re-check `git status` immediately
  before any write to the index, and copy a file aside instead of swapping it.
- Never `git add -A` or `commit -a`. Stage explicit paths, then compare the commit's insertion
  count with what this session edited. Unbundle a sweep while it is still unpushed.
- Always `git -C <path> <cmd>`, never `cd <path>; git <cmd>`: a failed `cd` runs the command in
  the primary checkout. `cd` persists in the tool shell and wedges relative paths afterwards.
- A branch tip is a snapshot of a tree that was green when pushed, not of the tree now. Verify
  a fix by reading the artifact it produced, not the log that says it ran.
- Do not background heavy Python pulls repeatedly: stopping the task orphans `python.exe` on
  Windows and the pile-up eats RAM.
- Prefer a new commit over amending. Commit or push only when asked.
