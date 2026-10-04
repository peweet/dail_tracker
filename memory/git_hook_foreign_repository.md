# Git hook environment and foreign repositories

Observed 2026-10-04 during a push from a linked worktree.

Git exports repository-local environment variables to hooks. A hook that runs
`git -C <temporary-directory> init` without clearing them can change the source
repository's shared configuration, including `core.bare`, instead of initializing
only the temporary directory. Changing directory alone does not isolate Git.

The pre-push archive gate now captures `git rev-parse --local-env-vars` in the
source repository and unsets those names in a subshell around the foreign init.
The source environment and all existing push guards remain active outside it.
This follows the [Git hook documentation](https://git-scm.com/docs/githooks).

Regression evidence: `test/tools/test_pre_push_git_isolation.py` makes a real
bare remote, primary checkout and linked worktree, then invokes the actual hook
through `git push`. The original hook failed the shared-config byte comparison;
the repaired hook passed, retaining `core.bare=false` and usable worktrees.
Python guard and lint commands are faked in this fixture; Git and the shell hook
are real. The fixture does not prove the separately tested guards' correctness.

Recheck with the locked dev/pipeline/api/mcp profile and
`python -m pytest test/tools/test_pre_push_git_isolation.py -q`.
