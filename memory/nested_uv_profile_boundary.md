# Keep nested UV commands within their selected profile

`tools/dev_env.py::profile_environment` sets `UV_PROJECT_ENVIRONMENT`, `VIRTUAL_ENV` and
`DAIL_ENV_PROFILE` for the child process. A nested `uv run` inherits that environment path even
when its project and dependency flags select another project. It can replace packages in the
original named environment while leaving both lockfiles unchanged.

Run public verification from the public profile. If a private evidence wrapper launches the
canonical public command, give that child an explicit public environment path and clear the
inherited private profile variables first. Preserve the named private environment for private
tests; it is not a superset of the public profile.

After an accidental cross-profile sync, restore the affected profile with
`py -3.12 tools/dev_env.py sync <profile>` and require `doctor <profile>` to report stable before
continuing. Record the environment interruption and invalidate any overlapping verification.
Do not change package pins or weaken the stale-profile refusal to make a command run.

Evidence: `tools/dev_env.py:123` sets the inherited environment; `run_in_profile` refuses a stale
profile. A confirmed public-command launch through a private wrapper changed installed packages;
the named private sync and doctor restored its locked dependency set.
