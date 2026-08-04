---
name: feedback_cli_testing_profile_and_console
description: CLI test commands must use the locked CI-equivalent uv profile and remain safe on legacy Windows consoles
metadata:
  type: feedback
---

## Lesson (2026-08-04)

`[tool.uv] default-groups = []` is intentional: it keeps the deployed Streamlit
runtime lean. It also means a bare `uv run python tools/dev.py test-fast` starts
without the `dev` group and optional pipeline/API/MCP capabilities. Test collection
then fails on missing packages such as `hypothesis`, `pandera`, or `openpyxl` before
it can provide useful feedback.

The same runner printed a Unicode arrow as its status prefix. A Windows CP1252
console could not encode it, so even `tools/dev.py lint --dry-run` failed before
running any check.

## How to apply

1. Keep ordinary application installs lean. Do not make `dev` a default uv group
   or use `--all-extras`: both bloat the deployed environment.
2. Every executable `tools/dev.py` task re-execs once through the locked
   `dev + pipeline + api + mcp` profile, which is the profile CI uses. Inspection
   commands such as `list`, `verify --plan`, and `--dry-run` stay cheap.
3. Documentation should show the explicit profile command to avoid the outer,
   lean `uv run` sync: `uv run --locked --group dev --extra pipeline --extra api
   --extra mcp python tools/dev.py <task>`.
4. CLI-owned status text stays ASCII. Console streams use `backslashreplace` so a
   UTF-8 diagnostic captured from a child process becomes an escape rather than
   aborting a legacy console.

## Regression proof

- `test/tools/test_dev.py` exercises the one-time profile re-exec and CP1252
  dry-run output.
- `test/tools/test_verify_changed.py` proves captured Unicode diagnostics are
  escaped safely on a strict CP1252 stream.

