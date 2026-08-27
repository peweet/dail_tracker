---
tier: RUNBOOK
status: LIVE
domain: infra
updated: 2026-08-26
supersedes: []
read_when: Codex Windows commands fail before process startup with apply deny-read ACLs, CreateProcess, or helper_unknown_error
key: RUNBOOK|LIVE|infra
---

# Codex Windows sandbox recovery

## Outcome

This workstation has a per-user Scheduled Task that checks Codex's deny-read ACL
state outside the Codex command runner. If the state becomes malformed, the task
retries validation, moves the malformed file and matching parse-error diagnostic
to timestamped backups, and exits. Codex then regenerates its own state on the
next setup attempt.

This matters because sandbox ACL setup happens before child-process startup. A
broken setup therefore makes `echo`, `pwd`, `git status`, `rg`, AST tools, MCP
navigation, and tests all appear unavailable at once. Installing more repository
search tools cannot help until process startup works.

## Recognise this exact incident

The confirmed 2026-08-24 and 2026-08-26 signature was:

```text
helper_unknown_error: apply deny-read ACLs
CreateProcess ... apply deny-read ACLs
parse deny-read ACL state ... expected value at line 1 column 1
```

The file `%USERPROFILE%\.codex\.sandbox\deny_read_acl_state.json` contained 22
NUL bytes instead of JSON. Do not infer this cause merely from a normal
permission-denied error inside a process. The distinctive boundary is that no
process starts, including a trivial `echo`.

## Automatic guard

Install or refresh the least-privilege per-user task from a normal PowerShell
terminal:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File tools\install_codex_windows_sandbox_guard.ps1 -RunNow
```

The task `DailTracker-CodexSandboxGuard` runs at logon and every two minutes
while this user is logged on. The short periodic process is intentional: a
Codex hook would depend on the same launch path that is broken during this
incident. Re-run the installer with `-IntervalMinutes 5` if slower detection is
preferred.

Inspect it and the repair log:

```powershell
Get-ScheduledTask -TaskName DailTracker-CodexSandboxGuard
Get-Content "$env:USERPROFILE\.codex\.sandbox\auto-repair.log" -Tail 20
```

Remove the task without deleting any evidence:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File tools\install_codex_windows_sandbox_guard.ps1 -Uninstall
```

If the repository moves, reinstall the task because its action stores the
absolute path to the guard script.

## Safety contract

`tools/codex_windows_sandbox_guard.ps1`:

- treats a missing or valid JSON state as healthy and makes no write;
- retries malformed state before acting, then revalidates under a named mutex;
- moves rather than deletes the malformed state;
- moves `setup_error.json` only when it identifies the same parse failure;
- writes one metadata-only repair line and never logs file contents;
- never modifies ACLs, stops processes, reads `.sandbox-secrets`, or creates a
  replacement state using an assumed private schema.

The timestamped `*.corrupt-auto-*.bak` files are retained for diagnosis. Their
small incident-driven growth is preferable to hiding a recurrence.

## Manual recovery if the task is absent

Run the guard from a normal PowerShell terminal, not through an affected Codex
worker:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File tools\codex_windows_sandbox_guard.ps1
```

Then fully restart Codex/VS Code if the current host remains in a partial setup
state. Do not delete the whole `.sandbox` directory, reset broad filesystem
ACLs, kill all Node or Codex processes, or expose `.sandbox-secrets`.

OpenAI's Windows sandbox guidance recommends the stronger `elevated` mode when
it works, and `unelevated` as a weaker fallback while setup problems are
investigated. It also recommends restarting Codex and collecting
`CODEX_HOME/.sandbox/sandbox.log` when sandboxing worked and then stopped. Never
send the contents of `CODEX_HOME/.sandbox-secrets/`.

Official reference: <https://learn.chatgpt.com/docs/windows/windows-sandbox>

## Separate defects that can look similar

- If PowerShell starts but `uv`, `rg`, or another executable is not found,
  inspect PATH. That is tool discovery, not a pre-start sandbox failure.
- If `uv` starts but cannot write its cache, place `UV_CACHE_DIR` in a permitted
  location. Do not classify it as ACL-state corruption.
- If one path is unreadable after startup, use a scoped read permission or
  `/sandbox-add-read-dir`; do not quarantine healthy global sandbox state.
- An ACL warning for an unrelated protected token file is not proof that the
  JSON state is malformed. The guard deliberately leaves unrelated setup errors
  intact.

## Verification

The guard's tests use temporary files; they never corrupt live Codex state:

```powershell
uv run --locked --group dev --extra pipeline --extra api --extra mcp pytest test/tools/test_codex_windows_sandbox_guard.py -q
```

For a live canary, run the installed task while the state is healthy and verify
that its timestamp and contents remain unchanged. Do not deliberately corrupt
the live file to test recovery.
