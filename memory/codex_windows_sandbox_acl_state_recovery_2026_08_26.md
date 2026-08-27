# Codex Windows deny-read ACL state recovery

## Trigger

Use this card when nearly every Windows worker reports `apply deny-read ACLs`,
`CreateProcess`, or `helper_unknown_error` before even `echo`, `pwd`, or
`git status` starts.

## Confirmed evidence

On 2026-08-24 and again on 2026-08-26, Codex's sandbox log identified a JSON
parse failure in `%USERPROFILE%\.codex\.sandbox\deny_read_acl_state.json`. On
the second incident, that file was exactly 22 NUL bytes. Quarantining the state
and linked parse diagnostic allowed Codex to regenerate valid JSON; a canary and
eight parallel process launches then passed.

This is a sandbox bootstrap failure, not evidence that `rg`, Git, AST scanners,
MCP tools, or the repository are broken. Those tools cannot run until the child
process crosses the sandbox setup boundary.

## Durable response

Read `doc/CODEX_WINDOWS_SANDBOX_RECOVERY.md`. The installed per-user task invokes
`tools/codex_windows_sandbox_guard.ps1` outside the Codex runner. The guard acts
only on malformed JSON after retries, preserves timestamped evidence, and leaves
state regeneration to Codex. It never resets ACLs, kills processes, reads
sandbox secrets, or moves an unrelated setup error.

Keep PATH failures and `uv` cache-write failures as separate diagnoses because
both occur after process startup. OpenAI's supported fallback is the weaker
`unelevated` Windows sandbox while elevated setup is investigated; a full host
restart is needed after changing the setting.
