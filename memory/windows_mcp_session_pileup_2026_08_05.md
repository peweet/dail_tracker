# Windows MCP session pileup — 2026-08-05

## Lesson

On Windows, the Dáil Tracker stdio MCP server can accumulate stale process trees under
a shared VS Code/Codex extension host. Do not infer that one extension-host parent is
one active chat session. Scan with:

```powershell
tools/spin_down_python.ps1 -Category mcp
tools/spin_down_python.ps1 -Category mcp -SpinDown -WhatIf
```

Use `-SpinDown` only after the preview. The default 20-minute age guard retains recent
processes; `-IncludeYoung` is the deliberately broad override.

## Evidence

On 2026-08-05, a performance investigation found 108 Python processes classified as
`mcp_server/server.py`, representing about 54 logical MCP sessions under the configured
`uv -> venv Python -> system Python` launch chain. The MCP-only cleanup reduced that to
zero. Many of the apparent cleanup failures were expected: killing a parent/child tree
caused later child PIDs in the scan to have exited already.

A later read-only scan found six fresh logical sessions, all ten minutes old or less.
This demonstrates that the extension starts fresh chains after a clear; process ancestry
alone cannot tell whether each is an active chat or a reconnect. Treat a small, recent
set as the baseline and a rising older set as the pileup signal.

Both `.mcp.json` and `.vscode/mcp.json` define `dail-tracker` for different clients. This
is a possible duplication vector, but it does not by itself explain dozens of sessions
from one Codex extension host. `.vscode/mcp.json` also enables a development watcher, so
file edits make clean restart behaviour important.

## Guardrail scope

`tools/spin_down_python.ps1` now reports logical MCP-server count and warns above seven.
It must not automatically kill every MCP descendant of a shared extension host: the host
does not expose enough process-level identity to separate a current chat from another
chat. The age guard is the safe default boundary.

Claude's durable-learning system did not previously prevent this because no matching
discovery row existed. Its closeout hook asks an agent to assess and manually record a
lesson; it does not create one automatically. Its SessionEnd hook stops Streamlit only,
not MCP server trees.

## Opt-in startup correction — 2026-10-04

Stopping old processes does not change MCP startup configuration. Five older
Codex groups contained 25 logical servers and 105 processes across dail-tracker,
Siting, Firstmate, Chrome DevTools, and Next DevTools. All 105 were confirmed gone
after the user approved cleanup; newer groups remained running.

Codex's effective configuration still enabled those servers for new sessions.
The disabled `siting_private` entry did not disable the separate `siting-private`
entry. Keep both aliases disabled. Repository MCP entries now default to
`enabled = false`; workstation Firstmate and Chrome DevTools defaults are also
disabled. The VS Code development watcher was removed, and this workstation's
`chat.mcp.autostart` is set to `never`.

The session-start health check was another independent launcher: it probed the
Claude-style `.mcp.json` entry even when Codex's server was disabled. It now
performs static validation by default and launches a live probe only with the
explicit `DAIL_SKIP_MCP_PROBE=0` opt-in. The regression test
`test_default_mcp_note_never_starts_server` failed with `default hook started MCP`
before the change; the existing probe/cache tests opt in explicitly.

Explicit Codex CLI activation for one session is, for example:

```powershell
codex -c 'mcp_servers.dail-tracker.enabled=true'
```

Enable only the requested server; do not persistently restore startup defaults.
Use `codex mcp list --json` to inspect the merged configuration without starting
servers. Existing clients can retain loaded configuration, so reload the Codex
client after changing defaults. In VS Code, use `MCP: List Servers` to explicitly
start a server. `chat.mcp.autostart` controls VS Code's own startup pass, not
independent Agent Host/Copilot SDK processes.

Sources: [Codex MCP configuration](https://developers.openai.com/codex/mcp) and
[VS Code MCP startup](https://code.visualstudio.com/docs/agent-customization/mcp-servers#automatically-start-mcp-servers).
