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
