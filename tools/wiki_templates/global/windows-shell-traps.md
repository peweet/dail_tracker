---
description: Windows + Git Bash + PowerShell 5.1 traps for tool-shell commands and hand-over commands (personal, every project)
---

# Windows shell traps

- Commands handed to the user run in PowerShell or cmd.exe, never bash. No `\` line
  continuations, no `$VAR`, no `&&` in PowerShell 5.1. Hand over fully resolved text with no
  placeholder: it gets pasted verbatim.
- "It didn't work" after a paste means check the resulting state, not re-issue the command.
  Bracketed-paste escapes can turn line one into "command not found" and run the rest in the
  wrong state.
- Never edit a tracked file with PowerShell `Get-Content | Set-Content`; the round-trip
  mojibakes every non-ASCII character. Use the Edit tool. Keep `.ps1` files ASCII-only:
  PowerShell 5.1 cannot parse a BOM-less UTF-8 file that holds an em dash.
- PowerShell refuses `npx.ps1` / `npm.ps1` / `wrangler.ps1` shims. Call the sibling `.cmd`
  shim. Do not suggest `Set-ExecutionPolicy`.
- The Bash tool's heredoc strips one backslash level even with a quoted delimiter. Write a
  patch script to the scratchpad with the Write tool and run the file.
- Git Bash's GNU grep prints nothing for `-i` combined with `-F`. Use `rg`, or drop `-F`.
- A backtick inside a double-quoted echo runs as command substitution. Quote labels that
  mention minified JS in single quotes.
- `cmd 2>&1 | tail -N` reports tail's exit code, not the gate's. Capture to a file and read
  the failure block. `| head -N` on a consumer sweep hides the one consumer that matters.
- A detached long-runner started with `Start-Process` from the tool shell dies when the tool
  call returns. Use a one-shot `schtasks /SC ONCE` task running an ASCII `.cmd` launcher.
- `ssh` from the Bash tool must be `/c/Windows/System32/OpenSSH/ssh.exe`; Git Bash's own
  `ssh` fails public-key auth from the tool shell.
- On Windows a file still being appended to shows a stale directory mtime for hours. Size and
  the job's own log are the liveness signals, never mtime.
- Measure process memory as private bytes, never working set.
