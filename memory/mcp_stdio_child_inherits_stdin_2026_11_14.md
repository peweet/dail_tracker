# MCP stdio server: a child process must never inherit stdin

Since at least October the `dail-tracker` and `siting-private` MCP tools mostly timed out at
360 s. Over the transcripts of 20 October to 14 November, `dail-tracker` answered once in 48
calls and `siting-private` four times, at a median of 336 s. The client logs show the shape:
the call times out, and then the server's reply arrives as "Received a response for an unknown
message ID" at the moment the connection drops. Fifty of the 98 timeouts logged since
1 October have that shape, so the server was finishing the work and the reply was being held.

The cause was reproduced on 14 November 2026. Under the stdio transport the server's stdin is
the JSON-RPC pipe from the client, and a reader thread is blocked on it. The code index spawned
`git rev-parse` and `git ls-files` (`mcp_server/code_index.py`) without saying where their
stdin came from, so each child inherited that pipe. On Windows a `git` child that inherits it
stalls at startup until the client writes again or closes the pipe. In the reproduction the
child stayed blocked for 11 s while the pipe was open and silent, and returned 0.02 s after the
pipe closed. With `stdin=subprocess.DEVNULL` it returned in 0.02 s. `tasklist` did not stall
either way. Run in-process, with no pipe involved, the same search takes about 5.6 s cold,
which is why profiling the search code never found the hang.

The fix passes `stdin=subprocess.DEVNULL` to every spawn under `mcp_server/`, and
`test/mcp_server/test_subprocess_stdin.py` holds that with an AST scan. With the fix, the
real `_git_visible_paths` returned 2,784 paths in 0.04 s under the stalling condition. A
round trip through the Claude Code client needs the server restarted (`/mcp`, reconnect
`dail-tracker`), because a running server keeps the code it started with.

Two things remain open. The private server's `planning/product/mcp/project_index.py` spawns
`git ls-files` without `stdin=`. Its `timeout=10` bounds each call, so it does not account for
every `siting-private` hang, and the rest of that server's call path has not been audited.
Second, any other long-lived stdio server on this machine has the same exposure.

To recognise it again, read the client log under
`%LOCALAPPDATA%\claude-cli-nodejs\Cache\<project>\mcp-logs-<server>\` before blaming the
navigation reflex or the RAM guard. A timeout followed within seconds by "unknown message ID"
means the server answered and something held the reply.
