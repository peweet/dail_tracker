You are working in the Dáil Tracker repo (Python). The MCP server lives at `mcp_server/server.py` — a FastMCP app where each `@mcp.tool` handler calls query functions from `dail_tracker_core`, using the module's shared connection accessor and the standard error-wrapping pattern.

Add a new MCP tool `member_question_count_by_year`, following the existing tool handler patterns exactly:

1. Signature: `member_question_count_by_year(member_name: str) -> list[dict]`.
2. It should reuse whichever existing core query function(s) the current member-questions tool uses, aggregating a count of questions per year for the member (aggregate in Python in the handler if the core function returns rows).
3. Follow the file's standard patterns for connection access, `SourceUnavailable` error wrapping, result shaping, and docstring style.
4. Place it in the appropriate domain section of the file per the section conventions.

At the very end of your response, output exactly this JSON block (fill in real values):

```json
{
  "files_read": [
    {"path": "mcp_server/server.py", "chars": 123456}
  ],
  "response_chars": 7890
}
```

`files_read` must list every file you read, with the total characters you actually received from reads of that file. `response_chars` is the total characters of code and text you produced.

Do NOT commit the change. Do NOT run any tests or the server. Stop after writing the code.
