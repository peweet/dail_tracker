# MCP server guidance

- Every tool is read-only. Keep `ToolAnnotations(readOnlyHint=True)` and return bounded, structured results.
- Tool names, signatures, and docstrings are an LLM-facing API. Prefer concise defaults and an explicit detailed mode for provenance-heavy output.
- Only repository-navigation tools may use `_ALWAYS`; domain tools must remain eligible for client-side deferral. `tools/check_mcp_catalog.py` enforces the always-loaded context budget.
- Reuse `dail_tracker_core.queries` and the shared lazy connection. Do not import the DuckDB/data stack eagerly at module import time.
- Add or update `test/mcp_server/` tests and run `uv run python tools/dev.py mcp-catalog`.
