# Core query guidance

- Keep this layer Streamlit-free and return stable, serialisable results for both API and MCP callers.
- Put reusable domain queries in `dail_tracker_core/queries/`; presentation-specific shaping belongs in `utility/data_access/`.
- Reuse the shared connection and view-registration helpers. Do not open ad-hoc DuckDB connections in query functions.
- Preserve source URLs, dates, and confidence/status fields at the boundary.
- Run the matching `test/dail_tracker_core/` tests; API-facing changes also require the relevant `test/api/` tests and basedpyright.
