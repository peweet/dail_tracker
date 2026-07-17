---
name: build-page
description: Build one Streamlit page from its YAML contract with bold UI
argument-hint: "[page_id]"
agent: agent
---

Build page: ${input:page_id}

Read only:
- `CLAUDE.md`
- `page_runbooks/${input:page_id}.md`
- `utility/page_contracts/${input:page_id}.yaml`
- shared policy/pattern files, the target page file, shared CSS, relevant `utility/ui` helpers

Do not inspect unrelated pages or generated data folders. Use `search_project` / the
dail-tracker MCP to place data — never scan a parquet.

Implement the page with bold UI while preserving data semantics (logic firewall: no queries
or transforms in the page — those live in `utility/data_access/`). Emit
`TODO_PIPELINE_VIEW_REQUIRED` for any missing data instead of inlining a query.
