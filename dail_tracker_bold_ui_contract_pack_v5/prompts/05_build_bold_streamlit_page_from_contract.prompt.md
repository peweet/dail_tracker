Build page: `<PAGE_ID>` from its contract.

This is a bold UI redesign, not a safe refactor.

The existing page is a functional reference, not a design reference.

Read only:
1. `CLAUDE.md`
2. `page_runbooks/<PAGE_ID>.md`
3. `utility/page_contracts/<PAGE_ID>.yaml`
4. `utility/page_contracts/_shared_ui_policy.yaml`
5. `utility/page_contracts/_interaction_patterns.yaml`
6. matching page file
7. `utility/styles/base.css`
8. relevant `utility/ui` helpers

You may modify only the files listed in the page runbook.

Required:
- materially different layout
- stronger editorial hero
- clearer controls
- better table/chart presentation
- source/provenance display
- CSV current-view export
- empty states
- shared CSS/helper reuse

Forbidden:
- backend changes
- raw Parquet scans
- persistent DuckDB file assumption
- view registration
- joins/groupby/metric definitions in Streamlit
- page-local CSS system

Missing data:
`TODO_PIPELINE_VIEW_REQUIRED: <specific missing item>`
