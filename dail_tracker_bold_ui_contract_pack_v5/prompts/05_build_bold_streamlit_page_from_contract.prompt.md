Build page: `<PAGE_ID>` from its contract.

This is a bold UI redesign, not a safe refactor.

If an existing page file exists: it is a functional reference, not a design reference.
If no existing page file exists (greenfield): build from the contract and runbook only.

Read only:
1. `CLAUDE.md`
2. `page_runbooks/<PAGE_ID>.md`
3. `utility/page_contracts/<PAGE_ID>.yaml`
4. `utility/page_contracts/_shared_ui_policy.yaml`
5. `utility/page_contracts/_interaction_patterns.yaml`
6. matching page file (if it exists)
7. `utility/shared_css.py`              ← primary CSS file; read before adding any new classes
8. relevant `utility/ui` helpers

Do NOT read `utility/styles/base.css` — it is a legacy file; new styles go in shared_css.py only.
You may modify only the files listed in the page runbook.

Required:
- materially different layout (or strong civic design if greenfield)
- stronger editorial hero
- clearer controls
- better table/chart presentation
- source/provenance display only if the contract's source_links section requires it
- CSV current-view export
- empty states
- shared CSS and helper reuse

Mandatory API rules — do not use the forbidden alternatives:
- `st.html(...)` — not `st.markdown(..., unsafe_allow_html=True)`
- `width="stretch"` on buttons — not `use_container_width=True`
- `st.segmented_control` — not `st.radio(horizontal=True)`
- `st.space(n)` — not `st.write("")` spacer
- `html.escape(value)` on all dynamic text inside HTML strings
- `background: #ffffff` on cards — not `var(--surface)`
- `:material/icon_name:` for icons — not emoji strings
- All new CSS in `utility/shared_css.py` — not page-local blocks

Forbidden:
- backend changes
- raw Parquet scans
- persistent DuckDB file assumption
- view registration in Streamlit
- joins/groupby/metric definitions in Streamlit
- page-local CSS system
- any forbidden API listed above

Missing data:
`TODO_PIPELINE_VIEW_REQUIRED: <specific missing item>`
