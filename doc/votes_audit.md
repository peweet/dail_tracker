# Civic UI Review — votes page

Reviewed `utility/pages_code/votes.py` and `utility/ui/vote_explorer.py` against `dail_tracker_bold_ui_contract_pack_v5/utility/page_contracts/votes.yaml`.

## High-severity

### 1. Deprecated `use_container_width=True` on plotly charts — Pass B
`vote_explorer.py:186` and `vote_explorer.py:234` pass `use_container_width=True` to `st.plotly_chart`. The skill and project-wide convention is `width="stretch"`. These are the two charts the votes page renders (party breakdown bar in Mode C, year stack in Mode B), so the violation hits user-visible output directly.

### 2. Topic classification performed in Streamlit — Pass A logic firewall
`votes.py:42-52` hardcodes `_TD_PICKER_TOPICS` (Housing, Health, Disability, Climate, Energy, Palestine, Neutrality, Education, Childcare) with ILIKE patterns, and `votes.py:236-252` executes them as `WHERE ({likes})` against `v_vote_member_detail`. The inline comment claims "presentation-only filter, not modelling" but this is theme classification — the contract itself says `theme column — theme classification is currently done via regex in Streamlit; must become a pipeline-owned column on v_vote_index` (votes.yaml:42-44). A `todo_callout` should be visible to users until the pipeline view exists, and the topic labels should not be hardcoded in the page file.

### 3. Filtering on `debate_title` is not an approved filter — Pass A
The contract's `approved_filters` for `v_vote_member_detail` are `vote_id`, `member_id`, `member_name`, `vote_type`, `vote_date`. `debate_title` is listed as an optional column but has no approved operators. The `_fetch_topical_votes` query at `votes.py:241-250` uses `debate_title ILIKE ?`, which exceeds the approved retrieval surface.

## Medium-severity

### 4. Identity and section headings are styled `<p>` tags, not real headings — Pass D #5
`vote_explorer.py:351` renders the TD name as `<p class="td-name">`, and `components.py:178-179` renders every section title as `<p class="section-heading">`. Screen readers cannot navigate by heading because there are no `<h2>`/`<h3>` elements between the page `<h1>` and content. Breaks the heading-level nesting requirement.

### 5. "Sponsored bills" placeholder rendered outside the bordered TD panel — Pass C
`votes.py:545-552` calls `render_td_panel` (which wraps its content in `st.container(border=True)`), then emits an `evidence_heading("Sponsored bills")` + `todo_callout` *after* the container closes. The TD profile panel reads as one bordered card with a stray heading floating beneath it. Either move the placeholder inside the container, or commit to omitting the section until the pipeline view exists.

### 6. Helpers used by the votes page bypass `st.html` — Pass B
`components.py:179` (`evidence_heading`) and `components.py:183-187` (`todo_callout`) use `st.markdown(..., unsafe_allow_html=True)` instead of `st.html`. Both helpers fire repeatedly on the votes page (Mode A `todo_callout`, Mode B/C section headings, every callout in `vote_explorer.py`). Standardise on `st.html` to match the rest of the codebase.

## Passed

- **Pass A** — backend untouched, no `read_parquet` / `parquet_scan`, no `CREATE VIEW` / `CREATE TABLE`, no GROUP BY-multi or HAVING/WINDOW, parameter binding consistent, hard-coded paths absent. `TODO_PIPELINE_VIEW_REQUIRED` is correctly raised for `td_sponsored_bills`, `td_vote_year_summary`, and the source URL provenance gap.
- **Pass B** — `st.html` used in the page itself, `width="stretch"` on the TD picker buttons, `st.segmented_control` for view toggle and member-list position filter, `html.escape` applied to every dynamic value, card backgrounds use `#ffffff` (no `var(--surface)` traps), no page-local CSS block.
- **Pass C** — material redesign vs old page (kicker + h1, year pills, division cards, two-stage TD flow with topical landing cards, Mode-C evidence panel with stat strip + party stack chart). Year pills used for primary navigation. Empty states are human and informative. Back buttons in main content area. Card-based primary index, no `st.dataframe` as primary control.
- **Pass D** — primary user question answered above the fold; drilldown obvious; provenance footer with Oireachtas attribution; "TD"/"TAA" not expanded but acceptable for a tracker-domain audience; reuses `dt-*`, `vt-*`, `td-*`, `td-pick-*` class families and `components.py` helpers; gov/opposition red/green pairing is mitigated by ✓/✗ glyphs and explicit "Voted Yes"/"Voted No"/"Carried"/"Lost" text on every pill, so deuteranopia distinction does not depend on hue alone.
