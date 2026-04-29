# Dáil Tracker Bold UI Contract Pack v5

This pack is designed to help Claude Code produce a more attractive, more functional Streamlit frontend without moving business logic into Streamlit.

The central correction in this version:

> Streamlit is a thin **data-semantics** layer, not a thin **user-experience** layer.

That means:
- The pipeline and in-process DuckDB registered analytical views own modelling, joins, rollups, metrics, flags, fuzzy matching, and raw Parquet access.
- Streamlit owns layout, interaction, charts over already-shaped data, tables, filter controls, source links, member drilldowns, CSV export, and strong visual hierarchy.

This pack does **not** assume a persistent `.duckdb` database file. It assumes analytical SQL views are registered into an in-process DuckDB connection by the application/pipeline exposition layer. Streamlit page files query those approved registered views only.

## What is included

```text
CLAUDE.md
INTEGRATION_GUIDE.md
AGENT_SETUP.md

page_runbooks/
  attendance.md
  votes.md
  interests.md
  payments.md
  lobbying.md
  member_overview.md
  committees.md
  legislation.md
  home_overview.md

prompts/
  00_bold_ui_redesign_protocol.md
  01_explore_relevant_files.prompt.md
  02_shape_page_ux_before_code.prompt.md
  03_create_or_update_contract.prompt.md
  04_create_pipeline_view_for_contract.prompt.md
  05_build_bold_streamlit_page_from_contract.prompt.md
  06_review_logic_firewall.prompt.md
  07_review_frontend_ux_accessibility.prompt.md
  08_review_design_drift.prompt.md
  09_review_sql_performance.prompt.md
  10_reduce_token_waste_scope.prompt.md
  11_design_critique_after_implementation.prompt.md
  12_library_choice_review.prompt.md
  13_apply_shared_behaviours_across_pages.prompt.md
  14_apply_votes_explorer_flow.prompt.md
  15_force_bolder_ui_redesign.prompt.md
  16_wire_todo_pipeline_view_required.prompt.md

.claude/
  commands/
  skills/
  agents/

docs/
  BOLD_UI_REDESIGN_PROTOCOL.md
  UI_DESIGN_SYSTEM.md
  LIBRARY_POLICY.md
  INTERACTION_PATTERNS.md
  REGISTERED_ANALYTICAL_VIEWS.md
  GOVERNMENT_SOURCE_LINKS.md
  TOKEN_DISCIPLINE.md
  CHART_AND_TABLE_STYLE_GUIDE.md
  PIPELINE_VIEW_BOUNDARY.md

utility/
  page_contracts/
  ui/
  data_access/

tools/
  check_streamlit_logic_firewall.py
  check_contract_shape.py
  check_prompt_context_budget.py
```

## Start here

1. Copy the contents of this pack into the root of your Dáil Tracker repository.
2. Start Claude Code from the repo root.
3. Tell Claude to read `CLAUDE.md` only.
4. Then open one page runbook, for example `page_runbooks/attendance.md`, and paste Step 1 only.
5. Move through the runbook one step at a time.

Do not ask Claude to redesign the whole app in one pass.
