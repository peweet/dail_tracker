# Dáil Tracker Streamlit AI Contract Pack

This pack is for a Streamlit app where the UI is a thin presentation layer over DuckDB analytical views that are created by the pipeline from Parquet-backed data.

The aim is to let Claude or ChatGPT produce a strong frontend without letting the model move joins, fuzzy matching, metric definitions, scraping, PDF parsing, or business logic into Streamlit.

## Contents

```text
CLAUDE.md
AGENT_SETUP.md
contracts/
  CONTRACT_AUTHORING_GUIDE.md
  PAGE_BRIEF_TEMPLATE.md
  PAGE_BRIEF_TEMPLATE.yaml
prompts/
  01_create_or_update_contract.prompt.md
  02_build_streamlit_page_from_contract.prompt.md
  03_create_pipeline_view_for_contract.prompt.md
  04_review_logic_firewall.prompt.md
  05_review_frontend_ux_accessibility.prompt.md
  06_review_design_drift.prompt.md
  07_review_sql_performance.prompt.md
  08_reduce_token_waste_scope.prompt.md
.claude/
  skills/streamlit-frontend/SKILL.md
  agents/*.md
utility/
  page_contracts/*.yaml
  data_access/contract_query_skeleton.py
  styles/CSS_REUSE_GUIDE.md
tools/check_streamlit_logic_firewall.py
docs/
  reconciled_frontend_strategy.md
  frontend_quality_cookbook.md
```

## Use this workflow

1. Fill in `contracts/PAGE_BRIEF_TEMPLATE.md` for a page.
2. Ask Claude to create or update the YAML contract with `prompts/01_create_or_update_contract.prompt.md`.
3. If the contract needs a missing view/column/metric, ask Claude to implement it in the pipeline with `prompts/03_create_pipeline_view_for_contract.prompt.md`.
4. Ask Claude to implement the page from the contract with `prompts/02_build_streamlit_page_from_contract.prompt.md`.
5. Review with prompts `04`, `05`, `06`, and `07`.
6. Run `python tools/check_streamlit_logic_firewall.py utility/pages_code/<page>.py`.

## Core rule

If SQL changes the meaning of the data, it belongs in the pipeline/view layer.

If SQL only selects, filters, sorts, and limits already-modelled data, it may live in Streamlit.
