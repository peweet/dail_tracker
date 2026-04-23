config.toml = theme tokens and framework-level styling
base.css = shared component polish
YAML = declare which assets and layout conventions a page uses
Streamlit page = render only


prompt for Claude code
You are editing only the Streamlit presentation layer.

Inputs:
- CLAUDE.md
- utility/styles/base.css
- utility/page_contracts/member_overview.yaml
- utility/style_contracts/newsroom_light.yaml
- utility/app.py
- mart schema or small sample rows

Rules:
- Keep the shared CSS system.
- Respect the page contract.
- Improve layout, hierarchy, filters, readability, chart choice, and table usability.
- Do not add joins, merges, fuzzy matching, KPI math, parsing, or reusable business logic in Streamlit.
- Allowed logic: widget state, local filtering on already-loaded mart columns, sorting, formatting, CSV export, empty states.
- If you think pandas/groupby/merge logic is needed, do not implement it.
- Instead add: TODO_GOLD_REFACTOR: <what should move to DuckDB/gold>


https://chatgpt.com/c/69e94700-3f58-83eb-aaf3-4fe59fd13630
for final polish
npx impeccable detect utility/