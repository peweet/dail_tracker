# Page Runbook — Member Overview

## Target

Page ID: `member_overview`
Page file: `utility/pages_code/member_overview.py`
User question: **What does the full public accountability record of this TD look like across all datasets?**

Reference product: **theyworkforyou.com** — a single politician page that aggregates attendance,
votes, interests, payments, and other public record evidence in one place. The user forms their
own view. The page does not editorialize.

---

## Skills and commands for this page

| Step | Invoke | What it does |
|---|---|---|
| Explore | `explore` agent | Read-only file scan, no edits |
| Shape | `/shape member_overview` | Design brief, no code |
| Implement | `/streamlit-frontend member_overview` | Bold UI from contract |
| Fast build | `/build-page member_overview` | Read contract + implement |
| Review | `/civic-ui-review member_overview` | Logic + UI + accessibility check |
| Full redesign | `/bold-redesign-page member_overview` | Plan before coding |
| Pipeline TODOs | `/pipeline-view [TODO item]` | Implement missing views/columns |

Each skill loads its own SKILL.md context. **Do not copy-paste the skill's instructions into your
prompt — just invoke the skill and add the page-specific supplements below.**

---

## Page-specific supplements (add these after invoking any skill)

These facts are not in the shared skills and must be provided alongside each invocation.

```
Member Overview supplements:

1. This is a GREENFIELD page — no existing page to preserve.

2. TWO-STAGE FLOW:
   Stage 1 = TD browse/index. Stage 2 = single TD full profile.
   Entry to Stage 2 can be: row click in Stage 1 table, sidebar selectbox,
   or direct URL via st.query_params.get("member") = join_key.

3. NO PROVENANCE FOOTER — this page merges 5-6 disparate sources.
   A combined provenance section is too complex and would be misleading.
   Any data quality caveat goes as a brief inline note in the relevant domain
   section only. No about expander. No provenance section anywhere.

4. STAGE 1 MART NOT BUILT YET:
   v_member_overview_browse does not exist.
   Stage 1 must show identity columns only (name, party, constituency, gov/opp).
   Add a visible callout listing the cross-dataset columns pending pipeline:
   attendance_rate, payment_total_eur, declared_interests_count,
   lobbying_interactions_count, revolving_door_flag.
   Mark each with TODO_PIPELINE_VIEW_REQUIRED: v_member_overview_browse.

5. STAGE 2 QUERIES EXISTING PER-DOMAIN VIEWS filtered by join_key:
   - v_attendance_member_year_summary  WHERE join_key = ?
   - v_payments_member_detail          WHERE join_key = ?
   - v_vote_td_summary                 WHERE join_key = ?
   - v_member_interests                WHERE join_key = ?
   - v_lobbying_summary                WHERE join_key = ?
   - v_lobbying_revolving_door         WHERE join_key = ?
   - v_legislation_index               WHERE join_key = ?
   These views exist now. Stage 2 CAN be implemented.

6. DOMAIN TABS — use st.tabs with Material icon labels:
   :material/calendar_today: Attendance
   :material/how_to_vote: Votes
   :material/interests: Interests
   :material/payments: Payments
   :material/groups: Lobbying
   :material/gavel: Legislation

7. CARD + RIGHT BUTTON HEIGHT RULE:
   For any ranked list row with a → button:
   Use CSS grid unit (display:grid; grid-template-columns:1fr auto; align-items:stretch).
   Right panel: <a href="?member={join_key}" class="dt-action">→</a>
   Never: ghost HTML, MutationObserver, setTimeout, position:absolute overlays.

8. COLOUR ACCESSIBILITY (mandatory):
   Government badge = blue. Opposition badge = amber. Never red/green.
   No colour pair may rely solely on red/green distinction.
   Reuse established pill palette from interests.py for category chips.

9. NO YEAR PILLS at page level — each domain tab manages its own year scope.
   Attendance and Payments tabs have per-tab year pills (st.pills, newest first).
   Votes, Interests, Lobbying, Legislation do not need year pills.

10. BACK NAVIGATION:
    st.button("← Back to all members") at the TOP of main content in Stage 2.
    Not sidebar only. Also clear st.query_params on back.
```

---

## Files Claude may modify

- `utility/pages_code/member_overview.py`
- `utility/shared_css.py`
- `utility/ui/components.py`
- `utility/ui/table_config.py`
- `utility/ui/export_controls.py`
- `utility/page_contracts/member_overview.yaml`

## Files Claude must not modify

- `pipeline.py`, `enrich.py`, `normalise_join_key.py`
- `data/`, `sql_views/`
- Any other page file

---

## Step 1 — Explore only

Use the `explore` agent. Paste this prompt:

```text
We are working on member_overview only.

Read only — do not edit any files:
1. CLAUDE.md
2. page_runbooks/member_overview.md
3. utility/page_contracts/member_overview.yaml
4. utility/page_contracts/_shared_ui_policy.yaml
5. utility/shared_css.py
6. utility/ui/components.py
7. sql_views/attendance_member_year_summary.sql
8. sql_views/payments_member_detail.sql
9. sql_views/vote_td_summary.sql
10. sql_views/member_interests_views.sql
11. sql_views/lobbying_summary.sql
12. sql_views/lobbying_revolving_door.sql
13. sql_views/legislation_index.sql

Do not scan data/ folders. Do not read other page files.

Return only:
1. Which per-domain views have a usable join_key filter column
2. Which views are missing a per-member filter (these need TODO_PIPELINE_VIEW_REQUIRED)
3. Whether legislation_index has a sponsor join_key column
4. Whether vote_td_summary exposes per-vote detail or aggregate only
5. CSS class families in shared_css.py reusable for this page
6. components.py helpers reusable for this page
```

---

## Step 2 — Shape before code

```text
/shape member_overview

[add the Page-specific supplements block from the runbook]
```

The `/shape` skill handles the generic Dáil Tracker design thinking. The supplements
give it the member_overview-specific constraints (no provenance, mart pending, etc.).

---

## Step 3 — Update contract if needed

Only if the shape output reveals contract gaps:

```text
Update utility/page_contracts/member_overview.yaml based on the shape plan.

Keep:
- data_access.mode: duckdb_in_process_registered_analytical_views
- persistent_duckdb_file: null
- NO provenance footer (see contract source_links note)
- TODO_PIPELINE_VIEW_REQUIRED for v_member_overview_browse and any missing columns

Do not add Streamlit-side modelling or joins.
```

---

## Step 4 — Implement

```text
/streamlit-frontend member_overview

[add the Page-specific supplements block from the runbook]
```

Or use the fast-build command which reads the contract and implements in one pass:

```text
/build-page member_overview

[add the Page-specific supplements block from the runbook]
```

The skill handles the generic data boundary and boldness requirements.
The supplements add what the skill does not know: no provenance, two-stage flow,
per-domain view list, mart pending, colour accessibility rules.

---

## Step 5 — If profile sections look too thin

Each domain tab should feel like an evidence panel, not a filtered dataframe dump.
If any tab is missing a headline stat, empty state, or visual affordance:

```text
/streamlit-frontend member_overview

The Stage 2 domain tabs are too sparse. For each tab add:
1. A bold headline stat (one number + one plain-language sentence)
2. A visual affordance where it adds insight:
   - Attendance: ProgressColumn on attendance_rate
   - Votes: simple aye/níl/absent breakdown with st.metric or mini bar chart
   - Interests: category pill strip reusing interests.py pill palette
   - Payments: NumberColumn formatted "€{:,.2f}" + TAA band always visible
   - Lobbying: revolving door badge if flagged (st.badge, color="orange")
   - Legislation: oireachtas_url as labelled link via st.column_config.LinkColumn
3. A human empty state per tab: empty_state(heading, body) from components.py

[add the Page-specific supplements block from the runbook]
```

---

## Step 6 — Review

```text
/civic-ui-review member_overview

Additional checks specific to this page:
- No provenance footer rendered anywhere (correct per contract)
- Stage 1 has visible TODO callout for pending mart columns
- Stage 2 back button is at TOP of main content, not sidebar only
- URL query param ?member=join_key loads Stage 2 directly
- All six domain tabs present (or explicit TODO if view blocks it)
- No red/green colour pairs used for any distinction
- st.html used (not unsafe_allow_html)
- width="stretch" on all buttons (not use_container_width)
- html.escape on all dynamic HTML text
```

---

## Docs to read by phase

### Pre-design (read before `/shape`)

| Doc | Why |
|---|---|
| `docs/BOLD_UI_REDESIGN_PROTOCOL.md` | Defines "materially different"; primary view noise budget; member profiles are the rich view |
| `docs/UI_DESIGN_SYSTEM.md` | Editorial tone; identity strip + stats + domain sections pattern; mandatory API rules table |
| `docs/PIPELINE_VIEW_BOUNDARY.md` | Confirms what belongs in pipeline vs Streamlit; SQL view inspection rule |
| `docs/INTERACTION_PATTERNS.md` | Two-stage flow mechanics; sidebar order; year pills; session state keys; back button rule |
| `utility/styles/CSS_REUSE_GUIDE.md` | Existing CSS families before adding any new class; card+button `:has()` pattern |

### During coding (load alongside `/streamlit-frontend`)

| Doc | Why |
|---|---|
| `docs/streamlit_skill_using_layouts.md` | `st.columns` alignment, containers, `horizontal=True`, borders for section grouping |
| `docs/streamlit_skill_displaying_data.md` | `st.dataframe` `column_config` — ProgressColumn, NumberColumn, LinkColumn, CheckboxColumn |
| `docs/streamlit_skill_choosing_selection_widgets.md` | `st.segmented_control` vs `st.pills` vs `st.selectbox` — which widget for which control |
| `docs/streamlit_skill_using_session_state.md` | Stage 1→2 navigation; `st.session_state.pop` for back button; key scoping |
| `docs/streamlit_skill_building_multipage_apps.md` | `st.query_params` for `?member=join_key` direct-link entry |
| `docs/streamlit_skill_improving_design.md` | Material icon names; `st.badge` syntax; `st.caption` vs `st.info`; spacing polish |
| `docs/CHART_AND_TABLE_STYLE_GUIDE.md` | Altair timeline strip pattern for Attendance tab; ranking cards vs tables decision |

### If tabs feel sparse (Step 5)

| Doc | Why |
|---|---|
| `docs/streamlit_skill_displaying_data.md` | Sparklines, progress bars, metric deltas for domain headline stats |
| `docs/UI_DESIGN_SYSTEM.md` | Hero statistic typography; leaderboard card anatomy |

### Review (load alongside `/civic-ui-review`)

| Doc | Why |
|---|---|
| `docs/BOLD_UI_REDESIGN_PROTOCOL.md` | Required-difference test; six dimensions checklist |
| `docs/UI_DESIGN_SYSTEM.md` | API rules table to verify compliance |
