# interests page — audit fixes

Date: 2026-05-03
Source: `/civic-ui-review interests`
Page: [utility/pages_code/interests.py](../utility/pages_code/interests.py)
Contract: `dail_tracker_bold_ui_contract_pack_v5/utility/page_contracts/interests.yaml`

5 high-severity, 9 medium-severity. Several fixes live in `utility/ui/components.py` and `utility/shared_css.py` and so will affect every page that uses those helpers — note this before scheduling the work.

## Recommended order of work

1. `components.py` cross-page hygiene — H4, H5, M3, M4 (small, lands before page work)
2. `interests.py` contract compliance — H1, H2, H3 (collapse into one change)
3. Inline-style extraction — M1
4. Emoji → Material icons — M2
5. Missing primary-view components — M5
6. Cleanup pass — M6, M7, M8, M9

---

## HIGH severity

### H1 — Streamlit reads parquet/CSV directly

- [ ] **Where:** [interests.py:81-88](../utility/pages_code/interests.py#L81-L88)
- **Problem:** `pd.read_parquet(...)` / `pd.read_csv(...)`. Violates contract `streamlit_may_read_parquet: false` and acceptance test `no_read_parquet_or_parquet_scan_in_page`.
- **Fix:** until `v_member_interests_detail` is registered, replace `_load_interests` body with a `todo_callout` and an `empty_state`. Per contract `view_fallback_policy`: "Never aggregate in Streamlit as a substitute for a missing view."

### H2 — `_fetch_member_index_fallback` is exactly the simulation the contract bans

- [ ] **Where:** [interests.py:237-272](../utility/pages_code/interests.py#L237-L272)
- **Problem:** `GROUP BY`, `COUNT(*)`, `COUNT(DISTINCT … CASE)`, `BOOL_OR`, `ROW_NUMBER() OVER`, plus `LOWER(TRIM(...)) <> 'no interests declared'` (string-cleaning fuzzy match). Violates `no_count_or_max_aggregation_in_streamlit`, `no_modelling_joins_or_groupby_in_streamlit`, `no_regex_fuzzy_matching_in_streamlit`, and `pipeline_simulation_policy.forbidden_even_in_simulation`.
- **Fix:** delete `_fetch_member_index_fallback`. The fallback path at [:660-697](../utility/pages_code/interests.py#L660-L697) becomes a single `todo_callout` for `v_member_interests_index`. The main `_load_ranking` path stays empty until the pipeline view exists.

### H3 — View registration in Streamlit

- [ ] **Where:** [interests.py:125](../utility/pages_code/interests.py#L125), [:162](../utility/pages_code/interests.py#L162), [:200](../utility/pages_code/interests.py#L200), [:236](../utility/pages_code/interests.py#L236)
- **Problem:** `con.register("v_member_interests", base)`. Violates `streamlit_may_register_views: false` / `no_create_view_or_table_in_page`.
- **Fix:** falls out automatically once H1/H2 are fixed — every `con.register` is upstream of those simulations.

### H4 — Section "headings" are paragraphs, not real headings

- [ ] **Where:** `utility/ui/components.py` — [`evidence_heading` :178-179](../utility/ui/components.py#L178-L179), [`member_profile_header` :366-374](../utility/ui/components.py#L366-L374)
- **Problem:** renders `<p class="section-heading">` and `<p class="td-name">`. Screen readers can't navigate the page by heading level. Pass D #5 violation. **Cross-page change.**
- **Fix:** change `evidence_heading` to render `<h2>`. Change `member_profile_header` to render the member name as `<h2>` (keep meta and badges as `<p>`). Style stays via class — only the tag changes.

### H5 — New / Removed declarations rely on red/green

- [ ] **Where:** [components.py:561-572](../utility/ui/components.py#L561-L572)
- **Problem:** `new` = `#16a34a` green; `removed` = `#dc2626` red. Mandatory colour rule forbids red/green pairs. Text `NEW` / `REMOVED` badges and `<s>` provide non-colour fallback so it isn't catastrophic, but the project palette is deuteranopia-safe blue/amber/teal/violet.
- **Fix:** swap to project palette. Suggested: `new` = teal (`border:#0e6655`, `bg:#f0fdfa`, matching `int-pill-company`); `removed` = amber/brown (`border:#78350f`, `bg:#fffbeb`, matching `int-pill-prop`). Keep `NEW` / `REMOVED` text and strikethrough.

---

## MEDIUM severity

### M1 — Inline `style="…"` attributes

- [ ] **Where (page):** [interests.py:422](../utility/pages_code/interests.py#L422), [:443-449](../utility/pages_code/interests.py#L443-L449), [:485](../utility/pages_code/interests.py#L485), [:508-510](../utility/pages_code/interests.py#L508-L510)
- [ ] **Where (components):** [hero_banner :144, :151](../utility/ui/components.py#L144), [stat_strip :163](../utility/ui/components.py#L163), [member_profile_header :368](../utility/ui/components.py#L368), [interest_declaration_item :573-577](../utility/ui/components.py#L573-L577)
- **Problem:** CLAUDE.md CSS rule explicitly forbids inline `style=""`.
- **Fix:** add named classes in `shared_css.py` (e.g. `.dt-callout-tight`, `.dt-source-doc-box`, `.dt-int-decl-row.is-new` / `.is-removed` / `.is-unchanged`) and replace inline `style=""` with class names.

### M2 — Emoji icons in pills

- [ ] **Where:** [interests.py:290-307](../utility/pages_code/interests.py#L290-L307) — 🔑 🏗️ 🏠 📈 🏢
- **Problem:** Pass B forbids emoji as icons (`:material/icon_name:` only). Screen readers announce emoji as their unicode names ("house with garden") — noisy.
- **Fix:** swap to Material symbols. Suggested: 🔑→`:material/key:`, 🏠→`:material/home:`, 🏗️→`:material/construction:`, 📈→`:material/trending_up:`, 🏢→`:material/apartment:`. Pills render via `st.html`, so embed Material icons via the existing pattern in `shared_css.py` rather than `st.write`.

### M3 — `unsafe_allow_html=True` in shared components

- [ ] **Where:** [evidence_heading :179](../utility/ui/components.py#L179), [todo_callout :183-187](../utility/ui/components.py#L183-L187), [member_profile_header :369-374](../utility/ui/components.py#L369-L374), [sidebar_date_range :385](../utility/ui/components.py#L385)
- **Problem:** Pass B requires `st.html`, not `st.markdown(..., unsafe_allow_html=True)`. **Cross-page change.**
- **Fix:** swap each `st.markdown(..., unsafe_allow_html=True)` → `st.html(...)`.

### M4 — Bug: malformed closing tag in `todo_callout`

- [ ] **Where:** [components.py:185](../utility/ui/components.py#L185)
- **Problem:** `<\code>` should be `</code>`. Renders as literal text on every page that uses `todo_callout`.
- **Fix:** one-character correction.

### M5 — Primary view missing contract-required components

- [ ] **inline_command_bar** — name filter + category filter + landlord/property flags. Absent. Acknowledged at [interests.py:651](../utility/pages_code/interests.py#L651).
- [ ] **category_chart_collapsed** — Altair horizontal bar inside expander, year-responsive. Absent.
- [ ] **export_button (primary view)** — only present on profile view at [:533](../utility/pages_code/interests.py#L533). Absent on browse.
- **Fix:** depends on `v_member_interests_index` and `v_member_interests_yearly_summary`. Until those exist, add `todo_callout` placeholders so the absences are tracked, satisfying `csv_export_exports_current_displayed_view` and `TODO_PIPELINE_VIEW_REQUIRED_used_for_missing_data`.

### M6 — `int-stat-pill` base background is non-white

- [ ] **Where:** [shared_css.py:869](../utility/shared_css.py#L869) — `background: var(--surface-deep)`
- **Problem:** the four civic variants override with explicit light tints, but a bare `int-stat-pill` (no modifier) renders on warm beige.
- **Fix:** change line 869 to `background: #ffffff;`.

### M7 — `_render_leaderboard` is dead code today

- [ ] **Where:** [interests.py:312-340](../utility/pages_code/interests.py#L312-L340)
- **Problem:** only invoked when `_load_ranking` returns non-empty, which it never does ([:223](../utility/pages_code/interests.py#L223) returns empty `DataFrame`).
- **Fix:** keep the function (matters once `v_member_interests_index` exists), but add a one-line comment noting it's the future-target rendering path. Once H2 is fixed, this becomes the only leaderboard path.

### M8 — Pagination state appears double-managed in fallback

- [ ] **Where:** [interests.py:672-697](../utility/pages_code/interests.py#L672-L697)
- **Problem:** manual `int_fb_page` counter in session state, then `pagination_controls(key_prefix="int_fb")` called below the cards. The helper namespaces its own state under `key_prefix` — likely collides.
- **Fix:** moot once H2 is fixed (fallback gets deleted). If kept in any form, drop the manual counter and call `pagination_controls(...)` *before* the card render so the returned `(page_size, page_idx)` drives the slice — matches the pattern at [:322-330](../utility/pages_code/interests.py#L322-L330).

### M9 — Profile view: silent absence when source-document URL is missing

- [ ] **Where:** [interests.py:440-449](../utility/pages_code/interests.py#L440-L449)
- **Problem:** if `interests_pdf_url` returns falsy, the user gets nothing — no callout, no message. Pass D #2 ("Solve a whole problem for users") wants missing data named, not blanked.
- **Fix:** add an `else` branch with a small caption pointing to the provenance footer, or a `todo_callout` for missing per-year source.

---

## Acceptance tests this audit closes

When the boxes above are ticked, these contract acceptance tests should pass:

- [ ] `no_read_parquet_or_parquet_scan_in_page` (H1)
- [ ] `no_create_view_or_table_in_page` (H3)
- [ ] `no_modelling_joins_or_groupby_in_streamlit` (H2)
- [ ] `no_count_or_max_aggregation_in_streamlit` (H2)
- [ ] `no_regex_fuzzy_matching_in_streamlit` (H2)
- [ ] `csv_export_exports_current_displayed_view` (M5)
- [ ] `TODO_PIPELINE_VIEW_REQUIRED_used_for_missing_data` (H1, H2, M5)
- [ ] `diff_feature_visible_and_prominent_in_profile` (already passing — preserved by H5 fix)
- [ ] `empty_categories_not_rendered_as_open_expanders` (already passing)

## What stays good (do not touch)

- Hero kicker + title only, no dek, no hero stats — Primary view simplicity rule
- Year pills via `st.pills`, newest-first, no "All years" default — Pass D #4
- Sidebar uses `st.segmented_control` for chamber — Pass B
- Provenance footer is one collapsed expander at the bottom
- `todo_callout` markers liberally used for missing pipeline views
- Back button in main content area, not sidebar
- Member drilldown via `→` button next to card — consistent with attendance / payments
