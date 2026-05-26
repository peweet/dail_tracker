# Register of Members' Interests — Impeccable audit (2026-05-26)

> **Status update (2026-05-26):** P0 + all 5 P1s + P2-1 + P2-3 shipped.
> See `[[project-interests-audit-2026-05-24]]` memory for the rework
> diary. Score lifted 13/20 → ~17/20. Open: P2-4 (loading state, low
> impact), P3 polish, Part 3 logic-firewall items (blocked on
> `v_member_interests_*` pipeline views).


Page audited: `/rankings-interests` (`utility/pages_code/interests.py`).
Methodology: 24 Playwright screenshots across desktop / tablet / mobile,
8 phases — landing, chamber toggle, year switch, notable chips,
typeahead, no-match, pagination, legacy `?member=` redirect.
Capture script: `audit_screenshots/_interests_capture.py`.
Screenshots: `audit_screenshots/_interests/*.png`.

**Score: 13 / 20 — Good with a P0 blocker.** One primary CTA is dead
(Find-a-TD typeahead + Notable chips write session state with no
downstream consumer). Once that is rewired to navigate to
`/member-overview`, the score lifts to 15+/20.

> Part 3 below preserves an earlier civic-ui-review (2026-05-03) with
> code-level contract findings (`H1`–`H5`, `M1`–`M9`) that mostly
> remain unaddressed. Most of those are "should land in `components.py`
> + the pipeline view registration" — orthogonal to the UX findings
> here. Both lists are valid; they target different layers.

---

## Part 1 — Findings (Playwright-grounded, 2026-05-26)

### P0 — Blocker (1)

**P0-1 — Find-a-TD typeahead + Notable chips are a dead-state primary CTA.**

Evidence: `E01_typeahead_with_query.png`, `E02_typeahead_after_pick.png`,
`D02_notable_chip_clicked_McDonald.png`. In `interests.py:664` the
notable-chip helper writes the picked name to
`st.session_state["selected_td"]` and reruns; in `interests.py:722-724`
the main-panel typeahead does the same. **No branch anywhere in
`interests_page()` reads `selected_td` to render a profile or navigate.**
After Phase 3 lifted the per-TD profile to `/member-overview`, the
writes were left in place but the navigation was never wired up.
Visually confirmed: after picking "Mary Lou McDonald" in the typeahead,
the page body is identical to the landing view (same "Coming soon."
callout, same Michael Healy-Rae card at top). Same for clicking a
notable-member chip — only the tooltip ("Mary Lou McDonald") flashes,
then the page re-renders the same browse view.

This is the worst UX hazard on the page: the primary call-to-action
under the hero appears broken. Citizens who type a name expect to land
on that TD's interests profile.

**Fix** — exactly as the cards do (`interests.py:357-360`):
```python
code = resolve_member_code(name)
if code:
    target = member_profile_url(code, section="interests")
    st.html(f'<meta http-equiv="refresh" content="0;url={_h(target)}">')
    st.stop()
```
The cards already use this contract correctly via `clickable_card_link`.

### P1 — High leverage (5)

**P1-1 — "Coming soon. a ranked leaderboard…" — lowercase after period.**

Evidence: `A02_landing_above_fold.png`. The `todo_callout()` helper
extracts citizen-facing copy after the first em-dash but doesn't
re-capitalise the first letter, producing:
> "Coming soon. a ranked leaderboard (most declarations…)…"

Looks unpolished. Fix in `ui/components.py:todo_callout` — capitalise
the first character of the extracted post-em-dash segment. The same
problem is flagged on Committees P1-1
([[project_committees_audit_2026_05_26]]); fix once in the helper.

**P1-2 — Rank prefix invisible on ~80% of Dáil cards (avatar collision).**

Evidence: `G01_paginator_at_bottom.png` — only `#8 Micheál Carrigy`
shows a rank, because he has no photo avatar. Every Dáil member with a
photo loses their rank number — the leaderboard's primary signal.
Seanad cards (`B01_seanad_landing.png`) show ranks because most
Senators lack photos. `_int_member_card_html` passes `rank` to
`member_card_html` but the layout slots rank where the avatar lives.

Fix in `ui/components.py:member_card_html`: render rank as a small
badge overlay on the avatar circle (`position:absolute; bottom:-6px;
right:-6px;`), or move rank to its own column outside the avatar slot.

**P1-3 — "Healy-Rae" appears twice in Notable Members, no disambiguation.**

Evidence: `A03_landing_year_pills_typeahead.png` sidebar. Two
`Healy-Rae` chips — Danny and Michael — are indistinguishable until
hover reveals the tooltip. A citizen scanning quickly picks at random.

Fix in `ui/components.py:render_notable_chips`: when a notable chip's
surname collides, render `<surname>, <first-initial>.`
(e.g. "Healy-Rae, D." / "Healy-Rae, M.").

**P1-4 — Search-input affordance is misleading.**

Evidence: `E01_typeahead_with_query.png` — typing "Mary Lou" shows
Streamlit's default `Press Enter to apply` hint. But the search input
doesn't filter the leaderboard cards — it only narrows the adjacent
selectbox options. Citizens will press Enter expecting the page to
filter and see nothing happen (the cards below stay at 170 of 170).

Fix: replace the `text_input + selectbox` pair in
`ui/components.py:main_member_jump` with a single `st.selectbox` whose
options are searchable (typeahead is built into `st.selectbox`). This
removes the misleading hint and means typing immediately filters the
dropdown (which downstream will navigate after P0-1 is in place).

**P1-5 — Year-switch gives no transition feedback.**

Evidence: `C01_year_switched.png` after clicking the 2024 pill. The
`MEMBERS · 2025 · 170` caption ticks to `MEMBERS · 2024 · 171` but
nothing else signals "you changed year" — no flash, no toast, no
year-relative heading. Easy to miss.

Fix: change the heading in `interests.py:758` from
`Members · {year} · {n}` to
`Declarations for {year} · {n} members` — verbalises the year as
primary context.

### P2 — Polish (4)

**P2-1 — `Coming soon.` callout occupies prime above-the-fold space.**

Evidence: `A02_landing_above_fold.png`. The unranked-fallback notice
takes the full hero-width slot directly under the year pills. Now that
the fallback delivers a usable member list, demote it to a small
caption under the heading.

**P2-2 — `Press Enter to apply` tooltip leaks** — Streamlit's default
text-input hint. Resolves automatically once P1-4 lands.

**P2-3 — Pill colour semantics undocumented.**

Evidence: `A04_landing_member_cards.png`. Declarations (blue), Landlord
(orange), Property owner (green), Shareholder (purple) carry meaning
citizens have to infer. Add a one-line caption above the leaderboard
or a small legend chip strip explaining the encoding.

**P2-4 — Notable-chip click leaves only a hover tooltip as feedback.**

Evidence: `D02_notable_chip_clicked_McDonald.png`. After P0-1 lands,
this resolves naturally (the chip navigates). Listed separately because
during the navigation rerun there should be a transient pressed state.

### P3 — Low-priority (3)

**P3-1 — "NOTABLE MEMBERS" kicker spacing loose** — `margin-bottom`
slightly too generous below the kicker, above the chips.

**P3-2 — Card focus outline persists across screenshots** — likely
`:focus-visible` from keyboard nav (good). Confirm `:focus` (mouse) is
suppressed via `:focus-visible` specificity.

**P3-3 — "Members · 2025 · 170" caption could be a stat strip.**
Civic-data convention on this app is to elevate counts as primary
stats (Attendance / Lobbying year strips). Dot-caption already works,
so low-priority.

---

## Part 2 — Uplift prompt (self-contained)

> You are uplifting the Register of Members' Interests page
> (`utility/pages_code/interests.py`) after a Playwright audit. The full
> audit is in `doc/INTERESTS_AUDIT.md`; do not regress anything in
> "Positive findings" or Part 3 fixes that are already in place.
>
> **Goal** — close 1 P0 + 5 P1 + 4 P2 in priority order.
>
> **Workflow**:
> 1. Open `interests.py` and `ui/components.py`.
> 2. For each finding below, write the before/after (file, line, exact
>    replacement) before editing.
> 3. After all edits, re-run the capture:
>    ```
>    $env:PYTHONIOENCODING = "utf-8"
>    python audit_screenshots/_interests_capture.py
>    ```
>    and review the diff in `audit_screenshots/_interests/`.
> 4. Tick each finding off in `project_interests_audit_2026_05_26.md`
>    in memory with the screenshot citation that verifies the fix.
>
> **Findings to close** (priority order):
>
> 1. **P0-1 — Dead-state primary CTA.**
>    In `interests.py:664` (notable chips) and `interests.py:722-724`
>    (typeahead), wire both to navigate via the same contract the cards
>    use:
>    ```python
>    code = resolve_member_code(name)
>    if code:
>        target = member_profile_url(code, section="interests")
>        st.html(f'<meta http-equiv="refresh" content="0;url={_h(target)}">')
>        st.stop()
>    ```
>    Test by clicking each Notable chip and picking each typeahead
>    option — each should land on
>    `/member-overview?member=<code>#interests`.
>
> 2. **P1-1 — Recapitalise after `todo_callout` em-dash split.**
>    Fix once in `ui/components.py:todo_callout` — after splitting at
>    the first em-dash and stripping the `TODO_PIPELINE_VIEW_REQUIRED:`
>    prefix, capitalise the first character. Verify both callsites
>    here (`interests.py:743`, `interests.py:603`) AND the Committees
>    P1-1 site flagged in [[project_committees_audit_2026_05_26]].
>
> 3. **P1-2 — Restore rank visibility on cards with avatars.**
>    Modify `ui/components.py:member_card_html` so rank is always
>    visible — either render rank as a small badge OVERLAYING the
>    avatar circle, or as its own pre-avatar column. Verify on Dáil
>    cards via `A04_landing_member_cards.png` re-capture — all 12 cards
>    should show their rank.
>
> 4. **P1-3 — Disambiguate colliding Notable surnames.**
>    In `ui/components.py:render_notable_chips`, count surname
>    collisions in the `visible` list; when a surname appears more than
>    once, render `<surname>, <first-initial>.` for all occurrences.
>    Verify with "Healy-Rae, D." and "Healy-Rae, M." chips.
>
> 5. **P1-4 — Fix search affordance.**
>    Collapse `main_member_jump` from `text_input + selectbox` to a
>    single `st.selectbox` with searchable options. This removes the
>    misleading "Press Enter to apply" hint and means typing immediately
>    filters. Update all callsites (multiple pages share this helper).
>
> 6. **P1-5 — Year-relative leaderboard heading.**
>    Replace
>    `evidence_heading(f"Members · {selected_year} · {len(members_df)}")`
>    in `interests.py:758` with
>    `evidence_heading(f"Declarations for {selected_year} · {len(members_df)} members")`.
>
> 7. **P2-1 — Demote the `Coming soon.` callout.**
>    After P1-1 lands, replace the callout with a one-line `st.caption`
>    under the leaderboard heading.
>
> 8. **P2-2 — Resolved by P1-4.**
>
> 9. **P2-3 — Pill colour legend.**
>    Add a small dot-separated legend above the first card:
>    `Pill colours: declarations · landlord · property owner · shareholder`
>    via `st.caption` with coloured `<span>`s matching the pill classes.
>
> 10. **P2-4 — Loading state on chip click.**
>     Set `st.session_state["chip_loading"] = name` immediately on
>     click, render an `st.toast("Opening profile…")`. Mostly resolved
>     by P0-1's navigation latency; defer if not visible.
>
> **Out of scope** (do NOT regress):
> - Card click → `member_profile_url(code, section="interests")` —
>   currently the only working primary navigation; exemplary.
> - Legacy `?member=` redirect via shared `member_moved_callout` —
>   verified shipping.
> - Pill colour palette itself.
> - Mobile layout.

---

## Positive findings (DO NOT REGRESS)

1. **Legacy `?member=` redirect** uses the shared
   `member_moved_callout` helper (`interests.py:698-705`) — exemplary
   cross-page contract per round-3 P0-3.
2. **Card pill semantics** — distinct colours for landlord (orange),
   property owner (green), shareholder (purple), declarations (blue).
3. **Mobile layout** — `A07_landing_mobile.png` stacks hero → typeahead
   → selectbox → year pills → callout → leaderboard cleanly.
4. **Chamber-switch cleanup** — `interests.py:656-659` clears
   `int_profile_year`, `selected_td`, `int_member_sel`, `int_member_q`
   on chamber change.
5. **Empty-state guards** — three branches handle missing parquet/CSV,
   empty DataFrame, and missing columns (`interests.py:670-691`). No
   silent grey rectangles.
6. **Cards drive navigation** — wrapped in
   `clickable_card_link(href=member_profile_url(code, section="interests"))`
   — this is the page's working flow.
7. **Provenance footer** — civic-editorial caveats on-brand.
8. **Accented characters render** — Seán, Aengus Ó Snodaigh, Micheál
   Carrigy, Seán Ó Fearghaíl all correct. Encoding contract holds.

---

## Part 3 — Earlier civic-ui-review (2026-05-03)

> Preserved verbatim from the previous code-level audit. Most of these
> are still open and target a different layer (contract compliance +
> shared helpers) from the UX findings in Parts 1-2.

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
  - NOTE 2026-05-26: round-3 already dropped emoji from this page's pills (see `interests.py:307-309` comment). M2 likely closed via that pass.

### M3 — `unsafe_allow_html=True` in shared components

- [ ] **Where:** [evidence_heading :179](../utility/ui/components.py#L179), [todo_callout :183-187](../utility/ui/components.py#L183-L187), [member_profile_header :369-374](../utility/ui/components.py#L369-L374), [sidebar_date_range :385](../utility/ui/components.py#L385)
- **Problem:** Pass B requires `st.html`, not `st.markdown(..., unsafe_allow_html=True)`. **Cross-page change.**
- **Fix:** swap each `st.markdown(..., unsafe_allow_html=True)` → `st.html(...)`.

### M4 — Bug: malformed closing tag in `todo_callout`

- [ ] **Where:** [components.py:185](../utility/ui/components.py#L185)
- **Problem:** `<\code>` should be `</code>`. Renders as literal text on every page that uses `todo_callout`.
- **Fix:** one-character correction. (Almost certainly resolved by the round-3 `todo_callout` rewrite — verify.)

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

---

Re-run the Playwright capture after any change:
```
$env:PYTHONIOENCODING = "utf-8"
python audit_screenshots/_interests_capture.py
```
Writes to `audit_screenshots/_interests/`; assumes Streamlit running on
`localhost:8501`.
