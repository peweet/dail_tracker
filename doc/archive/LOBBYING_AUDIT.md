# Lobbying page — impeccable audit (2026-05-26)

Captured via Playwright over `/rankings-lobbying` on the running Streamlit
app. 44 screenshots in [audit_screenshots/lobbying/](../audit_screenshots/lobbying/)
covering desktop / tablet / mobile, landing gateway, sidebar interaction
(name search + selectbox), gateway path clicks, Stage 2 org profile
(Ibec deep link), area Stage 2, revolving-door Stage 2a index + 2b
individual profile, org index with funding-profile + income-trend facets,
topic search (housing + immigration), legacy `?lob_pol=` redirect,
provenance, and bogus-org/bogus-DPO empty states.

This document has two parts:
1. **Audit findings** — what's wrong, why it matters, ranked by severity.
2. **The uplift prompt** — a single prompt ready to hand to a coding
   session to drive the rework.

Capture script: [audit_screenshots/_lobbying_capture.py](../audit_screenshots/_lobbying_capture.py).

---

## Part 1 — Audit findings

### Health score

| # | Dimension          | Score | Headline finding                                                                              |
|---|--------------------|-------|------------------------------------------------------------------------------------------------|
| 1 | Accessibility      | 3/4   | `nan` badges + `Data: None → None` confuse screen readers; otherwise clean heading hierarchy  |
| 2 | Performance        | 3/4   | Some pandas `value_counts` in Streamlit; expanders always render; ~30 fetch calls on landing  |
| 3 | Theming            | 3/4   | Generally clean; one inline `<style>` block; raw hex on a few lob-* classes                    |
| 4 | Responsive         | 4/4   | **Best responsive design in the app** — mobile hero + badges + glossary + filter + first gateway card all above the fold |
| 5 | Anti-patterns      | 2/4   | `st.metric` x8 (org + topic), `Data: None → None` bug, `nan` badges, hand-rolled redirect, value_counts |
| **Total** |            | **15/20** | **Good — most issues are polish; one P0 cosmetic; one P1 redirect-reliability bug.**     |

### Anti-patterns verdict

**Pass on the AI-slop test.** The page is the strongest editorial
artefact in the app — a real investigative-lookup tool, not a
dashboard. The three-path gateway (politicians / orgs / policy areas),
the dedicated revolving-door story, the topic search with explicit
"this is a free-text scan, not the official taxonomy" disclosure, the
CRO+Charity enrichment on the org index, and the breadcrumb-driven
Stage 2 flows together compose a genuinely distinctive product.

**The slop risk is in two small visible bugs** — `Data: None → None`
in the hero badge and `nan` rendered as a sector chip on org
profiles. They make a careful page look careless.

---

### Executive summary

Compared to attendance (11/20) and payments (13/20), lobbying is in
markedly better shape. Most of the [`project_audit_findings_2026_04_30`](../../../.claude/projects/c--Users-pglyn-PycharmProjects-dail-extractor/memory/project_audit_findings_2026_04_30.md)
backlog items have been quietly retired — `st.info` empty states,
`st.radio` view selector, `st.write("")` spacers, `var(--dt-primary)`
token reference, local `_section_heading` duplicate, missing
`sidebar_page_header` are all fixed. The page is mostly working.

- **1 P0 blocking issue** — hero badge renders `Data: None → None`
  on every landing because `v_lobbying_summary` returns NULL for
  `first_period` and `last_period`.
- **5 P1 major issues** — `nan` rendered as sector badge on org
  profiles + org index; hand-rolled legacy `?lob_pol=` redirect
  produces an apparently blank page; "Browse politicians" /
  "Browse policy areas" buttons navigate to whoever happens to be #1
  (not a browser); `st.metric` fintech-quad on topic + org Stage 2;
  pandas `value_counts` in topic Stage 2 violates the contract.
- **7 P2 minor issues** — 5× `use_container_width=True`, page-local
  `<style>` block, `todo_callout` leaks pipeline scaffolding,
  `name_join_key` fallback usage (deprecated), notable-chip uses
  `str.contains` fuzzy match, contract `v_lobbying_org_index` listed
  as TODO but now shipped, an unexplained orange "selected" tint on
  one card in the Ibec profile.
- **3 P3 nice-to-haves** — chamber filter on RD index has ~60 pills
  in one block; date-range filter defaults collapsed; recent-returns
  carries a developer-facing `re-run lobby_processing.py` message.

The single highest-leverage move is **populating `first_period` /
`last_period` in `v_lobbying_summary`** (P0-1) plus normalising NaN
to empty string in the org-profile sector rendering (P1-1). Both
small. Together they remove every visible "this page is broken"
signal from the page.

---

### P0 — Blocking (fix before next deploy)

**[P0-1] Hero badge reads `Data: None → None` on landing**

- **Location**: [`lobbying_2.py:529`](../utility/pages_code/lobbying_2.py#L529)
  inside `_render_landing`:
  ```python
  badges=[
      f"{total_returns:,} returns",
      f"{total_orgs:,} organisations",
      f"{total_pols:,} politicians",
      f"Data: {first_p} → {last_p}",      # ← renders None → None
  ]
  ```
  The fallback at [`lobbying_2.py:513-514`](../utility/pages_code/lobbying_2.py#L513-L514)
  uses `s.get("first_period", "—")` — but the key exists in
  `v_lobbying_summary`, with value `None`. `dict.get(key, default)`
  only returns the default if the key is missing, not if the value
  is None. So `first_p = None`, and the f-string formats `"None"`.
- **Verified in DuckDB**: `SELECT * FROM v_lobbying_summary LIMIT 1`
  returns `first_period: None`, `last_period: None`. The view is
  not populating those columns.
- **Evidence**: [`A02_landing_above_fold_desktop.png`](../audit_screenshots/lobbying/A02_landing_above_fold_desktop.png),
  [`H06_mobile_above_fold.png`](../audit_screenshots/lobbying/H06_mobile_above_fold.png) — visible in the badge row on every
  landing render, desktop and mobile.
- **Impact**: the editorial badge intended to anchor the user in time
  ("82,319 returns covering 2014 → 2025") instead reads as a literal
  "None" — the badge looks broken. On a page whose entire job is to
  surface a register of evidence, an obviously-broken time stamp is
  fatal to credibility.
- **Fix**:
  - **Upstream (preferred)**: populate `first_period` / `last_period`
    in `sql_views/lobbying_summary.sql` — `MIN(period_start_date)` /
    `MAX(period_end_date)` (or `period_start_date`) over the source
    rows.
  - **Page-side stopgap**: at
    [`lobbying_2.py:513-514`](../utility/pages_code/lobbying_2.py#L513-L514) replace `.get(key, "—")` with `or "—"`:
    ```python
    first_p = s.get("first_period") or "—"
    last_p  = s.get("last_period")  or "—"
    ```
    Then drop the "Data: None → None" badge entirely when both are
    "—". Same defensive pattern needed in `_render_org` at
    [`lobbying_2.py:1571-1573`](../utility/pages_code/lobbying_2.py#L1571-L1573) where the org hero claims `"Active {first_p} → {last_p}"` —
    Ibec's hero correctly shows `2015-09-01 → 2025-09-01` because the
    org view has the columns; but defending against NaN keeps the page
    honest if the org view ever has gaps.

---

### P1 — Major (fix this milestone)

**[P1-1] `nan` rendered as sector badge on Ibec profile + org-index #1 card**

- **Location**: org Stage 2 hero badges at [`lobbying_2.py:1555-1577`](../utility/pages_code/lobbying_2.py#L1555-L1577):
  ```python
  sector = str(org_row.get("sector", "") or "")
  ...
  badges = [b for b in [sector] if b]
  ```
  `org_row["sector"]` is `np.nan` for many rows (Ibec included).
  `str(np.nan)` is the string `"nan"`. The truthy filter
  `if b` passes "nan" because it's a non-empty string. So a badge
  literally reading `nan` gets rendered.
- **Evidence**: [`C04_org_profile_ibec.png`](../audit_screenshots/lobbying/C04_org_profile_ibec.png) — the Ibec hero shows a single tiny badge labelled `nan`.
  [`E02_org_index_above_fold.png`](../audit_screenshots/lobbying/E02_org_index_above_fold.png) — first card "Ibec / #1 / nan / 4,698 returns · 1,461 politicians" — `nan` is also rendered as the sector
  meta line on the org-index card via the `meta` argument to
  `_lob_card_html` at [`lobbying_2.py:697`](../utility/pages_code/lobbying_2.py#L697).
- **Impact**: same as P0-1 — a careful page looks careless. "nan" is
  developer scaffolding leaking into the user-facing UI.
- **Fix**: normalise NaN-to-empty everywhere the page reads from a
  pandas Series with potentially-NaN string columns. Two options:
  - **Helper**: add a `_safe_str(val) -> str` in `lobbying_2.py` that
    returns `""` for `None | nan | NaT` and `str(val).strip()` otherwise.
    Use it whenever rendering DB strings into HTML.
  - **Upstream**: cast `sector` and similar text columns to
    `COALESCE(sector, '')` in the SQL views so the page always gets
    an empty string. Lighter ongoing cost. Same finding applies to
    `meta` argument at [`lobbying_2.py:697-698`](../utility/pages_code/lobbying_2.py#L697-L698) and any other "should-be-empty-but-isn't" string column.

**[P1-2] Legacy `?lob_pol=` redirect renders an apparently blank page**

- **Location**: hand-rolled redirect block at
  [`lobbying_2.py:2142-2160`](../utility/pages_code/lobbying_2.py#L2142-L2160). After rendering an `st.html(...)` callout,
  the code calls `_clear_profile()` then `_clear_lob_qp()`. The
  query-params mutation triggers Streamlit's URL-state rerun; on the
  rerun the page falls to the `else` branch and renders the landing
  page. The intended "Politician profiles have moved" callout is
  visible for only a flash.
- **Evidence**: [`C07_legacy_pol_redirect.png`](../audit_screenshots/lobbying/C07_legacy_pol_redirect.png) — captured 3.5 s after navigating to `?lob_pol=Mary%20Lou%20McDonald`,
  the page renders only the top app banner. No callout, no landing
  content. The rerun is mid-flight when the screenshot is taken;
  a real user might see either the callout flashing past or a brief
  blank state followed by landing.
- **Impact**: anyone visiting a legacy bookmark or external link to
  a politician profile lands on a confusingly empty page (or a
  flashed callout that disappears before they can read it). The
  user doesn't know they were redirected.
- **Root cause**: hand-rolled duplicate of `member_moved_callout`
  from [`components.py:315-383`](../utility/ui/components.py#L315-L383) — the shared helper
  uses `resolve_member_code` (not the deprecated `name_join_key`)
  and calls `st.stop()` after rendering the callout, preventing the
  state-mutation-then-rerun cycle.
- **Fix**: replace the entire block with
  ```python
  member_moved_callout(
      sel_pol,
      section="lobbying",
      section_label="Per-TD lobbying",
      legacy_param="lob_pol",
      state_keys=("lob_selected_politician",),
  )
  ```
  This also picks up the cross-page `Per-td` → `Per-TD` casing fix
  whenever it lands (see attendance P1-4 and payments P1-3).

**[P1-3] "Browse politicians →" / "Browse policy areas →" buttons
navigate to a single record, not a browser**

- **Location**: [`lobbying_2.py:581-585`](../utility/pages_code/lobbying_2.py#L581-L585):
  ```python
  if st.button("Browse politicians →", key="lob_gw_pol", width="stretch"):
      idx = fetch_politician_index()
      if not idx.empty:
          _nav("pol", idx.iloc[0]["member_name"])
  ```
  Same pattern at [`lobbying_2.py:611-615`](../utility/pages_code/lobbying_2.py#L611-L615) for "Browse policy areas →".
- **Impact**: a user clicks "Browse politicians" expecting a
  navigable list (or a filtered leaderboard). Instead they're
  redirected to whichever politician happens to occupy rank #1 in
  `v_lobbying_index` — currently Paschal Donohoe. The button reads
  as a browse affordance but acts as "open the top result blindly".
  Worse, since politician profiles redirect to /member-overview
  (P1-2), the click triggers a *second* redirect.
- The contract calls for `path_gateway` with three real browse paths
  ([`lobbying.yaml:331`](../dail_tracker_bold_ui_contract_pack_v5/utility/page_contracts/lobbying.yaml#L331)). "Browse organisations →" works
  correctly — it opens the org index. The politicians + policy-areas
  paths do not have an equivalent index.
- **Fix**: build a politician index view + policy-area index view
  analogous to `_render_org_index`. Both should be ranked card lists
  with their own facets (chamber filter for politicians; return-count
  / org-count for areas). Until those exist, change the button label
  to "Top politicians →" / "Top policy areas →" so the affordance
  matches the behaviour.

**[P1-4] `st.metric` fintech-quad on topic + org Stage 2**

- **Location**:
  - Topic Stage 2: [`lobbying_2.py:1267-1271`](../utility/pages_code/lobbying_2.py#L1267-L1271) — 4 metrics: Returns matching, Distinct organisations, Policy areas spanned, Period.
  - Org Stage 2: [`lobbying_2.py:1584-1593`](../utility/pages_code/lobbying_2.py#L1584-L1593) — 3 or 4 metrics: Returns filed, Politicians targeted, Periods filed, Active span (days).
  - DPO Stage 2b individual: same pattern (per [`lobbying_2.py:1003-1204`](../utility/pages_code/lobbying_2.py#L1003-L1204)).
- **Evidence**: [`C04_org_profile_ibec.png`](../audit_screenshots/lobbying/C04_org_profile_ibec.png) — four bare-Streamlit `st.metric` blocks "Returns filed 4,698 / Politicians
  targeted 1,461 / Periods filed 31 / Active span (days) 3653" sit
  directly under the editorial hero. Same fintech-dashboard pattern
  PRODUCT.md's anti-references explicitly call out.
- **Impact**: visual register break — hero is editorial newspaper,
  metrics row is generic Streamlit. The rest of the page is
  civic-editorial; this block isn't.
- **Fix**: lift the `pay-totals-strip` pattern from payments
  ([`payments.py:275-287`](../utility/pages_code/payments.py#L275-L287)) into a `components.py` helper (e.g.
  `totals_strip(items: list[tuple[value, label]])`). Use on every
  Stage 2 view. Same fix lifts payments P1-2 — file it once.

**[P1-5] Topic Stage 2 violates the contract — pandas `value_counts` in Streamlit**

- **Location**: [`lobbying_2.py:1285-1292`](../utility/pages_code/lobbying_2.py#L1285-L1292):
  ```python
  area_counts = (
      detail["public_policy_area"]
      .fillna("(unspecified)")
      .value_counts()
      .head(10)
      .rename_axis("Policy area")
      .reset_index(name="Returns")
  )
  ```
  Contract [`lobbying.yaml:289-296`](../dail_tracker_bold_ui_contract_pack_v5/utility/page_contracts/lobbying.yaml#L289-L296) `forbidden_in_streamlit` lists `pandas_groupby` and
  `pandas_pivot`. `value_counts()` is a one-column GROUP BY by
  identity — exactly the pattern the contract forbids. The comment
  at [`lobbying_2.py:1278-1279`](../utility/pages_code/lobbying_2.py#L1278-L1279) acknowledges this ("display-side breakdown via pandas
  value_counts — UI aggregation only, no business logic") — but the
  contract doesn't carve out a "UI aggregation" exception.
- **Impact**: architectural rule erosion. Today it's `value_counts`
  for a display chart; next month someone adds `groupby` for "just a
  display table"; eventually the firewall is gone.
- **Fix**: expose `v_lobbying_topic_area_breakdown(keywords, area)` —
  registered view with the breakdown precomputed. Stream-it page
  does `SELECT public_policy_area, return_count FROM
  v_lobbying_topic_area_breakdown WHERE keyword_set = ? ORDER BY
  return_count DESC LIMIT 10` and renders the ProgressColumn dataframe
  unchanged. Same caching benefit (DuckDB) without the contract erosion.

---

### P2 — Minor

**[P2-1] `use_container_width=True` x5 deprecations**

- [`lobbying_2.py:1470, 1508, 1641, 1731, 1936`](../utility/pages_code/lobbying_2.py). Replace with `width="stretch"`. Same family
  as attendance P1-5 and payments P1-4.

**[P2-2] Page-local `<style>` block on org Stage 2**

- [`lobbying_2.py:1530-1535`](../utility/pages_code/lobbying_2.py#L1530-L1535):
  ```python
  st.html(
      "<style>"
      ".st-key-lob_org_switcher .stSelectbox > div > div,"
      '.st-key-lob_org_switcher [data-baseweb="select"] > div'
      "{background:#ffffff !important;}"
      "</style>"
  )
  ```
  PRODUCT.md "Avoid" list says "page-local CSS systems". Migrate to
  `shared_css.py` as `.lob-org-switcher` or generic
  `[data-testid="stSelectbox"]` rule.

**[P2-3] `todo_callout` leaks `re-run lobby_processing.py` to citizens**

- [`lobbying_2.py:652-654, 690-692, 727-729, 775-777, 786-789`](../utility/pages_code/lobbying_2.py) — every empty-state in the landing flow renders a `todo_callout`
  with text like `"v_lobbying_recent_returns — re-run lobby_processing.py
  to regenerate data/silver/lobbying/parquet/returns_master.parquet"`.
- The `todo_callout` helper strips the `TODO_PIPELINE_VIEW_REQUIRED:`
  tag and the developer prefix per its docstring, but its sentence
  parser at [`components.py:289`](../utility/ui/components.py#L289) splits on em-dash — and the trailer it surfaces ("re-run
  lobby_processing.py to regenerate ...") IS still developer
  scaffolding. The strip helper kicks in on the headline; the body
  trailer is whatever's after the first em-dash.
- **Fix**: rewrite each `todo_callout` body to put the citizen
  sentence after the em-dash and the developer scaffolding before
  (which gets stripped). Pattern:
  `todo_callout("v_lobbying_recent_returns — Recent lobbying activity
  is being prepared; check back soon.")` — now the citizen sees only
  "Recent lobbying activity is being prepared; check back soon."

**[P2-4] `name_join_key` still used as fallback**

- [`lobbying_2.py:1614`](../utility/pages_code/lobbying_2.py#L1614)
  (`_render_org` cards) and [`lobbying_2.py:674`](../utility/pages_code/lobbying_2.py#L674) (landing politicians leaderboard cards) — both use
  `name_join_key(name)` when `unique_member_code` is missing. The
  helper is deprecated per the round-3 audit memory (replaced
  network-wide by `resolve_member_code`).
- The legacy redirect block at [`lobbying_2.py:2147`](../utility/pages_code/lobbying_2.py#L2147) also uses it — folded into P1-2's fix.
- **Fix**: wrap with `resolve_member_code(name) or name_join_key(name)`
  as a true fallback, and add a comment noting that
  `name_join_key` is the legacy escape hatch until every
  `v_lobbying_*` view carries `unique_member_code`.

**[P2-5] Notable-chip handler uses pandas `str.contains` fuzzy match**

- [`lobbying_2.py:484`](../utility/pages_code/lobbying_2.py#L484):
  ```python
  m = idx[idx["position"].str.contains(chip, case=False, na=False)]
  ```
  Fuzzy-matching a chip label ("Minister for Finance") against the
  `position` column. Per contract `forbidden_in_streamlit` and the
  `no_fuzzy_matching` spirit — soft violation.
- Mild in practice (the chip set is small and fixed) but
  architecturally wrong. Each chip should resolve to a
  pre-computed `notable_target_kind` column on `v_lobbying_index`,
  or a registered view `v_lobbying_notable_targets` keyed by
  `target_label`.

**[P2-6] Contract drift — `v_lobbying_org_index` was TODO, is shipped**

- [`lobbying.yaml:89-135`](../dail_tracker_bold_ui_contract_pack_v5/utility/page_contracts/lobbying.yaml#L89-L135) lists `v_lobbying_org_index` with `status:
  TODO_PIPELINE_VIEW_REQUIRED` but DuckDB shows it exists (2,514
  rows). The org index page renders fine — including the CRO and
  Charity enrichment (`E02_org_index_above_fold.png` shows
  "Funding profile" and "Income trend" facets — these are the
  `enrichment_plan_v_next` columns from the contract). The
  promotion gate is already met. Update the contract to `status:
  required` and the `enrichment_plan_v_next` to reflect what
  shipped.

**[P2-7] Unexplained orange "selected" tint on a card in Ibec profile**

- [`C04_org_profile_ibec.png`](../audit_screenshots/lobbying/C04_org_profile_ibec.png) shows Declan Hughes's card (#3) in an orange tint while #1 #2 #4 are
  white. Not a deliberate "most recent" or "current activity"
  indicator — likely a stuck CSS hover state or an `att-hall-card-bad`
  class leaking from elsewhere. Investigate via DevTools; either
  remove the leaking class or document the intent.

---

### P3 — Polish

**[P3-1] Chamber filter on RD index is a flat wall of ~60 pills**

- [`D01_rd_index_full.png`](../audit_screenshots/lobbying/D01_rd_index_full.png) shows the "Filter by former chamber" row containing every former
  chamber as an unranked pill — local councils, government
  departments, the Dáil, the Seanad, regional bodies — all in one
  flat list, in alphabetical order. Six visible rows of pills, more
  on scroll.
- **Suggest**: group as `All / Dáil / Seanad / Departments / Local
  Councils / Other state bodies`, with a "More" toggle to reveal
  the long list. Or move to a typeahead selectbox if the user
  rarely picks more than one.

**[P3-2] Date-range filter defaults collapsed**

- [`lobbying_2.py:543`](../utility/pages_code/lobbying_2.py#L543) — `with st.expander("Filter by date range", expanded=False):`.
  Most users will scan the whole register without realising they
  can narrow it. Contract calls it the "global temporal control"
  ([`lobbying.yaml:330`](../dail_tracker_bold_ui_contract_pack_v5/utility/page_contracts/lobbying.yaml#L330)) — promote to a default-visible (but compact) pill row
  showing the active range, click-to-expand.

**[P3-3] Topics "Period" metric format is `2025-09 – 2025-10`**

- Topic Stage 2 [`lobbying_2.py:1271`](../utility/pages_code/lobbying_2.py#L1271): `c4.metric("Period",
  f"{first_p[:7]} – {last_p[:7]}")` renders inside an `st.metric`
  block. The `[:7]` truncates ISO date to YYYY-MM — clean — but the
  ndash spacing and the metric framing are awkward. Convert to a
  caption or a single-row pill within the totals strip after P1-4
  is fixed.

---

### Patterns and systemic issues

1. **Lobbying is the strongest page in the app.** Best mobile
   rendering, real editorial gateway, dedicated revolving-door story
   with civic-voice copy, CRO+Charity enrichment exposed on the org
   index, topic search with explicit caveat about taxonomic mismatch.
   When working on other pages, copy from here.

2. **The `nan` / `None` rendering bugs are a single class of
   problem.** Pandas + DuckDB return NaN/None for missing strings;
   `str(np.nan) == "nan"` and `f"{None}" == "None"`. The page
   needs one defensive `_safe_str` helper or upstream `COALESCE`
   discipline — once. Then every visible "this page is broken"
   moment goes away.

3. **The `Per-td` cross-page bug is now an oh-no-not-again signature.**
   Three audits in a row have flagged the same lowercase rendering
   from `member_moved_callout` (attendance P1-4, payments P1-3) +
   the hand-rolled lobbying duplicate (P1-2). All four use sites of
   the cross-page callout should converge on the shared helper, with
   the casing fix landed once.

4. **`st.metric` is the consistent fintech-residue across pages.**
   Attendance uses it on the profile (removed by Phase 6). Payments
   uses it on Rankings (P1-2). Lobbying uses it on three Stage 2
   views (P1-4). One `totals_strip` helper in `components.py` plus
   a one-PR sweep retires the pattern everywhere.

5. **Pandas aggregations in Streamlit are the next firewall battle.**
   Attendance had a single `bool[bool]` filter (acceptable). Payments
   has `.sum()` / `.n_unique()` (P0). Lobbying has `.value_counts()`
   (P1-5) + `str.contains` (P2-5). Each individually feels small;
   collectively they erode the rule. Each one should turn into a
   registered SQL view as a one-off ticket.

---

### Positive findings (keep these)

- **Mobile rendering is the best in the app**
  ([`H06_mobile_above_fold.png`](../audit_screenshots/lobbying/H06_mobile_above_fold.png)). The whole hero + badges + glossary + filter expander + first
  gateway card all visible at 390×844.
- **Revolving-door Stage 2 index** ([`D01_rd_index_full.png`](../audit_screenshots/lobbying/D01_rd_index_full.png)) — breadcrumb +
  strong editorial title ("Former Designated Public Officials on
  lobbying returns") + civic-voice dek ("treat as indicative, not a
  legal finding") + "MOST-ACTIVE FILERS" pill callout with top 3
  named. Sets the bar for accountability-tracker register UI.
- **Org index** ([`E02_org_index_above_fold.png`](../audit_screenshots/lobbying/E02_org_index_above_fold.png)) — breadcrumb + hero + state-funded toggle +
  funding profile facets (State-funded / Mostly donations / Mostly
  trading / Mixed) + income trend facets (Growing / Flat / Shrinking)
  + pagination 1-101 + per-page 25/50/100. Industrial-strength facet
  UI, CRO + Charity enrichment exposed cleanly.
- **Topic search caveat** ([`F02_topic_housing_above_fold.png`](../audit_screenshots/lobbying/F02_topic_housing_above_fold.png)) — "TOPIC SEARCH · NOT A REGISTERED POLICY AREA"
  kicker, "How this works" explainer panel, keyword pills, "False
  positives are possible — open a return's source link to verify".
  Exactly the civic-accountability honesty the project asks for.
- **Sidebar combines politician + org search in one selectbox**
  ([`B01_sidebar_default.png`](../audit_screenshots/lobbying/B01_sidebar_default.png)) — clean unified search affordance, `[Org]` prefix
  disambiguates results.
- **Most of [`project_audit_findings_2026_04_30`](../../../.claude/projects/c--Users-pglyn-PycharmProjects-dail-extractor/memory/project_audit_findings_2026_04_30.md) has been retired**: no `st.info`, no `st.radio`, no
  spacer `st.write("")`, no `var(--dt-primary)` token, no local
  `_section_heading`, no missing `sidebar_page_header`. Quiet work
  that visibly improved the page.

---

## Part 2 — The uplift prompt

The prompt below is ready to hand to a coding session. It assumes the
person taking it has access to the repo and to the impeccable skill
context (`PRODUCT.md`, project memory). Drop it into a new conversation
or paste it after `/impeccable craft lobbying page` to seed
shape-then-build.

> ### Lobbying page — comprehensive uplift
>
> Polish the strongest page in the app. The lobbying page already
> follows the contract better than attendance or payments — most of
> the [`project_audit_findings_2026_04_30`](../../../.claude/projects/c--Users-pglyn-PycharmProjects-dail-extractor/memory/project_audit_findings_2026_04_30.md) backlog has been quietly retired (no `st.info`,
> `st.radio`, spacer `st.write("")`, `var(--dt-primary)`, local
> `_section_heading`, or missing `sidebar_page_header`). The remaining
> issues are small surface bugs and one architectural cleanup. Hold to
> `PRODUCT.md` (Direct · Civic · Accountable) and the page contract's
> retrieval SQL policy. Stay inside Streamlit constraints; honour the
> project's logic-firewall split.
>
> Audit evidence — see [doc/LOBBYING_AUDIT.md](LOBBYING_AUDIT.md) and
> the 44 supporting screenshots in
> [audit_screenshots/lobbying/](../audit_screenshots/lobbying/).
>
> #### Goals (in priority order)
>
> 1. **Stop the page from looking broken — fix the `nan` / `None`
>    badges.** Two cosmetic bugs read as developer scaffolding:
>    - `Data: None → None` in the hero badge on every landing
>      ([`lobbying_2.py:529`](../utility/pages_code/lobbying_2.py#L529)) because `v_lobbying_summary` returns NULL for
>      `first_period` and `last_period`. Fix upstream: populate the
>      columns in `sql_views/lobbying_summary.sql` from
>      `MIN(period_start_date)` / `MAX(period_start_date)` on the
>      source. Page-side defence: use `s.get("first_period") or "—"`
>      (`.get(key, default)` only fires on missing keys, not None
>      values). Drop the badge entirely when both ends are "—".
>    - `nan` rendered as a sector badge on `_render_org`
>      ([`lobbying_2.py:1555-1577`](../utility/pages_code/lobbying_2.py#L1555-L1577)) and as the meta line on the org-index card
>      ([`lobbying_2.py:697`](../utility/pages_code/lobbying_2.py#L697)). Add a `_safe_str(val) -> str` helper that returns
>      `""` for `None`/`nan`/`NaT`, or `COALESCE(sector, '')` upstream.
>      Apply everywhere a DB string column flows into rendered HTML.
>
> 2. **Replace the hand-rolled legacy `?lob_pol=` redirect with the
>    shared `member_moved_callout` helper.** The current block at
>    [`lobbying_2.py:2142-2160`](../utility/pages_code/lobbying_2.py#L2142-L2160) renders an `st.html` callout, then calls `_clear_profile()`
>    and `_clear_lob_qp()` which mutate query params and trigger
>    a rerun. The callout flashes for milliseconds then disappears.
>    Replace with:
>    ```python
>    member_moved_callout(
>        sel_pol,
>        section="lobbying",
>        section_label="Per-TD lobbying",
>        legacy_param="lob_pol",
>        state_keys=("lob_selected_politician",),
>    )
>    ```
>    The helper renders + calls `st.stop()` so the callout is stable.
>    Also picks up the cross-page `Per-td` → `Per-TD` casing fix
>    (see attendance P1-4) whenever that lands in `components.py`.
>
> 3. **Make "Browse politicians →" and "Browse policy areas →"
>    actually browse.** Currently both buttons
>    ([`lobbying_2.py:581-585, 611-615`](../utility/pages_code/lobbying_2.py#L581-L615)) navigate to whoever happens to occupy rank #1 — the
>    button promises a browser but delivers a single profile (which
>    in the politicians case immediately redirects to /member-overview
>    via P1-2). Two options:
>    - **Preferred**: build `_render_politician_index` and
>      `_render_area_index` analogous to `_render_org_index`. Both
>      ranked card lists with facets (chamber filter / return count).
>      Trigger via `?lob_polindex=1` and `?lob_areaindex=1` query
>      params, with state keys `lob_view_politician_index` and
>      `lob_view_area_index` mirroring the existing `lob_view_org_index`
>      / `lob_view_revolving_door`.
>    - **Stopgap**: relabel the buttons to "Top politicians →" / "Top
>      policy areas →" so the affordance matches the behaviour.
>
> 4. **Replace `st.metric` blocks with the `totals_strip` helper.**
>    Three Stage 2 views (topic, org, DPO individual) each emit 3-4
>    `st.metric` calls. Lift the `pay-totals-strip` HTML pattern from
>    [`payments.py:275-287`](../utility/pages_code/payments.py#L275-L287) into a shared `totals_strip(items)` helper in
>    `components.py`. Use everywhere. Single sweep fixes payments
>    P1-2 in the same PR.
>
> 5. **Build `v_lobbying_topic_area_breakdown` and drop the
>    `value_counts` call.** Contract `forbidden_in_streamlit` lists
>    `pandas_groupby`; `value_counts()` is GROUP BY in disguise. The
>    page comment at [`lobbying_2.py:1278-1279`](../utility/pages_code/lobbying_2.py#L1278-L1279) acknowledges this. Move the breakdown to a
>    registered view, page does retrieval-only SELECT.
>
> 6. **Audit the `todo_callout` body strings** —
>    [`lobbying_2.py:652-654, 690-692, 727-729, 775-777, 786-789`](../utility/pages_code/lobbying_2.py) — strip "re-run lobby_processing.py" trailer; the citizen
>    sentence should be civic-voiced ("Recent lobbying activity is
>    being prepared; check back soon"), the developer scaffolding
>    should go before the em-dash so `todo_callout`'s sentence parser
>    discards it.
>
> #### Polish (P2 — fold into the same PR if time permits)
>
> - Replace `use_container_width=True` with `width="stretch"` at
>   [`lobbying_2.py:1470, 1508, 1641, 1731, 1936`](../utility/pages_code/lobbying_2.py).
> - Move the page-local `<style>` block at
>   [`lobbying_2.py:1530-1535`](../utility/pages_code/lobbying_2.py#L1530-L1535) into `shared_css.py` as a `.lob-org-switcher` rule.
> - Replace `name_join_key(name)` fallbacks at
>   [`lobbying_2.py:674, 1614`](../utility/pages_code/lobbying_2.py) with `resolve_member_code(name) or name_join_key(name)`.
> - Update [`lobbying.yaml:89-135`](../dail_tracker_bold_ui_contract_pack_v5/utility/page_contracts/lobbying.yaml#L89-L135) — `v_lobbying_org_index` is shipped, change status from
>   `TODO_PIPELINE_VIEW_REQUIRED` to `required` and reconcile
>   `enrichment_plan_v_next` with what's now live.
> - Investigate the orange tint on Declan Hughes card #3 in the Ibec
>   profile ([`C04_org_profile_ibec.png`](../audit_screenshots/lobbying/C04_org_profile_ibec.png)) — likely a leaking `att-hall-card-bad` class or stuck hover
>   state.
> - Replace the `str.contains` notable-chip handler at
>   [`lobbying_2.py:484`](../utility/pages_code/lobbying_2.py#L484) with either a registered `v_lobbying_notable_targets`
>   view or pre-computed `notable_target_kind` flag column.
>
> #### Polish (P3 — only if everything else lands)
>
> - Group the RD chamber filter pills (currently flat ~60-pill wall
>   in `D01`) into All / Dáil / Seanad / Departments / Local
>   Councils / Other.
> - Promote the date-range filter from `expander(expanded=False)` to
>   a compact default-visible pill row at the top of the landing page.
>
> #### Non-goals (don't do these in this PR)
>
> - No new Python/Polars enrichment in production paths; new logic
>   goes to `pipeline_sandbox/` per `project_pipeline_sandbox_rule.md`.
>   The new views in goal 3 are SQL-only.
> - Don't touch `pipeline.py` / `enrich.py` / `normalise_join_key.py`
>   (sandbox rule).
> - No CSS-architecture split or typography-scale collapse —
>   deferred design debt.
> - Don't fold `lobbying_3.py` (the experimental PoC) into the main
>   page — it has its own contract and lifecycle.
>
> #### Acceptance
>
> Re-run [audit_screenshots/_lobbying_capture.py](../audit_screenshots/_lobbying_capture.py) after the rework. New screenshots should show:
> - Hero badge displays a real date range (not `Data: None → None`)
>   on the landing page (P0-1 resolved).
> - No `nan` text anywhere in the rendered HTML (P1-1 resolved).
> - Visiting `?lob_pol=Mary%20Lou%20McDonald` renders a stable
>   "Member profiles have moved" callout — no blank flash (P1-2).
> - "Browse politicians →" and "Browse policy areas →" either open
>   index views or are relabelled to match behaviour (P1-3).
> - No `st.metric` calls on topic, org, or DPO Stage 2 views (P1-4).
> - No `value_counts()` / `groupby` / `merge` / `pivot` calls in
>   `lobbying_2.py` (P1-5).
> - Zero deprecation warnings in the Streamlit console.
> - All P2 polish issues from the checklist resolved.
>
> Re-run `/impeccable audit` on the lobbying page after the rework
> and target a health score of 18+/20.

---

## Appendix — Screenshot index

Phase A: `A01–A07` — landing on desktop (full + scrolled positions)
Phase B: `B01–B03` — sidebar default / search "Ibec" / search "McDonald"
Phase C: `C01–C07` — gateway path clicks (politicians/orgs/policy) → Ibec org profile (full + above fold) → area Health → legacy `?lob_pol=` redirect (rendered blank, see P1-2)
Phase D: `D01–D07` — revolving door Stage 2a index (full + above fold + cards), DPO Stage 2b individual (full + above fold + firms/clients + returns)
Phase E: `E01–E03` — org index (full + above fold + cards)
Phase F: `F01–F04` — topic search (housing full + above fold + results; immigration above fold)
Phase G: `G02–G03` — bogus org / bogus DPO empty states (G01 provenance shot skipped due to scroll-not-finding the expander)
Phase H: `H01–H11` — tablet + mobile responsive. **Mobile hero + badges + glossary + filter expander + first gateway card all above the fold** — best in the app.
