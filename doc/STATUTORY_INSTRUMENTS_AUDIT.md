# Statutory Instruments page — impeccable audit (2026-05-24)

> **Status update (2026-05-26):** P0 + most P1/P2 items below shipped.
> See `[[project-si-audit-2026-05-24]]` memory for the rework diary;
> the original audit findings below are kept for reference. Items
> still open: **P1-2** (mobile hero height), **P1-5 full** (dedicated
> EU-scrutiny sub-view), **P2-9 / P3-2 / P3-5** (deferred). Capture
> script + verifier live at `audit_screenshots/_si_capture.py` and
> `audit_screenshots/_si_verify_titles.py`.

Captured via Playwright over `/rankings-statutory-instruments` on the running
Streamlit app. 58 screenshots covering desktop, tablet, mobile, every facet
tab, every filter combination, search states, detail variants (default /
EU-minister / Health-dept / Substantive-or-base / deep-link cold-load),
pagination, empty states, focus traversal, and the EU-scrutiny callout
interaction.

This document has two parts:
1. **Audit findings** — what's wrong, why it matters, ranked by severity.
2. **The uplift prompt** — a single prompt you can hand to a coding session
   to drive the rework.

---

## Part 1 — Audit findings

### Health score

| # | Dimension          | Score | Headline finding                                                                              |
|---|--------------------|-------|------------------------------------------------------------------------------------------------|
| 1 | Accessibility      | 2/4   | Card "View detail" buttons low-contrast tertiary; ALL-CAPS titles fail readability             |
| 2 | Performance        | 3/4   | Two CSS injections per render; 10× tertiary widgets per page; facet counts recomputed eagerly  |
| 3 | Theming            | 3/4   | Hard-coded hex in `_inject_si_css` despite project tokens (`var(--surface)`, signal-good/bad)  |
| 4 | Responsive         | 2/4   | Mobile above-fold is hero + callout only — zero data visible until heavy scroll                |
| 5 | Anti-patterns      | 3/4   | No AI tells; respects civic-editorial brand. Card grid is purposeful, not generic              |
| **Total** |            | **13/20** | **Acceptable — significant work needed**                                                |

### Anti-patterns verdict

**Pass** on the AI-slop test. The page is recognisably civic-editorial:
serif titles, ink-on-paper surface, accent pills, no gradient text, no
glassmorphism, no hero-metric template. The two intentional overrides
documented in PRODUCT.md (side-stripe on cards, `#ffffff` over warm beige)
are applied consistently.

**The slop risk is in the data, not the design.** Card titles that are
walls of preamble text and ALL-CAPS make the page *look* AI-generated
because no human reviewer would ship cards like that — see findings P0-1
and P0-2 below.

---

### Executive summary

- **2 P0 blocking issues** — both data-quality bugs that make cards
  illegible for ~10-20% of rows.
- **6 P1 major issues** — cross-cutting UX problems with the card list,
  KPI strip, mobile layout, filter UX, empty/error states, and the
  EU-scrutiny information architecture.
- **9 P2 minor issues** — polish gaps in pill formatting, casing
  normalisation, copy, and dead/redundant code.
- **5 P3 nice-to-haves** — focus rings, subtle a11y improvements, sidebar
  cleanup.

The single highest-leverage move is **normalising SI titles upstream**
(silver/gold layer, not in Streamlit). Doing that one thing makes both
P0 issues disappear AND lifts the perceived quality of the page more
than any pure UI work.

---

### P0 — Blocking (fix before next deploy)

**[P0-1] SI titles contain entire instrument preamble text**

- **Location**: card list in `_render_si_index` /
  `_render_si_card`. Source field: `si_title` from
  `data/gold/parquet/statutory_instruments.parquet`.
- **Evidence**: screenshot `F01_pagination_footer.png` shows cards
  2026-115 / 2026-116 / 2026-114 with "titles" running ~80 words —
  *"STATISTICS (CONTINUING VOCATIONAL TRAINING SURVEY) ORDER 2026
  The Taoiseach, Micheál Martin T.D, in exercise of the powers
  conferred on him by section 25(1) of the Statistics Act 1993 (No. 21
  of 1993) and, other than in relation to subparagraphs (l) to (p) of
  paragraph 1 and paragraph 2 of the Schedule..."*
- **Impact**: cards become unscannable walls of text. A page meant to
  help citizens browse 5,891 instruments becomes hostile. Search-by-title
  also catches body words, polluting results.
- **Root cause**: upstream PDF extraction is concatenating the title
  with the opening citation. This is a `si_entity_enrichment.py` bug
  (or earlier — in the Iris-Oifigiúil PDF parser), not a UI bug.
- **Fix**: detect the citation cutoff and truncate. The pattern is
  consistent — after the year (e.g. "ORDER 2026") the next paragraph
  begins with a role + name ("The Taoiseach, …" or "The Minister …").
  Trim at the first occurrence of `r'(ORDER|REGULATIONS|RULES|SCHEME)\s+\d{4}'`
  followed by whitespace + capital letter. Hold the original in a
  separate `si_preamble` column if needed.

**[P0-2] SI titles render in ALL CAPS for some upstream rows**

- **Location**: same files. Visible on `G01_detail_default_full.png`
  ("CRIMINAL JUSTICE (FORENSIC EVIDENCE AND DNA DATABASE SYSTEM) ACT
  2014 (SECTION 110) (DESIGNATION OF THE UNITED STATES OF AMERICA)
  ORDER 2026") and most card titles in F01.
- **Impact**: ALL-CAPS body type is the single biggest readability
  killer in editorial design. The civic-newspaper register is wrecked.
  WCAG flags this for low-vision users too.
- **Fix**: normalise titles upstream. Run a casing pass —
  if title is ≥80% uppercase, apply title-case with a small stop-list
  (of, the, and, in, on, for, to, a, an), then re-uppercase content
  in parentheses that's an acronym (EU, US, IR, etc.). Validate by
  spot-checking 50 rows after.

---

### P1 — Major (fix this milestone)

**[P1-1] "View detail" button is visually detached from the card**

- **Location**: `_render_si_index`, the `st.html(card)` then `st.button`
  loop. Streamlit can't put a button inside an HTML block, so each card
  has an awkward floating button below it.
- **Evidence**: `F01_pagination_footer.png` — each card has a ~40px
  vertical gap to its button, breaking the click-affordance contract.
- **Impact**: the page reads as a list of cards with stray buttons,
  not a list of clickable rows. Vertical space waste is huge over 10
  cards / page.
- **Fix options**:
  - **A (preferred)**: wrap each card in an `<a href="?si={id}">`. Use
    `st.query_params` listening at the top of the page to set selected
    SI when the URL changes. Card becomes the click target; no
    separate button. This matches the cross-page contract used on the
    member-overview page.
  - **B**: use the dt-nav-anchor pattern from `feedback_css_card_pattern`
    (compact card + adjacent → button, fit-content + `:has()` to collapse
    the stHorizontalBlock row). Two-column layout with right-aligned
    chevron button.
  - C (least invasive): drop the button entirely, make the card title a
    link via `<a href="?si={id}">{title}</a>`. Lose the explicit CTA
    but gain scannability.

**[P1-2] Mobile above-the-fold has no data**

- **Evidence**: `I05_mobile_landing_full.png`, `I07_mobile_callout.png`
  — entire 390×844 viewport is hero + EU callout; not even the year
  pills are visible.
- **Impact**: a citizen who lands here on phone sees no instruments,
  no facets, no immediate value. The hero + dek + callout combined
  consume more than one full mobile viewport on iPhone 14.
- **Fix**: tighten the hero on mobile (shorter dek, lose the kicker on
  ≤640px), collapse the EU callout into a single-line linkout above
  the search, or move the search bar above the callout so the page
  is *immediately* useful. The callout itself can become a sticky
  ribbon at the top of the card list.

**[P1-3] KPI strip carries the same value twice in adjacent cells**

- **Location**: `_render_kpi_strip`, cells "Top policy domain" and
  "Most active department". Evidence: `D03_tab_operation_pills.png`
  shows both leading with "Finance" / "Finance banking tax".
- **Impact**: the eye lands on what looks like a typo (two Finances).
  In editorial design, repetition without intent reads as a mistake.
- **Fix**: either (a) deduplicate — when domain top == dept top
  (almost always true because both are derived from upstream
  classification), collapse to one cell showing "Finance / Finance
  banking tax · 1,132 SIs · 735 from Dept of Finance". Or (b) replace
  one cell with a different metric: pre-2014 vs post-2014 share, or
  median SIs per month.

**[P1-4] Active-filter chips look interactive but aren't**

- **Location**: `_render_facets`, the `.si-active-bar` block. Evidence:
  `E02_multi_filter_bar.png` shows chips "EU-derived", "regulations",
  "Finance" with no affordance to remove them — the user has to find
  the facet tab and unclick the pill.
- **Impact**: standard pattern violation. Every other faceted search
  on the web makes chips clickable to remove their filter.
- **Fix**: turn each chip into a button that calls `_clear_<facet>()`
  on click. Two options:
  - Real `st.button` styled as a chip (heavier but accessible).
  - HTML chip with an `<a href="?clear=<key>">` that listens via
    `st.query_params`. Lighter but needs a clear-handler at page top.

**[P1-5] EU scrutiny information is split across three places**

- **Location**: top callout, the "⚠ EU scrutiny" facet tab, and the
  filtered card list. Evidence: `B01_callout_visible.png`,
  `D10_tab_eu_scrutiny.png`, `B03_callout_filter_applied_cards.png`.
- **Impact**: the user reads roughly the same text twice (callout body
  + tab body), then sees a third UI for the same data (the cards).
  This is the page's accountability story — it should land once,
  hard.
- **Fix**: consolidate. Make the callout the primary surface (it
  already exists, is editorially strong, and has the action button).
  Replace the "⚠ EU scrutiny" tab with a dedicated `/eu-scrutiny`
  sub-page (or even a query-param view) that the callout's "Show
  these N SIs" button leads to — same content as the tab today, but
  in its own breathing room. The tab is currently the weakest piece
  of UI on the page (generic `st.metric` cards, no editorial voice).

**[P1-6] Not-found state is bare `st.warning`**

- **Location**: `statutory_instruments_page`, the `if match.empty:`
  branch. Evidence: `H02_detail_not_found.png` — a beige Streamlit
  warning box with raw "SI 'BOGUS_NONEXISTENT' not found." and a
  back button.
- **Impact**: the rest of the page is editorially crafted; this state
  reads as half-finished. Old bookmarks and typed URLs land here.
- **Fix**: use the `empty_state` component the project already has,
  with civic-voice copy: *"We couldn't find SI ‘BOGUS_NONEXISTENT’
  in the index. The corpus covers 2016 onwards — older instruments
  aren't yet in the dataset. Try the search above, or browse the
  index by year and department."* Add a search box inline.

---

### P2 — Minor

**[P2-1] Operation / policy pill labels mix casing**

- Evidence: `D03_tab_operation_pills.png` — "amendment · 2,133",
  "designation · 533", "commencement · 519", "revocation · 124",
  "establishment · 121", "licensing · 44", "fisheries · 75" all
  lowercase; sit next to "Substantive or base instrument", "Fees
  levies charges", "Statistics collection" in sentence case.
- **Cause**: `_pretty_token` only converts snake_case; plain
  lowercase taxonomy strings pass through.
- **Fix**: either fix upstream taxonomy strings, or expand
  `_pretty_token` to `.capitalize()` any all-lowercase token. (The
  comment in the function says it leaves human strings alone — but
  plain-lowercase strings aren't quite human either.)

**[P2-2] Double-space in pill format strings**

- Evidence: every pill from operation/policy facets:
  `"Substantive or base instrument  · 922"` — two spaces before
  the middle-dot.
- **Cause**: `format_func=lambda x: f"{_pretty_token(x)}  · {op_counts.get(x, 0):,}"`
  — line 563 in statutory_instruments.py, same for policy line 577.
- **Fix**: single space.

**[P2-3] `~40%` renders as `−40%` in the note**

- Evidence: `D03_tab_operation_pills.png` top — *"The responsible
  department is identified on **−40%** of instruments"*.
- **Cause**: the source string is `'identified on ~40% of instruments'`
  (line 862). Streamlit / fonts are mapping the ASCII tilde to a
  minus glyph in the chosen face.
- **Fix**: replace `~40%` with `≈40%` (Unicode `≈`) or
  `around 40%`.

**[P2-4] EU relationship values render with lowercase "u"**

- Evidence: `G05_detail_eu_minister_full.png` — pills "Eu full effect",
  "Eu instrument referenced".
- **Cause**: `_pretty_token` runs `.capitalize()`, which lowercases
  everything after the first letter.
- **Fix**: special-case the EU prefix, or use `.title()` with stoplist,
  or normalise the underlying values to start with "EU_".

**[P2-5] Pre-2014 Acts cross-link panel exposes raw internal IDs**

- Evidence: `G05_detail_eu_minister_full.png` — *"Act act_1993_statistics"*
  shown to user. The kicker says "MADE UNDER (PRE-2014 PRIMARY ACT,
  CURATED)" and the title is the clean "Statistics Act 1993", but the
  meta line dumps the slug.
- **Fix**: hide the meta line when the id starts with `act_` (it's an
  internal handle), or render it as a small `<code>` element below
  the kicker rather than full-width body text.

**[P2-6] Stat strip on detail shows "—" for missing department**

- Evidence: `G01_detail_default_full.png` — fourth stat cell shows
  long-dash with "DEPARTMENT" label, eating a quarter of the strip.
- **Fix**: collapse the cell when value is missing (drop the column
  rather than show a placeholder), OR show "Not identified" with
  a small footnote that ~40% of pre-2014 SIs have no detected
  department.

**[P2-7] "Show these N SIs →" duplicated in callout and tab**

- Evidence: `B01_callout_visible.png` vs `D10_tab_eu_scrutiny.png`
  — the same button appears twice on the page with the same on_click
  handler.
- **Fix**: drop the duplicate when collapsing per P1-5.

**[P2-8] Title parse bugs leak through**

- Evidence: `F01_pagination_footer.png` — SI 2026-108 reads
  *"...Restrictive Measures concerning Iran)(Human Rights)(No.2)..."*
  with missing spaces; `G01_detail_default_full.png` unmatched callout
  shows *"Evidence and DNA Database System) Act 2014"* with orphan
  closing paren.
- **Fix**: upstream cleanup. At minimum, regex-fix the obvious
  `)(`→`) (` and orphan-paren-balance check before write.

**[P2-9] "All departments" / "All operations" / "All policy areas"
pseudo-pills**

- Evidence: `D01_tab_department_pills.png` — first pill is "All
  departments" in the active state.
- **Issue**: this is a "clear this facet" affordance disguised as
  another value. Standard pattern is a clear-X icon on the active
  pill, not a separate "All" pill.
- **Fix optional**: keep "All" if it tests well — but consider an
  explicit "Reset facet" button at the right of each pill row.

---

### P3 — Polish

**[P3-1] Year-pill counts are misleading for the current year**

`2026 · 100` looks small next to `2025 · 570` because 2026 is
year-to-date. Add `(YTD)` suffix or italicise / dim the in-progress
year's count.

**[P3-2] Default 3-year selection bias**

Default selects 2026/2025/2024 — but the dataset spans 2016-2026.
A first-time visitor doesn't know that years before 2024 are also
available without scrolling and squinting at the pill row. Consider
defaulting to "All years" or to "Last 5 years" with a clearer label.

**[P3-3] Keyboard focus rings are the Streamlit default**

Evidence: `J02_focus_after_8_tabs.png` — focus state is the browser
default. Project tokens could supply a brand-aligned ring. Low impact
but cheap.

**[P3-4] Sidebar `page-subtitle` text duplicates state already in URL**

The sidebar shows "Statutory Instruments / SI detail" — the active nav
item is already highlighted; the subtitle adds no information.

**[P3-5] Two CSS injections per render**

`statutory_instruments_page` calls `inject_css()` then
`_inject_si_css()`. The page-local CSS uses hard-coded hex even though
PRODUCT.md notes existing tokens. Move SI tokens into `shared_css.py`
(or a per-page chunk per the deferred CSS-architecture-split debt
item) so the SI-specific block is just layout, not colour duplication.

---

### Patterns and systemic issues

1. **Data quality is the UI's biggest enemy here.** P0-1, P0-2, P2-1,
   P2-2, P2-4, P2-5, P2-8 all trace back to upstream extraction /
   taxonomy. The page does heroic display work over a messy frame.
   The single highest-leverage fix is a `si_title_clean` and
   `si_operation_pretty` pair in the gold layer or in a SQL view
   that wraps `v_statutory_instruments`.

2. **HTML-block-then-button is the wrong card pattern.** Used on the
   index, it forces a detached button. The project memory has a
   documented compact-card-with-adjacent-button pattern
   (`feedback_css_card_pattern.md`) that solves this. The SI page
   pre-dates that pattern and should adopt it.

3. **The EU-scrutiny story is the page's single most important
   editorial moment** and it's diluted by being told three times. One
   strong moment + one dedicated detail surface beats three weaker
   surfaces.

4. **Mobile is an afterthought.** No data above the fold, hero +
   callout dominate. The page was clearly designed at 1440 and
   responsive-checked, not designed for mobile-first.

---

### Positive findings (keep these)

- **EU scrutiny callout copy** — concrete, sourced, action-oriented.
  *"€1.54 m fine for failing to transpose the EU work-life balance
  directive on time"* is exactly the civic accountability voice
  PRODUCT.md asks for.
- **Side-stripe + serif hero pattern** — applied consistently with
  the rest of the app; honours the documented intentional override.
- **Tab labels carry their selected value** — `_tab_label` turning
  "Department" into "Department: Finance" once selected is a smart
  affordance that prevents the user losing track of state.
- **Deep-link round-trip works** — `?si=2026-117` cold-loads the
  detail panel without prior session state. Bookmark-friendly,
  share-friendly.
- **Pagination control is well-built** — numeric paginator with
  prev/next chevrons + ellipsis, "Showing 1-10 of 1,313 SIs" caption.
- **Empty-state on filters** uses the project `empty_state`
  component (not raw `st.warning`) — good.

---

## Part 2 — The uplift prompt

The prompt below is ready to hand to a coding session. It assumes the
person taking it has access to the repo and to the impeccable skill
context (PRODUCT.md, project memory). Drop it into a new conversation
or paste it after `/impeccable craft statutory instruments page` to
seed shape-then-build.

> ### Statutory Instruments page — comprehensive uplift
>
> Rework `utility/pages_code/statutory_instruments.py` and the
> upstream data it consumes (`data/gold/parquet/statutory_instruments.parquet`,
> built by `si_entity_enrichment.py`, surfaced via
> `sql_views/legislation_si_index.sql`) so that the page reads as a
> civic-editorial reference tool rather than a wireframe over a
> messy upstream frame. Hold to PRODUCT.md (Direct · Civic ·
> Accountable; editorial accountability journalism; ink-on-paper
> restraint; data is evidence) and the documented intentional
> overrides (side-stripe accent, `#ffffff` cards, signal-good/bad
> tokens). Stay inside Streamlit constraints; honour the project's
> logic-firewall split (no business metrics in the page, no JOIN /
> GROUP BY in retrieval SQL).
>
> Audit evidence — see `tmp/audit_si/AUDIT.md` and the 58 supporting
> screenshots in `tmp/audit_si/*.png`.
>
> #### Goals (in priority order)
>
> 1. **Make the cards readable.** Two upstream data bugs make ~10-20%
>    of cards walls of preamble text in ALL CAPS:
>    - Some `si_title` values include the instrument's full opening
>      citation. Truncate at the first `(ORDER|REGULATIONS|RULES|SCHEME)\s+\d{4}`
>      followed by whitespace + capital letter. Keep the original
>      in a new `si_preamble` column if any downstream view needs it.
>    - Some titles are ALL CAPS. If a title is ≥80% uppercase, apply
>      title-case with a stop-list (of, the, and, in, on, for, to, a,
>      an) and re-uppercase short acronyms in parentheses (EU, US, IR,
>      etc.). Add this normalisation as `si_title_clean` in the
>      enrichment script; surface it through `v_statutory_instruments`
>      (or a thin wrapper view).
>    - Don't fix this in Streamlit; fix it in the silver/gold layer
>      where it belongs.
>
> 2. **Fix the card-click contract.** Each `_render_si_card` is
>    currently an HTML block followed by a detached "View detail"
>    tertiary button. Replace with one of:
>    - Wrap the card body in `<a href="?si={id}">` and remove the
>      button; let the existing URL listener at the top of the page
>      pick up the change. Card becomes the click target.
>    - Or, adopt the documented compact-card + adjacent → button
>      pattern from `feedback_css_card_pattern.md`
>      (inline-flex + fit-content + `:has()` row collapse).
>    Either way, the visual gap between card and "View detail" must
>    disappear, and the click target must cover the whole card.
>
> 3. **Earn the mobile above-fold.** At 390×844 the page currently
>    shows only the hero and the EU callout — no data, no controls.
>    On ≤640px:
>    - Trim the hero dek to one sentence; drop the kicker.
>    - Collapse the EU callout to a single-row banner that links to
>      a dedicated `?view=eu-scrutiny` sub-view.
>    - Move the search input above the callout banner so it lands
>      above the fold.
>    - Year pills wrap to two rows at most; everything below that is
>      okay to require scroll.
>
> 4. **Consolidate the EU scrutiny story.**
>    - Keep the top-of-page editorial callout — it is the page's
>      strongest accountability moment.
>    - Remove the "⚠ EU scrutiny" facet tab.
>    - Add a `?view=eu-scrutiny` sub-view (or its own route under
>      `/eu-scrutiny`) that the callout's "Show these N SIs" button
>      navigates to. The sub-view holds the longer text, the metric
>      cards, the by-department breakdown, AND the filtered SI list.
>    - The same data should not be told three times on the same page.
>
> 5. **Make active-filter chips interactive.** In `_render_facets`,
>    the `.si-active-bar` chips are read-only. Each chip should be
>    a click-to-remove affordance. Prefer the `<a href="?clear=key">`
>    pattern with a top-of-page listener that calls the appropriate
>    `_clear_<facet>()`; fall back to native `st.button` styled as a
>    chip if the URL trick is too brittle.
>
> 6. **Rebalance the KPI strip.** Currently "Top policy domain" and
>    "Most active department" both lead with "Finance" / "Finance
>    banking tax". Either deduplicate (one cell showing the combined
>    story) or swap one for a different metric — recommended: pre-2014
>    vs post-2014 share, since it ties to the unmatched-parent-Act
>    callout that already appears on detail panels.
>
> 7. **Upgrade the not-found state.** `if match.empty:` currently
>    renders `st.warning("SI '{id}' not found.")`. Use the existing
>    `empty_state` component with civic-voice copy and an inline
>    search box; explain that the corpus covers 2016 onwards so old
>    bookmarks for earlier SIs won't resolve.
>
> #### Polish (P2 — fold into the same PR if time permits)
>
> - Single space (not double) before the middle-dot in pill format
>   strings — operation tab (`f"{_pretty_token(x)}  · {…}"` line 563)
>   and policy tab (line 577).
> - `~40%` → `≈40%` in the source note (line 862). The ASCII tilde
>   is rendering as a minus.
> - EU-relationship pill values like `Eu full effect` show lowercase
>   "u" because `.capitalize()` strikes after the first letter.
>   Special-case the EU prefix in `_pretty_token` (or normalise
>   upstream).
> - Operation/policy pill labels like `amendment`, `designation`,
>   `commencement`, `revocation`, `establishment`, `licensing`,
>   `fisheries` are lowercase next to sentence-case neighbours.
>   `_pretty_token` should `.capitalize()` any all-lowercase token,
>   or the upstream taxonomy should be normalised.
> - On the pre-2014 Acts cross-link panel, hide the raw `Act act_1993_statistics`
>   meta line (it's an internal slug — the user already has the clean
>   "Statistics Act 1993" title).
> - On the detail stat strip, drop the "Department" cell entirely
>   when the value is missing rather than rendering "—".
> - Cleanup: title-parse leakage like `)(` (missing space) and
>   orphan closing parens — fix at the parser, not in the UI.
>
> #### Non-goals (don't do these in this PR)
>
> - No new Polars / pandas enrichment in production paths beyond the
>   title cleaner; experimental enrichment goes to
>   `pipeline_sandbox/` per `project_pipeline_sandbox_rule.md`.
> - No CSS-architecture split (deferred design debt — defer until
>   page roster stable).
> - No typography-scale consolidation (also deferred).
> - Don't touch `pipeline.py` / `enrich.py` /
>   `normalise_join_key.py` (same sandbox rule).
>
> #### Acceptance
>
> Re-run `tmp/audit_si/capture.py` after the rework. New screenshots
> should show:
> - No card with body-text-as-title (P0-1 resolved).
> - No ALL-CAPS title on either the card list or detail panel (P0-2).
> - Every card is a single click target with no detached button (P1-1).
> - Mobile above-fold shows the search box AND a card preview or
>   at least one year-pill row (P1-2).
> - "⚠ EU scrutiny" tab is gone; callout still present; deep link
>   to the EU scrutiny view works (P1-5).
> - Active-filter chips remove their filter when clicked (P1-4).
> - Not-found state uses `empty_state` with civic copy (P1-6).
> - All P2 polish issues from the audit checklist resolved.
>
> Re-run `/impeccable audit` on the SI page after the rework and
> target a health score of 17+/20.

---

## Appendix — Screenshot index

Phase A: `A01-A05` — landing on desktop (full, above fold, mid, cards, bottom)
Phase B: `B01-B04` — EU scrutiny callout interaction
Phase C: `C01-C04 + _cards` — search states (fisheries / sanctions / covid / zero results)
Phase D: `D01-D10` — facet tabs (Department / Operation / Policy / Minister / EU scrutiny tab)
Phase E: `E01-E03` — active-filter scope bar (1 / many / cleared)
Phase F: `F01-F02` — pagination
Phase G: `G01-G13` — SI detail variants (default / EU-minister / cold-load deep link / Health dept / Substantive-or-base)
Phase H: `H01-H02` — empty / not-found
Phase I: `I01-I10` — tablet + mobile responsive
Phase J: `J01-J02` — keyboard focus traversal
Phase K: `K01` — year-pill defaults
