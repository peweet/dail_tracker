# Committees page — impeccable audit (2026-05-26)

Captured via Playwright over `/rankings-committees` on the running
Streamlit app. 24 screenshots in `audit_screenshots/_committees/*.png`
covering desktop, tablet, mobile, register landing + chamber toggle
(Dáil ↔ Seanad) + filter command bar + pagination, committee detail
(identity strip + composition chart + roster), Find-a-TD typeahead, and
legacy `?member=` redirect.

**Mode 2b (in-page TD profile) was lifted to /member-overview in Phase
8 — this audit covers what's left.** The Phase 8 + round-3 fixes (CSV
typeahead caption, empty-state guards, shared redirect callout, citizen-
friendly transitional notice) are all verified shipping.

This document has two parts:
1. **Audit findings** — what's wrong, why it matters, ranked by severity.
2. **The uplift prompt** — handoff-ready prompt for a fix session.

---

## Part 1 — Audit findings

### Health score

| # | Dimension          | Score | Headline finding                                                                          |
|---|--------------------|-------|--------------------------------------------------------------------------------------------|
| 1 | Accessibility      | 3/4   | Cards keyboard-accessible; typeahead caption helps; roster Party column truncates labels   |
| 2 | Performance        | 3/4   | CSV unpivot per chamber switch is cached; pagination loads page-by-page                    |
| 3 | Theming            | 3/4   | Tokens used; party-composition bar uses colour map; minor `Active` status not chip-styled  |
| 4 | Responsive         | 3/4   | Filter bar stacks cleanly on mobile; "Data refresh underway" callout doesn't compress      |
| 5 | Anti-patterns      | 3/4   | No AI-slop tells; party-composition stacked bar on register cards is exemplary civic viz   |
| **Total** |            | **15/20** | **Good (address weak dimensions)** — tied with Votes and Lobbying for best per-page  |

### Anti-patterns verdict

**Pass** on AI-slop. The page is civic-editorial: serif hero
("Who sits on which committee"), party-coloured stacked bars per
committee card, ink-on-paper cards, no gradient text, no glassmorphism,
no hero-metric template. The horizontal party-composition bar on each
register card is **the best small-multiples viz in the app** — at-a-
glance shows you the political balance of every committee. Don't
regress it.

The two intentional PRODUCT.md overrides (side-stripe on hero card,
`#ffffff` over warm beige) are applied consistently.

---

### Executive summary

- **0 P0 blocking issues.** The page works end-to-end across all stages.
- **6 P1 major issues** — opaque "Coming soon" callout, search input
  doesn't filter on type, roster column truncation, composition chart
  Y-axis truncation, typeahead-on-Enter no-op, redundant chamber
  suffixes on Dáil titles.
- **6 P2 minor issues** — stat-strip label/segment overlap, first-card
  peach highlight, Active-status as text not chip, long titles wrap,
  repeated Oireachtas.ie ↗ links, "TD" header label cryptic.
- **2 P3 polish** — sidebar chamber-context absent, sort caption on
  composition chart.

Highest-leverage single fix: **rewrite the `todo_callout` source-PDF
message** (P1-1). The round-3 P1-A helper makes the body collapse to
"More data coming soon." — a citizen-useless string occupying prime
above-the-fold real estate. Replace the source call with a real
citizen sentence that actually says what's coming.

---

### P1 — Major (fix before next release)

**[P1-1] `todo_callout` produces a vacuous "More data coming soon."**

- **Location**: `_stage_committee` ~line 526 in
  `utility/pages_code/committees.py`. Call:
  `todo_callout("source_document_url column on v_committee_sources")`.
- **Evidence**: `E01_committee_detail_full.png` shows the callout
  rendered between the identity strip and the composition/roster
  columns. Body text: just *"More data coming soon."* — the round-3
  P1-A helper rewrite strips internal scaffolding from the message; the
  source string has no em-dash or citizen sentence so the helper falls
  back to its default.
- **Impact**: occupies a full callout block above the fold on the
  committee detail page but tells the user nothing actionable. Worse
  than no callout — looks broken.
- **Recommendation**: rewrite the callsite message to give the helper
  something to extract, e.g.
  `todo_callout("Source documents — the official terms of reference + meeting transcripts will link here in a future release.")`.
  The em-dash split-pattern recovers the citizen sentence cleanly.

**[P1-2] Search input doesn't filter on type — Enter-to-apply trap**

- **Location**: `_stage_register` ~line 338 in `committees.py`. The
  search uses `st.text_input` without `on_change=`.
- **Evidence**: `C01_search_finance.png` — input field shows "Finance"
  with a red border and a "Press Enter to apply" inline hint, BUT the
  results below are unchanged (still showing all committees including
  Working Group of Committee Cathaoirligh, Standing Orders, etc — none
  containing "Finance").
- **Impact**: placeholder "e.g. Finance, Health…" suggests live filter
  behaviour; reality is Enter-to-submit. Users who type and don't
  press Enter see no results, conclude the filter is broken, or just
  scroll past their intended target. Common Streamlit usability bug.
- **Recommendation**: either
  - (a) accept the existing Streamlit default and update the
    placeholder to *"e.g. Finance, Health (press Enter)"* or add an
    explicit caption, OR
  - (b) implement live filter via `on_change=` callback that triggers
    `st.rerun`. (a) is the cheaper fix.

**[P1-3] Roster table Party column too narrow — labels truncated**

- **Location**: `_stage_committee` ~line 580. Uses
  `committee_roster_column_config(member_label[:-1])` from
  `utility/ui/table_config.py`.
- **Evidence**: `E03_committee_detail_composition_roster.png` mid-scroll
  — Party column shows `Social Dem` (truncated from "Social
  Democrats"), `Independer` (truncated from "Independent"),
  `Independer` (truncated from "Independent Ireland"). At desktop
  width, with TD + Party + Constituency + Role columns sharing the
  roster width, Party is squeezed.
- **Impact**: A citizen looking at a committee roster can't tell
  "Independent" from "Independent Ireland" — these are different
  parties politically.
- **Recommendation**: in `committee_roster_column_config`, set the
  Party column to `width="medium"` (vs default "small"). OR drop the
  Constituency column on this view (committee composition rarely
  needs constituency; it's already in the member profile).

**[P1-4] Composition chart Y-axis labels truncated**

- **Location**: `_stage_committee` Composition column ~line 562. Altair
  chart with `y=alt.Y("party:N", ...)`.
- **Evidence**: `E01_committee_detail_full.png` shows the Composition
  bar chart with Y-axis labels *"Fianna Fáil / Fine Gael / Social
  Democrats / Independent Irel..."* — last label truncated.
- **Impact**: same problem as P1-3 but on the visual side — bar chart
  is the headline composition viz, ending its Y-axis with
  "Independent Irel..." undermines the data.
- **Recommendation**: increase chart's left padding OR reduce font
  size on Y-axis labels OR truncate-with-ellipsis only when the label
  is too long for the column width (Altair `axis=alt.Axis(labelLimit=…)`).

**[P1-5] Typeahead "pick from suggestions" doesn't show suggestions on auto-type**

- **Location**: `_stage_register` cmd_r ~line 367 — `find_a_td_search`
  custom widget.
- **Evidence**: `F01_typeahead_with_query.png` — text "Mary Lou
  McDonald" is in the input, but no suggestions dropdown is visible.
  The round-3 P1-E fix added the caption *"Type a name then pick from
  the suggestions"* but the suggestion mechanism may not trigger
  reliably (depends on focus + actual keyboard typing vs. programmatic
  fill).
- **Impact**: a real user who types fast or pastes a name may not see
  suggestions appear and conclude the search is broken. The caption
  helps but doesn't guarantee the dropdown opens.
- **Recommendation**: verify the underlying `find_a_td_search` widget
  in real keyboard interaction (not just Playwright). If suggestions
  fail to open under common conditions (focus already away, paste
  event), add an explicit "Show suggestions" button OR a dropdown
  combobox replacement.

**[P1-6] Dáil committees carry redundant `(Dáil Éireann)` suffix**

- **Location**: `df_long["committee"]` from `_load` (`committees.py`
  ~line 116) — pulls `{prefix}_name_en` straight from the silver CSV.
- **Evidence**: `B01_seanad_register.png` shows Seanad committees with
  `(Seanad Éireann)` suffix (e.g. "Committee on Parliamentary
  Privileges and Oversight (Seanad Éireann)"). Same pattern visible on
  Dáil side in `A02_register_above_fold.png` (committee names ending
  in `(Dáil Éireann)`).
- **Impact**: the chamber pill at the top already establishes which
  chamber the user is in. The `(Dáil Éireann)` / `(Seanad Éireann)`
  suffix on every committee title is duplication noise that pushes
  the meaningful committee name later in the title.
- **Recommendation**: strip the chamber suffix in `_load()`:
  `c_name = re.sub(r"\s*\((Dáil|Seanad) Éireann\)\s*$", "", c_name)`.
  Keep the un-suffixed name in the data model; let the chamber pill
  carry the context.

---

### P2 — Minor (next pass)

**[P2-1] "ACTIVE MEMBERSHIPS" stat label clashes with "Active" status segment**

- **Location**: `_stage_register` stat strip ~line 396.
- **Evidence**: `A02_register_above_fold.png` shows the status
  segmented control with "Active" highlighted right above the stat
  strip's "645 ACTIVE MEMBERSHIPS" cell. Same word, different
  contexts.
- **Recommendation**: rename the stat to "Current memberships" or
  "Memberships in force" — clearer distinction from the status filter.

**[P2-2] First-card peach highlight is heavy without explicit selection**

- **Location**: CSS for the `#1` rank-chip column on committee cards.
- **Evidence**: `A02_register_above_fold.png`, `B01_seanad_register.png`
  — the first committee card has a peach background extending across
  the full card. Not a hover state on second viewing (consistent
  across screenshots). Possibly an intentional "currently focused"
  marker on the first card, but no other card has any state styling.
- **Recommendation**: make it clearly intentional (e.g. add a
  "Editorial pick / Most-active" label) OR drop the peach so cards
  are uniformly weighted.

**[P2-3] Identity strip's `Active` shown as inline text, not colored chip**

- **Location**: `committee_identity_strip` in `ui/components.py`.
- **Evidence**: `E01_committee_detail_full.png` — "Active · Parl.
  Administration · 29 members · Chair: Sean Fleming (Fianna Fáil)"
  — "Active" is just plain text in the metadata line. Register cards
  show ACTIVE as a green chip; detail page inconsistent.
- **Recommendation**: use the same colored chip on the detail identity
  strip as on the register cards.

**[P2-4] Long committee titles wrap awkwardly**

- **Location**: register card title rendering.
- **Evidence**: e.g. "Committee on Parliamentary Privileges and
  Oversight (Dáil Éireann) ACTIVE" — once P1-6 strips the chamber
  suffix this becomes shorter. May still wrap for the longer ones.
- **Recommendation**: covered partially by P1-6. If titles still wrap
  on mobile, ensure card height grows gracefully (no overlap with the
  party bar below).

**[P2-5] Repeated `Oireachtas.ie ↗` links per card — visual noise**

- **Location**: register card footer.
- **Evidence**: `A02_register_above_fold.png` — same pattern as Votes
  page P2-2. Five identical accent-coloured external links create a
  vertical column of click bait that distracts from the actual click
  target (the committee card itself).
- **Recommendation**: drop from cards, surface inside committee detail
  identity strip only (where it already is). OR de-emphasise on cards.

**[P2-6] Roster column header "TD" is cryptic**

- **Location**: `committee_roster_column_config` in
  `ui/table_config.py`.
- **Evidence**: `E01_committee_detail_full.png` Roster table column
  headers: "TD / Party / Constituency / Role". Glossary expansion of
  TD is fine for Dáil committees; mixed-chamber audiences may need
  "Member" or "Name".
- **Recommendation**: rename header to "Member" (chamber-neutral)
  since the parent page already shows Dáil/Seanad context.

---

### P3 — Polish

**[P3-1] Sidebar doesn't show current chamber**

- **Location**: `committees_page` sidebar.
- **Evidence**: Mobile screenshot `A07_register_mobile.png` — sidebar
  is collapsed. On desktop, the sidebar shows "Committees" highlighted
  in the nav but doesn't echo the chamber (Dáil vs Seanad). User who
  scrolls deep into the committee list might forget which chamber
  they're in.
- **Recommendation**: add a small "Dáil register" / "Seanad register"
  caption in the sidebar under the page header. Already done on the
  votes page (date-range caption).

**[P3-2] Composition chart could explicitly sort by seats**

- **Location**: `_stage_committee` Composition Altair chart, uses
  `y=alt.Y(... sort="-x", ...)` so it does sort by seats descending.
- **Evidence**: `E01_committee_detail_full.png` — Fianna Fáil
  (largest) at top, Fine Gael below, Social Democrats, Independent
  Ireland. **Already sorted correctly.** Re-evaluating — this is fine.
- **Recommendation**: no change needed; documented for completeness.

**[P3-3] No "Coming soon" / "in development" badge on the experimental
PoC Lobbying page in the sidebar**

- **Location**: sidebar nav.
- **Evidence**: `A02_register_above_fold.png` sidebar shows "Lobbying
  (PoC)" — fine, suffix is informative. No equivalent for any other
  page-in-development. Just a note for consistency.

---

### Positive findings — preserve these

1. **Party-composition stacked bars on every register card** — best
   small-multiples viz in the app. At-a-glance visual democracy:
   Fianna Fáil 8 / Sinn Féin 7 / Fine Gael 7 / etc. Don't regress.
2. **`Data refresh underway` callout** is appropriately citizen-
   friendly (round-3 P1-A fix shipping). Compare to the prior
   dev-jargon paragraph that named SILVER_MEMBERS_CSV / yaml refs.
3. **Find-a-TD caption** *"Type a name then pick from the suggestions
   to open that member's committee profile."* sets correct
   expectations (round-3 P1-E).
4. **Empty-state guards** on composition/roster (round-3 P1-D) prevent
   silent grey rectangles. Verified shipping — committee detail with
   missing data shows clean empty_states instead of empty df boxes.
5. **Legacy `?member=` redirect** uses the shared
   `member_moved_callout()` helper consistently — exemplary cross-
   page contract. Compare to votes which still has its old inline
   callout (P1-6 in votes audit).
6. **Mobile layout** — filter command bar stacks cleanly, callout
   doesn't compress, sidebar collapses.
7. **Chamber toggle** — Dáil ↔ Seanad swap correctly updates stats
   (61→32 committees / 132→59 members / etc) AND the typeahead
   placeholder updates from "Type a TD name…" to "Type a Senator
   name…". Context-aware. 

---

## Part 2 — Uplift prompt

Paste this prompt into a fresh Claude Code session when you're ready
to ship the fixes. Self-contained.

```
We're uplifting the Dáil Tracker Committees page (/rankings-committees)
based on the 2026-05-26 impeccable audit at doc/COMMITTEES_AUDIT.md.
Read that file first — it has the screenshots and findings.

Context to read first:
- doc/COMMITTEES_AUDIT.md (this audit)
- audit_screenshots/_committees/*.png (24 screenshots; review at least
  A02, B01, C01, C03, E01, E03, F01, G01)
- utility/pages_code/committees.py (the page)
- utility/ui/components.py:todo_callout (P1-1 helper behaviour)
- utility/ui/components.py:member_moved_callout (Phase 8 redirect helper)
- utility/ui/table_config.py:committee_roster_column_config (P1-3)
- The 6 recurring patterns memory:
  C:\Users\pglyn\.claude\projects\c--Users-pglyn-PycharmProjects-dail-extractor\memory\project_app_design_synthesis_2026_05_26.md

Scope: fix the 6 P1s and high-leverage P2s. Do NOT regress the
party-composition stacked bars on register cards — they're the
strongest civic data viz in the app.

Priority order:

1. **P1-1 (vacuous Coming-soon callout)** — rewrite the call:
   `todo_callout("Source documents — the official terms of reference + meeting transcripts will link here in a future release.")`.
   The em-dash + sentence-after-em-dash gives the helper a real
   citizen string to extract. Test by visually inspecting the
   committee detail page after the change.

2. **P1-2 (search input Enter-to-apply trap)** — update the
   placeholder to make Enter-to-apply explicit, OR wire
   `on_change=lambda: st.rerun()` for live filter behaviour. Pick (a)
   if changing the Streamlit submit pattern is risky; (b) for the
   better UX.

3. **P1-3 (Roster Party column truncated)** — in
   `utility/ui/table_config.py:committee_roster_column_config`, bump
   `Party` to `width="medium"` (default is "small"). Also consider
   dropping `Constituency` from this view — it's a committee roster,
   not a member directory.

4. **P1-4 (Composition Y-axis truncation)** — in `_stage_committee`
   composition Altair chart, add `axis=alt.Axis(labelLimit=180)` OR
   increase the chart's left padding so full party names fit
   ("Independent Ireland" should not become "Independent Irel...").

5. **P1-5 (typeahead suggestions inconsistent)** — verify the
   `find_a_td_search` widget in actual browser keyboard interaction
   (not Playwright). If reproducibly fails, replace with a
   `st.selectbox` combobox that shows suggestions on focus.

6. **P1-6 (redundant `(Dáil Éireann)` suffix on titles)** — strip the
   chamber suffix in `_load()` (committees.py ~line 116):
   `c_name = re.sub(r"\s*\((Dáil|Seanad)\s+Éireann\)\s*$", "", c_name).strip()`.
   The chamber pill already establishes context.

7. **P2-3 (Active as text, not chip)** — make the identity strip's
   "Active" / "Ended" a colored chip matching the register card
   pattern.

8. **P2-1 (ACTIVE MEMBERSHIPS label clash)** — rename stat to
   "Current memberships".

9. **P2-6 (Roster "TD" header → "Member")** — chamber-neutral.

After each fix:
- Re-run audit_screenshots/_committees_capture.py
- Compare new screenshots against the originals in
  audit_screenshots/_committees/
- Update project_committees_audit_2026_05_26.md memory entry with
  what shipped

Don't:
- Regress the party-composition stacked bars on register cards
- Touch lobbying_2.py / lobbying_3.py / statutory_instruments.py
  (other agents own those)
- Add new features outside the audit scope
- Change the v_committee_* views (those are pipeline work; only modify
  Streamlit code on this page + table_config.py)

Save findings updates and verification screenshots in the same audit
directory so future audits can diff.
```

---

## Capture / verify scripts

- `audit_screenshots/_committees_capture.py` — full Playwright sweep,
  24 screenshots across 7 phases. Re-run after each fix.

Re-run after any change:
```
python audit_screenshots/_committees_capture.py
```
