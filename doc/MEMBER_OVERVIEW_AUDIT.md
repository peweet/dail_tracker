# Member Overview — impeccable audit (2026-05-27)

> **Status update (2026-05-27 evening):** 2 P0 + 6 P1 + 6 P2 + 3 P3
> ALL SHIPPED in same-day fix sweep. Score lifted **11/20 → ~17/20**
> on the re-capture. Verification screenshots in the same folder
> overwrote the originals; the named findings below remain as the
> historical audit record.
> Open: the `v_lobbying_revolving_door` view should ALSO drop
> `former_position = 'TD'` upstream (P1-4 has a defensive UI guard
> but the pipeline-side cleanup is the proper fix).

Captured via Playwright over `/member-overview` on the running
Streamlit app. 33 screenshots in `audit_screenshots/_member_overview/*.png`
covering Stage 1 browse (desktop / tablet / mobile), Stage 2 profile
(closed expanders / hero / stat strip / Open-all / each section anchor),
minister stat-strip fallback (Darragh O'Brien — apostrophe-in-URL),
independent TD (Catherine Connolly), section-anchor deep links
(`#mo-section-interests`, `#payments`, `#votes`), not-found path,
sidebar Find-a-TD, and 5 legacy redirect URLs (each in a fresh
browser context per [[feedback_streamlit_playwright]]).

Capture script: `audit_screenshots/_member_overview_capture.py`.

**This is the first standalone audit of `/member-overview`.** The page
shipped through Phases 0–8 of the [[project-member-overview-consolidation]]
plan (2026-05-24) and absorbed the round-3 Playwright Tier A/B fixes
the same day, but the formal 20-criterion audit was deferred to
Phase 9 and never executed. This document closes that gap.

This document has two parts:
1. **Audit findings** — what's wrong, why it matters, ranked by severity.
2. **The uplift prompt** — handoff-ready prompt for a fix session.

---

## Part 1 — Audit findings

### Health score

| # | Dimension          | Score | Headline finding                                                                                       |
|---|--------------------|-------|--------------------------------------------------------------------------------------------------------|
| 1 | Accessibility      | 2/4   | Browse cards keyboard-accessible; 4 literal `<p class="section-heading">` not migrated to `<h2>`       |
| 2 | Performance        | 3/4   | Lazy-load gate works (≈5 SQL queries on cold load vs ≈25 with all sections open); 10 cached fetchers   |
| 3 | Theming            | 2/4   | Tokens used; 11+ inline `style=""` leaks; party affiliation has no party-colour signal anywhere        |
| 4 | Responsive         | 2/4   | Mobile data lands above fold; profile-nav buttons stack into 3 separate rows wasting 3× the space      |
| 5 | Anti-patterns      | 2/4   | No AI-slop tells; hero photo + credit + chips strong; revolving-door misfire shows for current TDs     |
| **Total** |            | **11/20** | **Acceptable — needs work** — significantly behind the 13–16/20 range of recently-audited pages.   |

### Anti-patterns verdict

**Pass** on AI-slop. The page is civic-editorial: serif name as h1,
ink-on-paper card surfaces, accent side-stripe on the hero, party
+ constituency in plain prose meta, no gradient text, no glassmorphism,
no hero-metric template. The hero block (avatar + photo credit + name +
party · constituency + role badge + social chips) is the strongest
"who this person is" identity strip in the app and should be preserved.

**The slop risk is the revolving-door badge.** It currently surfaces on
**current sitting TDs** because the SQL view's "former position" field
captures any prior position, including "TD" itself for re-elected
members. Mary Lou McDonald, Catherine Connolly, and Darragh O'Brien
all carry the orange warning chip — eroding the chip's signal value.
See P1-5.

---

### Executive summary

- **2 P0 blocking issues** — "Page not found" modal fires on every
  cold load of `/member-overview` (including the canonical `/`
  landing); the Phase 2 chrome's section-nav chip row never made it
  out of CSS into markup.
- **6 P1 major issues** — em-dash in stat strip when only one of
  attendance/payments is empty; SQL-view name leaks to citizens on
  the not-found state; mobile profile-nav buttons stack vertically
  consuming 3× the space; revolving-door badge misfires for current
  TDs; 4 literal `<p class="section-heading">` calls bypass the
  `evidence_heading()` `<h2>` helper; default stat-strip year uses the
  in-progress year (2026) instead of the most recent completed year.
- **6 P2 minor issues** — 11+ inline `style=""` attributes (CLAUDE.md
  forbids); "1,318 votes cast across 1,318 divisions" tautology when
  the numbers happen to match; party affiliation has no party-colour
  signal; hero photo credit wraps awkwardly on tall portraits;
  legacy-redirect callout's CTA is plain text-link rather than a
  proper button; revolving-door badge text in hero is unstyled
  `<span class="dt-badge dt-badge-revolving">` but no `.dt-badge-revolving`
  rule exists in `shared_css.py`.
- **3 P3 polish** — browse-stage hero claims "176 TDs" matching the
  pipeline but the older registry constant was 174 (provenance drift);
  Twitter chip renders as bare letter `X` not a glyph; Open-all
  button is a default Streamlit primary, not visually weighted.

Highest-leverage single fix: **P0-1 ("Page not found" modal).** Every
other finding on this page is silently overshadowed by a modal that
appears on direct load of the canonical TD page — the single most
important surface in the app per PRODUCT.md. Until that modal goes
away, citizens see a "broken" page first, evidence second.

---

### P0 — Blocking (fix before next release)

**[P0-1] "Page not found" modal fires on every cold load of /member-overview.**

- **Location**: app-router / `utility/app.py` interaction with the
  `member_overview_page` registered with `url_path="member-overview"`,
  `default=True`. Reproduces on direct URL: `/member-overview` AND
  `/member-overview?member=<code>`.
- **Evidence**: `A02_landing_above_fold.png`, `B02_profile_above_fold.png`,
  `B05_profile_expanders_closed.png`, `E02_minister_above_fold.png`,
  `F02_independent_above_fold.png`, `G_anchor_*.png`, `H01_not_found_bogus_code.png`,
  `K01_apostrophe_renders.png` — every Stage 2 screenshot captured
  before the modal was programmatically dismissed shows a centred
  modal: *"Page not found. The page that you have requested does not
  seem to exist. Running the app's main page."* The actual page
  renders BEHIND the modal (Mary Lou McDonald hero is visible behind
  the dialog), so the page DOES route correctly — but Streamlit's
  router emits a modal explaining a 404 that didn't happen.
- **Impact**: this is the most-trafficked page in the app
  (PRODUCT.md primary user journey is "citizen checking their local TD"),
  yet the first thing every citizen sees on cold load is a
  framework error dialog claiming the page doesn't exist. Trust is
  shot before any content lands.
- **Root cause** (suspected): the `default=True` flag was moved from
  `attendance_page` to `member_overview_page` during the consolidation
  rework. Streamlit's `st.navigation` treats the default page slug
  specially — when the URL matches the default's slug AND the router
  also tries to resolve via `st.query_params`, the second resolution
  can fire a 404 even though the first succeeded. Previous memory
  note ([[project-member-overview-consolidation]] round-3 P0 followup)
  flagged this as intermittent / Playwright-only; **this audit
  confirms it reproduces on direct fresh load**, not just multi-page
  chain navigation.
- **Recommendation** (in order of investigation cost):
  - (a) Remove `default=True` and add a router-level redirect from `/`
    to `/member-overview` (the explicit redirect is what should ship
    the user there — the magic `default` flag is causing the dual-resolution).
  - (b) Failing that, mute the modal with a script-injection workaround
    (an `MutationObserver` in `inject_css()` that removes the dialog
    when its inner text matches "Page not found"). Last-resort patch.

**[P0-2] Phase 2 section-nav chip row never renders — CSS-only feature.**

- **Location**: `utility/shared_css.py:1566-1597` defines
  `.mo-section-nav` + `.mo-section-chip` + `.mo-section-chip:hover`
  + `.mo-section-chip:focus-visible`. The companion `.mo-section-anchor`
  at `:1600` IS used (rendered at `member_overview.py:1021`). But the
  chip-row markup is **never emitted by any page code** — `grep -n
  "mo-section-nav\|mo-section-chip" utility/pages_code/member_overview.py`
  returns nothing.
- **Evidence**: `B02_profile_above_fold.png` and `B05_profile_expanders_closed.png`
  — between the stat strip and the "Open all sections" button there
  should be a chip row (per the Phase 2 plan: "section-nav chip row →
  Open all sections toggle → 7 expanders"). It isn't there. The
  capture filename `B04_profile_section_nav_chips.png` was named
  optimistically; the screenshot at that name shows the same content
  as B02 because nothing renders between B03 (stat strip) and B05
  (expanders).
- **Impact**: the Phase 2 plan promised a quick-jump strip so
  citizens could see all 7 dimensions at a glance and click straight
  to the one they want. Without it, the only way to navigate to
  Payments or Committees is to scroll past 6 other expander headers.
  Lost wayfinding on the longest page in the app.
- **Recommendation**: render the chip strip from `_PROFILE_SECTIONS`
  immediately after the stat strip / minister callout in
  `_render_stage2`:
  ```python
  chip_html = ['<nav class="mo-section-nav" aria-label="Profile sections">']
  for sid, label, _ in _PROFILE_SECTIONS:
      chip_html.append(
          f'<a class="mo-section-chip" href="#mo-section-{sid}">{_h(label)}</a>'
      )
  chip_html.append('</nav>')
  st.html("\n".join(chip_html))
  ```
  Same anchor format the URL hash already uses
  (`#mo-section-<sid>`). Restores the wayfinding contract Phase 2
  was designed around.

---

### P1 — Major (fix this milestone)

**[P1-1] Stat strip shows em-dash for Payments when attendance is present.**

- **Location**: `member_overview.py:935-957` — the `if att_empty and
  pay_empty:` branch (round-3 P1-F) only triggers when **both**
  attendance AND payments are empty. When only one is empty, the
  regular 3-column strip renders with a literal em-dash for the empty
  slot.
- **Evidence**: `B02_profile_above_fold.png`, `D01_open_all_full_desktop.png`,
  `F02_independent_above_fold.png` — every backbench-TD screenshot
  shows `26 / DAYS IN CHAMBER · 2026` and `791 / VOTES CAST · across
  791 divisions` followed by a bare `—` glyph above `PAYMENTS
  RECEIVED`. The TAA-payments parquet only covers ministers + a small
  subset of TDs (see [[project_payments_full_psa]]), so this em-dash
  is the rule for ~150 of 176 members, not the exception.
- **Impact**: a single em-dash among real numbers reads as broken data
  to citizens. The round-3 minister-fallback proved that explanatory
  copy in place of a stat is the better pattern; that fix should
  generalise to any single empty stat, not just the both-empty case.
- **Recommendation**: when only `pay_empty` is True, render the third
  stat as text instead of `—`:
  ```python
  pay_val = f"€{pay_total:,.0f}" if pay_total else "Not on file"
  pay_sub = "TAA · all years on record" if pay_total else (
      "Parliamentary Standard Allowance figures aren't tracked for "
      "this member"
  )
  ```
  Or — simpler — drop the cell entirely when missing (use a
  2-column strip with a small `st.caption` explaining the
  absence, mirroring SI P2-6 from [[project-si-audit-2026-05-24]]).

**[P1-2] Not-found state leaks raw SQL view name to citizens.**

- **Location**: `member_overview.py:810-816`.
- **Evidence**: `H01_not_found_bogus_code.png` — *"This TD is not in
  the dataset · No record matched `Not-A-Real-Member.D.1900-01-01` in
  `v_attendance_member_year_summary`. The link you followed may be
  out of date, or the pipeline has not yet ingested this member."*
  The view name `v_attendance_member_year_summary` is developer
  jargon visible to anyone hitting a stale bookmark.
- **Impact**: the not-found path is exactly when a citizen needs the
  most help, not the least. Showing them a database view name
  trains them to think this page is for engineers.
- **Recommendation**: rewrite the body to civic voice:
  ```python
  "We couldn't find this TD in the dataset. The link you followed "
  "may be out of date, or this member hasn't been added yet — the "
  "Oireachtas roster updates as the membership changes."
  ```
  Plus drop the `<code>{join_key}</code>` block — citizens don't
  need the slug echoed back. Keep the *Browse all TDs* link.

**[P1-3] Mobile profile-nav stacks into 3 separate rows.**

- **Location**: `member_overview.py:749-793` — `_render_profile_nav`
  uses `st.columns([1.4, 2.2, 2.2, 6])` for `[← All TDs] [← prev]
  [next →] [spacer]`. Streamlit columns collapse to one-per-row on
  mobile, so the four-column layout becomes 4 stacked rows (one of
  which is the invisible spacer).
- **Evidence**: `D03_open_all_full_mobile.png` — *"← All TDs" /
  "← Mary Butler" / "Matt Carthy →"* take 3 full-width rows, eating
  ~140px of vertical space at the top of every profile view on
  mobile.
- **Impact**: pushes the hero — the page's identity anchor — below
  the fold on mobile. The hero is what tells a citizen they're on
  the right TD's page; losing it kills wayfinding.
- **Recommendation**: at ≤640px wrap the three buttons inside a single
  `st.html('<div class="mo-prof-nav-mobile">...</div>')` row so they
  flow horizontally (or collapse to icon-only chevrons left + right
  + "All TDs" as a small back link). The existing desktop layout
  (`width_ratio=[1.4, 2.2, 2.2, 6]`) is fine.

**[P1-4] Revolving-door badge misfires for current TDs.**

- **Location**: `member_overview.py:832-833, 1049-1065`. The `_lobbying_rd`
  query (`v_lobbying_revolving_door`) returns a row whenever the
  member appears in any lobbying return as a "former position" —
  but for a re-elected TD, "former position = TD" is also recorded.
- **Evidence**: `D02_open_all_mid.png` shows the open Lobbying
  expander for Mary Lou McDonald with *"REVOLVING DOOR FLAG · Former
  position: **TD**. Appears on 1 lobbying return across 1 distinct
  firm."* — the "former position" is **TD**, her current job. The
  same badge fires on Catherine Connolly (`F02`) and Darragh O'Brien
  (`E02`) — both of whom are sitting members.
- **Impact**: the chip is meant to flag people who left office and
  then appeared on a lobbying register (the classic revolving-door
  pattern). When it surfaces for every current TD, it loses meaning
  — citizens learn to ignore it, and the genuinely-flagged
  ex-ministers blend in. The most politically potent signal on the
  page gets devalued.
- **Recommendation**: the `v_lobbying_revolving_door` view should
  exclude rows where `former_position = 'TD'` AND the member is
  currently sitting. This is a **pipeline-side fix** (`sql_views/lobbying_revolving_door.sql`
  or whichever view owns this). Streamlit-side, add a guard:
  ```python
  rd_html = ""
  if not rd_df.empty:
      pos = str(rd_df.iloc[0].get("former_position", "")).strip()
      if pos and pos.upper() != "TD":
          rd_html = '<span class="dt-badge dt-badge-revolving">Revolving door</span>'
  ```
  Same guard applies to the expander callout at lines 1049-1065.

**[P1-5] Four literal `<p class="section-heading">` calls bypass `evidence_heading()`.**

- **Location**: `member_overview.py:324, 390, 452, 677`. Same a11y
  issue we closed on legislation, votes, attendance, interests in
  the 2026-05-26/05-27 sweeps.
- **Evidence** — code grep:
  ```
  324: st.html('<p class="section-heading">Legislation sponsored</p>')
  390: st.html('<p class="section-heading">Statutory Instruments signed</p>')
  452: st.html('<p class="section-heading">Debate participation</p>')
  677: st.html(f'<p class="section-heading">{showing:,} TD{...}</p>')
  ```
  All four render visually identical to a proper `<h2>` (same
  `.section-heading` class), but screen readers can't navigate by
  heading level past the page `<h1>`. The `evidence_heading(text)`
  helper at `ui/components.py:328-339` was added for exactly this
  fix.
- **Impact**: the longest page in the app has *zero* navigable `<h2>`s
  inside the body — every section header is an unmarked paragraph.
  Screen-reader users can't skim the page structure.
- **Recommendation**: swap all 4 calls for `evidence_heading(...)`.
  The helper escapes text, so f-string interpolation rewires
  cleanly:
  ```python
  evidence_heading(f'{showing:,} TD{"s" if showing != 1 else ""}')
  ```

**[P1-6] Stat-strip default year is the in-progress year (2026).**

- **Location**: `member_overview.py:909` calls `_att_all_years()`
  which does `ORDER BY year DESC LIMIT 20`, then `:961` takes
  `att_df.iloc[0]["year"]` — i.e. the most recent, partial year.
- **Evidence**: `B02_profile_above_fold.png` shows *"26 / DAYS IN
  CHAMBER · 2026"* in late May 2026. The Dáil has sat ~40 days so
  far this year; 26 of 40 is a perfectly normal attendance rate.
  But citizens parsing it against the previous-year completed
  count (≈74 of 83) read it as a dramatic drop.
- **Impact**: makes every TD look like an absentee in the first half
  of any new year. Same bug we fixed in [[project-attendance-audit-2026-05-26]]
  P1-1 by adding `skip_current=True` to the in-page year filter.
- **Recommendation**: the stat-strip should default to the most
  recent **completed** year. Either skip the current calendar year
  in `_att_all_years` (return rows where `year < current_year`), or
  pick `att_df.iloc[1]["year"]` when the first row's year equals
  `today().year`. The completed-year cell can then be labelled
  cleanly as "DAYS IN CHAMBER · 2025". If the in-progress year is
  retained, append "(so far)" so the framing is honest.

---

### P2 — Minor (next pass)

**[P2-1] 11+ inline `style=""` attributes across the page.**

- **Location**: `member_overview.py` — incomplete grep:
  - `:359, :430` — `style="margin-bottom:0.3rem"` on `.leg-bill-card`
  - `:365, :437` — `style="margin-top:0.2rem"` on card footers
  - `:540-541` — `style="margin-top:0.2rem;font-size:0.85rem;
    color:var(--text-secondary);"` on debate-card meta line
  - `:615-616` — `<h1 style="margin:0.1rem 0 0.25rem;font-size:1.85rem;
    font-weight:700;font-family:'Zilla Slab',Georgia,serif;">` — the
    browse-stage `<h1>`'s entire typography lives inline
  - `:811` — `style="color:var(--text-meta)"` on the not-found body
  - `:815` — `style="margin-top:0.6rem;display:inline-block"` on the
    not-found CTA
  - `:900` — `style="margin:0.15rem 0 0.2rem"` on the profile `<h1>`
  - `:901` — `style="margin:0 0 0.55rem"` on the profile meta
  - `:951` — `style="margin:1rem 0 1.75rem"` on the minister callout
  - `:953` — `style="color:var(--text-secondary)"` on the minister body
- **Impact**: CLAUDE.md forbids inline `style=""`. The leaks here mean
  any theme/spacing/font-stack change needs 11+ separate edits to
  this one file. Same anti-pattern audited & fixed on legislation
  (round-3 P2-1) and interests (Part 3 M1).
- **Recommendation**: extract named classes in `shared_css.py`:
  - `.mo-browse-h1` for the browse hero `<h1>`
  - `.mo-profile-h1` for the profile `<h1>`
  - `.mo-profile-meta` for the meta-line
  - `.mo-callout-spaced` / `.mo-callout-secondary` for callouts
  Then replace inline styles with class references.

**[P2-2] "1,318 votes cast across 1,318 divisions" reads tautological.**

- **Location**: `member_overview.py:943-948` — the cabinet-member
  fallback line `f"<strong>{votes_cast:,}</strong> votes cast across
  <strong>{votes_div:,}</strong> divisions"`.
- **Evidence**: `E02_minister_above_fold.png` — *"1,318 votes cast
  across 1,318 divisions"*. The numbers happen to be equal because
  Darragh O'Brien voted on every division he attended. A citizen
  reading this thinks the sentence is broken (same number twice).
- **Recommendation**: when `votes_cast == votes_div`, collapse to
  "voted in all 1,318 divisions" or "1,318 divisions, one vote in
  each". Otherwise keep the two-number form (it conveys the absence
  of abstentions / missed votes).

**[P2-3] Party affiliation has no party-colour signal anywhere.**

- **Location**: hero meta line at `member_overview.py:901`
  (`<p class="td-meta">Sinn Féin · Dublin Central</p>`) and the
  browse-card meta via `clean_meta()` at `:700`.
- **Evidence**: `B02` Mary Lou McDonald "Sinn Féin · Dublin Central"
  + `E02` Darragh O'Brien "Fianna Fáil · Dublin Fingal East" +
  `F02` Catherine Connolly "Independent · Galway West" all render
  in the same neutral text colour with no swatch / pill / dot to
  signal party. The browse-stage party pill row (Fianna Fáil / Fine
  Gael / etc.) provides filter chips but never paints the actual
  member's party.
- **Impact**: party affiliation is the single most important meta
  attribute on a TD profile (more than constituency for a citizen
  trying to decide whether their representative is governing party
  or opposition). Plain prose buries it.
- **Recommendation**: add a `.party-swatch` rendered as a small
  square dot in front of the party text, coloured by party
  (signal-good/Sinn Féin orange/etc.). The `pages_code/committees.py`
  composition bar already maps every party to a colour; reuse that
  map.

**[P2-4] Hero photo credit wraps to 4 lines for tall portraits.**

- **Location**: `member_overview.py:838-842` — `caption_block` for
  the photo credit sits under the avatar.
- **Evidence**: `F02_independent_above_fold.png` Catherine Connolly —
  *"Photo: Office of the President of Ireland · Public domain ·
  Wikimedia Commons"* wraps to 4 lines under the 96×96px avatar,
  pushing the meta column visibly down. Mary Lou McDonald (B02)
  wraps to 3 lines. Darragh O'Brien (E02) wraps to 3 lines.
- **Impact**: makes the hero feel cramped and the credit line feel
  more important than it is. The credit is required attribution
  (CC BY licenses) but doesn't need this much visual weight.
- **Recommendation**: in `.dt-profile-avatar-credit`, set
  `max-width: 120px` (currently the credit can flow as wide as the
  avatar column allows) AND `font-size: 0.55rem` (currently 0.6rem).
  Alternative: collapse the credit to a small `i` icon that opens
  a tooltip with the full credit text.

**[P2-5] Legacy-redirect callout CTA is a plain text-link, not a button.**

- **Location**: shared `ui/components.py:member_moved_callout`.
- **Evidence**: `J01_legacy_att_td.png`, `J03_legacy_interests.png`,
  `J04_legacy_committees.png` — *"Open Mary Lou McDonald's profile →"*
  underlined-link styling. The accountability surfaces around it
  ARE buttons (Open all sections, Close all sections, etc.) so the
  text-link feels under-weighted.
- **Recommendation**: render the CTA as a `.dt-cta` styled link or
  a real `st.button` (with `st.markdown('[label](url)')` falling
  back if the helper needs to stay HTML-only).

**[P2-6] `.dt-badge-revolving` class is referenced but not defined.**

- **Location**: `member_overview.py:833` references
  `<span class="dt-badge dt-badge-revolving">`. `grep -n
  'dt-badge-revolving' utility/shared_css.py` returns nothing.
- **Evidence**: `B02_profile_above_fold.png` — the "Revolving door"
  badge does render visibly (warning icon + label) but its styling
  comes from the inherited `.dt-badge` rule + the `:material/warning:`
  icon on the sibling `st.badge` call inside the Lobbying expander.
  The dedicated `.dt-badge-revolving` modifier doesn't exist, so any
  intended differentiated styling (deeper orange? thicker border?
  pulse animation?) is silently absent.
- **Recommendation**: add `.dt-badge-revolving` to `shared_css.py`
  with a distinct warning-coloured background + border so the chip
  reads as a flag, not a routine label.

---

### P3 — Polish (nice-to-have)

**[P3-1] Browse hero says "176 TDs" but pipeline-internal constant is 174.**

- **Location**: `member_overview.py:677` computes `showing` from
  `_member_list(conn)` which queries `v_member_registry` — currently
  176 rows.
- **Evidence**: `A02_landing_above_fold.png` and `A04_landing_mid_cards.png`
  both show "176 TDs" as the count above the grid. The Dáil 34
  membership opened with 174 TDs (per the `member_registry.sql`
  comment); the 176 reflects late-elected by-election TDs absorbed
  later. Not a bug — but the file comment is stale and the
  provenance line at the page top doesn't explain the jump.
- **Recommendation**: low priority. Either trim to the 174 known-good
  members (drop by-election rows) or update the `member_registry.sql`
  comment + add a one-line provenance caption under the count.

**[P3-2] Twitter chip renders as bare letter "X" without a glyph.**

- **Location**: `ui/entity_links.py:social_icon_chip_html` for
  platform "twitter". The chip is rendering the visible-letter "X"
  rather than the X / Twitter icon.
- **Evidence**: `B02_profile_above_fold.png`, `E02_minister_above_fold.png`,
  `F02_independent_above_fold.png` — the social chip row ends with
  a small circle containing literal "x" / "X" character. Mary Lou
  shows "x" (lowercase), Darragh O'Brien shows "X" (uppercase), and
  Catherine has none. Inconsistent casing AND a fallback letter
  where a glyph should be.
- **Recommendation**: inspect `social_icon_chip_html("twitter", ...)`
  and either ship a proper X / Twitter SVG OR drop the chip when no
  glyph is available. The chip-without-glyph mode is worse than
  no chip.

**[P3-3] "Open all sections" button is a default Streamlit primary.**

- **Location**: `member_overview.py:992` uses `st.button(btn_label,
  key="mo_open_all_btn", help=...)`.
- **Evidence**: `D01_open_all_full_desktop.png` shows the button
  unstyled (default Streamlit white pill, dark text). Given the
  surrounding hero / stat-strip / expander chrome all use the
  brand serif + accent side-stripe pattern, the button visually
  disappears.
- **Recommendation**: wrap in a `.mo-open-all-btn-wrap` and add a
  CSS rule that gives the button slightly more weight (background:
  var(--accent-soft), border: 1px solid var(--accent)) — or move it
  to live INSIDE the section-nav chip row added in P0-2 as a
  trailing "Open all" pill matching the chip style.

---

### Patterns and systemic issues

1. **Lots of CSS scaffolding ships without matching markup.** P0-2
   `.mo-section-nav` is the most visible case, but the audit also
   surfaced `.dt-badge-revolving` (P2-6) — a class name in the HTML
   with no CSS rule on the other side. This pattern appears when
   work is split across multiple sessions and the integrating commit
   never lands. Worth a one-time `grep` audit of all `dt-*` / `mo-*`
   class names: every class should appear in both `shared_css.py`
   AND at least one `pages_code/*.py`.

2. **The em-dash fallback inherits the same fragility everywhere.**
   The round-3 P1-F fix specifically handled "both empty"; this audit
   shows "one empty" still leaks. The same pattern likely lurks in
   other stat strips on legislation, attendance, payments — every
   stat strip needs an empty-value philosophy more sophisticated
   than "render `—`".

3. **Pipeline + UI both leak SQL view names to citizens.** P1-2 on
   this page (`v_attendance_member_year_summary` in the not-found
   body) joins the same family as the round-3 P1-A todo-callout
   leaks and the committees `_transition_notice` leak — every empty/
   error state needs a citizen-voice rewrite review.

4. **The page is the only major surface without a formal audit.** Per
   the index-line catalogue in [[project_audit_fixes_2026_05_26]],
   every other dimension page now has a `doc/<PAGE>_AUDIT.md`. This
   doc closes the gap; the score (11/20) is significantly behind
   the recently-audited pages (13–16) but most findings are
   well-scoped surgical fixes, not architecture.

---

### Positive findings (keep these)

- **Hero block is the strongest identity strip in the app.** Avatar
  + photo credit + serif `<h1>` name + party · constituency meta +
  role badge + 5-platform social chip row + accent side-stripe. The
  whole composition is what `/about-this-page` should look like for
  every entity in the future.
- **Photo credit inline under the avatar — correct attribution.**
  Wikimedia Commons CC BY 2.0 / CC BY 4.0 / Public domain credits
  render under every photo without users having to hunt. Honours
  the license without dominating the layout.
- **Glossary strip on the browse stage** ("TD: Teachta Dála, a
  member of the Dáil" + "Accountability profile: attendance, votes,
  payments, lobbying, and legislation in one place"). Anti-jargon
  done correctly — small, secondary, citizens-first.
- **Lazy-load gate works.** The audit confirmed via timing that a
  cold load fires ~5 SQL queries; opening all 7 sections fires
  ~25. The `mo_open_<sid>` session-state gate is doing its job.
- **Cabinet-member fallback callout** (round-3 P1-F) is the right
  pattern for explanatory empty-data states — generalises in P1-1.
- **URL-driven everything.** `?member=<code>` round-trips through a
  fresh browser context; section anchors (`#mo-section-<sid>`) work;
  apostrophe in URL (Darragh O'Brien) round-trips through
  percent-encoding without breaking the lookup. Bookmark- and
  share-friendly.
- **Legacy redirects clean — no double render.** Each of the 5
  legacy URLs (`?att_td=`, `?member=` on payments / interests /
  committees, `?lob_pol=`) renders the shared
  `member_moved_callout` cleanly with `st.stop()`, no page body
  leaking underneath (round-3 P0 fix verified holding).
- **Browse-stage pagination + party-pill filter + Find-a-TD typeahead**
  all work end-to-end with `?member=` deep links on click.
- **Default-open Interests** — most politically potent section
  surfaces immediately per PRODUCT.md principle #3.

---

## Part 2 — Uplift prompt (self-contained)

> You are uplifting the Member Overview page
> (`utility/pages_code/member_overview.py`) after a Playwright audit.
> The full audit is in `doc/MEMBER_OVERVIEW_AUDIT.md`; do not regress
> anything in the "Positive findings" section.
>
> **Goal** — close 2 P0 + 6 P1 + 6 P2 in priority order; P3 polish
> optional. Target a re-audit score of 15+/20 (from 11/20 today).
>
> **Workflow**:
> 1. Open `member_overview.py`, `shared_css.py`, `app.py`,
>    `ui/components.py`.
> 2. For each finding, write the before/after (file, line, exact
>    replacement) before editing.
> 3. After all edits, re-run the capture:
>    ```
>    $env:PYTHONIOENCODING = "utf-8"
>    python audit_screenshots/_member_overview_capture.py
>    ```
>    Review diff in `audit_screenshots/_member_overview/`.
> 4. Update `project_member_overview_audit_2026_05_27.md` in memory:
>    tick each finding as "verified shipping" with the screenshot
>    citation.
>
> **Findings to close** (priority order):
>
> 1. **P0-1 "Page not found" modal.** In `app.py`, remove
>    `default=True` from the `member_overview_page` registration and
>    add a router redirect from `/` to `/member-overview`. If the
>    modal persists, inject a small `MutationObserver` in
>    `inject_css()` that removes any `div[role="dialog"]` whose body
>    text contains "Page not found".
>
> 2. **P0-2 Render the section-nav chip row.** In `_render_stage2`,
>    immediately after the stat strip / minister callout, emit:
>    ```python
>    chip_html = ['<nav class="mo-section-nav" aria-label="Profile sections">']
>    for sid, label, _ in _PROFILE_SECTIONS:
>        chip_html.append(
>            f'<a class="mo-section-chip" href="#mo-section-{sid}">{_h(label)}</a>'
>        )
>    chip_html.append('</nav>')
>    st.html("\n".join(chip_html))
>    ```
>    The CSS at `shared_css.py:1566-1597` already styles it.
>
> 3. **P1-1 Generalise the empty-stat fallback.** In `_render_stage2`,
>    when only `pay_empty` is True, replace the `—` cell with
>    `"Not on file"` (value) + `"Parliamentary Standard Allowance figures
>    aren't tracked for this member"` (sub). Same treatment for any
>    single-empty case.
>
> 4. **P1-2 Civic-voice not-found copy.** Rewrite `member_overview.py:810-816`
>    to drop the `v_attendance_member_year_summary` reference and the
>    `<code>{join_key}</code>` echo. Keep the Browse all TDs link.
>
> 5. **P1-3 Mobile profile-nav row.** In `_render_profile_nav`, swap
>    `st.columns(...)` for an `st.html(<div class="mo-prof-nav">...)`
>    that flows horizontally at all viewports. Add the CSS class to
>    `shared_css.py` with flex layout (gap 0.4rem, wrap).
>
> 6. **P1-4 Revolving-door misfire guard.** In `_render_stage2`
>    (~line 832) AND inside the Lobbying expander (~line 1049),
>    guard the badge / callout render with:
>    `if pos and pos.upper() != "TD":` — the genuine cases (former
>    Minister, former Senator) survive; current TDs no longer false-
>    positive. Document the pipeline-side fix needed in
>    [[project-member-overview-audit-2026-05-27]] memory.
>
> 7. **P1-5 Migrate 4 literal section-heading <p> calls.** Replace
>    `member_overview.py:324, 390, 452, 677` with `evidence_heading(...)`
>    (helper already imported via `ui.components` — add to the import
>    list if missing).
>
> 8. **P1-6 Skip the in-progress year on the stat strip.** In
>    `_att_all_years`, append `AND year < {datetime.date.today().year}`
>    to the WHERE clause OR pick the first non-current year in
>    `_render_stage2`. Document the in-progress-year choice
>    (`(so far)` suffix) if it's retained.
>
> 9. **P2-1 to P2-6 polish.** Extract inline styles to named classes,
>    rephrase the votes/divisions tautology, add a party-colour swatch,
>    tighten photo-credit width, button-ify the legacy-redirect CTA,
>    add the `.dt-badge-revolving` rule.
>
> **Out of scope** (do NOT regress):
> - Lazy-load session-state gate (`mo_open_<sid>` keys).
> - The hero block composition + photo credit + social chips.
> - The browse-stage party pill filter + Find-a-TD typeahead + pagination.
> - URL-driven everything (`?member=<code>`, section anchors).
> - Shared `member_moved_callout` helper (the J* redirect path).
> - The lifted-from-other-pages renderers (`render_member_interests`,
>   `render_member_lobbying`, etc.). Those are audited in their
>   parent pages.

---

## Part 3 — Positive findings recap (DO NOT REGRESS)

1. **Stage 1 → Stage 2 routing.** `?member=<code>` round-trip; section
   anchors; apostrophe URL encoding; back button clears state.
2. **Hero identity block.** Avatar + photo credit + name + meta +
   role badges + social chips — strongest entity-identity strip in
   the app.
3. **Lazy-load gate.** Cold load ~5 SQL queries; all-open ~25. Per-
   section session-state gate works.
4. **Default-open Interests.** PRODUCT.md principle #3 (most
   politically potent first) applied.
5. **Cabinet-member callout.** The pattern (round-3 P1-F) is the
   right shape — generalise it in P1-1.
6. **Legacy redirects.** All 5 `member_moved_callout` redirects
   render cleanly with `st.stop()` — no double rendering.
7. **Stage 1 browse.** Party-pill filter + Find-a-TD + paginated grid
   + URL deep-link on card click.
8. **Section anchors.** `#mo-section-<sid>` works with
   `member_profile_url(section=...)` for cross-page deep linking.

---

Re-run the Playwright capture after any change:
```
$env:PYTHONIOENCODING = "utf-8"
python audit_screenshots/_member_overview_capture.py
```
Writes to `audit_screenshots/_member_overview/`; assumes Streamlit
running on `localhost:8501` at `/member-overview` (the canonical TD
page slug, set via `url_path="member-overview"` in `utility/app.py`).
