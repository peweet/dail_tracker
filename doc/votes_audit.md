# Votes page — impeccable audit (2026-05-26)

> **Status update (2026-05-26):** All 6 P1s + 6 of 9 P2s shipped. See
> `[[project-votes-audit-2026-05-26]]` memory for the rework diary.
> Mode C left untouched as intended. Still open: P1-1 alt (bill grouping
> with sub-divisions, deferred on pipeline view), Appendix #2 (theme
> classification logic-firewall), Appendix #4 (heading semantics
> cross-page), P2-3 (year-pill count), P3 polish.


Captured via Playwright over `/rankings-votes` on the running Streamlit
app. 22 screenshots in `audit_screenshots/_votes/*.png` covering desktop,
tablet, mobile, Mode A landing + filters + show-all, Mode C division
evidence, TDs view + picker, Mode B legacy redirect, and edge-case
empty states. **Mode B in-page TD profile was lifted to /member-overview
in Phase 7 — this audit covers what's left.**

This document has three parts:
1. **Audit findings** — visual + interaction issues with severity ranks.
2. **The uplift prompt** — handoff-ready prompt for a fix session.
3. **Appendix: prior civic-ui-review** — earlier contract-compliance
   review (some findings retired post-Phase-7; others still apply).

---

## Part 1 — Audit findings

### Health score

| # | Dimension          | Score | Headline finding                                                                                  |
|---|--------------------|-------|----------------------------------------------------------------------------------------------------|
| 1 | Accessibility      | 3/4   | Cards keyboard-accessible; year pills work; mobile has two redundant arrow affordances per card    |
| 2 | Performance        | 3/4   | "Show all 207 divisions" renders the full DOM at once on click — no pagination after the threshold |
| 3 | Theming            | 3/4   | Tokens used; minor inline-style on the Mode A todo_callout. Mode B redirect uses old inline pattern|
| 4 | Responsive         | 3/4   | Cards stack 1-wide on mobile; titles wrap to 3-4 lines; "Oireachtas ↗" + "→" affordance overlap    |
| 5 | Anti-patterns      | 3/4   | Mode C is exemplary "data is the design"; TD picker has two redundant CTAs per card                |
| **Total** |            | **15/20** | **Good (address weak dimensions)** — best per-page score after Lobbying                     |

### Anti-patterns verdict

**Pass** on AI-slop. The page is recognisably civic-editorial: serif
titles in Zilla Slab, ink-on-paper cards, accent pills, party-coloured
stacked bars on Mode C. The two intentional PRODUCT.md overrides
(side-stripe on hero, `#ffffff` cards) are respected.

**Mode C (division evidence) is the strongest page in the entire app.**
Number row + party-breakdown stacked bar + member-votes filter is
exemplary "data IS the design". No anti-patterns, no chrome bloat, no
empty-em-dashes. Use this page as the polish target other pages should
match.

---

### Executive summary

- **0 P0 blocking issues.** The page works end-to-end across all modes.
- **6 P1 major issues** — duplicate bill titles in the index, redundant
  CTAs on the TD picker, invisible filter state in main panel, mobile
  double-affordance, picker badge semantics, and the Mode B redirect
  using the old inline pattern instead of the shared helper.
- **9 P2 minor issues** — margin glyph clarity, repeated source links,
  too many year pills, oversized picker button, orphaned "on" word,
  copy polish.
- **3 P3 polish** — party-breakdown sort, provenance prominence,
  sidebar caption hierarchy.

Highest-leverage single fix: **consolidate the TD picker card** (P1-2
+ P1-5 + P2-4) — one clean CTA per card plus clearer "VOTED YES on" /
"VOTED NO on" framing. This is a one-page-section change that retires
three findings at once.

The Appendix (prior review) flags **3 code-level issues that remain
unaddressed**: `use_container_width=True` on plotly charts (deprecated),
topic classification done in Streamlit (logic-firewall violation),
section headings as `<p>` not `<h2>`/`<h3>` (a11y).

---

### P1 — Major (fix before next release)

**[P1-1] Duplicate bill titles in the division index**

- **Location**: `_card_list_fragment` in `utility/pages_code/votes.py`
  (~line 379). Cards render from `_fetch_vote_index`.
- **Evidence**: `B01_outcome_carried.png` shows
  *"Remediation of Dwellings Damaged by the Use of Defective Concrete
  Blocks (Amendment) Bill 2025: Committee and Remaining Stages"* twice
  with different vote counts (72/63/0 vs 72/64/0). `C01_show_all_expanded.png`
  shows "Finance Bill 2025: Report and Final Stages" appearing 3+ times.
- **Impact**: bills go through multiple stages, each gets its own vote
  — but the cards don't visually distinguish them. Users see what looks
  like a duplicate, get confused, or worse mistake one stage for
  another. Margin badges (`△+N`) are the only differentiator and they're
  tiny.
- **Recommendation**: either
  - (a) group sequential votes on the same bill within an outer
    bill-card with N sub-divisions inside, OR
  - (b) add a visible stage-suffix pill (e.g. `Stage 4 of 5` /
    `Resumed`). The stage is already in the title between colons but
    needs visual weight.

**[P1-2] TD picker cards have two redundant CTAs**

- **Location**: `_render_td_picker` in `utility/pages_code/votes.py`
  (~line 375). Phase 7 round-3 refactor.
- **Evidence**: `E01_tds_view_picker_landing.png` shows each TD card
  with a tiny "Profile ↗" pill at top-right AND a heavyweight black
  uppercase "VIEW NAOISE Ó CEARÚIL'S RECORD →" button below it. Both
  go to the same `/member-overview?member=<id>#votes` URL.
- **Impact**: redundant clickable surfaces split focus. The button is
  visually loud (largest typographic element on the page); the pill is
  small and could be missed. Either is fine alone.
- **Recommendation**: drop the lower button. Keep the smaller "Profile
  ↗" pill OR make the whole card a single click target via
  `clickable_card_link` (the pattern used in `_render_mode_a`'s vote
  cards). One click target per card.

**[P1-3] Sidebar filter state isn't echoed in the main panel**

- **Location**: `_render_mode_a` (~line 452). Sidebar selectboxes for
  outcome + party.
- **Evidence**: `B01_outcome_carried.png` — sidebar shows
  `OUTCOME: Carried` but the main "147 divisions · showing first 25"
  caption gives no indication the filter is active. A user scrolling
  through results has no breadcrumb back to "you are filtered".
- **Impact**: violates GOV.UK service standard #4 (simple to use) —
  users lose track of their filter context. Common pattern bug, easy
  fix.
- **Recommendation**: append filter state to the count caption, e.g.
  `147 carried divisions in 2025 · showing first 25`. Already works
  for year; add outcome + party.

**[P1-4] Mobile cards have two arrow affordances per card**

- **Location**: `vt_division_card_html` in `utility/ui/vote_explorer.py`
  + Mode A card wrapper.
- **Evidence**: `A07_modeA_mobile.png` — each vote card has
  `Oireachtas ↗` (top-right, accent-coloured external link) AND `→`
  (right edge, internal navigation). On mobile they're close together
  and both look tappable. The external link goes to oireachtas.ie;
  the internal arrow opens `/rankings-votes?vote=<id>` (Mode C).
- **Impact**: at thumb-tap precision both are plausible targets.
  Confused taps degrade trust.
- **Recommendation**: visually distinguish — make the internal arrow
  the primary affordance (larger, accent-coloured) and the external
  link smaller/quieter (footer of the card, not header). Or move
  Oireachtas link to the inside of the Mode C detail panel where it
  belongs in context.

**[P1-5] "VOTED YES/NO" badge on TD picker reads as bill outcome**

- **Location**: `_td_pick_card_html` (~line 365 in `votes.py`).
- **Evidence**: `E01_tds_view_picker_landing.png` shows cards labelled
  *"X VOTED NO on …"* and *"✓ VOTED YES on …"* in red/green badges.
  The badge applies to the TD's vote, but visually reads as the bill's
  outcome (red = lost, green = carried).
- **Impact**: semantic confusion. A user seeing
  *"X VOTED NO on Energy and Fuel Costs Motion"* may think the motion
  failed (it didn't necessarily — the TD just voted no).
- **Recommendation**:
  - (a) reframe the badge as "TD's vote: NO" or use a quieter chip
    style (not signal-good/bad colours that imply outcome), OR
  - (b) add a separate small badge for the bill's actual outcome
    (CARRIED/LOST), distinct from the TD's vote.

**[P1-6] Mode B legacy redirect uses the OLD inline callout pattern**

- **Location**: `_render_mode_b_redirect` (~line 479 in `votes.py`).
- **Evidence**: `F01_modeB_redirect.png` shows the callout. The text
  says "TD voting profiles have moved" and the link is "Open profile
  →" (generic — no TD name). Other rankings pages (interests, payments,
  attendance, committees) use the shared `member_moved_callout(name,
  section, ...)` helper which says "Open Mary Lou McDonald's profile →".
- **Impact**: stylistic inconsistency across the cross-page contract.
  Round-3 audit Tier-A migration moved 4 pages to the shared helper
  but missed votes (because votes uses `member_id` directly, not
  `name_join_key`, so the bug-fix motivation didn't apply).
- **Recommendation**: refactor `_render_mode_b_redirect` to call
  `member_moved_callout`. Look up the TD name from `td_vote_summary`
  so the link can say "Open Mary Lou McDonald's profile →".

---

### P2 — Minor (next pass)

**[P2-1] Margin glyph `△+N` is opaque to citizens**

- **Location**: `vt_division_card_html`.
- **Evidence**: cards show `△ +100` / `△ +14` / `△ +2`. The triangle
  is delta/margin notation that may not be universally understood.
- **Recommendation**: replace with "margin: +N" or "won by N" / "lost
  by N" depending on outcome. Use accessible plain English.

**[P2-2] "Oireachtas ↗" link repeats on every card — visual noise**

- **Location**: card top-right. Every card has the same external link
  with the same accent colour.
- **Evidence**: `A02_modeA_above_fold_desktop.png` — five cards visible,
  five identical "Oireachtas ↗" links creating a vertical column of
  accent-coloured text.
- **Recommendation**: drop the external link from the index card and
  move it into the Mode C detail panel (where one source link is
  appropriate). OR de-emphasise on cards: small grey, no accent.

**[P2-3] 11 year pills near the limit of visual scan**

- **Location**: year_selector pills on Mode A.
- **Evidence**: `A02_modeA_above_fold_desktop.png` — 2026 through 2016
  in a single horizontal row.
- **Recommendation**: keep the most-recent 5 as pills, collapse older
  years into a "More years…" dropdown, OR use the same dropdown
  pattern as the sidebar party filter.

**[P2-4] TD picker button is shouty (ALL CAPS, black background)**

- **Location**: `_render_td_picker` after `entity_cta_html`. The CTA
  helper renders the dt-entity-cta class.
- **Evidence**: `E01_tds_view_picker_landing.png` — *"VIEW NAOISE Ó
  CEARÚIL'S RECORD →"* in black uppercase is the loudest typographic
  element on the page.
- **Recommendation**: covered by P1-2 if the button is dropped. If
  kept, switch to sentence case ("View Naoise Ó Cearúil's record →")
  and quieter button styling.

**[P2-5] Orphaned "on" word in TD picker cards**

- **Location**: `_td_pick_card_html`.
- **Evidence**: `E01_tds_view_picker_landing.png` — between the
  VOTED status badge and the bill title is a lone "on" floating on
  its own line.
- **Recommendation**: inline the "on" into the title sentence:
  *"Naoise Ó Cearúil voted NO on Organisation of Working Time…"* as
  one flowing sentence.

**[P2-6] Mode A todo_callout adds clutter to an otherwise-clean landing**

- **Location**: end of `_render_mode_a` (~line 467 in `votes.py`).
- **Evidence**: `A05_modeA_bottom_desktop.png` shows the citizen-facing
  "Source link quality" callout at the bottom — but it's below the
  fold and only applies to the external links above it.
- **Recommendation**: move the warning to where it's contextually
  relevant — inline next to the external-source links, or in the
  provenance expander. Don't render a static "some links may not
  work" notice for every visit.

**[P2-7] Invalid `?vote=` shows "for ID:"**

- **Location**: `_render_mode_c` empty branch (~line 517).
- **Evidence**: `G01_modeC_invalid_id.png` — *"No division on record
  for ID: nonexistent-vote-id."*
- **Recommendation**: drop the "ID:" preface (dev concept).
  *"No division on record for nonexistent-vote-id."* is sufficient
  and cleaner. Also: don't echo the literal user-supplied string into
  the message body when it's clearly malformed.

**[P2-8] `[Private Members]` suffix on bill titles is jargon**

- **Location**: vote card titles. Source: `debate_title` from
  `v_vote_member_detail`.
- **Evidence**: A02/A04/E01 all show titles ending with
  `[Private Members]`. The Glossary page may or may not explain this.
- **Recommendation**: render `[Private Members]` as a small dimmed
  pill *separate* from the title, so the title reads cleanly. Add a
  glossary entry: "Private Members' Bill — proposed by a TD or
  Senator who is not a government minister."

**[P2-9] Provenance is buried in a collapsed expander**

- **Location**: `_render_mode_a` end + `_render_mode_c` end. Both
  use `provenance_expander` from `ui/source_pdfs.py`.
- **Evidence**: `A05_modeA_bottom_desktop.png` — only the closed
  expander is visible. PRODUCT.md principle: civic accountability
  tool. Provenance should be prominent, not hidden.
- **Recommendation**: open by default, OR show the one-line summary
  ("Data sourced from the Oireachtas Open Data API") visibly with
  the expander as a "see details" affordance below.

---

### P3 — Polish

**[P3-1] PARTY BREAKDOWN sort order on Mode C is ambiguous**

- **Location**: `render_division_panel` in `ui/vote_explorer.py`.
- **Evidence**: `D01_modeC_division_detail_full.png` — parties listed
  Social Democrats / Sinn Féin / PBP / Labour / Indep Ireland /
  Independent / Green / Fine Gael / Fianna Fáil / Aontú / 100% RDR.
  Roughly inverse-government order? Not strictly alphabetical or by
  size.
- **Recommendation**: explicit sort — either by total vote count
  (largest bar first) or by gov/opposition (cohort grouping). Add a
  sort caption.

**[P3-2] Sidebar caption "Data covers 2016-01 to 2026-04" is low-prominence**

- **Location**: sidebar in `votes_page`.
- **Evidence**: `E01_tds_view_picker_landing.png` left rail.
- **Recommendation**: this is genuinely useful provenance — keep but
  bold the date range so it earns its place.

**[P3-3] Sidebar reflow between Dáil and TDs views is sudden**

- **Location**: votes_page view-toggle branch.
- **Evidence**: comparing `A01_modeA_full_desktop.png` (Outcome +
  Party selectboxes) vs `E01_tds_view_picker_landing.png` (Find a TD
  search + select).
- **Recommendation**: keep the View toggle stable position; the
  filter section below can change but a subtle transition (height
  animation) would soften the jump.

---

### Positive findings — preserve these

1. **Mode C (division evidence) is the strongest page in the app.**
   Number row + party-breakdown stacked bar + member-votes filter is
   exemplary data-as-design. Don't regress it.
2. **Empty states use `empty_state()` consistently** — vote not found
   on Mode A; division not found on Mode C. No silent grey rectangles.
3. **The Phase 7 cross-page contract works** — Mode B legacy redirects
   to `/member-overview?member=<id>#votes` correctly (just needs
   stylistic refresh per P1-6).
4. **TD picker editorial choice** (curated topical votes, not all
   TDs) is on-brand and citizen-friendly.
5. **Year pills are accessible and clear** — small caveat at P2-3 about
   total count but the pattern itself is good.

---

## Part 2 — Uplift prompt

Paste this prompt into a fresh Claude Code session when you're ready to
ship the fixes. Self-contained.

```
We're uplifting the Dáil Tracker Votes page (/rankings-votes) based on the
2026-05-26 impeccable audit at doc/VOTES_AUDIT.md. Read that file
first — it has the screenshots and findings.

Context to read first:
- doc/VOTES_AUDIT.md (this audit; especially Parts 1 + 3)
- audit_screenshots/_votes/*.png (22 screenshots; review at least
  A02, B01, C01, D01, E01, F01, G01)
- utility/pages_code/votes.py (the page)
- utility/ui/vote_explorer.py (shared render helpers used by both this
  page and member-overview's Votes expander)
- ui/components.py:member_moved_callout (the shared helper Mode B
  should adopt)
- The 6 recurring patterns memory:
  C:\Users\pglyn\.claude\projects\c--Users-pglyn-PycharmProjects-dail-extractor\memory\project_app_design_synthesis_2026_05_26.md

Scope: fix the 6 P1s and high-leverage P2s. Do NOT touch Mode C
(division evidence) beyond P3-1 (sort order) — it's the strongest page
in the app and should stay that way.

Priority order:

1. **P1-2 + P2-4 + P2-5 (TD picker consolidation)** — one click target
   per picker card; sentence case; inline the "on" word; drop the
   redundant "VIEW <NAME>'S RECORD →" button. Use clickable_card_link
   wrapping the whole card (mirrors Mode A vote cards).

2. **P1-1 (duplicate bill titles)** — at minimum add a visible stage
   pill (e.g. `Second Stage` / `Report Stage` / `Resumed`) extracted
   from the debate_title. Better: group sequential votes on the same
   bill into one card with N sub-divisions. The bill_id may not be in
   v_vote_index — verify and ask if a pipeline view change is needed.

3. **P1-6 (Mode B redirect to shared helper)** — migrate
   _render_mode_b_redirect to call member_moved_callout(name, section,
   legacy_param, state_keys). Look up the TD name from td_vote_summary
   so the link can say "Open Mary Lou McDonald's profile →".

4. **P1-5 (TD picker vote-vs-outcome badge clarity)** — either change
   the badge wording to "Voted NO" / "Voted YES" (drop the implicit
   outcome encoding) or add a separate small badge for the bill's
   outcome.

5. **P1-3 (filter breadcrumb)** — append outcome + party to the
   "147 divisions" count caption.

6. **P1-4 (mobile double-affordance)** — distinguish Oireachtas
   external link visually from the internal-nav arrow. Easiest: drop
   the external link from cards and surface it inside Mode C only.
   That also retires P2-2.

7. **Carry-overs from prior review (Appendix below)** that are still
   unaddressed:
   - `vote_explorer.py:186, 234` — plotly `use_container_width=True`
     → switch to `width="stretch"`
   - `votes.py:42-52, 236-252` — `_TD_PICKER_TOPICS` ILIKE filter is
     theme classification done in Streamlit; should become a pipeline-
     owned column on v_vote_index (logic-firewall violation per the
     contract).
   - `vote_explorer.py:351`, `components.py:178-179` — section
     headings as `<p>` not `<h2>/<h3>`; breaks heading-level a11y nav.

After each fix:
- Re-run audit_screenshots/_votes_capture.py
- Compare new screenshots against the originals in
  audit_screenshots/_votes/
- Update project_votes_audit_2026_05_26.md memory entry with what shipped

Don't:
- Re-do Mode C beyond a sort-order tweak (P3-1)
- Add new features outside the audit scope
- Touch lobbying_2.py / lobbying_3.py / statutory_instruments.py (other
  agents own those)
- Change the cross-page bridge (resolve_member_code) — that's settled

Save findings updates and verification screenshots in the same audit
directory so future audits can diff.
```

---

## Capture / verify scripts

- `audit_screenshots/_votes_capture.py` — full Playwright sweep, 22
  screenshots across 7 phases. Re-run after each fix.

Re-run after any change:
```
python audit_screenshots/_votes_capture.py
```

---

## Part 3 — Appendix: prior civic-ui-review

Earlier code/contract-compliance review (pre-Phase-7). Preserved here
for the findings that remain unaddressed. Some items are now obsolete
— flagged inline.

Reviewed `utility/pages_code/votes.py` and `utility/ui/vote_explorer.py`
against `dail_tracker_bold_ui_contract_pack_v5/utility/page_contracts/votes.yaml`.

### High-severity

#### 1. Deprecated `use_container_width=True` on plotly charts — Pass B
**Status: still unaddressed (2026-05-26).** Carried into uplift prompt.

`vote_explorer.py:186` and `vote_explorer.py:234` pass
`use_container_width=True` to `st.plotly_chart`. The skill and
project-wide convention is `width="stretch"`. These are the two charts
the votes page renders (party breakdown bar in Mode C, year stack in
Mode B), so the violation hits user-visible output directly.

#### 2. Topic classification performed in Streamlit — Pass A logic firewall
**Status: still unaddressed.** Carried into uplift prompt.

`votes.py:42-52` hardcodes `_TD_PICKER_TOPICS` (Housing, Health,
Disability, Climate, Energy, Palestine, Neutrality, Education,
Childcare) with ILIKE patterns, and `votes.py:236-252` executes them
as `WHERE ({likes})` against `v_vote_member_detail`. The inline comment
claims "presentation-only filter, not modelling" but this is theme
classification — the contract itself says
`theme column — theme classification is currently done via regex in
Streamlit; must become a pipeline-owned column on v_vote_index`
(votes.yaml:42-44). A `todo_callout` should be visible to users until
the pipeline view exists, and the topic labels should not be hardcoded
in the page file.

#### 3. Filtering on `debate_title` is not an approved filter — Pass A
**Status: still unaddressed.** Related to #2.

The contract's `approved_filters` for `v_vote_member_detail` are
`vote_id`, `member_id`, `member_name`, `vote_type`, `vote_date`.
`debate_title` is listed as an optional column but has no approved
operators. The `_fetch_topical_votes` query at `votes.py:241-250` uses
`debate_title ILIKE ?`, which exceeds the approved retrieval surface.

### Medium-severity

#### 4. Identity and section headings are styled `<p>` tags, not real headings — Pass D #5
**Status: still unaddressed.** Carried into uplift prompt.

`vote_explorer.py:351` renders the TD name as `<p class="td-name">`,
and `components.py:178-179` renders every section title as
`<p class="section-heading">`. Screen readers cannot navigate by
heading because there are no `<h2>`/`<h3>` elements between the page
`<h1>` and content. Breaks the heading-level nesting requirement.

#### 5. "Sponsored bills" placeholder rendered outside the bordered TD panel — Pass C
**Status: OBSOLETE — Phase 7 lifted Mode B (TD profile) to
/member-overview. The bordered-container issue no longer applies on
this page.** The same `render_td_panel` is now called from
member-overview with `show_header=False` (no bordered container).

#### 6. Helpers used by the votes page bypass `st.html` — Pass B
**Status: partial.** `components.py:179` (`evidence_heading`) and
`components.py:183-187` (`todo_callout`) historically used
`st.markdown(..., unsafe_allow_html=True)`. The round-3 P1-A fix
rewrote `todo_callout` to use `st.html`. `evidence_heading` still
uses `st.markdown`. Worth a follow-up sweep.

### Passed (in prior review)

- **Pass A** — backend untouched, no `read_parquet` / `parquet_scan`,
  no `CREATE VIEW` / `CREATE TABLE`, no GROUP BY-multi or
  HAVING/WINDOW, parameter binding consistent, hard-coded paths
  absent. `TODO_PIPELINE_VIEW_REQUIRED` is correctly raised for
  `td_sponsored_bills`, `td_vote_year_summary`, and the source URL
  provenance gap.
- **Pass B** — `st.html` used in the page itself, `width="stretch"`
  on the TD picker buttons, `st.segmented_control` for view toggle
  and member-list position filter, `html.escape` applied to every
  dynamic value, card backgrounds use `#ffffff` (no `var(--surface)`
  traps), no page-local CSS block.
- **Pass C** — material redesign vs old page (kicker + h1, year pills,
  division cards, two-stage TD flow with topical landing cards, Mode-C
  evidence panel with stat strip + party stack chart). Year pills used
  for primary navigation. Empty states are human and informative. Back
  buttons in main content area. Card-based primary index, no
  `st.dataframe` as primary control.
- **Pass D** — primary user question answered above the fold;
  drilldown obvious; provenance footer with Oireachtas attribution;
  "TD"/"TAA" not expanded but acceptable for a tracker-domain audience;
  reuses `dt-*`, `vt-*`, `td-*`, `td-pick-*` class families and
  `components.py` helpers; gov/opposition red/green pairing is
  mitigated by ✓/✗ glyphs and explicit "Voted Yes"/"Voted No"/
  "Carried"/"Lost" text on every pill, so deuteranopia distinction
  does not depend on hue alone.
