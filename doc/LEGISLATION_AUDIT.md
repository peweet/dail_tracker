# Legislation — Impeccable audit (2026-05-26)

> **Status update (2026-05-27):** P0 + all 5 P1s + all 5 P2s + P3-1
> shipped. See [[project-legislation-audit-2026-05-26]] memory for
> the rework diary, and `audit_screenshots/verify_legislation/V01..V06.png`
> for the verification shots. Score lifted 13/20 → ~17/20. Open: P1-1
> step 4 (Government/Private Member source-column segmented control —
> **blocked on `v_legislation_index_filtered` pipeline change**); P2-4
> (term-badge "(current)" suffix — **blocked, needs pipeline
> `current_term_number`**); P3-2 was "confirm only" (no code change),
> P3-3 (year-vs-introduction tooltip) skipped as low leverage.

Page audited: `/rankings-legislation` (`utility/pages_code/legislation.py`).
Methodology: 30 Playwright screenshots across desktop / tablet / mobile,
9 phases — index landing, phase selector (Dáil/Seanad/Enacted),
sidebar filters, pagination, bill detail, SI year/operation pills,
back button, empty state, mobile bill detail.
Capture script: `audit_screenshots/_legislation_capture.py`.
Screenshots: `audit_screenshots/_legislation/*.png`.

**Score: 13 / 20 — Good with one mobile-blocker.** The desktop
experience is strong (pipeline strip + segmented phase selector +
two-column bill detail with stage timeline + debates + SI nest). Mobile
overflows the pipeline strip, the page hero copy + "PIPELINE TODO"
callout contradict the data (Government Bills now ARE indexed but the
notice still says they aren't), and document labels render shouty
ALL CAPS. Once the mobile + Government-Bills + ALL-CAPS fixes land,
score lifts to 15+/20.

---

## Part 1 — Findings

### P0 — Blocker (1)

**P0-1 — Mobile pipeline strip overflows the viewport ("525 Enacted"
clipped off-screen).** Evidence: `A07_index_mobile.png`,
`A09_index_mobile_bottom.png`. The `.leg-pipeline-strip` flex layout
(`legislation.py:106-128`) keeps all three cards in a single row on a
390px-wide phone — `820 / 275 / 525` plus `→` separators. The third
card is cut off ("52 Enac…") and unreachable without horizontal scroll.
This is the page's hero stat strip — losing the Enacted count on mobile
removes the "where bills end up" anchor for citizens reading on a phone.

**Fix** — add a mobile media query in `shared_css.py` that switches
`.leg-pipeline-strip` to `flex-direction: column` (or `grid-template-
columns: 1fr` with vertical separators rendered as `↓`) below ~600px.

### P1 — High leverage (5)

**P1-1 — Hero + "Pipeline TODO" callout contradict the data.**
Evidence: `A02_index_above_fold.png`, `E02_bill_detail_above_fold.png`.
The hero dek says
> "Track where each **Private Members' Bill** stands in the legislative
> journey…"

and the callout below says
> "PIPELINE TODO  Government Bills are not yet indexed — the pipeline
> is currently scoped to Private Members' Bills only."

But the very first detail view (`E02_bill_detail_above_fold.png`)
shows "Microenterprise Loan Fund (Amendment) Bill 2024" sponsored by
the **Minister for Enterprise, Trade and Employment** — a Government
Bill. The code already knows this: `legislation.py:131-148` has a
`TODO_GOVT_BILLS` comment with a 4-step cleanup checklist that's never
been executed:

> 1. Remove this entire callout block.
> 2. Generalise hero copy at lines ~56-60 ("Private Members'" → "Bills").
> 3. Update the provenance text at lines ~207-212 to drop the
>    "Government Bills not yet included" caveat.
> 4. Add a Government / Private Member / All segmented control next to
>    the existing phase selector at lines ~127-134, and surface `source`
>    on the bill card meta strip at line ~174.

Citizens see two contradictory claims on the first surface they hit.
Execute the existing 4-step cleanup verbatim — it's a triple win
(removes wrong claim, opens dataset framing, lets users filter by
source).

**P1-2 — Document labels rendered ALL CAPS (`leg-source-label` styling).**
Evidence: `E04_bill_detail_documents.png`. Labels in the Documents
section are:
- "ADMINISTRATIVE REPRINT - AS AMENDED IN COMMITTEE [SEANAD ÉIREANN]"
- "AS INITIATED"
- "EXPLANATORY MEMORANDUM"
- "NUMBERED LIST [SEANAD]" (×2)

These labels carry real semantic content (which version of the bill
text, what kind of amendment list). At sentence case they'd be
readable in 1s; in all-caps letterspaced kicker style they take 2-3s
and feel shouty. The `leg-source-label` CSS class (or wherever the
uppercase transform lives in `shared_css.py`) should be sentence-case
body text — preserve the underlying API casing.

**P1-3 — Mobile bill-detail stat strip overflows.** Evidence:
`I01_mobile_bill_detail.png`. The 4-column `render_stat_strip` shows
"7 May 2026 / Introduced" + "Minister for Enterprise, Trade and
Employment / Sponsor" + "Report Stage / Current Stage" + cuts off the
"Method" stat entirely (off the right edge). Mobile loses one of the
four primary facts about the bill.

**Fix** — `render_stat_strip` should wrap to 2×2 grid below ~600px.
Likely needs a media query update in `shared_css.py` on whatever class
the stat strip uses (probably `.stat-strip` or similar).

**P1-4 — Sponsor "—" rendering noisy on 525 Enacted bills.** Evidence:
`B03_phase_enacted_cards.png`. Every Enacted bill card shows:
> "— · Enacted · Oireachtas ↗"

Leading em-dash adds visual noise without conveying anything. The
sponsor field is genuinely NULL in the API for older enacted bills.
Two options:

1. Drop the `{sponsor} · ` prefix when `sponsor in {None, "", "—"}`,
   leaving just `Enacted · Oireachtas ↗`.
2. Replace with `Sponsor unknown` once and hide the em-dash.

Option 1 is cleaner. Logic lives in `legislation.py:192, 209`.

**P1-5 — `PIPELINE TODO` kicker uses dev-jargon directly.** Evidence:
`A02_index_above_fold.png`. The custom `.leg-todo-callout` (not the
shared `todo_callout()` helper) renders the kicker as literal
"PIPELINE TODO" — a developer-facing label citizens see verbatim. Even
after P1-1 lands and this callout is removed, the *pattern* should
never exist on a citizen surface. Audit `shared_css.py` and
`legislation.py:141-148` to delete this custom callout entirely (the
shared `todo_callout()` already strips dev jargon).

### P2 — Polish (5)

**P2-1 — Inline `style=""` attributes across detail view.** Evidence:
code at `legislation.py:500-503` (EU badge), `:517-527` (SI cards),
`:526-527` (SI meta line), `:557-558` (pre-2014 act long-title).
CLAUDE.md forbids inline `style=""`. Move to named classes in
`shared_css.py` (matches Interests Part 3 M1 pattern).

**P2-2 — EU badge inline-color instead of `--signal-eu-*` token.**
Evidence: `legislation.py:500-503` uses literal `#fef3c7` /
`#fcd34d` / `#92400e`. The `signal-eu` class introduced in round 3
(see [[project_committees_audit_2026_05_26]] and the round-3 design
synthesis) already encapsulates this — swap the inline style for
`<span class="signal signal-eu">EU</span>`.

**P2-3 — Section headings rendered as `<p class="section-heading">`
not `<h2>`.** Evidence: `legislation.py:93, 175, 363, 423`. Same a11y
violation flagged on Interests Part 3 H4 — screen readers can't
navigate by heading level. **Cross-page fix**: change
`ui/components.py:evidence_heading` and the literal
`section-heading` `<p>` calls to `<h2>` (style stays via class).

**P2-4 — "27TH SEANAD" / "33RD DÁIL" badges lack context.** Evidence:
`E02_bill_detail_above_fold.png` shows the third badge as "27TH SEANAD".
Citizens don't know what numbered Seanad they're looking at or whether
27th = current. The bill identity strip should either:
- Drop the term-number badge entirely (the chamber is already implied
  by the stage timeline groups).
- Append "(current)" when it matches `current_term` from the API.

**P2-5 — `Bills · Oireachtas · Dáil Tracker` kicker has 3 separators.**
Evidence: `A02_index_above_fold.png`. Civic-data convention elsewhere
on this app is 2 segments. Drop `· Dáil Tracker` (the brand strip
already establishes that context up at the site banner).

### P3 — Low-priority (3)

**P3-1 — Long bill titles wrap to 2-3 lines on cards.** Evidence:
`D02_page_2.png` — "Forty-first Amendment of the Constitution (Right
to Housing) Bill 2026" runs to 2 lines. Acceptable, but could
`line-clamp: 2` with full title on hover.

**P3-2 — Card focus outline persists across captures** (orange border
on Defamation (Amendment) Bill 2024 in `B03_phase_enacted_cards.png`).
Likely `:focus-visible` from keyboard nav (good a11y). Confirm
`:focus` (mouse) is suppressed via `:focus-visible` specificity.

**P3-3 — Bill year vs introduction year mismatch may confuse.**
Evidence: `A04_index_cards.png` — "Microenterprise Loan Fund
(Amendment) Bill **2024**" has introduced date **7 May 2026**. The
bill carries year 2024 in its name because of Oireachtas bill
numbering conventions but was introduced in 2026. Not actionable
(real data), but a tooltip explaining "Bill 18 of 2024 — when the bill
was first numbered" could pre-empt the confusion.

---

## Part 2 — Uplift prompt (self-contained)

> You are uplifting the Legislation page
> (`utility/pages_code/legislation.py`) after a Playwright audit.
> The full audit is in `doc/LEGISLATION_AUDIT.md`; do not regress
> anything in the "Positive findings" section.
>
> **Goal** — close 1 P0 + 5 P1 + 5 P2 in priority order.
>
> **Workflow**:
> 1. Open `legislation.py`, `shared_css.py`, `ui/components.py`.
> 2. For each finding, write the before/after (file, line, exact
>    replacement) before editing.
> 3. After all edits, re-run the capture:
>    ```
>    $env:PYTHONIOENCODING = "utf-8"
>    python audit_screenshots/_legislation_capture.py
>    ```
>    Review diff in `audit_screenshots/_legislation/`.
> 4. Update `project_legislation_audit_2026_05_26.md` in memory:
>    tick each finding off as "verified shipping" with the screenshot
>    citation.
>
> **Findings to close** (priority order):
>
> 1. **P0-1 — Mobile pipeline strip overflow.** In `shared_css.py`,
>    add a `@media (max-width: 600px)` rule that switches
>    `.leg-pipeline-strip` to `flex-direction: column` (and the
>    `.leg-pipeline-sep` from `→` to `↓` via `content: "↓"` if it's a
>    pseudo-element, otherwise just rotate 90°). Verify on
>    `A07_index_mobile.png` recapture — all three stat cards visible
>    in one column.
>
> 2. **P1-1 — Execute the existing `TODO_GOVT_BILLS` cleanup verbatim.**
>    `legislation.py:131-148` has the 4-step list:
>    - Step 1: delete the `<div class="leg-todo-callout">…</div>`
>      block (lines 141-148).
>    - Step 2: rewrite hero dek (`legislation.py:77-81`) from
>      "Track where each Private Members' Bill stands…" to
>      "Track where each Bill stands in the legislative journey — from
>      First Reading in the Dáil through the Seanad to Presidential
>      signature."
>    - Step 3: in provenance text (`:251-255`), drop
>      "Private Members' Bills introduced to the Dáil. Government Bills
>      not yet included." → "All Bills introduced to the Oireachtas
>      (Private Members' and Government Bills)."
>    - Step 4: add a Government / Private Member / All segmented
>      control next to the existing phase selector (`:158-167`), and
>      surface `source` on the bill card meta strip (`:209`). Keep
>      the new control to ONE row on desktop — wrap below the phase
>      selector if width is tight.
>
> 3. **P1-2 — Document labels not all-caps.** Find the
>    `.leg-source-label` rule in `shared_css.py` and remove the
>    `text-transform: uppercase` + `letter-spacing` declarations.
>    Verify on `E04_bill_detail_documents.png` recapture — labels
>    sentence-case.
>
> 4. **P1-3 — Mobile stat strip wraps to 2×2.** In `shared_css.py`,
>    add `@media (max-width: 600px)` rule for the stat-strip class:
>    `grid-template-columns: 1fr 1fr` (was `repeat(4, 1fr)`). Verify
>    via `I01_mobile_bill_detail.png` recapture — all 4 stats visible
>    in a 2×2 grid.
>
> 5. **P1-4 — Drop sponsor em-dash on Enacted cards.** In
>    `legislation.py:192, 209`, change:
>    ```python
>    sponsor = row.get("sponsor", "—") or "—"
>    # → meta line uses {sponsor} · {stage}
>    ```
>    to:
>    ```python
>    sponsor = row.get("sponsor", "") or ""
>    meta = f"{sponsor} · {stage}" if sponsor else stage
>    ```
>    and use `meta` in the template. Verify via
>    `B03_phase_enacted_cards.png` recapture — no leading em-dashes.
>
> 6. **P1-5 — Delete `.leg-todo-callout`.** Already resolved by
>    P1-1's Step 1, but also remove the `.leg-todo-callout` /
>    `.leg-todo-label` CSS rules from `shared_css.py` so the pattern
>    can't be reintroduced. If a callout-style block is needed in
>    future, the shared `todo_callout()` helper is the canonical
>    path.
>
> 7. **P2-1 — Inline `style=""` extraction.** Add `.leg-pre2014-long-
>    title`, `.leg-si-card`, `.leg-si-meta` classes in
>    `shared_css.py`; replace inline styles in `legislation.py:500-503,
>    517-527, 526-527, 557-558`.
>
> 8. **P2-2 — Swap EU inline-color for `signal-eu` class.**
>    `legislation.py:500-503` →
>    `eu_badge = '<span class="signal signal-eu">EU</span>' if bool(row.get("si_is_eu")) else ""`.
>
> 9. **P2-3 — Section headings as `<h2>`.** Cross-page change:
>    `ui/components.py:evidence_heading` returns `<h2>` not `<p>`;
>    update the literal `<p class="section-heading">` calls in
>    `legislation.py:93, 175, 363, 423` to `<h2 class="section-heading">`.
>    Same fix lifts Interests Part 3 H4.
>
> 10. **P2-4 — Seanad/Dáil term badge context.** In
>     `legislation.py:599-606`, only render the `current_house` badge
>     when it's the current term. If pipeline supplies a
>     `current_term_number`, append "(current)" when match.
>
> 11. **P2-5 — Drop `· Dáil Tracker` kicker segment.** In
>     `legislation.py:75`, change
>     `kicker="Bills · Oireachtas · Dáil Tracker"` →
>     `kicker="Bills · Oireachtas"`.
>
> **Out of scope** (do NOT regress):
> - Pipeline strip on desktop — clear flow visualization, do not change
>   layout.
> - Phase selector with counts in labels — exemplary.
> - URL-driven bill detail via `?bill=<bill_id>` + `bill_detail_url()`
>   helper.
> - Two-column desktop detail (timeline | debates).
> - Stage timeline grouped by chamber.
> - Pre-2014 Act fallback view — handles synthetic IDs gracefully.
> - SI empty-state copy — already citizen-friendly.
> - Long-title `<p>` tag strip in `_render_bill_detail` (round-3 P1-B).
> - Provenance expander at bottom.

---

## Part 3 — Positive findings (DO NOT REGRESS)

1. **Pipeline strip (820 / 275 / 525) on desktop** — clear flow
   visualization with → separators. Best stat strip in the app for a
   process visualization. Just needs the mobile-stack fix (P0-1).
2. **Phase selector segmented control with counts in labels**
   ("Dáil Stages (820)", "Seanad Stages (275)", "Enacted (525)") —
   exemplary; citizens see counts before clicking.
3. **Bill cards as `clickable_card_link` with `href=?bill=X`** —
   URL-driven detail; back button + reload preserve state.
4. **Two-column desktop detail (timeline | debates)** — efficient
   use of width; matches civic-data convention of evidence-first.
5. **Stage timeline grouped by Dáil/Seanad/Presidential** — accurate
   Oireachtas legislative procedure semantics
   (`legislation.py:272-276`).
6. **SI section nested inside bill detail** — surfaces the secondary
   legislation under each Act with year + operation pill filters.
7. **Long-title `<p>` tag strip (round-3 P1-B fix)** — verified
   shipping on `E01_bill_detail_full.png`; description renders as
   clean prose.
8. **Pre-2014 Act fallback view** — handles synthetic
   `act_<year>_<slug>` IDs gracefully with a minimal hero + SI section.
9. **SI empty-state copy** — "Either none have been issued yet, this
   Bill predates the SI data window (2018), or it never became an
   Act" — citizen-friendly three-fold explanation in
   `E05_bill_detail_sis.png`.
10. **Provenance expander at bottom** — civic-editorial framing on
    bill detail and index landing.
11. **Back button** clears `leg_selected_bill_id` AND
    `st.query_params` (`legislation.py:573-576`) — proper state
    cleanup, no leaking URL state.

---

Re-run the Playwright capture after any change:
```
$env:PYTHONIOENCODING = "utf-8"
python audit_screenshots/_legislation_capture.py
```
Writes to `audit_screenshots/_legislation/`; assumes Streamlit running
on `localhost:8501`.
