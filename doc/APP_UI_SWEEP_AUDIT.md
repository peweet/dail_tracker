# Dáil Tracker — full-app UI sweep (impeccable audit)

**Date:** 2026-06-06 · **Scope:** all 14 routes + 4 mobile breakpoints
**Method:** `impeccable audit` 5-dimension technical scan, every route rendered live and
screenshotted (`audit_screenshots/_sweep_*.png`, script `_sweep_capture.py`). Brand context
from `PRODUCT.md` (editorial accountability journalism; light; ink-on-paper; side-stripe and
`#ffffff` cards are *documented intentional signatures*, not anti-patterns here).

## Audit Health Score

| # | Dimension | Score | Key finding |
|---|-----------|-------|-------------|
| 1 | Accessibility | 3/4 | Heading nesting correct, colour-safe + text-labelled; minor grey-on-beige contrast + a dead sidebar chevron on 2 pages |
| 2 | Performance | 3/4 | ~122KB stylesheet injected every render (documented debt); no layout-prop animation |
| 3 | Responsive | 3/4 | No horizontal overflow (Streamlit constrains); multi-stat strips cramp on mobile, header subtitle clips |
| 4 | Theming | 3/4 | Solid core token system; page-local CSS blocks use hardcoded hex |
| 5 | Anti-patterns | 3/4 | Distinctive editorial identity, NOT AI slop; recurring multi-stat top strips are the one borderline tell |
| **Total** | | **15/20** | **Good — address the weak dimensions** |

## Anti-patterns verdict: PASS (does NOT look AI-generated)
Distinctive editorial register: serif display headings on warm ink-on-paper neutrals, one sharp
accent, data as the hero. No gradient text, no glassmorphism, no generic SaaS-cream, no bounce
motion. The side-stripe and `#ffffff` cards are PRODUCT-documented signatures. The single tell to
watch is the **multi-stat top strip** that opens 6 pages (big number + label + supporting stats) —
it edges toward the banned hero-metric template, but each strip carries real summary data, so it
reads as borderline rather than slop.

---

## Findings by severity

### P1 — fix before release
1. **`hide_sidebar()` missing on Appointments + Corporate Notices.** *(public_appointments.py,
   corporate.py — Anti-pattern / Responsive / consistency.)* These are the only 2 of 14 pages that
   don't call it, so Streamlit's collapsed-sidebar expand chevron (`»`) shows and the masthead
   shifts (brand re-centres) — a dead control and a visible inconsistency. **Fix:** add
   `hide_sidebar()` at the top of both, as every other page does. *(→ `adapt`)*

### P2 — next pass
2. **Multi-stat strips cramp on mobile.** Procurement opens with ~7 stats in a row, SI with 5,
   Corporate with 4; at 390px these squeeze to ~50px columns. No overflow, but unreadable.
   **Fix:** wrap/stack the stat row below a breakpoint. *(Responsive → `adapt`)*
3. **The multi-stat top strip is a systemic borderline tell** (SI, Legislation, Corporate,
   Procurement, Payments, Committees). Consider demoting the densest ones (Procurement's 7-up) to
   an inline sentence or a 2-row grid. *(Anti-pattern → `quieter` / `distill`)*
4. **Page-local CSS uses hardcoded hex, not tokens** (`jd-*` and peers: `#14232b`, `#5b6b73`,
   `#e4e9ec` …). Drift risk against the `var(--*)` core. Tracked as "CSS architecture split" debt in
   PRODUCT.md. *(Theming → `extract`)*
5. **Truncated labels.** Votes stage pills ("Second Stage (…", "Committee an…"), Appointments
   adviser portfolios ("Minister of State at the Depart…"). **Fix:** wider labels or hover titles.
   *(Clarity → `clarify`)*
6. **Legislation bill cards are half-empty.** Wide full-width cards with content hard-left and the
   `→` arrow far-right leave a large dead middle. **Fix:** use the space (sponsor / amendment count)
   or narrow the card column. *(Layout → `layout`)*
7. **Global header subtitle clips on mobile** ("…searchable" cut). Cross-page. **Fix:** hide the
   subtitle below a breakpoint or allow it to wrap. *(Responsive → `adapt`)*

### P3 — polish
8. **En-dashes in body copy** (Interests "…pipeline view lands – showing…", Legislation dek
   "journey – from First Reading"). House rule prefers comma/colon/period. *(Copy → `clarify`)*
9. **Mojibake in Legal-Diary source labels** ("Master�s Court", "Full hearing � cases") — a
   pipeline decoding gap (judiciary extractor), visible in the UI. *(→ data fix, not UI)*
10. **Interests pill legend** uses blue/orange/green/purple — all text-labelled, but green+orange
    is a deuteranopia-adjacent pair worth a quick verify. *(A11y)*

---

## Systemic patterns
- **Multi-stat top strip** on 6 pages — consistent, but the one design tell + the mobile-cramp
  source. Worth a deliberate decision: keep as a house pattern (then make it responsive) or thin it.
- **`hide_sidebar()` coverage** — a per-page opt-in that 2 pages missed; a page-template/base
  function would make it impossible to forget.
- **Hardcoded hex in page-local CSS** — recurs wherever a page injects its own `<style>` block.
- **Header subtitle mobile clip** — one global masthead, one global fix.

## Positive findings (keep / replicate)
- **Distinctive, trustworthy identity** — passes the AI-slop test outright. Editorial, civic, not a
  dashboard.
- **Strongest pages:** Member Overview (clean directory), Lobbying register (quiet prose hero +
  progressive "where to investigate" entry points), Appointments (adviser-by-portfolio bars +
  hiring histogram + **CSV export** for the journalist audience), Committees (party-composition
  stacked bars), Judiciary Courts tab (clearance bars + break-even trend).
- **Colour is deuteranopia-safe and always text-labelled** (blue/amber, never red/green).
- **Charts answer real questions**, they are not decoration — exactly the PRODUCT principle.
- **Caveats shown, not hidden** (Procurement "Awarded value, not actual spend"; SI/Legislation
  coverage notes) — the no-inference civic frame holds.

## Recommended order
1. **P1** `adapt` — add `hide_sidebar()` to Appointments + Corporate (trivial, removes the only
   clear bug).
2. **P2** `adapt` — make the multi-stat strips wrap on mobile; fix the header subtitle clip.
3. **P2** `layout` / `clarify` — fill the Legislation bill cards; de-truncate Votes/Appointments labels.
4. **P2** `quieter` — decide the fate of the densest stat strips (Procurement).
5. `impeccable polish` once fixes land; re-run `impeccable audit` to re-score.

> Run these one at a time or in a batch. Re-run the audit after fixes to watch the score move.
