# Phase 0 — Full-app review, clutter ledger & IA proposal

**Date:** 2026-06-10 · **Method:** fresh Streamlit on :8534, Playwright sweep of all 16 visible
routes (desktop 1440 + tablet 768 + mobile 390), reviewed against PRODUCT.md + civic-ui-review +
impeccable lenses. Captures: `audit_screenshots/_rd_*.png`. **Status: awaiting sign-off — no edits made.**

This builds on `doc/APP_UI_SWEEP_AUDIT.md` (15/20). It does not re-litigate that audit's
"NOT A DEFECT" calls (em-dashes, non-overflowing strips).

---

## 1. The core problem, named

The app's identity is strong and consistent (editorial masthead, side-stripe hero band, glossary
strip, `#ffffff` cards, deuteranopia-safe colour). The clutter is **not** the look — it is **too
many stacked control/summary rows before the data**, and **a flat 16-item top nav** that scatters
related pages. Three systemic culprits:

### S1 — The multi-stat top strip (6+ pages)
A horizontal block of summary numbers sits between the hero and the data on: Payments (2-up),
Election (3-up, coloured top-borders), Legislation (3-up), Corporate (4-up), Committees (4-up),
Statutory Instruments (5-up, **mixes a number + a text value "Department of Finance" + a %**),
Procurement (**7-up**), Public Payments (**7-up**). impeccable flags this as the banned
"hero-metric template"; civic-ui-review Pass D§4 says "no hero stat strips above the data." It is
the app's single biggest design tell and the source of the mobile cramp.
**Decision needed:** one house pattern for all of them (recommend: demote to a single inline
sentence of context, or a quiet 1-line stat ribbon, never >4 items, never mixed types).

### S2 — Control-row pile-up (Procurement, Public Payments, SI, Legislation, Votes)
Worst offender **Procurement**: hero → caveat callout → 7-stat strip → "What these terms mean"
expander → "How complete is this data?" tabbed expander → year pills (2013–2026) → category
toggle → "most awards / highest value" sort toggle → *then* the supplier cards. ~6 control rows
before content. SI stacks search + year pills + a full wall of ~17 department chips. Legislation
stacks number-range + status + search + a wall of stage chips (First/Second/Committee/Report/Final
× Dáil+Seanad). **Decision needed:** collapse secondary controls into a single filter bar /
"Filters" disclosure; keep only the primary control (usually year pills) in the open.

### S3 — Flat 16-item nav, money pages scattered
Top nav renders **12 items + "4 more"**. The four money pages (Payments & Donations, Election,
Procurement, Public Payments) are spread across positions 5, 6, 12, and the overflow — a citizen
can't see "the money section." Same for the law/records pages (Legislation, SI, Corporate,
Judiciary). **Decision needed:** group into ~4 labelled sections (proposal in §3).

---

## 2. Per-page clutter ledger

Severity = redesign priority. ✅ = keep as a reference pattern, do not disturb.

| # | Page | Opening pattern | Rows before data | Verdict |
|---|------|-----------------|------------------|---------|
| 1 | Member Overview | hero + glossary strip + search + party pills | 3 | ✅ **Reference.** Minor: glossary strip slightly redundant with dek. |
| 2 | Attendance | hero + "Notable members" pill row + year pills | 3 | ✅ Strong split-column (high/low). Trim the "Notable members" pill row. |
| 3 | Votes | 2 toggles + 2 dropdowns + year pills | 4 | **P1.** 4 control rows; stage-pill truncation; consolidate into one filter bar. |
| 4 | Interests | hero + toggle + search + "Notable" pills + year pills + legend | 5 | **P1.** Most-politically-potent dataset, buried under 5 rows. Lift it up. |
| 5 | Payments & Donations | hero + glossary strip + year pills + 2-up stat | 4 | **P1.** Stat strip (S1); otherwise good ranked cards. |
| 6 | Election (label drift: "Election 2024" vs code "Election Spending") | hero + caveat + 3-up coloured stat | 3 | **P1.** Coloured-border stat trio = hero-metric tell (S1). |
| 7 | Lobbying | quiet prose hero + 3 entry cards + 3 topic cards | 0 stat strips | ✅ **Gold standard.** Progressive entry, no strip. Template for others. |
| 8 | Legislation | hero + 3 filters + 3-up stat + stage-chip wall | 4–5 | **P1.** S1 + S2; bill cards half-empty (dead middle). |
| 9 | Statutory Instruments | hero + 5-up mixed stat + search + year pills + dept-chip wall | 4 | **P0.** Mixed-type stat strip (worst S1); dept-chip wall (S2). |
| 10 | Appointments | hero + bars + histogram + search + year pills + filter chips | 4 | ✅ Good (charts answer real Qs, CSV export). Tighten control rows. |
| 11 | Corporate Notices | hero + 4-up stat + 2 charts + table | 3 | **P2.** S1; otherwise charts are purposeful. |
| 12 | Procurement | hero + caveat + **7-up stat** + 2 expanders + year + category + sort | **~6** | **P0.** Worst clutter in the app (S1 + S2). |
| 13 | Public Payments | hero + caveat + **7-up stat** + tabs + sort | ~4 | **P0.** Second-worst (S1 + S2). |
| 14 | Committees | hero + toggle + search + type + active toggle + typeahead + 4-up stat | 5 | **P1.** S1 + S2; party-composition bars ✅ keep. |
| 15 | Courts & Judiciary | hero + glossary strip + 4 tabs + court pills | 2 | ✅ **Reference.** Clean tabbed layout. |
| 16 | Glossary | (cold-load skeleton + dev toolbar in capture — re-verify) | — | **P2.** Confirm it hides toolbar + uses masthead like every other page. |

---

## 3. IA restructure proposal (preserve all `url_path` slugs + `entity_links.PAGES`)

Group the flat 16 into 4 labelled sections + a utility item. Slugs are unchanged — this is
presentational grouping via `st.navigation({section: [pages]})`. Order within groups puts the
citizen-first page first.

| Section | Pages (slug unchanged) |
|---|---|
| **Members & Parliament** | Member Overview · Attendance · Votes · Interests · Committees |
| **The Money** | Payments & Donations · Election Spending · Procurement · Public Payments |
| **Law & Records** | Legislation · Statutory Instruments · Corporate Notices · Courts & Judiciary |
| **Influence** | Lobbying · Appointments |
| _(utility)_ | Glossary |

Open question for sign-off: Streamlit's `position="top"` renders dict-grouped nav as delineated
groups; if the top widget's grouping proves too cramped at 16 items, the fallback is to keep a flat
bar but **reorder** so the 4 money pages are adjacent and the 4 law pages are adjacent (no grouping
chrome). Either way the scatter is fixed.

---

## 4. Recommended execution plan (Phases 1–3)

**Phase 1 — systemic, do once (highest leverage):**
1. **S1:** design ONE house stat treatment in `shared_css.py` + a `components.py` helper; replace
   all 8 strips. (impeccable `quieter`/`distill`; civic-ui-review gate.)
2. **S2:** a shared "filter bar / Filters disclosure" pattern; apply to Procurement, Public
   Payments, SI, Legislation, Votes, Committees.
3. **S3:** nav regrouping in `app.py` (+ verify `entity_links.PAGES`).
4. Type-scale collapse to the 6-step scale (PRODUCT.md deferred debt), page-by-page visual check.
5. Clear remaining open audit items (Legislation half-empty cards, label de-truncation, mobile
   subtitle clip, glossary toolbar, Election label drift).

**Phase 2 — per-page bold pass, priority order:**
P0: Procurement, Public Payments, Statutory Instruments →
P1: Interests, Legislation, Votes, Committees, Payments, Election →
P2: Corporate, Glossary; ✅ leave Member Overview, Attendance, Lobbying, Appointments, Judiciary as
references (light touch only).
Routing per `doc/APP_REDESIGN_BRIEF.md`: contract-backed pages via `bold-redesign-page`; the rest
(Procurement, Public Payments, SI, Election, Corporate, Appointments, Judiciary, Glossary) via
`shape` + `streamlit-frontend` + `civic-ui-review`.

**Phase 3 — re-sweep + re-score on a fresh server; run CI; update APP_UI_SWEEP_AUDIT.md.**
