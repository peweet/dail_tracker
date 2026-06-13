# Money Pages — Shape Brief (design only, no code)

**Created:** 2026-06-13 · **Skill:** `shape` · **Status:** brief for review.
**Scope:** NOT a ground-up redesign (app is 15/20, identity strong). Three bounded moves:
**(A)** a thin **"Follow the money" hub**; **(B)** **Public Payments** declutter + the **Category lens**;
**(C)** **Procurement** declutter; **(D)** generalise the **Company dossier** → Department / Council.
**Constitution:** `PRODUCT.md` (editorial accountability journalism; **cards/data are the hero, charts
only where they answer a question**; no hero-metric strips; keep side-stripe / `#ffffff` / signal tokens).
**Data ceiling:** `doc/MONEY_FLOW_DATA_AUDIT.md` — never sum across tiers/registers; no "total public
spend"; coverage shown honestly. **Reuse:** election `_money_map`, company `_dossier`, `pr-*` cards,
`components.py`, the built `v_payments_by_category*` views. Tussell-modelled, explicitly not ProZorro.

---

## 1. User question each surface answers

- **Hub ("Follow the money"):** *"Where does Irish public money go — and which record do I look at?"*
  Orients a low-literacy citizen across the three money lanes without making them learn "awarded vs paid".
- **Public Payments:** *"What did public bodies actually pay or order, to whom, for what?"* (+ the new
  **for what** = Category lens.)
- **Procurement:** *"Who wins public contracts, from which bodies, how competitive?"* (AWARDED — unchanged scope.)
- **Department / Council dossier:** *"Where does THIS body's money go?"* (one body, across the registers it touches.)
- **Company dossier (exists):** *"What is this firm's whole public-money footprint?"*

## 2. Current UI problems (named, bounded — not identity)

- **S1 — hero-metric strips.** Public Payments **7-up** and Procurement **7-up** stat strips sit between
  hero and data (PRODUCT.md bans hero metrics; civic-ui-review "no stat strips above the data"). The single
  biggest tell and the mobile cramp.
- **S2 — control-row pile-up.** Procurement: hero → caveat → 7-stat → glossary → tabs → sort → search →
  *then* data (~6 rows before a contract). Public Payments nearly as deep.
- **Wayfinding.** Four money pages (Payments/Election/Procurement/Public Payments) + hidden Company, flat
  nav, no front door — a citizen can't tell *awarded* from *paid* or find "my council".
- **Component drift.** `_card`/`_value_pill`/`_supplier_href` copy-pasted across procurement/public_payments/
  company; election uses a separate `e24-*` family. Same idea, three implementations.
- **Category gap.** "For what" is not answerable today (views built, no UI).

## 3. Bold redesigned layout (section order)

### A. "Follow the money" hub (new, thin)
```
 [editorial masthead hero]  Follow the money
   dek: Three separate public records. We never add them together — they measure
        money at different stages.
 ── The three lanes (reuse election _money_map: stripe cards + "≠ never sum" rail) ──
   ▌Contracts awarded   €7.69bn sum-safe · 59k awards   "up to (ceilings)"   → Procurement
   ▌Money paid out      €14.0bn paid · €11.0bn ordered   "actually paid/ordered" → Public Payments
   ▌Money to politics   salaries · donations · election spend                 → Payments / Election
   ⚠ Awarded ≠ paid ≠ to-politics. Different stages of different registers — never one total.
 ── Start from what you know (4 entry cards) ──
   [ A company ]  [ A department/agency ]  [ A category of spend ]  [ My council ]
 ── Coverage, plainly ──  inline sentence: "What's in, what's not" (over-€20k, voluntary, 63 bodies)
 [provenance footer]
```
The hub is **routing + honesty**, no new aggregation: lane cards link to existing pages; entry cards link
to Company / Department dossier / Category lens / Council dossier.

### B. Public Payments (declutter + 3rd tab)
```
 [hero]  Public-Body Payments
   dek + ONE inline context sentence  (replaces the 7-up strip — S1)
   caveat: "ordered vs paid, never one figure" (kept, it's load-bearing)
   glossary strip (kept — civic term help)
 [ Public bodies ] [ Suppliers ] [ What the money buys ]●        ← NEW tab
   …existing tabs unchanged…
   NEW tab = coverage panel (honesty FIRST) → ranked CATEGORY CARDS (bar is a thin
             in-card track, NOT a chart hero) → click → category profile
 [provenance footer]
```
Category profile (`?category=`): tier-split context line → **top-vendor cards** (drill to Company dossier)
→ year trend (small, in-section) → source link per line → CSV → caveats (fragmentation, paid≠winnable).

### C. Procurement (declutter only — same surgery as B)
7-up strip → one inline context sentence; collapse the caveat+glossary+sort+search pile into the tab
headers / a single control row. No structural change to the award data itself.

### D. Dossier template (generalise `company._dossier`)
One reusable **entity dossier**: identity strip → **lanes this entity touches** (awarded / paid / — labelled,
never summed) → top categories (paid) → top counterparties → trend → source links. Instantiate for
**Company** (exists), **Department**, **Council** (add AFS budget context block for councils).

## 4. Interaction model

- **Primary view** = ranked **cards** (the data is the hero, PRODUCT.md §1), never a dataframe on a primary surface.
- **Drill** = query-param routes, the established pattern: `?category=`, `?publisher=`, `?supplier=` (→ Company),
  new `?dept=` / `?council=`. Every vendor in a category → the vendor's Company dossier (one hop, cross-register).
- **Hub = front door**, not a dead-end: every lane and entry card routes into an existing or cloned surface.
- **Nav:** group the flat routes under the signed-off **"The Money"** section; hub becomes that section's landing.

## 5. Temporal behaviour

- Rankings are **all-time** (as today); year is shown as **range caption** ("Covering 2012–2026"), not pills,
  because the payment corpus is sparse/uneven per body (year pills would imply complete annual coverage we lack).
- **Trend = small in-section bars by year, tier-split** (SPENT vs COMMITTED as separate series, never stacked
  into one) — on category & dossier profiles only, where "over time" is the question. Not on browse/landing.

## 6. Source-link behaviour

- **Provenance footer kept** on every money surface (PRODUCT.md: civic, accountable; not omitted).
- **Per-line source link** ("View published source" → the body's own over-€20k PDF) on every payment line and
  every category-vendor row — 100% URL coverage on the payment fact. This is the trust spine; keep it prominent.
- Hub carries a **plain "what's in / what's not" coverage sentence**, not buried in the footer.

## 7. Chart and table strategy (charts answer a question, never decorate — PRODUCT.md)

- **Category overview = ranked CARDS** with a thin in-card value track (reuse election `e24-track`/`e24-bar`),
  NOT a bar-chart hero. The card *is* the data; the track is a legibility aid.
- **Trend = `st.bar_chart` / small Altair**, tier-split, only on profiles. No treemap (ProZorro look + implies
  completeness). No pie. No KPI tiles.
- **Tables = secondary only** (CSV export / journalist drill), never a primary view.

## 8. Empty-state copy (every section — "every row tells a story", PRODUCT.md §4)

- Hub lane with no data: *"No [awarded/paid] records loaded — this is a source/pipeline gap, not zero spend."*
- Category browse empty: *"No categories match. 15% of lines carry no published purpose and show as 'Uncategorised'."*
- Category profile, one tier absent: *"No paid lines in this category — it appears only as purchase-order commitments."*
- Dept/Council with no payments: *"This body hasn't published over-€20k payment lists we could parse."* (link to what is known)
- Uncategorised bucket: shown, never hidden — *"Purpose not published by the body."*

## 9. Visual differences from the old pages

- **Hero metric strips gone** → one inline context sentence (the single biggest visible change).
- **Fewer rows before data** — controls collapse into the tab/one row; data appears ~2 rows sooner.
- **A front door** (hub) replaces "land mid-ledger and guess".
- **Consistent card** — one `pr-*` family across all money surfaces (election `e24-*` reconciled in).
- **Category lens** — the new "for what" answer, in the bodies' own words, with coverage shown.
- Identity untouched: side-stripe cards, `#ffffff` paper, signal tokens, editorial masthead all preserved.

## 10. TODO_PIPELINE_VIEW_REQUIRED (data the UI needs but doesn't have yet)

- ➕ `v_payments_category_by_year`, `v_payments_supplier_categories`, `v_payments_supplier_by_year`,
  `v_payments_publisher_by_year` — for trends + the dossier category/trend blocks (spec'd, NOT built).
- ➕ Department/Council **dossier rollup views** (payments + AFS budget context) — the dossier template needs them.
- ⚠ **`dim_supplier` (operator-merge)** — until built, vendor totals stay per-published-name (Mosney ≠ Mosney
  Holidays). UI must show the fragmentation caveat; **not a blocker**, but per-entity totals improve after it.
- ⚠ **Over-quarantine fix** (better supplier classification) — recognizable firms (De La Rue, An Post,
  BearingPoint) are display-hidden; **data-side**, affects displayable totals. UI shows "X withheld" honestly.
- The Category lens itself is **registered + tested** (`v_payments_by_category*`) — ready.

## 11. Implementation plan (files · CSS · helpers — for the build phase, not now)

**New files**
- `utility/pages_code/money_hub.py` — the hub (routing + lanes + entry cards + coverage). Reuse
  `hero_banner`, `finding_lede`, `glossary_strip`, `clickable_card_link`; lanes from a shared `_money_map`.
- `utility/pages_code/dept_dossier.py` + `council_dossier.py` — OR one parametrised `entity_dossier.py`
  cloned from `company._dossier`.
- `sql_views/procurement/procurement_payments_by_category.sql` — append the 4 trend/category views (§10).

**Edited files**
- `utility/app.py` — group nav under "The Money"; register hub + dossiers (slugs unchanged for existing).
- `utility/pages_code/public_payments.py` — drop `_stats_strip` call → inline sentence; add 3rd tab +
  `?category=` route + `_render_categories`/`_render_category_profile`/`_render_trend`/`_render_coverage_panel`.
- `utility/pages_code/procurement.py` — drop the 7-stat strip → inline sentence; collapse control rows.
- `utility/pages_code/company.py` — add the "what they were paid for" block (shared with dossier template).
- `utility/data_access/public_payments_data.py` — register the category view file; add thin `fetch_*` wrappers.

**CSS (all in `utility/shared_css.py`)**
- Promote `e24-track` / `e24-bar` (in-card value track) into the shared `pr-*` family → `pr-track` / `pr-bar`
  (so categories, dossiers, election all use ONE track). Add `pr-lane` (hub three-lane stripe card, from `e24-tier`).
- No new hardcoded hex — use `var(--*)` / signal tokens. No ad-hoc `<style>`.

**Helpers (consolidate the drift — `utility/ui/components.py` or a new `ui/money.py`)**
- Lift the duplicated `_card`, `_value_pill`, `_supplier_href`, `_sort_toggle`, `_eur` out of procurement /
  public_payments / company into ONE shared money-card module. This is the "component drift" fix; do it as
  part of the build so the new surfaces don't add a 4th copy.

**Build sequencing** (smallest-risk first, each ships alone)
1. Declutter (S1) on both money pages — pure deletion, instant win, zero data risk.
2. Consolidate the duplicated card helpers into one module (invisible, enables the rest).
3. Build the 4 views + tests (backbone).
4. Category lens (tab + profile) on Public Payments.
5. The hub (routing only — reuses everything above).
6. Dossier template → Department / Council.
7. `civic-ui-review` + impeccable-craft pass on a FRESH server.

**Out of scope (explicit):** ProZorro treemap / "total spend" headline / "follow every euro"; SME-share metric;
operator-merge in UI; any change to the award (Procurement) data model; stripping PRODUCT.md signatures.
