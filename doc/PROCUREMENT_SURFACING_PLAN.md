# Procurement surfacing plan — QS / procurement-intelligence angle (2026-06-11)

Complements `COMMERCIAL_UPLIFT_PLAN.md` (which is API/export/MCP-only and rules the app UI
out of scope). This doc is the **app-side surfacing** plan: rich procurement data that exists
but is buried, viewed through a quantity-surveyor / procurement-professional lens, benchmarked
against paid products. UI is our own civic-editorial system — we mirror *functionality/data*
the paid tools surface, never their designs.

## 1. Diagnosis — rich data, poor surfacing

The data is strong; discovery and depth are the problem. After the 2026-06-10 sweep restructured
procurement into "two questions":
- **TED is buried 2–3 clicks deep** — Tab "Who wins contracts?" → a *Register* picker that
  defaults to eTenders → switch to "EU register (TED)". TED awards (current to ~2026-06) and the
  pre-award tender pipeline (with buyer estimates) are invisible by default.
- **"Flat / no detail"** — the browse level shows only **supplier rollup cards** (counts:
  "Deloitte — 329 awarded, 52 authorities"). Individual award detail (titles, values, dates) only
  appears on supplier drill-down.
- **Award € values mostly absent** in eTenders (below-threshold + framework ceilings), so the page
  honestly leads with award *counts* — which reads as flat if you came for values. The real € lives
  in TED awards (above-threshold), TED tender *estimates* (buyer pre-award), and the payments fact.
- **Year filter works but is buried** (only on the eTenders-supplier lens; passes `year=` to the
  view) — perceived as "not filterable / jumbled" because of the tab→register→lens nesting.
- **The QS valuation is MCP-only** — `C:\tmp\dail_mcp\qs_valuation.py` (`project_value_estimate`
  tool) estimates an indicative build value from a deliverable; it's inference, kept out of the app
  by the no-inference rule. **Now greenlit as an app test feature** (see §4).

## 2. What paid products surface (functionality to mirror, not copy)

- **Tussell** — framework finder, buyer spend profiles, supplier profiles, soon-to-expire
  contracts, opportunity/bid-no-bid analyser, transparency/diversity benchmarking. ([gov](https://www.tussell.com/gov), [insight](https://www.tussell.com/products/tussell-insight))
- **Stotles** — expiring-contract signals 12–24mo out, early buying signals (budgets, FOI patterns,
  meeting minutes), buyer context "what they spent, who holds their contracts, when they expire". ([platform](https://www.stotles.com/platform/build-pipeline))
- **Glenigan / Barbour ABI** — construction project pipeline leads tracked planning→tender→award→
  subcontract, competitor activity, capacity, regional/sector pipeline. ([Barbour ABI](https://barbour-abi.com/), [compare](https://crucible.io/insights/marketing/barbour-abi-or-glenigan-comparing-construction-data-platform-2025/))
- **BCIS (RICS)** — elemental €/m² cost benchmarks; the QS standard for early cost plans, tender
  estimates, project benchmarking. ([bcis.co.uk](https://www.bcis.co.uk/), [wiki](https://en.wikipedia.org/wiki/Building_Cost_Information_Service))

**Our defensible position** (per commercial plan): the *data layer* — awards + spend + competition
quality + enrichment joins. The two commercial gaps are live below-threshold tenders and contract
**end-dates** (the expiring-contracts signal — we have no duration fields). Our unique edges:
single-bid competition rates, CRO/lobbying/charity overlaps, actual payments (€15bn SPENT).

## 3. Surfacing plan (app-side)

1. **Lift TED + payments out of the picker** — make the three registers (eTenders / TED / payments)
   first-class and visible, not defaulted-away. Show value where it exists (TED awards, payments),
   counts where it doesn't (eTenders), each clearly grain-labelled (never summed).
2. **Award-level detail at browse** — surface representative awards (title, buyer, CPV, value-if-
   present, date) above/alongside the supplier rollups, so the page isn't only counts.
3. **One persistent year control** that scopes the active register (not buried per-lens).
4. **Buyer (authority) spend profiles** — we have payments + LA AFS budgets; a buyer view mirrors
   Tussell/Stotles buyer context using our data.
5. **Competition-quality surfacing** — single-bid rate is a genuine edge; surface per buyer/category.

## 4. QS valuation — greenlit app TEST feature (year-aware)

**Override noted:** inference is normally forbidden in the citizen app; the owner has greenlit the
QS valuation as a clearly-labelled **experimental** feature (2026-06-11). Guardrails: an
"Indicative estimate — not a disclosed figure" panel, method (RICS NRM elemental: units × m² ×
€/m²) + sources + caveats always shown, never asserted as the contract's value.

**Year-aware (owner requirement "go back in year"):** value a contract against costs of its **award
half-year**, the QS tender-date method:
`rate_at(award) = benchmark_rate × TPI(award_period) / TPI(basis_period)`.

**INGESTED 2026-06-11 (real, cited, curated source-of-truth):**
- `data/_meta/scsi_tender_price_index.csv` — official SCSI National Commercial Construction Tender
  Price Index, half-yearly **1998 H1 → 2025 H1** (base 1998 H1 = 100), the year deflator. Source:
  SCSI via Buildcost Construction Cost Guide H2-2025, Table 2.
- `data/_meta/qs_cost_benchmarks.csv` — €/m² (and per-unit/key/space/m) benchmarks across ~45
  building types: Buildcost H2-2025 (residential, education, health, office, retail, logistics,
  refurb, heritage, hotels, carparks, infrastructure), SCSI House Rebuilding Guide (region-specific),
  SelfBuild 2026. Each row carries `basis_period`, VAT basis, and exclusions.

**Build steps (next):**
- Bring a `qs_valuation` module into the repo (the out-of-repo one is the seed) that reads the two
  curated CSVs and applies the TPI year-adjustment; default to the award half-year of the contract.
- Surface it on a procurement contract/award (or as a standalone "indicative valuation" panel) with
  the experimental labelling, the €/m² range used, the TPI adjustment shown, and full sources.
- Tests: TPI lookup + interpolation; benchmark selection by category; year-adjustment maths;
  inference-labelling present.

## 5. Sources
SCSI Tender Price Index & Real Cost reports (scsi.ie); Buildcost Construction Cost Guide H2-2025
(buildcost.ie); SelfBuild Build Costs 2026 (selfbuild.ie); Tussell, Stotles, Barbour ABI/Glenigan,
BCIS/RICS (linked in §2).
