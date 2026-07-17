---
tier: SPEC
status: LIVE
domain: procurement
updated: 2026-07-13
supersedes: []
read_when: building or extending the buyer/public-body procurement dossier feature
key: SPEC|LIVE|procurement
---

# Buyer / Public-Body Procurement Dossier — Design

**Status:** design-only (no code written). Companion to `doc/PROCUREMENT_MASTER.md`,
`doc/PROCUREMENT_INTELLIGENCE_ROADMAP.md`, `doc/PUBLIC_PAYMENTS_FACT_SCHEMA.md`.
**Audience for the artefact:** a contractor / supplier deciding whether to bid for a public
buyer, who wants to understand the buyer *before* committing tender effort.
**Date:** 2026-06-28.

> **Update 2026-07-13 — the §3 buyer crosswalk is BUILT and promoted.** The curated CSV lives at
> `data/_meta/procurement_publishers/buyer_xref.csv` (90 bodies; the 30 needs_review rows repaired
> against the live registers — councils had latched onto defunct pre-2014 entities — see
> `data/_meta/procurement_publishers/README_buyer_xref.md`). Resolver:
> `dail_tracker_core/buyer_xref.py::resolve_buyer()` (fail-closed), tests in
> `test/dail_tracker_core/test_core_buyer_xref.py`. First consumer: the sandbox bid-pack's
> buyer-payments panel (fixes the "Limerick" vs "Limerick City and County Council" miss found in
> the 2026-07-13 live-tender scenario).

> **Verified 2026-06-28 against live SQL views + query module** (not just memory/agent report).
> All column names below are confirmed from `sql_views/procurement/*.sql` and
> `dail_tracker_core/queries/procurement.py`. The check **reduced the build estimate**: three
> panels I first marked "new" are already-shipped reuse — see §2/§4b. Outstanding genuinely-new
> work ≈ **1 view (authority×CPV) + the supplier-set context join + the `buyer_xref` crosswalk.**

This is the buyer-side mirror of the existing **Supplier Dossier**. The procurement page
already has thin per-buyer drilldowns (`_render_authority_profile`, the council
`_render_payments_publisher_profile`); this design specifies the *full* buyer dossier that
fuses every lens we hold about one public body.

---

## 0. The two rules this design cannot break

1. **Three money grains never sum across each other.** Every € in the dossier is tagged with
   a grain and the verb *is* the disambiguation:
   - **AWARDED** — contract-award ceiling (eTenders + TED). A planning signal, not spend.
   - **ORDERED / PAID** — realised spend (public-body POs / payments over €20k). Two
     sub-tiers: `po_committed` (ORDERED) and `payment_actual` (PAID).
   - **BUDGET** — audited council accounts (AFS) and CSO general-government totals. Context
     denominators only.
   - **PLANNED** — live-tender advertised estimates. Forward pipeline, never realised.

   A buyer's `€316.5m AWARDED` and `€4.06bn ORDERED` are the *same body at two lifecycle
   stages with no shared key* — adding them is a fiction. The dossier renders them in
   physically separate panels with a persistent grain badge and a "never summed" rail.

2. **No inference, provenance is the owner's domain.** Every figure is parsed-from-source and
   linked to `source_file_url`; coverage is partial so every total is a **floor** ("at least
   €Y from N documents — indicative, not audited"), never a bare authoritative total. Context
   overlaps (lobbying, diaries, distress) are **co-occurrence, never causation.**

---

## 1. The central design risk: buyer identity is not yet reconciled

A buyer dossier fuses award + competition + payments + live-tender data **for one body**.
Those four sources live in three different name-spaces with **no crosswalk today**:

| Source | Buyer key today | Distinct buyers (measured) |
|---|---|---|
| eTenders awards | `contracting_authority` (clean name) | **2,249** |
| TED awards + TED live tenders | `buyer_name` (clean name) | **909** |
| Public-body / LA payments | `publisher_name` (curated `publishers_seed.csv`) | **88** |
| AFS / NOAC (councils) | `council` / `slug` (31-row curated LA map) | 31 |

**Measured 2026-06-28 (validation §10) — the join is NAME-based, not org-id-based.** The OGP
`_<orgid>` suffix is essentially absent at the gold layer (**2 of 2,249** eTenders strings,
**2 of 909** TED), so `shared/buyer_clean.org_id_expr()` is a near-no-op here — eTenders and
TED reconcile on the **cleaned name** (29 of the top-30 eTenders buyers exact-match a TED
buyer). **The hard gap is awards/TED ↔ payments:** only **88** bodies disclose payments, and
they use SHORT forms (`Cork County`, `Donegal`, `Fingal`) where eTenders/TED use long forms
(`Cork County Council`). Of the top-30 eTenders buyers, only **7 exact-match a payments
publisher**, yet **22 actually have a payments lane** reachable once the name forms are
reconciled — a naive exact join reaches 7, the crosswalk recovers ~15 more. **payments ↔
AFS/NOAC is NOT even solved for councils:** AFS/NOAC key Dublin on `"Dublin City"` while
payments now keys it `"Dublin City Council"`, so the live `v_procurement_council_summary`
union **splits Dublin City into two separate index rows today** (§10) — a concrete bug the
canonical `buyer_id` resolves.

This was already flagged as the blocker that keeps `v_procurement_competition` unwired from
the page (`PROCUREMENT_INTELLIGENCE_ROADMAP.md`).

### Recommended fix — a curated `buyer_xref` crosswalk (the key enabler)

Build `data/_meta/procurement_publishers/buyer_xref.csv`, fail-closed, in the same trusted
pattern as `publishers_seed.csv` / `procurement_disclosed_bodies.csv`:

```
buyer_id            -- canonical id, reuse payments publisher_id where one exists (ie_la_dublin_city)
display_name        -- "Dublin City Council"
buyer_type          -- local_authority | central_gov | semistate | health | education | other
sector
etenders_name       -- exact eTenders contracting_authority string(s), pipe-delimited
ted_buyer_name      -- exact TED buyer_name string(s), pipe-delimited (variants differ)
payments_publisher_id  -- + the SHORT publisher_name form (the main mismatch to bridge)
live_buyer / live_org_id -- eTenders-platform live-tender buyer (buyer_org_id often NULL → name)
council_slug         -- AFS/NOAC join key, councils only
match_tier           -- curated_exact | name_only | single_register
```

Keying is on **exact register name strings** (org-id is absent at gold, §10), so the crosswalk
stores the variant strings each register uses for one body. Seed it from the 88 payment
publishers + the top-N award buyers by volume (covers ~95% of activity in a few dozen
hand-verified rows). **Every buyer dossier carries a `match_tier`**;
panels backed by a register *not* in this buyer's xref row render standalone with an
"identity not reconciled across registers" note rather than a fused number. This is the
honest fallback and it is what lets the dossier ship before the crosswalk is complete.

> **OWNER DECISION (do not decide autonomously):** approve the `buyer_xref` approach vs.
> a pure on-the-fly org-id join; and the `match_tier` labels/thresholds. Provenance &
> entity-resolution promotion are the owner's domain.

---

## 2. The 10 buyer questions → dossier panels → data surface

| # | Buyer question | Panel | Grain | Backing surface (✅ exists / 🆕 new) |
|---|---|---|---|---|
| 1 | What does this buyer buy? | `what_they_buy` (top CPV) | AWARDED | 🆕 `v_procurement_authority_cpv_summary` (buyer×CPV) |
| 2 | Which suppliers win? | `top_suppliers.awards_side` | AWARDED | ✅ `v_procurement_incumbency` covers the recurring set; 🆕 *optional* thin authority×supplier rollup for the full long-tail ranking |
| 3 | Typical award values? | `summary` (median/IQR) + `award_trend` | AWARDED | ✅ `authority_summary`, ✅ `v_procurement_authority_year_summary` |
| 4 | Which CPVs/categories active? | `what_they_buy` | AWARDED | 🆕 buyer×CPV view (as #1) |
| 5 | Recurring suppliers? | `top_suppliers.incumbency` | AWARDED | ✅ `v_procurement_incumbency` — already grain `(supplier_norm, contracting_authority)` w/ `n_distinct_years` |
| 6 | Low-bid / single-bid signals? | `competition` (two lenses) | (signal) | ✅ `v_procurement_competition` (TED lot-level, 2024+) **+ 🆕 eTenders award-level rate from `n_bids_received` (78.4% filled, all years 2013+, verified §4b)** — shown side by side, never merged |
| 7 | Live / recent tenders? | `live_tenders` | PLANNED | ✅ `v_procurement_live_tenders` + `_summary` (already buyer-keyed: `buyer`/`buyer_org_id`) + `v_procurement_ted_tenders` |
| 8 | Payments / POs visible? | `payment_trend` + `top_suppliers.payments_side` | ORDERED/PAID | ✅ `payments_for_publisher`, ✅ `payments_by_year` |
| 9 | Framework / call-off patterns? | `frameworks` | AWARDED (ceiling) | ✅ `v_procurement_call_off_links` (call-off→parent-framework ceiling, `parent_in_corpus` flag) |
| 10 | Audit / regulatory / corporate / public-record context? | `context_signals` | (context) | 🆕 supplier-set join over ✅ distress / lobbying / diary / charity views |
| — | Comparable recent awards | `comparable_recent_awards` | AWARDED | ✅ `awards_for_authority(buyer, year)` (notice links: `etenders_notice_url` / `ted_notice_link`) |
| — | Source coverage | `source_coverage` | meta | ✅ coverage JSONs in `data/_meta/` |
| — | Freshness | `freshness` | meta | ✅ coverage JSON periods + `source_fetch_failures` |
| — | Council context (LA buyers) | `budget_context` | BUDGET | ✅ `afs_total_by_year`, `afs_by_division`, `afs_capital_by_year`, NOAC scorecard |
| — | Denominator (share-of-national) | inside `summary` | BUDGET | ✅ `v_gov_finance_annual` (CSO GFA01) |

Net new build (post-verification): **1 buyer-scoped SQL view (authority×CPV) + 1 supplier-set
context join + 1 crosswalk CSV** — optionally a thin authority×all-suppliers rollup and an
eTenders bid-count rollup (§4b). Panels #5/#7/#9 reuse **already-shipped** views
(`v_procurement_incumbency`, `v_procurement_live_tenders(_summary)`,
`v_procurement_call_off_links`). This is a *surfacing* job, not data engineering — consistent
with the intelligence-roadmap finding.

---

## 3. Dossier schema

One canonical structured object, `buyer_dossier`, returned by query layer → assembled by a
`dossiers.buyer_dossier()` function → exposed via MCP tool + REST endpoint (§5). JSON shape:

```jsonc
{
  "buyer": {
    "buyer_id": "ie_la_dublin_city",
    "display_name": "Dublin City Council",
    "buyer_type": "local_authority",
    "sector": "local_government",
    "registers_present": ["etenders", "ted", "payments", "afs", "noac", "live_tenders"],
    "match_tier": "curated_exact",            // curated_exact|name_only|single_register
    "aliases": {
      "etenders_org_id": ["…"],
      "ted_buyer_names": ["Dublin City Council"],
      "payments_publisher_id": "ie_la_dublin_city",
      "council_slug": "dublin-city"
    }
  },

  // Q1/Q3 — headline. Each block is ONE grain; blocks never combined.
  "summary": {
    "awards": {                               // AWARDED
      "n_awards": 1873, "n_safe_awards": 0, "n_suppliers": 1041, "n_cpv": 0,
      "awarded_value_safe_eur": 316509293.63, "median_award_eur": null,
      "first_year": 0, "last_year": 0, "register": "etenders+ted"
    },
    "payments": {                             // ORDERED + PAID (sub-tiers kept apart)
      "ordered_po_eur": 3697109663.35, "paid_actual_eur": 0.0,
      "n_suppliers_paid": 2118, "first_year": 2012, "last_year": 2026,
      "vat_basis": "mixed|incl_vat|excl_vat", "publisher_id": "ie_la_dublin_city"
    },
    "budget_context": {                       // BUDGET — councils only, denominator
      "afs_net_revenue_eur": null, "afs_capital_eur": null, "year": null,
      "share_of_national_expenditure_pct": null  // vs CSO GFA01, clearly labelled
    },
    "headline_caveat": "AWARDED ≠ ORDERED ≠ BUDGET — three grains, never summed."
  },

  // Q1/Q4
  "what_they_buy": {
    "top_cpv": [{ "cpv_code": "", "cpv_description": "", "n_awards": 0,
                  "awarded_value_safe_eur": 0.0, "median_award_eur": 0.0,
                  "share_of_buyer_awards_pct": 0.0, "register": "etenders" }],
    "coverage_note": "CPV is award-side only; payments carry no CPV."
  },

  // Q2/Q5
  "top_suppliers": {
    "awards_side": [{ "supplier_norm": "", "supplier_display": "", "n_awards": 0,
                      "awarded_value_safe_eur": 0.0, "first_year": 0, "last_year": 0,
                      "n_years_active": 0, "is_recurring": false,
                      "cro_company_num": "", "match_confidence": 0.9 }],   // {0.0,0.5,0.9}
    "payments_side": [{ "supplier_norm": "", "ordered_eur": 0.0, "paid_eur": 0.0,
                        "n_lines": 0, "first_year": 0, "last_year": 0 }],
    "incumbency": {
      "top1_award_share_pct": 0.0, "top5_award_share_pct": 0.0, "hhi_award_value": 0.0,
      "n_repeat_suppliers": 0, "repeat_supplier_award_share_pct": 0.0,
      "market_label": "broad|moderately concentrated|concentrated"   // descriptive, not a verdict
    }
  },

  "award_trend":   [{ "year": 0, "n_awards": 0, "n_safe_awards": 0, "awarded_value_safe_eur": 0.0 }],
  "payment_trend": [{ "year": 0, "ordered_eur": 0.0, "paid_eur": 0.0, "n_lines": 0, "vat_basis": "" }],

  // Q6 — factual signal, never a verdict. TWO distinct lenses, never merged.
  "competition": {
    "ted_lot_level": {                   // TED eForms, 2024+, lot grain
      "n_notices": 261, "n_lots_with_bidcount": 207, "n_single_bid_lots": 68,
      "single_bid_lot_pct": 32.9, "n_uncompetitive_notices": 53, "n_price_only_notices": 16,
      "first_year": 2024, "last_year": 2026, "benchmark_lot_pct": 20.4,
      "sample_sufficient": true          // n_lots_with_bidcount >= 40
    },
    "etenders_award_level": {            // eTenders n_bids_received, 2013+, tender_id grain
      "n_awards_with_bidcount": 1680, "n_single_bid_awards": 233,
      "single_bid_pct": 13.9, "first_year": 2013, "last_year": 2026,
      "covers_sub_threshold": true,      // 65.6% of non-TED rows filled — the long tail
      "sample_sufficient": true          // n_awards_with_bidcount >= 40
    },
    "caveat": "Two DIFFERENT metrics — different corpus (TED vs eTenders), grain (lot vs award notice), and window (2024+ vs 2013+). Never merged into one rate. A single bid is often legitimate (niche/urgent/specialist) — a prompt to look, never proof of wrongdoing."
  },

  // Comparable recent awards — newest first, each a real notice link
  "comparable_recent_awards": [{
    "award_date": "", "supplier_norm": "", "cpv_code": "", "cpv_description": "",
    "value_eur": 0.0, "value_kind": "contract_award_value", "value_safe_to_sum": true,
    "procedure_type": "", "n_tenders_received": null, "notice_url": "", "register": "etenders|ted"
  }],

  // Q7 — PLANNED, forward pipeline
  "live_tenders": [{
    "title": "", "cpv_division": "", "estimated_value_eur": null,   // PLANNED, value_safe_to_sum=false
    "procedure": "", "submission_deadline": "", "days_to_deadline": null,
    "is_open": true, "notice_url": "", "register": "etenders|ted"
  }],

  // Q9 — framework ceilings: NEVER summed into spend
  "frameworks": {
    "n_framework_or_dps_awards": 0, "framework_ceiling_eur": 0.0,   // ceiling, not spend
    "n_call_offs": 0,
    "parent_agreements": [{ "parent_agreement_id": "", "title": "", "ceiling_eur": 0.0,
                            "n_call_offs": 0, "suppliers": [] }],
    "caveat": "Framework ceiling = maximum permissible, not money spent; never add to AWARDED or ORDERED totals."
  },

  // Q10 — supplier-keyed context; co-occurrence ONLY
  "context_signals": {
    "suppliers_with_distress_notice": [{ "supplier_norm": "", "notice_subtype": "",
                                         "notice_date": "", "iris_url": "" }],
    "n_suppliers_in_lobbying_register": 0,
    "n_suppliers_in_ministerial_diaries": 0,
    "n_charity_suppliers": 0,
    "caveat": "Entity co-occurrence across public registers. NOT evidence any award was influenced; exact-name match undercounts; solvent members' voluntary liquidations excluded as non-distress."
  },

  "source_coverage": [{ "register": "etenders", "present": true, "row_count": 0,
                        "year_span": "2013–2026", "completeness_note": "" }],
  "freshness":       [{ "register": "", "last_period": "", "staleness_days": null,
                        "status": "fresh|stale|broken", "source_url_sample": "" }],

  "caveats": ["…"],            // assembled, human-readable (see §7)
  "confidence": {             // see §6
    "buyer_match_tier": "curated_exact",
    "per_section": { "awards": "high", "payments": "high", "competition": "high",
                     "frameworks": "medium", "context_signals": "context-only" }
  }
}
```

---

## 4. SQL / API query requirements

### 4a. Reuse (already shipped — pass the buyer key)

| Need | Function / view | Buyer key |
|---|---|---|
| Award headline + median | `procurement.authority_summary()` (filter to buyer) | `contracting_authority` |
| Award trend | `v_procurement_authority_year_summary` | `contracting_authority` |
| Comparable recent awards | `procurement.awards_for_authority(authority, year)` | `contracting_authority` |
| Competition signal | `procurement.competition()` / `v_procurement_competition` | `buyer_name` |
| Payment trend | `procurement.payments_by_year(publisher_name, tier)` | `publisher_name` |
| Suppliers paid | `procurement.payments_for_publisher(publisher_name, tier)` | `publisher_name` |
| Publisher header | `procurement.payments_publisher_profile(publisher_name)` | `publisher_name` |
| Council budget (BUDGET) | `afs_total_by_year`, `afs_by_division`, `afs_capital_by_year` | `council` |
| Council performance | `v_la_noac_scorecard` | council (curated map) |
| 3-lane council index | `v_procurement_council_summary` | — |
| National denominator | `v_gov_finance_annual` (CSO GFA01) | year |
| Source freshness | `data/_meta/*coverage.json` + `source_fetch_failures()` | publisher / source |

### 4b. Genuinely new (1 view + 1 join + 1 crosswalk) — and what turned out to be reuse

**NEW — `v_procurement_authority_cpv_summary`** — the one missing buyer-grain view.
`GROUP BY contracting_authority, cpv_code`: `n_awards, awarded_value_safe_eur
(value_safe_to_sum only), median_award_eur, share_of_buyer_awards_pct, first_year, last_year`.
CPV median/IQR logic already exists in `procurement_cpv_summary.sql` — copy it, add the buyer
grouping key. Powers panels #1 + #4.

**REUSE (verified shipped) — no new view needed:**
- Panel #5 recurring suppliers → **`v_procurement_incumbency`**, already grain
  `(supplier_norm, contracting_authority)` with `n_awards`, `n_distinct_years`,
  `first/last_year`, `awarded_value_safe_eur`, `HAVING ≥2 awards`, plus
  `authority_is_central_purchasing`. Filter to the buyer; it *is* the recurrence panel.
- Panel #9 frameworks/call-offs → **`v_procurement_call_off_links`**, already links each
  call-off to its parent-framework ceiling with a `parent_in_corpus` flag (an unresolved
  parent is itself a transparency fact). Filter to `contracting_authority`; add a thin
  buyer-level count (`is_framework_or_dps`/`is_call_off` from `v_procurement_awards`).
- Panel #7 live tenders → **`v_procurement_live_tenders` + `v_procurement_live_tenders_summary`**
  (already per-buyer: `buyer`, `buyer_org_id`, `n_open_tenders`, `next_closing`,
  `closing_within_14d`, `est_value_floor_eur` PLANNED) + `v_procurement_ted_tenders` (TED).
  ⚠️ The live-tender `buyer_org_id` is the *eTenders-platform* internal id — it may not equal
  the OGP award org-id or the payments `publisher_id`, so the **`buyer_xref` must also carry
  the live `buyer_org_id`** (or join the live feed on cleaned buyer name). Just add a buyer
  `WHERE` to the existing query function.

**OPTIONAL thin rollups (only if the basics aren't enough):**
- authority×all-suppliers summary (incl. single-award winners) for a complete top-suppliers-
  by-value ranking — `v_procurement_incumbency` already covers the *recurring* (≥2-award)
  core, so this is only for the long tail.
- buyer-level **eTenders single-bid rate** from `n_bids_received` on `v_procurement_awards`
  — **VERIFIED VIABLE 2026-06-28** (DuckDB on the gold parquet): 78.4% of award rows carry a
  real bid count (every filled value ≥1), across **all years 2013–2026**, and **65.6% of
  *sub-EU-threshold* (non-TED-linked) rows are filled** — so it reaches the long tail TED
  cannot see. Single-bid share 17.4% of filled overall; **175 buyers** clear ≥40 bid-counted
  awards (vs ~30 in the TED view). Build it at **`tender_id` grain** (de-dup award×supplier
  rows, as the TED view de-dups notices) over filled rows only; apply a ≥40 sample guard;
  same "factual signal, never a verdict" rail. **⚠️ It is a DIFFERENT metric from the TED
  lot-level rate** (different corpus/grain/window) — surface both, never merge (DCC: 13.9%
  eTenders awards 2013–26 vs 32.9% TED lots 2024+).

**Supplier-set context join** — for the buyer's supplier set (award winners ∪ paid
   suppliers), join the existing context surfaces. **Validated keys (§10):**
   - **Lobbying** → reuse `procurement_lobbying_overlap.parquet` / `v_procurement_lobbying_overlap`,
     already keyed on the procurement `supplier_norm` (46 DCC suppliers matched cleanly). Do NOT
     route through `v_ministerial_diary_org_overlap` (keyed on `matched_org_name`, not norm).
   - **Distress** → `cbi_xref_corporate_notices` joins on `entity_norm`, but that column is
     **lower-case with a different suffix rule** than `supplier_norm`, so the raw join returns 0
     across all 38,335 suppliers — **add a normalisation-alignment step** (re-norm one side with
     `shared/name_norm`, or key on `cro_company_num`) before this panel returns anything.
   - **Charity** → `v_charity_financials_by_year` (overlap parquet path TBC).
   Returns **counts + a short distress list only** — co-occurrence framing enforced in the
   assembler, never a causal column.
6. **`data/_meta/procurement_publishers/buyer_xref.csv`** — the §1 crosswalk. Fail-closed:
   a missing register row ⇒ that panel renders single-register with a match-tier note.

### 4c. Assembler + transport

- **Query layer:** `dail_tracker_core/queries/procurement.py` gains buyer-scoped functions
  (`authority_cpv_summary`, `authority_supplier_summary`, `authority_frameworks`,
  `live_tenders_for_buyer`, `context_for_supplier_set`).
- **Dossier layer:** `dossiers.buyer_dossier(buyer)` resolves `buyer` (id or fuzzy name) via
  `buyer_xref`, calls the panels for the registers present, attaches caveats + confidence,
  returns the §3 object. Mirrors the existing `list_procurement_competition()` pattern.
- **MCP tool:** `buyer_dossier(buyer: str)` in the MCP server — the natural extension; the
  server currently exposes the *pieces* (`procurement_by_authority`, `procurement_competition`,
  `public_body_payments`) but no fused single-buyer drilldown.
- **REST:** `GET /v1/buyer/{buyer_id}/dossier` in `api/main.py` (alongside `/v1/data`), plus
  in-app CSV export via the shared `utility/ui/export_controls.py:export_button` (the
  roadmap noted procurement has zero in-app export today — the dossier should ship with it).

---

## 5. UI layout

Extends the existing buyer drilldowns into one **Buyer Dossier** page, reached from the
"Who actually gets paid?" publisher list and the "Who wins contracts?" authority list
(`?buyer=<id>`). Bold/designed components; dataframes are secondary (project convention).

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  DUBLIN CITY COUNCIL                              [Local authority · Dublin]   │  ← hero
│  Buyer dossier · registers: eTenders · TED · Payments · AFS · NOAC · Live     │
│  Identity matched across registers: ✔ curated                                  │
├──────────────────────────────────────────────────────────────────────────────┤
│  ⚠ Three money grains — shown separately, never added together.               │  ← persistent rail
│  ┌── AWARDED (ceilings) ──┐ ┌── ORDERED/PAID (spend) ─┐ ┌── BUDGET (accounts) ┐│
│  │ €316.5m safe-summable  │ │ €4.06bn ordered (PO>€20k)│ │ AFS net rev €… (yr) ││  ← 3 grain cards
│  │ 1,873 awards·1,041 sup │ │ 2,331 suppliers paid     │ │ NOAC scorecard ↗    ││
│  └────────────────────────┘ └──────────────────────────┘ └─────────────────────┘│
├──────────────────────────────────────────────────────────────────────────────┤
│  WHAT THEY BUY                          │  WHO WINS / WHO'S PAID                │
│  ▸ top CPV bars (share of awards)       │  ▸ top suppliers (awards) + recur ●   │
│  ▸ "active categories: N"               │  ▸ top suppliers (paid)               │
│                                         │  ▸ incumbency: top-5 = X% · repeat=Y% │
├──────────────────────────────────────────────────────────────────────────────┤
│  AWARD TREND (AWARDED) ── bar/yr        │  PAYMENT TREND (ORDERED/PAID) ── bar/yr│  ← two axes, side by side, NEVER merged
├──────────────────────────────────────────────────────────────────────────────┤
│  COMPETITION SIGNAL  ── single-bid lot rate 32.9%  ▮▮▮▮▮▯▯ vs 20% baseline      │
│  53 uncompetitive notices · 16 price-only · TED 2024+ · [why this isn't a verdict ⓘ]│  ← st.popover explainer
├──────────────────────────────────────────────────────────────────────────────┤
│  LIVE & RECENT                                                                 │
│  ▸ OPEN NOW (PLANNED): tender · deadline · est. value · [view on TED ↗]        │
│  ▸ COMPARABLE RECENT AWARDS: date · supplier · CPV · €value · bids · [notice↗] │
├──────────────────────────────────────────────────────────────────────────────┤
│  FRAMEWORKS & CALL-OFFS  ── N frameworks · ceiling €… (not spend) · parents ▾  │
├──────────────────────────────────────────────────────────────────────────────┤
│  CONTEXT (co-occurrence only)  ▾ collapsed by default                          │  ← drawer, clearly framed
│  · suppliers also on lobbying register: N   · in ministerial diaries: N        │
│  · supplier corporate-distress notices: list (Iris) · charity suppliers: N     │
├──────────────────────────────────────────────────────────────────────────────┤
│  COVERAGE & FRESHNESS  ── per-register: rows · year span · last updated · ●    │
│  CAVEATS  ── grain rules · floors-not-totals · 2024+ competition · match tier  │
│  [⤓ Export buyer dossier CSV]                                                   │
└──────────────────────────────────────────────────────────────────────────────┘
```

Design rules: persistent **grain badge** on every € (verb-led: "awarded/ordered/paid/
budget/advertised €X", never bare €); award counts shown as **counts not sums** where ceilings
dominate; competition + context behind an **explain-this-figure `st.popover`** (genuinely new
— unused elsewhere); context drawer collapsed and labelled co-occurrence; for non-council
buyers the BUDGET card degrades to "no audited-accounts lane for this body type."

---

## 6. Source-confidence rules

Three independent axes; the weakest governs the panel's badge.

**Axis A — buyer-match tier** (how sure we are the panels describe the *same body*):
`curated_exact` (in `buyer_xref`) › `name_only` (exact cleaned-name match across registers —
reliable for eTenders↔TED, 29/30; unreliable to payments' short forms) › `single_register`
(no fusion — panel stands alone). Below `curated_exact`, cross-register fusion to payments
(e.g. award→paid comparison) is **suppressed**, not guessed.

**Axis B — grain** (already enforced): AWARDED ceilings carry the `value_safe_to_sum` firewall
(framework ceilings + single-supplier awards ≥€50m excluded; pan-EU outliers excluded);
ORDERED/PAID split by `value_kind`; BUDGET never mixed in.

**Axis C — extraction confidence** (per fact row, payments are parsed-from-PDF):
`high` (born-digital / API: TED, eTenders, CSO) › `medium` (clean table parse) ›
`low` (OCR / blank-supplier / geometry-gap rows — filtered from rankings).

**Sample sufficiency:** competition shown only when `n_lots_with_bidcount ≥ 40`; otherwise
"insufficient sample, not ranked". **Register completeness:** TED *winners* go back to **2016**
via the `ted_ie_winner_history` layer (verified §10 — DCC has 985 pre-2024 winners; the old
"winners 2024+ only" claim, still in the MCP `data_coverage` caveat, is stale); the TED
*lot-level* competition rate is 2024+ only, but the eTenders *award-level* bid-count lens
covers 2013+ (§4b); eTenders awards all-time (2013+); payments per-publisher year span as
recorded in coverage JSON, and a payments lane exists for only **88** of 2,249 award buyers.

Badge mapping → UI: **Verified** (curated match + high extraction + safe grain) ·
**Indicative** (name/org-id match or medium extraction — "floor, not total") ·
**Context-only** (the overlap drawer — never scored as evidence).

---

## 7. Caveats (assembled into the dossier + page footer)

1. **Grains never sum.** AWARDED ceilings, ORDERED/PAID spend, and BUDGET accounts are three
   different kinds of money with no shared key. The dossier never adds across them and never
   computes "awarded − paid" (no contract→payment key exists).
2. **Totals are floors.** Every € is parsed from a published document; coverage is partial.
   Read "at least €Y from N documents", never "the buyer spent exactly €Y".
3. **Award ceiling ≠ spend.** A €Xm framework ceiling is the maximum permissible, not money
   committed or paid.
4. **Competition is a signal, not a verdict.** A single bidder is often legitimate (niche,
   specialist, urgent). Two distinct lenses are shown and **never merged**: the TED *lot-level*
   rate (2024+) and the eTenders *award-level* bid-count rate (2013+, 78.4% filled). They use
   different corpora/grains/windows and give different numbers (e.g. DCC 32.9% vs 13.9%). Rank
   only buyers with a healthy sample (≥40).
5. **Context is co-occurrence, not causation.** Suppliers appearing on the lobbying register,
   in ministerial diaries, or in Iris distress notices is *entity overlap*; it is **not**
   evidence any award was influenced or any supplier is unsound. Exact-name matching
   undercounts; solvent members' voluntary liquidations are excluded as non-distress.
6. **Buyer identity may be partial.** Where registers aren't reconciled for a body, panels are
   shown per-register with a match-tier note rather than fused.
7. **Payments grain & VAT vary by body** (`po_committed` vs `payment_actual`; incl/excl VAT) —
   never cross-sum differing VAT bases; the dossier flags `vat_basis`.
8. **TII road-grant / intergovernmental transfers and PPP-SPV payees** are not private
   procurement and are excluded from supplier rankings where flagged.
9. **CSO / AFS denominators are BUDGET grain** — used only to position spend as a share of a
   total, never added to procurement figures.

---

## 8. Worked example — Dublin City Council (real anchors + illustrative shape)

Real figures are live-queried (2026-06-28) and labelled **[real]**; structural detail is
**[illustrative]** (shape only — would be filled by the views above, not invented here).

**Buyer** — Dublin City Council · local_authority · registers: eTenders, TED, Payments, AFS,
NOAC, Live · match_tier `curated_exact` **[real: present in all registers]**.

**Three grain cards (never summed):**
- **AWARDED [real]** — €316.5m sum-safe · 1,873 awards · 1,041 suppliers (eTenders all-time;
  reconciles to the cent against the gold parquet, §10).
- **ORDERED [real]** — €4.06bn `po_committed` · 40,431 PO lines · 2,331 suppliers · 2012–2026
  (`ie_la_dublin_city`, POs over €20k; current on-disk gold. The MCP snapshot still shows
  €3.70bn / 39,226 lines — it loaded an earlier 85-publisher gold before the disclosed-BQ
  Tranche merge; query the parquet, not the MCP, for the live figure). *ORDERED €4.06bn and
  AWARDED €316.5m are different grains — shown apart, never blended.*
- **BUDGET [real]** — AFS net revenue expenditure **€390.2m** (2025, 8 service divisions,
  reconciled) · capital programme **€668.4m** (2025) · NOAC 2024 scorecard ↗. *Council key is
  "Dublin City" here vs "Dublin City Council" in payments — the dossier must canonicalise (§10).*

**Competition signal [real, two lenses]** — *TED lot-level (2024+):* single-bid rate **32.9%**
(68 of 207 lots) · 53 uncompetitive · 16 price-only — above the ~20% baseline and above peer
councils (Cork County 14.0%, Limerick 15.3%, South Dublin 23.8%). *eTenders award-level
(2013–26):* **13.9%** single-bid (233 of 1,680 bid-counted awards, 89.7% filled). The two
numbers differ because they measure different corpora/grains/windows — shown side by side,
never merged. A *prompt to look*, not a verdict; DCC runs many works lots where a single
specialist bid can be legitimate.

**What they buy [real]** — top CPVs: Construction work (82 awards, €9.16m AWARDED-safe),
Coaching services (35), Architectural & related services (29) — fits a council's works/housing
profile.

**Top suppliers [real]** — awards side: Roadstone (26 awards), Richard Nolan Civil Engineering
(21), SIAC Bituminous Products (19). Paid side (ORDERED): Bartra ODG €242m, Purcell
Construction €154m, Sisk €127m. Incumbency: **222 suppliers active ≥2 years** with DCC (max
8 years) — durable repeat-supplier base.

**Live & recent [real]** — OPEN NOW: **7 live DCC tenders** (matched by buyer name —
`buyer_org_id` is NULL on this feed, §10). COMPARABLE RECENT AWARDS: newest
`awards_for_authority("Dublin City Council")` rows; TED adds **446 winners (2024+) + 985
(2016–2023)** via `ted_ie_winner_history`, with notice links.

**Context (co-occurrence) [real]** — **46 DCC suppliers** also on the lobbying register
(Forvis Mazars, Grant Thornton, AECOM, Vodafone…) via the procurement-side overlap parquet —
joins cleanly on `supplier_norm`. The **distress** lane needs a normalisation-alignment step
first (raw `entity_norm` is lower-case, a different norm — §10) and CBI entities skew to
funds/ICAVs, so trade-supplier overlap is low. 98.8% of DCC's award suppliers are
CRO-resolvable. Overlap only, no causal claim.

**Coverage & freshness [real-shaped]** — eTenders 2013–2026; TED winners 2016–2026; payments
2012–2026 (`ie_la_dublin_city`); each with last period + a fresh/stale/broken dot from the
coverage JSONs and `source_fetch_failures`.

---

## 9. Build order (when greenlit)

0. **Owner sign-off** on `buyer_xref` + match-tier labels (§1) and the live-vs-cited
   completeness wording (provenance is owner-domain).
1. Crosswalk CSV `buyer_xref.csv` (seed the 88 publishers + top award buyers; key on names).
2. ONE new view (`v_procurement_authority_cpv_summary`) + buyer-scoped query filters over the
   reused `v_procurement_incumbency` / `v_procurement_call_off_links` /
   `v_procurement_live_tenders` + tests (SQL-view fixtures).
3. Supplier-set context join, co-occurrence framing enforced in the assembler.
4. `dossiers.buyer_dossier()` + MCP `buyer_dossier` tool + `/v1/buyer/{id}/dossier`.
5. Streamlit Buyer Dossier page (extends existing authority/publisher profiles) + CSV export.
6. Firewall check (`tools/check_streamlit_logic_firewall.py`) + civic-ui-review pass.

All steps are additive and boundary-safe; none touch the three-grain firewall or promote any
new aggregate to gold without data-anchored evidence.

---

## 10. Validation log — assertions tested against the data (2026-06-28)

Read-only DuckDB on the gold/silver parquets (not the MCP snapshot). Each assertion in the
plan was tested; results below. ✅ confirmed · ✳️ confirmed with refinement · ❌ corrected.

| # | Assertion | Result | Evidence |
|---|---|---|---|
| 1 | Buyer identity is unreconciled across registers | ✅ **strongly** | Distinct buyers: eTenders **2,249** / TED **909** / payments **88**. Top-30 eTenders buyers: 29 match TED by name, only **7 exact-match payments** |
| — | A curated crosswalk is needed (not exact join) | ✅ | Of top-30, **22 have a payments lane** reachable by reconciling short/long forms — exact join gets 7, crosswalk recovers ~15 more |
| 2 | eTenders+TED joinable on OGP **org-id** | ❌ **corrected** | Org-id suffix nearly absent at gold: **2 / 2,249** eTenders, **2 / 909** TED → join is **name-based**, not org-id |
| 3 | DCC AWARDED €316.5m | ✅ exact | Gold parquet: 1,873 awards · 1,041 suppliers · **€316,509,294** |
| 3 | DCC ORDERED ≈ €3.70bn | ✳️ **updated** | On-disk gold = **€4.06bn · 40,431 lines · 2,331 suppliers** (`po_committed`, 2012–26). MCP shows €3.70bn — it loaded a pre-Tranche 85-publisher gold |
| 3 | Grains are disjoint (never-sum) | ✅ | awards `value_kind` ∈ {contract_award_value, framework_or_dps_ceiling}; payments ∈ {po_committed} — no overlap |
| 4 | Every panel is non-empty/buildable for DCC | ✅ | CPV (Construction 82), suppliers (Roadstone/SIAC), incumbency (222 suppliers ≥2 yrs, max 8), frameworks (1,117 framework rows / 66 call-offs / 30 parents), live (7 open), paid (Bartra €242m) |
| 4 | Live-tender feed is buyer-keyed by `buyer_org_id` | ✳️ | `buyer_org_id` is **NULL** for DCC's 7 open tenders → live feed must join by cleaned **name** |
| 5 | TED winners are 2024+ only | ❌ **corrected** | `ted_ie_winner_history` carries 2016–2023 winners — DCC has **985 pre-2024 winners** (of 1,036 rows) + 446 in 2024+. The MCP `data_coverage` caveat is stale |
| 6 | BUDGET panel (AFS/NOAC) has real numbers | ✅ | DCC AFS **net revenue €390.2m** (2025, 8 divisions, reconciled), **capital €668.4m** (2025, 7 div); NOAC 2024 scorecard present (key `la`) |
| 6 | AFS/NOAC join to payments on one council key | ❌ **live gap found** | AFS/NOAC use **"Dublin City"**; payments uses **"Dublin City Council"** — they don't match on equality, so `v_procurement_council_summary`'s union **splits Dublin City into two index rows** today. The canonical `buyer_id` fixes this |
| 7 | Context join — lobbying | ✅ | **46 DCC suppliers** also on the lobbying register (Forvis Mazars, Grant Thornton, AECOM, Vodafone…) — joins cleanly on `supplier_norm` via the procurement-side `procurement_lobbying_overlap.parquet` |
| 7 | Context join — distress | ⚠️ **needs key-alignment** | Raw `supplier_norm`↔`entity_norm` join = **0 across all 38,335 suppliers** because `entity_norm` is lower-case with different suffix rules (`eircom`, `city quarter capital ii`) vs upper-case `supplier_norm` → must re-normalise one side. CBI entities also skew to funds/ICAVs (low trade-supplier overlap) |
| 7 | DCC suppliers CRO-resolvable (cross-register key) | ✅ | 692 award-company suppliers, **684 (98.8%) CRO-matched** |

**Net effect on the plan:** central thesis (buyer-identity crosswalk is the blocker) is
**empirically confirmed and quantified** — and it already bites a **live view**: AFS/NOAC
("Dublin City") vs payments ("Dublin City Council") split one council into two rows in
`v_procurement_council_summary`. The crosswalk keys on **names** (org-id is absent). Corrections:
TED winners reach back to 2016; DCC ORDERED is €4.06bn (MCP lags disk). New caveats: the
ORDERED/PAID panel exists for only **88 of 2,249** award buyers (degrade gracefully); the
**lobbying** context works out of the box (46 DCC suppliers) but the **distress** context needs
a normalisation-alignment step before it returns anything.
