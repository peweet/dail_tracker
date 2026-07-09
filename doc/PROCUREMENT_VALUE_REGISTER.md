# Procurement / Money Data-Value Register

*Generated 2026-07-08 by a scoped Fable discovery pass. Scope: purchase-order / public-body
payments, AFS (LA Annual Financial Statements), and tenders/awards (eTenders + TED + open
tenders). This is a **snapshot** — verify against live sources before acting on any line.*

## How this was built (anti-staleness spine)

Ground truth was taken from **live** sources, not memory:

- `data/_meta/runtime_data_manifest.json` (rebuilt 2026-07-07 — 140 runtime / 7 lineage / 38 dead)
- live MCP `data_coverage` + one aggregated duckdb query on the gold fact
- 55 registered `sql_views/procurement/*` + 7 `sql_views/payments/*`
- memory notes used as **leads only**, each reconciled against the above and tagged

Every entry carries a verification tag: **CONFIRMED-LIVE** (checked against manifest/MCP) /
**UNVERIFIED** (memory-only) / **STALE** (memory conflicts with live).

> Money grains never sum across each other: procurement AWARDS (ceilings) ≠ public-body
> PAYMENTS (realised) ≠ TED ≠ T&A allowances. Within payments, `payment_actual` ≠
> `po_committed`. The TED pan-EU ceiling (~€1.06tn) is **not** spend. `project_value_estimate`
> is flagged unreliable — do not quote it.

---

## Register (ranked by value × readiness)

### 1. Consolidated public-body payments — `data/gold/parquet/procurement_payments_fact.parquet`
- **Grain:** mixed `payment_actual` / `po_committed` per publisher (explicit `amount_semantics` col — never blur)
- **Live coverage:** 431,571 rows, 88 publishers, 2012–2026, €58.6bn sum-safe (MCP view shows 406k lines / 85 pubs after privacy/quarantine filters; 20.5k rows `public_display=False`). 41 cols incl. full confidence envelope (`value_kind`, `realisation_tier`, disclosure regime, CRO xref).
- **Surfaced:** MCP (`public_body_payments`, `top_payments`) + UI (`public_payments.py`, `follow_the_money.py`)
- **Value:** THE differentiator vs Tussell / Spend Network — realised-spend spine under awards; core of the paid BI product.
- **Already scoped:** `project_bi_spinout_architecture`, `doc/SOURCE_CONFIDENCE_SYSTEM.md`
- **Tag / effort:** CONFIRMED-LIVE · exists-just-wire (product layers)

### 2. eTenders awards — `procurement_awards.parquet`
- **Grain:** award ceiling
- **Live coverage:** 2013–2026, 44,165 awards (11,458 sum-safe = €11.76bn), 1,891 authorities, 10,017 suppliers
- **Surfaced:** MCP (`procurement_by_authority/cpv`, `access_to_contracts`) + `procurement.py`
- **Value:** market-share / incumbency analytics (incumbency, new_entrants, expiring_contracts views already registered)
- **Tag / effort:** CONFIRMED-LIVE · exists-just-wire

### 3. TED family — `ted_ie_awards / tenders / winner_history / buyer_history.parquet` (silver, runtime)
- **Grain:** TED notice (never sum with 1–2; pan-EU ceiling €1.06tn is NOT spend)
- **Live coverage:** 37,216 notices 2016–2026, €17.24bn safe ex-pan-EU; winners 2024+ only; winner_history covers 2016–23. Open tenders current (2024–25 dispatches, deadlines to 2035). Competition signal live (single-bid lot rates, 2024+).
- **Surfaced:** MCP (`open_tenders`, `procurement_competition`, `procurement_notice`) + UI. **Exception:** `ted_ie_buyer_history` is API-export only — no UI page, no MCP tool.
- **Tag / effort:** CONFIRMED-LIVE

### 4. Supplier↔CRO match confidence — `procurement_supplier_cro_match.parquet`
- **Live coverage:** in manifest, but `match_confidence` ({0.0/0.5/0.9}; 6,047 exact / 400 ambiguous / 3,532 no-match) is **dropped by every procurement view** (only `match_method` selected at `sql_views/procurement/procurement_supplier_summary.sql:53`; `match_confidence` appears in zero procurement views).
- **Value:** turns 400 silently-arbitrary company links into honest badges; prerequisite for trust-grade rails. Roadmap flagship (`doc/PROCUREMENT_INTELLIGENCE_ROADMAP.md`).
- **Tag / effort:** CONFIRMED-LIVE (gap still open) · exists-just-wire

### 5. eTenders live tenders — `silver/etenders_live_tenders.parquet`
- **Grain:** live national tender
- **Surfaced:** `procurement_live_tenders.sql` → `procurement.py`, but **NOT in `pipeline.py`** (grep = 0) — manual refresh, so currency is fragile.
- **Value:** the "a tender appeared" trigger for the tender-alert email product (`doc/TENDER_ALERT_SYSTEM_DESIGN.md` — intelligence half reusable, delivery shell greenfield)
- **Tag / effort:** CONFIRMED-LIVE (surface) / refresh-manual · moderate-build (automate + alert shell)

### 6. AFS (LA Annual Financial Statements) — `la_afs_divisions` + `la_afs_capital_divisions` + `afs_amalgamated_divisions` (silver, all runtime)
- **Grain:** AFS statement (by-division operating expenditure — NOT headline total; excludes reserve transfers, see `la_afs_metric_semantics`)
- **Live coverage:** 22/31 councils, 2016–2025 (revenue ~776 rows, capital ~782) + amalgamated national series
- **Surfaced:** **5 UI pages** (council_spending, your_council, constituency, procurement, public_payments) via 6 `procurement_afs_*` views incl. an `afs_vs_po_coverage` bridge; **NO MCP tool**
- **Value:** budget-vs-realised-spend lens per council; a small AFS MCP tool is cheap. Memory's "DEAD-END" = don't widen the 9-council OCR tail — the data itself is live and clean.
- **Tag / effort:** CONFIRMED-LIVE (memory partially STALE) · exists-just-wire (MCP tool); coverage-widening = reject

### 7. Disclosed national BQ PO extract (remainder)
- **Raw:** 582k rows / 216 bodies / 2011–2026 (`data/raw_bq/...csv`)
- **Absorbed into gold:** `disclosed_bq_po` lane 61,897 rows + `disclosed_bq_newbodies` 100,759 rows under only 16 publisher_ids — so the "+141 new bodies" are grouped / partially promoted; gold has 88 publishers total. ~420k raw rows remain un-promoted (much = HSE shared-lineage overlap, but real new-body history remains).
- **Value:** biggest cheap coverage jump for the payments spine
- **Tag / effort:** CONFIRMED-LIVE (partial) · moderate-build
- **Manifest bug:** `disclosed_bq_po_newbodies_fact` tagged **dead** though it's a consolidator input (should be lineage)

### 8. Supplier entity crosswalk — `supplier_entity_xref.parquet`
- Runtime, one view.
- **Value:** the spine asset for org-360 dossier + buyer dossier (`doc/ENTITY_CROSSWALK_ORG_DOSSIER_DESIGN.md`, `doc/BUYER_DOSSIER_DESIGN.md` — blocker is buyer-identity crosswalk, 4 new views)
- **Tag / effort:** CONFIRMED-LIVE · moderate-build

### 9. TD / Senator payments — `payments_full_psa` + `seanad_payments_full_psa`
- **Grain:** T&A allowances (4th grain — never mixed with the above)
- **Surfaced:** 7 payments views; UI `payments.py`
- **Tag / effort:** CONFIRMED-LIVE · done
- Dead cousins: `current_td/senator_payment_rankings.parquet` — superseded by `payments_zz_alltime_*` views; safe untrack candidates.

### 10. Orphaned / dead (money cluster)
- `sipo_campaign_spend_by_{category,detail,party}` ×3 — dead (election_2024 page reads `sipo_expense_*` instead)
- `cso_gfq01` quarterly gov-finance — dead (annual `cso_gfa01` **is** wired via `publicfinance_gov_finance_annual.sql` → procurement page; memory "no page yet" is STALE)
- payments quarantine ×2 (expected)
- `ted_ie_buyer_history` — API-only semi-orphan

---

## Freshness report

**STALE memory (drifted — correct these):**
- "LA payments fact unwired" → folded into gold, ~98k LA rows live
- "CSO gov finance no page" → wired
- "disclosed national PO sandbox-only" → 163k rows promoted (partially)
- "AFS dead-end" → live + surfaced on 5 pages (dead-end applies only to coverage-widening)

**CONFIRMED still true:**
- `etenders_live_tenders` not in `pipeline.py`
- `match_confidence` unsurfaced
- TED winners 2024+ only
- no email / watchlist infra (all 7 design docs exist on disk incl. `BID_INTELLIGENCE_PACK_ENGINE.md`)

**Surfaced NOWHERE:** `ted_ie_buyer_history` (API export only), `sipo_campaign_spend` ×3, td/senator payment rankings ×2, `cso_gfq01`. Manifest misclassifies `disclosed_bq_po_newbodies_fact` as dead.

**Not sampled:** did not re-profile CSO deflator tables or SIPO election money beyond the manifest. "Billing" as a named dataset does not exist — the payments corpus **is** the billing layer.

---

## Top 5 moves (value ÷ effort)

1. **Wire `match_confidence` into supplier views / UI** — one SQL column + a badge; fixes 400 silently-wrong-looking links (roadmap flagship).
2. **Automate `etenders_live_tenders` into `pipeline.py`** — unblocks the tender-alert product's trigger for near-zero build.
3. **Absorb remaining disclosed-BQ new-body history** into `procurement_payments_fact` — largest coverage jump on the crown-jewel spine.
4. **Small AFS MCP tool + AFS-vs-PO lens promotion** — view already exists (`procurement_afs_vs_po_coverage.sql`); makes the only un-MCP'd money family queryable.
5. **Tender-alert email MVP** per `doc/TENDER_ALERT_SYSTEM_DESIGN.md` — intelligence half is reuse; only the delivery shell is greenfield; first paid-product surface.
