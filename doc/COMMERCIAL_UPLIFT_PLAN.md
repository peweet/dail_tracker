# Commercial Uplift Plan — API links, procurement exports, MCP

Date: 2026-06-10. Scope: make the API/export/MCP channels commercially credible,
mirroring what Tussell and Stotles actually sell. The Streamlit app itself stays
free and is explicitly OUT of scope here (UI polish tracked separately).

Every change below was assumption-checked against real data before being planned.
Sections marked **SHIPPED** were implemented alongside this doc; the rest are
sequenced plans.

---

## 0. Assumption-check log (what changed vs the 2026-06-10 assessment)

| Claim in assessment | Verified result | Consequence |
|---|---|---|
| "Procurement is 164 days stale — pipeline halted" | **WRONG.** Pipeline fetched 2026-06-07 (fetch age 0). The OGP open-data CSV is a quarterly export with an inherent ~6-month lag — a forced re-download on 2026-06-06 still maxed at 2025-12-24 (`tools/check_freshness.py` gates on fetch age for exactly this reason; data.gov.ie confirms "updated quarterly", resource created 2025-10-10). | Fix is NOT a pipeline repair. Fix = (a) live-lane compensation (TED awards are current to 2026-06-05), (b) honest `data_currency` disclosure on every export (§4). |
| "VAT status unknown on 87% of payment rows" | **CONFIRMED**: 145,528 `unknown` across 55 publishers; 21,662 `incl_vat` (HSE 16,972 + Tusla 4,690 only). | VAT matrix + caveat (§3). |
| "HSE/Tusla individual-carer privacy review pending" | **ALREADY RESOLVED** in gold: all sole_trader_or_individual rows for HSE (6,072), Tusla (1,276), CHI (55), HPRA (30) carry `public_display = False`. | No action; memory updated. |
| "Sole-trader dossier URL may leak a person's full award history" | **CONFIRMED REAL.** `v_procurement_supplier_summary` filters `supplier_class='company'`, but `awards_for_supplier()` queries the unfiltered `v_procurement_awards`, and `build_supplier_dossier` returns `{"summary": None, "awards": [...]}`. 27,674 award rows are sole_trader_or_individual. Affects `/v1/procurement/suppliers/{norm}/dossier` AND the Streamlit drill-down (both call the same core query). | Guard at the query layer (§2). **SHIPPED** |
| "MCP server is out of repo, stdio-only" | **CONFIRMED**: `C:\tmp\dail_mcp\mcp_server.py` (880 lines, 39 tools + 6 prompts + 1 resource), hardcoded repo path, stdio transport. Originally out-of-repo by design (experiment isolation); it is now a product channel. | Move in-repo under CI (§5). **SHIPPED** |
| "No export mechanism exists" | **CONFIRMED**: no download endpoints, no UI buttons, no snapshots. | §4. **SHIPPED** |
| *(found while building §4)* gold privacy invariant held | **BREACHED**: 830 rows with `privacy_status='review_personal_data'` carried `public_display=True` in the gold payments fact (nta/nphdb/seai `*_reading_order` parsers set the status but not the gate; the consolidator trusted the incoming flag) — and the base view's `public_display` gate made those person-rows VISIBLE in the app/API. Caught by the §4 snapshot-leak test. | Fixed 3 ways 2026-06-11: gold repaired in place (830→0, row count unchanged); consolidator now re-derives `public_display` in `_conform` + refuses to write on breach (mirrors the extractor invariant); export filter carries the person-predicates as defense in depth. |
| TED tenders lane currency | Latest dispatch 2026-03-25 despite retrieval 2026-06-08 — the forward-pipeline lane is ~10 weeks behind even though the awards lane is current. Needs a look at the tenders query window/paginator. | Investigate in §7.1. |

---

## 1. What Tussell and Stotles actually sell (the mirror)

Researched 2026-06-10. Sources:
[Tussell plans](https://www.tussell.com/plans), [Tussell Insight](https://www.tussell.com/products/tussell-insight),
[Tussell Early Opportunities](https://www.tussell.com/products/tussell-early-opportunities),
[Stotles platform](https://www.stotles.com/platform), [Stotles pricing](https://www.stotles.com/pricing).

| Product element | Tussell | Stotles | Dáil Tracker today | Gap? |
|---|---|---|---|---|
| Aggregated tender feed (all portals, one feed) | yes | yes (UK **and Ireland**) | TED lane only (above-threshold); eTenders lane is quarterly + ~6mo lag | **GAP — the critical one** (§7.1) |
| Contract awards history | yes | yes | Strong: eTenders 59,439 + TED 13,230 + TED pre-2024 winner history 23,263 (unique) | parity/better |
| **Expiring contracts → re-tender alerts** | yes | yes (flagship signal) | **No end-date/duration fields anywhere** (verified: neither gold has them; eForms BT-36 / per-notice XML carries it) | **GAP** (§7.2) |
| Framework intelligence (lots, renewals, call-offs) | yes | yes (G-Cloud/DOS equivalents) | `is_framework_or_dps`, `Parent Agreement ID`, call-off flags exist; no framework registry/expiry view | **partial GAP** (§7.3) |
| Invoices / actual spend | yes (UK spend data) | no | **Strength**: payments fact 167k rows / 57 publishers / €15bn SPENT tier + LA payments | Dáil Tracker AHEAD |
| Buyer profiles + budgets | yes | yes (budgets, meeting minutes) | LA AFS revenue+capital built (silver); NTA minutes scoped | partial (§7.4) |
| Decision-maker contacts (80k+) | yes (credits) | yes (Expert tier) | none | **deliberate NON-GOAL** — GDPR posture + civic trust; do not build |
| Early/pre-tender opportunities (PINs, pipelines) | yes (add-on) | yes (signals) | TED PINs not ingested | GAP (§7.5) |
| AI bid qualification / bid library | no | yes | non-goal (workflow SaaS, different business) | skip |
| Alerts / saved searches | yes | yes | none (no user accounts) | later; needs keys first (§6) |
| API access | Pro tier add-on | integrations | built, not deployed | §6 |
| Competition analytics (single-bid rates) | reports | partial | **Strength**: per-buyer single-bid rate shipped (signal-not-verdict) | Dáil Tracker AHEAD |
| Supplier→registry enrichment | Companies House | partial | CRO match 50–65% + lobbying overlap + charity overlap | Dáil Tracker AHEAD (unique joins) |
| Price anchors | Starter→Pro tiers (£k/yr) | Growth from £475/mo; free tier | n/a | informs eventual NC-carve-out pricing |

**Positioning conclusion.** Dáil Tracker cannot (and should not) chase contacts or
bid-workflow SaaS. Its defensible mirror of Tussell/Stotles is the **data layer**:
awards + spend + competition quality + enrichment joins, exported cleanly with
honest value semantics. The two data gaps that matter commercially are **live
below-threshold tenders** and **contract end dates** (the expiring-contracts
signal). An Irish micro-competitor exists (TenderWatch.ie — eTenders email alerts),
which validates demand and confirms the alerting niche is reachable from public
sources.

---

## 2. Sole-trader dossier privacy guard — **SHIPPED**

- `dail_tracker_core/queries/procurement.py::awards_for_supplier` now excludes
  `supplier_class = 'sole_trader_or_individual'` — one filter protects both the
  API dossier and the Streamlit drill-down (single shared code path).
- Effect: `/v1/procurement/suppliers/{norm}/dossier` → 404 for a natural person;
  ranking views were already company-only so no UX regression.
- Rationale: row-level naming follows the published source (Circular 07/2012
  precedent), but a *composed per-person award dossier* is profile-building on a
  natural person — different GDPR character, quarantined at the query layer.
- Tests: core-level (sole-trader norm returns no rows) + API-level (dossier 404)
  in `test/dail_tracker_core/test_core_procurement_queries.py` and
  `test/api/test_api_extra.py`, real-data with skip guards.

## 3. Per-publisher VAT-basis matrix — **SHIPPED**

Problem (verified): 87% of payment rows have `vat_status='unknown'`; only HSE and
Tusla are confirmed `incl_vat`. Cross-publisher € totals silently mix VAT bases.

- `tools/build_vat_matrix.py` reads the gold payments fact and writes
  `data/_meta/procurement_payments_vat_matrix.json`: one entry per publisher —
  vat_status mix, row count, safe-to-sum value, years covered.
- A `vat_caveat` string now rides on public-payments responses in the core
  composition layer (same pattern as `_PROC_LOBBY_CAVEAT`), and the matrix is
  referenced by the export manifest (§4) so bulk consumers get it without asking.
- Closing the `unknown`s is a per-publisher documentation chore (read each
  publisher's notes page; record incl/excl). Tracked as backlog — the matrix
  makes the gap explicit instead of silent.

## 4. Bulk data exports `/v1/data` — **SHIPPED**

The product Tussell/Stotles monetize is clean, refreshed, well-documented bulk
data. Design (default-deny allow-list):

- `GET /v1/data` — manifest: every exportable resource with description, row
  count, licence + attribution text, **data_currency** (max record date AND last
  fetch — the honest two-clock disclosure the OGP lag demands), generation time,
  and the never-sum caveats for its value grain.
- `GET /v1/data/{resource}` — parquet download (`?format=csv` available).
- **Allow-listed**: procurement_awards, procurement_supplier_cro_match,
  ted_awards, ted_winner_history, ted_buyer_history, ted_tenders,
  procurement_payments_fact, procurement_lobbying_overlap.
- **Privacy filters baked into the snapshot, not the docs**: rows with
  `public_display = False` (payments) and `supplier_class =
  'sole_trader_or_individual'` / `privacy_status = 'review_personal_data'`
  (awards/TED) are EXCLUDED from generated export files. Filtered snapshots are
  materialized to `data/_export_cache/` keyed on source mtime and re-cut when the
  gold changes.
- **Hard-excluded forever** (tested, not just documented): anything SIPO-donor
  (addresses = PII), corporate_notices (personal-insolvency quarantine lives at
  view level, so the raw parquet must never ship), member interests, judiciary.
- Tests assert: manifest lists only allow-listed names; excluded names 404; a
  re-read of the generated payments snapshot contains zero `public_display=False`
  rows and zero sole-trader rows.

## 5. MCP server into the repo — **SHIPPED**

- `mcp_server/server.py` (+ `ted_conduit.py`, `qs_valuation.py`) moved from
  `C:\tmp\dail_mcp`; hardcoded `REPO` path replaced with `Path(__file__)`
  resolution; header rewritten (it is now version-controlled product surface).
- `[project.optional-dependencies] mcp` added — the app/Cloud deploy never
  installs it (same isolation the old out-of-repo location bought, without the
  single-disk-failure risk).
- Import smoke test (`pytest.importorskip("mcp")`) asserts the tool registry
  loads and tool count ≥ 39.
- Remote transport (streamable HTTP), API keys, and audit logging are §6 — do
  NOT expose this server beyond stdio until those exist.

## 6. API deployment — Dockerfile **SHIPPED**, hosting next

- `Dockerfile` (uv-based, `--extra api`, gold/silver parquet + sql_views baked
  into the image, uvicorn on :8080) + `.dockerignore` (bronze, logs, tests,
  screenshots excluded) + `deploy/fly.toml.example`.
- Hosting order: deploy container (Fly/Render) → put the existing Cloudflare
  zone in front (`api.dailtracker.ie`) → CDN handles rate limiting, CORS, and
  cache headers (Decision D9 in `doc/API_LAYER_PLAN.md` already delegates these).
- Keys/metering stay demand-gated (OpenSanctions NC-carve-out model, never a
  metered API). Alerts/saved searches only make sense after keys exist.

## 7. Data-gap build plans (the Tussell/Stotles parity backlog — NOT yet built)

### 7.1 Live below-threshold tender feed (top priority)
The eTenders open-data CSV can never power live alerts (quarterly + ~6mo lag).
The live surface is the eTenders platform itself (European Dynamics, migrated
2023). Plan: poll the platform's public current-notices listing (check RSS first
— ED platforms usually ship one; else the public search endpoint), normalise
into a `etenders_live_notices` silver lane (PLANNED tier, estimate_advertised,
never summed), reconcile against the quarterly CSV when it lands (the CSV
remains the authoritative awards record). Respect robots.txt + polite-bot rules
(single HTTP helper). TED tenders lane already covers above-threshold — but it
is itself 10 weeks behind (latest dispatch 2026-03-25 vs retrieval 2026-06-08);
diagnose the paginator/window in `extractors/ted_ireland_tenders_extract.py`
first — that may be a quick win.

### 7.2 Contract end dates → expiring-contracts signal (the Stotles flagship)
Verified: no duration/end-date columns in either gold. Sources that carry it:
TED eForms duration fields (BT-36/BT-536/BT-537) via per-notice XML — the
`winner_history` extractor already parses per-notice XML, so extend that parser
to award notices 2024+ and emit `contract_start/end/duration_months`. Then a
`v_procurement_expiring_contracts` view (contracts ending in next 6/12 months,
by buyer/CPV) — pure fact presentation ("contract ends 2026-11"), no inference
about re-tendering, consistent with the no-inference rule. eTenders CSV has no
duration; live lane (§7.1) may add it for below-threshold later.

### 7.3 Framework registry
`Parent Agreement ID` + `is_framework_or_dps` + `is_call_off` already exist on
the eTenders gold. Build a `v_procurement_frameworks` view: one row per
framework (parent agreement) with member suppliers, call-off counts/values,
first/last activity. Combined with §7.2 end dates this becomes framework-expiry
intelligence — both comparators charge for exactly this.

### 7.4 Buyer profiles
Join existing assets per contracting authority: awards + single-bid rate +
payments (where the buyer is also a publisher) + LA AFS division spend for
councils. Mostly a view + dossier composition job (`/v1/procurement/authorities/{id}/dossier`);
no new ingestion. Decision-maker contacts remain a non-goal.

### 7.5 TED PINs (early opportunities)
Prior Information Notices are in the same TED API the awards lane uses (notice
type filter). Small extractor delta; PLANNED tier semantics; powers the §7.1
forward pipeline for above-threshold buys ~6–12 months before tender.

### 7.6 Operational credibility items
- `public_payments_fact` consolidation is now gold with locked schema; the
  sandbox-era `amount_semantics` drift is mapped at consolidation. Remaining
  churn risk is per-publisher parser drift → golden-file fixtures per publisher
  (extend `test/extractors/test_procurement_payments_fact.py`).
- Source-health link checks: 104/107 sources skipped ("link check disabled").
  Enable in batches with the polite HTTP helper; gate CI with `--strict` only
  after a clean week.
- VAT `unknown` closure: per-publisher documentation sweep (§3).

## 8. Sequencing

1. **Done in this pass**: §2 privacy guard, §3 VAT matrix, §4 exports, §5 MCP
   in-repo, §6 Dockerfile.
2. **Next (days)**: deploy API container + Cloudflare front; fix TED tenders
   lane recency (§7.1 quick win); TED award end-dates spike (§7.2, one notice
   XML → confirm BT-36 presence).
3. **Then (weeks)**: eTenders live lane (§7.1), expiring-contracts view (§7.2),
   framework registry (§7.3), buyer dossiers (§7.4), PINs (§7.5).
4. **Demand-gated**: keys, alerts, NC carve-out licensing, MCP remote transport.
