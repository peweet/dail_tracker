---
tier: SPEC
status: LIVE
domain: procurement
updated: 2026-06-29
supersedes: []
read_when: building or evaluating the bid-intelligence pack engine (auto-assembled market-research pack for a matched tender)
key: SPEC|LIVE|procurement
---

# Bid-Intelligence Pack — Assessment & Engine

**Date:** 2026-06-29
**Status:** ASSESSMENT + working SANDBOX prototype. The prototype is boundary-safe and
runnable now; everything beyond it (productisation: feed reliability, accounts, alerts,
API/MCP exposure, real-terms wiring) is **owner-gated** per the sibling docs below.

> **Distinct angle of this doc.** It looks at the data through ONE lens: a *workflow product
> for contractors / bid managers / commercial managers / QS / SME suppliers* — "when a relevant
> tender appears, auto-assemble the historical market-research pack a human would otherwise build
> by hand." It is NOT a generic procurement dashboard.
>
> Reads alongside, and does not duplicate:
> - [`BI_SPINOUT_ARCHITECTURE.md`](BI_SPINOUT_ARCHITECTURE.md) — the commercial/business model, pricing, ethics line, brand split.
> - [`PROCUREMENT_INTELLIGENCE_ROADMAP.md`](PROCUREMENT_INTELLIGENCE_ROADMAP.md) — the UI surfacing phases (0–5).
> - [`TENDER_ALERT_SYSTEM_DESIGN.md`](TENDER_ALERT_SYSTEM_DESIGN.md) — the alert/email delivery shell (Phase 5).
> - [`BUYER_DOSSIER_DESIGN.md`](BUYER_DOSSIER_DESIGN.md) — buyer dossier + the buyer-identity crosswalk blocker.
> - [`PROCUREMENT_INFLATION_BENCHMARKING_DESIGN.md`](PROCUREMENT_INFLATION_BENCHMARKING_DESIGN.md) — the real-terms deflator design.
> - [`DATA_LIMITATIONS.md`](DATA_LIMITATIONS.md) §16–§17 + [`DATA_GRAINS.md`](DATA_GRAINS.md) — the never-sum money-grain rules this engine obeys.
>
> This doc is the **composition-engine + field-inventory + must-not-claim** layer. The alert system
> calls this engine per matched tender; the BI-spinout doc decides whether/how to sell it.

---

## 1. Commercial viability verdict

**Viable as a "historical market-research pack + bid/no-bid context" product. NOT viable — and must
never be sold — as a "what should I bid" pricing engine.**

- The analytical engine is ~80% built; the *product* (trigger-feed reliability + delivery) is ~10% built.
- The **moat** is the public-body PAYMENT spine (~424k lines) layered on awards: it shows *what a buyer
  actually paid a supplier after award*, which awards-only competitors (Tussell, Stotles, Spend Network)
  cannot cheaply replicate. (Same conclusion as `BI_SPINOUT_ARCHITECTURE.md` §0.)
- The **risk** is over-claiming pricing precision — which the codebase already refuses to do (the
  `v_procurement_bid_signal` docstring; the `project_value_estimate` MCP tool flagged unreliable).

Two honest weaknesses gate the "product":
1. **The trigger is not production-grade.** The live national-tender feed (`etenders_live_tenders`) is an
   experimental Playwright scraper, **manually run, not in `pipeline.py`**, with CPV ~0% filled.
2. **Zero delivery infra.** No accounts, watchlists, CPV subscriptions, scheduler, or email path
   (the gated Phase 5 — see `TENDER_ALERT_SYSTEM_DESIGN.md`).

---

## 2. Existing data assets

| Asset (table / view) | Layer · rows | Grain | What it gives a bidder | Key limit |
|---|---|---|---|---|
| eTenders national awards `procurement_awards` | gold · 62,763 | award × supplier (2013–2026) | supplier, contracting authority, award value, **No of Bids Received**, **SME bids / awarded SMEs**, procedure, competition type, framework/DPS flag, contract duration, CPV, title, TED links | **~71% CPV null**; value = award *ceiling*, not paid |
| TED EU awards `ted_ie_awards` + `ted_ie_winner_history` (`v_procurement_ted_winner_history`) | silver · ~37k | notice × winner (2016–2026) | buyer, winner, value, CPV+division, **procedure_type, is_uncompetitive_procedure, n_tenders_received, is_single_bid, award_criteria, is_price_only**, CRO | competition fields **2024+ only**; pan-EU outliers excluded |
| TED pre-award tenders `ted_ie_tenders` | silver · 12,902 | notice | buyer, CPV, procedure, **submission_deadline, is_still_open**, estimated value | EU-threshold only; estimate never summable |
| Live national tenders `etenders_live_tenders` | silver · 2,363 · **EXPERIMENTAL** | open opportunity | title, buyer, deadline, procedure, est value, detail_url; **sub-threshold** | **not in `pipeline.py`**, manual; **CPV ~0% filled** |
| Consolidated payments/PO fact `procurement_payments_fact` | gold · ~424k | payment/PO line | publisher, supplier, amount, **value_kind (payment_actual/po_committed)**, realisation_tier (SPENT/COMMITTED), vat_status, description, po_number, paid_flag, disclosure_basis, spend_category, cro, source_file_url | **no CPV**; extraction *floor*; CRO ~46%; never sum tiers/VAT |
| Supplier ↔ CRO match `procurement_supplier_cro_match` | gold · 9,979 | supplier | company number/status + `match_confidence` (0.0/0.5/0.9) | ~46–61% match; LLPs/truncated fail |
| Cross-register chain `v_procurement_entity_chain` | view | CRO company | one firm across eTenders + TED + payments, side by side | 3 grains, never summed |
| Derived signals | views | various | `bid_signal` (per-CPV award band + ceiling band + median bids + single-bid % + **SME win %**), `competition`/`competition_by_cpv`, `incumbency`, `supplier_dependency`, `new_entrants`, `quarter_profile`, `call_off_links`, `expiring_contracts(_etenders)` | all "signal, not verdict" |
| Enrichment overlays | gold | supplier | lobbying overlap, EPA compliance, charity overlap, corporate groups | co-occurrence only (ethics line — `BI_SPINOUT` §4) |
| Inflation deflators `cso_cpa07`, `cso_cpi_deflator` (+ WPM39 construction WPI) | gold | year | basis for **real-terms benchmarking** | no procurement real-terms view yet — see `PROCUREMENT_INFLATION_BENCHMARKING_DESIGN.md` |

**Extractor chains:** `procurement_etenders_extract.py` → awards; `ted_ireland_*` → TED;
`procurement_public_body_extract.py` / `procurement_la_payments_extract.py` /
`procurement_hse_tusla_materialize.py` / `disclosed_bq_po_extract.py` (+ NTA/NPHDB/SEAI/dept-readingorder)
→ silver payment facts; `procurement_payments_consolidate.py` → gold payment fact. The **only experimental
trigger-path** extractor is `etenders_live_tenders_extract.py`.

**Field availability (point 4 of the brief):**

| Field | Where | Notes |
|---|---|---|
| Supplier | awards (`supplier`), TED (`winner_name`), payments (`supplier_raw/normalised`) | individuals masked/quarantined |
| Buyer / authority | awards (`contracting_authority`), TED (`buyer_name`), payments (`publisher_name`) | **3 different keys, no shared id** |
| CPV (+ division) | awards, TED, bid_signal | ~71% null on national awards |
| Title | awards (`tender_title`), live (`title`), payments (`description`) | |
| Award value | awards (`value_eur`), TED (`award_value_eur`), tenders (`estimated_value_eur`) | ceiling/estimate, `value_safe_to_sum` |
| No. of bids | awards (`n_bids_received`), TED (`n_tenders_received`), bid_signal (`median_bids`) | TED bids 2024+ |
| SME | awards (`n_sme_bids_received`, `n_awarded_smes`), bid_signal (`sme_win_pct`) | |
| Framework / procedure / competition | awards (`is_framework_or_dps`, `procedure`, `competition_type`), TED (`procedure_type`, `is_uncompetitive_procedure`) | |
| Payment / PO value | payments (`amount_eur`, `value_kind`, `realisation_tier`, `po_number`) | SPENT vs COMMITTED |
| Source links | awards (`etenders_notice_url`, `ted_*_link`), payments (`source_file_url`), TED (`notice_url`) | |
| Dates | awards (`award_date`), TED (`dispatch_date`, contract dates), live (`deadline`), payments (`period/year/quarter`) | |
| Value safety | `value_safe_to_sum`, `value_kind`, `realisation_tier` everywhere | the never-sum spine |

---

## 3. Missing data / features

| Gap | Type | Severity | Note |
|---|---|---|---|
| Tender→history **pack builder** | feature | Critical | **Now prototyped** — see §5. |
| Reliable live-tender feed | data/infra | Critical | `etenders_live_tenders` experimental, manual, CPV-null. |
| Accounts + saved CPV/buyer watchlists | infra | Critical | none today (anonymous, cookieless). |
| Alerting + email delivery | infra | Critical | see `TENDER_ALERT_SYSTEM_DESIGN.md` (Phase 5). |
| CPV coverage on national awards | data quality | High | 71% null — mitigate with buyer + title-keyword match. |
| CPV enrichment on the live feed | data quality | High | needed to route tenders to subscriptions. |
| Real-terms benchmark view | feature | Medium | deflators exist; design in `PROCUREMENT_INFLATION_BENCHMARKING_DESIGN.md`. |
| Title/keyword FTS | feature | Medium | for null-CPV comparable matching. |
| Buyer-identity crosswalk (award↔payment↔TED names) | data | High | the blocker in `BUYER_DOSSIER_DESIGN.md`; the pack handles it honestly (name-key gap note). |
| Win-probability / bid-price model | — | **Do not build** | see §7. |

---

## 4. What the system can already answer for a contractor

Given a CPV / buyer / supplier, today, with the existing queries: who else wins this work; what this buyer
buys, from whom, how often; typical contract value (p25/median/p75, ceilings shown separately); how
competitive the buyer/market is (single-bid rate vs national baseline); SME winnability; incumbent
entrenchment; **what a supplier was actually paid (not just awarded)**; which contracts are ending soon
(re-bid windows); and competitor flags (lobbying/EPA/CRO). That is most of a manual QS research pack —
it just wasn't *assembled on a trigger and delivered*.

---

## 5. The bid-pack engine (built — sandbox prototype)

Location: [`pipeline_sandbox/bid_intelligence/`](../pipeline_sandbox/bid_intelligence/). Boundary-safe:
imports the existing production query functions **read-only and composes them** — writes nothing to
gold/silver, registers no view, modifies no production file.

`build_bid_pack(conn, cpv_code=, buyer=)` assembles, from existing registered views only:

| Block | Source query | Grain |
|---|---|---|
| `comparable_awards` (CPV-only, buyer-only, or **buyer ∩ CPV**) | `awards_for_cpv` / `awards_for_authority` | AWARD (ceiling/estimate) |
| `category_benchmark` (median / p25 / p75) | `cpv_summary` | AWARD |
| `market_signal` (award band + ceiling band + median bids + single-bid % + SME win %) | `bid_signal` (trade = CPV first 4 digits) | AWARD + competition |
| `buyer_awards` / `buyer_payments` (SPENT + COMMITTED separate) | `authority_summary` + `payments_publisher_profile` | AWARD / PAYMENT |
| `active_firms` (companies only) | derived count from `comparable_awards` | — |
| `incumbent_payment_evidence` (per leading firm) | `payments_for_supplier` | PAYMENT |
| `rebid_radar` | `expiring_contracts_etenders` filtered to the context | AWARD (advertised term) |

`render_report.py` turns a pack into a client-facing Markdown report (the *reports-first* artifact in
`BI_SPINOUT_ARCHITECTURE.md` §15). Run + self-checks:

```bash
PYTHONUTF8=1 .venv/Scripts/python pipeline_sandbox/bid_intelligence/run_demo.py
# writes sample_pack.json (structure) + sample_report.md (rendered report)
```

**Verified against live data** (CPV 72000000 IT services × Dublin City Council): median award €349,644
(IQR €119k–€1.02m), median 8 bids, 9.2% single-bid, 52.3% SME win; framework-ceiling band carried
separately (€520,800 median); Grant Thornton payment evidence €19.9m committed / €9.8m paid. Grain
discipline holds — DCC shows **€0 paid · €4.06bn ordered** (it publishes POs, not payments) as two
separate figures.

### Proposed MVP workflow (engine + delivery)
1. **Harden the trigger** — promote `etenders_live_tenders` to a scheduled LIVE chain + CPV-enrich it; add LIVE `ted_ie_tenders` as a second source.
2. **Subscriptions** — user saves watchlists by CPV / buyer / keyword.
3. **Match** new open tenders to subscribers (fallback to buyer + title-keyword when CPV null).
4. **Assemble** the pack via `build_bid_pack` (done).
5. **Deliver** as email/PDF digest + web pack page; end with a human bid/no-bid worksheet — never a recommendation.

---

## 6. API / query functions needed (net-new)

Most exist; the gap is orchestration + delivery:
- `build_bid_pack(...)` — **prototyped** (graduate into `dail_tracker_core/dossiers.py`).
- `match_tender_to_watchlists(tender)` / `live_tenders_since(ts)` — new (alerting loop).
- `comparable_awards(..., real_terms=False)` — wrapper adding the deflator join (new `v_procurement_awards_real_terms`).
- `award_title_search(q)` — FTS for null-CPV matching.
- Expose: `/v1/procurement/bid-pack` route + an MCP tool (after graduation).
- Watchlist/account CRUD + scheduler + email renderer — **infra, owner-gated.**

Reuse as-is: `cpv_summary`, `bid_signal`, `competition(_by_cpv)`, `incumbency_for_supplier`,
`dependency_for_supplier`, `payments_for_supplier`, `payment_lines_for_pair`,
`payments_publisher_profile`, `expiring_contracts_etenders`, `ted_for_supplier`,
`entity_chain_for_company`, `live_tenders`.

---

## 7. ⚠️ What the product must NOT claim (hard rails — enforced in the prototype)

- **Not a bid price; not a win probability.** The data cannot quote a job — no project size/area
  anywhere; 4.5×–15× intra-trade spread; framework ceilings 14×–79× above real awards
  (`v_procurement_bid_signal` docstring). `project_value_estimate` (MCP) is flagged unreliable — do not surface it.
- **Never sum/blend the three money grains.** eTenders awards, EU/TED awards, public-body payments
  (SPENT vs COMMITTED) stay separate, labelled. TED and eTenders are siblings (~66% overlap) — never unioned;
  pan-EU outliers excluded. Naive sums overstate ~24–40×.
- **No completeness claim.** Payments are extraction *floors* (~7% of state spend); 71% of national CPVs null;
  TED winners null pre-2024. Frame as "from the public record we hold."
- **No collusion/favouritism inference** from single-bid / incumbency / dependency / Q4 / lobbying overlap —
  structure facts, never verdicts.
- **No competitor turnover/financial-health** from award/payment values; **no private-sector work** is visible.
- **No individual profiling** — sole-trader/individual awardees excluded from competitor + payment blocks and name-masked in listings.
- **No buyer intent/budget/contact** that isn't in the data.

Safe promise: *"We assemble the historical market-research pack — comparable awards, buyer purchasing
history, likely incumbents, competition climate, inflation-adjusted benchmarks, and actual payment
evidence — so your bid manager and QS judge instead of gather. We do not price your bid."*

---

## 8. Graduation path (owner sign-off required)

1. Move `build_bid_pack` into `dail_tracker_core/dossiers.py`; add `/v1/procurement/bid-pack` + MCP tool + a test under `test/`.
2. Resolve the buyer-identity crosswalk (`BUYER_DOSSIER_DESIGN.md`) so `buyer_payments` matches reliably.
3. Wire the real-terms benchmark (`PROCUREMENT_INFLATION_BENCHMARKING_DESIGN.md`).
4. Promote + CPV-enrich the live-tender feed.
5. Build accounts / watchlists / scheduled diff + email (`TENDER_ALERT_SYSTEM_DESIGN.md`, Phase 5) — PII/consent/GDPR + scheduler.
