---
tier: RECORD
status: LIVE
domain: commercial
updated: 2026-07-09
supersedes: []
read_when: sizing the BI spinout market/TAM or unit economics, or picking up the unfinished competitive-landscape section before a pricing decision
key: RECORD|LIVE|commercial
---

# BI Spinout — Blind-Spot Analyses

**Date:** 2026-07-09 · **Status:** fills the four blind spots flagged in [doc/BI_SPINOUT_FABLE_ASSESSMENT.md](BI_SPINOUT_FABLE_ASSESSMENT.md) §6.
**Method:** web + `dail-tracker` MCP researched, adversarially verified for sourcing/rigour. Every external fact carries a URL; every projection is labelled an ESTIMATE with assumptions.
**Note:** the competitive-scan section is PENDING (research agent hit the session usage limit); re-run after reset. The other three sections completed and were finalized.
**Cross-links:** [doc/BI_SPINOUT_ARCHITECTURE.md](BI_SPINOUT_ARCHITECTURE.md) · [doc/BI_SPINOUT_STAGE_AB_PLAN.md](BI_SPINOUT_STAGE_AB_PLAN.md)

---

## Irish procurement-intelligence competitive landscape

> **PENDING — not yet generated.** The research agent for this section hit the session usage limit before completing. Re-run to fill it: `Workflow({scriptPath: '.../bi-spinout-blind-spots-wf_809675e3-9a0.js', resumeFromRunId: 'wf_809675e3-9a0'})` — the three completed sections replay from cache; only this section re-runs.

_Scope when re-run: Stotles, Tussell, BiP/Tracker Intelligence, Tenders Direct (Millstream), official eTenders/TED alerts, Irish-specific players — Irish coverage, features, public pricing, gaps, and where this product can/cannot differentiate; implication for the SME tier._

---

All internal counts verify against the live gold data (44,165 award rows 2013–2026; 10,017 suppliers; 1,891 authorities; 27,775 payment suppliers; TED 1,512 buyers / 14,630 winners). No invented figures, sources present, estimates labelled. Outputting the finalized section.

## Bottom-up TAM and unit economics

*Every headcount below is an ESTIMATE unless it carries a source; assumptions are stated inline. External facts are cited; the four internal counts are anchored to this project's own gold procurement data (via the `dail-tracker` MCP `data_coverage` tool: 44,165 award rows, 2013–2026).*

### 1. Market sizing — who could plausibly pay

| Segment | Count | Basis / assumption |
|---|---|---|
| Irish bid-writing / tender consultancies | **ESTIMATE: 30–60 firms + solo practitioners** | Web search surfaces ~8–10 dedicated firms by name (Tender Team, Bid Services, RFx Procurement, Bid Specialists, Tsaks Dublin, ContentWriterIreland, eTenders.ie consulting). Extrapolating to unlisted freelancers/boutiques and NI-based firms serving ROI gives 30–60. This is the *only* segment whose core business is tenders — highest willingness-to-pay, smallest count. |
| SME suppliers "active enough" to pay | **ESTIMATE: 1,500–3,000** | Project data shows **10,017 distinct suppliers** have won an eTenders award since 2013. Most are one-time or occasional winners. Filtering to repeat bidders (≥3 awards, or bidding several times a year) plausibly leaves ~15–30%. Against a base of 400,424 Irish SMEs ([CSO / Irish Times, Dec 2025](https://www.irishtimes.com/business/2025/12/15/businesses-operating-in-economy-rises-above-400000-for-first-time/)) — 92.6% micro — this is a tiny, hard-to-reach slice. SMEs win 25% of above-threshold and 53% of below-threshold contracts ([OGP/gov.ie](https://www.gov.ie/en/office-of-government-procurement/publications/smes/)), but *winning occasionally ≠ paying for intelligence*. |
| Enterprise / large-supplier accounts | **ESTIMATE: 100–300** | Big contractors, FM, IT, pharma, and the professional-services firms with dedicated bid teams (e.g. Deloitte Ireland: 349 awards across 55 authorities in project data). These have budgets but also the fewest of them, and many already buy Tussell/Stotles. |
| Public-sector buyer bodies | **1,891 contracting authorities** (project data; TED shows 1,512 EU-threshold buyers). Realistically payable: **ESTIMATE 20–80** | Five central purchasing bodies + 31 local authorities + larger agencies ([gov.ie ABCs of Procurement](https://www.gov.ie/en/office-of-government-procurement/publications/abcs-of-public-procurement/)). But buyers *are the data source*; most will not pay to see their own market. A benchmarking/spend-analytics angle might reach a few dozen. |

Context: Irish public procurement is **~€19–22bn/yr, ~12% of GDP** ([trade.gov](https://www.trade.gov/country-commercial-guides/ireland-selling-public-sector); [council.ie](https://council.ie/public-procurement-in-ireland-a-critical-review/)). Large spend, but the *paying audience for intelligence about it* is small.

### 2. Hours per report (incl. QA) — ESTIMATES

Assumes founder-level fluency with the existing pipeline/views, so data assembly is fast; QA is the tax because provenance must be data-anchored (project rule: no invented figures).

| Report type | Price | Prod. hrs | QA hrs | **Total** |
|---|---|---|---|---|
| A. Supplier/market snapshot (templated) | €1.5k | 6–10 | 2–4 | **~10 hrs** |
| B. Category / CPV market deep-dive | €3–6k | 18–28 | 6–10 | **~30 hrs** |
| C. Bespoke competitor/buyer dossier (strategic) | €8–10k | 40–60 | 10–20 | **~65 hrs** |

Blended: ~€3.5k at ~30 hrs → **~€115/productive hr gross** (before sales, tooling, tax).

### 3. ARR build-up — per-tier count × price (ESTIMATE)

Prices benchmarked to the only public comparator: **Stotles Basic £50/user/mo, Growth from £475/mo, Expert custom** ([stotles.com/pricing](https://www.stotles.com/pricing)); **Tussell** is premium with no public price ([tussell.com/plans](https://www.tussell.com/plans)). An Irish-only product should price *below* these.

**Serviceable TAM if fully penetrated (not achievable solo):**
- Consultancies: 40 × €3k = €120k
- Enterprise: 200 × €5k = €1.0m
- Active SMEs: 2,000 × €800 = €1.6m
- Buyers: 40 × €5k = €200k
- **Full-penetration TAM ≈ €2.9m ARR** — theoretical ceiling only.

**Realistic solo capture (5–15% penetration), Year 3:**
- Reports: 18–24 reports/yr @ €3.5k blended = **€65–85k**
- Metered API: 5–15 accounts @ €2–4k = **€15–45k** (labelled ESTIMATE; no Irish comparator exists — reasoned range)
- Subscriptions (if built): ~15 consultancies @ €2.5k + ~30 enterprise @ €4k + ~120 SME @ €700 = **€240k** at optimistic conversion; more likely half that

### 4. Three-year ARR ceiling — two scenarios (ESTIMATE)

| | Ireland-only | + EU/UK via TED |
|---|---|---|
| **Reports-led (no SaaS)** | €90–150k | €120–200k (add UK/EU one-off dossiers) |
| **Reports + API + modest SaaS** | €200–350k | €300–500k *nominal TAM*, but realistically capped by incumbents |

TED extension **explodes the TAM** — ~700–800k notices/yr worth **~€700–815bn** ([ted.europa.eu](https://ted.europa.eu/en/); [EC single-market](https://single-market-economy.ec.europa.eu/single-market/public-procurement/digital-procurement/tenders-electronic-daily_en)); project TED data holds 1,512 buyers / 14,630 winners. **But** cross-border is exactly where Tussell and Stotles already dominate with funded teams. A solo founder's *achievable* EU/UK share is a rounding error, not a multiplier. Treat TED as **enrichment for Irish clients' EU exposure**, not a market to win.

### 5. Founder time-split reality (solo, ~2,000 hrs/yr — ESTIMATE)

| Activity | Share | Hours |
|---|---|---|
| Free product + ETL pipeline upkeep | 30% | 600 |
| Sales / BD / scoping | 25% | 500 |
| Report production + QA | 35% | 700 |
| Admin / support / infra | 10% | 200 |

700 report hours ÷ ~30 hrs/report = **~23 reports/yr max**, i.e. a hard **reports-only revenue ceiling of ~€80–90k** — *and that already assumes near-zero support burden.* Every subscription customer adds unbudgeted SLA/incident/GDPR-subject-request load that comes straight out of the 600h keeping the free product and pipeline alive. Adding SaaS while solo doesn't grow the pie; it starves the asset the whole thing depends on.

### 6. The decision this informs

**Phase-5 SaaS (auth, billing, GDPR/DPA, scheduler, SLA) is not justified at this ceiling for a solo founder.** The recurring-subscription TAM tops out near €2.9m *only at full penetration*; realistic solo capture is €150–350k, and the incremental build + perpetual support/compliance load would consume the exact hours that keep the free civic product and pipeline running. SaaS economics need volume and a support function — neither exists solo, and the two segments large enough to matter (enterprise, cross-border) are where funded incumbents already sit.

**Rational end-state: reports + metered API.** Reports monetise the founder's scarce, defensible asset (the assembled Irish procurement graph) at high € per hour with zero recurring liability; a thin metered API captures the few technical buyers without the SaaS shell. Build subscriptions **only if** reports prove repeatable demand (a waitlist, not a hope) *and* generate enough margin to fund a second hire to carry support — i.e. SaaS is a *reinvestment decision after traction*, not a launch bet. Until then, Phase 5 is a distraction that risks the free product's integrity.

**Sources:** [trade.gov – Ireland public sector](https://www.trade.gov/country-commercial-guides/ireland-selling-public-sector) · [council.ie procurement review](https://council.ie/public-procurement-in-ireland-a-critical-review/) · [OGP SME page](https://www.gov.ie/en/office-of-government-procurement/publications/smes/) · [CSO/Irish Times business count](https://www.irishtimes.com/business/2025/12/15/businesses-operating-in-economy-rises-above-400000-for-first-time/) · [Stotles pricing](https://www.stotles.com/pricing) · [Tussell plans](https://www.tussell.com/plans) · [TED](https://ted.europa.eu/en/) · [EC – TED](https://single-market-economy.ec.europa.eu/single-market/public-procurement/digital-procurement/tenders-electronic-daily_en) · [gov.ie ABCs of Procurement](https://www.gov.ie/en/office-of-government-procurement/publications/abcs-of-public-procurement/) · Irish consultancies: [tenderteam.ie](https://tenderteam.ie/), [bidservices.ie](https://bidservices.ie/about-us/), [rfxprocurement.ie](https://www.rfxprocurement.ie/), [bidspecialists.ie](https://bidspecialists.ie/bid-consult/) · Internal counts: `dail-tracker` gold procurement data (1,891 authorities; 10,017 award-winning suppliers; 27,775 payment suppliers).

---

I have the verified numbers. Writing the finalized section now.

Key verified facts from the live spine:
- `procurement_competition` (TED 2024+, min_lots≥40): **50 buyers** clear the threshold (live `n_buyers=50`), against **1,512** distinct TED buyers and **1,891** total contracting authorities.
- `public_body_payments`: **85 publishers** of 406,111 lines — a small share of 1,891 authorities.
- `procurement_by_cpv` (eTenders): Construction 1,232 awards / 661 suppliers, only 442 valued (~36%); IT 1,125 awards / 318 suppliers, 182 valued (~16%).

## Public-sector buyers as a customer segment

Public-sector buyers — local authorities, the Office of Government Procurement (OGP), central departments, the ETBs, universities, and health bodies — are the most defensible willingness-to-pay segment in this market, and the one that most directly mirrors where the UK comparator earns real money. Tussell explicitly sells to government as well as to suppliers: "Central and Local Government authorities are already harnessing Tussell to make better procurement decisions, drive value-for-money, and promote social value," and it markets a dedicated public-buyer product line ([tussell.com/gov](https://www.tussell.com/gov), [tussell.com/products/tussell-insight](https://www.tussell.com/products/tussell-insight)). The NHS runs an equivalent internal capability, the NHS Spend Comparison Service, precisely because buyers value cross-body benchmarking — it "brings together procurement data from over 200 NHS organisations into a single, standardised platform … to analyse trends, benchmark pricing … achieve cost savings" ([england.nhs.uk](https://www.england.nhs.uk/nhs-commercial/central-commercial-function-ccf/nhs-spend-comparison-service/)). That in-house build is proof the need is real — but read honestly it also cuts the other way: a buyer with enough scale built rather than bought, which is exactly the competitive risk sized in "Where this loses" below.

### How big is this segment, in hard numbers

This is a small, finite, bounded market — and the data bounds it tightly. In the 2024+ TED award data (the only vintage that carries bid counts, so the only vintage that can benchmark), **only about 50 buyers clear the `min_lots ≥ 40` volume bar** needed to produce a statistically meaningful single-bid benchmark today (`procurement_competition`, live `n_buyers = 50`). Several of those are pan-EU or commercial semi-state bodies (Eurofound, GÉANT, ESB, Uisce Éireann), so the **core set of Irish public buyers you would actually sell benchmarking to is nearer 30–40**. That is against **1,512 distinct TED buyers** and **1,891 contracting authorities** in the awards spine — i.e. **the vast majority of authorities do not yet have enough coded 2024+ activity to benchmark at all**, and are not addressable at this grain.

The medium-term roster is still finite and countable:

| Buyer type | Approx. count |
|---|---|
| Local authorities | 31 |
| Education & Training Boards | ~16 |
| Universities + technological universities | ~13 |
| Central departments + major agencies/semi-states | ~40 |
| **Total addressable roster (ceiling)** | **~100–130 bodies** |

Multiplying through the subscription band gives the segment's revenue envelope:

- **Near-term SAM** — the ~30–50 buyers that can benchmark today, at **EUR 6k–15k/yr**, is a recurring-seat ceiling of **order EUR 0.2m–0.6m/yr** before any central deal. That is the honest near-term number this section exists to produce.
- **Medium-term SAM** — the full ~100–130-body roster at the same band is **order EUR 0.6m–2m/yr**, but it is gated on those bodies first accumulating benchmarkable 2024+ TED volume, and on winning a large share of a market where most bodies buy nothing like this today.

The single OGP/sector deal (below) sits on top of this, but the recurring-seat business is a sub-EUR-1m near-term segment, not a large one.

### What they would buy

1. **Competition / single-bid benchmarking.** "Am I getting fewer bids than my peers?" is a question a head of procurement cannot answer from their own ERP alone — it requires the whole-of-state comparison set. The single-bidder rate is the EU Single Market Scoreboard's triple-weighted, flagship procurement-integrity indicator ([single-market-scoreboard.ec.europa.eu](https://single-market-scoreboard.ec.europa.eu/business-framework-conditions/public-procurement_en)), so a buyer benchmarking against it is aligning with the metric Brussels already grades Ireland on.
2. **Category market maps** — for a given CPV, who the incumbents are, how concentrated supply is, typical award sizes, and where the market is thin (a single-supply risk) or deep (room to drive competition). Scope caveat: these rest only on CPV-coded awards (see the coverage note below), so they read as a directional lower bound, not a complete market.
3. **Supplier due-diligence** — award history, corporate identity (CRO), and distress/insolvency overlays before awarding.
4. **Spend analytics (covered bodies only).** Award ceilings vs. realised public-body payments, reconciled against the buyer's own picture — **but realised-payment disclosures exist for only ~85 publishing bodies** (`public_body_payments`, `n_publishers = 85`), a small fraction of the 1,891 authorities. For the large majority of target buyers there is **no realised-payments series to reconcile against**, so this is a **bonus deliverable for the covered minority (HSE, larger councils, a handful of departments), not a segment-wide selling point.** Pitch it only where the payments series exists.

### Why pay when the data is public

The data is public but *unusable at the buyer's desk*: it is fragmented across eTenders/OGP, TED, CRO and dozens of payment disclosures, uncategorised and un-benchmarked. Three durable reasons to pay:

- **Benchmarking against peers** — no single buyer can assemble the all-Ireland comparison set; that is the product, not the raw data.
- **Time-savings** — the CLES case study is exactly this: their own analysis of government spend "was costing their team too much time," so they bought the tool to "keep track … and save their team time" ([tussell.com/customers](https://www.tussell.com/customers)). Tussell's whole pitch is aggregating "raw, fragmented and uncategorised data from over 1000 sources" ([pixielabs.io case study](https://www.pixielabs.io/case-studies/tussell)).
- **EU procurement-compliance monitoring** — a buyer wants an early-warning read on its own competition metrics before the Commission or the C&AG flags them. The `procurement_competition` view actually surfaces the **single-bid rate, the uncompetitive-notice count, and the price-only-notice share** — those are the deliverable metrics. (A labelled "direct-award rate" is **not** in the view today; treat it as a build-if-feasible item, not a promised number.) Selling a body its own Scoreboard-methodology figure is a compliance service, not surveillance.

### Ethics and optics

This is the benign inverse of the political-dossier concern. A dossier assembles a *named individual's* diary, votes, and interests to profile a person — the harm is aggregation of personal political activity. Selling a public body benchmarking derived from public procurement data is different in kind: the subject is the **institution's own spending**, the buyer is accountable for that spending, and the analysis pushes toward *more* competition and compliance — the same direction the free civic product pushes. The only guardrails needed: keep it institution-grain (never profile a named official's decisions), and present single-bid figures with the mandatory caveat that a single bidder is a **factual signal, never a verdict** — often legitimate for niche/specialist supply or research equipment. Sold that way it reinforces, rather than compromises, the civic mission.

### Where this loses (competitive scan)

The product is not unopposed, and honesty about where it loses is the point:

- **Central in-house build (OGP, HSE).** The clearest willingness-to-pay proof points also cut against it. The NHS Spend Comparison Service is a **free, in-house** build aggregating 200+ bodies — direct evidence that a buyer with scale builds rather than buys. **OGP is the same risk domestically**: it already owns and runs eTenders and is the body most able to build this capability internally. The HSE (289 benchmarkable lots) is similarly build-not-buy. **The largest buyers are therefore build-not-buy risks, and should not be the beachhead.**
- **Buyers' own ERP / spend-cube vendors.** SAP, Oracle and specialist spend-analytics suppliers already sell a body its *internal* spend cube. They win the "my own spend" question outright.
- **The free EU Single Market Scoreboard.** It publishes the single-bid indicator gratis — but only at **country grain**, not per-buyer.

**The defensible wedge is the one none of those assembles: the all-Ireland, cross-body, per-buyer comparison set.** A buyer's ERP knows only its own spend; the Scoreboard knows only the country; no in-house team assembles the other 49 buyers' rates. That peer set is the product. Given the build-not-buy risk at the top, **the realistic beachhead is a mid-size single body** (a county council, an ETB, a mid-size university) — big enough to have benchmarkable volume, too small to build the comparison set itself.

### Evidence already in the data spine (award grain only — figures not summed across cuts)

The `procurement_competition` view already produces buyer-level single-bid benchmarking from **TED (EU Official Journal) 2024+ award notices**. A live example of the exact "how do I compare?" table a buyer would pay for:

| Buyer | Lots w/ bid-count | Single-bid lots | Single-bid rate |
|---|---|---|---|
| University of Galway | 92 | 68 | **73.9%** |
| University of Limerick | 49 | 26 | 53.1% |
| Donegal County Council | 74 | 29 | 39.2% |
| Dublin City Council | 213 | 70 | 32.9% |
| Irish Defence Forces | 72 | 21 | 29.2% |

A Dublin City Council procurement lead seeing 32.9% against a Galway university's 73.9% has an immediate, defensible peer benchmark — and each of those numbers is a lot-level rate within one buyer, not a cross-buyer total.

On the market-map side, the **separate** `procurement_by_cpv` view (eTenders award activity — a *different corpus* from the TED competition table above, with its own coverage and time bounds) shows category structure: Construction (CPV 45000000) is **1,232 awards across 661 suppliers**, whereas IT services (CPV 72000000) is a comparable **1,125 awards but only 318 suppliers** — a more concentrated market, the "where is supply thin?" signal a category manager buys.

**Two coverage caveats make these directional, not definitive:**
- **CPV-coding coverage.** Only roughly a third of awards in the spine carry a CPV code, so every count above is **coded awards only — a systematic lower bound**, not the whole market. The concentration comparison is a signal to investigate, not a settled market share.
- **Value coverage.** The award-value figures rest on the minority of awards that carry a value — **442 of 1,232 construction awards (~36%)** and **182 of 1,125 IT awards (~16%)** are valued. Medians and totals are computed on that valued subset, are sum-safe *within* a CPV, and must not be added across CPVs nor mixed with the payments grain.

### Pricing posture

Tussell does not publish list prices, so no exact benchmark is publicly findable. **ESTIMATE (reasoned range, not a quoted figure):** UK public-sector market-intelligence seats plausibly sit in the low-to-mid five figures sterling per year; an Irish solo product should price **below** that. A workable ladder:

- **Bespoke benchmarking report:** EUR 3k–8k (top of your stated EUR 1.5k–10k band — buyer reports carry more analyst framing than a supplier bid-pack).
- **Annual buyer subscription (single body):** **ESTIMATE EUR 6k–15k/yr**, assuming one-to-a-few seats and the peer-benchmark + category-map + due-diligence bundle. Assumptions: a body already spends far more than this on procurement staff time; the value is defensibility to the C&AG/NOAC, not headcount replacement.
- **Sector-wide / OGP-level licence:** a single central deal (OGP or a shared LGMA-type buy covering all 31 local authorities) is the highest-value target — **ESTIMATE EUR 40k–120k/yr** — but is also the **build-not-buy risk named above**, a long single-threaded sale for a solo founder, and should not gate near-term revenue.

### How it would be sold — framework vs. direct

Contract sizing must respect the Irish thresholds *and the aggregation rule that governs them*. For goods/services **below EUR 50,000 (ex-VAT)** a contracting authority need not advertise on eTenders and can buy directly on quotes ([OGP guidelines](https://ogp.gov.ie/public-procurement-guidelines-for-goods-and-services/); threshold per [gov.ie / Circular 05/2023](https://www.gov.ie/en/office-of-government-procurement/publications/abcs-of-public-procurement/)). **But the estimated contract value is the full term including renewals and options, and splitting a requirement — or re-buying annually — to stay under threshold is prohibited.** A recurring subscription priced at EUR 6k–15k/yr, valued over a typical 3–4 year term, is **EUR 24k–60k all-in and can breach EUR 50k**. So the "buy directly, no tender" path is only safe for a genuinely one-off, fixed-term engagement — not for open-ended annual supply.

- **Direct sale, pilot-first (the beachhead):** land one mid-size body on a **single fixed-term pilot engagement**, valued all-in (including any renewal or option) **comfortably under EUR 50k**, that produces the peer-benchmark deliverable and becomes the reference sale. Do **not** structure it as a rolling annual buy dressed up as one-offs — that is the anti-splitting breach.
- **Framework / DPS for anything ongoing or repeat:** to sell the *recurring* subscription lawfully across many bodies — and to clear EUR 50k / the EUR 143k central-government EU threshold ([MHC on 2024 thresholds](https://www.mhc.ie/latest/insights/new-eu-public-procurement-thresholds-from-1-january-2024)) — route it through an OGP framework or a **Dynamic Purchasing System** so buyers can call off compliantly. A DPS is the more solo-founder-friendly route: it stays open to new entrants, unlike a closed multi-supplier framework whose entry window you may have missed.

**Recommendation:** treat public buyers as the anchor segment for the reports-first phase, using a single **fixed-term, sub-EUR-50k pilot** to remove the tender barrier for the first sale — but plan the DPS/framework route as the Phase-2 unlock the moment supply becomes ongoing, because (a) serial annual direct buys are not lawful, and (b) the sector-wide central deal, not the individual seat, is where the real money in a market this small concentrates — while conceding it is also the biggest build-not-buy risk.

*Sources: [Tussell gov product](https://www.tussell.com/gov), [Tussell customers/CLES](https://www.tussell.com/customers), [Tussell/PixieLabs case study](https://www.pixielabs.io/case-studies/tussell), [NHS Spend Comparison Service](https://www.england.nhs.uk/nhs-commercial/central-commercial-function-ccf/nhs-spend-comparison-service/), [EU Single Market Scoreboard – public procurement](https://single-market-scoreboard.ec.europa.eu/business-framework-conditions/public-procurement_en), [OGP guidelines](https://ogp.gov.ie/public-procurement-guidelines-for-goods-and-services/), [gov.ie ABCs of Public Procurement](https://www.gov.ie/en/office-of-government-procurement/publications/abcs-of-public-procurement/), [MHC 2024 thresholds](https://www.mhc.ie/latest/insights/new-eu-public-procurement-thresholds-from-1-january-2024). Data figures from the project's own spine, kept to distinct corpora: the single-bid benchmark and buyer counts are from `procurement_competition` (**TED / EU OJ 2024+ award notices**, ~50 buyers clear `min_lots ≥ 40`); the category market map is from `procurement_by_cpv` (**eTenders award activity**, a separate corpus with its own coverage and time bounds); segment counts (1,512 TED buyers; 1,891 authorities; 85 payment publishers) from `data_coverage`. TED and eTenders are distinct corpora and are not one comparable award set.*

---

## Grant / institutional-funding alternative memo

*The commercial strategy docs never evaluated the non-commercial path. This memo does — with the same skepticism applied to the SaaS case. Bottom line up front: **grants are a viable way to fund the free civic-accountability side and the open-data plumbing, and to buy runway and credibility — but they are structurally incapable of being a substitute for the commercial spinout, and for a solo founder they carry a real distraction risk.** Treat them as a *complement*, chosen surgically, not as a business model.*

### Why the fit is awkward before we even look at programmes

Three structural facts shape everything below:

1. **Almost every funder here funds organisations, consortia, or non-profits — not solo commercial founders.** Landing most of this money means standing up a company-limited-by-guarantee / charitable vehicle, with a board, governance, and audited accounts. That is overhead a solo founder does not currently carry.
2. **Non-commercial / state-aid clauses bite exactly where the money is.** You generally **cannot use a public or philanthropic grant to build or subsidise a revenue-generating commercial product** — that is either an ineligible cost or unlawful state aid / double-funding. Grants can fund the *free* Dáil Tracker and open data/pipeline; they cannot legally bankroll the paid procurement-BI tool. This single fact is why grants and the commercial spinout are largely non-substitutable.
3. **Grant income is bursty, restricted, and non-recurring.** A €50k project grant is not €50k of ARR — it is a one-off, cost-reimbursement, milestone-reported obligation that ends. Recurring commercial revenue and one-off restricted grants are not the same asset class.

### Realistically applicable routes

**EU — thematic fit is strong, structural fit is weak**

- **CERV (Citizens, Equality, Rights and Values), 2021–27, €1.56bn total.** Explicitly funds "transparency and good governance" and capacity of civil society. On theme, a near-bullseye. But: consortium/CSO-oriented, co-funding expected, heavy reporting, **no National Contact Point in Ireland**, and Irish applicants have run ~35% success on 89 proposals. Not a solo-founder instrument. Fit for a *coalition* the project joins, not for the founder alone. [overview](https://commission.europa.eu/funding-and-tenders/find-funding/eu-funding-programmes/citizens-equality-rights-and-values-programme/citizens-equality-rights-and-values-programme-overview_en) · [Ireland guide](https://www.accesseurope.ie/funding-guides/citizens-equality-rights-and-values-programme-cerv)
- **Horizon Europe, Cluster 2 "Democracy & Governance."** Topics like disinformation, inequality-and-democracy, civic participation run ~**€2–5m per project, up to 100% funded**, with per-topic budgets ~€10.5m. Money is real, but these are **multi-partner research consortia led by universities**, 2–3 year academic deliverables — not product funding. Realistic only as a data/technical partner inside an academic consortium. [call area](https://rea.ec.europa.eu/funding-and-grants/horizon-europe-cluster-2-culture-creativity-and-inclusive-society/democracy-and-governance_en)
- **Digital Europe Programme (2021–27, ~€8.1bn; 2025–27 WP €1.3bn).** Interoperability/open-data work exists (e.g. a €6m interoperability MCP at 50% co-fund) but it targets **public administrations and large deployments**, 50% match, consortia. Poor solo fit. [programme](https://digital-strategy.ec.europa.eu/en/activities/digital-programme) · [2025–27 WP](https://digital-strategy.ec.europa.eu/en/library/work-programme-2025-2027-digital-europe-programme-digital)
- **NGI Zero / NLnet (EC Next Generation Internet).** **The best-fit EU instrument for a solo builder.** Grants **€5,000–€50,000** for free/open-source software, open standards and **open data**; >1,000 projects funded; light, developer-friendly process. Constraint: the funded work must be **open-sourced** — so it funds the *pipeline / open-data commons*, not the proprietary BI layer. [NGI0](https://nlnet.nl/NGI0/) · [apply](https://nlnet.nl/funding.html)

**Journalism / press-freedom funds — fund investigations and tools, not products**

- **Journalismfund Europe** — record **€5.26m granted in 2025**; European cross-border grants (~€280k/call), environmental (~€400k/call). Funds *journalism*, typically cross-border teams; a data project qualifies as the technical/data partner on an investigation, not as a standalone grantee. [2025 record](https://www.journalismfund.eu/news/journalismfund-europe-grants-record-eu5-million-2025) · [grants](https://www.journalismfund.eu/grants)
- **IJ4EU (Investigative Journalism for Europe)** — **€2m across 2024–25**, up to **€50k** (Investigation Support) / **€20k** (Freelancer); €1.6m for 2026–27. Same shape: cross-border teams, editorial output. [scheme](https://investigativejournalismforeu.net/grants/investigation-support-scheme/) · [EJC programme](https://ejc.net/for-funders/programmes/ij4eu)
- **Google / European Media & Information Fund (EMIF, run by Gulbenkian)** — Google seeded **€25m over 5 years**; historic DNI Innovation Fund ran to **€1m** per large project and **€150k** GNI Innovation grants (70/30 match). Real money for news-tech, but disinformation/fact-checking/media-literacy framed, and Google-origin funding raises independence-perception concerns for an *accountability* brand. [EMIF](https://gulbenkian.pt/emifund/) · [DNI history](https://blog.google/topics/google-europe/digital-news-initiative-20-million-funding-innovation-news)
- **Coimisiún na Meán (Ireland).** **€10m Journalism Schemes in 2025** aimed squarely at **under-provided local-democracy, local-authority and court reporting** — thematically adjacent to what procurement/council data enables; plus Sound & Vision (€6.5m round) and a €14m Shared Island Media Fund. Funds broadcasters/publishers/producers, so the route is a **journalism partner sub-contracting the data capability**, not a direct product grant. [Journalism Schemes](https://www.cnam.ie/general-public/media-development/support-funding/journalism-schemes/) · [€6.5m round](https://www.cnam.ie/e6-5-million-awarded-in-latest-round-of-sound-vision-funding/) · [Shared Island](https://www.cnam.ie/shared-island-media-fund-of-e14m-announced/)

**Philanthropic transparency funders — perfect theme, hardest to access**

- **Luminate.** Impact areas literally include **Financial Transparency** — "public funds… contracting… tracking expenditure… the data and standards that form the building blocks of transparency." That *is* this project. ~**$28.5m/yr across ~126 grants, median ~$190k.** But Luminate is largely invitation/strategy-led, global, funds established organisations, and has been narrowing focus — a solo Irish founder is a long shot without a track record and a non-profit home. [financial transparency](https://luminategroup.com/financial-transparency) · [grant scale](https://www.causeiq.com/organizations/luminate-foundation,823941326/)
- **Open Society Foundations / Transparency & Accountability Initiative** — donor collaborative in the same field; same access reality (relationship- and org-led, not solo-product). [TAI](https://luminategroup.com/investee/tai)

**Irish social-innovation / community funders — accessible but small and off-theme**

- **Rethink Ireland.** Impact Fund 2025 grants **up to €96k + capacity-building**; Connected Communities €1.5m across ~5 projects (part-funded via **DRCD's Dormant Accounts Fund**). Accessible and Irish, but framed around social exclusion/community wellbeing — a transparency data-tool is a stretch fit and must be pitched as social impact, non-profit. [Impact Fund](https://rethinkireland.ie/current_fund/impactfund/) · [Connected Communities](https://rethinkireland.ie/current_fund/connected-communities-fund-2025-2028/)
- **Community Foundation Ireland.** Runs a **Civil Society/Democratic-transformation** strand and small community grants (e.g. the €500k Bank of Ireland Community Fund). Small ticket sizes; democracy strand is the relevant door. [apply](https://www.communityfoundation.ie/apply-for-funding/) · [democracy strand](https://www.grantfinder.co.uk/civil-society-grant-programme-for-democratic-transformation-initiatives-across-ireland/)

### Head-to-head: commercial spinout vs. grant income

| Dimension | Commercial spinout | Grant / institutional funding |
|---|---|---|
| Income scale | **~€100–400k ARR ceiling** (per the TAM work) — recurring, compounding | Bursty, restricted, non-recurring; see estimate below |
| What it can fund | The paid procurement-BI product | Only the **free** civic side + open-data/pipeline (non-commercial clauses) |
| Recurrence | Recurring revenue = an asset | One-off, ends at project close |
| Effort per euro | Sales + delivery; solo-doable | Applications + consortium-building + audit-grade reporting; heavy for a solo |
| Vehicle needed | Sole trader / Ltd works | Usually **non-profit CLG/charity** with board & governance |
| Mission alignment | Tension: monetising public data; ethics firewall needed | High — the mission *is* the grant deliverable |
| Control | Full | Partial — funder priorities, restricted budgets, deliverables |
| Reputational / SLA / GDPR load | High — paying customers expect uptime, support, data-protection compliance, accuracy liability | Low-to-moderate — funders want reports, not 99.9% SLAs |
| Sustainability | Self-sustaining if it reaches ceiling | Perpetual re-application treadmill; no equity value |

**Plausible grant income — ESTIMATE (reasoned range, not a findable figure).**
Assumptions: solo founder stands up a lightweight non-profit vehicle in year 1; targets the genuinely solo-accessible doors (NGI0, one journalism-partner sub-award, one Irish social-innovation grant); does **not** attempt CERV/Horizon/Luminate solo. On those assumptions, realistic capturable income is roughly **€40k–€130k per year in the first ~3 years**, lumpy and restricted — e.g. one NGI0 grant (€30–50k, open-source only) + one journalism investigation partnership (€20–50k) + an occasional Rethink/CFI grant (€25–96k). This is an ESTIMATE; actual results depend heavily on fit, timing, and whether a non-profit vehicle exists. Crucially, **none of it is recurring**, and each tranche costs weeks-to-months of non-product labour to win and report. If the founder instead invested in relationships and a track record, a single **Luminate/CERV-scale** award (~€150k–€300k) is *conceivable* in years 2–3 — but that is a low-probability, org-and-relationship-gated outcome, not a plan.

### Honest recommendation: complement, with a real distraction warning

- **Not a substitute.** Grants legally cannot fund the commercial BI product, are non-recurring, and mostly require an organisational vehicle the founder doesn't have. They do not build an asset with enterprise value. If the goal is a *business*, grants are not it.
- **A genuine complement — for the free side and the plumbing.** The strongest honest use of grant money is to **fund the open-data pipeline and the free civic-accountability product** (NGI0 for the open-source data infrastructure; Coimisiún na Meán / Journalismfund / IJ4EU via journalism partners for local-democracy and procurement-accountability reporting; Rethink/CFI for social-impact framing). This subsidises the public-good half, builds credibility and a track record, and — done right — makes the *commercial* half cheaper to run because the shared pipeline is grant-funded. The ethics firewall already in the plan (lobbying/diary access = free only) maps cleanly onto a grant-funded free tier.
- **A distraction risk that must be managed.** For a solo founder, grant-chasing is a time sink with long, uncertain cycles and audit-grade reporting. Pursuing EU consortia (CERV, Horizon, Digital Europe) or courting Luminate *before* there is a track record would likely burn a year for little. **Rule of thumb: only pursue grants that (a) fit the free/open-data side, (b) are solo- or single-partner-accessible, and (c) pay for work you were going to do anyway.** Everything else is a distraction until the project has an organisational home and a reference-able record.

**Recommended posture:** run the reports-first commercial path as the *income* engine, and layer in **one or two surgical, open-data/journalism grants** (NGI0 first — smallest friction, right theme, funds the shared pipeline) to underwrite the free civic product and build the credibility that later unlocks larger philanthropic/EU money. Grants complement and de-risk; they do not replace the commercial ceiling.
