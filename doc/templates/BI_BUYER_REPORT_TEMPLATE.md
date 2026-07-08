# BI Buyer / Contracting-Authority Report — Analyst Template

**Date:** {{REPORT_DATE}}
**Status:** Analyst fill-in template — bespoke buyer dossier, reports-first phase (hand-built for 3–5 paying design partners; no accounts/alerts/persistence — that is Phase 5, owner-sign-off gated).
**Report subject:** {{AUTHORITY_NAME}}
**Authority sector:** {{AUTHORITY_CLASS}} — *analyst-classified descriptive metadata (central gov / local authority / health / semi-state); NO data-access function returns an authority class, so this is a manual label, never a data figure.*
**Prepared by:** {{ANALYST_NAME}}
**Corpus refresh:** {{CORPUS_REFRESH_DATE}}
**Cross-links:** [doc/BI_SPINOUT_ARCHITECTURE.md](../BI_SPINOUT_ARCHITECTURE.md) · [doc/PROCUREMENT_INTELLIGENCE_ROADMAP.md](../PROCUREMENT_INTELLIGENCE_ROADMAP.md)

> This is a fill-in-the-blanks template. Replace every `{{PLACEHOLDER}}` with a value read **only** through the data-access functions named in each section. Do not compute a figure the functions do not return. Sections marked **PHASE-2-TODO** have no wired data-access path today — leave them stubbed with the note shown, or wire the view first via the [pipeline-view](../PROCUREMENT_INTELLIGENCE_ROADMAP.md) skill. Every subject is an **organisation** (contracting authority / supplier org / CRO number); the generator must be structurally incapable of naming a natural person.

---

## How to generate this report (data-access calls, in order)

All calls go through `utility/data_access/procurement_data.py` (thin `st.cache_data` wrappers over `dail_tracker_core/queries/procurement.py`). Never read parquet/CSV or run SQL joins in report/page logic — aggregation lives in the registered pipeline view (see **Never-break rails → Logic firewall**).

1. **Profile (Section 1):** `fetch_authority_summary_result(conn, order_by="awards")` → select the single row where `contracting_authority == "{{AUTHORITY_NAME}}"`. Grain: **AWARDED**. View: `v_procurement_authority_summary` — projects exactly `contracting_authority`, `n_awards`, `n_suppliers`, `awarded_value_safe_eur` and nothing else.
2. **Award base (Sections 3–5):** `fetch_awards_for_authority(conn, "{{AUTHORITY_NAME}}")` → one row-level AWARDED frame that feeds top-suppliers, category mix, and framework counts. **Note:** this frame carries `n_bids_received` / `competition_type` (eTenders), but the Section 6 single-bid rate is **NOT** taken from here — Section 6 is TED-only (`v_procurement_competition`, 2024+). Do not fold an eTenders bid count into the TED competition metric.
3. **Value trend (Section 2 — PHASE-2-TODO):** no `authority_year_trend` exists. Interim: loop `fetch_authority_summary_result(conn, year=Y)` per year across `{{COVERAGE_WINDOW}}` and select this authority's row (`v_procurement_authority_year_summary` is a *cross-sectional* per-year ranking, not a per-authority time series).
4. **Single-bid competition (Section 6 — UNWIRED):** core `competition(conn)` over `v_procurement_competition` has **no data-access wrapper today**. Wire `fetch_competition_result` first, then match on **exact** `buyer_name`. If no exact match → emit the hard-fail string (see Section 6). Grain: enrichment, TED 2024+ only.
5. **Live tenders (Section 7):** `fetch_live_tenders_result(conn)` → filter to `buyer == "{{AUTHORITY_NAME}}"`. Grain: **PLANNED**.
6. **Expiring — national (Section 8):** `fetch_expiring_etenders_result(conn, months_ahead=24)` → filter `buyer_name == "{{AUTHORITY_NAME}}"`. Grain: **AWARDED** (advertised term, display-only).
7. **Expiring — TED (Section 8):** `fetch_expiring_contracts_result(conn, months_ahead=12)` → filter `buyer_name == "{{AUTHORITY_NAME}}"`. Grain: **AWARDED** (advertised term, display-only).

> Any interim per-authority rollup done on an already-filtered frame (Sections 3–5) must be a sanctioned display-only aggregation tagged `# logic_firewall: display_only`, and is a stopgap only — the correct fix is a registered buyer-keyed view.

---

## The three money grains never mix (read before filling any number)

| Grain | What it is | Source family | Can answer | Must NEVER |
|---|---|---|---|---|
| **AWARDED** | Contract award notice — the *ceiling/estimate* at signing | eTenders / TED award notices | "What was tendered, to whom" | Be summed with payments; be read as money paid |
| **PAID** | Cash actually disbursed (SPENT tier) | Public-body payment registers | "What left the buyer's account, by year" | Be summed with AWARDED; be blended with COMMITTED |
| **COMMITTED** | Purchase-order commitments (ordered, not yet paid) | PO registers | "What was ordered" | Be merged into PAID |
| **PLANNED** | Forward opportunity, open now | eTenders live / TED pipeline | "What's coming to market" | Be summed as if a transaction |
| **BUDGET** | Appropriation / estimate ceiling | Estimates / AFS | "What was allocated" | Be conflated with any of the above |

Verbatim master rule (`dail_tracker_core/caveats.py` → `MONEY_GRAINS`):
> procurement AWARDS, public-body PAYMENTS, and T&A allowances are three different value grains — NEVER sum across them

This dossier is an **AWARDED**-grain report. Payments to this authority live in a **separate register** (`fetch_payments_publisher_summary_result`, or public-payments `publisher_summary`) at PAID/COMMITTED grain and are **never summed or netted** against the awarded figures here.

---

## Section 1 — Authority profile (lead card)

**So-what (SME):** "Who is this body, what class of authority, and how active a buyer are they?"
**So-what (consultant):** "Frame the buyer — sector, register footprint, publishing behaviour."

**Data source:** `fetch_authority_summary_result(conn, order_by="awards")` → row for `{{AUTHORITY_NAME}}`. **Grain: AWARDED.** View: `v_procurement_authority_summary` — this function/view returns **only** the four columns below.

| Field | Value | Source column |
|---|---|---|
| Authority name | {{AUTHORITY_NAME}} | `contracting_authority` |
| Award notices (all-time) | {{N_AWARDS_TOTAL}} | `n_awards` |
| Distinct suppliers | {{N_SUPPLIERS}} | `n_suppliers` |
| Awarded value (sum-safe rows only) | {{AWARDED_VALUE_SAFE_EUR}} — *award ceiling, not spend* | `awarded_value_safe_eur` |

> **PHASE-2-TODO — these profile fields have NO wired data-access path and are NOT returned by `authority_summary`; do not fill or invent them.** `authority_summary` / `fetch_authority_summary_result` is all-time only (no 24-month window). Until a registered buyer-keyed view projects them, leave each stubbed rather than presenting a fill-in placeholder:
> - **Authority class / sector** — no data-access function returns an authority class. If shown at all it is analyst-assigned descriptive metadata (see header), never a data figure.
> - **Award notices (last 24 months)** — the function is all-time only; a 24-month split would require date-filtering + counting the awards frame in report logic, which the logic firewall forbids (GT-4 (b)). Wire `n_awards_24m` into a registered view first.
> - **Distinct CPV categories** — not returned here. If wanted, derive display-only from the Section 4 `fetch_awards_for_authority` frame (`# logic_firewall: display_only`); never attribute it to `authority_summary`.
> - **Publishes a payment register? / threshold / award & payment disclosure chips** — no authority function returns payment-register publication, a threshold, or disclosure flags. There is no data-access path for these today; do not present fill-in placeholders or a disclosure chip row.

**Caveats:**
- Publishing behaviour varies by body; a low count may reflect disclosure practice, not activity.
- `awarded_value_safe_eur` sums only rows where `value_safe_to_sum = TRUE`. Per the awards caveat (`api/routers/exports.py`, `procurement_awards`):
  > AWARD values are ceilings/estimates, NOT money paid. Frameworks/DPS repeat one ceiling across supplier rows. Sum ONLY rows where value_safe_to_sum — and even that is awarded value, never expenditure. Upstream publishes quarterly with an inherent ~6-month lag (see data_currency).

---

## Section 2 — Procurement value & volume trend

**So-what (SME):** "Is this buyer's tender pipeline growing? Is it worth building a relationship?"
**So-what (consultant):** "Trend and seasonality of the buyer's tendering."

> **PHASE-2-TODO — no wired per-authority time series.** There is no `authority_year_trend` function. `v_procurement_authority_year_summary` is a *cross-sectional* per-year ranking, not a per-buyer trend. **Interim:** loop `fetch_authority_summary_result(conn, year=Y)` per year and select this authority's row. **Proper fix:** add `authority_year_trend(conn, contracting_authority)` + a registered `v_procurement_authority_year_trend` view (pipeline-view skill). Do not compute the trend by aggregating raw rows in report logic.

**Grain: AWARDED (lead with notice COUNTS; awarded-value is a grain-labelled secondary panel).**

| Year | Award-notice count | Awarded ceiling (sum-safe €) — *not money paid* |
|---|---|---|
| {{YEAR}} | {{N_AWARDS_YEAR}} | {{AWARDED_VALUE_SAFE_YEAR}} |

**Caveats:**
- Counts are of *notices*; a single framework can generate many call-off notices, inflating the count.
- Awarded value is the contract ceiling at notice, not spend (see Section 1 verbatim caveat). Never stack the count and value series in one chart — a stacked axis reads as a sum.

---

## Section 3 — Top suppliers & concentration (incumbency)

**So-what (SME):** "Is this buyer an open field or locked up by a few incumbents — and in which categories is there room?"
**So-what (consultant):** "Identify incumbents and the contestable seams."

> **PHASE-2-TODO — `top_suppliers_for_authority` does not exist.** The nearest wired PAID-grain function is `payments_publisher_summary` (`fetch_payments_publisher_summary_result`) — top suppliers/spend of a *paying body* at PAID/COMMITTED grain, a **different register**; do not substitute it for AWARDED. **Interim:** derive the ranking display-only from the already-filtered `fetch_awards_for_authority(conn, "{{AUTHORITY_NAME}}")` frame, tagged `# logic_firewall: display_only`. **Proper fix:** register `top_suppliers_for_authority` + view.

**Grain: AWARDED, award-notice COUNTS (never value/spend).**

| Supplier (org) | # awards | CPVs | Most recent award |
|---|---|---|---|
| {{SUPPLIER_NAME}} | {{N_AWARDS}} | {{CPV_LIST}} | {{LAST_AWARD_DATE}} |
| …all others | {{N_AWARDS_OTHER}} | — | — |

Concentration facts (denominators shown, no "risk" label): top-5 suppliers hold **{{TOP5_SHARE_PCT}}%** of **{{DENOMINATOR_N_AWARDS}}** award notices. New suppliers winning a first award from this buyer in the last 12 months: **{{N_NEW_SUPPLIERS}}**.

**Caveats:**
- Concentration is measured on award *counts*, not value or spend. Descriptive only — not a competitiveness or fairness assessment. Never a pie summing value across grains.
- No contract end-date is shown in this section: `fetch_awards_for_authority` returns `award_date` and `contract_duration_months` but **no end-date column** — an end date would have to be derived (`award_date + contract_duration_months`), which this frame does not carry. The renewal/expiry calendar with published end dates lives in **Section 8** (`est_end_date` / `contract_end_date_est`).
- "New supplier" means first appearance in the award corpus for this buyer within the window, subject to entity-matching limits.
- Entity resolution is by normalised org name (`utility/pages_code/procurement.py`):
  > Grouped by name only; these may be different legal entities — confirm via the CRO number on each notice before treating them as one firm.

---

## Section 4 — Category / CPV mix

**So-what (SME):** "What does this buyer actually buy — is my offering even in their pattern?"
**So-what (consultant):** "Map the spend categories to match client capability."

> **PHASE-2-TODO — `authority_category_mix` does not exist.** **Interim:** count `cpv_code` / `cpv_description` / `category_label` display-only on the filtered `fetch_awards_for_authority` frame (`# logic_firewall: display_only`). **Proper fix:** register `authority_category_mix` + view.

**Grain: AWARDED, COUNTS by CPV (not value).**

| CPV code | Description | # awards | # distinct suppliers |
|---|---|---|---|
| {{CPV_CODE}} | {{CPV_DESC}} | {{N_AWARDS}} | {{N_SUPPLIERS}} |

**Caveats:**
- Distribution is on award counts, not value/spend.
- `fetch_awards_for_authority` returns `cpv_code` and `cpv_description`, **not** a `cpv_division` column. If you roll up to CPV *division*, that is the two-digit prefix of `cpv_code` computed as a display-only transform (`# logic_firewall: display_only`), never a returned field.
- CPV coding is buyer-assigned and inconsistent; adjacent codes may hold related activity.

---

## Section 5 — Framework use

**So-what (SME):** "How much of this buyer's book is locked inside frameworks/DPS I'd have to be on a panel to reach — versus open one-off contracts?"
**So-what (consultant):** "Size the framework-gated share before advising bid vs panel-application strategy."

> **Interim — display-only on the award base; a per-authority framework-mix aggregate is PHASE-2-TODO.** The raw signal is the `value_kind` column on `fetch_awards_for_authority` (values in gold: `contract_award_value` vs `framework_or_dps_ceiling`) and, on the TED expiring feed, `is_multi_supplier_framework` + `renewal_max`. Count display-only (`# logic_firewall: display_only`); do not sum ceilings.

**Grain: AWARDED, COUNTS.**

| Award type (`value_kind`) | # notices | Note |
|---|---|---|
| `contract_award_value` (one-off / call-off) | {{N_CONTRACT_AWARD}} | award value, not spend |
| `framework_or_dps_ceiling` (framework / DPS) | {{N_FRAMEWORK}} | **ceiling — repeats across supplier rows; never summed** |

Multi-supplier frameworks currently mapped to this buyer (TED expiring feed): **{{N_MULTI_SUPPLIER_FRAMEWORKS}}**.

**Caveats:**
- Framework/DPS rows carry a **ceiling**, not money paid, and repeat one ceiling across supplier rows — they are never summed and never summed with the one-off award rows.
- Per the awards caveat: "Frameworks/DPS repeat one ceiling across supplier rows. Sum ONLY rows where value_safe_to_sum — and even that is awarded value, never expenditure."

---

## Section 6 — Single-bid competition rate

**So-what (SME):** "How often does this buyer's tenders draw only one bidder — is it worth the bid cost?"
**So-what (consultant):** "Characterise the buyer's competitive intensity."

> **UNWIRED + KEY-MISMATCH — read this before filling anything in.** The per-buyer competition signal lives in `v_procurement_competition` (core `competition()`), which is **keyed on TED `buyer_name`**. This dossier's awards are keyed on eTenders `contracting_authority`. **These are different name strings with no crosswalk.** The competition panel must match on **exact `buyer_name`** and **hard-fail** — it must **never fuzzy-match** a nearby name, which would attribute another buyer's competition profile to this one.
>
> - `v_procurement_competition` has **no data-access wrapper today** (only `competition_by_cpv` is wired). Wire `fetch_competition_result` first.
> - If there is no **exact** `buyer_name` match, render exactly: **"No TED competition data for this buyer."** and stop. Do not substitute a CPV-level or fuzzy figure.
> - Do **not** back-fill this rate from the Section 2/3 awards frame's `n_bids_received` — that is eTenders bid data on a different key and grain, not the TED lot-level metric this section reports.

**Data source:** `v_procurement_competition` via `competition(conn)` (Phase-2 wrapper). **Grain: enrichment, TED 2024+ only.**

**Competition status:** {{COMPETITION_STATUS}} *(either the exact-match row below, or the literal string "No TED competition data for this buyer.")*

| Metric | Value |
|---|---|
| `buyer_name` (exact TED key) | {{TED_BUYER_NAME}} |
| Notices | {{N_NOTICES}} |
| Lots with a bid count | {{N_LOTS_WITH_BIDCOUNT}} |
| Single-bid lots | {{N_SINGLE_BID_LOTS}} |
| **Single-bid lot %** | {{SINGLE_BID_LOT_PCT}}% |
| Uncompetitive-procedure notices | {{N_UNCOMPETITIVE}} |
| Coverage window | {{FIRST_YEAR}}–{{LAST_YEAR}} |

**Caveat (verbatim — `dail_tracker_core/caveats.py` → `COMPETITION`, reproduce in full, never paraphrase):**
> single_bid_lot_pct = single-bid LOTS / lots-with-a-bid-count, from TED 2024+ award notices — each contract PART counted once (the honest lot-level rate; an earlier notice-level reading over-stated multi-lot buyers). A FACTUAL competition signal, NEVER a verdict: a single bidder is often legitimate — a niche/specialist supplier, bespoke research equipment, genuine urgency (research universities legitimately single-source a lot). It is the EU Single Market Scoreboard's procurement-integrity indicator: a prompt to look, not evidence of wrongdoing. Rank only buyers with a healthy n_lots_with_bidcount (min_lots default 40); small samples are noisy. Coverage is 2024+ only (the eForms era carries bid counts).

**Structure-fact rail (verbatim — `dail_tracker_core/queries/procurement.py`):**
> A single bid is a recorded fact, often wholly legitimate — the page carries that caveat; this query only surfaces the notices, never a verdict.

---

## Section 7 — Live tenders for this buyer

**So-what (SME):** "What is this buyer bringing to market right now — where do I aim?"
**So-what (consultant):** "Build the client-facing live pipeline for this authority."

**Data source:** `fetch_live_tenders_result(conn)` → filter to `buyer == "{{AUTHORITY_NAME}}"`. **Grain: PLANNED** (national eTenders, open now). Estimated values are opportunity estimates — never summed, never treated as awarded or paid.

> **Filtering caveat:** `live_tenders` has no `buyer` parameter — filter the returned frame by exact buyer name. The eTenders `buyer` string may differ from the `contracting_authority` used elsewhere in this dossier; treat a non-match as "no live tenders found for this buyer", not as zero activity.

| Title | Published | Submission deadline | Days to deadline | Procedure | Estimated value (€) — *opportunity estimate* | Link |
|---|---|---|---|---|---|---|
| {{TENDER_TITLE}} | {{PUBLISHED_DATE}} | {{SUBMISSION_DEADLINE}} | {{DAYS_TO_DEADLINE}} | {{PROCEDURE}} | {{ESTIMATED_VALUE_EUR}} | {{DETAIL_URL}} |

**Caveats:**
- Estimated values are PLANNED-grain estimates — an opportunity, not a transaction. Never summed with any AWARDED/PAID figure.
- A factual live feed, not a forecast of award.

---

## Section 8 — Expiring contracts (renewal calendar)

**So-what (SME):** "What is this buyer's current book that comes back to market, and when — where's my window?"
**So-what (consultant):** "Build the re-tender calendar for this authority."

**Data sources (two lanes, never unioned):**
- National eTenders: `fetch_expiring_etenders_result(conn, months_ahead=24)` → filter `buyer_name`.
- TED: `fetch_expiring_contracts_result(conn, months_ahead=12)` → filter `buyer_name`.

**Grain: AWARDED (advertised term — display-only). Dates only, no value totals.**

*National (eTenders):*

| Contract | Winner (org) | Award date | Duration (months) | Est. end date | Est-end basis | Award value (€) — *ceiling* |
|---|---|---|---|---|---|---|
| {{CONTRACT_NAME}} | {{WINNER_DISPLAY}} | {{AWARD_DATE}} | {{DURATION_MONTHS}} | {{EST_END_DATE}} | {{EST_END_BASIS}} | {{AWARD_VALUE_EUR}} |

*TED:*

| Contract (pub. no.) | Winners | CPV division | Concluded | Duration (months) | Renewal max | Est. end date | Multi-supplier framework? |
|---|---|---|---|---|---|---|---|
| {{PUBLICATION_NUMBER}} | {{WINNERS_DISPLAY}} | {{CPV_DIVISION}} | {{CONTRACT_CONCLUSION_DATE}} | {{CONTRACT_DURATION_MONTHS}} | {{RENEWAL_MAX}} | {{CONTRACT_END_DATE_EST}} | {{IS_MULTI_SUPPLIER_FRAMEWORK}} |

> **Renewal cadence note:** there is **no dedicated `renewal_cycle` function (PHASE-2-TODO)**. The only renewal signal today is the `renewal_max` column carried inside the TED expiring feed above.

**Caveats:**
- Contract end-dates are as published, frequently missing, and nominal; frameworks may extend. The timeline shows only awards with a stated end date — a discovery calendar, not a forecast of volume or re-award.
- Award values are AWARDED-grain ceilings (display-only); never summed, and the national and TED lanes are never unioned into one total.
- TED rail (verbatim — `api/routers/exports.py`, `ted_awards`):
  > Pan-EU framework outliers (is_pan_eu_outlier) carry ceilings that dwarf the Irish market — exclude them from totals. Single-bid is a factual signal, never a verdict. Sum only value_safe_to_sum rows; never add to payments (different grain).

---

## Section 9 — Data lineage & coverage

**So-what (both):** "How complete is this, and what was left out?"

| Source feed | Coverage window | Last refresh | Unmatched-record note |
|---|---|---|---|
| eTenders awards | {{ETENDERS_WINDOW}} | {{ETENDERS_REFRESH}} | {{ETENDERS_UNMATCHED}} |
| TED awards / competition | {{TED_WINDOW}} (2024+ for bid counts) | {{TED_REFRESH}} | {{TED_UNMATCHED}} |
| eTenders live tenders | {{LIVE_WINDOW}} | {{LIVE_REFRESH}} | — |
| Payment register (if published) | {{PAYMENTS_WINDOW}} | {{PAYMENTS_REFRESH}} | separate PAID/COMMITTED register — never summed here |

**Caveats:** award records that could not be crosswalked to this authority are **counted and shown here, not silently dropped** ({{N_UNMATCHED_AWARDS}} rows). Payment coverage is a **coverage fact**, not evidence of non-payment — not all bodies publish registers.

---

## Never-break rails (binding on this report)

Source-anchored to [doc/BI_SPINOUT_ARCHITECTURE.md](../BI_SPINOUT_ARCHITECTURE.md) §4/§6/§10/§15 and `tools/check_streamlit_logic_firewall.py`.

**1. Never-sum the grains.** AWARDED / PAID / COMMITTED / PLANNED / BUDGET are separate ledgers. Each is a separately labelled cell or series. No stacked bars, single totals, running sums, or side-by-side arithmetic that implies one number across grains. **TED is never unioned with national awards.** Sum only *within* a grain and only where `value_kind` / `value_safe_to_sum` permits — carry the grain tag on every figure.

**2. AWARDED is not paid.** This is an award-ceiling report. Payments to `{{AUTHORITY_NAME}}` are a separate register (`fetch_payments_publisher_summary_result` / public-payments `publisher_summary`, PAID/COMMITTED) and are never netted against these figures. If ever shown, the PUBPAY caveat rides verbatim (`dail_tracker_core/caveats.py` → `PUBPAY`):
> sum-safe spend only; never add to procurement AWARD values (different grain). VAT basis varies by publisher and is unconfirmed for most (only HSE/Tusla are documented incl-VAT), so cross-publisher totals mix VAT bases — see data/_meta/procurement_payments_vat_matrix.json for the per-publisher basis.

**3. No scores, no verdicts.** No influence/risk/access/concentration/competitiveness score, index, or ranking; no "uncompetitive", "corruption", "capture", "waste", "rigged" framing (CI-forbidden as claims). Structure facts (single-bid, incumbency, framework, renewal, concentration) are **facts with denominators, not accusations**, and keep their "recorded fact, often wholly legitimate / never a verdict" caveat **verbatim**.

**4. Company-class / PII double-gate.** The generator must be structurally incapable of naming a natural person. Exclude at export time any row where `supplier_class = 'sole_trader_or_individual'`, `public_display = FALSE`, or `privacy_status = 'review_personal_data'`. No re-identification of individual payment rows. Personal insolvency excluded by policy.

**5. Logic firewall.** No `pd.read_parquet`/`pl.scan_parquet`, no `pd.merge`, no `groupby().agg`, no `duckdb.connect(":memory:")`, no analytical SQL (`JOIN`/`GROUP BY`/window) in report or page logic. Every aggregate/join lives in a registered pipeline view. The only escape hatch is a display-only aggregation on an already-filtered subset, tagged `# logic_firewall: display_only` — not a substitute for a missing view (that is why the PHASE-2-TODO sections stay stubbed until wired).

**6. Competition panel hard-fails.** TED `buyer_name` ≠ eTenders `contracting_authority`. Exact match or the literal string "No TED competition data for this buyer." — never a fuzzy match.

**7. Attribution travels with the data** (see footer). Sell the software/service/curation, never the underlying facts.

---

## Sources & attribution (reproduce verbatim on every report)

**eTenders / OGP corpus** (awards, live tenders, national expiring) — licence **CC-BY-4.0**:
> Contains Irish Public Sector Data (Office of Government Procurement) licensed under CC-BY 4.0.

Award-data caveat:
> AWARD values are ceilings/estimates, NOT money paid. Frameworks/DPS repeat one ceiling across supplier rows. Sum ONLY rows where value_safe_to_sum — and even that is awarded value, never expenditure. Upstream publishes quarterly with an inherent ~6-month lag (see data_currency).

**TED / EU-OJ** (competition, TED expiring) — EU open data (Commission Decision 2011/833/EU):
> Contains information from TED (© European Union), reused under Decision 2011/833/EU.

TED caveat:
> Pan-EU framework outliers (is_pan_eu_outlier) carry ceilings that dwarf the Irish market — exclude them from totals. Single-bid is a factual signal, never a verdict. Sum only value_safe_to_sum rows; never add to payments (different grain).

**CRO (Companies Registration Office)** (supplier identity resolution):
> Contains Irish Public Sector Data licensed under a Creative Commons Attribution 4.0 International (CC BY 4.0) licence.

**Public-body payments** (only if a payments register is referenced) — licence **CC-BY-4.0 (per-publisher source lists; see source_landing_url per row)**:
> Compiled from official payment/PO publications of each public body (Circular 07/2012 / FOI publication schemes).

Payments caveat:
> SPENT (paid) and COMMITTED (ordered) are different tiers — never blend them in one total. VAT basis varies by publisher and is unconfirmed for most: see the vat_matrix reference in this manifest before comparing across publishers.

**lobbying.ie / SIPO** (only if a cross-register footprint is added — co-occurrence, never causation):
> Contains lobbying register data © Standards in Public Office Commission, reused under its PSI re-use policy.

**Pass-through obligation:** any re-publishing customer must honour attribution, the never-sum caveat, no re-identification of person rows, no resale of raw exports, and each source licence. CC-BY does not cover data protection; any feature naming a natural person triggers GDPR controller obligations.