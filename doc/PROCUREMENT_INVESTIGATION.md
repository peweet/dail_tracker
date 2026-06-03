# eTenders / public-procurement enrichment — investigation kickoff brief

> **Status:** investigation, planning/prototyping only. Sandbox rule applies — new
> code → `pipeline_sandbox/`, nothing wired into `pipeline.py`/`enrich.py` yet.
> Written so a fresh context window can resume cold. This is review-plan **Phase 5**.

## Goal

Decide whether to build a procurement enrichment, and if so design it: awarded
public contracts → matched to CRO companies, joinable to corporate notices /
lobbying orgs / Departments — **source-linked, confidence-scored, privacy-safe**.

## What we already know (verified 2026-06-02, `pipeline_sandbox/probe_etenders_procurement.py`)

- **Source:** data.gov.ie "Contract Notices Published on eTenders", **CC-BY 4.0**.
  ONE CSV resolved via CKAN `package_show` →
  `https://assets.gov.ie/static/documents/7ba65f1b/Public_Procurement_Opendata_Dataset.csv`
  (~43 MB, cached at `c:/tmp/etenders_opendata.csv`). Coverage 2013–2025.
- **Shape:** 100,106 notices, 30 columns. Key fields: `Tender ID`, `Contracting
  Authority`, `Tender/Contract Name`, `Notice Published Date/Contract Created Date`,
  `Main Cpv Code` (+ description), `Spend Category`, `Threshold Level`, `Procedure`,
  `Notice Estimated Value (€)`, `Cancelled Date`, `Award Published`,
  **`Awarded Value (€)`**, `No of Bids`, `No of SME Bids`, **`Awarded Suppliers`**,
  `TED Notice Link`, `TED CAN Link`, `Platform`.
- **Awards are a subset:** 40,474 / 100,106 (40.4%) have an awarded supplier.
- **`Awarded Suppliers` separator is `|`** (NOT `;`) — multi-supplier cells; leading
  `|` artifacts; HTML entities present (`&amp;`) → must decode/clean.
- **Supplier → CRO** (reusing `cro_normalise.name_norm_expr`, company-suffix subset
  of 8,225): **45.3% clean 1:1**, 3.1% ambiguous, 51.5% no match. Lower than
  corporate→CRO (76%) because many suppliers are **foreign / trade-named**
  (GmbH, "SOPEXA CHINA", "J. Jonker en Zonen").
- **Privacy:** 16,677 distinct suppliers, **50.7% have NO company suffix** — sole
  traders / individuals / public bodies = personal data. Keep company-suffix
  suppliers; **quarantine bare personal names**.
- **Public bodies:** 7,313 distinct `Contracting Authority` values (OGP, HSE, OPW,
  councils + a long tail of schools) → Department crosswalk feasible but tail-heavy.

## Open questions for the investigation

1. **Award vs notice grain:** confirm the award subset and dedupe (Parent Agreement
   ID, re-published notices). What is the true count of distinct awards?
2. **Supplier cleaning:** decode HTML entities, strip leading `|`, split on `|`,
   drop public-body "suppliers". Does CRO match rate rise past 45%?
3. **Foreign suppliers:** quantify how many no-match suppliers are non-Irish (no CRO
   possible) vs Irish-but-trade-named (recoverable). Is a foreign-entity flag enough?
4. **Sole-trader policy:** confirm the rule — keep only company-suffix suppliers, or
   keep all but badge/quarantine personal names? (GDPR-safe default = quarantine.)
5. **Contracting Authority → Department:** build/curate a crosswalk for the top-N
   authorities; accept long-tail `unknown`. Reuse any existing dept alias table?
6. **Cross-links worth it?** supplier(CRO) ↔ corporate notices (does a public
   supplier later appear in a receivership?), ↔ lobbying orgs (did a contractor
   lobby?). Measure overlap before promising a feature. **No causal/influence
   wording** — source-linked co-occurrence only.
7. **Value sanity:** are `Awarded Value (€)` fields clean/parseable? Currency/format?

## Likely deliverables (if it proceeds)

- `pipeline_sandbox/procurement_etenders_extract.py` → silver
  `procurement_awards.parquet` (one row per award-supplier) + coverage JSON.
- `procurement_supplier_cro_match.parquet` (supplier → company_num, match_method,
  match_confidence, foreign_flag, sole_trader_quarantined_flag).
- Proposed views: `v_procurement_awards`, `v_procurement_supplier_company_match`,
  later `v_lobbying_org_public_contracts`.
- A NEW page or a section — design TBD; not a primary view until data is proven.

## Constraints / project rules

- Sandbox rule (code → `pipeline_sandbox/`); parquet writers use
  `compression="zstd", compression_level=3, statistics=True`.
- **No inference in app UI:** a contract award is a fact; do not imply favouritism,
  influence, or wrongdoing. Source-link every row (TED link / dataset).
- **Privacy:** sole-trader / individual supplier names are personal data — quarantine
  by default; never publish a bare individual's contract row without a policy decision.
- Reuse the validated CRO matcher (`cro_normalise.name_norm_expr`); beware
  over-stripping collisions (see corporate→lobbying "Engineers Limited" false match).

## Supplier-name dirtiness — root cause + repair approaches (2026-06-03)

**Corrected diagnosis:** the "Deloitte/eloitte" fragmentation is **a SOURCE data
bug, not our CSV parse.** The OGP export itself stores first-character-truncated
spellings — `eloitte Ireland LLP` (Deloitte), `azars` (Mazars), `ell Products…`
(Dell), `atapac Limited` (Datapac), `ujitsu (Ireland) Limited` (Fujitsu). Verified
by pulling the raw cell for the affected Tender IDs. `truncate_ragged_lines` is NOT
the cause.

**Scale** (`pipeline_sandbox/probe_etenders_supplier_dedup.py`, on 17,527 distinct
spellings): **10.8% (1,891) start with a lowercase letter** — the signature of a
dropped leading capital. ~6,000 award rows hide under truncated names.

**Approach stack (recommended order):**
1. **Trailing-punctuation / normalisation tidy** — 1,463 names end in `,. & /` or
   dangling connectives ("James Harte &", "Murphy and Co.", "Ltd."). Cheap, safe.
2. **Deterministic suffix-repair (PRIMARY FIX)** — for each lowercase-initial name,
   prepend each capital A–Z and keep the one that matches an existing canonical
   name. **Repairs 944 / 1,891 (≈6,000 award rows) at high confidence** —
   `azars`→`Mazars`, `eloitte Ireland LLP`→`Deloitte Ireland LLP`,
   `atapac Limited`→`Datapac Limited`. Deterministic, no false merges observed.
3. **CRO-anchored canonical name** — use the matched CRO registered name as the
   final supplier identity (also collapses trade-name variants).
4. **difflib fuzzy — RESIDUAL ONLY, REVIEW-GATED.** For the ~947 truncated names
   whose full spelling never appears, fuzzy is the only lever but is **dangerous**:
   at cutoff 0.85 it produced false merges (`eircom Limited`→`Piercom Limited`,
   `ryan and associates`→`Moran and Associates`). Never auto-apply; high cutoff +
   manual review, or a small override CSV for high-award names only.

**Implication:** any per-supplier ranking MUST run repair steps 1–3 first; raw
`group_by(supplier)` is unreliable. Steps 1–2 are deterministic and should land in
the extractor; step 4 stays out of the automatic path.

## Other procurement data sources (the OGP eTenders CSV is NOT the only one) — 2026-06-03

Verified via data.gov.ie CKAN `package_search`. Tiers by value/effort:

**Tier 1 — OGP national, CC-BY, CSV:**
- **Contract Notices Published on eTenders** — what we ingested. Notice + framework
  *ceiling* values (NOT spend).
- **Contracts for Mini-Competitions and Standalone Awards** — SEPARATE OGP quarterly
  dataset (2023, 2024 Q1–Q4…). Cols: `Name of Contracting Authority · Client CA ·
  Title of Contract · Suppliers · Contract Start/End Date · CPV codes`. The actual
  framework call-offs that DON'T appear in the main notices. No value column, but
  clean supplier↔contract↔date links. **Best, cheapest next add.**

**Tier 2 — EU / TED:**
- **TED (ted.europa.eu)** Tenders Electronic Daily — all above-EU-threshold IE notices
  as EU open data with STRUCTURED AWARD VALUES + CPV + suppliers. Our eTenders CSV
  already has `TED Notice Link`/`TED CAN Link` → joinable. Big (EU-wide); own probe,
  filter to IE. **This is a real-value source the OGP ceilings can't provide.**

**Tier 3 — actual SPEND (fixes the ceiling≠spend caveat), fragmented:**
- **Procurement Related Payments over €20,000** — 18 datasets, per Dept/body, yearly CSV.
- **Purchase Orders over €20,000** — 106 datasets, per council/body, quarterly,
  MIXED formats (CSV/XLSX/XLS/PDF). Actual POs but a normalisation project across 100+
  publishers.

**Sequencing:** eTenders (have) → Mini-Competitions (easy) → TED (real values) →
spend datasets (big, later). Only TED or the spend datasets resolve the
value-inflation problem; the OGP notice ceilings never can.

## Multi-source PROBE RESULTS (2026-06-03) — all four sources tested

Probes: `probe_procurement_minicomp.py`, `probe_procurement_spend.py`,
TED via API (inline). Findings:

**1. eTenders notices (have)** — 100k notices / 40k awards, CRO 48% (post truncation
repair), values are framework CEILINGS not spend.

**2. Mini-Competitions & Standalone Awards** — 2,257 contracts, **2023–2024 only**,
923 distinct suppliers. CLEANER than eTenders (0 truncation). Cols: authority,
title, suppliers, contract start/end/signing dates, CPV — **NO value column**.
**88.1% supplier overlap with eTenders → only 110 net-new suppliers.** Verdict:
**marginal add**, low priority. CRO 1:1 = 45%.

**3. Actual-SPEND datasets** ("Payments over €20k" 29 CSV res / "Purchase Orders over
€20k" pdf+xlsx, ~14+ publishers) — REAL € paid (€5–8m/file, named payees: Dublin
Airport Authority, Micromail, RPS). BUT fragmented: per-body, per-year ~100-row
files, **mixed CSV/XLSX/PDF**, non-uniform schemas (embedded newlines, `(€)`→`(�)`,
different column names per body), CRO ~33%. Verdict: **highest fidelity, highest
normalisation cost — a project**, not a quick win. Cherry-pick big publishers later.

**4. TED (ted.europa.eu API v3)** — **THE real-value source. Public, no auth.**
`https://api.ted.europa.eu/v3/notices/search` (POST JSON). **8,230 Irish notices
WITH a named winner**; ~60% carry an explicit `result-value-notice` + currency
(Spaceship Digital €72k, RPS Group €135,068, KC PRINT €1.76m, Electrical World
€360k). Winners CRO-matchable. Query: `buyer-country=IRL AND winner-name=*`.
**Gotchas:** fields are MULTILINGUAL dicts (extract `['eng']`; titles can come back
in any EU language); eForms field codes (harvest valid names by sending a bad
`fields` value — the 400 lists the vocab); pagination over thousands; winner-name
has `_identifier` suffixes + duplicates to dedup. Verdict: **best next build for
real award values — medium ingestion effort, high value.**

**Recommendation:** TED first (real per-award values via API) → skip/deprioritise
Mini-Competitions (marginal) → spend datasets later (cherry-pick high-value bodies).
Only TED and the spend datasets solve the ceiling≠spend problem; TED is far cheaper.

## Data freshness, ETL need & API access (verified against raw docs 2026-06-03)

*(Complements the PROBE RESULTS above — that section proves the data; this one answers
"how fresh, do we need an ETL, are there APIs".)*

### Freshness — periodic FULL-FILE snapshot, ~quarterly, currently to end-2025
- CKAN package `contract-notices-published-on-etenders`: `metadata_modified`
  **2026-01-20**; the single CSV resource was `created` 2025-10-10 with `last_modified`
  null (full-replace, not append). Verified via `package_show`.
- Stated coverage **01/01/2013 → 31/12/2025** ([gov.ie OGP opendata](https://www.gov.ie/en/office-of-government-procurement/collections/opendata/));
  award publication mandatory >**€25,000** (Circular 05/2023).
- Cadence: whole-file republish, **roughly quarterly** (observed Oct 2025 → Jan 2026).
  **Mini-Competitions** ship as *separate* per-quarter CKAN packages (2024 Q4 modified
  2026-01-20). No incremental/real-time feed for either.

### ETL — needed, but a THIN re-download-on-change wrapper (not a scraper)
Because it's a full replacement, ingestion is trivial; the real work is the cleaning
already prototyped in `procurement_etenders_extract.py`. A freshness check fits the
existing refresh-script pattern: `package_show` → compare `metadata_modified` → on
change re-download + re-extract the whole file.

### APIs — TWO zero-auth paths (verified)
1. **data.gov.ie CKAN API** — `package_show` / `package_search`. This *is* the
   ingestion + freshness-poll API (confirmed by direct call).
2. **TED Search API** — see PROBE RESULTS #4 above for the IE numbers. Raw-doc
   confirmation: `POST /v3/notices/search`, REST/JSON, **NO AUTH for Search**
   (only TED's *write/management* services — Publication/Validation/Conversion/Dev-Ops —
   need EU Login + key). Pagination: standard **15,000 docs/query, 250/page**;
   **token-based = unlimited**; max 10,000 fields/page. Swagger `api.ted.europa.eu/swagger-ui`.
   No documented request rate-limit (use polite backoff).
   ([TED API](https://docs.ted.europa.eu/api/latest/index.html) ·
   [Search API](https://docs.ted.europa.eu/api/latest/search.html))
3. **etenders.gov.ie portal** — fetched: **no API / no OCDS / no open-data section.**
   The CKAN CSV is the sole official bulk feed.
4. **OCDS / OpenTender** ([OCP registry pub 58](https://data.open-contracting.org/en/publication/58)) —
   third-party scrape, **CC-BY-NC-SA** (non-commercial → incompatible with our CC-BY
   pipeline), coverage only to **Nov 2023**, download-only. **SKIP.**

### Spend-tier PDFs — DIGITAL, fitz-extractable, NO OCR needed (`probe_procurement_pdf.py`, 2026-06-03)
Ran the dedicated PDF probe. CKAN surfaces **10 PDF resources, all from one publisher
(Kildare County Council, "Purchase Orders Over 20k")** — the PDF slice is a narrow long
tail, not the bulk. **8/8 sampled PDFs are DIGITAL text-layer** (text_chars 6.6k–13.3k,
100–197 money tokens/doc) — **fitz word-geometry extracts them straight to rows; OCR is
NOT required** (the PaddleOCR scaffold is unneeded here). The earlier "mixed/may-need-OCR"
fear is disproven for this publisher.
- **Layout** = clean 3-col `supplier · € amount · category`, one row per purchase order:
  `LAWLER BUILDERS (ATHY) LIMITED | €139,850.00 | Construction Costs`;
  `AECOM Ireland Limited | €22,088.34 | Professional Fees`. Suppliers repeat across many
  rows (one row = one PO line, not a contract).
- **CRO** exact-name 1:1 = **50%** on a tiny 14-supplier sample (suffix-repair + fuzzy
  from the eTenders work lifts it). Split each row on the first € token.
- **Real cost = per-publisher schema normalisation**, not extraction. Each council names
  its columns / spaces its money differently (`€ 62,236.50` vs `€62,236.50`).

**Grain difference vs CSV/API (this is the key point):** the PDFs record **actual
transaction-level spend** — real money committed on each ≥€20k purchase order, with a
free-text spend *category* — whereas the eTenders CSV records the **competition/award**
(contract *ceiling* value, CPV, procedure) and TED records **per-award result values**.
PDFs answer "who got *paid* how much"; CSV/TED answer "who was *awarded* what". A council
PO supplier may never appear in eTenders (below-threshold / un-published direct buys), and
one framework award spawns many POs that never appear as awards. PDFs have a spend
category but NO CPV / no tender link; eTenders has CPV but no actual expenditure.

## Kickoff prompt (paste into a fresh window)

> Resume the eTenders/public-procurement enrichment investigation. Read
> `doc/PROCUREMENT_INVESTIGATION.md` first — self-contained brief, planning/proto
> only, sandbox rule applies (new code → `pipeline_sandbox/`). The open CSV is
> cached at `c:/tmp/etenders_opendata.csv` and the first probe is
> `pipeline_sandbox/probe_etenders_procurement.py`. Work the brief's open questions
> in order: clean suppliers properly (decode `&amp;`, split on `|`, drop public
> bodies), re-measure supplier→CRO, quantify foreign vs Irish-trade-named no-matches,
> and confirm the sole-trader quarantine rule. Goal: a go/no-go with a concrete data
> model and proposed `v_procurement_*` views. No-inference rule: a contract award is
> a fact, never evidence of influence or wrongdoing; quarantine sole-trader personal
> data by default.
