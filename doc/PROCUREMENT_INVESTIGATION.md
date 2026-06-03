# eTenders / public-procurement enrichment — investigation kickoff brief

> **Status:** investigation, planning/prototyping only. Sandbox rule applies — new
> code → `pipeline_sandbox/`, nothing wired into `pipeline.py`/`enrich.py` yet.
> Written so a fresh context window can resume cold. This is review-plan **Phase 5**.

## Executive summary — decision brief (2026-06-03)

Procurement spend is **one lifecycle split across levels & owners** (advertise → award →
commit/PO → pay → aggregate; EU / national / local). No source spans it — each exists to
satisfy a *separate obligation* (EU directives → TED; OGP → eTenders; Circular Fin
07/2012 → €20k PO lists; FOI 2014; NOAC → AFS). Fragmentation = layered rules by tier and
owner, "publish ≠ datafy", pre-open-data timing. The value-add is **stitching them via
CRO** into a "who-got-paid" ledger no single obligation produces.

**What's been done (evaluation only, no ETL):**
- **eTenders awards** — in gold (60,501 award-supplier rows); awarded value = *ceilings*,
  `value_safe_to_sum` flags set (naïve €570bn vs safe €23.3bn).
- **LA spend tier (the new corpus)** — seed registry for all **31 LAs**
  (`procurement_la_registry.py`): **~22 scrapeable now, 27 obtainable, 2 non-publishers**;
  parsed **22 councils / 5,771 rows** live; schema converges (`supplier·amount·description`);
  **digital everywhere, zero OCR**. National est. **~250–320k PO rows**, multi-year.
- **Format**: Excel/CSV = high-fidelity (CRO ~59–70%), minority of councils; PDF = the
  volume (16 councils, fitz + largest-x-gap), CRO ~35–66%.
- **Remaining sources measured**: TED API live (19,295 IE awards, **72% of 2025 carry real
  values**, zero-auth) = the real-value award layer; CKAN tabular (Kilkenny/Dept Housing) =
  minor/redundant or central-grain.

**Cost of the build (if greenlit):** bounded — **~10 of 31 councils need a ~1-line per-council
config** (column map / amount-sign / PO#-prefix / right-file selector); 3 need Playwright;
2 don't publish. One shared reader + the validated CRO matcher + a quarterly re-harvest.

**Open decisions (all gated on user — nothing started):**
1. Go/no-go on the **LA spend tier** (per-transaction "who got paid").
2. Whether to **pull TED** for real award values (highest-value single add, zero-auth).
3. If go: promote `procurement_la_registry.py` → `data/_meta/procurement_la_seed.csv` +
   build the shared reader; clear the ~10 configs + 3 Playwright councils.

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

**Tier 3 — actual SPEND (fixes the ceiling≠spend caveat):**
- **Procurement Related Payments over €20,000** / **Purchase Orders over €20,000.**
  ⚠️ **CORRECTION (census `probe_procurement_pdf.py` 2026-06-03):** the "106 datasets /
  100+ publishers" figure was a CKAN **free-text artifact** — `q="purchase orders over
  20"` returns 124 datasets but ~99 are GEOSPATIAL noise (EPA "Groundwater Pressures",
  Marine Institute seabed surveys) that merely contain "over"/"20". **Title-confirmed,
  only 25 spend datasets from 3 publishers on data.gov.ie:** Dept of Housing LG&H (29
  CSV resources, payments), Kilkenny CoCo (12 XLSX, POs), Kildare CoCo (10 PDF, POs).
  Most public bodies publish PO/payment listings on their OWN websites per Circular
  05/2023, **not** to the data.gov.ie portal — so the open-data spend corpus is small,
  and the "normalisation across 100+ bodies" framing was wrong.

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

**3. Actual-SPEND datasets** — REAL € paid (€5–8m/file, named payees: Dublin Airport
Authority, Micromail, RPS), per-body per-quarter ~100-row files, non-uniform schemas
(embedded newlines, `(€)`→`(�)`, different column names per body), CRO ~33%.
**Census correction (`probe_procurement_pdf.py`):** the open-data spend corpus on
data.gov.ie is SMALL, not 100+ bodies — only **25 title-confirmed datasets from 3
publishers**: Dept Housing LG&H (29 CSV, payments), Kilkenny CoCo (12 XLSX, POs),
Kildare CoCo (10 **PDF**, POs). The PDFs are **DIGITAL** (fitz extracts straight to
`supplier · €amount · category`; **no OCR**, 8/8 sampled). Verdict: highest fidelity,
but **smaller than feared** — 2 publishers are already tabular; the only PDF work is one
council, and it's fitz-trivial. Real cost is per-publisher column mapping, not scale.

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

## Off-portal LA harvest — Dublin region pilot (`probe_procurement_dublin_la.py`, 2026-06-03)

Since the open-data portals carry almost no council spend (3 publishers nationally), the
real data lives on each council's OWN website per Circular 05/2023. Probed all four Dublin
local authorities to size an off-portal harvest. **Result: four councils, four different
realities — no uniformity even within one region:**

| Council | Source | Format | Enumerable? | Coverage | Schema / grain |
|---|---|---|---|---|---|
| **South Dublin** | sdcc.ie | **XLSX** | ✅ clean URL template `…/purchase-order-over-20-000-quarter-{Q}-{YYYY}.xlsx` (19/20 quarters 200; 1 name-variant 404) | Q1 2020→present | `PO# · SUPPLIER · TOTAL(€) · DESCRIPTION · PAID(Y/N)`, ~172 PO rows/qtr — **richest**, line-level, even a paid-flag |
| **Fingal** | fingal.ie | **PDF** (digital) | ⚠️ scattered `…/sites/default/files/{upload-mm}/…20k….pdf` + `/media/<id>` — no template, must scrape a listing | 2013→2024 | `SupplierID · Acc element · Amount(€)`, line-level; € renders as `�` |
| **Dublin City** | smartdublin/data.gov.ie CKAN | CSV/XLS | — | **only 2012Q3–2014Q1 (abandoned)** | AGGREGATE prompt-payment return, **NOT** line-level POs — wrong grain + stale |
| **Dún Laoghaire-Rathdown** | dlrcoco.ie | — | ❌ | none | nothing published openly → FOI territory |

**Verified facts:** SmartDublin CKAN lists all 4 LAs as orgs (DCC 161 / Fingal 411 /
SDCC 230 / DLR 109 datasets) but spend coverage is ~nil; SDCC XLSX template HEAD-checks
200 for 19 of 20 recent quarters; Fingal PDFs are **digital (fitz, no OCR)**; DCC's only
spend dataset stops in 2014 and is the wrong grain; DLR's procurement page is policy PDFs
only.

**Implication for sizing:** an all-LA harvest is **bespoke-per-council** — different host,
format, URL pattern, schema and grain for each. Of the 31 LAs, expect ~half usable
(template-able XLSX/CSV or scrapeable digital PDF), the rest stale/aggregate/absent, with
**no shared schema** (each needs its own column map; two grains — line-level PO vs
aggregate return). The portal captures ~none of it. Sequencing if pursued: start with the
clean XLSX/CSV councils (SDCC-style template), add digital-PDF councils via fitz +
per-council listing scrape (Fingal/Kildare), and treat DCC-style aggregate returns and
non-publishers (DLR) as out of scope for a line-level spend layer.

## National SEED REGISTRY + scrape test (`procurement_la_seed.py`, 2026-06-03)

Built a per-council seed registry (council → finance landing page) and a generic
scrape-tester (harvest links → classify a real sample). **Now covers ALL 31 local
authorities.** Result: the off-portal harvest is far more tractable than "31 bespoke
scrapers" feared — a single-page harvest already works for ~half, schema CONVERGES, and
only 3 councils genuinely don't publish line-level PO data.

**Full tally (31 LAs) after clearing the fix-list (curl fallback + one-hop crawl added
to the harvester, 2026-06-03):**
- **~24 reachable over plain HTTP** (requests + curl fallback + one-hop sub-page crawl) —
  the 16 below **plus** Meath & Sligo (their `SSLError` was our Python TLS stack, NOT a
  server block — `curl` gets HTTP 200; added a curl fallback) and Clare, Leitrim, Laois,
  Louth, Fingal (files on a sub-page, reached by a one-hop crawl from the landing page).
- **4 publish behind a JS-rendered file list** (0 file links in raw HTML; Carlow/Cavan
  expose SPA/JSON markers) — Carlow, Cavan, Mayo, Roscommon. The *files* exist
  (e.g. **Mayo = 837 rows digital**, served as `getattachment/{guid}` URLs); only
  *enumeration* needs a rendered DOM (the project already has Playwright).
- **2 genuine non-publishers** — Dublin City (stale ≤2014 aggregate) and
  Dún Laoghaire-Rathdown (policy only).

So **~29 of 31 are obtainable**; only 2 truly don't publish line-level PO data.
Per-council link tallies/classifications: `c:/tmp/procurement_la/seed_report.json`.

> **Cross-validation note (2026-06-03):** a parallel context's `probe_procurement_pdf_counties.py`
> independently scanned several counties and **corrects two errors in the registry's first
> pass**, both caused by a seed URL pointing at the wrong page: **Donegal IS a publisher**
> (yearly PDFs at `donegalcoco.ie/media/{code}/YYYY.pdf`, **1,221 rows, digital** — the
> procurement landing page only lists the >€10m docs), and **Mayo has 837 digital rows**.
> Conversely this probe found a working **Cork City** route (the spending-and-revenue XLSX)
> that the counties probe had as blocked. Lesson: per-council coverage needs the *file-list*
> page, not the generic finance/procurement landing — the two probes' routes should be merged
> into one seed list. Both agree on the core finding: **digital everywhere, zero OCR.**

**The 16 ready now** (line-level supplier+amount+description, classified from a real sample):

| Council | Format | Sample schema | Files on page |
|---|---|---|---|
| South Dublin | XLSX | `PO·SUPPLIER·TOTAL·DESCRIPTION·PAID` | 220 |
| Cork City | XLSX | `Supplier·Sum of Gross Amount·Description` | 6 (+51 pdf) |
| Wicklow | XLSX/CSV | `Supplier·EURO·Description` | 111 |
| Monaghan | XLSX | `Supplier·Amount·Description` | 53 |
| Kilkenny | XLSX | `Ap/Ar ID·Period·EURO·DESCRIPTION` | 67 |
| Wexford | XLS/XLSX | (tabular; old `.xls` needs xlrd) | 68 |
| Cork County | PDF digital | `Supplier·Total·Description·Paid` | **107** |
| Kildare | PDF digital | `Supplier·Total·Description` | 52 |
| Westmeath | PDF digital | (Westmeath notice) | 55 |
| Waterford | PDF digital | `OrderNo·Supplier·…` | 44 |
| Galway County | PDF digital | `SUPPLIER·PRODUCT·EURO` (via **gaillimh.ie**) | 28 |
| Offaly | PDF digital | GL30 `Payments Greater than €20k` | 26 |
| Galway City | PDF digital | (PO PDFs; budgets page) | 10 |
| Kerry | PDF digital | (16 files; transient fetch err) | 16 |
| Longford | PDF digital | `SUPPLIER·EURO·DESCRIPTION` | 10 |
| Limerick | PDF digital | `Supplier·Paid·Description` | 2 |

**Three confirmed-at-scale positives:**
1. **Every actual PO PDF is DIGITAL (fitz, no OCR)** across all provinces — the only
   "SCANNED" hits were policy/guideline/contract docs (irrelevant). Zero-OCR confirmed nationally.
2. **Schema CONVERGES** to `Supplier · Amount(€) · Description` (± PO# / Period / Paid).
   Column order/naming differ, but it's one 3–4-field shape ⇒ ONE normaliser + a small
   per-council column-map, not 31 bespoke parsers. The Galway largest-x-gap reader handles
   PDF order differences.
3. **Deep archives** — Cork County 107, Wicklow 111, Kilkenny 67, Wexford 68, Westmeath 55:
   years of quarterly history off a single landing page.

**3 genuine non-publishers (exclude from a line-level layer):** Dublin City (only stale
≤2014 aggregate prompt-payment CSV), Dún Laoghaire-Rathdown (policy PDFs only),
Donegal (publishes only procurement >€10m, not PO-over-€20k).

**Fix-list — RESOLVED (harvester now has curl fallback + one-hop crawl):**
- *Was "TLS blocked"* → **FIXED via curl fallback**: Meath, Sligo. Their `SSLError` was
  our Python TLS stack; `curl` returns HTTP 200. (Galway-County's real WAF block was a
  separate case, solved earlier by the `gaillimh.ie` alt domain.)
- *Was "sub-page"* → **FIXED via one-hop crawl**: Clare, Leitrim, Laois, Louth, Fingal —
  the crawl follows a finance/procurement nav link to the actual file list. (Sampler
  sometimes grabs a neighbour doc e.g. a supplier-setup form; the *files* are present.)
- *Still need a headless browser (Playwright)* — Carlow, Cavan, Mayo, Roscommon: the file
  list is **JS-rendered** (0 file links in raw HTML; Carlow/Cavan expose SPA/JSON markers).
  Build-time fetch via the project's existing Playwright.
- *Uncertain* — Tipperary: 37 PDFs but the sample was a scanned *contracts* doc; its
  actual PO file needs a targeted check.

**Verdict for the build:** the seed-registry approach is validated. Plan: a committed
`council → landing_url → format → column_map → grain` registry + one shared reader (fitz
largest-x-gap for PDFs, direct read for XLSX/CSV) + the validated CRO matcher. Expect
~⅔ of 31 LAs to fall straight in; the rest are site-crawl (Fingal-type), browser-fetch
(Meath/Galway-County WAF), or out-of-scope (stale/non-publishers). OCR stays unneeded.
Report (`c:/tmp/procurement_la/seed_report.json`) holds the per-council link tallies.

## MERGED authoritative seed registry (`procurement_la_registry.py`, 2026-06-03)

Consolidated the two parallel probes' resolved routes into ONE registry (still pre-ETL —
not wired into `pipeline.py`). One row per LA: `council · region · entity · status · fmt ·
url (best file-list/sample) · pattern (quarter template) · schema · notes · src`. Emits
`c:/tmp/procurement_la/registry.csv`. Coverage:

| Status | # | Councils |
|---|---|---|
| READY-HTTP | 18 | South Dublin, Cork City, Wicklow, Monaghan, Kilkenny, Wexford, Cork County, Kildare, Westmeath, Waterford, Limerick, Offaly, Longford, Galway City, Galway County, Kerry, Meath, Sligo |
| READY-CRAWL | 4 | Clare, Leitrim, Laois, Fingal |
| NEEDS-RENDER | 5 | Mayo, Donegal, Carlow, Cavan, Roscommon (Mayo+Donegal data already confirmed) |
| NEEDS-CHECK | 2 | Louth, Tipperary |
| NON-PUBLISHER | 2 | Dublin City, Dún Laoghaire-Rathdown |

**22 scrapeable now (HTTP+crawl) · 27 obtainable incl. render-to-enumerate · 2 don't publish.**
Format mix 6 xlsx / 1 xls / 1 csv / 22 pdf — **every PO PDF digital, zero OCR.** Entities:
23 county + 2 city + 2 merged (Limerick/Waterford) + 4 Dublin. Promote to
`data/_meta/procurement_la_seed.csv` only on a build go-ahead.

## Coverage MEASURED — Excel vs PDF (`probe_procurement_coverage.py`, 2026-06-03)

Parsed the FULL set — live files (1–2 quarters each) across **22 of 31 councils**, matched
to CRO. Still evaluation (no ETL). Sampled totals: **5,771 PO rows, from one or two quarters
per council** (Galway County unblocked via `gaillimh.ie`).

| Format | Councils parsed | Rows | Avg CRO 1:1 |
|---|---|---|---|
| **XLSX** | 5 (SDCC, Cork City, Monaghan, Wicklow, Wexford) | 1,345 | **59%** |
| **CSV** | 1 (Wicklow) | 76 | 59% |
| **PDF** | 16 | 4,350 | **35%** (depressed by 5 mis-parses — see below) |

**Head-to-head conclusions:**
1. **Excel/CSV is the high-fidelity path — ~59–70% CRO**, near-zero parse work. Cleanly-parsed
   PDFs match comparably (Kildare 66%, Limerick 64%, Galway County 58%, Clare 56%, Mayo 54%);
   the 35% PDF average is dragged down by councils whose layout the generic reader mis-cuts.
2. **PDF is the volume** — 16 of 22 councils, most are PDF-only. The reader is unavoidable.
3. **Per-council parser debt is now ENUMERATED (the real cost number):** ~10 of 31 councils
   need a bespoke column-map/parse-config entry, the rest parse out-of-the-box:
   - *Parses but wrong* — Westmeath (CRO 0%, split mis-cut), Waterford (14%, OrderNo prefix),
     Fingal (0% — supplier published as an ID code, not a name), Laois (€397m = a total-row
     mis-grab), Leitrim (1 row — hit a prompt-pay aggregate).
   - *Skipped* — Kilkenny (supplier col `Ap/Ar ID(T)`; also on CKAN), Meath (no xlsx links via
     curl — likely pdf), Kerry / Sligo / Louth (fetched but sample was wrong-doc/aggregate).
   None are blockers — each is ~1 config (column map, prefix strip, or right-file selector).

**Clean out-of-the-box (~14):** South Dublin, Cork City, Monaghan, Wicklow, Wexford,
Cork County, Kildare, Longford, Galway City, Galway County, Limerick, Mayo, Donegal,
Clare, Tipperary, Offaly.

**Coverage estimate:** ~**160k rows across the 22 sampled councils' archives**; national
order-of-magnitude ≈ **250–320k PO rows**, multi-year (spans 2015→2026). Rough — a few file
counts include non-quarterly docs and a couple of councils couldn't be enumerated.

**Still not parseable over HTTP:** Carlow, Cavan, Roscommon (JS-rendered → Playwright);
Dublin City, DLR (non-publishers).

## Remaining sources — MEASURED (2026-06-03)

Checked the three non-council sources directly:

| Source | Measured | CRO | Notes |
|---|---|---|---|
| **eTenders gold** (have) | 60,501 award-supplier rows, value_eur on 34,460 | (matcher built) | Awarded-value = **ceilings**; `value_safe_to_sum` flags already in gold |
| **CKAN Kilkenny xlsx** | 896 rows / 8 qtrs 2018-19, €60.9m | 31% | Clean `Order Number·Supplier Name·Period·EURO·Description` **but amounts NEGATIVE** (debit sign → needs `abs()`). 12 qtrs on CKAN (2017-19); same data its own site continues post-2019 |
| **CKAN Dept Housing csv** | 1,431 rows / 2023-24, €86.3m | 45% | **CENTRAL gov** (the Dept's own payments, not an LA); 2014→2025 available, CC-BY |
| **TED API** (live, zero-auth) | **55,720 IE notices; 19,295 awards; 8,614 since 2024** | n/a | `POST /v3/notices/search`, eForms BT fields; **72% of 2025 awards carry a real award VALUE** (€ amounts); winner-name only ~21% via `winner-partname` (eForms links winners by org-ID → needs a resolve step) |

**Takeaways:**
- **TED is the real prize** — ~19k IE award notices, **real award values on ~72% of recent
  ones** (the only source that fixes ceiling≠spend at the award layer), public + zero-auth.
  Caveat: winner *name* needs an org-ID resolve; pre-eForms (≤2023) notices have empty BT
  value fields, so real-value coverage is strong only from 2024+.

### TED — PULLED & explored (`probe_ted_ireland.py`, 8,614 notices, 2026-06-03)
Paginated all IE `can-standard` award notices since 2024 (250/page × 35). Winner resolves
from `organisation-name-tenderer.eng[]`; `tender-value[]` is EUR.
- **4,428 of 8,614 (51%) carry a € value**; **median single award €245k**; span 2023-12→2026-06.
- ⚠️ **Headline €81bn total is MEANINGLESS — a data-quality trap.** `buyer-country=IRL`
  catches **pan-EU framework ceilings** where Ireland merely participates: the top "awards"
  are **GÉANT Vereniging** (the EU research-network body) €15.3bn / €14.85bn / €2.88bn IT
  frameworks — not Irish spend. IT-services shows €49bn almost entirely from these. **Use the
  median / per-notice values, exclude multi-supplier frameworks (1,005) and pan-EU buyers;
  never sum raw.**
- **Top buyers (real signal):** HSE (€906m), Irish Rail (€2.36bn), OGP (€599m), OPW, Education
  Procurement Service, Dublin City / South Dublin / Limerick councils, universities.
- **Winners skew FOREIGN** — European Dynamics, CloudFerro, VSHN, Telecom Italia, SoftwareONE,
  T-Systems — because TED captures big above-threshold EU framework winners. Irish names appear
  too (Deloitte Ireland, eir, AECOM Ireland, Arup, Ergo). → **CRO match only 35% by name**
  (vs ~50–65% in the LA PO layer): TED ≠ the Irish-SME spend, it's the big centralised/EU layer.
- **BIG WIN (verified):** `winner-identifier[]` **is the supplier's CRO number** and joins
  cleanly. (The probe first reported 0% — a type bug: string IDs vs Int64 `company_num`.)
  Re-tested: of identifiers that look like IE company numbers (172/288 ≈ 60% of a sample;
  the rest are VAT/foreign/names), **133 match CRO raw and 145/172 (~84%) after stripping
  non-digits + leading zeros.** So **Irish TED winners are ~84% matchable by ID** — far above
  the 35% name rate, and exact (no fuzzy). Identifier field is mixed (CRO numbers, VAT, even
  bare personal names → sole-trader **quarantine** applies). Summary: `c:/tmp/ted_ireland_summary.json`.
- **Verdict:** TED genuinely delivers real award values + a national who-buys map, zero-auth —
  but it's the **big-contract / centralised / EU-framework layer**, complementary to (not a
  substitute for) the LA per-transaction PO corpus. Two must-haves before any use: exclude
  pan-EU framework outliers, and reconcile the winner-identifier→CRO join.
- **CKAN tabular is a minor add** — Kilkenny CKAN duplicates a council we already reach
  (just cleaner + older quarters); Dept Housing is central-gov grain, not the LA corpus.
- **New per-council quirk logged:** Kilkenny stores PO amounts as negatives (sign
  convention) — one more ~1-line config, like the others.

## VALUE TAXONOMY & classification pattern (2026-06-03)

The single biggest way to mislead a user is to put a € figure on screen without saying
*which kind of money it is*. Every source measures a **different point in the spend
lifecycle**, and the figures are NOT interchangeable or summable across points. The
classification has two axes.

### Axis 1 — realisation tier (the spine: how real is this money?)

`PLANNED → AWARDED → COMMITTED → SPENT` (+ BUDGET as a parallel aggregate). Each tier
answers a different user question and must never be mixed or summed with another.

### Axis 2 — `value_kind` (controlled vocab; extends the 2 already in gold)

| `value_kind` | Tier | Plain-English verb | Summable? | Where it comes from |
|---|---|---|---|---|
| `estimate_advertised` | PLANNED | "expected ~€X" | **No** (pre-award guess) | eTenders/TED *notice* estimated value |
| `budget_allocated` | PLANNED (agg) | "budgeted €X" | within-LA/year only | AFS / NOAC service-division budgets |
| `contract_award_value` | AWARDED | "awarded €X" | **Caution** (commitment, may not all draw) | eTenders non-framework award; TED single award *(shipped)* |
| `framework_or_dps_ceiling` | AWARDED | "up to €X over N yrs (shared)" | **NO — the trap** | eTenders framework/DPS; TED multi-supplier framework (GÉANT) *(shipped)* |
| `po_committed` | COMMITTED | "ordered €X" | **Yes** (per PO) | LA Purchase-Orders-over-€20k (orders raised) |
| `payment_actual` | SPENT | "paid €X" | **Yes — true spend** | LA lists with a Paid flag; Dept "Procurement Related Payments" |

Note: a single contract can produce a row in *several* kinds (a notice `estimate`, an
`award_value`, then many `po_committed`/`payment_actual`) — with **no key linking them**, so
they must be presented as separate facts, never reconciled into "awarded vs spent = X left".

### The classification PATTERN (how to not overwhelm the user)

1. **One tier per view.** A page/section answers ONE question: *"Who got paid?"* (SPENT) or
   *"Who won contracts?"* (AWARDED) — never a blended list or total.
2. **Verb + confidence on every figure.** Render `value_kind`'s verb, never a bare €:
   "paid €X", "awarded up to €X", "estimated €X". The verb *is* the disambiguation.
3. **Headline number = the single summable kind in scope.** Sum only `payment_actual` or
   `po_committed`; for AWARDED show **"€X awarded across N contracts"** (a count, not a sum,
   because of ceilings) and always exclude `framework_or_dps_ceiling` + pan-EU outliers.
4. **Default to the realised layer, drill to the rest.** Lead with SPENT/COMMITTED (what
   people mean by "where did the money go"); offer AWARDED/PLANNED as progressive disclosure
   for procurement-savvy users.
5. **Persistent tier badge.** A small coloured pill (Paid / Committed / Awarded / Estimated)
   on every figure and card so the user always knows the realisation level at a glance.
6. **No cross-tier arithmetic** unless explicitly modelled and flagged — grains/identifiers
   don't reconcile, so "awarded minus paid" is a fiction by default.

This is the same firewall the gold already enforces (`value_safe_to_sum`) generalised to a
user-facing vocabulary: the data stays rich, but the UI only ever asks one honest question
at a time.

## BUDGET tier — FIRST INGEST of an uncovered source (`probe_la_finance_budget.py`, 2026-06-03)

All procurement micro-layers are covered by context windows (eTenders awards→gold; LA POs;
TED; mini-comp; semi-state/depts/health/edu; lobbying overlap). The one **taxonomy tier
nothing filled was BUDGET** — so ingested it. Source: **CSO PxStat**, official API, CC-BY,
**no scraping/no OCR**.
- **GFA04** — General Government expenditure by **ESA economic category**, **2000–2025**
  (current); 2025 = €124.3bn total expense (€36bn pay, €32bn social benefits, €21bn goods &
  services, €17bn capital). **GFA01** — revenue & expenditure 1995–2025.
- Ingested **1,887 rows**, each tagged `realisation_tier=BUDGET`, `value_kind=budget_allocated`,
  `value_safe_to_sum_within_table` (summable only within a matrix+year — never across tiers).
  Tidy CSV: `c:/tmp/la_finance_budget.csv`.
- **Honest limits found:** (1) the clean PxStat series is **general government** (central+local
  combined) by **economic category** — NOT per-LA, NOT by COFOG function. (2) The older
  NAH20/NAH27 and CEPHA tables are **historical (≤1995)** — discontinued; use the GFA series.
  (3) The richer **per-LA, by-service-division BUDGET** = the **amalgamated AFS** (Dept of
  Housing, all 31 LAs 2009–2023) which is **PDF on gov.ie** (datacatalogue entry is a pointer
  with 0 resources) → a heavier PDF extraction, **deferred**.
- **Net:** BUDGET tier now has a current, clean, national macro layer; the per-LA AFS PDF is
  the deferred deepening. Slots straight into the taxonomy with nothing to reconcile.

### Amalgamated AFS — SAMPLE ingested + DQ (`probe_afs_amalgamated.py`, 2026-06-03)
Pulled the 2020 audited amalgamation (gov.ie PDF, 49pp, digital) and extracted the
Income & Expenditure-by-service-division statement (p12). **DQ:**
- **8/8 divisions extracted; Σ gross = €6,750,822,111 vs printed €6,750,822,110 → €1 (rounding) = faithful.**
- Digital (no OCR) for 2020; older years may be scanned. Prior-year column present → time series.
- **⚠ SCOPE: the "amalgamated" AFS is the all-31-SUMMED national total — ZERO per-LA rows**
  (no council names in the doc). Per-council by-division = the **31 individual council AFS PDFs**
  (a separate, much larger ingest — the actual prize for per-constituency features).
- **⚠ DQ caught:** Note 16 (actual-vs-budget per division, p29) stacks an Expenditure *and* an
  Income sub-table → a naive line-parser mis-aligns; needs a targeted sub-table extractor.
- Accrual basis (revenue account) — "net expenditure" ≠ cash POs; **different grain from the PO
  layer, do not reconcile the two.**

**Tangible-benefits verdict:** the amalgamated AFS adds (1) spend by **service function**
(Housing/Roads/Water/… — the civic "what areas" cut CSO's economic-category GFA04 lacks),
(2) actual-vs-budget variance (Note 16, needs the sub-table parser), (3) a **national
denominator** to frame the micro procurement layers, (4) income-vs-expenditure (self-funding)
per division — **all cheap (1 PDF/yr) and clean.** BUT it's **national-only**; the per-LA
granularity that would power per-council/per-constituency civic features needs the 31
individual AFS PDFs. So: **amalgamated AFS = low-cost context/denominator; per-LA AFS = a
separate larger decision.**

### FULL INGEST complete (`afs_amalgamated_extract.py`, 2026-06-03) — 2016–2023
Ingested the I&E-by-division statement for **all 8 modern years (2016–2023, cut off at 2016
per request)**: **64 rows (8 yrs × 8 divisions), every year 8/8 and reconciling EXACTLY** to
its printed total → faithful extraction. Output (sandbox, NOT gold): `data/sandbox/parquet/
afs_amalgamated_divisions.parquet` (zstd), tagged `realisation_tier=SPENT`,
`value_kind=net_expenditure_actual`, `scope=all-31-LAs`.
- **DQ catches that the full run surfaced:** (a) **2019 reports in €millions with an "M"
  suffix** (`1,630.75 M`) while every other year uses full euros — made the number parser
  unit-aware. (b) Divisions matched by **keyword** (not exact string) to survive the wording
  drift across years. (c) Pre-2016 uses the old programme-group names → deliberately excluded.
- **Series shows real movement:** national LA gross revenue expenditure €4.0bn (2016) →
  €4.6bn (2018) → **€6.75bn (2020, COVID supports spike)** → €6.2bn (2021–22) → €6.7bn (2023);
  the 2020 jump reconciles exactly, so it's a real outturn surge, not a parse error.
- Net-by-division pivot confirms the funding story: Housing net ≈€0 (≈99% grant-funded),
  Recreation/Roads/Environment are the rates/LPT-funded net cost.
- Still NOT per-LA (amalgamated) and accrual-grain — the per-LA AFS remains the separate
  larger decision; Note-16 budget-vs-actual still needs the targeted sub-table parser.

### Data checks + unit tests (2026-06-03)
Validated beyond per-year reconciliation: **net = gross − income holds for 64/64 rows**
(max residual €10k = 2019 M-rounding); **cross-year consistency** — each year's restated
prior-year column equals the previous year's reported net across all 56 pairs (max €522k, 0
off by >€1m) = independent cross-document validation; 8 divisions every year; no negative
gross/income; the only >50% YoY moves are the tiny (~99% grant-funded) Housing/Misc net
divisions (genuinely volatile, not errors). Tests: `test/test_afs_amalgamated.py` (14 pass) —
`to_num` (incl. M-suffix/parens), a golden parse of a committed I&E page-text fixture, and the
invariants above on a committed 64-row golden parquet (`test/fixtures/afs/`, gitignore-negated).

### Medallion placement + promotion plan (when greenlit)
- **Bronze** = raw PDFs → `config.BRONZE_PDF_DIR/"afs"/{year}.pdf` (extractor self-fetches, like
  procurement's `ensure_csv` → headless-safe; immutable, re-derivable).
- **Silver** = the reconciled tidy fact → `config.SILVER_PARQUET_DIR/"afs_amalgamated_divisions.parquet"`
  (zstd/3/stats). It's a *conformed, source-faithful* fact (net=gross−income, reconciled,
  unit-normalised) — the natural silver home (like the CRO register), NOT a UI aggregate.
  Commit via a gitignore negation so Streamlit Cloud (clean clone, no ETL) can read it.
- **Gold** = SQL view(s) `sql_views/afs_*.sql` — `v_afs_divisions` (+ later `v_afs_division_trend`
  YoY, and `v_afs_per_capita` once population joins). All aggregation in views (firewall);
  `value_safe_to_sum` discipline carries over (sum within a year, never across tiers).
- **Transition (cbi/cro/procurement pattern):** (1) repoint download→bronze + output→silver;
  (2) add gitignore negation for the silver parquet; (3) register `("afs",
  "pipeline_sandbox/afs_amalgamated_extract.py")` in `pipeline.py` CHAINS (no deps → standalone)
  + a `_CHAIN_BLURBS` line; (4) add the `afs_*.sql` views + `utility/data_access/afs_data.py`
  (SELECT-only); (5) wire the tests into CI + a view-registration smoke test; (6) UI is a
  context/denominator panel, not a standalone page (deferred).

### Poller? — NO dedicated poller; annual freshness check
AFS is published **annually** (one audited PDF, mid-following-year). A continuous poller is
over-engineering. Instead: add AFS to `tools/check_freshness.py` with an **annual cadence**
(latest-year ingested), and a once-a-year "is next year's PDF up?" check that scrapes the
gov.ie collection page for a new year's link (reusing the existing PDF-poller pattern — URLs
are mostly predictable `…annual-financial-statement-{year}…`/`AFS_{year}.pdf` but carry guid
suffixes, so scrape-the-listing beats URL-guessing). Runs inside the existing freshness job.

## Story angles the data unlocks (eval — inference OK here, NOT in app UI)

The per-council spend corpus answers questions eTenders structurally cannot — *who actually
got paid*, locally, over time, and (via CRO) *who they are*. Candidate stories:

**Payment-layer (impossible on eTenders):**
- **Below-threshold / no-competition spend** — POs over €20k but under the €25k/€50k tender
  thresholds: money that never appears as a published competition.
- **Award vs actual spend drift** — eTenders framework *ceiling* vs the real call-off totals
  (the coverage JSON already shows €570bn naïve vs €23.3bn safe-to-sum — a 24× gap).
- **Repeat-winner concentration** — multi-year archives → "one supplier = €Xm over 6 years
  from Council Y"; % of a council's spend to its top-5 suppliers.

**Local-granularity (only because it's per-council):**
- **Does your council spend locally?** map supplier home vs council, tie to the
  constituency crosswalk + population for **per-capita** and **% spent locally**.
- **Council-vs-council benchmarking** — supplier diversity, consultancy/legal spend per head;
  pair with the AFS macro layer (budget by service division) for context.
- **Category trends** — PO descriptions give actual spend categories (Construction, Legal,
  Consultancy) — "consultancy spend up X% since 2019."

**Cross-link (the project's edge; CRO-matched):**
- **Lobbying ↔ contracts** — already built (`procurement_lobbying_overlap`, 123 firms on
  both registers): did a firm that lobbied a body later receive POs from it?
- **Public money → firms that later failed** (CRO corporate/insolvency).
- **Common directors winning across multiple councils.**

**Hard gate for the app (per [[feedback_no_inference_in_app]] / privacy):** ship these as
**source-linked co-occurrence, never implied influence/wrongdoing**; quarantine sole-trader/
individual payees (personal data). Inference belongs in this eval, not the UI.

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
