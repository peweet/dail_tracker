# Tangible Sources Review — eTenders/OGP (§3) + LA Budgets/AFS (§3.2) de-dup, plus full-family tractability ranking

**Reviewer pass:** 2026-06-05. **Focus doc:** `doc/dail_tracker_second_pass_tangible_sources.md`.
**Scope per task:** de-duplicate §3 (eTenders/OGP) and §3.2 (LA budgets/AFS) against the live extractors, produce an overlap ledger, then independently rank ALL families by build-tractability with extractor-shape / join-key / surface for the high-tractability ones. Analysis-first; a read-only probe (`pipeline_sandbox/probe_review_src2_tractability.py`) grounds the format/schema claims below.

---

## Claims Ledger

| claim | doc says | repo reality (path:line) | verdict |
|---|---|---|---|
| Dáil Tracker already ingests eTenders procurement | §3.1 "appears to ingest eTenders-style procurement" | `pipeline.py:76` chain `("procurement", "extractors/procurement_etenders_extract.py")` | confirmed |
| data.gov.ie eTenders open-data is an *additive* "alternative/stable source" | §3.1 reconciliation bullet 1 | the live extractor's source IS that dataset: `procurement_etenders_extract.py:39` URL `assets.gov.ie/.../Public_Procurement_Opendata_Dataset.csv`; `:46` landing `data.gov.ie/dataset/contract-notices-published-on-etenders`; `:43` "verified via data.gov.ie package_show". Probe: HTTP 200, text/csv, 43 MB. | **stale** (already the source, not additive) |
| Procurement-lobbying cross-references already ingested | §3.1 | `pipeline.py:81` `("procurement_lobbying", …procurement_lobbying_xref.py)` | confirmed |
| OGP framework expiry/lots/drawdown metadata is additive | §3.1 bullet 3 | live feed already carries `Competition Type` (`procurement_etenders_extract.py:117`), `is_framework_or_dps` (`:209`), `is_call_off` / `Parent Agreement ID` (`:210`), `value_kind` ∈ {framework_or_dps_ceiling, framework_call_off, contract_award_value} (`:220`). View exposes them (`sql_views/procurement_awards.sql:24-28`). The *per-framework page* metadata (human valid-from / expiry **date**, lot names, buyer-zone helpdesk) is NOT in the CSV. | **partly stale** (framework *classification* present; per-framework expiry *date* genuinely absent) |
| Quarterly mini-comp / standalone award CSVs are additive | §3.1 bullet 2 | not ingested anywhere; CKAN `package_show` shows 1 CSV per quarter, org=office-of-government-procurement; header = Contracting Authority / Client CA / Title / Supplier Name / Location / Signing+Start+End dates / CPV — **no award value column** | **confirmed additive** (Circular 10/14 >€25k awards; complements but does not duplicate the eTenders spine) |
| Amalgamated AFS already ingested | §3.2 (implied "current ingestion") | `pipeline.py:64` `("afs", afs_amalgamated_extract.py)` → silver `afs_amalgamated_divisions.parquet`; 2016–2023, 8 divisions, reconciled | confirmed |
| Per-LA AFS by division exists | §3.2 budget/AFS spine | `la_afs_extract.py` (Phase-1) → `la_afs_divisions.parquet`, ~21/31 councils, reconcile-gated, fitz+camelot dual-parser (`:68`,`:615`) | confirmed (not in `pipeline.py` yet; sandbox/silver) |
| LA per-transaction PO/payments >€20k exists | (not in this doc) | `procurement_la_payments_extract.py` builds `la_payments_fact.parquet` (cash-PO grain), NOT wired to `pipeline.py` | confirmed |
| LA **Budgets** (Tables A–F, pre-spend estimates) are ingested | §3.2 "Local Authority Budgets collection" + Fingal budget CSVs | no budget extractor exists; only AFS (audited actuals) and PO/payments. Budget = a distinct `value_kind` (PLANNED), genuinely absent | **confirmed additive** |
| Mini-comp / CSR / DPC / NWRA / CSO / planning are machine-readable | §3.1/§3.2/§4/§5/§6/§7 | probe confirms: mini-comp = CSV; CSR Q4-2025 = XLSX **and** CSV both published; DPC fines = HTML w/ 2 `<table>`, 41×'€', 35×'inquiry'; NWRA = real .xlsx (openxml, 10–31 KB); CSO register page = HTML 435 KB; NOAC PI = 10 MB digital PDF | confirmed |

---

## Architectural Assessment

**The headline §3.1 "additive value" is mostly already in the repo.** The single biggest claim — "data.gov.ie eTenders open-data as alternative/stable source" — is not additive: it is *the exact source the production `procurement` chain already downloads* (`procurement_etenders_extract.py:39`). The extractor is already the "stable open-data route", not the scraper the doc imagines replacing. Likewise framework/DPS/call-off **classification** and the never-sum value semantics are already modelled (`value_kind`, `value_safe_to_sum`, `is_framework_or_dps`, `is_call_off`) and surfaced in `v_procurement_awards`. So three of the five §3.1 bullets are redundant.

**What is genuinely additive in §3.1** is narrow but real:
1. **Quarterly mini-comp / standalone award CSVs** (Circular 10/14, >€25k). A *different population* from the eTenders contract-notice feed — these are below-OJEU mini-competition call-offs that buyers report separately. One clean CSV per quarter, no PDF, no OCR. Crucially it has **no value column**, so it is an *award-existence + CPV + authority + supplier* fact, never a money fact — it cannot be unioned into any €-total and must not be.
2. **OGP per-framework page expiry *dates* + lot names** — not in the CSV. But these are ~hundreds of bespoke HTML pages with no stable machine endpoint; high decay, low yield. Better as occasional manual reference than ETL.

**§3.2 (LA budgets/AFS):** the AFS *actuals* side is well covered (amalgamated national + per-LA by division, both reconcile-gated). The doc's residual additive item is the **LA Budgets collection** — the *pre-year estimate* (Tables A–F), a `PLANNED`-tier `value_kind` that does not exist anywhere yet. This is the one money-grain the project is missing on the LA axis (it has PLANNED-absent, has SPENT via AFS, has COMMITTED/SPENT via PO payments). It completes the budget→outturn story the doc explicitly wants ("distinguish planned budgets, estimated outturn, audited expenditure"). The Fingal CSV/API datasets the doc cites are real but per-council and partial; the **gov.ie 'Local Authority Budgets' collection** (standardised PDF Tables) is the spine, and it is a dual-parser PDF job analogous to AFS — moderate, not high, tractability.

---

## Devil's Advocate

- **eTenders "stable alternative" is a non-finding.** Recommending it as additive would re-ingest the existing source. Reject.
- **OGP framework pages are a per-page scraper trap.** Hundreds of hand-built `gov.ie/.../ogp-frameworks/<slug>` pages, each with idiosyncratic "valid from / expiry" prose. No bulk endpoint, no consistent template, high per-page decay when gov.ie restructures (it has, repeatedly). The *one* field of value (expiry date) does not justify a fragile N-page scraper; manual reference only.
- **Mini-comp CSV has no €value — resist the urge to sum it.** It would be tempting to treat "standalone awards over €25k" as spend. It carries no amount at all; any inferred €25k floor is a threshold, not a value. It is a relationship/coverage fact only (`value_safe_to_sum = false`, `value_kind = award_existence_no_value`).
- **NOAC / C&AG / OPR PDFs:** real and authoritative, but these are *narrative audit reports* (10 MB digital PDFs). NOAC PI report has tabular indicators worth extracting (housing vacancy, re-letting cost) but the rest is prose. Per `feedback_dual_parser_rule`, a 10 MB report whose value is ~6 indicator tables is uneconomic to fully parse — selective table extraction or manual-reference only. C&AG "housing schemes funding/delivery" is a single annual chapter = manual reference, not ETL.
- **The whole §1/§2 "board minutes" universe (Irish Rail, TII, NTA, HSE, LDA, SBCI, MARA) is the wrong shape for this app.** A lone board-minutes PDF is tangible-but-useless: unstructured prose, no join key, no money grain, and reading governance minutes to surface "signals" is exactly the *inference-in-app* the project forbids. Reject the entire minutes/FOI-log family as pipeline ETL.
- **PII landmines:** MARA Maritime Usage Licence submissions (named applicants, redacted-but-leaky), Irish Rail/LDA **FOI released-records PDFs** (third-party personal data), publicjobs appointment campaigns naming candidates. The precedent (`feedback_personal_insolvency_privacy`) bars surfacing named natural persons. Reject as sources.
- **State Boards / publicjobs:** board *membership* is legitimately public and joinable to TDs/Wikidata, but `membership.stateboards.ie` is a JS app with no documented bulk export — fragile scraper. Defer pending an open-data endpoint.

---

## Data Quality & Enrichments

For each **Build** item: `value_kind` + provenance/source-hash + zstd parquet + a reconciliation check (mirroring the existing extractors' discipline at `procurement_etenders_extract.py:240` / `la_afs_extract.py:579`).

- **Mini-comp CSV** → `value_kind = award_existence_no_value`, `realisation_tier = AWARDED`, `value_safe_to_sum = false` (no amount exists). Provenance: CKAN `resource_id` + `last_modified` + per-file SHA256 (CRO/CKAN pattern at `cro_financial_statements_extract.py:108`). Reconciliation: row-count per quarter vs CKAN `package_show` resource size; supplier_norm join-rate to the existing `procurement_supplier_cro_match` spine (sanity, not a money check).
- **LA Budgets (Tables A–F)** → `value_kind = budget_estimate`, `realisation_tier = PLANNED`, `value_safe_to_sum = true *within (council, year, table)* only`. NEVER union with AFS actuals or PO payments (3-money-grain rule). Reconciliation: Table A "total expenditure" line vs Σ division budgets (same reconcile-gate shape as `afs_amalgamated_extract.py:171`); dual-parser fitz vs camelot per `feedback_dual_parser_rule`.
- **DPC fines table** → `value_kind = regulatory_fine`, joinable org_name → CRO (45% clean-subset precedent). Provenance: page snapshot hash + retrieved_utc. Reconciliation: Σ fines vs DPC annual-report published total. Privacy: organisations only (no natural-person inquiries surface).
- **Social Housing CSR (XLSX/CSV)** → counts of homes by scheme/stage/LA, `value_kind = unit_count` (not money). Join key: LA name → constituency crosswalk (the same crosswalk gap flagged in `project_ssha_enrichment_source`). Reconciliation: Σ scheme homes vs the dataset's published headline (Q4-2025 = 51,638 homes / 3,151 schemes).
- **NWRA / Southern ERDF beneficiaries XLSX** → `value_kind = grant_committed` (EU co-funded operation ceiling, NOT paid), `realisation_tier = COMMITTED`. Join: beneficiary name → CRO. Reconciliation: Σ operation amounts vs programme CCI total.

---

## Build / Defer / Reject

| item | verdict | value/effort | reason |
|---|---|---|---|
| data.gov.ie eTenders open-data as "stable alternative source" | **Reject** | n/a | Already the live `procurement` source (`procurement_etenders_extract.py:39`). Zero new data. |
| Framework/DPS/call-off classification + value semantics | **Reject (done)** | n/a | Already modelled (`is_framework_or_dps`, `is_call_off`, `value_kind`, `value_safe_to_sum`) and surfaced in `v_procurement_awards`. |
| Quarterly mini-comp / standalone award CSVs (Circular 10/14) | **Build** | high / low | Genuinely additive population (sub-OJEU >€25k call-offs); one clean CSV/quarter, no OCR; completes the awards-coverage surface. No value → relationship fact only. |
| LA Budgets (Tables A–F, PLANNED tier) | **Build** | high / med | The one missing LA money-grain (estimate, not actual); completes budget→AFS→PO outturn story; dual-parser PDF like AFS, reconcile-gated. |
| DPC fines table | **Build** | high / low | 2-table HTML, org-named (no PII), CRO-joinable, completes corporate-enforcement surface; small + stable. |
| Social Housing CSR XLSX/CSV | **Build** | med / low | Ready XLSX+CSV, unit-count fact; value blocked only by the known LA→constituency crosswalk gap (shared with SSHA). Pairs with a future housing surface. |
| NWRA / Southern ERDF beneficiary XLSX | **Build** | med / low | Small clean .xlsx, COMMITTED grant ceiling, CRO-joinable; feeds a corporate/regional-funding enrichment. |
| CSO Register of Public Sector Bodies | **Defer** | med / low | Useful authoritative public-body universe (classification/canonicalisation aid), but a reference table, not a fact; wire when a procurement-authority canonicaliser needs it. |
| DHLGH planning datasets (ArcGIS/GeoJSON/CSV) | **Defer** | med / med | Genuinely machine-readable and rich, but no existing planning surface to feed; large scope; park until a planning page is scoped. |
| OGP per-framework expiry **date** + lot names | **Reject (as ETL)** | low / high | Hundreds of bespoke HTML pages, no bulk endpoint, high decay; one field of value. Manual reference only. |
| NOAC PI report indicator tables | **Defer (selective)** | med / med | A few tabular indicators (vacancy, re-letting) are worth selective extraction; the 10 MB prose body is not. Park behind an LA-performance surface. |
| C&AG / OPR audit chapters (PDF) | **Reject (as ETL)** | low / high | Narrative annual chapters; value is qualitative; dual-parser+reconciliation uneconomic. Manual reference. |
| Board-minutes / FOI-log family (Irish Rail, TII, NTA, HSE, LDA, SBCI, MARA, Enterprise Ireland) | **Reject** | low / high | Unstructured prose, no join key, no money grain; surfacing "signals" violates no-inference-in-app; PII in FOI-released records. |
| MARA MUL submissions / publicjobs candidate campaigns / FOI released-records PDFs | **Reject** | n/a | Named natural persons → PII bar (`feedback_personal_insolvency_privacy`). |
| State Boards membership (membership.stateboards.ie) | **Defer** | med / high | Membership is public + TD-joinable, but JS app with no bulk export = fragile scraper; revisit if an endpoint appears. |

### High-tractability extractor shapes (the Build items)

- **Mini-comp CSV** — `polars.read_csv` per CKAN resource (one/quarter); explode+clean supplier like the eTenders extractor; **join key** `supplier_norm` (→ `procurement_supplier_cro_match`) + `cpv_code` + `contracting_authority`; **feeds** the (future) Procurement page / existing `v_procurement_*` family as a coverage layer.
- **LA Budgets** — fitz+camelot dual reader (reuse `afs_amalgamated_extract.parse_ie` + `la_afs_extract.best_ie_page` machinery wholesale); **join key** `(council slug, year, division)` — same key as `la_afs_divisions`; **feeds** a per-LA finance surface alongside AFS actuals (planned-vs-actual).
- **DPC fines** — `pandas.read_html` / lxml on the 2 tables; **join key** `org_name → name_norm → CRO`; **feeds** the Corporate page enforcement badge (CBI-distress sibling).
- **CSR housing** — `polars.read_csv` (CSV resource); **join key** `LA → constituency crosswalk`; **feeds** a future housing-delivery surface.
- **ERDF beneficiaries** — `openpyxl`/`polars` per .xlsx; **join key** `beneficiary → name_norm → CRO`; **feeds** corporate/regional-funding enrichment.

---

## Bottom Line

The §3.1 "additive procurement value" is largely an illusion: the marquee item (data.gov.ie eTenders open data) is *already the production source*, and framework/DPS/call-off semantics are already modelled and surfaced — so reject those as redundant. The genuinely new procurement data is the **quarterly mini-comp/standalone-award CSVs** (a distinct sub-OJEU population, clean CSV, but value-less → relationship fact only) and, on §3.2, the **LA Budgets (Tables A–F)** PLANNED tier that completes the budget→AFS-actual→PO-payment money story the AFS extractors only half-cover. Independently ranked, the high-tractability/low-decay/surface-completing builds are mini-comp CSVs, DPC fines, CSR housing XLSX, and ERDF beneficiary XLSX, with LA Budgets a slightly higher-effort dual-parser PDF that is nonetheless the most strategically valuable. Everything in §1–§2 (board minutes, FOI logs) and the audit-chapter PDFs should be rejected as pipeline ETL — they are unstructured, join-keyless, PII-laden, or inference-inviting, and are at best occasional manual reference.
