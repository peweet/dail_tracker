---
tier: SPEC
status: LIVE
domain: money
updated: 2026-06-04
supersedes: []
read_when: designing/building the unified procurement+payments gold model or the value_kind/realisation_tier taxonomy
key: SPEC|LIVE|money
---

# Public Contracting & Payments — Unified Data Model + Schema Contract (DRAFT)

**Project:** Dáil Tracker
**Status:** DRAFT / design — **not built; no unified gold parquet yet.** (`procurement_awards`
gold + `ted_ie_awards` silver + per-LA AFS + `la_payments_fact` + `public_payments_fact`
sandbox all exist as *separate* tables today — this doc is the target model to conform them to.)
**Created:** 2026-06-03 · **Unified-model pass:** 2026-06-04
**Companion to:** `doc/PROCUREMENT_MASTER.md` (master + value taxonomy §8),
`doc/PROCUREMENT_MASTER.md`, `doc/DATA_MAP.md` (status board + grains).

> **Subordinate to the master plan `doc/PROCUREMENT_MASTER.md`.** Implements the master's
> **VALUE TAXONOMY (§4b): `value_kind` + `realisation_tier`**, `value_safe_to_sum` derived from
> `value_kind`. Where Part B below still says `amount_semantics`, that is the LEGACY column on
> the current sandbox parquet — it is **renamed to `value_kind` (+ a new `realisation_tier`)** in
> this model; do not carry `amount_semantics` forward. Tests/firewall reference master §7/§9.

---

# PART A — The unified model (OCDS-aligned dimensional design)

> **Why this part exists:** five source tables (eTenders, TED, LA payments, public-body
> payments, HSE/Tusla) all describe *a supplier, a public buyer, an amount, a date, a kind* —
> but at **different lifecycle stages that must never be summed together**. Rather than invent a
> scheme, this follows the established answers: **OCDS** (open-contracting lifecycle stages),
> **Kimball** (conformed dimensions, one fact per business process, explicit additivity), and
> **medallion staging→marts**. Scaled to a solo Polars + DuckDB-views project — **no SCD2, no
> surrogate-key framework, no warehouse**; the value is the contract + two conformed facts +
> view-layer enforcement.

## A.1 The lifecycle — `realisation_tier` ≡ OCDS stages (never sum across)

Every public euro is measured at exactly one stage. A euro at one stage is **not** comparable
to a euro at another; consolidation only ever happens *within* a stage.

| `realisation_tier` | OCDS stage | `value_kind` (controlled vocab) | additive? |
|---|---|---|---|
| **PLANNED** | tender / planning | `estimate_advertised`, `budget_allocated` | no |
| **AWARDED** | award | `contract_award_value` (caution), `framework_or_dps_ceiling` (**never**) | guarded |
| **COMMITTED** | contract | `po_committed` | yes (within publisher+period) |
| **SPENT** | implementation | `payment_actual` | **yes** (true spend) |

`value_safe_to_sum` is **derived** from `value_kind` (true for `po_committed`/`payment_actual`;
`contract_award_value` with caution; ceilings/estimates never). One rule, generalised — not
re-decided per source.

## A.2 Two grain-separated facts, one shared contract

Kimball: *awarding a contract* and *disbursing a payment* are different business processes →
**separate fact tables sharing conformed dimensions.** Physical separation makes a cross-grain
`SUM()` impossible (the project's #1 risk — the "€570bn" mirage).

| Fact | Tier(s) | Sources | Grain (one row =) |
|---|---|---|---|
| **`fct_award`** | AWARDED (+PLANNED) | eTenders, TED | a notice × awarded supplier |
| **`fct_payment`** | COMMITTED / SPENT | LA payments, public-body payments, HSE/Tusla | a published payment/PO line |

Both use the **same column contract** (§A.4) and the same dimensions. They are **never unioned
into one amount column**; they meet only on the supplier dimension, in a view, after the join
gate (Part B hard-rule). The **AFS budget facts** (`la_afs_divisions` / `_capital` —
council×year×division, *no supplier*) are a **third grain**: sibling tables, never in either.

## A.3 Conformed dimensions — built ONCE, referenced by both facts

| Dimension | Key | Built from | Notes |
|---|---|---|---|
| **`dim_supplier`** | `supplier_norm` (+ `cro_company_num`) | `cro_normalise.name_norm_expr` | the **single place** name→CRO matching happens; carries `supplier_class`, `name_truncated`. Today CRO lives in a *separate* eTenders match table, *inline* in TED, *absent* in payments — collapse to one dim. |
| **`dim_buyer`** | `buyer_id` | publisher seed + authority names | `buyer_type` {department, local_authority, semistate, agency, hospital, education_body}; optional parent department. |
| **`dim_cpv`** | `cpv_code` | source CPV | code → division → description (awards only). |
| date | `year`/`period` | — | lightweight; no calendar dim. |

The **lobbying overlap** and **SIPO** links are **bridges off `dim_supplier`** (co-occurrence
disclosures), **not facts and never summed in**.

## A.4 The shared column contract (both facts emit exactly this)

```
identity   source_dataset {etenders|ted|la_payments|public_payments|hse_tusla}
           payment_id/award_id (deterministic hash of the natural key)
           source_file_url, source_file_hash, source_row_number, source_page_number?,
           parser_name, parser_version, downloaded_at, source_caveat
parties    buyer_id → (dim_buyer); buyer_name_raw
           supplier_norm → (dim_supplier); supplier_raw, supplier_display
money      amount_eur, currency,
           realisation_tier {PLANNED|AWARDED|COMMITTED|SPENT},
           value_kind  (controlled vocab, §A.1),
           value_shared_across_suppliers, value_safe_to_sum (DERIVED), vat_status
class/time  cpv_code?, description_raw?, year, period_raw?, quarter?, event_date?
privacy    privacy_status {public|quarantined}, public_display, privacy_reason?
quality    data_quality_flags(struct: is_total_row, is_aggregate, amount_missing,
           name_truncated, amount_is_outlier, is_duplicate_candidate)
```

## A.5 Per-source → contract mapping (the staging layer)

| Source | buyer field → | amount → | supplier-norm → | tier | gap to close |
|---|---|---|---|---|---|
| eTenders `procurement_awards` | `Contracting Authority` | `value_eur` | `supplier_norm` | AWARDED | **add `realisation_tier`**; fold CRO from `procurement_supplier_cro_match` |
| TED `ted_ie_awards` | `buyer_name` | `award_value_eur` | `winner_name_norm` | AWARDED | **add `realisation_tier`**; CRO already inline |
| LA payments `la_payments_fact` | `publisher_name` | `amount_eur` | `supplier_normalised` | COMMITTED/SPENT | reference template — already has both axes; add CRO |
| public-body `public_payments_fact` | `publisher_name` | `amount_eur` | `supplier_normalised` | SPENT | **`amount_semantics` → `value_kind` + add `realisation_tier`**; add CRO |
| HSE / Tusla | (publisher) | (amount) | (norm) | SPENT | **materialise to parquet** (today a DQ JSON); map 3rd vocab → `value_kind` |

## A.6 Build pipeline (medallion: staging → union → marts)

1. **`stg_<source>`** — one transform per source that renames/recasts to the §A.4 contract
   (this *is* the §A.5 gap-closing work). Output stays the producer's layer.
2. **union same-grain producers** — `UNION ALL` the staged AWARD producers → `fct_award`; the
   staged PAYMENT producers → `fct_payment`. (Never union across the two.)
3. **conformed dims** — build `dim_supplier` (the one CRO match) + `dim_buyer` once; both facts
   reference them.
4. **marts/views** — `sql_views/*.sql` expose tier-scoped, additivity-safe metrics (§A.7).

## A.7 Additivity enforced in the semantic layer, not in humans

Every money metric in `sql_views/` **filters `value_safe_to_sum` AND is scoped to a single
`realisation_tier`** — e.g. a `total_paid` metric can only ever touch `payment_actual` rows; an
"awarded" figure is a COUNT (+ a guarded, caveated sum). A **test asserts no view sums across
`realisation_tier`** (extends master §7/§9). This is the OCDS "never sum across stages" rule and
the Kimball additivity rule, encoded once in the view layer.

## A.8 Every figure is EXTRACTION-DERIVED — there is no single authoritative total

**This is as important as the tier rule, and orthogonal to it.** Almost every euro in these
facts is **parsed out of a published document** (PDF purchase-order lists, AFS statements,
sometimes scanned reports) or a semi-structured export — **not read from an authoritative
ledger.** The number on screen is *"the figure we extracted from document X"*, not *"the amount
that was paid."* The two caveats are different axes:

- **`value_kind`/`realisation_tier`** answers *what KIND of money is this* (ceiling vs paid).
- **`extraction_confidence`** answers *how reliably do we even know the number* — and the honest
  answer is often "approximately."

Why the real number isn't clear, and the model must say so:

- **Extraction is lossy.** OCR/column mis-read, supplier-name bleed, VAT basis (incl/excl),
  grain confusion (line vs aggregate vs total row), multi-quarter cumulative files. Even when a
  row reconciles, the *figure* is only as good as the parse.
- **Coverage is partial.** 20/31 councils, ~19 publishers, HSE/Tusla pending; awards are
  ceilings. **No total is complete** — every aggregate is a *floor*, not *the* number.
- **Therefore "how much did X get / spend" is not knowable from these sources alone** — only
  *"at least €Y, across the documents we could read, as extracted."*

**Model carries it** (already partly in Part B): `extraction_status` {ok|partial|needs_review},
`extraction_confidence` {high|medium|low}, `source_file_url`/`source_page_number` on **every**
row; aggregates additionally report `n_source_documents` + the confidence mix.

**Presentation MUST state it** (see Part C): every € is a link to the document it came from;
totals are labelled *"based on figures extracted from N published documents — indicative, not an
audited total"*; low-confidence rows are visibly flagged; **never** render "Company X received
€Y" as a bare fact — always "€Y, extracted from [source]." (This is the
`feedback_no_inference_in_app` rule applied to *quantities*: present the verifiable extracted
figure + its source, never an asserted authoritative total.)

---

# PART B — `fct_payment` detail (the payment-grain fact)

> Part B is the detailed contract for **one of the two facts in Part A** (`fct_payment`,
> COMMITTED/SPENT). It is the most-evolved spec (built first, on the messiest sources) and the
> template the shared §A.4 contract is generalised from. Where it says `amount_semantics`, read
> `value_kind` + `realisation_tier` per the header note.

This is the proposed data contract for the unified public-body **payments / purchase-order**
corpus that the bespoke per-publisher parsers (HSE, Tusla, …) feed. It fixes the grain,
value-semantics, privacy, and provenance decisions *before* any row is written to gold.

---

## ⚠️ HARD RULE — DO NOT JOIN UNTIL THE DATA IS STABLE AND VERIFIED

**No cross-enrichment join — to CRO, lobbying, SIPO political finance, corporate/insolvency notices, CBI, charities, eTenders awards, or anything else — until the ingested payment data is stable and extensively verified, per publisher.**

A premature join is not just incomplete — it is **actively misleading**, and far more dangerous than missing data:

- A mis-parsed supplier name (digit-prefix bleed, wrapped cell, OCR error) joins to the **wrong** company and attributes public money to a firm that never received it.
- An unresolved entity split (`PFH TECHNOLOGY` vs `PFH TECHNOLOGY GROUP`) **understates** a supplier and silently breaks any "total received" figure.
- Joining lobbying or political-finance data onto unverified payments manufactures **apparent connections that may be parser artefacts** — exactly the kind of false "influence" inference the project forbids (see `feedback_no_inference_in_app`).
- A grain or VAT mix-up turns a defensible figure into a fabricated one the moment it is joined and summed.

**Gate for enabling any join:** for each publisher, (1) parser output reconciled against the source file by manual spot-check, (2) supplier-name quality passing the DQ battery, (3) `value_safe_to_sum` / `vat_status` / grain correctly set, (4) privacy quarantine applied, (5) entity resolution (CRO) reviewed for false matches. Only then may a row participate in a join, and even then cross-source co-occurrence is **co-occurrence only — never an influence claim**.

Until that gate is met, this table stands alone: source-linked records, summed only within a single publisher + grain + VAT basis, nothing inferred.

---

## 1. Design decisions

1. **Scope: money *out*, line-item grain only.** This table holds **payments and purchase orders** (departments, HSE, councils, agencies, education). eTenders **contract awards stay in `procurement_awards`** — a different grain (a notice, multi-supplier, value = awarded-not-spent, framework ceilings). The two are *never* unioned into one amount column; they meet only at the supplier spine (shared `supplier_norm` / `cro_company_num`) in a view — **after** the join gate above.

2. **One row = one published source line.** Natural key = `(publisher_id, source_file_hash, source_page_number, source_row_number)`. Re-extraction is idempotent; every figure is traceable to a specific row on a specific source file.

3. **Three columns gate every sum:** `value_safe_to_sum`, `vat_status`, `amount_semantics`. You cannot sum across VAT bases or across grains. Aggregate/category/total rows carry `value_safe_to_sum = false`.

4. **Privacy is a stored column, not a UI afterthought.** Sole-traders / individuals default to `quarantined` + `public_display = false`.

5. **Carry, don't normalise, VAT.** VAT basis differs per publisher (HSE incl., BIM excl.) and cannot be reliably reversed per line — so it is carried and views must group by it.

---

## 2. Columns

### Keys
| column | type | notes |
|---|---|---|
| `payment_id` | str (PK) | deterministic hash of the natural key; stable across re-runs |
| `content_hash` | str | hash of (publisher, year, quarter, supplier_norm, amount_eur, description, doc_ref) — **duplicate detection only**, never identity |

### Publisher & provenance (all non-null)
| column | type | notes |
|---|---|---|
| `publisher_id` | str | e.g. `ie_hse` |
| `publisher_name` | str | |
| `publisher_type` | enum | department / semi_state / state_body / agency / hospital / education_body / local_authority |
| `sector` | str | transport, health, … |
| `source_landing_url` | str | |
| `source_file_url` | str | the exact file |
| `source_file_name` | str | |
| `source_file_hash` | str | sha256 of the downloaded bytes |
| `source_page_number` | int? | null for xlsx/csv |
| `source_row_number` | int | |
| `downloaded_at` | date (UTC) | |
| `parser_name` | str | |
| `parser_version` | str | |
| `extraction_status` | enum | ok / partial / needs_review |
| `extraction_confidence` | enum | high / medium / low |

### Period (per-row — HSE proves files can be multi-quarter cumulative)
| column | type | notes |
|---|---|---|
| `period_raw` | str | verbatim ("Q42021", "Jan-25") |
| `year` | int? | |
| `quarter` | str? | Q1–Q4 |
| `period_start` | date? | derived only where unambiguous |
| `period_end` | date? | derived only where unambiguous |

### Supplier (the spine — touched by joins only after the gate)
| column | type | notes |
|---|---|---|
| `supplier_raw` | str | verbatim from source |
| `supplier_display` | str | light-cleaned (digit-strip, trim) for UI |
| `supplier_norm` | str | join key — **reuse `cro_normalise.name_norm_expr`**, do not invent a new one |
| `supplier_class` | enum | company / sole_trader_or_individual / foreign_company / public_body / unknown — reuse the eTenders classifier |
| `cro_company_num` | str? | populated by the existing CRO matcher — **only after the join gate** |
| `cro_match_method` | enum? | exact_norm / manual / none |
| `cro_match_confidence` | float? | |

### Amount & value semantics (the careful core)
| column | type | notes |
|---|---|---|
| `amount_eur` | float? | null ⟺ `data_quality_flags.amount_missing` |
| `currency` | str | default EUR, carried explicitly |
| `amount_semantics` | enum | payment / purchase_order / invoice_total / supplier_period_aggregate / category_total |
| `value_safe_to_sum` | bool | **true only** for line-item grains; false for aggregate/category/total |
| `vat_status` | enum | incl_vat / excl_vat / unknown |
| `paid_flag` | bool? | some bodies mark paid vs committed |
| `is_aggregate` | bool | true for `supplier_period_aggregate` / `category_total` |

### Description / classification
| column | type | notes |
|---|---|---|
| `description_raw` | str? | |
| `publisher_category` | str? | body's own category / GL description |
| `po_number` | str? | |
| `doc_ref` | str? | invoice / order / date reference |

### Privacy
| column | type | notes |
|---|---|---|
| `privacy_status` | enum | public / quarantined |
| `public_display` | bool | |
| `privacy_reason` | str? | sole_trader / individual_name / special_category |

### Quality & caveats
| column | type | notes |
|---|---|---|
| `data_quality_flags` | struct(bool) | `is_total_row`, `is_duplicate_candidate`, `amount_is_outlier`, `name_truncated`, `amount_missing`, `supplier_missing` |
| `source_caveat` | str | per-publisher caveat (VAT basis, "indicative", cumulative-file, …) |

---

## 3. Invariants (become `test_public_payments_gold_contract.py`)

1. `payment_id` unique; `source_file_url` and `source_row_number` non-null on every row.
2. `value_safe_to_sum = true` ⟹ `amount_semantics ∈ {payment, purchase_order, invoice_total}` **and** `is_aggregate = false` **and** `data_quality_flags.is_total_row = false`.
3. `privacy_status = quarantined` ⟹ `public_display = false`.
4. `supplier_class = sole_trader_or_individual` ⟹ `privacy_status = quarantined` unless a documented review overrides.
5. `cro_company_num` present ⟹ `supplier_class ∈ {company, foreign_company}`.
6. `amount_eur` null ⟺ `data_quality_flags.amount_missing = true`.
7. **Aggregation rule (enforced in SQL views):** any sum filters `value_safe_to_sum` **and** groups by `vat_status`; never sum a publisher's line items together with its own `supplier_period_aggregate` rows for the same period (double-count guard).
8. **Join gate (process invariant):** no `cro_company_num` / lobbying / SIPO / corporate-notice join is populated for a publisher until that publisher passes the verification gate in the hard-rule section above.

---

## 4. Deliberately excluded
- No `is_influence` / `is_distressed` / any derived-judgement column (`feedback_no_inference_in_app`).
- No stored blended "total public spend" field — that number exists only as a *filtered, grain- and VAT-aware* view, never a column.
- eTenders awards — separate `procurement_awards` table, joined only at the supplier spine, only after the gate.

---

## 5. Open decisions (provisional defaults below — confirm before build)
1. **Aggregate-grain sources (SVUH per-supplier totals, ESB Networks category totals).**
   *Provisional default: INCLUDE with `value_safe_to_sum = false` and `is_aggregate = true`*, so the supplier view can surface "SVUH paid United Drug €41.8m" without ever summing it into a line-item total. Requires the §3.7 double-count guard. (Alternative: a separate `public_payments_aggregate_fact`.)
2. **VAT.** *Provisional default: CARRY `vat_status`, force views to group by it* — do not normalise to one basis at parse time (irreversible per line).

---

## 6. Write convention
Parquet, partitioned by `publisher_type` then `year`; `compression="zstd"`, `compression_level=3`, `statistics=True` (per the project's parquet-write rule).

---

## 7. Build order (after this contract is agreed)
Per the plan's Phases 4→9: finish per-publisher parsers (one grain at a time, clearly labelled) → assemble `public_payments_fact.parquet` → DQ + privacy pass → **verification gate per publisher** → *only then* CRO/entity resolution and any cross-enrichment join → SQL views → tests → optional Procurement page.

---

## 8. Test & data-quality plan — **DEFERRED, do not build yet**

> **Status: PLAN ONLY.** Tests are *not* being written while extraction is in active flux —
> the publisher set, schema, and parser internals still churn, and tests on a moving target
> are waste/false-confidence. This section is the blueprint to execute **once the feature
> stabilises** (schema frozen, publisher set settled, privacy pass on). Build it then.

### 8.1 How it must slot into the existing CI
The repo CI (`.github/workflows/ci.yml`) already defines the gates this work has to fit:
- **`ruff check .` + `ruff format --check .`** gate **every committed `.py`** — so the
  sandbox scripts must be lint+format clean *before first commit* (today they are not:
  unused `amt_i` in `inspect_hse_tusla.py`, import-sort in `procurement_hse_tusla_parser.py`).
  While iterating, either keep them uncommitted or clean on commit.
- **`test` job** runs `pytest -m "not integration and not sql and not sources and not bronze"`
  → **only unmarked, fast, data-free tests run in CI.**
- **`sql-contracts` job** runs `pytest -m sql` against *committed gold* (asserts a gold
  parquet exists first).
- **`basedpyright`** is scoped to `services/` + pure-logic modules — `pipeline_sandbox/` is
  **not** typechecked today; no obligation unless that scope is widened.
- **`firewall`** is UI/data-access only — irrelevant to this ETL.

### 8.2 Tier 1 — pure-function unit tests *(CI, unmarked, no data files)* — highest value
Test the parsing primitives on **synthetic inputs** (no PDF/parquet needed, so they run in
CI and lock the logic that actually breaks):
- `clean_supplier` / `DIGIT_PREFIX`: `"539106 A HORTON LTD"` → `"A HORTON LTD"`; clean name unchanged.
- `to_eur`: `"€1,234.56"`→1234.56, `"(20)"`→-20, `"20,000.00 IACT"`→20000.0, junk→`None`.
- `norm_name`: case/space/trailing-digit normalisation is stable + idempotent.
- `cols_by_xcuts`: a synthetic word-row + HSE / Tusla cut lists → expected column buckets.
- `hse_row` / `tusla_row`: synthetic cell lists → correct field mapping, period parse, amount.
- seed `validate()`: catches a duplicate id / bad enum / missing url.

### 8.3 Tier 2 — seed registry test *(CI, unmarked — the CSV is committed)*
Against `data/_meta/procurement_publishers/publishers_seed.csv`: loads as CSV, required
columns present, `publisher_id` unique, enum membership for `source_status`/`source_format`/
`grain`, `landing_url` present where `source_status != NOT_FOUND`. (Mirrors plan §6 Phase-1
tests + §9 acceptance criteria.)

### 8.4 Tier 3 — gold-contract test *(marker: `integration`; local until promoted)*
Assert the §3 invariants against `public_payments_fact`:
unique `payment_id` + provenance non-null · safe-to-sum gate · privacy gate · sole-trader
default · cro⇒company · amount-null⇔flag · aggregation/double-count rule. Mark `integration`
so CI skips it (the parquet lives in `data/sandbox/`, gitignored). When the table is
promoted to committed gold, either move it to the `sql`-style contract job or add a tiny
fixture (next point).

### 8.5 Tier 4 — data-quality assertions *(marker: `integration`)*
Threshold checks (extend the existing `dq()` + `public_payments_coverage.json`, don't
duplicate): no unexpected negatives/zeros · **single-row outlier share < 50% per publisher**
(the TII €1.2bn guard) · period-null rate under threshold (currently ~12%) · supplier-name
quality (leading/trailing-digit %, very-short %, total-row count) · content-duplicate rate
per publisher (HSE 87 / Tusla 253 — needs a dedup policy) · **no VAT-basis mixing in any
summed view** · once privacy is on, **zero `public_display=true` among quarantined rows**.

### 8.6 Fixtures
Tier 3/4 without committed gold → a small hand-built **synthetic fixture parquet** (~20 rows
spanning both grains, VAT bases, a quarantined row, a total row, an outlier, a null-period
row) committed under the test tree. **Mind the `*.parquet` gitignore** — needs a negation
rule + `git add`, the exact trap documented for the SQL-view fixtures.

### 8.7 What is *not* planned (consistent with one-shot scope)
No CI job dedicated to procurement, no nightly DQ run, no automation/scheduling. The four
tiers above are the whole intended surface, and only Tiers 1–2 ever run in CI.

---

# PART C — Presentation / Information Architecture (when a page is built)

> Planning only — **no page is being built yet.** This fixes *how the multifaceted model is
> organised for a reader* so the IA is agreed alongside the data model. Supersedes the
> eTenders-only page sketch in `PROCUREMENT_MASTER.md` §5.

## C.1 Organise by ENTITY, with the lifecycle *inside* — not by source

The data answers two user questions ("follow a company", "follow a public body / my area")
through one lifecycle. Three ways to slice it; only one works:

- ❌ **By source** (an "eTenders page", a "TED page") — exposes plumbing; the reader should
  never need to know the data architecture.
- ❌ **By lifecycle at top level** ("Awarded page", "Paid page") — clean but not how people ask.
- ✅ **By entity** (a *company*, a *public body / area*) — matches real questions; the
  **lifecycle becomes the structure *inside* an entity** (the tiered dossier). Matches the
  project's org-centric `rankings-*` pattern.

**Sources disappear from the UI** — the reader sees *awarded / ordered / paid* (the
`realisation_tier`), never "eTenders vs TED vs LA POs".

## C.2 Section shape — a "Public Money" section, two entry points, dossier leaves

```
PUBLIC MONEY
├─ ① Landing / framing
│     "Three different things — awarded, ordered, paid — never added together."
│     + the EXTRACTION caveat (C.4): figures are read from published documents, indicative.
│     → [ Find a company ]    [ Find a public body ]
├─ ② SUPPLIER view (the Dossier)   — rank by contracts WON (count); leaf = tiered dossier
│        identity (CRO + ⚠confidence) · lobbying flag
│        ▸ AWARDED €X (ceilings excluded) ▸ COMMITTED €Y ▸ PAID €Z   — never summed
├─ ③ PUBLIC BODY view              — by dept/council/agency: who they buy from / pay
│        → council body-profile cross-links to its AFS budget (sibling page)
└─ ④ Methodology — awarded≠paid worked example · coverage · extraction caveat · provenance
```

**AFS budget = a sibling page, not part of this section** (different grain: council×service, no
supplier). It answers a constituent question ("what does my council spend on housing/roads").
**Cross-link** both ways with the council body-profile.

## C.3 The four never-blend layout rules (from the value taxonomy)

1. **One tier per section** — Awarded / Ordered / Paid never share a list or a total; persistent
   tier badge on each block.
2. **The verb is the disambiguation** — "awarded €X" / "ordered €Y" / "paid €Z", never a bare €.
3. **Count is the headline, € is caveated** — rank by contracts *won*; value only with its tier
   label + `value_safe_to_sum` filtering.
4. **No cross-tier arithmetic** — the two physical facts make "awarded + paid" impossible; the
   IA inherits that safety.

## C.4 The extraction caveat as a UI primitive (§A.8 made visible)

- **Persistent disclosure** near every figure block: *"Figures are extracted from published
  documents — indicative, not audited totals."*
- **Every € links to its `source_file_url`** (+ page) — the figure is always traceable.
- **Totals are framed as floors**: *"at least €Y, from N documents we could read"*, with the
  confidence mix shown — never "€Y" as the definitive amount.
- **Low `extraction_confidence` rows are visibly flagged** (and `needs_review` rows excluded
  from headline figures).
- Pairs with C.3: a number on this page always carries **two** qualifiers — its **tier**
  (what kind of money) and its **extraction status** (how well we know it).

## C.5 What this gives the reader
- Land on a **company** → its whole footprint across the lifecycle, each figure source-linked
  and tier-labelled (e.g. AECOM: *awarded* €14.93m / *ordered* €6.80m / *paid* €2.47m — three
  honest figures, never one).
- Land on a **public body / their council** → who it buys from, with a jump to its service-level
  budget.
- Never a misleading blended "€X total", and never a number presented as more certain than the
  document it was scraped from supports.
