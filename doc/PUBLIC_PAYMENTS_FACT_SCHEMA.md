# `public_payments_fact` — Schema Contract (DRAFT)

**Project:** Dáil Tracker
**Status:** DRAFT / design sketch — **not built, not in `pipeline.py`, no gold parquet yet**
**Companion to:** `doc/PROCUREMENT_SEMISTATE_EXPANSION_PLAN.md` (esp. §3 constraints, §5 columns, §11 pilot results)
**Created:** 2026-06-03

> **Subordinate to the master plan `doc/PROCUREMENT_BUILD_PLAN.md`.** This schema must
> implement the master's **VALUE TAXONOMY (§4b): `value_kind` + `realisation_tier`** — do **not**
> use the `amount_semantics` enum drafted in §2 below if it conflicts; reconcile to `value_kind`
> + `realisation_tier` and derive `value_safe_to_sum` from `value_kind`. The §8 test plan and
> any firewall rules **reference master §7 (tests) and §9 (firewall checklist)** rather than
> re-specify them. (See `project_procurement_phase_taxonomy` memory → PROCUREMENT DOC MAP.)

This document is the proposed data contract for the unified public-body **payments / purchase-order** corpus that the bespoke per-publisher parsers (HSE, Tusla, …) will eventually feed. It exists so the grain, value-semantics, privacy, and provenance decisions are fixed *before* any row is written to gold.

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
