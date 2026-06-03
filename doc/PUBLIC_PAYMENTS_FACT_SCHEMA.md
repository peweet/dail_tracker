# `public_payments_fact` — Schema Contract (DRAFT)

**Project:** Dáil Tracker
**Status:** DRAFT / design sketch — **not built, not in `pipeline.py`, no gold parquet yet**
**Companion to:** `doc/PROCUREMENT_SEMISTATE_EXPANSION_PLAN.md` (esp. §3 constraints, §5 columns, §11 pilot results)
**Created:** 2026-06-03

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
