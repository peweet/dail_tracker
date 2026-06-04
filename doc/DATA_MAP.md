# DATA MAP — public-money facts (where everything is, and what must never be conflated)

> **Purpose:** one place to orient when spinning up a context that touches the
> AFS / local-authority / procurement / public-payments data. Read this *first*.
> Last updated 2026-06-04.
>
> Companion docs: `doc/PROCUREMENT_BUILD_PLAN.md` (master procurement plan + value
> taxonomy §4b), `doc/PER_LA_AFS_BUILD_PLAN.md`, `doc/PUBLIC_PAYMENTS_FACT_SCHEMA.md`.
> Memory: `project_procurement_phase_taxonomy` (the DOC MAP + taxonomy),
> `project_la_afs_fact`, `project_la_payments_fact`, `project_procurement_etenders`.

---

## The mental model: 3 GRAINS, never sum across them

Every "€" below belongs to exactly one grain family. A euro in one family is **not**
comparable to — and must never be summed/reconciled with — a euro in another. This is the
single most important rule in this data space; the consolidation step (when it comes) only
unions *within* the payment grain.

```
BUDGET / BY-SERVICE-DIVISION     →  what a body spends per service area (AFS accounts)
AWARD / CONTRACT-CEILING         →  contracts advertised/awarded — NOT money paid
PAYMENT / SPENT (supplier-level) →  actual € to a NAMED supplier
```

The 2-axis value taxonomy (`realisation_tier` + `value_kind`) in
`PROCUREMENT_BUILD_PLAN.md` §4b is the controlled vocabulary for this; lock it before any
cross-source merge.

---

## A. Local-authority money — 4 facts, 3 grains (NEVER reconcile across them)

| Fact | File | Layer | Grain | Scope | Rows | Status |
|---|---|---|---|---|---|---|
| **Amalgamated AFS** | `data/silver/parquet/afs_amalgamated_divisions.parquet` | silver | revenue I&E by division | **national, all-31 summed**, 2016–23 | 64 | ✅ pipeline chain `afs` |
| **Per-LA AFS — revenue** | `data/silver/parquet/la_afs_divisions.parquet` | silver | revenue **net**-expenditure by division | per-council (21) | 168 | 🟡 sandbox, gitignored |
| **Per-LA AFS — capital** | `data/silver/parquet/la_afs_capital_divisions.parquet` | silver | **capital** expenditure by division | per-council (21) | 158 | 🟡 sandbox, gitignored |
| **LA payments** | `data/silver/parquet/la_payments_fact.parquet` | silver | **per-supplier** PO/payment | per-council, named suppliers | 11,091 | 🟡 sandbox, gitignored |

Producers:
- `pipeline_sandbox/afs_amalgamated_extract.py` → amalgamated
- `pipeline_sandbox/la_afs_extract.py` → per-LA revenue (reuses amalgamated `parse_ie`)
- `pipeline_sandbox/la_afs_capital_extract.py` → per-LA capital (reuses `la_afs_extract`)
- `pipeline_sandbox/procurement_la_payments_extract.py` → LA payments

**Why four facts, not one:**
- *amalgamated* = national budget **context** (all councils added together).
- *per-LA revenue* = each council's day-to-day spend **by service** — here **Housing nets ≈ €0**
  (HAP/RAS recoupment + tenant rents pass straight through; it's centrally/rent-financed).
- *per-LA capital* = each council's **build/acquire** programme by service — here **Housing is
  the dominant line** (€2.5bn pooled across 20, 34–68% of each council's capital) but **~98%
  DHLGH-grant-funded**.
- *LA payments* = **"who actually got paid"** (named companies, CRO-joinable) — the
  accountability micro-layer.

> The revenue vs capital split explains the "housing costs locals nothing" paradox: the
> revenue account shows the recoupment pass-through; the capital account shows the real
> (but centrally-funded) investment. Both say the same thing — the LA is largely a **conduit
> for centrally-financed housing**.

Coverage JSONs (provenance + reconciliation stats): `data/_meta/la_afs_coverage.json`,
`la_afs_capital_coverage.json`, `la_payments_coverage.json`.

---

## B. Procurement AWARDS — 2 facts (AWARD grain — ceilings, NOT spend)

| Fact | File | Layer | Rows | Status |
|---|---|---|---|---|
| **eTenders awards** | `data/gold/parquet/procurement_awards.parquet` | **gold** | 59,439 | ✅ LIVE — pipeline chain, 5 SQL views |
| ↳ supplier↔CRO match | `data/gold/parquet/procurement_supplier_cro_match.parquet` | gold | — | ✅ |
| ↳ lobbying overlap | `data/gold/parquet/procurement_lobbying_overlap.parquet` | gold | — | ✅ |
| **TED EU awards** | `data/silver/parquet/ted_ie_awards.parquet` | silver | 13,126 | 🟡 not wired |

⚠️ **Award values are framework/DPS CEILINGS, not money paid** — the "€570bn that isn't"
(24× overcount). Only `value_safe_to_sum` rows may be summed. Never concat into payments.
Producers: `procurement_etenders_extract.py`, `procurement_lobbying_xref.py`,
`ted_ireland_extract.py`. The only cluster actually shipped to the app
(`utility/data_access/procurement_data.py` + `sql_views/procurement_*.sql`; dedicated page
deferred).

---

## C. Central / semi-state PAYMENTS — 1 fact (PAYMENT grain, experimental)

| Fact | File | Layer | Rows | Status |
|---|---|---|---|---|
| **public_payments_fact** | `data/sandbox/parquet/public_payments_fact.parquet` | sandbox | ~8,021 | 🔴 NOT promoted |

19 central + semi-state publishers. ⚠️ **Still on the drifted `amount_semantics` column** —
must converge to `value_kind` + `realisation_tier` before promotion (HSE/Tusla, in
`procurement_hse_tusla_parser.py`, emits a DQ **JSON not a parquet** and a third vocab —
a cross-context coordination item). Producer: `procurement_public_body_extract.py`.

---

## D. Supporting / cross-reference facts (gold)

| Fact | File | Note |
|---|---|---|
| CRO ↔ corporate notices | `data/gold/parquet/cro_xref_corporate_notices.parquet` | company-number xref |
| CBI authorised firms (+ xrefs) | `data/sandbox/parquet/cbi_*.parquet` | Central Bank register |
| related docs | `data/silver/parquet/related_docs.parquet` | document linkage |

---

## Layer / status legend

- **gold / committed** (`data/gold/`) = live in the app, Cloud-readable. → only **eTenders
  procurement** + CRO xref.
- **silver** (`data/silver/`) = cleaned, pipeline-stage. AFS facts + TED + LA payments.
  ⚠️ **All AFS / LA / TED parquets are currently gitignored** (`*.parquet` global ignore;
  negation deferred to avoid a `.gitignore` edit race) → **NOT Cloud-readable**, local
  working files only.
- **sandbox** (`data/sandbox/`) = experimental, never promoted.

## ⚠️ Two complexity traps

1. **Stale duplicates in `data/sandbox/`.** Old copies of `procurement_awards`,
   `procurement_supplier_cro_match`, and `afs_amalgamated_divisions` live there as leftover
   probe artifacts. The **gold / silver** versions are authoritative — ignore the sandbox
   copies of these three.
2. **Almost nothing here is in the app.** Only the **eTenders procurement** cluster has
   shipped (gold + views + data_access). Every LA/AFS fact and `public_payments_fact` is
   sandbox or gitignored-silver — local only.

## Where consolidation stands (the union, gated)

Still the **ingestion phase** — sources parsed/tested piecemeal; **wholesale consolidation
NOT begun** (deliberate — premature merges corrupt the data). When it starts:
- **Only payment-grain sources union** → `public_payments_fact` + LA payments + HSE/Tusla,
  *after* all conform to the 2-axis taxonomy.
- **Award-grain** (eTenders, TED) and **budget/AFS-grain** (amalgamated, per-LA revenue,
  per-LA capital) stay as **separate sibling facts** — never concatenated in.

See `PROCUREMENT_BUILD_PLAN.md` §8c (ingestion backlog) + §8d (the gated union).
