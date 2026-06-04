# CRO Financial Statements dataset — exploration

**Explored:** 2026-06-04 · **Source:** `https://opendata.cro.ie/dataset/financial-statements` (CKAN DataStore, `datastore_active=True`)

## TL;DR verdict

**Park it — do not ingest now.** This dataset is a **filing index** (submission
metadata + a PDF filename), *not* the financial figures. Most of what it offers
(filing recency / accounts-overdue signal) is **already derived** from the
companies register's `last_accounts_date`. Its only net-new value is the pointer
to the actual accounts PDF — and realising that is a heavy, low-yield OCR project
(abridged SME accounts, ~200k PDFs/year, retrieval path unconfirmed).

## What it actually is

Two yearly CSV resources, both DataStore-active (queryable via
`datastore_search` / `datastore_search_sql`):

| Resource | DataStore id | Rows | Distinct companies |
|---|---|---|---|
| Financial Statements 2022 | `508d4f8a-74a1-40c7-8b86-cdf0d54a4929` | 213,931 | 209,038 |
| Financial Statements 2023 | `dd413039-f628-4931-9788-dfc38eaf6b99` | 121,387 | — |

Schema (8 fields — **all metadata, zero financial figures**):

```
file_name                      e.g. "126127218.pdf"  ← pointer to the filed accounts PDF
company_num         numeric    ← joins the companies register
company_name        text
submission_num      text       e.g. "SR1970603"
submission_rec_date timestamp  received
submission_eff_date timestamp  effective
submission_reg_date timestamp  registered
submissions_accounts_to_date   timestamp  accounting-period end (2022 file → all 2022-12-31 etc.)
```

Partitioned by **accounting-period year** (the 2022 file's `accounts_to_date`
range is exactly 2022-01-01 … 2022-12-31), not by filing date.

## Why it's largely redundant

The CRO **companies register** (already ingested → `data/silver/cro/companies.parquet`)
carries `last_accounts_date`, populated for **491,692 / 815,950** companies (~60%).
`cro_normalise.py` already derives `accounts_overdue_flag` from it. So "has the
company filed / how recent / is it overdue" is **covered at company grain**.

The financial-statements dataset only adds:
- per-submission history (multiple filings per company), and
- the **PDF filename** of each filed accounts document.

## The only real prize — and why it's hard

`file_name` points at the actual filed accounts, where real numbers (turnover,
balance sheet) live. But:
1. **Retrieval path unconfirmed** — the CSV gives a bare filename, not a URL; the
   CRO document-download endpoint/pattern was not established in this pass.
2. **Abridged accounts** — most Irish SMEs file under the small-company regime
   (often balance-sheet only, no P&L/turnover), so even parsed, the financials
   are sparse for the majority.
3. **Volume** — ~335k PDFs across 2022–23; OCR/parse at that scale is its own
   project (cf. the SIPO OCR effort).

## If revisited later

- Establish whether `file_name` is fetchable (CRO document service / object store).
- Sample 50 PDFs to measure how many carry full vs abridged accounts — that ratio
  decides whether parsing is worth it.
- If pursued, treat figures as **silver-only** until a value taxonomy + privacy
  review exists (named small companies / sole-trader-adjacent entities).

## Access notes (DataStore API)

- Filtered: `GET /api/3/action/datastore_search?resource_id=<id>&q=…`
- SQL: `POST /api/3/action/datastore_search_sql` with
  `sql=SELECT … FROM "<resource_id>" WHERE …` (verified working; supports
  `COUNT(DISTINCT …)`, `ILIKE`, etc.).
