# CRO Financial Statements dataset — exploration

**Explored:** 2026-06-04 · **Source:** `https://opendata.cro.ie/dataset/financial-statements` (CKAN DataStore, `datastore_active=True`)

## Status: free INDEX ingested 2026-06-04 · PDF figures parked (paywalled)

The **free filing index** is now ingested →
`pipeline_sandbox/cro_financial_statements_extract.py` →
`data/silver/cro/financial_statements.parquet` (335,318 filing events / 219,756
distinct companies; **100% join** to the CRO register on `company_num`). It is
kept for the one thing the register's single `last_accounts_date` cannot show:
**multi-year filing history / consistency** (109,742 companies filed in ≥2
period-years) and as the targeting map if the paid PDFs are ever pursued.

The **actual financial figures stay parked** — they're in the PDFs behind the
CORE paywall (~€2.50/doc), not the open data (see "the only real prize" below).

⚠️ **Caveat for any "went quiet / stopped filing" signal:** recent period-years
are *incomplete* (2023 was 121k vs 2022's 214k at ingest — filings still arriving
on later statutory deadlines). "Filed 2022 but not 2023" mixes genuine
non-filers with not-yet-filed, so never present it as delinquency without
accounting for the trailing window.

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

1. **The PDFs are PAYWALLED, not open data (decisive).** Confirmed 2026-06-04:
   the open-data portal publishes the CSV index **only** — no PDF bulk download,
   no public URL pattern (the dataset page says contact `registrar@cro.ie`; the
   object-store bucket `cro-bulk-store-public` 403s for these files). The
   documents are retrieved individually via **CORE** (core.cro.ie) at **from
   €2.50 per image**, pay-per-call, registered account only. So a "50-PDF
   sample" is **~€125 + a CORE account**, not a free scrape — and enriching at
   any scale is thousands × €2.50.
2. **Abridged accounts** — most Irish SMEs file under the small-company regime
   (often balance-sheet only, no P&L/turnover), so even if purchased+parsed, the
   financials are sparse for the majority.
3. **Volume** — ~335k filings across 2022–23; OCR/parse at that scale is its own
   project (cf. the SIPO OCR effort) — on top of the per-document fee.

**Net:** the free open data is the index (redundant with `last_accounts_date`);
the actual numbers sit behind a per-document paywall. The PDF path is **not a
free enrichment** and is parked pending an explicit decision to spend on CORE.

Sources: CRO Access to Data <https://cro.ie/services-and-help/access-to-cro-data/>;
CRO financial-statements dataset <https://opendata.cro.ie/dataset/financial-statements>
(CC BY 4.0, CSV/API only).

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
