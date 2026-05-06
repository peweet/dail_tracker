# payments_full_psa — integration plan

**Status:** plan only. No production files have been touched.
**Sandbox artefacts:**
  - [pipeline_sandbox/payments_full_psa_etl.py](payments_full_psa_etl.py) — schema-aware re-parser
  - [pipeline_sandbox/payments_2019_backfill_probe.py](payments_2019_backfill_probe.py) — 2019 PDF discovery
  - `data/gold/parquet/payments_full_psa.parquet` — output (already written)

## Why

`data/gold/parquet/payments_fact.parquet` (production) is TAA-only and silently drops:

1. **Jan-Apr 2020** rows (column-shift quarantine; recoverable)
2. **May-Jun 2020** rows (6-col schema dropped entirely)
3. **All PRA-side rows** at every period (`Vouched` / `MIN` / `CC` / `NoTAA` / blank band)

Effect: 2023 total reads as €5.45M when gript.ie publishes €5.95M. The new parquet reconciles to within 1.4%.

## Pre-flight (verifies the new parquet is a strict superset)

```bash
python pipeline_sandbox/payments_full_psa_etl.py
```

Then:

```python
import polars as pl
old = pl.read_parquet("data/gold/parquet/payments_fact.parquet")
new = pl.read_parquet("data/gold/parquet/payments_full_psa.parquet")

# TAA-only totals must match exactly per year
for y in range(2020, 2027):
    o = old.filter(pl.col("date_paid").dt.year() == y)["amount_num"].sum()
    n = new.filter((pl.col("date_paid").dt.year() == y) & (pl.col("payment_kind").is_in(["TAA","PSA_DUBLIN"])))["amount"].sum()
    print(y, round(o,2), round(n,2), "OK" if abs(o-n) < 0.01 else "DRIFT")
```

If any year says `DRIFT`, stop — investigate before proceeding.

---

## Steps

### 1. (Optional) Backfill 2019 PDFs into bronze

```bash
python pipeline_sandbox/payments_2019_backfill_probe.py            # discovery only
python pipeline_sandbox/payments_2019_backfill_probe.py --download # write into bronze
python pipeline_sandbox/payments_full_psa_etl.py                   # re-parse
```

Skip this step if matching gript's 2019 figure isn't a goal — the parser already
handles 2019 PDFs once they're in bronze.

### 2. Update [sql_views/payments_base.sql](../sql_views/payments_base.sql)

Repoint at the new parquet and project the columns the downstream views still
expect (`amount_num`, `payment_year`). Tiny diff — no view below this line
needs to change.

```sql
CREATE OR REPLACE VIEW v_payments_base AS
SELECT
    member_name,
    position,
    taa_band_raw,
    taa_band_label,
    date_paid,
    narrative,
    amount AS amount_num,
    EXTRACT(year FROM date_paid)::INTEGER AS payment_year,
    payment_kind,                              -- NEW: 'TAA'|'PSA_DUBLIN'|'PRA'|'PRA_MIN'
    source_pdf,                                -- NEW: provenance per row
    NULL::VARCHAR AS unique_member_code,
    NULL::VARCHAR AS party_name,
    NULL::VARCHAR AS constituency
FROM read_parquet('data/gold/parquet/payments_full_psa.parquet');
```

### 3. Wire the parser into [pipeline.py](../pipeline.py)

Add a stage that runs the sandbox ETL. Keep [payments.py](../payments.py) for now
(it still produces the silver `aggregated_payment_tables.parquet` that tests
exercise) — just append a follow-up call. Order: bronze fetch → existing
`payments.py` → `payments_full_psa_etl.py`.

Once the new parquet is the only consumer, delete the obsolete pieces in
[payments.py](../payments.py) (lines 154-189: silver→gold aggregation, top-TDs
output, quarantine writer). The new ETL replaces all of that.

### 4. Update tests

- [test/test_silver_parquet.py](../test/test_silver_parquet.py): add a row-count
  assert for `data/gold/parquet/payments_full_psa.parquet` (~11,500 rows;
  expect ~12,500 if 2019 backfilled). The existing schema check on
  `aggregated_payment_tables.parquet` stays valid until step 3 is finished.
- [test/test_sql_views.py:236](../test/test_sql_views.py#L236): change the
  `_skip_missing` target from `payments_fact.parquet` to
  `payments_full_psa.parquet` once step 2 ships.

### 5. Update the payments page provenance

[utility/pages_code/payments.py](../utility/pages_code/payments.py): the
provenance footer currently says "TAA-only" (or implies it). Update copy to
state TAA + PRA + Dublin + ministerial-PRA, with `payment_kind` available for
filter chips if you want a "TAA only" toggle. The headline numbers will
increase by ~10% — call this out in the changelog/release notes so users
don't think the data was wrong yesterday.

Also update the source string in
[sql_views/payments_summary.sql:14](../sql_views/payments_summary.sql#L14):

```sql
'Oireachtas Parliamentary Standard Allowance Records (TAA + PRA + Ministerial)'
   AS source_summary,
```

### 6. Update the page contract

[dail_tracker_bold_ui_contract_pack_v5/utility/page_contracts/payments.yaml](../dail_tracker_bold_ui_contract_pack_v5/utility/page_contracts/payments.yaml)
references `aggregated_payment_tables.csv` and an existing column-shift
`TODO_PIPELINE_VIEW_REQUIRED`. Replace those references with
`payments_full_psa.parquet` and remove the column-shift TODO (this plan resolves it).

### 7. Update memory

The reconciliation memory at
[memory/project_payments_full_psa.md](../../.claude/projects/c--Users-pglyn-PycharmProjects-dail-extractor/memory/project_payments_full_psa.md)
flips from "production is TAA-only, sandbox has full PSA" to "production is
full PSA". Either drop the memory or rewrite it to point at the new parquet.

---

## Acceptance criteria

- [ ] Pre-flight script reports no DRIFT for any year
- [ ] After step 2, `SELECT SUM(amount_num) FROM v_payments_base WHERE payment_year = 2023` returns ≈ €6,034,961 (was €5,453,839)
- [ ] [utility/pages_code/payments.py](../utility/pages_code/payments.py) renders without errors
- [ ] [test/test_sql_views.py](../test/test_sql_views.py) passes
- [ ] Provenance footer mentions both TAA and PRA components

## Rollback

Single-line revert in [sql_views/payments_base.sql](../sql_views/payments_base.sql)
points back at `payments_fact.parquet`. Nothing else needs reverting because
the column projection in step 2 preserves the existing downstream contract.
