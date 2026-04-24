# Test Suite — Dáil Tracker

## Philosophy

A data pipeline has three distinct failure modes that need different test types:

| Failure mode | Example | Caught by |
|---|---|---|
| Logic error | Sort-char join key drops a letter | Unit test on `normalise_join_key` |
| Schema drift | Oireachtas API renames a field | Pandera schema on silver/gold outputs |
| Data quality | A PDF row mis-parses attendance as 999 days | Pandera range Check on gold output |

Tests here follow that split: **unit → schema → integration**, cheapest first.

---

## Pandera in 60 seconds

Pandera validates DataFrames against a declared schema. You define a schema once; it runs at test time (and optionally at runtime in the pipeline).

The `DataFrameModel` approach (used here) looks like a dataclass:

```python
import pandera.polars as pa

class MySchema(pa.DataFrameModel):
    name: str = pa.Field(nullable=False)          # column must exist, no nulls
    count: int = pa.Field(ge=0, le=1000)           # bounds check
    date_str: str = pa.Field(nullable=True)        # nullable column

    class Config:
        strict = False  # allow extra columns (safe for wide DataFrames)

    @pa.dataframe_check
    def at_least_one_row(cls, df) -> bool:        # whole-dataframe check
        return len(df) > 0

# Validate:
MySchema.validate(polars_df)   # raises SchemaError on failure
```

`strict = False` is important for wide DataFrames (230+ columns in enriched attendance) — you only declare the columns you care about, extras are ignored.

---

## Test files and priorities

| File | Layer | What it covers | Priority |
|---|---|---|---|
| `test_gold_df.py` | Gold | master_td_list, enriched_attendance, committee_assignments | **High** |
| `test_silver_layer.py` | Silver | aggregated_td_tables, flattened_members, interests combined | **High** |
| `test_normaize_join_key.py` | Unit | Join key logic, accent/apostrophe handling | **High** |
| `test_pdf_extraction.py` | Bronze | Smoke tests on PDF extraction output shape | Medium |
| `steamlit_test_example.py` | App | Render smoke test (no business logic) | Low |

---

## Test categories

### 1. Schema validation (Pandera)
Declares what each output DataFrame must look like. Catches column renames, type changes, and missing fields introduced by upstream API or PDF changes.

### 2. Uniqueness checks
`unique_member_code` and `join_key` must be unique in master lists. Duplicates silently corrupt LEFT JOINs — they multiply rows.

### 3. Null checks on critical columns
`identifier`, `full_name`, `join_key` cannot be null in any gold output — they are join keys or display values. Nullable columns (party, constituency) are explicitly declared nullable in the schema.

### 4. ISO date format
Attendance dates are parsed from PDFs as strings (`2024-01-17`). A mis-parsed row produces garbage (`47` or `Chris`) that silently passes a string type check but breaks any downstream date arithmetic. Regex check: `^\d{4}-\d{2}-\d{2}$`.

### 5. Cardinality / range checks
- TD count: 127–174 (160 Dáil seats, allow churn)
- Sitting days per year: 0–300 (Dáil sits ~90–120 days/year; 300 is a generous ceiling that catches mis-parses like `999`)
- `year_elected`: 1900–2030

### 6. Join health
After enrichment, critical metadata columns (party, constituency) should have low null rates. High null rates indicate the join key is failing to match rows.

---

## PDF testing

PDF extraction is the hardest layer to test directly. The practical approach:

1. **Output shape test**: after `attendance.py` runs, silver CSV must have the 8 expected columns and at least 127 unique TD identifiers.
2. **No wildly out-of-range values**: sitting_days_count > 300 almost certainly means a footer row was included.
3. **Regex smoke test on raw output**: the `IRISH_NAME_REGEX` in `attendance.py` can match non-member text — testing that identifiers match a known TD whitelist catches this.

Full PDF fixture tests (embedding a sample PDF in the test suite) are possible but expensive to maintain. Deferred for now.

---

## Running the tests

```bash
# Fast — unit tests only, no files needed
pytest test/ -m "not integration" -v

# Full suite — requires pipeline to have run
pytest test/ -v

# Coverage report
pytest test/ --cov=. --cov-report=term-missing

# Single file
pytest test/test_gold_df.py -v
```

The `integration` mark requires gold/silver files to exist. Tests skip automatically (not fail) if files are missing, so CI can run the unit tests without pipeline output.

---

## Adding new tests

When a new pipeline step is added:
1. Add a Pandera schema for its output in the appropriate test file.
2. Write at least one unit test with inline sample data (no file I/O).
3. Write one integration test that reads the actual output file.

The sample data doubles as documentation: it shows exactly what a valid output row looks like.
