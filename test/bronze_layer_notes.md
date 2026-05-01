# Bronze Layer Testing Notes

> Foundation document for constructing `test_bronze.py` and `test_pdf_extraction.py`.
> Read alongside TEST_SUITE.md which defines the overall testing philosophy.

---

## What the bronze layer contains

| Source type | Location | Ingestion method |
|---|---|---|
| Attendance PDFs | `data/bronze/pdfs/attendance/` | Manual download, `attendance.py` scraper |
| Payment PDFs | `data/bronze/pdfs/payments/` | Manual download, `payments.py` scraper |
| Member interests PDFs (Dáil + Seanad) | `data/bronze/interests/` | Manual download, `member_interests.py` scraper |
| Members API JSON | `data/bronze/members/` | `flatten_members_json_to_csv.py` → oireachtas.ie API |
| Legislation API JSON | `data/bronze/legislation/` | `legislation.py` → oireachtas.ie API |
| Votes JSON/CSV | `data/bronze/votes/` | `transform_votes.py` → oireachtas.ie API |
| Lobbying CSVs | `data/bronze/lobbying_csv_data/` | Manual CSV export from lobbying.ie |

---

## What bronze layer tests check

Bronze tests answer one question: **did the raw input arrive and is it minimally readable?**
They do NOT validate content, schema, or data quality — that belongs at silver.

### 1. Endpoint availability — `@pytest.mark.sources`
Checks that source URLs are reachable. Network-dependent, slow (~70 HTTP HEAD requests).
Already implemented in `pdf_endpoint_check.py`. Wrap this as a pytest test file.

```python
@pytest.mark.sources
def test_all_pdf_endpoints_reachable():
    broken, all_ok = endpoint_checker()
    assert all_ok, f"Broken endpoints: {broken}"
```

Key design note: run this marker separately from `integration` — never require network in the default test run.

### 2. File existence — `@pytest.mark.bronze`
After ingestion steps have run, the expected bronze files should exist.
No network required; just checks `Path.exists()`.

```python
@pytest.mark.bronze
@pytest.mark.parametrize("path", [
    BRONZE_PDF_DIR / "attendance",
    MEMBERS_DIR,
    LOBBYING_RAW_DIR,
])
def test_bronze_directories_exist(path):
    assert path.exists(), f"Bronze directory missing: {path}"
```

For PDFs: check that at least N files exist in each subdirectory (don't enumerate every filename — they change as new PDFs are added).

### 3. File format smoke test — `@pytest.mark.bronze`
Checks that files are minimally valid:
- PDFs: file opens without error, has at least 1 page (`pypdf` or `pdfplumber`)
- JSON: `json.load()` succeeds, top-level key exists
- CSV: `pl.read_csv()` succeeds, row count > 0

Do NOT assert specific column names or values here — that is silver's job.

```python
@pytest.mark.bronze
def test_members_json_parseable():
    path = MEMBERS_DIR / "members.json"
    if not path.exists():
        pytest.skip("members.json not found — run API fetch first")
    with open(path) as f:
        data = json.load(f)
    assert isinstance(data, list) and len(data) > 0
```

### 4. File freshness — `@pytest.mark.bronze`
Bronze files should not be stale beyond a reasonable window. A missing or very old file
signals the ingestion step hasn't run, which will silently produce outdated silver outputs.

```python
import os, time

@pytest.mark.bronze
def test_members_json_not_stale():
    path = MEMBERS_DIR / "members.json"
    if not path.exists():
        pytest.skip()
    age_days = (time.time() - os.path.getmtime(path)) / 86400
    assert age_days < 90, f"members.json is {age_days:.0f} days old — re-fetch recommended"
```

Threshold guidance: PDFs don't change once published (immutable); JSON API data should be
re-fetched periodically; lobbying CSVs are manual so freshness window is looser.

---

## What bronze layer tests do NOT check

| Concern | Why it belongs at silver, not bronze |
|---|---|
| Column names | Bronze is raw; column contract is established by the extractor (silver) |
| Value ranges | Bronze data is unvalidated by definition |
| Join key integrity | Join keys are computed at silver |
| Null rates | Raw PDFs have missing values; silver extractors handle them |
| Data completeness | How many TDs appear is a silver/gold concern |

---

## Test markers — recommended registration in conftest.py

```python
def pytest_configure(config):
    config.addinivalue_line("markers", "integration: requires silver/gold pipeline output files")
    config.addinivalue_line("markers", "bronze: requires bronze ingestion to have run (no network)")
    config.addinivalue_line("markers", "sources: requires network — checks external PDF/API endpoints")
```

Running strategy:
```bash
pytest test/ -m "not integration and not bronze and not sources"   # unit tests only, instant
pytest test/ -m bronze                                             # file existence + format, no network
pytest test/ -m sources                                            # endpoint reachability, network required
pytest test/ -m integration                                        # silver/gold schema validation
```

---

## PDF testing — known challenges

PDFs are the hardest bronze source to test because:
1. They change URL format when new years are published (see `pdf_endpoint_check.py`)
2. Their internal structure varies across years — tables shift, headers change
3. Embedding a real PDF as a test fixture is expensive to maintain

**Recommended approach for `test_pdf_extraction.py`:**

- Do not embed full PDFs in the test suite
- Test the **extractor output** (silver CSV shape) not the PDF itself — that is already in silver tests
- At bronze, only check: PDF file opens, has ≥ 1 page, file size > 10KB (rules out empty/corrupt downloads)
- The extractor unit tests (`attendance.py`, `payments.py`) should use a minimal synthetic PDF fixture (1 row) rather than real government PDFs

Reference: TEST_SUITE.md § "PDF testing" already documents the reasoning for deferring full PDF fixture tests.

---

## API JSON testing

The oireachtas.ie API returns paginated JSON. Bronze tests should check:
1. The JSON file exists and is parseable (`json.load()`)
2. The top-level structure is a list/dict as expected (one-time assertion; update if API changes)
3. At least 127 member records are present (Dáil minimum seat count)

Do NOT test field names inside the JSON records — those map to bronze→silver transformation
and belong in silver tests.

---

## Lobbying CSV testing

Lobbying CSVs are manually exported from lobbying.ie and are not version-controlled.
Bronze tests should check:
1. At least one CSV file exists in `LOBBYING_RAW_DIR`
2. Each CSV opens without parse error
3. Row count > 0
4. File is not older than 180 days (lobbying returns are filed quarterly)

The `lobby_processing.py` pipeline has a quality threshold at year ≥ 2020 (see memory note
`project_lobbying_automation.md`). Bronze tests should not enforce this — that threshold
belongs in silver/lobbying pipeline tests.

---

## Planned test files

| File | Marker | Status |
|---|---|---|
| `test_bronze.py` | `@pytest.mark.bronze` | Not yet written — this document is the foundation |
| `test_pdf_extraction.py` | `@pytest.mark.bronze` | Referenced in TEST_SUITE.md, not yet written |
| (endpoint checks wrapped) | `@pytest.mark.sources` | Logic exists in `pdf_endpoint_check.py`, needs pytest wrapper |

---

## Relationship to upstream failure detection at silver

Silver schema tests indirectly signal bronze failures through output validation:
- Row count floors (`len(df) >= 127`) catch a pipeline that ran but extracted nothing
- Year coverage checks (`df["year"].max() >= 2023`) catch stale data

These are appropriate at silver because they test the output contract, not the source.
Bronze tests answer a separate question: is the raw input present and readable?
Mixing the two conflates infrastructure failures with data quality failures — keep them in separate files and separate markers.
