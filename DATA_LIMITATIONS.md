# DATA_LIMITATIONS.md

Known data quality issues, gaps, and silent-failure risks in the dail_extractor pipeline.
Inspired by theyworkforyou's DATA_LIMITATIONS.md pattern (§17.3b) and lobbyieng's data-limitations page (§17.5c).

---

## 1. API Encoding Failures (Fada Characters)

- **4 TDs** whose `member_id` URIs contain fada characters (Ó, é, á, etc.) fail when interpolated into `urllib.request` URLs in `oireachtas_api_service.py`.
- Failures are logged to `pipeline.log` but silently dropped from the `results` list — no legislation or questions data collected for these TDs.
- **Root cause:** URL is constructed via f-string (line ~63) without `urllib.parse.quote()`; `urllib.request.urlopen()` rejects the unencoded characters.
- **TD names not yet confirmed** — run the pipeline and check `pipeline.log` for `API call failed` entries to identify the 4 names.
- **Impact:** These TDs appear in the enriched CSV with NULL bill/question columns.

## 2. PDF Attendance Extraction Gaps

### 2a. Missing TD (127 of 128)
- One TD is not captured from any PDF. Identity unknown — compare `aggregated_td_tables.csv` against the API member list to find who.
- Likely cause: the name-detection regex (`^[A-ZÁÉÍÓÚ][a-zA-ZáéíóúÁÉÍÓÚ'\s\-]+$`) fails on that TD's line format.

### 2b. Swapped First / Last Name
- `attendance_2024.py` lines 31–32 assign `first_name = names[-1]` and `last_name = " ".join(names[:-1])`.
- For "John Smith" this produces `first_name = "Smith"`, `last_name = "John"`.
- Every attendance record in `aggregated_td_tables.csv` has names reversed.

### 2c. DataFrame Accumulation Bug
- The `dataframes.append(df)` / `pd.concat(dataframes)` pattern runs inside the PDF loop but the final write happens after the loop — earlier PDFs' data may overwrite or accumulate incorrectly depending on execution path.

### 2d. Hard-Coded Column Slice
- `df.iloc[:, :5]` assumes the first 5 columns are always date + 4 attendance types. If a PDF's table layout varies, real data is silently dropped or formatting artefacts kept.

### 2e. Attendance Scope
- PDFs only record **plenary sitting-day attendance**. Committee attendance, delegation travel, and pairing arrangements are not captured.
- Coverage: 7 PDFs spanning 1 Jan 2024 → 28 Feb 2026 (with a gap 9 Nov – 28 Nov 2024 between PDFs).

## 3. API Pagination & Truncation

| Endpoint | `limit` | Risk |
|----------|---------|------|
| Members | 200 | Safe (174 current members) |
| Legislation (per TD) | 1000 | **Truncates** if a TD has sponsored >1000 bills — no skip/offset loop |
| Questions (per TD) | 1000 | **Truncates** if a TD has asked >1000 questions — no skip/offset loop |

- No pagination loop exists. If the API returns exactly 1000 records, there is no check for whether more exist.

## 4. Date Range Gaps

| Endpoint | `date_start` | `date_end` | Issue |
|----------|-------------|-----------|-------|
| Members | 2024-01-01 | 2099-01-01 | Excludes historical TDs (pre-2024) |
| Legislation | 1900-01-01 | 2099-01-01 | Fetches full career — intentional but unfiltered |
| Questions | *(none)* | *(none)* | No date bounds at all (§16.2); fetches every question ever asked |

## 5. Bill Sponsorship Ambiguity

- Only `bill_sponsors_0_sponsor_isPrimary` is captured (index 0). Multi-sponsor bills lose all co-sponsors at index 1+.
- Same limitation for related documents: only index-0 related doc is kept.
- The API response **does** distinguish `isPrimary`, but the flattening only keeps the first sponsor element.

## 6. Fuzzy Join Risks (normalise_join_key.py → enrich.py)

- Join key is built by: strip accents → remove non-alpha → sort characters alphabetically.
- **Collision risk:** two different names that contain the same letters produce the same key (e.g., anagram names).
- **Unmatched attendance rows** are silently lost — left join is driven from the API side, so any PDF-only TD that fails key matching disappears.
- `enrich.py` deduplicates on `join_key` with `keep='first'` — if the API returns two members with the same sorted key, one is silently dropped.
- The `join_key` column is left in the final CSV (commented-out `.drop('join_key')` on line 23).

## 7. Column Dropping

- **~50 member columns** dropped (all URIs, gender, wiki metadata, committee date ranges, internal IDs).
- **~150 bill columns** dropped (amendment lists, debate records, stage completion, XML formats).
- All drops use `errors='ignore'` — if the API schema changes and a column disappears, no warning is raised.
- Dropped data includes debate records, amendment history, and committee date ranges that could be valuable for legislative analysis.

## 8. File I/O & Portability

- `attendance_2024.py` line 19: `os.chdir('C:\\Users\\pglyn\\...')` — absolute Windows path, breaks on other machines.
- `oireachtas_api_service.py` line 52: absolute path to CSV — same issue.
- Mixed relative/absolute paths across scripts; execution order and working directory matter.
- No pipeline orchestrator (main.py / Makefile) — scripts must be run manually in the correct sequence.

## 9. Incomplete Features

### 9a. payments.py
- 12 months of payment PDF URLs (Jan 2025 – Feb 2026) defined but **no extraction logic written**. Downloads are possible but parsing is not implemented.

### 9b. Questions Flattening
- Questions are fetched to `bills/questions_results.json` but no `flatten_questions.py` exists. Questions are not joined to the enriched TD dataset.

### 9c. Committee Normalisation
- Member flattening creates 5 committee slots per TD (wide format). Committees are not normalised to a separate table — analysis of committee membership across all TDs is difficult.

## 10. Data Validation Gaps

- `flatten_members_json_to_csv.py` prints "should be 175" but does **not** raise an error if the count differs.
- `fillna('Null')` converts NaN to the string `"Null"` — indistinguishable from real null when querying.
- No row-count assertions before/after joins or concatenations.
- No duplicate detection on the final enriched output.
- No schema validation on CSV outputs.
- `pipeline.log` is not committed to the repo — error history is lost between machines.

## 11. Hardcoded Values Summary

| Value | Location | Risk |
|-------|----------|------|
| `limit=200` (members) | oireachtas_api_service.py:36 | OK for now (174 members) |
| `limit=1000` (legislation) | oireachtas_api_service.py:63 | Truncation risk |
| `limit=1000` (questions) | oireachtas_api_service.py:65 | Truncation risk |
| `chamber_id=dail/34` | oireachtas_api_service.py | Locks to 34th Dáil only |
| `max_workers=5` | oireachtas_api_service.py:80 | Throttles API throughput |
| `.iloc[:, :5]` | attendance_2024.py:45 | Brittle if PDF format varies |
| Target TD count `175` | flatten_members_json_to_csv.py:15 | Not enforced |
| Target TD extraction `128` | attendance_2024.py | Only 127 achieved |