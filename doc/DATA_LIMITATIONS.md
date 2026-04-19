# DATA_LIMITATIONS.md

Known data quality issues, gaps, and silent-failure risks in the dail_extractor pipeline.
Inspired by theyworkforyou's DATA_LIMITATIONS.md pattern (§17.3b) and lobbyieng's data-limitations page (§17.5c).
---


# Member Interests: Office Holders and Senators

**Office holders** (e.g. Taoiseach, Tánaiste, Ministers, Attorney General, Ceann Comhairle, Leas-Cheann Comhairle, and certain committee chairs) are *not* required to publish the same Register of Interests as regular TDs. This is a legal and transparency gap, not a technical flaw. SIPO collects these declarations, but does not publish them for office holders. As a result, the dataset will always have missing or blank interests for these ~30 TDs, even though they are among the most powerful members.


**Family holdings**: There is no total obligation for politicians to declare property or interests held in a spouse's or child's name. SIPO does not publish this information, and the dataset cannot surface it. It is required in some ethics rules in practice many do not declare their full interests. Office holders are exempt in reporting.

**Sources:**
- [Irish Examiner: Gaps in property declarations](https://www.irishexaminer.com/news/politics/arid-41065895.html)
- [Irish Times: Ministerial property declarations](https://www.irishtimes.com/politics/2023/02/22/ex-minister-robert-troy-declares-more-properties-owned-in-latest-dail-register-of-interests/)
- [SIPO Guidelines (PDF)](https://data.oireachtas.ie/ie/oireachtas/committee/dail/34/committee_on_members_interests_of_dail_eireann/termsOfReference/2025/2025-12-18_guidelines-for-members-of-dail-eireann-who-are-not-office-holders-concerning-the-steps-to-be-taken-by-them-to-ensure-compliance-with-the-provisions-of-the-ethics-in-public_office-acts-1995-and-2001_en.pdf)

**Summary:**
- Office holders' interests are always missing by law, not by extraction failure.
- Senator interests are not yet included.
- Family holdings are not visible in any public dataset.


# DATA_LIMITATIONS.md

Known data-quality issues, silent-failure risks, and deliberate scope decisions in the dail_tracker pipeline.

Inspired by theyworkforyou's DATA_LIMITATIONS pattern and lobbyieng's data-limitations page. The goal is to be honest about what the dataset can and cannot tell you — both the gaps that should be fixed and the gaps that are intentional.

---

## 0. Deliberate Scope Decisions (Current)

These are **not bugs** — they are choices about what the dataset covers.

### 0.1 Sitting politicians only

The dataset tracks **currently sitting TDs of the 34th Dáil only**. Former TDs who have retired, lost their seat, or otherwise left the chamber are excluded.

**Why:**

- Former TDs are private citizens. Their declared interests, payments, and lobbying-contact records are already part of the historical public record but they are no longer exercising public authority. Continuing to surface their data in a live tracker adds limited civic value against a non-trivial privacy weight.
- The primary purpose of the project is accountability for people currently making decisions. A sitting TD's attendance pattern *this year* matters to a constituent. A former TD's attendance in 2019 does not — it's history, and it's already in the historical record at Oireachtas.ie.
- Scoping to sitting TDs also makes the fuzzy join key more reliable. Every additional historical TD is another node in the name-resolution graph and another opportunity for an anagram collision or near-spelling collision. Current membership is a bounded, well-defined list (~174 TDs).
- Pipeline reproducibility is easier: the 34th Dáil's member list is stable until the next general election. Historical data would require tracking Dáil numbers, term boundaries, and mid-term replacements (by-elections, resignations), which is a separate engineering problem.

**What this means in practice:**

- The member API call is filtered to `chamber_id=dail/34`. Prior Dáil terms are not fetched.
- Historical attendance, payments, interests, and lobbying records for former TDs are not deleted from upstream sources — they remain on Oireachtas.ie and lobbying.ie for anyone who wants them. They are simply not joined into this project's enriched output.
- If a TD leaves the Dáil mid-term (resignation, death, by-election loss), they will stop appearing in new pipeline runs once the upstream `/v1/members` response no longer lists them.
- If a future version of this project wants to cover the 33rd Dáil or earlier, the `chamber_id` parameter supports it — but the scope of this version is intentionally narrower.

### 0.3 Attendance = plenary sitting days only

Attendance numbers reflect attendance at plenary Dáil sittings as recorded in the official attendance PDFs. They do **not** include:

- Committee attendance (tracked separately upstream, not yet ingested)
- Delegation travel on official business
- Pairing arrangements (where absent members pair with the opposite side by agreement)
- Ministerial duties outside the chamber

A TD with low plenary attendance but heavy committee workload is not necessarily disengaged — they may be doing committee work the plenary PDF cannot see. This is the single biggest caveat on the attendance numbers, and why `REQUIREMENTS.md` treats committee-membership count as an independent "work surface" metric alongside attendance.

--

## 2. PDF Attendance Extraction Gaps

### 2a. Missing TD (127 of 128 detected)

One TD is not captured from any attendance PDF. The join direction fix in `enrich.py` means this TD still appears in the final enriched output (from the API side of the join), but with null attendance. The root cause in `attendance.py` PDF parsing remains unresolved.

Likely cause: the structural marker detection (`DESC_RE = re.compile(r"Deputy.*Limit:\s*\d+")`) fails on that TD's line format on a specific page.

### 2b. TDs with 0 attendance dropped

TDs with `0` in attendance fields are dropped by `dropna()` logic because `0` is being treated as empty. The join direction fix papers over this for the enriched output but the PDF parser should preserve legitimate `0` values.


---


## 3. API Pagination & Truncation

| Endpoint | `limit` | Risk |
|---|---|---|
| Members | 200 | Safe (~174 current members) |
| Legislation (per TD) | 1000 | **Truncates** if a TD has sponsored >1000 bills — no pagination implemented |
| Questions (per TD) | 1000 | **Truncates** if a TD has asked >1000 questions — no pagination implemented |

**Limitation:**
The pipeline does **not** implement true API pagination. It fetches only the first page of results for each TD per endpoint, using `limit` and `skip=0`. There is no loop to increment `skip` or fetch additional pages. If a TD has more than 1000 records, results will be silently truncated. This affects both legislation and questions endpoints, and is a known limitation.

**Status:**
The claim that "the pipeline now paginates all endpoints (members, legislation, questions) using skip/limit, so there is no silent truncation" is **incorrect**. Pagination is not implemented; silent truncation is still possible.

---

## 4. Date Range Gaps

| Endpoint | `date_start` | `date_end` | Issue |
|---|---|---|---|
| Members | 2024-01-01 | 2099-01-01 | Excludes historical TDs (pre-2024) — intentional per §0.1 |
| Legislation | 1900-01-01 | 2099-01-01 | Fetches full career — intentional but unfiltered |
| Questions | *(none)* | *(none)* | No date bounds at all; fetches every question ever asked |

Adding `date_start=2024-03-22` (34th Dáil start) to the questions URL would bring the questions payload in line with the project's stated scope and reduce data volume substantially for long-serving members.

---

## 5. Bill Sponsorship Ambiguity

- Only `bill_sponsors_0_sponsor_isPrimary` is captured (index 0). Multi-sponsor bills lose all co-sponsors at index 1+.
- Same limitation for related documents: only index-0 related doc is kept.
- The API response does distinguish `isPrimary`, but the flattening only keeps the first sponsor element.

The practical effect: bills sponsored by multiple TDs attribute only to the primary sponsor in this dataset. A co-sponsored bill appears in one TD's legislative record, not the others'.

---

## 6. Name-Based Join Risks

The PDF sources (attendance, payments, member interests) provide **no primary key** — only human-readable names in varying formats. The Oireachtas API similarly uses name-based URIs rather than stable numeric IDs. Deterministic joining across datasets is therefore impossible at the ingestion layer, and the pipeline relies on a sorted-character normalised key as a pragmatic workaround.

Known risks with this approach:

- **Spelling variations break joins.** "Ahern" vs "Aherne" produce different sorted keys. Silent NULLs in the enriched output.
- **Anagram collisions.** Two different names that happen to be character-level anagrams produce the same key. One record overwrites the other.
- **Unmatched rows are silently lost.** Left join is driven from the API side. Any PDF-only TD that fails key matching disappears without warning.
- **Deduplication is `keep='first'`.** If the API returns two members with the same sorted key, one is silently dropped.
- **Normalisation logic is duplicated** across `normalise_join_key.py` and `payments.py`. Any divergence guarantees join misses. Should be consolidated into a single shared service.

The `join_key` column is currently left in the final enriched CSV (commented-out `.drop('join_key')` in `enrich.py`).

---

## 7. Lobbying Pipeline Limitations


### 7e. Manual CSV download

The lobbying.ie export is manual — there is no API. Users must download CSV batches of 1000 records each from the lobbying.ie search UI and drop them into `lobbyist/raw/`. This means the dataset is a snapshot, not live. The date of snapshot should be recorded in pipeline metadata.

### 7f. Lobbying period coverage is caller-defined

The pipeline does not enforce contiguous or complete period coverage. If a user downloads batches covering 2024 Q1 and 2024 Q3 but not Q2, the "biggest movers" analysis will treat Q1→Q3 as consecutive and produce misleading deltas.

---

## 8. Column Dropping

- **~50 member columns** dropped (all URIs, gender, wiki metadata, committee date ranges, internal IDs).
- **~150 bill columns** dropped (amendment lists, debate records, stage completion, XML formats).
- All drops use `errors='ignore'` — if the API schema changes and a column disappears, no warning is raised.
- Dropped data includes debate records, amendment history, and committee date ranges that would be valuable for legislative analysis. They are not permanently lost — they remain in the raw JSON under `bronze/` equivalents — but they are not in the flattened CSV.

---

## 9. File I/O & Portability

- Absolute Windows paths (`C:\\Users\\pglyn\\...`) appear in multiple scripts. Break on any non-Windows machine and on any Windows machine that isn't the original developer's.
- `doc/config.py` defines relative paths via `pathlib.Path` but is not yet imported by consuming scripts.
- Mixed relative/absolute paths across scripts; execution order and working directory matter.
- No pipeline orchestrator — scripts must be run manually in the correct sequence.

---

## 10. Member Interests Extraction

- Nine declaration categories (Occupations, Shares, Land, Directorships, Gifts, Travel Facilities, Remunerated Positions, Contracts, Other) are extracted by section-header regex. If the PDF formatter changes a header, that category's data is silently dropped.
- TDs who filed a "nil" return are represented as genuinely empty rows. These are indistinguishable from TDs whose section was missed by the regex. No sentinel value flags "nil return vs extraction failure".
- Free-text interest descriptions are not categorised — joining them against lobbying activities (for conflict-of-interest detection) requires downstream NLP or keyword matching.

## 11. Questions Not Yet Analysed

Questions are fetched to `bills/questions_results.json` but no `flatten_questions.py` exists. Questions are not joined to the enriched TD dataset. The flattening is a known gap, not a rejection — tracked in `REQUIREMENTS.md`.

---

## 12. Committee Normalisation

Member flattening creates 5 committee slots per TD (wide format). Committees are not normalised to a separate dimension table. This makes cross-TD committee analysis (who sits with whom, which committees are Government-dominated, etc.) awkward without an UNPIVOT step.

The 5-slot limit is not enforced by the upstream API — a TD sitting on 6+ committees would silently lose their 6th entry. Current data doesn't appear to hit this limit but it's worth being explicit about.


## 13. Data Validation Gaps

- `flatten_members_json_to_csv.py` prints "should be 175" but does **not** raise an error if the count differs.
- `fillna('Null')` converts NaN to the string `"Null"` — indistinguishable from real null when querying.
- No row-count assertions before/after joins or concatenations.
- No duplicate detection on the final enriched output.
- No schema validation on CSV outputs.
- `pipeline.log` is not committed to the repo — error history is lost between machines.

---

## 14. Timeliness

- The member API is called on-demand — if a TD resigns, is replaced, or changes party between pipeline runs, that change is not reflected until the next run.
- PDFs are published on a lagged schedule (attendance PDFs appear weeks after the period they cover). Expect a 2–4 week lag between real-world events and their appearance in the dataset.
- The lobbying register updates quarterly with a legally mandated reporting delay. "Current" lobbying data is always 4–6 months behind real activity.

---

## 15. Hardcoded Values Summary

| Value | Location | Risk |
|---|---|---|
| `limit=200` (members) | oireachtas_api_service.py | OK for now (~174 members) |
| `limit=1000` (legislation) | oireachtas_api_service.py | Truncation risk |
| `limit=1000` (questions) | oireachtas_api_service.py | Truncation risk |
| `chamber_id=dail/34` | oireachtas_api_service.py | Locks to 34th Dáil — intentional per §0.1/0.2 |
| `max_workers=5` | oireachtas_api_service.py | Throttles API throughput |
| `.iloc[:, :5]` | attendance.py | Brittle if PDF format varies |
| Target TD count `175` | flatten_members_json_to_csv.py | Not enforced |
| Target TD extraction `128` | attendance.py | Only 127 achieved |
| Absolute Windows paths | multiple files | Portability blocker |


