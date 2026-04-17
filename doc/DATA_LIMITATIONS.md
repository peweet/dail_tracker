# DATA_LIMITATIONS.md

Known data quality issues, gaps, and silent-failure risks in the dail_extractor pipeline.
Inspired by theyworkforyou's DATA_LIMITATIONS.md pattern (§17.3b) and lobbyieng's data-limitations page (§17.5c).

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

## 6. Name-Based Join Risks (normalise_join_key.py → enrich.py)

The PDF sources (attendance, payments) provide **no primary key or unique identifier** for individual TDs — only human-readable names in varying formats (e.g., "Surname, Firstname", "Firstname Surname", with or without fadas). The Oireachtas API similarly uses name-based URIs rather than stable numeric IDs. This makes deterministic joining across datasets impossible at the early pipeline stage.

The normalised join key (strip accents → remove non-alpha → sort characters alphabetically) is a pragmatic workaround, but it introduces several known risks:

- **Minor spelling variations break joins.** A name rendered as "Ahern" in one PDF and "Aherne" in another produces different sorted keys (`aaacehinnrr` vs `aaaceehinnrr`). These mismatches cause silent NULLs in the enriched output. There is no fallback — the join is exact on the sorted key.
- **Collision risk:** two different names that are anagrams of each other produce the same key, causing one TD to overwrite the other in the join.
- **Unmatched rows are silently lost** — left join is driven from the API side, so any PDF-only TD that fails key matching disappears without warning.
- `enrich.py` deduplicates on `join_key` with `keep='first'` — if the API returns two members with the same sorted key, one is silently dropped.
- **Normalisation logic is currently duplicated** across `normalise_join_key.py` and `payments.py`. Any divergence between the two (e.g., one lowercases before stripping non-alpha and the other does not) will produce different keys for the same TD, guaranteeing a join miss. This should be consolidated into a single shared service.
- The `join_key` column is left in the final CSV (commented-out `.drop('join_key')` on line 23).

