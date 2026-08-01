# Capital pipeline extraction — quality report

**Source:** MyProjectIreland All Projects Feature Service (Project Ireland 2040 / NDP).
**Extracted:** 2026-08-01 · **Output:** `projects_clean.jsonl` (1,936 rows, 16 columns).
**Status:** SANDBOX. Nothing promoted, no parquet written, no gold touched.

```
https://services1.arcgis.com/eNO7HHeQ3rUcBllm/arcgis/rest/services/myProjectIreland_All_Projects/FeatureServer/0
```

## Recall

Measured against the service's own count, not assumed.

| Check | Result |
|---|---|
| `where=1=1&returnCountOnly=true` | `{"count":1936}` |
| Rows written | 1,936 |
| Recall vs source | **100.0% — COMPLETE** |

Fetched by paginating `resultOffset` in pages of 1,000 and continuing while
`exceededTransferLimit` is true, so the result does not depend on `maxRecordCount` (2000)
happening to exceed the row count. Geometry requested in EPSG:4326 and flattened to `lon`/`lat`.

**What this recall figure does not cover.** It measures recall against *the service*, which is
an end-2024 snapshot published May 2025. Recall against the real national capital programme as
of August 2026 is **unmeasured and unmeasurable from this source alone** — any project
initiated after end-2024 is absent and no count here would reveal it. Treat 100% as "we took
everything the publisher offers", never as "we have every capital project".

## Completeness

Per-field non-empty rate across all 1,936 rows.

| Field | Non-empty | Note |
|---|---|---|
| Name, Body, Description, Fund, Location, Region, Investment, Completion, Status, Year | 100.0% | |
| lon, lat | 100.0% | geometry present on every row |
| Link | 99.0% | |
| Eircode | **32.4%** | join key largely absent — use coordinates, not Eircode |
| Cities | 31.7% | |

**A non-empty field is not a usable field, and this dataset proves it.** `Year` is 100%
non-empty but only **76.0%** parses as an integer. The other 464 rows (24.0%) hold a *status
where a date belongs*:

| Value in `Year` | Rows |
|---|---|
| `Subject to Appraisal` | 376 |
| `Completed` | 81 |
| `Subject to appraisal` (lowercase variant) | 5 |
| `2030+` | 1 |
| `Q4 2027` | 1 |

This is the completeness-vs-correctness gap in one field. A schema or null check passes at
100%; a usability check fails at 76%. `Subject to Appraisal` is not missing data — it is
meaningful, marking the earliest-stage projects — but it must be lifted into a status column
rather than coerced to a null date. The lowercase variant means any parser must fold case.

**Structural gaps, not fixable downstream:**

- **No euro field exists.** `Investment` is a sector category (11 distinct values), `Fund` is a
  free-text funding-programme sentence with only 4 distinct values, a large share `N/A`.
  Anything needing project cost must join elsewhere.
- `Completion` and `Year` disagree on the `Subject to Appraisal` count (366 vs 376), so the two
  date fields are not derived from one another.

## What the data shows

Status distribution across all 1,936 rows:

| Stage | Rows |
|---|---|
| 5. Post Completion Review and Benefits Realisation | 833 |
| 2. Pre-Tender — Project Design, Planning and Procurement Strategy | 426 |
| 4. Implementation | 360 |
| 1. Strategic Assessment and Preliminary Business Case | 254 |
| 3. Post-Tender — Final Business Case | 63 |

**43% of the dataset is already complete** and carries no forward signal. The pre-tender set
(stages 1 and 2) is 680 rows, but that headline overstates what is actionable: only **271
(39.9%)** state a completion year of 2026 or later, 78 (11.5%) state a year that has already
elapsed, and 331 (48.7%) carry no parseable year — mostly `Subject to Appraisal`.

Pre-tender work concentrates hard: Housing and Sustainable Urban Development accounts for 346
of 680, and the top three bodies (Health 100, Housing 88, Transport 71) hold 38% of it. 80
distinct bodies appear in total.

## Text classifier — and the leakage found in it

TF-IDF (1–2 gram, `min_df=2`, sublinear) + LinearSVC, 5-fold stratified CV, predicting the
publisher-assigned `Investment` category from `Name` + `Description`. 1,907 docs across 9
classes (29 rows dropped from classes with under 25 examples).

| Configuration | 5-fold accuracy |
|---|---|
| Majority-class baseline | 0.429 |
| Raw text | 0.980 ± 0.004 |
| **Fund boilerplate stripped** | **0.937 ± 0.006** |

The 0.980 was not skill. Inspecting per-class coefficients showed top features like
`disruptive technologies`, `innovation fund`, `rural regeneration` and `urban regeneration` —
fund names, not project content. 43.9% of descriptions contain a "funded by…" clause, and
`Fund` maps near-deterministically onto `Investment` (Urban Regeneration fund → Housing 479
rows; Rural Regeneration fund → Rural Development 267). Removing that boilerplate costs **4.2
points**, and 0.937 is the honest content-only figure.

**Label caveat, stated because the last classifier exercise got this wrong.** These labels are
the *publisher's own* categorisation, not the output of one of our regexes — so unlike
`project_council_minutes_overnight_expansion_2026_08_01`, this measures agreement with an
external assignment rather than self-consistency. It is still not a golden set: the publisher's
taxonomy may itself be inconsistent, and the two weakest classes (Culture/Heritage/Sport recall
0.72, Other Sectors 0.73) suggest exactly that.

**What it is good for.** At 0.937 on content alone, project text carries enough signal to map
*other* sources — HSE capital plan lines, social housing report rows — into this same 11-category
taxonomy, which is the prerequisite for treating them as one pipeline. That is the intended use,
not re-predicting a field already present.

## Open

- No euro values anywhere in this source; cost must come from a joined source.
- `Body` has not been matched to the procurement buyer spine. Buyer-name keys differ across
  registers, so this needs the existing normaliser, not new matching.
- The backtest — do stage-1/2 projects subsequently appear in awards or eTenders, and after how
  long — is not run. That is the only test that would validate the pre-tender claim.
- Licence unresolved: the item asserts Government copyright over Tailte basemap content and
  warns further data may need permission before reuse. Not cleared for commercial use.
