# dbsect_harvest — graduation plan

Companion doc for [dbsect_harvest.py](dbsect_harvest.py). Describes what
the sandbox script does today, what counts it produces against real
bronze, the validation gates that must pass before this work is moved
into the main pipeline, and the exact set of edits needed to graduate
it.

This doc is the artefact the sandbox script is "transferred over" by.
The script itself stays untouched once it's been mirrored into the
main pipeline — the sandbox file becomes a working historical
reference, not a live dependency.

A companion sandbox script,
[dbsect_probe.py](dbsect_probe.py), takes a small stratified
cross-section of dbsects from the harvester output and calls the
Oireachtas API to confirm the response shape. Its findings are written
to [dbsect_probe_findings.md](dbsect_probe_findings.md) and are
summarised in §5 below.

---

## 1. What the sandbox produces today

Run from the repo root:

```bash
python pipeline_sandbox/dbsect_harvest.py
```

Reads three bronze JSONs and writes one silver parquet:

- inputs:
  - `data/bronze/legislation/legislation_results.json`
  - `data/bronze/questions/questions_results.json`
  - `data/bronze/votes/votes_results.json`
- output:
  - `data/silver/parquet/dbsect_index.parquet`

Schema (one row per `(debate_section_id, source, source_key)`):

| column              | type | example                |
|---------------------|------|------------------------|
| `debate_section_id` | str  | `dbsect_12`            |
| `source`            | str  | `bill` / `question` / `vote` |
| `source_key`        | str  | `2021_115`, question uri, voteId |
| `date`              | str  | `2021-11-17`           |
| `chamber`           | str  | `dail` / `seanad`      |
| `debate_uri`        | str  | full debateRecord uri  |
| `debate_title`      | str  | `Local Government ... First Stage` |

## 2. Counts against current bronze

Most recent run (record these in the migration PR description so we can
detect drift after a refresh):

| source   | edges   | distinct dbsect |
|----------|---------|-----------------|
| bill     | 1,290   | 69              |
| question | 120,139 | 3,007           |
| vote     | 1,667   | 76              |
| **all**  | 123,096 | **3,008**       |

Source overlap (how many sources cite the same dbsect):

| cited_by N sources | dbsect count |
|--------------------|--------------|
| 1                  | 2,930        |
| 2                  | 12           |
| 3                  | 66           |

The 66 dbsects cited by all three sources are the strongest "this
debate is real, structured, and politically substantive" signal in the
dataset. They're the right pilot population for any downstream work.

## 3. Validation gates before migrating

All four must pass on the latest bronze before we touch `services/`:

1. **Re-run is deterministic.** Two consecutive runs of
   `dbsect_harvest.py` produce a parquet with identical row count and
   identical distinct-dbsect count.
2. **Spot-check.** Pick five `dbsect_*` ids — one per cited-by-3
   bucket, two per cited-by-1 bucket — and manually open the
   corresponding `https://www.oireachtas.ie/en/debates/debate/...` URL
   built from `chamber + date + dbsect_id` (the URL pattern is already
   in [legislation_enrichment.py](legislation_enrichment.py)). Each
   page must load and match the `debate_title` we stored.
3. **No null dbsect.** `dbsect_index.parquet` has zero rows where
   `debate_section_id` is null or blank.
4. **No regressions in [debates.parquet](../data/silver/parquet/debates.parquet).**
   Distinct dbsects in the harvest's `bill` slice equal distinct
   `debateSectionId` in the existing debates parquet. If they don't,
   one side has a bug — fix before migration.

If any gate fails, do not migrate. The harvester is the cheap end of
this work; everything downstream assumes its output is trustworthy.

## 4. Migration — exact edits

The harvester does not call the API. It is a flattening step. So it
graduates into `pipeline.py`'s flatten/enrich layer, not into
`services/`. The new `services/` work is for the *next* lane (debates
endpoint), not for the harvester.

### 4.1 Move the harvester into the main pipeline

- New file `pipeline/flatten/dbsect_index.py` (or whatever the current
  flatten subpackage is named — see `legislation.py` for the existing
  convention). Copy `harvest_bills`, `harvest_questions`,
  `harvest_votes`, `_chamber_short`, `_norm_dbsect`, and `run()`
  verbatim. The only change: imports come from `config`, no `sys.path`
  hack.
- Wire into `pipeline.py` *after* the existing legislation /
  questions / votes flatten steps. It depends on all three bronzes
  existing.
- Drop `dbsect_harvest.py` from sandbox after the migration PR merges.
  Keep the `.md` (this file) for history.

### 4.2 Extend the existing legislation enrichment

[legislation_enrichment.py](legislation_enrichment.py) already builds
`debate_url_web` on `debates.parquet`. Once the harvester is in the
main pipeline, fold that enrichment into the same step and write
`debate_url_web` on `dbsect_index.parquet` instead — `debates.parquet`
becomes a derived view of the index filtered to `source = 'bill'`.

### 4.3 SQL view layer

- New `sql_views/v_dbsect_index.sql` that simply selects from
  `data/silver/parquet/dbsect_index.parquet`. Use the same
  `read_parquet(...)` style as
  [legislation_debates.sql](../sql_views/legislation_debates.sql).
- Refactor `legislation_debates.sql` to read from `v_dbsect_index`
  filtered to `source = 'bill'`. Verifies the view actually replaces
  the underlying parquet without a UI regression.
- New `sql_views/v_question_debates.sql` joining
  `read_parquet('data/silver/parquet/aggregated_td_tables.parquet')` →
  `dbsect_index` (where `source = 'question'`). This is the first
  user-visible deliverable — answers "what debate session did each of
  this TD's questions sit in" without a single new API call.

## 5. The next lane (out of scope here, but unblocked by this work)

Once `dbsect_index.parquet` is in main, two follow-on tasks become
small:

- **Lane A — structural listing** (`debates_by_day`). Call
  `/v1/debates?date_start=DATE&date_end=DATE&chamber=CH` per distinct
  `(date, chamber)` in the index. One JSON response per sitting day
  contains every dbsect on that day with `counts.speakerCount`,
  `counts.speechCount`, `parentDebateSection`, `bill`, `debateType`,
  and the canonical `formats.xml.uri`. Worklist size is
  ~`distinct (date, chamber) pairs` (≈700 today), which is far smaller
  than the 3,008 dbsect total. Needs a new scenario in
  [services/urls.py](../services/urls.py) /
  [storage.py](../services/storage.py) /
  [oireachtas_api_main.py](../services/oireachtas_api_main.py),
  mirroring the `legislation` scenario.
- **Lane A — content** (`debate_xml_per_section`). Call the AKN URL
  `https://data.oireachtas.ie/akn/ie/debateRecord/<chamber>/<date>/debate/mul@/<dbsect>.xml`
  per dbsect to get the actual speaker contributions. `<speech by="#…">`
  is the contributor attribution and is reliable. This is the only path
  to speaker content; the JSON listing returns `speakers: []` for every
  section. **Two header requirements** found by the probe — they are
  not optional:
  - `User-Agent` must look browser-ish (`Mozilla/5.0 (compatible; …)`)
  - `Referer` must be set to `https://www.oireachtas.ie/`
  Without both headers the AKN host returns 403. Add this to the
  shared session config when the lane is wired into `services/`, and
  document it in the Polite-HTTP-helper work in
  [SHORT_TERM_PLAN.md §6.3](../doc/SHORT_TERM_PLAN.md).
- **Lane B** (`debates_by_member`) — `/v1/debates?member_id=<TD>`.
  Probed in this round and **returned zero results** even on dates the
  TD demonstrably contributed to a debate. Treat `member_id` on
  `/debates` as **not functional** and do not build Lane B against it.
  The contributor join must come from parsing the `<speech by="…">`
  attribution out of the AKN XML in Lane A, not from a member-filtered
  listing.

The probe also surfaced two schema corrections to apply when this
graduates:

- **dbsect ids are not globally unique.** `dbsect_2` recurs every
  Dáil sitting day. The composite identity is
  `(date, chamber, debate_section_id)`. The current
  `dbsect_index.parquet` already carries date and chamber alongside
  each row, so this is just a join-key reminder for downstream views.
- **`/v1/debates` only returns `debateType=debate` records.** The
  `debate_type` query parameter is silently ignored — passing
  `writtens` returns the same response as no filter. Question-reply
  dbsects (the long tail — 2,930 of the 3,008 are cited only by
  questions) therefore cannot be discovered through `/v1/debates`.
  They must be reached via the AKN XML URL pattern, which the
  harvester already builds because the dbsect uri is in bronze.

## 6. Rollback / abort criteria

The harvester writes one parquet and reads from bronze. Rollback is
`rm data/silver/parquet/dbsect_index.parquet` plus reverting the main
pipeline PR. There is no migration of upstream data, no schema change
to existing parquets, and no new API call. If §3 gates fail, abort
without leaving partial state.
