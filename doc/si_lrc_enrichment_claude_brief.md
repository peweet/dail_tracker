# Claude Implementation Brief: Add LRC Enrichment to Statutory Instruments

## Purpose

Add a Law Reform Commission (“LRC”) enrichment layer to the existing Statutory Instruments (“SI”) data in `dail_tracker`.

This should improve SI discovery, classification, source-linking, and legal-context exploration.

This is **not** an authoritative legal-status engine. It is a **source-linked legal research enrichment**.

---

## Source Targets

### 1. LRC Classified List of In-Force Legislation

Source:

- https://revisedacts.lawreform.ie/classlist/intro

Use this as the highest-priority enrichment target.

It can enrich SIs with:

- LRC subject heading;
- LRC subheading path;
- department notation;
- eISB link;
- Revised Act link where available;
- LRC source URL;
- LRC update/retrieval metadata;
- LRC caveat.

Important: preserve LRC warnings about accuracy, completeness, and up-to-date status.

Do not treat a match as definitive proof of legal status.

Use wording such as:

> Listed by the LRC Classified List as in-force / classified under this subject heading, subject to LRC warnings and project matching confidence.

### 2. LRC Revised Acts

Sources:

- https://www.lawreform.ie/revised-acts/
- https://revisedacts.lawreform.ie/revacts

Use as a secondary enrichment source.

This can expose:

- Acts affected by SIs;
- commencement orders;
- textual amendments;
- non-textual legislative effects;
- editorial references;
- links between SIs and parent or affected Acts.

Do not present Revised Acts as replacing official legislation or eISB.

### 3. Revised Acts Annotation Documentation

Source:

- https://revisedacts.lawreform.ie/revacts/annotations

Use this to interpret annotation types.

Preserve these concepts:

- F-notes: textual amendments;
- C-notes: non-textual legislative effects / cross-references;
- E-notes: editorial notes, commencement references, subsidiary legislation references;
- commencement orders are statutory instruments.

---

## Non-Goals

Do not:

- assert definitive legal status;
- claim an SI is “in force” solely because it is matched;
- treat unmatched/null status as “not in force”;
- treat unmatched/null status as “in force”;
- use “superseded” unless explicitly source-backed and legally safe;
- present fuzzy matches as facts;
- silently hide source or match uncertainty;
- wire this into the main pipeline before extraction is reproducible and tested.

---

## Legal-Safety Language

Use:

- `listed_by_lrc_classified_list`
- `lrc_classified_in_force_candidate`
- `source_linked_classification`
- `not_checked`
- `unmatched`
- `match_confidence`
- `requires_manual_verification`
- `commencement_reference`
- `affects_revised_act`
- `annotation_reference`
- `source_caveat`

Avoid:

- `definitively_in_force`
- `valid`
- `invalid`
- `official_status`
- `legally_current`
- `legally_effective`
- `proved_in_force`
- `superseded`, unless specifically supported by source text and reviewed.

Suggested UI copy:

> This enrichment uses Law Reform Commission public resources as a source-linked legal research aid. It is not legal advice. LRC classification and project matches may be incomplete or out of date. “Not matched” or “not checked” does not mean the SI is not in force, and a match should be verified against the source.

---

## Existing Project Files To Inspect First

Inspect the current branch from scratch. Do not rely on old branch assumptions.

Likely relevant files:

```text
utility/pages_code/statutory_instruments.py
utility/pages_code/legislation.py
utility/data_access/legislation_data.py
utility/data_access/member_overview_data.py
utility/data_access/_sql_registry.py

sql_views/legislation_si_current_state.sql
sql_views/legislation_statutory_instruments*.sql
sql_views/legislation_*.sql
sql_views/member_*.sql

data/gold/parquet/statutory_instruments.parquet
data/gold/parquet/si_current_state.parquet
data/_meta/*si*
data/_meta/*coverage*

pipeline_sandbox/si_*
pipeline_sandbox/*legislation*
pipeline_sandbox/*lrc*

test/test_si_legal_state.py
test/test_sql_views.py
```

If actual filenames differ, update this plan to match the branch.

---

## Proposed Outputs

### 1. `data/gold/parquet/si_lrc_classified_list.parquet`

Expected grain:

> one row per matched SI per LRC classification path

Suggested columns:

```text
si_number
si_year
si_number_year
si_title_project
si_title_lrc
si_title_normalized_project
si_title_normalized_lrc

lrc_subject_heading
lrc_subject_heading_number
lrc_subheading_path
lrc_subheading_level_1
lrc_subheading_level_2
lrc_subheading_level_3

lrc_department_notation
lrc_department_name
lrc_department_source_text

lrc_classlist_url
lrc_entry_url
lrc_eisb_url
lrc_revised_act_url

listed_by_lrc_classified_list
lrc_in_force_listed
lrc_list_updated_to
lrc_source_warning

match_method
match_confidence
match_notes
requires_manual_review

source_retrieved_at
source_sha256
```

Notes:

- `lrc_in_force_listed` means “listed in the LRC Classified List of In-Force Legislation.”
- It is not a project assertion of definitive legal status.
- Preserve multiple rows if an SI appears under multiple subject paths.

### 2. `data/gold/parquet/si_lrc_revised_act_refs.parquet`

Expected grain:

> one row per SI reference found in Revised Act pages or annotations

Suggested columns:

```text
si_number
si_year
si_number_year
si_title_project
si_title_referenced_text

affected_act_title
affected_act_number
affected_act_year
affected_act_number_year
revised_act_url
revised_act_version_date
revised_act_updated_to

annotation_type
annotation_number
annotation_text
effect_verb
effect_date
affected_section_or_part
is_commencement_reference
is_textual_amendment_reference
is_non_textual_effect_reference
is_editorial_reference

source_url
source_anchor
source_retrieved_at
source_sha256

match_method
match_confidence
requires_manual_review
parse_notes
```

### 3. `data/gold/parquet/si_lrc_enrichment_summary.parquet`

Expected grain:

> one row per project SI

Suggested columns:

```text
si_number
si_year
si_number_year
si_title

has_lrc_classified_list_match
lrc_primary_subject_heading
lrc_subject_headings
lrc_subheading_paths
lrc_department_notations

has_revised_act_reference
revised_act_reference_count
commencement_reference_count
textual_amendment_reference_count
non_textual_effect_reference_count
editorial_reference_count

best_lrc_source_url
best_eisb_url
best_revised_act_url

lrc_match_confidence
lrc_requires_manual_review
lrc_enrichment_status
lrc_caveat
source_retrieved_at
```

Allowed `lrc_enrichment_status` values:

```text
matched_classified_list
matched_revised_act_reference
matched_both
not_matched
not_checked
parse_error
source_unavailable
manual_review_required
```

Do not use `in_force` as a standalone status.

---

## Matching Strategy

Canonical SI key:

```text
si_number_year = "{number}/{year}"
```

Recognise patterns such as:

```text
S.I. No. 123 of 2020
SI 123/2020
123 of 2020
No. 123/2020
```

Preserve:

```text
si_number
si_year
si_title_original
si_title_normalized
```

Match methods, in preferred order:

1. `exact_eisb_url`
2. `exact_number_year_and_title_normalized`
3. `exact_number_year`
4. `exact_lrc_url`
5. `number_year_title_fuzzy_high`
6. `title_fuzzy_only`
7. `manual`

Suggested confidence scoring:

```text
1.00 = exact eISB URL or exact SI number/year with compatible title
0.95 = exact SI number/year, title missing or weak
0.85 = exact SI number/year with title mismatch requiring review
0.70 = title fuzzy high but no number/year
0.50 = weak candidate only
```

Rules:

- Exact SI number/year is the main key.
- Title-only matches should never be public-facing without manual review.
- If number/year matches but title is materially different, set `requires_manual_review = true`.
- If multiple LRC entries match one SI, preserve all rows and aggregate only in the summary.

---

## Extraction Approach

Start in sandbox.

Create:

```text
pipeline_sandbox/si_lrc_classlist_extract.py
pipeline_sandbox/si_lrc_revisedacts_extract.py
pipeline_sandbox/si_lrc_enrichment_build.py
```

Do not wire into `pipeline.py` until outputs are stable and tests exist.

Create bronze/cache directories:

```text
data/bronze/lrc_classlist/
data/bronze/lrc_revisedacts/
```

For each fetched source, save metadata:

```text
url
retrieved_at
status_code
content_type
sha256
raw_path
```

Prefer stable source formats in this order:

1. structured HTML pages;
2. downloadable PDF/list files if HTML is incomplete;
3. small hand-built test fixtures;
4. OCR only if unavoidable.

Avoid OCR unless there is no better source.

Use polite fetching:

- central HTTP helper if one exists;
- low request rate;
- retries with backoff;
- cached outputs;
- deterministic build from cached bronze files.

---

## SQL Views To Add

Add new SQL views under `sql_views/`.

Suggested files:

```text
sql_views/legislation_si_lrc_classified_list.sql
sql_views/legislation_si_lrc_revised_act_refs.sql
sql_views/legislation_si_lrc_enrichment_summary.sql
```

The summary view should be UI-facing.

It should not overwrite existing SI legal-state semantics.

Recommended semantics:

- `si_current_state` remains the legal-state/source-effect layer.
- `si_lrc_enrichment_summary` adds classification and Revised Act relationship context.
- UI should show both, but clearly distinguish them.

---

## Data-Access Changes

Update or add to:

```text
utility/data_access/legislation_data.py
```

Potential functions:

```python
get_si_lrc_enrichment_summary(...)
get_si_lrc_classified_matches(...)
get_si_lrc_revised_act_refs(...)
get_si_subject_options(...)
get_si_department_options(...)
```

Return stable DataFrames with documented columns.

Do not hide errors as empty DataFrames. If the current data-access pattern catches exceptions, at least expose an `unavailable` flag or page warning.

---

## UI Changes

Update:

```text
utility/pages_code/statutory_instruments.py
utility/pages_code/legislation.py
```

Add optional panels:

### SI Legal Classification

Show:

- LRC subject heading;
- subheading path;
- department notation;
- LRC source URL;
- match confidence;
- manual-review flag.

### Revised Act References

Show:

- affected Act;
- annotation type;
- effect text;
- section/part;
- source link;
- whether it appears to be a commencement reference.

### Caveat Box

Show this near the enrichment panel:

> LRC enrichment is a source-linked research aid. It is not legal advice and may be incomplete or out of date. “Not matched” or “not checked” does not mean the SI is not in force.

---

## Metadata

Create metadata files such as:

```text
data/_meta/si_lrc_classified_list_coverage.json
data/_meta/si_lrc_revised_act_refs_coverage.json
data/_meta/si_lrc_enrichment_summary_coverage.json
```

Suggested fields:

```json
{
  "dataset": "si_lrc_enrichment_summary",
  "source": "Law Reform Commission",
  "source_urls": [
    "https://revisedacts.lawreform.ie/classlist/intro",
    "https://www.lawreform.ie/revised-acts/",
    "https://revisedacts.lawreform.ie/revacts/annotations"
  ],
  "retrieved_at": "...",
  "row_count": 0,
  "matched_si_count": 0,
  "unmatched_si_count": 0,
  "manual_review_count": 0,
  "parse_error_count": 0,
  "coverage_note": "LRC enrichment is a source-linked research aid and may be incomplete or out of date.",
  "legal_caveat": "Not legal advice. Not matched does not mean not in force."
}
```

---

## Tests

Add tests before routing this publicly.

Suggested files:

```text
test/test_si_lrc_enrichment.py
test/test_si_lrc_matching.py
test/test_si_lrc_sql_views.py
```

Test cases:

1. exact SI number/year match;
2. exact eISB URL match;
3. same number/year but title mismatch sets `requires_manual_review`;
4. title-only match is not public-facing without manual review;
5. unmatched SI becomes `not_matched` or `not_checked`, not `in_force`;
6. multiple subject paths are preserved;
7. multiple Revised Act references are preserved;
8. F-note maps to textual amendment;
9. C-note maps to non-textual legislative effect;
10. E-note maps to editorial/subsidiary legislation reference;
11. commencement order is flagged correctly;
12. source URL and retrieval metadata are preserved;
13. UI-facing summary has stable columns;
14. SQL views register successfully;
15. null enrichment does not break existing SI page.

---

## Acceptance Criteria

Implementation is acceptable when:

- extraction can run from cached bronze files;
- gold parquet outputs are deterministic;
- matching method and confidence are preserved;
- source URLs are preserved;
- LRC caveats are preserved;
- unmatched/null values are not treated as “in force”;
- fuzzy/title-only matches require manual review;
- SQL views register successfully;
- data-access functions expose stable schemas;
- Statutory Instruments page shows classification and caveats;
- tests cover dangerous legal-status edge cases;
- no OCR/heavy dependencies are added to normal CI;
- scripts remain in sandbox until stable enough to promote.

---

## Recommended First PR

Keep the first PR small.

Suggested scope:

1. Add `pipeline_sandbox/si_lrc_classlist_extract.py`.
2. Create small fixture from one or two LRC classlist pages.
3. Produce `si_lrc_classified_list.parquet`.
4. Add matching by SI number/year only.
5. Add coverage metadata.
6. Add SQL view.
7. Add tests for exact match, unmatched, title mismatch, source URL, and caveat.
8. Do not yet parse Revised Acts annotations.
9. Do not yet modify the public UI unless the data contract is stable.

---

## Recommended Second PR

1. Add UI panel to Statutory Instruments page.
2. Add subject-heading filters.
3. Add source/caveat component.
4. Add match-confidence display.
5. Add “requires manual review” badge.
6. Add metadata/freshness display.

---

## Recommended Third PR

1. Add Revised Acts annotation extraction.
2. Identify SI references in annotations.
3. Classify F/C/E-note references.
4. Flag commencement references.
5. Add Revised Act reference panel.
6. Add tests for annotation parsing and source preservation.

---

## Final Product Framing

The feature should be described as:

> LRC enrichment for SI discovery, classification, and Revised Act relationship research.

Not:

> definitive SI legal status.

Best UI label:

> LRC Classification & Revised Act References

Best caveat label:

> Source-linked research aid — not legal advice.
