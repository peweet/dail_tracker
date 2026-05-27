# Logic Firewall Audit — Streamlit Pages

**Date:** 2026-05-27
**Scope:** `utility/pages_code/*.py` + `utility/data_access/*.py`
**Rubric:** [PIPELINE_VIEW_BOUNDARY.md](../dail_tracker_bold_ui_contract_pack_v5/docs/PIPELINE_VIEW_BOUNDARY.md) + contract-pack [CLAUDE.md](../dail_tracker_bold_ui_contract_pack_v5/CLAUDE.md)
**Forbidden in Streamlit:** raw parquet/csv reads, joins for modelling, multi-dim GROUP BY, HAVING/WINDOW, fuzzy matching, business-metric definitions, classification logic, deduplication of source data, pandas `groupby`/`merge`/`pivot`.
**Permitted scalars only:** `COUNT(*)`, `COUNT(DISTINCT col)`, `MAX(col)`, `MIN(col)` for hero stats.

---

## Severity tiers

- **P0 — Hard violation:** raw file read, modelling join, business metric defined in UI, classification logic.
- **P1 — Pipeline work hidden in UI:** pandas `groupby`/`value_counts` driving a primary view, GROUP BY in retrieval SQL, derived flags, ranking/normalisation.
- **P2 — Boundary smell:** display-only `value_counts` for a single chip/table, in-page string normalisation for taxonomy/labels, in-memory DuckDB registration of pandas frames.

---

## P0 — Hard violations

### [utility/pages_code/interests.py](../utility/pages_code/interests.py)

The whole module is a parallel mini-pipeline.

- `_load_interests` ([interests.py:94-136](../utility/pages_code/interests.py#L94-L136)) — **reads silver parquet/CSV directly** (`pd.read_parquet(SILVER_INTERESTS_PARQUET)` / `pd.read_csv(SILVER_INTERESTS_CSV)`), then performs UI-side **column renames, flag derivation, type coercion, and category filtering** (`df = df[df["interest_category"] != "15"]`).
  - The file itself acknowledges this at [interests.py:242](../utility/pages_code/interests.py#L242): `"read_parquet is forbidden in Streamlit (streamlit_may_read_parquet: false)"` — and does it anyway.
- `_fetch_filter_options`, `_fetch_interests`, `_fetch_td_data`, `_fetch_member_index_fallback` ([interests.py:139-293](../utility/pages_code/interests.py#L139-L293)) — register the pandas frame into an **in-process DuckDB** (`duckdb.connect(":memory:")` + `con.register("v_member_interests", base)`), then run `GROUP BY member_name` + `ROW_NUMBER() OVER (...)` window-function ranking. This is a registered analytical view *re-implemented* inside Streamlit.
- TODO already on file ([interests.py:240](../utility/pages_code/interests.py#L240)): `TODO_PIPELINE_VIEW_REQUIRED: v_member_interests_index`.

**Fix:** materialise `v_member_interests_detail` + `v_member_interests_index` in `sql_views/` reading the silver parquet directly; reduce page to `SELECT ... FROM v_member_interests_index WHERE house=? AND declaration_year=?`. Page-side DuckDB connection collapses to the standard `get_*_conn` pattern.

### [utility/pages_code/committees.py](../utility/pages_code/committees.py)

Explicitly self-labelled as a transitional CSV scaffold ([committees.py:6-9](../utility/pages_code/committees.py#L6-L9)).

- `_load` ([committees.py:117-204](../utility/pages_code/committees.py#L117-L204)) — `pd.read_csv(SILVER_MEMBERS_CSV[chamber])`, then **unpivots wide `committee_*` / `office_*` columns**, derives `is_chair` from a string match on role title (`"cathaoirleach" in str(role).lower()`), normalises status to `{Active, Ended}`, and constructs Oireachtas committee URLs from a slug helper. This is pipeline-grade reshaping done at request time.
- `_committee_summary` ([committees.py:208-254](../utility/pages_code/committees.py#L208-L254)) — `df_long.groupby("committee").agg(...)` building members/parties/chairs rollup; second pass builds a chair-lookup dict from another `groupby`; third pass builds a per-committee party-seats list from `value_counts`. Three rollups in the page.
- Committee URL construction ([committees.py:107-111](../utility/pages_code/committees.py#L107-L111)) lives in the page — boundary doc lists "official URL construction" as pipeline-owned.

**Fix:** awaiting `v_committee_assignments`, `v_committee_member_detail`, `v_committee_sources`, `v_committee_party_seats` (already tracked in `committees.yaml § transition_state`). The page comments label its own groupbys as "permitted by transition_state" — the exception is documented but the debt is live.

### [utility/data_access/legislation_data.py](../utility/data_access/legislation_data.py)

- `fetch_pre2014_act_detail` ([legislation_data.py:172-188](../utility/data_access/legislation_data.py#L172-L188)) — `pd.read_csv(_PRE2014_CSV)` inside the data-access layer. The module's own header docstring forbids this ([legislation_data.py:9-13](../utility/data_access/legislation_data.py#L9-L13)).
- `fetch_si_composition` ([legislation_data.py:194-203](../utility/data_access/legislation_data.py#L194-L203)) — `GROUP BY si_operation` in retrieval SQL. Single-column count, but the contract is clear: only scalar aggregates (`COUNT`, `MAX`, `MIN`). This is an operation-mix metric and belongs in `v_bill_si_operation_mix` (or as additional columns on `v_bill_statutory_instruments`).

**Fix:** wrap pre-2014 lookup in a `sql_views/legislation_pre2014_acts.sql` view reading the CSV via `read_csv_auto`; promote operation-mix to a registered view.

---

## P1 — Pipeline work in UI

### [utility/pages_code/statutory_instruments.py](../utility/pages_code/statutory_instruments.py)

Goes through `fetch_si_entity_index()` cleanly, but post-load behaviour is heavy:

- `_apply_filters` ([statutory_instruments.py:274-295](../utility/pages_code/statutory_instruments.py#L274-L295)) — every facet filter is a pandas boolean mask on the full corpus held in memory. UI filtering is permitted, but doing it on a frame in RAM rather than parameter-bound SQL means the page hands the full dataset to pandas each render.
- `_render_kpi_strip` ([statutory_instruments.py:301-345](../utility/pages_code/statutory_instruments.py#L301-L345)) — computes "most active department" via `value_counts().head(1)`, EU share, enabling-Act link rate. These are headline civic metrics. They define the page's hero numbers and should live in `v_statutory_instruments_summary`.
- `_eu_scrutiny_stats` ([statutory_instruments.py:380-389](../utility/pages_code/statutory_instruments.py#L380-L389)) — boolean-AND mask combined with a date-cutoff constant (`_COMMITTEE_FORMED = 2025-12-01`), then `value_counts().head(5)` for departments. The cutoff is a **modelling constant** (date a committee was formed) embedded in UI code.
- Six more `value_counts(...).to_dict()` calls in `_render_facet_pills` family ([statutory_instruments.py:559-634](../utility/pages_code/statutory_instruments.py#L559-L634)) — year, department, operation, domain, minister counts. All chip widths driven by UI-side aggregation.
- Mojibake filtering ([statutory_instruments.py:196](../utility/pages_code/statutory_instruments.py#L196)) — `df = df[~df["si_title"].astype(str).str.contains("�", na=False)]`. Data quarantine in the UI ([feedback_gold_layer_quarantine](.) permits *flagged* WHERE filters as a quarantine, but this one is silent — no provenance, no expander).
- `_pretty_token` ([statutory_instruments.py:235-255](../utility/pages_code/statutory_instruments.py#L235-L255)) — taxonomy normalisation (snake_case → sentence-case, EU prefix preservation). Belongs in the view's `label` column.

**Fix:** introduce `v_statutory_instruments_summary` (totals, top dept, EU share, bill-link share, EU-since-committee count + by-dept rollup). Move title quarantine + label normalisation upstream. Keep `_apply_filters` as a thin pandas wrapper — acceptable for an in-page faceted explorer, but the source numbers must be view-owned.

### [utility/pages_code/lobbying_2.py](../utility/pages_code/lobbying_2.py)

- "Where filed" panel ([lobbying_2.py:1561-1591](../utility/pages_code/lobbying_2.py#L1561-L1591)) — `detail["public_policy_area"].value_counts().head(10)`. Self-labelled "UI aggregation only, no business logic" — but the **denominator interpretation** (how returns map to policy areas) *is* business semantics. Already a 10-row table with a progress bar; treat the count as a metric and put it in `v_lobbying_topic_policy_mix`.
- DPO label join ([lobbying_2.py:2316-2319](../utility/pages_code/lobbying_2.py#L2316-L2319)) — `csv_export["return_id"].astype(str).map(lambda rid: "; ".join(dpo_by_return.get(rid, [])))` — page-side dict-join used to enrich the CSV export with DPO names. This is a row-level join.
- Position fuzzy match ([lobbying_2.py:565-572](../utility/pages_code/lobbying_2.py#L565-L572)) — `idx[idx["position"].str.contains(full_position, case=False, na=False)]` used to resolve notable-target chips ("Taoiseach", "Finance"…) to a politician. Fuzzy matching is explicitly pipeline-owned per the rubric.

**Fix:** add `v_lobbying_politician_by_position` (canonical position → politician resolution); ship a flat `v_lobbying_returns_with_dpo` so the export is a SELECT. Policy-area breakdown becomes a view.

### [utility/pages_code/member_overview.py](../utility/pages_code/member_overview.py)

- `_named_parties` / `_party_pill_options` ([member_overview.py:951-973](../utility/pages_code/member_overview.py#L951-L973)) — runs `df["party_name"].value_counts()` twice and applies an `_OTHER_MIN = 3` threshold to decide which parties get pills vs collapsed into "Other / Independent". **`_OTHER_MIN = 3` is a modelling constant** ([feedback_iteration_process](.): UI thresholds drift silently).
- Multi-column substring search ([member_overview.py:1030-1036](../utility/pages_code/member_overview.py#L1030-L1036)) — OR over `member_name | party_name | constituency` in pandas. Display-side filter, acceptable, but worth noting it's not parameter-bound SQL.
- Deduplication ([member_overview.py:998](../utility/pages_code/member_overview.py#L998)) — `df.drop_duplicates(subset=["unique_member_code"], keep="first")`. Per the rubric: "If a view produces duplicate rows for the same key, this is a pipeline deduplication problem, not a UI display problem." `v_member_list` should already be unique per `unique_member_code`.

**Fix:** dedup belongs upstream of the view; if there's a real reason multiple rows exist, expose the discriminator. Add a `party_bucket` column (or accept the threshold as a registered metric).

### [utility/pages_code/votes.py](../utility/pages_code/votes.py)

- Topic-picker ILIKE OR chain ([votes.py:230-233](../utility/pages_code/votes.py#L230-L233)) — `" OR ".join(["debate_title ILIKE ?" for _ in _TD_PICKER_TOPICS])`. A constant Python list (`_TD_PICKER_TOPICS`) is being used to classify debates into topic buckets at the query layer. Topic classification is pipeline territory — the same string list determines which debates count as "Housing" / "Health" etc. across the app.

**Fix:** expose `v_debate_topic_bucket` (debate_id → bucket label).

---

## P2 — Boundary smells

| Location | Smell |
|---|---|
| [statutory_instruments.py:294](../utility/pages_code/statutory_instruments.py#L294) | `str.lower().str.contains` for title search — display-only, OK in isolation, but search semantics are still UI-defined |
| [committees.py:431](../utility/pages_code/committees.py#L431) | `summary["committee"].str.contains(search.strip(), case=False, na=False)` — display filter on already-rolled-up summary, ride-along of the bigger committees P0 |
| [lobbying_2.py:1031, 1131, 1232](../utility/pages_code/lobbying_2.py#L1031) | Three `str.contains` filters on text columns (member_name, policy_area, lobbyist_name) — display-only |
| [interests.py:189, 195](../utility/pages_code/interests.py#L189-L195) | Building parameterised `IN (?, ?, ?)` clauses in Python — fine in isolation, but it's inside the in-memory DuckDB pattern flagged as P0 |
| [member_overview.py:363, 392, 422](../utility/pages_code/member_overview.py#L363-L422) | `" AND ".join(clauses)` SQL assembly. Parameter-bound, retrieval-only — boundary doc allows this, just verify approved columns remain the allowlist |
| [legislation.py:350](../utility/pages_code/legislation.py#L350) | Comment mentions `(union of versions / related_docs / bill_amendments)` — verify this UNION lives in the SQL view, not in the page |
| [statutory_instruments.py:271](../utility/pages_code/statutory_instruments.py#L271) | `_COMMITTEE_FORMED = pd.Timestamp("2025-12-01")` — modelling constant on the date a committee was formed; meets the historical-fact exception but should be sourced/cited |

---

## Patterns

1. **In-memory DuckDB + registered pandas frame** (`interests.py`) — a Streamlit-side workaround for a missing registered view. Recognisable shape: `pd.read_parquet(...)` → `duckdb.connect(":memory:")` → `con.register(...)` → `GROUP BY` / `WINDOW`. Wherever you see `duckdb.connect(":memory:")` in `pages_code/`, that's the smell.
2. **`value_counts()` driving hero numbers** (`statutory_instruments.py`, `lobbying_2.py`, `committees.py`) — "top department", "most active sector", "policy-area mix" all computed in the page. These are the page's headline civic metrics and they belong in summary views.
3. **Self-acknowledged transitional CSV reads** (`committees.py`, `legislation_data.py` pre-2014, `interests.py`) — three pages explicitly comment that they're papering over missing pipeline views. The TODOs exist; the views don't.
4. **String-list classification** (`votes.py` topic-picker, `lobbying_2.py` notable-target position lookup) — Python constants used to bucket rows into civic categories at query time. Topic/role classification belongs in the pipeline so the same definition is used everywhere.
5. **Data-access modules calling themselves "retrieval-only" while doing more** — `legislation_data.py` declares `JOIN, GROUP BY, HAVING, WINDOW` forbidden in its docstring, then issues `GROUP BY si_operation` and reads a CSV directly. The header docstrings drift from the code.

---

## Recommended pipeline work (priority order)

1. `v_member_interests_detail` + `v_member_interests_index` → kill `interests.py` in-memory DuckDB + parquet read.
2. `v_committee_assignments` / `_member_detail` / `_sources` / `_party_seats` → kill `committees.py::_load` and `_committee_summary`.
3. `v_statutory_instruments_summary` (totals, top dept, EU share, bill-link share) + `v_statutory_instruments_eu_scrutiny` → kill `_render_kpi_strip` + `_eu_scrutiny_stats` aggregations.
4. `v_legislation_pre2014_acts` (view over the curated CSV) + `v_bill_si_operation_mix` → kill `legislation_data.py` raw read + GROUP BY.
5. `v_lobbying_returns_with_dpo` + `v_lobbying_politician_by_position` + `v_lobbying_topic_policy_mix` → kill `lobbying_2.py` row-level join + fuzzy position lookup + value_counts panel.
6. `v_debate_topic_bucket` → kill `votes.py::_TD_PICKER_TOPICS` ILIKE-OR chain.
7. Dedup `v_member_list` upstream so `member_overview.py:998` can drop `drop_duplicates`.

## Pages that are clean

- [utility/pages_code/payments.py](../utility/pages_code/payments.py)
- [utility/pages_code/attendance.py](../utility/pages_code/attendance.py) (after the 2026-05-26 attendance fixes)
- [utility/pages_code/lobbying_3.py](../utility/pages_code/lobbying_3.py)
- [utility/pages_code/legislation.py](../utility/pages_code/legislation.py) (modulo verifying the UNION comment at line 350)
- [utility/pages_code/glossary.py](../utility/pages_code/glossary.py)

All five go through their data-access module, use parameter-bound retrieval SQL only, and don't reach for pandas aggregations on the result.
