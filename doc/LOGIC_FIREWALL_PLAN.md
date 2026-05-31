# Logic Firewall Remediation Plan

**Date:** 2026-05-27
**Companion to:** [LOGIC_FIREWALL_AUDIT.md](LOGIC_FIREWALL_AUDIT.md)
**Goal:** Move every flagged join / aggregation / classification / raw-read out of `utility/pages_code/` and `utility/data_access/` into pipeline views, **without changing UI behaviour**.

---

## Non-negotiable constraints

1. **No UI changes.** Every page must render byte-identically before and after each swap. Card layouts, filter chips, KPI numbers, ordering, pagination, exports — all unchanged.
2. **No edits to** `pipeline.py`, `enrich.py`, `normalise_join_key.py`, or their dependencies. ([project_pipeline_sandbox_rule](.))
3. **New Python/Polars enrichment** → `pipeline_sandbox/` only. Self-contained, writes parquet, never imported by the main pipeline.
4. **New SQL views** → `sql_views/` directly. Always `SELECT`, never `INSERT/UPDATE/DELETE/CREATE TABLE`.
5. **Parquet writes** use `compression="zstd"`, `compression_level=3`, `statistics=True`. ([feedback_parquet_write_convention](.))
6. **TD-name matching** uses `normalise_df_td_name` (NFD + sort letters) where joins on member name are needed. ([project_td_name_join_key](.))

---

## Safe-swap pattern (apply to every fix)

For each (in-page ETL → registered view) migration:

1. **Build the view.** Either pure SQL in `sql_views/<name>.sql`, or `pipeline_sandbox/<name>_etl.py` → parquet → `sql_views/<name>.sql` reading that parquet.
2. **Snapshot UI output before the swap.** For the affected page, capture:
   - the `DataFrame` returned by the in-page loader (`.to_csv()` of a representative call)
   - the rendered card HTML (the `"\n".join(cards)` payload) for one or two representative inputs
   - a Playwright screenshot via the page's `audit_screenshots/_<page>_*.py` harness if one exists
3. **Verify the new view matches that snapshot.** Same row count, same columns, same dtypes (`pd.testing.assert_frame_equal` with `check_like=True`).
4. **Add a new `fetch_*` function in `utility/data_access/<page>_data.py`** that returns the view's DataFrame in the exact shape the page consumes today.
5. **Swap the page loader** from the in-page function to the new `fetch_*`. One call-site change.
6. **Re-snapshot.** Diff against step 2. Zero pixel / zero HTML / zero CSV difference is the bar.
7. **Delete the in-page logic.** `_load_*`, `_apply_filters` aggregators, etc. Remove now-unused imports (`pd.read_parquet`, `duckdb.connect(":memory:")`).
8. **Update the page YAML contract** in `utility/page_contracts/` to add the new view to `approved_registered_views`.

**Roll-back posture:** until step 7, the old in-page code is still there. If step 6 catches a diff, revert step 5 and investigate before deleting anything.

---

## Verification harness (build first, before any migration)

Two pieces of infra are needed to do this safely at scale:

### V1. Streamlit logic-firewall checker

`tools/check_streamlit_logic_firewall.py` is referenced in [06_review_logic_firewall.prompt.md](../dail_tracker_bold_ui_contract_pack_v5/prompts/06_review_logic_firewall.prompt.md) but **does not exist**. Build it:

```python
# tools/check_streamlit_logic_firewall.py
# Static AST check. Fails CI if any of these appear in utility/pages_code/ or
# utility/data_access/:
#   - pd.read_parquet / pd.read_csv / pl.read_* / pl.scan_*
#   - duckdb.connect(":memory:") + register pattern
#   - DataFrame.merge / pd.merge
#   - DataFrame.groupby(...) followed by .agg/.sum/.count/.first/.value_counts on a primary path
#   - SQL string literals containing "JOIN", "GROUP BY", "HAVING", " OVER ("
#   - regex / fuzzy matching helpers
# Allow-list: known display-only callsites (single-column value_counts on a
# subset for a chip row) flagged in code with `# logic_firewall: display_only`.
```

Why first: if you build this **before** the migrations, you can run it after every step 7 to confirm violations actually got removed. Without it, you'll regress silently.

### V2. Per-page output snapshot

For each page being migrated, write a one-shot Pytest that:

1. Calls every public `fetch_*` in the page's data-access module with representative args.
2. Hashes the resulting DataFrame (`hashlib.sha256(df.to_csv(index=False).encode())`).
3. Stores the hash in `tests/snapshots/<page>_data.json`.

Run before migration → capture baseline. Run after each migration step → diff. Use this to catch shape/dtype drift the eye misses.

---

## Phase 1 — pure SQL views (no sandbox)

Quick wins. No new Python enrichment, just SQL views over silver/gold parquets.

### 1.1 `v_legislation_pre2014_acts`

**Replaces:** `utility/data_access/legislation_data.py::fetch_pre2014_act_detail` ([legislation_data.py:172-188](../utility/data_access/legislation_data.py#L172-L188))

**View:**
```sql
-- sql_views/legislation_pre2014_acts.sql
CREATE OR REPLACE VIEW v_legislation_pre2014_acts AS
SELECT canonical_bill_id,
       act_short_title,
       CAST(act_year AS INTEGER) AS act_year,
       policy_domain
FROM read_csv_auto('data/silver/legislation/pre2014_acts.csv',
                   header=true, all_varchar=false);
```
(Confirm exact path with the existing `_PRE2014_CSV` constant before writing.)

**Data-access change:**
```python
@st.cache_data(ttl=3600)
def fetch_pre2014_act_detail(bill_id: str) -> dict:
    if not (isinstance(bill_id, str) and bill_id.startswith("act_")):
        return {}
    rows = _safe(
        "SELECT act_short_title, act_year, policy_domain"
        " FROM v_legislation_pre2014_acts WHERE canonical_bill_id = ?",
        [bill_id],
    )
    if rows.empty:
        return {}
    r = rows.iloc[0]
    return {"act_short_title": str(r["act_short_title"] or ""),
            "act_year": int(r["act_year"] or 0),
            "policy_domain": str(r["policy_domain"] or "")}
```

**Verification:** call with 5 known pre-2014 bill_ids; compare dict-equality with current behaviour.

---

### 1.2 `v_bill_si_operation_mix`

**Replaces:** `legislation_data.py::fetch_si_composition` ([legislation_data.py:194-203](../utility/data_access/legislation_data.py#L194-L203)) — the `GROUP BY si_operation` in retrieval SQL.

**View:**
```sql
-- sql_views/legislation_bill_si_operation_mix.sql
CREATE OR REPLACE VIEW v_bill_si_operation_mix AS
SELECT bill_id, si_operation, COUNT(*) AS n
FROM v_bill_statutory_instruments
WHERE si_operation IS NOT NULL
GROUP BY bill_id, si_operation;
```

**Data-access:** `SELECT si_operation, n FROM v_bill_si_operation_mix WHERE bill_id = ? ORDER BY n DESC`. Pure retrieval. Function signature/return unchanged.

---

### 1.3 `v_statutory_instruments_facet_counts`

**Replaces:** the six full-corpus `value_counts(...).to_dict()` calls in `statutory_instruments.py` that drive chip widths ([statutory_instruments.py:559-634](../utility/pages_code/statutory_instruments.py#L559-L634)) and `_render_kpi_strip` headline numbers when no filters applied.

**Per the trade-off chosen:** view owns full-corpus facet counts. KPI strip's filter-aware computation stays in-page (display-only aggregation on the active filter set, accepted as P2).

**View:** one view per facet, joined into a single materialised view in DuckDB:
```sql
-- sql_views/statutory_instruments_facet_counts.sql
CREATE OR REPLACE VIEW v_si_facet_counts AS
SELECT 'year' AS facet, CAST(si_year AS VARCHAR) AS value, COUNT(*) AS n
FROM v_statutory_instruments WHERE si_year IS NOT NULL
GROUP BY si_year
UNION ALL
SELECT 'department', si_department_label, COUNT(*)
FROM v_statutory_instruments WHERE si_department_label IS NOT NULL
GROUP BY si_department_label
UNION ALL
-- repeat for operation, policy_domain, minister
;
```

Plus a `v_statutory_instruments_summary` for the *unfiltered* hero numbers (total, top dept, EU share, bill-link share, year span). When filters become non-empty the page falls back to in-page pandas computation, **flagged with a `# logic_firewall: display_only` comment** so the V1 checker allow-lists it.

**Also during this phase:** move title-mojibake quarantine ([statutory_instruments.py:196](../utility/pages_code/statutory_instruments.py#L196)) into the view's WHERE clause, and move `_pretty_token` label normalisation ([statutory_instruments.py:235-255](../utility/pages_code/statutory_instruments.py#L235-L255)) into a `si_*_label` column produced by the pipeline.

**Verification:** load full corpus, render KPI strip with no filters → byte-identical to current rendering. Then apply each facet filter → identical chip layout because chip widths now come from the full-corpus view (same denominator).

---

### 1.4 `v_lobbying_returns_with_dpo`

**Replaces:** the row-level dict-join in `lobbying_2.py::csv_export` ([lobbying_2.py:2316-2319](../utility/pages_code/lobbying_2.py#L2316-L2319)).

**View:**
```sql
-- sql_views/lobbying_returns_with_dpo.sql
CREATE OR REPLACE VIEW v_lobbying_returns_with_dpo AS
SELECT r.*,
       string_agg(DISTINCT d.dpo_name, '; ' ORDER BY d.dpo_name) AS dpo_names
FROM v_lobbying_returns r
LEFT JOIN v_lobbying_dpo_by_return d USING (return_id)
GROUP BY r.return_id, r.<...all other r columns explicitly...>;
```
(Explicit column enumeration is required — DuckDB needs every selected column in GROUP BY or in an aggregate.)

**Data-access:** new `fetch_returns_with_dpo(...)` that selects from this view with the existing filters. CSV export gets the `dpo_names` column for free; the `.map(lambda ...)` block at line 2316-2319 disappears.

---

### 1.5 `v_lobbying_topic_policy_mix`

**Replaces:** "Where filed" panel's `value_counts().head(10)` ([lobbying_2.py:1561-1591](../utility/pages_code/lobbying_2.py#L1561-L1591)).

**View:** parameterised by topic; expressed as a SELECT with `WHERE topic_keyword IN (...)` against an existing topic-tagged returns view. If no such view exists, defer to Phase 2 (sandbox classifier).

---

### 1.6 `v_member_interests_detail` + `v_member_interests_index`

**Replaces:** all of [utility/pages_code/interests.py](../utility/pages_code/interests.py) data layer — the parquet read, the in-memory DuckDB, the rename/coerce/filter, the GROUP BY + ROW_NUMBER ranking.

**Detail view:**
```sql
-- sql_views/member_interests_detail.sql
CREATE OR REPLACE VIEW v_member_interests_detail AS
SELECT full_name        AS member_name,
       party            AS party_name,
       constituency_name AS constituency,
       TRY_CAST(year_declared AS INTEGER) AS declaration_year,
       interest_category,
       interest_description_cleaned AS interest_text,
       LOWER(is_landlord) = 'true'        AS landlord_flag,
       LOWER(is_property_owner) = 'true'  AS property_flag,
       house
FROM read_parquet('data/silver/interests/*.parquet')
WHERE interest_category <> '15';
```

**Index view (ranking):**
```sql
-- sql_views/member_interests_index.sql
CREATE OR REPLACE VIEW v_member_interests_index AS
WITH agg AS (
  SELECT house, declaration_year, member_name,
         MAX(party_name) AS party_name,
         MAX(constituency) AS constituency,
         COUNT(*) AS total_declarations,
         0 AS directorship_count,  -- TODO_PIPELINE_VIEW_REQUIRED
         COUNT(DISTINCT CASE WHEN interest_category = 'Land (including property)'
                              AND interest_text IS NOT NULL
                              AND TRIM(interest_text) <> ''
                              AND LOWER(TRIM(interest_text)) <> 'no interests declared'
                         THEN interest_text END) AS property_count,
         COUNT(DISTINCT CASE WHEN interest_category = 'Shares' AND ...
                         THEN interest_text END) AS share_count,
         BOOL_OR(landlord_flag) AS is_landlord,
         BOOL_OR(property_flag) AS is_property_owner
  FROM v_member_interests_detail
  WHERE member_name IS NOT NULL
  GROUP BY house, declaration_year, member_name
)
SELECT ROW_NUMBER() OVER (PARTITION BY house, declaration_year
                          ORDER BY total_declarations DESC, member_name) AS rank,
       *
FROM agg;
```

**Data-access:** new `utility/data_access/interests_data.py` with `get_interests_conn`, `fetch_interests_filter_options`, `fetch_interests`, `fetch_td_interests`, `fetch_member_index`. Mirrors the seven other data-access modules.

**Page change:** every page function calls the new data-access. `pd.read_parquet`, `pd.read_csv`, `duckdb.connect(":memory:")`, `con.register(...)` all gone.

---

## Phase 2 — pipeline_sandbox + view

Needs Polars enrichment before SQL can see the data.

### 2.1 Committees — ETL script + 4 views

**ETL script:** `committees_long_format_etl.py`

```python
# Reads silver/members/<chamber>.csv, unpivots committee_*/office_* wide
# columns into long format, derives is_chair, normalises status, strips the
# "(Dáil Éireann)" suffix, builds canonical Oireachtas committee URLs.
# Writes data/silver/committees/committee_assignments.parquet
#          data/silver/committees/office_holders.parquet
# Both with zstd / level=3 / statistics=True.
```

The reshape logic is a direct port of `committees.py::_load` ([committees.py:117-204](../utility/pages_code/committees.py#L117-L204)) — same rules, same dtypes. Run it ad-hoc; output committed (data tracked) so the SQL views can read it without a pipeline run.

**Four views** (matching the contract pack's `committees.yaml § transition_state` plan):

| View | Replaces |
|---|---|
| `v_committee_assignments` | `_load()` long-format DataFrame |
| `v_committee_member_detail` | `_committee_summary()` per-committee rollup (`members`, `parties`, `chairs`, `status`, `type`, `url`) |
| `v_committee_party_seats` | the `value_counts` party-seats lookup at [committees.py:246-248](../utility/pages_code/committees.py#L246-L248) |
| `v_committee_sources` | provenance — silver CSV `latest_fetch_timestamp`, row count, code version |

**Page change:** `committees.py` drops `_load`, `_committee_summary`, `_committee_slug`, `_committee_url`, `_coalesce`, all the `pd.read_csv` import surface. The transitional banner ([committees.py:260-301](../utility/pages_code/committees.py#L260-L301)) gets removed. Every UI render that consumed `df_long` / `offices` / `summary` now reads from the corresponding view.

**Risk:** this is the biggest UI surface area touched. `member_overview.py` also calls `render_member_committees(member_name, df_long, offices, ...)` ([member_overview.py:940-948](../utility/pages_code/member_overview.py#L940-L948)) — that helper signature stays the same but its caller now sources the frames from views.

---

### 2.2 `v_debate_topic_bucket` — topic classifier sandbox

**Sandbox script:** `pipeline_sandbox/debate_topic_bucket_etl.py`

```python
# Reads the silver debates parquet. For each debate, scans the title against
# each topic bucket's substring list (the existing _TD_PICKER_TOPICS data
# moved into a YAML constant under config/topic_taxonomy.yaml).
# Writes data/silver/debates/topic_buckets.parquet as long-format:
#   debate_id, topic_bucket, matched_substring
```

**View:**
```sql
-- sql_views/debate_topic_bucket.sql
CREATE OR REPLACE VIEW v_debate_topic_bucket AS
SELECT debate_id, topic_bucket
FROM read_parquet('data/silver/debates/topic_buckets.parquet');
```

**Votes page change:** the OR-chain ILIKE at [votes.py:230-233](../utility/pages_code/votes.py#L230-L233) becomes:
```sql
SELECT v.* FROM v_vote_index v
JOIN v_debate_topic_bucket b USING (debate_id)
WHERE b.topic_bucket IN (?, ?, ...)
```
…executed inside the data-access layer (this is a modelling join — it can only live there because the join is in a *registered view*, not in retrieval SQL). Actually: build `v_vote_index_with_topic_bucket` as the registered view that does the join, and the page does a flat SELECT.

**Taxonomy ownership:** the substring lists move out of Python into `config/topic_taxonomy.yaml` so the sandbox can re-classify without code changes.

---

### 2.3 `v_lobbying_politician_by_position` — canonical-position lookup

**Sandbox script:** `pipeline_sandbox/lobbying_canonical_position_etl.py`

```python
# Reads silver/lobbying politician index. For each row, normalises position
# strings (whitespace, case, "Minister for X" alias map) and matches against
# a canonical position list in config/canonical_positions.yaml.
# Writes data/silver/lobbying/politician_canonical_position.parquet:
#   member_name, position_raw, canonical_position
```

**View:** flat SELECT over that parquet.

**Page change:** [lobbying_2.py:565-572](../utility/pages_code/lobbying_2.py#L565-L572) becomes:
```python
m = fetch_politician_by_canonical_position(full_position)
```
No `str.contains` anywhere in the page.

---

## Phase 3 — boundary cleanup

Small, mechanical fixes once Phases 1–2 are landed.

### 3.1 De-dup `v_member_list`

`member_overview.py:998` calls `df.drop_duplicates(subset=["unique_member_code"], keep="first")`. Investigate why the view emits duplicates (silver members CSV likely has multiple Dáil-term rows per member, view doesn't pick latest). Fix at the view layer:

```sql
-- in sql_views/member_registry.sql or v_member_list source
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_member_code
                           ORDER BY dail_number DESC NULLS LAST) = 1
```

Then delete the page-side `drop_duplicates`.

### 3.2 `_OTHER_MIN = 3` — justify or relocate

This is a chip-layout threshold (parties with < 3 TDs collapse into "Other / Independent"). Two options:

- **Keep in UI** with a code comment marking it as a UI-display threshold (chip count is a layout concern, not a metric). Add `# logic_firewall: display_only`.
- **Promote** to a `party_bucket` column on `v_member_list` — pipeline owns the cut-off.

Recommend **Keep in UI** — chip layout density is a page-design concern, and changing it shouldn't require a pipeline rebuild. Just annotate.

### 3.3 SI title mojibake quarantine

Currently silent: `df = df[~df["si_title"].astype(str).str.contains("�", na=False)]` ([statutory_instruments.py:196](../utility/pages_code/statutory_instruments.py#L196)).

Move into `v_statutory_instruments` definition with an explicit `quarantine_flag` column. UI gets to surface the quarantine count in the provenance expander per [feedback_gold_layer_quarantine](.).

### 3.4 Data-access docstring drift

Three modules (`legislation_data.py`, `lobbying_data.py`, `payments_data.py`) declare `JOIN/GROUP BY/HAVING/WINDOW` forbidden in their headers. After Phase 1.2 lands, audit each module for remaining `GROUP BY` in retrieval SQL and either remove or relocate. The headers must match the code.

---

## Sequencing

```
Phase 0 (build first):  V1 checker + V2 snapshot harness                    [≈1d]
Phase 1.1 + 1.2:        Legislation pre2014 + bill-SI operation mix         [≈0.5d]
Phase 1.3:              SI facet counts + summary                           [≈1d]
Phase 1.4:              Lobbying returns-with-dpo                           [≈0.5d]
Phase 1.5:              Lobbying topic policy mix                           [≈0.5d]
Phase 1.6:              Interests detail + index                            [≈1d]
                        ── checkpoint: all P0/P1 in pure-SQL pages done ──
Phase 2.1:              Committees sandbox + 4 views                        [≈2d]
Phase 2.2:              Debate topic bucket                                 [≈1d]
Phase 2.3:              Lobbying canonical position                         [≈0.5d]
                        ── checkpoint: P0/P1 in sandbox-needed pages done ──
Phase 3.1–3.4:          Cleanup                                             [≈1d]
                        ── checkpoint: V1 checker passes clean ──
```

Run Phase 1 fixes **independently** — each is a standalone PR with its own before/after snapshot. Phase 2 fixes are larger and benefit from being grouped per-page (one committees PR, one votes PR, etc.).

**Deferred per [feedback_refactor_timing](.)** — execute when active ETL/feature work hits a stable plateau. This document is the plan; the audit + plan-during is fine now, execute later.

---

## Open trade-offs (decisions made, documented for future reviewers)

1. **SI KPI strip metric ownership.** Resolved: full-corpus facets in view; filter-aware KPIs in page (display-only). Page-side computation flagged with `# logic_firewall: display_only`.
2. **Vote topic taxonomy.** Resolved: pipeline_sandbox classifier writes (debate_id, bucket) parquet. YAML config file owns the substring lists. Re-classification requires re-running the sandbox script, not a code change.
3. **Committees reshape.** Resolved: pipeline_sandbox Polars script over pure-SQL UNNEST. Easier to test, easier to extend, contained.
4. **`_OTHER_MIN`.** Resolved: stays in UI as a display threshold, annotated.

---

## What does NOT change

- Page YAML contracts' `approved_columns` / `approved_filters` shape (only `approved_registered_views` grows).
- `utility/shared_css.py`, `utility/components.py`, every `dt-*` / `att-*` / `pay-*` class.
- Sidebar navigation, page slugs, query-param schema.
- Card rendering, KPI numbers, chip layouts, ordering, pagination, exports, provenance expanders.
- All 13 page files' rendering surface area. Edits are limited to data-loading callsites.

If any of the above changes during execution, that's a deviation from this plan and needs to be called out.
