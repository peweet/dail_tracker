# SI Amendment Graph — ETL Transition Plan (2026-06-04)

The SI→SI amendment/revocation graph as a directed-edge view, derived purely by
inverting the `affecting_sis` lists already recorded in `si_current_state`. No new
extractor, no new gold table — the data already flows through the pipeline.

## Why this and not LRC
Of the three enrichment axes explored, this is the **highest value-to-effort**:
data is 100% present (eISB-sourced, confidence-scored, provision-level), and the
build is a *view + UI*, not a scrape. The LRC subject classification (separate
doc `SI_LRC_ENRICHMENT_SPIKE.md`) is also worth shipping but needs a new gold
table; department backfill is parked (root cause is upstream in the actor parser).

## What the graph adds to the feature
Today the SI detail panel can only state the SI's own legal state ("revoked").
This view turns each SI into a node with two new relationship lists:
- **"Amended / revoked BY …"** (1,665 SIs have this) — the affecting instruments.
- **"This SI amends / revokes …"** (1,288 SIs, NEW) — the forward direction, which
  the page cannot show today; obtained by inversion.
Concrete: *S.I. 444/2024 Employment Permits Regulations* → revokes 18 prior
amendment SIs in one consolidating sweep. That genealogy is the engaging,
TheyWorkForYou-style inter-record linking the SI page currently lacks.

## Status — DONE
- [x] `sql_views/legislation_si_amendments.sql` → `v_si_amendments` (1,484 edges:
      1,315 revokes, 159 amends, 10 partially revokes). Registers via the
      `legislation_*` glob; reads parquet directly so load order is irrelevant.
- [x] Tests in `test/test_sql_views.py`:
      `test_v_si_amendments_inversion_contract` (synthetic fixture, precise:
      inversion, effect map, other_affected exclusion, no list fan-out, number/
      year parse, LEFT-JOIN title fill) + `test_v_si_amendments_executes`
      (real-data shape, effect enum, row-count == clean-state affecting count).
      Both pass; full SI section green.

## Grain & contract (locked by tests)
- One row per (amending SI → affected SI) clean edge.
- `effect ∈ {revokes, amends, partially revokes, amends and partially revokes}`.
- **`other_affected` is excluded** — its `affecting_sis` mixes indirect references
  (the 26 multi-affecting SIs are all `other_affected`); clean states are 100%
  single-affecting, so per-edge effect = the affected SI's `current_state`.
- Columns: `amender_{number,year,title,eli_url}`, `effect`, `current_state`,
  `provision_note`, `confidence`, `affected_{number,year,title,eli_url}`.
- `amender_title`/`affected_title` are NULL when that SI is outside the 2016–26
  gold window (LEFT JOIN, never a drop) — UI falls back to the "N/YYYY" citation.

## Remaining steps to ship

### 1. Data-access function — `utility/data_access/legislation_data.py`
Add `get_si_amendments(si_year, si_number) -> dict` returning both directions in
one round-trip, e.g.:
```python
def get_si_amendments(si_year: int, si_number: int) -> dict:
    con = get_legislation_conn()
    made = con.execute(
        "SELECT effect, affected_number, affected_year, affected_title, affected_eli_url, provision_note "
        "FROM v_si_amendments WHERE amender_number=? AND amender_year=? ORDER BY affected_year DESC, affected_number DESC",
        [si_number, si_year]).pl()        # "this SI amends/revokes …"
    received = con.execute(
        "SELECT effect, amender_number, amender_year, amender_title, amender_eli_url, provision_note, confidence "
        "FROM v_si_amendments WHERE affected_number=? AND affected_year=?",
        [si_number, si_year]).pl()        # "this SI was amended/revoked by …"
    return {"makes": made, "receives": received}
```
Return stable DataFrames; do not swallow to empty silently (mirror existing module
conventions). **Do NOT flatten into `v_statutory_instruments`** — it is 1-row-per-SI
and these are 1-to-many; flattening would fan out the index.

### 2. UI panel — `utility/pages_code/statutory_instruments.py` (SI detail)
A bidirectional "Amendment history" card, card-based per the dataframe rules:
- **"This SI amends / revokes"** — list `makes` rows: `{effect} S.I. {n}/{yr} — {title}` linking to the affected SI's page (internal) and eISB (external).
- **"Amended / revoked by"** — list `receives` rows with the `provision_note`
  (e.g. "Reg. 2 amended") so the user sees *what* changed.
- Render nothing (no empty card) when both are empty.
- **Logic-firewall**: the page only reads the view; it must not compute edges,
  parse `how_affected_raw`, or invert lists in Python. Add the `display_only`
  marker on any value_counts/aggregation if used (see firewall rule).

### 3. Pipeline / freshness — NOTHING to add
`v_si_amendments` reads `si_current_state.parquet`, owned by
`si_legislation_directory_extract.py` and already wired into the iris chain
(`iris_refresh.step_si_legal_state`). Freshness, rebuild, and provenance are
inherited — when the directory crawl updates revocations, the graph updates for
free. No `pipeline.py` change, no new coverage JSON.

### 4. Provenance line (UI footer)
"Amendment relationships derived from the eISB Legislation Directory via
`si_current_state` (updated to {directory_updated_to}). SI→SI only."

## Risks / honest caveats
- **Coverage is bounded by what eISB recorded** — this surfaces edges the
  directory crawl captured (≥95% gold coverage gate). It does NOT independently
  parse "(Amendment) Regulations" titles, so an amendment eISB never flagged
  won't appear. State this in provenance; don't imply completeness.
- **SI→SI only.** SI→Act textual amendments (LRC F/C/E notes) are out of scope.
- `provision_note` is lightly-cleaned eISB text ("Sch., pt. B amended"); display
  verbatim, don't parse it into structured provisions in the UI.

## Acceptance criteria
- [x] view registers in the legislation group; reads only gold parquet.
- [x] one row per clean edge; row count == clean-state affecting count.
- [x] effect enum locked; `other_affected` excluded; no list fan-out.
- [ ] `get_si_amendments` returns both directions with stable columns.
- [ ] SI detail panel shows bidirectional history, card-based, firewall-clean.
- [ ] empty state renders nothing (no orphan card).
