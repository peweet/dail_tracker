# Pipeline/View Boundary

## Belongs in pipeline / registered analytical views

- joins
- fuzzy matching
- normalisation
- metric definitions
- annual summaries
- member profiles
- vote result summaries
- official URL construction
- flags
- rankings
- rollups
- raw Parquet scans

## Belongs in Streamlit

- selecting approved columns
- filtering approved columns
- sorting approved columns
- limiting result sets
- formatting
- rendering
- CSV export of current view
- detail panels using approved detail views
- source link display

## Required wording

When Streamlit needs data that does not exist:

```text
TODO_PIPELINE_VIEW_REQUIRED: <specific missing item>
```

---

## Pre-build SQL view inspection rule (learned: attendance session)

**Before writing any UI classification, grouping, or filtering logic against a view column,
read the corresponding `.sql` file in `sql_views/` and confirm the column is computed from
real data — not hardcoded.**

Common traps found in real views:
- `'Present' AS attendance_status` — hardcoded string, useless for classification
- `NULL::VARCHAR AS mart_version` — always null, do not surface in UI logic
- `TRUE AS present_flag` — constant, cannot be used to filter

If you find a hardcoded column that the UI was going to branch on, do not try to work around
it. Replace the UI logic with a `todo_callout`:

```python
todo_callout(
    "TODO_PIPELINE_VIEW_REQUIRED: <column_name> on <view_name>. "
    "Currently hardcoded '<value>' — pipeline must expose the real value."
)
```

**Why:** Attendance had `attendance_status = 'Present'` hardcoded. UI code was written to
classify sitting-day vs other-day rows using `str.startswith("sitting")` — it silently
produced wrong counts (0 sitting days, 29 other days) that were surfaced to users.

---

## Permitted hardcoding for fixed historical values

The general rule is that hardcoded values in Streamlit are forbidden. This exception is narrow
and must be justified explicitly.

**Hardcoding is permitted when all four conditions hold:**

1. The value is a **fixed, immutable, historically-verified fact** — it cannot change after the
   fact (e.g. "the Dáil sat 83 days in 2024" is settled once 2024 is over).
2. The source is an **official published document** (e.g. Houses of the Oireachtas Commission
   Annual Report, Statute Book, Constitution).
3. The pipeline change required to expose the value programmatically would be
   **disproportionate** — e.g. it requires re-parsing a PDF, a new extraction step, or a schema
   change that is significantly more complex than the benefit warrants.
4. The constant is **annotated in code** with the source URL or document name so a future
   developer can verify it.

**Canonical example — `_YEAR_SITTING_DAYS` in `attendance.py`:**
```python
# Official plenary sitting-day counts from Houses of the Oireachtas Commission annual reports.
# Sources: oireachtas.ie/en/press-centre/press-releases/
_YEAR_SITTING_DAYS: dict[int, int] = {
    2020: 82,   # 2020 Annual Report
    2021: 94,   # 2021 Annual Report
    2022: 106,  # 2022 Annual Report
    2023: 100,  # 2023 Annual Report
    2024: 83,   # 2024 Annual Report
    2025: 82,   # 2025 partial period
}
```
The alternative — correctly extracting the total sitting-days denominator from the source PDFs
— would require non-trivial pipeline work. The values are immutable historical facts. Hardcoding
is the right call.

**NOT permitted — hardcoding is still forbidden when:**
- The value changes over time (threshold, weight, active-year cutoff, party list, etc.)
- The value is approximate or estimated
- The pipeline could reasonably expose it without major rework
- The value encodes model logic (scoring, ranking formula, flag definition)
- The value is mutable config that belongs in a settings file

---

## Duplicate rows from source data

If a view produces duplicate rows for the same key (e.g. same member + same date appearing
twice), this is a pipeline deduplication problem, not a UI display problem.

**Do not deduplicate in Streamlit.** Flag it:

```text
TODO_PIPELINE_VIEW_REQUIRED: deduplication or session_type column on <view_name>.
Source CSV contains multiple row types (e.g. sitting-day + committee-day) for the same date
with no distinguishing column exposed by the view.
```

Add a `#` row-number column to the displayed table as a temporary measure so rows are
at minimum distinguishable to the user:

```python
tl_table["#"] = range(1, len(tl_table) + 1)
```

**Why:** `v_attendance_timeline` joins `aggregated_td_tables.csv` which has one row per
session type per date. The view dropped the session-type column, producing identical-looking
duplicate rows. The UI had no way to label them correctly.
