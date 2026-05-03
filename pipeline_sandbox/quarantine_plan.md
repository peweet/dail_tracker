# Quarantine flow — planning doc

Standalone planning doc for graduating per-source quarantine tables from
sandbox into the main pipeline. Cross-references
[doc/dail_tracker_improvements_v4.md](../doc/dail_tracker_improvements_v4.md)
§5.5 (pipeline terms) and §7.4 (data-modelling terms), and prototypes the
shared writer in [quarantine.py](quarantine.py).

The status quo: only `payments.py` quarantines bad rows. Every other silver
writer (`attendance.py`, `member_interests.py`, `lobby_processing.py`,
`legislation.py`) drops rows silently via `dropna` / `filter(...)` / regex
guards. This makes pipeline debugging effectively impossible — a row count
shrinking from one run to the next is invisible until it's a wrong number on
a page.

No code changes pending in main pipeline. Read once, then plan a sprint
against the "Recommended first slice" section at the bottom.

---

## 1. What a quarantine row is

A quarantine row is the **original row that failed validation, preserved
verbatim, with metadata appended**. The metadata explains why it was
rejected and ties it back to the run that rejected it.

The four metadata columns the writer adds:

| Column | Type | Example |
|---|---|---|
| `_quarantine_rule` | str | `"taa_band_unrecognised"` |
| `_quarantine_reason` | str | `"Value '2/MIN' not in clean band set"` |
| `_run_id` | str | `"2026-04-30T08:14:11+00:00-a3f9c812"` |
| `_quarantined_at` | str (ISO) | `"2026-04-30T08:14:13+00:00"` |

The original row's columns are preserved as-is. Schemas across sources do
not need to align — each `_quarantine` table is per-source.

This is intentional. A common cross-source quarantine schema sounds tidy
but forces a JSON-column compromise that makes the quarantine table harder
to read than the source table. Per-source files stay readable by the same
SQL view that reads the silver table they came from.

---

## 2. File layout — the main open question

Two viable options:

**Option A — per-run files** (what §5.5 of the roadmap proposes):

```
data/silver/_quarantine/
  payments_2026-04-30T08-14-11.parquet
  payments_2026-04-29T08-14-11.parquet
  attendance_2026-04-30T08-14-11.parquet
  ...
```

Easy to diff between runs. Trivially deletable. No append concurrency
problems. **Cost:** the SQL view needs a glob read with a `_run_id` filter,
and dirs grow forever without a retention sweep.

**Option B — single file per source, append-only:**

```
data/silver/_quarantine/
  payments.parquet
  attendance.parquet
  ...
```

One file, one read, one view. **Cost:** debugging "what changed last run"
needs a `WHERE _run_id = ?` filter and the file grows linearly with no
natural deletion point.

**Recommendation:** Option A. The whole point of this work is to make
debugging easier. Option A makes "diff this run vs the last one a human
trusted" a one-liner; Option B requires a metadata join. Add a 30-day
retention sweep at the same time the writer ships so growth doesn't
become a problem worth designing around.

This decision is reversible — the writer signature is the same either way.

---

## 3. Rollout order

Driven by debugging value, not by alphabetical or any sense of fairness:

1. **`attendance.py`** — currently the worst offender. The
   `IRISH_NAME_REGEX` filter at [attendance.py:39](../attendance.py#L39)
   silently discards every line that doesn't match. We have no idea how
   many TD names are being lost this way per refresh.
2. **`member_interests.py`** — PDF parser failures get silently dropped
   inside `combine_years`. SIPO data is high-trust but the parser is brittle.
3. **`lobby_processing.py`** — the collective-DPO filter
   ([lobby_processing.py:295-296](../lobby_processing.py#L295-L296)) and
   `drop_nulls` calls on `returns_master` joins. Lobbying is the most
   analytically valuable source per v4 §0, so silent drops there have the
   highest blast radius.
4. **`legislation.py`** — flatten failures.
5. **`payments.py`** — already quarantines, just migrate from the bespoke
   `quarantined_payment_tables.parquet` layout onto the shared writer last,
   once the API has stabilised on 3-4 other sources.

---

## 4. SQL view layer

One registered view per source under [sql_views/](../sql_views/):

```sql
-- sql_views/payments_quarantine.sql
CREATE OR REPLACE VIEW payments_quarantine AS
SELECT
  *,
  _quarantine_rule,
  _quarantine_reason,
  _run_id
FROM read_parquet('data/silver/_quarantine/payments_*.parquet');
```

Joinable with the existing manifest table to filter "this run" or "last
known good run".

---

## 5. UI surface

Add a single helper to `utility/components.py`:

```python
def data_quality_expander(source: str, view_name: str | None = None) -> None:
    """Render an expandable 'data quality issues' panel below a page."""
    # SELECT count(*), _quarantine_rule, sample(...) FROM <source>_quarantine
    # render row count, top-5 rules, 10-row sample with reason
```

Drop into every page in one line — replaces the hand-written
`_QUARANTINE_NOTE` on the payments page. Per v4 §9.6 ("Empty states that
explain themselves"), an "all-data-quarantined" branch should also live in
this helper.

---

## 6. Out of scope for this slice

- Nightly summary that opens a GitHub issue when quarantine count exceeds
  N (v4 §5.5). Needs scheduled refresh + CI first (v4 §6.1).
- Schema-drift quarantine. New columns from upstream don't go into
  quarantine — they go into a separate "schema diff" log per v4 §6.5.
- Cross-source aggregate quarantine view. Not useful until 3+ sources
  participate.

---

## 7. Why this shape — validating practices

The pattern is not novel; it shows up under several names in mature data
platforms. The relevant references:

- **Medallion architecture (bronze/silver/gold).** Quarantine is the
  formal name for the rejected-rows tributary that feeds *out* of silver
  rather than into gold. Databricks' canonical writeup:
  https://www.databricks.com/glossary/medallion-architecture
- **Write-Audit-Publish (WAP) pattern.** Common in Apache Iceberg /
  lakehouse setups: write to a staging branch, audit (which is where
  quarantine lives), publish only the clean partition. Search "Iceberg
  Write-Audit-Publish" — the pattern predates any one blog post.
- **dbt data tests.** dbt's `test` resources quarantine failing rows into
  a `<test>_failures` table at run time — directly analogous to what we're
  building, just expressed in SQL instead of Polars. Reference:
  https://docs.getdbt.com/docs/build/data-tests
- **Great Expectations.** Same idea framed as "expectations + invalid-row
  capture". Reference: https://greatexpectations.io/
- **Dead-letter queues** from streaming systems (Kafka, SQS). The
  philosophical ancestor: failed messages go to a DLQ rather than being
  dropped. The argument for quarantine over silent drop is the same
  argument as the argument for DLQs over silent message loss.

The thread tying these together: **rejection without a paper trail is a
bug, not a feature.** Every one of these patterns exists because a team
got burned by a silent-drop pipeline and wrote up what they wished they
had built first. We have the chance to build it before getting burned a
second time (the first time was payments).

---

## 8. Recommended first slice

One PR, three files:

1. **Promote `pipeline_sandbox/quarantine.py` → `services/quarantine.py`.**
   No logic change. Just moves it out of sandbox once the API is endorsed.
2. **Wire `attendance.py` to call it** at the regex-filter step. Every
   line that fails `IRISH_NAME_REGEX` becomes a quarantine row with
   `_quarantine_rule="name_regex_failed"`.
3. **Add `sql_views/attendance_quarantine.sql`** and a manual smoke read
   from a page (no UI helper yet — that's slice 2).

Slice 1 ships zero UI. Slice 2 adds the `data_quality_expander` helper
and instruments member_interests + lobby_processing. Slice 3 migrates
payments off the bespoke layout.

The whole sequence is roughly a week of focused work, not a quarter.
