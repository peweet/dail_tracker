# Benchmark — oireachtas-explorer.ie bill page

Status: feature-gap notes, not yet planned.
Authored: 2026-05-06.
Reference example: https://oireachtas-explorer.ie/#/dail/34/bill/2026/39

A third-party explorer site has stronger bill-page features than our current `legislation.py`. This doc captures the gaps so we can decide what's worth adopting and where the pipeline needs new views.

## Where Dáil Tracker stands today

Page: `utility/pages_code/legislation.py`
Contract: `dail_tracker_bold_ui_contract_pack_v5/utility/page_contracts/legislation.yaml`
Backing views:
- `v_legislation_index` — one row per bill, primary sponsor only
- `v_legislation_detail` — one row per bill, primary sponsor only
- `v_legislation_timeline` — one row per stage event (no documents)
- `v_legislation_sources` — URL columns

## Gap 1 — Sponsors list is incomplete

**What the explorer shows:** all sponsors and co-sponsors of a bill, listed together.

**What we show:** primary sponsor only.

**Why:** the pipeline parquet (`data/silver/parquet/sponsors.parquet`) has one row per (bill, sponsor). Our index/detail views deduplicate to one row per bill, keeping only `sponsor_is_primary = true`:

```sql
-- sql_views/legislation_index.sql, lines 19-22
ROW_NUMBER() OVER (
    PARTITION BY bill_year, bill_no
    ORDER BY CASE WHEN sponsor_is_primary = true THEN 0 ELSE 1 END
) AS rn
...
WHERE rn = 1
```

The data is already there; the view throws it away.

**Fix (pipeline work):**

```text
TODO_PIPELINE_VIEW_REQUIRED: v_legislation_sponsors — all sponsor rows per bill_id.
Columns: bill_id, sponsor_name, unique_member_code, sponsor_is_primary,
sponsor_role (e.g. "Sponsor", "Co-sponsor"), party (if available).
Filtered in Streamlit by WHERE bill_id = ?.
```

**Streamlit consumer (not yet built):**
- In Stage 2 detail panel, replace the single "Sponsor: <name>" line with a sponsors block.
- Render as a list of name pills, primary first.
- Each pill links to `member_overview` via `?member=<unique_member_code>` (existing pattern, see `member_overview.yaml` `entry_points.url_query_param`).

## Gap 2 — Timeline is shallower

**What the explorer shows** (from the user's example):

```
2 Apr 2026   First Stage              34th Dáil
23 Apr 2026  Second Stage             34th Dáil · Current
23 Apr 2026  As Initiated             Bill version published
23 Apr 2026  Explanatory Memorandum   Related document published
```

Two distinct event types are interleaved on a single timeline:
1. **Stage events** (First Stage, Second Stage, …) — with chamber/Dáil term and a "Current" marker.
2. **Document events** (As Initiated, Explanatory Memorandum, As Passed by Dáil, etc.) — each one a distinct row showing when an artefact was published.

**What we show:** stage events only.

```sql
-- sql_views/legislation_timeline.sql
SELECT
    bill_id,
    stage_name,
    stage_date,
    stage_number,
    is_current_stage,
    chamber,        -- e.g. "Dáil Éireann" / "Seanad Éireann"
    bill_no, bill_year
FROM read_parquet('data/silver/parquet/stages.parquet')
```

We have stage events with chamber, but no Dáil term and no document events.

**Fixes (pipeline work, two distinct asks):**

```text
TODO_PIPELINE_VIEW_REQUIRED: dail_term column on v_legislation_timeline.
Source field likely on stages.parquet — confirm during view edit.
Display as "34th Dáil" alongside chamber.
```

```text
TODO_PIPELINE_VIEW_REQUIRED: v_legislation_documents — one row per published bill artefact.
Columns: bill_id, document_type ("As Initiated", "Explanatory Memorandum",
"As Passed by Dáil", …), publish_date, document_url.
Source: bills.parquet or a related parquet — confirm against bronze schema.
```

**Streamlit consumer (not yet built):**
- Merge stage events and document events into a single chronological timeline in the page (UNION-equivalent done by *displaying* both lists side by side; not a SQL JOIN — that's pipeline territory).
- Two visual treatments: stage rows have a chamber/term badge and "Current" marker; document rows have a document-type badge and a download/view link.
- Existing `_shared_ui_policy.yaml` permits this under `secondary_view_flow.timeline_or_calendar_chart`.

**Visual reference (the explorer's pattern):**

```
●  2 Apr 2026   First Stage              34th Dáil
│
●  23 Apr 2026  Second Stage             34th Dáil · Current
│
○  23 Apr 2026  As Initiated             Bill version published
│
○  23 Apr 2026  Explanatory Memorandum   Related document published
```

Filled circles = stage events, hollow = document events. Same vertical spine.

## Gap 3 — Cream List

**What the explorer shows:** a "Cream List" reference attached to the current stage of a live bill.

**What we show:** nothing.

**What this most likely is** (moderate confidence — verify before building):

The "Cream List" is the informal Oireachtas name for the **list of bills awaiting Second Stage in Dáil Éireann**, surfaced via the daily Order Paper (*Riar na hOibre*). The name comes from the cream-coloured paper it was historically printed on. A bill being "on the Cream List" means it has passed First Stage but is queued for Second Stage debate — a meaningful accountability signal because some bills sit on the Cream List for years and never progress.

**Verification needed before building:**

- Confirm the term and definition against an Oireachtas primary source (oireachtas.ie business pages, Standing Orders, or the Bills Office).
- Determine whether the Oireachtas API exposes "on cream list" as a field or whether it must be parsed from the daily Order Paper / Riar na hOibre PDF.
- Decide whether this is a *flag* on the bill (boolean: is it currently on the Cream List) or a *list* (the full set of bills currently on the Cream List, which is itself a sharable artefact).

**Pipeline work, contingent on verification:**

```text
TODO_PIPELINE_VIEW_REQUIRED: cream_list_status — boolean flag on v_legislation_index
indicating the bill is currently on the Dáil Cream List (awaiting Second Stage).
Source: TBD — likely the daily Order Paper (Riar na hOibre) at
https://www.oireachtas.ie/en/debates/find/?datePeriod=daily or an Oireachtas API field.
Flag must include a "last verified" timestamp because Cream List membership changes.
```

**Streamlit consumer (not yet built):**
- On the bill detail panel, render a small badge near the current stage: `st.badge("On Cream List", icon=":material/list:", color="blue")` when the flag is true.
- On the legislation index, allow filtering to "Currently on Cream List" via `st.segmented_control`.
- Editorial value is high: a stand-alone view of bills sitting on the Cream List for >12 months would be a strong civic accountability story (the kind theyworkforyou would publish).

## Suggested rollout

If we tackle this, sequence the pipeline work first — Streamlit is cheap once the views exist:

1. **`v_legislation_sponsors`** — easy, data is already in `sponsors.parquet`, just stop deduplicating. Sponsors block in detail panel.
2. **`dail_term` on `v_legislation_timeline`** — small column add, large readability gain.
3. **`v_legislation_documents`** — moderate; needs a source-schema check. Unlocks the dual-event timeline.
4. **Cream List flag** — research first. Possibly a sandbox parser if no API field exists. Highest editorial payoff but highest unknown.

## Relationship to existing planning

- `doc/dail_tracker_improvements_v4.md` is the broader improvements list — once these gaps are validated, fold them in there as concrete tickets.
- Both fixes 1–3 are pure SQL view edits in `sql_views/` — they qualify as "safe to create directly" per the contract pack's `CLAUDE.md` pipeline safety rule. No `pipeline.py` / `enrich.py` edits required.
- Cream List likely needs `pipeline_sandbox/` work (Order Paper parser) before a SQL view is possible.

## Open questions

- Does the Oireachtas API expose Cream List membership directly, or is it Order Paper scraping only?
- For document events (As Initiated, Explanatory Memorandum), is the source `bills.parquet` or a separate `documents.parquet`? Bronze schema check needed.
- Should the dual-event timeline merge happen in a SQL `UNION ALL` view (`v_legislation_timeline_merged`) or stay as two queries the page renders together? Probably the former — keeps the page thin.
