# Integration Guide

## 1. Copy the pack into your repo

From PowerShell:

```powershell
cd C:\Users\pglyn\PycharmProjects\dail_extractor
Expand-Archive -Path C:\path\to\dail_tracker_bold_ui_contract_pack_v5.zip -DestinationPath . -Force
```

Or copy these folders/files manually into the repo root:

```text
CLAUDE.md
AGENT_SETUP.md
page_runbooks/
prompts/
.claude/
docs/
utility/page_contracts/
utility/ui/
utility/data_access/
tools/
```

## 2. Start Claude Code from the repo root

```powershell
cd C:\Users\pglyn\PycharmProjects\dail_extractor
claude
```

## 3. First Claude prompt

Paste this:

```text
Read `CLAUDE.md` only. Do not inspect the whole repository yet.

Confirm the Dáil Tracker operating model in 6 bullets:
1. how data is exposed to Streamlit
2. what Streamlit may do
3. what Streamlit may not do
4. how page contracts work
5. how UI creativity is allowed
6. how you will avoid token waste
```

## 4. Work one page at a time

Recommended order:

```text
attendance
votes
interests
payments
lobbying
member_overview
committees
legislation
home_overview
```

Open the relevant runbook and paste Step 1 only:

```text
page_runbooks/attendance.md
```

Then continue step by step.

## 5. Do not delete old pages first

Use Git instead:

```powershell
git checkout -b ui-redesign-attendance
```

Let Claude modify the existing page only after it has produced a redesign plan.

## 6. Important architecture correction

This pack assumes:

```text
Parquet/gold outputs
→ application or pipeline bootstrap registers analytical SQL views
→ in-process DuckDB connection
→ Streamlit retrieval SQL
```

It does not assume:

```text
Streamlit connects directly to data/gold/dail.duckdb
```

If Claude tries to use a persistent DuckDB file or call `read_parquet()` inside a page, stop it and run the logic firewall review.

## 7. Use the TODO hook for wiring

If a UI needs a missing view, column, metric, source URL, or filter, Claude must write:

```text
TODO_PIPELINE_VIEW_REQUIRED: <specific missing item>
```

Then use:

```text
prompts/16_wire_todo_pipeline_view_required.prompt.md
```

to turn those TODOs into pipeline/view-layer work.
