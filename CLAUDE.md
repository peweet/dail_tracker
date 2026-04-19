# Dáil Tracker — Claude Context

## What this project is

A civic transparency tool that joins fragmented Irish parliamentary data into a single queryable dataset. It scrapes the Oireachtas API and PDFs, cleans and joins them, and surfaces the results via a Streamlit app.

The ambition: make it easy to ask cross-dataset questions — who is lobbying a TD, what interests have they declared, and how often do they show up?

---

## Pipeline

Run with `python pipeline.py`. Steps execute sequentially:

| Script | What it does | Output |
|---|---|---|
| `oireachtas_api_service.py` | Fetches members, legislation, questions from Oireachtas API | `members/members.json`, `bills/` |
| `flatten_members_json_to_csv.py` | Flattens members JSON | `data/silver/flattened_members.csv` |
| `attendance.py` | PyMuPDF scrapes attendance PDFs | `data/silver/aggregated_td_tables.csv` |
| `payments.py` | PyMuPDF scrapes parliamentary standard allowance PDFs | `data/silver/aggregated_payment_tables.csv` |
| `member_interest.py` | PyMuPDF scrapes Register of Members' Interests PDFs | `data/silver/dail_member_interests_*.csv` |
| `lobbying_processing.py` | Cleans lobbying.ie CSV exports | `data/silver/combined_lobbying_data.csv` |
| `enrich.py` | Joins attendance + members | `data/silver/enriched_td_attendance.csv` |
| `flatten_service.py` | Flattens bills JSON | `bills/drop_cols_flattened_bills.csv` |

The join key (`normalise_join_key.py`) is a sorted-character fuzzy key: NFD-normalise → lowercase → strip accents and apostrophes → sort characters. This handles Irish name variants (Ó Broin / O'Brien) across datasets that share no common identifier.

---

## Data layer

```
data/
  bronze/   # raw API responses and downloaded PDFs
  silver/   # cleaned, flattened CSVs (one file per dataset)
  gold/     # lobbyist.duckdb (analytical layer, partially built)
```

Current silver outputs that are live and working:
- `aggregated_td_tables.csv` — TD attendance (plenary sittings)
- `enriched_td_attendance.csv` — attendance joined with member metadata
- `flattened_members.csv` / `flattened_seanad_members.csv`
- `dail_member_interests_combined.csv` — declared business interests, landlord/property flags, per year
- `seanad_member_interests_combined.csv`
- `committee_assignments.csv`
- `lobbyist.duckdb` — lobbying data in DuckDB

---

## Streamlit app

Entry point: `utility/app.py` — run with `streamlit run utility/app.py`

Pages live in `utility/pages_code/`:
- `interests.py` — Register of Members' Interests (search by name, quick-select notable TDs)
- `committees.py` — committee assignments
- `attendance.py` — TD attendance tracker (date range, rankings, timeline, individual TD views; CSV export)

**File paths are currently hardcoded** to the local dev machine. Before deploying, paths need to be parameterised (see `utility/constants.py` and `doc/config.py` for the planned approach).

---

## Key technical decisions

- **PyMuPDF** (`fitz`) for PDF extraction — faster than camelot, handles page-spanning table records natively.
- **pandas** for JSON flattening and PDF table wrangling. **polars** for joins and larger tabular operations. Conversion between them is intentional and cheap at this scale.
- **DuckDB** for the lobbying gold layer (`data/gold/lobbyist.duckdb`). The rest of the pipeline still outputs CSVs.
- **Streamlit** for the dashboard — cheap to deploy, no JS required, CSV export is a first-class feature.

---

## Known limitations (trust code over docs)

- PDF column slicing (`iloc[:, :5]`) assumes fixed layout — breaks on unusual PDFs.
- No pagination loop — API calls cap at their `limit` param. Silent truncation if a TD has >1000 bills or questions.
- Name-based joins are fragile. Anagram collisions are possible. Minor spelling variants silently drop rows.
- Normalisation logic is duplicated between `normalise_join_key.py` and `payments.py` — divergence will cause join misses.
- The `member_interest.py` data covers multiple Dáil years (2020–2025) as separate CSVs, combined into one file.

---

## Design Context

### Users
Two audiences sharing the same interface:

**General public** — Irish citizens checking their TD's record. Occasional visitors, low data literacy, browsing on mobile or laptop during the day. Want a quick legible answer: attendance rate, declared interests, who is lobbying them.

**Journalists & researchers** — Power users cross-referencing datasets for investigative work. Need filterable tables and CSV export on every view. Will find the source on GitHub and may deploy themselves.

The **Register of Members' Interests** is the most politically potent dataset — publicly declared business dealings, directorships, shareholdings that could signal conflicts of interest. Surface it prominently.

### Brand personality
**Direct. Civic. Accountable.**

Emotional goal: a citizen or journalist should feel informed and empowered, not overwhelmed. Makes absence of action visible.

### Aesthetic direction
Editorial accountability journalism — investigative newspaper crossed with a data reference tool. Strong typographic hierarchy, high contrast, data tables as the hero. Light theme (used during the day, by journalists at desks and citizens on phones).

**Not**: the existing Oireachtas website (grey, bureaucratic), fintech dashboards (gradient accents, glassmorphism), or generic Streamlit defaults.

### Design principles
1. Data is the evidence — tables and numbers are the primary design element.
2. Accessible by default, powerful on demand — simple on load; CSV export and deep-dives available but not in the way.
3. Member interests first — the most politically significant dataset; don't bury it.
4. Every row tells a story — zero values and absences are data too.
5. Streamlit constraints are real — no custom JS; CSS theming via `st.markdown`; favour legibility over animation.
