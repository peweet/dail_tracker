# Dáil Tracker

A civic-data pipeline that ingests, joins, and analyses open data published by the Oireachtas (Irish parliament) and related public bodies. Built to make parliamentary activity — attendance, legislation, questions, payments, declared interests, and lobbying contacts — searchable and cross-referenceable in one place.

**Status:** in active development. Ingestion, flattening, and enrichment are working end-to-end for the 34th Dáil. DuckDB gold layer and Streamlit dashboard in progress.

---

## What's in the dataset

| Source | Format | Coverage | Script |
|---|---|---|---|
| TD members | Oireachtas API `/v1/members` | 34th Dáil (current) | `oireachtas_api_service.py` |
| Legislation | Oireachtas API `/v1/legislation` | Per TD, career-wide | `oireachtas_api_service.py` |
| Parliamentary questions | Oireachtas API `/v1/questions` | Per TD | `oireachtas_api_service.py` |
| Attendance | PDF scrape | Jan 2023 → Feb 2026 | `attendance.py` |
| Parliamentary Standard Allowance | PDF scrape | Jan 2024 → Feb 2026 | `payments.py` |
| Register of Members' Interests | PDF scrape | Latest annual declaration | `member_interest.py` |
| Lobbying register | CSV export from lobbying.ie | Multi-period | `lobbying_processing.py` |

**Scope note:** the dataset covers *sitting* TDs of the 34th Dáil only. Former TDs and private citizens who have since left politics are deliberately excluded — see `DATA_LIMITATIONS.md` for the rationale.

---

## Why this project exists

The data is public but fragmented. The Oireachtas API, lobbying.ie, and the Standards in Public Office Commission each publish high-quality data in isolation. A TD's parliamentary activity, their declared financial interests, and who lobbies them all live in different systems with different identifiers and different update cadences.

This project joins them. Once joined, questions that are currently hard to answer become tractable:

- Which TDs are lobbied most, and by whom?
- Do lobbied TDs ask parliamentary questions on the lobbied topic?
- Do declared financial interests overlap with the sectors lobbying the same TD?
- How does attendance correlate with committee load, ministerial office, or constituency?

---

## Pipeline overview

```
INGESTION (Bronze)
  oireachtas_api_service.py  → members.json, legislation_results.json, questions_results.json
  pdf_enpoint_check.py       → health-checks all PDF + API URLs

TRANSFORMATION (Silver)
  flatten_members_json_to_csv.py  → flattened_members.csv
  attendance.py                   → aggregated_td_tables.csv
  payments.py                     → aggregated_payment_tables.csv
  member_interest.py              → member_interests_grouped.csv
  lobbying_processing.py          → combined_lobbying_data.csv
                                  → lobby_break_down_by_politician.csv
                                  → most_lobbied_politicians.csv
  flatten_service.py              → drop_cols_flattened_bills.csv

ENRICHMENT (Silver → Gold)
  enrich.py                       → enriched_td_attendance.csv
                                     (attendance + members, joined on normalised key)
```

The join key is a sorted-character fuzzy key — NFD-normalise, lowercase, strip accents and apostrophes, then sort characters alphabetically. This matches "Ó Súilleabháin" to "O'Sullivan" across datasets that use different formatting conventions. The logic lives in `normalise_join_key.py`.

---

## Installation

Requires Python 3.12 and `uv` or `pip`.

```bash
git clone https://github.com/peweet/dail_tracker.git
cd dail_tracker
pip install -e .
```

Optional dependency groups defined in `pyproject.toml`:

```bash
pip install -e ".[duckdb]"   # DuckDB for the gold analytical layer
pip install -e ".[ocr]"      # ocrmypdf for scanned PDF fallback
pip install -e ".[dev]"      # pytest, ruff
```

---

## Running the pipeline

There is no single orchestrator script yet (see `REQUIREMENTS.md` for the `pipeline.py` roadmap). Scripts must be run in the order below:

```bash
python oireachtas_api_service.py       # fetch members, bills, questions
python flatten_members_json_to_csv.py  # flatten member JSON
python attendance.py                    # extract attendance PDFs
python payments.py                      # extract payment PDFs
python member_interest.py               # extract member interests PDF
python lobbying_processing.py           # process lobbying CSV exports
python enrich.py                        # join everything
python flatten_service.py               # flatten bills JSON
```

Outputs land in `members/`, `bills/`, and `lobbyist/`. These directories are gitignored — the pipeline regenerates them.

---

## Repository layout

```
dail_tracker/
├── pyproject.toml                         Dependencies, entry points
├── README.MD                              This file
├── REQUIREMENTS.md                        Roadmap, architecture, action items
├── DATA_LIMITATIONS.md                    Known gaps, scope decisions, silent-failure risks
├── ENGINEERING_REVIEW.md                  Senior-engineer critique + experimental ideas
│
├── oireachtas_api_service.py              API ingestion (members, bills, questions)
├── attendance.py                          Attendance PDF → CSV
├── payments.py                            Payment PDF → CSV
├── member_interest.py                     Member interests PDF → CSV
├── lobbying_processing.py                 Lobbying CSV → cleaned + exploded CSVs
├── flatten_members_json_to_csv.py         Member JSON flattening
├── flatten_service.py                     Bill JSON flattening
├── enrich.py                              Cross-dataset joins
├── normalise_join_key.py                  Fuzzy name normalisation
├── pdf_enpoint_check.py                   URL health checker
├── tear_down.py                           Cleanup intermediary files
├── read_scan_pdf.py                       OCR for scanned PDFs
│
├── utility/
│   ├── constants.py                       PDF + API URLs
│   └── select_drop_rename_cols_mappings.py Column configuration
│
├── doc/config.py                          Centralised paths (not yet wired in — see REQUIREMENTS.md)
│
├── test/                                  pytest (skeletal — see REQUIREMENTS.md)
└── lobbyist/raw/                          Manual CSV downloads from lobbying.ie
```

---

## Data sources

- **Oireachtas Open Data API** — `https://api.oireachtas.ie/` — members, legislation, questions, divisions, debates.
- **data.oireachtas.ie** — PDFs for attendance, parliamentary standard allowance, and the Register of Members' Interests.
- **lobbying.ie** — manual CSV exports from the Register of Lobbying.

All upstream data is published under open licences by the respective bodies. See each source's terms for specifics.

---

## Contributing

Fork and open a PR. The project is a work in progress — both pipeline correctness and analytical questions are open for collaboration. Issues for known bugs and desired features are tracked in `REQUIREMENTS.md`.

If you spot data you believe is wrong, please open an issue with the source, the expected value, and the observed value. PDF-extracted data is inherently brittle and every confirmed miss helps.

---

## A note on the data

Every best effort has been made to produce a high-quality dataset, but the pipeline is only as good as its sources. PDFs are scraped with heuristics that can fail on unusual formatting. APIs are called with filters that can silently drop edge cases. Name-based joins rely on a fuzzy key that can collide on anagrams or fail on minor spelling variants.

`DATA_LIMITATIONS.md` documents the known gaps honestly. Please treat numbers as starting points for investigation, not final answers.