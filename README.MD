# Dáil Tracker

A civic-data pipeline that ingests, joins, and analyses open data published by the Oireachtas (Irish parliament) and related public bodies. Built to make parliamentary activity — attendance, legislation, questions, payments, declared interests, and lobbying contacts — searchable and cross-referenceable in one place.

Only current Dail and Seanad are considered see `DATA_LIMITATIONS.md` for the rationale

**Status:** in active development. Ingestion, flattening, and enrichment are working end-to-end for the 34th Dáil. DuckDB gold layer and Streamlit dashboard in progress.

## Why this project exists

The data is public but fragmented. The Oireachtas API, lobbying.ie, and the Standards in Public Office Commission each publish high-quality data in isolation. A TD's parliamentary activity, their declared financial interests, and who lobbies them all live in different systems with different identifiers and different update cadences.

This project joins them. Once joined, questions that are currently hard to answer become tractable:

- Which TDs are lobbied most, and by whom?
- Do lobbied TDs ask parliamentary questions on the lobbied topic?
- What and how much are declared financial interests for politicians?

## Data sources

- **Oireachtas Open Data API** — `https://api.oireachtas.ie/` — members, legislation, questions, divisions, debates.
- **data.oireachtas.ie** — PDFs for attendance, parliamentary standard allowance, and the Register of Members' Interests.
- **lobbying.ie** — manual CSV exports from the Register of Lobbying.

All upstream data is published under open licences by the respective bodies. See each source's terms for specifics.
The pipeline uses three prinicpal sources.

1.  Official Dail Oireachtas API (no API key needed)
https://data.oireachtas.ie/

2. PDF official declarations. Many Oireachtas functions are published via PDF and not by API. PyMudf was employed to pull figures and text from the PDF's and transform it into tables
Member Interets: https://www.oireachtas.ie/en/members/register-of-members-interests/

Record of Attendance: https://www.oireachtas.ie/en/publications/?q=&topic%5B%5D=record-of-attendance
Example for a single TD:
https://data.oireachtas.ie/ie/oireachtas/members/recordAttendanceForTaa/2026/2026-01-16_deputies-verification-of-attendance-for-the-payment-of-taa-01-january-2025-to-30-november-2025_en.pdf

3. Lobbying.ie Returns register
https://www.lobbying.ie/app/home/search



## What's in the dataset

| Source | Format | Coverage | Script |
|---|---|---|---|
| TD members | Oireachtas API `/v1/members` | 34th Dáil (current) | `oireachtas_api_service.py` |
This serves as the authoritative list of TDs.
To maintain simplitiy for the pipeline for now only the 34th Dail members are considered. They are typicall 173 TD's (minus the Ceann Comhairle)

| Legislation | Oireachtas API `/v1/legislation` | Per TD, career-wide | `oireachtas_api_service.py` |
| Parliamentary questions | Oireachtas API `/v1/questions` | Per TD | `oireachtas_api_service.py` |
| Attendance | PDF scrape | Jan 2023 → Feb 2026 | `attendance.py` |
| Parliamentary Standard Allowance | PDF scrape | Jan 2024 → Feb 2026 | `payments.py` |
| Register of Members' Interests | PDF scrape | Latest annual declaration | `member_interest.py` |
| Lobbying register | CSV export from lobbying.ie | Multi-period | `lobbying_processing.py` |

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
There is a lot of inconsistencies in how names are recorded and this was an attempt to give join logic only in the early raw layer to join the most basic tables and get the unique members code. 
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

There is a single `pipeline.py` orchestration scriot. The script will order the following functions in strict order.

```function order
"pdf_endpoint_check.py"        
"pdf_downloader.py"            
"dummy_value"                  
"flatten_members_json_to_csv.py
"payments.py"                  
"attendance.py"                
"lobby_processing.py"          
"legislation.py"               
"member_interests.py"          
"flatten_service.py"           
"enrich.py"                    
"transform_votes.py"           s
"pdf_endpoint_check.py"        
"pdf_downloader.py"            
"dummy_value"                  
"flatten_members_json_to_csv.py
"payments.py"                  
"attendance.py"                
"lobby_processing.py"          
"legislation.py"               
"member_interests.py"          
"flatten_service.py"           
"enrich.py"                    
"transform_votes.py"           
"pdf_endpoint_check.py"        
"pdf_downloader.py"            
"dummy_value"                  
"flatten_members_json_to_csv.py
"payments.py"                  
"attendance.py"                
"lobby_processing.py"          
"legislation.py"               
"member_interests.py"          
"flatten_service.py"           
"enrich.py"                    
"transform_votes.py"           
"pdf_endpoint_check.py"        
"pdf_downloader.py"            
"dummy_value"                  
"flatten_members_json_to_csv.py
"payments.py"                  
"attendance.py"                
"lobby_processing.py"          
"legislation.py"               
"member_interests.py"          
"flatten_service.py"           
"enrich.py"                    
"transform_votes.py"           
"pdf_endpoint_check.py"        
"pdf_downloader.py"            
"dummy_value"                  
"flatten_members_json_to_csv.py
"payments.py"                  
"attendance.py"                
"lobby_processing.py"          
"legislation.py"               
"member_interests.py"          
"flatten_service.py"           
"enrich.py"                    
"transform_votes.py"           
"pdf_endpoint_check.py"        
"pdf_downloader.py"            
"dummy_value"                  
"flatten_members_json_to_csv.py
"payments.py"                  
"attendance.py"                
"lobby_processing.py"          
"legislation.py"               
"member_interests.py"          
"flatten_service.py"           
"enrich.py"                    
"transform_votes.py"           
"pdf_endpoint_check.py"        
"pdf_downloader.py"            
"dummy_value"                  
"flatten_members_json_to_csv.py
"payments.py"                  
"attendance.py"                
"lobby_processing.py"          
"legislation.py"               
"member_interests.py"          
"flatten_service.py"           
"enrich.py"                    
"transform_votes.py"           
"pdf_endpoint_check.py"        
"pdf_downloader.py"            
"dummy_value"                  
"flatten_members_json_to_csv.py
"payments.py"                  
"attendance.py"                
"lobby_processing.py"          
"legislation.py"               
"member_interests.py"          
"flatten_service.py"           
"enrich.py"                    
"transform_votes.py"           
```

Outputs land in `bronze/`, `silver/`, and `gold/` directories respectively. These directories are gitkept — the pipeline regenerates them. There are intermediate datasets that are discarded at cetain points as they dont serve any purpose.

---

## Repository layout
TO BE UPDATED
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

---

## Contributing

Fork and open a PR. The project is a work in progress — both pipeline correctness and analytical questions are open for collaboration. Issues for known bugs and desired features are tracked in `REQUIREMENTS.md`.

If you spot data you believe is wrong, please open an issue with the source, the expected value, and the observed value. PDF-extracted data is inherently brittle and every confirmed miss helps.

---

## A note on the data

Every best effort has been made to produce a high-quality dataset, but the pipeline is only as good as its sources. PDFs are scraped with heuristics that can fail on unusual formatting. APIs are called with filters that can silently drop edge cases. Name-based joins rely on a fuzzy key that can collide on anagrams or fail on minor spelling variants.


---

## Streamlit Dashboard & Data Interpretation

The project includes a multi-page Streamlit dashboard (see `utility/pages_code/`) for exploring the joined datasets:

- **Interests Register:** Search and browse all TDs' annual declarations of financial interests. The landing page provides clear guidance on how to interpret the data, including:
  - Who is required to declare (and who is not)
  - Limitations around office holders, household interests, and data completeness
  - A direct link to the official Dáil rules for transparency ([see here, PDF, pg. 5](https://data.oireachtas.ie/ie/oireachtas/committee/dail/34/committee_on_members_interests_of_dail_eireann/termsOfReference/2025/2025-12-18_guidelines-for-members-of-dail-eireann-who-are-not-office-holders-concerning-the-steps-to-be-taken-by-them-to-ensure-compliance-with-the-provisions-of-the-ethics-in-public-office-acts-1995-and-2001_en.pdf))
  - Context to help users avoid misinterpretation (e.g., more declared interests often means more transparency, not wrongdoing)
- **Committees:** Explore committee assignments and cross-reference with other datasets.
- **Lobbying:** Drill into lobbying returns, see which TDs are most lobbied, and view details by client/company.

**Performance profiling** is built in for dashboard pages, allowing you to diagnose slow loads and optimize the user experience.

**Trust and transparency:** The dashboard surfaces data limitations and interpretation guidance directly to users, helping journalists and citizens understand what the data does (and does not) show.

---