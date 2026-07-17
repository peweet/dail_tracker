---
tier: CONTEXT
status: LIVE
domain: local-gov
updated: 2026-06-20
supersedes: []
read_when: when working on local-authority accountability data — CE roster, NOAC collection rates, planning overturn rate, or derelict sites levy
key: CONTEXT|LIVE|local-gov
---

# Local Authority Accountability — the unelected executive layer

**Premise.** In Irish local government most power is held not by elected councillors
but by the appointed **Chief Executive** (the former county/city manager; in
Limerick, post the 2024 directly-elected-mayor reform, the **Director General**).
By law councillors hold only a short list of *reserved functions* (adopt the
budget/development plan, borrow, appoint the CE); **everything else is an
*executive function*** performed by the CE — staff, contracts, planning
permissions, day-to-day spend, housing allocation. The CE is appointed by the
Public Appointments Service / Minister for a 7-year term (extendable by 3), paid
€132,511–€189,301, and faces far less public scrutiny than elected members.
Legal basis: Local Government Act 2001 Part 14, as substituted by the Local
Government Reform Act 2014.

Goal: name that office per council, and pair it with *published, attributable*
performance signals so a citizen can see who runs their county and how it performs.

## What is BUILT (2026-06-20)

### 1. Chief Executive roster — DONE, verified
- `data/_meta/la_chief_executives.csv` — 31 councils, hand-curated, git-tracked
  (the `!data/_meta/*.csv` negation keeps it from the blanket `*.csv` ignore).
  Each name verified against an authoritative page (council site preferred);
  `source_url` carries it. Salary deliberately blank (not published per-council;
  the band is national). `term_end` blank unless the council stated it.
  Limerick row = `head_title` "Director General", `chief_executive` "Pat Daly".
- View `v_la_chief_executives` (`sql_views/constituency/constituency_la_chief_executives.sql`),
  registered in `CONSTITUENCY_FILES`. Join key `local_authority` matches
  `constituency_la_crosswalk.csv` / `la_afs_divisions.council` EXACTLY.
- Tripwire `test/sql_views/test_la_chief_executives.py` (6 tests): 31 distinct
  councils, no blank name/source, 3 City + 2 City&County + 26 County, all join
  the crosswalk, view builds.

### 2. NOAC collection-rate accountability layer — DONE
- `v_la_collection_rates` (sql_views/constituency/constituency_la_collection_rates.sql)
  over **gold `data/gold/parquet/noac_m2_collection_wide.parquet`** — the FULL per-LA
  M2 grid (31 LAs × 2020–2024; commercial rates, rent & annuities, housing loans),
  extracted via Camelot by `extractors/noac_collection_rates_extract.py` (PROMOTED
  2026-06-20: save_parquet, writes by default, fidelity-gated; in pipeline.py). View
  exposes 2024 rates + national medians (window). Joins CE roster 0 orphans.
- ⚠️ COURSE-CORRECTION: an earlier sandbox extractor of mine
  (`noac_collection_rates_extract.py`, best/worst-from-text) was REDUNDANT — the full
  per-LA table already existed in gold — and was **deleted**. Lesson:
  [[feedback_check_existing_data_before_pulling]] (grep data/gold for noac_*_wide).
- Framing is firewall-safe: published values beside the national benchmark, no
  composite score, no editorial label (the user's choice).

### 3. Planning overturn rate — DONE
- `v_la_planning_overturn` (sql_views/constituency/constituency_la_planning_overturn.sql)
  over silver `planning_appeal_outcomes.parquet`. Per-council ABP overturn rate vs
  26.4% national benchmark. ⚠️ Cork County absent from source (30/31). Test
  test/sql_views/test_la_planning_overturn.py (skips in CI).

### 4. Derelict Sites Levy — DONE (the sharpest enforcement signal)
- `extractors/derelict_sites_levy_extract.py` (PROMOTED 2026-06-20 from sandbox:
  save_parquet, writes by default, fidelity-gated, --download, in pipeline.py)
  parses the DHLGH 2024 annual-return XLSX (gov.ie, CC-BY; cached at
  doc/source_pdfs/2024_Derelict_Sites_Statistics.xlsx) →
  `data/gold/parquet/derelict_sites_levy_wide.parquet`. Fidelity GREEN: 31 LAs,
  per-LA sums reconcile to the file Total row. National: €8.59m levied, **€26.29m
  uncollected**; **9 councils levied nothing**.
- `v_la_derelict_sites_levy` (sql_views/constituency/constituency_la_derelict_sites_levy.sql):
  per-council levied/received/outstanding + `levied_nothing` flag + collection_rate
  (NULL where nothing levied; can exceed 100% via prior-year arrears) + national
  window totals. Maps "Limerick/Waterford City and County" → bare join key. Joins CE
  roster 0 orphans. Test test/sql_views/test_la_derelict_sites_levy.py (skips in CI).
- PROMOTED & WATCHED: extractor in extractors/ + pipeline.py; freshness watch
  `derelict_sites_levy` (tools/check_freshness.py, 400-day staleness) flags when the
  cached return ages out. Still 2024-only — re-fetch with --download when DHLGH
  publishes the next annual return (the watch will go amber to prompt it).

## NOAC extraction — what works, what doesn't
NOAC is **PDF-only** (no CSV/Excel/dashboard). BUT the report contains a mix of real
data **tables** and chart figures, and **Camelot DOES extract the tables**: the M2
collection grid and the H-series housing indicators are already pulled to full per-LA
gold parquets (`data/gold/parquet/noac_m2_collection_wide.parquet`,
`noac_h{1,2,3,4,6,7}_*_wide.parquet`) by the experimental extractors in
pipeline_sandbox/housing/ (noac_camelot_extract / noac_collection_wide_extract /
noac_housing_wide_extract). So per-LA NOAC data for those indicators IS available —
**check data/gold for `noac_*_wide` before assuming a number is unextractable.**
The chart-figure limitation only bites for indicators NOT yet run through Camelot;
the text layer still gives national averages + named best/worst as a fallback. Do
not OCR bar charts (local OCR banned on this box).

## Roadmap — more accountability signals (published-indicators framing)
Ordered by cleanliness of source:
1. ~~**Planning overturn rate**~~ — DONE (see §3 above).
2. ~~**Derelict Sites Levy charged vs collected**~~ — DONE (see §4 above). NOTE
   still: the **Vacant Homes Tax is Revenue-administered, not councils** — do not
   use it to judge a council. The remaining council-administered one is the **Vacant
   Sites Levy** (URHA 2015), a possible future add.
3. **AFS finance** — `la_afs_divisions` / capital: revenue & capital spend per
   council, already built; surface + CE join.
4. **More NOAC text highlights** — extend the extractor to other indicators where
   the text names best/worst (housing voids/re-let, planning enforcement, etc.).
5. **Local Government Audit Service** — per-council audit findings (PDF set).

## Page — "Who runs your county" — BUILT
`utility/pages_code/local_government.py` (`local_government_page`), nav under **Your
Area** → url_path `local-government`. Stack: page → `data_access/local_government_data.py`
(reuses the cached `get_constituency_conn`) → `dail_tracker_core/queries/local_government.py`
→ the registered `v_la_*` views. DISPLAY ONLY (no JOIN/GROUP/derivation on the page).
- INDEX: hero + national headline strip (`v_la_accountability_summary`: €26.29m
  uncollected, 9/31 levied €0, 26.4% overturn) + searchable card grid of 31 councils
  (CE named, "Appointed, not elected") → `?la=` soft-rerun.
- DOSSIER: CE hero (name, title, "appointed by PAS — not elected", salary band, source
  ↗) → reserved-vs-executive explainer (two cards) → FOUR performance cards: **Money
  collected** (NOAC M2), **Social housing management** (NOAC H-series via
  v_la_housing_performance — vacancy / re-let weeks / upkeep €-per-home / retrofit % /
  long-term-homeless %), **Derelict sites**, **Planning decisions**. Clean `lg-*`
  stat-row layout (big value · label · right-aligned "national X ▲/▼"; arrow tinted by
  direction-of-good, neutral where ambiguous like upkeep). Cork County omits the
  planning card (source gap).
- CSS: custom `lg-*` classes in shared_css.py REPLACED the earlier con-grain chips,
  which squished/overflowed multi-word stats (verified live via Playwright screenshot).
Tests: test/sql_views/ + test/extractors/ + query layer (~55 council tests, all green).
FOLLOW-UP: council choropleth (no LA-boundary polygon yet); optional "Council money"
scale card (v_procurement_council_summary already available); promote NOAC H-series
extractors out of pipeline_sandbox.
