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

### 2. NOAC collection-rate accountability layer — DONE (scoped)
- `extractors/noac_collection_rates_extract.py` → sandbox
  `pipeline_sandbox/_noac_output/noac_collection_rates.parquet` (+ coverage JSON
  in `data/_meta/`). Parses the three M2 revenue-collection indicators
  (commercial rates, rent & annuities, housing loans) for 2024: national average
  + NOAC's own named best/worst councils. 20 rows; joins CE roster with 0 orphans.
- Framing is firewall-safe: these are **NOAC's published verdicts** (it names the
  top-3/bottom-3 itself), not our composite score. Per the user's choice:
  published indicators only, no invented "delinquency score", no editorial label.

### 3. Planning overturn rate — DONE
- `v_la_planning_overturn` (sql_views/constituency/constituency_la_planning_overturn.sql)
  over silver `planning_appeal_outcomes.parquet`. Per-council ABP overturn rate vs
  26.4% national benchmark. ⚠️ Cork County absent from source (30/31). Test
  test/sql_views/test_la_planning_overturn.py (skips in CI).

### 4. Derelict Sites Levy — DONE (the sharpest enforcement signal)
- Source already existed: `pipeline_sandbox/housing/derelict_sites_levy_extract_experimental.py`
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
- FOLLOW-UP (promotion checkpoint): the extractor is still `_experimental`, writes
  via bare `write_parquet` (not `save_parquet`), and is 2024-only. Promote = move to
  extractors/, route through services.parquet_io, add to pipeline, re-fetch each Q2.

## KEY CONSTRAINT — NOAC ships chart images, not data
The NOAC Performance Indicator Report (46 indicators, 11 areas, annual since 2014)
is **PDF-only** — no CSV/Excel/dashboard exists. The full 31-council × 5-year grids
are drawn as **chart figures**; the text layer holds only the national average and
NOAC's named highest/lowest councils, plus occasional small exception tables. So a
complete per-LA/per-indicator parquet is NOT reliably extractable from this source.
**Do not** attempt to OCR the bar charts (unreliable; local OCR is also banned on
this box). To get full granular rankings we would need NOAC's underlying data
(request via info@noac.ie) or a different source.

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

## Page plan — "Who runs your county" (NOT yet built)
Per council: the **named CE + title + salary band + term** (the unelected office),
a short **reserved-vs-executive explainer** (the educational payload), then a
**published-indicator panel** beneath — collection rates with the national average
as the benchmark line, planning overturn rate, council spend — each attributed to
its source. Wire via the existing constituency/council connection. Follow the
project's card patterns (no `st.dataframe` on the primary view) and run the
`shape` / `impeccable` skills before building. Promotion of the NOAC parquet from
sandbox (tracking + gitignore negation) is a separate data-anchored checkpoint.
