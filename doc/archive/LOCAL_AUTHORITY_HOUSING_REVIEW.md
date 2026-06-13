# Local Authority & Housing Tile — Review

**Reviewer pass:** 2026-06-05. Scope: ONLY the Local Authority & Housing tile of
`doc/dail_tracker_local_housing_procurement_judiciary_plan.md` (sec 3 Tile 2, the
geography model, sec 6 housing/AFS contracts, sec 7 housing views, sec 8 housing
tests, the sec-13 "why not just Housing" argument) plus `doc/SSHA_social_housing_summary.md`.
Procurement and Judiciary are out of scope here (other review files).

Ground-truthed against the live repo. The single biggest correction: **the plan
was written as if housing is "documentation/scoping only". It is not.** There is a
substantial, already-built housing sandbox (`pipeline_sandbox/housing/`, 24
experimental extractors + a working locality PoC) that the plan never mentions. The
plan therefore re-specifies from scratch a system that largely exists, while the
*one* thing it adds — the constituency geography bridge — is the one piece that is
genuinely unbuilt and genuinely hard.

---

## Claims Ledger

| claim | doc says | repo reality (path:line) | verdict |
|---|---|---|---|
| Housing is "documentation/scoping only", build as Tile 2 | sec 0.48, table sec 2 (`Housing/SSHA … Documentation/scoping only`) | A full sandbox exists: `pipeline_sandbox/housing/` has 24 `*_experimental.py` incl. SSHA appendix extractors, NOAC, HAP, AHB, construction, a master build and a **working locality PoC** (`housing_locality_poc_experimental.py:1`) | **stale** |
| No housing SQL views exist | implied ("no housing SQL views observed", sec 2) | Confirmed: `sql_views/*housing*`, `*ssha*`, `*local_auth*` all return zero (Glob, no matches) | **confirmed** |
| LA AFS = chain in pipeline.py, "no profile model" | sec 2 table | `pipeline.py:64` wires `("afs", "extractors/afs_amalgamated_extract.py")` — the **national amalgamated** extractor only. The per-LA `la_afs_extract.py` is NOT wired (sandbox/Phase-0, `la_afs_extract.py:1`) | **confirmed (with nuance)** |
| AFS is "actuals by service division" | sec 2, sec 6 row (`afs_expenditure … audited/annual actual expenditure`) | True but underspecified. `la_afs_divisions.parquet` carries `value_kind=net_expenditure_actual`, `scope=single-LA`, and an explicit caveat: net-expenditure BY DIVISION, "NEVER reconcile against amalgamated or la_payments_fact" (`data/_meta/la_afs_coverage.json:602-606`) | **confirmed — but plan must inherit the "not total spend" warning** |
| SSHA is annual 31-LA net-need, Data Hub machine-readable, excludes HAP/RAS/SHCEP | sec 1, H1 (sec 3) | Confirmed by source doc (`SSHA_social_housing_summary.md:24-41`) AND a built PDF extractor proving 31-LA × 9-appendix extraction works (`ssha_appendix_full_extract_experimental.py:30-54`) | **confirmed** |
| SSHA net need excludes current supports — risk of misread as total need | risk register sec 11, H1 caveat | Source doc is emphatic: net need excludes HAP/RAS/SHCEP + transfers + duplicates; PBO "ongoing need" ≈ 113k vs net 60k (`SSHA_social_housing_summary.md:32-41`, `:780-783`) | **confirmed — critical** |
| Geography: `dimension_geography` / `bridge_la_constituency` / `bridge_small_area_constituency` is the model | sec 3 geography model (l.624-652) | These names exist ONLY in docs (`Grep bridge_la_constituency` → 3 doc files, **zero code/parquet/view**). No LA→constituency crosswalk exists anywhere in repo. The whole housing sandbox is LA-keyed with **no** constituency column (`Grep constituency` over `pipeline_sandbox/housing/` → no files) | **confirmed unbuilt — hand-waving** |
| Only existing geography join is constituency-level | — | `v_member_constituency_demographics` keys on constituency via the EC 2023 report (43/43 join), NOT an LA crosswalk (`sql_views/member_constituency_demographics.sql:23-32`). It is the *precedent for honesty*, not a bridge | **confirmed** |
| `housing_ssha_la_fact.parquet` is the proposed SSHA output | H1 (l.526), sec 6 schema | No such file. The built extractors emit `ssha_a1_{table}.parquet` (long format: la, year, category, count, page) — a **different, finer** shape than the plan's flat `metric_group/metric/submetric` (`ssha_appendix_full_extract_experimental.py:6-9`) | **wrong/stale** |
| LA payments fact exists, not pipeline-wired, silver | sec 1 evidence | Confirmed: `data/silver/parquet/la_payments_fact.parquet` exists; not in `pipeline.py` (Grep). Out of this tile's core scope but relevant to the "LA finance" section | **confirmed** |
| Homelessness may be region-grain, "do not force a false LA mapping" | H4 (l.608) | Confirmed by source doc: homelessness CSV is 8 regions, not 31 LAs / 43 constituencies (`SSHA_social_housing_summary.md:449`, `:669`) | **confirmed — good instinct** |
| AFS is a clean comparable cross-LA panel | implied by "AFS actuals by service division" tile section | FALSE. `la_afs_divisions` mixes years across councils (probe: 2019×8, 2023×16, 2024×136, 2025×8 rows) and only 21/31 councils present, ~11 reconciled; Meath=2019, DLR=2023, Kildare=2025-**unaudited** (`la_afs_coverage.json:1-4`, `:362`, `:422`, probe_review_lah_afs_schema output) | **wrong** |

---

## Architectural Assessment

**The plan re-invents an existing system.** The most consequential gap is that the
plan never inspected `pipeline_sandbox/housing/`. That sandbox already contains:
SSHA appendix extraction (A1.1–A1.9, fitz + camelot variants), NOAC H1–H7, HAP
funding (data + xlsx), AHB provision, construction/policy tables, PBO/ombudsman/
spending-review, a CSO open-CSV puller, **and** three consolidation builds
(`housing_la_master_build`, `housing_la_year_series_build`,
`housing_national_year_series_build`) feeding a runnable Streamlit PoC. So Tile 2's
"H1 build SSHA extractor" and "H3 construction status" are largely *done as
sandbox*; the real work is **promotion + boundary** (silver/gold + tests + views +
SQL contract), not greenfield extraction. The plan's sprint sequence (Sprint 2 =
build SSHA extractor) would duplicate working code.

**Nothing is promoted.** Critically, **no housing parquet is committed and none
currently sits in `data/gold/parquet/`** (only `cso_gfa01/gfq01/na012`). The
sandbox builds write to `data/gold/parquet/housing_la_master.parquet` etc. on
demand but those outputs are gitignored/absent. So despite the volume of code, the
*shippable* state is: nothing in gold, no view, no test, no SQL contract. That is
the honest baseline the plan should have started from.

**The data-contract shapes are wrong / will churn.** The plan's `housing_ssha_la_fact`
flat schema (`metric_group/metric/submetric/households/percent`) does not match the
built long-format `ssha_a1_{table}` (la/year/category/count/page). Specifying a new
schema in the plan, when a different one is already built and validated, guarantees
rework. Pick the built shape or consciously migrate — don't spec a third.

**The AFS contract under-warns.** Sec 6's AFS row says `afs_expenditure =
audited/annual actual expenditure`. The repo's own coverage JSON and a project
memory are explicit that this is **operating-expenditure-by-division (excl.
transfers to reserves), NOT a council's headline total spend** — SDCC reads €313.5m
vs published €364.9m (`la_afs_coverage.json:602-606`; memory
`project_la_afs_metric_semantics`). Any UI label of "total spend" is a factual
error. The plan must carry this caveat into the contract and the UI copy, exactly
as it (correctly) carries the SSHA net-need caveat.

**The geography bridge is the weakest part and the only genuinely new part.** The
three-table model (`dimension_geography` / `bridge_la_constituency` /
`bridge_small_area_constituency`) is sound *as a target design* but is presented
with more confidence than the evidence supports. The repo has zero crosswalk, and
the source doc's own reality-check walks back every "trivial spatial join" claim
(SA→constituency = "1–2 weeks first time", boundary file discovery "genuinely
awkward", `SSHA_social_housing_summary.md:658-659`). The plan's saving grace is the
v1 instruction "use local authority directly and only show constituency rollups
where the weight is defensible" (l.654) — that is the right call and should be
hardened into "**v1 = LA-only, constituency is out of scope until the spatial spike
lands**".

---

## Devil's Advocate

- **SSHA net-need misread as total need.** Highest-severity correctness risk. Net
  need (~60k) excludes HAP/RAS/SHCEP/transfers; true ongoing need ≈ 2× (PBO 113k).
  If the card says "households needing social housing in your area" it overstates
  by ~half in some LAs and understates the HAP-reliance story. Mitigation must be a
  **mandatory caveat attached at the view/contract layer**, not optional UI copy —
  and ideally surface the PBO recomputed "ongoing need" alongside, never net-need
  alone. The plan flags this (risk register) but rates it only "Medium"; given the
  no-inference invariant it is effectively High.

- **LA data shown as constituency/TD "performance" (no-inference breach).** The
  source doc is explicit that this data is LA/region/SA scale and "we can show
  constituency *context* around a TD, not what a TD personally did about housing"
  (`SSHA_social_housing_summary.md:617-622`). The plan's Tile-2 structure has a
  "Constituency/member context" section and the SSHA doc brainstorms overlaying
  need next to a TD's votes ("voted X while constituency has Y on the list",
  `:570`). That framing **directs the user to a conclusion** and breaches the
  no-inference-in-UI invariant. Council-level need is NOT a TD scorecard. Keep
  context strictly adjacent and neutrally labelled; never compute a derived
  "performance" signal in the app.

- **AFS mislabelled "total spend".** Covered above. Empirically verified: it is
  net-expenditure-by-division, year-mixed across councils, 21/31 coverage, ~half
  unreconciled. Presenting it as "council spend" or comparing councils on it
  (Kildare's 2025 *unaudited* row next to Cork's 2024 audited) is misleading.

- **Forced LA mapping for region-grain homelessness.** Plan correctly says don't
  force it (H4). Hold that line: homelessness is 8 regions; show "your region",
  never silently fan it to LA or constituency.

- **Drift into a national-stats dashboard.** The sec-13 "why not just Housing"
  argument is *correct* and worth keeping: a pure Housing page duplicates
  public statistical dashboards (CSO/Housing Agency already publish these). But the
  same risk re-enters via the SSHA doc's "Constituency Profile from SAPS",
  "Council Performance league table", and 8 enrichment-everywhere ideas. The tile
  must resist becoming a census portal; its differentiator is *joining LA need to
  the existing TD/constituency/spending/procurement spine*, not re-publishing CSO.

- **Building before procurement/hardening.** Per REVIEW_CONTEXT and the project
  memory `feedback_refactor_timing` + the stated bottleneck ("surfacing, not
  ingesting"), this tile should not start as a page build. Procurement's backend is
  closer to ready (its own review). For LA/Housing the correct first move is
  *promotion of one already-built source to a tested silver+view*, not a page.

- **The "two consecutive year-on-year rises" / refugee / Traveller narratives**
  in the SSHA doc are analytically interesting but are exactly the kind of
  inference that belongs in planning chat, not UI. The app must present the
  numbers with denominators and source links, never the "story".

---

## Data Quality & Enrichments

- **SSHA (H1)** is the cleanest, highest-yield, lowest-privacy source: fully
  aggregated (no individuals), annual since 2016, 31-LA, and *already extracts*
  cleanly (built sandbox proves A1.1–A1.9 lift as proper tables; Data Hub
  machine-readable is even better). This is the right first source — but as a
  **promotion job**, not a build job.
- **AFS** is real but caveat-heavy: net-expenditure-by-division, NOT total spend;
  year-mixed (2019–2025) across only 21/31 councils with ~half unreconciled.
  Usable as "housing-division operating expenditure for council X, year Y, audited"
  with a hard same-(council,year) sum rule and a "not total council spend" label.
  Do NOT build cross-LA league tables on it in v1.
- **One housing-money source** beyond SSHA: HAP funding xlsx and Construction
  Status CSV are both LA-aggregated, low-privacy, and already have sandbox
  extractors. Either is a defensible "one money source" to validate alongside SSHA
  — with strict value_kind discipline (grant_allocated ≠ grant_paid; CSR units in
  planning ≠ completed).
- **Geography bridge** is the enrichment that everything else is gated on for
  *constituency* framing — and it is unbuilt and multi-week (spatial join, versioned
  boundaries). The honest enrichment for v1 is the hand-curated 31-LA × 43-constituency
  M:N map with explicit "your LA covers constituencies …" copy
  (`SSHA_social_housing_summary.md:670`), labelled approximate, NOT a weighted
  rollup presented as exact.
- **Region→constituency** (homelessness) has no honest exact mapping; surface at
  region grain only.

---

## Build / Defer / Reject

| item | verdict | value/effort | reason |
|---|---|---|---|
| SSHA → tested silver + 1 SQL view (promote built sandbox) | **Build (minimal slice)** | High / Low–Med | Cleanest source, no privacy risk, extractor already works; just needs promotion + net-need caveat baked into the view + test (31 LAs/year) |
| Net-need caveat as a contract/view-level attribute (not UI-optional) | **Build** | High / Low | No-inference + the ~2× understatement risk make this mandatory, not cosmetic |
| One LA housing-money source (HAP funding xlsx OR Construction Status CSV) to silver+view | **Build (minimal slice)** | Med / Low–Med | Validates the value_kind discipline on housing money; both already have sandbox extractors |
| LA-only geography, labelled approximate/unknown; hand-curated LA×constituency M:N map | **Build (minimal slice)** | Med / Low | The only honest geography for v1; matches existing `v_member_constituency_demographics` honesty precedent |
| `dimension_geography` / `bridge_la_constituency` / `bridge_small_area_constituency` 3-table model | **Defer** | High / High | Sound target design but unbuilt; gated on a spatial spike (boundary GeoJSON discovery + SA→constituency join, multi-week). Do as a separate spike, not in the first slice |
| Weighted constituency rollups of LA/SSHA data | **Defer** | Med / High | Depends on the bridge; until weights are defensible, do not show |
| `housing_ssha_la_fact` flat schema as specified in sec 6 | **Reject (as written)** | — | Conflicts with the already-built long-format `ssha_a1_{table}` shape; adopt/migrate the built shape instead of speccing a third |
| AFS labelled/used as "council total spend" or cross-LA league table | **Reject** | — | Factually wrong (net-expenditure-by-division, year-mixed, 21/31, half-unreconciled). Same-(council,year) use only, with explicit caveat |
| Local Authority & Housing **page** build (Sprint 3) | **Defer** | High / High | No source is promoted yet; per bottleneck + refactor-timing, build the tested silver+views first; a page on un-promoted gold is premature |
| TD/constituency "housing performance" framing, votes-vs-need overlay | **Reject** | — | Breaches no-inference-in-UI; LA need is context, not a TD scorecard |
| Homelessness forced to LA/constituency grain | **Reject** | — | Region-grain only; plan already says so — hold the line |
| Full SAPS/Pobal constituency-profile + Council-Performance league + map view | **Defer** | High / High | Multi-week, spatial-join-gated; explicitly out of the minimal slice |
| Sec-13 "Local Authority & Housing" not "Housing" naming/positioning | **Build (accept)** | — / — | The argument is correct: keeps it joined to the TD/spine, avoids national-stats-dashboard drift |

---

## Bottom Line

The Tile-2 plan's *product instincts are right* — LA-anchored (not "Housing"),
SSHA-first, net-need-caveated, region-grain homelessness, LA-direct geography in
v1 — but it was written blind to the repo's reality in two opposite directions.
On the **build** side it under-credits a large existing housing sandbox (24
extractors + a working PoC), so it re-specs from scratch work that is largely done
and would be better framed as *promotion to tested silver+views*; it even invents a
new SSHA schema that conflicts with the validated built one. On the **geography**
side it over-credits itself: the three-table `dimension_geography`/`bridge_*` model
exists only in prose, there is no LA→constituency crosswalk anywhere, and the
spatial join is a multi-week spike, so that part is hand-waving and must be
deferred. The two non-negotiable correctness traps are confirmed live: AFS is
`net_expenditure_actual`-by-division (year-mixed, 21/31, half-unreconciled) and must
never be labelled "total spend"; and SSHA net need (~60k) is ~half of true ongoing
need (PBO ~113k) and must carry a contract-level caveat, never a TD/constituency
"performance" framing. **Recommended minimal slice before any page:** promote SSHA
to a tested silver fact + one caveated SQL view, add one LA housing-money source
(HAP-funding or construction-status) with strict value_kind, and ship LA-only
geography with explicit approximate/unknown labels — defer the constituency bridge,
the page, and all SAPS/map work.
