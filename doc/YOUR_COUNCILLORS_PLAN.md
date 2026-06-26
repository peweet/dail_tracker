# Your Councillors — feature plan & data-pull strategy

Status: **SCOPED + SOURCE-TESTED (2026-06-22). Build not started.** Companion to
`doc/FRONT_DOOR_PROTOTYPES.md` §B ("your area" personal hook) and the existing
"Who runs your county" page (`utility/pages_code/local_government.py`).

---

## 1. One-liner

On the existing *Your Area* surface, a citizen picks **County → Local Electoral Area** and
gets the councillors who represent them — **who** they are, **what they're paid**, **what they
actually decide** (reserved functions), and **who lobbies them** — each line a handoff into the
deep pages that already exist.

## 2. Why (the gap)

The corpus is **Oireachtas-only** (TDs/Senators via the Oireachtas API) plus the 31 council
**Chief Executives** (`data/_meta/la_chief_executives.csv`). The ~949 elected **councillors**
appear nowhere. They are the tier citizens interact with most, and they complete the
reserved-vs-executive accountability story the CE page started (LGA 2001 Part 14 / 2014 Reform).

Two structural facts shape everything below:
- **Councillors are elected by Local Electoral Area (LEA)** — ~166 LEAs across the 31 LAs
  (3–7 seats each), **not** Dáil constituency, so `constituency_la_crosswalk.csv` does not reach them.
- **Eircode→geography is proprietary/licensed** (Eircode/GeoDirectory) — we **cannot** build a free
  Eircode resolver. Entry is **County→LEA dropdown** in Phase 1; town-name geocode in Phase 2.

## 3. Source landscape (tested on Galway, 2026-06-22)

Ground truth: Galway County = **39** seats / 7 LEAs; Galway City = **18** / 3 LEAs.

| Source | County | City | Structure | Churn-aware? | Verdict |
|---|---|---|---|---|---|
| **Wikidata** | 1/39 | 0/18 | position items exist, unused | no | **OUT** |
| **Wikipedia** | 39/39 ✓ | 18/18 ✓ | name+LEA+party, by-LEA section | **yes** (`[a][b]` co-option footnotes) | **SPINE** |
| **Official 2024 results** (An Coimisiún Toghcháin / vote.ie / DHLGH) | national | national | name/party/LEA/count | no (frozen at election) | **baseline / validation** |
| **data.gov.ie open data** | — | rich | details **+ actual expenses (S142)** | yes (quarterly) | **enrichment, ~5 councils only** |
| **Lobbying.ie DPO** | national | national | per-council DPO lists (name/grade/role) | live | secondary roster source |
| **Council websites** | clean (galway.ie/en/councillors, 39 profiles) | **no roster page at all** | heterogeneous | live | enrichment where clean |

**Decision:** spine = **Wikipedia** (only complete + churn-aware + one parser for all 31), validated
against **official 2024 results**, enriched by **data.gov.ie** for the ~5 councils that publish it.
Wikidata dropped. Council-site scraping is per-council opt-in, not the backbone (Galway City proves a
council may publish *nothing* scrapable → 31 bespoke parsers is the trap to avoid).

data.gov.ie councillor coverage is **only**: Dublin City, Fingal, South Dublin, Dún Laoghaire-Rathdown,
Kildare. Everything else has no open-data councillor set today.

## 4. Salary, expenses & travel (authoritative: DHLGH directions + SI 236/2014 / 2021 Regs)

Source of truth = DHLGH *Allowances and Expenses of Elected Members of Local Authorities* (44-pp
directions, the canonical schedule) + Circular LG 06/2020 + ss.142–143 Local Government Act 2001.

Every councillor may receive:
- **Representational Payment (salary):** **€32,059** gross/yr, taxable (current; ~€25k at the 2021
  reform — indexed to a Senator's salary via the PSPP Act 2017 / Circular LG 06/2020).
- **Annual Expenses Allowance:** composite travel+subsistence+misc, ~**€3,162.36/yr**, by **indexed
  meeting bands**; **80% meeting attendance required for the full amount** → direct hook into our
  attendance data. Explicitly **not** a record of actual distance travelled.
- **Local Representation Allowance (LRA):** up to **€5,160/yr**, *vouched* (show €4,200 eligible spend
  + up to €960 petty cash).
- **Chairperson's allowances:** Cathaoirleach/Mayor, Municipal District chair, SPC chairs — capped
  maxima, officeholders only.
- **Conferences & training** + outside-body **travel & subsistence** — capped.

Baseline ≈ **€40k**, more for officeholders.

### Travel arrangements — what we can and cannot see
- **Rules (national):** the travel leg is part of the Annual Expenses Allowance (indexed by
  home→HQ distance band + 80% attendance), plus separate T&S for outside-body meetings, plus
  conference/foreign travel — all capped by the DHLGH schedule. This is *structure*, not journeys.
- **Actual spend (per-council, uneven):** the statutory **Section 142 Register** (every council must
  keep a register of payments to members) records actual amounts incl. travel/conference. **SDCC**
  publishes it as open **CSV** (ArcGIS Hub, quarterly); **Cork City** publishes quarterly **PDFs**
  incl. a dedicated **Foreign Travel** line (currently empty = a transparency signal in itself); most
  councils publish a PDF of some kind.
- **No national per-journey ledger exists.** Visibility = national *rules* + per-council *S142 actuals*
  (open CSV for ~5 councils, PDFs elsewhere). Honest framing required; do not imply a mileage log.

## 5. Responsibilities (reserved functions — ss.131/131A/131B LGA 2001)

Councillors decide **reserved functions**; the Chief Executive holds everything else (executive
functions). Concrete reserved functions to surface (AILG Leaflet 2 + Elected Members Guidance Manual):
- Adopt the **County/City Development Plan** + Local Area Plans (the planning-policy lever).
- Adopt the **annual budget** + Annual Financial Statement; approve **capital programmes**.
- Set **commercial rates** (annual rate on valuation) + the **LPT local adjustment factor** (vary
  property tax ±15%).
- **Appoint/remove the Chief Executive**; elect the Cathaoirleach/Mayor.
- Housing & environment policy, bye-laws, place/road naming, grant-scheme allocations.
- **NOT** individual planning permissions (those are executive = CE).

This is the same reserved-vs-executive split documented for
`la_chief_executives` — the two features are two halves of one accountability story.

---

## 6. Pull-it-all-in pipeline

All extractors follow house rules: `services/parquet_io.save_parquet` (atomic + `min_rows` floor),
logging via `setup_standalone_logging`, `--dry-run`, fidelity-gated writes, **no git in any
subagent**. Sandbox → vet → promote (`feedback_pipeline_changes_data_anchored_promotion`).

| # | Extractor | Source | Output | Notes |
|---|---|---|---|---|
| 1 | `extractors/councillors_wikipedia_extract.py` | 31 council Wikipedia articles, section "Councillors by electoral area" + Co-options subsection | `data/silver/.../councillors.parquet` | **the spine.** Parse with BeautifulSoup (lxml NOT installed → `pandas.read_html` unusable). Carry co-option/affiliation footnotes as a `status` flag. |
| 2 | `extractors/councillors_results_validate.py` | Official 2024 results (vote.ie / DHLGH; Wikipedia 2024-election pages as structured proxy) | coverage JSON | **validation, not a write** — assert per-LEA seat counts match Schedule 7 LGA 2001 (e.g. Galway County 39, City 18); flag any council off by ≥1 (= missed co-option or parse gap). |
| 3 | `extractors/councillors_opendata_extract.py` | data.gov.ie CKAN (`package_search`) for DCC / Fingal / SDCC / DLR / Kildare | `councillor_expenses.parquet` | details + **S142 actual expenses** (SDCC quarterly CSV via ArcGIS Hub). Honest coverage flag: 5 councils only. |
| 4 | `data/_meta/councillor_pay_schedule.csv` (curated, git-tracked) | DHLGH directions PDF (hand-curated, like `la_chief_executives.csv`) | — | salary €32,059 + allowance bands + LRA €5,160 + chair maxima; `effective_from` dated; one row per component. |
| 5 | `reference/lea_boundaries_extract.py` (**Phase 2**) | Tailte Éireann ArcGIS LEA layer (reuse `local_authority_boundaries_extract.py` pattern) | `data/_meta/lea_outlines.json` | refuse `--write` unless ~166 LEAs reconcile to the roster's distinct LEA list. Needed only for geocode entry. |

**pipeline.py chain** `councillors`: (1) → (2 validate) → (3 enrich) → `promote_gold`. Cadence: add to
`check_freshness.py` DATASETS (Wikipedia roster ~30-day, open-data quarterly, pay schedule 400-day).
The election-cycle is 5-yearly (next 2029) but **co-options churn continuously** — the freshness canary
on the Wikipedia source is the staleness guard, replacing a static 2024 dump.

### Roster schema (`councillors.parquet` / `data/_meta`)
`full_name, party, local_authority, local_electoral_area, seats_in_lea, status` (sitting / co-opted /
resigned), `since_date, source_url, as_of_date`. Join key `local_authority` MUST match
`constituency_la_crosswalk.csv` / `la_chief_executives.csv` **exactly** (plain "Cork City",
"Dún Laoghaire-Rathdown", Limerick/Waterford drop "City and County").

## 7. Views (`sql_views/constituency/`, registered in `CONSTITUENCY_FILES`)
- `v_la_councillors` — per-LEA councillor list + party composition + seat count (over the roster).
- `v_la_councillor_pay` — the national pay/allowance schedule (display reference, not per-person).
- `v_la_councillor_expenses` — S142 actuals where published (DCC/Fingal/SDCC/DLR/Kildare); honest
  `has_open_data` flag so the UI shows "not published as open data" for the other 26.
Dependency order: any view JOINing another registers **after** it (`feedback_sql_view_dependency_order`).

## 8. UI (display-only, no inference)
A **"Your councillors"** block on `local_government.py` (the Your Area surface):
County selector → LEA selector → councillor cards (name · party stripe · council). Reuse `lg-*`/`con-*`
card CSS. Each dossier: **role** (reserved-vs-executive explainer, framed against the CE), **pay**
(national schedule + actual expenses *where published*, with the 80%-attendance caveat), **who lobbies
them** (Phase 2). Provenance footer → Wikipedia + DHLGH + council source. Zero `st.dataframe` on the
primary view (`feedback_member_overview_no_dataframes`).

## 9. Voting history — data-availability limitation (NOT a build)
Unlike TDs (every Dáil division is named, via the Oireachtas API), **councillor voting is structurally
thin** and is documented here as a *limitation*, not a feature:
- **Named votes exist only for "roll-call"/recorded votes.** Standing Orders require minutes to record
  the names voting for/against/abstaining **only when a roll-call vote is taken**; the default is a
  voice vote or an unnamed division (count only). A roll-call must be specifically requested, so most
  decisions produce **no named record**.
- **Where it exists it's in PDF meeting minutes**, one set per council (e.g.
  `galway.ie/en/council-meetings`), 31 heterogeneous formats — **no central API, no standard schema**.
- **PROBED Galway minutes (2026-06-22): access is easy, format is the wall.** Galway County publishes
  ~10 recent PDFs (plenary + Municipal District); Galway City **112** archived (City publishes minutes
  even though it has no roster page). BUT **every PDF sampled is 100% SCANNED IMAGES — zero text layer**
  (32-pp county plenary = 0 chars / 173 images; MD minutes, City minutes 2021–2024 all the same). So
  extraction needs **OCR**, which is **off-box only** (PaddleOCR crashes the local Windows box —
  `feedback_paddleocr_crashes_local_box`; GPU off-box `feedback_ocr_use_gpu`). No `tesseract`/
  `pytesseract` locally either.
- **Verdict: poor effort/yield — OUT OF SCOPE (evidence-backed).** Chain = harvest scanned PDFs → OCR
  every page (off-box) → parse for the *rare* roll-call votes → ×31 councils, each a different scanned
  format. Access isn't the blocker; OCR-at-scale for sporadic yield is. The most consistently-present
  councillor signal in minutes is proposer/seconder of motions (still OCR-locked, and not a vote). If
  ever attempted: hand-curate one high-profile recorded vote per council, never a 31-council OCR parser.
  State plainly in the UI that a complete councillor voting record does not exist in public data.

*(NB: the lobbying-on-zoning cross-ref previously sketched here is **deferred as premature** — owner
decision 2026-06-22. Councillors-as-DPOs remains only a secondary roster source, §3.)*

## 10. Honesty guardrails
- Display-only; restate sources, never infer (`feedback_no_inference_in_app`).
- Wikipedia is **unofficial** — label it; council site / official results are authoritative if disputed.
- Pay = **entitlement schedule**, not per-person earnings (except published S142 actuals).
- Travel = **rules + per-council actuals where published**; never imply a national mileage log.
- `as_of_date` + co-option `status` on every councillor (churn honesty).

## 11. Phasing
- **Phase 1 (MVP):** Wikipedia roster (#1) + results validation (#2) + curated pay schedule (#4) +
  `v_la_councillors`/`v_la_councillor_pay` + County→LEA dropdown + "Your councillors" card. Display-only.
- **Phase 2:** open-data expenses (#3) + `v_la_councillor_expenses`; LEA boundaries (#5) + town-name
  geocode entry; freshness canary; local registers of interest.
  (Lobbying-on-zoning cross-ref and any voting-history parse are **deferred** — see §9.)

## 12. Risks
- **Maintenance/churn** (main risk) — co-options between elections; mitigated by Wikipedia's co-option
  tracking + the freshness canary. A static 2024 dump would silently rot.
- **Council-site heterogeneity** — proven (Galway City has no roster); keep scraping opt-in, never the spine.
- **Open-data ⊊ national** — only 5 councils; the expenses view must degrade honestly for the other 26.

## 13. Open questions
1. Confirm the official-results structured source to validate against (vote.ie scrape vs the per-county
   DHLGH results PDFs vs Wikipedia 2024-election pages as the structured proxy).
2. Phase-1 expenses: ship pay *schedule* only, or include the 5-council S142 actuals immediately?
3. Surface the Section 142 register link per council even where it's PDF-only (transparency pointer)?
