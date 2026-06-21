# Dáil Tracker — IDEAS (Master Consolidation)

**Purpose:** one place that consolidates the project overview + every new-ingestion / data-source / feature idea currently scattered across ~30 docs in `doc/`. This is a **map, not a commitment** — most items are scoped/probed, not built. Each entry cites its source doc(s); go there for detail.

**Last consolidated:** 2026-06-04.

**Status legend:**
`✅ shipped` (in gold / live) · `🟡 built-in-sandbox` (built+validated, not integrated, often gitignored) · `🔬 spike-done` (probed/validated, no ETL) · `📋 planned` (scoped, not built) · `💡 idea` (backlog) · `⛔ blocked/parked`

---

## 1. What Dáil Tracker is

An Irish civic-data transparency explorer — theyworkforyou-style public accountability tooling for the **current Oireachtas**. Medallion ETL (bronze/silver/gold parquet) → ~30 registered DuckDB SQL views → thin Streamlit (pandas) UI. Python/Polars ETL, parquet (zstd/L3/stats), YAML page contracts, uv lockfile, Streamlit Cloud target. CI: ruff, logic-firewall, basedpyright, pytest, SQL-contract tests.

**Defensible positioning** (per `COMPETITIVE_LANDSCAPE.md`): don't compete on standalone votes/debates (KildareStreet), lobbying dashboards (Lobbyieng), or payments (Gript). The uncontested ground is **cross-source joins** — a unified per-member profile linking interests + CRO + lobbying + Iris + payments — plus structuring sources nobody else structures (Register of Members' Interests, Iris Oifigiúil). Outreach target: investigative outlets (The Ditch, Noteworthy) who do this cross-referencing by hand.

**Two inviolable rules** (carried into every idea below):
- **No inference in app UI** (`feedback_no_inference_in_app`): planning chat can speculate; the app presents verifiable data only and never directs users to conclusions. Co-occurrence ≠ causation; award ≠ expenditure; "not checked" ≠ "in force".
- **Never union different money-meanings** (`DATA_MAP.md`): see §8.

---

## 2. Existing footprint (already shipped / live)

Core app pages: **attendance, member overview** (flagship), **votes, interests, payments, lobbying, legislation, committees**, plus **statutory instruments, corporate notices, appointments, glossary, lobbying PoC**.

Gold datasets: Oireachtas API (members/votes/debates/questions/legislation), attendance/payments/interests PDFs, lobbying.ie CSV, SIs + SI legal-state, Iris (appointments + corporate notices), eTenders procurement (gold), CRO companies (supplier-match backbone), constituency population, **Seanad equivalents** (see §7), judiciary core (validated).

---

## 3. Procurement, public payments & awards

The hardest-won corpus. Note the grain discipline (§8) — award ceilings, committed POs, and actual payments are **three different things**.

- **eTenders contract awards (OGP)** | ✅ gold (page deferred) | 100k notices / ~60k award-supplier rows, CC-BY; supplier→CRO ~45%; 123 firms on both procurement+lobbying registers. *Caveat: values are framework/DPS ceilings not spend (€570bn naïve vs €23.3bn safe, 24×); quarantine sole-traders.* | `PROCUREMENT_MASTER`, `PROCUREMENT_INVESTIGATION`
- **TED (EU Tenders Electronic Daily) IE awards** | ✅ silver `ted_ie_awards` (13,126 rows, no page) | above-threshold IE awards with **real** values (72% of 2025), winner=CRO# (~84% matchable). *Caveat: pan-EU framework outliers meaningless to sum; winners skew foreign; real-value coverage strong only 2024+.* | `PROCUREMENT_INVESTIGATION`
- **Public-body / semi-state PO+payment publishers** | 🟡 `public_payments_fact` (8,021 rows / 19 publishers) | supplier-level POs/payments from depts, OPW, TII, Screen Ireland, etc. *Caveat: privacy quarantine OFF; `amount_semantics` drifted (must converge to value_kind+realisation_tier); HARD no-join gate until per-publisher verified.* | `PROCUREMENT_SEMISTATE_EXPANSION_PLAN`, `PUBLIC_PAYMENTS_FACT_SCHEMA`
- **HSE + Tusla payments** | 🟡 bespoke parser (~19k rows, NOT merged) | HSE 16,972 rows (€6.39bn, 2021-25) + Tusla 1,980. *Caveat: emits DQ JSON not parquet; third vocab; content-dup dedup needed; Tusla may carry individual carers.* | `PROCUREMENT_SEMISTATE_EXPANSION_PLAN §11.5`
- **New publisher discovery sweep (17 bodies)** | 🔬 confirmed, not wired | 5 universities, 2 hospitals, Courts Service, NTMA/NDFA, Sport Ireland, SEAI, EPA, Pobal, Garda, Prisons. *Caveat: grain care (Pobal mixed); Garda/Prisons high-privacy; some 403 → browser-verify.* | `PROCUREMENT_SOURCE_DISCOVERY_2026_06_04`
- **`public_payments_fact` unified schema + VALUE TAXONOMY** | 📋 draft, not built | canonical payment-grain fact + the 2-axis `realisation_tier`(PLANNED→AWARDED→COMMITTED→SPENT/BUDGET) × `value_kind` contract. **Lock before any concat.** | `PUBLIC_PAYMENTS_FACT_SCHEMA`, `PROCUREMENT_MASTER §8`
- **Procurement-related-payments open datasets** | 🔬 census-corrected | only ~25 title-confirmed actual-spend datasets from 3 publishers — the open-data spend corpus is **small**, not 100+ bodies (most publish on own sites per Circular 05/2023). | `PROCUREMENT_INVESTIGATION`
- **Mini-competitions / standalone awards (OGP)** | 🔬 probed → SKIP | 88% supplier overlap with eTenders, marginal. | `PROCUREMENT_INVESTIGATION`
- **OCDS / OpenTender** | ⛔ skip | CC-BY-NC-SA (non-commercial, license-incompatible); stale to Nov 2023. | `PROCUREMENT_INVESTIGATION`
- **Department grant / subvention registers (Section 38-39, Sport/Arts/HEA)** | 💡 idea | discretionary grant allocations, geographic → constituency funding heat-map; "funded then lobbying". *Caveat: format varies per dept; multi-year double-count.* | `ENRICHMENTS B.3-B.5`

---

## 4. Local-authority finance

- **Amalgamated AFS (national)** | ✅ silver (64 rows, no page) | revenue I&E by service division, all-31-summed, 2016-23, reconciles exactly. *Caveat: national-only, zero per-council rows; accrual grain.* | `PER_LA_AFS_BUILD_PLAN`
- **Per-LA AFS — revenue (by-division, per-council)** | 🟡 Phase 0 (168 rows / 21 councils, gitignored, validated) | the per-constituency prize → Council Finance / "Your Area" page. *Caveat: 31 bespoke harvests; ~10% OCR tail; 4 JS councils need Playwright; op-ex-by-division NOT headline total spend.* | `PER_LA_AFS_BUILD_PLAN`, `project_la_afs_metric_semantics`
- **Per-LA AFS — capital (by-division, per-council)** | 🟡 159 rows / 21 councils, gitignored | housing-as-capital story (~€2.5bn pooled, ~98% DHLGH-grant-funded). | `PER_LA_AFS_BUILD_PLAN`
- **LA Purchase-Orders-over-€20k (31 councils)** | 🟡 `la_payments_fact` (11,091 rows, ~20 councils, gitignored) | per-transaction committed/spent supplier payments; ~250-320k national estimate → who-got-paid locally, repeat-winner concentration. *Caveat: bespoke per council; quarantine sole-traders.* | `PROCUREMENT_INVESTIGATION`, `PROCUREMENT_MASTER`
- **LA budget tables (planned-vs-actual)** | 🔬 scoped — best fit | SDCC/Fingal/Roscommon budget tables, **1:1 A-H division match to AFS** → planned-vs-actual variance. *Only ~3 of 31 councils publish structured CSV.* | `new_sources_value_and_features_claude_plan`, `DATA_MAP`
- **CSO GFA general-government budget (PxStat)** | 🔬 probed, not promoted | GFA04 by ESA economic category 2000-2025, CC-BY → BUDGET-tier macro layer. *Caveat: general-govt (central+local combined), NOT per-LA, NOT by function.* | `PROCUREMENT_INVESTIGATION`, `ENRICHMENTS H.1`

---

## 5. Political finance (SIPO / elections) — mission-defining

- **SIPO election EXPENSES OCR (GE2024)** | ✅ fact (`sipo_expenses_fact`, 401 candidates/9 parties, 19 tests) → Election Finance page (by-party/candidate/constituency, verified-vs-flagged honesty model). *Caveat: only party national-agent spend (a SUBSET — don't conflate with combined totals); 380/401 verified, 21 OCR-flagged held back; single election.* | `SIPO_OCR_INVESTIGATION`, `ELECTION_SPENDING_PAGE_SHAPE`
- **SIPO OCR engine swap (the key lever)** | 🔬 PaddleOCR validated | lifts amount-recovery ~85% → ~98-100%. **The scan is crisp; Tesseract is the bottleneck** (`€17,844.78`→`feireaa7e`). Closed-set constituency + statutory-cap checks become engine-independent validation. *Windows: `enable_mkldnn=False`; pipeline-only extra, never core deps.* | `SIPO_OCR_INVESTIGATION`
- **SIPO political DONATIONS register** | ⛔ blocked (OCR; own context) | who donated to whom — the higher-value prize, closes donor→lobbyist→vote loop. *Caveat: 105pp, zero text layer, never OCR'd; never imply donation = influence.* | `ENRICHMENTS A.1`, `SIPO_OCR_INVESTIGATION`
- **Candidate → member_registry fuzzy link** | 💡 deferred | OCR'd candidate names → `unique_member_code` so candidate cards click to /member-overview. v1 shows names as printed. | `ELECTION_SPENDING_PAGE_SHAPE`
- **SIPO ethics returns / referendum spending; election results** | 💡 idea | office-holder Ethics-Act returns (fills RoMI office-holder gap); per-constituency count-by-count results (safe-vs-marginal context). *Caveat: SIPO URLs broken (re-verify); boundary changes; electionsireland.org single-maintainer risk.* | `ENRICHMENTS A.2-A.5`

---

## 6. Statutory instruments & legislation

- **SI legal-state ("C1", eISB Directory)** | ✅ shipped | per-SI amended/revoked/partially-revoked state + which SI affected it, sourced + confidence-scored, 2016-26. *Caveat: discovery only — never positive-asserts "in force"; null = "not checked".* | `SI_LEGAL_STATE_C1_PLAN`
- **SI→SI amendment graph (`v_si_amendments`)** | 🟡 view+tests built, UI wiring left | 1,484 directed edges (1,315 revokes/159 amends/10 partial), both directions, pure inversion of `si_current_state`. → bidirectional "Amendment history" card. *Caveat: SI→SI only (no SI→Act); excludes `other_affected`.* | `SI_AMENDMENT_GRAPH_ETL_PLAN`
- **LRC Classified List subject enrichment** | 🔬 spike-done (PR1 built+tested in sandbox; verdict ship) | LRC subject/subheading per SI (36 subjects/251 leaves), 90.1% match; fills `si_policy_domain` for 84% of NULL-domain SIs; server-rendered HTML, no OCR → subject chips + topic browse + filters. *Caveat: LRC "in-force" listing LAGS revocations (56% of revoked SIs still listed) → must NOT be surfaced as legal status; `si_current_state` stays sole legal-state layer.* | `SI_LRC_ENRICHMENT_SPIKE`, `si_lrc_enrichment_claude_brief`
- **LRC Revised-Acts annotation refs (SI→Act, F/C/E-notes)** | 💡 idea ("skip PR3") | SI→affected-Act links + annotation effects. *Caveat: highest maintenance, lowest payoff; SI→Act link fragile.* | `si_lrc_enrichment_claude_brief`
- **All-sponsors list (`v_legislation_sponsors`)** | 📋 cheap view edit | all co-sponsors per bill (data already in `sponsors.parquet`, view discards it) → clickable sponsor pills. | `oireachtas_explorer_full_comparison`
- **Bill documents timeline (`v_legislation_documents`) + dail_term** | 📋 planned | per-artefact publish date+URL → interleaved stage+document timeline. | `legislation_benchmark_oireachtas_explorer`
- **View Bill PDF affordance** | 📋 design-only | bill-text + explanatory-memo PDF URLs (data present, returns NULL in views today) → "View Bill PDF" pill + Bill Text panel. *Caveat: new-tab links not iframe (X-Frame-Options).* | `view_bill_pdf_feature`
- **Cream List (bills stuck awaiting Second Stage)** | 💡 research-first | from daily Order Paper / Riar na hOibre → "stuck >12 months" accountability story. *Caveat: unverified whether API exposes it or needs Order Paper PDF scraping.* | `oireachtas_explorer_full_comparison`
- **Questions analytics** | 📋 data present (264k silver rows), needs ~6 `v_questions_*` views | asker leaderboard + ministry view + topic search + member-overview tab. *Caveat: joint-asker dedupe on `question_ref` (count distinct).* | `parliamentary_questions_feature`, `oireachtas_explorer_full_comparison`

---

## 7. Iris Oifigiúil — the executive-branch layer

Shared source `iris_notice_events_clean.csv`, filtered by category/subtype. The standout *new-surface* idea is a **"Government day to day" page** — the first executive (not Oireachtas) page, no equivalent in Irish civic data.

- **Public Appointments page** | 🟡 enrichment v1 built, design locked, page next | state-board/agency/SpAd/judicial appointments (1,248 rows) → patronage page "who appointed whom"; SpAd-per-minister ranking. *Caveat: 58% Irish-language (curated-template translation, not MT); appointee ~17% unextractable.* | `public_appointments_feature`
- **Corporate notices page (insolvency/examinership/ICAV)** | 📋 design locked, ETL prereqs done | ~35,894 notices + brand→parent-fund tagging → "who's calling in Irish loans" ranking + per-company search. *Caveat: ALL personal insolvency excluded (privacy); entity_name 75% clean; journalist/researcher tilt.* | `corporate_feature`
- **Receiver-appointers ("who's calling in Irish loans")** | 💡 absorbed into Corporate page | 2,620 receivership notices + curated brand→fund map (Promontoria→Cerberus etc.) → ranked top appointers, receiver-wave trend. *Caveat: Irish banks dominate; vulture-SPV is a minority slice.* | `receiver_appointers_feature`
- **Bill signings / Exchequer statements / Commission ToRs** | 💡 idea | presidential Act signings, quarterly Exchequer, Commission terms-of-reference → Act→SI completion, executive footprint. | `ENRICHMENTS K.3-K.5`
- **State Boards register (live roster + body universe)** | ✅ BUILT 2026-06-12 (no page yet) | membership.stateboards.ie scrape → silver `stateboards_roster`/`stateboards_boards` (2,061 seats / 196 boards, legal basis + gender balance + basis-of-appointment) → gold + `v_stateboards_roster`/`v_stateboards_boards`; `stateboards` pipeline chain. Gold carries **hand-curated** Wikidata outside-role identities only (`data/_meta/stateboards_wikidata_curated.csv`, 66 verified names / 70 seats). *Caveats: current roster only (no history); automated Wikidata name-matching was REMOVED same day — audit found ~1 in 4 auto-matches was the wrong same-named person; new names re-curated via `wikidata/stateboards_wikidata_enrich.py` candidate queue (un-wired, human-reviewed).* | `PUBLIC_RECORD_SOURCES_REVIEW` §shortlist-2

---

## 8. Judiciary

Validated 2026-06-04; provenance now in `data/sandbox/judiciary/README.md` (feature shipped: `utility/pages_code/judiciary.py` + `sql_views/judiciary/`). **Iris public-appointments is the canonical appointment spine — don't re-scrape Iris.**

**Green / validated:**
- Judicial appointment spine (Iris, 114 clean, 2016-26) ✅ data exists · Courts Service "The Judges" roster (~190 judges, ~97% join) 🔬 · Elevation detection (29 promotion chains) 🔬 · Gov.ie nominations (cause→nominee→bench) 🔬 · Judicial Council conduct stats (aggregate, fitz) 🔬 · Courts clearance CSV (CC-BY, 2017-24) 🔬 · 94-courthouse map (CC-BY) 🔬.
- **Planned:** High Court assignments/specialist lists 📋 · JAC vacancies (post-2025 regime) 📋.

**Blocked / forbidden:**
- Wikidata revolving-door (TD/Minister→judge) — validated but **excluded from UI** (false positives, 0 current-bench signal) · Judgments corpus ⛔ (copyright/redundant) · Legal Diary "cases up for judgement" ⛔ (privacy: names wards/minors/repossessions) · Judicial financial-disclosure ⛔ (no Irish regime — the *absence* is the story) · **Per-judge performance/bias scoring — FORBIDDEN third rail** (defamation + mission failure).

---

## 9. Public-money / legal / accountability — new sources (2026-06-04 scoping)

Data pulled + profiled, **no ETL** (`new_sources_value_and_features_claude_plan`, `new_public_money_legal_sources_claude_backlog`, memory `project_new_sources_scoping_2026_06_04`). Verdicts:

- **PAC report metadata** 🔬 — 38 reports, 100% URL-derivable, rides existing `oireachtas_pdf_poller` (best reuse).
- **C&AG report metadata** 🔬 — 252 docs (120 Special Reports + annuals to 1922); 2-stage crawl.
- **LA statutory audit reports (LGAS)** 🔬 — ~400 born-digital PDFs (31 councils × 2012-24), no OCR, clean council+year join; `value_kind=audit_finding`.
- **Housing Adaptation Grants** 🔬 — LA-aggregated, zero PII, per-capita angle; paid vs allocated kept separate. *Trap: 2008-11 in €'000s.*
- **REV / Voted Expenditure** 🔬 — CC-BY CSV, reconciles to published REV; **frozen at 2022; user lukewarm** (dept-level context only).
- **CPO cases (An Coimisiún Pleanála)** 🔬 GO with guard — scheme-level land-acquisition signal (NEVER spend); **Housing-Act cases leak private addresses → case-type quarantine + leak-string guard; never ingest PDFs.**
- **NTA board minutes** 🔬 — approval-signal feed (~0% euro values); segmenter + LLM finisher.
- **FOI/AIE disclosure logs** 🔬 — federated `foi_lead` layer (DLR/Justice/DCC cleanest); per-body adapters.
- **Project Ireland 2040** 🔬 — 1,936-project ArcGIS spine; **cost is band-only + sparse** (not a euro feed).
- **Sports Capital** ⛔ — host defunct/unreachable.

These mostly add *context/accountability/lifecycle* around existing spend data — see combination features in §13.

### 9b. Enforcement / EU-money / executive-diary sources (2026-06-12 ingestion round)

Four sources INGESTED to sandbox 2026-06-12 (`pipeline_sandbox/*_extract.py` → `data/sandbox/enrichment/`); not yet gold/views/UI. One blocked.

- **EU State Aid TAM (Ireland)** | 🟡 sandbox (`eu_tam_ireland_awards`, 15,593 awards 2016-2026) | every Irish state-aid award >€100k with named beneficiary — the structured source behind the §3 grant-registers idea (DAFM 7.8k / SBCI 3.5k / EI 1.9k / IDA 648). National-ID column = six-digit CRO number (36% of rows) → clean CRO join, no name-matching. *Caveats: value_kind=grant_awarded (AWARD, never union with payments); agri rows include natural-person farmers → `beneficiary_is_individual_suspected` quarantine flag; session-bound WebLogic crawl (LB-cookie malformed-Domain trap documented in extractor).* | `eu_tam_ireland_extract.py`
- **Ministerial diaries** | 🟡 sandbox (index 220 files / 13 listings; `ministerial_diary_entries` 14,935 parsed engagements 2017-2026, DETE incl. Tánaiste Varadkar 2.5k) | the OTHER side of the lobbying register — who ministers actually met; feeds Minister Activity (§12). No central hub: per-dept listings. DETE born-digital (4 layout generations parsed, 131/147 files); ALL DPER PDFs are image scans → `scanned_needs_offbox_ocr` queue (mirror SIPO pattern, never OCR locally). *Caveats: diaries self-curated/non-exhaustive; a diary meeting ≠ a lobbying return — co-occurrence wording only.* **Full extraction→gold→UI plan: `MINISTERIAL_DIARIES_BUILD_PLAN.md`** (adds Finance/Justice/DSP publishers found 2026-06-12). | `ministerial_diaries_extract.py`
- **CBI enforcement actions** | 🟡 sandbox (`cbi_enforcement_actions`, 140 actions 2007-2025, fines parsed from 112 statement PDFs; validates vs known record: BOI €100.5m, AIB €83.3m) | regulatory-sanction layer on the CRO/CBI-firm backbone. *Caveats: value_kind=sanction_fine never summed; enforcement-actions list ONLY — prohibition notices/adverse assessments EXCLUDED (natural-person privacy); `party_is_individual_suspected` flag for ex-officer cases; full list is inline `appData` JS on the hub page (no API).* | `cbi_enforcement_extract.py`
- **ISIF Irish portfolio** | 🟡 sandbox (`isif_portfolio`, 213 investments 2007-2026) | sovereign-fund equity/debt into named Irish companies — a state-money flow no other source sees. *Caveats: amounts stated in prose for only ~28% (rest undisclosed), mixed EUR/USD/GBP, value_kind=investment_commitment; no sector tags (client-side only).* | `isif_portfolio_extract.py`
- **CRO disqualified/restricted persons** | ⛔ blocked | CORE search API exists (`core.cro.ie/api/croperson/disqualifiedsearch`) but requires a per-search reCAPTCHA token — no bulk path without captcha circumvention (won't do). Revisit if CRO publishes bulk/open data; CEA annual-report aggregates are the fallback.
- **Diary↔lobbying org fuzzy match** | 📋 planned (user-requested 2026-06-12) | fuzzy-match organisation names inside `ministerial_diary_entries.subject` free text against lobbying-register client/lobbyist orgs (and CRO/supplier names) → "which companies met which ministers", and diary-side corroboration of lobbying returns. *Caveats: subjects are messy free text ("Meeting with Shein", "Wyeth …") — needs org-name gazetteer + conservative thresholds; strictly co-occurrence presentation (feedback_no_inference_in_app); explosion-counting risk as in lobbying joins.* Now Phase 5 (§7.2) of `MINISTERIAL_DIARIES_BUILD_PLAN.md` — gazetteer tiers, ≥90%-precision display gate, corroboration window-join.

---

## 10. Company / entity enrichment

- **CRO companies register** ✅ silver — directors, last_accounts_date, company_num; the supplier/lobbying/notice entity-resolution backbone. *Caveat: free search rate-limited; bulk paid.*
- **CRO Financial Statements** 🔬 PARK — free CSV is index-only (redundant); actual figures PAYWALLED (~€2.50/doc); mostly abridged SME accounts. | `CRO_FINANCIAL_STATEMENTS_EXPLORATION`
- **CRO↔corporate-notices xref / CBI authorised firms** ✅ gold xref / 🟡 CBI (13.8k rows) — corporate-distress badges. *Caveat: CBI heuristic PDF extraction, false positives.*
- **RBO (Beneficial Owners)** 💡 — humans owning ≥25%. *Caveat: restricted post-2022 CJEU; patchy.*
- **OpenCorporates / Companies House UK / Charities Regulator / Pensions Authority** 💡 — cross-jurisdiction linking, charity trustees, TD trusteeships. *Caveat: OC commercial-restricted; trustee-name resolution bottleneck.* | `ENRICHMENTS C.2-C.6`

---

## 11. Housing, CSO & geography

- **SSHA social-housing assessments + NOAC / HAP / Construction Status** 💡 (flagged) — per-LA waiting-list net need + council-performance H1-H7 → "Housing & Social Housing" + "Council Performance" pages. *Caveat: LA→constituency M:N crosswalk is the blocker; "net need" excludes HAP/RAS (PBO ~doubles it); fully aggregated.* | `SSHA_social_housing_summary`, `ENRICHMENTS H.1`
- **CSO PxStat (census/labour/economic) + constituency boundaries** ✅ population built / 💡 wider tables — per-capita ratios, normalised constituency figures. *Caveat: geographies don't align (county/NUTS3/LA ≠ constituency); needs crosswalk.* | `ENRICHMENTS H.1-H.2`
- **Property Price Register / Tailte Éireann / An Bord Pleanála** 💡 — corroborate declared property; developer↔donation overlap. *Caveat: address-resolution bottleneck; Tailte per-search paid.* | `ENRICHMENTS H.3-H.5`

---

## 12. Member / parliamentary & cross-dataset trace features

- **Highlights page (editorial front door)** 📋 — magazine front page, 5 ranked "stories" from existing views, replaces Attendance as default route. *Mostly UI; Story 1 needs `v_lobbying_index_year`.* | `highlights_page_idea`
- **Minister Activity page** 📋 — per-minister lobbying contacts + SIs signed, joined on topic. *Hardest gap: lobbying↔SI topic crosswalk; SI actor is free-text.* | `minister_activity_feature`
- **Policy-to-Action Trace** 📋 (MVP ~1.5-2wk; eISB link-out shipped) — co-located public-record timeline (PQ + lobbying + SI + eISB) per topic. *Strictly descriptive, no causal arrows.* | `policy_to_action_trace_scoping`
- **Lobbying-to-Regulation Timeline** 📋 — windowed join of lobbying returns → SIs by responsible dept. *Coincidence not causation; no parsed signing date (Iris issue_date proxy).* | `lobbying_to_regulation_timeline`

> **Consolidate before building:** Minister Activity, Policy-to-Action, and Lobbying-to-Regulation are three takes on the *same* lobbying↔SI↔topic join with the same hard deps (department alias table, lobbying↔SI topic crosswalk, SI responsible-actor resolution).

---

## 13. The payoff — combination "profile" features

The individual sources are useful; the value is assembling them into pages that **don't exist elsewhere in Irish civic tech** — and the project already owns the expensive middle (procurement/payments).

- **Infrastructure Project Profile** (most novel): Project Ireland 2040 (project+stage) → CPO (land) → NTA/TII board minutes (approval/award) → **procurement awards [existing]** → **payments [existing]** → C&AG (overruns). *Plan→land→approve→award→pay→audit* for one scheme.
- **Public Body Profile**: REV (voted budget) + procurement/payments [existing] + PAC/C&AG (audited) + FOI (what's asked) + board minutes (decisions). The voted→awarded→paid funnel.
- **Local Authority / "Your Area" Profile**: AFS actuals [built] + LA budget (planned, same A-H divisions) + LA payments [built] + Housing grants (per-capita) + LGAS audit reports.
- **Supplier Dossier**: procurement awards + payments + board-minute contract awards — every public euro + decision per company.
- **Highest-leverage story loops** (`ENRICHMENTS`): donor→lobbyist→vote (SIPO donations + lobbying + votes); declared-interest→state-contract (RoMI × CRO × eTenders); AG-to-bench pipeline; constituency funding heat-map.

---

## 14. Cross-cutting rules (apply to every ingestion)

- **Money grains never union** (`DATA_MAP`): three families — **BUDGET/by-division** (AFS), **AWARD/ceiling** (eTenders/TED — advertised/awarded, NOT paid), **PAYMENT/SPENT** (actual € to named supplier). A euro in one ≠ a euro in another. Concat only *within* the payment grain, after taxonomy conformance. Lock the 2-axis `realisation_tier` × `value_kind` vocab before any merge; wholesale consolidation has **not** begun.
- **Privacy quarantine**: exclude personal insolvency, sole-traders/individuals, private CPO landowners, FOI requester PII, Tusla individual carers, Revenue defaulters. Mirror the personal-insolvency view-level guard.
- **Safe wording**: SI "not checked" ≠ "in force"; lobbying/procurement = co-occurrence; award ≠ expenditure; MVL ≠ distress; entity-match confidence must be visible. (Planned: feature badges Stable/Beta/Experimental/Not-causation/Not-expenditure.)
- **Sandbox-first** (`project_pipeline_sandbox_rule`): new Polars enrichment → `pipeline_sandbox/`; SQL views → `sql_views/`; never touch `pipeline.py`/`enrich.py`/`normalise_join_key.py` directly.
- **New real-world claims need a source link** (`feedback_cite_news_claims`).

---

## 15. Product / operations / distribution themes

Per `dail_tracker_improvements_v4` ("operationalise, don't expand"), `SHORT_TERM_PLAN` (8-week alpha), `TICKETS` (DAIL-xxx), `cron_only_longevity_and_data_health_plan`, `CITATION_AND_DATA_PLAN`, `dail_tracker_suggestions_fixes`:

- **Seanad parity** ✅ built+integrated (`seanad-app-parity` branch) — Senators are first-class members at full TD parity (ETL on main, all gold built, all UI chamber-aware, identity keyed on `(unique_member_code, house)`). Open: Seanad attendance denominator, curated provenance-PDF list. | `SEANAD_*`
- **Auto-refresh + publish-don't-crawl** 📋 — `pipeline.py --refresh` + per-mart manifests, GitHub Actions cron, `data` branch, polite HTTP helper, golden-file parser tests, row-count/schema drift guards, 3-tier alerts. | `TICKETS`, `cron_only_longevity`
- **`dataset_health.json` + publish-guard PR workflow** 📋 — automate *refresh*, not *publication*; cron opens reviewable PRs. (This is the sister `current_source_health_coverage_gaps_claude_plan` theme.)
- **QueryResult wrapper** 📋 P0 — distinguish "no data" from "backend failed" across ~11 data-access modules.
- **Provenance footer + freshness badge** 📋 (DAIL-008/009).
- **Vote pagination fix** 📋 — replace `limit=1000` truncating fetch (Questions cap already lifted).
- **"Cite this view" + `/data` download page + Zenodo DOIs** 📋 — the actual product for newsrooms. | `CITATION_AND_DATA_PLAN`
- **Public data inventory page + RSS/Atom feeds + global entity search** 📋/💡 (deferred).
- **Architecture cleanup** (dim/fact/bridge, Streamlit-free query core, SQL manifest, Member Overview modularisation) — explicitly **deferred** past the alpha window. | `project_reorg_plan`

---

## 16. Biggest known gaps (honest)

Per `DATA_LIMITATIONS`:
- **Scope = sitting members / 34th Dáil / 2020+ PDF cut-off** (older layouts unparsed); office-holders partly outside the Register of Members' Interests; family/indirect holdings never visible.
- **Cron-staleness traps** (sharpest operational gap, DAIL-160-167): API steps no-op after first run, hard-coded PDF URLs, in-place PDF re-issues invisible, pipeline halts on first failure → "data as of <date>" unreliable.
- **Name-matching join keys** are an engineering compromise (collisions/misses); lobbying is manual-CSV with explosion-counting risk; deltas (TD turnover, SIPO determinations, SI revocation) are unwatched.

---

### Source-doc index (where the detail lives)
Live tracked detail docs: `DATA_MAP`, `PROCUREMENT_MASTER`, `PROCUREMENT_NUGGETS`, `MONEY_FLOW_DATA_AUDIT`, `PUBLIC_PAYMENTS_FACT_SCHEMA`, `PER_LA_AFS_BUILD_PLAN`, `new_public_money_legal_sources_claude_backlog`, `new_sources_value_and_features_claude_plan`, `ENRICHMENTS`, `SSHA_social_housing_summary`, `DATA_LIMITATIONS`. · Judiciary provenance now lives in `data/sandbox/judiciary/README.md`. · Older feature/scoping/audit docs (SI legal-state, corporate, public-appointments, election-spending, SIPO-OCR, second-pass reviews, etc.) have been retired to `doc/archive/` (local-only, not tracked).
