# Dáil Tracker — Consolidated Review Synthesis & Roadmap

**Synthesis pass:** 2026-06-05. **Action mode:** analysis-only (only this file written; no pipeline/extractor/gold/page edits).

Merges the seven parallel review outputs into one deduped, ranked roadmap:

- `doc/CBI_SECOND_PASS_REVIEW.md` (CBI)
- `doc/PROCUREMENT_TILE_REVIEW.md` (Proc)
- `doc/LOCAL_AUTHORITY_HOUSING_REVIEW.md` (LAH)
- `doc/JUDICIARY_LANE_REVIEW.md` (Jud)
- `doc/PUBLIC_RECORD_SOURCES_REVIEW.md` (PRS)
- `doc/TANGIBLE_SOURCES_REVIEW.md` (Tang)
- `doc/SHIP_VS_REBUILD_REVIEW.md` (Ship)

Invariants enforced throughout (`doc/REVIEW_CONTEXT.md §2`): logic firewall, no-inference-in-UI, never-union-money-grains, privacy (anonymise natural persons; runtime exceptions not `assert`), surfacing > ingesting.

---

## 0. URGENT — Incidents, not roadmap (fix before/independent of any feature work)

These are live defects on committed gold or invariant breaches. They are not backlog; they are P0 incidents. Build-Next and Defer below assume these are handled first.

### INC-1 — Judiciary gold parquet leaks real natural-person names **today** (P0, legal exposure)
- **What:** `data/gold/parquet/judicial_legal_diary_cases.parquet` is committed and ships to Streamlit Cloud (Jud claims-ledger, `.gitignore:295-297` re-includes it). A read-only probe found **8 of 602 rows expose ≥1 full personal name in clear** (Jud Devil's-Advocate; corroborated independently by Ship §"Data Quality" and Ship risk #1).
- **Root cause (verified in this synthesis):** `anonymise()` in `extractors/legal_diary_extract.py:197-207` has two bugs —
  - **(A)** `re.split(..., maxsplit=1, ...)` at `:201` splits on only the *first* `v`; a `… CONSOLIDATED WITH <Name> -v- <Co> PLC` second clause is never anonymised (1 row).
  - **(B)** `_is_org()` at `:193-194` tests the **whole side** for an org keyword; a mixed side like `<Name1> and <Name2> and <X> County Council` contains `council`, so the entire side (including the named individuals) is kept verbatim (7 rows — the dominant, scaling leak path).
- **Aggravating factor:** the page renders `case_anonymised` verbatim (`judiciary.py:276`) under an explicit "people shown by initials only" promise (`judiciary.py:318-321`) and links each row to the official diary — the promise *increases* liability (Ship #1).
- **Fix (all P0, gate public Tier C exposure):**
  1. Split on **all** `v`/`-v-` tokens and anonymise **per `and`-chunk**, not per whole side (Jud Build P0).
  2. Replace the column-name `assert` at `extractors/legal_diary_extract.py:339` with a **content-scanning runtime `PrivacyInvariantError`** (project invariant forbids `assert` under `-O`; the current check tests the wrong thing — a column named `case_anonymised` full of real names passes it) (Jud, Ship).
  3. Add `test/test_judiciary_privacy.py` — it does **not exist** today despite the plan naming it (Jud claims-ledger `wrong`). Golden cases 1–7 specified in Jud §Build (no residual TitleCase in non-org sides; multi-`v` coverage; org-side mixed-party per-chunk; protected-drop completeness; provenance present; page renders Tier C only via judge/list grouping).
- **Until all three pass: Tier C (anonymised cases) is HELD / beta-hidden.** Tier A (schedule) and a retitled Tier B (counts) are safe and stay public (Jud, Ship).

### INC-2 — `assert`-based privacy guards must become runtime exceptions
Same class as INC-1.2 but stated as a general sweep: `extractors/legal_diary_extract.py:339` is the confirmed instance; any other privacy `assert` is `-O`-strippable and breaches REVIEW_CONTEXT §2 (Jud, Ship).

### INC-3 — Unmarked `value_counts` / classification in the render path (P1 firewall residue)
Confirmed unmarked render-time grouping at `corporate.py:1580`, `statutory_instruments.py:473,570`, `judiciary.py:232` (Ship claims-ledger). On a public "we surface, we don't model" product this silently reclassifies on a data-shape change. Mark `# logic_firewall: display_only` where genuinely display-only, or push to a view. P1, not a launch blocker, but close on every shipped page.

### INC-4 — Public-body payments sandbox has privacy quarantine **deferred** (do-not-ship gate)
`extractors/procurement_public_body_extract.py:728` sets `public_display=True` while coverage records `privacy_quarantine_applied=False` (Proc claims-ledger, LAH/PRS corroborate). This sandbox fact must **not** reach any UI until a quarantine pass runs. It is the data behind the deferred payments/spend tab and the award→spend ratio — both held below.

---

## 1. Cross-review conflict reconciliation (resolved positions)

| # | Tension | Reviews | Resolved position |
|---|---|---|---|
| C-1 | **Ship now** (caveated beta) vs **hold/harden first** | Ship (partial ship), Jud (hold C), Proc (ship read-only), LAH (defer page) | **PARTIAL SHIP.** Ship the 11 mature parliamentary/money pages behind 5 low-effort fixes (Ship). **HOLD Judiciary Tier C** (INC-1). Build Procurement read-only page next. Keep Local Authority/Housing a "coming soon" tile. The split is by *page maturity*, not all-or-nothing. |
| C-2 | **What "hardening" is undone** — doc §5 implies a big build | Ship vs the doc | **Most is already built.** `QueryResult` (3-state) is frozen and wired (`dail_tracker_core/results.py:33`); `empty_state`/`todo_callout`/`page_error_boundary`/`sidebar_shell`/`glossary_strip`/provenance footers all exist (`utility/ui/components.py`). **Genuinely undone:** (a) global beta banner, (b) structured report-an-issue path, (c) wire already-generated `freshness.json` into a shared banner (no page reads it today), (d) surface `QueryResult.ok/unavailable_reason` at the page boundary (wrappers discard `.data` one line too early — `procurement_data.py:39`, `judiciary_data.py:31`), (e) close firewall residue (INC-3). |
| C-3 | **QueryResult-aware data access** — "future contract redesign" vs "10-line wrapper" | Proc (~10 lines), Ship (~1 line/wrapper) | **It is an additive ~10-line-per-domain wrapper.** Core already returns the 3-state result; only the `.data` unwrap discards it. Keep existing `.data` wrappers for back-compat; add `fetch_*_result() -> QueryResult` siblings. NOT a redesign. |
| C-4 | **Housing is "scoping only"** vs a large existing sandbox | LAH vs the plan | **Housing is substantially built in `pipeline_sandbox/housing/` (24 extractors + working PoC).** The real work is **promotion to tested silver+view+SQL contract**, not greenfield extraction. The plan re-specs a system that mostly exists and even invents a conflicting SSHA schema. (PRS/Tang agree the sources are mostly already scoped.) |
| C-5 | **eTenders data.gov.ie open-data as "additive/stable alternative"** | Tang, PRS, Proc | **Reject — it is already the live source.** `procurement_etenders_extract.py:39` downloads exactly that data.gov.ie CSV. Framework/DPS/call-off classification + value semantics are already modelled. Not additive. |
| C-6 | **`value_kind_legend.py` as a new module** | Proc | **Reject the new module; reuse `glossary_strip` (`components.py:299`), scoped to the 3 award value-kinds the feed actually emits.** CSS-in-component breaks the `shared_css.py`-only convention. The doc's wider legend (payment/budget/grant/afs kinds) misleads on an awards-only page. |
| C-7 | **§7 award→payment "candidate matcher"** vs what exists | Proc | **A different, simpler-grained artefact already exists** (`procurement_award_spend_link.py` is an entity-level JOIN, not a row-level fuzzy matcher) and it rides un-promoted, quarantine-deferred sandbox spend (INC-4). **Defer + re-spec; keep analytic-only.** |
| C-8 | **Geography bridge** (`dimension_geography`/`bridge_la_constituency`/`bridge_small_area_constituency`) | LAH | **Exists only in prose; zero crosswalk in code/parquet/view.** Multi-week spatial spike. **Defer.** v1 geography = LA-only + hand-curated LA×constituency M:N map labelled approximate (matches the existing `v_member_constituency_demographics` honesty precedent). |
| C-9 | **CBI expansion** = extend register extractor vs new extractor | CBI | **New event-stream extractor** (`cbi_legal_notices_extract.py`), reusing the postback fetch + `_norm_firm` join — NOT an extension of the snapshot extractor (different cadence/schema/join key). |
| C-10 | **Legal-diary into `pipeline.py`** | Jud (reject chain), Ship (build-before-launch) | **Keep it out of `pipeline.py`** — a forward-accumulating one-day poller correctly runs as a scheduled task (Jud). **But** document it as a deliberate non-chain and give it a freshness entry, because a daily-court page with no visible refresh cadence is a credibility risk (Ship). These are compatible: scheduled poller + freshness surfacing, not a batch chain. |

**Disagreements where two reviews diverge on the same item (flagged, resolved above):**
- *Legal-diary pipeline wiring* (C-10): Jud says reject-chain, Ship says build-before-launch → resolved as "non-chain but add freshness entry".
- *QueryResult wrapper size* (C-3): Proc "~10 lines", Ship "~1 line/wrapper" → same direction, trivial effort either way; not a real conflict.
- *SSHA net-need severity*: LAH upgrades the plan's "Medium" to effectively **High** (the ~2× understatement vs PBO is a no-inference correctness trap). Adopt High.

---

## 2. Deduped BUILD backlog (merged across reviews)

Duplicates collapsed into single line-items carrying the strongest rationale. Source briefs 3 & 4 (PRS/Tang) and the procurement tile (Proc) overlapped heavily on eTenders / LA-AFS / budgets / C&AG-NOAC / ERDF / State Boards — merged here.

| # | Item | Surface served | Reviews | Value/Effort | Verdict |
|---|---|---|---|---|---|
| B-1 | **Procurement read-only page** over the 5 existing `v_procurement_*` views (supplier/authority/CPV/lobbying-overlap + caveat panel). No new extractor/pipeline/parquet. | Procurement | Proc (primary), Ship, PRS, Tang | High / Low | **BUILD (1st)** — most UI-ready surface in the whole brief; gold+views+core+data-access all exist. |
| B-2 | **Wire `freshness.json` → one shared freshness banner** on every page (generated at `pipeline.py:89`, read by nobody). | All pages | Ship | High / Low | **BUILD** — closes §5.2 app-wide in one shot. |
| B-3 | **Global "public beta" banner + structured "report an issue" path** (not founder inbox). | All pages | Ship | High / Low | **BUILD** — both fully undone; the missing feedback path is a real launch gate (Ship #5). |
| B-4 | **Surface `QueryResult.ok/unavailable_reason` at the page boundary** — additive `fetch_*_result()` wrappers; stop discarding `.data`. | Procurement, Judiciary, all | Proc, Ship | High / Low | **BUILD** — core already 3-state; ~10 lines/domain. Distinguishes "no records" from "source failed". |
| B-5 | **Value-kind legend via existing `glossary_strip`**, scoped to the 3 award kinds. | Procurement | Proc, Ship | Med / Low | **BUILD (with B-1)** — reuse primitive, do NOT create a new module. |
| B-6 | **Procurement page smoke test** (import / missing-parquet→unavailable / zero-rows / **no individual `supplier_norm` reachable**). | Procurement | Proc | Med / Low | **BUILD (with B-1)** — the privacy-reachability assert is load-bearing (sole-trader PII). |
| B-7 | **Single shared `provenance_footer` component** consolidating the 6+ bespoke `_render_provenance`. | All pages | Ship | Med / Low | **BUILD** — the one genuinely-new component in the §7 list. |
| B-8 | **Close unmarked `value_counts`/classification** on shipped pages. (= INC-3) | All pages | Ship, Jud | Med / Med | **BUILD (P1)** — firewall integrity is the product's core promise. |
| B-9 | **CBI warning notices** → own gold table + `cbi_*` view; EXACT join to CRO/corporate_notices (NOT to authorised register — clone-name false positives). Fact-only, source-linked. | Corporate | CBI | High / Low-Med | **BUILD** — clean structured HTML, no natural persons, low decay, extends the shipped Corporate/Iris frame; postback + `_norm_firm` reusable. |
| B-10 | **CBI firm-level enforcement actions** → own gold table + view; EXACT join to CBI register + corporate_notices; **strip individual-named rows**; fine = new `value_kind=enforcement_fine`, never summed. | Corporate | CBI | High / Med | **BUILD (after B-9)** — strongest "regulatory history" signal; strengthens the repeat-distress panel. No "SANCTIONED" badge (inference) — render facts + link. |
| B-11 | **DPC fines table** → `value_kind=regulatory_fine`, org-named (no PII), CRO-joinable. 2-table HTML, `read_html`. | Corporate | Tang | High / Low | **BUILD** — completes the corporate-enforcement surface; small + stable. Sibling to CBI distress badge. |
| B-12 | **Promote SSHA to tested silver fact + 1 caveated SQL view** (promote built sandbox, don't rebuild). Net-need caveat as a **contract/view-level attribute, not optional UI copy**. Adopt the **built long-format `ssha_a1_{table}` shape**, not the plan's invented flat schema. | Local Authority & Housing | LAH, PRS | High / Low-Med | **BUILD (promotion job)** — cleanest, zero-PII, already extracts; the net-need-vs-PBO ~2× caveat is mandatory (no-inference). |
| B-13 | **One LA housing-money source** to silver+view (HAP funding xlsx OR Construction Status CSV; both have sandbox extractors). Strict value_kind (`grant_allocated ≠ grant_paid`). | Local Authority & Housing | LAH, PRS | Med / Low-Med | **BUILD** — validates value_kind discipline on housing money. |
| B-14 | **ERDF/EU-funds beneficiaries** (NWRA + Southern + DPER) → `value_kind=grant_or_subvention` (COMMITTED ceiling, never sum with award/payment). Probe-confirmed standardised XLSX; beneficiary+contractor → supplier/CRO dim; sole-trader quarantine on the tail. | Supplier Dossier / regional funding | PRS (shortlist #1), Tang | High / Low | **BUILD** — genuinely new, clean, joins to the supplier dim the project owns. (PRS and Tang differ on tier label — PRS `grant_or_subvention`, Tang `grant_committed/COMMITTED`; both agree never-sum.) |
| B-15 | **LA Budgets (Tables A–F, PLANNED tier)** → `value_kind=budget_estimate`, `realisation_tier=PLANNED`, sum only within (council,year,table). Dual-parser PDF reusing AFS machinery; reconcile-gated. | Local Authority & Housing (Council Finance) | Tang (primary), PRS | High / Med | **BUILD** — the one missing LA money-grain; completes budget→AFS-actual→PO-payment story. Higher effort than the CSV builds; strategically most valuable LA item. |
| B-16 | **Quarterly mini-comp / standalone-award CSVs** (Circular 10/14, >€25k) → `value_kind=award_existence_no_value`, `value_safe_to_sum=false` (no amount column). | Procurement (coverage layer) | Tang | High / Low | **BUILD** — distinct sub-OJEU population; clean CSV, no OCR; relationship fact only, must never be summed. |
| B-17 | **LA-only geography + hand-curated LA×constituency M:N map** labelled approximate/unknown. | Local Authority & Housing | LAH | Med / Low | **BUILD (with B-12)** — the only honest geography for v1; matches existing honesty precedent. |
| B-18 | **State Boards register** (current roster + public-body universe) — **probe-first** (membership.stateboards.ie returned a thin 8KB JS shell; fall back to publicjobs annual-report PDFs). Role+body+term only, no contact data. | Public Body Profile | PRS (shortlist #2), Tang (defer) | High / Med | **BUILD probe-first** — the missing public-body-universe spine joining publishers ↔ C&AG-audited ↔ AFS; complements Iris events. (PRS build-probe-first; Tang defers pending an endpoint — resolved as **probe-gated build**.) |
| B-19 | **Social Housing CSR XLSX/CSV** → `value_kind=unit_count`; LA→constituency gated (shares B-17 crosswalk). | Local Authority & Housing | Tang | Med / Low | **BUILD (with housing surface)** — ready XLSX+CSV, pairs with B-12/B-13. |
| B-20 | **CBI revocation notices** → status-change events on tracked register entities. | Corporate | CBI | Med / Med | **BUILD-lite / DEFER (3rd CBI family)** — partly already visible via "revoked" registers; ship only after B-9/B-10 prove the notices-extractor pattern. |
| B-21 | **Trivial sweeps:** stale `pipeline_sandbox/` paths in view headers (`procurement_awards.sql:2-3`, `corporate_cbi_distress.sql:7`); doc view-name fix `v_corporate_cbi_repeat_distress` (not `..._notice_...`). | Maintenance | CBI, Proc | Low / Low | **BUILD (trivial)** — cosmetic post-reorg drift; sweep when adjacent pages land. |

---

## 3. Build-Next shortlist (≤8)

Ordered by value/effort honouring **surfacing > ingesting** (top items surface already-built data; ingestion items only where they complete a half-built surface cleanly). Each tagged `[surface | review(s)]`.

1. **B-1 Procurement read-only page** `[Procurement | Proc/Ship/PRS/Tang]` — highest-ready surface; pure surfacing, zero ingestion.
2. **B-2 Freshness banner (wire existing `freshness.json`)** `[all pages | Ship]` — app-wide, generated-but-unread.
3. **B-3 Beta banner + report-an-issue** `[all pages | Ship]` — real launch gate.
4. **B-4 QueryResult page-boundary surfacing** `[Procurement/Judiciary/all | Proc/Ship]` — ~10 lines/domain, core already does the work.
5. **B-12 SSHA promotion to caveated silver+view** `[Local Authority & Housing | LAH/PRS]` — promotion not build; net-need caveat mandatory.
6. **B-9 CBI warning notices** `[Corporate | CBI]` — clean, no-PII, extends shipped Corporate frame.
7. **B-11 DPC fines table** `[Corporate | Tang]` — small, stable, org-only, completes enforcement surface.
8. **B-14 ERDF beneficiaries** `[Supplier Dossier | PRS/Tang]` — probe-confirmed clean, joins to existing supplier dim.

(B-5/B-6/B-21 ride along with B-1 as the same slice; B-8 firewall residue and B-7 provenance consolidation are app-hardening that lands alongside B-2/B-3.)

---

## 4. Defer list

Each `[surface | review(s)]` with the gate that must clear first.

- **Local Authority & Housing page** `[LAH | LAH/Ship/PRS]` — gate: ≥1 housing source promoted (B-12/B-13) + tests + views. Keep as "coming soon" tile.
- **Geography bridge** (`dimension_geography`/`bridge_*` 3-table model) `[LAH | LAH]` — gate: multi-week spatial spike (boundary GeoJSON + SA→constituency). Unbuilt in code.
- **Weighted constituency rollups of LA/SSHA data** `[LAH | LAH]` — gate: the bridge above + defensible weights.
- **Full payments/spend tab** (public-body + LA + HSE/Tusla) `[Procurement | Proc/PRS]` — gate: INC-4 quarantine pass + promotion; mixes grains.
- **LA-payments promotion** (`v_publicmoney_la_payments`, gold, tests) `[Procurement/Council Finance | Proc/LAH]` — gate: schema + 31-council coverage + privacy tests + freshness; 31 drift surfaces.
- **`procurement_award_spend_link` → UI** `[Procurement | Proc]` — gate: INC-4 + re-spec; entity-join ratio over un-promoted spend = inference risk. Analytic-only for now.
- **legal_diary/judiciary freshness entry + scheduled-refresh documentation** `[Judiciary | Jud/Ship]` — pairs with INC-1 HOLD; non-chain but needs visible cadence.
- **TED surfaced to UI** `[Procurement | Proc]` — silver enrichment; cross-reference only, no first-slice need.
- **CBI revocation notices** (B-20) `[Corporate | CBI]` — after B-9/B-10 prove the pattern.
- **C&AG + PAC report metadata** `[Public Body Profile | PRS]` — already scoped (rides existing poller); `value_kind=audit_finding`, metadata-only.
- **Project Ireland 2040 tracker** `[Infra Project Profile | PRS]` — already scoped, weak (cost is bands not €).
- **NOAC PI indicator tables (selective)** `[LA performance | Tang]` — a few tabular indicators worth selective extraction; 10MB prose body is not.
- **CSO Register of Public Sector Bodies** `[reference/canonicaliser | Tang]` — reference table not a fact; wire when a procurement-authority canonicaliser needs it.
- **DHLGH planning datasets (ArcGIS/GeoJSON)** `[planning | Tang/PRS]` — machine-readable but no surface to feed; spatial; pairs with housing/crosswalk.
- **Access-to-cash quarterly + CIT/Designated registers** `[Corporate | CBI]` — only genuine "local services" angle; CIT/Designated are postback-broken.
- **Prove `pipeline.py` headless on Ubuntu** `[infra | Ship]` — launch-blocking for any *daily-refresh* tile (per `project_freshness_architecture`); the freshness banner is only honest if refresh runs.
- **`Regulatory & Supervisory Outlook / Annual Report / APS`** `[Corporate | CBI]` — link-index only, no structured ingestion.

---

## 5. Out-of-Scope / Reject list

Each `[reason | review(s)]`.

- **CBI prohibition / Fitness-&-Probity / disqualification notices** `[names private natural persons + asserts misconduct = personal-insolvency privacy precedent + defamation | CBI]`.
- **CBI adverse assessments, IFSAT tribunal decisions** `[individual-naming, tiny volume, external tribunal docs | CBI]`.
- **CBI Dear CEO / thematic-supervision letters, AML/CFT bulletins, sector reports, consultation papers, AnaCredit, DORA** `[sector-narrative PDFs, not entity-keyed; "heightened supervisory focus" = inference; corporate-KYC mission not civic | CBI]`.
- **Board minutes (TII/NTA/HSE/LDA/SBCI/MARA/Irish Rail)** `[free-text PDF, ~0% extractable €, per-body 31-LA-style drift, PII; "surface signals" = inference-in-app. Award-in-minutes already = board_approved_award on NTA probe | PRS/Tang]`.
- **FOI / AIE disclosure logs (~20 bodies)** `[records-exist, no extractable content; requester PII; never joins. Keep as foi_lead lead-layer only, not ingestion | PRS/Tang]`.
- **Protected-disclosure annual counts** `[one integer per body/year — not a dataset | PRS]`.
- **MARA MUL submissions, publicjobs candidate campaigns, FOI released-records PDFs** `[named natural persons → PII bar | Tang]`.
- **Annual reports / governance codes / ARC ToRs / "Where Your Money Goes"** `[narrative context, no clean join key; a dashboard not a feed | PRS]`.
- **eTenders data.gov.ie open-data as "additive/stable alternative"** `[already the live procurement source, procurement_etenders_extract.py:39 | Tang/PRS/Proc]`.
- **Framework/DPS/call-off classification + value semantics as "new"** `[already modelled and surfaced in v_procurement_awards | Tang/Proc]`.
- **OGP per-framework expiry-date / lot-name scraper** `[hundreds of bespoke HTML pages, no bulk endpoint, high decay, one field of value — manual reference only | Tang]`.
- **C&AG / OPR audit chapters as full-text ETL** `[narrative annual chapters, qualitative value, dual-parser uneconomic — manual reference | Tang]`.
- **`value_kind_legend.py` as a new top-level module** `[duplicates glossary_strip; CSS-in-component breaks shared_css.py convention | Proc]`.
- **Legend term list with payment/budget/grant/afs kinds on an awards page** `[awards feed emits only 3 value_kinds; showing spend kinds misleads | Proc]`.
- **`housing_ssha_la_fact` flat schema as specified** `[conflicts with the validated built long-format ssha_a1_{table}; adopt/migrate, don't spec a third | LAH]`.
- **AFS labelled/used as "council total spend" or a cross-LA league table** `[factually wrong — net-expenditure-by-division, year-mixed 2019–2025, 21/31 councils, ~half unreconciled; same-(council,year) use only | LAH]`.
- **TD/constituency "housing performance" framing / votes-vs-need overlay** `[LA need is context not a TD scorecard — breaches no-inference-in-UI | LAH]`.
- **Homelessness forced to LA/constituency grain** `[region-grain (8 regions) only | LAH]`.
- **Adding a `judiciary`/`legal_diary` chain to pipeline.py** `[forward-accumulating one-day poller; scheduled-task design is correct — document as deliberate non-chain | Jud/Ship]`.
- **Merging CPO into Judiciary** `[CPO = Infra/LA context, landowner PII; gate behind its own privacy tests | Jud]`.
- **Re-opening the framework choice (Dash / from-scratch FastAPI+Next.js)** `[direction committed: Option A now / D later, partially built | Ship]`.
- **Re-building the QueryResult/shared-component layer as net-new** `[already built: results.py + ~8 components under existing names | Ship/Proc]`.
- **Further per-page inline CSS investment** `[throwaway against the eventual React swap | Ship]`.

---

## 6. Executive bottom line

**Fix immediately (incidents, not roadmap):** the Judiciary gold parquet leaks 8 of 602 real natural-person names into committed, Cloud-shipped data **today** via two reproducible `anonymise()` bugs (first-`v`-only split at `legal_diary_extract.py:201`; whole-side org test at `:193`), guarded only by a `-O`-strippable `assert` that checks a column *name* not content, with the golden privacy test the plan calls for not even existing — so **HOLD Tier C from public exposure** until the multi-party anonymiser is fixed, the assert becomes a content-scanning `PrivacyInvariantError`, and `test/test_judiciary_privacy.py` passes; also keep the un-quarantined public-body payments sandbox out of any UI. **Ship now (partial):** the 11 mature parliamentary/money pages behind five genuinely-small fixes (beta banner, report-an-issue, wire the already-generated `freshness.json`, surface `QueryResult`'s failure state at the boundary, close firewall residue) — because the "minimum hardening" the strategy doc frames as a big build is ~90% already done in `dail_tracker_core` and `components.py`. **Build first:** the read-only Procurement page over the five existing views (pure surfacing, no ingestion) plus, in priority order, the SSHA promotion-with-caveat, CBI warning notices, DPC fines, and ERDF beneficiaries — each completing a half-built surface and honouring never-union money grains. **Reject:** the entire board-minutes / FOI-log / protected-disclosure web and the CBI Dear-CEO/AML/due-diligence layer (off-mission, PII-laden, inference-bait, ~0% join), re-ingesting eTenders open-data (already the live source), the invented SSHA/value-legend/QueryResult re-builds (already exist or conflict with built shapes), any AFS-as-total-spend or TD-housing-performance framing, and re-opening the settled framework question.
