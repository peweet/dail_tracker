# Bronze / Silver Data-Quality, Connectivity & Join Audit

**Date:** 2026-06-22
**Scope:** Every meaningful silver + gold parquet (229 files), all 218 SQL views, and the
bronze raw layer. Goals: (1) find poorly-parsed / neglected / un-cleaned data; (2) find SQL
views that connect to nothing and decide whether they should; (3) find unrealized cross-domain
join opportunities and surfaceable insights.

**Method.** A deterministic DuckDB profiler (`c:/tmp/dq_audit/profile_parquet.py`, reusable —
see *Instrument*) was run over all 229 parquet files, emitting per-column null/empty/distinct/
min/max/sentinel/numeric-in-varchar/date-collapse/untrimmed/constant flags (1,151 flags total,
in `c:/tmp/dq_audit/flags_digest.md`). View connectivity was computed by extracting every
`CREATE VIEW` (218) and subtracting every name token referenced in `.py` and in other views'
SQL. Cross-domain joins were measured empirically with normalized-name DuckDB joins, not
guessed. Every flag was triaged against the **no-inference / present-as-filed rule** before
being called a defect — as-filed register free-text, CRA bands (`NONE`/`NA`), CSO suppression
markers, and the by-design wide-flatten member columns are **not** defects and were dismissed.

> Sections 1–5 are read-only observations from the first pass. **Section 6 (continuation pass)**
> records a deeper, procurement-focused sweep AND the one fix that was applied: the `paid_flag`
> leak (§1 HIGH) is now FIXED at source + patched in the existing parquet (silver + gold), with
> tests. All other findings remain observations for the owner to action. Nothing was committed to
> git; the patch is reversible via `git checkout` on the parquet files.

---

## 1. Data-quality defects (severity-ranked)

### 🔴 HIGH — `paid_flag` column-misalignment leak in `public_payments_fact`
- **File:** `data/silver/parquet/public_payments_fact.parquet` (109,337 rows).
- **Defect:** of 109,337 rows, `paid_flag` is `NULL` for 76,457, a genuine flag
  (`Y/N/Paid/Not Paid/Part Paid`) for 14,911, and **mis-mapped content for 17,750 rows (16.2%)**.
  Three *different* source columns leak into this one field depending on publisher:
  - **descriptions** — `Building Mtce`, `Constr Contract`, `Fitouts` (dept_education: 5,728 rows /
    4,072 distinct; dept_children: 4,128 / 2,791)
  - **payment-month dates** — `Dec-21`, `Nov-23`, `Sep-23`
  - **free-text categories** — ie_tii (4,156 / 37), ie_opw (2,272 / 289), dept_health, ie_courts,
    ie_revenue (187 / 185 ≈ all unique).
- **Root cause:** `extractors/procurement_public_body_extract.py::_pick_roles`. The `paid`-role
  regex pattern includes "payment type", so it greedily claims category/date columns. The existing
  mitigation `_looks_descriptive()` (~line 1389) only swaps `paid → description` when `description`
  is *empty* and the column looks textual; it never handles the date-leak case and never fires when
  `description` is already populated — which is why the worst publishers still leak thousands of rows.
- **Impact:** category/description recall understated for those publishers (real category text
  trapped in `paid_flag`); `paid_flag` unusable as a flag for ~16% of rows. **Money is unaffected**
  (`amount_eur` is clean — no negative or text-trapped amounts anywhere in the corpus).
- **Fix direction:** add a post-consolidation cleanup pass — for any `paid_flag` that is neither a
  known flag token nor null, route date-like values to a month/period field, move descriptive values
  into `description` when `description` is null, else null the leak. Add a DQ guard asserting
  `paid_flag ∈ {known flag tokens} ∪ {NULL}` (extends the existing `test_dq_sentinel_sweep.py` style).

### 🟠 MEDIUM — neglected / never-populated real columns
Each is a *declared* column that should carry data but is 100% empty (or unmapped). These are
distinct from the by-design wide-flatten nulls (which were dismissed).

| Dataset | Column(s) | Evidence | Note |
|---|---|---|---|
| `data/gold/parquet/judiciary_bench.parquet` | `assignment`, `assignment_term` | 0 / 194 non-null | Which court/division a judge sits in is never captured → the judiciary feature can't show a judge's assignment. |
| `data/gold/parquet/sipo_donations.parquet` | `donor_irish_citizen` | all-null (74 rows) | **Civic-significant:** non-citizen/foreign donations are restricted under SIPO rules. The PaddleOCR ETL declares the field but never extracts it. |
| `data/silver/charities/annual_reports.parquet` | `gross_income_schools`, `gross_expenditure_schools` | all-null (82,894 rows) | School-charity income/expenditure split declared but never mapped from the CRA register. |
| `data/silver/parquet/etenders_live_tenders.parquet` | `cpv_code`, `cpv_division` | all-null (2,363 rows) | Live tenders carry no CPV → cannot be filtered/segmented by procurement category (known limitation; category falls back to a label heuristic). |
| `data/silver/committees/office_holders.parquet` | `start`, `end` | all-null (78 rows) | Office-holder tenure dates never resolve (the slot→date mapping in `committees_long_format_etl.py` produces nulls). |

### 🟡 LOW — `eu_tam_state_aid.beneficiary_name` UTF-8 mojibake
- **File:** `data/gold/parquet/eu_tam_state_aid.parquet`.
- 9 rows carry the U+FFFD replacement character in `beneficiary_name` — Irish-language company
  names (`COILLTE CUIDEACHTA GHN�OMHA�OCHTA AINMNITHE`, i.e. *Gníomhaíochta*) where á/í were lost.
  Small, but it breaks exact-name joins for those entities. Re-decode the EU register source as UTF-8.

### Dismissed as **by-design / as-filed** (reviewed, not defects)
- Lobbying-register free-text (`specific_details`, `intended_results`, `public_policy_area`,
  `person_primarily_responsible`) untrimmed/sentinel hits — protected by the no-inference rule
  (filers type `N/A`/`None`/whitespace themselves). ~400k of the 1,151 flags are this class.
- Wide flattened member columns (`flattened_members`, `flattened_seanad_members`) being mostly
  null/constant — by design (one column per Nth committee/office/party slot).
- CRA bands `NONE`/`NA` (employees/volunteers/reserves) — real bands meaning "zero", already
  allow-listed in `test_dq_sentinel_sweep.py`.
- CSO PxStat `VALUE`-in-varchar across `cso_*.parquet` — `:` confidentiality-suppression markers,
  by-design.
- Metadata `DATE_COLLAPSE` (`ETL_DATE`, `ingested_date`, `fetched_at`) and the committee `start`
  55.5% = 2025-05-07 (the 34th Dáil term start) — expected.
- `seanad_member_interests_combined.constituency` all-null — senators have no constituency.

---

## 2. Orphan SQL views (defined but consumed by nothing)

10 of 218 views are referenced **only in tests** — no page, data-access module, MCP tool, or other
view reads them. Recommendations:

| View | Defined in | Recommendation |
|---|---|---|
| `v_payments_by_category` | procurement/procurement_payments_by_category.sql | **connect** — category-level public-spend rollup belongs on the Council/Public-spending page as a "spend by category" breakdown. |
| `v_payments_by_category_publisher` | (same file) | **connect** — sibling of the above; not referenced even in tests. |
| `v_payments_category_suppliers` | (same file) | **connect** — the transparent drill (every euro per category→supplier); wire as the category drill-down. |
| `v_procurement_eu_tam_state_aid` | procurement/procurement_eu_tam_state_aid.sql | **wire_to_mcp / page** — EU state-aid lens (IDA disclosed-aid story); add a state-aid section or MCP tool. |
| `v_procurement_ted_awards` | procurement/procurement_ted_awards.sql | **investigate** — the page uses `ted_supplier_summary`/`ted_tenders`/`ted_winner_history` but not this awards listing; either wire it as the TED awards table or drop if superseded. |
| `v_procurement_live_tenders_summary` | procurement/procurement_live_tenders.sql | **connect** — headline open-tender count for the procurement page hero. |
| `v_corporate_cbi_enforcement` | corporate/corporate_cbi_enforcement.sql | **connect** — CBI enforcement actions; belongs on the corporate/Logic-firewall page (distinct from `v_corporate_cbi_distress`, which *is* used). |
| `v_corporate_isif_portfolio` | corporate/corporate_isif_portfolio.sql | **connect / wire_to_mcp** — ISIF investee portfolio; pairs with the new payments×ISIF join (§3). |
| `v_attendance_timeline` | attendance/attendance_timeline.sql | **connect** — member attendance timeline; candidate for the member-overview attendance section. |
| `v_gov_finance_annual` | publicfinance/publicfinance_gov_finance_annual.sql | **keep (parked)** — the code already documents it as "intentionally unwired (no page yet)". Lowest priority; build a public-finance page or drop. |

None are wrong or broken — they're finished analytical views that were never given a consumer.
The payments-category trio and the corporate (CBI/ISIF) and EU-state-aid views are the
highest-value wirings (each adds a real feature with data already in gold).

---

## 3. Unrealized join opportunities + surfaceable insights (measured)

Overlaps below were **measured** with normalized-name DuckDB joins (`c:/tmp/dq_audit/join_probe.py`),
not assumed. Existing overlap views (`procurement_lobbying_overlap`, `procurement_charity_overlap`,
`ministerial_diary_org_overlap`) were checked first — the joins below are **new** (0 existing views
join these tables).

| New join | Matched entities | Surfaceable insight (examples are real, from the data) |
|---|---|---|
| ~~payments/awards × corporate distress notices~~ | **0 / 1 (refuted)** | **Verification killed this lead.** Raw name-matches (Uniphar, Dunnes Stores) were *routine* `companies_act_notice`s, not distress, plus name-collisions (the "Dunnes" insolvencies are Dunnes Fireplaces / Dunnes North West Foods, not the retailer). Filtering to genuine `corporate_insolvency` (36,600 entities): **payments × insolvency = 0**, **awards × insolvency = 1** (Aigean Marine Teoranta — a *solvent* members' voluntary liquidation). The "state contracts with distressed firms" angle does **not** hold in the current data. *Lesson for any future join: always filter `corporate_notices` to `notice_category='corporate_insolvency'` and guard against name collisions.* |
| **payments × ISIF investees** | 4 | The state both **invests in and pays** the same company: **Staycity**, **Fexco**, **Panelto Foods**, **Gore Street Capital**. Double public exposure. (Pairs with orphan view `v_corporate_isif_portfolio`.) |
| **payments/awards × EU state-aid (eu_tam)** | 35 / 34 | Recipients of EU-approved state aid that also receive direct procurement spend — mostly universities/councils (ATU, MTU) but also private firms. Total public support per entity = aid + contracts. |
| **EU state-aid × lobbying clients** | 44 | State-aid beneficiaries that lobby: **Glanbia**, **Boliden Tara Mines**, **Coillte**, **Liberty Insurance**. |
| **payments × minister diary org-mentions** | 62 | Orgs a minister met (diary) that the minister's department then paid. Many are state bodies (NTMA, NAMA, EPA) but the private-sector matches are the story. (`diary × lobbying` exists; `diary × payments` does not.) |
| **awards × minister diary org-mentions** | 52 | e.g. **Mainstream Renewable Power**, **Hewlett Packard Enterprise** — met a minister and won a contract. |
| **charities (register) × payments** | 54 | Registered charities receiving public payments (Capuchin Day Centre, Limerick Enterprise Development Partnership…). Partly covered by `procurement_charity_overlap`; the payments-fact leg is new. |

**Headline candidate for surfacing:** **Western Building Systems** appears in *both* the payments
fact and the top-lobbying-clients list — the modular-schools contractor at the centre of the 2018
school structural-defects controversy. (Real-world verification in §4.)

**Data-thinness note:** `SIPO donors × {payments, awards, lobbying}` returned **0** matches. The
classic "donor-then-contract" angle is currently impossible because `sipo_donations` is only 74 rows
(OCR-sourced) and donor names don't normalize-match supplier names. Flagged as a coverage gap, not a
clean negative.

---

## 4. Real-world verification

Three of the headline leads were checked against external sources. **One was refuted** — which is
the point of verifying before surfacing.

**✅ Confirmed — Western Building Systems (payments × lobbying).** WBS (Tyrone-based) built ~42
schools for the Department of Education over ~14 years; structural defects were found in 23, the
Department sued the firm, and it was paid **€60m+** even after safety flaws were first flagged in
2015 — and it subsequently won a contract for a hospital block at UL (2019). Our data independently
shows WBS in both the payments fact and the top-lobbying-clients list. A real, defensible
"firm-in-controversy still inside the state's money/lobbying flows" story.
Sources: [Irish Times — paid €60m since 2015 flag](https://www.irishtimes.com/news/education/school-building-firm-paid-60m-since-safety-flaws-first-flagged-in-2015-1.3673458),
[Irish Times — Dept sues contractor](https://www.irishtimes.com/news/crime-and-law/courts/high-court/department-of-education-sues-contractor-over-alleged-defects-to-school-buildings-1.3734435),
[TheJournal — wins UL hospital contract](https://www.thejournal.ie/western-building-systems-ul-4772259-Aug2019/).

**✅ Confirmed — ISIF double-exposure (payments × ISIF investees).** The state both *invests in* and
*pays* the same firms. ISIF made a **€10m** investment in **Staycity** (Irish aparthotel group) and a
**€20m debt facility** to **Fexco** (payments/tech), both 2021 — and both also appear as suppliers in
the public-payments fact. Verifiable, low-effort to surface (pairs with orphan view
`v_corporate_isif_portfolio`).
Sources: [NTMA — ISIF Portfolio of Investments 2024 (PDF)](https://www.ntma.ie/annualreport2024/documents/Portfolio-Ireland-Strategic-Investment-fund.pdf),
[ISIF portfolio](https://isif.ie/portfolio).

**❌ Refuted — "state contracts with distressed companies" (payments/awards × distress notices).**
See §3: the raw matches were routine Companies Act notices and name collisions; the true overlap with
genuine insolvency is 0 (payments) / 1 solvent MVL (awards). Not surfaced.

**Net:** of the measured cross-domain associations, the **ISIF double-exposure** and **Western
Building Systems** items are real and surfaceable today; the EU-state-aid×lobbying (Glanbia, Coillte,
Boliden Tara Mines) and diary×payments overlaps are large and worth a deeper pass; the distress angle
is a dead end with current data.

---

## 5. Recommended next actions (priority order)
1. **Fix `paid_flag` leak** (HIGH) — cleanup pass + DQ guard. Recovers description/category text for
   dept_education, dept_children, ie_tii, ie_opw and ~14 other publishers.
2. **Wire the payments-category trio + corporate (CBI/ISIF) + EU-state-aid orphan views** — six
   finished views, data already in gold, each adds a real feature.
3. **Build the new "double-exposure" joins** (§3) — payments×ISIF, payments/awards×distress,
   payments×diary. Highest investigative value; low effort (entity-name normalized join, the
   project already has `name_norm`/`supplier_normalised`).
4. **Backfill neglected columns** where the source supports it — `donor_irish_citizen` (re-OCR /
   form-field map), `judiciary_bench.assignment`, `office_holders` tenure dates.
5. **Re-decode eu_tam source as UTF-8** (9 mojibake rows).

## Instrument (reusable)
- `c:/tmp/dq_audit/profile_parquet.py` — single-file DuckDB profiler (no polars dependency).
- `c:/tmp/dq_audit/run_corpus.py` — runs it over every silver+gold parquet → `corpus.json` + `flags_digest.md`.
- `c:/tmp/dq_audit/slice_domains.py` — slices the corpus into per-domain bundles.
- `c:/tmp/dq_audit/join_probe.py` — normalized cross-domain overlap measurement.
These should be promoted into `tools/` and run in CI (the bronze/silver complement to the
view-level `test_dq_sentinel_sweep.py`).

---

# 6. Continuation pass — procurement deep-dive + cross-domain consistency (2026-06-22 eve)

A second, deeper pass focused where the owner flagged most risk: **procurement**. Method: DuckDB
consistency invariants over the gold `procurement_payments_fact` (247,457 rows) + `procurement_awards`
+ the source parsers/regexes, then a cross-domain sweep (votes/attendance/charity/SIPO/legislation +
join integrity). Severity 🔴/🟠/🟡.

## 6.0 FIX APPLIED — `paid_flag` leak (§1 HIGH) is resolved
- **Root cause fixed at source:** new `extractors/_paid_flag_clean.py` (single-source cleaner) wired
  into `procurement_public_body_extract.py` (silver) and `procurement_payments_consolidate.py` (gold,
  before spend-category derivation so recovered text also categorises).
- **Existing data patched offline:** `tools/patch_paid_flag_misalignment.py` cleaned silver + gold
  without re-crawling. Gold result: **18,357 leaked values cleared, 2,394 category texts recovered into
  `description`**, `paid_flag` now flag-or-null only (distinct = {Y,N,Paid,Not Paid,Part Paid,P,…}).
  Row counts and `amount_eur` sums invariant (asserted). dept_readingorder silver also had 448 cleared.
- **Tests:** `test/extractors/test_paid_flag_clean.py` (6 unit + 2 integration) — green. Reversible via git.

## 6.0b FIXES APPLIED — courts scramble (P1) + procurement garble (2026-06-23/25)
A follow-up "why is it occurring / can it be salvaged / find more garble" pass fixed three more issues
end-to-end (parser + re-parse + re-consolidate, all reconciliation-checked, reversible via git):

1. **ie_courts column scramble (P1) — SALVAGED.** Root cause: the Courts "PO analysis report" PDFs are a
   5-field reading-order record whose PO number + supplier name merge onto one line for 2016+ quarters;
   x-coordinate bucketing then split the name (body→PO column, legal suffix→supplier), and name_norm
   reduced the suffix-only supplier to empty. A bespoke `read_courts` reader (graduated into the extractor
   as `reader="reading_order_courts"`, validated first in `pipeline_sandbox/courts_reader/`) recovers
   **4,702 rows, 0 empty suppliers** (was 852), real names + PO numbers. It also showed the **old gold was
   €1,261m incl. €554m phantom period-TOTAL rows + 9× €2 artifacts**; real total ≈ **€695m**. Re-parsed
   (`--only ie_courts --merge`) + re-consolidated.
2. **Unattributable blank rows — DROPPED (512 rows / €873.2m).** Period/section totals emitted as
   amount-only rows (no supplier, description OR PO): **ie_opw €155.78m, ie_prisons €74.0m,
   dept_social_protection €397m/48 quarterly totals, dept_health €102.6m, ie_ntma €111.6m/358**. They were
   already `value_safe_to_sum=False`, so **no summable total changed** (€40.63bn before and after) — they
   only polluted browsing as "€155.78m to (no payee)". New `_drop_unattributable` in consolidation (filters
   before the reconciliation baseline; also treats a description that is *only* the bled amount as blank).
   Result: ie_opw max €155.78m→€19.73m, ie_prisons €74m→€0.68m.
3. **Amount-bled descriptions — CLEANED (5,777 rows).** The amount duplicated into `description`
   (`€80,000,000.00 Third Level Building…`). `_strip_bled_amount` removes the leading amount ONLY when it
   equals `amount_eur` (keeps real specs like `70% Bitumen Emulsion` and code-prefixes); `amount_eur`
   untouched; spend-category coverage 93.0%→93.2%.

Tests: `test/extractors/test_procurement_garble_guards.py` (3 unit + 2 integration) — green; the full
569-test extractor suite passes. **Still spotted, recorded for supervised follow-up** (money mostly
correct): dept_defence po/description scramble (supplier intact; po holds unit names like `AIR CORPS`);
dept_education 2,138 null-year rows (GUID filenames defeat `period_from_url`); ~9,962 code-prefixed
descriptions; ie_la_sligo €79.9m Roadbridge / galway_city €31.5m single rows look like real capital, not
totals. Full log: `c:/tmp/dq_audit/procurement_findings.md`.

## 6.1 Procurement defects (the "is procurement a mess?" answer)
The generic public-body parser's **column-role heuristic (`_pick_roles`) is the systemic weak point** —
it mis-assigns columns per publisher layout. Manifestations:

| # | Sev | Finding | Evidence | Status |
|---|---|---|---|---|
| P1 | 🔴 | **ie_courts supplier attribution scrambled** | 4,487 rows; 852 empty supplier (€693.5m). Real name sits in `po_number` (`EPIQ EUROPE`, `IPB INSURANCE`), `supplier_raw` holds only the suffix (`LIMITED`/`CLG`); populated rows are fragments (`LIMITED PARTNERSHIP`). | RECORDED — needs per-publisher reader + re-parse. **Misleading payee attribution.** |
| P7 | 🟠 | **ie_tusla `po_number` holds a payment DATE** | 14,991 rows match dd/mm/yyyy. PO field is wrong; a usable per-payment date is trapped there. | RECORDED |
| P2 | 🟠 | **8,314 rows (3.4%) have NULL year AND period** | dept_education 2,138, dept_defence 1,515, ie_marine 1,047, ie_la_meath 974… Root cause: `period_from_url` reads year only from the filename; GUID/hash URLs have none. Fix pattern exists (`period_from_text`, used for Mayo LA) but unwired here. | RECORDED |
| P3 | 🟠 | **7,829 rows empty `supplier_normalised`** (high-value) | ie_courts €693m (P1), dept_social_protection €428m/104 rows, ie_ntma €182.6m/887, ie_atu €120m, dept_defence €109m. Mix of stopword-only raw + genuine parser misses. | RECORDED |
| P4 | 🟡 | amount/number bled into `description` | 114 rows / 11 pubs (`€594,539.65…`, `2023`); overlaps P2 dept_education. | RECORDED |
| P5 | 🟡 | supplier fragments (len ≤2) | 3,982 rows: `UK`(105), `CO`(34), `SC`(24)… mix of legit (`BT`,`3M`) + split fragments. | RECORDED |
| P8 | 🟡 | awards 41% null `value_eur` | 26,003/62,763 null; 23,018 `value_shared_across_suppliers` — never sum naively. Likely inherent (TED/eTenders omit value). | RECORDED (caveat) |
| P9 | 🟡 | reading-order occasional supplier/desc mis-split | e.g. supplier `FORMERLY ACTION POINT` / desc `VIATEL TECHNOLOGY LIMITED`. Not systematic (0.2%). | RECORDED |
| P10 | 🟡 | `supplier_class='unknown'` = €2.05bn (5,243 rows) | conservative classifier; coverage note. | RECORDED |
| P11 | 🟡 | awards `t/a` normalisation artifacts | 651 truncated (`EIRCOM T A EIR BUSINESS`); `name_truncated` flag marks them. | RECORDED |

**Procurement positives (verified sound):** 0 true full-row duplicates; 0 public-body-class suppliers
flagged `value_safe_to_sum` (no public-transfer double-count — invariant holds); 0 CRO matches on ≤3-char
names (no obvious false positives); reading-order amounts plausible (0 zero, €1.7k–€28.3m); top amounts
real (Airbus €187m Air Corps, OPW €155m, NTA BusConnects €140m, BAM €116m); CRO match 48.2% (expected);
dept_children recovered to 22,100 rows/€4.14bn (the earlier 173/30k gap is fixed by commit 426475e). The
"€-as-�" console output is a cp1252 display artifact, **not** data mojibake.

## 6.2 Cross-domain consistency
| # | Sev | Finding | Evidence | Status |
|---|---|---|---|---|
| C1 | 🟠 | **charity income: 16 of 20 within-row anomalies unflagged** | `amount_implausible_flag` (median-over-filings, needs ≥3 filings) catches the extremes (Claddagh Watch €250.3bn, Senior Citizens Concern €134bn — both tiny charities) and correctly leaves HSE's real €27bn unflagged, BUT misses 16 internally-inconsistent rows (Bedford Row €321m/exp €488k, Seoda Beaga €223m/exp €24k, Rehab €151m/exp €142k). Add a within-row income≫expenditure test. Keep values as-filed; improve FLAGGING. | RECORDED |
| C2 | 🟡 | `vote_outcome='_'` on 760 rows | unparsed division result. | RECORDED |
| C3 | 🟡 | SI→bill match 390 vs 4,603 unmatched | mostly inherent (SIs derive from pre-corpus/EU Acts); recall check low priority. | RECORDED |
| C4 | 🟡 | planning: 136 negative decision-latency rows | all Sligo legacy app-numbers (08xxxxx); decision dated before receipt (source date swap), isolated. | RECORDED |

**Planning (495,632 rows) — sound:** decision logic internally consistent (0 granted-AND-refused, 0
decided-but-neither, 0 granted/refused-but-not-decided); classifier well-structured and the historical
**UNCONDITIONAL→Granted trap is correctly handled** (5,176 → Granted); geo 100% within Ireland bounds
(0 out-of-bounds — geometry quarantine holding). **Lobbying (1,132,386 rows) — sound:** 0 null
politician, 4,387 distinct, clean position taxonomy (TD 503,814 / Senator / Councillor 141,761 /
Minister / MEP…), 0 lobby-period end-before-start. **Encoding reconciled:** real U+FFFD mojibake exists
ONLY in `eu_tam.beneficiary_name` (9 rows); the `D�il`/`€`→`�` seen elsewhere is cp1252 console display,
data intact. Quick-sanity on judiciary (194 rows, 0 null names), corporate_notices (insolvency 44,201 vs
routine 5,562 — confirms the §3 distress-join correction), NOAC (31 LAs) showed no red flags.

**Remaining domains swept — all sound:** SIPO candidate expenses (473 rows; public+not-public=total holds, 0
sum-mismatch; the 116 non-reconciling rows are correctly flagged, not hidden; €0–49k plausible). **TED awards**
(13,341; 0 zero-tender rows — the prior `n_tenders_received=0` trap is RESOLVED; 0 multi-supplier-framework rows
flagged `value_safe_to_sum` — no double-count; €15.3bn max correctly flagged `is_pan_eu_outlier`). NOAC scorecard
(fire/roads % in [0,100]; revenue_balance −10.59%→4.32%, Sligo deficit matches). Diary engagements (110,100; 46
ministers; 1 null subject; entry_class well-distributed). No defects found in these.

## 6.4 Verdict
Procurement is **not a mess** — the money is right (amounts clean, no negatives, no public-transfer double-count,
no true duplicates, value_safe_to_sum invariants hold in both payments and TED) and the high-volume domains
(planning 495k, lobbying 1.1M, votes, participation) are internally consistent. The real procurement weakness is
**narrow and specific**: the generic public-body parser's column-role heuristic mis-maps columns for a handful of
publisher layouts (courts P1 = the one genuinely misleading case; tusla P7; the NULL-year/empty-supplier set
P2/P3). The paid_flag instance of that same class is now fixed. A per-publisher column-map override for ~5
publishers would close most of the remaining gap. Outside procurement, the only owner-decision item is the charity
within-row implausibility flag (C1).

**Consistency positives (recurring-bug class checked):** participation `turnout_pct ∈ [5.8,100]`, 0
out-of-range, 0 `missed>total` — the historically **recurring attendance-denominator bug is NOT present**
in current gold (prior fix holding); attendance `total_days vs sitting_days` 0 anomalies; votes 0 null
member/date across 1,843 divisions; payments `publisher_id` 0 null; member-key joins sound (the "missing"
codes are historic members, names denormalized so nothing lost); SIPO donations internally clean (thin).

## 6.3 Open questions for the owner (no harm done; recorded per instruction)
1. **ie_courts (P1):** fix needs a per-publisher column map + re-parse (network) — a pipeline change.
   Until then, should the €693m no-payee courts rows be suppressed/caveated in the UI to avoid showing
   "€693m to (no supplier)"? *(Misleading-attribution risk.)*
2. **charity flag (C1):** extend `amount_implausible_flag` with a within-row ratio test? (Refines an
   existing guard; regenerates charity silver.) The two billion-euro cases are already flagged.
3. **Systemic:** worth introducing a per-publisher column-map override table for the ~5 worst public-body
   layouts (courts, tusla, the NULL-year/empty-supplier set) rather than relying on the generic heuristic?

Full working logs: `c:/tmp/dq_audit/procurement_findings.md`, `c:/tmp/dq_audit/cross_domain_findings.md`.
