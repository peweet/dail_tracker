# Iris unsurfaced categories — real-world display-value assessment

**Date:** 2026-06-07
**Question:** of the Iris notice categories that land in silver but are **not** surfaced in a gold
table / UI page, which are worth displaying? Each was pressure-tested with online research and
judged on four axes.
**Companion:** the Irish-language sub-assessment is in
[IRIS_IRISH_LANGUAGE_ASSESSMENT.md](IRIS_IRISH_LANGUAGE_ASSESSMENT.md).

## The four axes (and the decisive one)

1. **Net-new?** — is this data already held elsewhere (gold SI, `si_current_state`, an existing page)?
2. **Civic/accountability value** — would the public/journalists care?
3. **Volume** — enough rows to justify a feature?
4. **Mission fit** — Dáil Tracker is a **parliamentary / public-accountability** tracker. Sectoral
   or industry data can be valuable yet **off-mission**. *This axis decides most calls.*

## Verdict table

| Category | Rows | What it is | Net-new? | Mission fit | Verdict |
|---|---|---|---|---|---|
| **Companies-Act corporate** | 713 | Schemes of arrangement, §509, insurance-company notices (missed by the comma-variant bug) | Yes | **On-mission** (extends existing Corporate page) | ✅ **BUILD** — recover into existing Corporate feature |
| **Local-authority speed-limit bye-laws** | ~250 of 458 | Council adoptions of special speed limits | Yes (no national aggregation exists) | Adjacent (local-gov accountability) | 🟡 **CANDIDATE** — timely, but off the core parliamentary mission |
| **Local-authority other bye-laws** | rest of 458 | Misc. council bye-laws | Partly | Adjacent | 🟡 bundle with above if built |
| **Fisheries management notices** | 432 | In-season catch/landing/gear limits by ICES area | Yes (operational, ≠ the Quota SIs) | **Off-mission** (industry/sectoral) | ❌ **SKIP** — real data, wrong audience |
| **NSAI Irish Standards** | 432 | Technical standards declarations/revocations | Yes | **Off-mission** (technical, sold commercially) | ❌ **SKIP** |
| **Limited partnerships** | 445 | LP dissolutions / interest assignments | Partly | Niche finance; **privacy-mixed** | ❌ **SKIP/DEFER** — thin + names individuals (pension trusts) |
| **Irish-language SI announcements** | 591 | Bilingual "Government today made Orders" | **No** (in gold or `si_current_state`) | n/a | ❌ **SKIP** (see companion doc) |
| **Iris member-interests notices** | 129 | Ethics/register-of-interests notices | Likely dup of the Interests page (register PDFs) | On-mission but duplicate | ❌ **SKIP/DEFER** |
| **Dormant accounts** | 11 | Dormant-account fund notices | Yes | Adjacent | ❌ **SKIP** — non-viable by volume |
| **RTFO determinations** | 3 | Renewable transport fuel obligation | Yes | Off-mission | ❌ **SKIP** — non-viable |
| **International agreements in force** | 3 | Treaties entered into force | No (DFA treaty series is the real source) | Adjacent | ❌ **SKIP** — non-viable + better source exists |
| **Bankruptcy / personal insolvency** | 3,850 | Individual adjudications | n/a | **Privacy-suppressed by policy** | ⛔ never surface ([[feedback_personal_insolvency_privacy]]) |

## The findings behind the calls

### ✅ Companies-Act corporate notices (713) — BUILD
Genuine corporate notices (schemes of arrangement, §509, insurance-company matters) missed only
because `has_companies_act` matches `"COMPANIES ACT 2014"` but not the comma form
`"COMPANIES ACT, 2014"` ([iris_oifigiuil_etl_polars.py:1018](../iris/iris_oifigiuil_etl_polars.py#L1018)).
They carry `IN THE MATTER OF X LIMITED`, so the existing corporate entity-extraction + privacy
filter already handle them. **On-mission** (corporate transparency is an existing chosen domain),
net-new, decent volume, near-zero design cost (feeds the existing page). *Highest-value, lowest-effort.*

### 🟡 Local-authority speed-limit bye-laws (~250) — CANDIDATE, defer
Timely and genuinely unaggregated: the 30 km/h urban rollout is being implemented council-by-council
via *special speed limit bye-laws* (a reserved function with statutory public consultation), targeted
operational by **31 March 2027** under the Road Safety Strategy. No one offers a **national** view of
every council's speed-limit changes — Iris is the one place they all surface, so an aggregated
tracker would be unique. *But* it's local-government, not parliamentary — adjacent to the mission,
not core. Build only if the app's scope deliberately widens to local government.
Sources: [gov.ie urban speed-limit review](https://www.gov.ie/en/department-of-transport/press-releases/minister-for-transport-directs-review-of-urban-speed-limits-to-enhance-road-safety/),
[Making of Speed Limit Bye-Laws (roadguidelines.ie)](https://www.roadguidelines.ie/wp-content/uploads/2025/10/Chapter-4-The-Making-of-Speed-Limit-Bye-Laws-September-2025.pdf).

### ❌ Fisheries management notices (432) — SKIP (off-mission)
A real, live stream: the Dept of Agriculture, Food & the Marine issues frequent management notices
setting catch/retention/landing limits and gear rules by ICES area, enforced by the Sea-Fisheries
Protection Authority — **distinct** from the annual Sea-Fisheries (Quotas) **Regulations** (which are
SIs already captured). So it *is* net-new operational data. But the audience is the fishing industry
and marine regulators, not parliamentary accountability — **off-mission**. Valuable data, wrong app.
Sources: [Fisheries Quota Management in Ireland (gov.ie)](https://assets.gov.ie/98545/0ec11577-ee8d-459f-8146-9bdf6eda5da2.pdf),
[SFPA Quotas](https://www.sfpa.ie/Statistics/Quotas).

### ❌ NSAI Irish Standards (432) — SKIP
Technical product/service standards from the National Standards Authority, developed for CEN/ISO
alignment and **sold commercially** via the NSAI standards shop. No accountability/transparency
angle; off-mission. Sources: [NSAI Standards](https://www.nsai.ie/standards/),
[NSAI Standards Store](https://shop.standards.ie/).

### ❌ Limited partnerships (445) — SKIP/DEFER
Ireland is a leading funds domicile and Investment Limited Partnerships (tax-transparent AIFs under
the 1994 Act as amended 2020) carry a genuine finance-transparency interest. **But** the substantive
data (partners, returns) goes to Revenue/Central Bank, not Iris; the Iris notices are thin
(dissolutions / interest assignments) and **privacy-mixed** — some name individuals via pension
trusts (*"The Trustees of The Pat Jones Pension Trust…"*). Not worth a feature; would need a
corporate-style suppression pass first. Sources:
[Investment Limited Partnerships (Revenue)](https://www.revenue.ie/en/companies-and-charities/financial-services/collective-investment-vehicles/investment-limited-partnerships.aspx),
[Investment Limited Partnerships Act 1994](https://www.irishstatutebook.ie/eli/1994/act/24/enacted/en/print.html).

### ❌ Tiny categories (dormant accounts 11, RTFO 3, international agreements 3) — SKIP
Volume alone disqualifies a feature. International agreements also have a better authoritative source
(the DFA Irish Treaty Series), so Iris adds nothing there.

## Updated recommendation (supersedes the broad "classify the unclassified" idea)

1. **Do P1 from the Irish-language doc** — route boilerplate to `publication_admin` (free quarantine cleanup).
2. **Recover the 713 Companies-Act corporate notices** into the existing Corporate page via the
   one-line comma-variant fix on `has_companies_act` — the single clear, on-mission, net-new win.
3. **Park speed-limit bye-laws** as the one *new-feature* candidate, contingent on a deliberate
   decision to widen scope to local government. If pursued, frame it as a national
   "where are speed limits changing?" tracker (unique aggregation, road-safety timely).
4. **Skip everything else** — off-mission (fisheries, NSAI), thin/privacy-mixed (LPs), non-viable by
   volume (dormant/RTFO/treaties), duplicate (Irish SIs, member-interests), or policy-suppressed
   (bankruptcy).

**Net:** of ~9 unsurfaced categories, exactly **one is a clear build** (Companies-Act corporate,
feeding an existing page) and **one is a deferred new-feature candidate** (speed limits). The rest
are correctly left in silver. The instinct that there's "a lot of unsurfaced Iris data" is true by
row-count but, judged on mission fit + net-new + volume, mostly correctly unsurfaced.

## Addendum — Machinery-of-Government: a lead that the validation downgraded

Decoding the Irish notices (see [IRIS_IRISH_LANGUAGE_ASSESSMENT.md](IRIS_IRISH_LANGUAGE_ASSESSMENT.md)
§"What these notices are in reality") showed they are mostly **delegation/transfer/rename SIs**, which
first looked like a novel, on-mission feature. **Validation against real-world equivalents reversed
that:** ministerial/department tenure history is already well-served (WhoGov, Wikidata,
TheyWorkForYou) **and already built in this app** (`v_member_ministerial_tenure`). The only
under-served sliver — functions delegated to junior ministers, sourced to the SI — is niche,
un-extracted (0/42), and already on irishstatutebook/vLex. **Net: not the prize it first appeared.**
Full evidence and the corrected verdict are in the deep-dive below.

## Deep-dive: the two build candidates compared (data reality + real-world handling)

### Choice A — Companies-Act corporate recovery

**Data we hold.** The 713 unclassified Companies-Act notices break down as: 703 carry
`IN THE MATTER OF … LIMITED` (entity extraction works), ~120 are bare `<60 char` fragments (junk),
89 insurance/assurance-company matters, 62 mergers/cross-border, 9 schemes of arrangement, 9 §509
(examinership). So **~590 substantive, recoverable** notices. They fit the **existing**
`corporate_notices.parquet` schema (35,966 rows: `notice_subtype, entity_name, brand_mentions,
parent_fund_mentions…`) and the existing Corporate page **exactly** — recovery is the one-line
`COMPANIES ACT, 2014` comma fix on `has_companies_act` ([:1018](../iris/iris_oifigiuil_etl_polars.py#L1018))
→ they flow through the existing enrichment.

**Real-world equivalent & how it's handled.** This is a **solved, comparator-validated** pattern:
[The Gazette (UK)](https://www.thegazette.co.uk/all-notices/content/116) surfaces corporate
insolvency + corporate notices in a structured, searchable product; OpenCorporates / Companies House
do company data. Our Corporate page is already the Irish equivalent. So Choice A is **completeness of
an established, externally-validated feature** — low risk, low effort, on-mission.

### Choice B — Machinery-of-Government view

**Data we hold (the constraint).** ~213 MoG SIs by title (`si_current_state`: 135 delegation +
51 transfer + 27 alteration; Iris gold has 42 with text). **But `si_minister_name` /
`si_minister_member_code` are 0/42 populated for these rows** (vs 42 % / 22 % across all SIs) — the
"who delegated which functions to which Minister of State" relationship is **in the SI body text,
not extracted**. So:
- **Tier 1 (buildable now, modest):** a *department-structure timeline* — every create/rename/
  transfer/delegation order by department + date + operation, linked to the SI. Title parsing only.
  Novel but somewhat dry.
- **Tier 2 (the compelling story, needs new extraction):** *which junior minister holds which
  delegated power* — requires parsing SI bodies and/or joining to the current gov.ie list, then to
  member codes for TD links. Real work; quality-uncertain.

**Real-world equivalent & how it's handled — CORRECTED (my "no one does this" claim was wrong).**
On validation, ministerial-role/portfolio history is **well-served**, and partly **already built in
this app**:

- **WhoGov dataset** (Nuffield/Oxford; Nyrup & Bramwell, *APSR*) — **58,000+ cabinet members, 177
  countries, 1966–2023**, with position, classification and **portfolio**. The canonical academic
  who-holds-which-ministry-when dataset; covers Ireland.
  [datafinder.qog.gu.se/dataset/wgov](https://datafinder.qog.gu.se/dataset/wgov),
  [APSR paper](https://www.cambridge.org/core/journals/american-political-science-review/article/abs/who-governs-a-new-global-dataset-on-members-of-cabinets/3AE11258F668EB95F5A9F6904EF80A45).
- **Wikidata** — models office tenure via `position held (P39)` with start/end qualifiers, explicitly
  queryable; *WikiProject every politician* targets senior-minister coverage.
  [WikiProject every politician](https://www.wikidata.org/wiki/Wikidata:WikiProject_every_politician).
- **mySociety / TheyWorkForYou** (our lodestar) — ships a `ministers.xml` (Parlparse) tracking
  *"all ministerial and parliamentary roles UK MPs have held"* with department.
  [mySociety datasets](https://data.mysociety.org/datasets/).
- **This app already has it.** `v_member_ministerial_tenure` (`ministerial_tenure.parquet`) is
  exactly "who ran each department, and when", **sourced from Wikidata**, with the
  `minister_on_date()` accountability primitive and `timeline()`/`current_ministers()` already in
  core ([ministerial.py](../dail_tracker_core/queries/ministerial.py)).

So **Tier 1 (department/minister tenure history) is not novel — it's solved, externally and
internally.** The *only* sliver that is plausibly under-served is **Tier 2's granular layer: the
specific statutory functions delegated to a named Minister of State, sourced to the delegation SI.**
WhoGov/Wikidata/TWFY track *positions* (titles), not the *delegated functions* conferred by the
orders. **But** that detail (a) lives in the SI body text we don't extract (0/42), (b) is already
legally accessible on irishstatutebook.ie / vLex, and (c) has thin demand. The honest read of "why
has no one built it": **not because it was overlooked — because the valuable layer is already done
and the remaining sliver is niche.**

### Verdict

| | A — Corporate recovery | B — Machinery-of-Government |
|---|---|---|
| Data ready | ✅ fits existing table+page; ~590 rows | ⚠️ titles+dates only; the power relationship is **un-extracted (0/42)** |
| Effort | **Low** (1-line fix + Iris re-run) | Tier 1 modest; **Tier 2 heavy** (new extraction) |
| Novelty | Low (completeness of existing feature) | **Low for Tier 1** (WhoGov/Wikidata/TWFY + our own `ministerial_tenure` already do it); only Tier 2's SI-delegation sliver is under-served — and that's niche |
| Real-world precedent | The Gazette, OpenCorporates, Companies House — strong | WhoGov, Wikidata, TheyWorkForYou — **strong** (claim corrected) |
| Risk | Low | Medium–high (extraction quality + **duplicates existing `v_member_ministerial_tenure`**) |

**Recommendation — REVISED after validation.** Ship **A now** (proven, low-risk completeness win).
**Downgrade B:** it is *not* the novel gap first claimed — the valuable layer (minister/department
tenure) is already solved externally **and already in this app** (`v_member_ministerial_tenure`),
and the only under-served sliver (functions delegated to junior ministers, per the SI) is niche,
un-extracted, and already on irishstatutebook/vLex. If anything, the cheap honest win here is a
**thin UI page over the existing `timeline()`/`current_ministers()` core functions** (a
"who ran each department, and when" view) — convenient, but explicitly *not* novel. Do not invest in
SI-body delegation extraction unless a specific accountability story demands it.

## Choice A — Companies-Act corporate recovery — ✅ IMPLEMENTED (2026-06-07)

**Shipped.** Recovered corporate notices that fell through to `other` into the existing Corporate
feature, via a new rule in `enrich_records` ([iris_oifigiuil_etl_polars.py:1094](../iris/iris_oifigiuil_etl_polars.py#L1094)),
gated on `notice_category == "other"` (so it can never steal SIs or perturb the MVL/CVL split).

**Actual result (the ~590 estimate was too high — corrected here):**
- Silver rebuild from cached bronze (`iris_silver_rebuild`): **clean 49,580 → 50,006 (+426)**,
  **quarantined 14,976 → 14,550 (−426)**, **`si_taxonomy` unchanged (6,933 → 6,933)** — confirms
  **no SIs were stolen**.
- Gold (`corporate_notices_enrichment --write`): **35,966 → 36,404**.
- Two recovery signals: a Companies-Act citation the strict flag missed (comma form / Assurance
  Acts / §509) **or** an `IN THE MATTER OF <X> LIMITED/DAC/PLC…` opener. Guards: body-length (drops
  bare page-shards), insolvency-verb exclusion (leaves liquidation/receiver to the insolvency
  rules), and a **global `LIMITED PARTNERSHIP` exclusion** (privacy — LPs can name individuals; the
  239 LP notices stay deferred).
- Privacy backstop intact: the enrichment's `_PERSONAL_INSOL_RE` still drops any bankruptcy-wording
  rows at gold-build (44 such rows in the corporate population were excluded).
- Tests: `test/iris/test_corporate_recovery_classification.py` (7, incl. SI-not-stolen,
  LP-excluded, fragment-excluded); full corporate+iris suite **56 passed**; ruff clean.

**Limited-partnership cleanup — ✅ DONE (2026-06-07).** Added a single global privacy chokepoint in
`corporate_notices_enrichment` next to the personal-insolvency exclusion: `_LIMITED_PARTNERSHIP_RE`
drops LP rows from **every** classification path (not just my rule). Gold **36,404 → 36,356**
(48 LP rows excluded across all paths), **0 LP remaining**; 297 personal-insolvency still excluded;
56 corporate+iris tests pass; ruff clean.

## Is the corporate-notice data actually valuable? (validated)

Honest answer: **as standalone insolvency monitoring, no — it's a commodity** already served better
elsewhere, and not for our audience. Iris is merely one raw feed those services aggregate:
- **Stubbs' Gazette** — Irish insolvency/judgment publication **since 1828**, subscribed by credit
  agencies, lawyers, debt/property firms, local authorities.
  [Stubbs' Gazette](https://www.stubbsgazette.ie/)
- **Vision-Net / CRIF** — commercial Irish company + insolvency + credit data, weekly.
  [vision-net.ie](https://www.vision-net.ie/)
- **Central Bank** publishes the births/insolvent-liquidations economic analysis.
  ([letter](https://www.centralbank.ie/docs/default-source/publications/economic-letters/vol-2020-no-13-irish-company-births-and-insolvent-liquidations-during-the-covid-19-shock-(mcgeever-sarchi-and-woods).pdf))

These track liquidations from "CRO submissions and notices published by CRIF Vision-Net, Iris
Oifigiúil, and Stubbs Gazette" — i.e. they already consume our source.

**The one genuinely on-mission, novel use — validated with a real join:** cross-reference insolvency
with the **political datasets the app uniquely holds**, via the existing CRO spine
(`cro_xref_corporate_notices` + `procurement_supplier_cro_match`). Joining on `company_num`:

> **112 distinct state contractors appear in insolvency/rescue notices (190 notices)** — incl.
> **Stobart Air (court winding-up, 2021)**. No commercial insolvency service frames this politically;
> it's taxpayer-exposure / contract-continuity accountability — exactly the app's mission.

Caveat (no-inference): ~half are `members_voluntary_liquidation` = **solvent** wind-ups (benign,
not failures); the accountability-interesting subset is CVL / court-winding / receivership. A feature
must distinguish them or it misleads. The richer cross-refs (insolvent company linked to a TD's
declared interest / a SIPO donor / a lobbying client) are more compelling still but need
**director-level** data (sparse/hard) — defer.

**Verdict:** the corporate-notices feature as a generic "who went bust" list adds little over Stubbs
/ Vision-Net. Its real, defensible value in *this* app is a **"state-contractor insolvency" view**
(procurement × insolvency on CRO) — grounded in a 112-company / 190-notice join — with MVL excluded
or clearly labelled. That is the thing worth building; the raw notices alone are not.

### Prototype exploration (2026-06-07) — `pipeline_sandbox/probe_state_contractor_insolvency.py`

Built a probe (sandbox only — no gold, no page) joining `procurement_awards` →
`procurement_supplier_cro_match` (exact_unique only) → `cro_xref_corporate_notices`, with honesty
rails (MVL/undetermined excluded, `value_safe_to_sum` only). What the data actually shows:

- **The MVL filter is everything.** 112 matched companies collapse to **27 genuinely distressed**
  (CVL / receivership / court winding-up / examinership / SCARP). The other 85 are solvent
  members'-voluntary or undetermined — counting them would have been misleading.
- **Quantifiable exposure is small and concentrated:** ~**€2.45M** safe-to-sum across the 27, most of
  it in ~6 firms (e.g. VIDAPPT €882k, **Rennicks Signs Ireland** €519k/24 awards/12 buyers → CVL,
  Fioru Software €261k/14 awards → CVL). Many show €0 because their awards are framework/call-off/
  shared (not safe to sum) — so award-value exposure is largely *unquantifiable* from this data.
- **Noise is real:** **3 of 27** have an award dated *after* the insolvency notice (e.g. Gutteridge
  Haskins & Davey, D.&E. McHugh) — a name-reuse / false-CRO-match smell that needs per-case
  verification. ~24 are genuine "won-a-contract-then-distressed".
- **Exposure is diffuse:** mostly local authorities (Dublin/Cork/Limerick/Sligo/Tipperary councils),
  ETBs, LGOPC, OPW — counts of 1–3 each.

**Prototype verdict:** the angle is **real, novel and on-mission** — but the dataset is **thin
(~27 cases) and noisy** (≈11% timing red-flags, name-match risk, most values not summable). It is
**not a statistical data product**; at most a **verified watchlist** of ~two dozen narrative cases
(Rennicks Signs is the strongest), each requiring manual review before publication (no-inference).
Recommend: keep as a probe / occasional journalistic lead, **not** a built page — unless paired with
director-level data to reach the higher-value TD/donor/lobbying connections, which remains deferred.

**Note:** silver CSVs and `corporate_notices.parquet` were regenerated locally (equivalent to a full
ETL re-run for rows in common, per `iris_silver_rebuild`). A normal pipeline run reproduces this.

---

### Original plan (for reference)

Recovers corporate notices that fell through to `other` into the existing Corporate feature.

### The change
**Classifier fix** — broaden `has_companies_act` at
[iris_oifigiuil_etl_polars.py:1018](../iris/iris_oifigiuil_etl_polars.py#L1018). Today it matches
only `"COMPANIES ACT 2014" / "COMPANIES ACTS 2014" / "THE COMPANIES ACTS"`, so it misses the comma
form (`"COMPANIES ACT, 2014"`), the assurance form (`"ASSURANCE COMPANIES ACT 1909"`) and
§509 references. Replace the literal list with a regex, e.g.
`t.str.contains(r"\bCOMPANIES ACTS?,?\s*(?:19|20)\d\d")` (keep `"THE COMPANIES ACTS"`). These rows
then fall through the **existing** `has_companies_act → corporate_notice` fallback
([:1047](../iris/iris_oifigiuil_etl_polars.py#L1047)) and flow to gold via the existing enrichment.

### Guards (must-haves)
1. **Fragment guard** — the ~120 bare `<60 char` "COMPANIES ACT, 2014" shards must NOT promote
   (they'd land as entity-less cards: the enrichment keep-filter only drops personal-insolvency, not
   empty rows — [corporate_notices_enrichment.py:106](../iris/corporate_notices_enrichment.py#L106)).
   Gate the new rule on a body signal: `raw_text` length ≥ ~60 **or** presence of
   `IN THE MATTER OF` / a company-form keyword.
2. **Privacy unchanged** — the personal-insolvency exclusion (`_PERSONAL_INSOL_RE`,
   [:101-107](../iris/corporate_notices_enrichment.py#L101-L107)) already runs on everything in scope,
   so recovered rows carrying bankruptcy wording are auto-excluded. Verify it still fires on the new rows.
3. **ICAV guard re-check** — the ICAV rule ([:1156](../iris/iris_oifigiuil_etl_polars.py#L1156)) is
   gated on `category=="other"`; the fix changes what stays "other", so confirm ICAV capture is
   unaffected.

### Validation (before commit)
- Category-count diff: `unclassified_other` drops ~590; `corporate_notice` rises correspondingly.
- Assert the 89 insurance / 62 merger / 9 scheme-of-arrangement / 9 §509 rows now classify corporate.
- Sample ~30 newly-promoted rows for **false positives** (notices that merely mention an Act but
  aren't corporate) — measure the rate; tighten the regex if >~5%.
- Confirm 0 personal-insolvency rows reach gold.
- Add a fixture test asserting `"… SECTION 509 OF THE COMPANIES ACT, 2014 …"` → `corporate_notice`.

### Re-run + surfacing
- Requires an **Iris ETL re-run** (cold rebuild, [[project_iris_cold_start_build]]) → re-run
  `corporate_notices_enrichment` → re-promote `corporate_notices.parquet` (35,966 → ~36,550).
- No SI-page impact (these aren't SIs). Corporate page row count rises ~1.5%; no schema/UI change —
  they render through the existing card path (null-entity rows already show "Company name not
  extracted in this notice").

### Effort / risk
One regex change + one guard + tests; the surfacing is free (existing schema + page). Main risk is
over-capture, bounded by the fallback-only routing, the entity/`bad_pat` filters, and the
false-positive sample check above.
