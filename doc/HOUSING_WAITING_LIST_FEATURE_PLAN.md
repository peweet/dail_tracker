# Feature plan — "Who's on the waiting list" (+ a national Housing screen)

Status: **PLAN** (not built). Extends the shipped housing work: supply (CSO),
need-total (SSHA waiting list), operations (NOAC) — all currently living only as
sections on the **Constituency** page. This plan adds the *composition* of housing
need ("who is waiting"), and proposes a dedicated **national Housing screen** as
its primary home.

## The three questions that prompted this

1. **Is there a breakdown by county?** — Yes, already. Every SSHA table is per
   **local authority** (31 LAs). LAs are county/city level; Dublin is 4 LAs (Dublin
   City, Fingal, South Dublin, DLR) and Cork is 2 (City, County), so a *true county*
   rollup sums those disjoint LA counts. So we can present at LA grain natively, and
   roll up to county trivially. No new extraction needed.

2. **Should we have a national screen?** — Strong yes (see §Placement). There is
   currently **no dedicated housing or national page** — housing is only a
   constituency-page section, which buries genuinely national data (a 61,719-person
   waiting list, county league tables) inside a per-area dossier.

3. **Did we use the CSO API to its full extent?** — **No — we use ~15% of it.** The
   extractor pulls **27 CSO PxStat tables (23 land in gold), but only 4 are
   referenced by any SQL view** (`cso_vac14`, `cso_hpm03`, `cso_ndq09`, `cso_gfa01`).
   Extracted-but-unsurfaced housing tables include: HAP scheme dynamics
   (HAP01/05/17/20/26/32), new-dwelling completion trends (NDA01/NDQ07), Census 2022
   housing-stock change (F2021) and **weekly rent by county (F2023B)**, RPPI price
   detail (HPM04/07/09), vacancy at LEA/ED grain (VAC15/16), and migration flows
   (PEA15). A national screen is the natural place to finally use them.

## Data inventory (all already in gold — nothing new to extract)

| Source | In gold | Surfaced today | This plan uses |
|---|---|---|---|
| SSHA waiting list | 9 tables (A1.1–A1.9), 31 LAs × 2024/25 | total + >4yr/>7yr only | time-dist, tenure, employment, household, citizenship |
| NOAC council perf. | 6 indicators | 5 (constituency cards) | (unchanged) |
| CSO PxStat | 23 parquets | 4 | F2023B rent, completions, HAP — phase 2 |

## The feature: "Who's on the waiting list"

Surface the *composition* of social-housing need, nationally and per county.

### Scope (confirmed): time-distribution + tenure + employment + household + citizenship
National 2025 headline values (already verified from gold):
- **Time on list** (A1.8): 35.2% waiting 4 yrs+, **18.9% over 7 yrs** (8 buckets).
- **Current tenure** (A1.7): 31% private rented · 26% with parents · 16% emergency/homeless.
- **Employment** (A1.2): 47% unemployed · 36% employed — the "working renter on the list" story.
- **Household** (A1.4): 61% single adult · 20% one adult + 1–2 children.
- **Citizenship** (A1.9): 71% Irish · 14% EEA · 13% non-EEA · 2% UK. **See §Citizenship.**

Held for a later pass (available, lower-signal or redundant): age (A1.1), income
source (A1.3, overlaps employment), main need (A1.5), accommodation requirement (A1.6).

### §Citizenship — sensitivity handling (the reason to "plan it out")
Citizenship of applicants is politically charged and easily weaponised. Rules for
including it without breaching the app's no-inference / present-verifiable-data-only
boundary ([[feedback_no_inference_in_app]], [[feedback_personal_insolvency_privacy]]):
- **Aggregate-only** — it already is (per-LA counts, no individuals). Never any
  person-level data. Safe on privacy grounds.
- **Neutral framing, no editorial.** Label exactly as the source does (Irish / EEA /
  non-EEA / UK citizen), cite "Housing Agency SSHA 2025", and **state the
  denominator clearly**: it's *main-applicant* citizenship as a share of qualified
  households — not "who gets housing", not immigration. A one-line factual caption.
- **Not a headline pill.** Place it *inside* the expandable breakdown alongside the
  other dimensions, not on the summary card — so it reads as one demographic facet
  among several, not a flagship number.
- **No derived ratios** (e.g. "X% non-Irish") in prose; show the four categories as a
  plain distribution and let the reader read it.

## Data model — new registered view(s)

`v_ssha_waiting_list_composition` — long/tidy, one row per
(la, year, dimension, category, count, pct_of_la_total). Built by UNION-ing the 5
SSHA wide tables into a common (dimension, category) shape with a `national` rollup
row (la = 'State') and per-LA rows. Pros: one view powers both the national screen
and any per-area slice; the page filters by `dimension`.

Companion `v_ssha_waiting_list_county` — the LA→county rollup (sum Dublin's 4,
Cork's 2) for a clean county league table.

Both follow the shipped pattern: explicit, gold-sourced, registered in dependency
order, swallow_errors-safe, guarded by the housing tripwire test.

## Placement — RECOMMENDATION: a national Housing screen (primary) + reuse on Constituency

- **Primary home: a new `Housing` page** (top-nav, url_path `housing`). It's national
  data; it deserves a national screen. Structure:
  1. **Need** — the waiting-list portrait (this feature): headline totals, the
     time-distribution bar, then the tenure/employment/household/citizenship
     breakdowns; a **county league table** (sortable: total, per-capita using CSO
     PEA08 population, % waiting 7yr+).
  2. **Supply** (phase 2) — finally use the unsurfaced CSO: completions trend
     (NDA01/NDQ09), vacancy (VAC14), median rent by county (**F2023B**), price (HPM03).
  3. **Delivery/performance** (phase 2) — NOAC national medians + county scorecard.
- **Secondary: keep the Constituency page slice.** The existing housing cards stay;
  add a compact "Who's waiting here" expander that reuses
  `v_ssha_waiting_list_composition` filtered to the serving council(s). No
  duplication — same view, different filter.

Rationale: the constituency expander answers "my area"; the national screen answers
"the country / compare counties" — the question a 61,719-row national dataset is
actually for. Building the view first means both surfaces are cheap.

## UX shape (per civic-ui conventions: cards on primary, no dataframes as primary)
- Headline: total waiting + YoY + the **18.9% over-7-years** as the lead civic number.
- **Time-on-list**: a single horizontal stacked bar (8 buckets, <6mo → 7yr+), the
  long-wait tail visually weighted. Native `st.html` bar or Altair (see display-data).
- **Each demographic dimension**: a compact card of labelled proportion bars (reuse
  the committee party-composition bar pattern noted as the best civic viz).
- **County league table**: secondary `st.dataframe` is acceptable here (drill-down,
  not primary) — sortable, with per-capita and >7yr columns.
- National ↔ county toggle via `?county=` query param (reuse spa_links soft-nav).

## Build phases
1. **View** `v_ssha_waiting_list_composition` (+ county rollup) + extend the tripwire
   test. Data-only, no UI — vet first.
2. **National Housing page** — Need section (this feature), incl. county table.
3. **Constituency expander** — reuse the view, filtered.
4. **Phase 2** — fold in the unsurfaced CSO supply tables + NOAC national scorecard
   (separate plan; this is where the "use CSO fully" work lands).

## Open decisions
1. **National page now, or constituency expander first?** (Rec: view first, then the
   national page — it's the higher-value home and makes the expander trivial.)
2. **County rollup vs raw LA grain** as the league-table unit? (Rec: offer both —
   default county, expandable to LA, since Dublin/Cork split matters.)
3. **Per-capita denominator** — use CSO PEA08 county population for "waiters per
   1,000"? (Rec: yes — it's the honest comparator and PEA08 is already in gold.)
4. **Phase-2 CSO scope** — which of the 19 unsurfaced tables are worth surfacing
   (F2023B rent + completions are the obvious wins; HPM04/VAC16 fine-grain may be
   noise). Needs its own scoping pass.

## Cross-references
- Shipped views: `sql_views/constituency/constituency_ssha_waiting_list.sql`,
  `constituency_council_housing_performance.sql`
- Extractors: `pipeline_sandbox/housing/ssha_appendix_wide_extract_experimental.py`,
  `noac_housing_wide_extract_experimental.py`, `extractors/cso_pxstat_extract.py`
- Tripwire: `test/sql_views/test_sql_views.py::test_constituency_housing_enrichment_views_build`
- Prior plans: `doc/archive/HOUSING_TIER_B_NOAC_PLAN.md`, `doc/SSHA_social_housing_summary.md`
