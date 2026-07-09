# Supplier Dossier — Analyst Fill-In Template

**Date:** 2026-07-08
**Status:** Template · Phase-0 (reports-first, hand-built for 3–5 paying design partners; no accounts/alerts/persistence)
**Cross-links:** [doc/BI_SPINOUT_ARCHITECTURE.md](../BI_SPINOUT_ARCHITECTURE.md) · [doc/PROCUREMENT_INTELLIGENCE_ROADMAP.md](../PROCUREMENT_INTELLIGENCE_ROADMAP.md)
**Canonical caveat source (render, never re-word):** [dail_tracker_core/caveats.py](../../dail_tracker_core/caveats.py) · export strings [api/routers/exports.py](../../api/routers/exports.py) · attribution [NOTICE.md](../../NOTICE.md)

This is a **fill-in-the-blanks analyst template**. Replace every `{{TOKEN}}` with the value returned by the named `data_access` call. Do not hand-compute aggregates in the report — the firewall requires every count/sum/join to come from a registered view via `utility/data_access/`. Any section marked **[requires Phase 2 view]** has no shipping function yet: leave it stubbed, do not invent a function or a number.

The **subject of this report is the company.** No person is ever the subject, scored, ranked, or profiled. There are **no scores, no verdicts, no risk/influence/concentration labels** anywhere — only stated facts with their denominators, grains, coverage windows, and caveats.

**Money-badge display strings — OWNER-SIGNED (do not choose in the template).** The five-label **grain taxonomy is fixed and binding** (see Never-break rail #1): **AWARDED** (national + EU/TED), **PAID**, **COMMITTED**, **PLANNED**, **BUDGET** — separately labelled cells, never summed. The **finer display wording on each money badge is an owner sign-off item** — render the tokens below, do not bake a string into the template:

| Token | Grain | Candidate wording (OPTIONS — owner signs the string table) |
|---|---|---|
| `{{OWNER_BADGE_AWARDED}}` | AWARDED (national) | "Award value" / "Awarded" / "Awarded ceiling — not money paid" |
| `{{OWNER_BADGE_CEILING}}` | AWARDED (framework/DPS ceiling) | "Framework ceiling" / "Source amount only" |
| `{{OWNER_BADGE_TED}}` | AWARDED (EU/TED) | "EU award value" / "Awarded (EU)" |
| `{{OWNER_BADGE_PAID}}` | PAID (SPENT) | "Cash paid" / "Paid" |
| `{{OWNER_BADGE_COMMITTED}}` | COMMITTED (ordered) | "PO committed" / "Ordered" |

Whether to add an explicit **"Not safe to sum" chip on the CEILING / TED cells** is also an owner decision (OPTION), not chosen here. Until the owner signs the string table, leave the badge wording tokenised — the fixed grain labels stay, only the finer wording defers.

---

## How to generate this report (call order)

All calls go through [utility/data_access/procurement_data.py](../../utility/data_access/procurement_data.py) (and `entity.py`/`public_payments.py` wrappers). Wrappers add `st.cache_data` and unwrap `.data`; the core signatures live in [dail_tracker_core/queries/](../../dail_tracker_core/queries/). Run in this order — step 1 yields the `supplier_norm` and `company_num` that later steps need.

| # | data_access call | Core fn / module | Grain | Feeds section |
|---|---|---|---|---|
| 1 | `fetch_supplier_summary_result(conn, ...)` | `supplier_summary` / proc | AWARDED | A1, A2, match pill |
| 2 | `fetch_entity_xref_result(conn, supplier_norm)` | `xref_summary` / ent | enrichment | A1, A7 |
| 3 | `fetch_entity_chain_for_company_result(conn, company_num)` — **company_num: str** | `entity_chain_for_company` / proc | multi-grain (NEVER sum) | A4 (centrepiece), A1 strip |
| 4 | `fetch_supplier_year_trend_result(conn, supplier_norm)` | `supplier_year_trend` / proc | AWARDED (per-year) | A2 |
| 5 | `fetch_incumbency_for_supplier_result(conn, supplier_norm)` | `incumbency_for_supplier` / proc | AWARDED (relationship) | A2, A3 |
| 6 | `fetch_dependency_for_supplier_result(conn, supplier_norm)` | `dependency_for_supplier` / proc | AWARDED (relationship, ≥5 awards) | A3 |
| 7 | `fetch_awards_for_supplier(conn, supplier_norm)` | `awards_for_supplier` / proc | AWARDED (row-level) | A5, A6 |
| 8 | *supplier-scoped per-publisher SPENT/COMMITTED* — **[requires Phase 2 view]** (`fetch_payments_supplier_summary_result` is an *unscoped top-60 leaderboard*, not per-supplier — do not filter it in the report) | `payments_supplier_summary` / proc | PAID / COMMITTED | A4 detail |
| 8b | `fetch_payment_lines_for_pair_result(conn, supplier_norm, publisher_name, tier=...)` | `payment_lines_for_pair` / proc | PAID / COMMITTED (leaf) | A4 detail |
| 9 | `fetch_competition_by_cpv_result(conn, ...)` | `competition_by_cpv` / proc | enrichment (TED 2024+) | A5 (category context) |
| 10 | `fetch_live_tenders_result(conn, sector=...)` · `fetch_ted_tenders_result(conn, sector=...)` | `live_tenders` · `ted_tenders` / proc | PLANNED | A6 |
| 11 | `fetch_charity_overlap_result(conn)` · `fetch_epa_compliance_result(conn, company_num)` — **company_num: int** | `charity_overlap` · `epa_compliance_for_supplier` / proc | enrichment | A7 |
| 12 | *(optional)* `fetch_lobbying_overlap_result(conn)` | `lobbying_overlap` / proc | enrichment (co-occurrence) | Optional panel |

**Landmine:** `company_num` is a **`str`** for `entity_chain_for_company` but an **`int`** for `epa_compliance_for_supplier` — same concept, different declared type. Cast accordingly.

---

## Cover

- **Supplier (report subject):** `{{SUPPLIER_LEGAL_NAME}}`
- **CRO number:** `{{CRO_NUMBER}}` · **normalised key (`supplier_norm`):** `{{SUPPLIER_NORM}}`
- **Prepared for:** `{{CLIENT_NAME}}` · **Prepared:** `{{REPORT_DATE}}`
- **Coverage windows:** eTenders/national `{{ETENDERS_WINDOW}}` · TED `{{TED_WINDOW}}` · Payment registers `{{PAYMENTS_WINDOW}}`

**Identity match confidence:** render as a single factual pill — the **owner-signed categorical tier label only**:

> `{{OWNER_TIER_LABEL}}`

Where `{{OWNER_TIER_LABEL}}` is the **owner-chosen wording** for the tier, mapped directly from the categorical `cro_match_method` view field (e.g. HARD / exact-name-unique / exact-name-ambiguous / name-only-cross-register — owner signs the string table; do not invent it). Do **not** compute or display a numeric confidence in the report: mapping the method string to `0.9`/`0.5`/`0.0` in the template would be a report-layer derivation of a figure that must come from a view column.
**Data source:** `fetch_supplier_summary_result` → `v_procurement_supplier_summary`, field `cro_match_method` (categorical); grain AWARDED. The **numeric `match_confidence` (0.9/0.5/0.0) and the `n_cro` ambiguity signal** are **[requires Phase 2 view]** — they exist in `procurement_supplier_cro_match.parquet` but are **not projected by either supplier view yet**. Surfacing them needs a two-view **additive SELECT** — in `procurement_supplier_summary.sql` **and** `procurement_supplier_year_summary.sql` — projecting `c.match_confidence` and `c.n_cro`. Leave `{{MATCH_CONFIDENCE_NUMERIC}}` stubbed until then.
**Owner gate (do not decide in the template):**
> - **Show the raw number, or only a tier?** Whether the pill shows the raw `0.9`/`0.5` confidence or only a categorical tier label is an owner sign-off item — not chosen here. Until signed off, render **only** the categorical tier label; the numeric value stays **[requires Phase 2 view]**. Do not present "— 0.9/0.5/0.0" as a settled part of the pill.
> - **Ambiguous-match policy.** Whether an **ambiguous match (`n_cro ≥ 2`) keeps showing an arbitrary `company_num` with a caveat, or is suppressed.** Until signed off, an ambiguous match must read as a *probable lead*, never a verified identity.

**Caveat to include (verbatim):**
> Grouped by name only; these *may be different legal entities* — confirm via the CRO number on each notice before treating them as one firm.

---

## A1 · Identity & register footprint (lead card)

*So-what (SME): "Is this the entity I think it is, is it still trading, and what is it?"*
*So-what (consultant): "Confirm the legal entity, its group, and which public registers it appears on before I write a word."*

**Fill-in prose.**
> `{{SUPPLIER_LEGAL_NAME}}` (CRO `{{CRO_NUMBER}}`) is recorded on the register as **`{{COMPANY_STATUS}}`**, company type `{{COMPANY_TYPE}}`, registered in `{{REGISTERED_COUNTY}}`. It appears across **`{{CROSS_REGISTER_COUNT}}`** public registers.
>
> Register-presence strip: Public procurement (eTenders) `{{IN_ETENDERS ✓/✗}}` · EU award notices (TED) `{{IN_TED ✓/✗}}` · Payment registers `{{IN_PAYMENTS ✓/✗}}` · Lobbying register `{{ON_LOBBYING_REGISTER ✓/✗}}` · Charity register `{{CHARITY_FLAG ✓/✗}}` · Corporate-distress records `{{CORP_NOTICE_FLAG ✓/✗}}` · EPA licence `{{HAS_EPA_LICENCE ✓/✗}}`.

No chart — a definition card + a chip row.

**Data source:**
- Identity fields (`company_num`, `company_status`, `cro_match_method`): `fetch_supplier_summary_result` → `v_procurement_supplier_summary`; grain **AWARDED**.
- Register flags/counts (`cross_register_count`, CRO/lobbying/charity/EPA/corporate-notice flags): `fetch_entity_xref_result` → `v_supplier_entity_xref`; grain **enrichment**. Use **`cross_register_count`** for the "appears across N public registers" statement — it is the full-footprint count that matches the 7-chip strip.
- Presence strip (`in_etenders`, `in_ted`, `in_payments`, `n_registers`): `fetch_entity_chain_for_company_result(company_num)` → grain **multi-grain enrichment (NEVER sum)**. **Note:** `n_registers` counts only the **procurement** registers spanned (etenders / ted / payments, max 3) — never use it for the headline "N public registers" line above; reserve it for procurement-register context only.
- **Corporate-distress chip is Iris-derived:** wherever `{{CORP_NOTICE_FLAG}}` renders, carry the Iris Oifigiúil attribution string + source URL (fact-only + attributed — see **Sources & attribution**).

**Caveat to include:** "Register presence is a record-existence fact only; it implies nothing about any award or outcome." Plus the CRO "may be different legal entities" string reproduced under **Cover**.

---

## A2 · Public-sector award footprint (grain: AWARDED)

*So-what (SME): "How big a player is this in public tenders, and is that growing or fading?"*
*So-what (consultant): "Sizing the incumbent's public book and its trajectory."*

**Fill-in prose.**
> `{{SUPPLIER_LEGAL_NAME}}` holds **`{{N_AWARDS}}`** award notices across **`{{N_AUTHORITIES}}`** buyers, first recorded `{{FIRST_AWARD_YEAR}}` and most recently `{{LAST_AWARD_YEAR}}`. Of these, `{{N_VALUE_SAFE_AWARDS}}` carry a sum-safe award value totalling **`{{AWARDED_VALUE_SAFE_EUR}}`** (`{{OWNER_BADGE_AWARDED}}`); a further `{{N_CEILING_NOTICES}}` are framework/DPS ceiling notices (`{{OWNER_BADGE_CEILING}}`) that are **not summed**.
> Top buyers: `{{TOP_BUYERS_TABLE}}` — one row per authority: authority · #awards · first year · last year · distinct years · awarded ceiling.

Charts (optional): award-**count** by year (bars, counts not €); a **separate, grain-labelled** "Awarded ceiling by year — not money paid" panel. Never combine the two.

**Data source:**
- Headline totals (`n_awards`, `n_authorities`, `n_value_safe_awards`, `awarded_value_safe_eur`, `n_ceiling_notices`): `fetch_supplier_summary_result` → `v_procurement_supplier_summary`; grain **AWARDED**. *(This view carries **no** supplier-wide first/last-award-year column — do **not** attribute `{{FIRST_AWARD_YEAR}}`/`{{LAST_AWARD_YEAR}}` to it.)*
- First / most-recent award year (`{{FIRST_AWARD_YEAR}}` / `{{LAST_AWARD_YEAR}}`): **[requires Phase 2 view]** — no supplier-scoped first/last-award-year column exists in `supplier_summary`. Candidate sources pending that view: the per-authority `first_year`/`last_year` on `fetch_incumbency_for_supplier_result`, or the earliest/latest `year` in `fetch_supplier_year_trend_result` — but a supplier-wide min/max is a rollup and belongs in a pipeline-owned view, not the report.
- Count/ceiling by year: `fetch_supplier_year_trend_result(supplier_norm)`; grain **AWARDED (per-year)**.
- Top-buyers table: `fetch_incumbency_for_supplier_result(supplier_norm, limit=8)` → grain **AWARDED (relationship)**.

**Caveat to include (verbatim — framework-ceiling never-sum):**
> AWARD values are ceilings/estimates, NOT money paid. Frameworks/DPS repeat one ceiling across supplier rows. Sum ONLY rows where value_safe_to_sum — and even that is awarded value, never expenditure. Upstream publishes quarterly with an inherent ~6-month lag (see data_currency).

---

## A3 · Category & buyer concentration (grain: AWARDED, counts)

*So-what (SME): "Is this a one-buyer specialist I can outflank elsewhere, or a broad incumbent?"*
*So-what (consultant): "Where is their moat, and where are they thin?"*

**Fill-in prose.**
> Of `{{TOTAL_AWARDS}}` award notices, **`{{TOP_AUTHORITY_SHARE_PCT}}%`** sit with `{{TOP_AUTHORITY}}` (`{{AWARDS_FROM_TOP_AUTHORITY}}` of `{{TOTAL_AWARDS}}` awards; `{{N_AUTHORITIES}}` distinct buyers in all). *[Buyer concentration is a descriptive count share, not a "dependency risk" label.]*
> Category (CPV) concentration: **[requires Phase 2 view]** — no supplier-level CPV-mix aggregate exists; do not compute it in the report. Row-level CPV codes are available for listing via `fetch_awards_for_supplier`.

Chart: horizontal bar of award-count share by buyer ("all others" bucket shown). Never a pie, never value across grains.

**Data source:**
- Buyer concentration: `fetch_dependency_for_supplier_result(supplier_norm)` → grain **AWARDED (relationship; only if ≥5 awards)**; fields `top_authority`, `top_authority_share_pct`, `awards_from_top_authority`, `total_awards`, `n_authorities`.
- CPV-mix share: **[requires Phase 2 view]** (nearest existing is authority-side `authority_category_mix`, also PHASE-2-TODO — neither is supplier-scoped).

**Caveat to include:** "Concentration is measured on award **counts**, not value or spend. A high share is a descriptive fact, not an assessment of the supplier or buyer."

---

## A4 · Awards-vs-payments footprint ⭐ (multi-grain, side by side — NEVER a total)

*So-what (SME): "Did the cash the incumbent actually received look like the contracts they won — and how much moves per year?"*
*So-what (consultant): "Reconcile the ceiling against the cash; find where the real recurring spend sits."*

Present the grains as **separate labelled cells in a single presence table — no total row, no derived ratio, no gap, no stacked bar.** Use the cross-register footprint (`entity_chain_for_company`) to lay awarded and paid **side by side with neutral coverage language.**

| Grain (own ledger) | Register | Value — its own ledger only | Coverage (presence) |
|---|---|---|---|
| **AWARDED (national)** | eTenders | `{{ETENDERS_AWARDED_VALUE_SAFE_EUR}}` (`{{OWNER_BADGE_AWARDED}}`) across `{{ETENDERS_AWARD_ROWS}}` rows / `{{ETENDERS_N_AUTHORITIES}}` buyers | `in_etenders = {{IN_ETENDERS}}` |
| **AWARDED (EU)** | TED | `{{TED_VALUE_SAFE_EUR}}` (`{{OWNER_BADGE_TED}}`) across `{{TED_AWARDS}}` notices / `{{TED_N_BUYERS}}` buyers | `in_ted = {{IN_TED}}` |
| **PAID (SPENT)** | Payment registers | `{{PAID_SAFE_EUR}}` (`{{OWNER_BADGE_PAID}}`) across `{{PAYMENT_LINES}}` lines / `{{PAYMENTS_N_PUBLISHERS}}` publishers | `in_payments = {{IN_PAYMENTS}}` |
| **COMMITTED (ordered)** | Payment registers | `{{COMMITTED_SAFE_EUR}}` (`{{OWNER_BADGE_COMMITTED}}`) | `in_payments = {{IN_PAYMENTS}}` |

The first-column **grain-taxonomy labels are the fixed never-break rail**; the value-badge wording (`{{OWNER_BADGE_*}}`) is **owner-signed** — see the money-badge string table at the top. Whether a **"Not safe to sum" chip** appears on the AWARDED / EU (ceiling / TED) cells is likewise an owner decision, not chosen here.

> Coverage note (neutral, mandatory): these are **four independent ledgers on different grains, shown side by side and never added, subtracted, or netted.** Payments may relate to awards not in the corpus, to sub-threshold purchasing, or to earlier contracts; awards may be frameworks that never draw down. Absence of a payment record is a **coverage** fact — not all bodies publish registers — **not** evidence of non-payment.

Per-publisher SPENT / COMMITTED detail (each tier its own cell, never blended): `{{PAYMENTS_SUPPLIER_TABLE}}` — publisher · realisation_tier · total_safe_eur. **A supplier-scoped per-publisher breakdown has no registered view today — [requires Phase 2 view]** (a `WHERE supplier_norm = …` per-publisher view). Do **not** reuse `fetch_payments_supplier_summary_result`: its signature is `(conn, *, tier=…, limit=60)`, an **unscoped top-60 per-supplier leaderboard** with no `supplier_norm` parameter — the target supplier may not appear in the top 60, and filtering/reshaping that frame to this supplier in the report would breach the logic firewall. For a **known (supplier, publisher) pair**, leaf lines come from `fetch_payment_lines_for_pair_result`.

Charts (optional): **paired panels** — "Awarded ceiling by year" | "Paid (SPENT) by year" — same x-axis, **separate y-axes, hard divider, two grain labels.** COMMITTED is a third, separately labelled panel. Never one axis, never a stack.

**Data source:**
- Side-by-side footprint: `fetch_entity_chain_for_company_result(company_num: str)` → grain **multi-grain (NEVER sum)**; fields `etenders_awarded_value_safe_eur`, `ted_value_safe_eur`, `paid_safe_eur`, `committed_safe_eur`, `in_etenders/in_ted/in_payments`, `n_registers`.
- Per-publisher SPENT / COMMITTED for this supplier: **[requires Phase 2 view]** — a supplier-scoped (`supplier_norm`-keyed) per-publisher registered view. `fetch_payments_supplier_summary_result(tier=…)` is an **unscoped top-60 leaderboard** over `v_procurement_payments_supplier_summary` and must **not** be filtered/reshaped to the supplier in the report.
- Leaf lines (known pair): `fetch_payment_lines_for_pair_result(supplier_norm, publisher_name, tier=...)`; grain **PAID / COMMITTED (line-level)**.

**Caveats to include (both verbatim):**

Money-grain master rule —
> procurement AWARDS, public-body PAYMENTS, and T&A allowances are three different value grains — NEVER sum across them

SPENT-vs-COMMITTED (payments) —
> SPENT (paid) and COMMITTED (ordered) are different tiers — never blend them in one total. VAT basis varies by publisher and is unconfirmed for most: see the vat_matrix reference in this manifest before comparing across publishers.

TED lane (do not union with national, do not total) —
> Pan-EU framework outliers (is_pan_eu_outlier) carry ceilings that dwarf the Irish market — exclude them from totals. Single-bid is a factual signal, never a verdict. Sum only value_safe_to_sum rows; never add to payments (different grain).

---

## A5 · Competition context — single-bid & field size (grain: AWARDED, counts)

*So-what (SME): "When this supplier wins, are they beating a field or walking in unopposed?"*
*So-what (consultant): "Where has the incumbent faced real competition, to price my client's bid?"*

**Fill-in prose.**
> Of `{{SUPPLIER_LEGAL_NAME}}`'s award notices, `{{N_BIDS_DISCLOSED}}` disclose a bidder count; the number-of-bidders distribution is `{{BID_COUNT_BUCKETS}}` (1 / 2–3 / 4+ / not disclosed). The **not-disclosed bucket is shown, not hidden.**
> Category-level competition context (the supplier's core CPV divisions): `{{COMPETITION_BY_CPV_TABLE}}` — cpv_division · single_bid_lot_pct · n_lots_with_bidcount (TED 2024+).
> Supplier-level single-bid **share**: **[requires Phase 2 view]** — no per-supplier single-bid aggregate exists; only the row-level `n_bids_received` from `fetch_awards_for_supplier` may be listed, never rolled up in the report.

Chart: bar of the supplier's awards by disclosed bidder-count bucket (row-level, from `awards_for_supplier`).

**Data source:**
- Row-level bid counts (`n_bids_received`, `competition_type`): `fetch_awards_for_supplier(supplier_norm)`; grain **AWARDED (row-level)**.
- Category context: `fetch_competition_by_cpv_result(min_lots=…)` → grain **enrichment (competition, TED 2024+)**. *(Per-buyer `competition()` exists in core but is **UNWIRED** — no `data_access` wrapper — do not call it from the report.)*
- Supplier single-bid share aggregate: **[requires Phase 2 view]**.

**Caveat to include (verbatim — competition):**
> single_bid_lot_pct = single-bid LOTS / lots-with-a-bid-count, from TED 2024+ award notices — each contract PART counted once (the honest lot-level rate; an earlier notice-level reading over-stated multi-lot buyers). A FACTUAL competition signal, NEVER a verdict: a single bidder is often legitimate — a niche/specialist supplier, bespoke research equipment, genuine urgency (research universities legitimately single-source a lot). It is the EU Single Market Scoreboard's procurement-integrity indicator: a prompt to look, not evidence of wrongdoing. Rank only buyers with a healthy n_lots_with_bidcount (min_lots default 40); small samples are noisy. Coverage is 2024+ only (the eForms era carries bid counts).

---

## A6 · Renewal cadence & pipeline signals (grain: AWARDED dates + PLANNED tenders)

*So-what (SME): "When does the incumbent's current book come back to market — when's my window?"*
*So-what (consultant): "Build the re-tender calendar."*

**Fill-in prose.**
> Contract end-dates for `{{SUPPLIER_LEGAL_NAME}}`'s awards (soonest first): `{{RENEWAL_TIMELINE}}` — dates only, no value; frameworks vs one-off split `{{FRAMEWORK_VS_ONEOFF}}`.
> Dedicated renewal-cycle analysis: **[requires Phase 2 view]** — `renewal_cycle` does not exist; the only structured signal is the `renewal_max` column carried inside the corpus-wide TED `fetch_expiring_contracts_result`, which is **not** supplier-filtered. Row-level `award_date` + `contract_duration_months` from `fetch_awards_for_supplier` may be **listed** but not aggregated into an estimated end-date in the report.
> Currently open opportunities in the supplier's core CPVs (a discovery aid, **PLANNED grain — an opportunity, not a transaction**): `{{OPEN_TENDERS_TABLE}}` — title · buyer · submission_deadline · estimated_value_eur · detail_url.

Chart: renewal timeline (Gantt of published end-dates, **dates only**); companion open-tender table.

**Data source:**
- Award dates / duration for listing: `fetch_awards_for_supplier(supplier_norm)`; grain **AWARDED (row-level)**.
- Renewal-cycle function: **[requires Phase 2 view]** (`renewal_cycle` PHASE-2-TODO).
- Open national pipeline: `fetch_live_tenders_result(sector=…)` → grain **PLANNED (national eTenders)**.
- Open EU pipeline: `fetch_ted_tenders_result(sector=…)` → grain **PLANNED (TED pre-award)**.

**Caveat to include:** "Contract end-dates are as published and frequently missing or nominal; frameworks may extend. Open-tender matching is by CPV/keyword and is a discovery aid, **not** a prediction of re-award. PLANNED estimates are never summed and are not spend."

---

## A7 · Cross-register footprint — corporate, charity, EPA (co-presence facts only)

*So-what (SME): "What else is publicly on record about this organisation as a corporate entity?"*
*So-what (consultant): "Complete the public-record picture — status, charity, EPA."*

**Fill-in prose.**
> Footprint (each row is the existence of a public record about the **organisation**, with a link): CRO status `{{COMPANY_STATUS}}`; corporate-distress records `{{CORP_NOTICE_FLAG ✓/✗}}` (`{{N_CORP_NOTICES}}`); charity register `{{CHARITY_FLAG ✓/✗}}` (`{{CHARITY_NAME}}`, gross income latest `{{GROSS_INCOME_LATEST_EUR}}`, gov-funded share `{{GOV_FUNDED_SHARE_LATEST}}`); EPA register `{{HAS_EPA_LICENCE ✓/✗}}` (`{{N_LICENCES}}` licences, `{{N_ENFORCEMENT_EVENTS}}` enforcement events).

Table only — register · present? · record count · link. **No network diagram, no influence visual** (a graph would imply connection and breach the no-framing rail).

**Data source:**
- Flags/counts + `cross_register_count`: `fetch_entity_xref_result(supplier_norm)` → `v_supplier_entity_xref`; grain **enrichment**.
- Charity detail: `fetch_charity_overlap_result(conn)` → grain **enrichment (co-occurrence via shared CRO)**.
- EPA detail: `fetch_epa_compliance_result(company_num: int)` → `v_procurement_epa_compliance`; grain **enrichment (NEVER sum)**.
- **Corporate-distress rows are Iris-derived (fact-only + attributed):** wherever `{{CORP_NOTICE_FLAG}}` / `{{N_CORP_NOTICES}}` render, carry the Iris Oifigiúil attribution string + source URL — per resolved owner decision (iii). See **Sources & attribution**.

**Caveat to include (verbatim — corporate notices):**
> corporate notices only (no individuals); a wind-up/receivership is a legal-status fact, not a verdict — and Members' Voluntary Liquidation is a SOLVENT wind-up, not distress

Plus: "Co-presence across registers is reported **without** asserting or implying any connection to any procurement award or outcome. No influence, access, advantage, or risk is measured, scored, or suggested." *(EPA and charity attribution/licence: owner to confirm — see Sources footer.)*

---

## A8 · Data lineage & unmatched records

*So-what (both): "How complete is this, and what did you leave out?"*

**Fill-in prose.**
> Coverage windows: eTenders `{{ETENDERS_WINDOW}}` · TED `{{TED_WINDOW}}` · Payment registers `{{PAYMENTS_WINDOW}}`. Last refresh per feed: `{{REFRESH_DATES}}`.
> Sum-safety: `{{N_VALUE_SAFE_AWARDS}}` of `{{N_AWARDS}}` award notices are sum-safe; `{{N_CEILING_NOTICES}}` framework/DPS ceiling notices are excluded from any total.
> Unmatched records (shown, not silently dropped): `{{N_UNMATCHED_AWARDS}}` award / `{{N_UNMATCHED_PAYMENTS}}` payment rows could not be crosswalked to CRO `{{CRO_NUMBER}}`.

**Data source:** sum-safe / ceiling counts from `fetch_supplier_summary_result` (`n_value_safe_awards`, `n_ceiling_notices`); coverage windows / refresh from each source's `data_currency` metadata carried on the export manifest ([api/routers/exports.py](../../api/routers/exports.py)). No single aggregate function — assemble from the per-source manifest fields.

**Caveat to include:** the full grain-and-coverage statement, once, in full (reproduce the money-grain master rule and the payments-coverage note from A4).

---

## OPTIONAL PANEL · Lobbying-register co-occurrence

> **OPTIONAL — HIGHEST REPUTATIONAL-RISK FEATURE. Per-report, owner-gated only.** Include only with explicit owner sign-off on this specific report. Held to the same no-inference discipline as the free side. **Co-occurrence ONLY.** Raw counts as **separate labelled facts**; **never** a score, ranking, index, or "influence"/"access" number. The **company is the subject — never any politician**.
>
> **Scope reversal (2026-07-08):** **ministerial-diary access is OUT of the paid product entirely** (free civic only — see [architecture §4](../BI_SPINOUT_ARCHITECTURE.md) and [Fable §7](../BI_SPINOUT_FABLE_ASSESSMENT.md)). This panel is **lobbying-register co-occurrence only**. Do **not** add diary/minister-meeting data to a paid report; if a client asks, point them to the free Dáil Tracker civic pages.
>
> **Hard gates:** (a) **never** in bulk exports or the API — reports only; (b) the **award-€ figures must NOT sit in the same table/line as the lobbying count** — they already appear in Sections A1–A2; here, reference them, do not restate them beside the lobbying figure (co-locating composes the causation narrative the caveat denies).

**Fill-in prose.**
> `{{SUPPLIER_LEGAL_NAME}}` appears on the public **lobbying register**: `{{N_LOBBY_RETURNS}}` returns, side `{{LOBBY_SIDE}}` (registrant/client). *(The company's award footprint is reported separately in Sections A1–A2 and is not restated here.)*
> Verify every match against the free public register: **lobbying.ie** → `{{LOBBYING_REGISTER_URL | https://www.lobbying.ie}}`.

Table only — register · present? · raw lobbying count · link out. **No award-€ column in this table. No fused number.**

**Data source:** `fetch_lobbying_overlap_result(conn)` → grain **enrichment (co-occurrence; never sum)**; use only `lobby_name`, `lobby_side`, `n_lobby_returns` here. The `n_award_rows` / `n_authorities` / `awarded_value_safe_eur` fields it also returns are **deliberately NOT rendered in this panel** (they belong in A1–A2).

**Caveats to include (all verbatim):**

Lobbying co-occurrence —
> Co-occurrence by ENTITY only: each company appears on BOTH the public-procurement award register and the lobbying register. NOT evidence that lobbying influenced any contract — there is no shared key linking a specific lobby to a specific award. Exact normalised-name matching undercounts (subsidiary / trading-name variants are missed). awarded_value_safe_eur is a per-supplier total carried on each of that supplier's lobby entities — never sum it across the nested lobby_entities.

Register access —
> Co-occurrence on the public lobbying register only — NOT evidence of improper influence.

Export-parity note — **this panel is never exported** (reports-only, per the hard gate above), so no export string applies.

---

## Sources & attribution

Carry the matching row on every report that uses the source (licence · attribution · caveat travel with the data — you sell the compilation/software, never the underlying facts).

| Source | Licence | Attribution (verbatim) |
|---|---|---|
| eTenders / OGP (national awards) | CC-BY-4.0 | Contains Irish Public Sector Data (Office of Government Procurement) licensed under CC-BY 4.0. |
| Public-body payments | CC-BY-4.0 (per-publisher source lists; see source_landing_url per row) | Compiled from official payment/PO publications of each public body (Circular 07/2012 / FOI publication schemes). |
| TED / EU-OJ | EU open data (Commission Decision 2011/833/EU) | Contains information from TED (© European Union), reused under Decision 2011/833/EU. |
| CRO (Companies Registration Office) | CC BY 4.0 | Contains Irish Public Sector Data licensed under a Creative Commons Attribution 4.0 International (CC BY 4.0) licence. |
| Iris Oifigiúil *(corporate-distress rows — `{{CORP_NOTICE_FLAG}}` / `{{N_CORP_NOTICES}}`; mandatory wherever they render)* | **Government copyright — NOT open** (solicitor-checklist item; fact-only + attributed) | Contains public sector information from Iris Oifigiúil © Government of Ireland. Source: https://www.irisoifigiuil.ie/ |
| Charities Regulator *(if A7 charity row used)* | CC BY 4.0 | Contains Irish Public Sector Data licensed under a Creative Commons Attribution 4.0 International (CC BY 4.0) licence. |
| lobbying.ie / SIPO *(optional panel — reports only, never exported)* | PSI re-use | Contains lobbying register data © Standards in Public Office Commission, reused under its PSI re-use policy. |
| EPA register *(if A7 EPA row used)* | **[owner to confirm — not in NOTICE.md]** | **[owner to confirm attribution string]** |
| ~~Ministerial diaries~~ | — | **Removed 2026-07-08 — diary access is free-civic-only, never in a paid report ([architecture §4](../BI_SPINOUT_ARCHITECTURE.md)).** |

---

## Never-break rails (binding on this report)

1. **Three (five-label) money grains never summed, unioned, or stacked.** AWARDED (national + TED), PAID, COMMITTED, PLANNED, BUDGET are **separately labelled cells** — no combined total, no stacked bar, no running sum, no side-by-side arithmetic implying one number. TED is a separate lane from national; never a combined "total awarded." Only sum **within** a grain and only where `value_safe_to_sum`. **The five-label grain taxonomy is fixed and binding; the finer money-badge display wording (and any "Not safe to sum" chip on CEILING/TED) is owner-signed — render the `{{OWNER_BADGE_*}}` tokens from the money-badge string table, never bake a string.**
2. **Awarded-vs-paid is shown side by side with neutral coverage language and NO derived ratio, gap, or netting** (A4). Absence of a payment record is a coverage fact, not non-payment.
3. **No scores, verdicts, rankings, or indices** — no influence/access/risk/concentration/competitiveness score; single-bid, incumbency, dependency, renewal, and cross-register facts stay **facts with denominators + their caveats**. Forbidden as claims in prose: *influence, corruption, waste, rigged, captured, influence-bought*.
4. **The company is always the subject.** Never a named individual, director, or beneficial owner. **Company-class / PII double-gate** inherited: exclude any row where `supplier_class = 'sole_trader_or_individual'`, `public_display = FALSE`, or `privacy_status = 'review_personal_data'`; no re-identification of person rows.
5. **Logic firewall.** Every count/sum/join/reshape comes from a registered view via `dail_tracker_core.queries` / `utility/data_access/`. If a figure needs a new aggregate, extend a **pipeline-owned view** — never compute it in this report. Sections marked **[requires Phase 2 view]** stay stubbed.
6. **Attribution, licence, caveat, and data_currency travel with every figure** — including exports. You sell curation/software, never the underlying facts. Iris-derived corporate-distress is **fact-only + attributed**: carry the Iris Oifigiúil acknowledgement string + URL on every use.
7. **Optional lobbying-co-occurrence panel** is opt-in, per-report owner-gated, highest reputational-risk, and **reports-only (never exported/API)**: raw lobbying counts only, company-as-subject, award-€ kept out of the lobbying table, `caveats.PROC_LOBBY` verbatim, link out to the free public register. **Ministerial-diary data is excluded from paid reports entirely** (free civic only).
8. **Caveat text is the user's domain** — reproduced verbatim from [dail_tracker_core/caveats.py](../../dail_tracker_core/caveats.py) / [api/routers/exports.py](../../api/routers/exports.py) / [NOTICE.md](../../NOTICE.md), **never re-worded or softened** below current strength.