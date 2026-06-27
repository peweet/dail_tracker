# Remaining orphan-data homing plan (post-2026-06-26 wiring)

**Created:** 2026-06-26. Companion to `doc/TIER1_PROMOTION_PLAN.md` (NOAC + disclosed-PO) and
`doc/PAYMENTS_CATEGORY_LENS_DESIGN.md`. All data below is **validated** (row counts / columns / sample
values checked this session). "Wired this session" = shipped; the rest need a placement decision or a
follow-up. No gold writes; no git run.

## Already wired this session (for context)
- ✅ **Payments "What the money buys"** lens — 3 views → Public Payments page (new tab + `?category=`).
- ✅ **CBI enforcement** (`v_corporate_cbi_enforcement`) — corporate page "More views" (103 named fines,
  Bank of Ireland €100.5m … Coinbase €30.7m). Never summed.
- ✅ **EU State-Aid** (`v_procurement_eu_tam_state_aid`) — procurement "wins" section, new register option.
  Ranked by `aid_element_value`. **Carries a data-quality caveat** (see §5).

---

## 1. ISIF portfolio — needs a HOME DECISION
**Data (validated):** `v_corporate_isif_portfolio`, **213 investees**, columns `investee_name, commitment_date,
description, amount_stated, amount_currency (EUR/USD mix), amount_is_up_to, value_kind, value_safe_to_sum
(=False)`. ISIF sovereign-fund *investment commitments* (Staycity €10m, Avenue Capital €150m homebuilder fund…).
**Constraint:** mixed currency + "up to" + `value_safe_to_sum=False` → **render as a named list, NEVER a sum.**

**Why it has no clean home:** the corporate page is *distress* (receiverships/liquidations); ISIF is the
opposite (state *investing in* firms). It doesn't belong there.

**Options (pick one):**
- **(A, recommended) Follow-the-money page** as a "State as investor" lane. The data-quality audit already
  paired ISIF with the "state *invests in* AND *pays* the same firm" double-exposure angle — Follow-the-money
  is the body⇄supplier trail surface, so an investor lane fits its IA and unlocks that cross-ref.
- **(B) A small standalone "State investments" section** on a public-money page (e.g. Public Payments footer
  or a new minor surface). Simpler, but no cross-ref payoff.
- Effort: Low (1 core query + fetch + list renderer; data is small). Risk: Low. **Blocker = the IA choice only.**

## 2. National government finance (`v_gov_finance_annual`) — needs a HOME DECISION
**Data (validated):** revenue / expenditure / surplus-deficit per year **2018→2025** (2025 expenditure
€133.8bn). Clean. **This is the missing denominator** for every "% of total public spend" figure the app
currently can't anchor.

**Why it has no clean home:** there is **no public-finance page**. That's exactly why it's orphaned.

**Options (pick one):**
- **(A, recommended) A reusable "national context" strip** — a small shared component that renders
  "€X is N% of all government spending in <year>" wherever a big total appears (Public Payments header,
  Procurement lede). Highest leverage: it makes existing money figures legible without a new page.
- **(B) A minimal "Public finances" page** — the full revenue/expenditure/balance trend as its own surface.
  More complete, more work, lower immediate leverage.
- Effort: A = Low-Med (one component, 1-2 call sites); B = Med (new page + nav). Risk: Low. **Blocker = scope choice.**

## 3. SIPO campaign-spend cubes — ❌ ALREADY WIRED (orphan parquet is stale)
**Resolved 06-27:** `sipo_campaign_spend_by_party/category/detail.parquet` are **stale precomputes**.
The Election 2024 page already renders the identical data from **live views** — `candidate_by_party`
(FG €1,066,304.71 / 59 candidates — matches the cube to the cent) and `candidate_by_category` (the 8
statutory categories 5A–5H, drawn by `_category_bars`). **No work needed.** Delete the dead parquets.

## 4. Seanad payment rankings — ❌ ALREADY WIRED (orphan parquet is stale)
**Resolved 06-27:** `v_payments_alltime_ranking` is **house-partitioned** (`RANK() OVER (PARTITION BY
house …)`) and returns 61 Seanad rows; the Payments page renders them via the Dáil/Seanad toggle
(`fetch_alltime_ranking("Seanad")`). The `current_senator_payment_rankings.parquet` is a **stale
precompute**, superseded by the live view. **No work needed.** Delete the dead parquet.

> **Audit caveat (important):** every "orphan gold parquet" flagged by the original audit that was
> checked (AFS-per-council, Seanad rankings, SIPO ×3, top_client_companies) turned out to be **already
> wired via a live view** — the parquet was a precompute abandoned during the logic-firewall migration.
> A parquet with no view is NOT a feature gap. The orphan-**view** findings were reliable; the
> orphan-**parquet** findings were not.

## 5. EU State-Aid data-quality — ✅ FIXED (2026-06-27)
Root cause was a **source artifact**, not a parse bug: the EC register records a scheme's whole budget
against one beneficiary (`aid_element_raw` literally reads `2,767,727,677 EUR` for "Clifford Brothers
Ardfert Potatoes" under SA.105798 — a 139-award horticulture scheme, median €38,950; the same firm also
appears correctly at €10,628). NBI's €2.977bn is legitimate (the only award in its scheme SA.54472).
**Fix:** `v_procurement_eu_tam_state_aid` now computes `aid_element_suspect_scheme_total` (a window flag:
the single largest in its scheme by >100×, ≥€100m, scheme has ≥2 awards) — flags exactly the 1 bad row,
never NBI; the page sets flagged rows aside and notes the count. Ranking now leads with NBI.

## 6. `v_procurement_live_tenders_summary` — recommend DEFER (low marginal value)
The **base** `v_procurement_live_tenders` is **already consumed** (the "Open right now" section). Only the
per-buyer *summary rollup* is orphan, and its `est_value_floor_eur` carries junk (1.0 / NaN) values. It would
add a "who's buying most right now" strip — minor, and the data needs cleaning first. **Recommend: skip** unless
a buyer-leaderboard is specifically wanted; note it so it isn't mistaken for missing coverage.

## 7. AFS — national layer ✅ WIRED (2026-06-27); discoverability still open
- **National amalgamated AFS — DONE.** `afs_amalgamated_divisions.parquet` (already in tracked silver:
  64 rows = 8 service divisions × 2016–2023, all-31-LA audited) now has two views
  (`v_procurement_afs_national_by_division` / `_by_year` in `procurement_afs_national.sql`), core queries
  (`afs_national_by_division/by_year`), `fetch_afs_national_*_result`, and a `_render_afs_national()` strip
  rendered at the TOP of the **Council Spending** page: 2023 net cost by service (Environmental €565m,
  Recreation €483m, Roads €469m…), a net-cost-over-time spine, and the "€6.68bn gross / €1.78bn net
  taxpayer-funded" lede. A BUDGET grain, never summed with the PO euros.
- **Discoverability — ✅ already solved (verified 06-27).** The Council Spending page's council index cards
  link via `_paid_publisher_href` to the per-council dossier, which renders the AFS lanes for LAs. So the
  per-council AFS *is* discoverable there (now framed by the national strip above it). No entry point needed.

## 8. Dead-parquet cleanup — ⚠️ DO NOT bulk-delete (verified 06-27)
The "orphan gold parquet" set is NOT free dead weight. Verification of the 20 zero-code-reference parquets
found they are: (a) **actively produced by pipeline code** — e.g. `votes/enrich.py` writes
`current_senator_payment_rankings.parquet` every run; (b) **registered in `data/_meta/runtime_data_manifest.json`
+ `gold_quality_baseline.json` + `output_baseline.json`** — deleting one trips the DQ/output-regression guards;
and (c) several are **raw CSO source cubes** (`cso_*`) kept as source material, plus an intentional empty
quarantine (`seanad_payments_full_psa_quarantine`). Retiring any one is a *coordinated* change (remove its
producer + de-register from the manifest and both baselines + fix dependents like `sql_queries/test_sql.py`),
not a file deletion — and is not worth doing for raw source data. **Recommendation: leave them.** The "stale"
concern (misleading audits) is already handled by the audit-lesson note in memory.

---

## Recommended order
1. **Seanad rankings** (§4) + **SIPO cubes** (§3) — clear homes, low risk, fast.
2. **gov-finance national strip** (§2A) — high leverage, one component.
3. **ISIF → Follow-the-money** (§1A) — needs your nod on the home, then low effort.
4. **AFS national + discoverability** (§7), **EU-TAM source fix** (§5) — follow-ups.
5. **live-tenders summary** (§6) — defer.

**Two decisions I need from you:** (1) ISIF home — Follow-the-money lane (A) or standalone section (B)?
(2) gov-finance — reusable context strip (A) or a Public-finances page (B)?
