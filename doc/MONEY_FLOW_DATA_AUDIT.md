# Money-Flow Data — Audit & Promotion Design

**Status:** audit + design (no code changes). **Created:** 2026-06-13.
**Scope:** every dataset that answers *"where does public money go?"* — procurement (eTenders + TED),
public-body payments, per-LA AFS budgets, LA payments, PSA payments to politicians, SIPO political
finance, charities, and the lobbying/revolving-door bridges.
**Companion to:** `doc/PUBLIC_PAYMENTS_FACT_SCHEMA.md` (the target unified model — this audit measures
actual-vs-target against it), `doc/PROCUREMENT_MASTER.md` (verified procurement figures + honesty rails).

> **One-line verdict.** The *model* is right (OCDS tiers + Kimball conformed dims are already designed
> and largely implemented in `procurement_payments_fact`). The *mess* is that the model isn't finished
> converging: **two overlapping payment facts**, a **consolidation gap hiding ~€2.9bn**, **supplier
> identity (CRO) matched in three different places**, and **no single citizen surface** that ties a
> company or a department to all its money lines while keeping the lifecycle tiers apart. All fixable
> without re-architecting.

---

## 0a. STATUS UPDATE — central-department ingest (2026-06-13, verified 14:46 gold)

**The central-department ingest (win #11) is DONE.** `procurement_payments_fact` was re-consolidated
today (file stamped 14:46) and now carries **48 publishers / 158,893 rows** (was 44 / 148k at audit time).
The previously-absent central departments are in gold and **public-displayable** (they will surface in
`v_public_payments`):

| Dept | Rows | Sum-safe | Tier | Top vendors (the consultancy/IT spend the media flagged) |
|---|---:|---:|---|---|
| Education & Youth | 5,815 | €2,563m | paid | Rhatigan, BAM/JJ Rhatigan (school building), HEA |
| Social Protection | 3,589 | €428m | committed | **Deloitte €67m, PFH €57m, Micromail €53m, Accenture €40m** |
| Health | 1,325 | €80m | paid | TIFCO, Accenture, PHD Media |
| Agriculture | 109 | €7m | committed | Grant Thornton, Rothschild, ESRI (thin — partial harvest) |

Parse quality validated: blank-supplier "Sum:" rows correctly downgraded to low-conf and excluded from
totals; max legitimate single rows are real (NTA BusConnects €140.6m → Graham; NPHDB children's hospital
€107.6m → BAM). Tier split holds: **paid €13.06bn / committed €8.41bn** (never blended).

**RESOLVED (2026-06-13, later same day):**
1. ✅ **§2.1 closed — 12 bodies recovered.** Re-parsed via `--merge --only` (Defence €1,129m, Climate
   €1,321m, Culture €203m, Beaumont €202m, TU Dublin €120m, Pobal, MTU, CHI, NLI, Sport Ireland, HPRA, CCPC)
   and re-consolidated. Gold is now **60 publishers / 193,221 rows** (was 48 / 158,893). **No body missing.**
2. ✅ **DQ bugs fixed.** OPW €155.8m blank-supplier row removed by re-parsing OPW (stale pre-guard row, not
   a code bug). A further **€702.5m of blank-supplier rows** across 14 kept publishers (NTMA, ATU, ESB, Courts,
   Housing…) — stale rows carried through `--merge` since before the 2026-06-05 guard — were cleared by
   re-parsing those publishers. **Defense-in-depth added:** `procurement_payments_consolidate.py` `_conform`
   now enforces the sum-safe invariant at the fold (blank-supplier + public_body recipient → not summable),
   so no source fact can ever leak again (no €-cap there — real €100m+ infra payments like NTA BusConnects
   €140.6m / children's hospital €107.6m stay summable). Verified gold: **0 blank-supplier, 0 public_body,
   0 UNKNOWN-tier sum-safe rows.** Clean tier split: **SPENT €13.99bn / COMMITTED €11.01bn** (public, never blended).
3. ✅ **Uplift — keystone contract test shipped.** `test/extractors/test_procurement_payments_fact.py` +5
   tests: sum-safe rows must have an identifiable supplier, be single known tier, positive amount, never a
   public_body transfer; plus a regression guard that the core money-flow bodies (Defence/Climate/central
   depts/Beaumont) stay present & displayable. **16/16 + 67 adjacent procurement tests green.**
4. ✅ **Reading-order departments ingested (DFAT/Justice/Transport).** Bespoke
   `extractors/procurement_dept_readingorder_parser.py` wired into `SOURCE_FACTS`. Gold = **63
   publishers / 207,504 rows**; +€2.35bn raw sum-safe (Justice €1.47bn / Transport €486m / DFAT €394m), clean
   parse (0 blank-supplier, 0 ≥€100m garbage). Newly visible: asylum-accommodation (Mosney, Millstreet),
   De La Rue (passports), BearingPoint, Bristow/CHC (SAR helicopters). **83 tests green.**
   - ⚠️ **Over-quarantine (display-side):** companies published without a legal suffix (De La Rue €101.5m,
     BearingPoint €51.3m, An Post €47.5m) are classed `sole_trader_or_individual` → hidden from the public
     view (DFAT loses 65% / €256.9m of displayable spend). Data is captured & correct; fix = better supplier
     classification (CRO fuzzy / known-entity), NOT a privacy relaxation.
5. ✅ **Spend-CATEGORY lens (source-grounded, no inference).** New pipeline-owned `spend_category` column on
   the gold payment fact (`canon_spend_category` in `procurement_payments_consolidate.py`) = the publisher's
   OWN published `description`, canonicalised ONLY for truncation + casing (leaked amounts/€ stripped,
   dangling connectors trimmed, acronyms preserved) — **never an invented taxonomy** ("department's exact
   words", owner decision). 84.5% coverage; nulls surface honestly as "Uncategorised". Registered views
   **`v_payments_by_category`** + **`v_payments_by_category_publisher`** (`sql_views/procurement/
   procurement_payments_by_category.sql`) — tier-separated, public+safe. Lifts asylum/direct-provision
   spend front-and-centre, unambiguously, in the department's words:
   **Justice → IP Accommodation €578m · Asylum Seeker Accommodation €208m · Ukraine Accommodation €149m.**
   Other source categories: School Building €2.13bn, IRCG/SAR Helicopter €228m, NBP Subsidy €803m,
   Pandemic Vaccine €213m. +6 contract/unit tests (88 total green).
   - **Category → vendor drill (full transparency):** `v_payments_category_suppliers` exposes every
     category's named vendors + amounts + tier, with `cro_company_num` surfaced. Reconciles to the euro
     against the category rollup (test guards no vendor dropped). Examples: *IP/Asylum Accommodation* →
     Cape Wrath Hotel €34m, Mosney €24m, Bridgestock, Millstreet Equestrian, Holiday Inn; *School Building*
     → Rhatigan ABM €426m, BAM, JJ Rhatigan; *SAR Helicopter* → Bristow €192m, CHC; *Pandemic Vaccine* →
     Pfizer €169m, Janssen, AstraZeneca. ⚠️ Vendor names are NOT operator-merged: "Bridgestock" (CRO
     342894) vs "Bridgestock Care" (587776) are different registrations, "Mosney" (CRO 11917) vs "Mosney
     Holidays" (no CRO) can't be verified as one — so each published name stays distinct (no guessing);
     the CRO column enables a downstream roll-up only where a match exists. True operator-merge = the
     deferred `dim_supplier` work.

---

## 0. How to read this doc

- **§1** — the data-trust scorecard (per dataset: grain, tier, is-it-trustworthy).
- **§2** — the structural problems, ranked, each with the evidence I observed in the parquet.
- **§3** — corrected headline figures (several cached numbers are stale).
- **§4** — the join / enrichment map (what connects to what, via which key, how clean).
- **§5** — the promotion workflow (silver→gold), staged, with the gates each step must pass.
- **§6** — the citizen "follow the money" view (IA + page design, not built).
- **§7** — immediate actions, in order.

Everything in §1–§4 was checked read-only with duckdb against the gold/silver parquet on 2026-06-13.
Where a memory note or doc disagreed with the parquet, the **parquet wins** and I say so.

---

## 1. Data-trust scorecard

Trust is scored 0 (never display) → 5 (citizen-ready). "Tier" is the OCDS lifecycle stage; euros at
different tiers **must never be summed together** (the project's #1 rule).

| Dataset | Rows | Grain | Tier | Money col (sum-safe?) | Trust | One-line verdict |
|---|---:|---|---|---|:--:|---|
| `procurement_awards` (gold) | 59,435 | notice × awarded supplier | **AWARDED** | `value_eur` (only when `value_safe_to_sum`) | **4** | Trustworthy *as counts*; €7.69bn sum-safe story holds; ceilings correctly excluded. Lead with counts. |
| `ted_ie_awards` (silver) | 13,341 | EU-notice × winner | **AWARDED** | `award_value_eur` (excl. `is_pan_eu_outlier`) | **3** | Good schema; CRO inline; never union/sum with eTenders (66% name overlap). Still silver. |
| `procurement_payments_fact` (gold) | 148,055 | published payment / PO line | **SPENT + COMMITTED** | `amount_eur` (when `value_safe_to_sum`, **within one tier**) | **4** | The canonical realised-spend fact, fully conformed (`value_kind`,`realisation_tier`,`vat_status`,`cro_company_num`). The backbone of "where it went" — *if* §2.1 is fixed. |
| `public_payments_fact` (gold) | 74,546 | payment / PO line | SPENT + COMMITTED | `amount_eur` (legacy `amount_semantics`) | **2** | **Should not be a surface.** It is an *input* to the consolidate. Legacy columns, no `realisation_tier`/`value_kind`/CRO. Holds 12 bodies the canonical fact is missing (§2.1). |
| `la_payments_fact` (silver) | 68,765 | LA over-€20k payment/PO | SPENT/COMMITTED | `amount_eur` (`value_safe_to_sum`) | **3** | Fully conformed; folded into the gold consolidate via `_load_la_fact`. Memory's "not wired" is **stale — it is wired.** |
| `hse_tusla_payments_fact` (gold/silver) | — | payment line | SPENT | `amount_eur` | **3** | Folded into the consolidate. |
| `la_afs_divisions` / `_capital` (silver) | 776 / ~158 | council × year × division | **BUDGET** (3rd grain) | `gross_expenditure` / `net_expenditure` | **3** | Council *budgets*, no supplier. Reconciles (21/21 capital). Context/denominator only — **never** in either fact. |
| `payments_full_psa` (gold) | — | politician × payment | **SPENT** (salaries/expenses) | reconciles gript.ie ~1.4% | **4** | Money *to politicians* — a distinct lane. Trustworthy; keep visually separate from contracts. |
| `sipo_donations` (gold) | 74 | donation | — (disclosure) | €161,578 / 7 parties | **4** | Exact-confirmed. Donor addresses = PII (exclude). |
| `sipo_campaign_spend_*` / `candidate_expenses_fact` | 446 expense rows | party/candidate × category | — | €5.35m candidate; 3 never-sum grains | **3** | OCR-sourced (trust ceiling); keystone view has 3 grains that must never be summed. Political-finance lane. |
| `charities_enriched` (gold) | 14,448 | charity (latest snapshot) | — (enrichment) | `gross_income_latest_eur`, `gov_funded_share_latest` | **2 (per-row) / 0 (totals)** | Per-charity fields usable; **no grand totals** — un-winsorised source has data-entry outliers. A *bridge/enrichment*, not a money fact. |
| `procurement_lobbying_overlap` (gold) | 182 | supplier × lobby-name match | — (co-occurrence) | `awarded_value_safe_eur` (**never sum**) | **2** | A bridge, not a fact. Rows fan out per match → naive sum over-counts ~67% (§3). No causation. |
| `revolving_door_dpos` (gold) | — | DPO ↔ politician link | — (co-occurrence) | n/a | **3** | Bridge; display with guard copy. |

**Takeaway:** the *facts* are mostly trustworthy at tier level. The trust problems are **structural**
(overlap, consolidation gap, identity scattered) and **presentational** (blended floors, naive sums),
not bad parsing.

---

## 2. Structural problems, ranked

### 2.1 ⛔ Two overlapping payment facts — and ~€2.9bn invisible (highest priority)

`public_payments_fact` (29 publishers) and `procurement_payments_fact` (44 publishers) are **not** a
clean subset relationship:

- **17 publishers appear in both, byte-identical** (e.g. Atlantic Technological University: same row
  counts and €m every year in both files). A naive consumer reading both would double-count these.
- **`procurement_payments_fact` is the intended canonical consolidated fact** — built by
  `extractors/procurement_payments_consolidate.py`, read by every public view
  (`v_public_payments*` in `sql_views/procurement/procurement_public_payments.sql`). `public_payments_fact`
  is one of its **inputs**, not a surface.
- **But 12 public bodies present in the input are missing from the consolidated gold** the UI reads:
  Beaumont Hospital, Children's Health Ireland (CHI), CCPC, Dept of Culture, Dept of Defence, Dept of
  Climate, HPRA, MTU, National Library of Ireland, Pobal, Sport Ireland, TU Dublin.
  **≈ €2.93bn** of sum-safe, public-displayable spend (before tier-splitting) is currently **invisible
  to citizens**.

Both gold facts were written 2026-06-12, so this is **not** simple staleness — it points at the
`--merge` mode (memory `project_procurement_publisher_coverage_gap`) carrying forward an older
consolidated base instead of re-pulling the full `public_payments_fact` input. The consolidate header
still says *"28 publishers… NO publisher overlap"*, which no longer matches reality.

**Fix:** run a **full** (non-merge) `procurement_payments_consolidate.py`, assert output publisher set ⊇
input publisher set, then **retire `public_payments_fact` as a readable gold file** (keep it only as a
staging input, or rename it `stg_public_payments`). One canonical payment fact, full stop.

### 2.2 Supplier identity (CRO) is matched in three different places

The "who is this company" key is scattered:

- eTenders: CRO lives in a **separate table** `procurement_supplier_cro_match` (join on `supplier_norm`).
- TED: CRO **inline** (`cro_company_num`).
- payments: CRO **inline** (`cro_company_num`) on `procurement_payments_fact`; **absent** on the legacy
  `public_payments_fact`.
- charities: CRO as `cro_number`.
- And the normalised-name column is spelled **three ways**: `supplier_norm` (awards, overlap, cro_match),
  `supplier_normalised` (payments), `winner_name_norm` (TED).

This blocks the single most valuable citizen feature — *one supplier profile across all its money lines*.

**Fix:** build `dim_supplier` **once** (the schema doc §A.3 already specifies this): one CRO match, one
canonical `supplier_norm`, carrying `supplier_class` + `name_truncated`. Every fact references it.

### 2.3 The blended "FLOOR" is the one number to police in the UI

`v_public_payments_supplier_summary` (`procurement_public_payments.sql:60`) **deliberately blends**
SPENT + COMMITTED into one `total_safe_eur` per supplier (≈ €14.58bn public-facing). The view header
labels it honestly as an *"indicative sum-safe FLOOR… never 'paid'"*. This is defensible **only** if the
page copy carries that neutral label.

**Fix:** audit `utility/pages_code/public_payments.py` to confirm this figure is never rendered as
"paid"/"spent". Prefer showing the tier-split publisher summary (which is correctly single-tier) as the
default, and the blended supplier floor only behind a labelled "all money lines (floor)" toggle.

### 2.4 AFS (budget) lives in the procurement view namespace — grain confusion risk

`la_afs_divisions` is a **third grain** (council × year × division, *no supplier*, BUDGET tier) but its
views sit under `sql_views/procurement/procurement_afs_*`. Nothing currently sums it into a fact, which
is correct — but the naming invites a future mistake. `procurement_afs_vs_po_coverage.sql` already does
the *right* thing (budget as denominator for payment coverage); keep that pattern and never let AFS
euros enter `fct_payment`.

### 2.5 Charity financials have no safe aggregate

`charities_enriched` is a **latest-snapshot** table (one row per charity, `*_latest` fields) and the
year-grain `v_charity_financials_by_year` reads from **silver**, *as filed, un-winsorised*. Per-charity
display is fine; **any "total charity income / total gov funding to charities" headline is corrupted**
by source data-entry outliers. Treat charities as an **enrichment/bridge** off `dim_supplier`
(via `cro_number`/`name_norm`), never as a money total.

### 2.6 TED is still silver; eTenders/TED must never be unioned

`ted_ie_awards` (13,341 rows) is well-conformed but sits in silver. 66% of TED winners also appear in
eTenders by normalised name → **never union or sum the two** (it would double-count the same award).
They meet only **per-firm, on the supplier dimension**, as cross-references.

---

## 3. Corrected headline figures (cached numbers that are now stale)

Verified against the parquet 2026-06-13. **Update memory** for the starred items.

| Figure | Cached / doc value | Observed now | Status |
|---|---|---|---|
| Procurement **awarded** sum-safe (company-gated) | €7.69bn / 8,074 ceilings excluded | **€7.69bn**, 7,129 awards, 8,074 ceiling notices excluded | ✅ CONFIRMED |
| Single-bid competition rate | 17.5% | **17.6%** (8,246 / 46,825) | ✅ CONFIRMED |
| SIPO 2024 party donations | 74 / €161,578 / 7 parties | **74 / €161,578 / 7** | ✅ CONFIRMED exactly |
| Supplier→CRO match rate | "45% clean" | **~53% `exact_unique`** (3,929/7,408); +240 `exact_ambiguous` must be excluded from "clean" | ⚠️ OFF (favourably) ★ |
| ★ Public realised **spend** total | "€8.21bn realised" | No single file yields it: `public_payments_fact` SPENT €7.55bn; `procurement_payments_fact` SPENT €13.23bn (gated €12.52bn) | ❌ STALE — figure names neither current fact ★ |
| ★ Procurement→lobbying overlap | €2.36bn naive / €1.497bn dedup | **€1.24bn naive / €0.74bn dedup** (file rebuilt → 182 rows / 132 suppliers) | ❌ STALE numbers; trap still real (~67% over-count) ★ |
| Privacy gate | enforced | `public_display=FALSE` quarantines 40,836 rows / €6.92bn from `procurement_payments_fact`; public views enforce `public_display=TRUE` | ✅ CONFIRMED enforced |
| Charity grand totals | — | **UNVERIFIABLE / do not print** (un-winsorised outliers) | ⛔ |

**Do not print, ever:** any cross-tier grand total; any AWARD + PAYMENT sum; any charity grand total;
any naive sum over the lobbying-overlap bridge.

---

## 4. Join / enrichment map

### 4.1 The spine: three conformed dimensions (build once, reference everywhere)

| Dimension | Canonical key | Present in (as) | Gap |
|---|---|---|---|
| **`dim_supplier`** | `supplier_norm` (+ `cro_company_num`) | awards (`supplier_norm`), payments (`supplier_normalised`), TED (`winner_name_norm`), overlap (`supplier_norm`), cro_match (`supplier_norm`), charities (`name_norm`/`cro_number`) | name column spelled 3 ways; CRO in 3 places (§2.2) |
| **`dim_buyer`** | `buyer_id` (+ `buyer_type`) | payments (`publisher_id`/`publisher_name`/`publisher_type`), awards (`Contracting Authority`), TED (`buyer_name`), AFS (`council`) | no shared `buyer_id`; awards/TED buyer names un-normalised |
| **`dim_cpv`** | `cpv_code` | awards (`Main Cpv Code`), TED (`cpv_code`/`cpv_division`) | payments have **no** CPV → "what was it for" only answerable on the AWARD side |

### 4.2 Safe joins (enrichment / co-occurrence) vs dangerous joins (would tempt a bad SUM)

| Edge | Key | Match quality | Citizen question it answers | Safe? |
|---|---|---|---|:--:|
| awards ↔ payments (per firm) | `supplier_norm` | high on company-class | "this firm was *awarded* X (ceiling) and *paid* Y" — shown as **two separate lines** | ✅ if tiers kept apart |
| eTenders ↔ TED (per firm) | `supplier_norm`/`winner_name_norm` | 66% name overlap | "full award footprint incl. EU-journal" | ✅ cross-ref only — **never union** |
| supplier ↔ CRO | `supplier_norm`→`company_num` | 53% exact-unique | "who legally is this company; still active?" | ✅ (exclude `exact_ambiguous`) |
| supplier ↔ charity | `cro_number` / `name_norm` | medium | "this recipient is a charity, X% gov-funded" | ✅ enrichment |
| supplier ↔ lobbying | `supplier_norm`/`lobby_name` | fan-out | "a contract-winner also lobbies" (no causation) | ⚠️ co-occurrence — **never sum `awarded_value_safe_eur`** |
| buyer ↔ AFS budget | `council`/`publisher` + `year` | LA-only | "council's payments vs its budgeted spend by division" | ✅ budget = denominator, not a fact |
| payments ↔ politician (revolving door) | DPO link | curated | "ex-official now at a paid supplier/DPO" | ⚠️ guard copy |

### 4.3 Top enrichment opportunities (citizen value × feasibility)

1. **Re-consolidate to recover the 12 missing bodies (§2.1)** — trivial, ~€2.9bn of visibility. *Do first.*
2. **Fold `procurement_supplier_cro_match` inline into `procurement_awards`** → unify supplier identity → enables #3.
3. **One `dim_supplier`** → the *supplier profile* page (the killer feature, §6).
4. **Supplier→charity bridge** → "is this recipient a charity, and how gov-dependent?"
5. **Buyer/department profile** → "where does Dept/Council X's money go" (payments by supplier, AWARD context, AFS budget for councils).
6. **CPV → plain-English service taxonomy** → "where money goes *by what it buys*" (award side only; flag payments have no CPV).
7. **AFS coverage ratio per council** (already prototyped in `procurement_afs_vs_po_coverage.sql`) → "we can see €X of this council's €Y budget at transaction level".
8. **Lobbying→award→payment chain** with the no-causation caveat, for the journalism audience.

---

## 5. The promotion workflow (silver → gold)

This formalises the medallion path the repo already half-implements, aligned to
`PUBLIC_PAYMENTS_FACT_SCHEMA.md` §A. The goal: **make a cross-tier or fan-out SUM physically impossible**,
and make every promotion pass the same gates.

```
  RAW (source files)
        │  extractors/*  (parsers — already exist per source)
        ▼
  SILVER  stg_<source>          one transform per source → the §A.4 column contract
   eTenders │ TED │ public-body │ HSE/Tusla │ LA payments │ LA AFS │ SIPO │ charities
        │                       (rename amount_semantics→value_kind, ADD realisation_tier, fold CRO)
        ▼
  CONFORMED DIMS (built once)   dim_supplier · dim_buyer · dim_cpv
        │
        ▼
  GOLD FACTS (grain-separated, never unioned across)
   ├─ fct_award      = eTenders ∪ TED           (AWARDED)            ← counts-led, ceilings caveated
   ├─ fct_payment    = public-body ∪ HSE ∪ NTA ∪ NPHDB ∪ SEAI ∪ LA  (COMMITTED/SPENT)  ← the "where it went" backbone
   └─ fct_afs_budget = la_afs_divisions/_capital (BUDGET, 3rd grain) ← context/denominator only
        │
        ▼  bridges OFF dim_supplier (never facts, never summed): lobbying_overlap · charities · revolving_door · sipo
        ▼
  MARTS / VIEWS  sql_views/*    every € metric filters value_safe_to_sum AND is scoped to ONE realisation_tier
```

### 5.1 Per-source promotion checklist (the gap-closing work)

| Source | Already conformed? | To promote cleanly, it needs |
|---|---|---|
| `procurement_awards` | tier implicit (all AWARDED); has `value_kind`,`value_safe_to_sum`,`is_framework_or_dps` | add explicit `realisation_tier='AWARDED'`; fold CRO inline from `procurement_supplier_cro_match` (exact-unique only) |
| `ted_ie_awards` | yes (`value_kind`, CRO inline, `is_pan_eu_outlier`) | add `realisation_tier='AWARDED'`; promote silver→gold; rename `winner_name_norm`→`supplier_norm` |
| `procurement_payments_fact` | **yes** (full taxonomy + CRO) | **fix the consolidation gap (§2.1)**; this becomes `fct_payment` |
| `public_payments_fact` | legacy (`amount_semantics`, no tier/CRO) | **demote to `stg_public_payments`** — input only, not a gold surface |
| `la_payments_fact` | yes | already folded; keep as a `fct_payment` input |
| `la_afs_divisions/_capital` | has `realisation_tier`,`value_kind` | promote to `fct_afs_budget`; **never** reference from `fct_payment` |
| `sipo_*` | 3 never-sum grains | keep as a separate **political-finance lane**; no contract change |
| `charities_enriched` | enrichment snapshot | keep as a **bridge** off `dim_supplier`; never a total |

### 5.2 The gates every gold promotion must pass (most already exist in the repo)

1. **Atomic write** — `services/parquet_io.save_parquet` (tmp→`os.replace`, zstd) — memory `project_atomic_parquet_migration`.
2. **Parquet write convention** — `compression="zstd"`, `compression_level=3`, `statistics=True` — memory `feedback_parquet_write_convention`.
3. **No-cross-tier-sum contract test** — assert no `sql_views/*.sql` sums across `realisation_tier`
   (extends `PUBLIC_PAYMENTS_FACT_SCHEMA.md` §A.7 / master §7). *This is the keystone test — write it.*
4. **Publisher-superset assertion** (new) — `fct_payment` publisher set ⊇ every staging input's publisher set (catches §2.1 forever).
5. **Privacy gate** — `public_display=TRUE` on every citizen-facing view; quarantine personal/individual rows; never display personal insolvency / sole-trader PII (memory `feedback_personal_insolvency_privacy`).
6. **DQ sentinel sweep** — null-sentinel + total-row contamination check (memory `project_dq_sentinel_sweep`).
7. **Reconciliation** — spot-reconcile a known total (gript.ie for PSA ~1.4%; AFS printed totals 21/21).
8. **Logic firewall** — UI reads **views only**, never raw parquet; no classification logic in the app
   (memory `feedback_no_inference_in_app`, the firewall markers). `payments_data` parquet-read violation
   (memory `project_payments_audit`) should be fixed as part of this.

### 5.3 Ordering (so nothing breaks mid-flight)

Dimensions before facts; facts before the views that JOIN them (CatalogException risk — memory
`feedback_sql_view_dependency_order`). Register: `dim_*` → `fct_*` → marts.

---

## 6. Citizen "follow the money" view (design only)

**Principle (theyworkforyou + GOV.UK):** a citizen should never have to understand "tiers" — but the UI
must never let them add up things that shouldn't be added. We do that by giving money **three lanes that
look different and never share a total**, and four ways in.

### 6.1 The three money lanes (never blended)

| Lane | Question | Source | Lead metric | Euro framing |
|---|---|---|---|---|
| **A — Contracts awarded** | "Who won public contracts?" | `fct_award` (eTenders+TED) | **count of awards** | "awarded up to €X (ceiling)" — never "paid" |
| **B — Money paid out** | "Where did the cash actually go?" | `fct_payment` | **€ paid** (single-tier) | "paid €X" / "ordered €X" — split, labelled |
| **C — Money to politics** | "Who funds politicians & parties?" | PSA + SIPO | € (per grain) | salaries / donations / election spend — kept apart |

Context rails (not lanes): **AFS budgets** ("council planned €Y") and **charity status** ("recipient is
a charity, Z% gov-funded").

### 6.2 Four entry points

1. **By recipient** → *Supplier profile* (the killer page). Search a company → one screen:
   identity (CRO, status), Lane A footprint (awards, counts-led, ceilings caveated), Lane B footprint
   (paid/ordered, tier-split), charity badge if applicable, lobbying co-occurrence (with no-causation
   caveat). All off `dim_supplier`. **This is what makes "where my money goes" tangible.**
2. **By public body** → *Buyer profile*. Department/council → top suppliers paid (Lane B), awards made
   (Lane A), and — for councils — budget context (AFS coverage ratio).
3. **By category** → *CPV / service*. "Where money goes by what it buys" — **award side only**, with an
   explicit note that paid-spend lacks a category breakdown.
4. **By place** → *Local authority*. One council: payments + AFS budget by division + coverage.

### 6.3 Non-negotiable UI rails (from the data semantics, not taste)

- **Lead with counts, verb on every euro** ("awarded", "up to … (ceiling)", "paid", "ordered") — never a bare €.
- **One tier per card/section.** The blended supplier FLOOR (§2.3) only behind a labelled toggle, never the headline.
- **No cross-lane totals.** Lane A + Lane B is never one number.
- **Provenance footer** on every figure (source file, parser, date) — existing pattern.
- **Honesty story as a feature.** The "€570bn that isn't" (framework-ceiling inflation) is a teachable
  signature moment, not something to hide — show it, then demolish it (per `PROCUREMENT_MASTER.md` §1).

(Hand to the `shape` / `impeccable` skills for the actual page brief once the data fixes in §7 land.)

---

## 7. Immediate actions (in order)

1. **Fix the consolidation gap (§2.1).** Run a full `procurement_payments_consolidate.py`; confirm the
   12 bodies + ~€2.9bn appear; add the **publisher-superset assertion** gate (§5.2.4). Then **demote
   `public_payments_fact`** to a staging input (stop shipping it as a readable gold file).
2. **Write the no-cross-tier-sum contract test (§5.2.3)** — the keystone guard; everything else leans on it.
3. **Fold CRO inline into `procurement_awards`** (exact-unique only) and **standardise the supplier-norm
   column name** across awards/payments/TED → unblock `dim_supplier`.
4. **Audit `public_payments.py`** for the blended-FLOOR label (§2.3) and the `payments_data` raw-parquet
   firewall violation.
5. **Promote `ted_ie_awards` silver→gold** with `realisation_tier`; keep eTenders/TED un-unioned.
6. **Update memory** for the stale figures (§3 ★): €8.21bn spend, the lobbying-overlap pair, the CRO
   match rate, and "la_payments_fact not wired" (it is).
7. Then design the citizen surface (§6) via `shape`/`impeccable`.

---

---

## 8. Evidence — real-data checks (duckdb, 2026-06-13)

Every claim below is a query result against the live parquet, not an estimate.

### 8.1 The invisible €2.9bn, tier-split (must NOT be shown as one number)

The 12 bodies missing from the consolidated gold (§2.1), broken down — it is **two tiers**, not one total:

| | Tier | € | Top contributors |
|---|---|---:|---|
| **Actually paid** | `payment_actual` | **~€1.52bn** | Dept Climate €1,306m · Beaumont €190m · CHI €15m · NLI €8m |
| **Ordered/committed** | `po_committed` | **~€1.41bn** | Dept Defence €1,082m · Dept Culture €143m · TU Dublin €99m · Pobal €59m |

*So the honest headline is "~€1.5bn of paid spend + ~€1.4bn of committed orders is currently invisible" —
never a blended €2.9bn.*

### 8.2 Double-count magnitude (why one canonical fact matters)

17 publishers appear in **both** gold payment facts as byte-identical rows. **Summing the two files
double-counts ~€4,694m (€4.7bn)** of sum-safe spend. This is the cost of not having a single fact.

### 8.3 Supplier profile is buildable today — Deloitte worked example

| Lane | Figure | Note |
|---|---|---|
| AWARDED (eTenders) | **352 awards, €135.2m sum-safe** | 127 framework-ceiling notices correctly excluded |
| PAID | **€93.0m** across 6 bodies (241 lines) | `payment_actual` |
| COMMITTED | €1.1m across 6 bodies | `po_committed` — shown separately |
| CRO identity | **`no_match`** | "Deloitte Ireland LLP" is an **LLP** → fails Ltd-only CRO match (the Big-4 gotcha) |

Cross-lane linkage at scale: **2,668 firms** have *both* an award and a payment on an exact
`supplier_norm = supplier_normalised` join (award norms 15,623; payment norms 19,056). That's the
addressable supplier-profile set today — and it grows once `dim_supplier` collapses the name variants
(Deloitte alone spans 3 award-norms + 4 payment-norms).

### 8.4 Lane B is ready — top recipients of *paid* money (public, sum-safe)

| Supplier | Paid | Bodies |
|---|---:|---:|
| John Sisk & Son | €446.7m | 3 |
| John Paul Construction | €330.7m | 3 |
| PFH Technology | €297.2m | 5 |
| Pfizer Healthcare | €294.0m | 1 |
| Duggan Bros | €274.8m | 1 |
| ByrneWallace Solicitors | €238.6m | 1 |
| Deloitte LLP | €92.9m | 6 |

Recognisable Irish construction / IT / pharma / legal names — exactly the "where it went" a citizen wants.

### 8.5 Buyers report different tiers → cannot be ranked on one number

| Buyer | Tier | € |
|---|---|---:|
| HSE | paid | €5,058m |
| OPW | paid | €2,390m |
| Cork City | **committed** | €816m |
| Meath | paid | €799m |
| TII | paid | €708m |
| NTA | **committed** | €607m |

HSE *paid* vs Cork City *committed* are not comparable. The publisher-summary view already splits by
tier — the UI must surface the tier, never a merged league table.

### 8.6 Competition quality — single-bid by authority (national avg 17.6%)

| Authority | Single-bid rate |
|---|---:|
| Kilkenny County Council | 45.9% |
| Beaumont Hospital | 44.2% |
| Inland Fisheries Ireland | 42.0% |
| Donegal County Council | 40.8% |
| Munster Technological University | 38.0% |

Defensible, journalism-grade transparency metric (counts-based, no euro ambiguity).

### 8.7 Charity bridge — real but THIN today (be honest)

Joining `procurement_payments_fact.cro_company_num → charities_enriched.cro_number` matches **only 233
payment rows / €15.1m** — e.g. Sophia Housing (59% gov-funded, €7.7m), Stewarts Care (96% gov-funded),
Tuath/Respond housing associations. Low coverage because (a) CRO match is itself ~53%, (b) charities are
often CLGs/LLPs that don't Ltd-match, and (c) most public money to charities flows via **grants**, not
the over-€20k procurement files. → ship as an **enrichment badge** ("recipient is a charity, X% gov-funded"),
**not** a "total to charities" figure. (The grant channel is a future ingestion, not a join fix.)

### 8.8 AFS as denominator — place example (Donegal)

Donegal AFS budget ≈ **€201m/yr** (Roads €63m, Development €33m, Housing €32m, Water €20m…); over-€20k
payments surfaced ≈ €597m (all years). The coverage ratio is the win — **but only when year-aligned**
(budget is per-year, payments are multi-year). `procurement_afs_vs_po_coverage.sql` is the right pattern.

### 8.9 The "€570bn that isn't" — signature honesty story (figure moved again)

| naive Σ | sum-safe | framework ceilings | inflation |
|---:|---:|---:|---:|
| **€570.7bn** | **€14.46bn** | €495.8bn | **39.5×** |

★ The sum-safe total is now **€14.46bn** (all suppliers) / **€7.69bn** (company-gated), inflation **39.5×**
— up from the master doc's 24.3× after the ≥€50M large-award guard (2026-06-08). **Update memory/PROCUREMENT_MASTER.md.**

---

## 9. Refined tangible wins (ranked by value × feasibility)

| # | Win | Tangible payoff (measured) | Effort | Depends on |
|---|---|---|---|---|
| **1** | **Full re-consolidate + publisher-superset gate** | Recovers **€1.52bn paid + €1.41bn committed** across 12 bodies, now invisible | **XS** (re-run script + 1 test) | — |
| **2** | **Demote `public_payments_fact` to staging** | Eliminates the **€4.7bn double-count** risk; one canonical fact | **XS** | #1 |
| **3** | **No-cross-tier-sum contract test** | Makes the project's #1 hazard structurally impossible; guards everything downstream | **S** | — |
| **4** | **`dim_supplier` (fold CRO inline + unify norm-name)** | Unlocks supplier profiles for **2,668+ firms**; Deloitte €135m awarded / €93m paid in one view | **M** | — |
| **5** | **Top-recipients leaderboard (Lane B)** | Ready now once #1 lands — Sisk €447m, PFH €297m… | **S** | #1 |
| **6** | **Buyer/department profile (tier-split)** | HSE €5.06bn, OPW €2.39bn — "where this body's money goes" | **S** | #1, #3 |
| **7** | **Single-bid competition flag** | Kilkenny 46% vs 17.6% avg — journalism-grade, view exists | **S** | — |
| **8** | **Charity enrichment badge** | Honest "X% gov-funded" on recipients (thin: €15m today) | **S** | #4 |
| **9** | **AFS coverage ratio per council** | Donegal €201m budget context; year-aligned | **M** | — |
| **10** | **"€570bn that isn't" explainer** | The teachable open-data-literacy moment (39.5×) | **XS** (copy) | — |

**The cheapest two wins (#1, #2) are also the highest-impact** — they fix correctness (€2.9bn visibility,
€4.7bn double-count) for roughly the cost of re-running one script and deleting a gold surface. Do them first.

---

## 10. Real-world benchmarks (where this sits vs the field)

The model is sound because it mirrors what the best public-money platforms already do:

- **USAspending.gov (US federal).** Separates **obligations/awards** from **outlays (payments)** and offers
  **recipient profiles** — *exactly* our AWARDED-vs-SPENT tiers + supplier profile (#4). It treats the two
  as different measures you never add — direct validation of §A.1/§2.1.
- **Open Contracting Data Standard (OCDS).** The "never sum across lifecycle stages" rule (planning→tender→award→implementation)
  is OCDS doctrine; the repo's `realisation_tier` already names these. The contract test (#3) is OCDS conformance.
- **Tussell / Stotles / Spend Network (UK procurement intelligence).** Commercial benchmark — supplier-centric,
  "company's full public footprint", framework-vs-call-off distinction. Our supplier profile (#4) + framework-ceiling
  honesty (#10) target the same job; memory notes their edge is **live tenders + contract end-dates** (a future ingest).
- **UK GOV.UK "spend over £25k" + Contracts Finder.** The transactional-payment disclosure regime our payment fact
  mirrors (Ireland's PO/payments over €20k). Same grain, same privacy quarantine of individuals.
- **OpenCorporates / OpenOwnership.** Entity-resolution platforms — the `dim_supplier`/CRO problem (#4, the Big-4-LLP
  gotcha) is the exact identity-matching they specialise in; the fix is a once-built dimension, not per-table matching.
- **TheyWorkForYou.** The project's spirit (memory `project_design_principles`): plain-English, counts-led, no
  inference — our citizen view (§6) follows it.
- **Ireland-specific gap:** independent charity-financials + government-funding mapping has been historically thin
  since the sector's dedicated transparency body wound down; our charity bridge (#8) is a *partial* fill (grants
  ingestion would complete it). *(Treat as planning-chat context; source any public claim before it reaches UI per
  `feedback_cite_news_claims`.)*

**Net:** Dáil Tracker is one consolidation fix and one conformed dimension away from doing, for Irish public money,
what USAspending does for US federal — at OCDS-conformant honesty that most national portals don't enforce.

---

## 11. Media cross-check — do our sums match what's reported? (2026-06-13)

Where our **grain matches the media's**, the figures and patterns line up. Where they diverge, it's
explained by two known limitations — sum-safe excludes frameworks, and the payment fact is **missing the
core central departments**. Sources are reputable Irish outlets (per `feedback_cite_news_claims`).

### 11.1 ✅ MATCH (pattern) — political donations: Sinn Féin top, FF/FG ≈ zero

Our GE2024 election-donation data: Sinn Féin €65,599 (top), Labour €31,550, PBP/Solidarity €22,551,
Green €17,792, Soc Dems €10,085, Aontú €10,000 — **no Fianna Fáil / Fine Gael**. This is the persistent,
media-documented pattern: Sinn Féin consistently leads disclosed donations (€80,190 in 2022) while Fianna
Fáil has "declared zero donations for four years running". **Caveat:** our file is GE2024 *election*
donations (`2024_election_donations.pdf`), a different stream from the *annual party statements* the press
usually totals — so this is a **pattern match, not an exact-figure match**. Don't equate the two grains.
Sources: [TheJournal — 2022 disclosures](https://www.thejournal.ie/sipo-disclosed-political-donations-2022-6247778-Dec2023/),
[Irish Times — FF zero donations](https://www.irishtimes.com/news/politics/fianna-fail-declares-zero-donations-for-fourth-year-running-1.3130060).

### 11.2 ✅ MATCH (directional) — John Sisk is the dominant public-works recipient

Our Lane B has **John Sisk & Son at the top: €446.7m paid** across 3 bodies. Media corroborates Sisk as
Ireland's biggest healthcare/public-works contractor — a €250m PPP for seven Community Nursing Units,
~£600m of healthcare projects on site, ~€1.5bn group turnover. Our cumulative-paid figure is a plausible
subset of that footprint (and PPP-financed work like the CNU deal would *not* fully appear in over-€20k
payment files). Sources: [The Construction Index — Sisk €250m healthcare](https://www.theconstructionindex.co.uk/news/view/sisk-wins-250m-irish-healthcare-contract),
[johnsiskandson.com — healthcare](https://www.johnsiskandson.com/what-we-do/sectors/healthcare).

### 11.3 ⚠️ PARTIAL — consultancy: our figures are a FLOOR, and reveal a coverage gap

- Media aggregate: **top-10 consultancy firms paid >€2bn by Government over 13 years** ([Irish Examiner, Jan 2026](https://www.irishexaminer.com/news/politics/arid-41783462.html)).
- Media specific: **BearingPoint received €25.13m from the Dept of Social Protection in 2024** (and €22.83m in 2023); DSP paid **€1.4m/week** to IT consultancies ([Irish Times, Feb 2025](https://www.irishtimes.com/ireland/2025/02/12/department-of-social-protection-paid-14m-a-week-to-consultancies-for-it-projects/)).
- **Our data:** Deloitte €135.2m / EY €228.6m / KPMG €33.7m / PwC €6.7m **awarded-safe** (eTenders 2013–2024,
  frameworks excluded). BearingPoint: only **€0.8m paid** (5 rows) but **€67.1m in awards** (107 rows).

**Why ours is lower — two concrete reasons:**
1. **Sum-safe excludes frameworks/call-offs**, which is *how most consultancy is bought*. Our award figure
   is a deliberate floor; the media €2bn counts framework drawdowns + actual payments.
2. **The big consultancy-buying departments are ABSENT from our payment fact.** Verified absent: **Social
   Protection, Justice, Finance, Public Expenditure, Foreign Affairs, Agriculture, Taoiseach.** That is
   exactly why BearingPoint's €25m DSP payment is invisible to us (we only see it on the *award* side).
   Present departments are mostly agencies/health/councils (HSE, HPRA, CHI, HEA) + the 12 recoverable bodies (§2.1).

→ **New tangible win (below): ingest the central departments' payment disclosures** — without them, "where
the money goes" structurally misses the biggest consultancy and IT spenders.

### 11.4 Verdict

| Claim | Our data | Match? |
|---|---|---|
| Sinn Féin leads donations, FF/FG ~zero | GE2024: SF €65.6k top, no FF/FG | ✅ pattern |
| Sisk = top public-works recipient | €446.7m paid, rank #1 Lane B | ✅ directional |
| Top-10 consultancy €2bn/13yr | Deloitte+EY+KPMG ≈ €397m awarded-safe (floor) | ⚠️ floor (frameworks excluded) |
| BearingPoint €25m from DSP, 2024 | €0.8m paid (DSP absent), €67m awards | ⚠️ payment coverage gap |

The cross-check **validates the model** (matching grains agree) and **surfaces a real, fixable gap**
(missing central departments) — it does *not* find our published figures contradicting the press.

### 11.5 Added tangible win

| # | Win | Tangible payoff | Effort |
|---|---|---|---|
| **11** | **Ingest central-department payment disclosures** (Social Protection, Justice, Finance, DPER, Foreign Affairs, Agriculture, Taoiseach) | Captures the biggest consultancy/IT spend (e.g. BearingPoint €25m/yr, DSP €1.4m/wk) currently invisible on the payment side | **M–L** (per-dept parsers; some are PDF) |

---

### Appendix — files of record

- Target model: `doc/PUBLIC_PAYMENTS_FACT_SCHEMA.md`, `doc/PROCUREMENT_MASTER.md`.
- Consolidate: `extractors/procurement_payments_consolidate.py` (header out of date re: publisher count/overlap).
- Public surface: `sql_views/procurement/procurement_public_payments.sql` (blended FLOOR caveat at line ~60;
  correct tier split in `procurement_payments_publisher_summary.sql`).
- No-causation bridge: `sql_views/procurement/procurement_lobbying_overlap.sql`.
- Gold facts: `data/gold/parquet/{procurement_payments_fact,public_payments_fact,procurement_awards,
  procurement_lobbying_overlap,procurement_supplier_cro_match,charities_enriched}.parquet`.
- Silver: `data/silver/parquet/{la_payments_fact,la_afs_divisions,la_afs_capital_divisions,ted_ie_awards,
  hse_tusla_payments_fact}.parquet`.
