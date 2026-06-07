# Procurement — Master Plan (consolidated)

**Status:** authoritative. Consolidates and **supersedes** the scattered procurement
docs (see §9). Written 2026-06-06 after verifying every headline figure against the
live parquet. The eTenders awards page is **shipped**; this doc defines the *uplift*
to a powerful, multi-register, non-misleading procurement page.

> One-line goal: surface **who wins Irish public contracts, from which bodies, for
> what, and where the same firms appear in the lobbying register** — leading with
> *counts* (trustworthy) and treating every euro figure with the honesty rails in §3,
> because the naive euro total is a ~24× lie (§1).

---

## 1. Verified figures (checked 2026-06-06 against the gold/silver parquet)

**eTenders gold** (`procurement_awards.parquet`, 59,439 award×supplier rows, 2013–2026):

| measure | value | note |
|---|---:|---|
| Naive Σ of every `value_eur` | **€570.74bn** | meaningless — never display as a total |
| └ of which framework/DPS ceilings | €495.8bn (87%) | 16,203 rows, **none** sum-safe |
| └ distinct framework notices, counted **once** | €41.6bn | the ceiling repetition inflates €41.6bn → €495.8bn (~12×) |
| **Sum-safe awarded value** (`value_safe_to_sum`) | **€23.46bn** | the only summable figure — and still *awarded*, not *paid* |
| Naive ÷ safe ratio | **24.3×** | the "€570bn that isn't" |
| Company-gated safe Σ (what the page shows today) | €13.12bn | company-class suppliers only (rankings slice) |

**The €570bn is real arithmetic but 96% artifact:** multi-supplier frameworks repeat
one multi-year ceiling across every supplier row. This *is* the page's signature
open-data-literacy story — show the vast number, then demolish it.

**TED silver** (`ted_ie_awards.parquet`, 13,126 EU-journal award notices, 2023–2026):

| measure | value | note |
|---|---:|---|
| Naive Σ | €624bn | absurd |
| └ of which 375 pan-EU outliers | €586bn | GÉANT-type research frameworks; Ireland is one of dozens of participants — already flagged `is_pan_eu_outlier` |
| **Sum-safe** (excl. outliers) | **€5.82bn** | award-grain, CRO-matched 69% |
| TED winners also in eTenders (by norm name) | 4,207 / 6,391 (66%) | ⇒ **never union/sum**; cross-reference per firm instead |

---

## 2. Who this is for (and what they want to see)

Designed for five real audiences. Every section below should answer at least one of
their first questions.

1. **Investigative journalists** (The Ditch, Noteworthy, RTÉ Investigates, Irish Times).
   *Want:* repeat winners; concentration (the "Big 4 consultancy" story — Deloitte/PwC/EY/KPMG);
   a named firm's full footprint; firms that **also lobby**; defensible, sourced, non-libellous framing.
2. **Opposition / TD staff & policy researchers.**
   *Want:* spend in a minister's domain (by contracting authority / department); framework usage; value-for-money lines.
3. **Engaged citizens / taxpayers.**
   *Want:* "where does the money go?"; recognisable names; plain-English scale; is this a lot or a little.
4. **SMEs / suppliers (market intelligence).**
   *Want:* who wins in **my category** (CPV); which authorities buy what I sell; framework/DPS opportunities.
5. **Academics / transparency NGOs** (Transparency International Ireland).
   *Want:* concentration metrics; cross-register overlap; data-quality honesty.

**Their headline questions, in priority order:**
- Who wins the most public contracts? *(count — the honest lead metric)*
- How concentrated is it? *(top-N share)*
- Which bodies award the most, and for what?
- Does a contract-winner also appear on the lobbying register? *(co-occurrence)*
- What's a specific company's full public-money footprint? *(search → profile, eTenders + TED)*
- How much of the headline is real vs framework ceilings? *(the €570bn story)*
- Is consultancy/IT spend rising over time? *(trend)*

---

## 3. Presentation principles (non-misleading rails — non-negotiable)

These are derived from the data semantics, not taste. The logic firewall enforces the
data side; copy enforces the rest.

1. **Lead with counts, not euros.** `n_awards` is the trustworthy metric; € is awarded
   value (a ceiling at best, never paid). Default every ranking to "Most awards".
2. **Verb on every figure.** Render "awarded €X", "up to €X (framework ceiling)",
   "paid €X" — **never a bare €**. The verb is the disambiguation.
3. **One tier per section.** Never blend award / ceiling / committed / paid / budget in a
   single list or total. Different `value_kind` = different section.
4. **Show the €570bn only to demolish it.** The naive-vs-safe contrast is the literacy
   device, explicitly framed; the naive number is never a standalone headline.
5. **Co-occurrence, never causation** (lobbying overlap). Allowed: "appears on both the
   procurement and lobbying registers." Banned: *influenced / bought / conflict /
   corruption / received €X in public money* (when value is only an award/ceiling).
6. **Privacy.** Company-class suppliers only in rankings; sole-traders/individuals
   quarantined (name withheld, award still disclosed). No individual reachable via a
   ranking drill-down (smoke-tested).
7. **Provenance on everything.** Source attribution + landing link + `retrieved_utc`;
   register clearly named (eTenders national vs TED EU-journal).
8. **Registers are siblings, never summed.** eTenders and TED are two *award* registers
   with 66% overlap; a firm's profile shows both, labelled, never added together.

---

## 4. Data inventory — what's valid to uplift, and what's gated

The governing model (carried from the old build plan §4b — **the value taxonomy**):
every value row carries **`realisation_tier` ∈ {PLANNED → AWARDED → COMMITTED → SPENT}**
(+ BUDGET aggregate) and a controlled **`value_kind`**; `value_safe_to_sum` is derived
from `value_kind`; **no cross-tier arithmetic** (the notice→award→PO→payment chain is
unlinked, so reconciliation is fiction).

| Source | Grain / tier | Where it lives | Verdict for THIS uplift |
|---|---|---|---|
| **eTenders awards** | AWARDED | **gold**, in page | ✅ shipped; enrich (the €570bn story, trends) |
| **The €570bn contrast** | AWARDED | gold (derivable now) | ✅ **UPLIFT NOW** — pure UI, zero new data, highest-impact |
| **TED EU-journal awards** | AWARDED | **silver**, enriched, not in page | ✅ **UPLIFT NOW** — promote to gold, show as a *separate register* + per-firm cross-reference. Strip `_NNNNN` org-id suffix from winner names first |
| **Public-body payments** (OPW, depts, HSE, Tusla, NTMA, universities…) | COMMITTED / SPENT | sandbox `public_payments_fact` (~72k rows, €14.3bn, 25 publishers) + HSE €6.39bn / Tusla €178m bespoke | ⛔ **GATED** — privacy quarantine is OFF (2,427 sole-trader rows `public_display=true`), no `vat_status` (incl- vs excl-VAT mixed), HSE/Tusla not merged, no views/tests. **The real prize, but cannot ship without the privacy + schema pass.** |
| **LA Purchase-Orders >€20k** | COMMITTED | silver prototype, 22–31 councils (~250–320k est.) | ⛔ **GATED** — quarantine applied (better), but 31 drift surfaces, no schema/coverage tests |
| **AFS by-division** | BUDGET | silver (amalgamated) + per-LA | ➖ separate sibling fact; out of scope for the procurement page |

**"Valid to uplift now" = the €570bn story + TED.** Both are real, ready, and additive
without the privacy landmines. The payment/spend tier is **deliberately deferred behind
its privacy gate** — surfacing un-quarantined sole-trader payments would be a PII breach
and a no-inference violation. It is the next milestone, not this one.

---

## 5. The page (target IA)

Single page `rankings-procurement`, entity-first, register-aware. Browse → drill-down.
All cards (no `st.dataframe` on primary views); all CSS in `shared_css.py` (`pr-*`);
logic in `sql_views/procurement_*.sql` only.

**Above the fold (lean — see the 2026-06-06 audit fix):**
- Hero (kicker / title / dek), **one** scale-anchor stat strip, glossary, year pills.

**The signature panel — "The €570bn that isn't":** a compact contrast showing the naive
Σ (€570.74bn, struck-through / greyed) → the sum-safe €23.46bn, with the 24× explainer
and one line on why (multi-supplier framework ceilings repeat). This is the methodology
*and* the hook.

**Tabs (one tier each):**
1. **Suppliers** — ranked by awards won (count default; value lens secondary). CRO chip +
   "also on lobbying register" badge. Concentration line: "top 10 firms hold N% of awards".
2. **Contracting authorities** — who buys most; drill to their awards.
3. **Categories (CPV)** — what's bought; drill to category awards.
4. **Lobbying overlap** — neutral co-occurrence disclosure cards.
5. **EU-level awards (TED)** *(new)* — clearly a *separate register*; safe value excl.
   pan-EU outliers; same honesty rails. Never summed with eTenders.

**Supplier profile (`?supplier=`)** — the journalist's view: headline reconciled
(€X across N contract awards; M framework ceilings listed, not payments — *shipped
2026-06-06*); full eTenders award history; **+ a TED panel** ("EU-level award notices for
this firm") clearly labelled and never added to the national total; CRO link; lobbying
co-occurrence block linking to `/rankings-lobbying`.

**Trends (where the data supports it):** awards-per-year by top category — answers "is
consultancy spend rising?" using counts, not summed ceilings.

**Footer:** provenance for every register + the value caveat.

---

## 6. Build plan (staged, each independently shippable)

- **Stage A — the €570bn panel** (pure UI, no data change). Add a `coverage_stats`-fed
  contrast strip + the 24× explainer. Highest impact / lowest risk. *Ship first.*
- **Stage B — TED uplift. ✅ SHIPPED 2026-06-06.** Exposed `ted_ie_awards` via
  `v_procurement_ted_awards` + `v_procurement_ted_supplier_summary` reading the enriched
  **silver** parquet directly (same precedent as the lobbying-overlap view — no gold-parquet
  duplication / gitignore dance, and the extractor's own design says "gold only when a view
  exposes it"). Winner-name `_NNNNN` suffix stripped in-view (display + recovered join-norm).
  New "EU-level awards (TED)" tab (count-led ranking, pan-EU **default-hidden** behind a
  toggle that reveals the €586bn shared-ceiling mirage) + a per-firm TED cross-reference
  panel on the eTenders supplier profile (matched on normalised name, **never summed**).
  Core/query/UI tests added. *Follow-up:* clean the suffix at the extractor source so
  `winner_name_norm` is clean for all rows (currently ~9% recovered in-view).
- **Stage C — concentration & trend. ✅ SHIPPED 2026-06-06.** Top-N market-share line on
  the Suppliers tab (`supplier_concentration` — top 10 firms = 4.6% of awards, "a broad
  market") + an awards-per-year trend bar chart (`awards_by_year`). Both pre-aggregated in
  the core/view layer; the page only renders. Tests added.
- **Stage D — the payment/spend tier (separate milestone, gated).** Only after: privacy
  quarantine pass on `public_payments_fact`, `vat_status` column, HSE/Tusla merged to the
  canonical `value_kind`+`realisation_tier` schema, CRO join, views + tests. Then a
  distinct **"Money actually paid"** section, never merged with awards.

Firewall checklist (gate every stage): no `read_parquet`/JOIN/GROUP BY/window in
`procurement.py` or `procurement_data.py`; every metric in a view; `value_safe_to_sum`
gating in the view; one tier per view; CC-BY attribution + `retrieved_utc` shown; no
inference/causal copy.

---

## 7. Owner decisions (LOCKED 2026-06-06)

1. **Scope:** ✅ **A + B + C now, AND start Stage D** (the gated spend tier). The full
   uplift, including the PII-sensitive payment work — done carefully, privacy-first.
2. **TED pan-EU outliers:** ✅ **default-hide** the 375 outliers from totals, **with a
   "show pan-EU frameworks" toggle** for completeness.
3. **Spend tier (Stage D):** ✅ in scope. Privacy quarantine pass + `vat_status` +
   HSE/Tusla merge to the canonical schema come *before* anything reaches the UI. No
   un-quarantined sole-trader payment is ever displayable.

---

## 8. Value taxonomy (canonical — carried verbatim from the old build plan §4b)

`realisation_tier`: **PLANNED → AWARDED → COMMITTED → SPENT** (+ BUDGET aggregate).

| `value_kind` | tier | UI verb | summable? | source |
|---|---|---|---|---|
| `estimate_advertised` | PLANNED | "expected ~€X" | no | eTenders/TED notice estimate |
| `budget_allocated` | PLANNED(agg) | "budgeted €X" | within LA/year only | AFS / NOAC |
| `contract_award_value` | AWARDED | "awarded €X" | caution | eTenders/TED single award |
| `framework_or_dps_ceiling` | AWARDED | "up to €X, shared" | **NO** | frameworks/DPS, pan-EU |
| `po_committed` | COMMITTED | "ordered €X" | yes | LA / public-body PO-over-€20k |
| `payment_actual` | SPENT | "paid €X" | **yes (true spend)** | "Paid" lists / dept payments |

Rules: every value row tagged both axes; `value_safe_to_sum` derived from `value_kind`;
one tier per view/section; the verb disambiguates; no cross-tier arithmetic; PDF→tabular
fidelity (one printed line = one typed row).

---

## 9. Superseded docs (folded into this one)

This master consolidates the following — keep for git history, but **read this first**:

- `PROCUREMENT_BUILD_PLAN.md` — original eTenders build (Phases 0–4) + value taxonomy §4b.
- `PROCUREMENT_INVESTIGATION.md` — lifecycle model, truncation-repair, TED API, anti-overwhelm UX.
- `PROCUREMENT_SEMISTATE_EXPANSION_PLAN.md` — public-body payment lane (HSE/Tusla/NTMA/etc., sandbox).
- `PROCUREMENT_SOURCE_DISCOVERY_2026_06_04.md` — +17 new payment publishers (universities/hospitals/agencies).
- `PROCUREMENT_COVERAGE_GAP_2026_06_05.md` — award-vs-spend coverage census (1,948 authorities vs 53 spend bodies).
- `PROCUREMENT_TILE_REVIEW.md` — ground-truth reviewer pass (what's real vs aspirational; build/defer/reject table).
- `dail_tracker_local_housing_procurement_judiciary_plan.md` (procurement parts) — the Tile-1 product framing.

The **payment/spend-tier schema contract** remains owned by
`PUBLIC_PAYMENTS_FACT_SCHEMA.md` (the canonical fact for Stage D) — not superseded.
