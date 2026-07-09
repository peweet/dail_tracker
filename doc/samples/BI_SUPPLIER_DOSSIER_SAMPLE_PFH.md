# Supplier Dossier — PFH Technology Group *(SAMPLE)*

| | |
|---|---|
| **Prepared for** | SAMPLE — sales-reference artifact (not a client deliverable) |
| **Report subject** | PFH Technology Group |
| **CRO number** | 415827 · status **Normal** · normalised key `PFH TECHNOLOGY` |
| **Prepared** | 2026-07-09 |
| **Coverage windows** | eTenders awards 2013–2026 · public-body payments 2012–2026 · TED winners 2024+ |
| **Data basis** | Live `dail-tracker` query layer; corpus snapshot per the [Provenance & manifest](#provenance--manifest) section |
| **Template** | [doc/templates/BI_SUPPLIER_REPORT_TEMPLATE.md](../templates/BI_SUPPLIER_REPORT_TEMPLATE.md) |

> **This is a demonstration sample built from real public data**, to show what a bespoke supplier
> dossier looks like end-to-end. Two display choices are **provisional pending owner sign-off** and are
> marked ⚠️ where they appear: (a) the money-badge wording and any "not safe to sum" chip; (b) the
> match-confidence tier label. The *figures* are real; only the wording of those two labels is not yet
> signed off.

**Identity match confidence:** ⚠️ *(provisional label — owner gate)* **`Verified — exact unique CRO match`**
— one company number resolves this supplier's normalised name with no ambiguity (`cro_match_method =
exact_unique`). This is the strongest of the match tiers; a supplier shown at a weaker tier would carry
"probable — confirm via the CRO number" instead.

---

## Key facts

Ranked by decision-relevance for a supplier or consultant sizing this competitor. *(Selection and
ordering are editorial; every figure below is a stated fact with its grain and source — no score, rank,
or inference.)*

1. **PFH is a top-tier state IT supplier on two separate registers.** €182.0m sum-safe **awarded**
   value across 281 eTenders award notices and 49 public bodies (2013–2026); and €519.4m sum-safe
   **paid** by public bodies across 60 publishers (2012–2026). **These are two different money grains —
   never added together** (see [§4](#4-awards-vs-payments--the-two-grains-side-by-side)).
2. **Its route to market is the OGP central frameworks, not one-off buyer tenders.** 198 of 281 award
   notices (70%) and €108.6m of sum-safe awarded value run through the **Office of Government
   Procurement** — PFH sits on national IT frameworks that other bodies buy through.
3. **Identity is CRO-verified:** exact-unique match to PFH Technology Group, company no. **415827**,
   status **Normal**.
4. **The biggest headline "awards" are framework CEILINGS, not PFH revenue.** The largest is a **€750m**
   OGP "IT services" framework ceiling (2024) — a shared maximum across *all* suppliers on that
   framework, never PFH's own money. Ceilings are excluded from the €182.0m sum-safe figure.
5. **Sustained, growing footprint:** award activity ramped from 2016, with sum-safe awarded value
   peaking at **~€55m in 2022**; PFH appears every year 2019–2025.
6. **Squarely an IT integrator/reseller:** the coded categories are IT services/consulting/software and
   computer equipment & supplies.
7. **No lobbying-register or EPA presence** — PFH is not on the lobbying register, so the (gated,
   reports-only) lobbying co-occurrence panel does not apply to this subject.

**Register presence:** eTenders (procurement) ✓ · EU award notices (TED) — *not queried for this sample*
· Public-body payments ✓ · Lobbying register ✗ · Charity register ✗ · Corporate-distress records —
*not queried* · EPA licence ✗

---

## 1. Identity & footprint

- **Legal name (as awarded):** PFH Technology Group · **CRO:** 415827 (Normal) · **key:** `PFH TECHNOLOGY`
- **Appears across 2 public money registers** in this corpus: the eTenders **award** register and the
  public-body **payments** register. *(Cross-register presence is reported without asserting any
  connection between the two beyond identity — see caveats.)*
- **Class:** company (company-class; the person-row/PII gate does not apply).

## 2. Public-procurement award activity — AWARDED grain ⚠️

> **Grain: AWARDED (eTenders).** ⚠️ *provisional badge wording.* These are **award ceilings / award
> values, not cash paid** — for what was actually paid see [§4](#4-awards-vs-payments--the-two-grains-side-by-side).

- **Sum-safe awarded value:** **€181,966,526** across **149** value-safe award notices.
- **Total award notices:** 281 across **49** contracting authorities. Of these, **97 are framework/DPS
  ceiling notices** and a further 35 carry non-summable award values — all **excluded** from the
  €182.0m figure (only `value_safe_to_sum` rows are added).

**Awarded value by year (sum-safe €):**

| Year | Notices | Sum-safe € |
|---|---:|---:|
| 2016 | 11 | €780,000 |
| 2017 | 15 | €1,182,555 |
| 2018 | 17 | €3,434,872 |
| 2019 | 31 | €24,758,776 |
| 2020 | 33 | €14,109,741 |
| 2021 | 31 | €27,831,384 |
| 2022 | 36 | €54,958,283 |
| 2023 | 26 | €15,469,669 |
| 2024 | 28 | €35,705,450 |
| 2025 | 36 | €3,708,547 |

*(2013–2015 and 2026 carry notices but €0 sum-safe value — early years and recent ceiling notices.)*

**Top contracting authorities (by sum-safe awarded €):**

| Authority | Notices | Sum-safe € |
|---|---:|---:|
| Office of Government Procurement | 198 | €108,572,272 |
| An Garda Síochána | 3 | €31,272,413 |
| Department of Social Protection | 9 | €10,996,800 |
| Commission for Communications Regulation | — | €8,500,000 |
| Iarnród Éireann–Irish Rail | — | €6,672,430 |
| Fáilte Ireland | 5 | €6,650,000 |
| Health Service Executive (HSE) | — | €3,487,038 |

**Largest award notices — mostly framework CEILINGS (never summed, not PFH revenue):**

| Award value | Date | Grain | Authority | Category |
|---:|---|---|---|---|
| €750,000,000 | 2024-05-02 | **Framework ceiling** ⚠️ | Office of Government Procurement | IT services |
| €475,000,000 | 2020-12-25 | Award value *(not safe to sum)* | An Garda Síochána | — |
| €300,000,000 | 2015-07-14 | **Framework ceiling** ⚠️ | Office of Government Procurement | — |
| €250,000,000 | 2024-03-01 | **Framework ceiling** ⚠️ | Office of Government Procurement | IT services |
| €238,310,000 | 2024-02-09 | **Framework ceiling** ⚠️ | Uisce Éireann | Desktop computers |

> These ceilings are the **whole framework's maximum shared across every supplier on it**, not money
> PFH won or was paid. They are shown for context and are **excluded from every total above**.

## 3. Concentration / incumbency

- **70% of PFH's award notices (198/281) and ~60% of its sum-safe awarded value run through the OGP.**
  Read as a structure fact: PFH's public-sector presence is anchored in central IT frameworks, which
  other bodies then call off against. *(A concentration fact, not a verdict — often the normal shape
  for a framework-panel IT supplier.)*
- Beyond OGP, award value is thin and episodic per individual buyer (An Garda Síochána is the next
  largest at €31.3m, largely one big 2020 award).

## 4. Awards vs payments — the two grains side by side

> **The single most important discipline in this dossier.** The two figures below measure **different
> things** and **must never be added, netted, or divided into a ratio.**

| | AWARDED (eTenders) ⚠️ | PAID (public bodies) ⚠️ |
|---|---:|---:|
| Sum-safe € | **€181,966,526** | **€519,388,250** |
| Scope | 149 value-safe award notices, 49 authorities | 1,673 payment lines, **60 publishers** |
| Period | 2013–2026 | 2012–2026 |
| Means | Contracted award value visible in the eTenders award register | Cash actually paid, over €20k, as disclosed by public bodies |

**Neutral coverage reading (no inference).** The paid figure is larger than the awarded figure because
the two registers capture different things over overlapping-but-not-identical scopes: the payments
register records **actual cash across 60 bodies** — including framework **call-offs**, below-threshold
and direct purchasing, and spend not tied to a discrete visible eTenders *award* notice — whereas the
awarded figure counts only sum-safe **award values** in the eTenders register (framework ceilings
excluded). The gap is a **coverage/scope fact, not a discrepancy, an overspend, or a finding.** No ratio
is computed.

## 5. Category & competition context

- **What PFH sells (coded CPV subset):** IT services (consulting/software/support), computer equipment
  & supplies, software-related services. A large share of PFH's notices carry **no CPV code**
  (frameworks especially), so category totals cover only the coded subset — treat as indicative.
- **Single-bid / competition baseline for PFH's categories:** *not filled for this sample.* In a client
  report this is populated from the per-CPV competition view (`competition_by_cpv`, TED 2024+), with the
  verbatim competition caveat below. **A single bidder is a factual signal, never a verdict.**

## 6. Open pipeline (PLANNED grain)

*Not queried for this sample.* In a client report, live/expiring opportunities relevant to PFH's CPV
mix are listed from the live-tender and expiring-contract views (PLANNED grain — advertised estimates,
**never summed** with awarded or paid figures).

## 7. Enrichment

- **EPA environmental licence:** none (`has_epa_licence = false`).
- **Charity register:** not present.
- **Corporate-distress (Iris Oifigiúil):** *not queried for this sample.* Where it renders in a client
  report it is **fact-only + attributed** (notice type/date/entity/status), reports-only, per the
  licensing gate.
- **Lobbying register:** not present → the gated lobbying co-occurrence panel does not apply here.

---

## Provenance & manifest

*Reproducibility note (per the reproducibility rail): these figures are re-derivable from the query
layer at the corpus snapshot below; a later pipeline refresh will move them, so a client report pins
this manifest.*

- **Generated:** 2026-07-09, from the live `dail-tracker` query layer (MCP tools `get_supplier`,
  `search_suppliers`, `public_body_payments`, `data_coverage`).
- **Corpus snapshot at generation:**
  - Procurement awards: 44,165 award rows, 11,458 sum-safe, 2013–2026; 10,017 suppliers, 1,891 authorities.
  - Public-body payments: 406,111 lines (397,894 sum-safe), 85 publishers, 2012–2026.
  - TED: 37,216 notices, winners 2024+.
- **Subject keys:** `supplier_norm = PFH TECHNOLOGY`; CRO 415827; payments `supplier_normalised = PFH TECHNOLOGY`.
- **To pin for a paying client:** archive the gold-parquet hashes + `data_currency` + the view-layer git SHA alongside the report.

## Sources & attribution

- **eTenders / Office of Government Procurement (awards):** *Contains Irish Public Sector Data (Office
  of Government Procurement) licensed under a Creative Commons Attribution 4.0 International (CC BY 4.0)
  licence.*
- **Public-body payments:** published by the individual public bodies under PSI re-use; attribute the
  publishing body. *(Per-publisher VAT basis varies — see the payments caveat.)*
- **CRO (company identity):** *Contains Irish Public Sector Data licensed under a Creative Commons
  Attribution 4.0 International (CC BY 4.0) licence.*

## Caveats *(verbatim from `dail_tracker_core/caveats.py` — never re-worded)*

- **Money grains (master rule):** "procurement AWARDS, public-body PAYMENTS, and T&A allowances are
  three different value grains — NEVER sum across them".
- **Procurement awards:** "eTenders/TED AWARD ceilings — the contracted maximum, NOT realised spend;
  for what was actually paid use public-body payments. Only the sum-safe value column is addable
  (framework/DPS ceilings and estimates are not), and award values are NEVER summed with the payments
  or T&A-allowance grains."
- **Public-body payments:** "sum-safe spend only; never add to procurement AWARD values (different
  grain). VAT basis varies by publisher and is unconfirmed for most (only HSE/Tusla are documented
  incl-VAT), so cross-publisher totals mix VAT bases — see
  data/_meta/procurement_payments_vat_matrix.json for the per-publisher basis."
- **Competition (when §5 is filled):** "single_bid_lot_pct = single-bid LOTS / lots-with-a-bid-count,
  from TED 2024+ award notices — each contract PART counted once … A FACTUAL competition signal, NEVER
  a verdict: a single bidder is often legitimate — a niche/specialist supplier, bespoke research
  equipment, genuine urgency … a prompt to look, not evidence of wrongdoing. … Coverage is 2024+ only."

---

## Rails honoured in this sample

- **Three grains never summed** — AWARDED €182.0m and PAID €519.4m shown as separate labelled cells,
  no total, no ratio, no netting; framework ceilings excluded from every sum.
- **No scores, verdicts, rankings, or indices** — OGP concentration and framework use are stated as
  structure facts with neutral language.
- **Company is the subject**; no natural person named; company-class gate satisfied.
- **CRO identity shown at its true match tier**; caveats reproduced verbatim; attribution on every source.
- **Provisional display choices flagged ⚠️** (badge wording, confidence-tier label) rather than silently
  baked in — these are the owner sign-off items the sample surfaces.
