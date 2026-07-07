# Competitor Analysis — Data Investigation & Enrichment Report

**Scope:** can the Dáil Tracker procurement data support "who do I compete against, and which
buyers are they strongest with?" for a contractor — using awards, TED, public-body payments and
CRO matches. Use case: an electrical / M&E / construction contractor.
**Method:** direct DuckDB queries against the gold/silver parquet (read-only). No pipeline changes.
**Status:** investigation complete; three enrichment probes built + validated.
**Provenance:** produced from a read-only investigation (no pipeline changes, nothing promoted to
gold or committed). The probe scripts and data artifacts named in §9 live in the session sandbox
(`c:/tmp/competitor_probe/`, gitignored) and are reproducible from those scripts — only this
document is checked in.

---

## Verdict

Competitor analysis is **feasible and produces genuinely useful results today** — but only at the
**supplier co-occurrence** level (firms appearing in *comparable awards*), never co-bidding
(losing bidders are never published). Out of the box it is **thin and fragmented**: it is gated by
three concrete data weaknesses, each of which I measured and enriched:

| Gap | Out-of-the-box | After enrichment | Lever |
|---|---|---|---|
| Trade coverage (CPV) | 161 electrical/M&E awards | **2,124** (13×) | trade tagger (CPV→title→spend-cat) |
| Competitors surfaced for one focal firm | 28 (CPV-only) | **760** (27×) | trade tagger + CRO anchor |
| eTenders↔TED buyer join | ~85% of TED rows matched (after NFKD-safe norm) | ~92% after curated crosswalk | NFKD fold + curated rebrand/alias map |
| Cross-grain entity identity | name key fragments firms | CRO bridges all 3 grains where it resolves | CRO-anchored entity key |

---

## 1. Data inventory (real figures)

| Fact | Path | Rows | Grain | Key fields |
|---|---|---|---|---|
| eTenders awards | `data/gold/parquet/procurement_awards.parquet` | 62,763 (44,834 company-class) | award × supplier | `supplier_norm`, `Contracting Authority`, `Main Cpv Code`, `Tender/Contract Name`, `value_eur`, `value_kind`, `value_safe_to_sum`, `is_framework_or_dps`, `is_call_off`, `No of Bids Received`, `No of Awarded SMEs` |
| TED winner history | `data/silver/parquet/ted_ie_winner_history.parquet` | 23,263 (**2016–2023 only**) | notice × winner | `winner_name_norm`, `buyer_name`, `cpv_code`, `cpv_division`, `award_value_eur`, `cro_company_num`, `cro_match_method` |
| TED awards (eForms) | `data/silver/parquet/ted_ie_awards.parquet` | 13,744 (**2024+**) | notice × winner | adds `n_tenders_received`, `is_single_bid`, `procedure_type` (bids exist **only here**) |
| Public-body payments | `data/gold/parquet/procurement_payments_fact.parquet` | 425,544 | payment line | `supplier_normalised`, `publisher_name`, `amount_eur`, `realisation_tier` (SPENT/COMMITTED), `value_safe_to_sum`, `cro_company_num`, `spend_category` |
| CRO supplier match | `data/gold/parquet/procurement_supplier_cro_match.parquet` | 9,979 | supplier_norm | `company_num`, `match_method`, `match_confidence`, `n_cro` |

**Never-sum rule holds throughout:** award ceiling ≠ contract award ≠ payment. Only
`value_safe_to_sum` rows add, and never across facts/tiers.

**Coverage truth that shapes everything:** only **32.8%** of award rows carry a real `Main Cpv Code`
(`Spend Category` 80.2%, title 100%). TED winners split across two files by year. Public-body
payments are a different grain and a partial (floor) corpus.

---

## 2. Discovery methods — what the data actually supports

| Method | Verdict | Evidence |
|---|---|---|
| **Same CPV** | works but **sparse** — exact CPV finds only 161 electrical/M&E awards | CPV present on 32.8% of rows |
| **Same buyer (alone)** | **noisy** — dominated by cross-sector vendors | Jones's top shared-buyer firms = AECOM, AtkinsRéalis, PFH (IT), EY, Deloitte, KONE — not rivals |
| **Same buyer + same trade** | **the strong signal** | filtered to electrical/M&E it yields real rivals: J Vaughan Electrical, Tritech, Quinn Downes, Hayes Higgins, Killaree, Germar |
| **Title keywords** | high recall, needs word-boundaries + role split | recovers 1,200+ CPV-less awards; mixes contractors / M&E *consultants* / *wholesalers* |
| **Repeated winners** | works (counts are reliable) | — |
| **Co-bidding ("beat X")** | **impossible** | losing bidders never published; only `No of Bids Received` count exists |

---

## 3. Enrichment #1 — construction/M&E trade tagger  ✅ built

`build_trade_tags.py` → **`award_trade_tags.parquet`** (62,763 rows). Assigns a `trade_family`
(`electrical` / `mechanical_hvac` / `me_combined` / `fire_security` / `building_civil` /
`construction_other`) using **CPV first, then title keyword, then Spend Category**, recording
`trade_source` (cpv|title|spendcat) as a confidence proxy.

- **Uplift:** electrical/M&E **161 → 2,124 awards, 123 → 1,010 suppliers, 61 → 309 buyers** (≈13×).
- **Precision:** 30-row eyeball of title/spend-cat tags ≈ 29/30 correct (heating upgrades, boiler
  servicing, HVAC maintenance, electrical contractor services, structured cabling, fire alarm &
  emergency lighting). One false positive — "c**rm and e**discovery" matched `m and e` as a
  substring → fixed with word boundaries (`\bm and e\b`).
- **Known refinement:** distinguish **role** (contractor vs M&E design consultant vs wholesaler) —
  title keywords pull all three; "competitor" should be like-for-like.

## 4. Enrichment #2 — buyer matching  ✅ spec'd + seed produced

Raw buyer names don't join: **2,249 eTenders vs 871 TED buyers, only 418 exact matches**; the same
buyer carries 4–6 variants (TCD ×4, HSE ×6).

- **Naive parenthetical-strip is harmful** — it merges 11 distinct "St Joseph's NS (Letterkenny / Cork / …)"
  into one (the bracket is a *location*, not an acronym). A **safe** acronym-only strip preserves them
  (+13 matches only).
- **NFKD accent-fold helps, modestly.** Adding `strip_accents` to the safe norm recovers ~260
  unmatched TED rows by fixing accent mismatches (`Iarnród Éireann`↔`Iarnrod`, `Bord na Móna`).
  Matched buyer-norms 418 (raw) → 431 (acronym-strip) → **432** (+NFKD). After NFKD-safe norm,
  **85.4% of TED winner-rows (19,871/23,263) are already at buyers reconciled to eTenders**; 14.6%
  (3,392 rows) remain unmatched.
- **Only about HALF of the unmatched residual is recoverable — the rest is genuinely TED-only**
  (re-verified, double-check 2026-06-29). Of the top-25 unmatched buyers (3,392 rows), ~47%
  (~1,600 rows) ARE recoverable rebrands/renames with an eTenders counterpart (Irish Water→Uisce
  Éireann, NUIG→University of Galway, GMIT→ATU, Cork IT→MTU, Dept Education **and Skills**→Dept
  Education, ETB `&`↔`and`), but ~half (~650–750 rows) are **bodies with no eTenders presence at
  all** (Ervia, Eurofound, HEAnet, EU agencies) — a crosswalk cannot and must not "recover" these;
  they are correctly out of scope for national-feed overlap.
- **Net effect of a curated crosswalk: TED-buyer reconciliation ~85% → ~92% of TED volume** (NOT
  "+70% of unmatched"; that earlier figure wrongly assumed all top-N unmatched were recoverable).
- **Curation is mandatory — auto-matching is unsafe.** The seed (`buyer_crosswalk_seed.csv`,
  40 high-volume targets + suggested canonical + score) gets the *verdict* roughly right but the
  *specific mapping* wrong ~half the time (Social Protection→Enterprise; Cork IT mis-flagged
  TED-only when it's a merger into MTU; every Institute of Technology→"Limerick IT"; Science
  Foundation→"Cheshire Foundation"). Safe only at j≥0.8 (word-order/`&` variants). Recommended
  spec: **safe acronym-strip + NFKD accent-fold + a hand-curated rebrand/alias map** (~30–50 rows
  covers the recoverable volume).
- **Practical upshot for the feature:** the high-volume *dual-feed* buyers that drive same-buyer
  competitor overlap (HSE, TCD, OGP, universities, councils, big departments) are already matched
  or trivially crosswalkable; the genuinely TED-only residual (Ervia/Eurofound/HEAnet) is largely
  irrelevant to a contractor's competitor map. So the headline % was overstated, but the feature
  impact is intact.

## 5. Enrichment #3 — entity / group resolution  ✅ demonstrated

The single biggest constraint. `entity_res.py` shows:

- **CRO number is the only reliable anchor, and where it resolves it bridges all three grains:**
  JONES ENGINEERING = **393931**, KIRBY ENGINEERING = **41839**, MERCURY = **225667**,
  SUIR PLANT = **134550** — identical across awards + TED + payments.
- **Name-stem grouping is unsafe** — the "JONES" stem merges Jones Engineering with Jones Day (law),
  JLL (property), Jones Business Systems (IT); "KIRBY" merges the M&E firm with Kirby Healy *accountants*.
- **Same firm → two CROs:** DORNAN = 183069 vs DORNAN ENGINEERING = 212757 (conf 0.9 vs 0.5).
- **Group structure visible in TED:** "JONES ENGINEERING T/A H A O NEILL" = CRO 10341 (a subsidiary) →
  group rollup needs a **CRO→parent map**.
- **Payments are the worst & hide large sums:** ledger-category bleed fragments one supplier into ~10
  names (Killaree across "RCT ONLY" / "CAPITAL CONTRACTS EXPENDITURE" / … ≈ €33M true vs €8M
  CRO-matched); "DESIGNER ENGINEERING" shows €83.5M with **no CRO** (unlinked, and worth a data-quality look).

**Implication for the metric:** the competitor entity key must be
`COALESCE('CRO:'||cro_company_num, 'name:'||supplier_norm)`, with name-only entities flagged
low-confidence; payments must be CRO-keyed or they won't join.

---

## 6. Metric definitions (grounded; never-sum-safe)

Computed over the **comparable universe** (focal's trades ∪ buyers), entity = CRO-anchored:

- **total_comparable_wins** — `COUNT(*)` in universe (count, always safe)
- **buyer_overlap / category_overlap** — distinct shared buyers / shared trade families
- **adjusted_awarded_value** — `SUM(value_eur) FILTER (value_safe_to_sum AND NOT is_large_award_review)` — frameworks/≥€50m ceilings excluded; report frameworks separately as count + ceiling band
- **median/avg award** — over sum-safe rows
- **recent_wins** — award_date ≥ 36 months
- **top_buyers / repeat_buyer_share** — incumbency signal (≥2 wins same buyer)
- **single_bid_exposure** — % `n_bids=1` (with its `n`) — *factual signal, never a verdict*
- **framework_exposure** — % `is_framework_or_dps OR is_call_off`
- **payment_evidence** — appears in payments? `n_publishers` + sum-safe paid/ordered (separate grain, never added to awards)
- **source_confidence** — CRO `match_method` + value-coverage + sample size + match basis (CPV>buyer>title)

**Safe wording:** "appears in comparable awards", "repeat winner", "likely competitor",
"same-buyer/category overlap". Banned: beat, favoured, influence, risk, captured.

---

## 7. Worked example — Jones Engineering (real numbers)

Focal CRO 393931; 19 buyers across electrical/mechanical/M&E/fire/building. Competitors = company-class
firms with a construction/M&E trade tag sharing ≥1 Jones buyer, CRO-anchored:

| entity | name | shared buyers | comparable wins | adj. awarded € (safe) | frameworks | conf |
|---|---|---|---|---|---|---|
| CRO:519061 | Triur Construction | 5 | 20 | €636,980 | 14 | 0.5 |
| CRO:232782 | Linham | 5 | 8 | €16,420,180 | 3 | 0.9 |
| CRO:241278 | John Sisk & Son | 4 | 8 | €48,995,339 | 3 | 0.9 |
| name:… | Carey Building Contractors | 4 | 12 | €33,192,353 | 2 | **0.0 (name-only)** |
| CRO:241986 | J Vaughan Electrical Contractors | 4 | 6 | €500,000 | 3 | 0.9 |
| CRO:310747 | Tritech Engineering | 4 | 5 | €0 (all framework) | 1 | 0.5 |
| CRO:354913 | Hayes Higgins Partnership (M&E consult.) | 4 | 7 | €60,000 | 4 | 0.9 |

**Distinct competitors surfaced: CPV-only = 28 → enriched = 760.** The mix correctly spans
M&E specialists and general construction (Jones bids both); a pure-electrical view filters
`trade_family IN ('electrical','mechanical_hvac','me_combined')`.

---

## 8. Caveats

1. No losing bidders → co-occurrence only, never "out-bid".
2. CPV 32.8% native — trade analysis depends on the tagger (title/spend-cat = medium/low confidence).
3. TED winners split 2016–23 (winner_history) vs 2024+ (awards); SME data eTenders-only; bids TED-2024+ only.
4. CRO 64% on award suppliers, far lower on payments; same firm can carry 2 CROs; groups span multiple CROs.
5. Payments names carry ledger-category bleed; big unlinked sums exist (Designer €83.5M no CRO).
6. Three money grains never sum.

## 9. Artifacts (in `c:/tmp/competitor_probe/`)

- `award_trade_tags.parquet` — 62,763 awards tagged with trade_family + source.
- `buyer_crosswalk_seed.csv` — 40 high-volume TED↔eTenders buyer pairs for curation.
- Probes: `invest1/2.py`, `build_trade_tags.py`, `buyer_norm2.py`, `entity_res.py`, `final_competitors.py`.

**Recommended build order (when greenlit):** (1) promote the trade tagger as a gold column on awards;
(2) curate the buyer crosswalk + add NFKD fold; (3) CRO-anchored entity key + name-only low-confidence
flag + CRO→parent group map; (4) `v_procurement_comparable_awards` (eTenders⊎TED) + competitor summary
view; (5) data_access + tests; (6) UI last.

---

## 10. Productisation — two concrete scopes (free vs paid)

Both sit on a small shared prerequisite layer and split along the existing free-Dáil-Tracker /
paid-BI-spinout line. Nothing here is built; all of it is greenlight-gated on the wording /
confidence-taxonomy / crosswalk-curation / pricing decisions (see §8 and the roadmap's user-domain list).

### Scope 0 — Shared prerequisites (build once)

| Prereq | What | Effort | Status |
|---|---|---|---|
| Trade tag | promote the CPV→title→spend-cat tagger as a gold column (`trade_family` + `trade_source`) | M | validated (161→2,124, ~95% precision) |
| Buyer norm + crosswalk | NFKD-safe buyer normaliser + ~30–50-row curated rebrand/alias map | M | seed produced; needs human curation |
| CRO-anchored entity key | `COALESCE('CRO:'||cro,'name:'||norm)` + surface `match_confidence`/`n_cro` | S | data already persisted; additive |

Free slice needs the first two; paid slice needs all three.

### Scope A — FREE: Buyer Dossier + Trade Market Map  *(the civic / reputation engine)*

~80% surfacing of views that already exist, plus the trade tag. User job: *understand a buyer or a trade*
(journalists, procurement officers, trade bodies, new entrants).

- **A1 · Buyer dossier** — top suppliers by trade, category/trade mix, incumbency (repeat-supplier share),
  **single-bid rate** (wires the built-but-unused `v_procurement_competition`), framework use, value trend,
  live/expiring tenders for this buyer. Built on `v_procurement_authority_summary` (exists) + trade tag +
  **buyer crosswalk** (the crosswalk is the prerequisite that unblocks the ~31% of TED competition buyers
  that don't join raw). New: `v_procurement_authority_category_mix`, incumbency wrapper, competition fetch
  wrapper. **Effort M.**
- **A2 · Trade market map** (per CPV-4) — who wins, concentration (top-N share), SME win rate, single-bid %,
  typical award band (p25/median/p75), expiring contracts, year trend. Built on `v_procurement_bid_signal`
  + `v_procurement_cpv_summary` (**both already exist** with median/IQR); the trade tag lifts coverage from
  the 161 CPV-only slice to the 2,124-award enriched universe. New: `n_buyers` + `v_procurement_cpv_top_parties`.
  **Effort L (light).**
- **Rails:** award grain only; single-bid = signal not verdict; SME rate carries the CPV-coverage caveat; never sum grains.
- **Out of scope:** per-firm anything, payments blending, person-row export.
- **Smallest free ship:** A2 alone — leans almost entirely on two existing views + the trade tag.

### Scope B — PAID: Contractor Research Pack  *(the BI-spinout supplier side)*

The one genuinely new asset (the competitor graph) + export, per firm. User job: *who do I compete against,
which buyers are they strongest with, where should I aim?* (SME contractors + bid consultants).

One generated report per firm (the Jones 760-competitor demo, productised):
1. **Your field** — N firms in your comparable awards (trades ∪ buyers), CRO-anchored, confidence-flagged.
2. **Competitor table** — comparable wins, shared buyers, shared trades, adjusted awarded value (sum-safe only),
   recent wins, framework exposure, single-bid exposure, payment evidence, source confidence.
3. **Buyer view** — who is strong with the buyers you serve + buyers in your trade you haven't won yet.
4. **Opportunity layer** — open TED tenders + expiring contracts in your trades (CPV-overlap; navigational suggestion).
5. **Export** — CSV/PDF, value-safe columns + caveat column + source links, through a `public_display`-gated view.

- **Built on:** new `v_procurement_comparable_awards` (eTenders⊎TED, deduped, trade tag + entity key) **M**;
  new `v_procurement_competitor_summary` (parameterised by focal entity) + data_access **M**; reuses trade tag,
  crosswalk, CRO key, `expiring_contracts`/`live_tenders` (exist), export helper (exists); UI report panel **M**.
- **Rails (what makes it sellable):** co-occurrence not co-bidding ("appears in comparable awards" / "likely
  competitor" / "repeat winner"); confidence flags mandatory; payments shown beside, never added to awards;
  **no lobbying/diary overlays in the paid pack** (ethics firewall — those stay free-side); pricing model
  unvalidated → ship as a research artifact, not a subscription oracle.
- **Out of scope (v1):** watchlists/alerts (needs accounts/PII/GDPR — separate gated build), CRO→parent
  group rollup (curated, later), any "beat/favoured/influence" framing.
- **Smallest paid ship:** `v_procurement_comparable_awards` + the competitor table on one supplier profile
  (no export, no opportunity layer) — proves the 28→760 value end-to-end.

### Sequencing

```
Scope 0 (prereqs) ─► Scope A (free)  ─► reputation + data validation (mostly surfacing, low risk)
        │
        └────────── ─► Scope B (paid) ─► revenue (one net-new view + export)
```

Scope A is mostly **wiring** (Phase 2/3 ingredients exist); Scope B's only genuinely new asset is the
comparable-awards graph plus export. Accuracy ceiling throughout = entity resolution (CRO 64% on awards,
lower on payments) — confidence flags are load-bearing, not polish.
