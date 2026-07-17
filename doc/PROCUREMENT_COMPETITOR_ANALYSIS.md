---
tier: RECORD
status: LIVE
domain: procurement
updated: 2026-07-10
supersedes: []
read_when: assessing whether/how supplier-side competitor analysis (co-occurrence, trade tagger, CRO anchor) can be built or extended
key: RECORD|LIVE|procurement
---

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
- **payment_evidence** — appears in payments? `n_publishers` + sum-safe paid/ordered (separate grain, never added to awards; **firm-level only** — buyer-level awarded-vs-paid is non-comparable, see §8.7)
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
7. **Buyer-level awarded-vs-paid is non-comparable** (HSE €447M awarded-safe vs €11.5bn paid = two different
   populations, not a gap) — award→payment corroboration is **firm-level only, never a ratio**; and name-key
   misses fake €0-paid (Total Highway Maintenance) → paid column must be CRO-keyed.

## 9. Artifacts & generation logic (in `c:/tmp/competitor_probe/` — gitignored sandbox)

**`README.md` there is the index** — it holds the regeneration order and the full figure→script map.
All scripts run read-only against the gold/silver parquet, **from the repo root**, on `.venv` python.

- `award_trade_tags.parquet` — 62,763 awards tagged with trade_family + trade_source. Regenerate:
  `scripts/build_trade_tags.py` (this script IS the definition of "Electrical & M&E": trade_family ∈
  {electrical, mechanical_hvac, me_combined}; CPV→title→spend-cat, priority order).
- `buyer_crosswalk_seed.csv` — 40 high-volume TED↔eTenders buyer pairs for curation. Regenerate:
  `scripts/gen_artifacts.py`.
- `procurement_intel_mockup.html` — the app design-target mockup (static; real figures hard-coded
  from the §10 grounding queries); published at
  https://claude.ai/code/artifact/f0b0eb99-6c51-4831-9cee-c88cc83bf259
- `scripts/` — the generation logic: `build_trade_tags.py` (§3), `verify_buyer.py` (§4),
  `gen_artifacts.py` (§4), `entity_res.py` (§5), `final_competitors.py` (§7, the 28→760 run),
  `ground_panels.py` (§10 grounded panel numbers + the A3-trap evidence), and
  `reverify_all.py` — a PASS/FAIL harness re-deriving every headline claim (claims are
  point-in-time 2026-07-09; exact counts drift slightly after source refreshes — 2026-07-10 run:
  17/20, all conclusion-bearing checks exact; drift was +45 company rows / +210 TED notices / +0.2pp CRO).
- Early one-off probes (`schema_dump/invest1/invest2/eyeball/buyer_norm(2)/defaulter_*`) were not
  kept — their findings are recorded in §1–§5 and re-derived by `reverify_all.py`; the defaulters
  evidence CSVs remain in `c:/tmp/bid_enrich_scoping/`.

**Recommended build order (when greenlit):** (1) promote the trade tagger as a gold column on awards;
(2) curate the buyer crosswalk + add NFKD fold; (3) CRO-anchored entity key + name-only low-confidence
flag + CRO→parent group map; (4) `v_procurement_comparable_awards` (eTenders⊎TED) + competitor summary
view; (5) data_access + tests; (6) UI last.

---

## 10. Productisation — two concrete scopes (free vs paid)

Both sit on a small shared prerequisite layer and split along the existing free-Dáil-Tracker /
paid-BI-spinout line. Nothing here is built; all of it is greenlight-gated on the wording /
confidence-taxonomy / crosswalk-curation / pricing decisions (see §8 and the roadmap's user-domain list).

### Grounded panel numbers (verified 2026-07-09 — real queries; treat as fact)

**A2 · Trade market map — M&E** (electrical + mechanical_hvac + me_combined): **2,124 awards · 1,010 firms ·
309 buyers**; top-10 firms hold only **11%** of awards — a broad, **non-degenerate** market, so a map is
genuinely informative; sum-safe award band **€50k p25 / €156k median / €380k p75** (n=484 valued);
**SME win-rate 91%** (n=2,020 awards with SME data); steady ~200–270 awards/yr 2020–2025. Top firms by count:
EWL Electric, Matt O'Mahony Associates, Jones Engineering, City Electrical Factors, Global Rail Services,
Delap Waller.

**A1 · Buyer dossier — HSE:** 2,197 company awards · 1,184 suppliers · **incumbency 63%** (share of awards to
repeat suppliers) · **framework share 60%** · **single-bid 13%** lot-level (40/311 lots, TED 2024+).
*Design-shaping caveat:* unsliced, HSE's top suppliers are all medical (Uniphar, Irish Hospital Supplies,
Fannin) — the dossier **must be trade-sliceable** or it is useless to a contractor persona. The trade tag is
load-bearing for the buyer dossier too, not just the market map.

**A3 / B·3 · Award→payment, firm level** (Jones shortlist rows, paid column added):

| firm | awarded-safe | paid-safe | reading |
|---|---:|---:|---|
| John Sisk & Son | €49M | €638M | firm-level corroboration at scale |
| Cumnor Construction | €0 | €122M | award-invisible but payment-visible — the paid grain adds real information |
| Triur Construction | 20 wins · 5 shared buyers | €33M | wins corroborated by realised spend |
| J Vaughan Electrical | €500k | €375k | like-for-like specialist, both grains small |
| Carey Building Contractors | €33M | €0 | name-only (no CRO) — €0 is likely a name-mismatch **artifact**, not a fact |

Two lessons: the paid column is the differentiating firm-level evidence (Sisk, Cumnor); and name-key coverage
limits are visible in-sample (Carey €0) — the paid column must be **CRO-keyed**, with name-only rows rendering
a flag, not a number. The **buyer-level** version of this comparison is banned — see A3.

### Scope 0 — Shared prerequisites (build once)

| Prereq | What | Effort | Status |
|---|---|---|---|
| Trade tag | promote the CPV→title→spend-cat tagger as a gold column (`trade_family` + `trade_source`) | M | validated (161→2,124, ~95% precision); **owned by this doc** |
| Buyer norm + crosswalk | NFKD-safe buyer normaliser + ~30–50-row curated rebrand/alias map | M | seed produced; needs human curation; **build spec owned by `doc/BUYER_DOSSIER_DESIGN.md`** (`buyer_xref.csv`, fail-closed) |
| CRO-anchored entity key | `COALESCE('CRO:'||cro,'name:'||norm)` + surface `match_confidence`/`n_cro` | S | data already persisted; additive; **build spec owned by `doc/ENTITY_CROSSWALK_ORG_DOSSIER_DESIGN.md`** (`entity_xref` + normaliser unification PR0) |

Free slice needs the first two; paid slice needs all three.

### Scope A — FREE: Buyer Dossier + Trade Market Map  *(the civic / reputation engine)*

~80% surfacing of views that already exist, plus the trade tag. User job: *understand a buyer or a trade*
(journalists, procurement officers, trade bodies, new entrants, **SME contractors** — this is where the
SME lives; they are the free audience, not the paid buyer, see Scope B).

- **A1 · Buyer dossier** — top suppliers by trade, category/trade mix, incumbency (repeat-supplier share),
  **single-bid rate** (wires the built-but-unused `v_procurement_competition`), framework use, value trend,
  live/expiring tenders for this buyer. **Must be trade-sliceable** — unsliced, a buyer like HSE surfaces only
  medical suppliers (see grounded numbers above); the trade tag is load-bearing here too. Built on
  `v_procurement_authority_summary` (exists) + trade tag + **buyer crosswalk** (the crosswalk is the
  prerequisite that unblocks the ~31% of TED competition buyers that don't join raw). The authoritative build
  spec for this panel is `doc/BUYER_DOSSIER_DESIGN.md` (see §11) — this doc adds the trade slice, not a rival
  spec. **Effort M.**
- **A2 · Trade market map** (per CPV-4) — who wins, concentration (top-N share), SME win rate, single-bid %,
  typical award band (p25/median/p75), expiring contracts, year trend. Built on `v_procurement_bid_signal`
  + `v_procurement_cpv_summary` (**both already exist** with median/IQR); the trade tag lifts coverage from
  the 161 CPV-only slice to the 2,124-award enriched universe. New: `n_buyers` + `v_procurement_cpv_top_parties`.
  **Effort L (light).**
- **A3 · Award→payment reality — FIRM-level card only.** ⚠️ **Corrected 2026-07-09: the buyer-level version
  of this panel is a trap.** Verified: HSE shows €447M awarded (sum-safe) vs €11.5bn paid (78,869 lines) — a
  26× "gap" that is actually **two non-comparable populations** (*paid* = ALL disclosed spend >€20k, incl.
  pharma/devices; *awarded* = only the tendered subset with sum-safe values). Rendered side-by-side on a buyer
  it reads as a false story. The card therefore renders at the **firm** level only: the same entity's
  awarded-safe and paid-safe, separately labelled, **corroboration not comparison** — the PFH sample's §4
  (`doc/samples/BI_SUPPLIER_DOSSIER_SAMPLE_PFH.md`) is the template. If a buyer page ever shows both figures,
  they are two separately-labelled figures with a hard "different populations — not comparable" line —
  **never a ratio, never a gap**. Paid column CRO-keyed only; a name-key miss renders as a flag, not €0
  (Total Highway Maintenance shows €0 paid purely from a name mismatch). Built on
  `procurement_payments_fact` (CRO-keyed) beside the firm's award rows. **Effort M.**
- **Rails:** award grain drives competition metrics; the A3 payment card is **firm-level**, a *separate grain
  shown beside*, never summed into awards, never a ratio/gap (buyer-level comparison banned — see A3);
  single-bid = signal not verdict; SME rate carries the CPV-coverage caveat; never sum grains.
- **Out of scope:** per-firm *competitor* analysis (that's Scope B — the A3 card is a single-firm fact
  display, not a comparison), buyer-level awarded-vs-paid comparison (banned — see A3), payment value summed
  into award value, person-row export.
- **Smallest free ship:** A2 alone — leans almost entirely on two existing views + the trade tag.

### Scope B — PAID: Contractor Research Pack  *(the BI-spinout supplier side)*

Two genuinely new assets, packaged per firm: the competitor graph **and the firm-level award→payment
corroboration layer**. The moat is the second one — rivals see the *notice*; almost none see whether it turned
into *money* — and it holds **only at the firm level** (buyer-level awarded-vs-paid is non-comparable; see the
A3 correction). User job: *who wins comparable work, which buyers are they strongest with, and did those awards
become payments?*

**Buyer = bid consultants / bid writers** (a professional, recurring, billable input they can pass through), plus
mid-tier contractor BD functions and teaming / M&A advisors. **The SME contractor is the FREE audience, not the
paid buyer** — cash-thin, no BD budget, buys work-winning alerts, not landscape analysis; monetise the consultant
who serves many SMEs, not the SME. **Gate:** validate willingness-to-pay with bid consultants (a paid one-off pack)
*before* any subscription / self-serve / watchlist build — pricing is unvalidated (see §8).

One **manually-QA'd** report per firm (the Jones probe productised — human-checked and confidence-tiered, not
an auto-published dragnet):
1. **Shortlist, not a roster** — a ranked, confidence-tiered **top ~30 per shared buyer / trade**, long tail
   collapsed to a count. The **760** from §7 is reported only as a *coverage denominator* ("760 firms appear in
   your comparable-award universe"), **never as the headline boast** — trade-tag precision (~95%) is not competitor
   *relevance* (a general-construction firm that won one M&E-tagged job is not a rival). Each row CRO-anchored + confidence-flagged.
2. **Competitor table** — comparable wins, shared buyers, shared trades, adjusted awarded value (sum-safe only),
   recent wins, framework exposure, single-bid exposure, source confidence.
3. **Award→payment reality (the moat — firm-level only)** — for each shortlisted rival, the same entity's
   awarded-safe and paid-safe figures, separately labelled, as corroboration (never summed, never a ratio;
   the PFH sample's §4 is the template, and buyer-level comparison is banned — see A3). Real rows: Sisk €49M
   awarded-safe / €638M paid-safe; Cumnor €0 awarded / €122M paid (award-invisible, payment-visible). This is
   what rival tender tools cannot show — they stop at the notice. Paid column CRO-keyed only; name-only rows
   (Carey €33M awarded / €0 paid — likely a mismatch artifact) render the flag, not the €0.
4. **Buyer view** — who is strong with the buyers you serve + buyers in your trade you haven't won yet.
5. **Opportunity layer** — open TED tenders + expiring contracts in your trades (CPV-overlap; navigational suggestion).
6. **Export** — CSV/PDF, value-safe columns + caveat column + source links, through a `public_display`-gated view.

- **Built on:** new `v_procurement_comparable_awards` (eTenders⊎TED, deduped, trade tag + entity key) **M**;
  new `v_procurement_competitor_summary` (parameterised by focal entity) + data_access **M**; reuses trade tag,
  crosswalk, CRO key, `expiring_contracts`/`live_tenders` (exist), export helper (exists); UI report panel **M**.
- **Rails (what makes it sellable):** co-occurrence not co-bidding ("appears in comparable awards" / "likely
  competitor" / "repeat winner"); confidence flags mandatory + manual QA before any pack ships; payments shown
  beside, never added to awards; **corporate-class entities only — no person-level profiles built from the
  supplier field** (sole traders appear as suppliers; same GDPR discipline as the rest of the product);
  **coverage caveat stated on every pack** — eTenders/TED skew to *above-threshold* work, so the data is thinnest
  at the small-value SME tier; the pack describes the *published-award* market, not the whole market;
  **no lobbying/diary overlays in the paid pack** (ethics firewall — those stay free-side); pricing model
  unvalidated → ship as a research artifact, not a subscription oracle.
- **Out of scope (v1):** watchlists/alerts (needs accounts/PII/GDPR — separate gated build, and gated on the WTP
  proof above), CRO→parent group rollup (curated, later), any "beat/favoured/influence" framing, person-row profiles.
- **Smallest paid ship:** `v_procurement_comparable_awards` + a **manually-QA'd top-~30 shortlist** and the
  **firm-level** award→payment card on one supplier profile (no export, no opportunity layer) — proves the
  shortlist + moat end-to-end on a single firm.

### Sequencing

```
Scope 0 (prereqs) ─► Scope A (free)  ─► reputation + data validation (mostly surfacing, low risk)
        │
        └────────── ─► Scope B (paid) ─► revenue (one net-new view + export)
```

Scope A is mostly **wiring** (Phase 2/3 ingredients exist); Scope B's genuinely new assets are the
comparable-awards graph **and the firm-level award→payment corroboration layer (the moat)**, plus export.
Accuracy ceiling throughout = entity resolution (CRO 64% on awards, lower on payments) — confidence flags are
load-bearing, not polish; the paid pack is graded by domain experts who spot every merged/split entity, so it
ships **manually QA'd and thresholded to a shortlist**, never as a raw 760-name dragnet.

---

## 11. Relationship to ongoing work (reconciliation, 2026-07-09)

This doc is one layer of a single program, not a competing plan. Cross-checked against the six sibling design
docs, the produced sample, and the sandbox prototype: **no hard conflicts**. The only tensions are sequencing
(manual research packs now vs alert-SaaS later) and the ethics gate on influence data — both resolved below.

| Asset | Relation | What it means for this doc |
|---|---|---|
| `doc/PROCUREMENT_INTELLIGENCE_ROADMAP.md` | **COMPLEMENT — parent** | The umbrella plan. Scope A = its Phase 2 (buyer dossier + CPV market maps); Scope B = its "bespoke reports", **not** its Phase 5 SaaS. Same blockers (CPV-null, buyer reconciliation, match-confidence surfacing, `public_display` gate on name-keyed paid export). This doc files under Phase 2/3 as the supplier/competitor-side detail. |
| `doc/BI_SPINOUT_ARCHITECTURE.md` | **COMPLEMENT — commercial wrapper** | Same free/paid rule ("seeing a public fact = free, recurring work = paid"); reports-first sequencing = this doc's pack-before-subscription WTP gate; already prices a **bid-consultant tier (€2,400–6,000/yr, white-labelled client reports)** — independently confirms Scope B's ICP. Its ethics bright line binds the pack: diary OUT of paid entirely; lobbying per-report + owner-gated, never bulk/API; competitor co-occurrence stays **award-side only**. |
| `doc/BUYER_DOSSIER_DESIGN.md` | **OVERLAP — supersedes A1's spec** | This IS the free buyer dossier, fully specified: `buyer_xref.csv` fail-closed crosswalk (draft at `c:/tmp/buyer_xref_draft/`), `v_procurement_authority_cpv_summary`, dual single-bid lenses (eTenders award-level + TED lot-level, never merged), incumbency, `dossiers.buyer_dossier()`. A1 here = that build + the trade slice + the firm-level A3 card; do not re-specify. |
| `doc/TENDER_ALERT_SYSTEM_DESIGN.md` | **COMPLEMENT — later graduation** | The opportunity layer's eventual SaaS form (accounts/email/scheduler = roadmap Phase 5, owner-gated PII build). **Not needed for the pack**: the opportunity layer ships now as `rebid_radar` + CSV. Reuses the same `buyer_xref`. Don't build the alert shell to satisfy this doc — it sits behind the WTP gate. |
| `doc/SOURCE_CONFIDENCE_SYSTEM.md` | **COMPLEMENT — substrate** | Supplies the confidence flags this doc calls load-bearing: A–D trust grade, match badges, the `exact_ambiguous`→"possible match" wording fix, export envelope columns. Adopt it under BOTH slices; do not re-invent flags in the pack. |
| `doc/ENTITY_CROSSWALK_ORG_DOSSIER_DESIGN.md` | **OVERLAP — supersedes Scope 0's entity-key spec** | This IS the CRO-anchored supplier spine (`entity_xref` + `v_entity_xref`, `match_tier` fail-closed, and **PR0 = unifying the 4 divergent name normalisers on `shared/name_norm`** — the foundational fix everything here needs, incl. the Carey/Total-Highway €0-paid artifacts). Its org-360 dossier is a *different artifact* (single-org deep-dive) from the comparative competitor pack — complementary outputs on the same spine. |
| `doc/samples/BI_SUPPLIER_DOSSIER_SAMPLE_PFH.md` | **OVERLAP — the pack's ancestor & template** | Already demonstrates the shape and wording: its §4 awarded-vs-paid side-by-side (separate labelled cells, **no ratio, no netting**) is the firm-level A3/B·3 card verbatim; caveats reproduced verbatim from `caveats.py`; provisional labels ⚠️-flagged for owner sign-off. The research pack = this sample + the competitor shortlist + the opportunity layer. |
| `pipeline_sandbox/bid_intelligence/` (`build_bid_pack`) | **OVERLAP — working prototype of Scope B** | Read-only composition of existing views (comparable awards, `bid_signal`, buyer payments, `active_firms`, `incumbent_payment_evidence`, `rebid_radar`, Markdown render) with the rails already enforced (no price, no win-probability, grains never blended, sole-traders excluded). **One gap: it keys competitors on `supplier_norm`, not CRO** — graduation requires the entity-xref spine. Graduate this prototype; don't rewrite it. |
| `c:/tmp/bid_enrich_scoping` (sandbox, not in repo) | **COMPLEMENT — opportunity-layer feed** | Demand-side capital pipelines (PI2040 tracker, social-housing construction status, school/HSE capital) map onto the pack's opportunity layer as future inputs. The risk overlays are settled: tax defaulters **KILLED** (defamation/GDPR/zero base rate); EU sanctions = internal data-hygiene only, never a user-facing flag. |

### Consolidation verdict

- **This doc stays authoritative for:** the trade tagger, the trade-market-map numbers, the competitor
  shortlist definition (top ~30, tail collapsed, 760 = denominator only), the firm-level A3 correction, the
  PAID ICP (bid consultants) + WTP gate.
- **It defers to:** BUYER_DOSSIER_DESIGN (buyer dossier build), ENTITY_CROSSWALK (entity spine + PR0),
  SOURCE_CONFIDENCE (flags/badges/export envelope), BI_SPINOUT (monetisation + ethics), ROADMAP (overall
  phasing), TENDER_ALERT (post-WTP-gate SaaS shell). No doc is redundant enough to delete; the near-dup risk
  was A1-vs-BUYER_DOSSIER and Scope-0-vs-ENTITY_CROSSWALK, both resolved by the ownership pointers above.
- **Two crosswalks, two name-spaces, never conflate:** `buyer_xref.csv` (buyer names) vs
  `entity_xref` (supplier CRO). Every doc that touches both warns about this; it is the one integration
  mistake available.

### Unified build sequence (across all docs)

1. **Foundations (each ships value alone):** ENTITY_CROSSWALK **PR0** (normaliser unification — fixes
   distress-join-zero and the fake-€0-paid artifacts) + SOURCE_CONFIDENCE Phase 0 (vocab lock) + ROADMAP
   Phase 0 (grain badge, confidence pill, export primitive).
2. **FREE slice:** curate `buyer_xref.csv` + `v_procurement_authority_cpv_summary` → BUYER_DOSSIER build
   (trade-sliceable A1) + promote the trade tagger to gold → A2 trade market map → firm-level A3 card.
   Surface `match_confidence` (the roadmap's headline fix) en route.
3. **PAID spine:** `entity_xref` → CRO-anchor `build_bid_pack.active_firms` → graduate the sandbox into
   `dossiers.py` → manually-QA'd shortlist packs in the PFH sample's shape → sell to bid consultants →
   **WTP gate**.
4. **Confidence badges + export envelope** across both slices (SOURCE_CONFIDENCE Phases 1–2).
5. **Only after the WTP gate clears:** TENDER_ALERT operational shell (accounts/email/scheduler) +
   watchlists (ROADMAP Phase 5).
