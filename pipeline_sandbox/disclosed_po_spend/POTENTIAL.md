# What this disclosed dataset enables

_Synthesis of six investigations over `data/raw_bq/bq-results-20260619-122315-1781871808837.csv` (582,119 rows, 216 bodies, 2011-q1 → 2026-q1). Read-only sandbox work; nothing promoted to silver/gold._

**The non-negotiable rule, applied throughout.** Bodies with ~100% blank PO publish PAYMENT lists (money actually paid); bodies with ~0% blank PO publish PURCHASE-ORDER commitments (money committed, may never fully pay); utilities/regulators publish per-category quarterly ROLL-UPS that are not EUR-20k line items at all. The split is roughly 99 payment bodies / 89 PO bodies / 28 mixed. These units must NEVER be summed as one "spend" figure — the ~EUR 117bn gross of the whole file is meaningless. Every euro below is **gross line value within a single body**, or is explicitly flagged gross-line-value-not-spend when it spans bodies.

---

## 1. How rich is it?

Richer than a typical thin EUR-20k transparency dump. A thin dump gives body + amount + date only; this gives **four usable analytic axes at once**: supplier, line-level description, body, and time (year + quarter).

**Fields & population.** Eight columns; the analytic core is near-complete: `Total` 100% populated (Float, EUR), `Supplier` 99.5% populated, `Description` 96.1% populated, year/quarter/entity 100%. The one structurally-empty field is `PO` (41.25% blank) — but that blank rate is a *signal, not missingness*: it is the payment-list vs PO-commitment divider (6 bodies publish no supplier at all). The `entity` field carries an "Agency : " prefix on many bodies that must be stripped.

**Supplier-name quality & fragmentation.** 41,990 raw distinct suppliers collapse to ~37,397 normalised — i.e. ~4,592 redundant variants caused by embedded newlines, double-spaces and ltd/limited/plc toggling. Worst offenders: `marine environmental resource conservation consultants` (10 spelling variants), `information security assurance services` (7), `image supply systems audio visual` (6), `thomson reuters professional uk` / `healy partners architects` / `rsm ireland business advisory` / `codec dss` (5 each). **Supplier names MUST be re-normalised before any cross-body rollup**; rough normalisation can also over-collapse (a bare "electric" pooled Electric Ireland + Electric Skyline + dozens of unrelated "*electrical ltd" sole traders — flagged and excluded). Names are leads, not legal identity (no CRO column in this extract).

**Description taxonomy — MIXED, body-dependent, NOT one bounded vocabulary.** 48,020 distinct descriptions. ~2,425 are shared controlled terms reused across bodies (covering 211k of 559k described rows, e.g. "Rent", "Insurance", "Energy / Utilities"); councils, HSE, Tusla and OPW use a tight AFS/chart-of-accounts controlled vocabulary (desc-reuse 0.005–0.02). But **45,594 of 48,020 descriptions are used by exactly one body** — a long free-text tail. Departments like Agriculture emit per-invoice free text ("Tranche 56", "Scrapie Fact Testing Aug 22", bare invoice numbers; reuse 0.23). Practical consequence: **cross-body category analytics only work on the ~2,425 shared coded terms**; everything else needs keyword themeing (see §3 trends, which leaves ~50% in `other`).

**Per-body density.** Row volume rises ~8x, from 7,420 rows (2012) to 75,937 (2025) — coverage *broadens* as bodies onboard. Dozens of bodies sit on unbroken 13–14-year quarterly runs.

**Richness verdict:** high row-and-axis richness, **medium** semantic richness. Total is 100% populated, supplier 99.5%, description 96%, across 216 bodies and 15 years. The caps are: description is body-dependent MIXED; supplier names are fragmented and need a normalisation pass; and the extract carries **no** `amount_semantics` / `value_safe_to_sum` / VAT / per-record source-URL / CRO enrichment that our parsed corpus adds. Net: it needs a normalisation + semantics-tagging pass to be analysis-ready, after which it is genuinely multi-dimensional.

---

## 2. How far back does it go?

**DEEP and BROADENING.** Spine runs **2011-q1 → 2026-q1**, dense and reliable from **2012**; 2011 (9 rows) and 2026 (15,029 rows) are partial tails.

**Overall timeline** (rows / gross line value — mixed semantics, NOT a spend sum):

| Year | rows | gross EUR |
|---|---:|---:|
| 2011 | 9 | 0.25m |
| 2012 | 7,420 | 765m |
| 2014 | 14,863 | 1.51bn |
| 2017 | 31,020 | 4.14bn |
| 2019 | 53,732 | 7.00bn |
| 2021 | 43,352 | 8.68bn |
| 2023 | 66,839 | 15.71bn |
| 2024 | 73,987 | 15.69bn |
| 2025 | 75,937 | 15.89bn |
| 2026* | 15,029 | 2.16bn (partial) |

**Long-run bodies.** The single deepest run is **Crawford Art Gallery — 60 unbroken quarters back to 2011-q1, 100% continuous**. Other 14-year 100%-continuous runs: Dept of Agriculture, Dept of Public Expenditure & Reform, Enterprise Ireland (each 57 quarters to 2026-q1), IDA, Dept of Foreign Affairs, State Laboratory (56q), then RSA, Companies Registration Office, An Garda Síochána, and Cork / Clare / Wicklow County Councils (54–55q, most starting 2012-q4).

**Sparse / one-off bodies** (≤2 quarters — recent onboarders, not historical gaps): Houses of the Oireachtas Service (2026-q1 only, EUR 7.87m), National Concert Hall, Judicial Council, Section 38 : Beaumont Hospital (EUR 255m across 2 quarters), An Coimisiún Toghcháin, NTMA Future Ireland Fund, NTMA Infrastructure/Climate/Nature Fund.

**Earlier history than our corpus — 32 bodies.** The HSE recovery is the headline already established: disclosed starts **2017-q3, ours 2021 → +4 years** (recovers 2017-q3..2020-q2, 11 quarters, plus the 2025-q4 + 2026-q1 tail; note a real internal gap 2020-q3..2021-q3 that the file does NOT fill; 30 distinct HSE quarters total). But the extract back-fills far more:

- **+11–12 years** on core departments/agencies: Agriculture (2012 vs ours 2024), Finance (2012 vs 2024), Enterprise Ireland (2012 vs 2023), Irish Prison Service (2013 vs 2024), National Transport Authority (2013 vs 2024).
- **+8 years** Revenue (2012 vs 2020), **+5y** Dept of Transport (2013 vs 2018).
- **+4–9 years across ~21 county councils**, most back to 2012-q4: Fingal +9, Longford +9, Donegal +6, Clare +6, Cork County +5, Kildare/Kilkenny/Meath/Monaghan/South Dublin/Westmeath +4.

_Caveat:_ the auto-CSV dedup mis-paired "Galway County Council" with our short publisher "Galway City", inflating one row to +13y; the true Galway County gap is +4y (Galway City separately +2y).

---

## 3. What can be determined from it?

### Supplier market structure & concentration

**No single supplier dominates the whole 216-body market** — even the largest, NBI Infrastructure DAC (EUR 1.34bn, broadband rollout) and BAM (EUR 1.16bn across 25 bodies), are sub-2% of gross. **Real power shows up WITHIN each body:**

| Body | top-5 share | top supplier |
|---|---:|---|
| Dept of Environment, Climate & Communications | 83.3% | NBI Infrastructure DAC (78.1% alone) |
| An Garda Síochána | 50.6% | Accenture (16.0%) |
| Office of Public Works | 49.5% | Sisk (13.0%) |
| Revenue | 49.0% | Accenture (16.8%) |
| Dept of Education | 39.3% | Rhatigan ABM (14.2%) |
| Dept of Defence | 29.8% | Airbus Defence & Space SAU (16.2%) |
| Dublin City Council | 18.1% | Bartra ODG (5.9%, social housing) |
| Health Service Executive | 13.0% | PFH Technology (3.3%) |
| DCEDIY | 11.6% | Cape Wrath Hotel (3.4%, IPAS accommodation) |

So OPW/Garda/Revenue run ~50% top-5 concentration; HSE and DCC are highly fragmented.

**Biggest single lines** (within-body, gross-line-value): Dept of Defence → Airbus EUR 187.4m ("AIR CORPS" military aircraft, 2023-q4); HSE → China Resources Pharmaceutical EUR 169.0m (Covid-era medical equipment, 2020-q2); NTA → Graham Projects EUR 140.6m (BusConnects, 2026-q1); National Paediatric Hospital Board → BAM EUR 107.6m ("Conciliator's Recommendation No.25 — Notice of Dissatisfaction", an NCH dispute settlement, 2024-q3); Dublin City Council → Bartra ODG EUR 68.5m (social housing main contract, 2025-q4); NTA → Alexander Dennis EUR 67.0m (bus fleet, 2021-q3).

_Three wrinkles:_ (1) some top "suppliers" are councils paid by TII as road-grant pass-throughs (Cork/Mayo County Council) or intra-state transfers (Higher Education Authority) — not arm's-length vendors; (2) EUR 17.27bn of gross sits in 3,122 blank-supplier roll-up rows (Irish Water EUR 11.05bn, EirGrid EUR 2.97bn, Gas Networks EUR 2.15bn, NAMA) where no supplier is identifiable; (3) names are only rough-normalised, so corporate groups split across variants are under-counted.

### Cross-body supplier network (firms embedded across the State)

This is the file's **superpower vs our 72-publisher parse: breadth.** Each body publishes its own EUR-20k file in isolation; this lets you invert to a supplier-centric view and answer "where is firm X embedded across the State, in how many bodies, and which ones" — which **no official portal exposes**. 576 suppliers touch ≥10 bodies; 179 touch ≥20; but 79% (28,963 of 36,509 normalised) appear at only one body, so the top of the list is meaningful.

The most embedded firms are **NOT utilities — they are IT resellers and Big-4 consultancies:**

| Supplier (class) | bodies | gross EUR (not spend) |
|---|---:|---:|
| datapac (IT reseller) | 123 | 100m |
| micromail (IT reseller) | 117 | 349m |
| pfh technology group (IT integrator) | 116 | 627m |
| dell (IT/hardware) | 109 | 139m |
| vodafone (telecoms utility) | 93 | 275m |
| kpmg (consultancy/audit) | 87 | 131m |
| mazars (consultancy/audit) | 85 | 26m |
| deloitte (advisory) | 82 | 446m |
| eir (telecoms utility) | 78 | 321m |
| pwc (advisory) | 77 | 155m |
| ey / ernst & young | 75 | 437m |
| grant thornton | 74 | 71m |

`n_bodies` is the trustworthy ranking key; gross is secondary (regime-mixed). Engineering consultants (RPS/Arup/AECOM 55–59 bodies) and one national legal firm (McCann FitzGerald 49) round out the concentrated-commercial tier. **Read separately:** several high-rank entries are State-to-State recharges, not vendors — Office of the C&AG (77 bodies, audit fees), OPW (49, accommodation recharge), Dublin City Council (42, shared services), Institute of Public Administration (43, training).

### Category / spend trends over time (with the regime caveat)

Themed into 14 categories × year, split by payment / PO / rollup regime (first-keyword-match wins; ~50% of gross stays in `other`, so themes are a floor). The cleanest comparable full-year pair is **2018 vs 2024** (2011 near-empty, 2026 partial); rising themes partly reflect more bodies onboarding.

**Standout growers (within a single regime):**
- **Asylum / IP / Ukraine accommodation (payment, DCEDIY): ~0 in 2018 → EUR 1.85bn in 2024, ≈24x** — the largest swing of any theme, driven by the Ukraine war + IP arrivals. The 2024→2025 "fall" to DCEDIY EUR 533m is a **machinery-of-government transfer** to the Dept of Justice (which re-publishes it as EUR 1,097m PO), **NOT a real cut** — report the two streams separately.
- **Construction / building (payment): +380%** (EUR 658m → 3.16bn), biggest theme by absolute euro (OPW + school building + DCEDIY); a further EUR 5.6bn sits in aggregated_rollup (TII/Irish Water capital), excluded.
- **Management consultancy (PO): +346%** (payment-side +171%); combined ~3x 2018→2024, recurring PAC interest.
- **IT / software / ICT (PO): +286%** (payment-side +125%) — broad cross-government digitalisation.
- **Energy / electricity / gas (PO): +225%** — the 2022–23 energy-crisis signature, concentrated in PO-bodies (LAs/state bodies); payment-side only +36%.
- **Legal / solicitor / barrister (PO): +258%**.

**Decliners / flat:** asylum (PO) −99% 2018→2024 is an artefact of the body move (returns to Justice 2025), not a decline; security (PO) −49%; medical/drugs/pharma (payment) effectively flat at +2% (HSE-dominated; the 2020 EUR 815m was a one-off Covid PPE/vaccine spike). Agency/locum staff (+44%) is a **floor** — most HSE agency cost sits in payroll, outside the EUR-20k regime.

### Cross-corpus leverage (the unique value)

The disclosed suppliers are **highly joinable to our gold corpus**, turning a payment line into a multi-corpus accountability graph (payment ↔ tender ↔ lobbying ↔ minister-met ↔ legal entity). Across the 79-firm candidate set (top-50 by gross + top-40 by reach), strict-equality match rates — **a FLOOR; CRO/fuzzy joins lift every cell:**

| Corpus | Match rate |
|---|---:|
| eTenders/TED award winner | **60.8%** (48/79) |
| Already in our parsed payments fact | 87.3% (69/79) |
| CRO via `procurement_supplier_cro_match` | 31.6% (25/79) |
| Lobbying register (lobbyist or client) | 30.4% (24/79) |
| Ministerial diaries (orgs ministers met) | 19.0% (15/79) |
| Charities register | 1.3% (irrelevant — commercial vendors) |

**Concrete linkages:**
- **Accenture** (disclosed EUR 637.1m / 20 bodies): met Min. Bruton (2017-04-27, 2017-09-05) and Min. Coveney (2024-01-09 "Visit Accenture R&D and Innovation"); 24 award rows / 7 authorities; CRO 340745 (exact_unique); lobbyist org. Quadruple match.
- **Deloitte** (EUR 446.0m / 82 bodies): met Min. McGrath (2020-11-08 call; 2023-09-22 Cork) and Min. Chambers (2025-03-02 Infrastructure Event); 368 award rows / 60 authorities; lobbyist org.
- **Roadstone** (EUR 638.4m / 36 bodies): lobbying client (3 politicians targeted); 125 award rows / 26 authorities; CRO 11035.
- **Vodafone** (EUR 274.9m / 93 bodies): met Min. Donohoe (2020-02-14, 2023-05-21 w/ Joakim Reiter); award winner; CRO resolved; lobbyist org.
- **IBM** (EUR 253.4m / 16 bodies): met Donohoe (2019), Harris (2022 MOU), Chambers (2025); award winner; lobbyist org.
- **PFH Technology** (EUR 626.8m / 116 bodies): 281 award rows / 49 authorities; CRO-resolved — whole-of-government IT reseller.

CRO is the recommended long-term join key: it resolves short trade names (SISK, LAGAN, SIAC, BAM) that miss strict name equality, and our own fact already carries a CRO number for 39.2% of the candidate firms (resolution is partly pre-computed). Verify `exact_ambiguous` matches (e.g. Abtran 260018 at 0.5) before publishing identity.

---

## Feature & story leads (ranked by value × feasibility on what we already hold)

1. **Lobbied-then-paid cross-ref** (highest value, ready now). 13 firms met a minister AND filed lobbying returns AND are large disclosed payees — Roadstone, Accenture, An Post, Deloitte, Kerry CC, Pfizer, Vodafone, IBM, KPMG, Mason Hayes Curran, McCann FitzGerald, Grant Thornton, Mazars. The strongest accountability story; all corpora already in gold.
2. **Whole-of-government footprint of firm X** (high value, high feasibility). One page per supplier listing every body that names them (Datapac 123, PFH 116, Dell 109), plus CRO identity, awards held, ministers met. This file is the *only* source giving this breadth — uniquely enabled.
3. **HSE history recovery** (high value, mechanical). Surface the +4-year HSE backfill (2017-q3..2020-q2) and the 2025-q4/2026-q1 tail we lack — with the real 2020-q3..2021-q3 internal gap honestly marked.
4. **Asylum/IP/Ukraine accommodation trend** (high editorial value, regime-careful). ~24x 2018→2024 to EUR 1.85bn, with the DCEDIY→Justice 2025 transfer explained so the "fall" is not misread as a cut. Ties straight into existing IPAS/Cape Wrath/Mosney work.
5. **IT-reseller & Big-4 concentration story** (medium-high). A small ring (Datapac/Micromail/PFH/Dell + Big-4/Mazars/Grant Thornton) touches ~half of all public bodies; fully entity-resolvable.
6. **Award-to-payment realisation bridge** (medium value, framing-sensitive). Link a disclosed payment line to a named tendered contract (60.8% of top firms are award winners); frame as "did the tender turn into payments", caveat both sides as not-directly-summable.
7. **Per-body supplier-concentration page** (medium, mechanical). Top-5 share per body (DECC 83% NBI; OPW/Garda/Revenue ~50%) — a clean "who dominates your body's spend" lens.
8. **Entity-resolve the file end-to-end** (enabling infrastructure). Push suppliers through `procurement_supplier_cro_match`, attach CRO, chain to corporate-distress / BAM-style group rollup; then extend every cross-ref to the 141 genuinely-new bodies (Irish Water, DCC, Garda) whose supplier strings join identically.

---

## Caveats & limits

- **Regime mix / not-safe-to-sum (the load-bearing one).** Payment lists, PO commitments and category roll-ups are different units; the ~EUR 117bn gross of the file is meaningless. Every euro figure must stay within one body or be flagged gross-line-value-not-spend. Cross-body supplier sums (the top-suppliers list) mix paid spend with forward commitments and are magnitude indicators, not totals.
- **Supplier-name fragmentation.** ~4,592 redundant variants from newlines/spacing/legal-suffix toggling; rough normalisation both under-merges (eir vs eir evo separate) and over-collapses (generic "electric" node, flagged/excluded). Names are leads, not legal identity — no CRO column in this extract.
- **Description is MIXED, not a taxonomy.** Coded controlled vocab for councils/HSE/Tusla/OPW; per-invoice free text for departments. 45,594/48,020 descriptions used by one body; cross-body category work only holds on ~2,425 shared coded terms, and themeing leaves ~50% in `other`.
- **Inter-state transfers masquerade as suppliers.** Cork/Mayo County Council (TII road-grant pass-through), Higher Education Authority, OPW, C&AG, ESB are public bodies, not arm's-length vendors — exclude from any private-market lens.
- **Blank-supplier roll-ups.** EUR 17.27bn (Irish Water EUR 11.05bn, EirGrid, Gas Networks, NAMA) names no supplier and has no meaningful concentration.
- **No enrichment columns.** The extract has no `amount_semantics` / `value_safe_to_sum` / VAT / per-record source URL / CRO — semantics must be re-derived from the PO-blank heuristic.
- **Coverage broadens over time.** Row volume rises ~8x 2012→2025 as bodies onboard, so theme growth is partly more bodies, not only more money. 2011 (9 rows) and 2026 (15,029 rows) are partial tails — do not chart as full years.
- **Match rates are a strict-equality floor.** CRO-bridged / fuzzy joins would lift every cross-corpus cell; verify `exact_ambiguous` CRO matches before asserting identity.
