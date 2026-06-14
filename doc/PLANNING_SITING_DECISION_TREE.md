# Citizen Siting-Check — Decision-Tree Content Spec

**Companion to** `PLANNING_PERMISSION_SCOPING.md` §23 (feature architecture) and
`planning_rules/SOURCE_REGISTRY.md` (the data layers). This doc designs the **content** of the tree:
the issue nodes, what each tells the user, the mitigation branch, and the An Bord Pleanála (ACP)
precedent link that acts as the "ultimate arbiter."

**Not** PlanX-style drag-drop flow-authoring or auto-generated code (overkill). This is a fixed,
hand-authored **issue catalogue**: for a given point, evaluate each node's trigger, show the ones that
fire, quote the rule, name the specialist, show the mitigation reality, and link a real ACP decision.

_Status: SCOPE / DESIGN — not built. 2026-06-14._

## 1. Design principles (the load-bearing rules)
1. **Issues, not verdicts.** Output is "here's what your site triggers + what the plan says + how the
   Board has ruled," never "you will be refused" or "build a grey bungalow" (the §23.4 liability line).
2. **Signpost the specialist, don't be the specialist.** "Likely a bat issue → engage an ecologist; if a
   roost is found you'll also need a lighting specialist" is signposting (fine). Designing the mitigation
   is the consultant's job (and liability).
3. **ACP precedent is the arbiter.** Every issue node links a **real, recent ACP decision** — a refusal
   *and*, where one exists, a grant-with-mitigation. The Board's own words settle "how bad is this."
4. **Mitigation is a first-class branch.** Each issue is tagged: procedural / mitigable-by-design /
   often-fatal — so the user sees whether the problem is a form to fill or a dealbreaker.
5. **Likelihood, not certainty.** Tie risk language to the §13 evidence (e.g. SAC sites refused ~2.4×
   baseline) — a signal, with the no-inference caveat.
6. **Version-stamped.** The quoted standard names the Development Plan in force (§18 temporal layer).

## 2. Tree structure — three layers
```
LAYER A — Universal gates (every site)          → always shown: AA screening, native planting, energy cert
LAYER B — Location triggers (spatial joins)     → the meat: fire only if the point hits a designation/condition
LAYER C — Type & siting (user inputs + DEM)     → house type/size + terrain → siting & design constraints
```
The user answers two cheap questions up front — **(i)** point (map-click/XY/Eircode), **(ii)** what they
want to build (one-off house / extension / multi-unit / commercial) — and the tree filters to the nodes
that actually apply.

## 3. The issue catalogue (Layer B + C nodes)
Each node = `trigger → source → plain-English flag → engage → mitigation class → ACP precedent`.
Mitigation classes: **P** = procedural (just submit the report) · **D** = mitigable by design/condition ·
**F** = often fatal / hard constraint.

| Issue | Trigger (source) | Plain-English flag | Engage | Mit. | What mitigates it |
|---|---|---|---|---|---|
| **Bats** | near SAC/SPA, mature trees, watercourse, or old/derelict structure | "Likely bat issue (Lesser Horseshoe = Annex II). A bat survey will be needed; if a roost is present, an NPWS derogation licence + sensitive lighting design." | ecologist; **lighting specialist** if roost | **D** | bat survey + NIS mitigation chapter + ecologist-supervised works + dark-corridor/lux-capped lighting (seen as a grant condition) |
| **European site (SAC/SPA)** — incl. **Lough Corrib, the Burren** | `point ∈/near SAC/SPA` (NPWS) | "Appropriate Assessment applies. If there's a water/effluent pathway to the European site, this is hard to grant." | ecologist (NIS author) | **D→F** | NIS proving no adverse effect on site integrity; **on karst near a SAC, effluent disposal is often not mitigable** (see Burren/Corrib precedents) |
| **Bog / peat** | peat subsoil (EPA Subsoils) or raised/blanket-bog NHA | "Peat soils: foundation stability, carbon/peat-disturbance and septic-percolation problems; possible peatland designation." | geotechnical engineer; ecologist if NHA | **D→F** | engineered foundations (cost); **percolation on peat frequently fails** → wastewater can be the dealbreaker |
| **Historic monument** | `point ∈ SMR Zone of Notification` (NMS) | "A recorded monument is nearby. An archaeological assessment and likely monitoring will be required." | licensed archaeologist | **P→D** | archaeological assessment + monitoring condition (usual outcome); only **F** if the monument itself is directly impacted |
| **Floodplain** | `point ∈ Flood Zone A/B` (OPW NIFM/CFRAM) | "Your site is in a flood zone. A house is 'highly vulnerable' use → a Justification Test is required, and in Zone A it is usually inappropriate." | flood-risk/hydrology consultant | **F** (Zone A) / **D** (B) | Justification Test (rarely passes for a one-off in Zone A) + finished-floor raising; **often not mitigable in Zone A** |
| **Septic / groundwater** | unsewered (EPA agglomeration) + high vulnerability/karst (GSI) | "No public sewer here; on-site wastewater needed. High groundwater vulnerability/karst means percolation may fail." | site-suitability assessor (EPA CoP) | **D→F** | trial hole + percolation test; proprietary treatment system; **if the site fails the EPA test it is not mitigable** |
| **Road / sightlines** | access onto road; nearest `highway=`+`maxspeed` (OSM); national road → TII | "Entrance sightlines must meet the DM standard for the road's speed; the visibility-splay land must be in your control. On a national road outside the 50–60 km/h zone, new houses are restricted to farm families." | transport/civil engineer | **D→F** | achieve x/y sightlines (control frontage + remove hedgerow); **F if you can't control the splay land or it's a restricted national road** |
| **Landscape / exposed siting** (the elevation case) | landscape Class 2/3, scenic route, protected view; **DEM** elevation/slope/skyline | "Sensitive/exposed landscape. A Visual Impact Assessment is likely. Siting/design will be constrained — low profile, integrate with the slope, vernacular form, muted materials, no skyline break." | architect (+ landscape architect) | **D** | low/stepped siting, single-storey or dormer, planting, recessive materials; *quote DM Std 8 — do not prescribe the design* |
| **Rural housing need / zoning** | agricultural zoning / strong-urban-pressure / Tier-6 settlement | "This area restricts houses to people with a demonstrated local need / functional need. Urban-generated rural housing is steered to settlements." | planning consultant | **F** (if you don't qualify) | demonstrate local connection / functional need; **if you don't qualify, this is the single most common refusal** and is not design-mitigable |
| **Protected structure / ACA** | near RPS/ACA/NIAH | "A protected structure or conservation area is involved. An Architectural Heritage Impact Assessment is required and design is constrained." | conservation architect | **D** | conservation-led design + AHIA; demolition/material change is harder |

> The wording above is the **template**; the live app must substitute the **verbatim DM Standard for the
> council in force** (from `planning_rules/<la>/`), not this generic phrasing.

## 4. Mitigation framework — the "other side"
Render each fired issue under one of three headers so the user immediately sees the shape of the problem:
- **🟢 Procedural (P)** — "you just need to commission report X." (archaeology monitoring, energy cert,
  most landscaping). Low risk.
- **🟡 Mitigable by design/condition (D)** — "this is commonly resolved with the right specialist + a
  condition." (bats+lighting, sightlines if you control the land, landscape siting, AHIA). Show *what the
  Board has accepted as mitigation* via the precedent link — not a prescription.
- **🔴 Often fatal (F)** — "this frequently isn't mitigable; get advice before you buy/design."
  (residential in Flood Zone A; effluent on karst beside a SAC; no qualifying local need in a restricted
  area; uncontrollable national-road sightlines). This is the honest, high-value warning.

A site beside **the Burren** illustrates the D→F nuance: the SAC + karst means effluent is the crux — the
node shows "AA applies; on karst, wastewater is the likely dealbreaker unless a compliant solution is
demonstrated," links the relevant ACP refusal, and names the ecologist + site-suitability assessor.

## 5. ACP precedent index — the "ultimate arbiter"
The feature's credibility lever: each issue links a **real ACP decision** in the user's region/issue.
- **Data:** ACP `Cases_2016_Onwards` feed (already in registry) + the order/inspector PDFs
  (`…/orders/d{case}.pdf`, `…/reports/r{case}.pdf`).
- **Build:** an **issue → exemplar-case index**. Phase 1 = **hand-curate** ~2–3 cases per issue per
  region (one refusal, one grant-with-mitigation). Phase 2 = **auto-tag** by text-mining inspector reports
  for issue keywords ("bat", "Lesser Horseshoe", "flood zone", "sightline", "local need", "karst",
  "Appropriate Assessment") → tag each case with the issues it turned on, then surface the nearest/most
  recent per issue.
- **Display:** "An Bord Pleanála, ABP-XXXXXX-YY (year): *refused, in part because '<verbatim reason>'*" with
  a link to the case page. The Board's own words, not ours — sidesteps the inference line entirely.

## 6. Commercial aspect (scope, not yet fleshed)
The free citizen triage is the funnel; the value sits one layer up.
- **Buyers / willingness to pay:** prospective self-builders (pre-purchase site due diligence), **farmers**
  (site disposal), **land/estate agents** (sell with a "planning readiness" report), **solicitors**
  (conveyancing site checks), **planning consultants/architects** (a fast first-pass triage tool).
- **Tiers:**
  1. *Free* — issues + rules + ACP precedent (the funnel; builds trust, the PlanX-equivalent).
  2. *Paid site report (€)* — a generated PDF: the full obligation set, the governing standards, matched
     precedents, terrain/siting analysis, and a "specialists you'll need" checklist. Pre-purchase due
     diligence is the killer use-case (don't buy a field you can't build on).
  3. *B2B API/subscription (€€)* — consultants/agents/solicitors run sites at volume.
- **Lead-gen (careful):** "engage an ecologist/lighting specialist/planning consultant" → an opt-in
  **referral marketplace** (revenue share). **Caveat:** referrals must not compromise the impartiality of
  the issue assessment (separate the neutral triage from any paid placement; disclose).
- **Comparators / gap:** PlanX (UK, gov, free, no design advice), Symbium/Gridics (US, parcel zoning,
  commercial). **Ireland has none** (§17) — the obligation-set reconstructor + ACP-precedent tagging is the
  defensible data moat.
- **Risk/positioning:** market as **"planning risk triage / due-diligence,"** explicitly *not* planning
  advice; the professional-advice disclaimer is both legal protection and the honest framing.

## 7. What it needs to build (delta on §23)
1. **Issue catalogue as data** — ✅ drafted at **`planning_rules/issue_catalogue.yaml`** (11 nodes: one
   block each with trigger predicate, source-layer refs, flag template, specialists, mitigation class,
   verified ACP precedents + curation stubs, and `council_overrides`). Engine loads this; `rule_ref`
   resolves verbatim text per-council from `planning_rules/<la>/`.
2. **The join service** (§23.7) to evaluate triggers — already-located national layers.
3. **DEM** for the landscape/siting node (the one missing source).
4. **ACP precedent index** — start curated, then keyword auto-tag (§5).
5. A simple **rendered tree UI** (filtered list of fired nodes, grouped P/D/F) — no flow-authoring engine.
