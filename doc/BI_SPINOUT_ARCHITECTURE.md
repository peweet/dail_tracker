---
tier: SPEC
status: LIVE
domain: commercial
updated: 2026-06-28
supersedes: []
read_when: scoping the BI spinout's business model, data reuse inventory, licensing, or ethics firewall before building any of it
key: SPEC|LIVE|commercial
---

# Commercial BI Spinout — Architecture & Business Model

**Date:** 2026-06-28
**Status:** PROPOSAL — strategy only, nothing built. Decisions flagged ⚠️ are owner-only.
**Premise:** Keep the civic side free and trusted as **Dáil Tracker** (politicians, votes, SIs,
interests, lobbying, accountability). Spin out a **separately-branded business-intelligence
product** that monetises public-procurement market intelligence (suppliers, buyers, CPV markets,
tenders, awards, payments, competitor tracking, alerts, exports, API).

> Reads alongside [`COMMERCIALISATION_PLAN.md`](COMMERCIALISATION_PLAN.md) (the AGPL/commercial
> dual-licensing scaffolding, already shipped) and
> [`PROCUREMENT_INTELLIGENCE_ROADMAP.md`](PROCUREMENT_INTELLIGENCE_ROADMAP.md) (the feature
> roadmap, Phases 0–5). This document is the *commercial/architecture* layer over both.

---

## 0. The one-paragraph thesis

The hard part is already done. The repo holds a production-grade procurement data spine — **43
procurement SQL views, three rigorously-separated money grains, CRO entity resolution, and a
deployed 16-router JSON API with 7 privacy-filtered bulk exports**. What is *missing* is exactly
the part you monetise: authentication/metering, per-user persistence (saved searches, watchlists,
alerts), email digests, and a distinct commercial brand/frontend. So this is **not a build, it's a
packaging-and-shell exercise** — and the cheapest validation path (reports + exports) needs almost
zero new code. The constraint is not engineering; it's (a) a genuinely modest Irish TAM, (b) the
data being free/PSI so you sell *curation and time-savings, not data*, and (c) a bright ethical
line that must keep political-influence inference on the free side and out of the paid product.

---

## 1. What already exists (reuse inventory)

### 1.1 Data & views — the BI spine is built

| Asset (real path) | Grain | What it gives the BI product |
|---|---|---|
| `data/gold/parquet/procurement_awards.parquet` (62,763 rows) | AWARDED | eTenders/OGP national awards; supplier-normalised, CRO-matched, `value_safe_to_sum` |
| `data/gold/parquet/ted_ie_awards.parquet` | AWARDED (EU) | TED/EU-OJ award notices; competition signals (single-bid %, uncompetitive procedures) |
| `data/gold/parquet/procurement_payments_fact.parquet` (~2M lines, 57 publishers) | PAID / COMMITTED | Public-body payments & POs >€20k; SPENT vs COMMITTED tiers, VAT flags |
| `data/silver/.../etenders_live_tenders.parquet` | PLANNED | Live open tender pipeline (forward opportunities) |
| `procurement_supplier_cro_match.parquet` (6,047 matched) | entity link | Supplier→CRO with `match_method` + `match_confidence` (0.0/0.5/0.9) |
| `epa_supplier_compliance.parquet`, `procurement_lobbying_overlap.parquet`, charity overlap | enrichment | EPA licences/enforcement, register co-occurrence (see ⚠️ ethics §4) |

**43 procurement views** in `sql_views/procurement/` already cover every MVP need:

- **Supplier:** `procurement_supplier_summary`, `_supplier_year_summary`, `_payments_supplier_summary`, `_supplier_dependency`, `_supplier_sector_breadth`, `_supplier_single_bid`
- **Buyer:** `procurement_authority_summary`, `_authority_year_summary`, `v_procurement_competition` (per-buyer single-bid — *built but unwired*)
- **CPV/market:** `procurement_cpv_summary` (median/IQR already computed), `_cpv_year_summary`, `_competition_by_cpv`, `v_procurement_bid_signal` (experimental "should I bid?")
- **Relationships:** `procurement_incumbency`, `_call_off_links`, `v_procurement_entity_chain` (cross-register footprint: in_etenders/in_ted/in_payments + paid/committed safe €), `_new_entrants`, `_expiring_contracts`, `_renewal_cycle`
- **Access surface:** `dail_tracker_core/queries/procurement.py` (130+ functions), `utility/data_access/procurement_data.py`, `public_payments_data.py`

### 1.2 API & exports — the distribution channel is built

- `api/main.py`: **16 routers**, ~40 endpoints, FastAPI, single in-memory DuckDB, stateless, dockerised.
- `api/routers/procurement.py`: suppliers, supplier dossier, competition, CPV, authorities, open-tenders, lobbying-overlap.
- `api/routers/exports.py` (`GET /v1/data`, `/v1/data/{resource}`): **7 allowlisted, privacy-filtered exports** (procurement_awards, supplier_cro_match, payments_fact, lobbying_overlap, ted_awards, ted_winner/buyer_history, ted_tenders) — each ships `licence`, `attribution`, `caveat`, `n_rows`, `data_currency`. Default-deny; person rows excluded at snapshot time.

### 1.3 Trust infrastructure — the moat is built

- **Three-money-grain never-sum discipline** (`value_safe_to_sum`, `value_kind`, lifecycle tiers) — the single hardest thing to get right, and the thing naive scrapers get *wrong*.
- **No-inference vocabulary** enforced architecturally: logic firewall (`tools/check_streamlit_logic_firewall.py` + the empty-allowlist ratchet test the IDE has open) keeps all joins/aggregations in views, never the UI.
- **CRO entity resolution** with explicit confidence tiers (the headline asset competitors can't cheaply replicate).
- Caveat strings embedded in `dail_tracker_core/dossiers.py` and every export manifest.

### 1.4 What does NOT exist (this is the whole spinout build)

| Missing capability | Needed for | Effort |
|---|---|---|
| Auth / API keys / rate-limit / usage metering | Paid API tier, abuse control | M |
| Per-user identity + persistence store | Saved searches, watchlists | L (the architectural lift) |
| Scheduled diff/digest job + email (PII) | Bid-intelligence alerts | L + GDPR |
| Separate commercial brand, domain, frontend | Reputation firewall | M |
| Billing (Stripe) + entitlements | Any paid tier | M |

This is **Phase 5** of the existing roadmap, which is explicitly *gated on owner sign-off* — correctly, because it introduces PII (emails), consent/GDPR, and scheduler infra the project deliberately avoids today.

---

## 2. Which endpoints stay free (the civic commitment)

Everything that makes Dáil Tracker a *public-accountability* tool stays free, open, anonymous, no-key:

- **All civic pages** (15): members, attendance, votes, committees, legislation, statutory instruments, judiciary, ministerial diaries, lobbying register, member interests (`what_they_own`), public appointments, elections, constituency, your-councillors, local-government governance.
- **All civic API routers:** members, legislation, votes, payments (TD allowances), lobbying, committees, ministerial, political_finance, judiciary, charities, corporate-notices, appointments.
- **The procurement *explorer* itself stays free at the civic-trust level** — browsing who won what, public-body payments, and the open-tender list is public-interest transparency and should never go behind a paywall. The paid product is *not* "see procurement data"; it is *workflow on top of it* (saved profiles, alerts, dossiers-as-a-service, exports at scale, API SLA). Keeping raw exploration free is also the top-of-funnel for the paid product.

**Rule of thumb:** *seeing a public fact = free; having the product do recurring work for you (watch, alert, dossier, export, integrate) = paid.*

---

## 3. Which workflows can be paid (value-added, not data-gating)

Paid value is **time saved and signal surfaced**, layered on free data:

| Paid workflow | Built on existing asset | New work |
|---|---|---|
| **Saved tender profiles** (by CPV / buyer / region / value band) | `etenders_live_tenders`, `v_procurement_expiring_contracts`, `_renewal_cycle` | persistence + matcher |
| **Bid-intelligence email digest** (new + expiring + renewal-due matching your profile) | same + scheduled diff | scheduler + email + consent |
| **Supplier dossiers** (awards, payments, incumbency, dependency, competition, EPA, CRO) | `v_procurement_entity_chain`, supplier views, `dossiers.py` | PDF/share render, confidence pill |
| **Buyer dossiers** (top suppliers, category mix, single-bid rate, framework use, expiring contracts) | `authority_summary` + `v_procurement_competition` + Phase 2 views | wire the unused competition view |
| **Category market maps** (CPV concentration, top parties, median/IQR, SME participation, trend) | `procurement_cpv_summary` (median/IQR exist) + Phase 2 | top-parties view |
| **Competitor tracking** (alert when a named *company* wins/gets paid/appears in a new market) | `entity_chain`, `incumbency`, payments | watchlist + diff |
| **Bulk exports at scale + API SLA** | `api/routers/exports.py` (built) | keys, quota, ToS pass-through |
| **Bespoke market reports** (one-off, human-curated) | everything above | analyst time only |

**The honest pitch:** *"The data is public; finding the signal across eTenders + TED + €2M payment
lines + CRO + frameworks, every week, without double-counting framework ceilings, is what you're
paying for."*

---

## 4. ⚠️ Avoiding the ethics trap (the load-bearing section)

The project's reputation is its only durable asset. Monetising the wrong thing destroys both the
free product's trust and the paid product's defensibility. The investigation confirms **no political
risk/influence/corruption scoring exists in the codebase today** — keep it that way.

### The bright line

> **Money flows about public bodies and companies = sellable. Inferences about named politicians or
> private individuals = never sellable, never scored, never alerted on.**

### What must NEVER be monetised (and mostly must not exist at all)

- Politician risk scores, influence scores, conflict-of-interest rankings.
- "Score TDs by voting alignment with lobbied interests" / "flag ministers who awarded contracts to declared interests."
- Personal dossiers on individuals; named-individual scoring of any kind.
- Speculative corruption claims; any "verdict" framing on a structure fact (single-bid, incumbency, renewal are facts, never accusations).

### The dangerous straddlers — diary OUT of paid, lobbying tightly gated

These exist as **co-occurrence** assets and are *correctly* framed today, but they are exactly what a
naïve "competitor intelligence" product would be tempted to sell — and selling them crosses the line
from market intelligence into **monetising political-access inference**:

- `procurement_lobbying_overlap` / `v_procurement_lobbying_overlap` ("this supplier also lobbies").
- `diary_company_influence` / ministerial-diary × contracts ("which companies have minister access").
- votes × interests cross-reference.

**Decision history.** My original recommendation was free-civic-only. The owner initially (2026-06-28)
allowed both as paid, co-occurrence only. After the Fable second-model assessment (2026-07-08,
[doc/BI_SPINOUT_FABLE_ASSESSMENT.md](BI_SPINOUT_FABLE_ASSESSMENT.md) §7), the owner **reversed the
diary half**. The split now is:

- **Ministerial-diary access (`diary_company_influence` / diary × contracts) — OUT of the paid product
  entirely.** Rationale (Fable, accepted): "co-occurrence only" governs the *arithmetic* but not the
  *meaning of the sale* — a bidder buys the diary panel precisely because minister-proximity matters to
  winning, so the commercial context performs the inference the caveat disclaims. The premise the risk
  was first accepted on ("not legal, just reputational") is **wrong for diaries**: office-holder diaries
  are personal data, and commercial resale is a different GDPR Art. 6(1)(f) balancing than civic
  republication. The revenue is ≈0 (optional panel, no tier priced on it); the downside is the free
  product's trust — the only durable asset and the entire top-of-funnel. Reversal costs nothing built
  (the diary-access path was Phase-2-TODO). Diary data stays a **free civic feature** inside Dáil
  Tracker only.
- **Lobbying-register overlap (`procurement_lobbying_overlap`) — MAY stay in the paid product, tightly
  gated.** SIPO's PSI policy invites republication, and supplier-keyed due-diligence facts are standard
  commercial practice. Guardrails (non-negotiable): **per-report, owner-gated; NEVER in bulk exports or
  the API; raw co-occurrence counts only — never a score/ranking/index/verdict**; the **award-€ totals
  must not sit in the same table as lobbying counts** (co-locating them composes the causation narrative
  the caveats deny); carry `caveats.PROC_LOBBY` verbatim; the subject is the **company, not any
  office-holder**; company-class/PII double-gate applies; "rigged/captured/influence-bought" framing is
  a hard CI-forbidden term. The §10 no-list holds in full.
- **votes × interests cross-reference — free civic only** (unchanged).

### Privacy / PII rails (already enforced — must carry into the paid product)

- **Company-class only.** `supplier_class='sole_trader_or_individual'`, `public_display=FALSE`, and
  `privacy_status='review_personal_data'` rows are excluded at snapshot/export time — the paid
  product, API, and every export must inherit the same double-gate.
- CRO-director / charity-trustee cross-referencing stays sandboxed pending a documented lawful basis
  — **not** in the paid product.
- Personal insolvency / individual bankruptcy stays excluded by policy.

---

## 5. Separating brand, UI, and data access

| Layer | Free (Dáil Tracker) | Paid (BI product) |
|---|---|---|
| **Brand** | "Dáil Tracker" — civic, neutral, trademark in progress | **New mark, own domain.** Do not extend the political brand onto a paid product — it trades civic trust for commercial gain and muddies the trademark. Candidates: **Bid Signal** (a `v_procurement_bid_signal` view already exists — brand-from-asset), *TenderLens*, *Margadh* (Irish: "market"). File its own IPOI/EUIPO mark. |
| **Frontend** | Streamlit, anonymous, `dailtracker.ie` | Separate frontend + separate domain. Can start as a *second Streamlit app* (same component library) to ship fast; migrate to a real web app only when SaaS demand is proven. |
| **Data access** | Open, no key, read-only views | **Authenticated, metered API tier** + entitlement gating over the *same* gold parquet / views. Optionally a read replica or a dedicated export bucket so paid load never degrades the free site. |
| **Code** | AGPL-3.0 (public repo) | *Your own* code — you own the copyright, so you run it commercially without buying anything (see §6). The auth/billing/persistence shell can live in a private module or private repo. |

**Critical architecture point:** both products read the **same data core** (pipeline → gold parquet
→ views → `dail_tracker_core.queries`). Do **not** fork the data. The split is at the *serving and
identity* layer, not the data layer — one curation pipeline, two front doors.

---

## 6. Licensing — how AGPL and upstream data licences actually bite

### 6.1 The AGPL/commercial dual-license affects *others*, not you

- The code is AGPL-3.0 with a commercial-licence option (scaffolding shipped). **You own 100% of the
  copyright** (sole author; CLA preserves this for future contributions). Therefore **you can run a
  closed-source commercial SaaS on your own code without buying anything** — the AGPL is the lever
  against *third parties* who want to host modified versions without source disclosure. Building your
  own BI SaaS is fully consistent with the dual-license model.
- Keep the auth/billing/persistence shell as code *you* own. Do **not** vendor any AGPL-incompatible
  third-party code into a part you intend to keep closed; and do not accept external contributions
  without the CLA (it would poison relicensing).

### 6.2 The data is the real constraint — you sell software/service, never the data

Per [`NOTICE.md`](../NOTICE.md), the datasets are third-party PSI under their own licences. You
**cannot** licence or resell the data; a customer must comply with each source licence independently.
Practically, the BI product's data spine is **commercial-friendly**, with one clear exception:

| Source | Licence | Commercial BI use? |
|---|---|---|
| eTenders / OGP corpus (data.gov.ie) | CC BY 4.0 | ✅ with attribution |
| CRO open data | CC BY 4.0 | ✅ with attribution |
| Charities Regulator | CC BY 4.0 | ✅ with attribution |
| lobbying.ie (SIPO) | PSI re-use (free, incl. publishing) | ✅ with acknowledgement — *but see ethics §4: free side only* |
| Public-body payments | PSI / per-publisher terms | ✅ likely — **verify per-publisher**, attribute |
| TED / EU-OJ | EU reuse (CC-BY-style) | ✅ likely — **verify exact EU reuse terms** |
| **Iris Oifigiúil** (corporate distress notices) | **Government copyright, NOT open; "personal use only"; facts-defensible** | ⚠️ **the weak link & highest legal risk.** **Owner decision (2026-06-28): include, fact-only + attributed**, accepting the risk pending review. Constraints: re-express *facts* only (notice type, date, entity, status) — **never verbatim gazette text or PDF layout**; carry the Iris acknowledgement string + source URL; **stays on the solicitor checklist (§14.2)** as a confirm-before-scaling item. |

### 6.3 Pass-through obligations (the bit people miss)

- An **API/export customer who re-publishes** your data must themselves honour CC-BY attribution,
  the never-sum caveats, and the no-PII rules. Your **API Terms of Service must contractually
  pass these obligations through** (attribution string, never-sum warning, no person-row
  re-identification, no resale of raw exports). The export manifests already carry the strings —
  bind them in the ToS.
- You may hold a **sui generis database right** in your *compilation/curation* (distinct from the
  source facts). You can assert rights in the compilation and the software; you cannot assert rights
  in the underlying public facts. Price and pitch on **curation + software + service**, never "our data."

### 6.4 GDPR

- CC-BY covers copyright/database rights only — **not** data-protection. Company-class procurement
  data is low-risk; the moment a sold feature names a natural person (sole trader, director, the email
  recipient of an alert) you are processing personal data. The **alert/email feature (Phase 5)
  triggers controller obligations**: lawful basis, privacy notice, consent for marketing, data
  subject rights, retention. Budget for a privacy notice + DPA review before launching alerts.

---

## 7. Product/architecture form — recommendation

The question: separate app / separate API / separate frontend over same data / clean-room /
reports-first / SaaS-later. Verdict per option:

| Option | Verdict | Why |
|---|---|---|
| **Clean-room re-implementation** | ❌ No | Pointless — you own the copyright. Rebuilding wastes the 43-view spine and the trust infrastructure that *is* the moat. |
| **Separate full SaaS app, now** | ❌ Not first | Front-loads the expensive identity/GDPR/billing lift before demand is proven. Highest cost, slowest validation. |
| **Separate frontend over the same data** | ✅ Yes (the structural answer) | Shared curation pipeline + views; new branded front door + authenticated API tier. One source of truth, two products. |
| **Separate authenticated/metered API** | ✅ Yes (early revenue) | The 7 exports + procurement router already exist; add keys + quota + ToS pass-through → sellable in weeks. |
| **Reports-first business** | ✅ **Start here** | Lowest cost to *validate willingness to pay*. Everything needed (dossiers, market maps, exports) already exists; the only input is analyst time. Proves the market before you build accounts. |
| **SaaS later** | ✅ Sequenced last | Saved profiles + alerts (Phase 5) are the real SaaS; build only once reports/API revenue proves the segment. |

**Recommended path: Reports-first → metered API/data subscriptions → branded self-serve frontend →
full SaaS (accounts/alerts).** Crawl-walk-run, each stage funding and de-risking the next, and each
stage reuses the same data core behind a progressively richer serving/identity layer.

---

## 8. Pricing hypothesis (⚠️ all figures are hypotheses to test, €/year unless noted)

| Tier | Audience | Price (hypothesis) | What they get |
|---|---|---|---|
| **Free civic** | Public, journalists, researchers, citizens | €0 | Full Dáil Tracker + free procurement *explorer* + open API (rate-limited, attribution) |
| **Researcher** | Academics, journalists, NGOs, single analysts | €120–€300/yr (or low monthly) | Higher API quota, bulk exports, saved searches, no email seats limit; **discount/free for accredited press & academia** (good for the civic mission + funnel) |
| **SME supplier** | One company watching its own markets | €600–€1,200/yr | Saved tender profiles (their CPVs/buyers), weekly bid-intel email, their own supplier dossier + 3–5 competitor watches, category map for their sector |
| **Bid consultant** | Bid-writing / tender-consultancy firms | €2,400–€6,000/yr | Multi-sector profiles, unlimited competitor/buyer watches, all dossiers, exports, multi-seat, white-labelled client reports |
| **Enterprise / API** | Large suppliers, agencies, data teams, integrators | €10k–€30k+/yr | API SLA, high/unmetered quota, all exports, account management, custom CPV/buyer feeds, optional data-warehouse delivery |
| **Bespoke reports** | Anyone, one-off | €1.5k–€10k per report | Human-curated market report (entry/incumbency/competition/forecast for a sector or buyer); the *first* revenue, validates segments |

Pricing logic: anchor on **hours saved** (a bid consultant charging €100+/hr saves days per tender
cycle) and on **comparables** (UK public-market-intelligence firms — Tussell, Stotles, Spend Network
— sell exactly this shape at far higher prices into a bigger market; Ireland is smaller, so price
below UK but well above "free data"). Keep press/academia free to protect the civic brand.

---

## 9. MVP feature set (mapped to existing assets)

Two MVP layers by build cost. **MVP-0 needs almost no new infra; MVP-1 is the accounts lift.**

### MVP-0 — "Reports + exports + dossiers" (ship in weeks, no accounts)
1. **Supplier dossiers** — render `v_procurement_entity_chain` + supplier views + confidence pill as a shareable/PDF dossier. *Exists; needs render + the Phase-0 confidence pill.*
2. **Buyer dossiers** — wire the **built-but-unused** `v_procurement_competition` + authority views into a buyer profile. *Mostly exists; wire it.*
3. **Category market maps** — CPV concentration/median/IQR/top-parties/trend. *median/IQR exist; add top-parties view (Phase 2).*
4. **Exports** — the 7 export resources already ship with licence/caveat/attribution; surface in-app via `export_controls.export_button` and as paid bulk downloads. *Built.*
5. **Source/caveat badges** — money-grain badges + match-confidence pill + explain-this-figure popover (**Phase 0 of the roadmap** — build these first; they are the trust UI the whole product leans on).
6. **Bespoke reports** — assemble the above by hand for paying customers. *No code; analyst time.*

### MVP-1 — "Profiles + alerts" (the SaaS lift — ⚠️ gated, Phase 5)
7. **Saved tender profiles** — persist {CPVs, buyers, regions, value bands}; match against `etenders_live_tenders` + `_expiring_contracts` + `_renewal_cycle`. *Needs identity + persistence.*
8. **Bid-intelligence emails** — scheduled diff of new/expiring/renewal-due vs each saved profile → digest. *Needs scheduler + email + consent (GDPR).*
9. **Competitor tracking** — watchlist of company-class entities; alert on new awards/payments/market entry via `entity_chain`/`incumbency`. *Needs watchlist store + diff.*

**Pipeline prerequisite for MVP-1:** national eTenders live snapshot has 0/2,363 CPV filled — CPV
enrichment must be promoted before CPV-keyed national alerts work (flagged in the roadmap).

---

## 10. What NOT to monetise (the explicit no-list)

Restating §4 as a hard product constraint, because it is the thing most likely to be eroded under
sales pressure:

- ❌ Politician / individual **risk scores, influence scores, conflict rankings**.
- ❌ **Personal dossiers** on any named natural person; named-individual scoring.
- ❌ **Speculative corruption claims**; any "verdict" on a structure fact.
- ❌ Selling **ministerial-diary access** analysis in any form (free civic only — reversed 2026-07-08, §4).
- ❌ Putting **lobbying-overlap** in bulk exports or the API, co-locating it with award-€ totals, or framing it as anything but raw per-report co-occurrence facts (§4).
- ❌ Re-identifying sole-trader/individual payment rows, CRO directors, charity trustees in any paid output.
- ❌ Implying the **data is yours** to licence, or that figures across grains can be summed.

---

## 11. Business-model critique (the honest risks)

**Strengths**
- The expensive, defensible work (three-grain discipline, CRO resolution, never-sum, no-inference) is *done* and is precisely what naïve competitors get wrong → genuine trust moat.
- Near-zero data-acquisition cost (PSI/CC-BY) → high gross margin on a service business.
- A deployed API + exports already exists → fast path to first revenue.
- Reports-first means you can validate willingness-to-pay before building accounts.

**Weaknesses / risks (do not paper over)**
1. **The data is free.** A determined buyer can get eTenders/CRO themselves. You sell *aggregation,
   dedup, entity-resolution, alerting, and hours saved* — the pitch must be relentlessly about that,
   not "access to data."
2. **Small TAM.** Irish public procurement is a modest market: a few large suppliers, a long tail of
   SMEs, a handful of bid consultancies. Revenue is more "lifestyle/profitable niche" than
   venture-scale unless extended (e.g. UK/EU via TED, which the TED corpus already touches).
3. **Free official competition.** eTenders/TED already offer free tender alerts. You must win on
   *cross-source intelligence* (awards↔payments, incumbency, competition, renewal forecasting) — not
   on tender listings, where the official tools are free and authoritative.
4. **Coverage honesty caps the hype.** Only ~7% of state spend is visible in payments; frameworks
   repeat ceilings; CPV fill is ~33%. The rigor that builds trust also limits the "total market" story
   — embrace it as a differentiator (honest > inflated) rather than fighting it.
5. **Iris/GDPR constraints** limit some enrichments (corporate distress; alerts/emails).
6. **Key-person concentration.** One author, one bespoke pipeline. The commercial obligation (SLA,
   freshness, support) is heavier than a hobby project — scope accordingly before signing enterprise
   SLAs.

**Verdict:** viable as a **focused, profitable niche / reports-led data business**, especially if it
later spans TED for UK/EU buyers. Not a land-grab SaaS; a high-trust intelligence service. The
free civic product is the credibility engine and top-of-funnel — keep it genuinely free and rigorous.

---

## 12. Roadmap (spinout sequencing)

```
Stage A — Foundations (no accounts)        Phase 0 trust UI (badges, confidence pill, explain panel,
                                           in-app export, API discoverability) + decide BI brand + file mark
Stage B — Reports-first (validate $)       Hand-built supplier/buyer/category reports for 3–5 design-partner
                                           customers; bespoke pricing; learn which segment pays
Stage C — Metered API / data subscription  Add API keys + quota + usage metering + ToS pass-through over the
                                           existing 7 exports + procurement router; Researcher/Enterprise tiers
Stage D — Branded self-serve frontend      Second (branded) app over the same views: dossiers, market maps,
                                           URL-saveable searches (the honest pre-accounts substitute)
Stage E — SaaS (the lift) ⚠️ owner sign-off Identity + persistence + saved profiles + scheduled bid-intel email
                                           + competitor watchlists; GDPR notice + DPA; CPV enrichment promoted
```

Stages A–B reuse the roadmap's Phase 0–2 directly. Stage E *is* the roadmap's Phase 5 — do not start
it autonomously; it carries the PII/consent/scheduler decisions.

---

## 13. Go-to-market

- **Top-of-funnel = the free civic product.** Journalists citing Dáil Tracker, the open procurement
  explorer, and the open API are the awareness engine. Add an unobtrusive "for suppliers/consultants:
  [BI product]" link from procurement pages — *funnel, not paywall*.
- **Wedge = bespoke reports for 3–5 design partners** (one bid consultancy, one mid-size supplier, one
  agency/journalist desk). Charge from day one; their feedback defines the SaaS feature set and which
  tier converts.
- **Content marketing on honest signal:** publish periodic free "state of the market" pieces (CPV
  concentration, single-bid trends, top buyers) — demonstrates the intelligence, drives inbound,
  reinforces the no-inference credibility.
- **Direct outreach** to bid consultancies and trade bodies (the highest-LTV, most-willing-to-pay
  segment) once 2–3 reports exist as proof.
- **Geographic extension** later: the TED corpus already covers EU/UK-relevant award notices — a
  natural expansion beyond the small domestic TAM once the model is proven.

---

## 14. Legal / licensing checklist (hand to the solicitor)

1. Confirm **TED/EU-OJ** and **per-publisher payments** licences permit commercial reuse + the
   attribution wording (NOTICE has the others).
2. ⚠️ **Iris Oifigiúil**: confirm commercial reuse of *facts* from corporate-distress notices, or
   exclude that enrichment from the paid product.
3. Draft **API/Export Terms of Service** that pass through: attribution, never-sum caveat, no
   re-identification of person rows, no resale of raw exports, source-licence compliance.
4. **GDPR** for the alert/email feature: lawful basis, privacy notice, consent for marketing,
   retention, data-subject rights, DPA review.
5. **Commercial licence agreement** ([`legal/COMMERCIAL_LICENCE_AGREEMENT_TEMPLATE.md`](../legal/COMMERCIAL_LICENCE_AGREEMENT_TEMPLATE.md)):
   confirm the data-exclusion clause covers the BI service, plus liability cap and Irish governing law.
6. **Trademark** the BI product name (separate from "Dáil Tracker"); keep brands distinct.
7. **CLA enforcement** live before any external contribution (protects your right to run the closed
   commercial shell).
8. Confirm the **company-class / PII double-gate** is inherited by every paid surface (API, exports,
   reports, alerts) — this is both a GDPR control and an ethics control.

---

## 15. Decisions

**Resolved (owner, 2026-06-28):**
- ✅ **Entry point = reports-first.** Hand-built supplier/buyer/category reports for 3–5 paying design
  partners before building accounts. (Matches recommendation.)
- ✅ **Iris-derived corporate-distress is IN the paid product — fact-only + attributed**, accepting
  risk pending solicitor confirmation. *Overrides the exclude recommendation.* **(Timing revised
  2026-07-08 per Fable §4/§7: confirm-before-first-SALE, not before scaling; Iris in hand-built
  reports only — never exports/API; solicitor brief expanded to sui generis DB right + site terms.)*

**Revised after Fable assessment (owner, 2026-07-08):**
- ✅ **Ministerial-diary access — OUT of the paid product entirely** (free civic only). Reverses the
  2026-06-28 "may be paid" position; see §4 and [Fable §7](BI_SPINOUT_FABLE_ASSESSMENT.md).
- ✅ **Lobbying-overlap — MAY stay paid, tightly gated**: per-report/owner-gated, never in bulk
  export/API, raw counts only, award-€ separated from lobbying counts (§4).

**Still open (⚠️ owner-only):**
1. **BI product name & brand** (recommend a fresh mark, not "Dáil Tracker"; "Bid Signal" reuses an existing asset name).
2. **Whether/when to commit to Phase 5** (accounts/alerts) — the GDPR + scheduler + PII lift.
3. **Geographic scope**: Ireland-only first, or design for TED/EU extension from the start.
