# Competitor & analogue landscape — EU / common-law civic + gov-data tools

**Status:** ACTIVE · **Domain:** product/strategy · **Date:** 2026-07-20 · **Scope:** UK,
Ireland, EU, common-law (US excluded by request).

Compiled from a 10-agent web sweep. **All claims are [Reported — subagent relay];**
sub-confidence tags (Verified-fetched vs snippet) are preserved per row. Nothing here was
personally re-fetched by the main loop. Re-verify a specific competitor before quoting it
externally.

## Headline finding

No product found — UK, Irish, or EU — combines **procurement + lobbying + votes + interests
+ planning** as a live, queryable, per-record cross-register join. Each lane has a strong
incumbent; the *integration* is the gap. [Indicative — absence after ~8 search rounds is not
proof; no counter-example surfaced across 40 sources.]

The closest structural peers each join 2–3 registers around ONE theme:
- **Digiwhist / opentender.eu** — procurement × company ownership × politician asset
  declarations, for conflict-of-interest scoring. Pan-EU incl. Ireland (153,777 tenders /
  43,223 awards, to Dec 2024). No lobbying, no votes. [Reported — live-UI join unverified,
  root 403'd; methodology from DIGIWHIST papers]
- **Romania PREVENT (ANI)** — procurement × integrity declarations × trade registry ×
  personnel, real-time conflict alerts. **Government enforcement system, not public.**
- **Slovakia RPVS** — beneficial-ownership disclosure IS the contract-eligibility gate
  (join forced by law, not by tool).
- **Abgeordnetenwatch (DE)** / **Civio "Quién Manda" (ES)** — money×power, but delivered
  as journalism / one-off investigations, not a live join layer.
- **OCCRP Aleph** — procurement (OCDS→FtM bridge) × sanctions × PEP × leaks, but an
  investigator's workbench, not a public dashboard.

**Nobody found joins lobbying-meetings → contract-awards, or "voted on X while holding an
interest in X" as a live flag.** That specific gap is the differentiator to protect.

## By category

### Procurement / spend intelligence
| Tool | Country | Data | Model | Verdict vs us |
|---|---|---|---|---|
| Tussell | UK | tenders + **invoice-grain spend, kept separate from award value** + 80k contacts | paid sub, 3 tiers | PARTIAL — same never-sum grain discipline; supplier-facing |
| Stotles | UK+IE(NI) | 8M notices, framework linkage, "declared intent" docs (budgets/minutes/FOI) | free→£75→£475→£873/mo | PARTIAL — supplier CRM/bid tool |
| Spend Network | global | 30 fields/record, 160 countries, BO data | B2B API/white-label | DIFFERENT — data vendor |
| Oscar Research | UK | 300k postholders × contracts | sub, custom quote | DIFFERENT — BD/M&A tool |
| BiP/Tracker Intelligence | UK+ROI | alerts, spend analysis, AI contract eval | sub, 4 tiers | DIFFERENT — bid tool |
| Contracts Finder / Find a Tender | UK gov | awards, OCDS, free API | free/OGL | PARTIAL — **awards only, 0 payment rows** |
| eTenders (data.gov.ie) | IE | award notices >€25k | free, CSV-only | PARTIAL — no payments, no entity res |
| TenderFlare / TenderWatch | IE | eTenders+TED, AI summaries | free beta | PARTIAL — SME discovery |
| OCP / OCDS | standard | planning→implementation schema (payments field exists) | nonprofit | BENCHMARK not competitor — the schema to model against |
| OpenSpending/OKFN | EU | budget files | dormant ~2021 | DIFFERENT, superseded |
| TheyBuyForYou | EU | procurement knowledge graph | **defunct 2020** | DEAD |

### Planning / land / property
| Tool | Country | Data | Model | Verdict |
|---|---|---|---|---|
| **PlanningAlerts.ie** | IE | LA applications + **ABP appeals** + decisions/FI/commencement | free / €1.99mo / biz | **CLOSEST IRISH COMPARABLE** — alert-first, not analytical |
| MyPlan.ie | IE | applications (10yr, 31 LAs) + zoning | free gov | map viewer only |
| LandScope.ie | IE | 2.5M parcels, 517k planning + grid + probate, evidence-chain | pilot | site-discovery for developers |
| buildinginfo.com | IE | construction pipeline + CRM | paid sub | sales-lead tool |
| ABP/pleanala.ie case search | IE | appeals 2016– , 70+ filters | free gov | official source; no analytics |
| Glenigan | UK+**ROI** | construction planning→award, hand-researched | custom quote | **watch — claims ROI coverage** |
| Searchland / LandInsight / Nimbus | UK | applications + ownership + outreach | metered sub | deal-sourcing CRMs |
| UK PlanIt | UK | 20.5M apps, 420 authorities, open API | donation | closest civic aggregator; apps-only, no appeals |
| Barbour ABI | UK | 1.6M projects + contacts | ~£4k+ bespoke | construction sales leads |
| PlanWatch/Plottr/MB Planning Alerts | UK | apps + objection-letter tools | 99p–£30/mo | commercial micro-SaaS; validates resident demand |

### Votes / interests / parliament
| Tool | Country | Join? | Note |
|---|---|---|---|
| TheyWorkForYou (mySociety) | UK×4 | per-person tabs, NOT queryable join | interests page won't link votes |
| WhoFundsThem (mySociety) | UK | interests × APPG × donations, **flags low-trust industries** | "separate but coordinated," manual |
| Abgeordnetenwatch | DE | income+committee+votes co-resident; cross-ref = journalism | closest money×power intent |
| WheresMyMEP / MPData.uk | EU/UK | votes + income + donations per person | no procurement/lobbying |
| OpenPolis | IT | votes×group×"force index" | deep analytics, no money join |
| Civio "Quién Manda" | ES | politician×business×influence map | investigative project |
| Regards Citoyens (NosDéputés) | FR | votes + interest-decl digitisation (separate projects) | join unconfirmed |
| Democracy Club | UK | candidates + polling stations | elections logistics only |

### Lobbying / ownership / integrity
| Tool | Join? | Note |
|---|---|---|
| **EU Integrity Watch** (TI-EU) | EC meetings × Transparency Register (live); revolving-door × register (investigative) | MEP meetings CAN'T join (EP data quality); Red Flags procurement is a SEPARATE unjoined tool |
| LobbyFacts.eu | Transparency Register × Commission meetings | single pairing |
| OpenCorporates | company registries, 140 juris | a data *input*, not a competitor |
| OpenSanctions | 412 sources, sanctions+PEP+BO+debarment, matcher API | compliance layer, not civic dashboard |
| Open Ownership BODS PoC | BO × OCDS × sanctions via SPARQL | proof-of-concept, never shipped |

## Feature ideas worth stealing (dev-forum / OSS / competitor harvest)

Ranked by fit to this repo:

1. **Splink** (UK MoJ, open-source) — probabilistic entity resolution across registers
   without a shared ID. Directly evaluable against our NFKD name-join; scalable, 7M+
   downloads. github.com/moj-analytical-services/splink [Reported — snippet]
2. **Cardinal** (Open Contracting Partnership) + **Digiwhist's 9 integrity indicators** —
   standardised procurement red-flag library (single-bidder, short ad periods, bid-price
   clustering, all-but-winner disqualified) computed from OCDS data. Turns our ad-hoc
   procurement audits into a reusable, auditable **scrutiny-priority score**. [Reported —
   Cardinal page fetched by subagent; repo URL unconfirmed]
3. **The "voted-on-X-while-holding-interest-in-X" flag** — NObody surveyed ships this live.
   We have votes + interests already; this is the single most differentiated feature
   available and it's a query, not an ingestion. [Indicative — gap inferred from survey]
4. **Lobbying-meeting → contract-award join** — also unfilled anywhere; our ministerial
   diaries + procurement make it reachable. (Respect the never-sum + 0≠absent caveats.)
5. **Graph visualisation** (ICIJ Datashare + Neo4j, used on Pandora Papers) — trace
   lobbying-org→TD→committee→award chains. **Caveat:** must visually distinguish
   matched vs not-matched edges or the 0≠absent rule is lost in the UI. [Reported — fetched]
6. **"Highlighted interests" pattern** (WhoFundsThem) — don't just list interests, flag the
   ones in low-public-trust sectors. Editorial judgement, but a strong surfacing idea.
7. **LLM-assisted scraper as fallback** for irregular sources (committee pages, council
   agenda PDFs) — mySociety's TheyWorkForYou approach; fits our council-minutes/RHF work.
8. **AI jargon-decoder** (TenderWatch, Claude-based) — plain-English notice summaries for
   citizen readers.
9. **Expiring-contract early-warning reframed as accountability** — Stotles/Oscar sell it
   as a sales signal; we'd surface "renewal without competition" as a scrutiny signal.
10. **OCDS Implementation-stage payments field** — the one schema that formally links award
    to payment; no live UK/EU publisher populates it, so doing award+payment+entity
    together fills a standard-sanctioned gap.

## Funding-model precedents

- **mySociety / SocietyWorks** — free civic tools funded by a wholly-owned trading
  subsidiary selling to councils (FixMyStreet Pro: "£7.12/emailed report vs 9p"). The
  clearest structural precedent for our dual-lane (accountability free + commercial arm).
- **OpenPolis (IT)** — nonprofit funded by selling data feeds to media (RAI, Sky, etc.).
- **Grant stack** — Quadrature, Lottery, Adessium, NED, Porticus (mySociety); Omidyar/
  Hewlett/Gates/GIZ (OCP). The accountability lane runs on grants, not subscriptions —
  consistent with our dual-licensing memory.

## Cautionary lessons (postmortems)

- **civictech.guide "graveyard"**: funding raised ≠ users retained (Jumo/Vote.com raised
  $20M+, failed); "only a minority do user research before choosing a tool"; **maintenance
  is the killer, not launch** — a 5-yr-old site's cert renewals are "a grudge purchase."
- **PlanningAlerts UK (mySociety) retired** — killed by "the amount of time needed to
  maintain the many scrapers." Our 31-council + semi-state scraper fleet is exactly this
  risk; the freshness-duty gap in the journalist plan is the same wound.
- **Pombola retired** — mySociety concluded legislatures differ too much for one
  configurable platform; pivoted to data infrastructure. **Argument for staying
  Ireland-specific**, not generalising the schema.
- **ODI research**: LLMs "rarely admitted when they didn't know" on public-service Q&A —
  reinforces our trust-tier gating on any natural-language query feature.

## Our pricing vs the market (consolidated from COMMERCIALISATION_PLAN + BI_SPINOUT)

The commercialisation plan explicitly anchors our pricing on these same competitors:
*"anchor on hours saved… and on comparables (UK public-market-intelligence firms — Tussell,
Stotles, Spend Network)"* [Verified — COMMERCIALISATION_PLAN.md:282-283]. Placing our
hypothesised tiers beside the competitor prices this sweep found:

| Our tier (hypothesis, €/yr) | Our price | Nearest competitor & their price |
|---|---|---|
| Free civic | €0 | mySociety model (grant-funded); PlanningAlerts.ie free tier |
| Researcher | €120–300 | — (press/academia discount = funnel + mission) |
| SME supplier | €600–1,200 | Stotles Sales Studio £75/mo ≈ £900/yr [Reported] |
| Bid consultant | €2,400–6,000 | Stotles Bid Studio £990/mo; Tussell "higher than typical monitoring" (unpubl.) |
| Enterprise/API | €10k–30k+ | Barbour ABI ~£4k+ bespoke; FiscalNote enterprise >$100k [Reported] |
| Bespoke report | €1.5k–10k/report | Oscar Research custom-quote reports |

[All our-tier figures: Verified — COMMERCIALISATION_PLAN.md:271-283, flagged there as
hypotheses to test. All competitor figures: Reported — this sweep, not re-fetched.]

The strategic read this pricing-adjacency confirms: **we are cheaper than the UK
procurement-intel incumbents on the commercial lane and free on the civic lane** — which is
only defensible because the *integration* (cross-register) is the value, not any single
lane where Tussell/Stotles already win at scale. The commercialisation plan's own moat line
agrees: *"the expensive, defensible work (three-grain discipline, CRO resolution, never-sum,
no-inference)… is precisely what naïve competitors get wrong"* [Verified —
COMMERCIALISATION_PLAN.md:329].

## Related internal docs — and the "competitor" naming collision

**Two docs say "competitor" and mean opposite things. Do not merge them.**

- **This doc (COMPETITOR_LANDSCAPE)** = *market rivals* — other products/companies (Tussell,
  mySociety, FiscalNote) that compete with our app. Read when scoping product strategy or
  answering "does this already exist?"
- **[PROCUREMENT_COMPETITOR_ANALYSIS.md](archive/PROCUREMENT_COMPETITOR_ANALYSIS.md)** = a *feature*
  — "which firms compete with a given contractor," built from our own award/payment data
  (trade tagger, CRO anchor, co-occurrence). It is a RECORD doc cross-linked to seven
  sibling design docs; dissolving it would break that web. Left in place, pointer only.

The commercial-strategy docs that *consume* this landscape (not merged — each owns a distinct
layer):

| Doc | Layer it owns |
|---|---|
| [COMMERCIALISATION_PLAN.md](COMMERCIALISATION_PLAN.md) | pricing tiers, licence/royalty model — the market-rival pricing above is pulled from here |
| [BI_SPINOUT_ARCHITECTURE.md](archive/BI_SPINOUT_ARCHITECTURE.md) | the commercial-lane entity + ethics firewall; bid-consultant ICP (€2,400–6,000/yr) |
| [PROCUREMENT_INTELLIGENCE_ROADMAP.md](archive/PROCUREMENT_INTELLIGENCE_ROADMAP.md) | the umbrella phasing the commercial features sit in |
| [TENDER_ALERT_SYSTEM_DESIGN.md](archive/TENDER_ALERT_SYSTEM_DESIGN.md) | the alerting SaaS shell (post willingness-to-pay gate) |

**Consolidation verdict (2026-07-20):** market-rival knowledge now lives in one place — this
doc. Competitor *pricing* was scattered across COMMERCIALISATION_PLAN and BI_SPINOUT; it is
now mirrored here beside the products it prices against. The procurement feature-doc keeps
its name but gets the disambiguation above so the collision can't mislead a future session.

## The single closest analogue to study

**diofanti.org** (Greece) — nonprofit, small-EU-state, government-spending dashboards for
citizens/journalists. Same shape as this project one market over. Both direct fetches were
rate-limited this session — **flagged for a dedicated follow-up fetch**, not yet verified.
