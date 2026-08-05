---
tier: PLAN
status: LIVE
domain: commercial
updated: 2026-08-05
supersedes: [BI_SPINOUT_ARCHITECTURE.md, BI_SPINOUT_STAGE_AB_PLAN.md, BI_SPINOUT_FABLE_ASSESSMENT.md, BI_SPINOUT_BLIND_SPOTS.md]
read_when: deciding whether or how to commercialise Dail Tracker procurement data, recruiting pilot customers, pricing an evidence pack, or considering procurement alerts, API access, or SaaS
key: PLAN|LIVE|commercial
---

# Procurement Commercial Validation Plan

**Decision:** run a six-week, paid, service-led validation. Do not build a separate procurement
platform, accounts, automated alerts, billing, or a commercial frontend before the validation gate
passes.

**Initial offer:** a human-reviewed, tender-specific **Procurement Evidence Brief** for Irish bid
consultancies and established repeat bidders. The customer buys research time saved and a traceable
evidence pack, not access to public data and not a recommendation about whether or how to bid.

**Status of every commercial claim in this document:** hypothesis until a customer pays. Dataset
figures are verified current-tree observations as of 2026-08-05; they are not market-size estimates.

## 1. Authority and scope

This is the live commercial plan. It supersedes the demand, pricing, sequencing, customer and
business-model claims in:

- `BI_SPINOUT_ARCHITECTURE.md`
- `BI_SPINOUT_STAGE_AB_PLAN.md`
- `BI_SPINOUT_FABLE_ASSESSMENT.md`
- `BI_SPINOUT_BLIND_SPOTS.md`

Those documents remain historical records. Their technical and ethical cautions may still be useful,
but they do not authorise a build or establish willingness to pay.

The following remain supporting technical references, not commercial approvals:

- `BID_INTELLIGENCE_PACK_ENGINE.md` and `pipeline_sandbox/bid_intelligence/` describe a prototype.
- `PROCUREMENT_MASTER.md` and `DATA_GRAINS.md` control money-grain correctness.
- `SOURCE_CONFIDENCE_SYSTEM.md` controls provenance and uncertainty presentation.
- `COMMERCIALISATION_PLAN.md` covers repository licensing scaffolding, not product demand.

This plan does not change the free civic product, licence any upstream data to customers, or authorise
commercial use of a source whose reuse terms have not been confirmed.

## 2. What is commercially interesting now

The defensible asset is not a list of tenders. It is the operational work needed to reconcile messy,
fragmented Irish public records while preserving source links and limitations:

| Current asset | Verified baseline | Commercial use | Binding limitation |
|---|---:|---|---|
| National eTenders awards | 62,763 supplier-award rows; 40,387 tender IDs; 2,249 buyers | buyer history, comparable awards, incumbency and competition context | only 20,597 rows have CPV; only 16,404 are safe to total |
| Public-body payments and POs | 384,208 lines; 85 publishers; 29,502 suppliers | separate evidence of disclosed paid or committed amounts | partial publisher coverage; thresholds and VAT bases differ; never total SPENT with COMMITTED |
| National live-tender snapshot | 2,349 rows; 1,200 with CPV | sub-EU-threshold opportunity context | snapshot retrieved 2026-07-13; portal reuse terms unconfirmed for commercial alerting |
| TED competition notices | 13,456 notices; 11,112 with estimates | current EU-threshold pipeline and CPV routing | estimate is PLANNED, not awarded or spent; 2024+ coverage |
| Supplier entity spine | 28,357 entities; 3,800 in both awards and payments; 6,468 with CRO | cross-register supplier research | matches are incomplete; absence is not zero and not proof of no relationship |
| Procurement query layer | 59 procurement SQL view definitions plus core queries, API and exports | repeatable research and report composition | a prototype is not a supported product or freshness SLA |

Sources: `data/_meta/procurement_coverage.json`,
`data/_meta/procurement_payments_fact_coverage.json`,
`data/_meta/etenders_live_tenders_coverage.json`, `data/_meta/ted_ie_tenders_coverage.json`, and
`data/_meta/supplier_entity_xref_coverage.json`.

These figures establish useful depth, not completeness. They must never be presented as the full Irish
procurement market or as annual State spend.

## 3. Commercial thesis

### The job to be done

When a bid team has identified a real public-sector opportunity, it spends time gathering fragmented
history before it can make its own judgement. The proposed service assembles the documented history,
labels what is missing, and hands the judgement back to the bid consultant or supplier.

The safe promise is:

> We assemble source-linked buyer, award, supplier, competition, payment and advertised-renewal
> evidence for a live procurement opportunity. Your bid team interprets it. We do not price the bid,
> predict the winner, or recommend whether to participate.

### Why not tender alerts

Tender discovery is already free or crowded:

- [eTenders supports CPV-based supplier email notifications](https://www.etenders.gov.ie/epps/cft/downloadInfoItem.do?documentId=1956715).
- [TenderFlare](https://tenderflare.com/) combines eTenders and TED with buyer analytics and was free
  in beta when checked on 2026-08-05.
- [TenderWatch Pro](https://tenderwatch.ie/upgrade) advertises instant alerts and competitor-award
  intelligence.
- UK platforms such as [Stotles](https://www.stotles.com/) already bundle free discovery, historical
  awards, buyer intelligence and analyst services.

This validates the underlying user problem but makes a generic Irish alert/search product a weak entry
point. Dail Tracker must win on difficult reconciliation, local evidence quality and analyst review.

## 4. Initial customers

### Primary: Irish bid consultancies and bid writers

Why test them first:

- they repeat the research job across several clients;
- they can assess accuracy and usefulness quickly;
- a brief can become a billable or white-labelled input to their existing service; and
- one relationship can generate repeat demand without a self-serve platform.

The pilot list should contain at least six consultancies with active public-sector tender work. Do not
count a friendly conversation, free review or expression of interest as demand.

### Secondary: established repeat bidders

Target organisations with an existing bid, commercial or business-development function and repeated
Irish public-sector participation. Avoid defining the segment by company size alone; the important
qualifier is a recurring research workflow and budget for bid support.

### Later, only if pulled by demand

- procurement platforms or consultancies seeking a white-labelled payments/entity enrichment feed;
- public-sector procurement teams seeking category or supplier-market research; and
- enterprise/API customers with a specified machine-readable use case.

Do not target micro-businesses as the first paid segment. Keep journalists, academics and civic users on
the free side unless they commission ordinary professional research under a separate engagement.

## 5. Product v0: Procurement Evidence Brief

### Input

One live tender or a tightly bounded buyer-and-category research question supplied by the customer.

### Required output

A short PDF or HTML brief, with a machine-readable appendix available where lawful:

1. **Opportunity record** — title, buyer, deadline, procedure, value if supplied, CPV if supplied, and
   the authoritative notice link.
2. **Five decision-relevant evidence findings** — selected by a human analyst and stated as facts, each
   with its source and limitation.
3. **Buyer history** — relevant published awards and purchasing patterns; buyer identity must be
   resolved through the curated crosswalk or marked unresolved.
4. **Comparable published awards** — a bounded, explained selection; one-off award values and framework
   ceilings shown separately.
5. **Supplier landscape** — documented repeat winners and firms appearing in comparable awards; never
   described as co-bidders unless the source proves it.
6. **Payment/PO evidence** — separately labelled SPENT and COMMITTED disclosures for relevant company
   entities, only where the entity match is supportable.
7. **Advertised renewal evidence** — contract terms or estimated end dates labelled as advertised or
   derived, never as verified renewal events.
8. **Coverage and provenance appendix** — data currency, source links, entity-match basis, unmatched
   counts, exclusions and the money-grain caveat.

### Product standard

- Aim for 5-8 useful pages before appendices. This is a design target, not a reason to omit evidence.
- Put findings first and methodology last; do not lead with a wall of caveats.
- Record actual analyst production time. Do not promise a turnaround SLA during the pilot.
- Pin each delivered brief to source-file hashes, retrieval dates and the repository revision.
- Reproduce every headline figure from a registered view/query or an explicitly documented manual
  source check.
- A missing or ambiguous match renders `unresolved`, not zero.

### Out of scope

- bid/no-bid recommendations;
- bid prices, target prices, probability or chance of winning;
- AI-written tender responses or objection/complaint drafting;
- claims of favouritism, corruption, influence, waste or procurement failure;
- political, lobbying or ministerial-diary material in the paid brief;
- personal dossiers, directors, sole traders or other natural-person profiling;
- comprehensive-market, comprehensive-spend or real-time claims; and
- combining AWARDED, PLANNED, SPENT, COMMITTED, BUDGET or TED values.

## 6. Pricing test

No price has been validated. The old EUR 1,500-10,000 report range is retired as a planning assumption.

For the first brief, test a quoted pilot price in the **EUR 750-1,500 before any applicable VAT** range. Choose the
quote based on the bounded scope and expected analyst work, not the number of rows, figures or leads.
Do not offer the work free in exchange for feedback; a free engagement tests interest, not willingness
to pay.

Record for every quote:

- quoted price and accepted/rejected outcome;
- expected and actual analyst hours;
- what the prospect expected to do with the brief;
- the closest alternative they would otherwise use;
- whether the cost can be passed through to a client; and
- the price and scope of any requested repeat.

Do not publish subscription, API or enterprise pricing until a customer requests that delivery model.

## 7. Six-week validation

### Week 0: make one credible sales artefact

- Select a current real tender in a data-rich category used by at least one target prospect.
- Rebuild the sandbox output into the Product v0 format.
- Manually verify buyer identity, the five findings and at least three headline figures.
- Add a snapshot manifest and correction contact.
- Remove generic tables that do not change a customer's research work.

This work may improve templates or correctness. It must not create accounts, alerts or a new frontend.

### Weeks 1-2: discovery and paid offers

- Approach ten named prospects: at least six bid consultancies and up to four repeat bidders.
- Conduct structured 30-minute conversations using the sample.
- Ask about their last real research task, hours spent, sources checked, errors feared, current tools,
  purchasing authority and what would make the sample unusable.
- End each qualified conversation with a paid offer for a real current opportunity.

Avoid asking whether the idea is “interesting”. Ask what they did last time and seek payment now.

### Weeks 2-5: deliver and observe

- Deliver up to three paid briefs manually.
- Record corrections, missing evidence, time by section and questions the customer still had to answer.
- Ask for permission to observe how the brief enters the customer's bid workflow.
- After delivery, offer another brief or a manually produced weekly evidence digest. No automated email
  infrastructure is needed for this test.

### Week 6: decide

Evaluate the gates below using invoices, delivery records and explicit customer requests. Do not grade
the pilot on compliments, website traffic, newsletter sign-ups or unpaid usage.

## 8. Gates

### Continue as a reports-led service only if all are true

1. At least **two independent customers pay** for a brief during the six-week window.
2. At least **one customer orders a second brief, agrees a standing order, or pays for a continuing
   manually produced digest** without another large discount.
3. The accepted price covers at least **two times recorded delivery labour cost**, using the owner's
   chosen internal hourly cost, before general overhead.
4. The evidence gaps do not invalidate the customer's main use case, and every material correction can
   be handled through a documented process.
5. No source licence, privacy or freshness issue prevents delivery of the paid scope.

Passing this gate validates a small professional service, not SaaS.

### Consider an alert product only if all are true

- at least two paying customers retain a manual weekly digest for six consecutive weeks;
- they identify same-day or next-day discovery as the value they will pay to retain;
- national and TED feeds meet a measured refresh target without depending on an attended laptop;
- eTenders live-portal reuse terms have been confirmed for the intended commercial use; and
- consent, suppression, correction and delivery-failure handling are designed before automation.

### Consider an API/enrichment product only if all are true

- at least two qualified organisations request machine-readable delivery independently;
- each provides a concrete schema, update cadence and integration workflow;
- one pays for a sample delivery or integration pilot; and
- source attribution, pass-through terms, person-row exclusion and service expectations are agreed.

### Stop or park commercialisation if any is true

- no prospect pays within six weeks;
- prospects value only generic listings or alerts available elsewhere;
- the necessary entity/buyer matches repeatedly fail on paid examples;
- customers demand bid pricing, win prediction or unsupported inference as the main value;
- delivery labour makes the accepted price uneconomic; or
- freshness or reuse rights cannot support the promised scope.

Parking commercialisation does not reduce the civic or research value of the procurement data.

## 9. Work allowed before the first gate

Allowed:

- one high-quality sample brief and its reproducibility manifest;
- correctness fixes required by a real paid example;
- a reusable report template and manual QA checklist;
- a simple engagement letter, invoice workflow and correction process;
- narrow scripts that reduce repeated analyst work without creating a product shell; and
- direct prospect research and interviews.

Not allowed before the reports gate passes:

- a new brand, domain or commercial frontend;
- user accounts, persistence, watchlists or entitlements;
- Stripe or other billing integration;
- scheduled personalised email alerts;
- a customer-facing prediction, scoring or recommendation model;
- a general-purpose competitor dashboard; or
- broad new ingestion justified only by a hypothetical future customer.

## 10. Readiness and risk controls

Before accepting the first payment:

- confirm the exact source scope used in that brief and its attribution;
- confirm commercial reuse terms for any live-portal-derived record included;
- exclude sole traders, individuals and `public_display = FALSE` rows;
- issue an engagement letter stating information-not-advice, liability terms and correction procedure;
- state the report snapshot date and that source records can change;
- retain the query inputs, output, manifest and customer-approved scope; and
- use a lawful basis and basic privacy notice for prospect/customer contact data.

Before scaling beyond a small pilot, obtain professional advice on upstream licences, contractual
liability, professional-indemnity cover, GDPR responsibilities and any source-specific database rights.

Operational moat should be described honestly: disciplined curation, refresh operations, source
provenance, Irish buyer/entity reconciliation and analyst review. The underlying public facts are not
exclusive.

## 11. Pilot scorecard

Maintain one row per prospect and one row per delivered brief. At minimum record:

| Area | Measures |
|---|---|
| Demand | qualified conversations, quotes, paid customers, repeat orders, reasons lost |
| Economics | price, analyst hours, rework hours, direct labour cost, contribution before overhead |
| Utility | customer's stated decision/workflow, sections used, research hours reportedly avoided |
| Quality | corrections, unresolved matches, missing sources, figures independently reproduced |
| Operations | source age at delivery, elapsed delivery time, manual failure points |
| Product pull | requests for digest, API, buyer research, category research or other recurring form |

The owner makes the final go/park decision. The scorecard supplies evidence; it does not generate an
automated commercial score.

## 12. Immediate next actions

1. Name ten prospects and identify the person who owns or performs bid research at each.
2. Pick one live tender that overlaps the first three prospects' work.
3. Produce the five-page sample and snapshot manifest from existing queries.
4. Prepare a one-page offer and engagement-letter draft.
5. Begin conversations and issue the first paid quote before undertaking further product engineering.

The next planning decision occurs after the first five prospect conversations or the first paid brief,
whichever comes first. Until then, this is a validation exercise, not a product build.
