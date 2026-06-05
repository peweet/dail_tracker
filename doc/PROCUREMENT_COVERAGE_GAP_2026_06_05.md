# Procurement coverage-gap assessment (2026-06-05)

Method: the **award-level** datasets (eTenders `procurement_awards`, TED `ted_ie_awards`) list
the *universe* of Irish public buyers. Cross-referencing the biggest of them against our
**spend-level** pull (line-item POs/payments-over-€20k) surfaces which bodies we're missing.

## Scale
- eTenders distinct contracting authorities: **1,948**
- TED distinct buyers: **864**
- Our spend-level bodies (POs/payments at line grain): **53**

So award-level is near-comprehensive; spend-level covers ~53 bodies. The list below is the
biggest spend-level gaps, by eTenders award volume / TED notice value, grouped by tractability.

## A. Clear wins — same publication pattern we already parse

**A1. Local authorities — 11 of 31 missing.** The LA-payments lane already parses 20 councils;
these publish the identical "POs/payments over €20k" lists:
> Carlow, Cavan, **Dublin City** (1,720 eTenders awards — the single biggest LA), Dún
> Laoghaire-Rathdown, **Fingal** (494), **Tipperary** (735), **Kerry** (361), Sligo (315),
> Laois, Louth, Roscommon
Wire into the LA registry (`extractors/procurement_la_*`). Highest-value, lowest-risk.

**A2. Government departments not yet wired.** These publish payment/PO-over-€20k collections on
gov.ie — the exact pattern the generic extractor already handles for Defence/Climate/Culture/
Housing:
> **Dept of Agriculture, Food & the Marine** (337 awards, €231m TED), **Dept of Social
> Protection** (€556m TED), **Dept of Foreign Affairs & Trade** (300), and the rest (Justice,
> Health, Education, Transport, Enterprise, Finance, DPER, Taoiseach…)
Add as new `cfg(...)` rows in `procurement_public_body_extract.py` (gov.ie collection URLs).

**A3. Other agencies / ETBs.** Fáilte Ireland (483 awards), ComReg (332), **Cork ETB** (313) and
the other ~14 ETBs (we only have City of Dublin ETB). Mostly gov.ie / FOI pages — wireable.

## B. Known-hard — flagged before, need a render/OCR/right-URL pass
- **Commercial semi-states (€bn-scale, JS-rendered or FOI-only):** Iarnród Éireann/Irish Rail
  (€2.38bn TED), **EirGrid** (€2.58bn), **Uisce Éireann** (€1.17bn), ESB/Electricity Supply
  Board (€366m), An Post, RTÉ, Bus Éireann, daa. These publish behind JS or only via FOI — need
  a browser-render harvest pass. (Memory: NOT_FOUND / JS-rendered set.)
- **Traditional universities (FOI-only €25k register):** UCC, University of Galway, TCD, UCD,
  Maynooth. We have the tech universities (TU Dublin, MTU, ATU); the traditional ones don't run
  a model-publication scheme.
- **Privacy / OCR / HTML-gated:** An Garda Síochána (332, privacy-high), Irish Prison Service
  (83, scanned→OCR), EPA (307, serves `.php` HTML). Sibling context is on some of these.

## C. Central aggregators — note, do NOT treat as line-item spend
- **Office of Government Procurement (OGP)** — 6,324 + 4,135 awards (by far the most) — and
  **Education Procurement Service (EPS)** — 2,040 awards / €2.3bn TED. These run **frameworks**
  that *other* bodies draw down. Their award volume is real but their "spend" is not independent
  line-item PO spend; pulling them as spend would double-count the drawdowns. They are the
  living illustration of the award-vs-spend distinction in the value taxonomy.

## False positives (already covered, name-match artifacts)
- **Irish Defence Forces** — covered via "Department of Defence" (its file is the combined
  "POs for the Department of Defence AND Defence Forces").

## Recommended order
1. **A1 (11 LAs)** + **A2 (departments)** — pure config additions reusing proven parsers; would
   add the largest tranche of real spend-level coverage with near-zero parser risk.
2. **A3 (ETBs / Fáilte / ComReg)** — config additions, slightly more URL hunting.
3. **B (semi-states render pass)** — biggest € but needs new harvest capability; do as a batch.
4. Leave **C (OGP/EPS)** at award-level only.
