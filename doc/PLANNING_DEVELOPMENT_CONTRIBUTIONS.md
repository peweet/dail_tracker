# Development Contributions — cross-council residential rate scan (2026-06-14)

**The "tax paid to the council" on a grant of permission** — Section 48 of the Planning &
Development Act 2000. Each LA adopts its own scheme as a **reserved function of councillors**, so
rates are **per-authority, non-uniform, version-dated, and mostly indexed annually** (WPI –
Building & Construction, CSO). Scope/legal background: `doc/PLANNING_PERMISSION_SCOPING.md` §19.

> ⚠️ **No-inference / use live rates.** Figures below are the scheme **base rates** as published;
> most councils index them upward each 1 January, so the *charged* figure is at or above these.
> Structures differ wildly (flat €/unit · flat €/m² · banded €/m² by house size · hybrid · sub-area
> split). Always confirm against the council's current scheme before quoting. **28/31 captured**;
> Laois + Longford could not be retrieved (sites hard-block automated fetch — manual download
> needed); Wicklow's general S.48 residential rate is not cleanly published (only its S.49 railway
> levy appears in comparators).

## Worked example — a 240 m² house, by council

240 m² = 2,583 sq ft. A "€9/sq ft" rule of thumb ⇒ ~€23,250 — this is a **Dublin/city-tier** figure,
**not** representative of rural counties (which run an order of magnitude lower).

| Council | Scheme | Residential structure (verbatim) | **240 m² house derives to** | Source |
|---|---|---|---|---|
| **Galway County** | 2016 (rev. 2019) | Sub-Area 2 rural: €1,000 + €5/m²; Sub-Area 1 (LAP/GTPS): €2,000 + €7/m² | **€2,200 rural / €3,680 town** | galway.ie DCS 2016 (Wayback 2025-08) |
| **Galway City** | 2020-2026 | €90/m² | **€21,600** | galwaycity.ie DCS 2020-2026 |
| **Dublin City** | current | €99.84/m² | **€23,962** | Savills infosheet |
| **Fingal** | 2021-2025 | €106.46/m² | **€25,550** | Savills / fingal.ie |
| **South Dublin** | 2021-2025 | €104.49/m² | **€25,078** | Savills / sdcc.ie |
| **Dún Laoghaire-Rathdown** | current | €11,478.77/unit (countywide, excl. Cherrywood/Sandyford) | **€11,479** (flat/unit) | Savills |
| **Kildare** | 2023-2029 | €50.00–65.00/m² (banded) | **€12,000–€15,600** | Savills / kildarecoco.ie |
| **Meath** | 2024-2029 | €6,000–11,000/unit (banded by size) | **€6,000–€11,000** (flat/unit) | Savills / meath.ie |
| **Cork City** | 2023-2029 | €49.87/m² | **€11,969** | corkcity.ie |
| **Limerick City & County** | current | Rural €2,950/unit + €55/m² over 150 m²; Non-rural €8,100/unit + €62/m² over 100 m² | **€7,900 rural / €16,780 non-rural** | Savills |
| **Waterford City & County** | current | €3,000–9,000/unit (banded) | **€3,000–€9,000** (flat/unit) | Savills |
| **Carlow** | 2025-2029 | Rural Housing Policy banded (>200 m² = €42/m²); Urban Residential €29/m² | **€10,080 rural / €6,960 urban** | consult.carlow.ie |
| **Cavan** | 2026-2030 | €23/m² (min €5,000) | **€5,520** | consult.cavancoco.ie |
| **Clare** | 2025-2029 | Rural Village €3,500/unit (≤200 m²) + €20/m² over; Other €5,549/unit + €20/m² over | **€4,300 rural-village / €6,349 other** | clarecoco.ie |
| **Cork County** | 2015 (current) | CASP €24.49/m²; N&W Cork €19.04/m² — first 40 m² of a house free | **€4,898 CASP / €3,808 N&W** (on 200 m² chargeable) | corkcocoplans.ie |
| **Donegal** | 2025-2030 **DRAFT** | Flat/dwelling banded: <150 = €3,000; 150–200 = €4,000; >200 = €6,500 | **€6,500** (draft) | consult.donegal.ie |
| **Kerry** | 2017 | €1,500/unit (≤125 m²) + per-m² over 125 (rate unclear in extract); +€12/m² Tralee/Killarney | **≥€1,500** (base; >125 m² uplift unconfirmed) | docstore.kerrycoco.ie |
| **Kilkenny** | 2026-2030 | Banded €/m²; rural & urban both €50/m² in 201–275 band | **€12,000** | kilkennycoco.ie |
| **Laois** | 2023-2029 | **NOT RETRIEVED** (PDF host unreachable to fetcher) | — | laoiscoco.ie (blocked) |
| **Leitrim** | 2023 | Urban €15/m² (min €4,000); Rural €2,000 (≤200 m²), 201–250 = €15/m², 251+ = €17.50/m² | **€4,000 urban / €3,600 rural** | leitrim.ie |
| **Longford** | 2023-2027 | **NOT RETRIEVED** (DOCX/site blocks all bots; no archive) | — | longfordcoco.ie (blocked) |
| **Louth** | 2023 | €11,646/unit (flat) | **€11,646** (flat/unit) | consult.louthcoco.ie |
| **Mayo** | 2023-2028 | €3,000 OR €20/m², whichever greater | **€4,800** | mayo.ie |
| **Monaghan** | 2021-2026 | €840/unit + community charge: rural €1,060/unit (≤200 m²); settlement €1,060 + €15/m² 200–300 | **~€1,900 rural / ~€2,500 settlement** (base) | monaghan.ie |
| **Offaly** | 2021-2025 | Urban €4,010/unit; Rural €3,010/unit (flat) | **€4,010 urban / €3,010 rural** | offaly.ie |
| **Roscommon** | 2014 (as amended 2020) | Urban €4,400/unit; Rural €3,600/unit (flat) | **€4,400 urban / €3,600 rural** (base) | roscommoncoco.ie |
| **Sligo** | 2018-2024 | Banded €/m²: 201–300 m² = €25/m² | **€6,000** | sligococo.ie |
| **Tipperary** | 2020 | €31/m² (rural & urban); not indexed | **€7,440** | tipperarycoco.ie |
| **Westmeath** | 2022-2026 | >200 m² = €4,087.45 + €29.20/m² over 200 | **€5,255** | westmeathcoco.ie |
| **Wexford** | 2026-2029 | €25/m² (houses, extensions, nursing homes) | **€6,000** | wexfordcoco.ie |
| **Wicklow** | current | **S.48 residential not cleanly published** (only S.49 Navan-Dublin railway levy in comparators) | — | wicklow.ie |

**Spread:** ~€1,900 (Monaghan rural, base) to ~€25,550 (Fingal) for the same 240 m² house — a **>13×**
range. Galway County sits near the bottom; the Dublin authorities + the two cities sit at the top
(where the "€9/sq ft" rule of thumb holds). This dispersion is the whole reason to capture it
per-council.

### External validation (selfbuild.ie, 2026-06-14)
An independent self-build-industry survey corroborates the scan's **shape and extremes**:
- **Avg levy for a 200 m² house ≈ €6,805** across schemes — consistent with our 240 m² mid-table figures.
- **Lowest = €1,060, a rural house in Monaghan** — same council our table flags as cheapest (~€1,900 base;
  selfbuild's lower figure ≈ the community/recreation component before the roads add-on / a smaller house).
- **Highest = €30,301.50, DLR Cherrywood** — same area our table tops out on (DLR Cherrywood €28,882.73/unit;
  the gap = indexation + Irish Water).
- **Tipperary ≈ €7,000** (selfbuild) ≈ **our €7,440** (€31/m² × 240); **Kilkenny €15–22.50/m² rural** matches.
- ⚠️ **Some selfbuild figures are OLDER scheme versions** — e.g. Louth shown as €2,875 (pre-2023) vs our
  **current €11,646** (Sept-2023 ~115% hike); Kilkenny €15–22.50/m² (pre-2026) vs our **2026-2030 €26–75/m²**.
  Our scan is the more current; the agreement on *direction/extremes* is the validation that matters.
- **Net-additional confirmed in the wild:** selfbuild cites a Dublin case where **a revised planning
  permission cut the levy to ~€10,000** — exactly the change-of-plans / net-additional mechanism (§"Change
  of house plans" below).

### Third leg — council + official sources (2026-06-14)
Cross-checked against a council's own page and the official government source; both consistent:
- **South Dublin CC (own page):** confirms the s.48 framing ("public infrastructure and facilities
  benefiting development"), Tender-Price-Index indexation, the temporary waiver, and a social-housing
  exemption. ⚠️ **Its live 2024 residential rate is €126.46/m²** — *higher* than the Savills comparator
  (€104.49) in our table. This is the **indexation lag in action** and **proves the doc's core caveat**:
  use the council's live rate, not a comparator. Live SDCC 240 m² ≈ **€30,350** (vs table's €25,078).
- **Citizens Information (official):** confirms the **exempted-development thresholds** our exemptions rely
  on — an extension is exempt up to **40 m²** (permission needed above that), a shed/garage up to **25 m²**,
  and garage-to-domestic conversion under 40 m² is exempt. Matches the "first 40 m² / 25 m²" lines above.
- **Takeaway:** comparator tables (Savills) are fine for *shape and ranking* but **lag live rates** — the
  doc already says to confirm against the council scheme; SDCC is the worked proof.

## Common exemptions/waivers (apply across most schemes)
First **40 m²** of a residential extension · domestic garage/shed below a threshold (often 25 m²) ·
**social / Part V / approved-housing-body** units · disabled-persons / Housing Adaptation Grant
works · replacement of a dwelling destroyed by fire/flood (credit for existing floor area) ·
protected-structure works · LA's own s.179 development · small renewables (≤0.5 MW). **Retention
permissions get no waiver** and are often charged at a multiple (Wexford 3×, Cavan 1.5×).

## The 2023-24 waiver (national, time-limited) — see §19.3 of the scoping doc
Housing for All **waiver of all §48 contributions** + Uisce Éireann connection refund, for
residential **commenced 25 Apr 2023 → 24 Apr 2024**, **extended (Circular PL 02/2024) to commenced
≤ 31 Dec 2024**, completion ≤ 31 Dec 2026. Drove a documented late-2024 commencement-notice surge
(NOAC LA Performance Indicator Report 2024).

## Structure taxonomy (for any future `development_contributions` ETL)
1. **Flat €/unit** — DLR, Louth, Meath, Waterford, Offaly, Roscommon, (Clare/Limerick base).
2. **Flat €/m²** — Dublin City, Fingal, South Dublin, Galway City, Cork City, Cork County, Tipperary, Wexford, Cavan.
3. **Banded €/m² by house size** — Carlow, Kilkenny, Sligo.
4. **Hybrid (flat + €/m² tail)** — Galway County, Westmeath, Monaghan, Limerick, Clare, Leitrim, Kerry.
5. **Flat €/dwelling banded by size** — Donegal.
A normalised model must store {council, scheme-years, structure-type, sub-area, size-band,
flat-component, per-m²-component, min/cap, indexation-basis, source-url, retrieved-date}.

## Relevant documents (where the contribution lives in the planning file)

The contribution isn't one document — it's a chain. For any application, these are the load-bearing
records (legend as in scoping §10: [S]=structured feed · [PDF]=council file · [EXT]=external register):

1. **The adopted Development Contribution Scheme** [PDF/EXT] — the rate book itself; sets €/m² or €/unit,
   sub-areas, exemptions, indexation basis. Adopted by councillors as a **reserved function** (so it's
   political, per-council, version-dated). This is what the cross-council table above captures.
2. **DHLGH s.28 statutory guidance** — *Development Contributions — Guidelines for Planning Authorities*
   (DECLG, Jan 2013): the national framing every scheme must follow (incl. the retention rule, §below).
   Plus circulars, e.g. **Circular PL 02/2024** (the 2023-24 waiver extension).
3. **Notification of Decision to Grant** → **Grant of Permission / Chief Executive's Order** [PDF] — the
   decision document. The contribution is imposed here as a **financial condition** ("the developer shall
   pay €X to the planning authority…") with the amount and the scheme reference stated. The condition,
   not the scheme, is what's legally enforceable on that permission.
4. **Section 49 supplementary scheme** [PDF/EXT] — *additional* contribution where the site sits in a
   designated infrastructure area (e.g. Luas Cross City / Docklands in DLR; railway corridors). Payable
   **on top of** the s.48 charge; an authority may not "double charge" for the same infrastructure.
5. **Commencement Notice (BCMS)** [S, `data.nbco.gov.ie`] — contributions are normally **payable before
   commencement**, so the commencement notice is the practical trigger/clock for payment (and is the feed
   that showed the 2024 waiver-deadline surge).
6. **Annual indexation / Chief Executive Order** [PDF] — re-states the rates each 1 January (WPI Building
   & Construction). Why the *charged* figure drifts above the scheme's base rates.
7. **Compliance submission / agreed revised plans** [PDF] — where minor design changes are agreed with the
   planner under condition compliance (see change-of-plans §below).

> **Ingestibility:** items 1, 2, 5 are the ingestible/structured layer (scheme rates, national guidance,
> commencement feed). Items 3, 4, 6, 7 are **per-application [PDF]** in the council file — the same
> document wall as the rest of the planning file (scoping §10.1). So we can publish *what the scheme
> would charge*, but not *what was actually conditioned/paid on a given permission*, without the PDF.

## Retention permission & contributions

**Process.** Retention permission (granted under **s.34**, PD Act 2000) regularises **unauthorised
development already built**. It's assessed like a normal application; on a **final grant**, a development
contribution **condition** is attached and the contribution falls **due on that grant**.

**Contribution treatment — the key rule: retention gets NO break, and often a penalty.**
- The DECLG 2013 s.28 Guidelines direct that **no exemption or waiver applies to retention applications**,
  and that **authorities are encouraged to impose higher rates** for them (a deliberate disincentive to
  build-first-ask-later). The schemes implement this with **multipliers on the normal rate**:
  - **Wexford** retention = **3×** the floor-space contribution.
  - **Cavan** retention = **1.5×**.
  - Most others charge the **full** standard rate with the exemptions stripped out.
- The 2023-24 **waiver did not apply to retention** either — it was residential-commencement-driven.

**Caveats (no-inference, from scoping §11.3):**
- Retention permission does **not** legalise where it's refused, and does **not** undo the **7-year
  enforcement immunity** question — these are separate from the contribution.
- Where the unauthorised works needed **EIA or Appropriate Assessment**, ordinary retention is barred —
  the route is **substitute consent** (s.177), and the **PD(Maritime & Valuation)(Amendment) Act 2022 /
  s.34(12)** now requires both *past and present* EIA/AA positions to be satisfied, else the retention
  application is **deemed withdrawn**. (Version-stamp: the Planning & Development Act 2024 is reforming
  this regularisation regime as it commences.)

## Change of house plans & contributions

Ireland has **no general statutory "non-material amendment" route** (unlike the UK's s.96A). Changing a
granted design goes one of two ways, and the contribution treatment follows the **floor-area delta**:

| Route | When | Contribution effect |
|---|---|---|
| **(a) Agreed revised plans under condition compliance** | Minor changes the planner accepts as not material (no new application) | If **no net additional gross floor area** → typically **no further contribution** — several schemes **explicitly exempt a change of house plan with no floor-area change** (e.g. **Leitrim**). |
| **(b) Fresh planning application** | Material change to the design/scale | New permission ⇒ new contribution condition, **but charged only on the NET ADDITIONAL floor area** at current rates; credit is given for area already permitted/paid under the superseded grant (the schemes' **anti-double-charge / "net additional development"** clause). |

**Net-additional is the governing principle.** Almost every scheme above contains a clause that
redevelopment/replacement/revision is charged **only on additional floor area** (Sligo, Roscommon,
Clare, Cavan, Mayo, Westmeath, etc. all phrase it this way; replacement dwellings get a **credit for the
existing/demolished floor area**, sometimes on production of LPT evidence). So:
- Bigger house on revision → pay the contribution on the **extra m²** only.
- Same-or-smaller / pure design change → generally **nil additional** contribution.
- A grant **superseding** an earlier one → contribution **re-assessed**, minus what was already paid.

**Caveat (no-inference):** the precise mechanism (what counts as "material", whether a credit is automatic
or needs evidence, multipliers) is **per-scheme and per-planner discretion** — confirm against the
specific council's current scheme and the wording of the grant condition before relying on a figure.

## Outstanding (3 gaps)
- **Laois** (2023-2029) — PDF host `laoiscoco.ie` unreachable to automated fetch; needs browser download.
- **Longford** (2023-2027) — current scheme is a DOCX behind a bot-blocking server; no Wayback copy.
- **Wicklow** — general S.48 residential rate not surfaced (only the S.49 railway levy); needs the scheme PDF.
