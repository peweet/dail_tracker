---
tier: RECORD
status: LIVE
domain: sources
updated: 2026-06-22
supersedes: []
read_when: needing worked example FOI-request leads generated from the combined spend+FOI-log join, to demo or pitch the journalist flow
key: RECORD|LIVE|sources
---

# FOI case library — 20 worked leads across 4 categories

**Companion to** `doc/JOURNALIST_FOI_FLOW_DESIGN.md`. This applies the 6-stage journalist→FOI flow to
20 real cases mined live from the combined corpus (2026-06-22): **5 councils, 5 semi-state/state
bodies, 5 unclear ministerial-diary meetings, 5 thin lobbying returns.** Every figure is from an
actual query; the "already asked?" line is a real probe of publicinformation.ie's FOI log where run.

**Caveats that travel with every case (non-negotiable):**
- **Grain:** payment figures are `po_committed`/paid (>€20k PO disclosures) — never summed with award
  ceilings. A large/concentrated payment is a *prompt to look*, not evidence of wrongdoing.
- **Single-bid / concentration:** factual signals, never verdicts — often legitimate (sole specialist,
  framework, genuine urgency).
- **Lobbying & diaries:** self-filed/self-curated; co-occurrence and access, **not** proof of influence.

The flow per case: **① anomaly (our data) → ② where the record stops → ③ already asked? (their FOI
log) → ④ the exact FOI ask** (to the body, with their FOI email from the register).

---

## A. Councils (5)

### A1 — Galway City: a €31.5m purchase order in a single line
- **①** Galway City paid **O'Connell Contracts (OCC) €31.5m in ONE payment line** — 29.4% of the
  council's entire >€20k PO spend in the corpus.
- **②** One PO number, no contract reference, no project name, no payment schedule.
- **③** *Already asked?* No prior FOI on this contract in Galway City's log → clean new request.
- **④** *"Please provide the contract, tender outcome and payment schedule for the works/services
  paid to O'Connell Contracts (OCC) recorded as a ~€31.5m purchase order, including the project name,
  contract value, variations, and current status."*

### A2 — Sligo: €80.4m to a contractor that later collapsed ★
- **①** Sligo paid **Roadbridge €80.4m across 2 lines** (28.9% of its PO spend). Roadbridge entered
  receivership in 2021 — a council's single biggest contractor subsequently failed.
- **②** No record of which project, retention monies, or what happened to incomplete works post-receivership.
- **③** *Already asked?* **0 Roadbridge FOI records anywhere in the corpus** — entirely un-asked.
- **④** *"Records relating to all contracts with Roadbridge Ltd, including contract value, sums paid,
  retention monies held at the date of its receivership (2021), the status of any incomplete works,
  and any additional cost incurred to complete them."*

### A3 — Waterford: a €116.3m single-line payment to BAM Civil
- **①** Waterford paid **BAM Civil €116.3m in ONE line** (18.7% of its PO spend).
- **②** No project name or contract reference attached to a nine-figure payment.
- **③** *Already asked?* No prior FOI tying BAM to a named Waterford project in the log.
- **④** *"Please identify the project(s) to which the ~€116.3m paid to BAM Civil relates, the contract
  value at award versus final cost, the procurement procedure used, and a breakdown of any
  contract-variation or settlement amounts."*

### A4 — Meath: €206.4m to one firm across 1,013 payments
- **①** Meath paid **M.A. Regan / McEntee Partners €206.4m over 1,013 lines** — 23.6% of council PO
  spend, the heaviest single-supplier relationship of any council in the corpus.
- **②** The framework/contract that authorises 1,013 separate payments to one supplier isn't in the data.
- **③** *Already asked?* No prior FOI on this supplier relationship in Meath's log.
- **④** *"The contract(s) or framework agreement(s) under which payments totalling ~€206m were made to
  M.A. Regan/McEntee Partners, the original tender, the basis for repeat awards, and any review of
  value-for-money on this supplier."*

### A5 — Limerick: €65.4m to a single firm of solicitors
- **①** Limerick City & County paid **Leahy Reidy Solicitors €65.4m across 218 lines**.
- **②** Legal spend at this scale usually reflects CPO/litigation/land — none of which is in the PO
  description field.
- **③** *Already asked?* Council legal-cost FOIs are common but none on this firm/total in the log.
- **④** *"A breakdown of the ~€65.4m paid to Leahy Reidy Solicitors by matter type (CPO, litigation,
  conveyancing, advisory), the largest individual matters by cost, and how this firm was procured."*

---

## B. Semi-state & state bodies (5)

### B1 — NTMA: a €182.6m block of payments with no named supplier ★
- **①** The **NTMA shows €182.6m across 887 lines with a null supplier**, plus €119.1m in "Programme
  Fees" to the EFSF. The €182.6m counterparties are absent.
- **②** Supplier name redacted/blank — exactly the field a reader needs. (NTMA also runs the State
  Claims Agency, so much of this may be claim settlements.)
- **③** *Already asked?* **NTMA has 39 FOI records, the relevant ones Refused** — they resist
  disclosure here, so narrow the ask and cite the refusals for internal review.
- **④** *"For payments of €20,000+ recorded without a named payee 2020–2025: the category of each
  (e.g. legal settlement, fee, grant), the number and aggregate value by category, and the legal
  basis for withholding payee identity."*

### B2 — OPW: a €155.8m payment with blank supplier and blank description ★
- **①** The Office of Public Works has a **single ~€155.8m line with no supplier and no description**
  — the largest unexplained single entry of any body in the corpus.
- **②** Both the payee and purpose fields are empty.
- **③** *Already asked?* **0 FOI records on OPW construction contractors** in the log — unexplored.
- **④** *"Please identify the payee, purpose, contract reference and date of the purchase order of
  approximately €155.8m, and explain why the payee and description were not published."*

### B3 — HSE: €238.6m to a law firm, tagged "purchase of buildings" ★
- **①** The HSE paid **Byrne Wallace Solicitors €238.6m across 81 lines, described "Purchase of
  buildings"** — a nine-figure sum to a law firm under a property-purchase label.
- **②** No split between purchase consideration, conveyancing fees, and which properties.
- **③** *Already asked?* **0 FOI records on HSE building/premises purchases** — virgin territory.
- **④** *"For property acquisitions where Byrne Wallace Solicitors acted for the HSE: the properties
  purchased, purchase price per property, the firm's fees separately from purchase consideration, and
  how the legal services were procured."*

### B4 — ESB Networks: €117.7m across 10 lines, no named supplier
- **①** ESB Networks DAC shows **€117.7m over 10 lines with a null supplier**.
- **②** Ten very large payments, no counterparty disclosed.
- **③** *Already asked?* Not present in the disclosure-log corpus (ESB is a commercial semi-state with
  limited FOI scope — confirm coverage first).
- **④** *"The counterparties, purpose and contract references for the ten purchase orders totalling
  ~€117.7m, and the basis on which supplier identities were not published."*

### B5 — HSE: €294m to Pfizer for pandemic vaccines (the "already-worked" path)
- **①** HSE paid **Pfizer Healthcare €294m across 122 lines, "Pandemic Vaccines"**.
- **②** The unit pricing and contract terms are the classic redactions in the published COVID contracts.
- **③** *Already asked?* **117 vaccine/Pfizer FOI records, Part-Granted** — heavily worked. *Fetch the
  existing releases first*; only then file a narrower request on what was redacted.
- **④** *"The per-dose pricing, total quantity and any wastage/write-off for COVID-19 vaccine purchases
  2021–2024, to the extent not already released under [cite prior FOI refs], and the value of doses
  disposed of unused."*

---

## C. Ministerial-diary meetings recorded with no subject (5)

The diary corpus has **30,327 external_meeting entries**; many log a date and minister but a subject
of just "Meeting" / "Private meeting". FOI target = the *department*, asking for the calendar entry,
attendees and briefing for that slot. (Diaries are self-curated and non-exhaustive — access, not influence.)

| # | Date | Minister | Dept | Logged subject |
|---|---|---|---|---|
| C1 | 2025-06-16 | Browne | Housing | "Meeting" |
| C2 | 2025-04-16 | Carroll MacNeill | Health | "Private meeting" |
| C3 | 2025-05-12 | Lawless | Further & Higher Ed | "Private meeting" |
| C4 | 2024-12-16 | Harris | Taoiseach | "Meeting" |
| C5 | 2024-09-16 | Chambers | Finance | "MEETING" |

- **②** Counterparty and topic both absent — the entry discloses that a meeting happened and nothing else.
- **③** *Already asked?* Diary-specific FOIs are uncommon; check the department's log for "diary"/"calendar".
- **④ (template):** *"In respect of the external meeting recorded in [Minister]'s published diary on
  [date] with no subject given (logged only as '[subject]'): the names and organisations of all
  attendees, the agenda or purpose, and any briefing note or follow-up correspondence."*

These are strongest when cross-referenced: if the same date/minister has **no** matching lobbying
return, the "who was in the room" question is sharper (an external meeting that left no register trace).

---

## D. Lobbying returns that are light on detail (5)

SIPO returns are self-filed; many disclose a policy area but a `specific_details` field of a single
word. The FOI target = the **public body that was lobbied** (the DPO/minister/council named in the
return), asking for *its* records of the contact — flipping a thin self-report into documented detail.

| # | Lobbyist | Policy area | `specific_details` (verbatim) | Period |
|---|---|---|---|---|
| D1 | **DRES Developments Ltd** | Development & Zoning | "Zoning" (6 chars) | 2025–26 |
| D2 | **Aspect Developments (ADC) Ltd** | Development & Zoning | "Zoning" | 2025 |
| D3 | Irish Farmers' Association | Energy/Agriculture | "Solar" / "Water" / "Fodder" (5–6 chars) | 2024–25 |
| D4 | IBM | Education & Training | "P-TECH" | 2024 |
| D5 | Car Rental Council of Ireland | Transport | "UK ETA" | 2024 |

- **②** What was actually sought, from whom, and the decision it concerned are all absent from the return.
- **③** *Already asked?* For zoning specifically, **67 zoning FOI records exist and recent ones were
  Granted** — strong precedent that planning authorities release this, so D1/D2 are high-probability wins.
- **④ (D1/D2 — developers on zoning):** *"All records — correspondence, meeting notes, submissions —
  between [developer] (or agents acting for it) and the planning authority/Minister concerning the
  zoning or rezoning of lands in [area] during [period], and any internal assessment of those
  representations."*
- **④ (D3–D5 — sectoral/corporate):** *"Records of representations received from [organisation] on
  [topic] during [period], including the specific policy change sought and any official response."*

D1/D2 are the standout public-interest leads: **property developers lobbying on zoning with a
six-character disclosure**, and the FOI route to the real detail is proven to succeed.

---

## E. MCP-constructed stories (access · revolving door · sectoral clusters)

These three are built from the MCP access/influence tools and are richer than the terse leads above —
each is a publishable narrative with the evidence already assembled. All carry the standing caveats:
the lobbying/diary registers record **access, not influence**; co-occurrence is **not** causation.

### E1 — The revolving door: a former Agriculture chief now lobbies his old department ★★
- **The story (MCP-verified, `dpo_lobbying_profile`):** **Philip Carroll**, a former **Assistant
  Secretary at the Department of Agriculture, Food and the Marine**, is now a registered lobbyist for
  **Ibec** with **122 returns**. His most-targeted official is **Brendan Gleeson — the department's
  current Secretary General — lobbied 49 times**, followed by senior officials Sinéad McPhilips (29),
  Chief Veterinary Officer Martin Blake (19), and former Sec-Gen Aidan O'Driscoll, plus ministers
  Creed, McConalogue, Heydon and Coveney. A man who once ran parts of the department now lobbies its
  most senior civil servants on Ibec's behalf.
- **② Where the record stops:** the lobbying return discloses *that* contact happened, never the
  substance, the meeting records, or whether any post-employment "cooling-off" restriction applied.
- **③ Already asked?** Post-employment / cooling-off FOIs are **near-virgin — only 3 records in the
  entire corpus** (Part-Granted). Open territory.
- **④ FOI (to Dept of Agriculture):** *"All correspondence, meeting notes and minutes of contacts
  between Philip Carroll (and/or Ibec) and the Secretary General or Assistant Secretaries of the
  Department 2020–2025; and any record of post-employment restriction, conflict-of-interest
  declaration or SIPO 'cooling-off' consideration applying to his move from the Department to Ibec."*

### E2 — The Land Development Agency: 106 ministerial meetings, zero register trace ★★
- **The story (MCP-verified, `ministerial_diary_organisation`):** the **Land Development Agency** —
  the State body at the centre of housing and public-land policy — logged **106 meetings with 16
  ministers** from 2018–2025, yet has **0 lobbying-register returns** (as a State body it's exempt).
  Many entries are public sod-turnings, but several are substantive private meetings: *"Briefing:
  State Lands and LDA Report"* (Min. Browne, 2025-04-01), repeated private *"Meeting with John
  Coleman, LDA"* (CEO), and a Cabinet-Committee-on-Housing LDA meeting. **`company_influence` returns
  empty** — the LDA's land transfers and development costs are *not* in the procurement/payments
  corpus either. So it is **access-heavy and money-opaque**: huge ministerial contact, no register
  trail, no spend trail.
- **② Where the record stops:** subjects are logged but the briefing content, the State-lands list,
  and the valuations are nowhere public.
- **③ Already asked?** LDA FOIs exist and **release partially — 91 records, Part-Granted** → precedent
  the briefings can be obtained.
- **④ FOI (to Dept of Housing / Taoiseach's Dept):** *"The briefing note titled 'State Lands and LDA
  Report' provided to Minister Browne on/around 1 April 2025, the agenda and minutes of the private
  LDA CEO meetings logged in 2025, and the schedule of State lands transferred to or earmarked for the
  LDA with indicative valuations."*

### E3 — The data-centre access cluster during the grid crunch ★
- **The story (MCP-verified, `who_ministers_meet` topic="data centre"):** across 2024, then-Minister
  **Eamon Ryan (DECC/Transport)** held a dense run of data-centre meetings — **Amazon Web Services,
  Microsoft, Echelon Data Centres, Vantage Data Centres, Digital Infrastructure Ireland**, and the
  energy regulators **GNI / CRU / EirGrid** — during the contested period of data-centre grid-
  connection limits and energy-cap policy. The diary subjects are clear ("Meeting with Microsoft on
  Data Centres"); what's absent is *what was sought and conceded*.
- **② Where the record stops:** the diary names the counterparties and topic, but the submissions,
  briefing notes and any commitments are not public.
- **③ Already asked?** Energy/data-centre FOIs are common; check the DECC log for these specific
  meeting dates before filing.
- **④ FOI (to DECC):** *"All submissions, briefing notes and follow-up correspondence arising from the
  Minister's 2024 meetings with Amazon Web Services, Microsoft, Echelon, Vantage and Digital
  Infrastructure Ireland on data centres, and the records of the GNI/CRU/EirGrid meetings of
  March 2024 on data-centre grid connections."*

### E4 — IDA Ireland: 365 ministerial meetings, the FDI deals nobody sees ★★
- **The story (MCP-verified, `ministerial_diary_organisation`):** **IDA Ireland** is the
  **second-most-met organisation in the entire diary corpus — 365 meetings with 28 ministers** (2015–
  2026) — yet has **0 lobbying-register returns** (state body, exempt). The access-gap is systemic:
  IDA 365 + HSE 211 + Enterprise Ireland 180 + LDA 106 + HEA 76 + NTA 75 = **~1,013 ministerial
  meetings across six state bodies, none on the register.** Among IDA's are confidential, named
  client-company engagements — *"Engagement with IDA client company – Tokyo Electron"*, *"Meeting with
  IDA Client company Fullbright Medical"*, *"Meet with IBM (IDA South-East office)"*, *"Datavant – IDA
  client company jobs announcement"* — plus "Regional Property Programme" presentations.
- **② Where the record stops:** the IDA negotiates grants, property deals and incentive packages with
  multinationals; the diary names the company and date but the *terms* are confidential and absent from
  our procurement/payments corpus (`company_influence` empty).
- **③ Already asked?** Enterprise/IDA FOIs exist; check the DETE log for these named meetings — much
  IDA commercial detail is exempt (s.36 commercially sensitive), so expect partial release and frame
  accordingly.
- **The FOI value is NOT the grants.** The R&D/training grants on the EU State Aid register (IDA
  ≈€1.63bn, mostly cumulative multi-year R&D — Boston Scientific's "€66m" is 34 awards 2017–25, biggest
  single €9.4m) are *routine and already disclosed*. Target instead what the register can't show and
  the meetings are about: **property/site deals, bespoke location packages, tax inducements.**
- **④ FOI (to DETE / IDA):** *"For the IDA client-company engagements in ministers' 2025 diaries
  (incl. Tokyo Electron, Fullbright Medical) and the Regional Property Programme presentation of 26 Nov
  2025: the briefing notes and minutes; any records of IDA property, site, fit-out or lease/land
  support and other non-grant inducements offered to those companies; and the heads of terms of any
  investment-package commitments — to the extent not already on the EU State Aid register."*

> A further angle was tested and **dropped for lack of a verifiable hook**: regulated firms in repeat
> corporate distress (e.g. PFS Card Services, Independent Trustee Company) — `company_influence`
> returned empty, so no public-money link could be confirmed; left out rather than asserted.

---

## How a journalist would actually run this

1. Pick a lead (or let the app surface the highest-signal ones — single biggest single-line PO,
   collapsed-contractor exposure, null-supplier blocks, zoning developers).
2. The app shows the grain-correct figure + where our record stops + the **already-asked verdict**
   (virgin / refused-before / granted-before) from publicinformation.ie's log.
3. One click drafts the scoped FOI to the body's registered FOI email and launches it through the
   outreach engine; the classified reply (and any released figures) flow back.

**The differentiator, shown by the probes above:** the flow tells the journalist *before they spend a
request* that Roadbridge/OPW/HSE-buildings are un-asked (file now), NTMA refuses (narrow + appeal),
and Pfizer/zoning are already part-released (fetch first). No existing Irish tool does that.
