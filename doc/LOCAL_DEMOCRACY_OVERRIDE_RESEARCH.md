---
tier: CONTEXT
status: LIVE
domain: local-gov
updated: 2026-07-14
supersedes: []
read_when: when researching or fact-checking how councillors are overruled (OPR/Ministerial Directions, s.4 motions, budget-rejection dissolution) for a local-democracy piece
key: CONTEXT|LIVE|local-gov
---

# How Councillors Get Overruled — research findings for a local-democracy piece

> **TIER: CONTEXT — NOT FOR THE APP FRONT END.**
> This is the *library*: long-form, cited, auditable — go here to CHECK a claim. The narrative and
> historical material below (Mahon, s.4 motions, dissolutions, the Council of Europe critique) fails
> the promotion gate — it is not per-council, not refreshable, and partly evaluative. See
> [[feedback_promote_vs_context_gate]]. The durable, cross-referenceable claims — and the list of
> claims that are WRONG — are distilled in memory: `reference_local_government_domain`.
> Only the Schedule 14A register, the OPR Directions fact, and the per-council usage data are
> promotable; see [LOCAL_DEMOCRACY_FEATURE_DESIGN.md](LOCAL_DEMOCRACY_FEATURE_DESIGN.md) §6.

**Date:** 2026-07-14 · **Status:** RESEARCH + BUILD ASSESSMENT. Nothing built; no data ingested.
**Origin:** owner found that the Office of the Planning Regulator can effectively overrule councillors on rezoning, and asked what other override mechanisms exist.
**Method:** 4 parallel research strands (planning-plan overrides · planning bypass/appeal · money & governance · data availability), each source-cited; the two sharpest legal claims re-verified by me against the primary text (Law Reform Commission *Revised Acts*).

---

## 1. Bottom line

**The OPR/rezoning mechanism is real, is used constantly, and is only one rung on a ladder.** Councillors hold an *enumerated* list of reserved functions; by residual clause everything else belongs to the unelected Chief Executive (LGA 2001 s.149(4)). Of the few powers councillors do hold, the two biggest — **adopting the development plan** and **adopting the budget** — each have an override attached: the plan can be rewritten over their heads by Ministerial Direction on the Planning Regulator's recommendation, and refusing the budget can get the entire council **removed and replaced by a commissioner whom the council itself must pay for**. Both have happened; the planning one happens routinely. **A 2022 parliamentary answer records that of 22 development plans assessed to adoption, 14 — 64% — drew a formal override recommendation against the elected members.**

**But the honest complication, which the piece must carry:** the OPR exists *because of* the Mahon Tribunal into planning corruption, and much of what it strikes is councillors zoning land **in flood zones** (Meath, Sligo) or in scattered, un-serviced locations. This is not a clean "democracy stolen" story. It is a "democracy constrained — sometimes for very good reason" story. Our no-inference rule means we present the machinery and the counts and let the reader judge.

**Buildability: strong.** The spine (the OPR's own register of Directions) is public, fetchable, and countable per council at small-to-medium effort — and **no official running total of it exists**, so a per-council counter would be an original contribution rather than a re-publication.

---

## 2. The ladder of override (each rung verified)

### Rung 0 — the structural asymmetry that makes all the rest possible
**LGA 2001 s.149(4):** *"Every function of a local authority which is not a reserved function is … an executive function."* Councillors get a list; the Chief Executive gets **the residue**. This residual clause is the root of the whole story.

### Rung 1 — most planning was never theirs
Deciding an individual planning application is an **executive** function (the CE/planners), not a councillor vote. Councillors set the plan; they do not grant permissions.
- **The one reverse lever:** *material contravention*, **PDA 2000 s.34(6)** — where officials want to grant something that breaks the adopted plan, councillors must vote it through, and it takes **three-quarters of the *total* membership** (not of those present). Note it is a *pro-development* override, and the resulting permission is still appealable.

### Rung 2 — the OPR → Ministerial Direction chain (the mechanism the owner found)
Created by **Planning and Development (Amendment) Act 2018, Part 2**; operational **April 2019**; a direct recommendation of the **Mahon Tribunal**.

| Step | Section (PDA 2000 as amended) | What happens |
|---|---|---|
| 1 | s.31AM(1)–(7) | OPR evaluates the draft plan/variation and makes **recommendations** |
| 2 | — | **Members lawfully vote against them.** The flashpoint. |
| 3 | s.31AM | CE must notify the OPR in writing, with reasons, **within 5 working days** |
| 4 | **s.31AM(8)** | Where the plan is not consistent with the OPR's recommendations, the OPR **"shall issue … a notice to the Minister containing recommendations that the Minister … rectify the matter"** |
| 5 | s.31 | Minister issues a **draft Direction**; the affected parts of the plan **do not take effect** |
| 6 | s.31AN(4) | OPR recommends the final Direction |
| 7 | s.31(17) / s.31AN(11) | Final Direction has **immediate effect**, is **"deemed to be incorporated into the plan"**, and the councillors' offending provisions are **"deemed not to be included"** |

Two facts that make this vivid and are **verified**:
- At the draft-Direction consultation stage, **"the OPR only receives submissions from elected members"** — the public is not consulted on the reversal of a public vote.
- **There is no appeal.** The only remedy is judicial review, on process.
- In practice the Direction is signed by a **Minister of State**, not the Cabinet Minister (Burke, O'Donnell, Dillon, Cummins across the cases below).
- **Asymmetry (important):** this power is *restrictive only* — the Minister/OPR can strike a zoning, never create one.

**Real cases where councillors' votes were struck:**

| Plan | What members did | Outcome |
|---|---|---|
| **Meath CDP 2021–27** | Zoned land the SEA said to omit, and land **in flood-risk areas** | Final Direction, Feb 2022 |
| **Galway County CDP 2022–28** | Zoning extensions "uncoordinated and piecemeal" | Direction issued |
| **Fingal CDP 2023–29** | Employment/Food Park land outside settlement boundaries | Direction, 28 Jul 2023 — **St Margaret's, Coolquay, Courtlough un-zoned** |
| **Clare CDP 2023–29** | Members' rezonings at a special meeting | **20 parcels re-zoned by ministerial order; 15 knocked out of Residential** — Kilrush to unzoned *"white lands"*; Mullagh, Liscannor, Broadford back to *Agricultural* |
| **Sligo CDP 2024–30** | **40 amendments made contrary to OPR recommendations**, incl. zoning for vulnerable uses **in flood zones A/B** | Plan took effect *"except for those parts subject to a Draft Ministerial Direction"* |
| **Wicklow CDP Variation 2** | — | OPR proposed Direction, Aug 2025 — machinery still live |

**The countable headline (PQ, 25 Oct 2022):** since April 2019 the OPR assessed **22** city/county development plans to adoption; **14** drew a s.31AM(8) notice; **all 14** draft Directions issued; final Directions recommended on **9**, issued on **6** at that date. *(Rated HIGH-not-certain — corroborated twice via search, direct source fetch timed out. Verify before printing.)*

### Rung 3 — SPPRs: override without any Direction at all
Ministerial guidelines under **s.28** may contain **Specific Planning Policy Requirements (SPPRs)** with which planning authorities **"shall comply."** Where an SPPR conflicts with the development plan, **the SPPR wins and the plan must be amended**. The plan must also be consistent with the **Regional Spatial & Economic Strategy**, which must be consistent with the **National Planning Framework** — and inconsistency is itself a ground for a s.31 Direction. So central policy can bind straight through the councillors' plan without anyone issuing a Direction.

### Rung 4 — the full bypass: the council isn't even in the room
- **Strategic Housing Developments (2016 Act, ran 2017–2021):** schemes of **100+ homes** applied **directly to An Bord Pleanála**, bypassing the council entirely — no official decision, no councillor input, and **no right of appeal** (only judicial review). **401 applications.** ~**18%** were judicially reviewed and, of contested cases that concluded, the board **lost roughly 90%** — permissions quashed. Replaced Dec 2021 by **Large-scale Residential Developments**, restoring the first-instance decision to councils (to the *officials*, not the members).
- **Strategic Infrastructure (2006 Act):** motorways, major energy, large utilities go **straight to the board**.
- **Marine (MARA, 2021 Act):** offshore consenting sits with a central agency and the national board; councils are peripheral.

### Rung 5 — appeal: the national board can reverse the council
Any grant/refusal can be appealed and decided afresh. **⚠️ Note carefully: this overturns the Chief Executive's planners, *not* councillors** (see §4 — this is a trap).
**Naming update:** An Bord Pleanála was **renamed An Coimisiún Pleanála**, effective **18 June 2025** (Planning and Development Act 2024, Part 17). **Our live UI still says "An Bord Pleanála" — that's a factual refresh we owe regardless of this piece.**

### Rung 6 — the nuclear option: refuse the budget, lose the council
The budget is the councillors' single biggest reserved power (**LGA 2001 ss.102–103**: members *"shall by resolution adopt"* it). Refusing to is caught by **Part 21, "Consequential Provisions on Failure to Perform Functions."**

- **s.216(1)(e)** — the Minister **may remove the members from office** where the authority *"refuses or wilfully neglects to comply with any … express requirement imposed by enactment."*
- **s.218** — the Minister **"shall … appoint"** a **commissioner**. And **s.218(4)**: the commissioner's pay *"shall be paid out of revenues of the relevant local authority."* **The council pays for its own replacement.**
- **s.219** — during the removal period the commissioner exercises **every reserved function** — the entire democratic layer is absorbed into one appointee.

**It is not theoretical:**

| Year | Council | Trigger | Who took over |
|---|---|---|---|
| 1924 | **Cork Corporation** | Dissolved after inquiry | **Philip Monahan**, commissioner (to 1929) |
| 1924–30 | **Dublin Corporation** | Dissolved | Three commissioners |
| **1969–74** | **Dublin Corporation** | ⭐ **Struck a rate 5/3d below the manager's figure.** Minister **Kevin Boland** removed the members | **John Garvin — a former Secretary of the Department of Local Government — ran Dublin for five years.** Council restored at the 1974 elections |
| **1985–88** | **Naas UDC** | ⭐ **Refused to pass a budget** — the last council whose members were suspended for it | ~3 years |
| Nov 2019 | **Dublin City Council** | Budget standoff | *Near-miss* — replacement of all 63 members by a commissioner was genuinely on the table |

### Rung 7 — the councillors' one real counter-power, progressively de-fanged
**LGA 2001 s.140** lets members, by resolution, **require the CE to do a specific thing** — and s.140(9) says the CE *"shall cause [it] … to be implemented."* A genuine command, not a request (needs 2 signatures; one-third of total membership in favour).

But **s.140(10)** excludes:
- **(e) planning** — *inserted by the Local Government Reform Act 2014, s.52(d), commenced **1 June 2014***;
- **(f) any benefit "to any named person or group"** — an explicit anti-clientelism bar.

**The backstory is the piece's spine in miniature:** s.140 is the successor to the notorious **"section 4 motion"** (1955 Act), by which councillors directed managers **to grant planning permissions** — the very instrument the **Mahon Tribunal** found at the heart of planning corruption. It was stripped of planning in 2014. *The one real power councillors held over officials was removed in precisely the area where it had been abused.*

### Rung 8 — the watchdogs report; they do not bite
The **Local Government Audit Service** auditor issues an opinion and report; the council merely **"considers"** it (ss.120–121). **NOAC** scrutinises, monitors, evaluates and **recommends** — it has **no power to direct**. *The money watchdogs bark, they don't bite.* The only genuine financial override is the ministerial removal power at Rung 6.

---

## 3. ⚠️ THE LAW CHANGED UNDER US — 31 December 2025

**Do not write this piece citing only the 2000 Act.** The **Planning and Development Act 2024** development-plan chapters commenced **31 Dec 2025** (S.I. 633/2025). Changes that matter here:

- **Development plans now run 10 years, not 6** — *councillors get a plan-making vote half as often.* Arguably the single biggest quiet reduction in local democratic input in the whole reform.
- **s.28 guidelines → National Planning Statements** (binding *National Planning Policies and Measures* + guidance).
- **New override chain (ss.63–65):** grounds now include being **"materially inconsistent with"** the National Planning Framework or National Planning Policies and Measures. **s.64: the Minister directs the OPR, and the OPR itself issues the draft Direction** (within 10 working days) → consultation → OPR recommends → **Minister issues the final Direction** (s.65). *(I verified this personally: an earlier research pass characterised it as a wholesale transfer of power to the unelected regulator — that is an overstatement. The Minister still triggers the process and still signs the final Direction.)* If the Minister **declines**, reasons must be **laid before both Houses** — a new transparency hook worth watching.
- **⭐ s.67 — "Urgent direction requiring chief executive to vary development plan."** **Verified by me against the primary text.** The Minister may direct a planning authority *"requiring the **chief executive** … to vary the development plan,"* and the varied plan *"shall take effect on the making of the variation … **by the chief executive**."* **Elected members are not mentioned in the section at all** — no resolution, no vote, no consultation.
  **Honest framing (required):** this is gated on *"an event or situation with significant national, regional or strategic implications"* plus urgency. It is an **emergency power, not the routine route.** The accurate sentence is: *"In an urgency scenario, the Minister can bypass the elected members entirely and have the unelected Chief Executive rewrite the development plan."* It is **not** "the Minister can do this at will."

**Open legal question — flag for a solicitor:** the LRC revised acts still show PDA 2000 s.31/s.31AM as in force with no repeal annotation despite the 2024 Act's commencement. Likely a consolidation lag or transitional preservation. Doesn't affect the historical cases (all ran under the 2000 Act), but don't assert which Act is operative *today* without checking.

---

## 4. 🚨 The never-blend rule (a correctness trap, same class as the three money grains)

There are **three structurally different override relationships** and merging them into one "overruled" number would be factually wrong:

| # | Who is overruled | By whom | Do we have data? |
|---|---|---|---|
| 1 | The council's **planners** (the Chief Executive's executive decision) | An Bord Pleanála / An Coimisiún Pleanála, on appeal | ✅ **Already live** — 16,064 appeals, 31 councils, 2016–2026, **27.2% overturned** |
| 2 | The **elected councillors'** plan/zoning votes (a reserved function) | OPR → Minister, by s.31 Direction | ❌ **Not ingested — this is the piece's spine** |
| 3 | The council **entirely** (bypassed at first instance) | SHD / SID / MARA → straight to the board | ⚠️ Partially inside our appeals data; a clean dataset exists |

**Our existing planning-overturn metric is #1.** Its own view header already says so: *"planning permission is an EXECUTIVE function … an accountability signal for the Chief Executive's office — **not the elected councillors**."* Presenting it under a "councillors overruled" banner would be a factual error. This must be a hard rail if we build.

---

## 5. What we already hold that feeds this

- **An Bord Pleanála appeal outcomes** — 16,064 clear-vs-clear appeals, **all 31 councils**, 2016–2026, **27.2% overturned** (council-grant→refused 1,973; council-refuse→granted 2,391). Source: An Coimisiún Pleanála ArcGIS, **CC-BY-4.0, weekly**. *(Measures #1 above.)*
- **Councillor roll-call votes** — 4,958 named votes across 5 councils (Carlow, Cork City, Kilkenny, Laois, Fingal), and **the motions already contain the override material verbatim** — e.g. Carlow members voting on *"rezoning 0.66 ha from Open Space and Amenity to New Residential."* For those five councils we can show **the actual vote a Direction might later strike.** That's the emotional core of the piece and we already own it.
- **Reserved-vs-executive framing** — already built and live on "Who Runs Your County" (the CE dossier leads with *"appointed by PAS — not elected"*).
- **Meeting agendas** (221, all 31 councils) — but grep finds *material contravention* in only **3/221** and *s.140* in **0/221**. Those overrides are buried in full minutes, not agendas.
- **Nothing at all** on OPR, Ministerial Directions, development plans, or zoning. Confirmed gap.

## 6. What's newly available

| Source | Where | Machine-readable | Countable per council | Effort |
|---|---|---|---|---|
| **⭐ OPR register of Directions** — the actual record of councillors being overruled | [opr.ie/recommendations-made-by-the-opr-to-the-minister](https://www.opr.ie/recommendations-made-by-the-opr-to-the-minister/) — **31 named plans** | HTML index → PDF doc-chain | **YES** | **S–M** |
| OPR submissions/recommendations per council | opr.ie (by-LA index, 2019–2026) | HTML index → PDFs | Yes (counts) | M |
| **SHD bypass dataset** (2017–21) | data.gov.ie / GeoHive, ArcGIS | **YES** (CSV/GeoJSON/REST) | **YES** | **S** |
| Final Directions | gov.ie ("Section 31 Ministerial Direction issued to *[Council]*") | PDF | Yes | S |
| SHD judicial-review outcomes | — | **NO** | **NO** | narrative only |
| Council dissolutions / commissioners | statute + press + archives | **NO** | hand-curated CSV | S |

**Two facts make the OPR register the obvious spine:** it is on **opr.ie, which has no WAF problem** (gov.ie 403s us), and **no official running total of s.31 Directions exists anywhere** — the last authoritative count is the 2022 PQ. A maintained per-council counter would be genuinely new.

---

## 7. Recommendation

**Build it — the piece is real, the spine is cheap, and it lands squarely in the civic mission.** Suggested shape, in dependency order:

1. **Phase 1 (S–M) — the spine.** Scrape the OPR register → a small `la_plan_directions` fact: `(council, plan, stage, date, outcome, source_url)`, where stage ∈ {OPR notice, draft Direction, OPR proposed final, **final Direction issued**, *Minister declined*}. That last value matters — Sligo shows the Minister sometimes **doesn't** follow the OPR, and a counter that only counts overrides would misrepresent the process. Surface as *"Times this council's development plan was overruled"* on the council dossier.
2. **Phase 2 (S) — the bypass count.** SHD applications per council from data.gov.ie: *"N large housing schemes decided over your council's head, 2017–21."*
3. **Phase 3 (narrative) — the explainer page.** "The ladder of override": the eight rungs above, with statutory basis, the historical commissioner cases (hand-curated CSV, ~6 rows), and the s.140/section-4/Mahon backstory. This is where the *un*-countable material (dissolutions, JR outcomes) lives honestly, as explainer rather than fake metrics.
4. **Tie-in we already own:** for the five roll-call councils, link the **actual councillor votes on rezoning** to the plan they belong to.

**Honesty rails (non-negotiable):**
- **Never blend the three override types** (§4). Separate views, separate counts, separate copy.
- **Carry the counter-case.** Meath and Sligo were overruled for zoning **flood-prone land**. The piece must state what the regulator's stated reason was, in the regulator's words, and must not imply the override was illegitimate. No "democracy stolen" framing — present the machinery, present the counts, let the reader judge. (This is the existing no-inference rule doing exactly its job.)
- **The OPR can only strike, never create** — say so, or readers will assume a general power.
- **s.67 is an emergency power** — do not present it as the routine route.
- Update **"An Bord Pleanála" → "An Coimisiún Pleanála"** in live copy (renamed 18 Jun 2025) regardless.

**Also fix, unrelated to the piece:** `doc/LOCAL_AUTHORITY_ACCOUNTABILITY.md` still says Cork County is missing from the appeals data — it's **31/31** now.

---

## 8. Confidence ledger

**Verified from primary source (safe to print):** LGA 2001 ss.102, 103, 140, 149, 216–220 (text quoted); PDA 2000 ss.31, 31AM(8), 31AN, 34(6); PDA 2024 ss.63–65, **67** (I re-read s.64 and s.67 myself); OPR establishment/date/Mahon origin; the s.140(10)(e) planning exclusion dated **1 June 2014** (LGRA 2014 s.52(d)); ACP rename 18 Jun 2025; SHD dates/401 applications; plan cycle 6→10 years.

**High but verify before printing:** the **22 / 14 / 6** PQ figures (Oct 2022 — direct fetch timed out); the SHD **~90% quash** rate (press-reported, 2021–22 vintage, and it measures *loss-rate once challenged*, which is **not** the same as the ~18% *incidence* of JR — the "one in four" political claim conflates them); the Clare Direction date (month-level).

**Corrected during this research:** the claim that PDA 2024 s.64 hands draft-Direction power *from* the Minister *to* the OPR — **overstated**; the Minister still directs the OPR to issue it and still signs the final Direction.

**Do not print:** a cross-reference to "s.10A" in s.216(1)(c)–(d) (could not identify the parent Act); a section number for the auditor's *surcharge* power (not in LGA 2001 Part 12 — survives from older legislation, source unconfirmed); the claim that Dublin was dissolved in "1959 and 1973" (a garbled secondary quote — the real episode is **1969–74**); pre-2001 removal section numbers (cite those cases by fact, not section).
