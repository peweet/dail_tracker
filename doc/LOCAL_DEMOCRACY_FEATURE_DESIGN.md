---
tier: SPEC
status: LIVE
domain: local-gov
updated: 2026-07-14
supersedes: []
read_when: before building or refining the "Who decides in your county?" feature, or deciding which local-democracy claims are promotable to the app front end
key: SPEC|LIVE|local-gov
---

# "Who decides in your county?" — feature design brief

> **TIER: SPEC — drives the app, but only the gate-passing subset.**
> Sections §4a (Schedule 14A register), §4e (OPR Directions) and §5 (per-council usage) are
> **promotable**. §3 (Council of Europe / Moorhead), §4d and §4f (the history, the receipts) and the
> *"hostage arrangement"* thesis are **CONTEXT — hold back from the front end** (owner steer,
> 2026-07-14): they are national, static, narrative or our own framing. See
> [[feedback_promote_vs_context_gate]] for the 3-question gate and
> `reference_local_government_domain` (memory) for the distilled engine.

**Date:** 2026-07-14 · **Status:** DESIGN — not built. Decision-ready; needs owner sign-off on the open questions in §7.
**Companion:** [LOCAL_DEMOCRACY_OVERRIDE_RESEARCH.md](LOCAL_DEMOCRACY_OVERRIDE_RESEARCH.md) (the statutory override chain, verified).
**Owner steer that shaped this:** *"we have it slanted to OPR too much… the tension between the councillors and executive is interesting and the government above them"* and *"stick to what published government documents already say."* Both applied. The OPR is now one item in §4, not the thesis. Every number below comes from a published government or intergovernmental document — nothing is derived by us.

---

## 1. What the feature is

**A three-cornered power map of every county: the elected councillors, the unelected Chief Executive, and the government above.** It answers four questions a citizen — or a councillor — cannot currently get answered anywhere:

1. **How much power does my council have at all?** (spoiler: less than almost anywhere in Europe, and that is an *official finding*, not our opinion)
2. **What can my councillors actually decide?** (a statutory list nobody has ever put in front of them)
3. **What do they *not* decide?** (nearly everything — and the law says so in one line)
4. **What happens when they try?** (the executive can refuse them; the Minister can overrule them; refusing the budget can get them abolished)

**The thing we are NOT building:** an "overrides counter" or an autonomy score. No derived metrics. (An earlier funding-dependency ratio was explored and **dropped** — it was our invention and it was confounded by grant-funded housing pass-throughs.)

---

## 2. The spine: published documents only

| Source | What it gives us | Type |
|---|---|---|
| **Council of Europe, Congress of Local and Regional Authorities — *Monitoring of the European Charter of Local Self-Government in Ireland*, CG(2023)45-17**, adopted 25 Oct 2023 | The headline facts. Official, intergovernmental, damning. | Published report |
| **Local Government Act 2001** (as amended), esp. **ss.130, 131–131B, 136, 140, 145–147, 149, 183, 216–220 + Schedule 14A** | The powers, the thresholds, the residual clause, the trapdoor | Statute |
| **Local Government Reform Act 2014**, esp. **Sch. 3** (inserts Schedule 14A) and **s.52(d)** | The reserved-functions list; the s.140 planning exclusion | Statute |
| **"Putting People First" — Action Programme for Effective Local Government (2012)** | The *reform's own words* on why councillors lost the planning power | Gov. policy doc |
| **Moorhead Report** — Review of the Role and Remuneration of Local Authority Elected Members (2020) | The uncomfortable truth about how councillors use what they have | Gov.-commissioned |
| **AILG Elected Members' Information Leaflets 2/3/5 + Guidance Manual** | How councillors are *actually told* their own powers | Representative body |
| **OPR register of Directions** (opr.ie) | The plan/zoning overrides — **already built**, 33 plans / 20 councils | Regulator's register |
| **Our own data** | Which powers each council actually *used* | Dáil Tracker |

---

## 3. The four headline facts (all quotable, all sourced)

> **1. Irish local government is 8.0% of total public expenditure. The EU average is 23.3%.**
> *"…among the smallest within the European Union and far below the European Union average of 23.3%."* — Congress, **para 32**
> ⚠️ Honesty note: this is a share of **public expenditure**, *not* GDP — so the usual "Irish GDP is distorted by multinationals" objection **does not apply**. Safe to use.

> **2. On the Local Autonomy Index, Ireland scores 42 against an average of 57 — "a rank only just above Hungary and the Republic of Moldova"… and "was not affected by the reforms in 2014."** — Congress, **para 38**

> **3. The law gives councillors a list; it gives the Chief Executive everything else.**
> **LGA 2001 s.149(4):** *"Every function of a local authority which is not a reserved function is … an executive function."*
> Ireland was found **non-compliant with Article 6.1** of the Charter because *"the council has no influence over the administrative structure of the local authority, as this is entirely an executive function."* — Congress, **para 86**

> **4. And yet — only 5% of councillors said their statutory/governance functions were a priority; 51% put individual representation first.** — **Moorhead Report**

**Fact 4 is the piece's integrity.** Without it we are running a grievance narrative. With it, the story is genuinely tragic and genuinely fair: *councillors have been stripped of power — and they have largely stopped reaching for the power they still hold.*

---

## 4. The model — three actors, and what actually passes between them

### 4a. What councillors CAN decide — and the fine print that guts half of it

The statutory list is **Schedule 14A** (inserted by LGRA 2014 Sch. 3): **182 reserved functions**, each with its conferring provision, split exactly as the law splits them —

- **Part 1 — 45 functions:** performed by **municipal district members**
- **Part 2 — 24 functions:** may be performed by MD members **or** the full council
- **Part 3 — 113 functions:** performed by the **full local authority**

⚠️ **Do not claim this is the complete universe.** Reserved functions also sit in ss.131/131A/131B and are scattered across scores of other Acts. AILG's own count is *"more than 180."* Present Schedule 14A as **the statutory schedule**, not as "every power that exists."

**The insight that makes this more than a list — a reserved function is not always a power.** Three genuinely different things are lumped together, and the difference is *in the statute*, not in our opinion:

| Character | Example | Why (statutory) |
|---|---|---|
| **REAL POWER** | Adopt the development plan; set the LPT factor (±15%, →+25%/−15% from 2027); set the commercial rate (ARV); adopt the budget | Members decide; nothing happens without their vote |
| **VETO ONLY — silence = consent** | **s.183 land disposal**; **Part 8** (the council's own development) | The disposal/development **proceeds by default** unless members resolve otherwise. Doing nothing is a yes. |
| **RUBBER-STAMP** | Corporate Plan (**s.134**: the CE *"shall prepare"* it); Annual Report (**s.221**) | Members approve a document the executive wrote |

That taxonomy is the most valuable thing in the feature and it is **sourced, not inferred**.

### 4b. What they CANNOT decide
Every individual planning permission · all staff and HR (**s.159**) · all procurement and contracts · every individual housing allocation · enforcement · day-to-day spending. By operation of **s.149(4)**.

### 4c. The levers — the horizontal tension, with receipts

**Chief Executive → councillors**
- **Writes the budget they "adopt"** (s.102(3) — prepared *"under the direction of the chief executive"*); must only *"take account of"* the district budgetary plans.
- **Writes the report on the plan and on every public submission** — members may depart from it, but only against the expert record.
- **s.183 and Part 8 default to "yes"** — he needs members to do *nothing*.
- **Can simply refuse them.** ⭐ **Louth County Council, Nov 2007: 24 councillors voted for two s.140 motions to permit houses at Killineer. Manager Conn Murray refused, ruling them *ultra vires*.** Even before the 2014 reform, the executive could say no.
- **Outlasts them** — 7 years (extendable to 10) against a Cathaoirleach's 1.

**Councillors → Chief Executive**
- **s.140** — require him to do a specific act (notice signed by 2 members; **⅓ of total membership** to carry). **But s.140(10)(e), inserted by LGRA 2014 s.52(d) with effect from 1 June 2014, excludes ALL planning functions** — which is the only thing it was ever really used for.
- **s.136 monthly management report** — the 2014 quid pro quo. **The bargain was: you lose the power to command; you gain the right to be told.**
- **Remove him — s.146:** notice signed by ⅓ of members; **¾ of the TOTAL membership** must vote in favour; **and the Minister must sanction it**.
  🔴 **UNRESOLVED — do not publish either way:** we could not establish whether s.146 has *ever* been used. Do **not** print "it has never happened."
  ⚠️ **Two errors in circulation — do not repeat:** that CE removal needs *two-thirds* (**it is three-quarters + Ministerial sanction** — the Council of Europe report itself gets this wrong at para 28), and that PDA 2024 cut material contravention to two-thirds (**it remains three-quarters of total membership**, PDA 2024 s.99).

### 4d. The trapdoor — the budget
The budget is their biggest formal power, and **exercising it against the executive can get them abolished**: failure to adopt → **Part 21 (ss.216–220)** → the Minister may remove the members and appoint a **commissioner**, whose salary the council pays.
⭐ **Cork City Council, Nov 2014** — councillors failed to adopt the budget; abolition and a commissioner were openly on the table; they passed it **17–12** on the second attempt, one member urging colleagues back *"from the brink."*

> **A power you cannot exercise without destroying yourself is not a power. It is a hostage arrangement.** That sentence is the thesis of the piece.

### 4e. The government above
The OPR/Ministerial Direction chain (**built** — 33 plans, 20 councils, incl. the 2 cases where the **Minister declined** to overrule). Plus the continuing erosion, all documented: development plans extended **6 → 10 years** (a councillor can now serve a full term and never vote on one); the **Land Development Agency** carve-out from s.183 (council land transferring without members' approval — *secondary sources only, verify before use*).

### 4f. The honest history — why this happened
Not "power was stolen." **Power was forfeited.** The **s.4 motion** (1955 Act) let councillors order the manager to grant planning permission. The **Mahon Tribunal** found a generation of them had sold the zoning of Dublin. The 2014 reform removed the planning power — and *Putting People First* says so in the government's own words. The OPR itself exists because of Mahon. **The regulator that overrules councillors on rezoning was created because councillors were corrupt about rezoning.** Both halves of that sentence must survive into the copy.

---

## 5. What we already hold that makes this per-council and real

The register isn't an explainer if we can show **which powers each council actually used**:

| Reserved function | Do we have the record? |
|---|---|
| Set the LPT factor (±15%) | ✅ **All 31 councils × 2023–2026** — a real power, exercised annually (`v_la_lpt_adjustment`) |
| Adopt the budget | ✅ **All 31 × 2019–2026** (`la_budget_divisions`) |
| Adopt the development plan | ✅ …and **when it was overruled** (`v_la_plan_directions` — 20 councils) |
| Dispose of land (s.183) | ⚠️ **59 of 221 agendas** — they *do* use this veto |
| Part 8 (own development) | ⚠️ **33 of 221 agendas** |
| Material contravention (¾) | ⚠️ **3 of 221 agendas** — nearly never |
| **s.140 direction to the CE** | 🔴 **0 of 221 agendas — the dead power** |
| Named votes on rezoning | ✅ 5 councils (Carlow's *"rezoning 0.66 ha from Open Space and Amenity to New Residential"*) |

**"s.140: 0 of 221" is the single most eloquent number in the whole feature.** Coverage caveat required: *"in the meetings we have processed"* — not "never happened."

---

## 6. Proposed information architecture

Two artifacts. **They must not be merged** — one is national and editorial, one is per-council and factual.

### A. A national explainer — *"How much power does your council actually have?"*
One page, identical for everyone. Carries the Council of Europe findings, the three-actor model, the s.4→Mahon→2014→OPR history, and the *"hostage arrangement"* thesis. This is where the **un-countable** material lives honestly (Cork City 2014, Louth 2007, the commissioner history) — as sourced narrative, not fake metrics.

### B. A per-council **Powers Register** — *"What your councillors can decide"*
Slots into the existing **Your Council** page, after the power statement. For each reserved function:

> **the function** (statute's own words) · **its provision** · **who exercises it** (your municipal district / the full council) · **its real character** (power / veto / rubber-stamp) · **can it be overridden, and by whom** · **has this council used it** (from our data, honestly caveated)

Grouped by theme (planning · money · governance · services · bye-laws), with the ~12 that actually matter surfaced and the long tail behind a disclosure. **This is the artifact that serves councillors themselves** — the answer to *"councillors are not aware of all their rules and prerogatives."*

The already-built **OPR Directions card** then sits underneath it as §4e — the consequence, not the headline.

---

## 7. Open questions for the owner

1. **Register scope:** all 182 Schedule 14A functions browsable, or a curated ~12 "powers that matter" with the full list behind a disclosure? *(Recommendation: curated front, full list behind — most of the 182 are obscure bye-law powers and the signal-to-noise is real.)*
2. **The character taxonomy (power / veto / rubber-stamp):** it is statutorily grounded but it *is* a classification we apply. Does it clear the no-inference bar? *(My read: yes — "the disposal proceeds unless members resolve otherwise" is the statute talking, not us. But it is the one judgement call in the design and it's yours.)*
3. **Where does the national explainer live** — a new page under Your Area, or a section of the existing hub?
4. **Currency:** Schedule 14A is the 2014 list and the **PDA 2024 has since moved things** (10-year plans, new Direction chain, s.99). Ship with an "as enacted, amendments noted" caveat, or do an amendment pass first?
5. **The 8% / 23.3% stat** is the strongest thing we have. Comfortable leading the national explainer with a Council of Europe finding?

## 8. Do NOT
- Merge the three override relationships into one number (regulator↔councillors ≠ board↔planners ≠ bypass). Never-blend, same as the money grains.
- Print "no CE has ever been removed" (unverified), "two-thirds" for CE removal or material contravention (both wrong), or the LDA s.183 carve-out without reading the LDA Act itself.
- Present the OPR as a villain. It exists because of Mahon, and much of what it strikes is zoning in flood zones.
- Revive the funding-dependency ratio.
