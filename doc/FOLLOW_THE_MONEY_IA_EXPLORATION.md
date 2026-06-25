# Follow the Money — information-architecture exploration

Status: **EXPLORATION ONLY** (2026-06-21). No reorg done. No code changed beyond a
provisional search box on the landing (which this doc may recommend repurposing or removing).

## Why this doc exists

The trigger was a reader question on
`/follow-the-money?paid_supplier=JOHN+SISK+SON&paid_tier=SPENT`:

> "Is this not a duplication of suppliers? Or is it materially different? Why don't we just
> bundle it into supplier and keep Follow the Money for projects?"

That instinct is half right and half blocked by the data. This doc walks through it carefully
because **Follow the Money is the most plausibly monetisable surface in the app** — the
journalist / competitor-intel / researcher "where did the money go" question is the one people
pay for (see `doc/commercial_*`, memory `project_commercial_uplift_2026_06_11`). Getting its
shape right is worth more than getting it shipped fast.

---

## Part 1 — The data foundation (what is, and is not, in the corpus)

Everything below is constrained by what the published data actually carries. The honest map:

### What exists as a first-class entity
| Entity | Source | Keyed by | Notes |
|---|---|---|---|
| **Public body** (payer) | each body's >€20k PO/payment lists | publisher name | ~real entity; the *who spends* |
| **Supplier / company** (payee) | same lists, CRO-matched | `supplier_normalised` | the *who gets paid* |
| **Line item** (ledger) | same lists | `supplier × body` edge | the terminus; PO number, amount, body's own description |
| **Award** | eTenders / TED | resourceId / notice | a *ceiling at point of award* — different grain, never summed with paid |
| **Corporate group** | curated `data/_meta/supplier_groups.csv` | `group_slug` | hand-curated rollup (BAM); the only genuinely modelled consolidation |

### What does NOT exist — the two walls
1. **No sub-contractor tier.** Public records stop at the *direct* contractor. What BAM pays
   its own subcontractors is published nowhere. The ledger is the floor of the graph.
   (Confirmed: `payments_fact` has no sub-tier column; zero description rows say "subcontract".)
2. **No project grain.** There is **no project ID, scheme code, or programme key** on any
   payment or award row. The data is `(body, supplier, line)`. The children's hospital only
   "works" as a project because **NPHDB is a single-purpose body** — the body *is* the project.
   BAM is a corporate *group*, not a project.

### The one lead that could make "projects" real (not yet ingested)
**Project Ireland 2040** (🔬 scoped, not built — `doc/IDEAS.md` L121, `doc/new_sources_...` L53):
- **1,936 capital projects** via ArcGIS: name, delivery body, county, sector, lifecycle stage
  (Strategic Assessment → Planning → Implementation → Complete), year.
- **Cost is band-only and sparse (~18% joinable)** — it is *not* a euro feed. It cannot tell
  you "€X spent on MetroLink."
- Its value is as a **navigation spine**, not a money fact: a named project + its delivery
  body + lifecycle stage, onto which the *existing* award/payment data can be *attached by
  delivery body + name matching* (fuzzy). The sketched lifecycle (`doc/IDEAS.md` L172):
  `PI2040 (project+stage) → CPO (land) → board minutes (approval) → awards [existing] →
  payments [existing] → C&AG (overruns)` — *plan → land → approve → award → pay → audit* for
  one scheme. This is described in IDEAS as "the most novel" page in the whole backlog.

**Implication for the user's idea:** "keep Follow the Money for projects" is only literally
buildable *after* a PI2040 ingest, and even then the money attached to a project is a fuzzy
join, not a clean total. Without it, "projects" can only mean curated examples (NPHDB, BAM) or
a relabelled body-first view.

---

## Part 2 — The confusion is real: three "supplier" surfaces

There are currently **three** places a reader meets a supplier, and they overlap:

| Surface | Route | What it is | Supplier view depth |
|---|---|---|---|
| **Companies** | `/company?supplier=` | Entity-first flagship: one firm's *full* footprint — awards (eTenders) + TED + paid + lobbying overlap, three registers side by side | **Richest** |
| **Procurement → Suppliers tab** | `/rankings-procurement` | A ranked, searchable leaderboard of paid suppliers | Drill-in → supplier profile |
| **Follow the Money → supplier node** | `/follow-the-money?paid_supplier=` | A waypoint in the trail | **Payments-only — a strict subset of Companies** |

### The decisive finding
Follow the Money's supplier node calls `_render_payments_supplier_profile` — the **payments
slice only**. The Companies page shows that **plus** awards, TED and lobbying overlap. So *as a
destination*, the Follow-the-Money supplier node is a **thinner copy of a page that already
exists**. The reader's "isn't this a duplication" is correct: for a *firm*, Companies is the
better and richer home.

### What Follow the Money uniquely offers (its true reason to exist)
Strip the supplier overlap away and what remains is genuinely distinct from Companies:
1. **The body node** — "who does *this public body* pay?" Companies has no concept of bodies.
2. **The walk + bounded breadcrumb** — `body → supplier → ledger → back`, keeping your place
   (the `_money_flow` trail rail; the explicit anti-feature is the endless breadcrumb).
3. **Corporate-group rollups** — BAM's ~18 legal entities (incl. PPP SPVs with no CRO) as one
   node. The only genuinely new modelling in the whole feature.

In one line: **Companies is firm-first depth; Follow the Money is money-flow navigation.**

---

## Part 3 — Reorg options (the user's first question)

Four options, lightest to heaviest. Each rated on **effort**, **clarity gain**, and
**commercial impact** (does it strengthen the part people would pay for?).

### Option A — Reframe Follow the Money as body / money-flow first
- **Change:** Follow the Money becomes body- & programme-first. Its supplier *node* stops
  showing the thin payments-only card and **links out to the full Companies dossier** (one
  source of truth). The landing search becomes a **body search** (supplier search already
  lives on Companies). Keep the trail + group rollups.
- **Pros:** Removes the duplication with one rule ("firm = Companies; flow = Follow the
  Money"). No data work. Each page gets a clean, non-overlapping job.
- **Cons:** Loses the ability to *stay in the trail* on a supplier node (you hand off to
  Companies, which is not part of the breadcrumb). Mitigated by Companies linking back.
- **Effort:** Low. **Clarity:** High. **Commercial:** Medium-high (sharpens the flow tool).

### Option B — Option A + retire the Procurement → Suppliers tab
- **Change:** As A, plus make Procurement's Suppliers tab defer to Companies (drop the
  standalone supplier leaderboard, or turn its cards into hand-offs). Result: **exactly one
  supplier surface** (Companies).
- **Pros:** Maximum clarity — there is one and only one place to meet a firm.
- **Cons:** Procurement loses an in-context "who gets paid here" leaderboard; some users
  expect suppliers *within* the procurement section. Need a cross-link so it's not a dead end.
- **Effort:** Low-medium. **Clarity:** Highest. **Commercial:** Medium-high.

### Option C — Merge Follow the Money into Companies / Procurement (delete the page)
- **Change:** Remove Follow the Money as a top-level page. Fold the body-walk into Public
  Payments / Procurement; supplier browsing into Companies.
- **Pros:** Fewest pages.
- **Cons:** **Loses the standalone "follow the money" entry point and brand** — which is the
  exact phrase that signals the monetisable promise. The trail becomes a buried sub-feature.
  Bodies still need a home and don't cleanly have one.
- **Effort:** Medium. **Clarity:** Medium (fewer pages ≠ clearer if the flow tool is buried).
  **Commercial:** **Negative** — buries the headline feature.

### Option D — Keep all three, just relabel / cross-link for clarity
- **Change:** No features move. Tighten nav titles and add explicit cross-links ("For one
  company's full record, see Companies"; "To follow the flow, see Follow the Money").
- **Pros:** Smallest effort; reversible; lets you watch usage before committing.
- **Cons:** The thin-supplier-subset duplication remains; the confusion is *labelled*, not
  *removed*.
- **Effort:** Tiny. **Clarity:** Low-medium. **Commercial:** Neutral.

### Reorg summary
| | Effort | Clarity | Commercial | Reversible |
|---|---|---|---|---|
| A · body/flow-first | Low | High | Med-high | Yes |
| B · A + retire Suppliers tab | Low-med | Highest | Med-high | Mostly |
| C · merge away the page | Med | Med | **Negative** | Hard |
| D · relabel only | Tiny | Low-med | Neutral | Yes |

---

## Part 4 — What "projects" can mean (the user's second question, walked carefully)

This is the crux for monetisation, so each option gets the full treatment: what's buildable,
a concrete example, the limits, and the willingness-to-pay it unlocks.

### Framing 1 — Curated programmes + corporate groups ("projects" = featured examples)
- **What it is:** Lean into the featured tiles already on the landing. Hand-curate a small set
  of single-purpose bodies (NPHDB = National Children's Hospital) and corporate groups (BAM),
  each a one-click starting node into the trail.
- **Buildable now?** **Yes — it already half-exists** (`_FEATURED`, `_FEATURED_GROUPS`).
  Extending it is just curation + the existing `supplier_groups.csv` pattern.
- **Concrete example:** "National Children's Hospital" tile → NPHDB body node → BAM group node
  → ledger showing the €130.7m is overwhelmingly *dispute settlements*, not base build.
- **Limits:** It is a **curated list, not an auto-generated register.** It looks like a
  "projects" page but only covers what you hand-pick. Honest, but doesn't scale to "all
  projects."
- **WTP unlocked:** Medium. Great for *storytelling* and demos; thin as a *data product*
  (a buyer can't query "all my sector's projects").

### Framing 2 — Body-first (drop the word "projects")
- **What it is:** Don't call it projects. Frame the page as **"follow money from a public body
  outward"**: pick a body → see who it pays → drill to records. Bodies are real entities; the
  page tells the truth about what the data is.
- **Buildable now?** **Yes — this is essentially the current page, honestly labelled.**
- **Concrete example:** Search "Transport Infrastructure Ireland" → ranked suppliers it pays →
  ledger per supplier.
- **Limits:** "Body" is less emotionally resonant than "project" — a reader cares about *the
  MetroLink*, not *TII*. You're giving them the payer, and asking them to infer the project.
- **WTP unlocked:** Medium. Solid, defensible, but doesn't differentiate from a generic
  "public body payments" view.

### Framing 3 — Project Ireland 2040 spine (the real "projects" view — requires ingest)
- **What it is:** Ingest the PI2040 1,936-project ArcGIS layer as a **navigation spine**.
  Follow the Money gains a **project node**: pick a named capital project → its delivery body
  + lifecycle stage → attach the *existing* awards/payments by delivery body + name match →
  optionally CPO / board-minute / C&AG context. *Plan → land → approve → award → pay → audit.*
- **Buildable now?** **No — needs the PI2040 ingest first** (scoped, not built). After that,
  the money attachment is a **fuzzy join** (delivery body + fuzzy name), not a clean key.
- **Concrete example:** "MetroLink" → stage: Planning → delivery body: TII → CPO land
  authorisations → board approvals → eTenders awards → payments — one scheme's whole lifecycle.
- **Limits (state them in the UI):** PI2040 **cost is band-only + sparse (~18%)**, so you
  **cannot show a reliable € total per project.** The join from project → spend is
  *indicative*, not audited. This is a *navigation and accountability* product, not a
  "this project cost €X" product.
- **WTP unlocked:** **Highest.** IDEAS calls the Infrastructure Project Profile "the most
  novel page in Irish civic tech." A named-project lifecycle that nobody else assembles is the
  thing a journalist / contractor / analyst would actually pay for — *provided* the fuzzy-join
  honesty is front and centre.

### Framing 4 — Both (body-first default + curated featured + PI2040 later)
- **What it is:** Body-first as the honest default browse now (Framing 2), curated programmes/
  groups featured on top as starting points now (Framing 1), and PI2040 project nodes added
  later (Framing 3) when ingested — same trail, one more node type.
- **Buildable now?** Yes for the first two; the third is a clean later extension.
- **WTP unlocked:** Builds toward Highest without blocking on the ingest. **This is the path
  that doesn't paint you into a corner.**

### Projects-framing summary
| | Buildable now | € total per project? | Differentiation | WTP |
|---|---|---|---|---|
| 1 · curated examples | Yes (half-built) | No | Low (curated list) | Med |
| 2 · body-first | Yes (already is) | No | Low | Med |
| 3 · PI2040 spine | After ingest | No (band-only, ~18%) | **Highest** | **Highest** |
| 4 · both / staged | Yes, staged | No | Highest (eventually) | Highest |

---

## Part 5 — The commercial lens (who pays, and for what)

Who would pay for this surface, and which option serves them:
- **Journalists / NGOs:** want the *story* — "where did the children's-hospital money go,"
  named scheme to named firm. Served by **curated examples + project lifecycle** (Framings 1+3).
- **Competitors / market intel (a Sisk rival):** want "who is winning what, from which bodies,
  in my sector." Served by **Companies depth + body-first flow** (firm and body lenses).
- **Researchers / analysts:** want the *graph* and exports — body⇄supplier⇄ledger, group
  rollups. Served by **the trail + group consolidation + `/v1/data` exports**.

The common thread: **the differentiated, payable asset is the *assembly nobody else does* —
the named project / body / group walked end to end with honest grain labels.** A thin supplier
copy is not that; the Companies dossier and the project-lifecycle trail are.

**Therefore the reorg and the projects framing point the same way:** push *firm* depth to
Companies, keep Follow the Money as the *flow/assembly* tool, and invest the project spine
(PI2040) there because that is the highest-WTP, least-replicable thing.

---

## Part 6 — Recommendation (for decision — not yet executed)

> A recommendation, not a decision. Nothing here is built.

1. **Reorg: Option A now** (body/flow-first; supplier node hands off to Companies; landing
   search becomes body search). Lowest risk, removes the duplication, reversible. Hold Option B
   (retire Suppliers tab) until usage data justifies it; **reject Option C** (it buries the
   monetisable headline).
2. **Projects: Framing 4 (staged).** Body-first + curated featured *now*; commit to the
   **PI2040 ingest as the next data investment** to unlock the real project node — the
   highest-WTP, most novel thing in the backlog. Bake the "cost is band-only / spend is a fuzzy
   join" honesty into the UI from day one.
3. **The provisional landing search box:** under Option A it should search **bodies** (and,
   later, **projects**), not suppliers — supplier search already lives on Companies. Repurpose
   it; don't ship it as a supplier search.

## Part 7 — Open questions for you
- **Naming:** keep "Follow the Money" (strong, monetisable phrase) and let *project* be a node
  *inside* it? Or split a separate "Infrastructure / Projects" page later once PI2040 lands?
- **Suppliers tab fate:** retire now (Option B) or wait for usage signal?
- **Sequencing:** do the cheap Option-A reorg first and ingest PI2040 after, or treat PI2040 as
  the headline and do the reorg as part of that bigger build?
- **Commercial gate:** is the project-lifecycle view a *paid-tier* feature (exports/API) while
  the body/firm walk stays free? That choice shapes how hard the project node is gated.
