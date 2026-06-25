# Front Door — prototyping the shallow layer over a deep app

Status: **EXPLORATION / PROTOTYPE PLAN** (2026-06-21). No code changed. This doc is a menu of
front-door approaches to *prototype*, not a committed build. It is the companion to
`doc/FOLLOW_THE_MONEY_IA_EXPLORATION.md` (that one reorganises *one* dense page; this one asks
what the *first screen* should be).

---

## The problem, stated plainly

The app is **significant but dense**. It holds things that exist nowhere else (the only public
copy of HSE €20k payments; procurement × lobbying × diary overlap; recovered asylum/accommodation
spend; council accountability). But every entry point is *depth-first*:

- `/` lands on the **member-overview page** — a TD dossier you must already have a TD in mind to use.
- Every other top-level page is a **league table / ranking** (`rankings-*`): payments, votes,
  procurement, lobbying, attendance. These reward a user who already knows what they're looking for.

There is **no screen that answers a question the visitor already has** before they've learned the
app's vocabulary (CPV, SI, PO, division). That is why "I get some users but most find it dense /
not interesting" — the front door opens onto a filing cabinet, not a story or a hook.

**The fix is not less data.** The corpus is the moat. The fix is a thin, opinionated layer on top
whose only job is to convert a cold visitor into one engaged session — *then* hand them down into
the depth that already exists.

The litmus test for every prototype below: **a stranger with no civic-tech vocabulary lands on it
and within 10 seconds wants to click.**

---

## Design principles for the front door (the rules each prototype must obey)

1. **One question per surface, answered in one sentence**, with the data as the *receipt* beneath.
2. **Lead with a story or a hook, not a navigable corpus.** Nobody arrives wanting to "explore
   procurement." They arrive because of a headline, or because of *their* area.
3. **The personal hook ("your TD / your area") is the highest-converting move** in every civic tool.
4. **Density is allowed — after the hook lands.** Every front-door surface ends in a "show me the
   data / go deeper" handoff into an *existing* page. Front door = funnel, not destination.
5. **No new pipeline work to prototype.** Every approach below maps to gold/views that already
   exist, so prototyping is a UI/IA job. (PI2040 and any new ingest are explicitly out of scope
   here — see the FTM exploration doc for that track.)
6. **Honest grain labels survive the simplification** (no summing award ceilings with paid; no
   "this project cost €X" where the data is band-only). Per `feedback_no_inference_in_app`.

---

## The five candidate approaches (to prototype side by side)

Each is specified as: **the question it answers · the data it rides on (existing) · prototype scope
· effort · what "it worked" looks like.** They are not mutually exclusive — the recommendation is
to prototype 2–3 and let usage choose.

### A. Story tiles — "Here is a thing that happened to your money"
*The children's-hospital / BAM exemplar, generalised.*

- **Question answered:** "Where did the money for [the thing I've heard of] actually go?"
- **Data (exists):** `payments_fact` + curated `supplier_groups.csv` (BAM rollup) + the
  `follow-the-money` trail. The NCH story is already half-built as `_FEATURED` / `_FEATURED_GROUPS`
  in `follow_the_money.py`. The killer fact is already in the data: **NPHDB's €130.7m to the BAM
  group is overwhelmingly *dispute settlements*, not base build.**
- **Prototype scope:** A landing strip of 4–6 hand-curated **story cards**, each = a headline + one
  number + one sentence of context + a "follow the trail" button into the existing FTM walk.
  Reference card spec below (§ "The reference prototype").
- **Effort:** **Low.** It's curation + a card component + wiring to routes that already resolve. No
  new data.
- **It worked if:** story-card click-through beats the current cold league-table bounce; users who
  click a story go *deeper* (reach a ledger/dossier) rather than leaving.
- **Honesty guardrail:** every number is a *receipt*, captioned with its grain ("committed", "paid",
  "dispute settlements") and a source link. No editorialising beyond what the ledger says.

### B. Personal hook — "Your area / your TD"
- **Question answered:** "What does this app know about *me* / *where I live*?"
- **Data (exists):** `constituency.py`, `member_overview.py`, attendance gold, `local_government.py`
  ("Who runs your county" — 31 CE roster + collection/planning/derelict views), council spending.
- **Prototype scope:** A single **postcode/area or TD search box as the hero**, resolving to a
  one-screen "your area" card: your TDs (with the attendance "do they turn up" number), your council
  (who runs it + one accountability metric), one local spend fact. Each line is a handoff into the
  existing deep page.
- **Effort:** **Low–medium.** Pages exist; this is an aggregating front card + the area→constituency
  resolve (constituency page already does the mapping).
- **It worked if:** the box gets used; "your area" sessions go longer than cold landings.
- **Note:** this is the single most-proven pattern in civic tech (TheyWorkForYou, mySociety). If we
  prototype only one thing, it's a strong default — but it's *less shareable* than a story (you can't
  tweet "your area").

### C. "What changed this week" — a reason to come back
- **Question answered:** "Anything new I should know?"
- **Data (exists):** the refresh pipeline already lands new rows weekly (live tenders, payments,
  lobbying, diaries, votes). Source-health + freshness canaries already track *what* updated.
- **Prototype scope:** An **auto-generated digest feed**: "5 things that changed" — biggest new
  payment, a new large tender, a notable new lobbying return / ministerial meeting, a close vote.
  Templated one-liners with a deep link each. Starts hand-curated weekly; later auto-templated off
  the freshness deltas.
- **Effort:** **Medium** (templating + picking "notable" thresholds; the data deltas exist).
- **It worked if:** return visits rise; the digest is the thing people subscribe to / share. This is
  the only approach that builds a *habit* rather than a one-off visit — raw density never does that.
- **Risk:** "notable" is an editorial judgement; keep it threshold-driven + transparent so it isn't
  inference dressed as fact.

### D. "One big number" — a single visceral fact, daily/weekly
- **Question answered:** none explicitly — it's a *curiosity hook*, the front-page splash.
- **Data (exists):** `top_payments`, `procurement_by_*`, diaries, donations — any view with a
  headline figure.
- **Prototype scope:** A full-bleed hero with **one number, one sentence, one link** ("€130.7m — what
  the Children's Hospital body paid BAM, nearly all of it dispute settlements →"). Rotates.
- **Effort:** **Low** (it's essentially story tiles, A, reduced to one at a time).
- **It worked if:** the splash earns the first click. Largely subsumed by A; worth prototyping only
  as a *layout variant* of A (hero-single vs strip-of-cards), not a separate build.

### E. Guided questions — "What do you want to know?"
- **Question answered:** lets the visitor self-select a question instead of facing a nav menu.
- **Data (exists):** all of it — this is a routing layer.
- **Prototype scope:** 4–6 big plain-language question buttons on the landing: "Does my TD turn up?"
  · "Who does my council pay?" · "Who's lobbying the government?" · "Where did the children's-hospital
  money go?" Each routes into an existing page pre-filtered. No prose, just doors labelled in human
  words instead of civic jargon.
- **Effort:** **Low.** It's a relabelled nav as a hero. Cheapest of all.
- **It worked if:** click distribution shows people pick questions they'd never have found in the
  sidebar. Good as a *baseline* to compare richer approaches against.

### Approaches at a glance
| | Question it answers | Rides on (exists) | Effort | Shareable? | Builds habit? |
|---|---|---|---|---|---|
| A · Story tiles | "where did *that* money go" | payments_fact + groups + FTM trail | Low | **Yes** | No |
| B · Your area/TD | "what's here about *me*" | constituency/member/local_gov | Low-med | Weak | No |
| C · What changed | "anything new" | refresh deltas + freshness | Med | Some | **Yes** |
| D · One big number | (curiosity splash) | any headline view | Low | Yes | No |
| E · Guided questions | "what *can* I ask" | all (routing) | Low | No | No |

---

## The reference prototype — Children's Hospital / BAM story card

This is the one to build first because it's lowest-effort (half-exists), most shareable, and proves
the whole "story → trail" funnel. Use it as the template for every other story card.

**Front (the card on the landing):**
- Eyebrow: `FOLLOW THE MONEY`
- Headline: **"The Children's Hospital's €130.7m to BAM was nearly all dispute settlements"**
- One number, big: **€130.7m** · caption: *committed by NPHDB to the BAM group*
- One sentence: "The body that builds the National Children's Hospital paid the BAM construction
  group €130.7m — the ledger shows the bulk are dispute settlements, not base construction."
- CTA button: **"Follow the trail →"** → existing route
  `/follow-the-money?...` (NPHDB body node → BAM group node → ledger).

**Back (where the click lands — already built):** the FTM walk: NPHDB body → BAM group rollup
(~18 legal entities incl. PPP SPVs with no CRO) → line-item ledger, with the "committed vs paid vs
settlement" grain labelled and source links per row.

**Why this one proves the model:** it's a name everyone recognises (children's hospital), a number
that lands without context, a genuinely non-obvious finding (settlements, not build), and the depth
it funnels into *already works*. If this card doesn't convert, no story card will — and we learn
that cheaply.

---

## How to prototype (cheap, isolated, measurable)

1. **One isolated landing variant, behind a flag.** Build a `front_door` sandbox/home variant gated
   by an env flag (cf. `DAIL_EXPERIMENTAL`) so the current member-overview home is untouched and the
   variant is one toggle to show/kill. Per `feedback_pipeline_changes_data_anchored_promotion` —
   sandbox → vet → promote; nothing loose in prod.
2. **Curate, don't model.** All v1 story cards are a small hand-curated list (like `_FEATURED`), in a
   `data/_meta/front_door_stories.csv` or a module constant. No new pipeline. Each row = eyebrow,
   headline, number, caption, sentence, target route.
3. **Reuse the card components.** Pull from `shared_css.py` / `ui/components.py` card patterns
   (`feedback_css_card_pattern`) — whole-card-as-`<a href>` or card+→button. No new CSS philosophy.
4. **Wire to routes that already resolve.** Every CTA points at an existing page/param. If a target
   doesn't exist yet, the card is out of scope for v1.
5. **Measure the funnel, not vanity.** The metric that matters: *did a cold landing reach a depth
   page?* Instrument click-through on cards and whether the session reaches a ledger/dossier/profile.
   Compare against the current cold-landing baseline.
6. **Prototype 2–3 layouts of the same data** (story strip A, your-area hook B, guided questions E)
   and let the click data choose — they share the same underlying routes, so it's layout work.

---

## What this is NOT (scope guardrails)

- **Not a redesign of the deep pages.** They stay exactly as they are; the front door funnels *into*
  them. (Page-level boldness/redesign is a separate `bold-redesign-page` track.)
- **Not new data.** No PI2040, no new ingest. Anything needing an ingest belongs in the FTM
  exploration doc's "projects" track, not here.
- **Not modelling/inference in UI copy.** Story sentences restate what the ledger says, captioned
  with grain + source. Per `feedback_no_inference_in_app`.
- **Not a commitment to all five.** This is a menu. Decision needed (below) on which 2–3 to build.

---

## Data validation — what the digging actually uncovered (2026-06-21)

Queried `data/gold` / `data/silver` directly (DuckDB over the parquet) to confirm each approach is
*data-backed*, not just plausible. Result: **all five are buildable on existing data.** Specifics:

### A · Story tiles — VALIDATED, and stronger than the memo
`procurement_payments_fact.parquet` (247,457 rows). The National Children's Hospital story holds up
and is sharper than "€130.7m, mostly settlements":
- Publisher **"National Paediatric Hospital Development Board"** → supplier **BAM BUILDING** =
  **€130.7m** across 5 rows. Of that, **€126.7m is two line items** whose *own descriptions* read:
  - €107.6m (2024): *"Conciliator's Recommendation No. 25 — Notice of Dissatisfaction issued,
    payment made on receipt of Bond"*
  - €19.1m (2025): *"Conciliator's Recommendation No. 29 — Notice of Dissatisfaction issued…"*
- So the "dispute settlement, not base build" claim is **literally in the ledger text** — the
  strongest possible provenance for a story card. No inference needed; we quote the description.
- **Grain caveat to print:** `amount_semantics = po_committed` — these are *committed* figures, not
  confirmed cash-out. Caption as "committed", link the source row. (Per `feedback_no_inference_in_app`.)
- `supplier_groups.csv` confirmed: BAM rollup = 9 legal entities incl. 3 PPP SPVs with no CRO.
  Note: `WILLS BAM JV`, `BAMFORD BUS`, `BAMOS…` also match `%bam%` but are **not** BAM group — the
  curated csv is the authority, don't fuzzy-match on the string.

### B · Personal hook — VALIDATED
- `attendance_by_td_year.parquet` carries `full_name, year, constituency, party_name, is_minister,
  sitting_days, total_days` — current to **2026**. The "does your TD turn up?" number is one lookup.
- `data/_meta/constituency_la_crosswalk.csv` maps constituency → local authority (with multi-LA
  flags), so area → TDs → council resolves with no new data.
- Council layer real: `noac_m2_collection_wide` (per-LA rates/rent/loan collection %),
  `la_chief_executives.csv`, derelict-sites + planning views. "Who runs your county" is populated.

### C · What changed — VIABLE, on the right source
- The genuinely *weekly-fresh* feed is `etenders_live_tenders.parquet`: 2,363 rows, `published_date`
  current to **2026-06-18** (3 days ago), with title/buyer/value/deadline/CPV. This is what a
  "5 things that changed this week" digest should lead on.
- Payments are **quarterly**, not weekly (and the period field maxes at a future-looking `2026-Q4`
  — a known labelling quirk; don't build a "this week" claim on payments). So the digest = live
  tenders weekly + payment/lobbying/vote drops when they land. Confirmed the cadence is real.

### D / E — trivially backed (they ride A's and the routing layer's data). No separate risk.

### Concrete story-card candidates beyond NCH/BAM (all query-confirmed)
1. **Council accountability:** Sligo County collected only **74%** of commercial rates in 2024
   (Leitrim 79%, Donegal 80%) — `noac_m2_collection_wide`. Punchy, local, shareable.
2. **Lobbying × contracts:** **Grant Thornton** — 89 lobbying returns, 176 award rows across 56
   public authorities (`procurement_lobbying_overlap`). **TRAP (from memory
   `project_mcp_procurement_lobbying_overlap`):** rows duplicate per lobby-name — **never sum
   `awarded_value_safe_eur`** for a headline; cite one firm's de-duped figure and frame as overlap,
   not causation.
3. Room for a fourth: a single-bid procurement fact or a ministerial-diary/lobbying corroboration.

### One real issue surfaced — text encoding (mojibake)
Description and party fields carry **latin-1/utf-8 mojibake**: `Conciliator�s`, `Sinn F�in`. The
front door puts this text *on the hero* — so the prototype must render it clean (fix the decode at
read, not in the parquet). Small, but a story card that says "Conciliator�s Recommendation" is
unshippable. Flag it as a v1 acceptance criterion.

---

## Recommendation & open questions

**Recommendation:** prototype **A (story tiles)** first using the Children's Hospital / BAM card as
the reference, alongside **B (your-area hook)** as the proven personal-hook baseline, both behind one
flag on an isolated home variant. Treat **C (what-changed digest)** as the fast-follow because it's
the only one that builds a *return habit* — but it needs threshold tuning, so it lags A/B. Fold D
into A as a layout variant; keep E as the cheap baseline to measure the richer ones against.

**Open questions for you:**
1. **Hero choice:** does `/` lead with a *story strip* (A) or a *your-area box* (B)? Or split-hero
   with both? (My lean: story strip top, area box directly beneath — story earns the click, area
   earns the *second* click.)
2. **Story sourcing:** who curates the story cards, and how many at launch (4? 6?) — and do we want a
   pipeline to *suggest* candidate stories from the data later, or stay hand-curated indefinitely?
3. **Keep member-overview as `/`?** Or does the new front door become `/` and member-overview move to
   `/member-overview` only? (It already has that route; this is a one-line nav change.)
4. **First three story cards beyond NCH/BAM** — candidates from the corpus: a council accountability
   fact, a procurement single-bid fact, a ministerial-diary/lobbying overlap. Which three?
5. **Success bar:** what click-through / depth-reach number would make us promote the front door from
   flag to default?
