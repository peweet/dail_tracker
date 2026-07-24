# Journalist MCP — design plan

**Status:** PROPOSED (no code) · **Domain:** mcp / product · **Date:** 2026-07-20 ·
**Users named 2026-07-20:** two archetypes — a local paper (Connacht Tribune) and a
national with a data desk (Irish Times). See "Named users" below; it re-prioritises
everything after it.

A second, narrowly-scoped MCP entrypoint that answers questions the way a newsroom needs
them answered: attribution first, every figure carrying its trust band, licence and
data-as-of date, with the repo's privacy and never-sum guards enforced as hard denials
rather than conventions. The dev MCP serves the person who builds the data; this serves
the person who publishes from it.

## Why now

Three things converged. The trust-tier layer (`services/data_contracts.py` —
`TRUST_TIER_LABEL`, `derive_trust_tier`, `assess_trust`) is being built out right now and
needs a consumer that makes it pay. The IPAS findings memory holds publication-grade
material whose constraints (identity gate, never-causal) are currently prose conventions
a tool could enforce. And the journalism style sources read for the communication-rules
work turn out to specify tool behaviour, not just prose style — see next section.

## What the journalism sources dictate (the design inputs)

From the BBC News Styleguide, "Attribution first" (p22) and "Numbers & measures"
(p51–53) [Verified — PyMuPDF extraction from the fetched PDF, 2026-07-20]:

- *"Broadcasters should always identify the source of an assertion before making it —
  always say who before you say what they said or did."* → the response envelope leads
  with the source, and composed lead sentences read "According to <publisher> data
  (as of <date>), …" — never figure-first-source-later.
- *"The Chancellor has shocked industry by raising corporation tax by 10 per cent, from
  20 per cent to 30 per cent"* — the BBC's own example of the percentage/percentage-point
  error, with *"a doubling is an increase of 100 per cent"* and *"three times greater
  than is the same as four times as great as"*. → change-expression must be computed by
  the tool, not phrased by the model.
- *"Comparisons only make easy sense if they are expressed in the same format"* (their
  teachers-2.9%-vs-Lord-Chancellor-£22,700 example) and *"do not mix decimals, fractions
  and percentages in one story"*. → comparison outputs return both figures in both
  formats.
- *"A story with too many figures numbs the listener. Simplify wherever you can, round
  up or down"* → a `audience` parameter selecting the ONS three-tier rounding (public /
  decision-maker / analyst) already documented in COMMUNICATION_STYLE.md §5, coarse
  tiers always flagged.

From the Guardian/GNM editorial code [Reported — search coverage; the domain blocks
fetching. Verify against the PDF (uploads.guim.co.uk GNM editorial code 2023) before
relying on wording]:

- Attribute to the source; be honest about sources even when they can't be named. →
  citations carry publisher + licence + retrieval date, and gaps say so.
- Correct significant errors as soon as possible. → published figures must be traceable
  to data-as-of, and dataset revisions must be discoverable after the fact
  (`data_freshness` below is the corrections-desk tool).

From the UK Government Analysis Function (already in COMMUNICATION_STYLE.md §5):
"treat with caution" is banned; a caveat is two lists — what the numbers **can** and
**cannot** support. → the envelope has `can[]` / `cannot[]` fields, populated per
dataset from fact_cards.

## Shape

**Separate entrypoint, shared query layer:** `mcp_server/journalist_server.py`
importing the same modules as `server.py`, exposing only the journalist toolset. The
alternative — a mode flag on the existing server — makes one process serve two trust
levels and one accidental registration leaks a dev tool (raw SQL, code_index) to an
external audience; the separate entrypoint makes the permission boundary a file
boundary. Trade-off accepted: a second process to configure and test.

**Every tool returns one envelope:**

```
source        first field, always — publisher, dataset, as_of, retrieval date, licence
claim         the figure/statement, phrased attribution-first
band          Verified | Reported | Extracted | Indicative — read from data_contracts,
              never restated locally (the one-definition rule in evidence.md)
can / cannot  what this number supports and what it does not (Analysis Function form)
caveats       join-key semantics (0 = not-matched ≠ absent), coverage gaps, revisions
```

Only `Verified` may render unqualified; lower bands ship with their caveat text
composed into the claim — the UI one-way gate, applied at the API instead.

## Tools (phased)

Ordering note after naming users: the envelope layer and Phase-1/2 tools are shared by
both archetypes, but the *transport* order flips — the CT brief generator (not an MCP
client) consumes them first, and the hosted MCP ships with the IT pilot.

**Phase 1 — pure reuse, highest value:**
- `cite_figure(dataset, context)` — a publication-ready citation pack from fact_cards +
  coverage + source-health: publisher, URL, licence (eTenders CC-BY etc.), data-as-of,
  coverage %, trust band, one-line method.
- `data_freshness(dataset)` — refresh heartbeat, last revision, known gaps. The
  corrections-desk tool: "has the number I published moved?"

**Phase 2 — deterministic text logic:**
- `express_change(old, new, kind)` — returns the BBC-correct phrasing: percentage vs
  percentage points, doubling = +100%, both-formats comparisons, audience-tier rounding.
  Small, pure, and eliminates the single most common published-numbers error class.
- `claim_check(sentence)` — decompose a draft sentence into figures; each comes back
  with band + can/cannot + a refusal where the sentence sums across money grains
  (the never-sum check via fact_grain, returned as three lanes plus the reason).

**Phase 3 — needs the entity crosswalk (design exists in memory):**
- `entity_dossier(org)` — org-360 with the identity gate hard-coded: name similarity is
  never identity; association phrasing only, never causal (IPAS constraints as code).
- `verify_published(figure, claimed_source)` — reproduce a published number against
  gold; returns Verified-with-query or cannot-reproduce-plus-closest.

**Phase 4:**
- `quote_context(debate_ref)` — surrounding debate text against quote-mining. Caveat:
  speaker-level linkage is blocked on AKN-XML parsing (debate→TD memory); listings-level
  only until that lands.
- `methodology_pack(figures[])` — the "show your workings" appendix assembled from the
  envelopes already emitted.

## Hard guards (denials, not conventions)

- **Privacy:** individual-insolvency queries refused (never-individual rule); SIPO donor
  addresses never returned (PII); no people-search shapes (ABP corpus rule); the
  `public_display` flag enforced in the shared query layer — a bypass of that flag has
  already happened once via a hand-rolled view (MCP cheap-scan memory), which is the
  argument for enforcing it below the tool layer.
- **Grain:** cross-grain sums refused with the three lanes returned separately. TED
  never summed with anything.
- **Causality:** response templates contain no causal connectives; associations are
  phrased as associations. (Deterministic because it's template text, not model prose.)

## Non-goals

Could reasonably be goals, excluded on purpose: a publishing CMS or story editor (the
envelope feeds one, it isn't one); people-search of any kind (privacy rules make this
structural, not just out of scope); real-time media monitoring (media-mentions is
PARKED in memory); paid-product packaging (bid-intel scoping memory found pricing not
viable — this is the public-interest lane of the dual-licensing split, and the BI
spinout's ethics firewall points the commercial lane elsewhere).

## What gets harder

The trust-tier module becomes a public API — schema changes in `data_contracts.py`
break an external consumer, so its in-flight changes need to settle first. Privacy
failures become external-facing, which raises the stakes of the deny paths — they need
their own test lane (deny-shape fixtures, run in the fast suite). And every new gold
dataset now owes an envelope entry (can/cannot lists in fact_cards) before the
journalist MCP can serve it — a per-dataset authoring cost the dev MCP never charged.

## Named users — what each can actually do

Two archetypes, deliberately at opposite ends of technical capacity. All dataset facts
in this section are [Reported — project_your_councillors_feature and related memories,
2026-07 vintage; re-verify row counts against gold before quoting to a newsroom].

### Connacht Tribune (local/regional — Galway patch, no data desk)

**Day-1 inventory for their patch, from existing gold/silver:** full councillor roster
(Galway County 39/39, City 18/18 — the two councils where the Wikipedia source was
accuracy-tested), meeting agendas with upcoming-meeting flags, Galway County's parsed
standing orders, CE accountability framing, council spending/AFS/budget lanes, planning
applications, procurement by local bodies, LGAS audit reports, local TDs' votes,
attendance, interests and PQs, councillor pay schedule (Representational Payment
€32,059/yr) with S142 actuals where the council publishes them.

**The honest gap is itself a story:** Galway City verifiably holds no named council
votes and Galway County records proposer/seconder + AGREED only — the named-votes gold
covers Carlow, Cork City, Kilkenny, Laois and Fingal (4,958 rows), not Galway. A
patch-scoped tool must return this as a `cannot` with the structural reason (standing
orders don't require roll-calls), not as an empty result. For a local paper,
"your council leaves no named voting record" is a publishable accountability finding
the tracker can substantiate.

**Interface reality:** a two-person newsroom will not run an MCP client. The material
interface for this archetype is a **generated weekly patch brief** — agendas ahead,
new planning applications, new awards/payments touching the patch, any new audit
findings — every figure carrying its citation pack. The tender-alert email design
(memory) is the direct precedent. MCP-via-claude.ai-connector is a bonus for one
tech-curious reporter, not the plan. **Implication: the envelope layer is the product;
MCP is one of its transports, and for this user not the first one.**

### Irish Times (national — has a data desk)

**Day-1 draw:** the cross-register national sets — procurement awards/payments,
lobbying returns, PQs, ministerial diaries, SIPO, judiciary, IPAS spend — with the
never-sum and join-semantics guards enforced by the tool rather than trusted to the
reporter. A data desk can consume a hosted MCP directly (or flat exports); for them
`entity_dossier`, `claim_check` and `verify_published` are the value, and the
methodology pack is what their standards desk will ask for.

**Interface:** hosted read-only MCP over the R2 runtime copy — the publish chain
(tools/publish_runtime_to_r2.ps1) already ships the runtime data to R2, so the server
reads from that, not from this machine. Single API key per newsroom, rate-limited.

## Pilots — the acceptance tests this plan previously lacked

- **CT pilot (4–6 weeks):** auto-generate the weekly Galway patch brief; success = one
  published story citing the tracker with a correct citation pack, zero corrections
  attributable to our data. Effort is mostly assembly — brief generator over the
  envelope layer plus the existing email precedent.
- **IT pilot:** data desk gets the hosted MCP with `cite_figure`, `entity_dossier`,
  `claim_check`; success = they reproduce one of their own published figures against
  gold, or one dossier contributes to a published piece. Gate: the privacy deny-test
  lane must exist before any external key is issued.

## Reversibility and remaining unknowns

Additive and two-way until hosting: issuing an external newsroom a key is the
one-way-ish step (expectations, freshness obligations, correction duty). Remaining
unknowns: **licence/pricing posture** (CT free public-interest lane; whether IT pays a
commercial licence is the dual-licensing question — deliberately not designed here),
and **freshness duty** — an external newsroom consuming the data creates an obligation
to keep the R2 runtime current and to surface revisions, which today is a manual
publish chain on one machine.

---

# Part 2 — Detailed design (2026-07-20)

Everything below is buildable from the repo as it stands; view/file names cited from
memory carry a verify-before-build note. Illustrative content is marked ILLUSTRATIVE —
no invented figure below should ever be quoted as data.

## 2.1 The envelope — one schema, three transports

New module `services/press_envelope.py`. Pure assembly: reads fact_cards, coverage,
source-health and `data_contracts` trust tiers; performs no queries itself.

```json
{
  "source": {
    "publisher": "Office of Government Procurement",
    "dataset": "procurement_awards",
    "as_of": "2026-07-18",
    "retrieved": "2026-07-20",
    "licence": "CC-BY 4.0",
    "url": "<canonical source url>"
  },
  "claim": "According to OGP data (as of 18 July 2026), <figure-bearing sentence>",
  "band": "Verified",
  "band_mechanism": "reproduced by query <query_hash> against gold",
  "can":    ["compare awarded values within eTenders across years", "..."],
  "cannot": ["be summed with payments or budget figures (different money grain)",
             "support a causal claim about lobbying influence", "..."],
  "caveats": ["0 = not-matched, not absent (join coverage ~X%)"],
  "ledger_id": "brf-2026-07-20-galway-014"
}
```

Rules encoded in the schema itself: `source` is the first field (BBC attribution-first);
`claim` is composed source-first; `band` is read from `data_contracts.TRUST_TIER_LABEL`,
never restated; `can`/`cannot` come from the dataset's press block (2.2); every emission
gets a `ledger_id` (2.5).

## 2.2 fact_cards press blocks — the authoring cost, made explicit

Each press-served dataset gains a `press` block in fact_cards:

```json
"press": {
  "publisher": "...", "licence": "...", "source_url": "...",
  "method_line": "one sentence, plain English, how this data is produced",
  "can": ["..."], "cannot": ["..."]
}
```

Authoring is manual and deliberate — the can/cannot lists are editorial judgements the
user makes once per dataset, not model output. Priority order for the first ~10 blocks:
the CT patch set (meeting agendas, planning applications, council payments/AFS/budgets,
LGAS audit reports, councillor roster + pay, procurement by local bodies) plus the IT
set openers (procurement awards/payments, lobbying returns). A dataset with no press
block is **unservable** by every press transport — that is the enforcement point.

## 2.3 Tool internals

**`claim_check(sentence)`** — the external twin of this session's `style_lint`:
1. Extract figures (same FIGURE_RE family as tools/hooks/style_lint.py — the internal
   linter and the press checker deliberately share one regex module so they can't
   drift).
2. Resolve each figure's dataset via fact_cards search; unresolvable → `band:
   "Indicative", cannot: ["be attributed to any tracker dataset"]`.
3. Grain check: if the sentence arithmetic spans money grains, return the refusal
   envelope with the three lanes separated.
4. Reproduce where cheap (count/sum against the named gold) → band per outcome.

**`express_change(old, new, kind)`** — pure function in `services/press_numbers.py`,
~50 lines plus tests. Returns all of: percentage change, percentage-point change (when
`kind="rate"`), the BBC-correct doubling/tripling phrase, and both-format comparison
strings. Also imported by UI copy helpers — it is not MCP-only.

**`cite_figure(dataset, context)`** — envelope minus claim-composition; the citation
pack alone.

**`data_freshness(dataset | ledger_id)`** — with a ledger_id it re-runs that emission's
query and reports drift (2.5). With a dataset it returns heartbeat + last revision.

**`patch_inventory(county)`** — what the tracker can and cannot say about a patch,
generated from press blocks + coverage. For Galway this is where "no named council
votes exist; standing orders do not require roll-calls" is returned as a structural
`cannot` [Reported — your-councillors memory; verify against v_la_standing_orders
before shipping].

## 2.4 The CT patch brief — shape

Generator `tools/press_brief.py --county galway` → markdown (email/PDF later; the
tender-alert email design is the delivery precedent). Sections, each item enveloped:

```
GALWAY PATCH BRIEF — week of <date>            (ILLUSTRATIVE shape, not real data)

COMING UP
• <Council> meets <date> — agenda highlights: <items>       [agenda source + link]

NEW ON THE RECORD
• <N> new planning applications in <LEAs>; <M> decisions    [cite pack]
• New payments over €20k by <council>: <suppliers>          [cite pack + grain caveat]
• LGAS audit report published for <body>: <finding line>    [cite pack]

YOUR REPRESENTATIVES
• <TD> asked <N> PQs (<topics>); <vote> in <division>       [cite pack each]

WHAT WE CANNOT TELL YOU
• Named council votes: none exist for Galway — <structural reason + SO link>
```

The last section is mandatory and generated from `cannot` lists — the honesty rail as
a standing feature, not a disclaimer.

## 2.5 The citation ledger — corrections made mechanical

Every envelope emission appends `{ledger_id, dataset, query_hash, figure, as_of}` to
`data/_meta/press_ledger.jsonl`. A scheduled job (canary precedent) re-runs ledger
queries after each data refresh; drift beyond tolerance emits a correction notice
naming the ledger_id — which the brief printed. This is the GNM "correct significant
errors as soon as possible" duty implemented as code: the publisher can trace any
printed figure to its query and learn when it moved. Retention and tolerance are
per-dataset settings in the press block.

## 2.6 Privacy deny lane — gate before any external key

`test/press/test_deny_shapes.py`, in the fast suite, asserting refusal envelopes for:
individual-insolvency queries; SIPO donor addresses; people-search shapes (name-in,
person-profile-out) against ABP/planning corpora; `public_display` bypass (the one
known historical bypass class); cross-grain sums; TED-summed-with-anything. Each deny
returns a `cannot` envelope with the reason — the same shape as every other response,
so a refusal is citable too.

## 2.7 Phasing with dependencies

| Phase | Builds | Depends on | Est. effort |
|---|---|---|---|
| P0 | trust-tier work settles; press blocks for ~10 datasets | user's in-flight data_contracts changes | authoring sessions, user-led |
| P1 | press_envelope + press_numbers + claim_check/cite_figure/data_freshness on the existing dev server | P0 | 2–4 sessions [Indicative — estimate] |
| P2 | patch brief generator (Galway), manual weekly send + ledger | P1 | 2–3 sessions [Indicative] |
| P3 | CT pilot ops: scheduled brief, correction monitor, right-of-reply pack | P2 + a willing newsroom | ongoing |
| P4 | hosted MCP (journalist_server.py, R2-read, API key) + deny lane green | P1 + deny tests + an actual data desk | separate project |

Kill criteria, stated now: if the CT pilot produces no published story in two months,
stop at P2 — the envelope layer and ledger still serve the user's own publishing, so
nothing is wasted; P4 never starts without a named data desk.

## Sources

- BBC News Styleguide (PDF) — attribution-first p22, numbers p51–53 [Verified — fetched
  and extracted 2026-07-20]
- GNM editorial code of practice (uploads.guim.co.uk, 2023 PDF) [Reported — search
  coverage only]
- Government Analysis Function, communicating uncertainty — via doc/COMMUNICATION_STYLE.md §5
- Repo: services/data_contracts.py trust tiers · data/_meta/fact_cards.json ·
  fact_grain (never-sum) · join_map · source-health registry · entity-crosswalk design
  and IPAS constraints (memory)
