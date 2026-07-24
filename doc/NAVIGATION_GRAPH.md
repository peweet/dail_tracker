---
tier: CONTEXT
status: LIVE
domain: ui
updated: 2026-06-20
supersedes: []
read_when: before adding, hiding, or removing a page link, or judging whether a link creates a contextual cul-de-sac (entity doesn't travel)
key: CONTEXT|LIVE|ui
---

# Navigation Graph — composability map & defect register

*Living map of how the app's pages link to one another as an entity graph.*
Last verified empirically: 2026-07-21 — all claimed edges re-confirmed on live
DOM, **0 mismatches** (`audit_screenshots/_nav_graph_edges.py`). The prior
2026-06-20 register was also machine-checked; the 07-21 pass corrected the
harness itself (see "Test method" below) and added a static ratchet
(`tools/check_nav_graph.py`, in the fast suite) that flags any NEW contextual
cul-de-sac at commit time.

## Why this exists

The product question: *is the click-path circular (natural overlaps flow into
one another) or full of cul-de-sacs (dead ends)?* That is a question about the
**navigation graph** — entities as nodes, links as edges. This doc is the
single source of truth for which edges exist, which are missing, and which are
the real defects.

## The two edge classes (read this first)

Empirical testing surfaced a distinction the code-only audit missed, and it
reframes everything:

1. **Global nav chrome** — the top-nav strip (~16 links) is present on *every*
   page. It means **there are no absolute cul-de-sacs**: you can always jump
   anywhere. These are NOT graph edges; ignore them when scoring composability.
   (Trap: a naive "count the `<a>` tags" audit counts these and concludes every
   page is well-connected. It isn't.)

2. **Contextual entity edges** — a link that **carries the current entity with
   you** (Procurement supplier → `/company?supplier=<that supplier>`). This is
   the real graph. Composability = the entity travels. A **contextual
   cul-de-sac** = you can leave the page via nav chrome, but you *cannot carry
   your entity* — you drop to a generic list and must re-search.

> The defect is **lost context**, not **lost navigation**. Score every edge by
> "does the entity travel?", not "is there an anchor?".

## Node inventory (entity → canonical page)

| Entity | Canonical page | Builder | Status |
|---|---|---|---|
| Member / TD | `/member-overview?member=` | `member_profile_url` | ✅ canonical hub |
| Supplier / company | `/company?supplier=` | `company_profile_url` | ✅ canonical |
| Division / vote | `/rankings-votes?vote=` | `division_url` | ✅ |
| Bill | `/rankings-legislation?bill=` | `bill_detail_url` | ✅ |
| Statutory Instrument | `/rankings-statutory-instruments?si=` | `si_detail_url` | ✅ |
| **Public body / authority / publisher / council** | `/body?buyer=` | `body_profile_url` | ✅ canonical (shipped 2026-07-23; the 3 drills funnel here, gated on the buyer_xref crosswalk) |
| Lobbying organisation | — | `?lp3_org=` in-page only | ❌ island |
| Judge / court | `?judge=` / `?court=` in-page only | — | island |
| Party · constituency · department · policy area · state board | — | — | island (likely honest leaves) |

**Update 2026-07-23 — the buyer node shipped.** The structural gap below (seller has
`/company`, buyer had nothing) is now closed: `/body?buyer=` is the buyer-side mirror,
resolving any register name via the `buyer_xref` crosswalk and showing the AWARDS and
PAYMENTS lanes as distinct never-summed grains. The three fragmented drills (procurement
`?authority=`, public_payments `?publisher=`, council_spending `?council=`) now carry a
gated "View this body's full dossier" hand-off to it. MVP scope: awards + payments lanes +
top suppliers (each linking back to `/company`, closing supplier↔body). Deferred: the
authority×CPV panel, competition/incumbency/live-tender/budget/NOAC enrichment, and a
canonical fix of the crosswalk's council name-form rows (the dossier tolerates them via
`buyer_core` normalisation). The deep dive below is kept as the original diagnosis.

## Defect register (verified 2026-06-20)

`V` = verified in live DOM. Severity: **H** primary subject, **M** secondary.

### False dead-ends — entity named, target exists, not linked
| # | Page | Entity (plain text) | Target | Sev | State |
|---|---|---|---|---|---|
| 1 | Public Payments | supplier (drill detail) | `/company` | H | **V → FIXED 2026-06-20** |
| 2 | Member Overview | bills sponsored | `bill_detail_url` | H | **V → FIXED 2026-06-20** |
| 3 | Member Overview | companies in interests | `/company` | H | open |
| 4 | Committees | roster members | `member_profile_url` | H | **FIXED 2026-06-20** (roster dataframe LinkColumn → profile) |
| 5 | Corporate Notices | firm in feed cards | `/company` (when matched) | H* | open (*needs supplier_norm join) |
| 6 | Constituency | council name | council page | H | **FIXED 2026-06-20** (standalone "Who runs this council →" links per serving council) |
| 7 | Committees | committee chair | `member_profile_url` | M | **FIXED 2026-06-20** (identity strip; register cards skip — nested anchor) |
| 8 | Legislation | bill sponsor (a TD/Minister) | `member_profile_url` | M | **FIXED 2026-06-20** (detail stat strip; bill list cards skip — nested anchor) |
| 9 | Procurement | authority in award rows | in-page `?authority=` | M | **by design** — the award row is already an `<a>` to its source notice (no nested anchors); the buyer is reachable via the now-linked **Buyer relationships** panel |
| 10 | Lobbying | politicians in targeted lists | `member_profile_url` | M | **FIXED 2026-06-20** (return-card politician names; the ranked "targeted" cards already navigate via `?lp3_result_pol=`) |

### Contextual return edges missing (one-way slides)
| Page | Has only | Missing | Note |
|---|---|---|---|
| Company | global nav chrome to Procurement/Public Payments | *contextual* "this supplier's awards/payments" return | **V**: my earlier "no link at all" was wrong — the only back-links are nav chrome that drops the supplier. |
| Corporate Notices | — | firm → `/company` (reciprocal of company→notices) | static |
| Member Overview lobbying | `?lp3_org=` (clicks *away*, loses member) | keep-in-context affordance | static |

## Deep dive — the supplier/money graph (richest payoff)

```
   SELLER side                              BUYER side
 ┌──────────────────┐                  ┌──────────────────────────┐
 │ /company  ✅      │   awards →       │ public body / authority /│
 │ supplier dossier │   ← payments     │ council  ❌ NO NODE       │
 └──────────────────┘                  └──────────────────────────┘
   ▲  ▲  ▲                               drills scattered across:
   │  │  └ corporate notices (static)     • procurement ?authority=
   │  └─── procurement ✅ (works)         • public_payments ?publisher=
   └────── public_payments ✅ FIXED       • council_spending ?council=
```

**Diagnosis:** suppliers compose because they have one canonical node. Public
bodies don't — a body's money story is shattered across Procurement (awards),
Public Payments (payments) and Council Spending (council lane), with no page
that unifies them. You can never answer "everything about how *this body*
spends" in one place. That is the app's deepest cul-de-sac — a missing *node*,
not a missing link.

**High-payoff build: a canonical `/body` (authority) page** mirroring
`/company`. It would unify, per body: awards published, payments made, council
lane (if applicable), top suppliers paid. Three wins fall out:
1. Creates the **reciprocal of `/company`** → supplier↔body becomes a true
   bidirectional loop (the core "overlaps flow into one another").
2. Collapses the three scattered buyer drills into one destination (fixes #9
   and the council island in #6).
3. Gives the now-fixed Public Payments edge a richer counterpart.

⚠️ **Data contract:** `public_payments_fact` (spend) and eTenders/TED (award)
are **never summable** — different grains. The `/body` page must show awards and
payments as distinct lanes, never a combined total. That's a view/contract
concern, not UI.

## Fix waves

- **Wave A — pure wiring (entity + target both exist):** #1 ✅, #2 ✅, then #4,
  #6, #7, #8. Highest value/effort; no data-logic change.
- **Wave B — contextual return edges:** Company → contextual procurement/
  payments; Corporate Notices → Company.
- **Wave C — new nodes:** `/body` canonical page (the payoff); matched-firm
  linking in Corporate Notices.

## Test method & artifacts

Live DOM on a freshly-restarted server (per `feedback_validate_fresh_server`);
the server idle-reaps on this box, so spawn detached via WMI
`Win32_Process.Create` and re-poll `/_stcore/health`. Note `localhost`
resolves to IPv6 here — use `127.0.0.1`.

- `audit_screenshots/_nav_graph_edges.py` — **the single live-DOM verifier**
  (2026-07-21). Drives each claimed edge on its CORRECT detail state and asserts
  present/absent. It subsumes the six exploratory 06-20 probes (four-claims test,
  chrome-vs-contextual model validation v1/v2, back-link characterisation, post-fix
  re-tests), now archived under `audit_screenshots/_archive/_nav_graph_*.py` — the
  model narrative they proved lives in "The two edge classes" above.

⚠️ **Two harness traps the 07-21 re-verification exposed — both were false
"mismatches", not app regressions.** State-indexing is subtler than "hit the detail
page": some edges are *further* gated, and a blind first-candidate seed lands on an
ineligible entity.
1. The `member → bill` edge lives behind the `?section=legislation` router
   (`member_overview.py:_section_legislation`) — the default overview never shows it,
   so drive `?section=legislation`. And a member with no bills in their own section
   (e.g. seeded off an old bill's sponsor) is a legitimate absent → iterate sponsors.
2. The `public-payments → company` CTA is **registry-gated** (`public_payments.py:523`,
   `supplier_norm in _awards_register_norms()`): most paid suppliers are payments-only
   and get *no* link, by design (never a false hand-off). Seed suppliers from the
   Procurement landing (awards-registered) or the edge reads as broken when it isn't.

Convention going forward: score edges by *does the entity travel?*. The static ratchet
`tools/check_nav_graph.py` (fast suite, `test/tools/test_nav_graph.py`) flags any known
entity column (`bill_id`, `supplier_normalised`, `unique_member_code`, `vote_id`,
`si_id`, `contracting_authority`) rendered without a matching `entity_links` helper.
Detection is AST-level so comments/docstrings don't false-positive; a page that IS an
entity's canonical destination is exempt; accepted drops are grandfathered in BASELINE.
