# Public decisions seed registry

**Status: SEED LIST ONLY — nothing here is ingested.** Sandbox per the repo rule
(experiments live in `pipeline_sandbox/`); promotion of any row is a separate,
user-gated decision.

`seed_registry.csv` — 71 candidate corpora of decision-bearing public documents
(minutes, rulings, inspection reports, funding decisions, auction results, roll-call
votes), Ireland plus the EU tier where an Irish person/org/money overlap exists.
Compiled 2026-07-20 from the RHF West probe session (memory:
`project_public_minutes_landscape`); second sweep same day added energy, water and
accountancy-oversight clusters with endpoint checks on the least-certain rows.

**Pattern from the second sweep:** commercial semi-state boards (ESB, Uisce Éireann)
and standards committees (NSAI/CEN) are closed — verified, not assumed. For those, the
public paper trail is the *oversight layer around them*: WAB quarterly reports, CRU
decisions, IAASA inspections, EPA enforcement, RESS auction results. Watch the
watchdogs, not the boardrooms.

**Pattern from the third sweep (semi-states/NGOs/sport/private):** publication follows
**accountability pressure, not ownership**. The NTA — a statutory authority — publishes
board minutes (2013–2025, pre-redacted) while commercial ESB doesn't; the IHRB and GAA
DRA — private bodies — publish quasi-judicial rulings because they operate as
tribunals; SCEP publishes per-club allocations in CSV because grant allocation demands
it. Corollaries: (a) never write a body off by sector — the consolidated semi-state
row says check per body; (b) for members' associations (GAA) the tractable record is
the *public money edge* (SCEP, Sport Ireland conditions), not internal governance;
(c) some rows need no ingestion at all — the An Taisce row is a **derived lens** over
the planning corpus we already hold.

## Epistemic status — read before trusting any row

- `probed_high_yield` (1 row): verified this session against a real document
  (c:/tmp/minutes_probe/FINDINGS.md).
- `candidate_verified_endpoint` (7 rows): publication endpoint confirmed by search
  2026-07-20 (IAASA, RESS results, WAB, NTA minutes, SCEP allocations, IHRB
  referrals, GAA DRA) — content/format still unprobed.
- `shipped_overlap` (5 rows): already ingested by this repo; listed only to mark the
  pattern precedent.
- `blocked_likely` / `blocked_confidential` (4 rows): closure verified or near-verified
  (ESB board, Uisce Éireann board, NSAI committees; NAMA long-standing).
- `candidate` (everything else): **publication status, format and URL are unverified**
  — `source_hint` is a starting point, not a confirmed endpoint. The HSE probe showed
  why this matters: the legacy hse.ie PDF paths silently return HTML now; only the
  about.hse.ie API serves documents.

## Columns

`person_key` — which person spine the corpus joins (councillors / appointees / TDs /
MEPs / judges / none). Rows joining an EXISTING spine rank higher: the novel product
is the cross-corpus person record ("the appointee record"), not any single corpus.

`money_link` — which money grain the decisions touch. Never-sum rules apply at query
time, not here.

`joins_existing` — the repo dataset a row would join. Empty join surface = weak
candidate regardless of corpus quality.

`priority` — 1: person-keyed to a spine we hold AND tractable. 2: strong single-source
or strong join. 3: real but heterogeneous, low-grammar, or PII-laden.

## Ranking logic (why the top rows are top)

Priority 1 clusters around one insight from casting wide: **serving Irish councillors
appear in four corpora beyond their council** — Regional Health Forums, Regional
Assemblies, the EU Committee of the Regions delegation, and some university/ETB
boards. Each is a roster join we can already do (74% naive match in the RHF probe;
surname_key closes most of the rest). The EP roll-call row is the same move for the
14 MEPs. That cross-corpus person record is the thing nobody else in Ireland has.

## Verification protocol for promoting a row

1. Confirm the publication endpoint serves documents (not an HTML shell).
2. One-document probe: born-digital vs scanned, decision grammar, ID scheme.
3. Person-key sample join against the relevant spine.
4. PII pass (WRC/Ombudsman rows especially).
5. Then — and only then — a row graduates to a scoped extractor proposal.
