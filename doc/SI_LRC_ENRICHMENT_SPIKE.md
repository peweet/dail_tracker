# SI LRC Enrichment — Verdict Spike (2026-06-04)

Sandbox spike answering: **is the LRC Classified List a meaningful SI enrichment,
or overkill?** Implements PR1 of `doc/si_lrc_enrichment_claude_brief.md` end to
end (source → ingest → clean → map → measure → test), gold untouched.

## Verdict: ship the PR1 core. The numbers clear the bar.

| Metric | Result | Read |
|---|---|---|
| **Match rate** (our SIs that get an LRC subject) | **90.1%** (5,335/5,924) | well above the 90% "worth it" bar |
| Match rate, recent years | 96–100% (2022–26), 82–83% (2016–18) | strongest where traffic is |
| **Gap fill** — SIs with NULL `si_policy_domain` that LRC classifies | **816 of 972 (84%)** | the clearest data-quality win |
| Taxonomy granularity | 36 subjects + 251 leaves (matched) vs 18 existing domains | a genuinely finer lens |
| Multi-subject SIs | 0.3% (18) | minor; don't oversell |

Source is **fully server-rendered HTML** (no JS, no OCR) — 36 pages, ~19,536
distinct SIs, "Updated to 1 June 2026". Ingest is deterministic from a bronze
cache. The eISB ELI href (`/eli/YYYY/si/N`) gives the number/year key; gold's
`(si_year, si_number)` is unique (0 dup keys), so the 90.1% is honest.

## Clear win #1 — data quality (fills empty topics)
816 SIs that have **no topic at all today** gain a subject. Examples:
- *Referendum Commission (Establishment) Order 2019* → Election and Referendum Law
- *Employment (Miscellaneous Provisions) Act 2018 (Commencement)* → Employment Law
- *European Union (… Tobacco …) Regulations* → Health and Health Services

## Clear win #2 — search/browse (finds SIs whose titles hide the topic)
A citizen searching free text misses SIs whose **title never contains the topic
word**. LRC subject/subheading browse catches them:
- **Rented housing**: title "rent" → 93 hits; the *Residential Tenancies* leaf
  surfaces **173 more** (commencement orders etc. that never say "rent").
- **Fishing**: title "fish" → 101; LRC adds **58** (Merchant Shipping, Inland
  Fisheries, Sea Fisheries) — e.g. *Wild Salmon and Sea Trout Tagging Scheme*.
- **Dogs**: *Welfare of Greyhounds Regulations* found via the Dogs leaf.

(Caveat on the sim: broad leaf terms like "Health"/"Equality" produced false
positives for the tobacco/disability probes — those rows are a query artefact,
not an LRC win. The Tenancies/Fisheries/Dogs wins are real.)

## Data-quality findings to handle on promotion
- **120 duplicate occurrence rows** (same SI+subject+path) — collapsed in the builder.
- **2 catch-all leaves** are weak browse pages: "ECA Section 3 Statutory
  Instruments" (1,507) and "General" (691). Builder down-ranks them so a specific
  leaf wins as `primary_leaf`; UI should de-emphasise them as landing pages.
- **52 of 251 leaves hold a single SI** — too thin to feature; browse on subjects
  + populated leaves only.
- 3.8% of LRC `li` entries have no parseable number/year (EU-reg / format noise) —
  dropped from the keyed table.

## Legal safety (locked by tests)
Status vocabulary is **`matched_classified_list` | `not_matched`** only — never
`in_force`/`valid`/`official_status`. "Not matched" ≠ "not in force". Matched
rows carry a caveat + `exact_number_year` method + confidence 1.0. The
redundant LRC "in-force" flag is **deliberately not surfaced** — `si_current_state`
remains the sole legal-state layer.

## Artifacts (all sandbox, gold untouched)
- `pipeline_sandbox/si_lrc_classlist_extract.py` — fetch+parse, bronze cache + provenance (`--offline`/`--refresh`)
- `pipeline_sandbox/si_lrc_analyze.py` — the verdict analysis
- `pipeline_sandbox/si_lrc_topic_sim.py` — integrity + topic-search simulation
- `pipeline_sandbox/si_lrc_enrichment_build.py` — deterministic one-row-per-SI summary + coverage JSON
- `pipeline_sandbox/_lrc_output/si_lrc_enrichment_summary.parquet` (5,924 rows)
- `test/test_si_lrc_enrichment.py` — 11 tests (parse contract + legal-safety invariants), all pass
- `data/bronze/lrc_classlist/` — 36 cached pages + `.meta.json` provenance

---

# Second pass (2026-06-04) — deeper profiling + dual-source cross-validation

Scripts: `si_lrc_explore2.py`, `si_lrc_discrepancy.py` (sandbox, read-only).

## Headline upgrade: 99.3% coverage of *in-force* SIs
The raw 90.1% understates the win. The "misses" are overwhelmingly revoked SIs
the LRC in-force list rightly omits:
- **89% of the 589 unmatched SIs are `revoked`** per `si_current_state` (523/589).
  Only **31 in-force SIs** genuinely lack an LRC subject + 20 with no state info.
- Among the **4,214 SIs that are currently in force, LRC classifies 4,183 = 99.3%.**

So LRC non-match is a *coherent signal* (spent/revoked), not parse failure.

## Dual-source cross-validation (the integrity win)
LRC lists only "in-force" legislation; `si_current_state` (eISB Directory) tracks
revocations. Crossing them:
- **4,183 SIs corroborated** by both sources (in_force_as_made AND LRC-listed).
- Combining the two sources, **only 20 of 5,924 SIs are in neither**.
- **BUT: 56.2% of revoked SIs (672/1,195) are still listed by LRC as in-force.**
  64% of those are EU sanctions/food-control SIs in the "ECA Section 3" catch-all
  (e.g. *EU (Restrictive Measures Concerning Ukraine) Regs 2026*, revoked by
  S.I. 96/2026 at confidence 0.92, yet still LRC-listed). Non-EU revoked lags too.

**Consequence (locks the design):** LRC's "in-force" listing is NOT a reliable
legal-status signal — it materially lags revocations. `si_current_state` is the
sole authoritative legal-state layer. The UI must NOT surface LRC "listed = in
force", and should suppress that implication for any SI we already know is
revoked. This empirically confirms the brief's instinct and the spike verdict.

## What gold already holds (null profile) — scopes future enrichment
- `si_policy_domain` 16.4% null → LRC fills 84% (the PR1 win, confirmed).
- `si_parent_legislation` **54.7% populated** (the SI→Act link half-exists already).
- `si_department` **56.8% null** — a separate, larger gap (LRC dept notation for
  SIs unverified; not pursued here).

## SI→Act link (brief Source #2) — second-tier, murky; do NOT prioritise
- Classlist associates SIs with a parent Act by **document proximity** (Acts live
  in `<table class="acts"><tr id="actN">`, SIs in a separate `<li id="siN">`
  list) — fuzzier than the clean subheading nesting.
- Of the 2,686 NULL-parent SIs, only **21% have the parent Act in their title**;
  the other 2,114 are mostly **EU regulations** (parent = European Communities
  Act 1972, low civic value to surface).
- Net: SI→Act is real but diluted and fragile → reinforces **skip PR3**.

## Parse note (no action needed)
The 797 unparsed LRC entries are **pre-1948 "S.R.& O." statutory rules & orders**
(1930s–40s, with `[Vol. IX p. 695]` citations) — entirely outside our 2016–26
gold window, so zero impact on match rate. Extending the regex would only add
historic instruments we don't hold.

## Recommended next steps (only if promoting)
1. Add `sql_views/legislation_si_lrc_enrichment_summary.sql` reading the summary; LEFT-JOIN into `v_statutory_instruments` (additive, never overwrites domain/state).
2. UI: subject chip + populated-leaf browse on the SI page; caveat box; eISB link. De-emphasise catch-all leaves.
3. **Skip PR3** (Revised Act annotation parsing) until a concrete need appears — highest maintenance cost, lowest civic payoff.
