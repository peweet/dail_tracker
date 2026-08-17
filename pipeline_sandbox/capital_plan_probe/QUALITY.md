# HSE Capital Plan 2026 — feasibility probe for PublicSignal

**Date:** 2026-08-17 · **Status:** SANDBOX PROBE. Nothing promoted, no parquet, no product code touched.
Follow-on from `doc/PUBLISHED_MINUTES_AND_CAPITAL_PIPELINE_REGISTER.md` §3, testing whether the
document actually parses into (project, stage, €) rows before proposing anything to PublicSignal.

Source: [HSE Capital Plan 2026](https://about.hse.ie/publications/hse-capital-plan-2026/),
PDF mirror `https://www.drugsandalcohol.ie/45231/1/HSE_Capital_Plan_2026.pdf` (82pp, 6.8MB,
cached at `corpus/HSE_Capital_Plan_2026.pdf`).

## Finding 1 (`hse_capital_plan_probe.py`) — the narrative bullets are NOT project-level triples

The register doc's "project-level with euro values" characterisation was drawn from two hand-picked
quotes, not a full-document measurement. Measured over all 4,913 text lines: 48 lines carry a euro
figure, of which only **2/48 (4.2%)** carry a stage keyword on the same line, and **8/48 (16.7%)**
within a ±2-line window. Inspecting those 8 shows most pair TWO DIFFERENT bullets (a euro-figure
bullet for one project sitting next to a stage-update bullet for an unrelated project in the same
"In 2026 we will:" list) rather than one project's own cost+stage. **Euro figures here are mostly
programme-level** ("Invest €173.04m in construction of public residential care" — a whole
programme, not one named scheme). Do not build a triple-extractor against this section.

## Finding 2 (`hse_appendix_parse.py`) — Appendices 1/2/2A/3/4 are a real project register

Pages 54-79 hold a repeating 4-field block (Capital Reference No. / Facility / Brief Project
Description / Status) that is NOT narrative — it parses cleanly once the actual status vocabulary
is used (discovered empirically, not assumed): `Construction`, `Complete`, `Tender`,
`Detailed Design`, `Design Feasibility`, `Appraisal`, `Appraisal - AG 0`.

**614 project rows, 597 (97.2%) carry a matched status.** Breakdown:

| Appendix | Rows | Status counts |
|---|---|---|
| 1 — Construction Phase | 202 | Construction 116 · Complete 41 · **Tender 34** |
| 2 — Design Phase | 193 | Detailed Design 90 · Design Feasibility 68 · Appraisal 25 · Appraisal-AG0 8 |
| 2A — Appraisal | 156 | Appraisal 82 · Appraisal-AG0 53 · Design Feasibility 11 · Detailed Design 4 · Construction 2 · Complete 3 |
| 3 — Disability Design/Construction | 56 | Detailed Design 15 · Design Feasibility 20 · Construction 10 · Tender 4 · Complete 4 |
| 4 — Climate Action | 7 | Detailed Design 4 · Construction 3 |

**38 rows (Appendix 1 + 3) are at `Tender` status right now** — named facility, one-sentence
description, a stable HSE Capital Reference Number (e.g. `11188A`, `10008F`) that is almost
certainly a durable cross-year key, since it looks like an internal capital-project ID rather than
a plan-year-scoped code. Sample: `10008F | Beaumont Hospital | Radiation Oncology - Phase 2
Facilities incl. Equipping | Tender`.

**Structural gap, matches the MyProjectIreland finding: no euro value at project grain.** Price only
exists in the narrative section (Finding 1), at programme level, and does not join back to these
rows by any field present here. Anything wanting a price per Appendix row needs a separate source
or stays priceless, same caveat as the national Feature Service.

**Known parse artifact (minor, not fixed here):** a small number of facility names wrap across two
PDF lines (`Mater Misericordiae University` / `Hospital`) and get glued into the description field
by the naive line-join. Fixable with a facility-name gazetteer or a second wrap-detection pass; not
attempted in this probe.

## Completeness

Measured over the parsed appendix register (`hse_appendix_parse.py`, Findings 2): **614/614
detected 4-field blocks parsed into rows; 597/614 (97.2%) carry a matched status** against the
empirically-discovered vocabulary; the remaining 17 rows parse but hold no recognised status
token. At project grain, euro values are 0/614 by structure (price exists only at programme
level in the narrative — 48 euro-bearing lines of 4,913, Finding 1). No row was dropped for
being malformed; the known defect is field-boundary bleed (wrapped facility names glued into
the description), not row loss.

## Recall

**Not measured.** No independent ground-truth count of the appendix entries was established
(no manual page count of the 26 appendix pages, no second parser), so rows the block detector
missed entirely are invisible to this probe. The wrap artifact above shows the detector
tolerates two-line names by gluing rather than splitting, which suggests silent row loss is
the less likely failure shape — but that is inference, not measurement. Before promotion,
recall needs a manual count of at least two appendix pages against parser output.

## What this changes about the §3 recommendation

The original register doc ranked "HSE Capital Plan" as a single 82pp text-layer PDF with occasional
priced highlights. It is actually two different sources bound in one PDF: a thin, sparse narrative
(bad for extraction) and a genuinely clean 614-row project register with a 97.2%-populated status
field and a stable per-project ID (good for extraction, no price). The register is closer in shape
to the MyProjectIreland Feature Service (named projects, closed-vocabulary lifecycle status, no
price) than to a priced capital plan — but sector-scoped to health, richer per-project description
text, and with an ID that plausibly tracks a project across plan years, which the Feature Service's
`Eircode`-sparse rows do not offer.

## RESOLVED — year-over-year stability check (`hse_multi_year.py`, 2026-08-17)

**The Capital Reference No. IS stable across plan years.** Fetched the [HSE Capital Plan
2025](https://www.drugsandalcohol.ie/43044/1/HSE_Capital_plan_2025.pdf) and re-ran the same
appendix parse (heading format differs year to year — 2025 says bare `"Appendix 1"`, 2026 says
`"Appendix 1 - Health Projects in Construction Phase"` — `find_appendix_ranges()` handles both).

**430/553 (77.8%) of 2026's rows share a ref_no with a 2025 row** [Verified —
`hse_year_over_year_diff.json`]. Of those 430, 34 have a status on only one side (parse gap, not a
real signal — excluded) and 240 show no change; **156 show a genuine stage transition**. 20 of those
moved INTO `Tender` this year from an earlier stage (mostly `Detailed Design → Tender`, a few
`Appraisal → Tender`) — e.g. `10008F | Beaumont Hospital | Radiation Oncology Phase 2 | Detailed
Design → Tender`. Full transition matrix in `hse_year_over_year_diff.json`; clean per-year rows in
`hse_projects_2025.jsonl` / `hse_projects_2026.jsonl` (supersede `hse_appendix_rows.json`, which
used hardcoded 2026-only page ranges — `hse_multi_year.py` auto-locates appendix ranges per
document and is now the authoritative parser).

**This is the leading indicator the source was worth checking for.** A project the 2025 plan showed
in Detailed Design and the 2026 plan shows in Tender is visible in the capital plan roughly a year
before it would surface as an eTenders/TED notice — assuming it does reach market via those
channels, which was not checked here (that's the backtest the original register doc recommended;
still not run).

## Checked and RULED OUT this session — the other capital plans do not share HSE's structure

The register doc's §3 table implied DCC, UCC, Dublin Port and daa all give project-level, priced
capital plans similar to HSE's. Fetching and probing the current documents does not support that
for the two checked directly:

- **DCC Capital Programme 2026-2028** ([PDF](https://www.dublincity.ie/sites/default/files/2026-04/dublincitycouncil_capitalprogramme_2026-2028-web.pdf),
  98pp) is narrative prose grouped by Programme Group, not a per-project table. No repeating
  field structure, no stable per-project ID, and per-project euro figures are stated inline in
  prose only occasionally (e.g. "€59m" for the Public Lighting Infrastructure project). **The
  register doc's own example — "Bluebell Phase 1 | €7.39m/€14.78m/€14.78m | Total €36.96m" —
  does not appear anywhere in this document**: 0 matches for `7.39`, `14.78`, or `36.96` across
  all 98 pages [Verified — full-text search, this session]. Bluebell is named 3 times, always as
  a bullet-point project name or in narrative, never with those figures. Either that quote came
  from a different DCC document (an earlier edition, a councillor briefing table) or the table
  it cites has since been dropped — the register doc's citation needs correcting, not reused.
- **UCC five-year capital plan**: no downloadable PDF exists. It is a
  [news-release page](https://www.ucc.ie/en/news/2026/ucc-publishes-five-year-capital-plan.html)
  naming 8 projects in prose (Tyndall €130m, Cork University Business School €60m, Dentistry
  €20m+, Sports €17m+, Digital infrastructure €30m+, plus three with no figure) [Verified — WebFetch
  of the page, this session]. Real, named, priced — but a one-time announcement on a webpage, not
  a recurring document with a per-project ID. Worth a manual note somewhere, not worth building an
  extractor for 8 rows with no update mechanism.
- **Dublin Port Masterplan 2040 / daa CIP: not fetched this session** — the Dublin Port document
  found by search is a spatial/strategic masterplan last reviewed 2018 (a different genre from an
  annual capital-allocation plan), which combined with the DCC/UCC results made a third narrative
  confirmation low-value. Treating the register doc's original "named sub-programmes, not a
  register" read as sufficient rather than re-verifying. **This is an n=3 sample (HSE, DCC, UCC)
  generalising to "annual statutory capital plans have this structure, masterplans/press releases
  don't"** [Indicative — pattern held 3/3, not re-checked against Dublin Port or daa directly].

## What this changes

HSE looks structurally distinct among the sources checked, not just the best of a similar set: it
is the one **annual, statutory, appendix-formatted capital plan** among these five. The DCC/UCC/
Dublin Port/daa documents are a masterplan, a programme-group narrative and a press release
respectively — none share the repeating ref-no/facility/status block that makes HSE both parseable
and diffable year over year. If this pattern holds, the next-highest-value move is not more capital
*plans* in this list, but checking whether **other bodies publish the same genre of document** —
an annual statutory capital plan with a numbered project appendix — rather than re-probing sources
already shown to be narrative.

## PRECISION AUDIT (`hse_precision_audit.py`, 2026-08-17) — gate item 1 of 4, CLEARED for this subset

Independent check of the 20 `→ Tender` transitions, not reusing the sequential parser's row
logic: located every raw occurrence of each `ref_no` as an exact standalone line in both years'
PDFs and read the surrounding text myself, rather than trusting another regex to grade the first
one. **All 20 ref_nos occur exactly once per year** — no collision with a cross-reference mention
(the document does inline-cite ref numbers inside prose elsewhere, e.g. "enabling work for Cap
Ref #11643", but never as a standalone line, so `REF_RE`'s exact-line match structurally avoids
that trap). **20/20 read as genuine, coherent same-project matches in both years** — facility and
description evolve in expected, human ways (added detail, shortened boilerplate, a generic
placeholder name replaced by a real site name once one existed — `14162` went from
"Decongregation Residence at Killygordon" in 2025 to "Ballinacor, Killygordon, Lifford" in 2026),
never garbled or ref_no-mismatched. Full raw dumps in `hse_precision_audit_raw.json`
[Verified — manual read of independently-generated context windows, this session].

**Two things the audit surfaced, disclosed rather than fixed:**
- **A real status-vocabulary gap.** `"Review"` appears as a status token in the source (row
  `11671`) and isn't in `STATUS_VOCAB`, so that row (not one of the 20 audited) parses with
  `status: null`. Quantified across the whole document: **50/~600 rows unmatched in the 2025
  plan, 20/~600 in 2026** [Verified — rerun with a miss-counter, this session] — a modest,
  disclosed completeness gap, not a correctness one; none of the 20 audited rows hit it.
- **Facility name is not a safe cross-year join field** (the `14162` example above) — confirms
  `ref_no` is the only field that should be used to link a project across plan years, which the
  extractor already does.

**What this does and does not clear.** Precision is now measured, not assumed, for exactly the
highest-stakes subset (the rows that would actually render as a live lead). It is **not** a
full-set audit — the other ~576 rows outside this transition set were not read against source.
One layer stays unaudited even for these 20: I re-extracted text with fitz rather than visually
checking a rendered PDF page, so a systematic fitz mis-read of a specific word is not ruled out
(low-risk on a text-layer PDF with this much internal repetition, but not zero).

## Not done in this probe (deliberately, per the PublicSignal new-source gate)

No product code touched, nothing written outside `pipeline_sandbox/`, no `data/gold/parquet`
(this is a Dept. of Health document; its reuse licence has not been checked — same open question
flagged for MyProjectIreland in the register doc), no LLM call, no promotion, no backtest against
eTenders/TED award data (the check that would confirm Tender-stage HSE rows actually reach market
through those channels). All of that is an owner decision, not an implementation one.
