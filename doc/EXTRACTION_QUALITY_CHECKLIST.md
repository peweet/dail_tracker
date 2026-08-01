# Extraction quality checklist

**Read when:** starting ANY new extraction/ingestion (new source, new corpus, new classifier
over extracted text). Enforced deterministically by `tools/check_conventions.py` [R12]: an
extraction output directory must ship a quality report containing the section markers below.
Distilled from the 2026-07-31 literature passes + the council-minutes build (2026-08-01);
sources in memory: `reference_verification_calibration_literature_2026_07_31`,
`feedback_source_fidelity_audit_method`, `feedback_proving_absence_needs_broad_search`.

## The report an extraction must ship (section markers R12 checks)

### `## Completeness`
Correctness gates (clean/quarantine) say what you captured is good; they say nothing about
what you MISSED. State the expected universe (the denominator) and where it comes from:
meeting cadence × councils × years, a register's published row count, a sitemap, an index
page. If the denominator is assumed rather than checked, band it `[Indicative]` and say so.
"Docs harvested" alone is not completeness.

### `## Recall`
A rule/regex classifier has unknown recall until measured. Either report measured per-class
precision AND recall against a labeled set (accuracy alone hides rare-class failure), or
write "recall unmeasured" explicitly. Proving a class is absent from a doc needs a BROAD
search (looser nets, synonyms, section-number-free phrasings), not the absence of a strict-
pattern hit.

## Labeling protocol (when building the golden set)
- **P(True) phrasing**: verifier prompts ask "is this specific item an X — true?" per item,
  never "rate your confidence in these labels" (Kadavath 2022 — calibration holds in that
  format only).
- **Independent verification** (CoVe): the verify pass reads the RAW text fresh; it never
  sees the draft labels it is checking.
- **External oracle only** (Huang ICLR 2024): no self-review loops. Oracles here: the source
  document's own text, a query result, the user's spot-check, cross-channel consistency
  (e.g. attributed votes vs the minutes' own Result line).
- **Include verified negatives** per class — recall is unmeasurable without them.
- Fewer than ~5 checked examples behind a structural claim → the claim stays `Indicative`
  and says its sample size (evidence.md).

## When a model tier earns its place (measured across two domains, 2026-08-01)
Rules stay tier 1 always — they are provenance-clean and precise by construction. Add a
model tier ONLY when both hold:
- **Labeled volume**: tens of thousands of rule-labeled rows to train on (diaries: 90k →
  93% top-of-queue precision; minutes: hundreds/class → 0–50% on confounded classes).
- **Miss shape is noise-variants, not vocabulary-confounds**: OCR mangling ("Government
  Rusiness", "1eaders Questions") is an unbounded variant space rules cannot enumerate —
  the model absorbs it. If misses share vocabulary with a DIFFERENT class ("Chief
  Executive" in attendance headers), the model learns the publisher, not the class.
Ship it as a **two-tier classifier**: rules first; model only on the rules' residual,
behind margin gates **calibrated by band-sampling** (precision collapses at low margins —
sample each band before trusting it; noisy classes get higher gates); a
`*_source` column ("rule" | "model") so provenance survives to the UI; model import
guarded so its absence degrades to rules-only. Worked example:
`extractors/diary_entry_classify.py` `model_fallback()` + `test_diary_classify.py`
invariants test.

## Mechanics (existing repo rules, restated for one-stop reading)
- Quarantine with a reason code, never drop silently; re-triage buckets when tooling
  improves (winocr recovered 74/91 docs the day it landed).
- Declare every new dependency in pyproject the day it is first imported — `uv sync` prunes
  undeclared packages (camelot 07-20, winocr 07-31, scikit-learn 07-26).
- OCR output is `Extracted`-band; derived claims inherit `min()` of their inputs.
- New HTTP fetching goes through `services/http_engine` (R1); a WAF 403 may mask a 404 —
  verify the resource exists before blaming the fetch.
