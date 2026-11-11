---
tier: PLAN
status: LIVE
domain: infra
updated: 2026-11-11
supersedes: []
read_when: assessing or resuming the approved offline extraction and CI failure-triage pilot
key: PLAN|LIVE|infra
---

# Linnaeus-style failure-triage pilot

The owner approved this bounded pilot on 11 November 2026. Its outcome is a measured
decision about internal troubleshooting assistance. The first stage can conclude that
there is insufficient evidence to justify model work. Approval does not establish that
retained logs have reviewed labels or that a classifier will save time.

## Observable seam and scope

Given a retained public extraction or CI incident, suggest a failure family, show the
supporting operational evidence and retrieve relevant previously reviewed incidents.
Return `unknown` when the evidence or validated model coverage is insufficient.
The original failure, exit status and recovery authority remain the source of truth.

Start with public pipeline step records and their retained logs. The pilot is offline:
no production integration, automatic repair, status rewriting, customer-document input,
paid model calls or new external log processor. Preserve unrelated checkout changes.
Use the existing scikit-learn dependency for the initial comparison.

## Stage 1: opportunity and evidence inventory

Inspect known operational paths, with explicit file-count and byte limits. Record the
observation time, source identity, date range, parse/coverage errors and omissions.
Count runs, manifests, failed steps, available diagnostic files and reviewed incidents
separately. Pipeline elapsed time is not operator diagnosis time. Several failed steps,
retries or repeated signatures may represent one underlying incident.

Look for independently reviewed cause/resolution labels, uncertain cases, review
provenance and operator effort. A field called `error` or a rule-generated category is
not a reviewed cause label. Never infer the size of a supervised corpus from log volume.
Do not load raw operational text into an agent transcript; inspect schemas and bounded
aggregates first. A metadata inventory does not establish diagnostic log quality.

Proceed to supervised comparison only when reviewed independent examples and a useful
manual-work baseline are available. Otherwise retain an `insufficient-evidence` result,
state exactly what was searched, and stop model implementation. This is an evaluated
feasibility outcome, not a rejected algorithm or evidence of no future use case.

## Stage 2: reviewed corpus

Use a small, explicitly versioned label guide. Initial families may include source
transport/access, environment/bootstrap, parsing/schema, application/test and resource
admission. Keep uncertain, mixed-cause and unsupported examples as `unknown`. A blocked
operation that launched no child is distinct from a failed child process.

Review against retained operational evidence and the confirmed resolution where one
exists. Record reviewer identity/role, evidence sufficiency and disagreements. Include
verified negatives and examples from the rules' unresolved residual. Independently
reviewed labels are required; rule predictions and a model's own outputs are not an oracle.

Group retries and incidents with the same evidenced underlying cause before allocating
splits. Fingerprints alone do not establish cause identity. Uncertain group assignments
stay out of the confirmatory test set. Preserve traceability pointers separately from
features. Exclude identifiers, paths containing identities, URLs with query credentials,
commands, secrets, resolution text and later diagnosis from features. Audit free text
and retention before use; incomplete or truncated context needs an explicit sufficiency
decision. Redaction is not permission to expand collection or retention.

## Stage 3: offline comparison

Compare existing deterministic rules/signatures, TF-IDF similar-incident retrieval,
TF-IDF logistic regression and TF-IDF LinearSVC. Retrieval can reference only training
incidents and their reviewed resolutions. Do not index validation or test incidents.

Fit vocabulary, weighting, preprocessing and learned feature selection on training data
only. Reserve later incident groups for validation and a final untouched test set; keep
whole groups in one partition. Remove groups spanning time boundaries or allocate them
wholly before choosing cutoffs. Report novel families rather than forcing them into a
known category. Freeze the dataset, split, feature policy, model parameters, random seed
and package versions. Bound training workers, wall time and memory.

Choose per-class abstention thresholds on validation data. Preserve raw SVM scores as
decision margins; probability claims require separate held-out calibration evidence.
The model is a candidate router, not proof of root cause.

## Stage 4: evaluation and decision rules

Report per-class precision/recall, confusion counts, accepted-suggestion precision,
coverage of eligible unresolved incidents, abstention and novel-family error rates.
Publish numerators, denominators and uncertainty, including the excluded/unknown cases.
Compare against the strongest baseline at comparable coverage and precision.

The approved starting targets are 95% accepted-suggestion precision, 30% coverage of
eligible unresolved incidents and 20% less diagnosis time. These are trial criteria,
not observations. Before a confirmatory test, freeze the supported classes and require
at least 60 independent accepted test incidents per supported class, with a one-sided
95% Wilson lower precision bound of at least 95%. Unsupported classes abstain. Report
the overall and per-class bounds; insufficient counts make the result inconclusive.

Measure operator effort in a randomized, matched review exercise. Balance review order,
use independent incident groups, and avoid giving one operator the same incident twice.
Record diagnosis correctness, time, wrong turns and reviewer disagreement. Pre-register
the timing sample/power calculation after the baseline variance is observed and before
the held-out exercise. Report the uncertainty of the paired time improvement; a median
alone does not establish savings. Include labeling and maintenance effort in the value
assessment. Missing operator-time measurements prevent a time-saving claim.

## Outputs and continuation

Retain the inventory receipt, reviewed label guide/corpus manifest, split manifest,
reproducible comparison, errors and timing report, and a decision of `proceed`, `hold`
or `insufficient-evidence`. Operational inputs and reviewed incidents remain in ignored
local storage; publish only reviewed non-sensitive aggregate findings.

A successful offline comparison can justify an advisory shadow trial. Keep evidence
links and human corrections visible, freeze each model release and review drift. A new
runtime service or dependency adoption requires a separate measured justification.

## SVM and library choice

`extractors/diary_entry_classify.py` already implements a rule-first LinearSVC residual
fallback with provenance and class-specific margin gates. Its training labels are
rule-derived, so the code path does not establish independent precision. Existing
council-minutes SVM exercises also need an independently reviewed test set before their
scores can support adoption.

LinearSVC is the first SVM candidate for sparse text. It uses LIBLINEAR. scikit-learn's
SVC already exposes LIBSVM; a separate LIBSVM installation adds no demonstrated capability
for this pilot. Try a kernel SVC only after a measured linear-model gap justifies its
extra tuning and resource cost. Native deployment or interoperability needs must be
concrete before adding a direct binding.

## References

- [Linnaeus experience paper](https://arxiv.org/abs/2103.06927)
- [LIBSVM source and documentation](https://github.com/cjlin1/libsvm)
- [scikit-learn SVM implementations and scaling](https://scikit-learn.org/stable/modules/svm.html)
- [Extraction quality and independent labeling](EXTRACTION_QUALITY_CHECKLIST.md)
- [Pipeline run schema](../manifest.py)

## Execution record

The initial inventory and its dated evidence are recorded in
[`tools/evals/LINNAEUS_TRIAGE_INVENTORY.md`](../tools/evals/LINNAEUS_TRIAGE_INVENTORY.md).
Model comparison remains conditional on the Stage 1 gate.
