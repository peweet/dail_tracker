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

## Capture reviewed incidents and effort

The local metadata CLI is
[`tools/evals/incident_triage.py`](../tools/evals/incident_triage.py). It prepares the
missing baseline; it does not train a model or establish corpus readiness. Run from the
public repository root:

```powershell
python tools/evals/incident_triage.py template --output logs/linnaeus-pilot/incident-draft.json
python tools/evals/incident_triage.py summary
```

The output directory must exist. The template refuses to replace an existing file.
It starts as `synthetic`, `unreviewed`, with unknown family and null incident time and
effort. Replace `incident_id` with a unique opaque alias, set the actual timezone-aware
`incident_at`, and select the actual workflow and operation state. Set `operational`
only for a real observed incident. Keep blocked/no-child operations distinct from
failed operations; an exception signature alone does not settle the cause.

All fields are required, but nullable fields preserve missing evidence. Version `v1`
uses this initial label guide, subject to review against the retained evidence:

| Family | Evidence needed to distinguish it |
| --- | --- |
| `source_access` | Source transport or access problem, supported by source/request evidence. |
| `environment` | Local dependencies, configuration or bootstrap prevented the operation. |
| `parsing_schema` | Observed input structure and parser/schema mismatch explain the failure. |
| `application` | Application or test logic defect supported by investigation. |
| `resource_admission` | Resource limits, capacity or admission controls prevented progress. |
| `unknown` | Unresolved, mixed, disputed or insufficiently supported cause. |

Set `confirmed` only when review supports a known family and cause group. It requires
reviewer kind/alias, explicit timezone-aware `reviewed_at`, evidence and resolution
references, and affirmative evidence-sufficiency and privacy attestations. Use
`uncertain` when review cannot support confirmation. A useful unresolved incident can
be retained without a confirmed resolution; it does not count as label-eligible.

Use the same `cause_group_id` for retries sharing an evidenced underlying cause.
Distinct aliases are human-attested groups, not proof of statistical independence.
Reviewer and evidence references are opaque local aliases: lowercase letters, digits,
dots, underscores or hyphens, at most 80 characters. Keep their mapping and supporting
material in the existing controlled case record. Do not put names, raw logs, commands,
URLs, customer text, secrets or resolution prose in the journal.

Record actual hands-on diagnosis effort as `diagnosis_seconds`. Use `timed` for a timer
measurement, `self_reported` for a retrospective estimate, or `unmeasured` with JSON
`null`. Zero is accepted only as an explicitly supplied measurement or estimate; missing
effort never becomes a zero observation. Avoid double-counting one diagnosis across
several retries. Values must be finite and between zero and 31,536,000 seconds (one
year) per record; this is an input guard, not an expected diagnosis duration.
Labeling and maintenance effort, correctness, wrong turns and review
disagreements belong in the controlled case record and the later timing study; this
minimal journal is not the complete benchmark corpus or an ROI calculation.

After filling the draft, record its first revision:

```powershell
python tools/evals/incident_triage.py record --input logs/linnaeus-pilot/incident-draft.json --expected-revision 0
python tools/evals/incident_triage.py summary
```

The default database is ignored `logs/linnaeus-pilot/reviews.sqlite3`. To correct the
same incident, edit the draft and submit its last recorded revision number, for example
`--expected-revision 1`. Each successful command returns the new revision. History is
retained; a stale revision is rejected. Summary uses only the latest revision, so
changing a confirmation to `uncertain` removes it from eligible counts.

Summary distinguishes all recorded categories from eligible labels. Eligibility requires
an operational, human-confirmed record with the required supporting attestations;
the operation must be `failed` or `blocked`. Synthetic, agent-confirmed and unknown
operation records are excluded. It reports eligible incidents,
distinct cause groups and effort availability separately. It emits no incident or
reviewer identifiers. Timed and self-reported seconds have separate totals; interpret
zero totals with their observation counts. An absent database returns an empty summary
without creating one.

Input is bounded to 16 KiB and rejects unsupported fields and schema versions. Shape
validation cannot verify human identity, the truth of evidence, safe alias contents or
permission to retain data. The local SQLite file is neither encrypted nor a tenant
isolation mechanism; use appropriate workstation access and retention controls. Only
reviewed non-sensitive aggregates may be published. Do not move this journal into a
customer-facing workflow without a separate design and review.

## Future commercial adoption gates

Commercialization creates two plausible opportunities with separate acceptance evidence:

| Opportunity | Trigger to investigate | Evidence before adoption |
| --- | --- | --- |
| Internal support triage and related-case retrieval | Recurring incidents impose measured diagnosis work. | Reviewed cause groups, usable retained evidence, and improvement over rules and retrieval at the Stage 4 precision, coverage and effort criteria. |
| Document routing or quality-review queues | Manual sorting of document families or extraction problems becomes a measured bottleneck. | A separate human-reviewed corpus and taxonomy, held-out councils/templates/time periods, class-specific error costs, abstention, and a measured reduction in review work. |

Start support assistance with rules and related-case retrieval when evidence permits;
the supervised trial need not be the first useful intervention. Neither feature may
promote a suggested category to a confirmed cause, authoritative planning conclusion
or permission outcome. Support success does not validate document routing.

Before a commercial shadow trial, name the operator, supported scope, fallback and
rollback; establish permitted data use, retention and tenant isolation; and freeze the
evaluation and model versions. Measure labeling, ongoing review, maintenance and
infrastructure costs alongside saved diagnosis effort. Commercial volume alone does not
prove benefit, and the initial inventory cannot establish a break-even point.

Promote only after the held-out quality and effort gates pass and shadow corrections
show acceptable behavior. Monitor abstention, novel cases, corrections and changing
source/document formats. Retain the existing workflow when evidence is inconclusive.
Library, project-distribution and input-data rights require their own review for the
chosen commercial delivery; technical suitability does not settle those questions.

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

The capture CLI and commercial adoption gates were added on 11 November 2026. Its
local smoke check created an unreviewed draft and reported zero stored incidents; no
operational labels or diagnosis times were invented to populate the journal. The
independent-label and measured-effort gaps therefore remain open.

Capture verification: 19 focused tests passed, including concurrent revision conflicts,
corrupt schema/history, bounded JSON, privacy of output and unknown-effort handling.
Independent review and targeted Ruff lint/format checks passed. The full-profile public
`verify --no-cache` and `check` commands stopped at lint on 31 errors in unrelated
generated `pytest-of-pglyn/pytest-24` fixtures at 11:27 UTC; later gate stages did not run.
These results establish focused local behavior, not full-repository or model acceptance.
