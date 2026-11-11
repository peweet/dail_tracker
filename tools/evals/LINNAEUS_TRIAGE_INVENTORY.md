# Linnaeus pilot: initial evidence inventory

Observed **11 November 2026, 08:10:22 UTC** in the public Dail Tracker checkout.
Decision: **insufficient-evidence for supervised comparison or a time-saving claim**.
The approved [pilot plan](../../doc/LINNAEUS_TRIAGE_PILOT.md) remains the continuation
contract. This finding concerns the inspected local evidence, not the usefulness of
SVM as a method or the existence of other incident archives.

## Measured local coverage

| Observation | Result |
| --- | --- |
| Run directories visited, including step directories | 331 |
| Per-run manifest candidates / parseable records | 165 / 165 |
| Recorded run start range | 27 May to 10 November 2026 |
| Run statuses | 122 ok, 28 partial, 1 failed, 14 recorded as running |
| Step records | 1,156: 1,022 ok, 120 failed, 14 recorded as running |
| Distinct failed step/script/status groups | 41 |
| Repeated step/script/status groups | 25, containing 104 failed-step records |
| Duplicate run IDs | 0 |
| Manifests with finished time and wall duration | 151 |
| Rollup rows | 163; two per-run IDs absent from the rollup |
| Cause/resolution/reviewed-label fields in inspected run/step records | 0 |
| Operator-seconds/triage-seconds fields in those records | 0 |
| Input bytes read for the reproducible metadata receipt | 694,684 |
| Parse, traversal or byte-limit omissions in that receipt | 0 |

The recurrence grouping uses `(step name, script, status)`. These are operational
groups, not error fingerprints, independent incidents or ground-truth cause classes.
Fourteen records still say `running`; this inventory did not establish that their
processes remain active. Pipeline durations do not measure human diagnosis effort.
The two unmatched rollup rows indicate reconciliation uncertainty; they do not establish
that diagnostic logs were lost. Run manifests and the rollup are untracked local state.

The separate fetch-report snapshot is dated **4 September 2026**, and is not current
incident history. It contains two extractors, six file-stage failures
(`not_expected_filetype=4`, `http_404=1`, `timeout=1`), three zero-harvest events and
zero breaker trips. Four records have one attempt and two have two attempts. The
writer keeps a last-run snapshot, not a cumulative incident ledger
([source](../../services/fetch_report.py)). Its machine categories are not reviewed
cause labels.

## Reviewed labels and effort

`test/expected_failures.yaml` has empty active `pytest` and `jobs` lists. Its retired
prose note is not an active label. This ledger concerns known environment differences;
it would not by itself constitute a general log-triage gold set even if populated.

A bounded read-only search of `doc/`, `memory/`, `tools/`, `test/` and `.github/` found
no incident ledger pairing independently reviewed cause/resolution labels with these
run records, and no operator diagnosis-time measurements. This is a scoped negative
finding, not proof of absence from the whole workstation or remote services.

An audit replay at **08:19:46 UTC** retained the exact discovery command
`rg --files doc memory tools test .github`, with default ignore rules. It scanned UTF-8
`.md`, `.py`, `.yaml`, `.yml`, `.toml`, `.json` and `.sql` files, capped at 5,000 files,
256 KiB per file and 32 MiB total. Of 755 candidate paths, 722 files totaling 5,731,355
bytes were scanned. Three outputs from this task were deliberately excluded, 29 paths
had other extensions, and one file exceeded the per-file limit; no total-byte or
file-count limit was reached and no read/reparse error was recorded. Ignored files,
other extensions and the oversized file remain outside this negative finding.

The exact case-insensitive search expressions were:

```text
reviewed_label|independently.reviewed|cause.{0,40}resolution|resolution.{0,40}cause
operator_seconds|triage_seconds|diagnosis.{0,20}(?:seconds|minutes|time)
incident.{0,30}(?:ledger|corpus|dataset)|(?:ledger|corpus|dataset).{0,30}incident
```

The five matching files were policy, tests, a fixture generator and an evidence note;
they did not supply an operational incident corpus. Keyword misses alone cannot prove
that differently named records do not exist. The local query and search receipt are
`logs/linnaeus-pilot/audit_label_search.py` and `label_search.json`, schema
`linnaeus-label-search/1`. The ordered searched-path/content snapshot SHA-256 is:

```text
fbcf15c0beaec750db5597b88cf48fa7796984dec5623755e71b0907aa99d2ed
```

## Method and receipt

The metadata query inspected only `logs/runs/**/manifest.json`,
`data/manifests/manifest.json` and `data/_meta/fetch_failures.json`. It pruned reparse
directories/files and was capped at 2,000 directories, 500 manifest candidates,
1 MiB per input and 16 MiB total. No cap was reached. Diagnostic log contents were
not read by that query. It emitted allowlisted aggregate fields and no raw IDs,
hostnames, scripts, URLs, exception text or credentials.

An independent read-only scout checked the aggregate counts and inspected the public
schemas and the bounded label/effort search. Its fetch-report totals supplement the
metadata receipt, which records only that snapshot's top-level schema keys.

The local query and receipt are retained under ignored `logs/linnaeus-pilot/` as
`inventory.py` and `inventory.json`. The receipt schema is `linnaeus-local-inventory/1`.
Its ordered input-path/content snapshot SHA-256 is:

```text
9a52a5fc5f291e3d5656eeb56b6120077a93489730736235d14dbcacfc047193
```

The digest identifies the inputs observed by this query; it is not a filesystem
snapshot, proof of retention completeness or a fingerprint of raw diagnostic logs.

The core counts can be independently reproduced with this local-only metadata query:

```python
import json
from collections import Counter
from pathlib import Path

runs = [json.loads(p.read_text(encoding="utf-8"))
        for p in sorted(Path("logs/runs").glob("**/manifest.json"))]
steps = [step for run in runs for step in run["steps"]]
failed = [step for step in steps if step.get("status") == "failed"]
groups = Counter((step.get("name"), step.get("script"), step.get("status"))
                 for step in failed)
print({"runs": len(runs), "steps": len(steps), "failed_steps": len(failed),
       "step_script_groups": len(groups),
       "repeated_groups": sum(n > 1 for n in groups.values()),
       "records_in_repeated_groups": sum(n for n in groups.values() if n > 1)})
```

The compact recipe assumes the already-inspected local files. Use the bounded local
query for reparse/size/coverage reporting. A changed source needs a new dated receipt.

Schema references: [run manifest](../../manifest.py),
[run paths and optional retention](../../services/run_paths.py),
[fetch reporting](../../services/fetch_report.py), and
[expected-failure validation](../check_expected_failures.py).

## Decision and resumption

Stop at the Stage 1 gate. No corpus was relabeled, no classifier was trained and no
model-quality, calibration, capacity or productivity result is claimed. No separate
LIBSVM dependency is justified by this inventory.

Resume when independent review supplies a labeled corpus with cause-group identities,
source sufficiency decisions and verified negatives, plus a baseline of operator
effort. Preserve unknown cases and review disagreements. Freeze the group/time split,
feature policy, supported classes, uncertainty criteria and timing design before
running the comparison. Retrospective pipeline durations cannot substitute for those
operator measurements.

Off-host CI artifacts, issue trackers, private-product incidents and manually held
review histories were outside this local inventory. Their existence and suitability
remain unknown. Expanding to a separate source should produce its own bounded inventory
and privacy review before changing the current decision.

## Verification scope

- The bounded local query reproduced the scout's run, step and recurrence counts.
- Markdown lint and relative links passed for the authored plan and this report.
- `python tools/build_doc_index.py --check` passed after regeneration.
- `python tools/check_expected_failures.py` confirmed the empty active ledger is valid.
- Full-profile `verify --plan` was inspected; SQL and fast suites were selected because
  the shared checkout has unrelated data and untracked changes. Those suites were not
  run for this documentation-only outcome, and no full verification receipt is claimed.
- The generated index retains two heading-spacing warnings also present in `HEAD`.
  Its source generator was not changed as part of the pilot.
- Model training, held-out model evaluation and operator timing were not run: the
  reviewed-label and effort-evidence gate was not met.
