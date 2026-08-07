# CE reports corpus — extraction quality

Scope: the Chief Executive monthly report lane built by
[council_ce_reports_corpus_build.py](../../extractors/council_ce_reports_corpus_build.py) from
`ce_reports_manifest.jsonl`. CE reports are officer reports, not council minutes; this lane never
writes to the council-minutes corpus.

Figures below are as generated on 2026-08-07 and come from
`data/_meta/council_ce_reports_corpus_coverage.json` [Verified — build run 2026-08-07T08:13].

## Completeness

Of 20 manifest records, 17 published: 1,594 chunks and 540 lead sentences across 15 councils.
Three records were excluded, each attributable to a named council:

| Council | Reason | Meaning |
|---|---|---|
| Leitrim | `needs_ocr` | Scanned PDF, no text layer. One of two Leitrim records; the other published. |
| Cork County | `unresolved_report_month` | Neither report label nor filename states month and year. |
| Waterford | `not_ce_report_agenda` | Classified as a meeting agenda, not a CE report. |

Cork County and Waterford therefore harvested source documents but published none. They are listed
in `harvested_councils_without_published_documents` and the build logs a warning per council; before
this was added they were absent from `documents_by_council` with nothing recording why.

The seed registry holds 31 statutory councils: 24 `confirmed`, 4 `absent` (no CE report published
online), 3 `engineering` (source needs work). Published coverage of 15 councils is therefore 15 of
24 confirmed sources, not 15 of 31 authorities — the denominator matters when quoting coverage.

Per-document completeness is not measured. The builder chunks every page the extractor produced, but
nothing checks that page count matches the source PDF, so a partial text extraction would publish
silently as a short document.

## Recall

Two distinct recall questions. Lead recall is now measured; document recall is not.

### Lead recall — measured, 58–90% (point estimate 67%)

Method: strict-vs-broad gap probe ([ce_lead_recall_probe.py](ce_lead_recall_probe.py)), following
the validated hybrid recipe. The strict net is the shipped `procurement_leads()`; the broad net is a
loose commercial-activity net. Both run over the same sentence spans, using the extractor's own
`_lead_spans()` so the units match.

Over the 17 published documents: 21,134 sentence spans, 540 strict leads, and 951 gap sentences
(broad hit, strict miss). A TF-IDF + LinearSVC model ranked the gap; the top 218 (max 15/council,
stratified so no single council carries the sample) formed the labelling queue.

Sixty queue rows were labelled by two independent readers, neither shown the strict net's output.
Agreement was 59/60 (98%); the single disagreement was "monitors purchased", a genuine edge case.
Counting only rows both readers called a lead: **17/60 = 28.3%** (Wilson 95% CI 18.5–40.8%).

Applying that rate to the full 951-sentence gap gives ~269 missed leads and **recall ≈ 67%**. The
Wilson interval on the sample rate alone puts that at **58–75%** — but sampling noise is not the
only uncertainty, and the other sources push in one direction only:

- **Rank bias (pushes recall up).** The sample came from the model-ranked *head* of the gap, so its
  positive rate is an **upper** bound on the rate across all 951. Positive rate by rank tertile
  within the labelled sample: 42% → 35% → 10%, so the decline is visible, though each bucket holds
  only ~20 rows. The true miss count is likely below 269, making 67% pessimistic.
- **Stratification cap (direction unknown).** The queue caps at 15 rows per council, so its council
  mix is roughly uniform while the real gap is not — Kildare is 16.5% of the 951-row gap but 6.9% of
  the queue; Dún Laoghaire-Rathdown is 1.9% against the same 6.9%. Per-council positive rates in the
  sample (Kildare 0/4, DLR 2/4) are far too small to say which way this bends the estimate.
- **Floor case (not a finding).** If the unlabelled remainder of the gap were entirely clean, recall
  would be ~90%. The tail is unlabelled, so this is a bound, not a measurement.

The honest statement: **lead recall is between 58% and 90%, most likely near 67%** [Extracted — 60
rows double-labelled, extrapolated to a 951-row gap]. The 58% floor comes from sampling noise on
n=60, not from any of the biases above. Narrowing this needs the gap *tail* sampled and the
extrapolation re-weighted by each council's true gap share, rather than one flat rate.

### What the misses look like

The 17 confirmed misses cluster into a small number of phrasings, which makes them addressable
rather than diffuse: consultants engaged (7), other appointments (4), design team appointed (4),
quotations (1), other (1).

The structural cause is visible in the contract. `_APPOINTMENT` only contributes a lead when
`_COMMERCIAL_PARTY` also matches ([council_ce_reports_contract.py:92](../../extractors/council_ce_reports_contract.py#L92)),
and "design team" is absent from that party list. Likewise `_AWARD` cannot produce a lead on its own
— it requires `tender` or `contract` present
([council_ce_reports_contract.py:90](../../extractors/council_ce_reports_contract.py#L90)) — so
"the works were awarded to X" is invisible. Re-running the shipped `procurement_leads()` over all 17
confirmed-miss quotes returns 0 leads for every one, so the gap reproduces against the live
extractor rather than being a probe artifact. Folding these phrasings into the anchors is the obvious
recall lever, but per the recipe it must be a measured fold-back with the probe re-run, not an
ad-hoc regex widening.

### Document recall — unmeasured

What fraction of published CE reports the harvester finds. The harvester takes the latest N per
council from a seed landing page (default 2), so the corpus is deliberately a recent slice, not an
archive. No council's full report history has been enumerated to compare against.

### Precision — not estimated from the promotion set

Twenty-eight recent, forward-looking rows have completed an assisted source-page review and are
promotable. This deliberately conservative set is not a random sample, so it does not estimate the
strict net's overall false-positive rate. The remaining candidates stay in the private queue.

## Publication state

`promotion_permitted` is derived per lead from `ce_report_lead_reviews.csv`, not hardcoded. A lead
becomes promotable only when a reviewer sets `relevance_status=REVIEWED_RELEVANT`, resolves
`site_relationship` to `MATCHED` or `NOT_MATCHED`, and supplies a source-verified project name plus
an allowed procurement stage. An absent or partial ledger leaves the lead `review_queue_only`.

As of the 7 August 2026 build: 28 assisted source-review decisions, 28 promotable leads and 542 in
the review queue.

The leads remain `Extracted` band. Promotion means the source page supports a named forward-work
observation; it does not verify current tender status, availability or eventual contracting route.
