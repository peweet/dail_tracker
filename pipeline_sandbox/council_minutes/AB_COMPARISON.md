# A/B — regex strict nets (A) vs LinearSVC recall-hunter (B)

Labeled gap sample: golden_labels.json ([Reported — independent P(True) snippet judgments], per-class n below). est_A_recall = strict/(strict + flagged x B_prec) [Extracted — extrapolation from the sample; treat as ranking, not certification].

| class | strict(A) | B flagged | B precision (n) | est. A recall | verdict |
|---|---|---|---|---|---|
| reserved_real_budget | 98 | 109 | 20% (10) | 82% | B noisy here — A's anchor is the safer signal |
| reserved_real_devplan | 83 | 127 | 50% (10) | 57% | mixed — B useful as ranked verification queue only |
| reserved_real_rates | 50 | 45 | 40% (10) | 74% | mixed — B useful as ranked verification queue only |
| reserved_veto_s183 | 335 | 75 | 100% (10) | 82% | A under-recalls; B flags are near-pure — widen A with B's phrasings |
| exec_noted_mgmt | 72 | 265 | 0% (10) | 100% | B confounded here — keep A only |
| exec_noted_annual | 29 | 4 | 100% (4) | 88% | A under-recalls; B flags are near-pure — widen A with B's phrasings |

## The simplify-to-sklearn question

NOT validated as a replacement. The sample shows the two systems fail in opposite places: B recovers real misses A's section-number anchors can't see (every sampled s.183 flag was a genuine disposal phrased without 's.183'; all 4 annual-report flags real), but B collapses where surface vocabulary confounds it (all 10 sampled mgmt-report flags were attendance headers naming the Chief Executive, and only 2/10 budget flags were adoption events). A regex anchored on a statute section has near-perfect precision by construction; B has no such guarantee anywhere. The validated shape is the HYBRID the works-type classifier already proved: keep A as anchors, use B to hunt A's misses into a ranked verification queue, fold confirmed phrasings back into A. Cheaper AND more robust than either alone; simpler-than-both is not on the table on this evidence (n=10/class).

Duplicate-doc caveat: the sample re-confirmed duplicate corpus entries (ModernGov + orphan-merge overlap) — dedup before any promoted counts.
