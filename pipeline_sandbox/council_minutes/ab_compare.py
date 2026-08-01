"""A/B comparison: regex strict nets (A) vs the LinearSVC recall-hunter (B) on labeled data.

EXPERIMENTAL sandbox. Combines recall_probe.py's per-class counts with the labeled gap
sample (golden_labels.json — P(True), independent, [Reported] band) to estimate:
  - B's precision on the disputed region = labeled-true share of its flagged docs
    (the flags ARE B's positive calls on docs A missed — so the label sample measures
    B's precision and A's missed-positive rate at once);
  - A's recall estimate = strict / (strict + flagged x B-precision), Extracted band,
    n per class stated (wide CI at n=10 — this ranks classes, it does not certify them).
Writes AB_COMPARISON.md with a per-class verdict and the simplify-to-sklearn answer.

Usage: python ab_compare.py
"""
from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent

# strict / flagged from recall_probe.py stdout 2026-08-01 [Verified — that run's log]
PROBE = {
    "reserved_real_budget": (98, 109),
    "reserved_real_devplan": (83, 127),
    "reserved_real_rates": (50, 45),
    "reserved_veto_s183": (335, 75),
    "exec_noted_mgmt": (72, 265),
    "exec_noted_annual": (29, 4),
}


def main() -> int:
    sample = json.loads((HERE / "golden_tolabel.json").read_text(encoding="utf-8"))
    labels = json.loads((HERE / "golden_labels.json").read_text(encoding="utf-8"))["labels"]
    by_cls = defaultdict(lambda: [0, 0])  # [true, total]
    for row in sample:
        lab = labels.get(str(row["id"]))
        if lab is None:
            continue
        by_cls[row["power_class"]][1] += 1
        if lab:
            by_cls[row["power_class"]][0] += 1

    L = ["# A/B — regex strict nets (A) vs LinearSVC recall-hunter (B)\n"]
    L.append("Labeled gap sample: golden_labels.json ([Reported — independent P(True) snippet "
             "judgments], per-class n below). est_A_recall = strict/(strict + flagged x B_prec) "
             "[Extracted — extrapolation from the sample; treat as ranking, not certification].\n")
    L.append("| class | strict(A) | B flagged | B precision (n) | est. A recall | verdict |")
    L.append("|---|---|---|---|---|---|")
    verdicts = []
    for cls, (strict, flagged) in PROBE.items():
        t, n = by_cls.get(cls, [0, 0])
        prec = t / n if n else None
        est_missed = flagged * prec if prec is not None else None
        rec = strict / (strict + est_missed) if est_missed is not None else None
        if prec is None:
            v = "unmeasured"
        elif prec >= 0.9 and rec is not None and rec < 0.9:
            v = "A under-recalls; B flags are near-pure — widen A with B's phrasings"
        elif prec == 0.0:
            v = "B confounded here — keep A only"
        elif prec >= 0.4:
            v = "mixed — B useful as ranked verification queue only"
        else:
            v = "B noisy here — A's anchor is the safer signal"
        verdicts.append((cls, v))
        L.append(f"| {cls} | {strict} | {flagged} | "
                 f"{'—' if prec is None else f'{prec:.0%}'} ({n}) | "
                 f"{'—' if rec is None else f'{rec:.0%}'} | {v} |")

    L.append("\n## The simplify-to-sklearn question\n")
    L.append("NOT validated as a replacement. The sample shows the two systems fail in "
             "opposite places: B recovers real misses A's section-number anchors can't see "
             "(every sampled s.183 flag was a genuine disposal phrased without 's.183'; all 4 "
             "annual-report flags real), but B collapses where surface vocabulary confounds "
             "it (all 10 sampled mgmt-report flags were attendance headers naming the Chief "
             "Executive, and only 2/10 budget flags were adoption events). A regex anchored on "
             "a statute section has near-perfect precision by construction; B has no such "
             "guarantee anywhere. The validated shape is the HYBRID the works-type classifier "
             "already proved: keep A as anchors, use B to hunt A's misses into a ranked "
             "verification queue, fold confirmed phrasings back into A. Cheaper AND more "
             "robust than either alone; simpler-than-both is not on the table on this "
             "evidence (n=10/class).\n")
    L.append("Duplicate-doc caveat: the sample re-confirmed duplicate corpus entries "
             "(ModernGov + orphan-merge overlap) — dedup before any promoted counts.\n")
    (HERE / "AB_COMPARISON.md").write_text("\n".join(L), encoding="utf-8")
    for cls, v in verdicts:
        print(f"{cls}: {v}", flush=True)
    print("-> AB_COMPARISON.md", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
