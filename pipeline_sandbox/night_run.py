"""Overnight driver: sequence the council-minutes expansion + semi-state probe.

EXPERIMENTAL sandbox. Stages (each logged, each skippable on failure — a stage crash
does not kill the night):
  0. Wait for the in-flight winocr recovery run to finish (polls its log for 'DONE',
     max WAIT_RECOVERY_H) — it writes the same jsonl files the harvest merges into.
  1. Reprocess the remaining stale-reason quarantine buckets (low_text /
     unrecognised_doctype / connection errors) via council_minutes_ocr_recover.py.
  2. night_harvest.py — all-31-council breadth harvest (new docs, fitz+winocr).
  3. decisions_extract.py — decisions + steering over the full corpus.
  4. semistate_probe.py — board-minutes discovery for semi-state bodies
     (lobbying-register + CRO seeded).
  5. TF-IDF+LinearSVC doc_type classifier exercise over the corpus (if sklearn
     available) -> CLASSIFIER_REPORT.md.

Global deadline: DEADLINE_H hours from launch, exported as NIGHT_DEADLINE_TS so the
harvest/probe stop cleanly mid-list instead of being killed.

Usage: python pipeline_sandbox/night_run.py
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
CM = HERE / "council_minutes"
SS = HERE / "semistate_minutes"
RECOVERY_LOG = Path("C:/tmp/council_ocr_recover_run.log")
WAIT_RECOVERY_H = 3.0
DEADLINE_H = 8.5
PY = sys.executable

START = time.time()
DEADLINE_TS = START + DEADLINE_H * 3600


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def run_stage(name: str, cmd: list[str], cwd: Path) -> bool:
    if time.time() > DEADLINE_TS:
        log(f"SKIP {name} — global deadline passed")
        return False
    log(f"=== STAGE {name} ===")
    env = {**os.environ, "NIGHT_DEADLINE_TS": str(DEADLINE_TS), "PYTHONUNBUFFERED": "1",
           "PYTHONIOENCODING": "utf-8"}
    try:
        rc = subprocess.run(cmd, cwd=str(cwd), env=env, timeout=DEADLINE_TS - time.time() + 600).returncode
        log(f"=== STAGE {name} exit={rc} ===")
        return rc == 0
    except subprocess.TimeoutExpired:
        log(f"=== STAGE {name} TIMEOUT (killed at deadline) ===")
        return False
    except Exception as e:  # noqa: BLE001
        log(f"=== STAGE {name} ERROR {type(e).__name__}: {e} ===")
        return False


def wait_recovery() -> None:
    log("stage 0: waiting for in-flight winocr recovery to finish")
    until = time.time() + WAIT_RECOVERY_H * 3600
    while time.time() < until:
        try:
            txt = RECOVERY_LOG.read_text(encoding="utf-8", errors="replace")
            if "\nDONE " in txt or txt.startswith("DONE "):
                log("recovery run DONE — proceeding")
                return
        except OSError:
            pass
        time.sleep(60)
    log(f"WARNING: recovery not DONE after {WAIT_RECOVERY_H}h — proceeding anyway "
        "(its end-of-run file write may clobber harvest merges; check in the morning)")


def classifier_exercise() -> None:
    try:
        import json
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.model_selection import cross_val_score
        from sklearn.svm import LinearSVC
    except ImportError:
        log("sklearn absent — classifier exercise skipped")
        return
    docs = [json.loads(l) for l in (CM / "meetings_clean.jsonl").read_text(encoding="utf-8").splitlines() if l.strip()]
    texts, labels = [], []
    for d in docs:
        p = CM / d.get("text_path", "")
        if d.get("text_path") and p.exists():
            texts.append(p.read_text(encoding="utf-8")[:20000])
            labels.append(d["doc_type"])
    from collections import Counter
    dist = Counter(labels)
    keep = {k for k, v in dist.items() if v >= 8}
    pairs = [(t, l) for t, l in zip(texts, labels) if l in keep]
    if len(set(l for _, l in pairs)) < 2:
        log(f"classifier exercise: <2 viable classes in corpus labels {dict(dist)} — skipped")
        return
    X = [t for t, _ in pairs]
    y = [l for _, l in pairs]
    vec = TfidfVectorizer(ngram_range=(1, 2), max_features=30000, sublinear_tf=True)
    Xv = vec.fit_transform(X)
    scores = cross_val_score(LinearSVC(class_weight="balanced"), Xv, y, cv=5)
    rpt = (
        "# doc_type classifier exercise (TF-IDF 1-2gram + LinearSVC, 5-fold CV)\n\n"
        f"Corpus: {len(pairs)} docs, classes {dict(Counter(y))}.\n\n"
        f"Accuracy: **{scores.mean():.3f} ± {scores.std():.3f}** (folds: "
        + ", ".join(f"{s:.3f}" for s in scores)
        + ")\n\nCaveat: labels are the REGEX classifier's own output, so this measures "
        "learnability/consistency of the regex labels, not ground truth. A human-labeled "
        "golden set is the next step (same recipe as the works-type classifier, "
        "project_siting_extension_works_classifier_2026_07_26).\n"
    )
    (CM / "CLASSIFIER_REPORT.md").write_text(rpt, encoding="utf-8")
    log(f"classifier exercise: acc={scores.mean():.3f}±{scores.std():.3f} on {len(pairs)} docs -> CLASSIFIER_REPORT.md")


def main() -> int:
    log(f"night run start; deadline in {DEADLINE_H}h")
    wait_recovery()
    run_stage("1-requarantine", [PY, "council_minutes_ocr_recover.py",
                                 "--reason", "low_text",
                                 "--reason", "unrecognised_doctype",
                                 "--reason", "extract_err_ConnectionError",
                                 "--reason", "extract_fetch_fail"], CM)
    run_stage("2-harvest", [PY, "night_harvest.py"], CM)
    run_stage("3-decisions", [PY, "decisions_extract.py"], CM)
    run_stage("4-semistate", [PY, "semistate_probe.py"], SS)
    classifier_exercise()
    log("NIGHT RUN COMPLETE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
