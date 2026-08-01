"""Merge ORPHANED corpus text files into meetings_clean.jsonl bookkeeping.

EXPERIMENTAL sandbox. moderngov_harvest.py (Fingal + Dublin City) writes extracted .txt
straight into corpus/<slug>/ without adding rows to meetings_clean.jsonl — so every
downstream consumer keyed off the jsonl (decisions_extract, quality report, promote)
never sees those councils (the classic wiring gap:
feedback_wiring_gap_parity_check_2026_07_31). This scans corpus/*/*.txt for files no
jsonl row references, classifies each with the SAME doc_type/classify machinery, and
appends clean rows (quarantining failures) + regenerates QUALITY_ASSESSMENT.md.

Usage: python merge_orphan_corpus.py
"""
from __future__ import annotations

import csv
import json
from pathlib import Path

import council_minutes_consolidate as base

HERE = Path(__file__).resolve().parent

# corpus dir slug -> display name, from the seeds file (slug() of the display name)
SLUG_TO_LA = {
    base.slug(r["local_authority"]): r["local_authority"]
    for r in csv.DictReader(open(HERE / "council_seeds.csv", encoding="utf-8"))
}


def main() -> int:
    clean = [json.loads(l) for l in (HERE / "meetings_clean.jsonl").read_text(encoding="utf-8").splitlines() if l.strip()]
    quar = [json.loads(l) for l in (HERE / "quarantine/quarantine.jsonl").read_text(encoding="utf-8").splitlines() if l.strip()]
    votes = [json.loads(l) for l in (HERE / "member_votes_all.jsonl").read_text(encoding="utf-8").splitlines() if l.strip()]
    referenced = {r.get("text_path") for r in clean if r.get("text_path")}

    added = skipped = 0
    for p in sorted(base.CORPUS.glob("*/*.txt")):
        rel = f"corpus/{p.parent.name}/{p.name}"
        if rel in referenced:
            continue
        la = SLUG_TO_LA.get(p.parent.name, p.parent.name.replace("_", " ").title())
        text = p.read_text(encoding="utf-8", errors="replace")
        rec = {"url": "", "local_authority": la, "meeting": p.stem, "status": "text",
               "text_chars": len(text), "orphan_merged": True}
        dtype = base.doc_type("", text)
        rec["doc_type"] = dtype
        rec.update(base.parse_struct(text))
        ok, reason = base.classify(rec, text, dtype)
        rec["clean"], rec["reason"] = ok, reason
        if ok:
            rec["text_path"] = rel
            clean.append(rec)
            added += 1
        else:
            quar.append(rec)
            skipped += 1

    (HERE / "meetings_clean.jsonl").write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in clean), encoding="utf-8")
    (HERE / "quarantine/quarantine.jsonl").write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in quar), encoding="utf-8")
    base.write_quality_report(clean, quar, votes)
    print(f"orphan merge: +{added} clean, {skipped} quarantined, total_clean={len(clean)}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
