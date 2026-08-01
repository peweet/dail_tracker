"""Finishing pass for the council-minutes corpus: dedup + label fix + date backfill.

EXPERIMENTAL sandbox. Three defects the quality work surfaced, fixed at SOURCE
(meetings_clean.jsonl + corpus/) so every downstream consumer inherits the fix:
  1. DEDUP — ModernGov + orphan-merge overlap stored the same meeting under two filenames
     (minutes_council_meeting_X vs minutes_mid_Y; confirmed in golden_labels.json notes).
     Content-hash on normalised text; loser rows dropped, loser .txt moved to
     corpus_dupes/ (archive, don't delete). Prefer the record WITH a url, then the
     non-minutes_mid name.
  2. LABEL — June v1 rows say "Galway" where they mean "Galway County" (join-key drift).
  3. DATES — only 189/822 plenary docs had filename-parseable years (COMPLETENESS.md);
     minutes open with "held on <Day>, <D>th <Month>, <Year>" — backfill meeting_date
     (ISO) from the first 1,500 chars where the filename gave nothing.

Then re-runs the downstream chain (votes -> decisions -> value classify -> completeness
-> quality report) so all reports reflect the deduped corpus.
Usage: python finish_pass.py
"""
from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

import council_minutes_consolidate as base
from extractors.council_decisions_extract import _fname_date  # moved to production 2026-08-01

HERE = Path(__file__).resolve().parent
DUPES = HERE / "corpus_dupes"

_MONTHS = {m: i + 1 for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july",
     "august", "september", "october", "november", "december"])}
_PAGE_DATE = re.compile(
    r"held on\s+(?:\w+day[,\s]+)?(\d{1,2})(?:st|nd|rd|th)?\s+(?:day of\s+)?"
    r"(" + "|".join(_MONTHS) + r")[,\s]+(20\d{2})", re.I)
_PAGE_DATE2 = re.compile(
    r"(\d{1,2})(?:st|nd|rd|th)?\s+(" + "|".join(_MONTHS) + r")[,\s]+(20\d{2})", re.I)


def page_date(text: str) -> str:
    m = _PAGE_DATE.search(text[:1500]) or _PAGE_DATE2.search(text[:1500])
    if m:
        return f"{m.group(3)}-{_MONTHS[m.group(2).lower()]:02d}-{int(m.group(1)):02d}"
    return ""


def main() -> int:
    DUPES.mkdir(exist_ok=True)
    clean = [json.loads(l) for l in (HERE / "meetings_clean.jsonl").read_text(encoding="utf-8").splitlines() if l.strip()]

    # 2. label fix at source
    for d in clean:
        if d["local_authority"] == "Galway":
            d["local_authority"] = "Galway County"

    # 1. dedup by content hash
    def keep_score(d: dict) -> tuple:
        return (bool(d.get("url")), "minutes_mid_" not in d.get("meeting", ""))

    by_hash: dict[str, dict] = {}
    dropped = 0
    kept: list[dict] = []
    for d in clean:
        p = HERE / d.get("text_path", "")
        if not d.get("text_path") or not p.exists():
            kept.append(d)
            continue
        text = p.read_text(encoding="utf-8", errors="replace")
        h = hashlib.sha256(re.sub(r"\s+", " ", text[:30000]).encode()).hexdigest()
        prev = by_hash.get(h)
        if prev is None:
            by_hash[h] = d
            kept.append(d)
        else:
            loser = d if keep_score(d) <= keep_score(prev) else prev
            winner = prev if loser is d else d
            if loser is prev:  # replace previously kept
                kept[kept.index(prev)] = winner
                by_hash[h] = winner
            lp = HERE / loser["text_path"]
            if lp.exists():
                lp.rename(DUPES / lp.name)
            dropped += 1

    # 3. date backfill from page text
    backfilled = 0
    for d in kept:
        if _fname_date(d.get("meeting", "") or d.get("url", "")):
            continue
        p = HERE / d.get("text_path", "")
        if d.get("text_path") and p.exists():
            iso = page_date(p.read_text(encoding="utf-8", errors="replace"))
            if iso:
                d["meeting_date"] = iso
                backfilled += 1

    (HERE / "meetings_clean.jsonl").write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in kept), encoding="utf-8")
    print(f"dedup: -{dropped} duplicate docs (archived to corpus_dupes/) | "
          f"kept {len(kept)} | dates backfilled from page text: {backfilled}", flush=True)

    # downstream chain on the deduped corpus
    # votes + decisions graduated to extractors/ (run as modules from ROOT);
    # the analytical passes stay sandbox-side
    for cmd, cwd in (
        ([sys.executable, "-m", "extractors.council_votes_extract"], HERE.parents[1]),
        ([sys.executable, "-m", "extractors.council_decisions_extract"], HERE.parents[1]),
        ([sys.executable, "minutes_value_classify.py"], HERE),
        ([sys.executable, "completeness_table.py"], HERE),
    ):
        r = subprocess.run(cmd, cwd=str(cwd))
        print(f"  {' '.join(cmd[1:])} exit={r.returncode}", flush=True)

    quar = [json.loads(l) for l in (HERE / "quarantine/quarantine.jsonl").read_text(encoding="utf-8").splitlines() if l.strip()]
    votes = [json.loads(l) for l in (HERE / "member_votes_all.jsonl").read_text(encoding="utf-8").splitlines() if l.strip()]
    base.write_quality_report(kept, quar, votes)
    print("QUALITY_ASSESSMENT.md regenerated — finish pass complete", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
