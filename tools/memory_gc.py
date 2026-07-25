"""Memory garbage collection — report rot in the auto-memory store; archive on request.

The memory dir only grows (275 files as of 2026-07-25) and recall quality decays as
stale or orphaned cards accumulate — the MemoryBank/decay idea from the agent-memory
literature, applied to this repo's file memory. This tool REPORTS by default and only
moves files with --archive (never deletes: feedback_archive_dont_delete). It never
touches MEMORY.md/MEMORY_COLD.md content, and anything linked from the HOT index or
carrying safety language is never an archive candidate.

What it flags:
  orphans        — memory files linked from NEITHER index (unreachable by recall
                   except via FTS; usually a forgotten index line, sometimes rot)
  stale          — not modified in --stale-days (default 60) AND not hot-linked
  broken_links   — [[name]] references whose target file doesn't exist
  corrected      — files whose body contains a correction/overturned marker; their
                   headline claim may no longer hold — re-verify before citing

Usage:
  python tools/memory_gc.py                 # report only
  python tools/memory_gc.py --archive       # move orphan+stale files to archive/
  python tools/memory_gc.py --stale-days 90
"""
from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path

MEM = Path.home() / ".claude" / "projects" / "c--Users-pglyn-PycharmProjects-dail-extractor" / "memory"
INDEXES = ("MEMORY.md", "MEMORY_COLD.md")
SAFETY_RE = re.compile(r"never[- ]sum|privacy|quarantine|SAFETY|never push|PII", re.I)
CORRECTED_RE = re.compile(r"⚠?\s*CORRECTED|overturned|superseded|FALSE ALARM|invalid(?:ated)?\b", re.I)
LINK_RE = re.compile(r"\[\[([\w-]+)\]\]")
MDLINK_RE = re.compile(r"\]\(([\w-]+)\.md\)")


def scan(stale_days: int) -> dict:
    index_text = ""
    for ix in INDEXES:
        p = MEM / ix
        if p.exists():
            index_text += p.read_text(encoding="utf-8")
    hot_text = (MEM / "MEMORY.md").read_text(encoding="utf-8") if (MEM / "MEMORY.md").exists() else ""
    linked = set(MDLINK_RE.findall(index_text))
    hot_linked = set(MDLINK_RE.findall(hot_text))

    cards = {}
    for f in sorted(MEM.glob("*.md")):
        if f.name in INDEXES:
            continue
        body = f.read_text(encoding="utf-8", errors="replace")
        cards[f.stem] = {"path": f, "body": body, "age_d": (time.time() - f.stat().st_mtime) / 86400}

    names = set(cards)
    orphans = sorted(n for n in names if n not in linked)
    stale = sorted(
        n for n, c in cards.items()
        if c["age_d"] > stale_days and n not in hot_linked and not SAFETY_RE.search(c["body"])
    )
    broken = sorted({
        f"{n} -> [[{t}]]"
        for n, c in cards.items()
        for t in LINK_RE.findall(c["body"])
        if t not in names and t not in {Path(ix).stem for ix in INDEXES}
    })
    corrected = sorted(n for n, c in cards.items() if CORRECTED_RE.search(c["body"]))
    # archive candidates: orphaned AND stale AND not safety/hot — the strictest cut
    candidates = sorted(set(orphans) & set(stale))
    return {"total": len(cards), "orphans": orphans, "stale": stale, "broken_links": broken,
            "corrected": corrected, "archive_candidates": candidates}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--archive", action="store_true",
                    help="move archive_candidates into memory/archive/ (never deletes)")
    ap.add_argument("--stale-days", type=int, default=60)
    args = ap.parse_args()

    r = scan(args.stale_days)
    print(f"memory cards: {r['total']}  (indexes + archive excluded)")
    for key in ("orphans", "stale", "corrected", "archive_candidates"):
        vals = r[key]
        print(f"\n{key} ({len(vals)}):")
        for n in vals[:30]:
            print(f"  {n}")
        if len(vals) > 30:
            print(f"  ... and {len(vals) - 30} more")
    print(f"\nbroken [[links]] ({len(r['broken_links'])}):")
    for b in r["broken_links"][:20]:
        print(f"  {b}")

    if args.archive and r["archive_candidates"]:
        dest = MEM / "archive"
        dest.mkdir(exist_ok=True)
        moved = []
        for n in r["archive_candidates"]:
            src = MEM / f"{n}.md"
            try:
                src.rename(dest / src.name)
                moved.append(n)
            except OSError as exc:
                print(f"  SKIP {n}: {exc}")
        (dest / "_archive_log.jsonl").open("a", encoding="utf-8").write(
            json.dumps({"ts": time.strftime("%Y-%m-%dT%H:%M:%S"), "moved": moved}) + "\n")
        print(f"\narchived {len(moved)} card(s) to memory/archive/ — restore by moving back")
    elif args.archive:
        print("\nnothing meets the archive bar (orphaned AND stale AND non-safety)")


if __name__ == "__main__":
    main()
