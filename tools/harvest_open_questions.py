"""Open-questions harvester — collect OPEN items scattered across memory files
into the single register memory/OPEN_QUESTIONS.md.

Memory cards record open items inline ("OPEN TODO:", "OPEN GAPS:", "Ph4 open")
and they age silently — nothing re-surfaces them, so a question stays open until
someone happens to reread the right card. The register gives them one home with a
status and a resolution citation; this harvester finds candidates and appends the
ones the register doesn't already hold.

Idempotent by design: each harvested line carries a stable key (<!-- oq:HASH -->,
sha1 of source file + normalised text). A key already present anywhere in the
register — including in the manually curated section, wherever the line was moved
or edited — is never re-appended, so running twice produces no diff. The
harvester only ever APPENDS to the "Harvested candidates" section; triage (moving
a candidate up to Tracked, setting status, recording the resolution) is manual.

Usage:
  python tools/harvest_open_questions.py            # report + append new candidates
  python tools/harvest_open_questions.py --dry-run  # report only, write nothing
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
from pathlib import Path

MEM = Path.home() / ".claude" / "projects" / "c--Users-pglyn-PycharmProjects-dail-extractor" / "memory"
REGISTER = MEM / "OPEN_QUESTIONS.md"
SKIP = {"MEMORY.md", "MEMORY_COLD.md", "OPEN_QUESTIONS.md"}

# Conservative on purpose: only explicit OPEN markers, not every ⚠ caveat — most
# ⚠ lines are permanent warnings (traps, never-sum rules), not questions awaiting
# an answer. Widen only if a real open item is shown to slip through.
MARKER_RE = re.compile(r"OPEN\s+(?:TODO|GAPS?|QUESTION)|OPEN:|\bTODO:|(?:^|\s)Ph\d+\s+open\b", re.IGNORECASE)


def _key(source: str, text: str) -> str:
    norm = re.sub(r"\s+", " ", text.strip().lower())
    return hashlib.sha1(f"{source}|{norm}".encode()).hexdigest()[:10]


def harvest() -> list[tuple[str, str, str]]:
    """Return (key, source_file, cleaned_line) for every OPEN-marked line."""
    out = []
    for f in sorted(MEM.glob("*.md")):
        if f.name in SKIP:
            continue
        for line in f.read_text(encoding="utf-8", errors="replace").splitlines():
            if MARKER_RE.search(line):
                clean = re.sub(r"\s+", " ", line.strip("-*# \t")).strip()
                if len(clean) > 240:
                    clean = clean[:237] + "..."
                out.append((_key(f.name, clean), f.name, clean))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Harvest OPEN items from memory cards into OPEN_QUESTIONS.md.")
    ap.add_argument("--dry-run", action="store_true", help="report candidates, write nothing")
    args = ap.parse_args()

    if not MEM.exists():
        print(f"memory dir not found: {MEM}")
        return 1
    if not REGISTER.exists():
        print(f"register not found: {REGISTER} — create it first (see its header for format).")
        return 1

    existing = REGISTER.read_text(encoding="utf-8")
    found = harvest()
    fresh = [(k, src, txt) for k, src, txt in found if f"oq:{k}" not in existing]
    print(
        f"{len(found)} OPEN-marked lines in {len({s for _, s, _ in found})} memory files; {len(fresh)} not yet in the register."
    )

    if not fresh:
        print("register is up to date — no diff.")
        return 0
    for _k, src, txt in fresh:
        print(f"  + [{src}] {txt[:100]}")
    if args.dry_run:
        print("(dry run — nothing written)")
        return 0

    lines = [f"- <!-- oq:{k} --> **[{src.removesuffix('.md')}]** {txt}  · status: OPEN" for k, src, txt in fresh]
    REGISTER.write_text(existing.rstrip() + "\n" + "\n".join(lines) + "\n", encoding="utf-8")
    print(f"appended {len(fresh)} candidate(s) to {REGISTER.name}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
