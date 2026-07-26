"""Cheap trigger-keyed lookup over tools/discoveries.jsonl.

The point: a future session about to explore a topic can get the hard-won one-liner
for a few hundred tokens instead of re-deriving it over a 1M-token session. Companion
to MEMORY.md — this is the fast index, the memory slug holds the full detail + bands.

    python tools/discoveries.py afs gross          # rows whose trigger/text match ALL terms
    python tools/discoveries.py --domain siting    # everything in one feature area
    python tools/discoveries.py --list             # every id + one-liner, ranked by cost
    python tools/discoveries.py --add              # append-a-row template printed to stdout

Rebuild the candidate ranking that feeds this file with:  python tools/token_ledger.py
"""

from __future__ import annotations

import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "discoveries.jsonl")
_BAND_RANK = {"high": 0, "med": 1, "low": 2, None: 3}


def load():
    rows = []
    with open(DATA, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            o = json.loads(line)
            if str(o.get("id", "")).startswith("_"):
                continue
            rows.append(o)
    return rows


def _haystack(r) -> str:
    return " ".join(
        [
            r.get("id", ""),
            r.get("domain", ""),
            r.get("discovery", ""),
            " ".join(r.get("trigger", [])),
        ]
    ).lower()


def find(terms):
    terms = [t.lower() for t in terms]
    hits = [r for r in load() if all(t in _haystack(r) for t in terms)]
    return sorted(hits, key=lambda r: _BAND_RANK.get(r.get("cost_band"), 3))


def show(rows):
    if not rows:
        print("no discovery matched — this may itself be worth capturing after you find it")
        return
    for r in rows:
        band = r.get("cost_band", "?")
        anchor = r.get("cost_anchor_out")
        anchor_s = f" ~{anchor // 1000}k out" if anchor else ""
        print(f"[{band}{anchor_s}] {r['id']}  ({r.get('domain', '')})")
        print(f"    {r['discovery']}")
        print(f"    -> MEMORY.md: {r.get('memory', '?')}\n")


def main(argv):
    if not argv or argv[0] in ("-h", "--help"):
        print(__doc__)
        return
    if argv[0] == "--list":
        show(sorted(load(), key=lambda r: _BAND_RANK.get(r.get("cost_band"), 3)))
        return
    if argv[0] == "--domain":
        show([r for r in load() if r.get("domain") == argv[1]])
        return
    if argv[0] == "--add":
        print(
            json.dumps(
                {
                    "id": "kebab-slug",
                    "domain": "feature",
                    "trigger": ["kw1", "kw2"],
                    "discovery": "one line that avoids re-derivation",
                    "cost_band": "high|med|low",
                    "cost_anchor_out": None,
                    "memory": "memory_slug_without_dot_md",
                }
            )
        )
        return
    show(find(argv))


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main(sys.argv[1:])
