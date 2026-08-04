"""Provider-neutral, trigger-keyed lookup over tools/discoveries.jsonl.

The point: a future session about to explore a topic can get the hard-won one-liner
for a few hundred tokens instead of re-deriving it over a 1M-token session. Companion
to the repository memory cards — this is the fast index, while a memory slug may hold
the full detail and evidence bands.  Checked-in public cards win, followed by the
private Siting knowledge base and finally a workstation-local Claude Code import.

    python tools/discoveries.py afs gross          # rows whose trigger/text match ALL terms
    python tools/discoveries.py --domain siting    # everything in one feature area
    python tools/discoveries.py --list             # every id + one-liner, ranked by cost
    python tools/discoveries.py --add              # append-a-row template printed to stdout

Rebuild the candidate ranking that feeds this file with:  python tools/token_ledger.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
DATA = HERE / "discoveries.jsonl"
_BAND_RANK = {"high": 0, "med": 1, "low": 2, None: 3}


def default_memory_roots(root: Path = ROOT, home: Path | None = None) -> list[Path]:
    """Return memory-card roots in portable-first precedence order.

    The ``planning/product/claude`` name is retained for private-repository history;
    its Markdown notes are provider-neutral and are also read by Codex.  Personal
    Claude memory is only a compatibility fallback and is never required in CI or on
    another developer's machine.  Codex's own ``~/.codex/memories`` is generated
    state and is injected by Codex itself, so this lookup deliberately does not parse
    or edit it.
    """
    home = home or Path.home()
    roots = [
        root / "memory",
        root / "planning" / "product" / "claude" / "memory",
    ]
    roots.extend((home / ".claude" / "projects").glob("*/memory"))
    return roots


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


def resolve_memory(slug: str, roots: list[Path] | None = None) -> Path | None:
    if not slug:
        return None
    if roots is None:
        roots = default_memory_roots()
    for root in roots:
        path = root / f"{slug}.md"
        if path.is_file():
            return path
    return None


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
        detail = resolve_memory(str(r.get("memory", "")))
        if detail is not None:
            try:
                shown = detail.relative_to(ROOT)
            except ValueError:
                shown = detail
            print(f"    -> detail: {shown}")
        print()


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
