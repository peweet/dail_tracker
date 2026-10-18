"""Provider-neutral, trigger-keyed lookup over tools/discoveries.jsonl.

The point: a future session about to explore a topic can get the hard-won one-liner
for a few hundred tokens instead of re-deriving it over a 1M-token session. Companion
to the repository memory cards — this is the fast index, while a memory slug may hold
the full detail and evidence bands. Automatic detail lookup uses public checkout
cards by default and includes private checkout cards only with explicit scope.
Workstation compatibility roots require an explicitly requested external lookup.

    python tools/discoveries.py afs gross          # rows whose trigger/text match ALL terms
    python tools/discoveries.py --domain siting    # everything in one feature area
    python tools/discoveries.py --list             # every id + one-liner, ranked by cost
    python tools/discoveries.py --add              # append-a-row template printed to stdout
    python tools/discoveries.py --scope private <keywords>  # explicitly include private lessons

Recall labels are source-matched, unverified, or recheck. Source-matched means
the recorded bytes still match, not that the claim is certified. Superseded,
withdrawn, invalid and wrong-scope records are omitted. No files are modified.

Rebuild the candidate ranking that feeds this file with:  python tools/token_ledger.py
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
DATA = HERE / "discoveries.jsonl"
_BAND_RANK = {"high": 0, "med": 1, "low": 2, None: 3}

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from tools.memory_policy import private_path, select  # noqa: E402 - standalone CLI bootstrap


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


def load(path: Path | str | None = None):
    rows = []
    with open(path or DATA, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                o = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(o, dict):
                continue
            row_id, text, triggers = o.get("id"), o.get("discovery"), o.get("trigger")
            if (
                not isinstance(row_id, str)
                or not row_id.strip()
                or row_id.startswith("_")
                or not isinstance(text, str)
                or not text.strip()
                or not isinstance(triggers, list)
                or not triggers
                or any(not isinstance(trigger, str) or not trigger.strip() for trigger in triggers)
                or (o.get("domain") is not None and not isinstance(o["domain"], str))
                or (o.get("cost_band") is not None and not isinstance(o["cost_band"], str))
            ):
                continue
            o.update(id=row_id.strip(), discovery=text.strip(), trigger=[trigger.strip() for trigger in triggers])
            rows.append(o)
    return rows


def _haystack(r) -> str:
    return " ".join(
        [
            r.get("id", ""),
            r.get("domain") or "",
            r.get("discovery", ""),
            " ".join(r.get("trigger", [])),
        ]
    ).lower()


def find(terms, *, scope="public"):
    terms = [t.lower() for t in terms]
    rows = load()
    matched = {r["id"] for r in rows if all(t in _haystack(r) for t in terms)}
    hits = [r for r in rows if r["id"] in matched]
    return sorted(select(hits, root=ROOT, scope=scope), key=lambda r: _BAND_RANK.get(r.get("cost_band"), 3))


def resolve_memory(slug: str, roots: list[Path] | None = None, *, scope: str = "public") -> Path | None:
    if not isinstance(slug, str) or not re.fullmatch(r"[\w-]{1,160}", slug):
        return None
    if roots is None:
        roots = [ROOT / "memory"]
        if scope == "private":
            roots.append(ROOT / "planning/product/claude/memory")
    matches: dict[Path, Path] = {}
    for root in roots:
        path = root / f"{slug}.md"
        try:
            if path.resolve().is_relative_to(root.resolve()) and path.is_file():
                matches[path.resolve()] = path
        except (OSError, RuntimeError, ValueError):
            continue
    return next(iter(matches.values())) if len(matches) == 1 else None


def resolve_detail(row: dict, *, scope: str = "public", root: Path | None = None) -> Path | None:
    """Bind detail provenance to the finding namespace, not merely the caller cwd."""
    root = root or ROOT
    namespace = row.get("scope")
    known_private = row.get("domain") == "siting" or private_path(row.get("repo_path") or "")
    if namespace == "public":
        roots = [root / "memory"]
    elif namespace == "private" or known_private:
        if scope != "private":
            return None
        roots = [root / "planning/product/claude/memory"]
    elif scope == "public":
        roots = [root / "memory"]
    else:
        # Unscoped legacy findings can use admitted roots, but a slug collision
        # must not silently pick evidence from a different namespace.
        roots = [root / "memory", root / "planning/product/claude/memory"]
    detail = resolve_memory(row.get("memory", ""), roots, scope=scope)
    if detail is None or not detail.resolve().is_relative_to(root.resolve()):
        return None
    return detail


def show(rows, *, scope="public"):
    if not rows:
        print("no eligible discovery matched — check the topic and selected scope before adding a new finding")
        return
    for r in rows:
        band = r.get("cost_band", "?")
        anchor = r.get("cost_anchor_out")
        anchor_s = f" ~{anchor // 1000}k out" if type(anchor) is int and anchor > 0 else ""
        recall = r["_recall"]
        print(f"[{band}{anchor_s}; {recall['state']}] {r['id']}  ({r.get('domain', '')})")
        print(f"    {recall['text']}")
        for source in recall["sources"]:
            print(f"    -> source: {source}")
        detail = resolve_detail(r, scope=scope)
        if detail is not None:
            try:
                shown = detail.relative_to(ROOT)
            except ValueError:
                shown = detail
            print(f"    -> detail: {shown}")
        print()


def main(argv):
    scope = "public"
    argv = list(argv)
    if "--scope" in argv:
        at = argv.index("--scope")
        if at + 1 >= len(argv) or argv[at + 1] not in {"public", "private"}:
            print("--scope requires public or private", file=sys.stderr)
            return 2
        scope = argv[at + 1]
        del argv[at : at + 2]
    if not argv or argv[0] in ("-h", "--help"):
        print(__doc__)
        return
    if argv[0] == "--list":
        show(
            sorted(select(load(), root=ROOT, scope=scope), key=lambda r: _BAND_RANK.get(r.get("cost_band"), 3)),
            scope=scope,
        )
        return
    if argv[0] == "--domain":
        if len(argv) < 2:
            print("--domain requires a domain", file=sys.stderr)
            return 2
        show(select([r for r in load() if r.get("domain") == argv[1]], root=ROOT, scope=scope), scope=scope)
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
                    "scope": "public",
                    "revision": 1,
                    "status": "active",
                    "repo_path": "memory/memory_slug_without_dot_md.md",
                }
            )
        )
        return
    show(find(argv, scope=scope), scope=scope)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.exit(main(sys.argv[1:]))
