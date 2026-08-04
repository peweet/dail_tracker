"""Measure dail-tracker MCP *steering* adoption across Claude Code transcripts.

Why this exists: token_ledger.py counts Read/Grep/Bash exploration cost but never
counts mcp__dail-tracker__* calls, so the one number the token-optimisation critique
rests on ('~0.4 steering calls / 100 turns') was a manual one-off, not reproducible.
This closes that loop.

The finding it was built to track (measured 2026-07-20): domain query tools
(who_was_minister, get_supplier, ...) ARE adopted; the *navigation* tools the
CLAUDE.md first-move routing table is built on (describe_dataset / search_project /
code_outline / view_deps) are called ~never. Adoption is a navigation-reflex gap, not an
"MCP is unused" gap.

Reads transcripts off disk; prints only aggregates so nothing floods a context window.
The session_context.py SessionStart hook reuses scan_transcript() on the SINGLE most
recent transcript — never call scan_all() from a hook (too heavy, same reason the
discoveries note refuses to run token_ledger).

Usage: python tools/check_mcp_adoption.py
"""

from __future__ import annotations

import glob
import json
import os
from collections import defaultdict

PROJ = os.path.expanduser("~/.claude/projects/c--Users-pglyn-PycharmProjects-dail-extractor")

MCP_PREFIX = "mcp__dail-tracker__"
# The cheap-retrieval / navigation tools the CLAUDE.md "First move — route the
# question" table points at. These are the reflex we want to see adopted; the
# domain query tools are a separate (already-healthy) population.
STEER = {
    "describe_dataset",
    "list_datasets",
    "search_project",
    "code_outline",
    "py_deps",
    "py_refs",
    "json_peek",
    "view_deps",
    "column_deps",
}
# The raw-exploration alternatives a navigation tool would have displaced.
RAW = {"Read", "Grep", "Glob"}


def scan_transcript(path: str) -> dict:
    """Count one transcript. Cheap enough for a hook to run on a single file."""
    turns = 0
    raw = defaultdict(int)
    mcp_total = 0
    steer = 0
    by_tool = defaultdict(int)
    try:
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                try:
                    o = json.loads(line)
                except Exception:
                    continue
                if o.get("type") != "assistant":
                    continue
                turns += 1
                content = (o.get("message") or {}).get("content")
                if not isinstance(content, list):
                    continue
                for b in content:
                    if not (isinstance(b, dict) and b.get("type") == "tool_use"):
                        continue
                    name = b.get("name", "")
                    if name in RAW:
                        raw[name] += 1
                    elif name.startswith(MCP_PREFIX):
                        mcp_total += 1
                        short = name[len(MCP_PREFIX) :]
                        by_tool[short] += 1
                        if short in STEER:
                            steer += 1
    except Exception:
        pass
    return {
        "turns": turns,
        "raw_total": sum(raw.values()),
        "raw": dict(raw),
        "mcp_total": mcp_total,
        "steer": steer,
        "by_tool": dict(by_tool),
    }


def scan_all() -> list[dict]:
    rows = []
    for path in glob.glob(os.path.join(PROJ, "*.jsonl")):
        r = scan_transcript(path)
        r["sid"] = os.path.basename(path)[:8]
        r["mtime"] = os.path.getmtime(path)
        rows.append(r)
    return rows


def _rate(n: int, turns: int) -> str:
    return f"{100 * n / max(1, turns):.2f}/100t"


def main() -> None:
    import sys

    sys.stdout.reconfigure(encoding="utf-8")
    rows = scan_all()
    turns = sum(r["turns"] for r in rows)
    raw = sum(r["raw_total"] for r in rows)
    mcp = sum(r["mcp_total"] for r in rows)
    steer = sum(r["steer"] for r in rows)
    with_mcp = sum(1 for r in rows if r["mcp_total"])

    print(
        f"sessions={len(rows)}  assistant_turns={turns:,}  sessions_with_any_mcp={with_mcp}"
        f" ({100 * with_mcp / max(1, len(rows)):.0f}%)"
    )
    print(f"RAW Read/Grep/Glob   {raw:,}")
    print(f"MCP all              {mcp:,}   {_rate(mcp, turns)}")
    print(f"MCP navigation subset {steer:,}   {_rate(steer, turns)}   <- the reflex the routing table wants")
    print(f"ratio                1 MCP call per {raw / max(1, mcp):.0f} raw reads")

    # trend: newest 20 sessions vs the rest, so a fix shows up as movement
    by_recent = sorted(rows, key=lambda r: r["mtime"], reverse=True)
    recent, older = by_recent[:20], by_recent[20:]
    for label, grp in (("last 20 sessions", recent), ("older", older)):
        t = sum(r["turns"] for r in grp)
        s = sum(r["steer"] for r in grp)
        m = sum(r["mcp_total"] for r in grp)
        print(f"  {label:<18} nav={_rate(s, t):<10} all_mcp={_rate(m, t)}")

    agg = defaultdict(int)
    for r in rows:
        for k, v in r["by_tool"].items():
            agg[k] += v
    print("\n=== MCP calls by tool (nav-subset starred) ===")
    for k, v in sorted(agg.items(), key=lambda kv: -kv[1]):
        print(f"  {v:>5,}  {k}{'  *' if k in STEER else ''}")

    # The pruning view: registered tools that have NEVER been called. Evidence for
    # merge/retire decisions — but only meaningful over a window where the server
    # could actually connect (broken 07-17→07-25; see the workspacefolder memory).
    try:
        import re as _re

        with open(
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "mcp_server", "server.py"),
            encoding="utf-8",
        ) as fh:
            src = fh.read()
        registered = set(_re.findall(r"@mcp\.tool[^\n]*\)\s*\ndef (\w+)", src))
        unused = sorted(registered - set(agg))
        print(f"\n=== registered but never called: {len(unused)}/{len(registered)} ===")
        for i in range(0, len(unused), 5):
            print("  " + ", ".join(unused[i : i + 5]))
    except Exception as exc:  # a pruning hint must never break the adoption report
        print(f"(never-called section unavailable: {type(exc).__name__})")


if __name__ == "__main__":
    main()
