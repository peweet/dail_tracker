"""Scan Claude Code transcript JSONL for token spend + discovery candidates.

Measures what IS measurable (per-session token spend, exploration intensity) and
extracts discovery-marker snippets as candidates for a compact discoveries dataset.
Reads transcripts off disk; prints only aggregates so nothing floods a context window.
"""

from __future__ import annotations

import glob
import json
import os
import re
import sys
from collections import defaultdict

PROJ = os.path.expanduser("~/.claude/projects/c--Users-pglyn-PycharmProjects-dail-extractor")
EXPLORE_TOOLS = {"Read", "Grep", "Glob", "Bash", "PowerShell", "Explore", "Agent", "Task"}
# markers that tend to precede a hard-won discovery in assistant prose
MARKERS = re.compile(
    r"\b(turns out|it turns out|the real (issue|problem|cause)|actually the|"
    r"gotcha|the trap|watch out|don't (scan|read|sum|delete|infer)|"
    r"the catch|silently (no-?ops|fails|drops)|false (alarm|positive)|"
    r"misfires?|the reason .{0,40} exists|counter-?intuitive|"
    r"you'd think|non-obvious|the fix is)\b",
    re.IGNORECASE,
)


def text_of(block):
    if isinstance(block, dict):
        return block.get("text", "") or ""
    return ""


def scan():
    rows = []
    candidates = []  # (out_tokens_session, session, snippet)
    for path in glob.glob(os.path.join(PROJ, "*.jsonl")):
        sid = os.path.basename(path)[:8]
        out = fresh_in = cache_read = 0
        turns = 0
        tool_counts = defaultdict(int)
        mem_writes = mcp = 0
        first_prompt = ""
        markers = []
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                try:
                    o = json.loads(line)
                except Exception:
                    continue
                typ = o.get("type")
                msg = o.get("message") or {}
                if typ == "user" and not first_prompt:
                    c = msg.get("content")
                    if isinstance(c, str):
                        first_prompt = c[:140]
                    elif isinstance(c, list):
                        for b in c:
                            t = text_of(b)
                            if t and "<system-reminder>" not in t:
                                first_prompt = t[:140]
                                break
                if typ == "assistant":
                    turns += 1
                    u = msg.get("usage") or {}
                    out += u.get("output_tokens", 0) or 0
                    fresh_in += (u.get("input_tokens", 0) or 0) + (u.get("cache_creation_input_tokens", 0) or 0)
                    cache_read += u.get("cache_read_input_tokens", 0) or 0
                    content = msg.get("content")
                    if isinstance(content, list):
                        for b in content:
                            if isinstance(b, dict) and b.get("type") == "tool_use":
                                name = b.get("name", "")
                                if name in EXPLORE_TOOLS:
                                    tool_counts[name] += 1
                                if name.startswith("mcp__dail-tracker__"):
                                    mcp += 1
                                if name in ("Write", "Edit"):
                                    p = (b.get("input") or {}).get("file_path", "")
                                    if "/memory/" in p.replace("\\", "/") or "MEMORY.md" in p:
                                        mem_writes += 1
                            t = text_of(b)
                            if t:
                                for m in MARKERS.finditer(t):
                                    s = max(0, m.start() - 60)
                                    e = min(len(t), m.end() + 90)
                                    markers.append(t[s:e].replace("\n", " ").strip())
        explore = sum(tool_counts.values())
        rows.append(
            {
                "sid": sid,
                "out": out,
                "fresh_in": fresh_in,
                "cache_read": cache_read,
                "turns": turns,
                "explore": explore,
                "mcp": mcp,
                "mem_writes": mem_writes,
                "prompt": first_prompt,
                "markers": markers[:3],
            }
        )
        for mk in markers:
            candidates.append((out, sid, mk))
    return rows


def main():
    rows = scan()
    tot_out = sum(r["out"] for r in rows)
    tot_fresh = sum(r["fresh_in"] for r in rows)
    tot_read = sum(r["cache_read"] for r in rows)
    print(
        f"SESSIONS={len(rows)}  total_output={tot_out:,}  total_fresh_input={tot_fresh:,}  total_cache_read={tot_read:,}"
    )
    print(f"avg_output/session={tot_out // max(1, len(rows)):,}\n")

    print("=== TOP 20 SESSIONS BY OUTPUT TOKENS (generation cost) ===")
    for r in sorted(rows, key=lambda r: r["out"], reverse=True)[:20]:
        print(
            f"{r['out']:>8,} out | {r['explore']:>3} explore | {r['turns']:>4}t | mem={r['mem_writes']} | {r['sid']} | {r['prompt'][:70]}"
        )

    print("\n=== TOP 15 BY EXPLORATION INTENSITY (Read/Grep/Bash/Agent calls) ===")
    for r in sorted(rows, key=lambda r: r["explore"], reverse=True)[:15]:
        print(
            f"{r['explore']:>4} explore | {r['out']:>8,} out | mem={r['mem_writes']} | {r['sid']} | {r['prompt'][:60]}"
        )

    print("\n=== HIGH-COST SESSIONS THAT PRODUCED A MEMORY (discovery was captured) ===")
    capd = [r for r in rows if r["mem_writes"] > 0]
    for r in sorted(capd, key=lambda r: r["out"], reverse=True)[:15]:
        print(f"{r['out']:>8,} out | {r['explore']:>3} exp | mem={r['mem_writes']} | {r['sid']} | {r['prompt'][:60]}")

    print("\n=== DISCOVERY-MARKER SNIPPETS from the 12 most expensive sessions ===")
    top_sids = {r["sid"] for r in sorted(rows, key=lambda r: r["out"], reverse=True)[:12]}
    for r in rows:
        if r["sid"] in top_sids and r["markers"]:
            print(f"[{r['sid']}] {r['prompt'][:50]}")
            for mk in r["markers"]:
                print(f"    … {mk}")


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
