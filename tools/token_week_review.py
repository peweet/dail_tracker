"""Rolling 7-day token-spend review across session transcripts.

Companion to token_ledger.py (lifetime discovery/output scan). This one answers the
weekly cost question: where did INPUT tokens go — per-turn context-size buckets,
tool-result payload per tool, most re-read files, biggest single results, nav reflex.
Prints only aggregates/top-N so nothing floods a context window.

Usage:  .venv/Scripts/python tools/token_week_review.py [--days N]

Reading the output (calibrated 2026-07-31, see memory
project_token_spend_week_review_2026_07_31):
  - cache_read is ~96% of raw tokens and ~60% of price-weighted spend; the bucket
    table shows how much of it is billed on turns already past 200k context —
    the /clear lever. That week: 78% past 200k.
  - "FILES READ >=N TIMES" exposes files needing a SECTION MAP
    (tools/section_map.py --check misses untracked files — the private siting lane).
  - "MCP→READ ESCALATIONS": a nav tool answered but the same file was then Read WHOLE
    (no offset/limit — bounded span-Reads are the intended follow-up) within the next
    ~8 tool results. A recurring rate per tool says its return shape isn't sufficient
    (Self-Route S5, doc/TOKEN_OPTIMIZATION_LITERATURE_2026_07_31.md). Proximity
    heuristic (last-2-path-segments substring match, one count per nav call), not
    causation.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import sys
import time
from collections import Counter, deque

PROJ = os.path.expanduser("~/.claude/projects/c--Users-pglyn-PycharmProjects-dail-extractor")

NAV_TOOLS = {
    "mcp__dail-tracker__search_project",
    "mcp__dail-tracker__describe_dataset",
    "mcp__dail-tracker__list_datasets",
    "mcp__dail-tracker__code_outline",
    "mcp__dail-tracker__view_deps",
    "mcp__dail-tracker__column_deps",
    "mcp__dail-tracker__py_refs",
    "mcp__dail-tracker__py_deps",
}

# price weights relative to fresh input (Anthropic ratios: cache read 0.1x,
# cache write 1.25x, output 5x) — for share-of-spend, not absolute dollars
W_CR, W_CC, W_OUT, W_IN = 0.1, 1.25, 5.0, 1.0


def blk_text(b):
    if isinstance(b, dict):
        if isinstance(b.get("text"), str):
            return b["text"]
        c = b.get("content")
        if isinstance(c, str):
            return c
        if isinstance(c, list):
            return "".join(blk_text(x) for x in c)
    if isinstance(b, str):
        return b
    return ""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7)
    args = ap.parse_args()
    cutoff = time.time() - args.days * 86400

    files = [p for p in glob.glob(os.path.join(PROJ, "*.jsonl")) if os.path.getmtime(p) > cutoff]
    sessions = []
    tool_calls = Counter()
    tool_result_toks = Counter()
    read_targets = Counter()
    read_counts = Counter()
    big_results = []
    buckets = {}
    raw_nav = mcp_nav = mcp_domain = 0
    agent_spawns = Counter()
    nav_calls_by_tool = Counter()
    escalations = Counter()  # MCP nav call → the same file Read anyway (S5: tool needs a better return shape)

    for path in files:
        sid = os.path.basename(path)[:8]
        out = fresh_in = cache_read = cache_create = turns = max_cr = 0
        first_prompt = ""
        pending = {}
        recent_nav = deque(maxlen=8)  # (tool_name, input+result text) — the escalation window
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                try:
                    o = json.loads(line)
                except Exception:
                    continue
                typ = o.get("type")
                msg = o.get("message") or {}
                if typ == "user":
                    c = msg.get("content")
                    if not first_prompt and not o.get("isSidechain"):
                        if isinstance(c, str):
                            first_prompt = c[:120]
                        elif isinstance(c, list):
                            for b in c:
                                t = blk_text(b)
                                if t and "<system-reminder>" not in t:
                                    first_prompt = t[:120]
                                    break
                    if isinstance(c, list):
                        for b in c:
                            if isinstance(b, dict) and b.get("type") == "tool_result":
                                txt = blk_text(b)
                                est = len(txt) // 4
                                name, hint = pending.pop(b.get("tool_use_id"), ("?", ""))
                                tool_result_toks[name] += est
                                if name == "Read" and hint:
                                    read_targets[hint] += est
                                if name in NAV_TOOLS:
                                    recent_nav.append([name, f"{hint} {txt[:20000]}".replace("\\", "/")])
                                if est > 4000:
                                    big_results.append((est, name, hint, sid))
                elif typ == "assistant":
                    turns += 1
                    u = msg.get("usage") or {}
                    out += u.get("output_tokens", 0) or 0
                    fresh_in += u.get("input_tokens", 0) or 0
                    cache_create += u.get("cache_creation_input_tokens", 0) or 0
                    cr = u.get("cache_read_input_tokens", 0) or 0
                    cache_read += cr
                    max_cr = max(max_cr, cr)
                    k = (
                        "<100k"
                        if cr < 100_000
                        else "100-200k"
                        if cr < 200_000
                        else "200-400k"
                        if cr < 400_000
                        else ">400k"
                    )
                    b = buckets.setdefault(k, [0, 0])
                    b[0] += 1
                    b[1] += cr
                    content = msg.get("content")
                    if isinstance(content, list):
                        for b2 in content:
                            if isinstance(b2, dict) and b2.get("type") == "tool_use":
                                name = b2.get("name", "")
                                inp = b2.get("input") or {}
                                hint = ""
                                if name == "Read":
                                    hint = (inp.get("file_path") or "").replace("\\", "/")
                                    read_counts[hint] += 1
                                    # bounded span-Reads are the INTENDED follow-up to an
                                    # outline/search hit — only a whole-file Read is an escalation
                                    if "offset" not in inp and "limit" not in inp:
                                        rel2 = "/".join(hint.split("/")[-2:])
                                        for entry in reversed(recent_nav):
                                            if rel2 and rel2 in entry[1]:
                                                escalations[entry[0]] += 1
                                                entry[1] = ""  # one escalation per nav call
                                                break
                                elif name in ("Grep", "Glob"):
                                    hint = inp.get("pattern", "")[:50]
                                elif name in ("Bash", "PowerShell"):
                                    hint = (inp.get("command") or "")[:50]
                                elif name in ("Agent", "Task"):
                                    agent_spawns[inp.get("subagent_type", "?")] += 1
                                    hint = (inp.get("description") or "")[:50]
                                elif name.startswith("mcp__"):
                                    hint = json.dumps(inp)[:50]
                                tool_calls[name] += 1
                                pending[b2.get("id")] = (name, hint)
                                if name in ("Read", "Grep", "Glob"):
                                    raw_nav += 1
                                elif name in NAV_TOOLS:
                                    mcp_nav += 1
                                    nav_calls_by_tool[name] += 1
                                elif name.startswith("mcp__dail-tracker__"):
                                    mcp_domain += 1
        if turns:
            sessions.append(
                {
                    "sid": sid,
                    "out": out,
                    "fresh": fresh_in,
                    "cc": cache_create,
                    "cr": cache_read,
                    "max_cr": max_cr,
                    "turns": turns,
                    "prompt": first_prompt,
                }
            )

    tot_out = sum(s["out"] for s in sessions)
    tot_fresh = sum(s["fresh"] for s in sessions)
    tot_cc = sum(s["cc"] for s in sessions)
    tot_cr = sum(s["cr"] for s in sessions)
    tot_turns = sum(s["turns"] for s in sessions)
    print(f"WINDOW: last {args.days} days | sessions={len(sessions)} | turns={tot_turns:,}")
    print(f"output={tot_out:,}  fresh_input={tot_fresh:,}  cache_CREATE={tot_cc:,}  cache_READ={tot_cr:,}")
    grand = tot_out + tot_fresh + tot_cc + tot_cr
    weighted = tot_out * W_OUT + tot_fresh * W_IN + tot_cc * W_CC + tot_cr * W_CR
    print(
        f"cache_read: {100 * tot_cr / max(1, grand):.1f}% of raw tokens, "
        f"{100 * tot_cr * W_CR / max(1, weighted):.0f}% of price-weighted spend"
    )

    print("\n=== CACHE_READ BY PER-TURN CONTEXT SIZE (the /clear lever) ===")
    for k in ["<100k", "100-200k", "200-400k", ">400k"]:
        n, t = buckets.get(k, [0, 0])
        print(f"{k:>9}: {n:>6} turns  {t:>14,} cache_read  ({100 * t / max(1, tot_cr):.0f}%)")

    print("\n=== TOP 10 SESSIONS BY FRESH COST (out + fresh_in + cache_create) ===")
    for s in sorted(sessions, key=lambda s: s["out"] + s["fresh"] + s["cc"], reverse=True)[:10]:
        cost = s["out"] + s["fresh"] + s["cc"]
        print(f"{cost:>10,} fresh | {s['turns']:>4}t | peak-ctx {s['max_cr']:>8,} | {s['sid']} | {s['prompt'][:55]}")

    print("\n=== TOOL CALLS + RESULT PAYLOAD (est tokens, chars/4) ===")
    for name, n in tool_calls.most_common(15):
        print(f"{n:>5} calls | {tool_result_toks.get(name, 0):>9,} est-tok returned | {name}")

    print("\n=== FILES READ >=5 TIMES (section-map candidates) ===")
    for p, n in read_counts.most_common(25):
        if n >= 5:
            print(f"{n:>3}x | {read_targets.get(p, 0):>8,} tok | {'/'.join(p.split('/')[-3:])}")

    print("\n=== BIGGEST SINGLE TOOL RESULTS (>4k est tok) ===")
    for est, name, hint, sid in sorted(big_results, reverse=True)[:12]:
        print(f"{est:>7,} tok | {name:<10} | {sid} | {hint[:60]}")

    print("\n=== MCP→READ ESCALATIONS (nav answered, file then Read WHOLE within ~8 results) ===")
    if not escalations:
        print("none detected")
    for nname, n in escalations.most_common():
        calls = nav_calls_by_tool.get(nname, 0)
        short = nname.replace("mcp__dail-tracker__", "")
        print(f"{n:>4} of {calls:>4} calls ({100 * n / max(1, calls):.0f}%) | {short}")
    print("a recurring escalation rate means that TOOL's return shape isn't answering — fix the tool, not the caller")

    print("\n=== NAV REFLEX ===")
    print(
        f"raw Read/Grep/Glob={raw_nav}  mcp_nav={mcp_nav}  mcp_domain={mcp_domain}  "
        f"ratio raw:mcp_nav = {raw_nav}:{mcp_nav}"
    )
    print(f"agent spawns: {dict(agent_spawns)}")


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
