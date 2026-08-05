"""Known-item Recall@K over the app's OWN retrieval surfaces, via the live MCP server.

Extends tools/evals/tool_retrieval_recall.py (ToolLLM-style catalog eval) to the search
tools the app ships, using the stricter known-item method: sample a REAL document the
index claims to cover, query a distinctive phrase drawn from it, and check that document
comes back in the top K. No authored gold can be wrong — the document is its own gold.

Surfaces:
  precedents — search_planning_precedents. Samples N corpus files, queries a mid-file
      text phrase (gold = the case id parsed from the filename). Separately probes raw
      CASE-NUMBER queries: DuckDB's create_fts_index defaults strip non-alphabetic
      characters from tokens, so digits should be unsearchable — these probes measure
      that gap rather than repeat the phrase eval.
  project — search_project. Authored gold queries whose target paths were existence-
      checked before authoring; hit = gold path substring in the ranked results.
  speeches / questions — search_speeches / search_questions. Round-trip: pull one real
      item for a named member via their feed tool, extract a phrase from it, then check
      the corpus-wide search surfaces that member in the top K. Spot-check grade (a few
      members), labelled as such in output.

Transports (--transport, default 'direct'):
  direct — import mcp_server.server and call the tool functions in-process. Same deployed
      code (the @mcp.tool decorator returns the plain function; _cur() builds the shared
      capped connection exactly as the server does) minus the stdio subprocess. Chosen
      after two server-path runs stalled unexplained under RAM pressure (2026-08-01,
      memory/project_retrieval_recall_evals_2026_08_01.md) while direct probes ran in
      seconds. What it does NOT exercise: MCP serialization and the server process itself.
  mcp — the original stdio client path, kept for when the box has headroom.

Usage:  .venv/Scripts/python tools/evals/app_retrieval_recall.py [--suite all|precedents|project|speeches|questions] [--n 20] [--transport direct|mcp]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

REPO = Path(__file__).resolve().parents[2]
CORPUS = Path(os.environ.get("ABP_CORPUS_DIR") or "c:/tmp/dail_new_sources/bronze/abp_inspector_reports")

_ALPHA = re.compile(r"[A-Za-zÀ-ÿ]{4,}")

# search_project golds — every path existence-checked 2026-08-01 before authoring.
PROJECT_GOLDS = [
    ("atomic parquet write with row floor guard", "services/parquet_io.py"),
    ("BLAS thread cap first import entry point", "services/runtime_env.py"),
    ("duckdb connection memory cap policy", "mcp_server/resource_policy.py"),
    ("streamlit page business logic firewall checker", "tools/check_streamlit_logic_firewall.py"),
    ("session closeout record", "tools/session_closeout.py"),
    ("block heavy commands when RAM is low", "tools/hooks/guard_memory.py"),
    ("nudge grep toward index tools", "tools/hooks/grep_routing_hint.py"),
    ("source health link checker", "tools/build_source_health.py"),
    ("append session token totals at session end", "tools/hooks/session_token_ledger.py"),
    ("inspector report full text search", "planning/product/mcp/precedent_fts.py"),
]

SPEECH_MEMBERS = ["Pearse Doherty", "Michael Healy-Rae", "Ivana Bacik", "Matt Carthy"]


def _phrase_from(text: str, skip: int = 200, n: int = 10) -> str:
    """n consecutive alphabetic words (>=4 chars) starting `skip` words in — past the
    boilerplate header, letters-only so the digit question is probed separately."""
    words = _ALPHA.findall(text)
    if len(words) <= skip + n:
        skip = max(0, len(words) // 2 - n)
    return " ".join(words[skip : skip + n])


def _walk_strings(obj) -> str:
    if isinstance(obj, str):
        return obj
    if isinstance(obj, dict):
        return " ".join(_walk_strings(v) for v in obj.values())
    if isinstance(obj, list):
        return " ".join(_walk_strings(v) for v in obj)
    return str(obj)


def _longest_text(obj) -> str:
    """The single longest string leaf — in a feed row that is the spoken/question text,
    not names or dates, so round-trip phrases come from actual content."""
    if isinstance(obj, str):
        return obj
    leaves = []
    if isinstance(obj, dict):
        leaves = [_longest_text(v) for v in obj.values()]
    elif isinstance(obj, list):
        leaves = [_longest_text(v) for v in obj]
    return max(leaves, key=len, default="")


_DIRECT_MOD = None


def _direct(tool: str, args: dict):
    """Call the deployed tool function in-process (mcp.tool returns the plain function)."""
    global _DIRECT_MOD
    if _DIRECT_MOD is None:
        import mcp_server.server as _DIRECT_MOD  # noqa: PLW0603
        import services.runtime_env  # noqa: F401 — BLAS cap before anything heavy
    fn = getattr(_DIRECT_MOD, tool)
    fn = getattr(fn, "fn", fn)  # tolerate a wrapping decorator variant
    return fn(**args)


async def _call(session: ClientSession | None, tool: str, args: dict) -> dict | list | str:
    if session is None:
        return _direct(tool, args)
    res = await session.call_tool(tool, args)
    for block in res.content:
        if getattr(block, "type", "") == "text":
            try:
                return json.loads(block.text)
            except Exception:
                return block.text
    return {}


def _tick(msg: str) -> None:
    print(f"    {msg}", flush=True)


def _rank_of_case(result, case: str) -> int | None:
    hits = result.get("hits", []) if isinstance(result, dict) else []
    for i, h in enumerate(hits, 1):
        if str(h.get("abp_case", "")) == case:
            return i
    return None


async def suite_precedents(session: ClientSession, n: int) -> None:
    files = sorted(CORPUS.glob("r*.txt"))
    if not files:
        print(f"precedents: corpus absent at {CORPUS} — skipped")
        return
    step = max(1, len(files) // n)
    sample = files[::step][:n]
    target = CORPUS / "r313603.txt"  # yesterday's zero-hit case, kept in-sample deliberately
    if target.exists() and target not in sample:
        sample[0] = target

    ranks = []
    for f in sample:
        case = re.match(r"r(\d+)", f.stem).group(1)
        phrase = _phrase_from(f.read_text(encoding="utf-8", errors="replace"))
        res = await _call(session, "search_planning_precedents", {"query": phrase, "limit": 10})
        r = _rank_of_case(res, case)
        ranks.append((case, r, phrase[:60]))
    hit1 = sum(1 for _, r, _ in ranks if r == 1)
    hit5 = sum(1 for _, r, _ in ranks if r is not None and r <= 5)
    print(f"\nprecedents (known-item phrase, n={len(ranks)}):  R@1 {hit1}/{len(ranks)}  R@5 {hit5}/{len(ranks)}")
    for case, r, p in ranks:
        if r is None or r > 5:
            print(f"  MISS r{case}  rank={r}  phrase='{p}…'")

    digit_probe = [re.match(r"r(\d+)", f.stem).group(1) for f in sample[:5]]
    dhits = 0
    for case in digit_probe:
        res = await _call(session, "search_planning_precedents", {"query": case, "limit": 10})
        if _rank_of_case(res, case) is not None:
            dhits += 1
    print(
        f"precedents (raw case-NUMBER probe, n={len(digit_probe)}):  found {dhits}/{len(digit_probe)}"
        f"  {'— digits ARE searchable' if dhits else '— digits stripped by the FTS tokenizer (DuckDB ignore default)'}"
    )


async def suite_project(session: ClientSession) -> None:
    rows = []
    for query, gold in PROJECT_GOLDS:
        res = await _call(session, "search_project", {"query": query, "limit": 12})
        hits = res.get("hits", res.get("results", [])) if isinstance(res, dict) else []
        pos = None
        for i, h in enumerate(hits, 1):
            if gold.lower() in _walk_strings(h).lower().replace("\\", "/"):
                pos = i
                break
        _tick(f"project: '{query[:40]}…' rank={pos}")
        rows.append((query, gold, pos))
    hit1 = sum(1 for _, _, p in rows if p == 1)
    hit5 = sum(1 for _, _, p in rows if p is not None and p <= 5)
    print(f"\nsearch_project (authored golds, n={len(rows)}):  R@1 {hit1}/{len(rows)}  R@5 {hit5}/{len(rows)}")
    for q, g, p in rows:
        if p is None or p > 5:
            print(f"  MISS '{q}' -> {g}  rank={p}")


async def _roundtrip(
    session: ClientSession | None, feed_tool: str, search_tool: str, member: str
) -> tuple[str, int | None]:
    feed = await _call(session, feed_tool, {"name_or_code": member, "limit": 5})
    text = _longest_text(feed)
    # Strip the member's own name first — PQ text opens with 'Deputy X asked the
    # Minister…', and a phrase containing the name makes the round-trip trivially
    # succeed without testing content recall at all.
    for part in member.split():
        text = re.sub(re.escape(part), " ", text, flags=re.I)
    phrase = _phrase_from(text, skip=10, n=8)
    if not phrase:
        _tick(f"{feed_tool}: no text for {member}")
        return member, None
    res = await _call(session, search_tool, {"query": phrase, "limit": 10})
    surname = member.split()[-1].lower()
    hits = res.get("hits", res.get("results", [])) if isinstance(res, dict) else []
    rank = None
    for i, h in enumerate(hits, 1):
        if surname in _walk_strings(h).lower():
            rank = i
            break
    _tick(f"{search_tool}: {member} phrase='{phrase[:40]}…' rank={rank}")
    return member, rank


async def suite_roundtrip(session: ClientSession, feed_tool: str, search_tool: str, label: str) -> None:
    rows = [await _roundtrip(session, feed_tool, search_tool, m) for m in SPEECH_MEMBERS]
    hit5 = sum(1 for _, r in rows if r is not None and r <= 5)
    print(f"\n{label} (member round-trip SPOT CHECK, n={len(rows)}):  member in top-5 {hit5}/{len(rows)}")
    for m, r in rows:
        print(f"  {m}: rank={r}")


async def _run_suites(session: ClientSession | None, args) -> None:
    if args.suite in ("all", "precedents"):
        await suite_precedents(session, args.n)
    if args.suite in ("all", "project"):
        await suite_project(session)
    if args.suite in ("all", "speeches"):
        await suite_roundtrip(session, "member_speeches", "search_speeches", "search_speeches")
    if args.suite in ("all", "questions"):
        await suite_roundtrip(session, "get_member_questions", "search_questions", "search_questions")


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--suite", default="all", choices=["all", "precedents", "project", "speeches", "questions"])
    ap.add_argument("--n", type=int, default=20)
    ap.add_argument("--transport", default="direct", choices=["direct", "mcp"])
    args = ap.parse_args()

    if args.transport == "direct":
        await _run_suites(None, args)
        return 0

    env = os.environ.copy()
    env.update(
        {
            "PYTHONUTF8": "1",
            "PYTHONIOENCODING": "utf-8",
            "OPENBLAS_NUM_THREADS": "1",
            "OMP_NUM_THREADS": "1",
            "MKL_NUM_THREADS": "1",
            "NUMEXPR_NUM_THREADS": "1",
        }
    )
    params = StdioServerParameters(
        command=str(REPO / ".venv" / "Scripts" / "python.exe"),
        args=["mcp_server/server.py"],
        cwd=str(REPO),
        env=env,
    )
    async with stdio_client(params) as (read, write), ClientSession(read, write) as session:
        await session.initialize()
        await _run_suites(session, args)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
