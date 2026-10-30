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

from mcp_server.code_index import DEFAULT_SCAN_POLICY

REPO = Path(__file__).resolve().parents[2]
CORPUS = Path(os.environ.get("ABP_CORPUS_DIR") or REPO / "data/_sandbox/dail_new_sources/bronze/abp_inspector_reports")

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


def _normalize_repo_path(value: object) -> str | None:
    """Normalize separators and harmless leading ``./`` without changing case."""
    if value is None:
        return None
    path = str(value).replace("\\", "/")
    while path.startswith("./"):
        path = path[2:]
    if not path or path == "." or path.startswith("/") or ":/" in path[:3]:
        return None
    return path


def _gold_is_eligible(gold: object) -> bool:
    path = _normalize_repo_path(gold)
    # The public scanner's private-tree hint is a second boundary beside the
    # generic path policy; checking the prefix here avoids probing that tree.
    return bool(path and not path.startswith("planning/product/") and DEFAULT_SCAN_POLICY.allows(path))


def _classify_gold(gold: object) -> tuple[str | None, str | None]:
    path = _normalize_repo_path(gold)
    if gold is None:
        return None, "missing gold path"
    if path is None:
        return None, "invalid repository-relative path"
    if path.startswith("planning/product/"):
        return path, "private tree excluded from public scan policy"
    if not DEFAULT_SCAN_POLICY.allows(path):
        return path, "excluded by public scan policy"
    return path, None


def _parse_line_span(value: object) -> tuple[int, int] | None:
    if isinstance(value, str):
        match = re.fullmatch(r"\s*(\d+)\s*-\s*(\d+)\s*", value)
        if not match:
            return None
        start, end = (int(part) for part in match.groups())
    elif isinstance(value, (tuple, list)) and len(value) == 2:
        if not all(isinstance(part, int) and not isinstance(part, bool) for part in value):
            return None
        start, end = value
    else:
        return None
    return (start, end) if 1 <= start <= end else None


def _surface_rows(result: object, surface: str) -> list[dict]:
    """Read one result surface, including the metadata tool's legacy ``hits`` key."""
    if isinstance(result, list):
        return [row for row in result if isinstance(row, dict)] if surface == "metadata" else []
    if not isinstance(result, dict):
        return []
    if surface == "metadata":
        rows = result.get("results")
        if not rows:
            rows = result.get("hits")
    else:
        rows = result.get("content_spans")
        if not rows:
            rows = result.get("spans")
    return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def _path_match(row: dict, gold: str) -> bool:
    return _normalize_repo_path(row.get("path")) == gold


def _row_covers(row: dict, gold: str, expected_span: tuple[int, int] | None, *, span_aware: bool) -> bool:
    if not _path_match(row, gold):
        return False
    if not span_aware:
        return True
    actual = _parse_line_span(row.get("span"))
    if actual is None:
        return False
    if expected_span is None:
        return True
    return actual is not None and actual[0] <= expected_span[0] and actual[1] >= expected_span[1]


def _rank(rows: list[dict], gold: str, expected_span: tuple[int, int] | None, *, span_aware: bool) -> int | None:
    for position, row in enumerate(rows, 1):
        if _row_covers(row, gold, expected_span, span_aware=span_aware):
            return position
    return None


def _metrics(ranks: list[int | None]) -> dict:
    count = len(ranks)
    r1_hits = sum(rank == 1 for rank in ranks)
    r5_hits = sum(rank is not None and rank <= 5 for rank in ranks)
    reciprocal = sum(1 / rank for rank in ranks if rank is not None)
    return {
        "ranks": ranks,
        "denominator": count,
        "r1_hits": r1_hits,
        "r5_hits": r5_hits,
        "r1": r1_hits / count if count else None,
        "r5": r5_hits / count if count else None,
        "mrr": reciprocal / count if count else None,
    }


def _metadata_first_union(
    metadata: list[dict], content: list[dict]
) -> tuple[list[dict], list[list[tuple[dict, bool]]]]:
    """Deduplicate by normalized path while retaining metadata-first ordering."""
    rows: list[dict] = []
    candidates: list[list[tuple[dict, bool]]] = []
    by_path: dict[str, int] = {}
    tagged_rows = [(row, False) for row in metadata] + [(row, True) for row in content]
    for row, span_aware in tagged_rows:
        path = _normalize_repo_path(row.get("path"))
        if path is None:
            continue
        index = by_path.get(path)
        if index is None:
            by_path[path] = len(rows)
            rows.append(row)
            candidates.append([(row, span_aware)])
        else:
            candidates[index].append((row, span_aware))
    return rows, candidates


def evaluate_project_recall(cases: list[dict]) -> dict:
    """Score offline project retrieval results without probing the filesystem.

    Metadata and content spans remain separate collections. Golds excluded by the
    public scan policy are disclosed and omitted from ordinary recall denominators.
    """
    eligible_cases: list[dict] = []
    excluded_paths: list[str] = []
    metadata_ranks: list[int | None] = []
    content_ranks: list[int | None] = []
    union_ranks: list[int | None] = []
    excluded_reasons: list[dict] = []
    span_expected_invalid = 0
    audit_cases: list[dict] = []
    for case in cases:
        expected = case.get("expected_span")
        expected_supplied = expected is not None
        expected_span = _parse_line_span(expected) if expected_supplied else None
        expected_invalid = expected_supplied and expected_span is None
        content_expected_span = expected_span if expected_span is not None else ((0, 0) if expected_supplied else None)
        gold, exclusion_reason = _classify_gold(case.get("gold"))
        audit_case = {
            "query": case.get("query"),
            "gold": gold,
            "eligible": exclusion_reason is None,
            "reason": exclusion_reason,
            "expected_span_invalid": expected_invalid,
            "metadata_rank": None,
            "content_rank": None,
            "union_rank": None,
            "either_rank": None,
        }
        if exclusion_reason is not None:
            excluded_reasons.append({"gold": case.get("gold"), "path": gold, "reason": exclusion_reason})
            excluded_paths.append(gold if gold is not None else str(case.get("gold")))
            audit_cases.append(audit_case)
            continue
        if expected_supplied and expected_span is None:
            span_expected_invalid += 1
        result = case.get("result", {})
        metadata = _surface_rows(result, "metadata")
        content = _surface_rows(result, "content")
        metadata_rank = _rank(metadata, gold, expected_span, span_aware=False)
        content_rank = _rank(content, gold, content_expected_span, span_aware=True)
        metadata_ranks.append(metadata_rank)
        content_ranks.append(content_rank)
        union, union_candidates = _metadata_first_union(metadata, content)
        union_rank = None
        for position, _row in enumerate(union, 1):
            if any(
                _row_covers(row, gold, content_expected_span, span_aware=span_aware)
                for row, span_aware in union_candidates[position - 1]
            ):
                union_rank = position
                break
        union_ranks.append(union_rank)
        audit_case.update(
            metadata_rank=metadata_rank,
            content_rank=content_rank,
            union_rank=union_rank,
            either_rank=min((rank for rank in (metadata_rank, content_rank) if rank is not None), default=None),
        )
        audit_cases.append(audit_case)
        eligible_cases.append(case)

    either_ranks = [
        min((rank for rank in (metadata_rank, content_rank) if rank is not None), default=None)
        for metadata_rank, content_rank in zip(metadata_ranks, content_ranks, strict=True)
    ]
    return {
        "total": len(cases),
        "eligible": len(eligible_cases),
        "excluded": len(excluded_paths),
        "excluded_paths": excluded_paths,
        "excluded_reasons": excluded_reasons,
        "span_expected_invalid": span_expected_invalid,
        "cases": audit_cases,
        "metadata": _metrics(metadata_ranks),
        "content_spans": _metrics(content_ranks),
        "union": {
            **_metrics(union_ranks),
            "ordering": "metadata-first, then content_spans; deduplicated by normalized path",
        },
        "either_surface": {
            **_metrics(either_ranks),
            "meaning": "coverage across independent metadata/content top-K lists; not a fused ranking",
        },
    }


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


async def suite_project(session: ClientSession | None, json_path: Path | None = None) -> dict:
    cases = []
    for query, gold in PROJECT_GOLDS:
        if not _gold_is_eligible(gold):
            _tick(f"project: '{query[:40]}…' excluded by public scan policy: {gold}")
            cases.append({"query": query, "gold": gold, "result": {}})
            continue
        res = await _call(session, "search_project", {"query": query, "limit": 12})
        cases.append({"query": query, "gold": gold, "result": res})
    report = evaluate_project_recall(cases)
    metadata_ranks = report["metadata"]["ranks"]
    eligible_cases = [case for case in cases if _gold_is_eligible(case["gold"])]
    for case, rank in zip(eligible_cases, metadata_ranks, strict=True):
        _tick(f"project: '{case['query'][:40]}…' rank={rank}")
    metadata = report["metadata"]
    print(
        f"\nsearch_project (authored golds, n={len(cases)}):  "
        f"R@1 {metadata['r1_hits']}/{metadata['denominator']}  R@5 {metadata['r5_hits']}/{metadata['denominator']}"
    )
    for case, rank in zip(eligible_cases, metadata_ranks, strict=True):
        q, g = case["query"], case["gold"]
        if rank is None or rank > 5:
            print(f"  MISS '{q}' -> {g}  rank={rank}")
    for label in ("metadata", "content_spans", "union", "either_surface"):
        metrics = report[label]
        mrr = "unknown" if metrics["mrr"] is None else f"{metrics['mrr']:.3f}"
        display_label = "either-surface coverage (independent lists; not fused)" if label == "either_surface" else label
        print(
            f"  {display_label}: R@1 {metrics['r1_hits']}/{metrics['denominator']}  "
            f"R@5 {metrics['r5_hits']}/{metrics['denominator']}  MRR {mrr}"
        )
    print(
        f"  eligibility: {report['eligible']} eligible, {report['excluded']} excluded "
        "by public scan policy; union ordering = metadata-first, then content_spans, "
        "deduplicated by normalized path"
    )
    if report["excluded_paths"]:
        print(f"  excluded gold paths: {', '.join(report['excluded_paths'])}")
    if json_path is not None:
        json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"  project recall artifact -> {json_path}")
    return report


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
        await suite_project(session, args.json)
    if args.suite in ("all", "speeches"):
        await suite_roundtrip(session, "member_speeches", "search_speeches", "search_speeches")
    if args.suite in ("all", "questions"):
        await suite_roundtrip(session, "get_member_questions", "search_questions", "search_questions")


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--suite", default="all", choices=["all", "precedents", "project", "speeches", "questions"])
    ap.add_argument("--n", type=int, default=20)
    ap.add_argument("--transport", default="direct", choices=["direct", "mcp"])
    ap.add_argument("--json", type=Path, help="write the offline project-recall metrics artifact")
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
