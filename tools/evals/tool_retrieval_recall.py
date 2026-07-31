"""Tool-retrieval Recall@K benchmark over the dail-tracker MCP catalog.

Applies the evaluation methodology of ToolLLM (Qin et al. 2023, arXiv:2307.16789):
LLM-generated synthetic instructions per API, a retriever over the tool catalog, and
Recall@K — was the gold tool inside the top-K retrieved candidates — as the metric.
K ∈ {1, 3, 5} matches the paper's choice of small K (they report 3 and 5).

What this measures and what it does not: Claude Code's real ToolSearch runs inside
the closed harness and cannot be driven from a script, so the retriever here is a
BM25 stand-in over the same text ToolSearch sees (tool names + descriptions; the
docs describe ToolSearch as keyword matching over exactly these fields). Scores
therefore measure the CATALOG'S RETRIEVABILITY under a keyword retriever, not
ToolSearch itself. Absolute recall is optimistic (the query author saw the
descriptions); the per-tool ranking — which descriptions fail their own queries —
is the actionable output, per the ToolLLM finding that retrieval quality tracks
how well description vocabulary covers real query phrasings.

Usage:
    .venv/Scripts/python tools/evals/tool_retrieval_recall.py            # summary + worst tools
    .venv/Scripts/python tools/evals/tool_retrieval_recall.py --misses   # every miss with what outranked gold
    .venv/Scripts/python tools/evals/tool_retrieval_recall.py --json out.json

Inputs: tools/evals/tool_retrieval_queries.json (the synthetic instruction set) and
a catalog JSON dumped from the live server's tools/list (see --catalog; the eval
refuses to run without one rather than silently re-parsing server.py, so the text
scored is exactly what the wire serves).
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
QUERIES = Path(__file__).with_name("tool_retrieval_queries.json")

K_VALUES = (1, 3, 5)
NAME_WEIGHT = 3  # name tokens repeated — retrieval systems weight the name field above prose

_TOKEN = re.compile(r"[a-z0-9]+")


def _tokens(text: str) -> list[str]:
    """Lowercase word tokens; underscores/punctuation split so 'py_refs' -> ['py','refs']."""
    return _TOKEN.findall(text.lower())


class BM25:
    """Plain Okapi BM25 (k1=1.2, b=0.75), stdlib only. No stemming — a deliberate
    floor: if recall is good without stemming it survives stemming too, and the real
    ToolSearch's normalisation is unknown."""

    def __init__(self, docs: dict[str, list[str]], k1: float = 1.2, b: float = 0.75):
        self.k1, self.b = k1, b
        self.docs = docs
        self.doc_len = {d: len(t) for d, t in docs.items()}
        self.avg_len = sum(self.doc_len.values()) / max(1, len(docs))
        self.tf = {d: Counter(t) for d, t in docs.items()}
        df: Counter = Counter()
        for t in docs.values():
            df.update(set(t))
        n = len(docs)
        self.idf = {w: math.log(1 + (n - c + 0.5) / (c + 0.5)) for w, c in df.items()}

    def rank(self, query: str) -> list[tuple[str, float]]:
        q = _tokens(query)
        scores = {}
        for d in self.docs:
            tf, dl = self.tf[d], self.doc_len[d]
            s = 0.0
            for w in q:
                if w not in tf:
                    continue
                f = tf[w]
                s += self.idf.get(w, 0.0) * f * (self.k1 + 1) / (f + self.k1 * (1 - self.b + self.b * dl / self.avg_len))
            if s > 0:
                scores[d] = s
        return sorted(scores.items(), key=lambda kv: -kv[1])


def _load_catalog(path: Path) -> dict[str, dict]:
    cat = json.loads(path.read_text(encoding="utf-8"))
    return {t["name"]: t for t in cat}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--catalog", type=Path, required=True, help="tools/list dump JSON (name/description/always_load)")
    ap.add_argument("--misses", action="store_true", help="print every query where gold missed top-5")
    ap.add_argument("--json", type=Path, help="write full per-query results to this path")
    args = ap.parse_args()

    catalog = _load_catalog(args.catalog)
    qset = json.loads(QUERIES.read_text(encoding="utf-8"))["queries"]

    missing = [e["tool"] for e in qset if e["tool"] not in catalog]
    if missing:
        print(f"ERROR: query set names tools absent from the catalog: {missing}", file=sys.stderr)
        return 1
    uncovered = sorted(set(catalog) - {e["tool"] for e in qset})
    if uncovered:
        print(f"note: {len(uncovered)} catalog tools have no queries (not scored): {uncovered}")

    docs = {
        name: _tokens(t["name"]) * NAME_WEIGHT + _tokens(t.get("description") or "")
        for name, t in catalog.items()
    }
    bm25 = BM25(docs)

    rows = []
    for entry in qset:
        gold = entry["tool"]
        for q in entry["queries"]:
            ranked = [name for name, _ in bm25.rank(q)]
            pos = ranked.index(gold) + 1 if gold in ranked else None
            rows.append({"tool": gold, "query": q, "rank": pos, "top5": ranked[:5]})

    n = len(rows)
    print(f"\n{n} queries over {len({r['tool'] for r in rows})} tools "
          f"(catalog: {len(catalog)} tools, retriever: BM25 name×{NAME_WEIGHT}+description)\n")
    for k in K_VALUES:
        hit = sum(1 for r in rows if r["rank"] is not None and r["rank"] <= k)
        print(f"  Recall@{k}: {hit}/{n} = {hit / n:.3f}")

    per_tool = {}
    for r in rows:
        per_tool.setdefault(r["tool"], []).append(r)
    scored = []
    for tool, rs in per_tool.items():
        r5 = sum(1 for r in rs if r["rank"] is not None and r["rank"] <= 5) / len(rs)
        r1 = sum(1 for r in rs if r["rank"] == 1) / len(rs)
        scored.append((r5, r1, tool))
    scored.sort()

    worst = [s for s in scored if s[0] < 1.0]
    print(f"\nTools below perfect Recall@5 ({len(worst)} of {len(scored)}):")
    for r5, r1, tool in worst:
        flag = "*" if catalog[tool].get("always_load") else " "
        print(f"  {flag}{tool:38} R@5={r5:.1f}  R@1={r1:.1f}")
    if not worst:
        print("  (none)")

    if args.misses:
        print("\nMisses (gold outside top-5):")
        for r in rows:
            if r["rank"] is None or r["rank"] > 5:
                print(f"  [{r['tool']}] '{r['query']}'")
                print(f"      rank={r['rank']}  top5={r['top5']}")

    if args.json:
        args.json.write_text(json.dumps(rows, ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"\nper-query results -> {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
