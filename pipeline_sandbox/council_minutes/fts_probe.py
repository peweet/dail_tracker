"""SQLite FTS5 probe: assess the UNPROMOTED minutes corpus's usefulness pre-load.

DISCOVERY PROBE (one-harness rule carve-out, feedback_one_harness_per_repeatable_process):
production minutes-search belongs in mcp_server/text_fts.py as a corpus spec; THIS is the
cheap assessment of whether that build is worth it. Indexes corpus/*/*.txt into an FTS5
table (porter stemming, BM25) + the decisions rows, then runs a civic-question battery
and reports what the corpus can answer that the July-13 gold (roster/coverage/agendas
CSVs, no text search) cannot.

Usage: python fts_probe.py            # build index + run battery
       python fts_probe.py "query"    # ad-hoc single query
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
DB = HERE / "minutes_fts_probe.sqlite"

BATTERY = [
    "data centre", "flood relief", "derelict sites", "greenway", "wind farm",
    "housing crisis", "vacant homes", "IPAS OR asylum", "playground", "bus shelter",
    "Uisce Eireann", "material contravention", "disposal of land", "casual vacancy",
]


def build() -> sqlite3.Connection:
    con = sqlite3.connect(DB)
    con.executescript("""
        DROP TABLE IF EXISTS docs;
        CREATE VIRTUAL TABLE docs USING fts5(council, meeting, doc_type, body, tokenize='porter unicode61');
    """)
    meta = {}
    for line in (HERE / "meetings_clean.jsonl").read_text(encoding="utf-8").splitlines():
        if line.strip():
            d = json.loads(line)
            if d.get("text_path"):
                meta[d["text_path"]] = d
    n = 0
    for tp, d in meta.items():
        p = HERE / tp
        if p.exists():
            la = "Galway County" if d["local_authority"] == "Galway" else d["local_authority"]
            con.execute("INSERT INTO docs VALUES (?,?,?,?)",
                        (la, d["meeting"][:80], d.get("doc_type", ""),
                         p.read_text(encoding="utf-8", errors="replace")))
            n += 1
    con.commit()
    print(f"indexed {n} docs -> {DB.name} ({DB.stat().st_size // 1024} KB)", flush=True)
    return con


def query(con: sqlite3.Connection, q: str, limit: int = 3):
    return con.execute(
        "SELECT council, meeting, snippet(docs, 3, '[', ']', '…', 12) "
        "FROM docs WHERE docs MATCH ? ORDER BY bm25(docs) LIMIT ?", (q, limit)).fetchall()


def main() -> int:
    con = build()
    if len(sys.argv) > 1:
        for r in query(con, sys.argv[1], 8):
            print(f"  {r[0]:14} | {r[1][:38]:38} | {r[2][:90]}")
        return 0
    print(f"\n{'query':26} hits councils  top hit")
    for q in BATTERY:
        rows = con.execute("SELECT count(*), count(DISTINCT council) FROM docs WHERE docs MATCH ?", (q,)).fetchone()
        top = query(con, q, 1)
        top_s = f"{top[0][0]}: {top[0][2][:70]}" if top else "-"
        print(f"{q:26} {rows[0]:4} {rows[1]:8}  {top_s}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
