"""SQLite FTS5 probe over the PQ written-answer TABLE + ATTACHMENT corpus.

DISCOVERY PROBE (one-harness carve-out, feedback_one_harness_per_repeatable_process):
production text search belongs in mcp_server/text_fts.py as a corpus spec; THIS is the
cheap assessment of whether that build is worth it — same pattern as
pipeline_sandbox/council_minutes/fts_probe.py.

The question this answers
-------------------------
The harvested corpus does not describe itself: the commonest attachment labels are
"Table" (124), "Info" (53), "Table 1" (18). Ranking by size found only redundant
material (NTPF waiting lists), yet the IDA 317-site register — data published nowhere
else — was sitting unlabelled in the same corpus. So size-ranking is the wrong probe
and label-ranking is impossible.

The fix is to index each table WITH THE QUESTION THAT PROVOKED IT. A TD asking "the
full list of sites the IDA holds in its land bank ... in tabular form" is the topic
label the attachment lacks. Search over question text, table header and cell values
together, and the IDA-shaped disclosures become findable by topic.

Grain: one FTS row per TABLE or ATTACHMENT, carrying
  question_text  — every question in that answer SECTION (see caveat)
  header         — the table's header row
  body           — cell values (attachments: real values; inline tables: the census
                   header + first-column sample, which is all the census captured)

⚠ CAVEAT — SECTION-level question attribution. A written-answer section bundles many
Q&As; the census recorded tables per SECTION, not per question_ref. So question_text
here is every question in the section, and a hit means "this topic appears in the
section containing this table", not "this table answers this question". Precise
attribution exists in pq_ida_land_tables.py (pending-question walk) and should be
used before any promotion. Good enough to FIND things; not good enough to CITE.

Usage:
    python -m pipeline_sandbox.pq_disclosures.pq_fts_probe            # build + battery
    python -m pipeline_sandbox.pq_disclosures.pq_fts_probe "query"    # ad-hoc search
"""

from __future__ import annotations

import services.runtime_env  # noqa: F401  # MUST be first: caps BLAS threads

import re
import sqlite3
import sys
from pathlib import Path

import polars as pl

DB = Path("data/_sandbox/pq_fts_probe.sqlite")
_CELLS = Path("data/_sandbox/pq_attachment_cells.parquet")
_ATTACH_CENSUS = Path("data/_sandbox/pq_attachment_census.parquet")
_TABLE_CENSUS = Path("data/_sandbox/pq_table_census.parquet")
_FULL = Path("data/_sandbox/pq_disclosures_full.parquet")

_MAX_BODY = 4_000       # chars of cell text per doc; enough to match, small enough to store
_MAX_QTEXT = 3_000

# Civic battery. Mixes (a) topics we KNOW are redundant (waiting lists) as controls,
# (b) the proven win (IDA land), (c) plausible hidden registers. A useful index
# distinguishes them; a useless one returns noise for all.
BATTERY = [
    "IDA land bank hectares sites",
    "vacant derelict site register local authority",
    "state land bank disposal",
    "hospital waiting list specialty",          # control: known redundant (NTPF)
    "employment permits nationality",           # control: known redundant (DETE)
    "military aircraft Shannon landing overflight",
    "data centre grid connection electricity",
    "asylum accommodation hotel contract",
    "school building programme cost",
    "housing delivery social units county",
    "consultancy external contracts spend",
    "flood relief scheme funding",
    "offshore wind foreshore licence",
    "nursing home inspection compliance",
]


def _questions_by_section() -> dict[str, str]:
    """xml_uri -> concatenated question text for that answer section."""
    df = (
        pl.scan_parquet(_FULL)
        .select(["xml_uri", "question_text"])
        .collect()
        .group_by("xml_uri")
        .agg(pl.col("question_text").str.concat(" || ").alias("qs"))
    )
    return {r["xml_uri"]: (r["qs"] or "")[:_MAX_QTEXT] for r in df.iter_rows(named=True)}


def _attachment_docs(qmap: dict[str, str]) -> list[tuple]:
    """One doc per attachment: real cell values as the body."""
    url_to_uri = {
        r["attachment_url"]: r["xml_uri"]
        for r in pl.read_parquet(_ATTACH_CENSUS).iter_rows(named=True)
    }
    cells = pl.scan_parquet(_CELLS)
    agg = (
        cells.group_by("attachment_url")
        .agg(
            pl.col("date").first(),
            pl.col("department").first(),
            pl.col("section_title").first(),
            pl.col("col_name").drop_nulls().unique().str.concat(" | ").alias("header"),
            pl.col("value").str.concat(" ").alias("body"),
            pl.len().alias("n_cells"),
        )
        .collect()
    )
    docs = []
    for r in agg.iter_rows(named=True):
        uri = url_to_uri.get(r["attachment_url"], "")
        docs.append(
            (
                "attachment",
                r["date"] or "",
                r["department"] or "",
                r["section_title"] or "",
                (r["header"] or "")[:1000],
                (r["body"] or "")[:_MAX_BODY],
                qmap.get(uri, ""),
                r["attachment_url"],
                int(r["n_cells"]),
            )
        )
    return docs


def _inline_table_docs(qmap: dict[str, str]) -> list[tuple]:
    """One doc per inline table. The census kept only header + first-column sample."""
    t = pl.read_parquet(_TABLE_CENSUS)
    docs = []
    for r in t.iter_rows(named=True):
        docs.append(
            (
                "inline_table",
                r["date"] or "",
                r["department"] or "",
                r["section_title"] or "",
                (r["header"] or "")[:1000],
                (r["first_col_sample"] or "")[:_MAX_BODY],
                qmap.get(r["xml_uri"], ""),
                r["xml_uri"],
                int(r["n_rows"] or 0),
            )
        )
    return docs


def build() -> sqlite3.Connection:
    DB.parent.mkdir(parents=True, exist_ok=True)
    if DB.exists():
        DB.unlink()
    con = sqlite3.connect(DB)
    # unicode61 keeps digits as tokens, so question refs (52988/22) and figures
    # (316.8079) stay searchable — verified in the battery below.
    con.executescript(
        """
        CREATE VIRTUAL TABLE docs USING fts5(
            kind, date, department, section_title, header, body, question_text, src,
            n UNINDEXED,
            tokenize='porter unicode61'
        );
        """
    )
    qmap = _questions_by_section()
    rows = _attachment_docs(qmap) + _inline_table_docs(qmap)
    con.executemany("INSERT INTO docs VALUES (?,?,?,?,?,?,?,?,?)", rows)
    con.commit()
    return con


def _fts_query(q: str) -> str:
    """Turn a natural phrase into an OR query.

    ⚠ FTS5 `MATCH 'a b c'` is an implicit AND — every term must be present. On the
    first battery run that returned ZERO hits for "IDA land bank hectares sites",
    a topic we had already extracted 317 rows from: no single doc happened to carry
    all five words. 4 of 14 battery queries failed the same way. BM25 already ranks
    docs matching more terms higher, so OR gives recall without losing precision at
    the top. Quoted phrases are passed through untouched.
    """
    if '"' in q or " OR " in q or " AND " in q:
        return q
    terms = [t for t in re.findall(r"[A-Za-z0-9']+", q) if len(t) > 1]
    return " OR ".join(terms) if terms else q


def search(con: sqlite3.Connection, q: str, limit: int = 6) -> list[tuple]:
    q = _fts_query(q)
    return con.execute(
        "SELECT kind, date, department, n, substr(header,1,70), substr(question_text,1,150), src "
        "FROM docs WHERE docs MATCH ? ORDER BY bm25(docs) LIMIT ?",
        (q, limit),
    ).fetchall()


def main(argv: list[str]) -> int:
    if DB.exists() and argv:
        con = sqlite3.connect(DB)
    else:
        print("building index ...")
        con = build()
        n = con.execute("SELECT count(*) FROM docs").fetchone()[0]
        print(f"indexed {n:,} docs -> {DB}")

    if argv:
        for row in search(con, argv[0], limit=10):
            print(f"[{row[0]:<12}] {row[1]} {row[2] or '':<14} n={row[3]:<6} {row[4]}")
            print(f"    Q: {row[5]}")
        return 0

    for q in BATTERY:
        hits = search(con, q, limit=3)
        print("=" * 78)
        print(f"QUERY: {q}   ({len(hits)} shown)")
        for row in hits:
            print(f"  [{row[0]:<12}] {row[1]} {(row[2] or '')[:14]:<14} n={row[3]:<6} {row[4]}")
            print(f"      Q: {(row[5] or '')[:130]}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
