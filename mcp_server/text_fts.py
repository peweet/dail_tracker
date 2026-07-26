"""BM25 full-text search over the speech/question corpora (DuckDB `fts` extension).

Additive 2026-07-25: the member_speeches / get_member_questions tools filter by
ILIKE substring INSIDE one member's feed; nothing answered "who said what about X"
corpus-wide, and substring match has no ranking or stemming ("housing crisis"
misses "crisis in housing"). This module materialises the two text corpora into a
derived cache DB (.cache/text_fts.duckdb, gitignored, rebuilt when the source
view's fingerprint changes) and serves BM25-ranked hits with a porter stemmer.

The ILIKE paths are untouched — new capability, not a replacement, so existing
behaviour cannot regress. Build cost is paid once per corpus refresh (CTAS from
the view + PRAGMA create_fts_index); the fingerprint (row count + max date) is
checked at most once per process per corpus.

Requires the `fts` extension: DuckDB autoloads it on first use, downloading once
per DuckDB version. Offline with no cached extension -> a clear {"error"}, never
a crash.
"""

from __future__ import annotations

import contextlib
from pathlib import Path

import duckdb

CACHE_REL = ".cache/text_fts.duckdb"

CORPORA = {
    "speeches": {
        "view": "v_member_speeches",
        "date_col": "speech_date",
        "text_col": "speech_text",
        "cols": ("speech_date", "house", "business", "speaker_raw", "unique_member_code", "speech_text", "debate_url"),
    },
    "questions": {
        "view": "v_member_questions",
        "date_col": "question_date",
        "text_col": "question_text",
        "cols": (
            "question_date",
            "question_type",
            "ministry",
            "topic",
            "question_text",
            "question_ref",
            "oireachtas_url",
            "unique_member_code",
        ),
    },
}

_CHECKED: dict[str, bool] = {}  # fingerprint verified once per process per corpus
_SNIPPET = 280


def _fingerprint(cur, spec) -> tuple[int, str]:
    n, maxd = cur.execute(
        f"SELECT count(*), coalesce(max({spec['date_col']})::varchar, '') FROM {spec['view']}"
    ).fetchone()
    return int(n), str(maxd)


def _cache_fingerprint(cache: Path, kind: str) -> tuple[int, str] | None:
    try:
        con = duckdb.connect(str(cache), read_only=True)
        try:
            row = con.execute("SELECT n, maxd FROM meta WHERE kind = ?", [kind]).fetchone()
        finally:
            con.close()
        return (int(row[0]), str(row[1])) if row else None
    except Exception:
        return None


def _build(kind: str, cur, repo: Path, fp: tuple[int, str]) -> None:
    """Materialise one corpus into the cache via ATTACH on the union connection,
    then index it in a direct connection (create_fts_index wants an unqualified
    table name). Raises on failure — caller turns that into an {"error"}."""
    spec = CORPORA[kind]
    cache = repo / CACHE_REL
    cache.parent.mkdir(parents=True, exist_ok=True)
    cols = ", ".join(spec["cols"])
    with contextlib.suppress(Exception):
        cur.execute("DETACH textfts")
    cur.execute(f"ATTACH '{cache.as_posix()}' AS textfts")
    try:
        cur.execute(
            f"CREATE OR REPLACE TABLE textfts.{kind} AS SELECT row_number() OVER () AS rid, {cols} FROM {spec['view']}"
        )
    finally:
        cur.execute("DETACH textfts")
    con = duckdb.connect(str(cache))
    try:
        with contextlib.suppress(Exception):  # first build — no index yet
            con.execute(f"PRAGMA drop_fts_index('{kind}')")
        # defaults: porter stemmer, english stopwords, lowercase — exactly what we want
        con.execute(f"PRAGMA create_fts_index('{kind}', 'rid', '{spec['text_col']}')")
        # meta is written ONLY after a successful index build — a fingerprint written
        # earlier would mark a half-built cache as fresh and it would never rebuild
        con.execute("CREATE TABLE IF NOT EXISTS meta (kind VARCHAR PRIMARY KEY, n BIGINT, maxd VARCHAR)")
        con.execute("DELETE FROM meta WHERE kind = ?", [kind])
        con.execute("INSERT INTO meta VALUES (?, ?, ?)", [kind, fp[0], fp[1]])
    finally:
        con.close()


def _ensure(kind: str, cur, repo: Path) -> None:
    if _CHECKED.get(kind):
        return
    spec = CORPORA[kind]
    fp = _fingerprint(cur, spec)
    if _cache_fingerprint(repo / CACHE_REL, kind) != fp:
        _build(kind, cur, repo, fp)
    _CHECKED[kind] = True


def search(kind: str, query: str, cur, repo: Path, year: int = 0, limit: int = 10) -> dict:
    """BM25-ranked corpus hits. `cur` is a cursor on the union connection (used for
    the staleness fingerprint and, when stale, the rebuild)."""
    if kind not in CORPORA:
        return {"error": f"unknown corpus: {kind}"}
    if not query.strip():
        return {"error": "empty query"}
    spec = CORPORA[kind]
    try:
        _ensure(kind, cur, repo)
    except Exception as e:  # noqa: BLE001 — surface, never crash the server
        _CHECKED.pop(kind, None)
        return {
            "error": f"corpus build failed ({type(e).__name__}: {e}) — "
            "first build needs the DuckDB fts extension (one-time download)"
        }
    limit = max(1, min(int(limit), 50))
    where = ["score IS NOT NULL"]
    params: list = [query.strip()]
    if year:
        where.append(f"year({spec['date_col']}) = ?")
        params.append(int(year))
    params.append(limit)
    try:
        con = duckdb.connect(str(repo / CACHE_REL), read_only=True)
        try:
            con.execute("INSTALL fts; LOAD fts")  # match_bm25 macro needs the extension loaded
            rows = con.execute(
                f"SELECT *, fts_main_{kind}.match_bm25(rid, ?) AS score FROM {kind} "
                f"WHERE {' AND '.join(where)} ORDER BY score DESC LIMIT ?",
                params,
            ).fetchall()
            names = [d[0] for d in con.description]
        finally:
            con.close()
    except Exception as e:  # noqa: BLE001
        return {"error": f"search failed ({type(e).__name__}: {e})"}
    out = []
    for r in rows:
        rec = dict(zip(names, r, strict=False))
        rec.pop("rid", None)
        txt = str(rec.get(spec["text_col"]) or "")
        if len(txt) > _SNIPPET:
            rec[spec["text_col"]] = txt[:_SNIPPET] + "…"
        rec["score"] = round(float(rec["score"]), 3)
        rec[spec["date_col"]] = str(rec[spec["date_col"]])
        out.append(rec)
    return {
        "corpus": kind,
        "query": query,
        "hits": out,
        "count": len(out),
        "ranking": "bm25 + porter stemming (this is relevance search, not the "
        "member feed — use member_speeches/get_member_questions for a "
        "TD's complete record)",
    }
