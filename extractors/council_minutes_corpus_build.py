"""Materialise the council-minutes text corpus into a gold parquet for FTS serving.

The harvest/OCR pipeline (pipeline_sandbox/council_minutes/) leaves 870+ clean docs as
corpus/<council>/*.txt + meetings_clean.jsonl bookkeeping. This packs them into ONE
parquet (council, meeting, meeting_date, doc_type, source_status, source_url, body) so
the DuckDB FTS harness (mcp_server/text_fts.py corpus 'council_minutes' →
search_council_minutes) and any registered view can serve them — the council-side
sibling of the speeches/questions corpora. Provenance travels: source_status marks
OCR-derived text (Extracted band) vs born-digital.

Run:  .venv/Scripts/python -m extractors.council_minutes_corpus_build
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import polars as pl

from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[1]
SANDBOX = ROOT / "pipeline_sandbox" / "council_minutes"
OUT = ROOT / "data" / "gold" / "parquet" / "council_minutes_corpus.parquet"


def main() -> int:
    setup_standalone_logging("council_minutes_corpus_build")
    rows = []
    n_docs = 0
    for line in (SANDBOX / "meetings_clean.jsonl").read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        d = json.loads(line)
        tp = d.get("text_path")
        p = SANDBOX / tp if tp else None
        if not p or not p.exists():
            continue
        n_docs += 1
        text = p.read_text(encoding="utf-8", errors="replace")
        # CHUNK grain, not doc grain: minutes run 10-500KB, so a doc-grain hit snippets
        # the meeting-notice boilerplate at the head instead of the matched passage
        # (measured 2026-08-01 — first build was doc-grain and every concise hit showed
        # "Notice of Meeting"). ~2,000-char chunks split on paragraph boundaries keep
        # BM25 focused and make the concise snippet BE the relevant passage.
        chunks, buf = [], ""
        for para in text.split("\n\n"):
            if len(buf) + len(para) > 2000 and buf:
                chunks.append(buf)
                buf = para
            else:
                buf = f"{buf}\n\n{para}" if buf else para
        if buf.strip():
            chunks.append(buf)
        base = {
            "council": "Galway County" if d["local_authority"] == "Galway" else d["local_authority"],
            "meeting": d.get("meeting", "")[:120],
            "meeting_date": d.get("meeting_date", "") or "",
            "doc_type": d.get("doc_type", ""),
            "source_status": d.get("status", ""),
            "source_url": d.get("url", "") or "",
        }
        rows += [{**base, "chunk": i, "body": c} for i, c in enumerate(chunks)]
    df = pl.DataFrame(rows)
    save_parquet(df, OUT)
    log.info("council_minutes_corpus: %d chunks from %d docs / %d councils -> %s",
             len(df), n_docs, df["council"].n_unique(), OUT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
