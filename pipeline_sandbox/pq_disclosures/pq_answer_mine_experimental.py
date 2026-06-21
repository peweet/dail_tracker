"""
pq_answer_mine_experimental.py — EXPERIMENTAL parliamentary-answer disclosure miner.

Status: sandbox prototype, NOT wired into pipeline.py. Writes only to an isolated
sandbox parquet; touches no canonical fact. Gate any app surface behind
DAIL_EXPERIMENTAL=1 (see project convention).

Idea
----
Our `questions.parquet` holds the *question* text but not the minister's *answer*,
where the real disclosures live (named bodies + specific euro figures that exist in
no structured dataset). Each question's bronze record already carries the AKN-XML
URI of the written-answer section that contains its reply
(`question.debateSection.formats.xml.uri`, under `/writtens/`). So we don't crawl:
we read the bronze JSON we already fetched, dedupe section URIs, fetch a *bounded*
number of section XMLs, parse the block sequence into (question -> reply) pairs, and
extract euro figures from the reply prose.

This is the engine that would have surfaced the NCI €10m capital / B4-subhead grant
breakdown automatically.

Bounded by design: `--limit` caps how many section XMLs are fetched (smoke default 30),
`--since` scopes to recent dates. Fetched XML is cached under c:/tmp so reruns are free.

Run:
    python -m pipeline_sandbox.pq_disclosures.pq_answer_mine_experimental --limit 30
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
import urllib.request
from pathlib import Path

import pandas as pd

from config import BRONZE_DIR
from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

logger = logging.getLogger(__name__)

_BRONZE_QUESTIONS = BRONZE_DIR / "questions" / "questions_results.json"
_CACHE_DIR = Path("c:/tmp/pq_answer_cache")
_OUT = Path("data/_sandbox/pq_disclosures_experimental.parquet")
_UA = {"User-Agent": "dail-extractor pq-disclosure-prototype (sandbox)"}

# Question opener: "892. Deputy Donna McGettigan asked the Minister ..."
_Q_OPEN = re.compile(r"^\s*(\d+)\.\s+Deput(?:y|ies)\s+(.+?)\s+asked\b", re.I)
# Responder line that introduces the reply, e.g. "Minister for Health (Deputy ...)"
_RESPONDER = re.compile(r"^\s*Ministers?(?:\s+of\s+State)?\b", re.I)
# Oireachtas reference like [36013/25]
_REF = re.compile(r"\[(\d{1,6}/\d{2,4})\]")
# Euro figure: €413,000 / €1.6 million / €2,886,000 / €10m
_EURO = re.compile(r"€\s?\d[\d,]*(?:\.\d+)?(?:\s?(?:million|billion|bn|m)\b)?", re.I)

# Masthead boilerplate blocks to ignore (bilingual Official Report header).
_BOILERPLATE = re.compile(
    r"PARLAIMINTE|PARLIAMENTARY DEBATES|DÁIL ÉIREANN|TUAIRISC|OFFICIAL REPORT"
    r"|^Vol\.|^No\.|^D[ée] |^Tuesday|^Wednesday|^Thursday|^Monday|^Friday",
    re.I,
)


def _section_uris_from_bronze(since: str | None) -> list[dict]:
    """Distinct written-answer section XML URIs from the bronze questions JSON.

    One entry per section (a section bundles many Q&As), carrying the section
    title and department so we don't re-derive them from the XML.
    """
    raw = json.load(open(_BRONZE_QUESTIONS, encoding="utf-8"))
    seen: dict[str, dict] = {}
    for page in raw:
        for r in page.get("results") or []:
            q = r.get("question", {})
            if q.get("questionType") != "written":
                continue
            date = (q.get("date") or "")[:10]
            if since and date < since:
                continue
            ds = q.get("debateSection") or {}
            uri = ((ds.get("formats") or {}).get("xml") or {}).get("uri")
            if not uri or uri in seen:
                continue
            seen[uri] = {
                "xml_uri": uri,
                "date": date,
                "department": (q.get("to") or {}).get("showAs"),
                "section_title": ds.get("showAs"),
            }
    return sorted(seen.values(), key=lambda d: d["date"], reverse=True)


def _fetch(uri: str) -> str:
    """Fetch a section XML, caching to disk so reruns never re-hit the API."""
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache = _CACHE_DIR / (uri.rsplit("/", 1)[-1])
    if cache.exists():
        return cache.read_text(encoding="utf-8")
    req = urllib.request.Request(uri, headers=_UA)
    data = urllib.request.urlopen(req, timeout=30).read().decode("utf-8")
    cache.write_text(data, encoding="utf-8")
    time.sleep(0.4)  # be polite to the API
    return data


def _blocks(xml: str) -> list[str]:
    """Ordered, whitespace-collapsed text of each <block> after </meta>."""
    body = xml.split("</meta>", 1)[-1]
    out = []
    for raw in re.findall(r"<block\b[^>]*>(.*?)</block>", body, re.S):
        txt = re.sub(r"<[^>]+>", " ", raw)
        txt = re.sub(r"\s+", " ", txt).strip()
        if txt and not _BOILERPLATE.search(txt):
            out.append(txt)
    return out


def parse_section(blocks: list[str], meta: dict) -> list[dict]:
    """Walk a section's blocks into one row per question_ref with its reply.

    Grouped questions (several asked together, one shared reply) each get a row
    carrying the same answer text.
    """
    rows: list[dict] = []
    mode = "seek"
    q_text: list[str] = []
    refs: list[str] = []
    qnums: list[str] = []
    deputies: list[str] = []
    responder: str | None = None
    answer: list[str] = []

    def flush():
        if not refs:
            return
        ans = " ".join(answer).strip()
        figs = _EURO.findall(ans)
        for ref in refs:
            rows.append(
                {
                    **meta,
                    "question_ref": ref,
                    "question_numbers": ", ".join(dict.fromkeys(qnums)),
                    "deputies": "; ".join(dict.fromkeys(deputies)),
                    "responder": responder,
                    "question_text": " ".join(q_text).strip()[:2000],
                    "answer_text": ans[:6000],
                    "has_reply": bool(ans) and "reply not received" not in ans.lower(),
                    "n_euro_figures": len(figs),
                    "euro_figures": " | ".join(figs[:40]),
                    "source_xml_url": meta["xml_uri"],
                }
            )

    for b in blocks:
        m = _Q_OPEN.match(b)
        if m:
            if mode == "answer":  # previous Q&A finished
                flush()
                q_text, refs, qnums, deputies, responder, answer = [], [], [], [], None, []
            mode = "question"
            q_text.append(b)
            refs += _REF.findall(b)
            qnums.append(m.group(1))
            deputies.append(m.group(2))
        elif _RESPONDER.match(b) and mode == "question":
            responder = b
            mode = "answer"
        elif mode == "answer":
            answer.append(b)
        elif mode == "question":
            q_text.append(b)
            refs += _REF.findall(b)
    flush()
    return rows


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, default=30, help="max section XMLs to fetch (smoke bound)")
    ap.add_argument("--since", default=None, help="only sections dated >= YYYY-MM-DD")
    args = ap.parse_args(argv)

    setup_standalone_logging("pq_answer_mine")

    sections = _section_uris_from_bronze(args.since)
    logger.info("written-answer sections available in bronze: %d", len(sections))
    sections = sections[: args.limit]
    logger.info("fetching %d sections (limit=%s, since=%s)", len(sections), args.limit, args.since)

    rows: list[dict] = []
    for i, sec in enumerate(sections, 1):
        try:
            xml = _fetch(sec["xml_uri"])
            rows.extend(parse_section(_blocks(xml), sec))
        except Exception as e:  # prototype: log + continue, never crash the run
            logger.warning("section %s failed: %s", sec["xml_uri"], e)
        if i % 10 == 0:
            logger.info("  ...%d/%d sections", i, len(sections))

    if not rows:
        logger.warning("no Q&A rows parsed")
        return 1

    df = pd.DataFrame(rows).drop_duplicates(subset=["question_ref"])
    _OUT.parent.mkdir(parents=True, exist_ok=True)
    save_parquet(df, _OUT)

    # ---- smoke summary ----
    n = len(df)
    replied = int(df["has_reply"].sum())
    with_eur = int((df["n_euro_figures"] > 0).sum())
    logger.info("=" * 60)
    logger.info("parsed %d question/answer rows from %d sections", n, len(sections))
    logger.info("  with a substantive reply : %d (%.0f%%)", replied, 100 * replied / n)
    logger.info("  reply discloses €figure  : %d (%.0f%%)", with_eur, 100 * with_eur / n)
    logger.info("wrote %s", _OUT)
    top = df[df.n_euro_figures > 0].sort_values("n_euro_figures", ascending=False).head(8)
    logger.info("--- richest disclosures (most figures) ---")
    for _, r in top.iterrows():
        logger.info(
            "[%s] %s | %s figs | %s",
            r["question_ref"],
            (r["department"] or "")[:32],
            r["n_euro_figures"],
            r["euro_figures"][:120],
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
