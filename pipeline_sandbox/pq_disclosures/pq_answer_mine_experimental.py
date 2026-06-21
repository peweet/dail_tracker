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

Run (smoke):
    python -m pipeline_sandbox.pq_disclosures.pq_answer_mine_experimental --limit 30

FULL-CORPUS RUN (later — ~221k sections; deferred deliberately):
    # First pass — concurrent fetch fills the disk cache; resumable if killed.
    python -m pipeline_sandbox.pq_disclosures.pq_answer_mine_experimental \
        --limit 0 --workers 8 --out data/_sandbox/pq_disclosures_full.parquet
    # Then rank departments on the full corpus:
    python -m pipeline_sandbox.pq_disclosures.pq_transparency_report \
        --in data/_sandbox/pq_disclosures_full.parquet --min-n 50
  Notes: cache (c:/tmp/pq_answer_cache) makes reruns free, so a killed run
  resumes for nothing. Incremental refresh = re-run with --since <last_date>.
  At 8 workers expect a few hours; run in the background, not foreground.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
import urllib.request
import xml.etree.ElementTree as ET
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
_Q_OPEN = re.compile(r"^\s*\d+\.\s+Deput(?:y|ies)\s+(.+?)\s+asked\b", re.I)
# Oireachtas reference like [36013/25]
_REF = re.compile(r"\[(\d{1,6}/\d{2,4})\]")
# Euro figure: €413,000 / €1.6 million / €2,886,000 / €10m
_EURO = re.compile(r"€\s?\d[\d,]*(?:\.\d+)?(?:\s?(?:million|billion|bn|m)\b)?", re.I)

# --- transparency classification ------------------------------------------
# A refusal only counts if the QUESTION sought concrete data. This guards
# against flagging withholding language that appears in pure policy answers.
_DATA_SOUGHT = re.compile(
    r"\b(?:number|how many|amount|amounts|cost|costs|total|totals|list|detail|details"
    r"|breakdown|value|values|expenditure|spend|spent|paid|payments?|sum|figure|figures"
    r"|how much|tabular|statistics)\b",
    re.I,
)

# Hard refusals: department declines to give data it could be expected to hold.
_REFUSAL = [
    ("commercial_sensitivity", re.compile(
        r"commercial(?:ly)?\s+sensitiv|commercial(?:\s+in)?\s+confidence"
        r"|place the State at a (?:commercial )?disadvantage", re.I)),
    ("not_published", re.compile(
        r"\b(?:does not|do not|no longer)\s+publish|not\s+publish(?:ed)?\b", re.I)),
    ("not_held_centrally", re.compile(
        r"not\s+(?:held|collated|collected|compiled|maintained|recorded)\s+centrally"
        r"|do(?:es)?\s+not\s+(?:hold|collate|collect|compile)\b"
        r"|not\s+readily\s+available|information\s+is\s+not\s+(?:held|available)", re.I)),
    ("disproportionate_cost", re.compile(
        r"disproportionate\s+(?:amount\s+of\s+time|cost|use\s+of)", re.I)),
    ("privacy_safety", re.compile(
        r"unable\s+to\s+publish\s+any\s+information\s+that\s+would\s+identify"
        r"|to\s+protect\s+the\s+(?:privacy|safety|identity)\s+of"
        r"|could\s+identify\s+(?:an?\s+)?(?:individual|person|applicant)", re.I)),
    ("data_protection", re.compile(
        r"on\s+data\s+protection\s+grounds|under\s+(?:the\s+)?(?:GDPR|data\s+protection)"
        r"|prohibited\s+by\s+(?:the\s+)?data\s+protection|data\s+protection\s+(?:legislation|grounds|reasons)", re.I)),
    ("would_prejudice", re.compile(r"would\s+prejudice\b|prejudice\s+the\s+(?:State|negotiat|outcome)", re.I)),
]
# Deflection: legitimately the agency's data, not the minister's — tracked
# separately, NOT counted as a refusal in the headline transparency rate.
_DEFLECTION = re.compile(
    r"operational\s+matter\s+for|matter\s+for\s+the\s+(?:HSE|Garda|Commissioner|board"
    r"|management|relevant)|referred?\s+(?:the\s+)?(?:question|matter)\s+to", re.I)
# Positive disclosure beyond a bare € figure: tabulated data.
_TABULAR = re.compile(
    r"\b(?:table\s+below|following\s+table|set\s+out\s+(?:in\s+the\s+table|below)"
    r"|in\s+tabular\s+form|as\s+follows)\b", re.I)


def classify(question: str, answer: str) -> dict:
    """Classify one Q&A for the transparency index. Conservative: a refusal
    requires that the question sought data AND a refusal phrase is present."""
    data_sought = bool(_DATA_SOUGHT.search(question or ""))
    has_fig = bool(_EURO.search(answer or ""))
    has_tab = bool(_TABULAR.search(answer or ""))
    refusal_type = next((name for name, pat in _REFUSAL if pat.search(answer or "")), None)
    is_refusal = bool(data_sought and refusal_type)
    return {
        "data_sought": data_sought,
        "discloses": bool(has_fig or has_tab),
        "is_refusal": is_refusal,
        "refusal_type": refusal_type if is_refusal else None,
        "is_deflection": bool(data_sought and _DEFLECTION.search(answer or "")),
    }


def _strip_ns(tag: str) -> str:
    """'{http://...}question' -> 'question'."""
    return tag.rsplit("}", 1)[-1]


def _text(el: ET.Element) -> str:
    """All descendant text of an element, whitespace-collapsed."""
    return re.sub(r"\s+", " ", "".join(el.itertext())).strip()


def _section_uris_from_bronze(since: str | None, section_filter: str | None = None) -> list[dict]:
    """Distinct written-answer section XML URIs from the bronze questions JSON.

    One entry per section (a section bundles many Q&As), carrying the section
    title and department so we don't re-derive them from the XML.

    ``section_filter`` (regex, case-insensitive) keeps only sections whose title
    or department matches — lets us target a topic (e.g. asylum, RTÉ) and fetch
    only the relevant answers instead of the whole corpus.
    """
    pat = re.compile(section_filter, re.I) if section_filter else None
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
            title = ds.get("showAs") or ""
            dept = (q.get("to") or {}).get("showAs") or ""
            if pat and not (pat.search(title) or pat.search(dept)):
                continue
            seen[uri] = {
                "xml_uri": uri,
                "date": date,
                "department": dept or None,
                "section_title": title or None,
            }
    return sorted(seen.values(), key=lambda d: d["date"], reverse=True)


def _fetch(uri: str) -> str:
    """Fetch a section XML, caching to disk so reruns never re-hit the API."""
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    # Cache key must include the date: dbsect_833.xml exists for *every* sitting
    # day, so a basename-only key collides across dates.
    key = uri.split("/debateRecord/", 1)[-1].replace("/", "_")
    cache = _CACHE_DIR / key
    if cache.exists():
        return cache.read_text(encoding="utf-8")
    req = urllib.request.Request(uri, headers=_UA)
    data = urllib.request.urlopen(req, timeout=30).read().decode("utf-8")
    cache.write_text(data, encoding="utf-8")
    time.sleep(0.4)  # be polite to the API
    return data


def parse_section(xml: str, meta: dict) -> list[dict]:
    """Parse a written-answer section's AKN-XML into one row per question_ref.

    Structure (per Oireachtas AKN):
        <debateSection name="writtenAnswer">
          <question by="#Asker" eId="pq_892"><p>892. Deputy ... [36013/25]</p></question>
          <question .../>                       # grouped Qs share one reply
          <speech by="#Minister" as="#role"><from>Minister for ...</from><p>...</p></speech>
        </debateSection>

    Consecutive <question> elements accumulate; the next <speech> is their shared
    answer. Each question_ref emits a row carrying that answer.
    """
    try:
        root = ET.fromstring(xml)
    except ET.ParseError as e:
        logger.warning("xml parse error %s: %s", meta.get("xml_uri"), e)
        return []

    rows: list[dict] = []
    pending: list[dict] = []  # questions awaiting a reply

    def flush(answer: str, responder: str | None, minister_ref: str | None):
        figs = _EURO.findall(answer)
        has_reply = bool(answer) and "reply not received" not in answer.lower()
        for q in pending:
            cls = classify(q["text"], answer) if has_reply else {
                "data_sought": False, "discloses": False, "is_refusal": False,
                "refusal_type": None, "is_deflection": False,
            }
            rows.append(
                {
                    **{k: meta[k] for k in ("date", "department", "section_title", "xml_uri")},
                    "question_ref": q["ref"],
                    "deputy": q["deputy"],
                    "asker_ref": q["by"],
                    "responder": responder,
                    "minister_ref": minister_ref,
                    "question_text": q["text"][:2000],
                    "answer_text": answer[:6000],
                    "has_reply": has_reply,
                    "n_euro_figures": len(figs) if has_reply else 0,
                    "euro_figures": " | ".join(figs[:40]) if has_reply else "",
                    **cls,
                    "source_xml_url": meta["xml_uri"],
                }
            )

    # Walk the debateSection children in document order.
    for sec in root.iter():
        if _strip_ns(sec.tag) != "debateSection":
            continue
        for el in list(sec):
            tag = _strip_ns(el.tag)
            if tag == "question":
                txt = _text(el)
                m = _Q_OPEN.match(txt)
                for ref in _REF.findall(txt):
                    pending.append(
                        {
                            "ref": ref,
                            "text": txt,
                            "deputy": (m.group(1) if m else None),
                            "by": (el.get("by") or "").lstrip("#") or None,
                        }
                    )
            elif tag == "speech" and pending:
                responder = next(
                    (_text(c) for c in el if _strip_ns(c.tag) == "from"), None
                )
                paras = [
                    _text(c) for c in el if _strip_ns(c.tag) == "p"
                ]
                flush(" ".join(paras).strip(), responder, (el.get("by") or "").lstrip("#") or None)
                pending = []
    return rows


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, default=30,
                    help="max section XMLs to fetch; 0 = no cap (full corpus)")
    ap.add_argument("--since", default=None, help="only sections dated >= YYYY-MM-DD (incremental)")
    ap.add_argument("--section-filter", default=None, help="regex on section title/department to target a topic")
    ap.add_argument("--workers", type=int, default=1,
                    help="concurrent fetch threads (use 6-8 for a full-corpus run)")
    ap.add_argument("--out", default=str(_OUT), help="output parquet path")
    args = ap.parse_args(argv)

    setup_standalone_logging("pq_answer_mine")

    sections = _section_uris_from_bronze(args.since, args.section_filter)
    logger.info("written-answer sections available in bronze: %d", len(sections))
    if args.limit:
        sections = sections[: args.limit]
    logger.info("processing %d sections (limit=%s, since=%s, workers=%d)",
                len(sections), args.limit or "ALL", args.since, args.workers)

    # Fetch phase — concurrent and cache-aware (resumable: cached sections never
    # re-hit the API, so a killed full run resumes for free on the next launch).
    done = [0]

    def _grab(sec):
        try:
            _fetch(sec["xml_uri"])
        except Exception as e:  # log + continue; a full run must not die on one section
            logger.warning("fetch failed %s: %s", sec["xml_uri"], e)
        done[0] += 1
        if done[0] % 50 == 0:
            logger.info("  fetched %d/%d", done[0], len(sections))

    if args.workers > 1:
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            list(ex.map(_grab, sections))
    else:
        for sec in sections:
            _grab(sec)

    # Parse phase — sequential from cache (cheap, deterministic).
    rows: list[dict] = []
    for sec in sections:
        try:
            rows.extend(parse_section(_fetch(sec["xml_uri"]), sec))
        except Exception as e:
            logger.warning("parse failed %s: %s", sec["xml_uri"], e)

    if not rows:
        logger.warning("no Q&A rows parsed")
        return 1

    df = pd.DataFrame(rows).drop_duplicates(subset=["question_ref"])
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    save_parquet(df, out)

    # ---- smoke summary ----
    n = len(df)
    replied = int(df["has_reply"].sum())
    with_eur = int((df["n_euro_figures"] > 0).sum())
    logger.info("=" * 60)
    logger.info("parsed %d question/answer rows from %d sections", n, len(sections))
    logger.info("  with a substantive reply : %d (%.0f%%)", replied, 100 * replied / n)
    logger.info("  reply discloses €figure  : %d (%.0f%%)", with_eur, 100 * with_eur / n)
    logger.info("wrote %s", out)
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
