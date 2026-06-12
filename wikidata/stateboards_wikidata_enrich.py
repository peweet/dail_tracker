"""Wikidata enrichment for the State Boards roster — who is this person OUTSIDE the board?

The register (extractors/stateboards_roster_extract.py) publishes name + role +
term + basis of appointment but NO occupation/employer. For the subset of board
members notable enough to have a Wikidata item (company CEOs, academics, former
politicians — exactly the accountability-relevant subset), this module attaches
their public-record outside roles.

SOURCE: Wikidata SPARQL (WDQS), bulk VALUES queries — the MediaWiki search API
throttles anonymous clients to a crawl (429 + long Retry-After observed live),
so per-name wbsearchentities is not viable for ~1,900 names. Exact label/alias
lookups are POS-indexed in WDQS, so one query resolves ~80 names at once.

Unlike the TD socials ETL there is no ID property to join on — matching is BY
NAME, so the policy is deliberately conservative and every row carries its
match status:

    matched    exactly ONE living human whose label/alias equals the register
               name (en / en-gb / en-ca / ga) AND with an Irish signal
               (P27 = Ireland, or "irish"/"ireland" in the en description)
    ambiguous  2+ candidates passed the filter — no data attached
    none       queried, no candidate passed
    skipped    placeholder rows ("Vacant" etc.) — never queried
    NULL       not yet queried (smoke run / early abort)

Downstream/UI MUST surface matched rows as "possible Wikidata match", never as
asserted fact (name-collision risk is real; see wikidata_match column).

Reads  : data/silver/parquet/stateboards_roster.parquet
Caches : data/bronze/wikidata/stateboards_names_cache.json  (per-name verdicts;
         re-runs only query names not yet in the cache)
Writes : data/gold/parquet/stateboards_roster.parquet  (silver + wikidata_* cols)

PRIVACY: stores occupation/employer/position labels + QID only — no dates of
birth, no family, no contact data.

Usage:
    python wikidata/stateboards_wikidata_enrich.py --max-names 80   # smoke
    python wikidata/stateboards_wikidata_enrich.py                  # full
    python wikidata/stateboards_wikidata_enrich.py --cached-only    # offline
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
import unicodedata
from pathlib import Path

import polars as pl
import requests

try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
from services.parquet_io import save_parquet  # noqa: E402

logger = logging.getLogger(__name__)

_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
_UA = "dail-tracker/1.0 (civic accountability data project; stateboards enrichment)"
_LANGS = ("en", "en-gb", "en-ca", "ga")  # label languages tried per name
_CHUNK = 80  # names per match query (x4 language tags in VALUES)
_MAX_LABELS = 8  # cap per role list — politicians can hold dozens of P39s

_SILVER = _ROOT / "data" / "silver" / "parquet" / "stateboards_roster.parquet"
_CACHE = _ROOT / "data" / "bronze" / "wikidata" / "stateboards_names_cache.json"
_OUT = _ROOT / "data" / "gold" / "parquet" / "stateboards_roster.parquet"

# Register rows that are placeholders, not people.
_NON_PERSON = re.compile(r"^\s*(vacant|vacancy|vacancies|tbc|to be confirmed)\b", re.IGNORECASE)
_HONORIFIC = re.compile(
    r"^\s*(dr|prof|professor|mr|mrs|ms|cllr|councillor|sen|senator|judge|justice|rev|fr)\.?\s+",
    re.IGNORECASE,
)
# Trailing professional qualifications as published ("Adrienne Cawley, B.L.").
_SUFFIX = re.compile(r",?\s*(b\.?l\.?|s\.?c\.?|k\.?c\.?|q\.?c\.?)\s*$", re.IGNORECASE)

_WIKIDATA_COLS = {
    "wikidata_match": pl.Utf8,
    "wikidata_qid": pl.Utf8,
    "wikidata_url": pl.Utf8,
    "wikidata_label": pl.Utf8,
    "wikidata_description": pl.Utf8,
    "wikidata_occupations": pl.Utf8,
    "wikidata_employers": pl.Utf8,
    "wikidata_positions_held": pl.Utf8,
}


# ---------------------------------------------------------------------------
# Pure helpers — unit-testable, no network
# ---------------------------------------------------------------------------


def clean_name(name: str) -> str:
    """The register name as queried: honorific prefix + qualification suffix
    stripped, whitespace collapsed. Accents are KEPT — WDQS label lookup is
    exact, and register names carry correct fadas."""
    s = _HONORIFIC.sub("", (name or "").strip())
    s = _SUFFIX.sub("", s)
    return re.sub(r"\s+", " ", s).strip()


def fold_name(name: str) -> str:
    """Accent/case/honorific-insensitive comparison key ("Dr Seán Ó Foghlú" ==
    "sean o foghlu") — used for dedupe, not for the WDQS lookup."""
    s = unicodedata.normalize("NFD", clean_name(name))
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.lower()


def is_person_name(name: str) -> bool:
    return bool((name or "").strip()) and not _NON_PERSON.match(name)


def _sparql_literal(s: str) -> str:
    return '"' + s.replace("\\", "\\\\").replace('"', '\\"') + '"'


def match_query(names: list[str]) -> str:
    """One bulk match query: exact label/alias hits that are living humans,
    with the Irish-signal ingredients attached. Indexed lookups only — no
    regex/contains over the full label table."""
    values = " ".join(f"{_sparql_literal(n)}@{lang}" for n in names for lang in _LANGS)
    return f"""
SELECT DISTINCT ?name ?item ?itemLabel ?desc ?irishCitizen WHERE {{
  VALUES ?name {{ {values} }}
  ?item rdfs:label|skos:altLabel ?name .
  ?item wdt:P31 wd:Q5 .
  FILTER NOT EXISTS {{ ?item wdt:P570 [] }}
  BIND(EXISTS {{ ?item wdt:P27 wd:Q27 }} AS ?irishCitizen)
  OPTIONAL {{ ?item schema:description ?desc . FILTER(LANG(?desc) = "en") }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en,en-gb,en-ca,ga". }}
}}
"""


def roles_query(qids: list[str], prop: str) -> str:
    """Role labels (one property at a time — querying P106/P108/P39 together
    would cross-product the rows)."""
    values = " ".join(f"wd:{q}" for q in qids)
    return f"""
SELECT ?item ?vLabel WHERE {{
  VALUES ?item {{ {values} }}
  ?item wdt:{prop} ?v .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en,en-gb,en-ca,ga". }}
}}
"""


def decide(candidates: list[dict]) -> dict:
    """Apply the match policy to one name's candidate set (each candidate:
    qid/label/desc/irish_citizen, already living-human-exact-label filtered)."""
    passing = [
        c
        for c in candidates
        if c["irish_citizen"] or re.search(r"irish|ireland", c.get("desc") or "", re.IGNORECASE)
    ]
    if not passing:
        return {"match": "none", "n_candidates": 0}
    if len({c["qid"] for c in passing}) > 1:
        return {"match": "ambiguous", "n_candidates": len({c["qid"] for c in passing})}
    c = passing[0]
    return {
        "match": "matched",
        "n_candidates": 1,
        "qid": c["qid"],
        "label": c.get("label"),
        "description": c.get("desc"),
    }


# ---------------------------------------------------------------------------
# WDQS
# ---------------------------------------------------------------------------


def _sparql(query: str, attempts: int = 4) -> list[dict]:
    """POST one query; same long-backoff retry as wikidata_socials_etl (WDQS
    throttles to ~1 req/min during active outage windows)."""
    last: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            resp = requests.post(
                _SPARQL_ENDPOINT,
                data={"query": query},
                headers={"User-Agent": _UA, "Accept": "application/sparql-results+json"},
                timeout=120,
            )
            resp.raise_for_status()
            return resp.json()["results"]["bindings"]
        except Exception as exc:  # noqa: BLE001 — retry every transient cause
            last = exc
            wait = 65 * attempt
            logger.warning("WDQS attempt %d/%d failed: %s — sleeping %ds", attempt, attempts, exc, wait)
            if attempt < attempts:
                time.sleep(wait)
    raise RuntimeError(f"WDQS failed after {attempts} attempts: {last}")


def _val(binding: dict, var: str) -> str | None:
    return binding.get(var, {}).get("value")


def match_names_bulk(names: list[str]) -> dict[str, list[dict]]:
    """cleaned name -> candidate list (deduped by qid)."""
    out: dict[str, list[dict]] = {n: [] for n in names}
    rows = _sparql(match_query(names))
    for b in rows:
        name = _val(b, "name")
        uri = _val(b, "item") or ""
        qid = uri.rsplit("/", 1)[-1]
        if name not in out or not qid.startswith("Q"):
            continue
        if any(c["qid"] == qid for c in out[name]):
            continue
        out[name].append(
            {
                "qid": qid,
                "label": _val(b, "itemLabel"),
                "desc": _val(b, "desc"),
                "irish_citizen": _val(b, "irishCitizen") == "true",
            }
        )
    return out


def roles_bulk(qids: list[str]) -> dict[str, dict[str, list[str]]]:
    """qid -> {occupations, employers, positions_held} (labels, sorted, capped)."""
    out: dict[str, dict[str, list[str]]] = {q: {"occupations": [], "employers": [], "positions_held": []} for q in qids}
    for prop, key in (("P106", "occupations"), ("P108", "employers"), ("P39", "positions_held")):
        for i in range(0, len(qids), 200):
            for b in _sparql(roles_query(qids[i : i + 200], prop)):
                qid = (_val(b, "item") or "").rsplit("/", 1)[-1]
                label = _val(b, "vLabel")
                if qid in out and label and label not in out[qid][key]:
                    out[qid][key].append(label)
    for rec in out.values():
        for key in rec:
            rec[key] = sorted(rec[key])[:_MAX_LABELS]
    return out


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------


def _load_cache() -> dict:
    if _CACHE.exists():
        return json.loads(_CACHE.read_text(encoding="utf-8"))
    return {"names": {}}


def _save_cache(cache: dict) -> None:
    _CACHE.parent.mkdir(parents=True, exist_ok=True)
    _CACHE.write_text(json.dumps(cache, indent=1, sort_keys=True), encoding="utf-8")


def _record_to_row(name: str, rec: dict) -> dict:
    qid = rec.get("qid")
    return {
        "member_name": name,
        "wikidata_match": rec["match"],
        "wikidata_qid": qid,
        "wikidata_url": f"https://www.wikidata.org/wiki/{qid}" if qid else None,
        "wikidata_label": rec.get("label"),
        "wikidata_description": rec.get("description"),
        "wikidata_occupations": "; ".join(rec.get("occupations", [])) or None,
        "wikidata_employers": "; ".join(rec.get("employers", [])) or None,
        "wikidata_positions_held": "; ".join(rec.get("positions_held", [])) or None,
    }


def run(*, max_names: int = 0, cached_only: bool = False) -> dict:
    if not _SILVER.exists():
        raise SystemExit(f"stateboards enrichment: silver roster missing — run the extractor first ({_SILVER})")
    roster = pl.read_parquet(_SILVER)
    names = sorted(roster["member_name"].drop_nulls().unique().to_list())

    cache = _load_cache()
    name_cache: dict[str, dict] = cache["names"]

    todo = [n for n in names if n not in name_cache and is_person_name(n)]
    if max_names:
        todo = todo[:max_names]
    if cached_only:
        todo = []
    logger.info("stateboards enrichment: %d names, %d to query (%d cached)", len(names), len(todo), len(name_cache))

    # Phase 1 — bulk identity match, chunked; cache checkpointed per chunk so an
    # aborted run resumes where it stopped.
    try:
        for i in range(0, len(todo), _CHUNK):
            chunk = todo[i : i + _CHUNK]
            cleaned = {n: clean_name(n) for n in chunk}
            candidates = match_names_bulk(sorted(set(cleaned.values())))
            for orig, cl in cleaned.items():
                name_cache[orig] = decide(candidates.get(cl, []))
            _save_cache(cache)
            logger.info("  matched chunk %d-%d / %d", i + 1, i + len(chunk), len(todo))
    except RuntimeError as exc:
        logger.error("stateboards enrichment: WDQS gave up (%s) — building gold from cache so far", exc)

    # Phase 2 — outside-role labels for matched names that don't have them yet.
    pending = sorted({rec["qid"] for rec in name_cache.values() if rec.get("qid") and "occupations" not in rec})
    if pending and not cached_only:
        try:
            roles = roles_bulk(pending)
            for rec in name_cache.values():
                if rec.get("qid") in roles:
                    rec.update(roles[rec["qid"]])
            _save_cache(cache)
        except RuntimeError as exc:
            logger.error("stateboards enrichment: role fetch failed (%s) — qids kept, labels on next run", exc)

    rows = []
    for name in names:
        if not is_person_name(name):
            rec: dict = {"match": "skipped"}
        else:
            # Never queried (smoke run / early abort) -> null, NOT "none" —
            # "none" means "queried Wikidata and nothing passed".
            rec = name_cache.get(name) or {"match": None}
        rows.append(_record_to_row(name, rec))
    wd = pl.DataFrame(rows, schema={"member_name": pl.Utf8, **_WIKIDATA_COLS})

    gold = roster.join(wd, on="member_name", how="left").sort(["department", "body", "member_name"])
    save_parquet(gold, _OUT)

    stats = {
        "rows": gold.height,
        "names": len(names),
        "matched": int((wd["wikidata_match"] == "matched").sum()),
        "ambiguous": int((wd["wikidata_match"] == "ambiguous").sum()),
        "none": int((wd["wikidata_match"] == "none").sum()),
        "skipped": int((wd["wikidata_match"] == "skipped").sum()),
        "unqueried": int(wd["wikidata_match"].is_null().sum()),
    }
    logger.info("stateboards gold: wrote %s — %s", _OUT, stats)
    print(f"stateboards gold: {stats}")
    return stats


if __name__ == "__main__":
    from services.logging_setup import setup_standalone_logging

    setup_standalone_logging("stateboards_wikidata_enrich")
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--max-names", type=int, default=0, help="smoke run: query at most N uncached names")
    ap.add_argument("--cached-only", action="store_true", help="no network; build gold from the existing cache")
    args = ap.parse_args()
    run(max_names=args.max_names, cached_only=args.cached_only)
