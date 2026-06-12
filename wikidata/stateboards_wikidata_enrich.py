"""Wikidata CANDIDATE-QUEUE generator for the State Boards roster — NOT pipeline-wired.

This module never feeds gold. It exists only to regenerate the human review
queue behind data/_meta/stateboards_wikidata_curated.csv — the HAND-CURATED
identity list that extractors/stateboards_roster_extract.py joins into gold.

Why demoted (2026-06-12): the automated name match was audited and ~1 in 4
"matched" names was the wrong same-named person ("Irish singer" on the Central
Bank Commission, hurlers on the Medical Council, a 16th-century bishop on the
Law Reform Commission). Name-only matching has no discriminating key (the
register publishes no DOB/employer), so it cannot reach publishable precision —
only a human can split the Makhloufs from the hurlers. The curated CSV is that
human pass (66 identities, curated 2026-06-12, 8 externally verified).

RE-CURATION WORKFLOW (when the roster gains new names):
    1. run the extractor (refreshes silver)
    2. python wikidata/stateboards_wikidata_enrich.py
       -> appends new-name verdicts to the bronze cache, then writes
          data/bronze/wikidata/stateboards_candidates.csv = matched/ambiguous
          names NOT yet in the curated CSV
    3. a human reviews the candidates (verify identity!) and appends approved
       rows to data/_meta/stateboards_wikidata_curated.csv
    4. re-run the extractor (or its build_gold) to fold them into gold

SOURCE: Wikidata SPARQL (WDQS), bulk VALUES queries — the MediaWiki search API
throttles anonymous clients to a crawl (429 + long Retry-After observed live),
so per-name wbsearchentities is not viable for ~1,900 names. Exact PREFERRED-
label lookups (no aliases — see match_query) are indexed in WDQS, so one query
resolves ~80 names at once. Match policy in decide(): living human + exact
label (en/en-gb/en-ca/ga) + Irish signal; 2+ survivors -> ambiguous.

Reads  : data/silver/parquet/stateboards_roster.parquet
         data/_meta/stateboards_wikidata_curated.csv      (to exclude done names)
Caches : data/bronze/wikidata/stateboards_names_cache.json (per-name verdicts;
         re-runs only query names not yet in the cache)
Writes : data/bronze/wikidata/stateboards_candidates.csv   (the review queue)

PRIVACY: stores occupation/employer/position labels + QID only — no dates of
birth, no family, no contact data.

Usage:
    python wikidata/stateboards_wikidata_enrich.py --max-names 80   # smoke
    python wikidata/stateboards_wikidata_enrich.py                  # full
    python wikidata/stateboards_wikidata_enrich.py --cached-only    # offline
"""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import re
import sys
import time
import unicodedata
from pathlib import Path

import polars as pl
import requests

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))

logger = logging.getLogger(__name__)

_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
_UA = "dail-tracker/1.0 (civic accountability data project; stateboards enrichment)"
_LANGS = ("en", "en-gb", "en-ca", "ga")  # label languages tried per name
_CHUNK = 80  # names per match query (x4 language tags in VALUES)
_MAX_LABELS = 8  # cap per role list — politicians can hold dozens of P39s

_SILVER = _ROOT / "data" / "silver" / "parquet" / "stateboards_roster.parquet"
_CACHE = _ROOT / "data" / "bronze" / "wikidata" / "stateboards_names_cache.json"
_CURATED = _ROOT / "data" / "_meta" / "stateboards_wikidata_curated.csv"
_CANDIDATES = _ROOT / "data" / "bronze" / "wikidata" / "stateboards_candidates.csv"

# Register rows that are placeholders, not people.
_NON_PERSON = re.compile(r"^\s*(vacant|vacancy|vacancies|tbc|to be confirmed)\b", re.IGNORECASE)
_HONORIFIC = re.compile(
    r"^\s*(the|hon|dr|prof|professor|mr|mrs|ms|cllr|councillor|sen|senator|judge|justice|rev|fr)\.?\s+",
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
    s = (name or "").strip()
    # Iterate: register styles stack honorifics ("The Hon. Mr. Justice X").
    while True:
        stripped = _HONORIFIC.sub("", s)
        if stripped == s:
            break
        s = stripped
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
    """One bulk match query: exact PREFERRED-label hits that are living humans,
    with the Irish-signal ingredients attached. Indexed lookups only — no
    regex/contains over the full label table.

    Deliberately NOT skos:altLabel: alias matching pulled in people primarily
    known under a different name (caught live: register "Aidan Murphy" -> actor
    Aidan Gillen via birth-name alias). The register lists the name a member
    serves under; requiring the preferred label to equal it is the right bar."""
    values = " ".join(f"{_sparql_literal(n)}@{lang}" for n in names for lang in _LANGS)
    return f"""
SELECT DISTINCT ?name ?item ?itemLabel ?desc ?irishCitizen WHERE {{
  VALUES ?name {{ {values} }}
  ?item rdfs:label ?name .
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
        c for c in candidates if c["irish_citizen"] or re.search(r"irish|ireland", c.get("desc") or "", re.IGNORECASE)
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
        logger.error("stateboards enrichment: WDQS gave up (%s) — queue built from cache so far", exc)

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

    # The review queue: matched/ambiguous names a human has NOT yet curated.
    # This NEVER touches gold — approved rows are appended by hand to
    # data/_meta/stateboards_wikidata_curated.csv (see module docstring).
    already_curated: set[str] = set()
    if _CURATED.exists():
        already_curated = set(pl.read_csv(_CURATED, infer_schema_length=0)["member_name"].to_list())

    queue = [
        _record_to_row(name, rec)
        for name in names
        if (rec := name_cache.get(name)) is not None
        and rec["match"] in ("matched", "ambiguous")
        and name not in already_curated
    ]
    qdf = pl.DataFrame(queue, schema={"member_name": pl.Utf8, **_WIKIDATA_COLS})
    _CANDIDATES.parent.mkdir(parents=True, exist_ok=True)
    qdf.write_csv(_CANDIDATES)

    stats = {
        "names": len(names),
        "cached": len(name_cache),
        "already_curated": len(already_curated),
        "review_queue": qdf.height,
    }
    logger.info("stateboards candidates: wrote %s — %s", _CANDIDATES, stats)
    print(f"stateboards candidates: {stats} -> {_CANDIDATES}")
    return stats


if __name__ == "__main__":
    from services.logging_setup import setup_standalone_logging

    setup_standalone_logging("stateboards_wikidata_enrich")
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--max-names", type=int, default=0, help="smoke run: query at most N uncached names")
    ap.add_argument("--cached-only", action="store_true", help="no network; rebuild the queue from the existing cache")
    args = ap.parse_args()
    run(max_names=args.max_names, cached_only=args.cached_only)
