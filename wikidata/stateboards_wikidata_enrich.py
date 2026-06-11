"""Wikidata enrichment for the State Boards roster — who is this person OUTSIDE the board?

The register (extractors/stateboards_roster_extract.py) publishes name + role +
term + basis of appointment but NO occupation/employer. For the subset of board
members notable enough to have a Wikidata item (company CEOs, academics, former
politicians — exactly the accountability-relevant subset), this module attaches
their public-record outside roles.

SOURCE: Wikidata MediaWiki API (wbsearchentities + wbgetentities). Unlike the
TD socials ETL there is no ID property to join on — matching is BY NAME, so the
policy is deliberately conservative and every row carries its match status:

    matched    exactly ONE living human whose en label/alias equals the name
               (accent/case-folded) AND with an Irish signal (P27 = Ireland,
               or "irish"/"ireland" in the en description)
    ambiguous  2+ candidates passed the filter — no data attached
    none       no candidate passed
    skipped    placeholder rows ("Vacant" etc.) — never queried

Downstream/UI MUST surface matched rows as "possible Wikidata match", never as
asserted fact (name-collision risk is real; see wikidata_match column).

Reads  : data/silver/parquet/stateboards_roster.parquet
Caches : data/bronze/wikidata/stateboards_names_cache.json  (per-name verdicts;
         re-runs only query names not yet in the cache)
Writes : data/gold/parquet/stateboards_roster.parquet  (silver + wikidata_* cols)

PRIVACY: stores occupation/employer/position labels + QID only — no dates of
birth, no family, no contact data.

Usage:
    python wikidata/stateboards_wikidata_enrich.py --max-names 10   # smoke
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

_API = "https://www.wikidata.org/w/api.php"
_UA = "dail-tracker/1.0 (civic accountability data project; stateboards enrichment)"
_SLEEP = 0.1

_SILVER = _ROOT / "data" / "silver" / "parquet" / "stateboards_roster.parquet"
_CACHE = _ROOT / "data" / "bronze" / "wikidata" / "stateboards_names_cache.json"
_OUT = _ROOT / "data" / "gold" / "parquet" / "stateboards_roster.parquet"

_Q_HUMAN = "Q5"
_Q_IRELAND = "Q27"
_P_INSTANCE_OF = "P31"
_P_CITIZENSHIP = "P27"
_P_DEATH = "P570"
_P_OCCUPATION = "P106"
_P_EMPLOYER = "P108"
_P_POSITION = "P39"
_MAX_LABELS = 8  # cap per list — politicians can hold dozens of P39s

# Register rows that are placeholders, not people.
_NON_PERSON = re.compile(r"^\s*(vacant|vacancy|vacancies|tbc|to be confirmed)\b", re.IGNORECASE)
_HONORIFIC = re.compile(
    r"^\s*(dr|prof|professor|mr|mrs|ms|cllr|councillor|sen|senator|judge|justice|rev|fr)\.?\s+",
    re.IGNORECASE,
)

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


def fold_name(name: str) -> str:
    """Accent/case/honorific-insensitive comparison key ("Dr Seán Ó Foghlú" ==
    "sean o foghlu"). NFD-decompose then drop combining marks — same trick as
    the TD name join key."""
    s = _HONORIFIC.sub("", (name or "").strip())
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", s).strip().lower()


def is_person_name(name: str) -> bool:
    return bool((name or "").strip()) and not _NON_PERSON.match(name)


def _claim_qids(entity: dict, prop: str) -> list[str]:
    out = []
    for claim in entity.get("claims", {}).get(prop, []):
        value = claim.get("mainsnak", {}).get("datavalue", {}).get("value")
        if isinstance(value, dict) and value.get("id"):
            out.append(value["id"])
    return out


def candidate_passes(entity: dict, name: str) -> bool:
    """Living human + exact (folded) label/alias match + Irish signal."""
    if _Q_HUMAN not in _claim_qids(entity, _P_INSTANCE_OF):
        return False
    if entity.get("claims", {}).get(_P_DEATH):
        return False
    key = fold_name(name)
    label = entity.get("labels", {}).get("en", {}).get("value", "")
    aliases = [a.get("value", "") for a in entity.get("aliases", {}).get("en", [])]
    if key not in {fold_name(x) for x in [label, *aliases] if x}:
        return False
    desc = entity.get("descriptions", {}).get("en", {}).get("value", "").lower()
    irish = _Q_IRELAND in _claim_qids(entity, _P_CITIZENSHIP) or "irish" in desc or "ireland" in desc
    return irish


# ---------------------------------------------------------------------------
# Wikidata API
# ---------------------------------------------------------------------------


def _get(session: requests.Session, params: dict, attempts: int = 4) -> dict:
    params = {**params, "format": "json"}
    last: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            r = session.get(_API, params=params, headers={"User-Agent": _UA}, timeout=60)
            r.raise_for_status()
            time.sleep(_SLEEP)
            return r.json()
        except Exception as exc:  # noqa: BLE001 — retry transient causes
            last = exc
            logger.warning("wikidata api attempt %d/%d failed: %s", attempt, attempts, exc)
            time.sleep(5 * attempt)
    raise RuntimeError(f"wikidata api failed after {attempts} attempts: {last}")


def search_candidates(session: requests.Session, name: str) -> list[str]:
    data = _get(
        session,
        {
            "action": "wbsearchentities",
            "search": _HONORIFIC.sub("", name.strip()),
            "language": "en",
            "uselang": "en",
            "type": "item",
            "limit": 10,
        },
    )
    return [hit["id"] for hit in data.get("search", [])]


def fetch_entities(session: requests.Session, qids: list[str], props: str) -> dict[str, dict]:
    """Batched wbgetentities (50/request)."""
    out: dict[str, dict] = {}
    for i in range(0, len(qids), 50):
        batch = qids[i : i + 50]
        data = _get(
            session,
            {
                "action": "wbgetentities",
                "ids": "|".join(batch),
                "props": props,
                "languages": "en",
            },
        )
        out.update(data.get("entities", {}))
    return out


def resolve_name(session: requests.Session, name: str, label_cache: dict[str, str]) -> dict:
    """One register name -> cache record (match status + outside-role labels)."""
    qids = search_candidates(session, name)
    if not qids:
        return {"match": "none", "n_candidates": 0}
    entities = fetch_entities(session, qids, "labels|aliases|descriptions|claims")
    passing = [e for e in entities.values() if candidate_passes(e, name)]
    if not passing:
        return {"match": "none", "n_candidates": 0}
    if len(passing) > 1:
        return {"match": "ambiguous", "n_candidates": len(passing)}

    ent = passing[0]
    role_qids = {
        "occupations": _claim_qids(ent, _P_OCCUPATION)[:_MAX_LABELS],
        "employers": _claim_qids(ent, _P_EMPLOYER)[:_MAX_LABELS],
        "positions_held": _claim_qids(ent, _P_POSITION)[:_MAX_LABELS],
    }
    missing = sorted({q for qs in role_qids.values() for q in qs} - label_cache.keys())
    if missing:
        for qid, e in fetch_entities(session, missing, "labels").items():
            label_cache[qid] = e.get("labels", {}).get("en", {}).get("value", qid)
    labels = {k: sorted(label_cache.get(q, q) for q in qs) for k, qs in role_qids.items()}
    return {
        "match": "matched",
        "n_candidates": 1,
        "qid": ent["id"],
        "label": ent.get("labels", {}).get("en", {}).get("value"),
        "description": ent.get("descriptions", {}).get("en", {}).get("value"),
        **labels,
    }


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------


def _load_cache() -> dict:
    if _CACHE.exists():
        return json.loads(_CACHE.read_text(encoding="utf-8"))
    return {"names": {}, "labels": {}}


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
    label_cache: dict[str, str] = cache["labels"]

    todo = [n for n in names if n not in name_cache and is_person_name(n)]
    if max_names:
        todo = todo[:max_names]
    if cached_only:
        todo = []
    logger.info("stateboards enrichment: %d names, %d to query (%d cached)", len(names), len(todo), len(name_cache))

    session = requests.Session()
    failures = 0
    for i, name in enumerate(todo, 1):
        try:
            name_cache[name] = resolve_name(session, name, label_cache)
        except RuntimeError as exc:
            # Persistent API failure: keep what we have, finish gold from cache.
            failures += 1
            logger.warning("giving up on %r: %s", name, exc)
            if failures >= 5:
                logger.error("stateboards enrichment: 5 hard API failures — proceeding with cache only")
                break
        if i % 100 == 0:
            logger.info("  %d/%d names queried", i, len(todo))
            _CACHE.parent.mkdir(parents=True, exist_ok=True)
            _CACHE.write_text(json.dumps(cache, indent=1, sort_keys=True), encoding="utf-8")
    _CACHE.parent.mkdir(parents=True, exist_ok=True)
    _CACHE.write_text(json.dumps(cache, indent=1, sort_keys=True), encoding="utf-8")

    rows = []
    for name in names:
        rec = name_cache.get(name) or {"match": "skipped" if not is_person_name(name) else "none"}
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
