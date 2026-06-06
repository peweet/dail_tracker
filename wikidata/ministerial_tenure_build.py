#!/usr/bin/env python3
"""
ministerial_tenure_build.py

Builds the historical ministerial-tenure table that lets Statutory
Instruments resolve to the minister who signed them — back to 2016, not
just the current government.

SOURCE: Wikidata SPARQL. The Oireachtas API only carries the current term's
ministerial offices (verified — historical memberships return empty office
arrays). Wikidata models 'position held' (P39) with start (P580) / end
(P582) qualifiers across every Irish government, so one query returns every
'Minister for …' tenure since 2016.

OUTPUT: data/silver/ministerial_tenure.parquet — one row per
(department, holder, span):
    department_key, department_label, minister_name, member_code,
    start_date, end_date, wikidata_person, wikidata_position
- department_key uses the same canonical keys as si_department_aliases.csv,
  so si_entity_enrichment can join SI department → tenure directly.
- member_code is the unique_member_code where the minister is in
  flattened_members (i.e. still a sitting member); null for ministers who
  have since left the Oireachtas — their SIs resolve to a name, not a
  clickable profile.

The raw SPARQL CSV is cached to data/bronze/wikidata/ for provenance.

NOTE: this query covers 'Minister for …' senior departments only. The
Taoiseach and 'The Government' (collective) are not 'Minister for …'
positions, so SIs signed under those keys keep their department but no
person — a small, documented gap.
"""

from __future__ import annotations

import io
import logging
import re
import time
import unicodedata

import pandas as pd
import requests

from config import BRONZE_DIR, SILVER_DIR
from iris.si_entity_enrichment import canonicalise_department, load_department_aliases

logger = logging.getLogger(__name__)

_MEMBERS_CSV = SILVER_DIR / "flattened_members.csv"
_OUT = SILVER_DIR / "ministerial_tenure.parquet"
_RAW_OUT = BRONZE_DIR / "wikidata" / "ministerial_tenure_raw.csv"

_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
_USER_AGENT = "dail-tracker/1.0 (civic accountability data project)"

# Every 'Minister for …' position whose jurisdiction (P1001) is Ireland
# (Q27), with the start (P580) / end (P582) qualifiers of each holder's
# tenure. Kept to tenures still open or ending 2016 or later.
_QUERY = """
SELECT ?person ?personLabel ?positionLabel ?start ?end WHERE {
  ?person p:P39 ?st .
  ?st ps:P39 ?position .
  ?position wdt:P1001 wd:Q27 .
  ?position rdfs:label ?pl .
  FILTER(LANG(?pl) = "en" && STRSTARTS(?pl, "Minister for")) .
  OPTIONAL { ?st pq:P580 ?start . }
  OPTIONAL { ?st pq:P582 ?end . }
  FILTER(!BOUND(?end) || ?end >= "2016-01-01"^^xsd:dateTime)
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}
ORDER BY ?positionLabel ?start
"""


def _norm_name(name: str) -> str:
    """Accent-stripped, lower-cased, punctuation-free name for matching
    Wikidata person labels against flattened_members.full_name."""
    n = unicodedata.normalize("NFKD", str(name))
    n = "".join(c for c in n if not unicodedata.combining(c))
    n = re.sub(r"[^a-z0-9 ]", " ", n.lower())
    return re.sub(r"\s+", " ", n).strip()


def fetch_wikidata(attempts: int = 4) -> pd.DataFrame:
    """Run the SPARQL query; cache the raw CSV to bronze for provenance.
    The Wikidata SPARQL endpoint drops connections intermittently — retry
    with backoff before giving up."""
    last_err: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            resp = requests.post(
                _SPARQL_ENDPOINT,
                data={"query": _QUERY},
                headers={"User-Agent": _USER_AGENT, "Accept": "text/csv"},
                timeout=90,
            )
            resp.raise_for_status()
            _RAW_OUT.parent.mkdir(parents=True, exist_ok=True)
            _RAW_OUT.write_text(resp.text, encoding="utf-8")
            return pd.read_csv(io.StringIO(resp.text))
        except Exception as exc:  # noqa: BLE001 — network flakiness, retry all
            last_err = exc
            logger.warning("Wikidata fetch attempt %d/%d failed: %s", attempt, attempts, exc)
            if attempt < attempts:
                time.sleep(3 * attempt)
    raise SystemExit(f"Wikidata SPARQL fetch failed after {attempts} attempts: {last_err}")


def member_code_map() -> dict[str, str]:
    """{normalised full name -> unique_member_code} for sitting members."""
    if not _MEMBERS_CSV.exists():
        logger.warning(
            "flattened_members.csv not found — member_code will be null for every minister: %s", _MEMBERS_CSV
        )
        return {}
    df = pd.read_csv(_MEMBERS_CSV, low_memory=False)
    out: dict[str, str] = {}
    for r in df.to_dict("records"):
        nm = _norm_name(r.get("full_name", ""))
        code = r.get("unique_member_code")
        if nm and isinstance(code, str):
            out.setdefault(nm, code)
    return out


def load_current_offices(aliases) -> list[dict]:
    """Current-government senior-minister spans from flattened_members.csv.
    Wikidata can lag the most recent reshuffle (and misses a department or
    two); the live members file backstops the recent end of each tenure
    chain, with member_code already attached."""
    if not _MEMBERS_CSV.exists():
        return []
    df = pd.read_csv(_MEMBERS_CSV, low_memory=False)
    out: list[dict] = []
    for i in range(1, 7):
        ncol, scol, ecol = (f"office_{i}_name", f"office_{i}_start_date", f"office_{i}_end_date")
        if ncol not in df.columns:
            continue
        for r in df[df[ncol].notna()].to_dict("records"):
            name = str(r[ncol])
            if not name.startswith("Minister for "):
                continue
            key, label = canonicalise_department(name, aliases)
            if not key:
                continue
            start = pd.to_datetime(r.get(scol), errors="coerce", utc=True)
            if pd.isna(start):
                continue
            end = pd.to_datetime(r.get(ecol), errors="coerce", utc=True)
            out.append(
                {
                    "department_key": key,
                    "department_label": label,
                    "minister_name": str(r["full_name"]),
                    "member_code": str(r["unique_member_code"]),
                    "start_date": start.tz_localize(None),
                    "end_date": None if pd.isna(end) else end.tz_localize(None),
                    "wikidata_person": "",
                    "wikidata_position": "flattened_members (current government)",
                }
            )
    return out


def run() -> dict:
    aliases = load_department_aliases()
    raw = fetch_wikidata()
    codes = member_code_map()
    logger.info("Wikidata returned %d raw tenure rows", len(raw))

    rows: list[dict] = []
    unmapped: set[str] = set()
    for r in raw.to_dict("records"):
        pos = str(r.get("positionLabel", "") or "")
        key, label = canonicalise_department(pos, aliases)
        if not key:
            unmapped.add(pos)
            continue
        start = pd.to_datetime(r.get("start"), errors="coerce", utc=True)
        if pd.isna(start):
            continue
        end = pd.to_datetime(r.get("end"), errors="coerce", utc=True)
        name = str(r.get("personLabel", "") or "").strip()
        rows.append(
            {
                "department_key": key,
                "department_label": label,
                "minister_name": name,
                "member_code": codes.get(_norm_name(name)),
                "start_date": start.tz_localize(None),
                "end_date": None if pd.isna(end) else end.tz_localize(None),
                "wikidata_person": str(r.get("person", "") or "").rsplit("/", 1)[-1],
                "wikidata_position": pos,
            }
        )

    wikidata_n = len(rows)
    rows.extend(load_current_offices(aliases))
    logger.info("merged %d Wikidata + %d current-government office spans", wikidata_n, len(rows) - wikidata_n)

    out = (
        pd.DataFrame(rows)
        .drop_duplicates(subset=["department_key", "minister_name", "start_date"])
        .sort_values(["department_key", "start_date"])
        .reset_index(drop=True)
    )

    _OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(_OUT, index=False, compression="zstd", compression_level=3)

    n = len(out)
    coded = int(out["member_code"].notna().sum())
    logger.info("ministerial_tenure: wrote %s", _OUT)
    logger.info("  %d tenure spans across %d departments", n, out["department_key"].nunique())
    logger.info("  linked to a sitting member_code: %d (%.0f%%)", coded, 100 * coded / n if n else 0)
    if unmapped:
        logger.info("  positions not mapped to a department_key: %s", "; ".join(sorted(unmapped)))
    return {
        "tenures": n,
        "departments": int(out["department_key"].nunique()),
        "with_member_code": coded,
        "unmapped": sorted(unmapped),
    }


if __name__ == "__main__":
    from services.logging_setup import setup_standalone_logging

    setup_standalone_logging("ministerial_tenure_build")
    run()
