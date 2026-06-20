"""Constituency -> Local-Authority crosswalk, parsed from the EC 2023 report.

The authoritative, data-anchored bridge between the 43 Dáil constituencies (the
unit the app's parliamentary data is keyed on) and the 31 local authorities (the
unit the council-spending facts — AFS revenue/capital, LA purchase-orders — are
keyed on). Geography does NOT nest: a council area can span several
constituencies (Dublin City spans 5+) and a constituency can draw on several
councils (Sligo-Leitrim spans Sligo, Leitrim and a sliver of Donegal). This
crosswalk is therefore many-to-many and is used only to surface "the council(s)
serving this area" as CONTEXT — council euros are never apportioned into a
per-constituency figure.

Source (same PDF as ec_constituency_pop_extract.py — reused from cache):
  Constituency Review Report 2023, **Appendix 1 — "Specification of Recommended
  Dáil Constituencies"** (pages 127-140 of the report). Every constituency's
  definition explicitly names each county/city it draws from, e.g.
    "Cork East — In the county of Cork, the electoral divisions of: ..."
    "Limerick City — In the city and county of Limerick, ..."
    "Dublin Bay North — In the city of Dublin, ... and in the county of Fingal ..."
  We extract those "(city|county) of X" mentions per constituency block and map
  each to its local-authority name **using the exact spelling found in the LA
  spending facts** (so the downstream JOIN is clean — e.g. "Dun Laoghaire-Rathdown"
  with no fada, matching la_afs_divisions.council).

link_type ('primary' | 'partial') is a transparent, name-based qualifier so the
UI can de-emphasise sliver councils (e.g. Donegal under Sligo-Leitrim). A link is
'primary' when the council's county name appears in the constituency name, the
constituency draws on a single council, or it is a Dublin-family constituency
(which are multi-council by design); otherwise 'partial' (a cross-county sliver).
This is NOT an area weight — true area shares come later with the LEA spatial
crosswalk; until then 'partial' just means "covers only part of this area".

Writes : data/_meta/constituency_la_crosswalk.csv  (--write; curated reference,
         git-tracked via .gitignore negation per project_curated_meta_reference_files)

Integrity self-checks (all must pass before --write):
  * exactly 43 constituencies, every one matching a canonical registry name
  * every constituency maps to >= 1 local authority
  * all 31 local authorities appear at least once
  * every local_authority string is one of the 31 canonical LA spellings
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import re
import sys
import unicodedata
from pathlib import Path

import requests

from paths import PROJECT_ROOT as _ROOT

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

_PDF_URL = "https://www.rte.ie/documents/news/2023/08/constituency-review-report-2023.pdf"
_PDF_CACHE = _ROOT / "data" / "_meta" / "ec_review_2023.pdf"
_OUT_CSV = _ROOT / "data" / "_meta" / "constituency_la_crosswalk.csv"
_APPENDIX1_PAGES = range(126, 134)  # 0-based; Appendix 1 spec, up to the App-2 table
_APPENDIX1_END_MARKER = "STATISTICS RELATING"  # start of Appendix 2 — bound the parse here

# Canonical constituency names (registry / v_member_constituency_demographics spelling),
# kept local so this module needs no polars import (avoids the WMI import-hang trap).
_CONSTITUENCIES = [
    "Carlow-Kilkenny",
    "Cavan-Monaghan",
    "Clare",
    "Cork East",
    "Cork North-Central",
    "Cork North-West",
    "Cork South-Central",
    "Cork South-West",
    "Donegal",
    "Dublin Bay North",
    "Dublin Bay South",
    "Dublin Central",
    "Dublin Fingal East",
    "Dublin Fingal West",
    "Dublin Mid-West",
    "Dublin North-West",
    "Dublin Rathdown",
    "Dublin South-Central",
    "Dublin South-West",
    "Dublin West",
    "Dún Laoghaire",
    "Galway East",
    "Galway West",
    "Kerry",
    "Kildare North",
    "Kildare South",
    "Laois",
    "Limerick City",
    "Limerick County",
    "Longford-Westmeath",
    "Louth",
    "Mayo",
    "Meath East",
    "Meath West",
    "Offaly",
    "Roscommon-Galway",
    "Sligo-Leitrim",
    "Tipperary North",
    "Tipperary South",
    "Waterford",
    "Wexford",
    "Wicklow",
    "Wicklow-Wexford",
]

# The 31 local authorities, spelled EXACTLY as in the LA spending facts
# (la_afs_divisions.council / procurement_payments_fact.publisher_name).
_LOCAL_AUTHORITIES = [
    "Carlow",
    "Cavan",
    "Clare",
    "Cork City",
    "Cork County",
    "Donegal",
    "Dublin City",
    "Dun Laoghaire-Rathdown",
    "Fingal",
    "Galway City",
    "Galway County",
    "Kerry",
    "Kildare",
    "Kilkenny",
    "Laois",
    "Leitrim",
    "Limerick",
    "Longford",
    "Louth",
    "Mayo",
    "Meath",
    "Monaghan",
    "Offaly",
    "Roscommon",
    "Sligo",
    "South Dublin",
    "Tipperary",
    "Waterford",
    "Westmeath",
    "Wexford",
    "Wicklow",
]
_DUBLIN_LAS = {"Dublin City", "Fingal", "South Dublin", "Dun Laoghaire-Rathdown"}


def _ascii_key(s: str) -> str:
    """Lower, drop accents/non-letters — for fuzzy name matching across vintages."""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z]", "", s.lower())


_CONSTITUENCY_BY_KEY = {_ascii_key(n): n for n in _CONSTITUENCIES}


def _canon_constituency(heading: str) -> str | None:
    """Map a report heading (e.g. 'Roscommon Galway', mojibake 'D�n Laoghaire')."""
    if "laoghaire" in heading.lower():
        return "Dún Laoghaire"
    return _CONSTITUENCY_BY_KEY.get(_ascii_key(heading))


def _county_to_la(kind: str, name: str) -> str | None:
    """Map a report '(city|county) of <name>' mention to a canonical LA spelling."""
    n = name.strip()
    low = _ascii_key(n)
    if "laoghaire" in n.lower():
        return "Dun Laoghaire-Rathdown"
    if low == "southdublin":
        return "South Dublin"
    if kind.lower() == "city":
        return {
            "cork": "Cork City",
            "dublin": "Dublin City",
            "galway": "Galway City",
            "limerick": "Limerick",
            "waterford": "Waterford",
        }.get(low)
    # county side
    county_map = {
        "cork": "Cork County",
        "galway": "Galway County",
        "dublin": "Dublin City",
        "fingal": "Fingal",
        "limerick": "Limerick",
        "waterford": "Waterford",
    }
    if low in county_map:
        return county_map[low]
    # otherwise the county shares its name with its LA (Clare, Donegal, Kerry, ...)
    cand = n.title().replace("  ", " ").strip()
    return cand if cand in _LOCAL_AUTHORITIES else None


def fetch_pdf(dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    r = requests.get(_PDF_URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=120)
    r.raise_for_status()
    dest.write_bytes(r.content)
    return dest


_HEAD_RE = re.compile(r"\n([A-Z][^\n]{2,44}?)\s*[–—–�-]\s*(\d+)\s*[Mm]embers\b")
# '(in/and) the (city)(and county)? of <Name>' up to the first delimiter
_CTY_RE = re.compile(
    r"the (county|city)(?: and county)? of ([A-Za-z][\w .\-]*?)"
    r"(?=,|;|\.| except| of the| from | the electoral| and the|\n| in the)",
    re.IGNORECASE,
)


def parse_appendix1(pdf_path: Path) -> list[dict]:
    import fitz  # PyMuPDF — lazy import

    doc = fitz.open(pdf_path)
    full = "\n".join(doc[i].get_text() for i in _APPENDIX1_PAGES)
    cut = full.find(_APPENDIX1_END_MARKER)
    if cut > 0:
        full = full[:cut]

    heads = [(m.start(), re.sub(r"\s+", " ", m.group(1)).strip(), int(m.group(2))) for m in _HEAD_RE.finditer(full)]
    if len(heads) != 43:
        raise RuntimeError(f"Appendix-1 layout drift: found {len(heads)} headings, expected 43.")

    rows: list[dict] = []
    for idx, (pos, heading, seats) in enumerate(heads):
        end = heads[idx + 1][0] if idx + 1 < len(heads) else len(full)
        block = full[pos:end]
        constituency = _canon_constituency(heading)
        if constituency is None:
            raise RuntimeError(f"Unrecognised constituency heading: {heading!r}")
        las: list[str] = []
        for kind, cn in _CTY_RE.findall(block):
            cn = re.sub(r"\s+", " ", cn).strip()
            if not cn or cn[0].islower():
                continue
            la = _county_to_la(kind, cn)
            if la and la not in las:
                las.append(la)
        if not las:
            raise RuntimeError(f"No local authority parsed for {constituency!r}")
        rows.append({"constituency_name": constituency, "seats": seats, "las": las, "key": _ascii_key(constituency)})
    return rows


def build_crosswalk(parsed: list[dict]) -> list[dict]:
    # Reverse index: which constituencies each LA serves (for the shared flag).
    serves: dict[str, set[str]] = {}
    for r in parsed:
        for la in r["las"]:
            serves.setdefault(la, set()).add(r["constituency_name"])

    out: list[dict] = []
    for r in parsed:
        name, key, las = r["constituency_name"], r["key"], r["las"]
        dublin_family = name.startswith("Dublin") or name == "Dún Laoghaire"
        for la in las:
            la_county_key = _ascii_key(la.replace(" City", "").replace(" County", ""))
            in_name = la_county_key and la_county_key in key
            primary = bool(in_name) or len(las) == 1 or (dublin_family and la in _DUBLIN_LAS)
            out.append(
                {
                    "constituency_name": name,
                    "local_authority": la,
                    "seats": r["seats"],
                    "link_type": "primary" if primary else "partial",
                    "la_serves_multiple_constituencies": len(serves[la]) > 1,
                    "constituency_multi_la": len(las) > 1,
                    "source_key": "Electoral Commission Constituency Review 2023, App. 1",
                }
            )
    return out


def integrity_check(rows: list[dict]) -> dict:
    constituencies = {r["constituency_name"] for r in rows}
    las = {r["local_authority"] for r in rows}
    checks = {
        "constituencies_43": len(constituencies) == 43,
        "all_constituencies_canonical": constituencies <= set(_CONSTITUENCIES),
        "every_constituency_mapped": constituencies == set(_CONSTITUENCIES),
        "all_31_las_present": las == set(_LOCAL_AUTHORITIES),
        "all_las_canonical": las <= set(_LOCAL_AUTHORITIES),
    }
    return {"checks": checks, "n_rows": len(rows), "n_las": len(las), "green": all(checks.values())}


def write_csv(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = [
        "constituency_name",
        "local_authority",
        "seats",
        "link_type",
        "la_serves_multiple_constituencies",
        "constituency_multi_la",
        "source_key",
    ]
    rows_sorted = sorted(rows, key=lambda r: (r["constituency_name"], r["local_authority"]))
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        w.writerows(rows_sorted)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--pdf", type=Path, default=_PDF_CACHE)
    args = ap.parse_args()

    if not args.pdf.exists():
        print(f"Downloading EC report → {args.pdf} ...")
        fetch_pdf(args.pdf)
    print(f"Parsing Appendix 1 from {args.pdf} ...")

    parsed = parse_appendix1(args.pdf)
    rows = build_crosswalk(parsed)
    rpt = integrity_check(rows)

    print(f"\n=== constituency_la_crosswalk — {rpt['n_rows']} links, {rpt['n_las']} LAs ===")
    for nm, ok in rpt["checks"].items():
        print(f"  [{'GREEN' if ok else 'FAIL'}] {nm}")
    for r in sorted(parsed, key=lambda r: r["constituency_name"]):
        print(f"  {r['constituency_name']:<22} -> {', '.join(r['las'])}")
    print(f"  >>> overall: {'GREEN' if rpt['green'] else 'RED'}")

    if args.write and rpt["green"]:
        write_csv(rows, _OUT_CSV)
        print(f"\n  Wrote {_OUT_CSV.relative_to(_ROOT)}")
    elif args.write:
        print("\n  REFUSING to write — integrity checks failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
