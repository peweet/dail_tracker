"""EXPERIMENTAL (tracked code, gitignored sandbox data) — enrich the EPA licence register with the
true CORPORATE OPERATOR (``organisation_name``) from the EPA LEAP profile list.

The WFS facility layer ([[epa_licence_register_scrape]]) carries the *facility* Name, which for many
waste/UWW sites is a place ("Dunsink Landfill", "Shanagolden") that never matches CRO. The LEAP profile
list carries the operating ORGANISATION ("Fingal County Council", "Enva Ireland Limited") — a far better
CRO-match key. This pull fetches the whole profile list once (paginated, no auth) and maps each licence
to its operator, so [[epa_capability_register]] can re-key the CRO join on the corporate entity.

Source: EPA LEAP API ``/api/v1/LicenceProfile/licenceprofilesearchlist`` (CC-BY, no auth). Join key is
the profile/registration base code (``profile_number``, e.g. "W0184") to the WFS ``reg_code``.

Output (gitignored): data/sandbox/parquet/epa_licence_operators.parquet
Run: ./.venv/Scripts/python.exe pipeline_sandbox/epa_operator_enrich.py
"""

from __future__ import annotations

import contextlib
import logging
import sys
import time
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.logging_setup import setup_standalone_logging  # noqa: E402
from services.parquet_io import save_parquet  # noqa: E402

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

log = logging.getLogger(__name__)

API = "https://data.epa.ie/leap"
ENDPOINT = "/api/v1/LicenceProfile/licenceprofilesearchlist"
UA = "dail-tracker-research/1.0 (civic data project; EPA LEAP open data CC-BY-4.0)"
PER_PAGE = 2000
DELAY_S = 0.3
OUT = ROOT / "data/sandbox/parquet/epa_licence_operators.parquet"

KEEP = {
    "profile_number": "profile_number",
    "active_licence_regno": "active_regno",
    "organisation_name": "operator_name",
    "organisation_id": "operator_id",
    "active_licence_status": "operator_licence_status",
    "active_licence_sector": "operator_sector",
    "county": "operator_county",
    "town": "operator_town",
    "uww_priority_site": "uww_priority_site",
}


def _new_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "application/json"})
    return s


def pull() -> pd.DataFrame:
    session = _new_session()
    rows: list[dict] = []
    page = 1
    total = None
    while True:
        r = session.get(API + ENDPOINT, params={"per_page": PER_PAGE, "page": page}, timeout=120)
        r.raise_for_status()
        j = r.json()
        total = total or j.get("count")
        items = j.get("list", [])
        if not items:
            break
        for it in items:
            rows.append({our: it.get(src) for src, our in KEEP.items()})
        log.info("  page %d: +%d (total %d / %s)", page, len(items), len(rows), total)
        if total and len(rows) >= total:
            break
        page += 1
        time.sleep(DELAY_S)
    df = pd.DataFrame(rows)
    # one row per profile_number (latest licence version); profile_number is the join base to WFS reg_code
    df = df[df["profile_number"].notna()].drop_duplicates("profile_number", keep="first").reset_index(drop=True)
    return df


def main() -> None:
    setup_standalone_logging("epa_operator_enrich")
    df = pull()
    save_parquet(df, OUT)
    log.info("WROTE %s — %d profiles | %d with an operator_name", OUT, len(df), int(df["operator_name"].notna().sum()))


if __name__ == "__main__":
    main()
