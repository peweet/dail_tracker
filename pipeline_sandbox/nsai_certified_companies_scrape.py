"""EXPERIMENTAL (tracked code, gitignored sandbox data) — scrape the NSAI certified-company
register: every firm holding an NSAI-issued certification (ISO 9001/14001/45001/50001/27001/13485,
food-safety, and product/CE schemes), with the standard held and the scope of registration.

This is the SUPPLY side of the procurement story — who is provably qualified — and the firm-linkage
half of the supplier-capability register built in [[nsai_capability_register]]. NSAI is the one Irish
source where ISO management-system certs are publicly enumerable (elsewhere they are self-declared).

Mechanism (reverse-engineered): the search is an ExpressionEngine form POSTing to
``/certification/results/`` with a per-form ``XID`` CSRF token plus ``standard_number`` /
``standard_title`` / ``company_name`` fields. No pagination — every match returns in one response.
We enumerate by each management-system standard number AND every ``standard_title`` scheme, then
de-duplicate on (project_file_number, standard_number, company).

UTF-8: the site serves utf-8 without a charset header, so requests mis-guesses latin-1 and mojibakes
the fadas ("Iarnród"→"Iarnr�d"). We pin ``resp.encoding = 'utf-8'`` before reading text.

Output (gitignored): data/sandbox/parquet/nsai_certified_companies.parquet
Run: ./.venv/Scripts/python.exe pipeline_sandbox/nsai_certified_companies_scrape.py
"""

from __future__ import annotations

import contextlib
import html as ihtml
import re
import sys
import time
import urllib.parse
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.logging_setup import setup_standalone_logging  # noqa: E402
from services.parquet_io import save_parquet  # noqa: E402

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import logging  # noqa: E402

log = logging.getLogger(__name__)

FORM_URL = "https://www.nsai.ie/certification/search-for-a-certified-company/"
RESULTS_URL = "https://www.nsai.ie/certification/results/"
OUT = ROOT / "data/sandbox/parquet/nsai_certified_companies.parquet"
UA = "dail-tracker-research/1.0 (civic data project; +https://www.nsai.ie certified-company register)"
STORE_LINK = "https://shop.standards.ie/en-ie/search/standard/?searchTerm={}&publisher=NSAI"
REQUEST_DELAY_S = 0.5

# management-system standards (the firm-linkable Type-B credentials); the standard_number field is a
# substring match, so "9001" catches "I.S. EN ISO 9001:2015".
MGMT_NUMBERS = [
    "9001",
    "14001",
    "45001",
    "50001",
    "27001",
    "13485",
    "22000",
    "20000",
    "27701",
    "37001",
    "44001",
    "22301",
    "55001",
    "41001",
    "27017",
    "27018",
    "17025",
    "15189",
    "3834",
    "1090",
    "17100",
    "13606",
    "39001",
    "21001",
    "22716",
    "28000",
    "29993",
    "54001",
    "42001",
    "56002",
    "30301",
    "22483",
]


def _new_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    return s


def _get_text(resp: requests.Response) -> str:
    """Force utf-8 decoding (NSAI serves utf-8 with no charset header)."""
    resp.encoding = "utf-8"
    return resp.text


def _form_state(session: requests.Session) -> tuple[str, list[str]]:
    """Return (cert-form XID token, standard_title dropdown options) from a fresh form fetch.

    The page carries two forms; only the second (action=/certification/results/) is the
    certified-company search and has its own single-use XID. We isolate it by splitting on <form>.
    """
    html = _get_text(session.get(FORM_URL, timeout=30))
    xid = None
    for frag in re.split(r"(?=<form\b)", html):
        if "certification/results/" in frag and "XID" in frag:
            m = re.search(r'name=["\']XID["\'][^>]*value=["\']([^"\']*)["\']', frag)
            xid = m.group(1) if m else None
    titles: list[str] = []
    sel = re.search(r'<select[^>]*name=["\']standard_title["\'][^>]*>(.*?)</select>', html, re.S)
    if sel:
        for val, _ in re.findall(r'<option[^>]*value=["\']([^"\']*)["\'][^>]*>(.*?)</option>', sel.group(1), re.S):
            if val.strip():
                titles.append(ihtml.unescape(val).replace("\xa0", " ").strip())
    if not xid:
        raise RuntimeError("could not locate certified-company form XID — site markup changed")
    return xid, titles


def _clean(x: str) -> str:
    return re.sub(r"\s+", " ", ihtml.unescape(re.sub(r"<[^>]+>", "", x))).strip()


def _parse_cards(html: str) -> list[dict]:
    out = []
    for art in re.findall(r'<article class="[^"]*result[^"]*">(.*?)</article>', html, re.S):

        def g(label: str, art: str = art) -> str:
            m = re.search(label + r":\s*([^<]+)", art)
            return _clean(m.group(1)) if m else ""

        comp = re.search(r'article__title">([^<]+)<', art)
        out.append(
            {
                "project_file_number": g("Project File Number"),
                "company": _clean(comp.group(1)) if comp else "",
                "location": g("Location"),
                "standard_number": g("Standard Number"),
                "standard_title": g("Standard Title"),
                "scope": g("Scope Of Registration"),
            }
        )
    return out


def _query(session: requests.Session, field: str, value: str) -> list[dict]:
    """One search. Fetches a fresh XID (single-use CSRF) then POSTs the chosen field."""
    xid, _ = _form_state(session)
    data = {"XID": xid, "standard_number": "", "standard_title": "", "company_name": "", "keywords": ""}
    data[field] = value
    resp = session.post(RESULTS_URL, data=data, headers={"Referer": FORM_URL}, timeout=90)
    return _parse_cards(_get_text(resp))


def _standard_code(standard_number: str) -> str:
    """The I.S. code portion (before the dash description), for the catalogue store link."""
    return standard_number.split("-")[0].strip() or standard_number


def scrape() -> pd.DataFrame:
    session = _new_session()
    _, titles = _form_state(session)
    queries = [("standard_number", n) for n in MGMT_NUMBERS] + [("standard_title", t) for t in titles]
    log.info(
        "NSAI scrape: %d queries (%d mgmt numbers + %d title schemes)", len(queries), len(MGMT_NUMBERS), len(titles)
    )

    rows, seen = [], set()
    for i, (field, value) in enumerate(queries, 1):
        try:
            recs = _query(session, field, value)
        except Exception as exc:  # noqa: BLE001
            log.warning("query %d (%s=%s) failed: %s", i, field, value[:40], exc)
            time.sleep(1.0)
            continue
        new = 0
        for rec in recs:
            key = (rec["project_file_number"], rec["standard_number"], rec["company"])
            if key not in seen and rec["company"]:
                seen.add(key)
                rows.append(rec)
                new += 1
        if i % 20 == 0 or field == "standard_number":
            log.info(
                "  [%d/%d] %s=%s hits=%d new=%d total=%d", i, len(queries), field, value[:34], len(recs), new, len(rows)
            )
        time.sleep(REQUEST_DELAY_S)

    df = pd.DataFrame(rows)
    df["standard_code"] = df["standard_number"].map(_standard_code)
    df["store_link"] = df["standard_code"].map(lambda c: STORE_LINK.format(urllib.parse.quote(c)))
    return df


def main() -> None:
    setup_standalone_logging("nsai_certified_companies_scrape")
    df = scrape()
    save_parquet(df, OUT)
    log.info(
        "WROTE %s — %d certs / %d companies / %d standards",
        OUT,
        len(df),
        df["company"].nunique(),
        df["standard_code"].nunique(),
    )


if __name__ == "__main__":
    main()
