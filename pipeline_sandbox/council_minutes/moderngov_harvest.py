"""Harvest council meeting minutes from ModernGov portals (Fingal + Dublin City).

Both councils run Civica ModernGov, which has stable, scrape-friendly HTML:
  https://<host>/ieListMeetings.aspx?CId=<committee-id>&Year=<year>   -> meeting list
  https://<host>/ieListDocuments.aspx?CId=<cid>&MId=<mid>             -> one meeting's docs
  https://<host>/documents/g<gid>/<name>.pdf?T=<n>                    -> the PDF (the ?T=
                                                                         suffix is REQUIRED)
mgWebService.asmx is disabled on both — plain HTML parsing only.

  - Fingal  (meetings.fingal.ie, Council CId=129): publishes born-digital "Printed minutes"
    WITH NAMED ROLL-CALL VOTES -> feeds parse_fingal_prose in council_votes_extract.py.
  - Dublin City (councilmeetings.dublincity.ie, Monthly Council CId=142, 2014->): the largest
    born-digital minutes corpus in the country, but NO named votes ("put and carried") — value
    is the agenda/decision text (s.183 disposals, Part 8s, management reports).

Writes PDFs + extracted .txt into corpus/<slug>/ alongside the other councils' minutes, so
the existing extractors sweep them with zero special-casing.

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/council_minutes/moderngov_harvest.py [--years 2024,2025,2026]
"""

from __future__ import annotations

import argparse
import contextlib
import re
import sys
import time
from pathlib import Path
from urllib.parse import quote, urljoin
from urllib.request import Request, urlopen

import fitz  # PyMuPDF

HERE = Path(__file__).resolve().parent
CORPUS = HERE / "corpus"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

PORTALS = [
    {
        "slug": "fingal",
        "host": "https://meetings.fingal.ie",
        "cid": 129,
        "minutes_re": re.compile(r"printed minutes", re.I),
    },
    {
        "slug": "dublin_city",
        "host": "https://councilmeetings.dublincity.ie",
        "cid": 142,
        "minutes_re": re.compile(r"(public )?minutes", re.I),
    },
]


def get(url: str) -> bytes:
    time.sleep(0.5)  # politeness — ModernGov instances rate-limit bursts
    return urlopen(Request(url, headers={"User-Agent": UA}), timeout=90).read()


def meetings_for_year(host: str, cid: int, year: int) -> list[tuple[int, str]]:
    """(MId, date-text) for each meeting listed for a committee-year."""
    html = get(f"{host}/ieListMeetings.aspx?CId={cid}&Year={year}").decode("utf-8", errors="replace")
    out, seen = [], set()
    # anchors like ieListDocuments.aspx?CId=129&MId=6247&Ver=4 with link text carrying the date
    for m in re.finditer(r'href="(ieListDocuments\.aspx\?CId=\d+&(?:amp;)?MId=(\d+)[^"]*)"[^>]*>([^<]*)', html):
        mid = int(m.group(2))
        if mid not in seen:
            seen.add(mid)
            out.append((mid, re.sub(r"\s+", " ", m.group(3)).strip()))
    return out


def minutes_pdf_url(host: str, cid: int, mid: int, minutes_re: re.Pattern) -> str | None:
    html = get(f"{host}/ieListDocuments.aspx?CId={cid}&MId={mid}").decode("utf-8", errors="replace")
    for m in re.finditer(r'href="([^"]*documents/g\d+/[^"]+?\.pdf\?T=\d+)"[^>]*>([^<]*)', html, re.I):
        href, label = m.group(1), m.group(2)
        if minutes_re.search(label) or minutes_re.search(href):
            # ModernGov hrefs carry literal spaces AND embedded CR/LF ("Printed minutes\r\n08th-…")
            # — urlopen rejects control chars and won't auto-encode spaces (the Sligo lesson):
            # strip the control chars, then percent-encode keeping '%' safe.
            href = re.sub(r"[\r\n\t]+", " ", href.replace("&amp;", "&")).strip()
            return quote(urljoin(host + "/", href), safe="!#$%&'()*+,/:;=?@[]~")
    return None


def mdate_slug(date_text: str, mid: int) -> str:
    """'Monday, 9th February, 2026 ...' -> minutes_council_meeting_09_02_2026; falls back to MId."""
    m = re.search(
        r"(\d{1,2})(?:st|nd|rd|th)?\s+(January|February|March|April|May|June|July|August|"
        r"September|October|November|December)[ ,]+(20\d{2})",
        date_text,
        re.I,
    )
    if not m:
        return f"minutes_mid_{mid}"
    months = "january february march april may june july august september october november december".split()
    mo = months.index(m.group(2).lower()) + 1
    return f"minutes_council_meeting_{int(m.group(1)):02d}_{mo:02d}_{m.group(3)}"


def harvest(portal: dict, years: list[int]) -> None:
    slug, host, cid = portal["slug"], portal["host"], portal["cid"]
    dest = CORPUS / slug
    dest.mkdir(parents=True, exist_ok=True)
    got = skipped = failed = 0
    for year in years:
        try:
            meetings = meetings_for_year(host, cid, year)
        except Exception as exc:  # noqa: BLE001
            print(f"  {slug} {year}: list failed ({exc})")
            continue
        for mid, date_text in meetings:
            base = mdate_slug(date_text, mid)
            pdf_path = dest / f"{base}.pdf"
            txt_path = dest / f"{base}_pdf.txt"
            if txt_path.exists():
                skipped += 1
                continue
            try:
                url = minutes_pdf_url(host, cid, mid, portal["minutes_re"])
                if not url:
                    failed += 1
                    continue
                body = get(url)
                if body[:4] != b"%PDF":
                    failed += 1
                    continue
                pdf_path.write_bytes(body)
                doc = fitz.open(pdf_path)
                text = "\n".join(doc[i].get_text("text") for i in range(doc.page_count))
                doc.close()
                txt_path.write_text(text, encoding="utf-8")
                got += 1
            except Exception as exc:  # noqa: BLE001 — one meeting must not kill the run
                failed += 1
                print(f"  {slug} MId={mid}: {str(exc)[:90]}")
    print(f"  {slug}: downloaded {got}, cached {skipped}, failed/no-minutes {failed}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", default="2024,2025,2026")
    args = ap.parse_args()
    years = [int(y) for y in args.years.split(",") if y.strip()]
    for portal in PORTALS:
        harvest(portal, years)


if __name__ == "__main__":
    main()
