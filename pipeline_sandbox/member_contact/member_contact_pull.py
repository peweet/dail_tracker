"""
member_contact_pull.py  (SANDBOX — pipeline_sandbox/)
-----------------------------------------------------
Scrape the official contact-details block from each sitting member's
oireachtas.ie profile page.

Why this exists
    The Oireachtas members API (api.oireachtas.ie/v1/members) exposes NO contact
    fields — confirmed by walking the member representation (only showAs / image /
    memberships / memberCode / names). The only place the Oireachtas publishes a
    member's office address, phone number(s) and @oireachtas.ie email is the HTML
    profile page we already deep-link to as "Official profile":

        https://www.oireachtas.ie/en/members/member/<unique_member_code>/

    The slug in that URL is byte-for-byte our `unique_member_code` (memberCode),
    so no fuzzy matching is needed.

What it parses (from the `.c-member-about__contact-details -short` block)
    address      — `.c-member-about__address .c-member-about__item-value`
    phones       — every `.c-member-about__phone a[href^="tel:"]` (display text)
    email        — `.c-member-about__email a[href^="mailto:"]`
    website_url  — the `.c-member-about__web-item` whose img alt is "Website"

    Every field is nullable: newly-elected members often show only the Leinster
    House address, no phone/email yet. That is real-world sparsity, surfaced
    transparently, not a parse failure.

Output (SANDBOX — writes NOTHING to data/silver):
    ./_out/member_contact_details.parquet — one row per member, columns:
        unique_member_code, address, phone_primary, phone_all, email,
        website_url, profile_url, source_url, scraped_date

Run:
    python -m pipeline_sandbox.member_contact.member_contact_pull --max 5   # smoke
    python -m pipeline_sandbox.member_contact.member_contact_pull           # full
"""

from __future__ import annotations

import argparse
import datetime
import html as _htmllib
import logging
import re
import time
from pathlib import Path

import polars as pl
import requests

from config import SILVER_PARQUET_DIR
from services.parquet_io import save_parquet

logger = logging.getLogger(__name__)

OUT_DIR = Path(__file__).parent / "_out"
_OUT = OUT_DIR / "member_contact_details.parquet"

_MEMBERS_PARQUET = SILVER_PARQUET_DIR / "flattened_members.parquet"
_SEANAD_MEMBERS_PARQUET = SILVER_PARQUET_DIR / "flattened_seanad_members.parquet"

_PROFILE_FMT = "https://www.oireachtas.ie/en/members/member/{code}/"
_USER_AGENT = "dail-tracker/1.0 (civic accountability data project; contact details)"

# The contact block is the FIRST `c-member-about__contact-details -short` div.
# A second `c-member-about__contact-details` (no `-short`) holds committee
# membership — we slice strictly to the first to avoid bleeding committee links
# into the website field.
_CONTACT_OPEN = re.compile(r'<div class="c-member-about__contact-details -short">')
_CONTACT_NEXT = re.compile(r'<div class="c-member-about__contact-details(?:")')


def _clean(text: str) -> str:
    """Unescape HTML entities and collapse whitespace on an inner-text fragment."""
    return re.sub(r"\s+", " ", _htmllib.unescape(text)).strip()


def member_codes() -> list[str]:
    """Distinct `unique_member_code`s across both flattened-members parquets."""
    codes: set[str] = set()
    for pq in (_MEMBERS_PARQUET, _SEANAD_MEMBERS_PARQUET):
        if not pq.exists():
            logger.warning("%s not found — skipping its members: %s", pq.name, pq)
            continue
        df = pl.read_parquet(pq, columns=["unique_member_code"])
        codes |= set(df["unique_member_code"].drop_nulls().unique().to_list())
    return sorted(codes)


def _extract_block(html: str) -> str | None:
    """Return the inner HTML of the first `-short` contact-details div, or None."""
    m = _CONTACT_OPEN.search(html)
    if not m:
        return None
    start = m.end()
    nxt = _CONTACT_NEXT.search(html, start)
    end = nxt.start() if nxt else start + 4000
    return html[start:end]


def parse_contact(html: str) -> dict:
    """Parse one member page's contact block into a flat dict (all keys present,
    values may be None / empty). Pure function — unit-testable, no network."""
    out = {
        "address": None,
        "phone_primary": None,
        "phone_all": None,
        "email": None,
        "website_url": None,
    }
    block = _extract_block(html)
    if block is None:
        return out

    addr = re.search(
        r'c-member-about__address.*?c-member-about__item-value">(.*?)</p>',
        block,
        re.DOTALL,
    )
    if addr:
        val = _clean(re.sub(r"<[^>]+>", " ", addr.group(1)))
        out["address"] = val or None

    phones = [
        _clean(re.sub(r"<[^>]+>", "", txt))
        for txt in re.findall(r'<a href="tel:[^"]*">(.*?)</a>', block, re.DOTALL)
    ]
    phones = [p for p in phones if p]
    if phones:
        out["phone_primary"] = phones[0]
        out["phone_all"] = " / ".join(phones)

    email = re.search(r'<a href="mailto:([^"]+)"', block)
    if email:
        out["email"] = _clean(email.group(1)) or None

    # Website = the web-item whose preceding img alt is exactly "Website".
    web = re.search(
        r'alt="Website"[^>]*>\s*<a[^>]*href="([^"]+)"',
        block,
    )
    if web:
        out["website_url"] = _clean(web.group(1)) or None

    return out


def fetch_one(code: str, session: requests.Session, attempts: int = 3) -> dict:
    """Fetch + parse one member. Network failures retry with linear backoff; a
    final failure yields an all-null row (the member simply has no contact data
    on file as far as this run could see)."""
    url = _PROFILE_FMT.format(code=code)
    last_err: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            resp = session.get(url, headers={"User-Agent": _USER_AGENT}, timeout=45)
            resp.raise_for_status()
            row = parse_contact(resp.text)
            row["unique_member_code"] = code
            row["profile_url"] = url
            row["source_url"] = url
            return row
        except Exception as exc:  # noqa: BLE001 — retry every transient cause
            last_err = exc
            if attempt < attempts:
                time.sleep(2 * attempt)
    logger.warning("contact fetch failed for %s: %s", code, last_err)
    return {
        "unique_member_code": code,
        "address": None,
        "phone_primary": None,
        "phone_all": None,
        "email": None,
        "website_url": None,
        "profile_url": url,
        "source_url": url,
    }


_COLUMNS = [
    "unique_member_code",
    "address",
    "phone_primary",
    "phone_all",
    "email",
    "website_url",
    "profile_url",
    "source_url",
    "scraped_date",
]


def run(max_n: int | None = None, sleep: float = 0.3) -> pl.DataFrame:
    codes = member_codes()
    if max_n:
        codes = codes[:max_n]
    logger.info("Scraping contact details for %d members", len(codes))

    today = datetime.date.today().isoformat()
    session = requests.Session()
    rows: list[dict] = []
    for i, code in enumerate(codes, 1):
        row = fetch_one(code, session)
        row["scraped_date"] = today
        rows.append(row)
        if i % 25 == 0:
            logger.info("  %d/%d done", i, len(codes))
        time.sleep(sleep)

    df = pl.DataFrame(rows).select(_COLUMNS)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    save_parquet(df, _OUT)

    def _filled(col: str) -> int:
        return int(df[col].is_not_null().sum()) if df.height else 0

    n = df.height
    logger.info("member_contact_details: wrote %s (%d rows)", _OUT, n)
    logger.info("  with address: %d", _filled("address"))
    logger.info("  with phone:   %d", _filled("phone_primary"))
    logger.info("  with email:   %d", _filled("email"))
    logger.info("  with website: %d", _filled("website_url"))
    return df


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    p.add_argument("--max", type=int, default=None, help="Smoke-test: only the first N members.")
    p.add_argument("--sleep", type=float, default=0.3, help="Per-request politeness delay (s).")
    args = p.parse_args()

    from services.logging_setup import setup_standalone_logging

    setup_standalone_logging("member_contact_pull")
    run(max_n=args.max, sleep=args.sleep)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
