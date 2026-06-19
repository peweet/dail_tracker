#!/usr/bin/env python3
"""
member_contact_extract.py — official contact details per Oireachtas member.

SOURCE: each member's public profile page on oireachtas.ie, e.g.
    https://www.oireachtas.ie/en/members/member/<unique_member_code>/

The Oireachtas members API (api.oireachtas.ie/v1/members) exposes NO contact
fields — the only place the Oireachtas publishes a member's office address,
phone number(s) and @oireachtas.ie email is that HTML profile page (the same
page the Member Overview hero already deep-links to as "Official profile").
The slug in the URL is byte-for-byte our `unique_member_code` (memberCode),
so the join is exact — no fuzzy matching.

Parses the first `.c-member-about__contact-details -short` block:
    address      — `.c-member-about__address .c-member-about__item-value`
    phones       — every `.c-member-about__phone a[href^="tel:"]` (display text)
    email        — `.c-member-about__email a[href^="mailto:"]`
    website_url  — the `.c-member-about__web-item` whose img alt is "Website"

Every field is nullable. Newly-elected members commonly show only the Leinster
House address; a handful of stale/former-seat codes have no block at all. That
sparsity is surfaced as honest NULLs, never imputed.

OUTPUT: data/silver/parquet/member_contact_details.parquet — one row per member:
    unique_member_code, address, phone_primary, phone_all, email,
    website_url, profile_url, source_url, scraped_date

Consumed by sql_views/member/member_contact_details.sql → v_member_contact_details
→ the Member Overview "Contact" block.

Usage:
    python -m extractors.member_contact_extract --max 5   # smoke test
    python -m extractors.member_contact_extract           # full run
"""

from __future__ import annotations

import argparse
import datetime
import html as _htmllib
import logging
import re
import time

import polars as pl
import requests

from config import BRONZE_DIR, SILVER_PARQUET_DIR
from services.parquet_io import save_parquet

logger = logging.getLogger(__name__)

_OUT = SILVER_PARQUET_DIR / "member_contact_details.parquet"
_RAW_OUT = BRONZE_DIR / "members" / "member_contact_details_raw.csv"
_MEMBERS_PARQUET = SILVER_PARQUET_DIR / "flattened_members.parquet"
_SEANAD_MEMBERS_PARQUET = SILVER_PARQUET_DIR / "flattened_seanad_members.parquet"

_PROFILE_FMT = "https://www.oireachtas.ie/en/members/member/{code}/"
_USER_AGENT = "dail-tracker/1.0 (civic accountability data project; contact details)"

# The contact block is the FIRST `c-member-about__contact-details -short` div; a
# second `c-member-about__contact-details` (no `-short`) holds committee
# membership. Slice strictly to the first so committee links can't leak into the
# website field.
_CONTACT_OPEN = re.compile(r'<div class="c-member-about__contact-details -short">')
_CONTACT_NEXT = re.compile(r'<div class="c-member-about__contact-details(?:")')

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
    """Parse one member page's contact block into a flat dict (all data keys
    present, values may be None). Pure function — unit-testable, no network."""
    out: dict = {
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
        out["address"] = _clean(re.sub(r"<[^>]+>", " ", addr.group(1))) or None

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

    web = re.search(r'alt="Website"[^>]*>\s*<a[^>]*href="([^"]+)"', block)
    if web:
        out["website_url"] = _clean(web.group(1)) or None

    return out


def fetch_one(code: str, session: requests.Session, attempts: int = 3) -> dict:
    """Fetch + parse one member. Network failures retry with linear backoff; a
    final failure yields an all-null data row (transparent — the run could not
    see contact data for this member)."""
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


def run(max_n: int | None = None, sleep: float = 0.25) -> dict:
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

    _RAW_OUT.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(_RAW_OUT)  # bronze provenance copy
    _OUT.parent.mkdir(parents=True, exist_ok=True)
    save_parquet(df, _OUT)

    def _filled(col: str) -> int:
        return int(df[col].is_not_null().sum()) if df.height else 0

    summary = {
        "rows": df.height,
        "address": _filled("address"),
        "phone": _filled("phone_primary"),
        "email": _filled("email"),
        "website": _filled("website_url"),
    }
    logger.info("member_contact_details: wrote %s (%d rows)", _OUT, df.height)
    logger.info("  with address: %d", summary["address"])
    logger.info("  with phone:   %d", summary["phone"])
    logger.info("  with email:   %d", summary["email"])
    logger.info("  with website: %d", summary["website"])
    return summary


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    p.add_argument("--max", type=int, default=None, help="Smoke-test: only the first N members.")
    p.add_argument("--sleep", type=float, default=0.25, help="Per-request politeness delay (s).")
    args = p.parse_args()

    from services.logging_setup import setup_standalone_logging

    setup_standalone_logging("member_contact_extract")
    run(max_n=args.max, sleep=args.sleep)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
