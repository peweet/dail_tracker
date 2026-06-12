"""ISIF (Ireland Strategic Investment Fund) Irish portfolio — sandbox ingestion.

Scoped 2026-06-12 (new-enrichment-sources round 2). The full portfolio is
server-rendered as ~213 cards on a single page at https://isif.ie/portfolio
(no API, no pagination — the "load more" button is client-side reveal; the
sector <select> is client-side too and carries no per-card markup, so cards
are NOT sector-tagged here).

Each card: investee name, commitment date (<time datetime>), free-text
description that usually leads with the commitment amount ("€140m commitment
to ..." / "$20m commitment ...").

VALUE SEMANTICS (project_procurement_phase_taxonomy): these are investment
COMMITMENTS by a sovereign fund — value_kind=investment_commitment,
realisation_tier=COMMITTED. NEVER union with procurement awards or payment
facts. amount parsing is best-effort from prose: mixed currencies (EUR/USD/GBP)
and "up to" phrasing → value_safe_to_sum=False on every row.

Outputs -> data/sandbox/enrichment/
  isif_portfolio.parquet / .csv          one row per portfolio card
Raw HTML snapshot -> data/sandbox/enrichment/raw/isif_portfolio.html

Run: .venv/Scripts/python.exe pipeline_sandbox/isif_portfolio_extract.py
"""

from __future__ import annotations

import logging
import re
from datetime import date
from pathlib import Path

import polars as pl
import requests
from bs4 import BeautifulSoup

from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

log = logging.getLogger(__name__)

PORTFOLIO_URL = "https://isif.ie/portfolio"
HEADERS = {"User-Agent": "Mozilla/5.0 (dail-tracker civic-data ingestion; contact p.glynn18@gmail.com)"}
OUT_DIR = Path("data/sandbox/enrichment")
RAW_DIR = OUT_DIR / "raw"

# "€140m commitment", "$20m commitment", "£5.5m", "€1.2bn", "€500,000"
_AMOUNT_RE = re.compile(
    r"(?P<cur>[€$£])\s?(?P<num>\d+(?:[.,]\d+)?)\s?(?P<scale>bn|billion|m|million|k)?",
    re.IGNORECASE,
)
_CUR_NAME = {"€": "EUR", "$": "USD", "£": "GBP"}
_SCALE = {"bn": 1e9, "billion": 1e9, "m": 1e6, "million": 1e6, "k": 1e3, None: 1.0}


def parse_lead_amount(description: str) -> tuple[float | None, str | None, bool]:
    """Best-effort (amount, currency, is_up_to) from the card's lead sentence.

    Only the FIRST money mention is taken — descriptions sometimes cite fund
    target sizes later in the sentence which are not ISIF's own commitment.
    """
    m = _AMOUNT_RE.search(description)
    if not m:
        return None, None, False
    num = float(m.group("num").replace(",", "."))
    scale = _SCALE[(m.group("scale") or "").lower() or None]
    is_up_to = bool(re.search(r"\bup to\b", description[: m.start()], re.IGNORECASE))
    return num * scale, _CUR_NAME[m.group("cur")], is_up_to


def extract_cards(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict] = []
    for card in soup.select("article.card"):
        heading = card.select_one(".card__heading")
        timetag = card.select_one("time.card__subheading")
        desc = card.select_one(".card__description")
        img = card.select_one("img.card__image-source")
        name = heading.get_text(strip=True) if heading else None
        if not name:
            continue
        description = desc.get_text(" ", strip=True) if desc else ""
        amount, currency, is_up_to = parse_lead_amount(description)
        rows.append(
            {
                "investee_name": name,
                "commitment_date": (timetag.get("datetime") if timetag else None),
                "commitment_year_label": (timetag.get_text(strip=True) if timetag else None),
                "description": description,
                "amount_stated": amount,
                "amount_currency": currency,
                "amount_is_up_to": is_up_to,
                "image_url": (img.get("src") if img else None),
                "value_kind": "investment_commitment",
                "realisation_tier": "COMMITTED",
                "value_safe_to_sum": False,
                "source_url": PORTFOLIO_URL,
            }
        )
    return rows


def main() -> int:
    setup_standalone_logging("isif_portfolio_extract")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    log.info("Fetching %s", PORTFOLIO_URL)
    r = requests.get(PORTFOLIO_URL, headers=HEADERS, timeout=60)
    r.raise_for_status()
    (RAW_DIR / "isif_portfolio.html").write_text(r.text, encoding="utf-8")

    rows = extract_cards(r.text)
    if len(rows) < 150:
        # The page held 213 cards at build time; a collapse below 150 means the
        # markup changed and the parser is silently dropping cards.
        log.error("Only %d cards parsed (expected ~213) — markup drift?", len(rows))
        return 1

    df = pl.DataFrame(rows).with_columns(
        pl.col("commitment_date").str.to_date("%Y-%m-%d", strict=False),
        pl.lit(date.today()).alias("ingested_date"),
    )
    n_amounts = df["amount_stated"].is_not_null().sum()
    log.info("Parsed %d cards, %d with a lead amount (%.0f%%)", len(df), n_amounts, 100 * n_amounts / len(df))
    log.info(
        "Currency split: %s",
        df.group_by("amount_currency").len().sort("len", descending=True).to_dicts(),
    )

    save_parquet(df, OUT_DIR / "isif_portfolio.parquet")
    df.write_csv(OUT_DIR / "isif_portfolio.csv")
    log.info("Wrote %s (%d rows)", OUT_DIR / "isif_portfolio.parquet", len(df))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
