"""Central Bank of Ireland enforcement actions — sandbox ingestion.

Scoped 2026-06-12 (new-enrichment-sources round 2), building on the read-only
findings in probe_review_cbi_legal_notices.py / doc/CBI_SECOND_PASS_REVIEW.md.

SOURCE SHAPE: the enforcement-actions hub page embeds its full document list
inline as a ``var appData = [...]`` JS array (140 entries, 2007-2025): date,
document title, PDF url. The Vue table on the page just renders that array —
no AJAX endpoint, no pagination. We parse the array, then download each
public-statement PDF and regex the fine amount from the text layer.

SCOPE / PRIVACY: this ingests the enforcement-actions list ONLY (settlement
public statements). Prohibition notices and adverse assessments live on
sibling pages and are EXCLUDED — they name natural persons in
fitness-and-probity matters and collide with the personal-insolvency privacy
precedent. Enforcement actions are mostly against firms, but a minority name
individuals sanctioned in a professional capacity (ex-officers); rows carry a
``party_is_individual_suspected`` heuristic flag so any future view can decide
explicitly. (feedback_personal_insolvency_privacy)

VALUE SEMANTICS: value_kind=sanction_fine. Fines are real monetary penalties
but parse confidence varies (older PDFs are scans, some actions carry
disqualifications instead of fines) → value_safe_to_sum=False; never union
with payment/award facts.

Outputs -> data/sandbox/enrichment/
  cbi_enforcement_actions.parquet / .csv     one row per enforcement action
PDF cache -> C:/tmp/cbi_enforcement_pdfs/ (transient)

Run: .venv/Scripts/python.exe pipeline_sandbox/cbi_enforcement_extract.py
"""

from __future__ import annotations

import html as htmllib
import logging
import re
import time
from datetime import date
from pathlib import Path
from urllib.parse import urljoin

import fitz  # PyMuPDF
import polars as pl
import requests

from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

log = logging.getLogger(__name__)

HUB_URL = "https://www.centralbank.ie/news-media/legal-notices/enforcement-actions"
HEADERS = {"User-Agent": "Mozilla/5.0 (dail-tracker civic-data ingestion; contact p.glynn18@gmail.com)"}
OUT_DIR = Path("data/sandbox/enrichment")
PDF_CACHE = Path("C:/tmp/cbi_enforcement_pdfs")

_ENTRY_RE = re.compile(
    r'\{\s*"type":\s*"(?P<type>[^"]*)",\s*"date":\s*"(?P<date>[^"]*)",\s*'
    r'"documentName":\s*decodeTitle\("(?P<name>.*?)"\),\s*'
    r'"url":\s*decodeTitle\("(?P<url>.*?)"\)',
    re.DOTALL,
)
# Title phrasings drifted over the years; try each in order:
#   "... between the Central Bank of Ireland and Ulster Bank Ireland DAC"
#   "... Enforcement Action against AXA Life Europe DAC"
#   "Settlement Notice - Coinbase Europe Limited (Sanctions confirmed ...)"
_PARTY_RES = [
    re.compile(
        r"between (?:the )?(?:Central Bank(?: of Ireland)?|Financial Regulator) and (.+)$",
        re.IGNORECASE,
    ),
    re.compile(r"enforcement action against (.+)$", re.IGNORECASE),
    re.compile(r"^Settlement Notice\s*-\s*(?:Enforcement Action against\s*)?(.+)$", re.IGNORECASE),
]
_TITLE_TAIL_RE = re.compile(r"\s*\((?:Coinbase|Sanctions)[^)]*\)|\s+\d{1,2} [A-Z][a-z]+ \d{4}$")
_CORP_TOKENS = re.compile(
    r"\b(limited|ltd|plc|dac|icav|uc|ucits|bank|insurance|assurance|company|"
    r"holdings|group|society|union|fund|management|investments?|services|"
    r"stockbrokers|securities|capital|finance|financial|life|re|asset)\b\.?",
    re.IGNORECASE,
)
# €83,300,000 / €1,400,000 / €83.3 million / €192,500
_EURO_RE = re.compile(r"€\s?(\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)?)\s*(million|m\b|billion)?", re.IGNORECASE)
_FINE_CTX_RE = re.compile(
    r"(?:fined|fine of|monetary penalty of|fine imposed[^€]{0,40}|imposed a fine of)\s*"
    r"€\s?(\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)?)\s*(million|billion)?",
    re.IGNORECASE,
)


def _euro_to_float(num: str, scale: str | None) -> float:
    v = float(num.replace(",", ""))
    if scale:
        v *= 1e9 if scale.lower().startswith("b") else 1e6
    return v


def parse_app_data(page_html: str) -> list[dict]:
    i = page_html.find("var appData")
    j = page_html.find("];", i)
    if i < 0 or j < 0:
        raise RuntimeError("appData array not found on hub page — markup drift?")
    rows = []
    for m in _ENTRY_RE.finditer(page_html[i:j]):
        title = htmllib.unescape(m.group("name")).strip()
        url = urljoin(HUB_URL, htmllib.unescape(m.group("url")).strip())
        party = None
        for party_re in _PARTY_RES:
            if pm := party_re.search(title):
                party = pm.group(1).strip().rstrip(".").strip()
                # strip "(Sanctions confirmed by the High Court)" tails and
                # trailing date stamps some titles carry
                party = _TITLE_TAIL_RE.sub("", party).strip()
                break
        rows.append(
            {
                "notice_date": m.group("date").strip(),
                "title": title,
                "party_name": party,
                "pdf_url": url,
                "doc_type": m.group("type").strip(),
            }
        )
    return rows


def extract_fine(pdf_path: Path) -> tuple[float | None, bool, int]:
    """(fine_amount_eur, has_text_layer, n_euro_mentions) from a statement PDF."""
    try:
        doc = fitz.open(pdf_path)
        text = "".join(page.get_text() for page in doc)
    except Exception as e:  # corrupt download etc.
        log.warning("PDF unreadable %s: %s", pdf_path.name, e)
        return None, False, 0
    if len(text.strip()) < 100:
        return None, False, 0  # scanned, no text layer
    mentions = _EURO_RE.findall(text)
    fine = None
    ctx = _FINE_CTX_RE.findall(text)
    if ctx:
        # The headline fine is reliably the LARGEST fine-context amount (texts
        # also mention the pre-discount figure; we want the imposed fine, but
        # taking max keeps gross-vs-net ambiguity on the conservative side and
        # is flagged via n_euro_mentions for manual QC).
        fine = max(_euro_to_float(n, s) for n, s in ctx)
    return fine, True, len(mentions)


def main() -> int:
    setup_standalone_logging("cbi_enforcement_extract")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    PDF_CACHE.mkdir(parents=True, exist_ok=True)

    log.info("Fetching %s", HUB_URL)
    r = requests.get(HUB_URL, headers=HEADERS, timeout=60)
    r.raise_for_status()
    rows = parse_app_data(r.text)
    if len(rows) < 100:
        log.error("Only %d entries parsed (expected ~140) — markup drift?", len(rows))
        return 1
    log.info("Parsed %d enforcement-action entries from appData", len(rows))

    sess = requests.Session()
    sess.headers.update(HEADERS)
    for i, row in enumerate(rows):
        cache = PDF_CACHE / f"{i:03d}_{Path(row['pdf_url'].split('?')[0]).name[-80:]}"
        if not cache.exists():
            try:
                pr = sess.get(row["pdf_url"], timeout=60)
                pr.raise_for_status()
                cache.write_bytes(pr.content)
                time.sleep(0.4)  # polite
            except Exception as e:
                log.warning("Download failed %s: %s", row["pdf_url"], e)
                row.update({"fine_amount_eur": None, "has_text_layer": None, "n_euro_mentions": 0})
                continue
        fine, has_text, n_euro = extract_fine(cache)
        row.update({"fine_amount_eur": fine, "has_text_layer": has_text, "n_euro_mentions": n_euro})
        if (i + 1) % 25 == 0:
            log.info("  %d/%d PDFs processed", i + 1, len(rows))

    df = pl.DataFrame(rows).with_columns(
        pl.col("notice_date").str.to_date("%d/%m/%Y", strict=False),
        pl.col("party_name")
        .map_elements(
            lambda p: bool(p) and not bool(_CORP_TOKENS.search(p)) and len(p.split()) <= 4,
            return_dtype=pl.Boolean,
        )
        .alias("party_is_individual_suspected"),
        pl.lit("sanction_fine").alias("value_kind"),
        pl.lit(False).alias("value_safe_to_sum"),
        pl.lit(HUB_URL).alias("source_url"),
        pl.lit(date.today()).alias("ingested_date"),
    )

    n_fine = df["fine_amount_eur"].is_not_null().sum()
    n_scan = (df["has_text_layer"] == False).sum()  # noqa: E712
    n_party = df["party_name"].is_not_null().sum()
    log.info(
        "rows=%d | party extracted=%d | fine parsed=%d | scanned/no-text=%d | individuals suspected=%d",
        len(df),
        n_party,
        n_fine,
        n_scan,
        df["party_is_individual_suspected"].sum(),
    )

    save_parquet(df, OUT_DIR / "cbi_enforcement_actions.parquet")
    df.write_csv(OUT_DIR / "cbi_enforcement_actions.csv")
    log.info("Wrote %s (%d rows)", OUT_DIR / "cbi_enforcement_actions.parquet", len(df))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
