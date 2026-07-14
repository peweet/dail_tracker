"""EU State Aid Transparency Award Module (TAM) — all Ireland awards, sandbox ingestion.

Scoped 2026-06-12 (new-enrichment-sources round 2). Every individual state-aid
award >€100k (lower thresholds for agri/fisheries) granted by Irish authorities
must be published here — this is the structured source behind the
"department grant registers" idea in doc/archive/IDEAS.md §3 (IDA/EI/DAFM-style grant
aid with named beneficiaries). ~15,600 IE awards at build time.

SOURCE SHAPE (all discovered by probe, no documented API):
  https://webgate.ec.europa.eu/competition/transparency — WebLogic app.
  - Session affinity via an LB_TRANSPARENCY cookie whose Set-Cookie carries a
    MALFORMED Domain attribute (".europa.eu/competition/transparency") that
    python-requests' cookie jar silently rejects → we re-set it manually into
    the jar (a manual "Cookie:" header would be dropped on redirect hops).
  - Flow: GET /public?lang=en (registers language + issues LB cookie)
        → GET /public/search?lang=en (country form + CSRFTOKEN)
        → POST /public/search  countries=CountryIRL
        → POST /public/search/results (criteria left empty = everything)
        → GET  /public/search/results?offset=N&max=100 paginates the
          server-side result set bound to the session.
  - Full cell values live in <td title="..."> (display text is truncated);
    National ID column is the SIX-DIGIT CRO COMPANY NUMBER for companies →
    direct join to the CRO supplier backbone (cro_num), far cleaner than
    name-matching.

VALUE SEMANTICS: value_kind=grant_awarded, realisation_tier=AWARDED — aid
AWARDS, not payments; aid_element for guarantees/loans is the subsidy
equivalent, nominal_amount the face value. NEVER union with payment facts
(project_procurement_ted_overlap). Ranges like "30000-60000" appear for
small-band disclosures → kept as text in *_raw, parsed floats only for
point values; value_safe_to_sum=False.

PRIVACY: beneficiaries are mostly companies but agri schemes include sole
traders / natural-person farm holders. Mirror the eTenders quarantine:
``beneficiary_is_individual_suspected`` heuristic flag for view-level
exclusion (feedback_personal_insolvency_privacy precedent applies to
display, not ingestion).

Outputs -> data/sandbox/enrichment/
  eu_tam_ireland_awards.parquet / .csv     one row per aid award
Page cache -> C:/tmp/tam_ie_pages/ (transient; delete to force refetch)

Run: .venv/Scripts/python.exe pipeline_sandbox/eu_tam_ireland_extract.py
"""

from __future__ import annotations

import logging
import re
import time
from datetime import date
from pathlib import Path

import polars as pl
import requests
from bs4 import BeautifulSoup

from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

log = logging.getLogger(__name__)

BASE = "https://webgate.ec.europa.eu/competition/transparency"
HEADERS = {"User-Agent": "Mozilla/5.0 (dail-tracker civic-data ingestion; contact p.glynn18@gmail.com)"}
OUT_DIR = Path("data/sandbox/enrichment")
PAGE_CACHE = Path("C:/tmp/tam_ie_pages")
PAGE_SIZE = 100
SLEEP_S = 0.4

COLUMNS = [
    "country",
    "aid_measure_title",
    "sa_number",
    "ref_no",
    "national_id",
    "beneficiary_name",
    "beneficiary_type",
    "region",
    "sector_nace",
    "aid_instrument",
    "objective",
    "nominal_amount_raw",
    "aid_element_raw",
    "date_granted",
    "granting_authority",
    "entrusted_entity",
    "financial_intermediary",
    "published_date",
    "other_beneficiary_ms",
    "third_country",
]

_AMOUNT_RE = re.compile(r"^([\d,]+(?:\.\d+)?)\s*([A-Z]{3})$")
_CORP_TOKENS = re.compile(
    r"\b(limited|ltd|plc|dac|clg|icav|uc|ulc|teoranta|teo|company|holdings|"
    r"group|farms?|co-?op|cooperative|society|partnership|college|university|"
    r"institute|council|board|trust|centre|center|club|association|services|"
    r"ireland|irish|&|and sons?)\b\.?",
    re.IGNORECASE,
)


def open_session() -> requests.Session:
    """Establish the TAM session: LB cookie (manual re-set), lang, country=IRL."""
    s = requests.Session()
    s.headers.update(HEADERS)
    r1 = s.get(f"{BASE}/public?lang=en", timeout=60)
    r1.raise_for_status()
    for sc in r1.raw.headers.get_all("Set-Cookie") or []:
        if sc.startswith("LB_TRANSPARENCY="):
            name, val = sc.split(";")[0].split("=", 1)
            # requests rejects the server's malformed Domain attribute, so the
            # affinity cookie must be planted in the jar by hand.
            s.cookies.set(name, val, domain="webgate.ec.europa.eu", path="/competition/transparency")
    if "LB_TRANSPARENCY" not in s.cookies:
        raise RuntimeError("LB_TRANSPARENCY cookie not issued — flow drift?")

    r2 = s.get(f"{BASE}/public/search?lang=en", timeout=60)
    csrf = re.findall(r'name="CSRFTOKEN"[^>]*value="([^"]+)"', r2.text)
    if not csrf:
        raise RuntimeError("No CSRFTOKEN on country-selection page — flow drift?")
    r3 = s.post(
        f"{BASE}/public/search",
        data=[
            ("CSRFTOKEN", csrf[0]),
            ("resetSearch", "true"),
            ("countries", "CountryIRL"),
            ("_countries", ""),
            ("_selectAll", ""),
            ("_grantingAuthorityRegions", ""),
        ],
        timeout=60,
    )
    r3.raise_for_status()
    csrf2 = re.findall(r'name="CSRFTOKEN"[^>]*value="([^"]+)"', r3.text) or csrf
    # Empty criteria = the full Ireland result set, bound to this session.
    r4 = s.post(f"{BASE}/public/search/results", data=[("CSRFTOKEN", csrf2[0]), ("lang", "en")], timeout=120)
    r4.raise_for_status()
    if "resultsTable" not in r4.text:
        raise RuntimeError("Results page lacks resultsTable — flow drift?")
    (PAGE_CACHE / "offset_0.html").write_text(r4.text, encoding="utf-8")
    return s


def cell_text(td) -> str:
    """Full value: title attribute beats the (possibly truncated) display text."""
    title = (td.get("title") or "").strip()
    text = re.sub(r"\s+", " ", td.get_text(" ", strip=True)).strip()
    if title and (text.endswith("...") or len(title) > len(text)):
        # National-ID cells abuse title for a tooltip ("Six digit Company
        # Registration") — keep the text there.
        if title.lower().startswith(("six digit", "ten digit", "registration")):
            return text
        return title
    return text


def parse_rows(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", id="resultsTable")
    if table is None:
        return []
    out = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) != len(COLUMNS):
            continue  # header / spacer rows
        row = {col: cell_text(td) or None for col, td in zip(COLUMNS, tds, strict=True)}
        # the ref-no cell links to the award detail page — keep the id
        a = tds[3].find("a", href=True)
        row["award_detail_url"] = a["href"] if a else None
        out.append(row)
    return out


def fetch_all(s: requests.Session) -> list[dict]:
    first = (PAGE_CACHE / "offset_0.html").read_text(encoding="utf-8")
    offsets = [int(x) for x in re.findall(r"offset=(\d+)&amp;max=\d+", first)]
    last_offset_10 = max(offsets) if offsets else 0
    total_estimate = last_offset_10 + 10  # first page paginates at max=10
    log.info("Total Ireland awards advertised: ~%d", total_estimate)

    rows: list[dict] = []
    for offset in range(0, total_estimate, PAGE_SIZE):
        cache = PAGE_CACHE / f"page_{offset:06d}.html"
        if cache.exists():
            html = cache.read_text(encoding="utf-8")
        else:
            r = s.get(f"{BASE}/public/search/results?offset={offset}&max={PAGE_SIZE}", timeout=120)
            r.raise_for_status()
            html = r.text
            cache.write_text(html, encoding="utf-8")
            time.sleep(SLEEP_S)
        page_rows = parse_rows(html)
        if not page_rows:
            log.warning("offset=%d returned 0 rows — stopping pagination", offset)
            break
        rows.extend(page_rows)
        if (offset // PAGE_SIZE) % 20 == 0:
            log.info("  offset=%d, %d rows so far", offset, len(rows))
    return rows


def main() -> int:
    setup_standalone_logging("eu_tam_ireland_extract")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    PAGE_CACHE.mkdir(parents=True, exist_ok=True)

    s = open_session()
    rows = fetch_all(s)
    if len(rows) < 10_000:
        log.error("Only %d rows (expected ~15,600) — incomplete crawl, not writing", len(rows))
        return 1

    # mostly-null columns (third_country, entrusted_entity) get their first
    # value far beyond the default 100-row inference window
    df = pl.DataFrame(rows, infer_schema_length=None)

    def split_amount(col: str, prefix: str) -> list[pl.Expr]:
        ext = pl.col(col).str.extract_groups(r"^([\d,]+(?:\.\d+)?)\s*([A-Z]{3})$")
        return [
            ext.struct["1"].str.replace_all(",", "").cast(pl.Float64, strict=False).alias(f"{prefix}_value"),
            ext.struct["2"].alias(f"{prefix}_currency"),
        ]

    df = df.with_columns(
        *split_amount("nominal_amount_raw", "nominal_amount"),
        *split_amount("aid_element_raw", "aid_element"),
        pl.col("date_granted").str.to_date("%d/%m/%Y", strict=False),
        pl.col("published_date").str.to_date("%d/%m/%Y", strict=False),
        pl.col("national_id").str.extract(r"^(\d{5,7})$").alias("cro_company_num"),
        pl.col("beneficiary_name")
        .map_elements(
            lambda b: bool(b) and not bool(_CORP_TOKENS.search(b)) and len(b.split()) <= 4,
            return_dtype=pl.Boolean,
        )
        .alias("beneficiary_is_individual_suspected"),
        pl.lit("grant_awarded").alias("value_kind"),
        pl.lit("AWARDED").alias("realisation_tier"),
        pl.lit(False).alias("value_safe_to_sum"),
        pl.lit(date.today()).alias("ingested_date"),
    )

    dup = df.group_by("ref_no").len().filter(pl.col("len") > 1).height
    log.info(
        "rows=%d | date range %s -> %s | cro_num join candidates=%d (%.0f%%) | "
        "individual-suspected=%d | dup ref_no groups=%d",
        len(df),
        df["date_granted"].min(),
        df["date_granted"].max(),
        df["cro_company_num"].is_not_null().sum(),
        100 * df["cro_company_num"].is_not_null().sum() / len(df),
        df["beneficiary_is_individual_suspected"].sum(),
        dup,
    )
    log.info(
        "top granting authorities: %s",
        df.group_by("granting_authority").len().sort("len", descending=True).head(5).to_dicts(),
    )

    save_parquet(df, OUT_DIR / "eu_tam_ireland_awards.parquet")
    df.write_csv(OUT_DIR / "eu_tam_ireland_awards.csv")
    log.info("Wrote %s (%d rows)", OUT_DIR / "eu_tam_ireland_awards.parquet", len(df))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
