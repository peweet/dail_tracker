"""
payments_full_psa_etl.py — full PSA (TAA + PRA) re-parser, schema-aware

STATUS: SANDBOX. Not wired into pipeline.py. Run directly:
    python pipeline_sandbox/payments_full_psa_etl.py

WHY THIS EXISTS
---------------
The current production parser (`payments.py`) assumes a single 5-column PDF
schema (Name, TAA_Band, Narrative, Date, Amount) and quarantines anything that
doesn't fit. That schema only matches PDFs from Jul 2020 onwards.

Jan-Jun 2020 PDFs use a different layout, and the Jul 2020+ PDFs themselves
contain PRA-side rows the production parser also throws into quarantine
because the band column reads "Vouched" / "MIN" / blank instead of an integer.

Net effect today:
- Jan-Apr 2020 column-shifted into the quarantine table (recoverable)
- May-Jun 2020 dropped entirely (6-col schema, parser expects 5)
- 2020-2026 PRA-side rows quarantined (Vouched / MIN / Dub / blank band)

Combined we lose ~€4-5M from the published PSA totals, which is exactly the
gap users see when comparing our totals to gript's published figures.

PDF SCHEMA HISTORY (verified by reading bronze PDFs)
----------------------------------------------------
Jan 2020 - Apr 2020   : 5 cols, no narrative
    Name | PRA flag | TAA Band | Date of Payment | Amount
May 2020 - Jun 2020   : 6 cols, with narrative
    Name | PRA flag | TAA Band | Narrative | Date Paid | Amount Paid
Jul 2020 onwards      : 5 cols, no PRA-flag column; PRA appears as separate
                        rows where TAA Band = "Vouched"/"MIN"
    Name | TAA Band | Narrative | Date Paid | Amount Paid

For Jan-Jun 2020 the PRA-flag column is just an indicator ("Vouched" or
empty); no separate PRA amount is published. So PRA amounts for that period
are NOT recoverable from these PDFs — the full-PSA gap there is a source-data
limitation, not a parser limitation.

OUTPUT
------
data/gold/parquet/payments_full_psa.parquet — one row per published payment
with columns:
    member_name, position, payment_kind, taa_band_raw, taa_band_label,
    date_paid, narrative, amount, source_pdf

`payment_kind` values:
    TAA           — distance-banded travel allowance
    PSA_DUBLIN    — flat allowance for sub-25km TDs (replaces TAA)
    PRA           — vouched representation allowance (ordinary TD)
    PRA_MIN       — vouched representation allowance at minister rate
    PRA_FLAG_ONLY — Jan-Jun 2020 only: PRA was claimed but amount not in PDF

This file is intentionally separate from `payments_fact.parquet` so the
production page keeps working unchanged. To wire this in, replace the
parser stage in payments.py with the schema-aware logic here and update
sql_views/payments_base.sql to read this parquet.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

import fitz  # PyMuPDF
import polars as pl

# Reach into project root from pipeline_sandbox/
PROJECT_ROOT = Path(__file__).resolve().parents[1]
BRONZE_PDF_DIR = PROJECT_ROOT / "data" / "bronze" / "pdfs" / "payments"
GOLD_PARQUET_DIR = PROJECT_ROOT / "data" / "gold" / "parquet"
OUTPUT_PARQUET = GOLD_PARQUET_DIR / "payments_full_psa.parquet"
OUTPUT_CSV = PROJECT_ROOT / "data" / "gold" / "csv" / "payments_full_psa.csv"
QUARANTINE_PARQUET = GOLD_PARQUET_DIR / "payments_full_psa_quarantine.parquet"

TAA_LABELS = {
    "Dublin": "Dublin / under 25 km",
    "1":  "Band 1 — 25–60 km",
    "2":  "Band 2 — 60–80 km",
    "3":  "Band 3 — 80–100 km",
    "4":  "Band 4 — 100–130 km",
    "5":  "Band 5 — 130–160 km",
    "6":  "Band 6 — 160–190 km",
    "7":  "Band 7 — 190–210 km",
    "8":  "Band 8 — over 210 km",
    "9":  "Band 9 (unmapped)",
    "10": "Band 10 (unmapped)",
    "11": "Band 11 (unmapped)",
    "12": "Band 12 (unmapped)",
}

# Filename → period helper, used to synthesize narrative for Jan-Apr 2020 PDFs
# and to attribute every row to a known publication.
MONTH_NAMES = {n: i for i, n in enumerate(
    ["january","february","march","april","may","june",
     "july","august","september","october","november","december"], start=1
)}

_FILENAME_PERIOD_RE = re.compile(
    r"for-(?:(\d{1,2})-(\d{1,2})-)?([a-z]+)-(\d{4})_en\.pdf$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class PdfPeriod:
    year: int
    month: int
    month_name: str
    day_from: int | None
    day_to: int | None

    @property
    def narrative(self) -> str:
        if self.day_from and self.day_to:
            return f"PSA {self.day_from} - {self.day_to} {self.month_name.capitalize()} {self.year}"
        return f"PSA {self.month_name.capitalize()} {self.year}"


def _parse_filename_period(path: Path) -> PdfPeriod | None:
    m = _FILENAME_PERIOD_RE.search(path.name.lower())
    if not m:
        return None
    day_from, day_to, month_name, year = m.groups()
    month = MONTH_NAMES.get(month_name)
    if month is None:
        return None
    return PdfPeriod(
        year=int(year),
        month=month,
        month_name=month_name,
        day_from=int(day_from) if day_from else None,
        day_to=int(day_to) if day_to else None,
    )


# ---------------------------------------------------------------------------
# Schema detection
# ---------------------------------------------------------------------------
# We detect schema by joining the first non-None header row tokens and matching
# against known patterns. Falling back on column count is fragile because empty
# trailing cells can mask the real width.

_HEADER_TOKENS_RE = re.compile(r"[a-z]+", re.IGNORECASE)


def _normalise_header(row: list) -> str:
    """Concatenate header text into a lowercase token string for matching."""
    parts: list[str] = []
    for cell in row:
        if cell is None:
            continue
        parts.extend(_HEADER_TOKENS_RE.findall(str(cell).lower()))
    return " ".join(parts)


def _is_header_row(row: list) -> bool:
    text = _normalise_header(row)
    return "name" in text and ("amount" in text or "taa" in text)


# Maps each known schema to the column index of each logical field.
# A value of None means the field is not present in that schema and must be
# synthesised (e.g. narrative from filename for Jan-Apr 2020).
SCHEMAS: dict[str, dict[str, int | None]] = {
    # Jan-Apr 2020:  Name | PRA | TAA Band | Date | Amount
    "v2020_h1_early": {
        "name": 0, "pra_flag": 1, "taa_band": 2, "narrative": None,
        "date": 3, "amount": 4,
    },
    # May-Jun 2020:  Name | PRA | TAA Band | Narrative | Date | Amount
    "v2020_h1_late": {
        "name": 0, "pra_flag": 1, "taa_band": 2, "narrative": 3,
        "date": 4, "amount": 5,
    },
    # Jul 2020+:     Name | TAA Band | Narrative | Date | Amount
    "v2020_h2_plus": {
        "name": 0, "pra_flag": None, "taa_band": 1, "narrative": 2,
        "date": 3, "amount": 4,
    },
}


def _detect_schema(header_text: str, ncols: int) -> str | None:
    if "pra" in header_text and "narrative" in header_text:
        return "v2020_h1_late"
    if "pra" in header_text:
        return "v2020_h1_early"
    if "narrative" in header_text or "date paid" in header_text:
        return "v2020_h2_plus"
    # Header tokens differ across PDFs; fall back on shape.
    if ncols == 6:
        return "v2020_h1_late"
    if ncols == 5:
        return "v2020_h2_plus"
    return None


# ---------------------------------------------------------------------------
# Field cleaning
# ---------------------------------------------------------------------------

_AMOUNT_NUMERIC_RE = re.compile(r"[^0-9.\-]")


def _clean_amount(value) -> float | None:
    if value is None:
        return None
    cleaned = _AMOUNT_NUMERIC_RE.sub("", str(value))
    if not cleaned or cleaned in {"-", ".", "-."}:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


# Dates appear in three formats across the corpus: DD/MM/YYYY (most),
# M/D/YYYY (May-Jun 2020), and YYYY-MM-DD (rare).
_DATE_FORMATS = ("%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d")


def _parse_date(value, expected_year: int | None = None) -> date | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    for fmt in _DATE_FORMATS:
        try:
            d = datetime.strptime(s, fmt).date()
        except ValueError:
            continue
        # When both DD/MM and MM/DD succeed (e.g. 5/3/2020), the parse below
        # may pick the wrong one. Cross-check against the expected publication
        # year/month and skip implausible values.
        if expected_year and abs(d.year - expected_year) > 1:
            continue
        return d
    return None


def _split_position(name_cell: str) -> tuple[str, str]:
    """Names appear as 'Deputy Adams, Gerry' / 'Minister Harris, Simon' /
    'Taoiseach Varadkar, Leo'. Split on the first space. If the leading token
    is not a known position, treat the whole cell as the name (Deputy default).
    """
    if not name_cell:
        return "Deputy", ""
    parts = name_cell.strip().split(" ", 1)
    if len(parts) == 2 and parts[0] in {"Deputy", "Minister", "Taoiseach", "Tánaiste", "Tanaiste"}:
        return parts[0], parts[1].strip()
    return "Deputy", name_cell.strip()


def _normalise_band(raw) -> str:
    if raw is None:
        return ""
    return str(raw).strip()


def _classify_payment(band_raw: str, schema_key: str, pra_flag_raw: str | None) -> str:
    """Map (band, schema) to a payment_kind tag.

    Jan-Jun 2020 schemas only carry one amount per row; the PRA-flag column is
    purely an indicator. So even if pra_flag is "Vouched", the amount we're
    seeing is the TAA / Dublin / ministerial-rate disbursement, not a PRA
    amount. PRA amounts for that period live outside these PDFs.
    """
    band = (band_raw or "").strip()
    band_upper = band.upper().replace(" ", "")  # collapse "No TAA" / "NoTAA"
    schema = schema_key

    # Composite bands like "MIN/8", "2/MIN", "Dub/MIN" indicate a role change
    # mid-month (TD became minister or vice versa). Treat the non-MIN side as
    # the canonical kind so the row is kept; this matches how the Oireachtas
    # publishes these arrears entries.
    if "/" in band_upper:
        parts = [p for p in band_upper.split("/") if p]
        non_min = [p for p in parts if p != "MIN"]
        if non_min:
            band_upper = non_min[0]
        else:
            band_upper = "MIN"

    if band_upper in {"DUBLIN", "DUB", "DUBIN", "DULIN"}:
        # Pre-Jul-2020 the row carries the Dublin TD allowance.
        # Jul-2020+ a "Dublin" band row is the Dublin allowance (TAA bands
        # don't apply under 25 km). "Dubin" / "Dulin" are OCR mis-reads
        # observed in 2020 PDFs.
        return "PSA_DUBLIN"
    if band_upper == "MIN":
        return "PRA_MIN"
    if band_upper == "CC":
        # Ceann Comhairle — chair of the Dáil; receives a fixed allowance in
        # lieu of standard banded TAA. Roll into PRA_MIN bucket so it doesn't
        # silently inflate TAA totals.
        return "PRA_MIN"
    if band_upper == "VOUCHED":
        return "PRA"
    if band_upper == "NOTAA":
        return "PRA"  # TD waived TAA; amount shown is their PRA receipt
    if band == "":
        # Empty band: in Jan-Jun 2020 these are ministers (no TAA banding); in
        # Jul-2020+ these are the PRA-side rows for non-minister TDs.
        if schema in {"v2020_h1_early", "v2020_h1_late"}:
            return "PRA_MIN"
        return "PRA"
    if band_upper.isdigit():
        return "TAA"
    # Anything else (e.g. "Kenny", "y Vouched") is malformed.
    return "UNKNOWN"


# ---------------------------------------------------------------------------
# Per-PDF extraction
# ---------------------------------------------------------------------------

@dataclass
class ExtractedRow:
    member_name: str
    position: str
    payment_kind: str
    taa_band_raw: str
    taa_band_label: str | None
    date_paid: date | None
    narrative: str
    amount: float | None
    source_pdf: str
    schema: str

    def is_money_shaped(self) -> bool:
        return (
            self.amount is not None
            and 0 < self.amount < 100_000
            and self.date_paid is not None
            and bool(self.member_name)
            and self.payment_kind != "UNKNOWN"
        )


def _iter_rows_from_pdf(pdf_path: Path) -> Iterable[ExtractedRow]:
    period = _parse_filename_period(pdf_path)
    if period is None:
        return  # skip files we can't attribute

    fallback_narrative = period.narrative

    with fitz.open(pdf_path) as doc:
        for page in doc:
            for table in page.find_tables().tables:
                ext = table.extract()
                if not ext:
                    continue
                # Find the header row (first row containing "Name" + "Amount").
                header_idx = None
                for i, row in enumerate(ext[:3]):  # only check first few rows
                    if _is_header_row(row):
                        header_idx = i
                        break
                if header_idx is None:
                    # Some pages start mid-table (continuation page). Use the
                    # most-frequent column-count to guess the schema.
                    header_text = ""
                    ncols = max((len(r) for r in ext), default=0)
                    schema_key = _detect_schema(header_text, ncols)
                    data_rows = ext
                else:
                    header_text = _normalise_header(ext[header_idx])
                    ncols = len(ext[header_idx])
                    schema_key = _detect_schema(header_text, ncols)
                    data_rows = ext[header_idx + 1:]

                if schema_key is None:
                    continue

                schema = SCHEMAS[schema_key]

                for row in data_rows:
                    if not row or not any(row):
                        continue
                    name_cell = row[schema["name"]] if schema["name"] is not None else None
                    if not name_cell or "Parliamentary Standard Allowance" in str(name_cell):
                        continue

                    position, full_name = _split_position(str(name_cell))
                    band_raw = _normalise_band(row[schema["taa_band"]] if schema["taa_band"] is not None and schema["taa_band"] < len(row) else "")
                    pra_flag = (
                        str(row[schema["pra_flag"]]).strip() if schema["pra_flag"] is not None and schema["pra_flag"] < len(row) else None
                    )
                    narr_idx = schema["narrative"]
                    narrative = (
                        str(row[narr_idx]).strip() if narr_idx is not None and narr_idx < len(row) and row[narr_idx]
                        else fallback_narrative
                    )
                    date_idx = schema["date"]
                    amount_idx = schema["amount"]
                    raw_date = row[date_idx] if date_idx is not None and date_idx < len(row) else None
                    raw_amount = row[amount_idx] if amount_idx is not None and amount_idx < len(row) else None

                    payment_kind = _classify_payment(band_raw, schema_key, pra_flag)

                    yield ExtractedRow(
                        member_name=full_name,
                        position=position,
                        payment_kind=payment_kind,
                        taa_band_raw=band_raw,
                        taa_band_label=TAA_LABELS.get(band_raw),
                        date_paid=_parse_date(raw_date, expected_year=period.year),
                        narrative=narrative,
                        amount=_clean_amount(raw_amount),
                        source_pdf=pdf_path.name,
                        schema=schema_key,
                    )


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def build_full_psa() -> dict[str, int]:
    pdfs = sorted(BRONZE_PDF_DIR.glob("*.pdf"))
    print(f"Found {len(pdfs)} PSA PDFs in {BRONZE_PDF_DIR}")

    rows: list[ExtractedRow] = []
    for pdf in pdfs:
        before = len(rows)
        for r in _iter_rows_from_pdf(pdf):
            rows.append(r)
        print(f"  {pdf.name}: {len(rows) - before} rows")

    df = pl.DataFrame(
        {
            "member_name":  [r.member_name for r in rows],
            "position":     [r.position for r in rows],
            "payment_kind": [r.payment_kind for r in rows],
            "taa_band_raw": [r.taa_band_raw for r in rows],
            "taa_band_label": [r.taa_band_label for r in rows],
            "date_paid":    [r.date_paid for r in rows],
            "narrative":    [r.narrative for r in rows],
            "amount":       [r.amount for r in rows],
            "source_pdf":   [r.source_pdf for r in rows],
            "schema":       [r.schema for r in rows],
        }
    )

    # Dedup: same member, same date_paid, same amount, same kind = duplicate
    # (some PDFs are republished or share rows across listings).
    df = df.unique(subset=["member_name", "date_paid", "amount", "payment_kind"], keep="first")

    is_clean = (
        pl.col("amount").is_not_null()
        & pl.col("amount").is_between(1, 100_000)
        & pl.col("date_paid").is_not_null()
        & (pl.col("payment_kind") != "UNKNOWN")
        & pl.col("member_name").str.len_chars().gt(0)
    )

    clean = df.filter(is_clean)
    quarantine = df.filter(~is_clean)

    OUTPUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    clean.write_parquet(OUTPUT_PARQUET)
    clean.write_csv(OUTPUT_CSV)
    quarantine.write_parquet(QUARANTINE_PARQUET)

    return {"clean_rows": clean.height, "quarantine_rows": quarantine.height}


if __name__ == "__main__":
    import io, sys
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    stats = build_full_psa()
    print()
    print(f"Wrote {stats['clean_rows']:,} clean rows -> {OUTPUT_PARQUET}")
    print(f"Wrote {stats['quarantine_rows']:,} quarantine rows -> {QUARANTINE_PARQUET}")
