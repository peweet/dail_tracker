"""
Committees long-format ETL.

Reads the silver flattened-members parquet for both chambers and unpivots
the wide ``committee_N_*`` / ``office_N_*`` slot columns into long-format
parquets the UI can consume directly through registered analytical views.

Replaces the in-page reshape that used to live in
``utility/pages_code/committees.py::_load`` — two ``df.iterrows()`` passes
over the silver CSV on every page render. The iterrows loop was the actual
hot path on the committees page.

Outputs (both with zstd / level=3 / statistics=True):

* ``data/silver/committees/committee_assignments.parquet`` — one row per
  (member × committee slot) where a committee is present. Columns match
  the contract the page previously built in-memory:
    chamber, name, party, constituency, dail_number,
    committee, committee_url, type, status, role, is_chair, start, end
* ``data/silver/committees/office_holders.parquet`` — one row per
  (member × office slot) where an office is present. Columns:
    chamber, name, party, office, start, end

Run ad-hoc::

    python committees_long_format_etl.py

Safe to re-run; outputs are overwritten atomically by Polars.
"""

from __future__ import annotations

import re
import sys
import unicodedata
from pathlib import Path

import polars as pl

from config import SILVER_DIR, SILVER_PARQUET_DIR

_SILVER_PARQUET_DIR = SILVER_PARQUET_DIR
_OUT_DIR = SILVER_DIR / "committees"

_SILVER_INPUTS: dict[str, Path] = {
    "Dáil": _SILVER_PARQUET_DIR / "flattened_members.parquet",
    "Seanad": _SILVER_PARQUET_DIR / "flattened_seanad_members.parquet",
}

# Map committees.py's status normalisation.
_STATUS_MAP: dict[str, str] = {"Live": "Active", "Dissolved": "Ended"}

# Strip "(Dáil Éireann)" / "(Seanad Éireann)" suffix from committee names
# (carried over from committees.py P1-6 fix — the chamber pill already
# establishes context, so the suffix is duplication noise).
_CHAMBER_SUFFIX_RE = re.compile(
    r"\s*\((?:Dáil|Seanad)\s+Éireann\)\s*$",
)


def _committee_slug(name: str | None) -> str | None:
    """Mirror committees.py::_committee_slug for canonical Oireachtas URLs."""
    if name is None:
        return None
    s = str(name)
    suffix = ""
    chamber_patterns: list[tuple[tuple[str, ...], str]] = [
        (("Dáil Committee on ", "Dail Committee on "), "-dail"),
        (("Seanad Committee on ",), "-seanad"),
    ]
    matched = False
    for prefixes, suf in chamber_patterns:
        for prefix in prefixes:
            if s.startswith(prefix):
                s = s[len(prefix) :]
                suffix = suf
                matched = True
                break
        if matched:
            break
    if not matched:
        for prefix in (
            "Joint Committee on ",
            "Select Committee on ",
            "Committee on ",
        ):
            if s.startswith(prefix):
                s = s[len(prefix) :]
                break
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii").lower()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"\s+", "-", s.strip())
    return (s + suffix) if s else None


def _committee_url(name: str | None, dail_number) -> str | None:
    if name is None or dail_number is None:
        return None
    slug = _committee_slug(name)
    try:
        return f"https://www.oireachtas.ie/en/committees/{int(dail_number)}/{slug}/" if slug else None
    except (ValueError, TypeError):
        return None


def _committee_slot_records(row: dict, slot: int, chamber: str) -> dict | None:
    """Build one assignment record for committee_N or return None if empty."""
    name_raw = row.get(f"committee_{slot}_name_en")
    if name_raw is None or name_raw == "":
        return None
    name_clean = _CHAMBER_SUFFIX_RE.sub("", str(name_raw)).strip()
    role = row.get(f"committee_{slot}_role_title") or "Member"
    return {
        "chamber": chamber,
        "name": row.get("full_name") or "Unknown",
        "party": row.get("party") or "Unknown",
        "constituency": row.get("constituency_name"),
        "dail_number": row.get("dail_number"),
        "committee": name_clean,
        "committee_url": _committee_url(name_clean, row.get("dail_number")),
        "type": row.get(f"committee_{slot}_type") or "Unknown",
        "status": _STATUS_MAP.get(row.get(f"committee_{slot}_main_status"), "Unknown"),
        "role": role,
        "is_chair": "cathaoirleach" in str(role).lower(),
        "start": (row.get(f"committee_{slot}_role_start_date") or row.get(f"committee_{slot}_member_start_date")),
        "end": (row.get(f"committee_{slot}_role_end_date") or row.get(f"committee_{slot}_member_end_date")),
    }


def _office_slot_records(row: dict, slot: int, chamber: str) -> dict | None:
    name_raw = row.get(f"office_{slot}_name")
    if name_raw is None or name_raw == "":
        return None
    return {
        "chamber": chamber,
        "name": row.get("full_name") or "Unknown",
        "party": row.get("party") or "Unknown",
        "office": str(name_raw).strip(),
        "start": row.get(f"office_{slot}_start_date"),
        "end": row.get(f"office_{slot}_end_date"),
    }


def _detect_slots(cols: list[str], prefix: str) -> list[int]:
    """Discover which N values appear in <prefix>_N_* columns."""
    seen: set[int] = set()
    for c in cols:
        m = re.match(rf"{prefix}_(\d+)_", c)
        if m:
            seen.add(int(m.group(1)))
    return sorted(seen)


def build() -> None:
    _OUT_DIR.mkdir(parents=True, exist_ok=True)
    assignments: list[dict] = []
    offices: list[dict] = []

    for chamber, path in _SILVER_INPUTS.items():
        if not path.exists():
            print(f"WARN: {chamber} silver parquet missing: {path}", file=sys.stderr)
            continue
        df = pl.read_parquet(path)
        cols = df.columns
        committee_slots = _detect_slots(cols, "committee")
        office_slots = _detect_slots(cols, "office")
        # Polars iter_rows(named=True) yields dicts with proper Python scalars.
        for row in df.iter_rows(named=True):
            for slot in committee_slots:
                rec = _committee_slot_records(row, slot, chamber)
                if rec is not None:
                    assignments.append(rec)
            for slot in office_slots:
                rec = _office_slot_records(row, slot, chamber)
                if rec is not None:
                    offices.append(rec)

    # Silver date strings look like "2025-05-21 00:00:00+00:00" — explicit
    # format with %z, then convert to naive (matching the old pandas
    # to_datetime(..., utc=True).dt.tz_localize(None) behaviour).
    _DATE_FMT = "%Y-%m-%d %H:%M:%S%z"

    def _parse_dates(df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            pl.col("start").str.to_datetime(format=_DATE_FMT, time_zone="UTC", strict=False).dt.replace_time_zone(None),
            pl.col("end").str.to_datetime(format=_DATE_FMT, time_zone="UTC", strict=False).dt.replace_time_zone(None),
        )

    if assignments:
        df_a = _parse_dates(pl.DataFrame(assignments)).with_columns(
            pl.col("dail_number").cast(pl.Int32, strict=False),
        )
    else:
        df_a = pl.DataFrame(
            schema={
                "chamber": pl.Utf8,
                "name": pl.Utf8,
                "party": pl.Utf8,
                "constituency": pl.Utf8,
                "dail_number": pl.Int32,
                "committee": pl.Utf8,
                "committee_url": pl.Utf8,
                "type": pl.Utf8,
                "status": pl.Utf8,
                "role": pl.Utf8,
                "is_chair": pl.Boolean,
                "start": pl.Datetime,
                "end": pl.Datetime,
            }
        )

    if offices:
        df_o = _parse_dates(pl.DataFrame(offices))
    else:
        df_o = pl.DataFrame(
            schema={
                "chamber": pl.Utf8,
                "name": pl.Utf8,
                "party": pl.Utf8,
                "office": pl.Utf8,
                "start": pl.Datetime,
                "end": pl.Datetime,
            }
        )

    assignments_path = _OUT_DIR / "committee_assignments.parquet"
    offices_path = _OUT_DIR / "office_holders.parquet"
    df_a.write_parquet(
        assignments_path,
        compression="zstd",
        compression_level=3,
        statistics=True,
    )
    df_o.write_parquet(
        offices_path,
        compression="zstd",
        compression_level=3,
        statistics=True,
    )
    print(f"wrote {assignments_path} — {len(df_a)} rows")
    print(f"wrote {offices_path} — {len(df_o)} rows")


if __name__ == "__main__":
    build()
