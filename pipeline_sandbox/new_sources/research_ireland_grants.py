"""P0 grants — Research Ireland / SFI grant commitments (SANDBOX).

Grants are a THIRD public-money channel: never summed with procurement awards
or payments facts. Every amount here is a **multi-year COMMITMENT** (grant
award total), not an annual disbursement — hence ``grant_basis="commitment"``
and ``value_safe_to_sum=False`` on every row. Raw period fields are kept; no
annualisation.

Sources (both resolved live from the data.gov.ie CKAN API, licence CC-BY-4.0):
  1. ``research-ireland-grant-commitments`` — current cumulative file from
     Research Ireland (SFI+IRC merged agency), CSV on media.researchireland.ie
     (cp1252-encoded, embedded "€" in amounts).
  2. ``science-foundation-ireland-grants-commitments`` — legacy SFI open-data
     CSV (utf-8), frozen at 2024-07-31. Kept for rows whose grant id does NOT
     appear in the current file (dataset lineages differ; IRC-era awards only
     exist in #2... and pre-merger detail only in the legacy file).

Overlap between the two files is flagged per-row (``id_in_both_sources``) and
``is_current_source`` marks the preferred row per grant id — consumers must
filter on it before any aggregation, because the same grant can appear in both
datasets with a revised commitment amount.

privacy_tier="public_professional": lead applicants (PIs) are named
individuals published in their professional capacity by the funder itself.

Output: c:/tmp/dail_new_sources/silver/research_ireland_grants.parquet
"""
from __future__ import annotations

import io
import re
import sys
import time
from pathlib import Path

import polars as pl
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import cache_raw, now_iso, write_silver  # noqa: E402

# assets.gov.ie / gov.ie-family CDNs 403 a bot UA — same browser spoof as
# extractors/procurement_etenders_extract.py:55. Sent to sfi.ie /
# media.researchireland.ie too, in case they adopt the same WAF.
GOVIE_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "Referer": "https://www.gov.ie/"}
CKAN_SHOW = "https://data.gov.ie/api/3/action/package_show"

DATASETS = [
    # (source_dataset tag, CKAN package id, pinned fallback CSV url)
    (
        "research_ireland_current",
        "research-ireland-grant-commitments",
        "https://media.researchireland.ie/wp-content/uploads/2026/04/"
        "Research-Ireland-Grant-Commitments-to-January-2026-09.04.26.csv",
    ),
    (
        "sfi_legacy_2024_07",
        "science-foundation-ireland-grants-commitments",
        "https://www.sfi.ie/about-us/governance/open-data/Open-Data-2024-07-31.csv",
    ),
]

# header -> canonical name (headers stripped first; both files share concepts)
COLMAP = {
    "proposal id": "grant_id",
    "project id": "grant_id",
    "programme name": "programme",
    "sub-programme": "sub_programme",
    "supplement": "supplement",
    "lead applicant": "lead_applicant",
    "orcid": "orcid",
    "orcid id": "orcid",
    "research body": "research_body",
    "research body ror id": "research_body_ror",
    "funder name": "funder_name",
    "funder ror id": "funder_ror",
    "crossref funder registry id": "funder_crossref_id",
    "proposal title": "project_title",
    "project title": "project_title",
    "start date": "start_date_raw",
    "actual start date": "start_date_raw",
    "end date": "end_date_raw",
    "final end date": "end_date_raw",
    "current total commitment": "amount_raw",
    "award amount": "amount_raw",
}

CANONICAL = [
    "grant_id", "programme", "sub_programme", "supplement", "lead_applicant",
    "orcid", "research_body", "research_body_ror", "funder_name", "funder_ror",
    "funder_crossref_id", "project_title", "start_date_raw", "end_date_raw",
    "amount_raw",
]


def fetch_bytes(url: str, timeout: int = 120) -> tuple[bytes, dict]:
    time.sleep(0.4)
    r = requests.get(url, headers=GOVIE_HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.content, {
        "source_url": r.url,
        "source_last_modified": r.headers.get("last-modified"),
        "fetched_at": now_iso(),
    }


def resolve_csv(package_id: str, fallback_url: str) -> tuple[str, str, str | None]:
    """CKAN package_show -> (csv_url, licence, resource_created). Pinned fallback on any blip."""
    try:
        r = requests.get(CKAN_SHOW, params={"id": package_id}, headers=GOVIE_HEADERS, timeout=45)
        r.raise_for_status()
        res = r.json()["result"]
        licence = res.get("license_id") or "unknown"
        for rs in res.get("resources", []):
            u = (rs.get("url") or "").strip()
            if u.lower().endswith(".csv"):
                if u != fallback_url:
                    print(f"  {package_id}: CKAN resolved a newer CSV url: {u}")
                return u, licence, rs.get("created")
    except Exception as e:  # noqa: BLE001 — a CKAN blip must not kill the run
        print(f"  {package_id}: CKAN resolve failed ({type(e).__name__}: {e}) — using pinned url")
    return fallback_url, "CC-BY-4.0", None


def decode_csv(raw: bytes) -> tuple[str, str]:
    """Irish gov CSVs are utf-8(-sig) or cp1252 (€ = 0x80). Detect, record which."""
    try:
        return raw.decode("utf-8-sig"), "utf-8-sig"
    except UnicodeDecodeError:
        return raw.decode("cp1252"), "cp1252"


# "Ã" almost never occurs in genuine Irish text; it is the signature first byte of a
# utf-8 sequence mis-decoded as cp1252 (e.g. "GrÃ¡inne"). Repair is a safe round-trip:
# if the string wasn't really utf-8, the round-trip raises and we keep the original.
_MOJIBAKE_RE = re.compile("Ã")


def _repair_mojibake(s: str | None) -> str | None:
    """The SFI legacy CSV mixes encodings row-by-row: a handful of rows are
    utf-8 inside an otherwise-cp1252 file. Re-round-trip only strings that
    show the mis-decode signature."""
    if s and _MOJIBAKE_RE.search(s):
        try:
            return s.encode("cp1252").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            return s
    return s


_YEAR_RE = re.compile(r"(19|20)\d{2}")
_DATE_FORMATS = ["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%y", "%m/%d/%Y"]


def _parse_dates(df: pl.DataFrame, raw_col: str, out_col: str) -> pl.DataFrame:
    """Best-effort date parse across the formats seen in these files; raw kept."""
    parsed = pl.coalesce([
        pl.col(raw_col).str.strip_chars().str.to_date(fmt, strict=False) for fmt in _DATE_FORMATS
    ])
    return df.with_columns(parsed.alias(out_col))


def load_one(tag: str, package_id: str, fallback_url: str) -> pl.DataFrame:
    url, licence, res_created = resolve_csv(package_id, fallback_url)
    raw, meta = fetch_bytes(url)
    fname = url.rsplit("/", 1)[-1] or f"{tag}.csv"
    _, sha = cache_raw("research_ireland_grants", f"{tag}__{fname}", raw)
    text, enc = decode_csv(raw)
    print(f"  {tag}: {len(raw):,} bytes, encoding={enc}, sha={sha[:12]}…")

    df = pl.read_csv(io.BytesIO(text.encode("utf-8")), infer_schema_length=0)  # all utf8, no guessing
    df = df.rename({c: COLMAP.get(c.strip().lower(), c.strip().lower().replace(" ", "_")) for c in df.columns})
    unmapped = [c for c in df.columns if c not in CANONICAL]
    if unmapped:
        print(f"  {tag}: NOTE unmapped columns kept out of canonical set: {unmapped}")
    for col in CANONICAL:  # align schema across the two files
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))
    df = df.select(CANONICAL)
    if enc == "cp1252":  # repair per-row utf-8 strays in a cp1252 file
        df = df.with_columns([
            pl.col(c).map_elements(_repair_mojibake, return_dtype=pl.Utf8).alias(c)
            for c in ("lead_applicant", "research_body", "project_title", "programme")
        ])

    # amount: strip €/commas/spaces; empty -> null. COMMITMENT grain, never safe to sum.
    df = df.with_columns(
        pl.col("amount_raw").str.replace_all(r"[€,\s]", "").replace("", None)
        .cast(pl.Float64, strict=False).alias("amount_committed_eur")
    )
    df = _parse_dates(df, "start_date_raw", "start_date")
    df = _parse_dates(df, "end_date_raw", "end_date")
    df = df.with_columns(
        pl.coalesce([
            pl.col("start_date").dt.year(),
            pl.col("start_date_raw").str.extract(_YEAR_RE.pattern, 0).cast(pl.Int32, strict=False),
        ]).alias("start_year"),
        pl.coalesce([
            pl.col("end_date").dt.year(),
            pl.col("end_date_raw").str.extract(_YEAR_RE.pattern, 0).cast(pl.Int32, strict=False),
        ]).alias("end_year"),
    )

    return df.with_columns(
        pl.lit(tag).alias("source_dataset"),
        pl.lit("commitment").alias("grant_basis"),
        pl.lit(False).alias("value_safe_to_sum"),
        pl.lit(meta["source_url"]).alias("source_url"),
        pl.lit(sha).alias("source_document_hash"),
        pl.lit(meta["fetched_at"]).alias("fetched_at"),
        pl.lit(res_created).alias("source_published_date"),
        pl.lit(meta["source_last_modified"]).alias("source_last_modified"),
        pl.lit(f"ckan_resolve+csv_download({enc})").alias("extraction_method"),
        pl.lit("high").alias("confidence"),
        pl.lit("public_professional").alias("privacy_tier"),  # named PIs, professional capacity
        pl.lit(licence).alias("licence"),
    )


def run() -> None:
    frames = [load_one(tag, pkg, url) for tag, pkg, url in DATASETS]
    current, legacy = frames[0], frames[1]

    cur_ids = set(current["grant_id"].drop_nulls().to_list())
    both = pl.col("grant_id").is_in(list(cur_ids))
    legacy = legacy.with_columns(both.alias("id_in_both_sources"), (~both).alias("is_current_source"))
    current = current.with_columns(
        pl.col("grant_id").is_in(legacy.filter(pl.col("id_in_both_sources"))["grant_id"].to_list())
        .alias("id_in_both_sources"),
        pl.lit(True).alias("is_current_source"),
    )
    df = pl.concat([current, legacy], how="vertical")
    out = write_silver("research_ireland_grants", df)

    # ---- profile (counts only; NEVER print summed € — commitments don't sum) ----
    overlap = int(legacy["id_in_both_sources"].sum())
    print(f"\nSILVER: {out}  rows={df.height}  (current={current.height}, legacy={legacy.height}, "
          f"legacy-ids-also-in-current={overlap})")
    cur_only = df.filter(pl.col("is_current_source"))
    print(f"  preferred rows (is_current_source): {cur_only.height}")
    print(f"  start_year range: {df['start_year'].min()} … {df['start_year'].max()}")
    for col in ("grant_id", "lead_applicant", "research_body", "amount_committed_eur", "start_date", "programme"):
        nulls = df[col].null_count()
        print(f"  null rate {col}: {nulls}/{df.height} ({100*nulls/df.height:.1f}%)")
    print("  top research bodies by grant COUNT (not €):")
    for r in cur_only.group_by("research_body").len().sort("len", descending=True).head(8).to_dicts():
        print(f"    {r['len']:>5}  {r['research_body']}")
    print("  top programmes by grant COUNT:")
    for r in cur_only.group_by("programme").len().sort("len", descending=True).head(5).to_dicts():
        print(f"    {r['len']:>5}  {r['programme']}")


if __name__ == "__main__":
    run()
