"""extractors/cro_financial_statements_extract.py — CRO filing INDEX.

Fetches + normalises the CRO financial-statements FILING INDEX (free CKAN open
data) → data/silver/cro/financial_statements.parquet. Keyed on company_num, so it
joins the CRO register (and through that, procurement supplier / lobbying org
matches).

What this IS / is NOT
---------------------
It is the submission INDEX: per-filing metadata + the *paywalled* document
filename. It is NOT the financial figures — those sit in the PDFs behind the CORE
paywall (~€2.50/doc). See doc/SOURCES.md.

Why ingest it (benefit over the register's last_accounts_date)
--------------------------------------------------------------
The companies register carries a single *latest* accounts date. This index is the
full filing EVENT LOG, so it surfaces multi-year filing consistency/gaps and is
the targeting map if the paid PDFs are ever pursued. Faithful event grain: ALL
filings are kept (a company can file more than once per period — amendments).

Idempotent: each yearly CSV is re-downloaded only when the CKAN last_modified is
newer than what the coverage sidecar records (or --force / missing bronze).

Run:
    python extractors/cro_financial_statements_extract.py
    python extractors/cro_financial_statements_extract.py --force
"""

from __future__ import annotations

import argparse
import contextlib
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

import orjson
import polars as pl
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import config  # noqa: E402
from services.parquet_io import save_parquet  # noqa: E402

CKAN_BASE = "https://opendata.cro.ie"
PACKAGE_ID = "financial-statements"
USER_AGENT = "dail-tracker-bot/0.1 (+https://github.com/peweet/dail_tracker; mailto:p.glynn18@gmail.com)"

BRONZE_DIR = config.BRONZE_DIR / "cro"
SILVER_OUT = config.SILVER_DIR / "cro" / "financial_statements.parquet"
COVERAGE_OUT = config.PROJECT_ROOT / "data" / "_meta" / "cro_financial_statements_coverage.json"

# source columns (the 8-field index). Renamed/typed in normalise().
EXPECTED_COLUMNS = {
    "file_name",
    "company_num",
    "company_name",
    "submission_num",
    "submission_rec_date",
    "submission_eff_date",
    "submission_reg_date",
    "submissions_accounts_to_date",
}
DATE_COLS = ["submission_rec_date", "submission_eff_date", "submission_reg_date", "submissions_accounts_to_date"]
MIN_TOTAL_ROWS = 200_000  # 2022 alone is ~214k; floor rejects a truncated transfer

# what the source-health registry needs (read by tools/build_source_registry.py).
SOURCE_META = {
    "source_id": "cro_financial_statements",
    "name": "CRO financial-statements filing index",
    "owner_module": "cro_financial_statements_extract",
    "ckan_base": CKAN_BASE,
    "package_id": PACKAGE_ID,
    # health = age of the silver we hold; upstream updates only a few times/year,
    # and this is not yet wired into a gold chain, so the threshold is generous.
    "silver_pattern": "data/silver/cro/financial_statements.parquet",
    "stale_after_days": 200,
}

_YEAR_RE = re.compile(r"(20\d\d)")


class SourceDrift(Exception):
    """CKAN structure or CSV schema no longer matches."""


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def _parse_ckan_date(raw: str | None) -> str | None:
    """Normalise a CKAN timestamp to an ISO date string (or None)."""
    if not raw:
        return None
    with contextlib.suppress(ValueError):
        return datetime.fromisoformat(raw).date().isoformat()
    return raw[:10]


def resolve_resources(session: requests.Session) -> list[dict]:
    """Return [{year, url, last_modified}] for each CSV resource in the package."""
    r = session.get(f"{CKAN_BASE}/api/3/action/package_show", params={"id": PACKAGE_ID}, timeout=60)
    r.raise_for_status()
    body = r.json()
    if not body.get("success"):
        raise SourceDrift(f"package_show(id={PACKAGE_ID}) success=false")
    out = []
    for res in body.get("result", {}).get("resources", []):
        if (res.get("format") or "").upper() != "CSV":
            continue
        m = _YEAR_RE.search(res.get("name") or "") or _YEAR_RE.search(res.get("url") or "")
        if not m:
            raise SourceDrift(f"cannot derive year from resource {res.get('name')!r}")
        out.append(
            {
                "year": int(m.group(1)),
                "url": res.get("url"),
                "last_modified": _parse_ckan_date(res.get("last_modified") or res.get("created")),
            }
        )
    if not out:
        raise SourceDrift("no CSV resources in financial-statements package")
    return sorted(out, key=lambda d: d["year"])


def download_csv(session: requests.Session, url: str, dest: Path) -> int:
    """Stream the (redirecting) CSV to dest. Returns bytes written."""
    written = 0
    with session.get(url, stream=True, allow_redirects=True, timeout=(15, 600)) as r:
        r.raise_for_status()
        tmp = dest.with_suffix(".csv.partial")
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                if chunk:
                    f.write(chunk)
                    written += len(chunk)
        tmp.replace(dest)
    return written


def normalise(frames: list[pl.DataFrame]) -> pl.DataFrame:
    """Concatenate yearly frames into the silver event log. Faithful: keeps every
    filing row (no dedup). Types company_num, parses dates, adds period_year."""
    df = pl.concat(frames, how="vertical_relaxed")
    missing = EXPECTED_COLUMNS - set(df.columns)
    if missing:
        raise SourceDrift(f"index missing expected columns: {sorted(missing)}")
    date_exprs = [
        pl.col(c).cast(pl.Utf8).str.slice(0, 10).str.strptime(pl.Date, format="%Y-%m-%d", strict=False).alias(c)
        for c in DATE_COLS
    ]
    df = df.with_columns(
        pl.col("company_num").cast(pl.Int64, strict=False),
        pl.col("company_name").cast(pl.Utf8).str.strip_chars(),
        *date_exprs,
    ).rename({"submissions_accounts_to_date": "accounts_period_end"})
    df = df.with_columns(pl.col("accounts_period_end").dt.year().alias("period_year"))
    return df.select(
        [
            "company_num",
            "company_name",
            "submission_num",
            "file_name",
            "submission_rec_date",
            "submission_eff_date",
            "submission_reg_date",
            "accounts_period_end",
            "period_year",
        ]
    )


def _coverage(df: pl.DataFrame, resources: list[dict], bronze_rows: dict[int, int]) -> dict:
    by_year = df.group_by("period_year").agg(pl.len().alias("rows")).sort("period_year").to_dicts()
    return {
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source": f"{CKAN_BASE}/dataset/{PACKAGE_ID}",
        "note": "CRO financial-statements FILING INDEX (metadata only; PDFs paywalled). "
        "See doc/SOURCES.md.",
        "resources": [
            {"year": r["year"], "last_modified": r["last_modified"], "bronze_rows": bronze_rows.get(r["year"])}
            for r in resources
        ],
        "silver_rows": df.height,
        "distinct_companies": int(df["company_num"].n_unique()),
        "rows_by_period_year": by_year,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--force", action="store_true", help="re-download every year")
    args = ap.parse_args()

    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    session = _session()
    try:
        resources = resolve_resources(session)
    except (requests.RequestException, SourceDrift) as e:
        print(f"[cro_fs] resolve failed: {e}", file=sys.stderr)
        return 2 if isinstance(e, SourceDrift) else 1

    held = {}
    if COVERAGE_OUT.exists():
        with contextlib.suppress(Exception):
            for r in orjson.loads(COVERAGE_OUT.read_bytes()).get("resources", []):
                held[r["year"]] = r.get("last_modified")

    bronze_rows: dict[int, int] = {}
    for res in resources:
        bronze = BRONZE_DIR / f"financial_statements_{res['year']}.csv"
        fresh = bronze.exists() and held.get(res["year"]) == res["last_modified"]
        if fresh and not args.force:
            print(f"[cro_fs] {res['year']}: current (last_modified={res['last_modified']}), reuse bronze")
        else:
            nbytes = download_csv(session, res["url"], bronze)
            print(f"[cro_fs] {res['year']}: downloaded {nbytes / 1e6:.1f} MB -> {bronze.name}")

    frames = []
    for res in resources:
        bronze = BRONZE_DIR / f"financial_statements_{res['year']}.csv"
        f = pl.read_csv(bronze, infer_schema_length=20_000)
        bronze_rows[res["year"]] = f.height
        frames.append(f)

    df = normalise(frames)
    if df.height < MIN_TOTAL_ROWS:
        print(f"[cro_fs] row floor not met: {df.height} < {MIN_TOTAL_ROWS}", file=sys.stderr)
        return 2

    save_parquet(df, SILVER_OUT)
    cov = _coverage(df, resources, bronze_rows)
    COVERAGE_OUT.write_bytes(orjson.dumps(cov, option=orjson.OPT_INDENT_2))

    print(
        f"[cro_fs] wrote {SILVER_OUT.relative_to(config.PROJECT_ROOT)}  "
        f"rows={df.height:,}  distinct_companies={cov['distinct_companies']:,}"
    )
    for r in cov["rows_by_period_year"]:
        print(f"    period {r['period_year']}: {r['rows']:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
