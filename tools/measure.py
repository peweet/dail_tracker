"""Direct data-quality measurement over the built warehouse.

Imports ONLY polars to stay immune to the venv's broken requests/simplejson
chain. Reports row counts, unique_member_code null/coverage, and duplicate
rates on the tables that feed the cross-source member profile.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parent.parent
SILVER_P = ROOT / "data" / "silver" / "parquet"
SILVER_LOB_P = ROOT / "data" / "silver" / "lobbying" / "parquet"
GOLD_P = ROOT / "data" / "gold" / "parquet"
QUAR = ROOT / "data" / "silver" / "_quarantine"


def line(s=""):
    print(s, flush=True)


def scan_dir(label: str, d: Path):
    line(f"\n=== {label}  ({d.relative_to(ROOT)}) ===")
    if not d.exists():
        line("  (missing)")
        return
    for f in sorted(d.glob("*.parquet")):
        try:
            df = pl.read_parquet(f)
            n, c = df.shape
            extra = ""
            if "unique_member_code" in df.columns:
                col = df["unique_member_code"]
                nn = col.null_count()
                blank = (col.cast(pl.Utf8).str.strip_chars() == "").sum() if n else 0
                missing = nn + (blank or 0)
                cov = 100 * (n - missing) / n if n else 0
                extra = f" | umc: {cov:5.1f}% filled ({missing} null/blank)"
            line(f"  {n:>8,} rows x {c:>3} cols  {f.name}{extra}")
        except Exception as e:  # noqa: BLE001
            line(f"  ERROR reading {f.name}: {e}")


def dup_check(path: Path, keys: list[str], label: str):
    if not path.exists():
        line(f"\n[dup] {label}: file missing")
        return
    df = pl.read_parquet(path)
    keys = [k for k in keys if k in df.columns]
    if not keys:
        line(f"\n[dup] {label}: none of key cols present ({df.columns[:6]}…)")
        return
    total = df.height
    uniq = df.select(keys).unique().height
    line(
        f"\n[dup] {label} on {keys}: {total:,} rows, {uniq:,} unique, "
        f"{total - uniq:,} dup rows ({100 * (total - uniq) / total:.2f}%)"
    )


def member_code_distinct():
    fm = SILVER_P / "flattened_members.parquet"
    if not fm.exists():
        return
    df = pl.read_parquet(fm)
    if "unique_member_code" in df.columns:
        codes = df["unique_member_code"]
        line(
            f"\n[members] flattened_members: {df.height} rows, "
            f"{codes.n_unique()} distinct unique_member_code, "
            f"{codes.null_count()} null"
        )


def quarantine_report():
    line("\n=== QUARANTINE (rejected rows) ===")
    if not QUAR.exists():
        line("  (no quarantine dir)")
        return
    for f in sorted(QUAR.glob("*.parquet")):
        try:
            df = pl.read_parquet(f)
            rule = df["_quarantine_rule"][0] if "_quarantine_rule" in df.columns and df.height else "?"
            line(f"  {df.height:>7,} rows  rule={rule!r:<40} {f.name[:55]}")
        except Exception as e:  # noqa: BLE001
            line(f"  ERROR {f.name}: {e}")


if __name__ == "__main__":
    scan_dir("SILVER parquet", SILVER_P)
    scan_dir("SILVER lobbying parquet", SILVER_LOB_P)
    scan_dir("GOLD parquet", GOLD_P)
    member_code_distinct()
    # Join-key integrity on the cross-source member tables
    dup_check(GOLD_P / "payments_full_psa.parquet", ["unique_member_code", "year"], "payments_full_psa")
    dup_check(
        SILVER_P / "td_attendance_fact_table.parquet", ["unique_member_code", "year", "period"], "attendance_fact"
    )
    dup_check(GOLD_P / "current_dail_vote_history.parquet", ["vote_id", "unique_member_code"], "vote_history")
    dup_check(SILVER_P / "flattened_members.parquet", ["unique_member_code"], "flattened_members")
    quarantine_report()
    line("\n[done]")
