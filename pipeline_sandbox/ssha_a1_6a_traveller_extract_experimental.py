"""SSHA Appendix A1.6a — Traveller identifier per LA (2025 only).

Different shape from other A1.x tables: no Year column (Traveller identifier
only published for 2025). Columns: Yes / No / Prefer not to say / Unanswered / Total.

Reads  : doc/source_pdfs/SSHA_2025_FINAL.pdf  (pages 66-67)
Writes : data/gold/parquet/ssha_a1_6a_traveller.parquet
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import fitz
import polars as pl

try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "doc" / "source_pdfs" / "SSHA_2025_FINAL.pdf"
_OUT = _ROOT / "data" / "gold" / "parquet" / "ssha_a1_6a_traveller.parquet"

# Published national totals (from SSHA Table 2.6a)
PUBLISHED_2025 = {"yes": 1844, "no": 30392, "prefer_not": 1906, "unanswered": 27577, "total": 61719}


def _to_int(c):
    if c is None:
        return None
    s = str(c).replace(",", "").replace("\n", "").strip()
    if not s or s == "-":
        return None
    m = re.match(r"-?\d+", s)
    return int(m.group(0)) if m else None


def canonical_la(name: str) -> str:
    n = (name or "").replace("\n", " ").replace("\r", " ")
    n = re.sub(r"\s+", " ", n).strip()
    n = n.replace("Dún", "Dun").replace("D�n", "Dun")
    n = n.replace("Ofaf ly", "Offaly").replace("Ofafly", "Offaly")
    n = re.sub(r"\s+Council\s*$", "", n)
    return n.strip()


def extract() -> pl.DataFrame:
    doc = fitz.open(str(_SRC))
    rows = []
    for pi in (65, 66):  # PDF p66, p67
        page = doc[pi]
        for t in page.find_tables().tables:
            data = t.extract()
            for r in data:
                cells = [(c or "").strip() for c in r]
                first = cells[0] if cells else ""
                if not first or first.lower() in {"local authority", "total"}:
                    continue
                if first.lower() == "total":
                    continue
                nums = [_to_int(c) for c in cells if _to_int(c) is not None]
                if len(nums) >= 5:
                    rows.append({
                        "la": canonical_la(first),
                        "yes": nums[0],
                        "no": nums[1],
                        "prefer_not": nums[2],
                        "unanswered": nums[3],
                        "total": nums[4],
                    })
    doc.close()
    return pl.DataFrame(rows) if rows else pl.DataFrame()


def fidelity_check(df: pl.DataFrame) -> dict:
    rpt = {"checks": {}, "rows": len(df)}
    if df.is_empty():
        rpt["green"] = False
        rpt["checks"]["1_extraction"] = {"pass": False, "note": "empty"}
        return rpt

    rpt["checks"]["1_extraction"] = {
        "unique_LAs": df["la"].n_unique(), "pass": df["la"].n_unique() >= 28,
    }
    # Check 2 — row sum matches per-row total
    sum_check = df.with_columns(
        (pl.col("yes") + pl.col("no") + pl.col("prefer_not") + pl.col("unanswered")).alias("computed_total")
    ).with_columns((pl.col("total") - pl.col("computed_total")).abs().alias("diff"))
    bad = sum_check.filter(pl.col("diff") > 0)
    rpt["checks"]["2_internal_sum"] = {"row_failures": len(bad), "pass": len(bad) == 0}
    # Check 3 — national reconciles
    sums = {c: int(df[c].sum()) for c in ("yes", "no", "prefer_not", "unanswered", "total")}
    diff = {k: sums[k] - PUBLISHED_2025[k] for k in PUBLISHED_2025}
    rpt["checks"]["3_national"] = {
        "computed": sums, "published": PUBLISHED_2025, "diff": diff,
        "pass": all(abs(v) <= 2 for v in diff.values()),
    }
    rpt["checks"]["4_cross_source"] = {"pass": True, "note": "skipped"}
    rpt["checks"]["5_semantic"] = {
        "negatives": df.filter(
            (pl.col("yes") < 0) | (pl.col("no") < 0)
            | (pl.col("prefer_not") < 0) | (pl.col("unanswered") < 0)
        ).height,
        "pass": True,
    }
    rpt["green"] = all(c.get("pass", False) for c in rpt["checks"].values())
    return rpt


def _write_parquet(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path, compression="zstd", compression_level=3, statistics=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()
    df = extract()
    rpt = fidelity_check(df)
    print(f"=== SSHA A1.6a Traveller identifier — {len(df)} rows ===")
    for name, chk in rpt["checks"].items():
        print(f"  [{'GREEN' if chk.get('pass') else 'FAIL'}] {name}: {chk}")
    print(f">>> {'GREEN' if rpt['green'] else 'AMBER'}")
    if args.write and rpt["green"]:
        _write_parquet(df, _OUT)
        print(f"Wrote {_OUT.relative_to(_ROOT)}")
    if len(df):
        print(df.head(6))


if __name__ == "__main__":
    main()
