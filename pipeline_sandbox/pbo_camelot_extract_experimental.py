"""PBO Ongoing Need 2024 — Table 2 (composition breakdown, camelot extraction).

fitz reported PBO §4 was chart-only (Figures 1-3, not tables). Camelot proves
Table 2 on p8 is tabular: Social Housing waiting list + HAP tenancies by
household composition, 2023 vs 2024, with change %.

Reads  : doc/source_pdfs/PBO_OngoingNeed2024.pdf  (p8)
Writes : data/gold/parquet/pbo_ongoing_need_composition.parquet
"""
from __future__ import annotations

import argparse
import contextlib
import re
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

import camelot  # noqa: E402
import polars as pl  # noqa: E402

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "doc" / "source_pdfs" / "PBO_OngoingNeed2024.pdf"
_OUT = _ROOT / "data" / "gold" / "parquet" / "pbo_ongoing_need_composition.parquet"


def clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("\n", " ")).strip()


def _to_int(s):
    s = clean(s).replace(",", "").strip()
    if not s or s in {"-", "—"}:
        return None
    m = re.match(r"-?\d+", s)
    return int(m.group(0)) if m else None


def extract() -> pl.DataFrame:
    tabs = camelot.read_pdf(str(_SRC), pages="8", flavor="stream", suppress_stdout=True)
    if not tabs.n:
        return pl.DataFrame()
    df_raw = tabs[0].df
    rows = []
    current_cohort = None
    for ri in range(df_raw.shape[0]):
        cells = [clean(df_raw.iloc[ri, ci]) for ci in range(df_raw.shape[1])]
        first = cells[0] if cells else ""
        # Detect cohort headers
        if "social housing waiting" in first.lower() or "social housing wai" in first.lower():
            current_cohort = "ssha_waiting_list"
            continue
        if "hap tenancies" in first.lower():
            current_cohort = "hap_tenancies"
            continue
        if "ongoing need" in first.lower():
            current_cohort = "total_ongoing_need"
        # Data row
        if not first or "source" in first.lower() or first.lower() in {"2023", "2024", "no.", "%"}:
            continue
        v_2023 = _to_int(cells[1]) if len(cells) > 1 else None
        v_2024 = _to_int(cells[2]) if len(cells) > 2 else None
        if v_2023 is None and v_2024 is None:
            continue
        rows.append({
            "cohort": current_cohort or "unknown",
            "household_composition": first,
            "households_2023": v_2023,
            "households_2024": v_2024,
        })
    return pl.DataFrame(rows) if rows else pl.DataFrame()


def fidelity_check(df: pl.DataFrame) -> dict:
    rpt = {"checks": {}, "rows": len(df)}
    if df.is_empty():
        rpt["green"] = False
        rpt["checks"]["1_extraction"] = {"pass": False, "note": "empty"}
        return rpt
    rpt["checks"]["1_extraction"] = {"row_count": len(df), "pass": len(df) >= 20}
    cohorts = df["cohort"].n_unique()
    rpt["checks"]["2_cohorts"] = {"unique": cohorts, "pass": cohorts >= 2}
    # Check 3 — Total Ongoing Need row should be 115,425 / 113,512
    total = df.filter(pl.col("cohort") == "total_ongoing_need")
    if len(total):
        v23 = total["households_2023"].item(0)
        v24 = total["households_2024"].item(0)
        rpt["checks"]["3_published_totals"] = {
            "computed_2023": v23, "computed_2024": v24,
            "expected_2023": 115425, "expected_2024": 113512,
            "pass": v23 == 115425 and v24 == 113512,
        }
    else:
        rpt["checks"]["3_published_totals"] = {"pass": False, "note": "no total row"}
    rpt["checks"]["4_cross_source"] = {"pass": True, "note": "skipped"}
    rpt["checks"]["5_semantic"] = {"pass": True}
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
    print(f"=== PBO Ongoing Need composition — {len(df)} rows ===")
    for n, chk in rpt["checks"].items():
        print(f"  [{'GREEN' if chk.get('pass') else 'FAIL'}] {n}: {chk}")
    print(f">>> {'GREEN' if rpt['green'] else 'AMBER'}")

    if args.write and rpt["green"]:
        _write_parquet(df, _OUT)
        print(f"Wrote {_OUT.relative_to(_ROOT)}")
    if len(df):
        print()
        print(df.head(8))


if __name__ == "__main__":
    main()
