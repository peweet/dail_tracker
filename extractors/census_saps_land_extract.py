"""Census 2022 SAPS — small-area / LEA / county landings.

Lands the already-downloaded Census 2022 Small Area Population Statistics CSVs
as parquet, with light type cleanup. Each row = one geography × 795 variables.

Reads  : doc/source_pdfs/_samples/SAPS_2022_SA.csv
         doc/source_pdfs/_samples/SAPS_2022_LEA.csv
         doc/source_pdfs/_samples/SAPS_2022_county.csv
Writes : data/gold/parquet/census_saps_<level>.parquet
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import polars as pl

try:  # noqa: SIM105
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))  # repo root, for first-party imports
_SAMPLES = _ROOT / "doc" / "source_pdfs" / "_samples"
_OUT = _ROOT / "data" / "gold" / "parquet"

from services.parquet_io import save_parquet  # noqa: E402

LEVELS = {
    "small_area": "SAPS_2022_SA.csv",
    "lea": "SAPS_2022_LEA.csv",
    "county": "SAPS_2022_county.csv",
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()

    for level, fname in LEVELS.items():
        src = _SAMPLES / fname
        if not src.exists():
            print(f"[skip] {fname} missing")
            continue
        # SAPS CSVs are large (~40MB for SA); read with infer_schema_length=0 to
        # avoid expensive type-inference, then trust string columns.
        try:
            df = pl.read_csv(src, infer_schema_length=2000)
        except Exception:
            # Latin-1 fallback for older council CSVs
            try:
                decoded = src.read_bytes().decode("latin-1").encode("utf-8")
                from io import BytesIO

                df = pl.read_csv(BytesIO(decoded), infer_schema_length=2000)
            except Exception as e:
                print(f"[{level}] FETCH FAIL: {e}")
                continue
        print(f"=== census_saps_{level} — {len(df):,} rows × {len(df.columns)} cols ===")
        if args.write:
            out = _OUT / f"census_saps_{level}.parquet"
            save_parquet(df, out)
            print(f"  Wrote {out.relative_to(_ROOT)} ({out.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
