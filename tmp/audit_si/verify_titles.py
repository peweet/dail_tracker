"""Post-fix verifier for the SI title cleanup.

Runs after iris_oifigiuil_etl_polars.py + si_entity_enrichment.py.
Reports:
  - how many titles still look ALL CAPS  (P0-2)
  - how many titles still carry preamble keywords  (P0-1)
  - sampled diffs vs the pre-fix state (loaded from
    data/gold/parquet/statutory_instruments.parquet.pre-fix.bak if present)
  - the four canonical regressions:
      2026-114, 2026-115, 2026-116  (Statistics, Taoiseach-signed)
      2026-117                      (Criminal Justice, ALL CAPS bleed)
"""
from __future__ import annotations
from pathlib import Path
import pandas as pd

PARQUET = Path("C:/Users/pglyn/PycharmProjects/dail_extractor/data/gold/parquet/statutory_instruments.parquet")

PREAMBLE_KEYWORDS = [
    " The Minister ",
    " The Taoiseach,",
    " The Government,",
    " in exercise of the powers ",
    " I, ",
    " WHEREAS ",
]


def is_all_caps(t: str) -> bool:
    if not isinstance(t, str) or not t:
        return False
    letters = [c for c in t if c.isalpha()]
    if not letters:
        return False
    return sum(1 for c in letters if c.isupper()) / len(letters) >= 0.8


def has_preamble(t: str) -> bool:
    if not isinstance(t, str):
        return False
    return any(kw.lower() in t.lower() for kw in PREAMBLE_KEYWORDS)


def main() -> None:
    if not PARQUET.exists():
        print(f"MISSING: {PARQUET}")
        return
    df = pd.read_parquet(PARQUET)
    total = len(df)
    n_caps = int(df["si_title"].apply(is_all_caps).sum())
    n_preamble = int(df["si_title"].apply(has_preamble).sum())
    print(f"Total SIs: {total:,}")
    print(f"  ALL-CAPS titles: {n_caps} ({n_caps/total:.1%})")
    print(f"  Preamble-bleeding titles: {n_preamble} ({n_preamble/total:.1%})")
    print()

    print("=== Canonical regressions ===")
    for sid in ("2026-114", "2026-115", "2026-116", "2026-117", "2026-071"):
        row = df[df["si_id"] == sid]
        if row.empty:
            print(f"  {sid}: NOT FOUND")
            continue
        t = row["si_title"].iloc[0]
        caps = "CAPS!" if is_all_caps(t) else "ok-case"
        pre = "PREAMBLE!" if has_preamble(t) else "ok-len"
        print(f"  {sid}  [{caps}/{pre}]  {t[:120]}{'...' if len(t) > 120 else ''}")
    print()

    print("=== 10 longest titles (preamble-bleed canaries) ===")
    df["_len"] = df["si_title"].astype(str).str.len()
    for _, row in df.nlargest(10, "_len").iterrows():
        print(f"  {row['si_id']}  ({row['_len']} chars)  {row['si_title'][:120]}...")
    print()

    print("=== Random sample of 10 cleaned titles ===")
    for _, row in df.sample(n=min(10, total), random_state=42).iterrows():
        print(f"  {row['si_id']}  {row['si_title'][:120]}")


if __name__ == "__main__":
    main()
