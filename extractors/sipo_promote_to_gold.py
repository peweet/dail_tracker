"""Promote the SIPO GE2024 silver facts → gold (an extractors/ script that writes
data/gold/parquet/, the sanctioned medallion pattern like the other extractors).

Promotes BOTH tracks:
  * donations    — data/silver/sipo/sipo_donations_fact.parquet → sipo_donations.parquet
  * expenses     — data/silver/sipo/sipo_expenses_fact.parquet  → sipo_expenses_fact.parquet
                   (the Part-3 candidate-summary fact the v_sipo_expenses_base view reads)
  * categories   — data/silver/sipo/sipo_expense_categories_fact.parquet → sipo_expense_categories.parquet
  * items        — data/silver/sipo/sipo_expense_items_fact.parquet → sipo_expense_items.parquet
                   (Part-4 national-agent itemised spend, consumed by v_sipo_party_national_*)

⚠️ PRIVACY (non-negotiable): gold/parquet/ is COMMITTED to the public repo. Donor
NAMES + AMOUNTS are the public SIPO record and may be promoted; donor HOME ADDRESSES
are NOT — `donor_address_raw` is DROPPED here so it can never reach git or the UI.
No-inference: gold carries figures + flags only.

Run:  ./.venv/Scripts/python.exe extractors/sipo_promote_to_gold.py
"""

from __future__ import annotations

import contextlib
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from services.data_contracts import ColumnRule, enforce_contract  # noqa: E402
from services.parquet_io import save_parquet  # noqa: E402

# The elections we hold validated OCR ground-truth for. A gold fact carrying any
# other tag was built from a source we have not checked — that is drift, not data.
SIPO_ELECTION_EVENTS = frozenset({"GE2024"})


def _guard_sipo(df: pl.DataFrame, *, name: str, money_cols: tuple[str, ...]) -> None:
    """Runtime drift gate run BEFORE the gold write (mirrors the inline PII guard below).

    Pure-Polars via services.data_contracts — no test-only dependency, so it imports
    cleanly on the Cloud core-deps install. These facts are OCR-derived, so it halts
    the promote when the silent failure mode shows up:
      * a negative euro figure (an OCR sign / column-misalignment parse error);
      * an ``election_event`` outside the validated set (an unverified source).
    Anchored 2026-06-27: passes on all current gold (0 negatives, election_event=GE2024).
    """
    negatives = {c: df.filter(pl.col(c) < 0).height for c in money_cols if c in df.columns}
    negatives = {c: n for c, n in negatives.items() if n}
    if negatives:
        raise RuntimeError(f"{name}: negative euro values {negatives} — OCR sign/parse error, refusing to promote")
    rules = (ColumnRule("election_event", SIPO_ELECTION_EVENTS, "hard"),) if "election_event" in df.columns else ()
    enforce_contract(df, name=name, rules=rules, required_columns=("party",), nonnull_columns=()).raise_if_failed()

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

SILVER = ROOT / "data/silver/sipo"
GOLD = ROOT / "data/gold/parquet"

# Canonicalise party names to the registry/expenses spelling (donations forms use
# verbose legal names + an OCR all-caps; normalise so the two facts join cleanly).
PARTY_CANON = {
    "the labour party": "Labour",
    "people before profit - solidarity": "People Before Profit/Solidarity",
    "the green party / an comhaontas glas": "Green Party",
    "social democrats": "Social Democrats",
    "sinn féin": "Sinn Féin",
    "aontú": "Aontú",
    "fianna fáil": "Fianna Fáil",
    "fine gael": "Fine Gael",
}


def canon_party(name: str | None) -> str | None:
    if not name:
        return None
    return PARTY_CANON.get(name.strip().lower(), name.strip())


def address_columns(df: pl.DataFrame) -> list[str]:
    """Columns that look like they carry a postal address (PII). Donor NAMES + AMOUNTS
    are the public SIPO record and stay; only addresses are dropped/guarded. Name-based
    so a renamed/extra address column (not just donor_address_raw) is still caught."""
    return [c for c in df.columns if "address" in c.lower()]


def promote_donations() -> None:
    src = SILVER / "sipo_donations_fact.parquet"
    if not src.exists():
        print(f"  !! no donations fact at {src}")
        return
    df = pl.read_parquet(src)
    # DROP donor_address_raw (PII) + any address-bearing columns before gold.
    drop = address_columns(df)
    df = df.drop(drop)
    df = df.with_columns(
        pl.col("party").map_elements(canon_party, return_dtype=pl.Utf8).alias("party"),
        pl.lit("GE2024").alias("election_event"),
    )
    # PRIVACY INVARIANT (runtime, -O-proof; BEFORE the write): no address column may reach
    # committed gold. The old assert was -O-strippable AND ran post-write — both defeated it.
    leaked = address_columns(df)
    if leaked:
        raise RuntimeError(f"PII leak: address column(s) {leaked} must not reach gold")
    GOLD.mkdir(parents=True, exist_ok=True)
    out = GOLD / "sipo_donations.parquet"
    _guard_sipo(df, name="sipo_donations", money_cols=("value_eur",))
    save_parquet(df, out)
    print(f"  donations -> {out.relative_to(ROOT)}  ({df.height} rows, address dropped: {bool(drop)})")
    print("  parties:", sorted(df["party"].unique().to_list()))
    print("  columns:", df.columns)


def promote_expenses() -> None:
    """Part-3 candidate-summary expenses → gold. No PII (candidate names + amounts
    are the public SIPO record); the schema the v_sipo_expenses_base view selects is
    carried through unchanged. Party names are already canonical (from PARTY_JOBS)."""
    src = SILVER / "sipo_expenses_fact.parquet"
    if not src.exists():
        print(f"  !! no expenses fact at {src}")
        return
    df = pl.read_parquet(src)
    GOLD.mkdir(parents=True, exist_ok=True)
    out = GOLD / "sipo_expenses_fact.parquet"
    _guard_sipo(
        df,
        name="sipo_expenses_fact",
        money_cols=("expenditure_eur", "amount_assigned_eur", "statutory_limit_eur"),
    )
    save_parquet(df, out)
    blank = df.filter(pl.col("candidate_name_raw").str.strip_chars() == "").height
    print(f"  expenses  -> {out.relative_to(ROOT)}  ({df.height} rows, {blank} blank-name)")
    print("  parties:", sorted(df["party"].unique().to_list()))


def promote_expense_categories() -> None:
    """Part-4 national-agent CATEGORY TOTALS → gold. The 8 statutory headings
    (4A–4H) + the Overall total, per party, off the return's "Expenses Review"
    page. No PII (party-level figures only). `category_total_eur` is the printed
    official figure; `items_sum_eur`/`reconciles` flag where our line-item
    extraction is incomplete for a heading. Consumed by v_sipo_party_national_*."""
    src = SILVER / "sipo_expense_categories_fact.parquet"
    if not src.exists():
        print(f"  !! no Part-4 categories fact at {src}")
        return
    df = pl.read_parquet(src).with_columns(
        pl.col("party").map_elements(canon_party, return_dtype=pl.Utf8).alias("party"),
        pl.lit("GE2024").alias("election_event"),
    )
    GOLD.mkdir(parents=True, exist_ok=True)
    out = GOLD / "sipo_expense_categories.parquet"
    _guard_sipo(df, name="sipo_expense_categories", money_cols=("category_total_eur", "items_sum_eur"))
    save_parquet(df, out)
    print(f"  categories-> {out.relative_to(ROOT)}  ({df.height} rows)")
    print("  parties:", sorted(df["party"].unique().to_list()))


def promote_expense_items() -> None:
    """Part-4 national-agent LINE ITEMS → gold. One row per Ref (A1, A10, …):
    section, category, ref, item_description, cost. No PII (these are the party's
    own supplier/expense lines, the public SIPO record). `flag` marks OCR rows to
    verify. Consumed by v_sipo_party_national_items."""
    src = SILVER / "sipo_expense_items_fact.parquet"
    if not src.exists():
        print(f"  !! no Part-4 items fact at {src}")
        return
    df = pl.read_parquet(src).with_columns(
        pl.col("party").map_elements(canon_party, return_dtype=pl.Utf8).alias("party"),
        pl.lit("GE2024").alias("election_event"),
    )
    GOLD.mkdir(parents=True, exist_ok=True)
    out = GOLD / "sipo_expense_items.parquet"
    _guard_sipo(df, name="sipo_expense_items", money_cols=("cost_eur",))
    save_parquet(df, out)
    print(f"  items     -> {out.relative_to(ROOT)}  ({df.height} rows)")
    print("  parties:", sorted(df["party"].unique().to_list()))


def main() -> None:
    print("=== PROMOTE SIPO silver -> gold ===")
    promote_donations()
    promote_expenses()
    promote_expense_categories()
    promote_expense_items()
    print("done.")


if __name__ == "__main__":
    main()
