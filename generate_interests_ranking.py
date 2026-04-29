"""
generate_interests_ranking.py
------------------------------
Pipeline script — produces data/gold/parquet/interests_member_ranking.parquet.
One row per (member_name, house, year_declared).

All aggregation lives here, NOT in Streamlit.
Run:  python generate_interests_ranking.py
"""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from config import GOLD_PARQUET_DIR, SILVER_DIR

# ── Source paths ──────────────────────────────────────────────────────────────
_SILVER: dict[str, Path] = {
    "Dáil":   SILVER_DIR / "dail_member_interests_combined.csv",
    "Seanad": SILVER_DIR / "seanad_member_interests_combined.csv",
}

_OUTPUT = GOLD_PARQUET_DIR / "interests_member_ranking.parquet"

# ── Category → field mapping ──────────────────────────────────────────────────
_CATEGORY_COUNTERS: dict[str, str] = {
    "Directorships":                                   "directorship_count",
    "Land (including property)":                       "property_count",
    "Shares":                                          "share_count",
    "Gifts":                                           "gift_count",
    "Travel Facilities":                               "travel_count",
    "Contracts":                                       "contract_count",
    "Occupations":                                     "occupation_count",
    "Remunerated Position":                            "remunerated_count",
    "Property supplied or lent or a Service supplied": "service_count",
}

# Patterns that make a highlight snippet more titillating
_LOCATION_RE = re.compile(
    r"\b(France|Spain|Italy|Portugal|UK|Switzerland|Evian|Monaco|Marbella|"
    r"Cannes|Tuscany|Algarve|Ibiza|Majorca|Mallorca|Tenerife|Paris|London|"
    r"abroad|overseas|foreign|holiday home|villa|apartment|penthouse|"
    r"hotel|resort|chalet)\b",
    re.IGNORECASE,
)
_MONEY_RE = re.compile(r"[€$£]\s*\d|[\d,]+\s*(?:euro|dollars?|pounds?)", re.IGNORECASE)


def _score_highlight(text: str) -> int:
    """Higher score = more interesting headline material."""
    if not text or text.lower() in ("no interests declared", "nan"):
        return -1
    s = 0
    s += len(text) // 20          # longer = more specific
    if _LOCATION_RE.search(text):
        s += 50                   # foreign / notable location
    if _MONEY_RE.search(text):
        s += 20
    if "letting" in text.lower():
        s += 10
    if "holiday" in text.lower():
        s += 15
    if "landlord" in text.lower():
        s += 10
    return s


def _best_highlight(texts: pd.Series) -> str:
    """Pick the single most interesting declaration text for a member-year."""
    valid = [
        t for t in texts.dropna().tolist()
        if str(t).strip().lower() not in ("no interests declared", "", "nan")
    ]
    if not valid:
        return ""
    scored = sorted(valid, key=_score_highlight, reverse=True)
    raw = scored[0]
    # Trim to ~160 chars at word boundary
    if len(raw) > 160:
        raw = raw[:157].rsplit(" ", 1)[0] + "…"
    return raw


def _load_house(house: str) -> pd.DataFrame:
    path = _SILVER[house]
    if not path.exists():
        print(f"  SKIP {house}: file not found at {path}")
        return pd.DataFrame()
    df = pd.read_csv(path, low_memory=False)
    df.columns = df.columns.str.strip()

    # Normalise column names to contract shape
    # Drop Irish-language constituency column to avoid duplicate after rename
    if "constituency" in df.columns and "constituency_name" in df.columns:
        df = df.drop(columns=["constituency"])
    df = df.rename(columns={
        "full_name":                    "member_name",
        "party":                        "party_name",
        "constituency_name":            "constituency",
        "year_declared":                "declaration_year",
        "interest_description_cleaned": "interest_text",
    })
    for csv_col, contract_col in [
        ("is_landlord",       "landlord_flag"),
        ("is_property_owner", "property_flag"),
    ]:
        if csv_col in df.columns:
            df[contract_col] = df[csv_col].astype(str).str.lower() == "true"
        elif contract_col not in df.columns:
            df[contract_col] = False

    if "declaration_year" in df.columns:
        df["declaration_year"] = pd.to_numeric(df["declaration_year"], errors="coerce")
    df = df[df["interest_category"] != "15"]

    df["house"] = house
    return df


def _build_ranking(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate one row per (member_name, house, declaration_year).
    All GROUP BY lives here — never in Streamlit.
    """
    is_real = (
        df["interest_text"].notna()
        & ~df["interest_text"].str.strip().str.lower().isin(["no interests declared", "", "nan"])
    )
    real_df = df[is_real].copy()

    rows = []
    for (name, house, year), grp in df.groupby(
        ["member_name", "house", "declaration_year"], sort=False
    ):
        real_grp = real_df[
            (real_df["member_name"] == name)
            & (real_df["house"] == house)
            & (real_df["declaration_year"] == year)
        ]

        # Per-category counts
        cat_counts: dict[str, int] = {}
        for cat, field in _CATEGORY_COUNTERS.items():
            cat_counts[field] = int(
                real_grp[real_grp["interest_category"] == cat].shape[0]
            )

        info = grp.iloc[0]
        rows.append({
            "member_name":        name,
            "house":              house,
            "party_name":         str(info["party_name"]) if pd.notna(info["party_name"]) else "",
            "constituency":       str(info["constituency"]) if pd.notna(info["constituency"]) else "",
            "declaration_year":   int(year),
            "total_declarations": len(real_grp),
            "is_landlord":        bool(grp["landlord_flag"].any()),
            "is_property_owner":  bool(grp["property_flag"].any()),
            "top_interest":       _best_highlight(real_grp["interest_text"]),
            **cat_counts,
        })

    if not rows:
        return pd.DataFrame()

    out = pd.DataFrame(rows)

    # Rank within (house, year) by total_declarations DESC
    out["rank"] = (
        out.groupby(["house", "declaration_year"])["total_declarations"]
        .rank(method="min", ascending=False)
        .astype(int)
    )
    out = out.sort_values(
        ["house", "declaration_year", "rank"], ascending=[True, False, True]
    ).reset_index(drop=True)
    return out


def main() -> None:
    GOLD_PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    frames = []
    for house in ("Dáil", "Seanad"):
        print(f"Loading {house}…")
        df = _load_house(house)
        if not df.empty:
            frames.append(df)

    if not frames:
        print("No data loaded — aborting.")
        return

    combined = pd.concat(frames, ignore_index=True)
    print(f"Total rows loaded: {len(combined):,}")

    ranking = _build_ranking(combined)
    print(f"Ranking rows: {len(ranking):,}")
    print(ranking[["member_name", "house", "declaration_year", "rank", "total_declarations"]].head(10).to_string())

    ranking.to_parquet(_OUTPUT, index=False)
    print(f"\nWritten → {_OUTPUT}")


if __name__ == "__main__":
    main()
