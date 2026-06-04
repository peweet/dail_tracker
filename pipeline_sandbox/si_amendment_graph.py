"""SI amendment/revocation graph — what we can show NOW from si_current_state.

si_current_state already records, for each SI, the REVERSE edges: which later SIs
amended/revoked it (affecting_sis + how_affected_raw, with provision detail). Two
things are derivable with zero new sourcing:

  DIR 1 (already have): "This SI was amended/revoked BY ..."   — read directly.
  DIR 2 (new, by inversion): "This SI amends/revokes ..."      — invert the edges.

DIR 2 is the genuine new win: today an SI page can say "I was amended by X" but
not "I amend X". Inverting affecting_sis gives the forward direction for free.

This is SI->SI amendments (internal, complete). SI->Act textual amendments
(F/C/E notes) would need the LRC Revised Acts source = PR3 = out of scope.

Read-only except a sandbox demo parquet. Gold untouched.
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

STATE = ROOT / "data/gold/parquet/si_current_state.parquet"
GOLD = ROOT / "data/gold/parquet/statutory_instruments.parquet"
OUT = ROOT / "pipeline_sandbox/_lrc_output/si_amendment_forward_edges.parquet"


def hr(t):
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def main():
    s = pl.read_parquet(STATE).unique(subset=["si_year", "si_number"])
    g = pl.read_parquet(GOLD).select("si_year", "si_number", "si_title")

    # effect verb from current_state (the effect ON the affected SI)
    EFFECT = {
        "revoked": "revokes",
        "partially_revoked": "partially revokes",
        "amended": "amends",
        "amended_and_partially_revoked": "amends/partially revokes",
        "other_affected": "affects",
    }
    s = s.with_columns(
        pl.col("current_state").replace_strict(EFFECT, default=None).alias("effect"),
        # short provision note = the part of how_affected_raw before the "||"
        pl.col("how_affected_raw").str.split(" || ").list.first().alias("provision_note"),
    )

    affected = (
        s.filter(pl.col("affecting_sis").list.len() > 0)
        .select("si_year", "si_number", "effect", "provision_note", "affecting_sis")
        .explode("affecting_sis")
        .rename({"affecting_sis": "amender_key"})
        .with_columns(
            pl.col("amender_key").str.extract(r"^(\d+)/", 1).cast(pl.Int64).alias("amender_number"),
            pl.col("amender_key").str.extract(r"/(\d+)$", 1).cast(pl.Int64).alias("amender_year"),
        )
    )

    hr("DIR 1 — 'This SI was AMENDED/REVOKED by ...' (already available)")
    by_state = affected.group_by("effect").len().sort("len", descending=True)
    print("affected-side edges by effect:")
    print(by_state)
    print(f"distinct SIs that have an 'affected by' story: "
          f"{affected.select('si_year','si_number').n_unique()}")

    hr("DIR 2 — 'This SI AMENDS/REVOKES ...' (NEW, by inversion)")
    # join amender -> its title; affected -> its title
    fwd = (
        affected.join(
            g.rename({"si_year": "amender_year", "si_number": "amender_number", "si_title": "amender_title"}),
            on=["amender_year", "amender_number"], how="left",
        )
        .join(
            g.rename({"si_title": "affected_title"}),
            on=["si_year", "si_number"], how="left",
        )
        .rename({"si_year": "affected_year", "si_number": "affected_number"})
    )
    in_gold = fwd.filter(pl.col("amender_title").is_not_null())
    print(f"forward edges total                 : {fwd.height}")
    print(f"  ...where the amender is in gold    : {in_gold.height}")
    print(f"distinct amending SIs (in gold)     : {in_gold.select('amender_year','amender_number').n_unique()}")
    # which SIs are the busiest amenders? (a 'most active instruments' angle)
    busiest = (
        in_gold.group_by("amender_year", "amender_number", "amender_title")
        .agg(pl.len().alias("n_affected"))
        .sort("n_affected", descending=True)
    )
    print("\nbusiest amending SIs (touch the most other SIs):")
    for r in busiest.head(8).iter_rows(named=True):
        print(f"  {r['amender_number']}/{r['amender_year']}  affects {r['n_affected']:3d}  | {str(r['amender_title'])[:50]}")

    hr("Concrete bidirectional example")
    # pick a busy amender, show what it revokes/amends
    top = busiest.head(1).to_dicts()[0]
    print(f"AMENDER: S.I. {top['amender_number']}/{top['amender_year']} — {top['amender_title'][:60]}")
    eg = in_gold.filter(
        (pl.col("amender_year") == top["amender_year"]) & (pl.col("amender_number") == top["amender_number"])
    ).select("effect", "affected_number", "affected_year", "affected_title", "provision_note")
    for r in eg.head(6).iter_rows(named=True):
        print(f"   {r['effect']} S.I. {r['affected_number']}/{r['affected_year']} "
              f"({str(r['provision_note'])[:30]}) — {str(r['affected_title'])[:42]}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    in_gold.select(
        "amender_year", "amender_number", "amender_title",
        "effect", "affected_year", "affected_number", "affected_title", "provision_note",
    ).write_parquet(OUT, compression="zstd", compression_level=3, statistics=True)
    print(f"\nwrote {OUT.relative_to(ROOT)} ({in_gold.height} forward edges)")


if __name__ == "__main__":
    main()
