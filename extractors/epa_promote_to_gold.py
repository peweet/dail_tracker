"""Promote the EPA supplier-compliance fact (sandbox → gold) — the medallion pattern
(an extractors/ script that writes data/gold/parquet/, like the other promotions).

Source: the vetted sandbox accountability view (pipeline_sandbox/epa_accountability_view.py),
which joins EPA licences + EPA enforcement to CRO-matched firms. This promotes ONE gold fact —
``epa_supplier_compliance.parquet`` — one row per CRO company: its EPA licence portfolio and its EPA
enforcement record, keyed on ``company_num`` so the company dossier can read it via a registered view.

What it deliberately does NOT carry: money. Public-money figures already live on the dossier (awards /
payments panels); re-stating them next to enforcement counts would imply a causal link the data does
not support (no-inference rule). This fact is licences + compliance only.

⚠️ PRIVACY (non-negotiable): gold/parquet/ is COMMITTED to the public repo. Company names + EPA
licence/enforcement records are the public EPA register. Sole-trader / named-individual licence holders
(``looks_individual``) are DROPPED here so a private person's regulatory record never reaches git or the
UI. Only CRO-matched companies are promoted (the dossier keys on company_num anyway).

No-inference: gold carries counts + flags only; the ``enforcement_crawled`` flag distinguishes
"assessed, no events" from "not assessed" so a 0 is never read as a clean bill of health.

Run:  ./.venv/Scripts/python.exe extractors/epa_promote_to_gold.py
"""

from __future__ import annotations

import contextlib
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from services.parquet_io import save_parquet  # noqa: E402

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

SANDBOX = ROOT / "data/sandbox/parquet"
GOLD = ROOT / "data/gold/parquet"

# columns carried to gold (counts + flags + flattened licence portfolio); NO money, NO personal data
_COUNTS = [
    "n_licences",
    "n_enforcement_events",
    "n_incident",
    "n_complaint",
    "n_non_compliance",
    "n_open",
]


def promote_supplier_compliance() -> None:
    src = SANDBOX / "epa_accountability_view.parquet"
    if not src.exists():
        print(f"  !! no accountability view at {src} — run pipeline_sandbox/epa_accountability_view.py first")
        return
    df = pl.read_parquet(src)

    # only CRO-matched COMPANIES (the dossier keys on company_num); DROP named individuals (PII)
    df = df.filter(pl.col("cro_company_num").is_not_null() & ~pl.col("looks_individual"))

    out = df.select(
        pl.col("cro_company_num").cast(pl.Int64).alias("company_num"),
        # flatten the licence portfolio to scalar strings for a simple view + render
        pl.col("licence_classes").list.join(", ").alias("licence_classes"),
        pl.col("licence_statuses").list.join(", ").alias("licence_statuses"),
        pl.col("any_active_licence"),
        pl.col("is_public_body"),
        pl.col("uww_priority_site").cast(pl.Boolean),
        pl.col("enforcement_crawled"),
        pl.col("last_record_date"),
        *[pl.col(c).cast(pl.Int64) for c in _COUNTS],
    )

    # PRIVACY INVARIANT (runtime, -O-proof; BEFORE the write): no named-individual licence holder and no
    # address/location/town/name column may reach committed gold. company_num is the only identity that ships.
    leaked = [c for c in out.columns if any(t in c.lower() for t in ("address", "location", "town", "name", "facility"))]
    if leaked:
        raise RuntimeError(f"PII leak: column(s) {leaked} must not reach gold")

    GOLD.mkdir(parents=True, exist_ok=True)
    dest = GOLD / "epa_supplier_compliance.parquet"
    save_parquet(out, dest)
    crawled = out.filter(pl.col("enforcement_crawled")).height
    with_events = out.filter(pl.col("n_enforcement_events") > 0).height
    print(f"  epa_supplier_compliance -> {dest.relative_to(ROOT)}  ({out.height} companies)")
    print(f"    enforcement-assessed: {crawled} | with >=1 event: {with_events} | columns: {out.columns}")


def main() -> None:
    print("=== PROMOTE EPA supplier-compliance sandbox -> gold ===")
    promote_supplier_compliance()
    print("done.")


if __name__ == "__main__":
    main()
