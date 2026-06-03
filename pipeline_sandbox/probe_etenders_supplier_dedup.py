"""PROBE (throwaway): characterise eTenders supplier-name dirtiness and test
canonicalisation approaches.

Finding that prompted this: the SOURCE data has first-character truncations
("eloitte Ireland LLP" = Deloitte, "ell Products" = Dell, "atapac" = Datapac).
So per-supplier rankings are split across variant spellings. This probe measures
the scale and tests repair strategies (stdlib only — rapidfuzz not installed).

Approaches tested:
  1. lowercase-initial signal  — a real company name rarely starts lowercase;
     it usually means the leading capital was dropped.
  2. suffix-match repair       — truncated name is a tail of a longer canonical
     name (len differs by 1-2): "eloitte Ireland LLP" ⊂ "Deloitte Ireland LLP".
  3. difflib fuzzy cluster     — get_close_matches to the canonical (capitalised) set.
  4. trailing-punctuation tidy — "Accenture,." / "James Harte &".

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/probe_etenders_supplier_dedup.py
Reads the sandbox awards parquet. Writes nothing.
"""

from __future__ import annotations

import difflib
import re
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

AWARDS = ROOT / "data/sandbox/parquet/procurement_awards.parquet"


def hr(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def main() -> None:
    aw = pl.read_parquet(AWARDS)
    sup = (
        aw.select("supplier")
        .filter(pl.col("supplier").str.len_chars() >= 3)
        .group_by("supplier").len().rename({"len": "n_awards"})
    )
    distinct = sup.height
    names = sup["supplier"].to_list()
    name_set = set(names)
    hr("BASIS")
    print(f"distinct supplier spellings: {distinct:,}")

    # --- approach 1: lowercase-initial signal -----------------------------
    lc = sup.filter(pl.col("supplier").str.contains(r"^[a-z]"))
    hr("APPROACH 1 — lowercase-initial (likely dropped leading capital)")
    print(f"names starting lowercase: {lc.height:,}  ({lc.height / distinct:.1%})")
    print(lc.sort("n_awards", descending=True).head(12).to_dicts())

    # --- approach 2: suffix-match repair ----------------------------------
    # for each lowercase-initial name, does Capital+name (any A-Z) exist?
    repaired = []
    longer_lookup = {n.lower(): n for n in names}
    for row in lc.iter_rows(named=True):
        nm = row["supplier"]
        hit = None
        for c in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
            cand = (c + nm).lower()
            if cand in longer_lookup:
                hit = longer_lookup[cand]
                break
        if hit:
            repaired.append((nm, hit, row["n_awards"]))
    hr("APPROACH 2 — suffix-match repair (prepend a capital → known name)")
    print(f"lowercase names repaired to an existing canonical: {len(repaired):,} / {lc.height:,}")
    for nm, hit, n in sorted(repaired, key=lambda x: -x[2])[:12]:
        print(f"  {nm!r}  ->  {hit!r}   (+{n} awards merged)")

    # --- approach 3: difflib fuzzy to canonical set -----------------------
    # canonical = capital-initial names; try to map leftover lowercase via fuzzy
    canon = [n for n in names if re.match(r"^[A-Z0-9]", n)]
    leftover = [nm for nm in lc["supplier"].to_list() if not any(nm == r[0] for r in repaired)]
    fuzzy_hits = 0
    samp = []
    for nm in leftover[:400]:
        m = difflib.get_close_matches(nm, canon, n=1, cutoff=0.85)
        if m:
            fuzzy_hits += 1
            if len(samp) < 10:
                samp.append((nm, m[0]))
    hr("APPROACH 3 — difflib fuzzy (cutoff 0.85) for the residual")
    print(f"residual lowercase names: {len(leftover):,}; fuzzy-matched (sampled 400): {fuzzy_hits}")
    for a, b in samp:
        print(f"  {a!r}  ~>  {b!r}")

    # --- approach 4: trailing punctuation / connective noise --------------
    noisy = sup.filter(pl.col("supplier").str.contains(r"[,.&/]\s*$|\s&$|,\.$"))
    hr("APPROACH 4 — trailing punctuation / dangling connectives")
    print(f"names with trailing punctuation/connective: {noisy.height:,}")
    print(noisy.select("supplier").head(10).to_series().to_list())

    # --- impact estimate --------------------------------------------------
    hr("IMPACT")
    merge_awards = sum(n for _, _, n in repaired)
    print(f"approach-2 alone re-merges {len(repaired):,} spellings ({merge_awards:,} award rows) into existing names")
    print(f"lowercase-initial dirt is {lc.height / distinct:.1%} of distinct suppliers")
    print("recommendation order: tidy trailing punct -> suffix-repair (deterministic, safe)")
    print("  -> difflib fuzzy for residual (review-gated) -> CRO-anchored canonical name as final key")


if __name__ == "__main__":
    main()
