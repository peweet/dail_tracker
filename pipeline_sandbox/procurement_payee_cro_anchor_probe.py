"""SANDBOX probe: recover Bucket-2 entity-variant links via CRO-NUMBER ANCHORING.

Follow-up to procurement_unlinked_payees_probe.py. That probe matched unlinked payee names
DIRECTLY against award winner names (string-to-string) — workable but noisy, because both sides
are messy extracted strings. This probe is the higher-precision route recommended in the
2026-06-15 investigation: anchor through the CRO company register instead.

The idea — and why it is safe where blind fuzzy matching is not:
  1. Resolve every TENDER WINNER (eTenders + both TED layers) to a CRO company_num via the
     exact-unique normalised-name map (the same map procurement_award_spend_link.py uses). This
     yields AWARD_CRO = the set of registered companies that demonstrably won a tender.
  2. Build a SMALL dictionary = the CRO register rows for those company_nums only (a few thousand
     canonical legal names), NOT the full 817k register.
  3. Fuzzy-match each UNLINKED payee name against that small dictionary of canonical names.
  A match only counts when the payee resolves to a company that INDEPENDENTLY appears in the award
  data — double corroboration (payee≈registered-name AND that exact company won a tender). A random
  fuzzy hit won't coincidentally be a tender winner, so the false-recovery rate is far below blind
  payee↔award string matching. Matching against the canonical CRO name (complete legal name) also
  beats matching against a truncated/abbreviated award string.

Gate (high precision): >=1 distinctive (rare-in-dictionary, non-generic) shared token AND two-sided
token overlap >= 0.7. Still REVIEW candidates, never auto-joined (PUBLIC_PAYMENTS_FACT_SCHEMA gate).

Outputs (gitignored sandbox tree):
  data/sandbox/parquet/procurement_payee_cro_anchor_candidates.parquet
  data/sandbox/procurement_payee_cro_anchor_summary.json

Run: ./.venv/Scripts/python.exe pipeline_sandbox/procurement_payee_cro_anchor_probe.py
"""

from __future__ import annotations

import contextlib
import json
import sys
from collections import defaultdict
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from shared.name_norm import name_norm_expr  # noqa: E402

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

CRO_REGISTER = ROOT / "data/silver/cro/companies.parquet"
SPEND_PARQUETS = ["public_payments_fact", "nphdb_payments_fact", "seai_payments_fact", "nta_payments_fact"]
ETENDERS = ROOT / "data/gold/parquet/procurement_awards.parquet"
TED_LAYERS = ["ted_ie_awards", "ted_ie_winner_history"]
OUT_CANDIDATES = ROOT / "data/sandbox/parquet/procurement_payee_cro_anchor_candidates.parquet"
OUT_SUMMARY = ROOT / "data/sandbox/procurement_payee_cro_anchor_summary.json"

# reuse the generic-procurement stoplist from the sibling probe (a lone generic shared token is noise)
from pipeline_sandbox.procurement_unlinked_payees_probe import GENERIC_TOKENS  # noqa: E402

RARE_DF = 25  # distinctive = appears in <= this many winner-dictionary names
MIN_OVERLAP = 0.7  # two-sided token overlap floor (stricter than the string-vs-string probe's 0.6)


def toks(key: str) -> frozenset[str]:
    return frozenset(t for t in key.split() if len(t) > 2)


def load_cro_exact_unique() -> pl.DataFrame:
    """EXACT-UNIQUE normalised-name -> company_num map (names mapping to >1 company dropped)."""
    cro = (
        pl.read_parquet(CRO_REGISTER)
        .select(["name_norm", "company_num"])
        .filter(pl.col("name_norm").is_not_null() & (pl.col("name_norm").str.len_chars() >= 4))
    )
    counts = cro.group_by("name_norm").agg(pl.col("company_num").n_unique().alias("n"))
    keep = counts.filter(pl.col("n") == 1).select("name_norm")
    return cro.join(keep, on="name_norm", how="inner").unique("name_norm")


def award_company_nums(cro_map: pl.DataFrame) -> tuple[set[int], set[str]]:
    """Companies that won a tender: (set of CRO company_num, set of award normalised names)."""
    names = []
    et = pl.read_parquet(ETENDERS).filter(pl.col("value_safe_to_sum"))
    names.append(et.with_columns(name_norm_expr("supplier_raw").alias("k")).select("k"))
    for layer in TED_LAYERS:
        fp = ROOT / f"data/silver/parquet/{layer}.parquet"
        if fp.exists():
            t = pl.read_parquet(fp).filter(pl.col("value_safe_to_sum") & pl.col("winner_name").is_not_null())
            names.append(t.with_columns(name_norm_expr("winner_name").alias("k")).select("k"))
    award_norms = pl.concat(names).filter(pl.col("k").str.len_chars() >= 3).unique()
    resolved = award_norms.join(cro_map.rename({"name_norm": "k"}), on="k", how="left")
    cro_set = set(resolved.filter(pl.col("company_num").is_not_null())["company_num"].to_list())
    return cro_set, set(award_norms["k"].to_list())


def load_spend(cro_map: pl.DataFrame) -> pl.DataFrame:
    """Payee universe with exact-unique CRO number attached (mirror of the link extractor's filter)."""
    parts = []
    for p in SPEND_PARQUETS:
        fp = ROOT / f"data/silver/parquet/{p}.parquet"
        if fp.exists():
            d = pl.read_parquet(fp)
            keep = ["supplier_raw", "supplier_class", "amount_eur", "value_safe_to_sum", "extraction_confidence"]
            parts.append(d.select([c for c in keep if c in d.columns]))
    pay = pl.concat(parts, how="vertical_relaxed").filter(
        pl.col("value_safe_to_sum")
        & (pl.col("supplier_class") != "public_body")
        & (pl.col("extraction_confidence") != "low")
        & pl.col("supplier_raw").is_not_null()
        & (pl.col("supplier_raw").str.strip_chars() != "")
    )
    pay = pay.with_columns(name_norm_expr("supplier_raw").alias("k")).filter(pl.col("k").str.len_chars() >= 3)
    pay = pay.group_by("k").agg(
        pl.col("supplier_raw").first().alias("payee_name"),
        pl.col("amount_eur").sum().alias("paid_eur"),
    )
    return pay.join(cro_map.rename({"name_norm": "k"}), on="k", how="left")


def main() -> None:
    cro_map = load_cro_exact_unique()
    award_cro, award_norms = award_company_nums(cro_map)
    pay = load_spend(cro_map)

    # Production link (procurement_award_spend_link.py) = entity-key match: CRO num in award set,
    # else exact normalised name in award names. Replicate it to isolate what is NEWLY recovered.
    # fill_null(False): company_num is null for non-CRO-resolved payees; is_in() returns null
    # there, and ~null would silently DROP those rows from both linked and unlinked.
    already = pl.col("company_num").is_in(list(award_cro)).fill_null(False) | pl.col("k").is_in(list(award_norms))
    linked = pay.filter(already)
    unlinked = pay.filter(~already)
    print(f"{'=' * 78}\nCRO-ANCHOR RECOVERY OF UNLINKED PAYEES\n{'=' * 78}")
    print(f"tender-winning companies with a CRO number : {len(award_cro):,}")
    print(f"payee universe   : {pay.height:,}  €{pay['paid_eur'].sum():,.0f}")
    print(f"  already linked : {linked.height:,}  €{linked['paid_eur'].sum():,.0f}")
    print(f"  unlinked       : {unlinked.height:,}  €{unlinked['paid_eur'].sum():,.0f}")

    # Dictionary = canonical CRO names of tender-winning companies only (a few thousand rows).
    reg = (
        pl.read_parquet(CRO_REGISTER)
        .filter(pl.col("company_num").is_in(list(award_cro)) & pl.col("name_norm").is_not_null())
        .unique("company_num")
        .select(["company_num", "company_name", "name_norm", "company_status"])
    )
    dict_tok = {
        r["name_norm"]: (r["company_num"], r["company_name"], toks(r["name_norm"])) for r in reg.iter_rows(named=True)
    }
    df_count: dict[str, int] = defaultdict(int)
    for _, _, ts in dict_tok.values():
        for t in ts:
            df_count[t] += 1
    inv: dict[str, list[str]] = defaultdict(list)
    for nn, (_, _, ts) in dict_tok.items():
        for t in ts:
            inv[t].append(nn)
    print(f"winner-name dictionary (canonical CRO names): {reg.height:,}")

    rows = []
    for r in unlinked.iter_rows(named=True):
        pt = toks(r["k"])
        if not pt:
            continue
        cands: set[str] = set()
        for t in pt:
            cands.update(inv.get(t, []))
        best = None
        for nn in cands:
            cnum, cname, at = dict_tok[nn]
            shared = pt & at
            if not shared:
                continue
            distinctive = [t for t in shared if df_count[t] <= RARE_DF and t not in GENERIC_TOKENS]
            if not distinctive:
                continue
            overlap = min(len(shared) / len(pt), len(shared) / len(at))
            if overlap < MIN_OVERLAP:
                continue
            if best is None or overlap > best[1]:
                best = (cnum, overlap, cname, nn, sorted(distinctive))
        if best is not None:
            cnum, overlap, cname, nn, distinctive = best
            rows.append(
                {
                    "payee_key": r["k"],
                    "payee_name": r["payee_name"],
                    "paid_eur": r["paid_eur"],
                    "cro_company_num": cnum,
                    "cro_company_name": cname,
                    "cro_name_norm": nn,
                    "overlap": round(overlap, 2),
                    "distinctive_tokens": " ".join(distinctive),
                    "confidence": "high" if overlap >= 0.85 else "medium",
                }
            )
    cand = pl.DataFrame(rows).sort("paid_eur", descending=True) if rows else pl.DataFrame()
    rec_eur = cand["paid_eur"].sum() if cand.height else 0.0
    print(f"\nCRO-ANCHORED recoveries (unlinked -> a tender-winning company): {cand.height:,}  €{rec_eur:,.0f}")
    if cand.height:
        for r in (
            cand.group_by("confidence")
            .agg(pl.len().alias("n"), pl.col("paid_eur").sum().alias("e"))
            .sort("e", descending=True)
            .iter_rows(named=True)
        ):
            print(f"    {r['confidence']:<7} {r['n']:>4}  €{r['e']:,.0f}")
        print("  top recoveries (payee  ->  CRO-registered winner):")
        for r in cand.head(20).iter_rows(named=True):
            print(
                f"    [{r['confidence']:<6}] €{r['paid_eur']:>12,.0f}  {r['payee_name'][:30]:<31} -> "
                f"{r['cro_company_name'][:36]:<37} (#{r['cro_company_num']})"
            )

    new_link_eur = linked["paid_eur"].sum() + rec_eur
    print(
        f"\nmoney-linkage: exact €{linked['paid_eur'].sum():,.0f} "
        f"+ CRO-anchored €{rec_eur:,.0f} = €{new_link_eur:,.0f} "
        f"({100 * new_link_eur / pay['paid_eur'].sum():.1f}% of payee universe, up from "
        f"{100 * linked['paid_eur'].sum() / pay['paid_eur'].sum():.1f}%)"
    )

    OUT_CANDIDATES.parent.mkdir(parents=True, exist_ok=True)
    if cand.height:
        cand.write_parquet(OUT_CANDIDATES, compression="zstd", compression_level=3, statistics=True)
    summary = {
        "tender_winning_companies_with_cro": len(award_cro),
        "payee_universe_entities": pay.height,
        "payee_universe_eur": float(pay["paid_eur"].sum()),
        "already_linked_entities": linked.height,
        "already_linked_eur": float(linked["paid_eur"].sum()),
        "cro_anchored_recovered_entities": cand.height,
        "cro_anchored_recovered_eur": float(rec_eur),
        "cro_anchored_high_conf_entities": int(cand.filter(pl.col("confidence") == "high").height)
        if cand.height
        else 0,
        "combined_linked_eur": float(new_link_eur),
        "combined_linkage_pct_of_eur": round(100 * new_link_eur / pay["paid_eur"].sum(), 1),
        "method": "anchor unlinked payees through the CRO register to companies that INDEPENDENTLY "
        "won a tender (double corroboration). Fuzzy gate: >=1 distinctive shared token AND two-sided "
        "overlap >=0.7. REVIEW candidates only — never auto-joined (PUBLIC_PAYMENTS_FACT_SCHEMA gate).",
    }
    OUT_SUMMARY.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"\nwrote {OUT_CANDIDATES}\nwrote {OUT_SUMMARY}")


if __name__ == "__main__":
    main()
