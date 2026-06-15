"""SANDBOX probe: why do big public payees NOT link to a tender, and what is recoverable?

Follow-up to extractors/procurement_award_spend_link.py. That extractor links a payee to a
tender award only on an EXACT entity key (CRO number, else exact normalised name). This probe
takes the payees it leaves UNLINKED and sorts the gap into the two actionable buckets found in
the 2026-06-15 investigation:

  Bucket 2  ENTITY-VARIANT SPLITS  — same firm under a different spelling on each side
            ("PFH Tech Group" paid vs "PFH TECHNOLOGY GROUP" awarded; "Duggan Bros" vs
            "Duggan Brothers Ltd"; "Ove Arup & Partners" vs "Arup"). Recoverable by FUZZY
            name matching — but only as REVIEW candidates, never an auto-join (a wrong merge
            attributes public money to the wrong company — PUBLIC_PAYMENTS_FACT_SCHEMA hard rule).
            Generic procurement words (SERVICES/GROUP/CONSTRUCTION/…) cause false matches, so a
            candidate needs (a) >=1 DISTINCTIVE shared token (rare + non-generic) AND (b) TWO-SIDED
            token overlap >=0.6 — both names mostly explained by what they share, which rejects
            "BAM Schools Bundle Three" ~ "Three Ireland" stub matches. 'high' = overlap >=0.8.

  Bucket 3  NON-SUPPLIER TRANSFERS — the payee is itself a public body / fund, not a vendor
            (Higher Education Authority, Chief State Solicitor's Office, EFSF, Prize Bond Co.).
            These are grant / inter-body transfers that should arguably carry
            supplier_class=public_body and drop out of the supplier ranking entirely. Flagged by
            a conservative public-body name pattern; output is a reclassification REVIEW list.

The residual after 2 & 3 is dominated by genuinely un-tendered PPP/concession SPVs
(NBI Infrastructure DAC, school bundles, road concessions) — no eTenders/TED award notice exists
for them, so they are reported as a residual, not a fixable gap.

Outputs (all under the gitignored sandbox tree — this is a probe, not a pipeline step):
  data/sandbox/parquet/procurement_unlinked_payee_candidates.parquet   (bucket 2 review candidates)
  data/sandbox/parquet/procurement_unlinked_nonsupplier_flags.parquet  (bucket 3 review list)
  data/sandbox/procurement_unlinked_payees_summary.json

Run: ./.venv/Scripts/python.exe pipeline_sandbox/procurement_unlinked_payees_probe.py
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

SPEND_PARQUETS = ["public_payments_fact", "nphdb_payments_fact", "seai_payments_fact", "nta_payments_fact"]
ETENDERS = ROOT / "data/gold/parquet/procurement_awards.parquet"
TED_LAYERS = ["ted_ie_awards", "ted_ie_winner_history"]
OUT_CANDIDATES = ROOT / "data/sandbox/parquet/procurement_unlinked_payee_candidates.parquet"
OUT_NONSUPPLIER = ROOT / "data/sandbox/parquet/procurement_unlinked_nonsupplier_flags.parquet"
OUT_SUMMARY = ROOT / "data/sandbox/procurement_unlinked_payees_summary.json"

# Bucket 3 — tokens that mark a payee as a public body / fund / transfer, not a vendor.
# STRONG tokens almost never occur in a real trading-company name → flag outright. WEAK tokens
# (DEPARTMENT/AGENCY/OFFICE/BOARD/…) DO appear inside vendor names ("Dell Computer … Department",
# "Rating Agency Fees", "X Institute Ltd") so a lone WEAK hit is reported as a lower-confidence
# watchlist, never auto-flagged. Matched on the normalised key's whitespace-split tokens.
STRONG_PUBLIC_TOKENS = frozenset(
    [
        "AUTHORITY",
        "COUNCIL",
        "COMHAIRLE",
        "CONTAE",
        "UNIVERSITY",
        "OLLSCOIL",
        "OIREACHTAS",
        "GARDA",
        "REVENUE",
        "EXCHEQUER",
        "OPW",
        "HSE",
        "TUSLA",
        "TEAGASC",
        "POBAL",
        "FORAS",
        "UISCE",
        "EFSF",
        "ESM",
        "NTMA",
        "NAMA",
        "COMPTROLLER",
        "ROINN",
        "COIMISIUN",
        "GNIOMHAIREACHT",
    ]
)
WEAK_PUBLIC_TOKENS = frozenset(
    [
        "DEPARTMENT",
        "OFFICE",
        "OIFIG",
        "COMMISSION",
        "AGENCY",
        "BOARD",
        "BORD",
        "INSTITUTE",
        "COLLEGE",
        "COLAISTE",
        "HOSPITAL",
        "OSPIDEAL",
        "ETB",
        "MINISTER",
        "AIRE",
        "EXECUTIVE",
        "SOLICITOR",
    ]
)

# Generic procurement words that share across unrelated firms — a match on these alone is noise.
# Used to require at least one DISTINCTIVE (non-generic, low document-frequency) shared token.
GENERIC_TOKENS = frozenset(
    [
        "SERVICES",
        "SERVICE",
        "GROUP",
        "CONSTRUCTION",
        "ENGINEERING",
        "CONTRACTORS",
        "CONTRACTS",
        "MANAGEMENT",
        "CONSULTING",
        "CONSULTANTS",
        "SOLUTIONS",
        "SYSTEMS",
        "TECHNOLOGY",
        "TECHNOLOGIES",
        "BUILDING",
        "CIVIL",
        "DESIGN",
        "BUILD",
        "PROJECT",
        "PROJECTS",
        "INTERNATIONAL",
        "NATIONAL",
        "IRISH",
        "MOBILITY",
        "HEALTHCARE",
        "MEDICAL",
        "SECURITY",
        "PROFESSIONAL",
        "BUSINESS",
        "ADVISORY",
        "CARE",
        "SUPPLIES",
        "PRODUCTS",
        "ENERGY",
        "UTILITIES",
        "FACILITIES",
        # domain words that look rare but matched unrelated firms in the first pass
        "INFRASTRUCTURE",
        "SCHOOLS",
        "SCHOOL",
        "PARTNERS",
        "PARTNERSHIP",
        "PARTNERSHIPS",
        "BUNDLE",
        "ROUTE",
        "TUNNEL",
        "BOND",
        "PPP",
        "ASSET",
        "ASSETS",
        "DEVELOPMENT",
        "OPERATIONS",
        "OPERATION",
        "HOLDING",
        "INVESTMENTS",
    ]
)

RARE_DF = 40  # a shared token is "distinctive" if it appears in <= this many award names


def toks(key: str) -> frozenset[str]:
    return frozenset(t for t in key.split() if len(t) > 2)


def load_spend() -> pl.DataFrame:
    """Payee universe = the rankings-page spend side (mirror of the link extractor's filter)."""
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
    return pay.group_by("k").agg(
        pl.col("supplier_raw").first().alias("payee_name"),
        pl.col("amount_eur").sum().alias("paid_eur"),
    )


def load_award_keys() -> pl.DataFrame:
    """All tender-award winner names (eTenders + both TED layers) as one normalised-key table."""
    frames = []
    et = pl.read_parquet(ETENDERS).filter(pl.col("value_safe_to_sum"))
    frames.append(et.with_columns(name_norm_expr("supplier_raw").alias("k")).select(["k"], supplier=pl.col("supplier")))
    for layer in TED_LAYERS:
        fp = ROOT / f"data/silver/parquet/{layer}.parquet"
        if fp.exists():
            t = pl.read_parquet(fp).filter(pl.col("value_safe_to_sum") & pl.col("winner_name").is_not_null())
            frames.append(
                t.with_columns(name_norm_expr("winner_name").alias("k")).select(["k"], supplier=pl.col("winner_name"))
            )
    aw = pl.concat(frames, how="vertical_relaxed").filter(pl.col("k").str.len_chars() >= 3)
    return aw.group_by("k").agg(pl.col("supplier").first().alias("award_name"))


def main() -> None:
    pay = load_spend()
    aw = load_award_keys()
    award_keys = set(aw["k"].to_list())

    linked = pay.filter(pl.col("k").is_in(list(award_keys)))
    unlinked = pay.filter(~pl.col("k").is_in(list(award_keys)))
    paid_total = pay["paid_eur"].sum()
    print(f"{'=' * 78}\nUNLINKED PAYEE INVESTIGATION\n{'=' * 78}")
    print(f"payee universe : {pay.height:,} entities  €{paid_total:,.0f}")
    print(f"  exact-linked : {linked.height:,}  €{linked['paid_eur'].sum():,.0f}")
    print(f"  UNLINKED     : {unlinked.height:,}  €{unlinked['paid_eur'].sum():,.0f}")

    # ── Bucket 3: non-supplier / public-body transfers ──────────────────────────────────────
    is_strong = pl.col("k").map_elements(lambda k: len(toks(k) & STRONG_PUBLIC_TOKENS) > 0, return_dtype=pl.Boolean)
    is_weak = pl.col("k").map_elements(lambda k: len(toks(k) & WEAK_PUBLIC_TOKENS) > 0, return_dtype=pl.Boolean)
    nonsupplier = unlinked.filter(is_strong).sort("paid_eur", descending=True)
    watchlist = unlinked.filter(~is_strong & is_weak).sort("paid_eur", descending=True)
    rest = unlinked.filter(~is_strong)  # weak hits still eligible for fuzzy (they may be real vendors)
    print(
        f"\n[Bucket 3] STRONG public-body payees (reclassify) : {nonsupplier.height:,}  €{nonsupplier['paid_eur'].sum():,.0f}"
    )
    for r in nonsupplier.head(10).iter_rows(named=True):
        print(f"    €{r['paid_eur']:>13,.0f}  {r['payee_name'][:50]}")
    print(
        f"  WEAK watchlist (review, may be vendors)         : {watchlist.height:,}  €{watchlist['paid_eur'].sum():,.0f}"
    )

    # ── Bucket 2: fuzzy entity-variant candidates (IDF-weighted, distinctive-token gated) ─────
    award_tok = {k: toks(k) for k in award_keys}
    df_count: dict[str, int] = defaultdict(int)
    for ts in award_tok.values():
        for t in ts:
            df_count[t] += 1
    inv: dict[str, list[str]] = defaultdict(list)  # rare token -> award keys (candidate generation)
    for k, ts in award_tok.items():
        for t in ts:
            if df_count[t] <= 400:  # skip ultra-common tokens for candidate generation only
                inv[t].append(k)

    aw_name = dict(zip(aw["k"].to_list(), aw["award_name"].to_list(), strict=True))
    rows = []
    for r in rest.iter_rows(named=True):
        pk = r["k"]
        pt = toks(pk)
        if not pt:
            continue
        cands: set[str] = set()
        for t in pt:
            cands.update(inv.get(t, []))
        best = None
        for ak in cands:
            at = award_tok[ak]
            shared = pt & at
            if not shared:
                continue
            distinctive = [t for t in shared if df_count[t] <= RARE_DF and t not in GENERIC_TOKENS]
            if not distinctive:  # must share at least one rare, non-generic token
                continue
            # Two-sided overlap: BOTH names must be mostly explained by their shared tokens.
            # This is the discriminator that single-token containment missed — it keeps
            # "Ganson Building Civil Engineering" ~ "Ganson Building and Civil Engineering"
            # (both ~fully overlap) while rejecting "BAM Schools Bundle Three" ~ "Three Ireland"
            # and "NTMA" ~ "The PC Agency" (one side is a 1-word stub → low overlap on the other).
            both = min(len(shared) / len(pt), len(shared) / len(at))
            if both < 0.6:
                continue
            if best is None or both > best[1]:
                best = (ak, both, sorted(distinctive))
        if best is not None:
            ak, score, distinctive = best
            tier = "high" if score >= 0.8 else "medium"
            rows.append(
                {
                    "payee_key": pk,
                    "payee_name": r["payee_name"],
                    "paid_eur": r["paid_eur"],
                    "award_key": ak,
                    "award_name": aw_name.get(ak, ""),
                    "overlap": round(score, 2),
                    "distinctive_tokens": " ".join(distinctive),
                    "confidence": tier,
                }
            )
    cand = pl.DataFrame(rows).sort("paid_eur", descending=True) if rows else pl.DataFrame()
    cand_eur = cand["paid_eur"].sum() if cand.height else 0.0
    print(f"\n[Bucket 2] fuzzy variant-split candidates  : {cand.height:,}  €{cand_eur:,.0f} (REVIEW, not auto-merged)")
    if cand.height:
        by_tier = cand.group_by("confidence").agg(pl.len().alias("n"), pl.col("paid_eur").sum().alias("eur"))
        for r in by_tier.sort("eur", descending=True).iter_rows(named=True):
            print(f"    {r['confidence']:<7} {r['n']:>4}  €{r['eur']:,.0f}")
        print("  top candidates:")
        for r in cand.head(18).iter_rows(named=True):
            print(
                f"    [{r['confidence']:<6}] €{r['paid_eur']:>12,.0f}  {r['payee_name'][:30]:<31} ~ "
                f"{r['award_name'][:34]:<35} ({r['distinctive_tokens']})"
            )

    # ── Residual (genuinely un-tendered: PPP/concession SPVs etc.) ───────────────────────────
    matched_keys = set(cand["payee_key"].to_list()) if cand.height else set()
    residual = rest.filter(~pl.col("k").is_in(list(matched_keys))).sort("paid_eur", descending=True)
    print(f"\n[Residual] no exact, no public-body, no fuzzy : {residual.height:,}  €{residual['paid_eur'].sum():,.0f}")
    print("  (dominated by PPP/concession SPVs with no tender-award notice — top:)")
    for r in residual.head(10).iter_rows(named=True):
        print(f"    €{r['paid_eur']:>13,.0f}  {r['payee_name'][:50]}")

    OUT_CANDIDATES.parent.mkdir(parents=True, exist_ok=True)
    if cand.height:
        cand.write_parquet(OUT_CANDIDATES, compression="zstd", compression_level=3, statistics=True)
    nonsupplier.write_parquet(OUT_NONSUPPLIER, compression="zstd", compression_level=3, statistics=True)
    hi = cand.filter(pl.col("confidence") == "high") if cand.height else cand
    summary = {
        "payee_universe_entities": pay.height,
        "payee_universe_eur": float(paid_total),
        "exact_linked_entities": linked.height,
        "exact_linked_eur": float(linked["paid_eur"].sum()),
        "unlinked_entities": unlinked.height,
        "unlinked_eur": float(unlinked["paid_eur"].sum()),
        "bucket3_strong_nonsupplier_entities": nonsupplier.height,
        "bucket3_strong_nonsupplier_eur": float(nonsupplier["paid_eur"].sum()),
        "bucket3_weak_watchlist_entities": watchlist.height,
        "bucket3_weak_watchlist_eur": float(watchlist["paid_eur"].sum()),
        "bucket2_candidate_entities": cand.height,
        "bucket2_candidate_eur": float(cand_eur),
        "bucket2_high_confidence_entities": hi.height if cand.height else 0,
        "bucket2_high_confidence_eur": float(hi["paid_eur"].sum()) if cand.height else 0.0,
        "residual_untendered_entities": residual.height,
        "residual_untendered_eur": float(residual["paid_eur"].sum()),
        "method": "exact entity-key link is the floor. bucket2 = high-precision fuzzy candidates "
        "for MANUAL REVIEW: needs >=2 distinctive (rare, non-generic) shared tokens OR full "
        "token-containment + 1; 'high' = containment, 'medium' = multi-token only. bucket3 = "
        "STRONG public-body token => reclassify to supplier_class=public_body; WEAK token => "
        "watchlist. Nothing is auto-joined (PUBLIC_PAYMENTS_FACT_SCHEMA join gate). Known miss: "
        "abbreviations (BROS vs BROTHERS) are not matched by design — precision over recall.",
    }
    OUT_SUMMARY.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"\nwrote {OUT_CANDIDATES}\nwrote {OUT_NONSUPPLIER}\nwrote {OUT_SUMMARY}")


if __name__ == "__main__":
    main()
