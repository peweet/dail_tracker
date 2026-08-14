"""Company entity crosswalk -> committed gold (the organisation-360 spine).

Additive, net-new gold. Does NOT modify or re-baseline any existing output: it reads
the per-register gold as-is and re-normalises the entity names to the CANONICAL key
(shared/name_norm.name_norm_expr) at build time, so the divergent-normaliser bug
(corporate/charity keys were lowercase / missing NFD -> a raw join to supplier_norm
yielded ~0) is bridged HERE without touching the four source extractors.

Anchor = the procurement-supplier universe (keyed on ``supplier_norm``) — the same key
the company dossier page (utility/pages_code/company.py, /company?supplier=) is entered
on. **Since 2026-07-18 that universe is AWARD suppliers UNION PAYMENT suppliers**; v1 was
award-only, which made the spine blind to companies that are paid but never named as an
award winner (JV / PPP-bundle / operating-subsidiary vehicles). For each supplier it
LEFT-joins, on the canonical key, its cross-register presence:
CRO identity, lobbying footprint, corporate-notice count, charity status, EPA licence.

⚠️ ``awarded_value_safe_eur`` and ``paid_value_safe_eur`` are DIFFERENT MONEY GRAINS
(contracted vs disbursed). Carry both, show both, NEVER sum them — see ``_anchor()``.

⚠️ This is ENTITY resolution, not GROUP resolution. Distinct legal entities of one
corporate group (``BAM CIVIL`` / ``BAM BUILDING`` / ``WILLS BAM JV`` …) stay SEPARATE
rows: they are different names for different legal persons, so the canonical name key
cannot merge them — that needs a parent/subsidiary hierarchy CRO does not publish.
That is exactly the fusion the company page tries to show today but under-matches,
because its corporate panel joins on CRO ``company_num`` only (misses notices that never
got a CRO number but whose name matches a known supplier).

v1 scope is procurement-anchored (the page's universe). A fuller union spine (entities
that appear on the corporate/lobbying registers but are NOT procurement suppliers) +
ministerial-diary panel are the documented follow-ons (doc/archive/ENTITY_CROSSWALK_ORG_DOSSIER_DESIGN.md).

FRAMING (project rule, feedback_no_inference_in_app): co-occurrence by ENTITY only. A
company appearing on several registers is NOT evidence one caused another — there is no
key linking a specific lobby/meeting to a specific contract. Exact normalised-name / CRO
matching UNDERCOUNTS (subsidiary / trading-name variants missed) and short generic names
can collide; treat counts as floors, not verdicts. Sole traders / individuals are already
excluded upstream (supplier_class filter).

Inputs (all committed gold, read-only):
  data/gold/parquet/procurement_awards.parquet
  data/gold/parquet/procurement_payments_fact.parquet (also supplies its own cro_company_num —
    see the CRO note on _payment_suppliers())
  data/gold/parquet/procurement_supplier_cro_match.parquet
  data/gold/parquet/procurement_lobbying_overlap.parquet
  data/gold/parquet/corporate_notices.parquet
  data/gold/parquet/charities_enriched.parquet
  data/gold/parquet/epa_supplier_compliance.parquet

Outputs (committed gold):
  data/gold/parquet/supplier_entity_xref.parquet
  data/_meta/supplier_entity_xref_coverage.json

Run:  ./.venv/Scripts/python.exe extractors/entity_xref_build.py
"""

from __future__ import annotations

import contextlib
import json
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from services.parquet_io import save_parquet  # noqa: E402

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

from shared.name_norm import name_norm_expr  # noqa: E402

GOLD = ROOT / "data/gold/parquet"
AWARDS = GOLD / "procurement_awards.parquet"
PAYMENTS = GOLD / "procurement_payments_fact.parquet"
CRO_MATCH = GOLD / "procurement_supplier_cro_match.parquet"
OVERLAP = GOLD / "procurement_lobbying_overlap.parquet"
CORP = GOLD / "corporate_notices.parquet"
CHAR = GOLD / "charities_enriched.parquet"
EPA = GOLD / "epa_supplier_compliance.parquet"
OUT = GOLD / "supplier_entity_xref.parquet"
OUT_COV = ROOT / "data/_meta/supplier_entity_xref_coverage.json"

MIN_LEN = 4  # kill single-token collisions (same floor as procurement_lobbying_xref)


def hr(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def _award_suppliers() -> pl.DataFrame:
    """Award-side anchor: one row per company-class, non-truncated supplier_norm."""
    aw = pl.read_parquet(AWARDS)
    return (
        aw.filter((pl.col("supplier_class") == "company") & ~pl.col("name_truncated"))
        .filter(pl.col("supplier_norm").str.len_chars() >= MIN_LEN)
        .group_by("supplier_norm")
        .agg(
            pl.col("supplier").mode().first().alias("display_name_award"),
            pl.len().alias("procurement_award_rows"),
            pl.col("value_eur").filter(pl.col("value_safe_to_sum")).sum().alias("awarded_value_safe_eur"),
        )
    )


def _payment_suppliers() -> pl.DataFrame:
    """Payment-side anchor: one row per company-class payment supplier.

    Added 2026-07-18. The v1 spine anchored on AWARDS ONLY, which made it blind to any
    company that is paid but never appears as a named award winner — the majority case for
    JV / PPP-bundle / operating-subsidiary vehicles. Measured before this change: BAM's spine
    row showed €115.9m awarded and €0 paid, while €942.7m of actual BAM payments sat in seven
    entities (BAM CIVIL, BAM BUILDING, WILLS BAM JV, BAM SCHOOLS BUNDLE THREE, BAM COURTS
    BUNDLE, BAM GLASGIVEN JV, BAM) that were ABSENT from the spine entirely.

    PRIVACY: filtered to ``supplier_class == 'company'``, which is what keeps natural persons
    out of a public cross-register spine (the award side excludes them the same way). Verified
    2026-07-18: every company-class payment row carries ``privacy_status='ok'`` and
    ``public_display=true``, so this filter alone satisfies the privacy rail — 6,042
    sole-trader/individual, 1,345 public-body and 820 id-code names stay out by construction.

    NOTE the asymmetry with the award side: the payments fact has NO ``name_truncated`` flag,
    so truncated payment names cannot be excluded the way award names are. The MIN_LEN floor
    is the only guard here; treat payment-only display names as less clean.

    CRO: the payments fact already carries its own ``cro_company_num`` — an exact
    normalised-name join against the CRO register done in
    extractors/procurement_payments_consolidate.py's ``_attach_cro()``, independent of the
    award-side match in procurement_supplier_cro_match.parquet. Until 2026-08-14 this column
    was read from PAYMENTS but silently dropped here, so every payment-only supplier (no
    award row) showed ``has_cro=False`` even when this exact match existed — the anchor union
    landed 2026-07-18 but never picked it up. Carried through as ``cro_company_num_from_payments``
    and coalesced onto the award-side match in ``main()`` (award-side wins when both exist,
    since procurement_supplier_cro_match.parquet explicitly resolves exact/ambiguous; this
    column does not).
    """
    return (
        pl.read_parquet(
            PAYMENTS,
            columns=[
                "supplier_normalised",
                "supplier_raw",
                "supplier_class",
                "amount_eur",
                "value_safe_to_sum",
                "cro_company_num",
            ],
        )
        .filter(pl.col("supplier_class") == "company")
        .filter(pl.col("supplier_normalised").is_not_null())
        .filter(pl.col("supplier_normalised").str.len_chars() >= MIN_LEN)
        .group_by("supplier_normalised")
        .agg(
            pl.col("supplier_raw").mode().first().alias("display_name_payment"),
            pl.len().alias("payment_rows"),
            pl.col("amount_eur").filter(pl.col("value_safe_to_sum")).sum().alias("paid_value_safe_eur"),
            pl.col("cro_company_num").drop_nulls().first().alias("cro_company_num_from_payments"),
        )
        .rename({"supplier_normalised": "supplier_norm"})
    )


def _anchor() -> pl.DataFrame:
    """The anchor universe = award suppliers ∪ payment suppliers (both company-class).

    ⚠️ THREE-GRAIN RULE: ``awarded_value_safe_eur`` (what was CONTRACTED) and
    ``paid_value_safe_eur`` (what was PAID OUT) are DIFFERENT MONEY GRAINS and must NEVER be
    summed or added together — see [[reference_data_map]]. They are carried side by side so a
    consumer can show both, never one total. A supplier legitimately has one without the other:
    an award with no payment record, or payments under a vehicle that never won a named award.
    """
    aw = _award_suppliers()
    pay = _payment_suppliers()
    return (
        aw.join(pay, on="supplier_norm", how="full", coalesce=True)
        .with_columns(
            pl.col("procurement_award_rows").fill_null(0),
            pl.col("payment_rows").fill_null(0),
        )
        .with_columns(
            in_awards=pl.col("procurement_award_rows") > 0,
            in_payments=pl.col("payment_rows") > 0,
            # Prefer the award-side display name (cleaner: truncation-filtered) and fall
            # back to the payment-side raw name for payment-only entities.
            display_name=pl.coalesce("display_name_award", "display_name_payment"),
        )
        .drop("display_name_award", "display_name_payment")
    )


def _cro() -> pl.DataFrame:
    return pl.read_parquet(CRO_MATCH, columns=["supplier_norm", "company_num"]).unique(subset=["supplier_norm"])


def _lobbying() -> pl.DataFrame:
    """Total distinct lobbying returns per supplier (overlap is one row per lobby entity)."""
    return (
        pl.read_parquet(OVERLAP, columns=["supplier_norm", "n_lobby_returns"])
        .group_by("supplier_norm")
        .agg(pl.col("n_lobby_returns").sum().alias("lobby_returns"))
    )


def _corporate() -> pl.DataFrame:
    """Corporate-notice count per CANONICAL entity key (re-normed from raw entity_name)."""
    return (
        pl.read_parquet(CORP, columns=["entity_name"])
        .with_columns(supplier_norm=name_norm_expr("entity_name"))
        .filter(pl.col("supplier_norm").str.len_chars() >= MIN_LEN)
        .group_by("supplier_norm")
        .agg(pl.len().alias("corporate_notices"))
    )


def _charities() -> pl.DataFrame:
    return (
        pl.read_parquet(CHAR, columns=["registered_charity_name"])
        .with_columns(supplier_norm=name_norm_expr("registered_charity_name"))
        .filter(pl.col("supplier_norm").str.len_chars() >= MIN_LEN)
        .select("supplier_norm")
        .unique()
        .with_columns(is_charity=pl.lit(value=True))
    )


def _epa() -> pl.DataFrame:
    return (
        pl.read_parquet(EPA, columns=["company_num", "n_licences"])
        .filter(pl.col("n_licences") > 0)
        .select("company_num")
        .unique()
        .with_columns(has_epa_licence=pl.lit(value=True))
    )


def main() -> None:
    sup = _anchor()
    hr("PROCUREMENT SUPPLIER ANCHOR (company-class, matchable)")
    print(f"distinct suppliers: {sup.height:,}")
    print(
        f"  award-side: {int(sup['in_awards'].sum()):,} | payment-side: {int(sup['in_payments'].sum()):,} | "
        f"both: {int((sup['in_awards'] & sup['in_payments']).sum()):,}"
    )

    xref = (
        sup.join(_cro(), on="supplier_norm", how="left")
        # Award-side match (procurement_supplier_cro_match.parquet) wins when both exist — it
        # explicitly resolves exact/ambiguous; the payments fact's own match does not. Fills the
        # gap for payment-only suppliers, who have no row in the award-side table at all.
        .with_columns(pl.coalesce("company_num", "cro_company_num_from_payments").alias("company_num"))
        .drop("cro_company_num_from_payments")
        .join(_lobbying(), on="supplier_norm", how="left")
        .join(_corporate(), on="supplier_norm", how="left")
        .join(_charities(), on="supplier_norm", how="left")
        .join(_epa(), on="company_num", how="left")
        .with_columns(
            pl.col("lobby_returns").fill_null(0),
            pl.col("corporate_notices").fill_null(0),
            pl.col("is_charity").fill_null(value=False),
            pl.col("has_epa_licence").fill_null(value=False),
        )
        .with_columns(
            has_cro=pl.col("company_num").is_not_null(),
            on_lobbying_register=pl.col("lobby_returns") > 0,
            has_corporate_notice=pl.col("corporate_notices") > 0,
        )
        .with_columns(
            # How many registers BEYOND procurement this entity co-occurs on (0-4).
            cross_register_count=(
                pl.col("on_lobbying_register").cast(pl.Int32)
                + pl.col("has_corporate_notice").cast(pl.Int32)
                + pl.col("is_charity").cast(pl.Int32)
                + pl.col("has_epa_licence").cast(pl.Int32)
            )
        )
        .select(
            "supplier_norm",
            "display_name",
            "company_num",
            "has_cro",
            "in_awards",
            "in_payments",
            "procurement_award_rows",
            # ⚠️ NEVER add these two together — awarded (contracted) and paid (disbursed)
            # are different money grains. See _anchor() and [[reference_data_map]].
            "awarded_value_safe_eur",
            "payment_rows",
            "paid_value_safe_eur",
            "on_lobbying_register",
            "lobby_returns",
            "has_corporate_notice",
            "corporate_notices",
            "is_charity",
            "has_epa_licence",
            "cross_register_count",
        )
        # Rank by cross-register reach, then by the larger of the two money grains — a
        # max(), NOT a sum: adding awarded to paid would breach the three-grain rule.
        .sort(
            ["cross_register_count", "awarded_value_safe_eur", "paid_value_safe_eur"],
            descending=True,
            nulls_last=True,
        )
    )

    # Row floor: the anchor is thousands of suppliers; a tiny frame means a broken input.
    save_parquet(xref, OUT, min_rows=1000)

    n_multi = int((xref["cross_register_count"] >= 2).sum())
    OUT_COV.write_text(
        json.dumps(
            {
                "supplier_entities": xref.height,
                "in_awards": int(xref["in_awards"].sum()),
                "in_payments": int(xref["in_payments"].sum()),
                "in_both": int((xref["in_awards"] & xref["in_payments"]).sum()),
                "with_cro": int(xref["has_cro"].sum()),
                "on_lobbying_register": int(xref["on_lobbying_register"].sum()),
                "with_corporate_notice": int(xref["has_corporate_notice"].sum()),
                "is_charity": int(xref["is_charity"].sum()),
                "has_epa_licence": int(xref["has_epa_licence"].sum()),
                "on_2plus_extra_registers": n_multi,
                "anchor": "procurement supplier universe (supplier_norm), company-class: AWARD suppliers "
                "(non-truncated) UNION PAYMENT suppliers. Extended from award-only 2026-07-18.",
                "match_method": "exact CANONICAL normalised-name (shared/name_norm.name_norm_expr) + CRO "
                "company_num. Award-side suppliers match via procurement_supplier_cro_match.parquet "
                "(exact_unique/ambiguous resolved); payment-only suppliers match via the exact "
                "name_norm join already attached to procurement_payments_fact.parquet by "
                "procurement_payments_consolidate.py (no ambiguity resolution) — added 2026-08-14.",
                "never_sum": "awarded_value_safe_eur and paid_value_safe_eur are DIFFERENT MONEY GRAINS "
                "(contracted vs disbursed) — show side by side, NEVER add together.",
                "caveat": "Co-occurrence by ENTITY only — the SAME organisation appears on several public "
                "registers. NOT evidence one caused another; there is no key linking a specific lobby or "
                "meeting to a specific contract. Exact normalised-name / CRO matching UNDERCOUNTS (subsidiary "
                "and trading-name variants missed) and short generic names can collide — counts are floors, "
                "not verdicts. Sole traders / individuals excluded upstream. Distinct legal entities, JVs and "
                "PPP vehicles of the same corporate GROUP remain SEPARATE rows — name normalisation cannot "
                "merge them (that needs a parent/subsidiary hierarchy CRO does not publish), so a group's "
                "true total is spread across rows.",
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    hr("SUPPLIER ENTITY XREF -> gold")
    print(f"rows: {xref.height:,} | with CRO: {int(xref['has_cro'].sum()):,}")
    print(
        f"lobbying: {int(xref['on_lobbying_register'].sum()):,} | "
        f"corporate notice: {int(xref['has_corporate_notice'].sum()):,} | "
        f"charity: {int(xref['is_charity'].sum()):,} | epa: {int(xref['has_epa_licence'].sum()):,}"
    )
    print(f"on >=2 extra registers: {n_multi:,}")
    pl.Config.set_fmt_str_lengths(34)
    pl.Config.set_tbl_rows(12)
    print(
        xref.filter(pl.col("cross_register_count") >= 2)
        .select("display_name", "company_num", "lobby_returns", "corporate_notices", "is_charity", "has_epa_licence")
        .head(12)
    )
    print(f"\nwrote {OUT}\nwrote coverage {OUT_COV}")


if __name__ == "__main__":
    main()
